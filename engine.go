package aether

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BabySid/aether/artifact"
	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/expr"
	"github.com/BabySid/aether/hook"
	"github.com/BabySid/aether/idgen"
	"github.com/BabySid/aether/internal"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/secret"
	"github.com/BabySid/aether/store"
)

// Engine is the core workflow scheduling engine.
// All dependencies are injected via Option.
type Engine struct {
	store         store.Store
	executorReg   *executor.Registry
	exprEvaluator expr.Evaluator
	taskBroker    broker.TaskBroker
	artifactStore artifact.Store
	secretStore   secret.Store
	hookNotifier  hook.Notifier
	idGen         idgen.Generator
}

// New creates an Engine with the given options.
func New(opts ...Option) (*Engine, error) {
	e := &Engine{}
	for _, opt := range opts {
		opt(e)
	}

	// Validate required dependencies
	if e.store == nil {
		return nil, fmt.Errorf("aether: %w: Store is required, use WithStore()", ErrValidation)
	}
	if e.executorReg == nil || len(e.executorReg.Types()) == 0 {
		return nil, fmt.Errorf("aether: %w: at least one ExecutorPlugin is required, use WithExecutor()", ErrValidation)
	}
	if e.idGen == nil {
		return nil, fmt.Errorf("aether: %w: IDGenerator is required, use WithIDGenerator()", ErrValidation)
	}
	if e.taskBroker == nil {
		return nil, fmt.Errorf("aether: %w: TaskBroker is required, use WithTaskBroker()", ErrValidation)
	}

	return e, nil
}

// Submit parses, validates, and dispatches a workflow for execution.
// Returns the workflow run ID. Non-blocking.
//
// All checks (marshal, fill defaults, validate, resolve entrypoint) are
// performed before any state is persisted to Store.
func (e *Engine) Submit(ctx context.Context, wf *model.Workflow) (uint64, error) {
	// 1. Nil check
	if wf == nil {
		return 0, fmt.Errorf("aether: %w: workflow must not be nil", ErrValidation)
	}

	// 2. Marshal raw workflow JSON (immutable snapshot)
	rawJSON, err := json.Marshal(wf)
	if err != nil {
		return 0, fmt.Errorf("aether: marshal workflow: %w", err)
	}

	// 3. Fill defaults
	internal.FillDefaults(wf)

	// 4. Validate (structural + semantic)
	if err := internal.Validate(wf); err != nil {
		return 0, fmt.Errorf("aether: %w: %v", ErrValidation, err)
	}

	// 5. Resolve entrypoint template
	entry := internal.FindTemplate(wf, wf.Spec.Entrypoint)
	if entry == nil {
		return 0, fmt.Errorf("aether: %w: entrypoint template %q not found", ErrValidation, wf.Spec.Entrypoint)
	}

	// 6. Determine entrypoint template type
	templateType := internal.ResolveTemplateType(entry)
	if templateType == "" {
		return 0, fmt.Errorf("aether: %w: entrypoint template %q has no dag/executor/loop", ErrValidation, wf.Spec.Entrypoint)
	}

	// --- All checks passed. Now persist and dispatch. ---

	// 7. Generate workflow run ID and persist WorkflowRun
	workflowRunID := e.idGen.Generate()
	run := &store.WorkflowRun{
		RunID:    workflowRunID,
		Workflow: rawJSON,
		Status:   model.PhaseRunning,
	}
	if err := e.store.CreateWorkflowRun(ctx, run); err != nil {
		return 0, fmt.Errorf("aether: create workflow run: %w", err)
	}

	// 8. Create entry TaskRun (Pending, ParentRunID=0)
	entryTaskRun := &store.TaskRun{
		RunID:         e.idGen.Generate(),
		WorkflowRunID: workflowRunID,
		ParentRunID:   0,
		Depth:         0,
		TaskName:      wf.Spec.Entrypoint,
		TemplateName:  wf.Spec.Entrypoint,
		TemplateType:  templateType,
		Status:        model.PhasePending,
	}
	if err := e.store.CreateTaskRun(ctx, entryTaskRun); err != nil {
		return 0, fmt.Errorf("aether: create entry task run: %w", err)
	}

	// 9. Start scheduling via advanceScope
	if err := e.advanceScope(ctx, workflowRunID, wf, 0); err != nil {
		return 0, fmt.Errorf("aether: %w", err)
	}

	return workflowRunID, nil
}

// Get retrieves the current execution state of a workflow. Non-blocking.
func (e *Engine) Get(ctx context.Context, workflowID uint64) (*WorkflowExecution, error) {
	// 1. Get workflow run
	run, err := e.store.GetWorkflowRun(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("aether: %w", err)
	}

	// 2. Get all task runs
	taskRuns, err := e.store.ListTaskRuns(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("aether: list task runs: %w", err)
	}

	// 3. Assemble WorkflowExecution
	exec := &WorkflowExecution{
		WorkflowID: run.RunID,
		Phase:      run.Status,
		Msg:        run.Message,
		Outputs:    run.Outputs,
		Metrics:    run.Metrics,
		Tasks:      make([]TaskExecution, 0, len(taskRuns)),
	}

	totalTasks := len(taskRuns)
	completedTasks := 0
	for _, tr := range taskRuns {
		exec.Tasks = append(exec.Tasks, TaskExecution{
			TaskID:   tr.RunID,
			Name:     tr.TaskName,
			Path:     tr.Path,
			Template: tr.TemplateName,
			Phase:    tr.Status,
			Metrics:  tr.Metrics,
		})
		if tr.Status.IsTerminal() {
			completedTasks++
		}
	}

	if totalTasks > 0 {
		exec.Progress = fmt.Sprintf("%d/%d", completedTasks, totalTasks)
	}

	return exec, nil
}

// Resume resumes an awaiting task and progresses the DAG. Non-blocking.
func (e *Engine) Resume(ctx context.Context, workflowID uint64, taskID uint64, payload map[string]any) error {
	// 1. Get the task run
	tr, err := e.store.GetTaskRun(ctx, taskID)
	if err != nil {
		return fmt.Errorf("aether: %w", err)
	}

	// 2. Validate
	if tr.WorkflowRunID != workflowID {
		return fmt.Errorf("aether: %w: task %d does not belong to workflow %d", ErrInvalidState, taskID, workflowID)
	}
	if tr.Status != model.PhaseRunning {
		return fmt.Errorf("aether: %w: task %d is not in Running phase (current: %s)", ErrInvalidState, taskID, tr.Status)
	}

	// 3. Build outputs from payload and mark as succeeded
	outputs := &model.Outputs{
		Phase: model.PhaseSucceeded,
	}
	if payload != nil {
		params := make([]model.Parameter, 0, len(payload))
		for k, v := range payload {
			valJSON, _ := json.Marshal(v)
			params = append(params, model.Parameter{
				Name:  k,
				Value: valJSON,
			})
		}
		outputs.Parameters = params
	}

	tr.Status = model.PhaseSucceeded
	tr.Outputs = outputs
	if err := e.store.UpdateTaskRun(ctx, tr); err != nil {
		return fmt.Errorf("aether: update task run: %w", err)
	}

	// 4. Advance the scope this task belongs to
	wf, err := e.loadWorkflow(ctx, tr.WorkflowRunID)
	if err != nil {
		return err
	}
	return e.advanceScope(ctx, tr.WorkflowRunID, wf, tr.ParentRunID)
}

// Cancel cancels a running workflow. Non-blocking.
func (e *Engine) Cancel(ctx context.Context, workflowID uint64) error {
	// 1. Get workflow run
	run, err := e.store.GetWorkflowRun(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("aether: %w", err)
	}
	if run.Status.IsTerminal() {
		return fmt.Errorf("aether: %w: workflow %d already in terminal state %s", ErrInvalidState, workflowID, run.Status)
	}

	// 2. List all task runs
	taskRuns, err := e.store.ListTaskRuns(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("aether: list task runs: %w", err)
	}

	// 3. Cancel/skip non-terminal tasks
	for _, tr := range taskRuns {
		if tr.Status.IsTerminal() {
			continue
		}
		switch tr.Status {
		case model.PhaseRunning:
			_ = e.taskBroker.Cancel(ctx, tr.RunID)
			tr.Status = model.PhaseError
			tr.Message = "cancelled by user"
			_ = e.store.UpdateTaskRun(ctx, tr)
		case model.PhasePending:
			tr.Status = model.PhaseSkipped
			tr.Message = "cancelled by user"
			_ = e.store.UpdateTaskRun(ctx, tr)
		}
	}

	// 4. Mark workflow as Error and fire hooks
	_ = e.store.UpdateWorkflowRunStatus(ctx, workflowID, model.PhaseError, "cancelled by user")

	var wf model.Workflow
	if err := json.Unmarshal(run.Workflow, &wf); err == nil {
		internal.FillDefaults(&wf)
		internal.FireWorkflowHooks(ctx, e.hookNotifier, &wf, workflowID, model.PhaseError)
	}

	return nil
}

// OnTaskCompleted is invoked when a task finishes execution.
// It persists the result, fires task-level hooks, then re-evaluates the task's
// scope to dispatch newly-unblocked tasks or finalize the workflow.
//
// # Call sites
//
//   - Local broker: invoked synchronously via the CompletionHandler callback
//     immediately after the executor returns.
//   - Distributed broker: called by the external consumer (MQ subscriber,
//     HTTP webhook handler, etc.) when a remote worker reports its result.
//
// # Hook resolution limitation (TODO)
//
// Task hooks (onSuccess, onFailure, ...) are declared on the call-site task node
// inside the parent DAG, not on the template itself. The correct approach is to
// look up the task node from tr.ParentRunID's DAG. Currently we only look in the
// entrypoint DAG, so hooks on tasks inside nested DAGs are silently missed.
// This will be fixed once scope-aware task resolution is implemented.
func (e *Engine) OnTaskCompleted(ctx context.Context, result *broker.TaskResult) {
	// 1. Persist result: update Status, Message, and Outputs in Store.
	tr, err := e.store.GetTaskRun(ctx, result.TaskRunID)
	if err != nil {
		return
	}

	tr.Status = result.Phase
	tr.Message = result.Message
	tr.Outputs = result.Outputs
	_ = e.store.UpdateTaskRun(ctx, tr)

	// 2. Fire task-level hooks (e.g., onSuccess, onFailure).
	//
	// Hooks are declared on the DAG task node (call-site), not on the template.
	// Example DAG task with a hook:
	//   tasks:
	//     - name: "review"
	//       template: "review-task"
	//       hooks:
	//         onFailure: "notify-failure"   ← fires if this task fails
	//
	// TODO: look up the task from tr.ParentRunID's DAG template for correct
	// nested-scope support. The current entrypoint-only lookup is a known gap.
	wf, err := e.loadWorkflow(ctx, tr.WorkflowRunID)
	if err != nil {
		return
	}
	tmpl := internal.FindTemplate(wf, wf.Spec.Entrypoint)
	if tmpl != nil && tmpl.DAG != nil {
		task := internal.FindTask(tmpl.DAG, tr.TaskName)
		internal.FireTaskHooks(ctx, e.hookNotifier, task, tr.WorkflowRunID, result.Phase)
	}

	// 3. Re-advance the scope this task belongs to.
	// tr.ParentRunID identifies the scope (DAG/Loop container) that owns this task.
	// advanceScope will find newly-ready tasks, or finalize the scope if all are done.
	_ = e.advanceScope(ctx, tr.WorkflowRunID, wf, tr.ParentRunID)
}

// loadWorkflow loads and deserializes the workflow from a WorkflowRun.
func (e *Engine) loadWorkflow(ctx context.Context, workflowRunID uint64) (*model.Workflow, error) {
	run, err := e.store.GetWorkflowRun(ctx, workflowRunID)
	if err != nil {
		return nil, err
	}
	var wf model.Workflow
	if err := json.Unmarshal(run.Workflow, &wf); err != nil {
		return nil, err
	}
	internal.FillDefaults(&wf)
	return &wf, nil
}
