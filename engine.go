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
// All checks (marshal, fill defaults, validate, resolve entrypoint, find root tasks)
// are performed before any state is persisted to Store.
func (e *Engine) Submit(ctx context.Context, wf *model.Workflow) (uint64, error) {
	if wf == nil {
		return 0, fmt.Errorf("aether: %w: workflow must not be nil", ErrValidation)
	}

	// 1. Resolve WorkflowTemplateRef if present
	if wf.Spec.WorkflowTemplateRef != nil {
		if err := internal.ResolveWorkflowTemplateRef(ctx, wf, e.store); err != nil {
			return 0, fmt.Errorf("aether: %w", err)
		}
	}

	// 2. Marshal raw workflow JSON (immutable snapshot, after ref resolution)
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
	tmpl := internal.FindTemplate(wf, wf.Spec.Entrypoint)
	if tmpl == nil {
		return 0, fmt.Errorf("aether: %w: entrypoint template %q not found", ErrValidation, wf.Spec.Entrypoint)
	}

	// 6. For DAG templates, find root tasks (pre-check before persisting)
	var rootTasks []model.Task
	if tmpl.DAG != nil {
		rootTasks = internal.FindRootTasks(tmpl.DAG)
		if len(rootTasks) == 0 {
			return 0, fmt.Errorf("aether: %w: no root tasks found in DAG", ErrValidation)
		}
	}

	// --- All checks passed. Now persist and dispatch. ---

	// 7. Generate workflow run ID and store
	workflowRunID := e.idGen.Generate()
	run := &store.WorkflowRun{
		RunID:    workflowRunID,
		Workflow: rawJSON,
		Status:   model.PhaseRunning,
	}
	if err := e.store.CreateWorkflowRun(ctx, run); err != nil {
		return 0, fmt.Errorf("aether: create workflow run: %w", err)
	}

	// 8. Fire onStart hook
	internal.FireWorkflowHooks(ctx, e.hookNotifier, wf, workflowRunID, model.PhaseRunning)

	// 9. For DAG templates, create TaskRuns and dispatch root tasks
	if tmpl.DAG != nil {
		if err := e.dispatchTasks(ctx, workflowRunID, rootTasks, tmpl.DAG, wf, nil); err != nil {
			return 0, err
		}
	}

	// 10. For loop templates, expand and dispatch iterations
	if tmpl.Loop != nil {
		if err := e.handleLoopTemplate(ctx, workflowRunID, wf.Spec.Entrypoint, tmpl, wf, nil); err != nil {
			return 0, err
		}
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

	// 4. Schedule next tasks
	return e.scheduleNext(ctx, tr.WorkflowRunID)
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
// It updates the task state, fires task hooks, progresses the DAG by computing
// and dispatching the next ready tasks, and finalizes the workflow if all tasks are done.
//
// Call sites:
//   - Local broker: invoked directly via CompletionHandler callback
//   - Distributed: called by the external consumer (MQ subscriber, HTTP handler, etc.)
func (e *Engine) OnTaskCompleted(ctx context.Context, result *broker.TaskResult) {
	// 1. Update task run status in Store
	tr, err := e.store.GetTaskRun(ctx, result.TaskRunID)
	if err != nil {
		return
	}

	tr.Status = result.Phase
	tr.Message = result.Message
	tr.Outputs = result.Outputs
	_ = e.store.UpdateTaskRun(ctx, tr)

	// 2. Fire task-level hooks
	run, err := e.store.GetWorkflowRun(ctx, tr.WorkflowRunID)
	if err != nil {
		return
	}

	var wf model.Workflow
	if err := json.Unmarshal(run.Workflow, &wf); err != nil {
		return
	}
	internal.FillDefaults(&wf)

	tmpl := internal.FindTemplate(&wf, wf.Spec.Entrypoint)
	if tmpl != nil && tmpl.DAG != nil {
		task := internal.FindTask(tmpl.DAG, tr.TaskName)
		internal.FireTaskHooks(ctx, e.hookNotifier, task, tr.WorkflowRunID, result.Phase)
	}

	// 3. Schedule next tasks
	_ = e.scheduleNext(ctx, tr.WorkflowRunID)
}
