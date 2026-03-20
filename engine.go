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
	pendingPhase := model.PhasePending
	run := &store.WorkflowRun{
		RunID:    workflowRunID,
		Workflow: rawJSON,
		Status:   &pendingPhase,
	}
	if err := e.store.CreateWorkflowRun(ctx, run); err != nil {
		return 0, fmt.Errorf("aether: create workflow run: %w", err)
	}

	// 8. Create entry TaskRun (Pending, ParentRunID=0)
	entryPending := model.PhasePending
	entryTaskRun := &store.TaskRun{
		RunID:         e.idGen.Generate(),
		WorkflowRunID: workflowRunID,
		ParentRunID:   0,
		Depth:         0,
		Scope:         "",
		TaskName:      wf.Spec.Entrypoint,
		TemplateName:  wf.Spec.Entrypoint,
		TemplateType:  templateType,
		Status:        &entryPending,
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
	var phase model.Phase
	var msg string
	if run.Status != nil {
		phase = *run.Status
	}
	if run.Message != nil {
		msg = *run.Message
	}
	exec := &WorkflowExecution{
		WorkflowID: run.RunID,
		Phase:      phase,
		Msg:        msg,
		Outputs:    run.Outputs,
		Metrics:    run.Metrics,
		Tasks:      make([]TaskExecution, 0, len(taskRuns)),
	}

	totalTasks := len(taskRuns)
	completedTasks := 0
	for _, tr := range taskRuns {
		var taskPhase model.Phase
		if tr.Status != nil {
			taskPhase = *tr.Status
		}
		exec.Tasks = append(exec.Tasks, TaskExecution{
			TaskID:   tr.RunID,
			Name:     tr.TaskName,
			Path:     tr.Scope + tr.TaskName,
			Template: tr.TemplateName,
			Phase:    taskPhase,
			Metrics:  tr.Metrics,
		})
		if tr.Status != nil && tr.Status.IsTerminal() {
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
	if tr.Status == nil || *tr.Status != model.PhaseRunning {
		status := model.Phase("<nil>")
		if tr.Status != nil {
			status = *tr.Status
		}
		return fmt.Errorf("aether: %w: task %d is not in Running phase (current: %s)", ErrInvalidState, taskID, status)
	}

	// 3. Build outputs from payload and complete the task.
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

	succeeded := model.PhaseSucceeded
	empty := ""
	_, err = e.store.UpdateTaskRun(ctx, &store.TaskRun{
		RunID:   tr.RunID,
		Token:   tr.Token,
		Status:  &succeeded,
		Message: &empty,
		Outputs: outputs,
	})
	if err != nil {
		// Token mismatch: another Resume or Cancel already transitioned this task — treat as no-op.
		return nil
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
	if run.Status != nil && run.Status.IsTerminal() {
		return fmt.Errorf("aether: %w: workflow %d already in terminal state %s", ErrInvalidState, workflowID, *run.Status)
	}

	// 2. List all task runs
	taskRuns, err := e.store.ListTaskRuns(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("aether: list task runs: %w", err)
	}

	// 3. Cancel/skip non-terminal tasks.
	//
	// Running tasks: send cancellation signal to broker, then transition to
	// PhaseCancelled (not PhaseError — the task was not broken, it was stopped).
	// Using PhaseCancelled keeps Error semantics clean (system failures only) and
	// prevents the retry policy from re-scheduling cancelled tasks.
	//
	// Pending tasks: transition directly to PhaseCancelled via Get+Update.
	// They have not started yet, so no broker signal is needed.
	// Previously these were set to PhaseSkipped, but Skipped means "when-condition
	// evaluated to false" — a different semantic from user-initiated cancellation.
	cancelled := model.PhaseCancelled
	cancelMsg := "cancelled by user"
	for _, tr := range taskRuns {
		if tr.Status == nil || tr.Status.IsTerminal() {
			continue
		}
		switch *tr.Status {
		case model.PhaseRunning:
			_ = e.taskBroker.Cancel(ctx, tr.RunID)
			_, _ = e.store.UpdateTaskRun(ctx, &store.TaskRun{
				RunID:   tr.RunID,
				Token:   tr.Token,
				Status:  &cancelled,
				Message: &cancelMsg,
			})
		case model.PhasePending:
			_, _ = e.store.UpdateTaskRun(ctx, &store.TaskRun{
				RunID:   tr.RunID,
				Token:   tr.Token,
				Status:  &cancelled,
				Message: &cancelMsg,
			})
		}
	}

	// 4. Mark workflow as Cancelled.
	// Use Get + UpdateWorkflowRun to avoid overwriting a terminal status already set by finalizeWorkflow.
	wfRun, err := e.store.GetWorkflowRun(ctx, workflowID)
	if err == nil && (wfRun.Status == nil || !wfRun.Status.IsTerminal()) {
		_, _ = e.store.UpdateWorkflowRun(ctx, &store.WorkflowRun{
			RunID:   wfRun.RunID,
			Token:   wfRun.Token,
			Status:  &cancelled,
			Message: &cancelMsg,
		})
	}

	var wf model.Workflow
	if err := json.Unmarshal(run.Workflow, &wf); err == nil {
		internal.FillDefaults(&wf)
		internal.FireWorkflowHooks(ctx, e.hookNotifier, &wf, workflowID, model.PhaseCancelled)
	}

	return nil
}

// OnTaskStarted is invoked when a worker begins executing a task.
// It transitions the leaf task itself, its ancestor containers (DAG/Loop), and
// the WorkflowRun from Pending to Running, so that their Running state accurately
// reflects the moment real work begins rather than the moment the task was dispatched.
//
// Both the leaf task transition and the ancestor walk use Get+Update with Token to
// remain idempotent under concurrent or duplicate invocations.
//
// # Call sites
//
//   - Local broker: invoked synchronously via the StartHandler callback
//     immediately before the executor starts.
//   - Distributed broker: called by the external consumer (MQ subscriber,
//     HTTP webhook handler, etc.) when a remote worker reports it has started.
func (e *Engine) OnTaskStarted(ctx context.Context, taskRunID uint64) {
	tr, err := e.store.GetTaskRun(ctx, taskRunID)
	if err != nil {
		return
	}
	// 1. Transition the leaf task itself: Pending → Running (idempotent via Token).
	if tr.Status != nil && *tr.Status == model.PhasePending {
		running := model.PhaseRunning
		empty := ""
		_, _ = e.store.UpdateTaskRun(ctx, &store.TaskRun{
			RunID:   taskRunID,
			Token:   tr.Token,
			Status:  &running,
			Message: &empty,
		})
	}
	// 2. Transition ancestor containers (DAG/Loop) and the WorkflowRun: Pending → Running.
	_ = e.markAncestorsRunning(ctx, tr.WorkflowRunID, tr.ParentRunID)
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
	// 1. Get current task run state first to check idempotency.
	tr, err := e.store.GetTaskRun(ctx, result.TaskRunID)
	if err != nil {
		return
	}
	// Guard: only process if currently Running (duplicate callbacks or cancel race).
	if tr.Status == nil || *tr.Status != model.PhaseRunning {
		return
	}

	// 2. Persist result: Running → terminal phase + outputs.
	var taskOutputs *model.Outputs
	var taskMetrics *model.Metrics
	if result.Outputs != nil {
		taskOutputs = result.Outputs
		taskMetrics = result.Outputs.Metrics
	}
	phase := result.Phase
	msg := result.Message
	_, err = e.store.UpdateTaskRun(ctx, &store.TaskRun{
		RunID:   tr.RunID,
		Token:   tr.Token,
		Status:  &phase,
		Message: &msg,
		Outputs: taskOutputs,
		Metrics: taskMetrics,
	})
	if err != nil {
		// Token mismatch: another writer (Cancel or duplicate callback) already handled it.
		return
	}

	// Re-fetch to get updated state for downstream logic.
	tr, err = e.store.GetTaskRun(ctx, result.TaskRunID)
	if err != nil {
		return
	}

	wf, err := e.loadWorkflow(ctx, tr.WorkflowRunID)
	if err != nil {
		return
	}

	// 3. Retry check: before firing hooks or advancing scope, determine whether the
	// task should be retried.
	//
	// Retry only applies to leaf tasks (TemplateType == "task"). DAG and Loop
	// containers are not retried directly.
	//
	// To find the retry policy we need the parent container's template name so we can
	// look up the call-site task node in its DAG. For top-level tasks (ParentRunID==0)
	// the parent template is the workflow entrypoint.
	if tr.TemplateType == model.TemplateTypeTask {
		parentTemplateName := wf.Spec.Entrypoint
		if tr.ParentRunID != 0 {
			parentTR, perr := e.store.GetTaskRun(ctx, tr.ParentRunID)
			if perr == nil {
				parentTemplateName = parentTR.TemplateName
			}
		}

		retryPolicy := internal.ResolveRetryPolicy(wf, parentTemplateName, tr.TaskName)
		needRetry, rerr := internal.ShouldRetry(ctx, tr, retryPolicy, e.exprEvaluator)
		if rerr != nil {
			// Expression evaluation failed — treat as non-retryable and fall through.
			needRetry = false
		}

		if needRetry {
			// Reset the task to Pending (increments RetryCount) and re-dispatch.
			// Skip hooks and advanceScope — the task is not done yet.
			if resetOK, _ := e.resetForRetry(ctx, tr.RunID); resetOK {
				// Re-fetch to get updated RetryCount before dispatch.
				if updatedTR, gerr := e.store.GetTaskRun(ctx, tr.RunID); gerr == nil {
					_ = e.dispatchLeafTask(ctx, tr.WorkflowRunID, wf, updatedTR)
				}
			}
			return
		}
	}

	// 4. Fire task-level hooks (e.g., onSuccess, onFailure).
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
	tmpl := internal.FindTemplate(wf, wf.Spec.Entrypoint)
	if tmpl != nil && tmpl.DAG != nil {
		task := internal.FindTask(tmpl.DAG, tr.TaskName)
		taskPhase := result.Phase
		internal.FireTaskHooks(ctx, e.hookNotifier, task, tr.WorkflowRunID, taskPhase)
	}

	// 5. Re-advance the scope this task belongs to.
	// tr.ParentRunID identifies the scope (DAG/Loop container) that owns this task.
	// advanceScope will find newly-ready tasks, or finalize the scope if all are done.
	_ = e.advanceScope(ctx, tr.WorkflowRunID, wf, tr.ParentRunID)
}

// resetForRetry resets a terminal task back to Pending and increments RetryCount.
// Returns (true, nil) on success, (false, nil) if the task is not eligible for retry
// (not terminal, or already Cancelled), (false, err) on store error.
func (e *Engine) resetForRetry(ctx context.Context, taskRunID uint64) (bool, error) {
	tr, err := e.store.GetTaskRun(ctx, taskRunID)
	if err != nil {
		return false, err
	}
	// Business check: only Error/Timeout/Failed are retried; Cancelled is not.
	if tr.Status == nil || !tr.Status.IsTerminal() || *tr.Status == model.PhaseCancelled {
		return false, nil
	}

	pending := model.PhasePending
	empty := ""
	newCount := 0
	if tr.RetryCount != nil {
		newCount = *tr.RetryCount + 1
	} else {
		newCount = 1
	}
	_, err = e.store.UpdateTaskRun(ctx, &store.TaskRun{
		RunID:      tr.RunID,
		Token:      tr.Token,
		Status:     &pending,
		Message:    &empty,
		RetryCount: &newCount,
	})
	return err == nil, err
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
