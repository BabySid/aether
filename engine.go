package aether

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
	"github.com/BabySid/aether/timeout"
)

// Engine is the core workflow scheduling engine.
// All dependencies are injected via Option.
type Engine struct {
	store          store.Store
	executorReg    *executor.Registry
	exprEvaluator  expr.Evaluator
	taskBroker     broker.TaskBroker
	artifactStore  artifact.Store
	secretStore    secret.Store
	hookNotifier   hook.Notifier
	idGen          idgen.Generator
	timeoutWatcher timeout.Watcher
	stopFn         context.CancelFunc // set by Start; called by Stop
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
	if e.timeoutWatcher == nil {
		return nil, fmt.Errorf("aether: %w: TimeoutWatcher is required, use WithTimeoutWatcher()", ErrValidation)
	}

	return e, nil
}

// Submit parses, validates, and dispatches a workflow for execution.
// Returns the workflow run ID. Non-blocking.
//
// All checks (marshal, fill defaults, validate, resolve entrypoint) are
// performed before any state is persisted to Store.
func (e *Engine) Submit(ctx context.Context, wf *model.Workflow) (string, error) {
	// 1. Nil check
	if wf == nil {
		return "", fmt.Errorf("aether: %w: workflow must not be nil", ErrValidation)
	}

	// 2. Marshal raw workflow JSON (immutable snapshot)
	rawJSON, err := json.Marshal(wf)
	if err != nil {
		return "", fmt.Errorf("aether: marshal workflow: %w", err)
	}

	// 3. Fill defaults
	internal.FillDefaults(wf)

	// 4. Validate (structural + semantic)
	if err := internal.Validate(wf); err != nil {
		return "", fmt.Errorf("aether: %w: %v", ErrValidation, err)
	}

	// 5. Resolve entrypoint template
	entry := internal.FindTemplate(wf, wf.Spec.Entrypoint)
	if entry == nil {
		return "", fmt.Errorf("aether: %w: entrypoint template %q not found", ErrValidation, wf.Spec.Entrypoint)
	}

	// 6. Determine entrypoint template type
	templateType := internal.ResolveTemplateType(entry)
	if templateType == "" {
		return "", fmt.Errorf("aether: %w: entrypoint template %q has no dag/executor/loop", ErrValidation, wf.Spec.Entrypoint)
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
	// Set workflow-level deadline if a timeout is configured.
	if wf.Spec.Timeout != "" {
		if d, parseErr := internal.ParseDuration(wf.Spec.Timeout); parseErr == nil && d > 0 {
			dl := time.Now().Add(d)
			run.Deadline = &dl
		}
	}
	if err := e.store.CreateWorkflowRun(ctx, run); err != nil {
		return "", fmt.Errorf("aether: create workflow run: %w", err)
	}

	// 8. Create entry TaskRun (Pending, ParentRunID="" for top-level scope)
	entryPending := model.PhasePending
	entryTaskRun := &store.TaskRun{
		RunID:         e.idGen.Generate(),
		WorkflowRunID: workflowRunID,
		ParentRunID:   "",
		Depth:         0,
		Scope:         "",
		TaskName:      wf.Spec.Entrypoint,
		TemplateName:  wf.Spec.Entrypoint,
		TemplateType:  templateType,
		Status:        &entryPending,
	}
	if err := e.store.CreateTaskRun(ctx, entryTaskRun); err != nil {
		return "", fmt.Errorf("aether: create entry task run: %w", err)
	}

	// 9. Start scheduling via advanceScope
	if err := e.advanceScope(ctx, workflowRunID, wf, ""); err != nil {
		return "", fmt.Errorf("aether: %w", err)
	}

	return workflowRunID, nil
}

// Get retrieves the current execution state of a workflow. Non-blocking.
func (e *Engine) Get(ctx context.Context, workflowID string) (*WorkflowExecution, error) {
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
		WorkflowRun: run,
		Tasks:       taskRuns,
	}

	totalTasks := len(taskRuns)
	completedTasks := 0
	for _, tr := range taskRuns {
		if tr.Status != nil && tr.Status.IsTerminal() {
			completedTasks++
		}
	}
	if totalTasks > 0 {
		exec.Progress = fmt.Sprintf("%d/%d", completedTasks, totalTasks)
	}

	return exec, nil
}

// Resume re-dispatches a suspended task with an incremental payload.
//
// The payload is merged (last-writer-wins) into the task's accumulated Inputs,
// so the executor receives the full history of all Resume payloads on top of the
// original resolved inputs. The executor decides whether to remain suspended
// (return ExecCodeSuspended again) or complete (ExecCodeSucceeded/Failed/Error).
//
// Resume is a no-op if the task is no longer in PhaseRunning (e.g. already timed
// out, cancelled, or completed by a concurrent Resume).
func (e *Engine) Resume(ctx context.Context, workflowID string, taskID string, payload map[string]any) error {
	// 1. Fetch and validate.
	tr, err := e.store.GetTaskRun(ctx, taskID)
	if err != nil {
		return fmt.Errorf("aether: %w", err)
	}
	if tr.WorkflowRunID != workflowID {
		return fmt.Errorf("aether: %w: task %q does not belong to workflow %q", ErrInvalidState, taskID, workflowID)
	}
	if tr.Status == nil || *tr.Status != model.PhaseRunning {
		// Task already completed, timed out, or cancelled — treat as no-op.
		return nil
	}

	// 2. Merge payload into the task's accumulated Inputs (payload keys win).
	mergedInputs := internal.MergeInputsWithPayload(tr.Inputs, payload)
	updated, err := e.store.UpdateTaskRun(ctx, &store.TaskRun{
		RunID:  tr.RunID,
		Token:  tr.Token,
		Inputs: mergedInputs,
	})
	if err != nil {
		// Token mismatch: a concurrent Resume or Cancel already handled this task.
		return nil
	}

	// 3. Re-dispatch the task to the broker so the executor runs again with the
	// merged inputs. It bypasses the binding step (inputs are already merged) and
	// does not reset the Deadline (the original deadline remains in effect).
	// The executor decides the next phase (suspend/succeed/fail).
	wf, err := e.loadWorkflow(ctx, tr.WorkflowRunID)
	if err != nil {
		return err
	}

	// Resolve taskDecl: named template or inline executor on the DAG task node.
	// Reuse resolveTaskDecl (same logic as dispatchLeafTask step 1).
	tr = updated
	var parentTR *store.TaskRun
	if tr.ParentRunID != "" {
		parentTR, err = e.store.GetTaskRun(ctx, tr.ParentRunID)
		if err != nil {
			return fmt.Errorf("aether: resume: get parent TaskRun %q: %w", tr.ParentRunID, err)
		}
	}
	taskDecl, _, _ := internal.ResolveTaskDecl(wf, tr, parentTR)
	if taskDecl == nil {
		return fmt.Errorf("aether: resume: template %q not found for task %q", tr.TemplateName, tr.TaskName)
	}

	assignment, err := internal.BuildTaskAssignment(tr.WorkflowRunID, tr, taskDecl, nil, wf)
	if err != nil {
		return fmt.Errorf("aether: resume: %w", err)
	}
	// Use stored (merged) inputs — skip re-binding.
	assignment.Inputs = tr.Inputs
	return e.taskBroker.Dispatch(ctx, assignment)
}

// Cancel cancels a running workflow. Non-blocking.
func (e *Engine) Cancel(ctx context.Context, workflowID string) error {
	// 1. Get workflow run
	run, err := e.store.GetWorkflowRun(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("aether: %w", err)
	}
	if run.Status != nil && run.Status.IsTerminal() {
		return fmt.Errorf("aether: %w: workflow %q already in terminal state %s", ErrInvalidState, workflowID, *run.Status)
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
func (e *Engine) OnTaskStarted(ctx context.Context, taskRunID string) {
	tr, err := e.store.GetTaskRun(ctx, taskRunID)
	if err != nil {
		return
	}
	// 1. Transition the leaf task itself: Pending → Running (idempotent via Token).
	// Also record StartedAt in Metrics — the engine is the authoritative Metrics writer.
	if tr.Status != nil && *tr.Status == model.PhasePending {
		running := model.PhaseRunning
		empty := ""
		startedAt := time.Now().UTC().Format(time.RFC3339)
		_, _ = e.store.UpdateTaskRun(ctx, &store.TaskRun{
			RunID:   taskRunID,
			Token:   tr.Token,
			Status:  &running,
			Message: &empty,
			Metrics: &model.Metrics{StartedAt: startedAt},
		})
	}
	// 2. Transition ancestor containers (DAG/Loop) and the WorkflowRun: Pending → Running.
	_ = e.markAncestorsRunning(ctx, tr.WorkflowRunID, tr.ParentRunID)
}

// OnTaskCompleted is invoked when a task finishes execution.
// It persists the result, fires task-level hooks, then re-evaluates the task's
// scope to dispatch newly-unblocked tasks or finalize the workflow.
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

	// 2. Load the workflow (needed for phase conditions, output merge, hooks, retry).
	wf, err := e.loadWorkflow(ctx, result.WorkflowRunID)
	if err != nil {
		return
	}

	// --- Phase derivation (engine is the single Phase writer) ---
	//
	// Phase is derived from ExecOutputs.Code, then optionally overridden by
	// the task's phaseConditions expression (user-defined override).
	//
	// PhaseSkipped and PhaseCancelled are set exclusively by the engine itself
	// and never come from a TaskResult.
	//
	// Fetch parentTR once here: it is also needed later for declOutputs lookup,
	// so we avoid a second GetTaskRun(parentRunID) call below.
	var parentTR *store.TaskRun
	if tr.ParentRunID != "" {
		parentTR, _ = e.store.GetTaskRun(ctx, tr.ParentRunID)
	}
	// Look up phaseConditions from named template or inline DAG task node.
	var phaseConditions *model.PhaseConditions
	taskTmpl := internal.FindTemplate(wf, tr.TemplateName)
	if taskTmpl != nil && taskTmpl.Task != nil {
		phaseConditions = taskTmpl.Task.PhaseConditions
	} else if parentTR != nil {
		parentTmpl := internal.FindTemplate(wf, parentTR.TemplateName)
		if parentTmpl != nil && parentTmpl.DAG != nil {
			dagTask := internal.FindTask(parentTmpl.DAG, tr.TaskName)
			if dagTask != nil {
				phaseConditions = dagTask.PhaseConditions
			}
		}
	}
	phase := internal.EvalPhaseConditions(ctx, phaseConditions, e.exprEvaluator, result)

	// --- Suspend fast-path (ExecCodeSuspended → PhaseRunning) ---
	//
	// The executor returned ExecCodeSuspended: the task remains in PhaseRunning.
	// Merge the executor's partial outputs into any accumulated outputs from
	// previous Execute calls (new keys are added; existing keys are overwritten
	// by the latest executor result). Return early — no hooks, no retry, no
	// advanceScope; the DAG must not progress until a future Resume finalizes.
	if phase == model.PhaseRunning {
		var accumulatedParams []model.Parameter
		if tr.Outputs != nil {
			accumulatedParams = tr.Outputs.Parameters
		}
		if result.ExecOutputs != nil {
			accumulatedParams = internal.MergeParameters(accumulatedParams, result.ExecOutputs.Parameters)
		}
		mergedOutputs := &model.Outputs{
			Phase:       model.PhaseRunning,
			ExecOutputs: model.ExecOutputs{Parameters: accumulatedParams},
		}
		var suspendMsg string
		if result.ExecOutputs != nil {
			suspendMsg = result.ExecOutputs.Message
		}
		_, _ = e.store.UpdateTaskRun(ctx, &store.TaskRun{
			RunID:   tr.RunID,
			Token:   tr.Token,
			Status:  &phase,
			Message: &suspendMsg,
			Outputs: mergedOutputs,
		})
		return
	}

	// 3. Retry check: happens before metrics/outputs computation and UpdateTaskRun.
	//
	// Retry only applies to leaf tasks (TemplateType == "task"). DAG and Loop
	// containers are not retried directly.
	//
	// phase is already derived above but not yet persisted — tr.Status is still
	// Running. If the task needs a retry we reset it directly (Token is still valid)
	// and dispatch without ever writing a terminal state, saving the metrics and
	// outputs computation entirely.
	if tr.TemplateType == model.TemplateTypeTask {
		parentTemplateName := wf.Spec.Entrypoint
		if parentTR != nil {
			// Reuse parentTR already fetched above; it holds the correct TemplateName.
			parentTemplateName = parentTR.TemplateName
		}

		retryPolicy := internal.ResolveRetryPolicy(wf, parentTemplateName, tr.TaskName)
		needRetry, rerr := internal.ShouldRetry(ctx, tr, phase, retryPolicy, e.exprEvaluator)
		if rerr != nil {
			// Expression evaluation failed — treat as non-retryable and fall through.
			needRetry = false
		}

		if needRetry {
			// tr.Status is still Running and Token is valid — reset directly to Pending.
			// Skip metrics, outputs, hooks and advanceScope — the task is not done yet.
			pending := model.PhasePending
			emptyMsg := ""
			newCount := 1
			if tr.RetryCount != nil {
				newCount = *tr.RetryCount + 1
			}
			retryTR, retryErr := e.store.UpdateTaskRun(ctx, &store.TaskRun{
				RunID:      tr.RunID,
				Token:      tr.Token,
				Status:     &pending,
				Message:    &emptyMsg,
				RetryCount: &newCount,
			})
			if retryErr == nil {
				_ = e.dispatchLeafTask(ctx, result.WorkflowRunID, wf, retryTR)
			}
			return
		}
	}

	// --- Metrics computation (engine is the single Metrics writer) ---
	//
	// StartedAt was recorded by OnTaskStarted. FinishedAt and Duration are
	// computed here from the current wall clock and the stored StartedAt.
	finishedAt := time.Now().UTC()
	finishedAtStr := finishedAt.Format(time.RFC3339)
	taskMetrics := &model.Metrics{FinishedAt: finishedAtStr}
	if tr.Metrics != nil && tr.Metrics.StartedAt != "" {
		taskMetrics.StartedAt = tr.Metrics.StartedAt
		if startedAt, parseErr := time.Parse(time.RFC3339, tr.Metrics.StartedAt); parseErr == nil {
			taskMetrics.Duration = finishedAt.Sub(startedAt).Round(time.Millisecond).String()
		}
	}
	if tr.RetryCount != nil && *tr.RetryCount > 0 {
		taskMetrics.Retries = *tr.RetryCount
	}

	// --- Build taskOutputs ---
	//
	// Merge strategy for output parameters:
	//   executor result wins → template-declared value fills gaps (mock/default)
	//
	// This allows workflow JSON to declare static mock output values on
	// outputs.parameters[].value, which are used when the executor does not
	// produce an output for that parameter (e.g. echo/noop executors in playground).
	var taskOutputs *model.Outputs
	if result.ExecOutputs != nil {
		taskOutputs = &model.Outputs{
			Phase:       phase,
			Metrics:     taskMetrics,
			ExecOutputs: *result.ExecOutputs,
		}
	}
	// Merge template-declared output parameter values into taskOutputs.
	//
	// For named-template tasks:  look up wf.spec.templates by tr.TemplateName.
	// For inline-executor tasks: tr.TemplateName is empty; look up the task node
	//   in the parent DAG template instead.
	var declOutputs *model.Outputs
	if taskTmpl != nil && taskTmpl.Task != nil {
		declOutputs = taskTmpl.Task.Outputs
	} else if parentTR != nil {
		// Reuse parentTR already fetched above for phaseConditions.
		parentTmpl := internal.FindTemplate(wf, parentTR.TemplateName)
		if parentTmpl != nil && parentTmpl.DAG != nil {
			dagTask := internal.FindTask(parentTmpl.DAG, tr.TaskName)
			if dagTask != nil {
				declOutputs = dagTask.Outputs
			}
		}
	}
	if declOutputs != nil {
		taskOutputs = internal.MergeOutputsWithDecl(taskOutputs, declOutputs)
	}

	var msg string
	if result.ExecOutputs != nil {
		msg = result.ExecOutputs.Message
	}
	updatedTR, err := e.store.UpdateTaskRun(ctx, &store.TaskRun{
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
	// Use the returned state directly — avoids an extra GetTaskRun round-trip.
	tr = updatedTR

	// 4. Fire task-level hooks (e.g., onSuccess, onFailure).
	//
	// Hooks are declared on the DAG task node (call-site), not on the template.
	// parentTR (fetched above for phaseConditions) points to the direct parent
	// container, so we use its TemplateName to find the correct DAG and task node —
	// this works for tasks at any nesting depth, not just the entrypoint DAG.
	if parentTR != nil {
		parentTmpl := internal.FindTemplate(wf, parentTR.TemplateName)
		if parentTmpl != nil && parentTmpl.DAG != nil {
			task := internal.FindTask(parentTmpl.DAG, tr.TaskName)
			internal.FireTaskHooks(ctx, e.hookNotifier, task, result.WorkflowRunID, phase)
		}
	}

	// 5. Re-advance the scope this task belongs to.
	// tr.ParentRunID identifies the scope (DAG/Loop container) that owns this task.
	// advanceScope will find newly-ready tasks, or finalize the scope if all are done.
	_ = e.advanceScope(ctx, result.WorkflowRunID, wf, tr.ParentRunID)
}

// Start launches the Engine's background services (currently: timeout watchdog).
// It returns immediately; all background goroutines run until Stop is called or
// the parent ctx is cancelled. Pair every Start call with a Stop call.
func (e *Engine) Start(ctx context.Context) error {
	innerCtx, cancel := context.WithCancel(ctx)
	e.stopFn = cancel
	if err := e.timeoutWatcher.Start(innerCtx); err != nil {
		cancel()
		e.stopFn = nil
		return fmt.Errorf("aether: start timeout watcher: %w", err)
	}
	go e.watchTimeouts(innerCtx)
	return nil
}

// Stop shuts down all background services started by Start.
// It is safe to call Stop multiple times; subsequent calls are no-ops.
func (e *Engine) Stop() {
	if e.stopFn != nil {
		e.stopFn()
		e.stopFn = nil
	}
}

// watchTimeouts consumes TimeoutEvents from the Watcher and drives the state machine.
func (e *Engine) watchTimeouts(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-e.timeoutWatcher.Events():
			switch ev.Kind {
			case timeout.KindTask:
				e.OnTaskTimeout(ctx, ev.RunID)
			case timeout.KindWorkflow:
				e.OnWorkflowTimeout(ctx, ev.RunID)
			}
		}
	}
}

// OnTaskTimeout is invoked (by the watchdog) when a TaskRun has exceeded its Deadline.
//
// It is idempotent: if the task is already in a terminal state (completed normally,
// cancelled, etc.) the call is a no-op.  In a multi-Engine deployment multiple
// instances may race here; the Store's Token-based optimistic lock ensures only
// one writer succeeds.
func (e *Engine) OnTaskTimeout(ctx context.Context, taskRunID string) {
	tr, err := e.store.GetTaskRun(ctx, taskRunID)
	if err != nil {
		return
	}
	// Idempotency: already terminal → nothing to do.
	if tr.Status != nil && tr.Status.IsTerminal() {
		return
	}

	// Best-effort broker cancellation (fast-path termination if the Broker is alive).
	_ = e.taskBroker.Cancel(ctx, taskRunID)

	// Drive the existing completion path with a Timeout result.
	// OnTaskCompleted handles Token-based optimistic locking, hooks, retry checks,
	// and scope advancement — reusing it avoids duplicating that logic here.
	e.OnTaskCompleted(ctx, &broker.TaskResult{
		TaskRunID:     taskRunID,
		WorkflowRunID: tr.WorkflowRunID,
		ExecOutputs: &model.ExecOutputs{
			Code:    model.ExecCodeTimeout,
			Message: "task deadline exceeded (watchdog)",
		},
	})
}

// OnWorkflowTimeout is invoked (by the watchdog) when a WorkflowRun has exceeded
// its Deadline.  It cancels all non-terminal tasks and marks the workflow Timeout.
//
// Idempotent: safe to call multiple times or concurrently from multiple Engine instances.
func (e *Engine) OnWorkflowTimeout(ctx context.Context, workflowRunID string) {
	wr, err := e.store.GetWorkflowRun(ctx, workflowRunID)
	if err != nil {
		return
	}
	// Idempotency: already terminal → nothing to do.
	if wr.Status != nil && wr.Status.IsTerminal() {
		return
	}

	// Cancel all non-terminal task runs.
	// If the store fails we cannot guarantee all tasks are terminated, so abort
	// rather than marking the workflow done while tasks may still be running.
	trs, err := e.store.ListTaskRuns(ctx, workflowRunID)
	if err != nil {
		return
	}
	timeoutPhase := model.PhaseTimeout
	timeoutMsg := "workflow deadline exceeded (watchdog)"
	for _, tr := range trs {
		if tr.Status != nil && tr.Status.IsTerminal() {
			continue
		}
		_, _ = e.store.UpdateTaskRun(ctx, &store.TaskRun{
			RunID:   tr.RunID,
			Token:   tr.Token,
			Status:  &timeoutPhase,
			Message: &timeoutMsg,
		})
		_ = e.taskBroker.Cancel(ctx, tr.RunID)
	}

	// Mark the workflow itself as Timeout.
	// Use wr.Token fetched at the top — the Token-based optimistic lock guards
	// against a concurrent writer; if the Token is stale the update is a no-op.
	wfMsg := "workflow deadline exceeded"
	_, _ = e.store.UpdateWorkflowRun(ctx, &store.WorkflowRun{
		RunID:   workflowRunID,
		Token:   wr.Token,
		Status:  &timeoutPhase,
		Message: &wfMsg,
	})

	// Fire workflow hooks.
	var wf model.Workflow
	if jsonErr := json.Unmarshal(wr.Workflow, &wf); jsonErr == nil {
		internal.FillDefaults(&wf)
		internal.FireWorkflowHooks(ctx, e.hookNotifier, &wf, workflowRunID, model.PhaseTimeout)
	}
}

// loadWorkflow loads and deserializes the workflow from a WorkflowRun.
func (e *Engine) loadWorkflow(ctx context.Context, workflowRunID string) (*model.Workflow, error) {
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
