// engine.go contains only the public API of the Engine (exported methods).
// All unexported helper methods belong in engine_helper.go or other engine_*.go files.
package aether

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/BabySid/aether/artifact"
	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/cron"
	"github.com/BabySid/aether/errsink"
	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/expr"
	"github.com/BabySid/aether/hook"
	"github.com/BabySid/aether/idgen"
	"github.com/BabySid/aether/internal"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/secret"
	"github.com/BabySid/aether/store"
	"github.com/BabySid/aether/timeout"
	"github.com/BabySid/aether/vars"
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
	errorSink      errsink.ErrorSink
	idGen          idgen.Generator
	timeoutWatcher timeout.Watcher
	stopOnce sync.Once           // ensures Stop logic runs exactly once
	stopFn   context.CancelFunc // set by Start; called by Stop

	// varsSources holds engine-level Source registrations (global, per-engine).
	// These are injected into every VarBuilder via newVarBuilder(), giving each
	// workflow run access to the variables they provide (e.g. system.os, system.arch).
	varsSources []vars.Source

	// cronScheduler is the optional cron scheduling backend.
	// Nil when CronWorkflow support is not configured.
	cronScheduler cron.Scheduler
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

// --- Submit ---

// Submit parses, validates, and dispatches a workflow for execution.
// Returns the workflow run ID. Non-blocking.
//
// All checks (marshal, fill defaults, validate, resolve entrypoint) are
// performed before any state is persisted to Store.
func (e *Engine) Submit(ctx context.Context, wf *model.Workflow) (string, error) {
	return e.submitInternal(ctx, wf, "")
}

// SubmitCronWorkflow validates and registers a CronWorkflow for periodic execution.
// Returns the system-generated cronID.
func (e *Engine) SubmitCronWorkflow(ctx context.Context, cw *model.CronWorkflow) (string, error) {
	if e.cronScheduler == nil {
		return "", fmt.Errorf("aether: %w: CronScheduler not configured, use WithCronScheduler()", ErrNotSupported)
	}
	if cw == nil {
		return "", fmt.Errorf("aether: %w: CronWorkflow must not be nil", ErrValidation)
	}

	// Marshal immutable snapshot.
	rawJSON, err := json.Marshal(cw)
	if err != nil {
		return "", fmt.Errorf("aether: marshal CronWorkflow: %w", err)
	}

	// Fill defaults and validate.
	internal.FillCronDefaults(cw)
	if err := internal.ValidateCronWorkflow(cw); err != nil {
		return "", fmt.Errorf("aether: %w: %v", ErrValidation, err)
	}

	// Generate cronID and persist.
	cronID := e.idGen.Generate(idgen.Context{WorkflowKind: "CronWorkflow"})
	record := &store.CronWorkflowRecord{
		CronID:       cronID,
		CronWorkflow: rawJSON,
	}
	if err := e.store.CreateCronWorkflow(ctx, record); err != nil {
		return "", fmt.Errorf("aether: create CronWorkflow: %w", err)
	}

	// Register with scheduler (unless suspended).
	if !cw.Spec.Suspend {
		if err := e.cronScheduler.Add(cronID, cw.Spec.Schedule, cw.Spec.Timezone, func() {
			e.triggerCronRun(cronID)
		}); err != nil {
			return "", fmt.Errorf("aether: register cron schedule: %w", err)
		}
	}

	return cronID, nil
}

// --- Get ---

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

	// 3. Project store types into public read-only types
	exec := &WorkflowExecution{
		RunID:     run.RunID,
		Outputs:   run.Outputs,
		Metrics:   run.Metrics,
		CreatedAt: run.CreatedAt,
	}
	if run.Status != nil {
		exec.Status = *run.Status
	}
	if run.Message != nil {
		exec.Message = *run.Message
	}

	tasks := make([]TaskExecution, len(taskRuns))
	completedTasks := 0
	for i, tr := range taskRuns {
		tasks[i] = TaskExecution{
			RunID:         tr.RunID,
			WorkflowRunID: tr.WorkflowRunID,
			ParentRunID:   tr.ParentRunID,
			Depth:         tr.Depth,
			Scope:         tr.Scope,
			TaskName:      tr.TaskName,
			TemplateName:  tr.TemplateName,
			TemplateType:  tr.TemplateType,
			CreatedAt:     tr.CreatedAt,
			Inputs:        tr.Inputs,
			Outputs:       tr.Outputs,
			Metrics:       tr.Metrics,
		}
		if tr.Status != nil {
			tasks[i].Status = *tr.Status
			if tr.Status.IsTerminal() {
				completedTasks++
			}
		}
		if tr.Message != nil {
			tasks[i].Message = *tr.Message
		}
		if tr.RetryCount != nil {
			tasks[i].RetryCount = *tr.RetryCount
		}
	}
	exec.Tasks = tasks
	if len(tasks) > 0 {
		exec.Progress = fmt.Sprintf("%d/%d", completedTasks, len(tasks))
	}

	return exec, nil
}

// GetCronWorkflow returns the execution state of a CronWorkflow and its triggered runs.
func (e *Engine) GetCronWorkflow(ctx context.Context, cronID string) (*CronWorkflowExecution, error) {
	if e.cronScheduler == nil {
		return nil, fmt.Errorf("aether: %w: CronScheduler not configured, use WithCronScheduler()", ErrNotSupported)
	}

	if _, err := e.store.GetCronWorkflow(ctx, cronID); err != nil {
		return nil, fmt.Errorf("aether: %w", err)
	}

	runs, err := e.store.ListWorkflowRunsByCronID(ctx, cronID)
	if err != nil {
		return nil, fmt.Errorf("aether: list workflow runs: %w", err)
	}

	exec := &CronWorkflowExecution{ID: cronID}
	for _, run := range runs {
		wfExec, err := e.Get(ctx, run.RunID)
		if err != nil {
			continue
		}
		exec.Runs = append(exec.Runs, *wfExec)
	}
	return exec, nil
}

// --- CronWorkflow management ---

// UpdateCronWorkflow updates the mutable fields of a CronWorkflow.
// WorkflowSpec is immutable; changes to it return ErrValidation.
func (e *Engine) UpdateCronWorkflow(ctx context.Context, cronID string, cw *model.CronWorkflow) error {
	if e.cronScheduler == nil {
		return fmt.Errorf("aether: %w: CronScheduler not configured, use WithCronScheduler()", ErrNotSupported)
	}
	if cw == nil {
		return fmt.Errorf("aether: %w: CronWorkflow must not be nil", ErrValidation)
	}

	// Load existing record.
	record, err := e.store.GetCronWorkflow(ctx, cronID)
	if err != nil {
		return fmt.Errorf("aether: %w", err)
	}

	// Unmarshal existing CronWorkflow to check WorkflowSpec immutability.
	var existing model.CronWorkflow
	if err := json.Unmarshal(record.CronWorkflow, &existing); err != nil {
		return fmt.Errorf("aether: unmarshal existing CronWorkflow: %w", err)
	}

	// Check WorkflowSpec immutability by comparing serialized JSON.
	oldSpec, _ := json.Marshal(existing.Spec.WorkflowSpec)
	newSpec, _ := json.Marshal(cw.Spec.WorkflowSpec)
	if string(oldSpec) != string(newSpec) {
		return fmt.Errorf("aether: %w: WorkflowSpec is immutable; delete and re-submit to change it", ErrValidation)
	}

	// Fill defaults and validate.
	internal.FillCronDefaults(cw)
	if err := internal.ValidateCronWorkflow(cw); err != nil {
		return fmt.Errorf("aether: %w: %v", ErrValidation, err)
	}

	// Marshal updated CronWorkflow.
	rawJSON, err := json.Marshal(cw)
	if err != nil {
		return fmt.Errorf("aether: marshal CronWorkflow: %w", err)
	}

	// Update store.
	record.CronWorkflow = rawJSON
	if err := e.store.UpdateCronWorkflow(ctx, record); err != nil {
		return fmt.Errorf("aether: update CronWorkflow: %w", err)
	}

	// Re-register with scheduler.
	e.cronScheduler.Remove(cronID)
	if !cw.Spec.Suspend {
		if err := e.cronScheduler.Add(cronID, cw.Spec.Schedule, cw.Spec.Timezone, func() {
			e.triggerCronRun(cronID)
		}); err != nil {
			return fmt.Errorf("aether: register cron schedule: %w", err)
		}
	}

	return nil
}

// DeleteCronWorkflow stops scheduling and removes a CronWorkflow.
// Already-created WorkflowRuns are NOT cascade-deleted.
func (e *Engine) DeleteCronWorkflow(ctx context.Context, cronID string) error {
	if e.cronScheduler == nil {
		return fmt.Errorf("aether: %w: CronScheduler not configured, use WithCronScheduler()", ErrNotSupported)
	}

	e.cronScheduler.Remove(cronID)
	if err := e.store.DeleteCronWorkflow(ctx, cronID); err != nil {
		return fmt.Errorf("aether: %w", err)
	}
	return nil
}

// --- Control ---

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
	if tr.Status == nil || *tr.Status != model.PhaseSuspended {
		// Task not suspended (already completed, still running, timed out, or cancelled) — no-op.
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
	if dispatchErr := e.taskBroker.Dispatch(ctx, assignment); dispatchErr != nil {
		return dispatchErr
	}

	// Fire task-level OnResume hook.
	if parentTR != nil {
		parentTmpl := internal.FindTemplate(wf, parentTR.TemplateName)
		if parentTmpl != nil && parentTmpl.DAG != nil {
			task := internal.FindTask(parentTmpl.DAG, tr.TaskName)
			internal.FireResumeHook(ctx, e.hookNotifier, e.errorSink, task, tr.WorkflowRunID, tr.RunID)
		}
	}
	return nil
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
	// Created/Ready tasks: transition directly to PhaseCancelled via Get+Update.
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
		case model.PhaseRunning, model.PhaseSuspended:
			if brokerErr := e.taskBroker.Cancel(ctx, tr.RunID); brokerErr != nil {
				e.reportError(ctx, brokerErr, errsink.ErrorContext{
					WorkflowRunID: workflowID, TaskRunID: tr.RunID,
					Operation: "cancel.brokerCancel", Severity: errsink.SeverityWarning,
				})
			}
			if _, updateErr := e.store.UpdateTaskRun(ctx, &store.TaskRun{
				RunID:   tr.RunID,
				Token:   tr.Token,
				Status:  &cancelled,
				Message: &cancelMsg,
			}); updateErr != nil && !errors.Is(updateErr, store.ErrTokenMismatch) {
				e.reportError(ctx, updateErr, errsink.ErrorContext{
					WorkflowRunID: workflowID, TaskRunID: tr.RunID,
					Operation: "cancel.updateTaskRun", Severity: errsink.SeverityError,
				})
			}
		case model.PhaseCreated, model.PhaseReady:
			if _, updateErr := e.store.UpdateTaskRun(ctx, &store.TaskRun{
				RunID:   tr.RunID,
				Token:   tr.Token,
				Status:  &cancelled,
				Message: &cancelMsg,
			}); updateErr != nil && !errors.Is(updateErr, store.ErrTokenMismatch) {
				e.reportError(ctx, updateErr, errsink.ErrorContext{
					WorkflowRunID: workflowID, TaskRunID: tr.RunID,
					Operation: "cancel.updateTaskRun", Severity: errsink.SeverityError,
				})
			}
		}
	}

	// 4. Mark workflow as Cancelled.
	// Use Get + UpdateWorkflowRun to avoid overwriting a terminal status already set by finalizeWorkflow.
	wfRun, err := e.store.GetWorkflowRun(ctx, workflowID)
	if err == nil && (wfRun.Status == nil || !wfRun.Status.IsTerminal()) {
		if _, updateErr := e.store.UpdateWorkflowRun(ctx, &store.WorkflowRun{
			RunID:   wfRun.RunID,
			Token:   wfRun.Token,
			Status:  &cancelled,
			Message: &cancelMsg,
		}); updateErr != nil && !errors.Is(updateErr, store.ErrTokenMismatch) {
			e.reportError(ctx, updateErr, errsink.ErrorContext{
				WorkflowRunID: workflowID,
				Operation:     "cancel.updateWorkflowRun", Severity: errsink.SeverityError,
			})
		}
	}

	var wf model.Workflow
	if err := json.Unmarshal(run.Workflow, &wf); err == nil {
		internal.FillDefaults(&wf)

		// Fire task-level cancel hooks for all cancelled tasks.
		for _, tr := range taskRuns {
			if tr.Status == nil || tr.Status.IsTerminal() {
				continue // already terminal before cancel — hook not applicable
			}
			if tr.ParentRunID != "" {
				if parentTR, pErr := e.store.GetTaskRun(ctx, tr.ParentRunID); pErr == nil {
					parentTmpl := internal.FindTemplate(&wf, parentTR.TemplateName)
					if parentTmpl != nil && parentTmpl.DAG != nil {
						task := internal.FindTask(parentTmpl.DAG, tr.TaskName)
						internal.FireTaskHooks(ctx, e.hookNotifier, e.errorSink, task, workflowID, tr.RunID, model.PhaseCancelled)
					}
				}
			}
		}

		internal.FireWorkflowHooks(ctx, e.hookNotifier, e.errorSink, &wf, workflowID, model.PhaseCancelled)
	}

	return nil
}

// --- Task callbacks ---

// OnTaskStarted is invoked when a worker begins executing a task.
// It transitions the leaf task itself, its ancestor containers (DAG/Loop), and
// the WorkflowRun from Ready to Running, so that their Running state accurately
// reflects the moment real work begins rather than the moment the task was dispatched.
//
// Both the leaf task transition and the ancestor walk use Get+Update with Token to
// remain idempotent under concurrent or duplicate invocations.
func (e *Engine) OnTaskStarted(ctx context.Context, taskRunID string) {
	tr, err := e.store.GetTaskRun(ctx, taskRunID)
	if err != nil {
		return
	}
	// 1. Transition the leaf task itself: Ready → Running (idempotent via Token).
	// Also record StartedAt in Metrics — the engine is the authoritative Metrics writer.
	if tr.Status != nil && *tr.Status == model.PhaseReady {
		running := model.PhaseRunning
		empty := ""
		startedAt := time.Now().UTC().Format(time.RFC3339)
		if _, updateErr := e.store.UpdateTaskRun(ctx, &store.TaskRun{
			RunID:   taskRunID,
			Token:   tr.Token,
			Status:  &running,
			Message: &empty,
			Metrics: &model.Metrics{StartedAt: startedAt},
		}); updateErr != nil && !errors.Is(updateErr, store.ErrTokenMismatch) {
			e.reportError(ctx, updateErr, errsink.ErrorContext{
				WorkflowRunID: tr.WorkflowRunID, TaskRunID: taskRunID,
				Operation: "onTaskStarted.updateTaskRun", Severity: errsink.SeverityWarning,
			})
		}
	}
	// 2. Transition ancestor containers (DAG/Loop) and the WorkflowRun: Ready → Running.
	wfPromoted, markErr := e.markAncestorsRunning(ctx, tr.WorkflowRunID, tr.ParentRunID)
	if markErr != nil {
		e.reportError(ctx, markErr, errsink.ErrorContext{
			WorkflowRunID: tr.WorkflowRunID, TaskRunID: taskRunID,
			Operation: "onTaskStarted.markAncestorsRunning", Severity: errsink.SeverityWarning,
		})
	}

	// 3. Fire workflow-level OnStart hook (exactly once, when first promoted to Running).
	if wfPromoted {
		if wf, loadErr := e.loadWorkflow(ctx, tr.WorkflowRunID); loadErr == nil {
			internal.FireWorkflowHooks(ctx, e.hookNotifier, e.errorSink, wf, tr.WorkflowRunID, model.PhaseRunning)
		}
	}

	// 4. Fire task-level OnStart hook.
	if wf, loadErr := e.loadWorkflow(ctx, tr.WorkflowRunID); loadErr == nil && tr.ParentRunID != "" {
		if parentTR, parentErr := e.store.GetTaskRun(ctx, tr.ParentRunID); parentErr == nil {
			parentTmpl := internal.FindTemplate(wf, parentTR.TemplateName)
			if parentTmpl != nil && parentTmpl.DAG != nil {
				task := internal.FindTask(parentTmpl.DAG, tr.TaskName)
				internal.FireTaskHooks(ctx, e.hookNotifier, e.errorSink, task, tr.WorkflowRunID, taskRunID, model.PhaseRunning)
			}
		}
	}
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
	// Guard: only process if currently Running or Suspended (duplicate callbacks or cancel race).
	// Suspended tasks re-enter OnTaskCompleted after a Resume() re-dispatches them.
	if tr.Status == nil || (*tr.Status != model.PhaseRunning && *tr.Status != model.PhaseSuspended) {
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
		var parentErr error
		parentTR, parentErr = e.store.GetTaskRun(ctx, tr.ParentRunID)
		if parentErr != nil {
			e.reportError(ctx, parentErr, errsink.ErrorContext{
				WorkflowRunID: result.WorkflowRunID, TaskRunID: result.TaskRunID,
				Operation: "onTaskCompleted.getParentTR", Severity: errsink.SeverityWarning,
			})
		}
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
	phase := internal.EvalPhaseConditions(ctx, phaseConditions, e.exprEvaluator, result, &internal.EvalErrorContext{
		Sink: e.errorSink, WorkflowRunID: result.WorkflowRunID, TaskRunID: tr.RunID,
	})

	// --- Suspend fast-path (ExecCodeSuspended → PhaseSuspended) ---
	//
	// The executor returned ExecCodeSuspended: the task transitions to PhaseSuspended.
	// Merge the executor's partial outputs into any accumulated outputs from
	// previous Execute calls (new keys are added; existing keys are overwritten
	// by the latest executor result). Return early — no hooks, no retry, no
	// advanceScope; the DAG must not progress until a future Resume finalizes.
	if phase == model.PhaseSuspended {
		var accumulatedParams []model.Parameter
		if tr.Outputs != nil {
			accumulatedParams = tr.Outputs.Parameters
		}
		if result.ExecOutputs != nil {
			accumulatedParams = internal.MergeParameters(accumulatedParams, result.ExecOutputs.Parameters)
		}
		mergedOutputs := &model.Outputs{
			Phase:       model.PhaseSuspended,
			ExecOutputs: model.ExecOutputs{Parameters: accumulatedParams},
		}
		var suspendMsg string
		if result.ExecOutputs != nil {
			suspendMsg = result.ExecOutputs.Message
		}
		if _, suspendErr := e.store.UpdateTaskRun(ctx, &store.TaskRun{
			RunID:   tr.RunID,
			Token:   tr.Token,
			Status:  &phase,
			Message: &suspendMsg,
			Outputs: mergedOutputs,
		}); suspendErr != nil && !errors.Is(suspendErr, store.ErrTokenMismatch) {
			e.reportError(ctx, suspendErr, errsink.ErrorContext{
				WorkflowRunID: result.WorkflowRunID, TaskRunID: tr.RunID,
				Operation: "onTaskCompleted.suspend", Severity: errsink.SeverityError,
			})
		}
		// Fire task-level OnSuspend hook.
		if parentTR != nil {
			parentTmpl := internal.FindTemplate(wf, parentTR.TemplateName)
			if parentTmpl != nil && parentTmpl.DAG != nil {
				task := internal.FindTask(parentTmpl.DAG, tr.TaskName)
				internal.FireTaskHooks(ctx, e.hookNotifier, e.errorSink, task, result.WorkflowRunID, tr.RunID, model.PhaseSuspended)
			}
		}
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
			e.reportError(ctx, rerr, errsink.ErrorContext{
				WorkflowRunID: result.WorkflowRunID, TaskRunID: tr.RunID,
				Operation: "onTaskCompleted.retryExpression", Severity: errsink.SeverityWarning,
			})
			needRetry = false
		}

		if needRetry {
			// tr.Status is still Running and Token is valid — reset directly to Created.
			// Skip metrics, outputs, hooks and advanceScope — the task is not done yet.
			pending := model.PhaseCreated
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
				if dispatchErr := e.dispatchLeafTask(ctx, result.WorkflowRunID, wf, retryTR); dispatchErr != nil {
					e.reportError(ctx, dispatchErr, errsink.ErrorContext{
						WorkflowRunID: result.WorkflowRunID, TaskRunID: tr.RunID,
						Operation: "onTaskCompleted.retryDispatch", Severity: errsink.SeverityError,
					})
					e.tryMarkTaskError(ctx, result.WorkflowRunID, tr.RunID, dispatchErr)
				}
			}
			return
		}
	}

	// --- Metrics computation (engine is the single Metrics writer) ---
	//
	// StartedAt was recorded by OnTaskStarted. FinishedAt and Duration are
	// computed here from the current wall clock and the stored StartedAt.
	// Retries is taken from tr.RetryCount (authoritative) rather than Metrics.Retries.
	taskMetrics := internal.ComputeMetricsFinish(tr.Metrics)
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
			internal.FireTaskHooks(ctx, e.hookNotifier, e.errorSink, task, result.WorkflowRunID, tr.RunID, phase)
		}
	}

	// 5. Re-advance the scope this task belongs to.
	// tr.ParentRunID identifies the scope (DAG/Loop container) that owns this task.
	// advanceScope will find newly-ready tasks, or finalize the scope if all are done.
	if advErr := e.advanceScope(ctx, result.WorkflowRunID, wf, tr.ParentRunID); advErr != nil {
		e.reportError(ctx, advErr, errsink.ErrorContext{
			WorkflowRunID: result.WorkflowRunID, TaskRunID: result.TaskRunID,
			Operation: "onTaskCompleted.advanceScope", Severity: errsink.SeverityCritical,
		})
		e.tryMarkWorkflowError(ctx, result.WorkflowRunID, advErr)
	}
}

// --- Timeout callbacks ---

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
	if brokerErr := e.taskBroker.Cancel(ctx, taskRunID); brokerErr != nil {
		e.reportError(ctx, brokerErr, errsink.ErrorContext{
			WorkflowRunID: tr.WorkflowRunID, TaskRunID: taskRunID,
			Operation: "onTaskTimeout.brokerCancel", Severity: errsink.SeverityWarning,
		})
	}

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
		if _, updateErr := e.store.UpdateTaskRun(ctx, &store.TaskRun{
			RunID:   tr.RunID,
			Token:   tr.Token,
			Status:  &timeoutPhase,
			Message: &timeoutMsg,
		}); updateErr != nil && !errors.Is(updateErr, store.ErrTokenMismatch) {
			e.reportError(ctx, updateErr, errsink.ErrorContext{
				WorkflowRunID: workflowRunID, TaskRunID: tr.RunID,
				Operation: "onWorkflowTimeout.updateTaskRun", Severity: errsink.SeverityWarning,
			})
		}
		if brokerErr := e.taskBroker.Cancel(ctx, tr.RunID); brokerErr != nil {
			e.reportError(ctx, brokerErr, errsink.ErrorContext{
				WorkflowRunID: workflowRunID, TaskRunID: tr.RunID,
				Operation: "onWorkflowTimeout.brokerCancel", Severity: errsink.SeverityWarning,
			})
		}
	}

	// Mark the workflow itself as Timeout.
	// Use wr.Token fetched at the top — the Token-based optimistic lock guards
	// against a concurrent writer; if the Token is stale the update is a no-op.
	wfMsg := "workflow deadline exceeded"
	if _, updateErr := e.store.UpdateWorkflowRun(ctx, &store.WorkflowRun{
		RunID:   workflowRunID,
		Token:   wr.Token,
		Status:  &timeoutPhase,
		Message: &wfMsg,
	}); updateErr != nil && !errors.Is(updateErr, store.ErrTokenMismatch) {
		e.reportError(ctx, updateErr, errsink.ErrorContext{
			WorkflowRunID: workflowRunID,
			Operation:     "onWorkflowTimeout.updateWorkflowRun", Severity: errsink.SeverityError,
		})
	}

	// Fire workflow hooks.
	var wf model.Workflow
	if jsonErr := json.Unmarshal(wr.Workflow, &wf); jsonErr == nil {
		internal.FillDefaults(&wf)
		internal.FireWorkflowHooks(ctx, e.hookNotifier, e.errorSink, &wf, workflowRunID, model.PhaseTimeout)
	}
}

// --- Lifecycle ---

// Start launches the Engine's background services (currently: timeout watchdog).
// It returns immediately; all background goroutines run until Stop is called or
// the parent ctx is cancelled. Pair every Start call with a Stop call.
func (e *Engine) Start(ctx context.Context) error {
	innerCtx, cancel := context.WithCancel(ctx)
	e.stopFn = cancel
	e.stopOnce = sync.Once{} // reset for a new Start cycle
	if err := e.timeoutWatcher.Start(innerCtx); err != nil {
		cancel()
		return fmt.Errorf("aether: start timeout watcher: %w", err)
	}
	go e.watchTimeouts(innerCtx)

	// Reload active CronWorkflows and re-register with the scheduler (crash recovery).
	if e.cronScheduler != nil {
		if err := e.reloadCronWorkflows(ctx); err != nil {
			e.reportError(ctx, err, errsink.ErrorContext{
				Operation: "start.reloadCronWorkflows", Severity: errsink.SeverityWarning,
			})
		}
	}
	return nil
}

// Stop shuts down all background services started by Start.
// It is safe to call Stop concurrently or multiple times; only the first call takes effect.
func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		if e.stopFn != nil {
			e.stopFn()
		}
		if e.cronScheduler != nil {
			if closer, ok := e.cronScheduler.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		}
	})
}
