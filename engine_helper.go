package aether

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/BabySid/aether/errsink"
	"github.com/BabySid/aether/idgen"
	"github.com/BabySid/aether/internal"
	"github.com/BabySid/aether/internal/binding"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
	"github.com/BabySid/aether/timeout"
)

// newVarBuilder returns an VarBuilder pre-seeded with all engine-level Sources.
// Per-call providers (WorkflowArgsProvider, SiblingTaskRunsProvider, etc.) should be
// appended after this call via WithProvider or the With* convenience methods.
//
// Engine-level providers have lower priority: per-call providers appended later will
// overwrite any keys set by engine-level providers for the same name.
func (e *Engine) newVarBuilder() *binding.VarBuilder {
	b := binding.NewVarBuilder()
	for _, p := range e.varsSources {
		b.WithSource(p)
	}
	return b
}

// watchTimeouts consumes TimeoutEvents from the Watcher and drives the state machine.
func (e *Engine) watchTimeouts(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-e.timeoutWatcher.Events():
			e.handleTimeoutEvent(ctx, ev)
		}
	}
}

// handleTimeoutEvent processes a single timeout event with panic recovery,
// ensuring the watchdog goroutine survives even if a handler panics.
func (e *Engine) handleTimeoutEvent(ctx context.Context, ev timeout.TimeoutEvent) {
	defer func() {
		if r := recover(); r != nil {
			e.reportError(ctx, fmt.Errorf("panic in timeout handler: %v", r), errsink.ErrorContext{
				Operation: "watchTimeouts.panic", Severity: errsink.SeverityCritical,
			})
		}
	}()
	switch ev.Kind {
	case timeout.KindTask:
		e.OnTaskTimeout(ctx, ev.RunID)
	case timeout.KindWorkflow:
		e.OnWorkflowTimeout(ctx, ev.RunID)
	}
}

// reportError sends an error to the ErrorSink (if configured).
// It is a no-op when no ErrorSink is present.
func (e *Engine) reportError(ctx context.Context, err error, ec errsink.ErrorContext) {
	if e.errorSink != nil {
		e.errorSink.OnError(ctx, err, ec)
	}
}

// tryMarkWorkflowError is the last-resort fallback: when a critical-path operation
// fails (advanceScope, dispatchLeafTask), try to transition the workflow to PhaseError
// so it does not hang in a non-terminal state forever.
func (e *Engine) tryMarkWorkflowError(ctx context.Context, workflowRunID string, cause error) {
	wr, getErr := e.store.GetWorkflowRun(ctx, workflowRunID)
	if getErr != nil {
		e.reportError(ctx, fmt.Errorf("tryMarkWorkflowError: get workflow: %w (original: %v)", getErr, cause), errsink.ErrorContext{
			WorkflowRunID: workflowRunID,
			Operation:     "tryMarkWorkflowError",
			Severity:      errsink.SeverityCritical,
		})
		return
	}
	if wr.Status != nil && wr.Status.IsTerminal() {
		return
	}
	errPhase := model.PhaseError
	errMsg := fmt.Sprintf("engine internal error: %v", cause)
	_, updateErr := e.store.UpdateWorkflowRun(ctx, &store.WorkflowRun{
		RunID:   wr.RunID,
		Token:   wr.Token,
		Status:  &errPhase,
		Message: &errMsg,
	})
	if updateErr != nil {
		e.reportError(ctx, fmt.Errorf("tryMarkWorkflowError: update failed: %w (original: %v)", updateErr, cause), errsink.ErrorContext{
			WorkflowRunID: workflowRunID,
			Operation:     "tryMarkWorkflowError",
			Severity:      errsink.SeverityCritical,
		})
	}
}

// tryMarkTaskError is a fallback that transitions a task to PhaseError when a
// critical operation (e.g. retry dispatch) fails, preventing the task from hanging.
func (e *Engine) tryMarkTaskError(ctx context.Context, workflowRunID, taskRunID string, cause error) {
	tr, getErr := e.store.GetTaskRun(ctx, taskRunID)
	if getErr != nil {
		e.reportError(ctx, fmt.Errorf("tryMarkTaskError: get failed: %w (original: %v)", getErr, cause), errsink.ErrorContext{
			WorkflowRunID: workflowRunID, TaskRunID: taskRunID,
			Operation: "tryMarkTaskError", Severity: errsink.SeverityCritical,
		})
		return
	}
	if tr.Status != nil && tr.Status.IsTerminal() {
		return
	}
	errPhase := model.PhaseError
	errMsg := fmt.Sprintf("engine internal error: %v", cause)
	if _, updateErr := e.store.UpdateTaskRun(ctx, &store.TaskRun{
		RunID:   tr.RunID,
		Token:   tr.Token,
		Status:  &errPhase,
		Message: &errMsg,
	}); updateErr != nil {
		e.reportError(ctx, fmt.Errorf("tryMarkTaskError: update failed: %w (original: %v)", updateErr, cause), errsink.ErrorContext{
			WorkflowRunID: workflowRunID, TaskRunID: taskRunID,
			Operation: "tryMarkTaskError", Severity: errsink.SeverityCritical,
		})
	}
}

// submitInternal is the shared submission path. cronWorkflowID is non-empty
// when the workflow is triggered by a CronWorkflow.
func (e *Engine) submitInternal(ctx context.Context, wf *model.Workflow, cronWorkflowID string) (string, error) {
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
	workflowRunID := e.idGen.Generate(idgen.Context{WorkflowKind: wf.Kind})
	pendingPhase := model.PhaseCreated
	run := &store.WorkflowRun{
		RunID:          workflowRunID,
		Workflow:       rawJSON,
		Status:         &pendingPhase,
		CronWorkflowID: cronWorkflowID,
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

	// 8. Create entry TaskRun (Created, ParentRunID="" for top-level scope)
	entryPending := model.PhaseCreated
	entryTaskRun := &store.TaskRun{
		RunID: e.idGen.Generate(idgen.Context{
			WorkflowRunID: workflowRunID,
			WorkflowKind:  wf.Kind,
			TaskName:      wf.Spec.Entrypoint,
			TemplateName:  wf.Spec.Entrypoint,
		}),
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

// fireTaskHookFromParent resolves the parent DAG template, finds the task node, and fires
// the appropriate task-level hooks. This extracts the repeated pattern of:
//   parentTmpl := FindTemplate(wf, parentTR.TemplateName)
//   if parentTmpl != nil && parentTmpl.DAG != nil { task := FindTask(...); FireTaskHooks(...) }
func (e *Engine) fireTaskHookFromParent(ctx context.Context, wf *model.Workflow, parentTR *store.TaskRun, tr *store.TaskRun, workflowRunID string, phase model.Phase) {
	parentTmpl := internal.FindTemplate(wf, parentTR.TemplateName)
	if parentTmpl != nil && parentTmpl.DAG != nil {
		task := internal.FindTask(parentTmpl.DAG, tr.TaskName)
		internal.FireTaskHooks(ctx, e.hookNotifier, e.errorSink, task, workflowRunID, tr.RunID, phase)
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
