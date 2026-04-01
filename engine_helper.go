package aether

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BabySid/aether/errsink"
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
			switch ev.Kind {
			case timeout.KindTask:
				e.OnTaskTimeout(ctx, ev.RunID)
			case timeout.KindWorkflow:
				e.OnWorkflowTimeout(ctx, ev.RunID)
			}
		}
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
