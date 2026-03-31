package internal

import (
	"context"

	"github.com/BabySid/aether/errsink"
	"github.com/BabySid/aether/hook"
	"github.com/BabySid/aether/model"
)

// NotifyHook sends a hook event if the notifier is configured.
// Hook errors are reported to the ErrorSink (if provided) but never propagated
// to the caller — hooks are fire-and-forget by design.
func NotifyHook(ctx context.Context, notifier hook.Notifier, sink errsink.ErrorSink, event *hook.Event) {
	if notifier == nil || event == nil {
		return
	}
	if err := notifier.Notify(ctx, event); err != nil {
		if sink != nil {
			sink.OnError(ctx, err, errsink.ErrorContext{
				WorkflowRunID: event.WorkflowRunID,
				TaskRunID:     event.TaskRunID,
				Operation:     "hook." + string(event.HookType),
				Severity:      errsink.SeverityWarning,
			})
		}
	}
}

// FireWorkflowHooks fires the appropriate workflow-level hook based on the phase.
func FireWorkflowHooks(ctx context.Context, notifier hook.Notifier, sink errsink.ErrorSink, wf *model.Workflow, workflowRunID string, phase model.Phase) {
	if notifier == nil || wf.Spec.Hooks == nil {
		return
	}

	hooks := wf.Spec.Hooks
	eventCtx := map[string]any{
		"workflowRunID": workflowRunID,
		"workflowName":  wf.Metadata.Name,
	}

	makeEvent := func(ht hook.Type, tmpl string) *hook.Event {
		return &hook.Event{
			HookType:      ht,
			Scope:         hook.ScopeWorkflow,
			WorkflowRunID: workflowRunID,
			WorkflowName:  wf.Metadata.Name,
			Template:      tmpl,
			Context:       eventCtx,
		}
	}

	// Fire phase-specific hook
	switch phase {
	case model.PhaseRunning:
		if hooks.OnStart != nil {
			NotifyHook(ctx, notifier, sink, makeEvent(hook.OnStart, hooks.OnStart.Template))
		}
	case model.PhaseSucceeded:
		if hooks.OnSuccess != nil {
			NotifyHook(ctx, notifier, sink, makeEvent(hook.OnSuccess, hooks.OnSuccess.Template))
		}
	case model.PhaseFailed:
		if hooks.OnFailure != nil {
			NotifyHook(ctx, notifier, sink, makeEvent(hook.OnFailure, hooks.OnFailure.Template))
		}
	case model.PhaseError, model.PhaseTimeout:
		if hooks.OnError != nil {
			NotifyHook(ctx, notifier, sink, makeEvent(hook.OnError, hooks.OnError.Template))
		}
	case model.PhaseCancelled:
		if hooks.OnCancel != nil {
			NotifyHook(ctx, notifier, sink, makeEvent(hook.OnCancel, hooks.OnCancel.Template))
		}
	}

	// Always fire onExit (if configured) for terminal phases
	if phase.IsTerminal() && hooks.OnExit != nil {
		exitCtx := make(map[string]any, len(eventCtx)+1)
		for k, v := range eventCtx {
			exitCtx[k] = v
		}
		exitCtx["phase"] = string(phase)
		exitEvent := makeEvent(hook.OnExit, hooks.OnExit.Template)
		exitEvent.Context = exitCtx
		NotifyHook(ctx, notifier, sink, exitEvent)
	}
}

// FireTaskHooks fires the appropriate task-level hook based on the phase.
func FireTaskHooks(ctx context.Context, notifier hook.Notifier, sink errsink.ErrorSink, task *model.Task, workflowRunID, taskRunID string, phase model.Phase) {
	if notifier == nil || task == nil || task.Hooks == nil {
		return
	}

	hooks := task.Hooks
	eventCtx := map[string]any{
		"workflowRunID": workflowRunID,
		"taskRunID":     taskRunID,
		"taskName":      task.Name,
	}

	makeEvent := func(ht hook.Type, tmpl string) *hook.Event {
		return &hook.Event{
			HookType:      ht,
			Scope:         hook.ScopeTask,
			WorkflowRunID: workflowRunID,
			TaskRunID:     taskRunID,
			TaskName:      task.Name,
			Template:      tmpl,
			Context:       eventCtx,
		}
	}

	switch phase {
	case model.PhaseRunning:
		if hooks.OnStart != nil {
			NotifyHook(ctx, notifier, sink, makeEvent(hook.OnStart, hooks.OnStart.Template))
		}
	case model.PhaseSucceeded:
		if hooks.OnSuccess != nil {
			NotifyHook(ctx, notifier, sink, makeEvent(hook.OnSuccess, hooks.OnSuccess.Template))
		}
	case model.PhaseFailed:
		if hooks.OnFailure != nil {
			NotifyHook(ctx, notifier, sink, makeEvent(hook.OnFailure, hooks.OnFailure.Template))
		}
	case model.PhaseError, model.PhaseTimeout:
		if hooks.OnError != nil {
			NotifyHook(ctx, notifier, sink, makeEvent(hook.OnError, hooks.OnError.Template))
		}
	case model.PhaseSuspended:
		if hooks.OnSuspend != nil {
			NotifyHook(ctx, notifier, sink, makeEvent(hook.OnSuspend, hooks.OnSuspend.Template))
		}
	case model.PhaseCancelled:
		if hooks.OnCancel != nil {
			NotifyHook(ctx, notifier, sink, makeEvent(hook.OnCancel, hooks.OnCancel.Template))
		}
	}

	if phase.IsTerminal() && hooks.OnExit != nil {
		exitCtx := make(map[string]any, len(eventCtx)+1)
		for k, v := range eventCtx {
			exitCtx[k] = v
		}
		exitCtx["phase"] = string(phase)
		exitEvent := makeEvent(hook.OnExit, hooks.OnExit.Template)
		exitEvent.Context = exitCtx
		NotifyHook(ctx, notifier, sink, exitEvent)
	}
}

// FireResumeHook fires the task-level OnResume hook when a suspended task is
// re-dispatched via Engine.Resume(). This is separate from FireTaskHooks because
// Resume transitions PhaseSuspended → PhaseRunning, and PhaseRunning in
// FireTaskHooks fires OnStart (not OnResume).
func FireResumeHook(ctx context.Context, notifier hook.Notifier, sink errsink.ErrorSink, task *model.Task, workflowRunID, taskRunID string) {
	if notifier == nil || task == nil || task.Hooks == nil || task.Hooks.OnResume == nil {
		return
	}
	NotifyHook(ctx, notifier, sink, &hook.Event{
		HookType:      hook.OnResume,
		Scope:         hook.ScopeTask,
		WorkflowRunID: workflowRunID,
		TaskRunID:     taskRunID,
		TaskName:      task.Name,
		Template:      task.Hooks.OnResume.Template,
		Context: map[string]any{
			"workflowRunID": workflowRunID,
			"taskRunID":     taskRunID,
			"taskName":      task.Name,
		},
	})
}
