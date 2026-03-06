package internal

import (
	"context"

	"github.com/BabySid/aether/hook"
	"github.com/BabySid/aether/model"
)

// NotifyHook sends a hook event if the notifier is configured.
// It is a no-op if notifier is nil.
func NotifyHook(ctx context.Context, notifier hook.Notifier, event *hook.Event) {
	if notifier == nil || event == nil {
		return
	}
	_ = notifier.Notify(ctx, event)
}

// FireWorkflowHooks fires the appropriate workflow-level hook based on the phase.
func FireWorkflowHooks(ctx context.Context, notifier hook.Notifier, wf *model.Workflow, workflowRunID uint64, phase model.Phase) {
	if notifier == nil || wf.Spec.Hooks == nil {
		return
	}

	hooks := wf.Spec.Hooks
	eventCtx := map[string]any{
		"workflowRunID": workflowRunID,
		"workflowName":  wf.Metadata.Name,
	}

	// Fire phase-specific hook
	switch phase {
	case model.PhaseRunning:
		if hooks.OnStart != nil {
			NotifyHook(ctx, notifier, &hook.Event{
				HookType:   hook.OnStart,
				WorkflowID: workflowRunID,
				Template:   hooks.OnStart.Template,
				Context:    eventCtx,
			})
		}
	case model.PhaseSucceeded:
		if hooks.OnSuccess != nil {
			NotifyHook(ctx, notifier, &hook.Event{
				HookType:   hook.OnSuccess,
				WorkflowID: workflowRunID,
				Template:   hooks.OnSuccess.Template,
				Context:    eventCtx,
			})
		}
	case model.PhaseFailed:
		if hooks.OnFailure != nil {
			NotifyHook(ctx, notifier, &hook.Event{
				HookType:   hook.OnFailure,
				WorkflowID: workflowRunID,
				Template:   hooks.OnFailure.Template,
				Context:    eventCtx,
			})
		}
	case model.PhaseError, model.PhaseTimeout:
		if hooks.OnError != nil {
			NotifyHook(ctx, notifier, &hook.Event{
				HookType:   hook.OnError,
				WorkflowID: workflowRunID,
				Template:   hooks.OnError.Template,
				Context:    eventCtx,
			})
		}
	}

	// Always fire onExit (if configured) for terminal phases
	if phase.IsTerminal() && hooks.OnExit != nil {
		eventCtx["phase"] = string(phase)
		NotifyHook(ctx, notifier, &hook.Event{
			HookType:   hook.OnExit,
			WorkflowID: workflowRunID,
			Template:   hooks.OnExit.Template,
			Context:    eventCtx,
		})
	}
}

// FireTaskHooks fires the appropriate task-level hook based on the phase.
func FireTaskHooks(ctx context.Context, notifier hook.Notifier, task *model.Task, workflowRunID uint64, phase model.Phase) {
	if notifier == nil || task == nil || task.Hooks == nil {
		return
	}

	hooks := task.Hooks
	eventCtx := map[string]any{
		"workflowRunID": workflowRunID,
		"taskName":      task.Name,
	}

	switch phase {
	case model.PhaseSucceeded:
		if hooks.OnSuccess != nil {
			NotifyHook(ctx, notifier, &hook.Event{
				HookType:   hook.OnSuccess,
				WorkflowID: workflowRunID,
				Template:   hooks.OnSuccess.Template,
				Context:    eventCtx,
			})
		}
	case model.PhaseFailed:
		if hooks.OnFailure != nil {
			NotifyHook(ctx, notifier, &hook.Event{
				HookType:   hook.OnFailure,
				WorkflowID: workflowRunID,
				Template:   hooks.OnFailure.Template,
				Context:    eventCtx,
			})
		}
	case model.PhaseError, model.PhaseTimeout:
		if hooks.OnError != nil {
			NotifyHook(ctx, notifier, &hook.Event{
				HookType:   hook.OnError,
				WorkflowID: workflowRunID,
				Template:   hooks.OnError.Template,
				Context:    eventCtx,
			})
		}
	}

	if phase.IsTerminal() && hooks.OnExit != nil {
		eventCtx["phase"] = string(phase)
		NotifyHook(ctx, notifier, &hook.Event{
			HookType:   hook.OnExit,
			WorkflowID: workflowRunID,
			Template:   hooks.OnExit.Template,
			Context:    eventCtx,
		})
	}
}
