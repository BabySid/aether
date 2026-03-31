// Package hook defines the lifecycle hook notification abstraction for aether.
package hook

import (
	"context"
	"errors"
)

// Notifier sends lifecycle hook notifications (optional).
type Notifier interface {
	// Notify sends a hook event.
	Notify(ctx context.Context, event *Event) error
}

// Type identifies a lifecycle hook.
type Type string

const (
	OnStart   Type = "onStart"
	OnSuccess Type = "onSuccess"
	OnFailure Type = "onFailure"
	OnError   Type = "onError"
	OnSuspend Type = "onSuspend"
	OnResume  Type = "onResume"
	OnCancel  Type = "onCancel"
	OnExit    Type = "onExit"
)

// Scope identifies the level at which a hook fires.
type Scope string

const (
	ScopeWorkflow Scope = "workflow"
	ScopeTask     Scope = "task"
)

// Event is the payload for Notifier.Notify.
type Event struct {
	HookType      Type
	Scope         Scope
	WorkflowRunID string
	WorkflowName  string
	TaskRunID     string // only for ScopeTask
	TaskName      string // only for ScopeTask
	Template      string
	Context       map[string]any
}

// CompositeNotifier fans out a single Notify call to multiple Notifiers.
// All notifiers are called; errors are joined. Individual failures do not
// prevent other notifiers from being called.
type CompositeNotifier []Notifier

func (c CompositeNotifier) Notify(ctx context.Context, event *Event) error {
	var errs []error
	for _, n := range c {
		if err := n.Notify(ctx, event); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
