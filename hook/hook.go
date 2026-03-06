// Package hook defines the lifecycle hook notification abstraction for aether.
package hook

import "context"

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
	OnExit    Type = "onExit"
)

// Event is the payload for Notifier.Notify.
type Event struct {
	HookType   Type
	WorkflowID uint64
	Template   string
	Context    map[string]any
}
