// Package timeout defines the timeout detection abstraction for aether.
//
// The Watcher interface decouples timeout detection from the Engine:
// the Engine only consumes TimeoutEvents and drives the state machine;
// how and when those events are produced is entirely up to the Watcher
// implementation (in-process polling, Redis ZSET scanner, DB cron job, …).
//
// Design invariants:
//
//  1. Events are delivered at-least-once.  Callers MUST be idempotent.
//  2. The Watcher does not modify any state — it only discovers and emits.
//  3. Deadline data lives in the Store (store.TaskRun.Deadline /
//     store.WorkflowRun.Deadline), so the Watcher is stateless and
//     survives Engine restarts without losing pending timeouts.
package timeout

import "context"

// Kind distinguishes the type of entity that has timed out.
type Kind int

const (
	// KindTask indicates a single TaskRun has exceeded its deadline.
	KindTask Kind = iota
	// KindWorkflow indicates an entire WorkflowRun has exceeded its deadline.
	KindWorkflow
)

// TimeoutEvent describes a single timed-out entity.
// It is intentionally minimal: only the identity is carried here.
// The Engine reads additional context (phase, outputs, …) from the Store
// when handling the event.
type TimeoutEvent struct {
	Kind  Kind
	RunID string // TaskRun.RunID (KindTask) or WorkflowRun.RunID (KindWorkflow)
}

// Watcher detects deadline expiry and streams TimeoutEvents to the Engine.
//
// Lifecycle:
//
//	watcher.Start(ctx)   — begins background detection; stops when ctx is cancelled.
//	<-watcher.Events()   — consume events in a select loop.
type Watcher interface {
	// Start launches background detection.  Returns immediately; detection
	// runs in one or more goroutines until Stop is called.
	// Calling Start more than once is a no-op (implementations may log a warning).
	Start(ctx context.Context) error

	// Stop shuts down background detection.
	// It is safe to call Stop multiple times; only the first call takes effect.
	Stop()

	// Events returns the channel on which TimeoutEvents are delivered.
	// The channel is never closed; callers should stop consuming when ctx is done.
	Events() <-chan TimeoutEvent
}
