// Package errsink defines the error observation interface for the aether engine.
//
// ErrorSink is an optional, non-blocking channel for engine-internal errors.
// It is injected via aether.WithErrorSink and allows external systems (monitoring,
// alerting) to observe errors that occur inside void callbacks (OnTaskStarted,
// OnTaskCompleted) or non-critical paths (hook notifications, ancestor marking).
//
// ErrorSink never influences the engine's scheduling decisions — it is purely
// an observation outlet. If no ErrorSink is configured, errors are silently
// discarded (the engine's behaviour is identical either way).
package errsink

import "context"

// Severity classifies the impact of an engine-internal error.
type Severity int

const (
	// SeverityInfo indicates expected, benign events (e.g. token mismatch
	// from concurrent writers — normal optimistic-lock contention).
	SeverityInfo Severity = iota

	// SeverityWarning indicates non-critical errors that do not affect
	// workflow correctness (e.g. hook notification failure, ancestor marking failure).
	SeverityWarning

	// SeverityError indicates errors on the critical scheduling path that
	// may cause individual tasks to stall (e.g. dispatchLeafTask failure).
	SeverityError

	// SeverityCritical indicates errors that may cause the entire workflow
	// to hang permanently (e.g. advanceScope failure without fallback).
	SeverityCritical
)

// ErrorContext carries structured metadata about where an error occurred.
type ErrorContext struct {
	WorkflowRunID string
	TaskRunID     string
	Operation     string // e.g. "advanceScope", "dispatchLeafTask", "hook.onSuccess"
	Severity      Severity
}

// ErrorSink receives engine-internal errors for external observation.
//
// Implementations MUST be safe for concurrent use and SHOULD return quickly.
// Long-running work (HTTP calls, disk I/O) should be offloaded to a background
// goroutine or channel to avoid blocking the engine's scheduling loop.
type ErrorSink interface {
	OnError(ctx context.Context, err error, ec ErrorContext)
}
