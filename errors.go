// Package aether implements the Graph Workflow Protocol engine.
package aether

import "errors"

var (
	// ErrInvalidState indicates an operation is invalid for the current state.
	ErrInvalidState = errors.New("invalid state")

	// ErrValidation indicates a workflow validation failure.
	ErrValidation = errors.New("validation error")

	// ErrNotSupported indicates that a feature requires an optional dependency
	// that was not injected (e.g. CronScheduler for CronWorkflow operations).
	ErrNotSupported = errors.New("not supported")
)
