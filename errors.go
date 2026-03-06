// Package aether implements the Graph Workflow Protocol engine.
package aether

import "errors"

var (
	// ErrInvalidState indicates an operation is invalid for the current state.
	ErrInvalidState = errors.New("invalid state")

	// ErrValidation indicates a workflow validation failure.
	ErrValidation = errors.New("validation error")
)
