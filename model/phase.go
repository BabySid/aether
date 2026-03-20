// Package model defines the data structures mapped from the Graph Workflow Protocol (aether/v1).
package model

// Phase represents the execution phase of a workflow or task.
type Phase string

const (
	PhasePending   Phase = "Pending"
	PhaseRunning   Phase = "Running"
	PhaseSucceeded Phase = "Succeeded"
	PhaseFailed    Phase = "Failed"
	PhaseError     Phase = "Error"
	PhaseTimeout   Phase = "Timeout"
	PhaseSkipped   Phase = "Skipped"
	PhaseCancelled Phase = "Cancelled"
)

// IsTerminal returns true if the phase is a terminal state.
func (p Phase) IsTerminal() bool {
	switch p {
	case PhaseSucceeded, PhaseFailed, PhaseError, PhaseTimeout, PhaseSkipped, PhaseCancelled:
		return true
	default:
		return false
	}
}
