// Package model defines the data structures mapped from the Graph Workflow Protocol (aether/v1).
package model

// Phase represents the execution phase of a workflow or task.
//
// State machine for all task types (leaf task, DAG container, Loop container):
//
//	Created → Ready → Running → (terminal)
//
// Created: task has been persisted to the store but the engine has not yet
//
//	decided to activate it (dependencies not met, concurrency limit, etc.).
//
// Ready: the engine has committed to execute this task.
//
//	For a leaf task this means Broker.Dispatch() has been called.
//	For a container (DAG/Loop) this means its first child leaf has been dispatched.
//
// Running: execution has actually begun.
//
//	For a leaf task this means OnTaskStarted has been called by the broker.
//	For a container this means its first child leaf has transitioned to Running.
type Phase string

const (
	// PhaseCreated is the initial state: task exists in the store but has not
	// yet been scheduled. Previously named "Pending".
	PhaseCreated Phase = "Created"
	// PhaseReady means the engine has committed to execute this task.
	// For leaf tasks: Broker.Dispatch() has been called.
	// For containers: the first child leaf task has been dispatched.
	PhaseReady   Phase = "Ready"
	PhaseRunning Phase = "Running"

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
