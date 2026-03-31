package aether

import (
	"time"

	"github.com/BabySid/aether/model"
)

// WorkflowExecution is the read-only return type of Engine.Get.
// It contains only the fields that external callers need to observe workflow
// state. Internal store fields (Token, Deadline, raw Workflow JSON, UpdatedAt)
// are deliberately excluded to keep the public API stable and decoupled from
// storage internals.
type WorkflowExecution struct {
	RunID     string
	Status    model.Phase    // zero value ("") when not yet set
	Message   string         // human-readable status message
	Outputs   *model.Outputs // workflow-level outputs, nil until finalized
	Metrics   *model.Metrics // workflow-level timing metrics
	CreatedAt time.Time
	Progress  string           // "completed/total", empty when no tasks
	Tasks     []TaskExecution  // all task runs, in creation order
}

// TaskExecution is the read-only view of a single task run within a workflow.
type TaskExecution struct {
	// Immutable
	RunID         string
	WorkflowRunID string
	ParentRunID   string // "" = top-level scope
	Depth         int
	Scope         string
	TaskName      string
	TemplateName  string
	TemplateType  string
	CreatedAt     time.Time

	// Mutable (already dereferenced from store pointer types)
	Status     model.Phase
	Message    string
	Inputs     *model.Inputs
	Outputs    *model.Outputs
	Metrics    *model.Metrics
	RetryCount int
}
