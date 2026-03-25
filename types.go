package aether

import (
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// WorkflowExecution is the return type of Engine.Get.
// It embeds store.WorkflowRun (all persisted fields) and adds two computed fields.
// Token (internal write-token) is intentionally not re-exported — callers should
// never need to supply a token when reading execution state.
type WorkflowExecution struct {
	*store.WorkflowRun                  // all WorkflowRun fields (RunID, Status, Message, Outputs, Metrics, …)
	Progress           string           // "completed/total" task count, empty when no tasks
	Tasks              []*store.TaskRun // all TaskRuns for this workflow run, in creation order
}

// Phase returns the current phase of the workflow run.
// Returns the zero value (empty string) when Status is nil.
func (e *WorkflowExecution) Phase() model.Phase {
	if e.WorkflowRun == nil || e.Status == nil {
		return ""
	}
	return *e.Status
}

// Msg returns the human-readable message of the workflow run.
// Returns empty string when Message is nil.
func (e *WorkflowExecution) Msg() string {
	if e.WorkflowRun == nil || e.Message == nil {
		return ""
	}
	return *e.Message
}
