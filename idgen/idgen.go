// Package idgen defines the ID generation abstraction for aether.
package idgen

// Context provides metadata about the entity being ID-generated, allowing
// implementations to embed structured information (e.g. kind prefix, workflow
// run correlation) into the generated ID.
type Context struct {
	WorkflowRunID string // non-empty when generating a TaskRun ID
	WorkflowKind  string // "Workflow" or "CronWorkflow"
	TaskName      string // non-empty when generating a TaskRun ID
	TemplateName  string // non-empty when generating a TaskRun ID
}

// Generator generates globally unique string IDs for workflow runs and task runs.
// Using string IDs decouples the framework from any specific numeric scheme and
// allows implementations to use UUIDs, Snowflake IDs, or monotonic counters.
type Generator interface {
	// Generate returns a new unique ID as a string.
	Generate(ctx Context) string
}
