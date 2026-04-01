package model

// Workflow is the top-level resource for a graph workflow.
type Workflow struct {
	APIVersion string       `json:"apiVersion"` // aether/v1
	Kind       string       `json:"kind"`       // Workflow | CronWorkflow
	Metadata   Metadata     `json:"metadata"`
	Spec       WorkflowSpec `json:"spec,omitempty"`
}

// WorkflowSpec defines the specification for a Workflow.
type WorkflowSpec struct {
	Entrypoint     string     `json:"entrypoint,omitempty"`
	Arguments      *Arguments `json:"arguments,omitempty"`
	Timeout        string     `json:"timeout,omitempty"`
	Priority       int        `json:"priority,omitempty"`
	MaxNestedDepth int        `json:"maxNestedDepth,omitempty"`
	Templates      []Template `json:"templates,omitempty"`
	Hooks          *Hooks     `json:"hooks,omitempty"`
}

// CronWorkflow is the top-level resource for a scheduled workflow.
type CronWorkflow struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"` // CronWorkflow
	Metadata   Metadata         `json:"metadata"`
	Spec       CronWorkflowSpec `json:"spec"`
}

// Concurrency policy constants for CronWorkflow.
const (
	ConcurrencyAllow   = "Allow"
	ConcurrencyForbid  = "Forbid"
	ConcurrencyReplace = "Replace"
)

// CronWorkflowSpec defines the specification for a CronWorkflow.
type CronWorkflowSpec struct {
	Schedule                   string       `json:"schedule"`
	Timezone                   string       `json:"timezone,omitempty"`
	StartAt                    string       `json:"startAt,omitempty"`                    // optional RFC3339
	EndAt                      string       `json:"endAt,omitempty"`                      // optional RFC3339
	ConcurrencyPolicy          string       `json:"concurrencyPolicy,omitempty"`          // Allow, Forbid, Replace
	StartingDeadlineSeconds    *int         `json:"startingDeadlineSeconds,omitempty"`    // nil = no deadline
	SuccessfulJobsHistoryLimit int          `json:"successfulJobsHistoryLimit,omitempty"`
	FailedJobsHistoryLimit     int          `json:"failedJobsHistoryLimit,omitempty"`
	Suspend                    bool         `json:"suspend,omitempty"`
	WorkflowSpec               WorkflowSpec `json:"workflowSpec"`
}

