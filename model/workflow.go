package model

// Workflow is the top-level resource for a graph workflow.
type Workflow struct {
	APIVersion string       `json:"apiVersion"` // aether/v1
	Kind       string       `json:"kind"`       // Workflow | CronWorkflow | WorkflowTemplate
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

// CronWorkflowSpec defines the specification for a CronWorkflow.
type CronWorkflowSpec struct {
	Schedule                   string       `json:"schedule"`
	Timezone                   string       `json:"timezone,omitempty"`
	ConcurrencyPolicy          string       `json:"concurrencyPolicy,omitempty"` // Allow, Forbid, Replace
	StartingDeadlineSeconds    int          `json:"startingDeadlineSeconds,omitempty"`
	SuccessfulJobsHistoryLimit int          `json:"successfulJobsHistoryLimit,omitempty"`
	FailedJobsHistoryLimit     int          `json:"failedJobsHistoryLimit,omitempty"`
	Suspend                    bool         `json:"suspend,omitempty"`
	WorkflowSpec               WorkflowSpec `json:"workflowSpec"`
	WorkflowMetadata           *Metadata    `json:"workflowMetadata,omitempty"`
}

// WorkflowTemplate is the top-level resource for a reusable workflow template.
type WorkflowTemplate struct {
	APIVersion string       `json:"apiVersion"`
	Kind       string       `json:"kind"` // WorkflowTemplate
	Metadata   Metadata     `json:"metadata"`
	Spec       WorkflowSpec `json:"spec"`
}
