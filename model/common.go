package model

// Arguments represents the arguments passed to a task or workflow.
type Arguments struct {
	Parameters []Parameter `json:"parameters,omitempty"`
	Artifacts  []Artifact  `json:"artifacts,omitempty"`
}

// Inputs defines the inputs for a template.
type Inputs struct {
	Parameters []Parameter `json:"parameters,omitempty"`
	Artifacts  []Artifact  `json:"artifacts,omitempty"`
}

// Outputs defines the outputs of a workflow or task execution.
type Outputs struct {
	Phase      Phase       `json:"phase,omitempty"`
	Code       int         `json:"code,omitempty"`
	Msg        string      `json:"msg,omitempty"`
	Metrics    *Metrics    `json:"metrics,omitempty"`
	Progress   string      `json:"progress,omitempty"`
	Parameters []Parameter `json:"parameters,omitempty"`
	Artifacts  []Artifact  `json:"artifacts,omitempty"`
}

// Metrics holds execution timing and retry information.
type Metrics struct {
	StartedAt  string `json:"startedAt,omitempty"`
	FinishedAt string `json:"finishedAt,omitempty"`
	Duration   string `json:"duration,omitempty"`
	Retries    int    `json:"retries,omitempty"`
}

// Resources defines compute resource requirements.
type Resources struct {
	CPU    any    `json:"cpu,omitempty"`    // string or number
	Memory string `json:"memory,omitempty"` // e.g. "512Mi"
	GPU    string `json:"gpu,omitempty"`
}

// Retry defines the retry policy for a leaf task (executor type only).
// DAG and Loop containers do not support retry directly.
type Retry struct {
	// Limit is the maximum number of retries. 0 means no retry.
	Limit int `json:"limit,omitempty"`
	// Expression is an optional boolean expression that controls when to retry.
	// When set, a retry only occurs if this expression evaluates to true.
	// The expression can reference the task's own execution result via:
	//   tasks.<name>.phase                          — e.g. "Failed", "Error", "Timeout"
	//   tasks.<name>.code                           — exit code
	//   tasks.<name>.msg                            — error message
	//   tasks.<name>.outputs.parameters.<param>     — output parameter value
	// When omitted, any non-Succeeded phase triggers a retry (up to Limit).
	Expression string `json:"expression,omitempty"`
}

// ContinueOn defines conditions under which execution continues despite failures.
type ContinueOn struct {
	Failed  bool `json:"failed,omitempty"`
	Error   bool `json:"error,omitempty"`
	Timeout bool `json:"timeout,omitempty"`
}

// PhaseConditions defines custom expressions to determine task phase.
type PhaseConditions struct {
	Succeeded string `json:"succeeded,omitempty"`
	Failed    string `json:"failed,omitempty"`
	Error     string `json:"error,omitempty"`
}
