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
	CPU    interface{} `json:"cpu,omitempty"`    // string or number
	Memory string      `json:"memory,omitempty"` // e.g. "512Mi"
	GPU    string      `json:"gpu,omitempty"`
}

// Retry defines the retry policy.
type Retry struct {
	Limit      int      `json:"limit,omitempty"`
	Backoff    *Backoff `json:"backoff,omitempty"`
	Expression string   `json:"expression,omitempty"`
}

// Backoff defines the backoff strategy for retries.
type Backoff struct {
	Duration    string  `json:"duration,omitempty"`    // e.g. "5s"
	Factor      float64 `json:"factor,omitempty"`      // multiplier, e.g. 2.0
	MaxDuration string  `json:"maxDuration,omitempty"` // e.g. "5m"
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
