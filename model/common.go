package model

// ExecCode is the standard return code for ExecOutputs.Code.
// Executor implementations MUST use these constants; the engine layer
// derives Phase from Code (single Phase writer).
//
// Code-to-Phase mapping (performed by the engine in OnTaskCompleted):
//
//	ExecCodeSucceeded → PhaseSucceeded
//	ExecCodeSuspended → PhaseSuspended (await pattern)
//	ExecCodeFailed    → PhaseFailed    (business failure)
//	ExecCodeError     → PhaseError     (system/framework-level error)
//	ExecCodeTimeout   → PhaseTimeout   (task exceeded its deadline)
//
// PhaseSkipped and PhaseCancelled are set exclusively by the engine itself
// and never appear in ExecOutputs.
const (
	// ExecCodeSucceeded indicates the task completed successfully.
	ExecCodeSucceeded = 0
	// ExecCodeSuspended indicates the task is suspended, waiting for an external
	// Resume() call (await pattern). The engine converts this to PhaseSuspended.
	ExecCodeSuspended = 1
	// ExecCodeFailed indicates the task completed with a business failure.
	// The engine converts this to PhaseFailed.
	ExecCodeFailed = 2
	// ExecCodeError indicates a system-level error (e.g. executor panic, OOM,
	// container killed, network failure). The engine converts this to PhaseError.
	ExecCodeError = 3
	// ExecCodeTimeout indicates the task exceeded its deadline.
	// In local mode the broker sets this when the task context times out;
	// in distributed mode the engine's watchdog may set it directly without
	// receiving a TaskResult.
	ExecCodeTimeout = 4
)

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

// ExecOutputs is the business data produced by an executor.
// It is the single authoritative structure for executor return values,
// shared across executor.Plugin, broker.TaskResult, and model.Outputs.
// Phase and Metrics are NOT included here; they are framework concerns filled
// by the broker/engine layer, not by the executor itself.
type ExecOutputs struct {
	Code       int         `json:"code,omitempty"`
	Message    string      `json:"message,omitempty"`
	Parameters []Parameter `json:"parameters,omitempty"`
	Artifacts  []Artifact  `json:"artifacts,omitempty"`
}

// Outputs defines the complete output of a workflow or task execution.
// Phase and Metrics are filled by the framework (broker/engine).
// ExecOutputs (Code, Message, Parameters, Artifacts) originate from the executor.
// The embedded fields are JSON-flat (no wrapper key).
type Outputs struct {
	Phase   Phase    `json:"phase,omitempty"`
	Metrics *Metrics `json:"metrics,omitempty"`
	ExecOutputs
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

// ExecutorSchema is the self-description of an executor plugin.
// Best practice: use executor.SchemaOf[ConfigStruct, OutputStruct]() to derive this
// from Go struct types; avoid hand-writing field names as strings.
type ExecutorSchema struct {
	Type        string `json:"type"`                  // matches Plugin.Type()
	Version     string `json:"version"`               // schema version, e.g. "1.0"
	Description string `json:"description,omitempty"`

	// Inputs declares the accepted input parameters (derived from Config struct).
	// nil = accepts any inputs (permissive mode).
	Inputs *Inputs `json:"inputs,omitempty"`

	// Outputs declares the fixed set of output parameters produced by this executor.
	//   non-nil → static outputs: engine validates task.outputs fields at submit time
	//             and auto-fills task.outputs when omitted.
	//   nil → dynamic outputs: fields are determined at runtime.
	Outputs *ExecOutputs `json:"outputs,omitempty"`
}
