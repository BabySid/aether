package model

import "encoding/json"

// Executor defines how a task is executed.
type Executor struct {
	Type   string          `json:"type"`   // script, function, await
	Config json.RawMessage `json:"config"` // scriptConfig | functionConfig | awaitConfig
}

// ScriptConfig is the configuration for script-type executors.
type ScriptConfig struct {
	Runtime string `json:"runtime"` // e.g. "python3", "bash"
	Source  string `json:"source"`  // inline script content
}

// FunctionConfig is the configuration for function-type executors.
type FunctionConfig struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// AwaitConfig is the configuration for await-type executors.
type AwaitConfig struct {
	Message string `json:"message,omitempty"`
}
