package model

// Executor defines how a task is executed.
type Executor struct {
	Type string `json:"type"` // executor type identifier, e.g. "echo", "http", "shell"
}
