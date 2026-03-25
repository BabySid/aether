// Package executor defines the task executor plugin abstraction for aether.
package executor

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BabySid/aether/model"
)

// Plugin defines a task executor implementation.
type Plugin interface {
	// Type returns the executor type identifier (e.g. "script", "function", "await").
	Type() string

	// Execute runs the task, blocking until completion or ctx cancellation.
	// The returned *model.ExecOutputs carries the business data (Code, Message,
	// Parameters, Artifacts). Phase is NOT set by the executor; the broker/engine
	// layer is the single writer of Phase, derived from (error, ctx.Err(), Code):
	//   ctx.Err() != nil         → PhaseTimeout
	//   error != nil             → PhaseError
	//   Code == ExecCodeSuspended → PhaseRunning  (await pattern)
	//   Code == ExecCodeFailed    → PhaseFailed
	//   Code == ExecCodeSucceeded → PhaseSucceeded
	Execute(ctx context.Context, req *ExecuteRequest) (*model.ExecOutputs, error)
}

// ExecuteRequest is the input to a Plugin.
// It carries all information a plugin needs to execute the task and emit
// structured logs. Timeout is forwarded from TaskAssignment so the plugin
// can respect the deadline independently of the context (e.g. pass it to
// a subprocess or a remote API call).
type ExecuteRequest struct {
	// Identifiers — useful for structured logging and distributed tracing.
	TaskRunID     string
	WorkflowRunID string
	TaskName      string
	TemplateName  string

	// Execution configuration — opaque to the framework, parsed by the plugin.
	Config json.RawMessage

	// Runtime inputs and constraints.
	Inputs    *model.Inputs
	Resources *model.Resources
	Timeout   string // e.g. "30m"; empty means no deadline beyond ctx
}

// Registry manages registered executor plugins, routing by Type().
type Registry struct {
	plugins map[string]Plugin
}

// NewRegistry creates an empty executor registry.
func NewRegistry() *Registry {
	return &Registry{
		plugins: make(map[string]Plugin),
	}
}

// Register adds an executor plugin. Returns an error if the type is already registered.
func (r *Registry) Register(plugin Plugin) error {
	t := plugin.Type()
	if _, exists := r.plugins[t]; exists {
		return fmt.Errorf("executor type %q already registered", t)
	}
	r.plugins[t] = plugin
	return nil
}

// Get returns the executor plugin for the given type.
func (r *Registry) Get(executorType string) (Plugin, bool) {
	p, ok := r.plugins[executorType]
	return p, ok
}

// Types returns all registered executor type names.
func (r *Registry) Types() []string {
	types := make([]string, 0, len(r.plugins))
	for t := range r.plugins {
		types = append(types, t)
	}
	return types
}
