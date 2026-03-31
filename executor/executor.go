// Package executor defines the task executor plugin abstraction for aether.
package executor

import (
	"context"
	"fmt"

	"github.com/BabySid/aether/model"
)

// Plugin is the executor plugin interface. All executor implementations must satisfy it.
type Plugin interface {
	// Type returns the unique executor type identifier (e.g. "http", "shell").
	Type() string

	// Schema declares the executor's contract. Use SchemaOf[Config, Output]() to
	// derive it from struct types rather than writing field names by hand.
	Schema() model.ExecutorSchema

	// Execute runs the task. Use OutputFrom for type-safe output construction.
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

	// Runtime inputs and constraints.
	Inputs     *model.Inputs
	Resources  *model.Resources
	Timeout    string // e.g. "30m"; empty means no deadline beyond ctx
	RetryCount int    // number of retries already consumed (0 = first attempt)
}

// Registry manages registered executor plugins, routing by Type().
// It also maintains a SchemaRegistry keyed by executor type.
type Registry struct {
	plugins map[string]Plugin
	schemas map[string]model.ExecutorSchema
}

// NewRegistry creates an empty executor registry.
func NewRegistry() *Registry {
	return &Registry{
		plugins: make(map[string]Plugin),
		schemas: make(map[string]model.ExecutorSchema),
	}
}

// Register adds an executor plugin. Returns an error if the type is already registered.
// It also caches the plugin's Schema() result in the SchemaRegistry.
func (r *Registry) Register(plugin Plugin) error {
	t := plugin.Type()
	if _, exists := r.plugins[t]; exists {
		return fmt.Errorf("executor type %q already registered", t)
	}
	r.plugins[t] = plugin
	r.schemas[t] = plugin.Schema()
	return nil
}

// Get returns the executor plugin for the given type.
func (r *Registry) Get(executorType string) (Plugin, bool) {
	p, ok := r.plugins[executorType]
	return p, ok
}

// GetSchema returns the cached ExecutorSchema for the given executor type.
func (r *Registry) GetSchema(executorType string) (model.ExecutorSchema, bool) {
	s, ok := r.schemas[executorType]
	return s, ok
}

// Types returns all registered executor type names.
func (r *Registry) Types() []string {
	types := make([]string, 0, len(r.plugins))
	for t := range r.plugins {
		types = append(types, t)
	}
	return types
}

// Schemas returns all cached ExecutorSchemas.
func (r *Registry) Schemas() []model.ExecutorSchema {
	schemas := make([]model.ExecutorSchema, 0, len(r.schemas))
	for _, s := range r.schemas {
		schemas = append(schemas, s)
	}
	return schemas
}
