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
	Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResult, error)
}

// ExecuteRequest is the input to a Plugin.
type ExecuteRequest struct {
	TaskRunID uint64
	Config    json.RawMessage
	Inputs    *model.Inputs
	Resources *model.Resources
}

// ExecuteResult is the output from a Plugin.
type ExecuteResult struct {
	Phase   model.Phase
	Code    int
	Msg     string
	Outputs *model.Outputs
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
