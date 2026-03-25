// Package binding provides unified parameter binding for aether workflows.
//
// It covers three concerns:
//
//  1. EvalEnv construction (env.go): builds a flat key→value snapshot from
//     workflow arguments, resolved inputs, sibling task runs, and loop iteration data.
//
//  2. Template interpolation (interpolate.go): expands {{key}} placeholders in
//     strings using an EvalEnv, preserving original Go types when possible.
//
//  3. Inputs binding (bind.go): merges template-declared inputs with call-site
//     arguments and resolves all valueFrom references through the EvalEnv.
//
//  4. Outputs collection (collect.go): collects DAG or loop container outputs
//     from child TaskRun results after the container has terminated.
//
// Key design invariant: Binder and Collector never access []*store.TaskRun directly.
// All task-run data is pre-flattened into EvalEnv by EnvBuilder. This keeps
// resolution logic independent of the storage layer.
package binding

import (
	"encoding/json"
	"fmt"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// EvalEnv is a flat map of all variables available for expression evaluation
// and template variable interpolation at a given execution point.
//
// Key space:
//
//	"workflow.parameters.<name>"                 — workflow-level arguments
//	"inputs.parameters.<name>"                   — current template's bound inputs
//	"tasks.<name>.phase"                         — sibling task phase string
//	"tasks.<name>.code"                          — sibling task exit code
//	"tasks.<name>.msg"                           — sibling task message
//	"tasks.<name>.outputs.parameters.<param>"    — sibling task output parameter
//	"loop_iter.index"                            — current loop iteration index
//	"loop_iter.item"                             — current iteration scalar item
//	"loop_iter.<field>"                          — current iteration object field
type EvalEnv map[string]any

// EnvBuilder collects data from multiple sources and produces an EvalEnv snapshot.
// Use the With* methods to inject only the sources relevant to the current execution point,
// then call Build() to obtain the immutable EvalEnv.
type EnvBuilder struct {
	env EvalEnv
}

// NewEnvBuilder returns an empty EnvBuilder.
func NewEnvBuilder() *EnvBuilder {
	return &EnvBuilder{env: make(EvalEnv)}
}

// WithWorkflowArgs injects workflow-level arguments into the env.
// Produces keys: "workflow.parameters.<name>"
// The value is the parameter's Value; if empty, falls back to Default.
func (b *EnvBuilder) WithWorkflowArgs(args *model.Arguments) *EnvBuilder {
	if args == nil {
		return b
	}
	for _, p := range args.Parameters {
		raw := p.Value
		if len(raw) == 0 || string(raw) == "null" {
			raw = p.Default
		}
		b.env["workflow.parameters."+p.Name] = unmarshalAny(raw, p.Name)
	}
	return b
}

// WithResolvedInputs injects the current template's already-bound inputs into the env.
// Produces keys: "inputs.parameters.<name>"
// This is used so that loop itemsFrom expressions can reference resolved loop inputs.
func (b *EnvBuilder) WithResolvedInputs(inputs *model.Inputs) *EnvBuilder {
	if inputs == nil {
		return b
	}
	for _, p := range inputs.Parameters {
		raw := p.Value
		if len(raw) == 0 || string(raw) == "null" {
			raw = p.Default
		}
		b.env["inputs.parameters."+p.Name] = unmarshalAny(raw, p.Name)
	}
	return b
}

// WithSiblingTaskRuns injects same-scope sibling task run state and outputs.
// Produces keys:
//
//	"tasks.<name>.phase"
//	"tasks.<name>.code"
//	"tasks.<name>.msg"
//	"tasks.<name>.outputs.parameters.<param>"
func (b *EnvBuilder) WithSiblingTaskRuns(runs []*store.TaskRun) *EnvBuilder {
	for _, tr := range runs {
		prefix := "tasks." + tr.TaskName
		if tr.Status != nil {
			b.env[prefix+".phase"] = string(*tr.Status)
		} else {
			b.env[prefix+".phase"] = ""
		}
		if tr.Outputs != nil {
			b.env[prefix+".code"] = tr.Outputs.Code
			b.env[prefix+".msg"] = tr.Outputs.Message
			for _, p := range tr.Outputs.Parameters {
				key := fmt.Sprintf("%s.outputs.parameters.%s", prefix, p.Name)
				b.env[key] = unmarshalAny(p.Value, p.Name)
			}
		}
	}
	return b
}

// WithLoopIteration injects loop iteration parameters.
// item may be:
//   - a scalar (string, int, …) → stored under "loop_iter.item"
//   - a map[string]any         → each key stored under "loop_iter.<field>"
//
// "loop_iter.index" is always set to index.
func (b *EnvBuilder) WithLoopIteration(index int, item any) *EnvBuilder {
	b.env["loop_iter.index"] = index
	if m, ok := item.(map[string]any); ok {
		for k, v := range m {
			b.env["loop_iter."+k] = v
		}
	} else if item != nil {
		b.env["loop_iter.item"] = item
	}
	return b
}

// Build returns the constructed EvalEnv snapshot.
// The returned map should be treated as read-only.
func (b *EnvBuilder) Build() EvalEnv {
	result := make(EvalEnv, len(b.env))
	for k, v := range b.env {
		result[k] = v
	}
	return result
}

// unmarshalAny tries to decode raw JSON into a Go value.
// On failure it returns the raw bytes as a string so the env always has something useful.
func unmarshalAny(raw json.RawMessage, _ string) any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	return v
}
