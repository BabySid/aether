// Package binding provides unified parameter binding for aether workflows.
//
// It covers three concerns:
//
//  1. EvalVars construction (env.go): builds a flat key→value snapshot from
//     workflow arguments, resolved inputs, sibling task runs, and loop iteration data.
//
//  2. Template interpolation (interpolate.go): expands {{key}} placeholders in
//     strings using an EvalVars, preserving original Go types when possible.
//
//  3. Inputs binding (bind.go): merges template-declared inputs with call-site
//     arguments and resolves all valueFrom references through the EvalVars.
//
//  4. Outputs collection (collect.go): collects DAG or loop container outputs
//     from child TaskRun results after the container has terminated.
//
// Key design invariant: Binder and Collector never access []*store.TaskRun directly.
// All task-run data is pre-flattened into EvalVars by VarBuilder. This keeps
// resolution logic independent of the storage layer.
//
// # Extensibility via vars.Source
//
// The variable namespace is extensible through the vars.Source interface.
// Each source owns one namespace (e.g. "system", "workflow", "tasks") and
// returns a flat map of key→value pairs that are merged into the EvalVars.
//
// Built-in sources (WorkflowArgsSource, ResolvedInputsSource,
// SiblingTaskRunsSource, LoopIterationSource) are value objects in the
// vars package that carry their data at construction time. Users can follow
// the same pattern to add custom namespaces (e.g. "system.*", "tenant.*").
//
// Registration:
//   - Engine-level (global): use aether.WithVarsSource(s).
//   - Per-run: create a value object and pass it via VarBuilder.WithSource(&MySource{...}).
//
// Merge order: sources registered first have lower priority; later sources
// overwrite earlier ones for the same key.
package binding

import (
	"encoding/json"

	ivars "github.com/BabySid/aether/internal/vars"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
	"github.com/BabySid/aether/vars"
)

// EvalVars is a flat map of all variables available for expression evaluation
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
type EvalVars map[string]any

// VarBuilder collects data from multiple Sources and produces an EvalVars snapshot.
//
// Sources are indexed by Namespace(): each Source is placed into the bucket
// identified by its namespace string, enabling namespace-scoped lookup and
// selective (lazy) evaluation via Build(namespaces...).
//
// Use WithSource to register vars.Source implementations, or use the
// convenience With* methods for the built-in source types.
// Call Build() to obtain the EvalVars.
//
// Priority: sources registered later within the same namespace overwrite
// earlier ones for duplicate keys. Namespaces written later overwrite
// earlier namespaces for any colliding key.
type VarBuilder struct {
	// byNamespace maps namespace → ordered list of Sources for that namespace.
	// Multiple Sources may share the same namespace; they are applied in registration order.
	byNamespace map[string][]vars.Source
	// nsOrder records the first-registration order of each namespace,
	// so Build() produces a deterministic result regardless of map iteration order.
	nsOrder []string
}

// NewVarBuilder returns an empty VarBuilder.
func NewVarBuilder() *VarBuilder {
	return &VarBuilder{
		byNamespace: make(map[string][]vars.Source),
	}
}

// WithSource registers a vars.Source under its Namespace().
// Sources within the same namespace are applied in registration order;
// later registrations overwrite earlier ones for duplicate keys.
func (b *VarBuilder) WithSource(s vars.Source) *VarBuilder {
	if s == nil {
		return b
	}
	ns := s.Namespace()
	if _, exists := b.byNamespace[ns]; !exists {
		b.nsOrder = append(b.nsOrder, ns)
	}
	b.byNamespace[ns] = append(b.byNamespace[ns], s)
	return b
}

// WithWorkflowArgs injects workflow-level arguments into the env.
// Produces keys: "workflow.parameters.<name>"
// The value is the parameter's Value; if empty, falls back to Default.
//
// Convenience wrapper around WithSource(&vars.WorkflowArgsSource{Args: args}).
func (b *VarBuilder) WithWorkflowArgs(args *model.Arguments) *VarBuilder {
	return b.WithSource(&ivars.WorkflowArgsSource{Args: args})
}

// WithResolvedInputs injects the current template's already-bound inputs into the env.
// Produces keys: "inputs.parameters.<name>"
// This is used so that loop itemsFrom expressions can reference resolved loop inputs.
//
// Convenience wrapper around WithSource(&vars.ResolvedInputsSource{Inputs: inputs}).
func (b *VarBuilder) WithResolvedInputs(inputs *model.Inputs) *VarBuilder {
	return b.WithSource(&ivars.ResolvedInputsSource{Inputs: inputs})
}

// WithSiblingTaskRuns injects same-scope sibling task run state and outputs.
// Produces keys:
//
//	"tasks.<name>.phase"
//	"tasks.<name>.code"
//	"tasks.<name>.msg"
//	"tasks.<name>.outputs.parameters.<param>"
//
// Convenience wrapper around WithSource(&vars.SiblingTaskRunsSource{Runs: runs}).
func (b *VarBuilder) WithSiblingTaskRuns(runs []*store.TaskRun) *VarBuilder {
	return b.WithSource(&ivars.SiblingTaskRunsSource{Runs: runs})
}

// WithLoopIteration injects loop iteration parameters.
// item may be:
//   - a scalar (string, int, …) → stored under "loop_iter.item"
//   - a map[string]any         → each key stored under "loop_iter.<field>"
//
// "loop_iter.index" is always set to index.
//
// Convenience wrapper around WithSource(&vars.LoopIterationSource{Index: index, Item: item}).
func (b *VarBuilder) WithLoopIteration(index int, item any) *VarBuilder {
	return b.WithSource(&ivars.LoopIterationSource{Index: index, Item: item})
}

// Build constructs an EvalVars snapshot from registered Sources.
//
//   - Build()                  — triggers all registered Sources (full build).
//   - Build("system", "tasks") — triggers only the listed namespaces; Sources in
//     other namespaces are not called (lazy / on-demand build).
//
// Within a namespace, Sources are applied in registration order; later
// registrations overwrite earlier ones for duplicate keys.
// The returned map should be treated as read-only.
func (b *VarBuilder) Build(namespaces ...string) EvalVars {
	env := make(EvalVars)
	toProcess := b.nsOrder
	if len(namespaces) > 0 {
		toProcess = namespaces
	}
	for _, ns := range toProcess {
		for _, s := range b.byNamespace[ns] {
			for k, v := range s.Vars() {
				env[k] = v
			}
		}
	}
	return env
}

// unmarshalAny tries to decode raw JSON into a Go value.
// On failure it returns the raw bytes as a string so callers always have something useful.
func unmarshalAny(raw json.RawMessage) any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	return v
}
