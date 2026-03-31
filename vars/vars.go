// Package vars defines the Source extension point for aether workflow variable namespaces.
//
// A Source contributes a flat key→value map to the workflow evaluation environment (EvalVars).
// Variables can then be referenced as {{namespace.key}} placeholders in workflow templates.
//
// # Built-in sources
//
// Four built-in per-run value objects cover the standard variable namespaces:
//
//   - WorkflowArgsSource   — "workflow.parameters.<name>"
//   - ResolvedInputsSource — "inputs.parameters.<name>"
//   - SiblingTaskRunsSource — "tasks.<name>.{phase,code,msg,outputs.parameters.*}"
//   - LoopIterationSource  — "loop_iter.{index,item,<field>}"
//
// One built-in global source exposes runtime metadata:
//
//   - SystemSource — "system.{os,arch}"
//
// # Custom sources
//
// Implement Source to add custom variable namespaces.
//
// ## Static value object (per-run data injected at construction)
//
//	type TenantSource struct {
//	    TenantID string
//	    Tier     string
//	}
//	func (s *TenantSource) Namespace() string { return "tenant" }
//	func (s *TenantSource) Vars() map[string]any {
//	    return map[string]any{
//	        "tenant.id":   s.TenantID,
//	        "tenant.tier": s.Tier,
//	    }
//	}
//
//	engine, _ := aether.New(
//	    aether.WithVarsSource(&TenantSource{TenantID: "acme", Tier: "enterprise"}),
//	)
//
// ## Dynamic / real-time computation
//
// Vars() is called each time the evaluation environment is built, so it can
// return values computed at call time — not at registration time.
// This is useful for timestamps, counters, or any data that must reflect the
// state of the world at the moment a task's inputs are resolved.
//
//	// ClockSource exposes the current wall-clock time under the "now" namespace.
//	// Because Vars() is evaluated lazily, every task that references {{now.*}}
//	// receives the time at which its inputs are resolved, not when the engine started.
//	type ClockSource struct{}
//
//	func (s *ClockSource) Namespace() string { return "now" }
//	func (s *ClockSource) Vars() map[string]any {
//	    t := time.Now().UTC()
//	    return map[string]any{
//	        "now.unix":      t.Unix(),                        // e.g. 1711857600
//	        "now.rfc3339":   t.Format(time.RFC3339),          // e.g. "2026-03-31T04:00:00Z"
//	        "now.date":      t.Format("2006-01-02"),          // e.g. "2026-03-31"
//	        "now.year":      t.Year(),                        // e.g. 2026
//	        "now.month":     int(t.Month()),                  // e.g. 3
//	        "now.day":       t.Day(),                         // e.g. 31
//	    }
//	}
//
//	engine, _ := aether.New(
//	    aether.WithVarsSource(&ClockSource{}),
//	)
//
// With this registered, any workflow template can reference:
//
//	{"name": "report-date", "value": "{{now.date}}"}
//	{"name": "run-at",      "value": "{{now.rfc3339}}"}
//
// # Lifecycle
//
// Sources have two lifecycle patterns:
//
//  1. Global singleton: stateless, registered once with aether.WithVarsSource.
//     Vars() may still compute dynamic values on each call (e.g. ClockSource,
//     SystemSource). Safe to share across all workflow runs.
//
//  2. Per-run value object: carries run-specific data injected at construction.
//     The engine creates a new instance per execution context. The built-in
//     WorkflowArgsSource, SiblingTaskRunsSource, LoopIterationSource, and
//     ResolvedInputsSource all follow this pattern and serve as reference
//     implementations.
package vars

import (
	"encoding/json"
	"fmt"
	"runtime"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// Source is the extension point for adding custom variable namespaces to
// the workflow evaluation environment.
//
// Each source owns one logical namespace. Vars() returns a flat map of
// fully-qualified key→value pairs (e.g. {"system.os": "linux"}).
// Keys should share the source's namespace prefix to avoid collisions.
//
// See package-level documentation for lifecycle patterns and examples.
type Source interface {
	// Namespace returns a short identifier for this source (e.g. "system", "tenant").
	// Used for documentation and debugging; does not restrict the key space of Vars().
	Namespace() string

	// Vars returns the flat key→value map contributed by this source.
	// Called once per evaluation context build. The map must not be mutated after return.
	Vars() map[string]any
}

// ---------------------------------------------------------------------------
// Built-in sources
// ---------------------------------------------------------------------------

// WorkflowArgsSource contributes workflow-level argument variables.
//
// Namespace: "workflow"
// Keys produced: "workflow.parameters.<name>"
//
// Per-run value object: construct a new instance for each workflow run.
//
// Example:
//
//	s := &vars.WorkflowArgsSource{Args: run.Arguments}
type WorkflowArgsSource struct {
	Args *model.Arguments
}

func (s *WorkflowArgsSource) Namespace() string { return "workflow" }

func (s *WorkflowArgsSource) Vars() map[string]any {
	if s.Args == nil {
		return nil
	}
	result := make(map[string]any, len(s.Args.Parameters))
	for _, param := range s.Args.Parameters {
		raw := param.Value
		if len(raw) == 0 || string(raw) == "null" {
			raw = param.Default
		}
		result["workflow.parameters."+param.Name] = unmarshalAny(raw)
	}
	return result
}

// ResolvedInputsSource contributes the current template's resolved input variables.
//
// Namespace: "inputs"
// Keys produced: "inputs.parameters.<name>"
//
// Per-run value object: construct a new instance with the resolved Inputs for
// the current template execution.
type ResolvedInputsSource struct {
	Inputs *model.Inputs
}

func (s *ResolvedInputsSource) Namespace() string { return "inputs" }

func (s *ResolvedInputsSource) Vars() map[string]any {
	if s.Inputs == nil {
		return nil
	}
	result := make(map[string]any, len(s.Inputs.Parameters))
	for _, param := range s.Inputs.Parameters {
		raw := param.Value
		if len(raw) == 0 || string(raw) == "null" {
			raw = param.Default
		}
		result["inputs.parameters."+param.Name] = unmarshalAny(raw)
	}
	return result
}

// SiblingTaskRunsSource contributes sibling task run state and output variables.
//
// Namespace: "tasks"
// Keys produced:
//
//	"tasks.<name>.phase"
//	"tasks.<name>.code"
//	"tasks.<name>.msg"
//	"tasks.<name>.outputs.parameters.<param>"
//
// Per-run value object: construct a new instance with the sibling TaskRuns
// available at the current execution point.
type SiblingTaskRunsSource struct {
	Runs []*store.TaskRun
}

func (s *SiblingTaskRunsSource) Namespace() string { return "tasks" }

func (s *SiblingTaskRunsSource) Vars() map[string]any {
	if len(s.Runs) == 0 {
		return nil
	}
	result := make(map[string]any)
	for _, tr := range s.Runs {
		prefix := "tasks." + tr.TaskName
		if tr.Status != nil {
			result[prefix+".phase"] = string(*tr.Status)
		} else {
			result[prefix+".phase"] = ""
		}
		if tr.Outputs != nil {
			result[prefix+".code"] = tr.Outputs.Code
			result[prefix+".msg"] = tr.Outputs.Message
			for _, param := range tr.Outputs.Parameters {
				key := fmt.Sprintf("%s.outputs.parameters.%s", prefix, param.Name)
				result[key] = unmarshalAny(param.Value)
			}
		}
	}
	return result
}

// LoopIterationSource contributes loop iteration index and item variables.
//
// Namespace: "loop_iter"
// Keys produced:
//
//	"loop_iter.index"       — always set to Index
//	"loop_iter.item"        — set when Item is a scalar (non-map)
//	"loop_iter.<field>"     — set for each field when Item is a map[string]any
//
// Per-run value object: construct a new instance for each loop iteration.
type LoopIterationSource struct {
	Index int
	Item  any
}

func (s *LoopIterationSource) Namespace() string { return "loop_iter" }

func (s *LoopIterationSource) Vars() map[string]any {
	result := make(map[string]any)
	result["loop_iter.index"] = s.Index
	if m, ok := s.Item.(map[string]any); ok {
		for k, v := range m {
			result["loop_iter."+k] = v
		}
	} else if s.Item != nil {
		result["loop_iter.item"] = s.Item
	}
	return result
}

// SystemSource is a built-in global Source that exposes runtime environment
// information under the "system" namespace.
//
// Namespace: "system"
// Keys produced:
//
//	"system.os"   — operating system name (e.g. "linux", "darwin", "windows")
//	"system.arch" — CPU architecture (e.g. "amd64", "arm64")
//
// Stateless singleton: safe to share across workflow runs. Register at engine level:
//
//	engine, _ := aether.New(
//	    aether.WithVarsSource(&vars.SystemSource{}),
//	)
type SystemSource struct{}

func (s *SystemSource) Namespace() string { return "system" }

func (s *SystemSource) Vars() map[string]any {
	return map[string]any{
		"system.os":   runtime.GOOS,
		"system.arch": runtime.GOARCH,
	}
}

// unmarshalAny tries to decode raw JSON into a Go value.
// On failure it returns the raw bytes as a string so the env always has something useful.
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
