// Package ivars provides the built-in per-run variable sources for aether.
//
// These sources are internal engine implementation details that carry
// run-specific data (workflow arguments, resolved inputs, sibling task runs,
// loop iteration context). They implement the public vars.Source interface
// and are used by the binding layer to build the evaluation environment.
//
// User-facing sources (like SystemSource) remain in the public vars package.
package ivars

import (
	"encoding/json"
	"fmt"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// WorkflowArgsSource contributes workflow-level argument variables.
//
// Namespace: "workflow"
// Keys produced: "workflow.parameters.<name>"
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
