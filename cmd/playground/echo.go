// echo.go — built-in "echo" executor for playground.
//
// # Design
//
// The echo executor supports an "outputs" config that declares the expected
// output parameters, each with an explicit type and optional value.
// Supported types: int, bool, string, array, object.
//
// On execution:
//  1. All input parameters are printed to the log.
//  2. Outputs are built as a *mix* of inputs + declared outputs:
//     - Inputs are echoed back first.
//     - Outputs defined in config are merged on top (override same-named inputs).
//     - If an output entry carries no value, a zero-value for the declared type is used.
//
// Workflow JSON example:
//
//	"executor": {
//	  "type": "echo",
//	  "config": {
//	    "outputs": [
//	      {"name": "status",  "type": "string", "value": "ok"},
//	      {"name": "count",   "type": "int",    "value": 42},
//	      {"name": "success", "type": "bool",   "value": true},
//	      {"name": "tags",    "type": "array",  "value": ["a","b","c"]},
//	      {"name": "meta",    "type": "object", "value": {"key": "val"}}
//	    ]
//	  }
//	}
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/model"
)

// echoCapabilityType enumerates the supported output value types.
type echoCapabilityType string

const (
	capTypeInt    echoCapabilityType = "int"
	capTypeBool   echoCapabilityType = "bool"
	capTypeString echoCapabilityType = "string"
	capTypeArray  echoCapabilityType = "array"
	capTypeObject echoCapabilityType = "object"
)

// echoOutput describes a single expected output parameter in the config.
type echoOutput struct {
	Name  string             `json:"name"`
	Type  echoCapabilityType `json:"type"`
	Value json.RawMessage    `json:"value,omitempty"` // optional; falls back to zero-value
}

// echoConfig is the parsed form of the executor config block.
type echoConfig struct {
	Outputs []echoOutput `json:"outputs"`
}

// zeroValueFor returns the JSON zero-value for each supported type.
func zeroValueFor(t echoCapabilityType) json.RawMessage {
	switch t {
	case capTypeInt:
		return json.RawMessage(`0`)
	case capTypeBool:
		return json.RawMessage(`false`)
	case capTypeString:
		return json.RawMessage(`""`)
	case capTypeArray:
		return json.RawMessage(`[]`)
	case capTypeObject:
		return json.RawMessage(`{}`)
	default:
		return json.RawMessage(`null`)
	}
}

// validateValue ensures the raw JSON value matches the declared output type.
// It returns an error string (empty = OK) suitable for log output.
func validateValue(out echoOutput) string {
	if len(out.Value) == 0 {
		return ""
	}
	var v interface{}
	if err := json.Unmarshal(out.Value, &v); err != nil {
		return fmt.Sprintf("output %q: invalid JSON: %v", out.Name, err)
	}
	ok := false
	switch out.Type {
	case capTypeInt:
		_, ok = v.(float64)
	case capTypeBool:
		_, ok = v.(bool)
	case capTypeString:
		_, ok = v.(string)
	case capTypeArray:
		_, ok = v.([]interface{})
	case capTypeObject:
		_, ok = v.(map[string]interface{})
	default:
		ok = true // unknown type: pass-through
	}
	if !ok {
		return fmt.Sprintf("output %q: value type mismatch (declared=%s, got=%T)", out.Name, out.Type, v)
	}
	return ""
}

// EchoExecutor prints inputs and returns a mixed output of inputs + configured capabilities.
type EchoExecutor struct{}

func newEcho() *EchoExecutor { return &EchoExecutor{} }

func (e *EchoExecutor) Type() string { return "echo" }

func (e *EchoExecutor) Execute(_ context.Context, req *executor.ExecuteRequest) (*model.ExecOutputs, error) {
	// ── Step 1: echo all inputs, build base output map ──────────────────────
	paramIdx := make(map[string]int) // name → index in params slice
	var params []model.Parameter

	if req.Inputs != nil {
		for _, p := range req.Inputs.Parameters {
			paramIdx[p.Name] = len(params)
			params = append(params, model.Parameter{
				Name:  p.Name,
				Type:  p.Type,
				Value: p.Value,
			})
		}
	}

	// ── Step 2: log inputs ───────────────────────────────────────────────────
	var inputLines []string
	for _, p := range params {
		inputLines = append(inputLines, fmt.Sprintf("%s(%s)=%s", p.Name, p.Type, string(p.Value)))
	}
	if len(inputLines) == 0 {
		log.Printf("[echo] taskRunID=%s  inputs=(none)", req.TaskRunID)
	} else {
		log.Printf("[echo] taskRunID=%s  inputs=[%s]", req.TaskRunID, strings.Join(inputLines, ", "))
	}

	// ── Step 3: parse declared outputs from config and merge ─────────────────
	if len(req.Config) > 0 {
		var cfg echoConfig
		if err := json.Unmarshal(req.Config, &cfg); err != nil {
			log.Printf("[echo] taskRunID=%s  WARNING: cannot parse config: %v", req.TaskRunID, err)
		} else {
			for _, out := range cfg.Outputs {
				if out.Name == "" {
					log.Printf("[echo] taskRunID=%s  WARNING: skipping output entry with empty name", req.TaskRunID)
					continue
				}

				// Validate type
				switch out.Type {
				case capTypeInt, capTypeBool, capTypeString, capTypeArray, capTypeObject:
					// valid
				default:
					log.Printf("[echo] taskRunID=%s  WARNING: output %q has unknown type %q, treating as string",
						req.TaskRunID, out.Name, out.Type)
					out.Type = capTypeString
				}

				// Validate value vs type
				if msg := validateValue(out); msg != "" {
					log.Printf("[echo] taskRunID=%s  WARNING: %s", req.TaskRunID, msg)
				}

				// Resolve final value: use provided or fall back to zero-value
				val := out.Value
				if len(val) == 0 {
					val = zeroValueFor(out.Type)
				}

				// Merge: override if input already carries this name
				if idx, exists := paramIdx[out.Name]; exists {
					params[idx].Type = string(out.Type)
					params[idx].Value = val
				} else {
					paramIdx[out.Name] = len(params)
					params = append(params, model.Parameter{
						Name:  out.Name,
						Type:  string(out.Type),
						Value: val,
					})
				}
			}
		}
	}

	// ── Step 4: log final outputs ────────────────────────────────────────────
	var outputLines []string
	for _, p := range params {
		outputLines = append(outputLines, fmt.Sprintf("%s(%s)=%s", p.Name, p.Type, string(p.Value)))
	}
	log.Printf("[echo] taskRunID=%s  outputs=[%s]", req.TaskRunID, strings.Join(outputLines, ", "))

	return &model.ExecOutputs{
		Parameters: params,
	}, nil
}
