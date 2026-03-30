// echo.go — built-in "echo" executor for playground.
//
// # Design
//
// The echo executor supports an "outputs" input that declares the expected
// output parameters, each with an explicit type and optional value.
// Supported types: int, bool, string, array, object.
//
// All control parameters are passed as regular task inputs:
//
//   - suspend (bool): when true, returns ExecCodeSuspended on first call.
//     Call eng.Resume() with {"__resumed": true} to unblock.
//   - autoResumeAfter (string): duration (e.g. "1s"); LocalBroker auto-calls
//     Resume() after the delay. Playground-only convenience.
//   - failCount (int): return ExecCodeFailed on the first N attempts.
//   - outputs (array): JSON array of {name, type, value?} output declarations.
//
// Workflow JSON example:
//
//	"executor": {"type": "echo"},
//	"inputs": {
//	  "parameters": [
//	    {"name": "suspend",         "value": true},
//	    {"name": "autoResumeAfter", "value": "1s"},
//	    {"name": "outputs", "value": [{"name": "approved", "type": "bool", "value": true}]}
//	  ]
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

// echoOutput describes a single expected output parameter.
type echoOutput struct {
	Name  string             `json:"name"`
	Type  echoCapabilityType `json:"type"`
	Value json.RawMessage    `json:"value,omitempty"` // optional; falls back to zero-value
}

// resumedMarker is the input parameter name injected by the test/caller via
// eng.Resume() to signal that this is a resumed (not first) execution.
const resumedMarker = "__resumed"

// echoInputParams are the well-known control inputs for the echo executor.
const (
	echoInputSuspend         = "suspend"
	echoInputAutoResumeAfter = "auto-resume-after"
	echoInputFailCount       = "fail-count"
	echoInputOutputs         = "outputs"
)

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

// Schema: echo 的输出由 inputs.outputs 在运行时声明，使用 DynamicOutputs 哨兵。
func (e *EchoExecutor) Schema() executor.ExecutorSchema {
	return executor.SchemaOf[executor.DynamicOutputs, executor.DynamicOutputs](
		"echo", "1.0", "Echoes inputs and produces declared outputs",
	)
}

func (e *EchoExecutor) Execute(_ context.Context, req *executor.ExecuteRequest) (*model.ExecOutputs, error) {
	// ── Step 1: index inputs, build base output map ──────────────────────────
	paramIdx := make(map[string]int) // name → index in params slice
	var params []model.Parameter
	resumed := false

	// control values read from inputs
	var suspend bool
	var failCount int
	var declaredOutputs []echoOutput

	if req.Inputs != nil {
		for _, p := range req.Inputs.Parameters {
			switch p.Name {
			case resumedMarker:
				resumed = true
				continue // internal signal, do not echo
			case echoInputSuspend:
				_ = json.Unmarshal(p.Value, &suspend)
				continue // control param, not echoed
			case echoInputAutoResumeAfter:
				continue // consumed by LocalBroker, not echoed
			case echoInputFailCount:
				_ = json.Unmarshal(p.Value, &failCount)
				continue // control param, not echoed
			case echoInputOutputs:
				_ = json.Unmarshal(p.Value, &declaredOutputs)
				continue // control param, not echoed
			default:
				paramIdx[p.Name] = len(params)
				params = append(params, model.Parameter{
					Name:  p.Name,
					Type:  p.Type,
					Value: p.Value,
				})
			}
		}
	}

	// ── Step 2: log inputs ───────────────────────────────────────────────────
	var inputLines []string
	for _, p := range params {
		inputLines = append(inputLines, fmt.Sprintf("%s(%s)=%s", p.Name, p.Type, string(p.Value)))
	}
	if len(inputLines) == 0 {
		log.Printf("[echo] taskRunID=%s taskName=%s templateName=%s  inputs=(none) resumed=%v",
			req.TaskRunID, req.TaskName, req.TemplateName, resumed)
	} else {
		log.Printf("[echo] taskRunID=%s taskName=%s templateName=%s  inputs=[%s] resumed=%v",
			req.TaskRunID, req.TaskName, req.TemplateName, strings.Join(inputLines, ", "), resumed)
	}

	// ── Step 2b: suspend gate ────────────────────────────────────────────────
	if suspend && !resumed {
		log.Printf("[echo] taskRunID=%s taskName=%s  suspended — call Resume with {%q: true} to continue",
			req.TaskRunID, req.TaskName, resumedMarker)
		return &model.ExecOutputs{
			Code:    model.ExecCodeSuspended,
			Message: "suspended; awaiting external resume signal",
		}, nil
	}

	// ── Step 2c: failCount gate ──────────────────────────────────────────────
	if failCount > 0 && req.RetryCount < failCount {
		log.Printf("[echo] taskRunID=%s taskName=%s  simulated failure (attempt %d of %d, failCount=%d)",
			req.TaskRunID, req.TaskName, req.RetryCount+1, failCount+1, failCount)
		return &model.ExecOutputs{
			Code:    model.ExecCodeFailed,
			Message: fmt.Sprintf("simulated failure on attempt %d (failCount=%d)", req.RetryCount+1, failCount),
		}, nil
	}

	// ── Step 3: merge declared outputs ───────────────────────────────────────
	for _, out := range declaredOutputs {
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

	// ── Step 4: log final outputs ────────────────────────────────────────────
	var outputLines []string
	for _, p := range params {
		outputLines = append(outputLines, fmt.Sprintf("%s(%s)=%s", p.Name, p.Type, string(p.Value)))
	}
	log.Printf("[echo] taskRunID=%s taskName=%s templateName=%s  outputs=[%s]",
		req.TaskRunID, req.TaskName, req.TemplateName, strings.Join(outputLines, ", "))

	return &model.ExecOutputs{
		Parameters: params,
	}, nil
}
