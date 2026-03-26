// echo.go — built-in "echo" executor for playground.
//
// # Design
//
// The echo executor supports an "outputs" config that declares the expected
// output parameters, each with an explicit type and optional value.
// Supported types: int, bool, string, array, object.
//
// It also supports a "suspend" mode: when config.suspend=true the executor
// returns ExecCodeSuspended on the first call, putting the task into
// PhaseRunning (awaiting resume). The caller must invoke eng.Resume() with
// a payload containing {"__resumed": true} (or any other key) to unblock the
// task. On the resumed call the executor detects the "__resumed" marker in
// inputs, skips the suspend gate, and completes normally.
//
// To make the suspend/resume flow observable in a single CLI run without
// external tooling, set config.autoResumeAfter to a duration string (e.g.
// "1s"). LocalBroker detects ExecCodeSuspended and, if autoResumeAfter is
// set, spawns a goroutine that calls eng.Resume() after the specified delay,
// injecting {"__resumed": true} as the resume payload. This lets the workflow
// proceed automatically while still exercising the full suspend → resume path.
//
// It also supports a "failCount" mode: when config.failCount=N the executor
// returns ExecCodeFailed on the first N calls (retryCount < failCount), then
// succeeds on the (N+1)-th attempt. This makes retry behaviour observable
// without an external failure source.
//
// On execution:
//  1. All input parameters are printed to the log.
//  2. If suspend=true and not yet resumed → return ExecCodeSuspended.
//  3. If retryCount < failCount → return ExecCodeFailed (simulated failure).
//  4. Outputs are built as a *mix* of inputs + declared outputs:
//     - Inputs are echoed back first (excluding the __resumed marker).
//     - Outputs defined in config are merged on top (override same-named inputs).
//     - If an output entry carries no value, a zero-value for the declared type is used.
//
// Workflow JSON example:
//
//	"executor": {
//	  "type": "echo",
//	  "config": {
//	    "suspend": true,
//	    "autoResumeAfter": "1s",
//	    "outputs": [
//	      {"name": "approved", "type": "bool", "value": true}
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
	// Suspend, when true, causes the executor to return ExecCodeSuspended on the
	// first invocation. The task moves to PhaseRunning (awaiting external resume).
	// Call eng.Resume() with payload {"__resumed": true} to unblock the task.
	Suspend bool `json:"suspend,omitempty"`
	// AutoResumeAfter, when non-empty, causes LocalBroker to automatically call
	// eng.Resume() after the specified duration (e.g. "1s") whenever the executor
	// returns ExecCodeSuspended. The resume payload is {"__resumed": true}.
	// This field is playground-only: it makes suspend/resume observable in a
	// single CLI run without needing an external resume trigger.
	AutoResumeAfter string `json:"autoResumeAfter,omitempty"`
	// FailCount, when > 0, causes the executor to return ExecCodeFailed on the
	// first FailCount attempts (retryCount < FailCount). The task will succeed
	// on the (FailCount+1)-th attempt. Use with retry.limit >= FailCount.
	FailCount int          `json:"failCount,omitempty"`
	Outputs   []echoOutput `json:"outputs"`
}

// resumedMarker is the input parameter name injected by the test/caller via
// eng.Resume() to signal that this is a resumed (not first) execution.
const resumedMarker = "__resumed"

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
	// Skip the __resumed marker from the visible output (it is an internal signal).
	paramIdx := make(map[string]int) // name → index in params slice
	var params []model.Parameter
	resumed := false

	if req.Inputs != nil {
		for _, p := range req.Inputs.Parameters {
			if p.Name == resumedMarker {
				resumed = true
				continue // do not include in outputs
			}
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
		log.Printf("[echo] taskRunID=%s taskName=%s templateName=%s  inputs=(none) resumed=%v",
			req.TaskRunID, req.TaskName, req.TemplateName, resumed)
	} else {
		log.Printf("[echo] taskRunID=%s taskName=%s templateName=%s  inputs=[%s] resumed=%v",
			req.TaskRunID, req.TaskName, req.TemplateName, strings.Join(inputLines, ", "), resumed)
	}

	// ── Step 2b: suspend gate ────────────────────────────────────────────────
	// Parse config early so we can check the suspend flag before doing any work.
	var cfg echoConfig
	if len(req.Config) > 0 {
		if err := json.Unmarshal(req.Config, &cfg); err != nil {
			log.Printf("[echo] taskRunID=%s taskName=%s  WARNING: cannot parse config: %v",
				req.TaskRunID, req.TaskName, err)
		}
	}

	if cfg.Suspend && !resumed {
		log.Printf("[echo] taskRunID=%s taskName=%s  suspended — call Resume with {%q: true} to continue",
			req.TaskRunID, req.TaskName, resumedMarker)
		return &model.ExecOutputs{
			Code:    model.ExecCodeSuspended,
			Message: "suspended; awaiting external resume signal",
		}, nil
	}

	// ── Step 2c: failCount gate ──────────────────────────────────────────────
	// Simulate failures for the first cfg.FailCount attempts so retry paths
	// are observable without an external failure source.
	if cfg.FailCount > 0 && req.RetryCount < cfg.FailCount {
		log.Printf("[echo] taskRunID=%s taskName=%s  simulated failure (attempt %d of %d, failCount=%d)",
			req.TaskRunID, req.TaskName, req.RetryCount+1, cfg.FailCount+1, cfg.FailCount)
		return &model.ExecOutputs{
			Code:    model.ExecCodeFailed,
			Message: fmt.Sprintf("simulated failure on attempt %d (failCount=%d)", req.RetryCount+1, cfg.FailCount),
		}, nil
	}

	// ── Step 3: parse declared outputs from config and merge ─────────────────
	if len(req.Config) > 0 {
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
	log.Printf("[echo] taskRunID=%s taskName=%s templateName=%s  outputs=[%s]",
		req.TaskRunID, req.TaskName, req.TemplateName, strings.Join(outputLines, ", "))

	return &model.ExecOutputs{
		Parameters: params,
	}, nil
}
