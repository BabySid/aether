// verify.go — assertion structures and verification logic for integration testing.
//
// Used by the playground CLI (-verify flag).
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"

	aether "github.com/BabySid/aether"
	"github.com/BabySid/aether/model"
)

// ─────────────────────────────────────────────────────────────────────────────
// Assertion structures
// ─────────────────────────────────────────────────────────────────────────────

// TaskAssertion describes expectations for a single task run.
type TaskAssertion struct {
	TaskName           string                     `json:"taskName"`
	ExpectPhase        string                     `json:"expectPhase,omitempty"`
	ExpectOutputs      map[string]json.RawMessage `json:"expectOutputs,omitempty"`
	ExpectRetryCount   *int                       `json:"expectRetryCount,omitempty"`
	ExpectTemplateType string                     `json:"expectTemplateType,omitempty"`
	ExpectScope        string                     `json:"expectScope,omitempty"`
	ExpectDepth        *int                       `json:"expectDepth,omitempty"`
	ExpectInputs       map[string]json.RawMessage `json:"expectInputs,omitempty"`
}

// WorkflowAssertion is the top-level structure of an assertion file for Workflow.
type WorkflowAssertion struct {
	ExpectPhase     string                     `json:"expectPhase,omitempty"`
	ExpectTaskCount *int                       `json:"expectTaskCount,omitempty"`
	ExpectTasks     []TaskAssertion            `json:"expectTasks,omitempty"`
	ExpectOutputs   map[string]json.RawMessage `json:"expectOutputs,omitempty"`

	// ExpectSubmitError, when non-empty, means Submit should fail with an
	// error containing this substring. The workflow is not executed.
	ExpectSubmitError string `json:"expectSubmitError,omitempty"`

	// Test orchestration: delays for auto-resume/auto-cancel after a task
	// enters PhaseSuspended. These are read by the CLI -verify mode.
	AutoResumeAfter string `json:"autoResumeAfter,omitempty"`
	AutoCancelAfter string `json:"autoCancelAfter,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Verification
// ─────────────────────────────────────────────────────────────────────────────

// Verify checks exec against the assertion and returns a list of failure
// descriptions. An empty slice means all checks passed.
func Verify(exec *aether.WorkflowExecution, assert *WorkflowAssertion) []string {
	var failures []string
	fail := func(format string, args ...any) {
		failures = append(failures, fmt.Sprintf(format, args...))
	}

	// 1. workflow phase
	if assert.ExpectPhase != "" {
		if got := string(exec.Status); got != assert.ExpectPhase {
			fail("workflow phase: got %q, want %q", got, assert.ExpectPhase)
		}
	}

	// 2. task count
	if assert.ExpectTaskCount != nil {
		if got := len(exec.Tasks); got != *assert.ExpectTaskCount {
			fail("task count: got %d, want %d", got, *assert.ExpectTaskCount)
		}
	}

	// 3. workflow-level outputs
	if len(assert.ExpectOutputs) > 0 {
		var params []model.Parameter
		if exec.Outputs != nil {
			params = exec.Outputs.Parameters
		}
		verifyParameters(&failures, "workflow", "output", params, assert.ExpectOutputs)
	}

	// 4. per-task expectations
	verifyTasks(&failures, exec.Tasks, assert.ExpectTasks)

	return failures
}

// ─────────────────────────────────────────────────────────────────────────────
// shared helpers
// ─────────────────────────────────────────────────────────────────────────────

// verifyTasks checks per-task expectations against actual task executions.
func verifyTasks(failures *[]string, tasks []aether.TaskExecution, expects []TaskAssertion) {
	// Build index: taskName → last TaskExecution (last wins for retries)
	taskByName := make(map[string]aether.TaskExecution)
	for _, task := range tasks {
		taskByName[task.TaskName] = task
	}

	for _, ta := range expects {
		task, found := taskByName[ta.TaskName]
		if !found {
			*failures = append(*failures, fmt.Sprintf("task %q: not found (available: %v)", ta.TaskName, taskNameList(tasks)))
			continue
		}

		prefix := fmt.Sprintf("task %q", ta.TaskName)

		// phase
		if ta.ExpectPhase != "" {
			if string(task.Status) != ta.ExpectPhase {
				*failures = append(*failures, fmt.Sprintf("%s phase: got %q, want %q", prefix, task.Status, ta.ExpectPhase))
			}
		}

		// outputs (subset match)
		if len(ta.ExpectOutputs) > 0 {
			var outParams []model.Parameter
			if task.Outputs != nil {
				outParams = task.Outputs.Parameters
			}
			verifyParameters(failures, prefix, "output", outParams, ta.ExpectOutputs)
		}

		// inputs (subset match)
		if len(ta.ExpectInputs) > 0 {
			var inParams []model.Parameter
			if task.Inputs != nil {
				inParams = task.Inputs.Parameters
			}
			verifyParameters(failures, prefix, "input", inParams, ta.ExpectInputs)
		}

		// retryCount
		if ta.ExpectRetryCount != nil {
			if task.RetryCount != *ta.ExpectRetryCount {
				*failures = append(*failures, fmt.Sprintf("%s retryCount: got %d, want %d", prefix, task.RetryCount, *ta.ExpectRetryCount))
			}
		}

		// templateType
		if ta.ExpectTemplateType != "" {
			if task.TemplateType != ta.ExpectTemplateType {
				*failures = append(*failures, fmt.Sprintf("%s templateType: got %q, want %q", prefix, task.TemplateType, ta.ExpectTemplateType))
			}
		}

		// scope
		if ta.ExpectScope != "" {
			if task.Scope != ta.ExpectScope {
				*failures = append(*failures, fmt.Sprintf("%s scope: got %q, want %q", prefix, task.Scope, ta.ExpectScope))
			}
		}

		// depth
		if ta.ExpectDepth != nil {
			if task.Depth != *ta.ExpectDepth {
				*failures = append(*failures, fmt.Sprintf("%s depth: got %d, want %d", prefix, task.Depth, *ta.ExpectDepth))
			}
		}
	}
}

// verifyParameters checks that all expected key-value pairs exist in the actual parameters.
// label is used in error messages (e.g. "output" or "input").
func verifyParameters(failures *[]string, prefix, label string, actual []model.Parameter, expect map[string]json.RawMessage) {
	byName := make(map[string]json.RawMessage, len(actual))
	for _, p := range actual {
		byName[p.Name] = p.Value
	}
	for name, wantVal := range expect {
		wantVal = expandDynamicValue(wantVal)
		gotVal, exists := byName[name]
		if !exists {
			*failures = append(*failures, fmt.Sprintf("%s %s %q: not found", prefix, label, name))
			continue
		}
		if !jsonEqual(gotVal, wantVal) {
			*failures = append(*failures, fmt.Sprintf("%s %s %q: got %s, want %s",
				prefix, label, name, string(gotVal), string(wantVal)))
		}
	}
}

// jsonEqual compares two json.RawMessage values semantically (ignoring whitespace).
func jsonEqual(a, b json.RawMessage) bool {
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	ab, _ := json.Marshal(av)
	bb, _ := json.Marshal(bv)
	return bytes.Equal(ab, bb)
}

// taskNameList returns task names for error messages.
func taskNameList(tasks []aether.TaskExecution) []string {
	names := make([]string, len(tasks))
	for i, t := range tasks {
		names[i] = t.TaskName
	}
	return names
}

// ─────────────────────────────────────────────────────────────────────────────
// dynamic value expansion
// ─────────────────────────────────────────────────────────────────────────────

// dynamicVars maps {{...}} placeholders to runtime values.
var dynamicVars = map[string]string{
	"{{runtime.os}}":   runtime.GOOS,
	"{{runtime.arch}}": runtime.GOARCH,
}

// expandDynamicValue replaces dynamic placeholders in assertion values.
// For example, "{{runtime.os}}" is replaced with the JSON-quoted runtime.GOOS.
func expandDynamicValue(raw json.RawMessage) json.RawMessage {
	s := string(raw)
	// Only expand if the raw value is a JSON string containing a placeholder.
	for placeholder, value := range dynamicVars {
		quoted := `"` + placeholder + `"`
		if strings.Contains(s, quoted) {
			jsonVal, _ := json.Marshal(value)
			s = strings.ReplaceAll(s, quoted, string(jsonVal))
		}
	}
	return json.RawMessage(s)
}
