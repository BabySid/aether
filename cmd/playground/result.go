// result.go — machine-readable JSON result for playground.
//
// The JSON is designed to be easy to consume by tests, CI scripts, or
// assertion tools. It contains a flat, stable schema with no HTML or
// presentation concerns.
//
//	{
//	  "runID":      "...",
//	  "workflow":   "path/to/workflow.json",
//	  "phase":      "Succeeded",
//	  "elapsedMs":  123,
//	  "startTime":  "2026-03-25T10:00:00.000+08:00",
//	  "outputs":    [...],
//	  "tasks": [
//	    {
//	      "runID":        "...",
//	      "taskName":     "fetch-data",
//	      "scope":        "main-pipeline/",
//	      "templateName": "fetch-data",
//	      "templateType": "task",
//	      "phase":        "Succeeded",
//	      "message":      "",
//	      "retryCount":   0,
//	      "inputs":       [...],
//	      "outputs":      [...],
//	      "startedAt":    "...",
//	      "finishedAt":   "...",
//	      "duration":     "42ms"
//	    }
//	  ]
//	}
package main

import (
	"encoding/json"
	"time"

	aether "github.com/BabySid/aether"
	"github.com/BabySid/aether/model"
)

// ResultParam is a single parameter in the machine-readable result.
type ResultParam struct {
	Name  string          `json:"name"`
	Type  string          `json:"type,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
}

// ResultTask holds the execution record for a single task run.
type ResultTask struct {
	RunID        string        `json:"runID"`
	TaskName     string        `json:"taskName"`
	Scope        string        `json:"scope"`
	TemplateName string        `json:"templateName"`
	TemplateType string        `json:"templateType"`
	Phase        model.Phase   `json:"phase"`
	Message      string        `json:"message,omitempty"`
	RetryCount   int           `json:"retryCount"`
	Inputs       []ResultParam `json:"inputs,omitempty"`
	Outputs      []ResultParam `json:"outputs,omitempty"`
	StartedAt    string        `json:"startedAt,omitempty"`
	FinishedAt   string        `json:"finishedAt,omitempty"`
	Duration     string        `json:"duration,omitempty"`
}

// PlaygroundResult is the top-level machine-readable result document.
type PlaygroundResult struct {
	RunID     string        `json:"runID"`
	Workflow  string        `json:"workflow"`
	Phase     model.Phase   `json:"phase"`
	ElapsedMs int64         `json:"elapsedMs"`
	StartTime string        `json:"startTime"`
	Outputs   []ResultParam `json:"outputs,omitempty"`
	Tasks     []ResultTask  `json:"tasks"`
}

// toResultParams converts a model.Parameter slice to []ResultParam.
func toResultParams(params []model.Parameter) []ResultParam {
	if len(params) == 0 {
		return nil
	}
	out := make([]ResultParam, len(params))
	for i, p := range params {
		out[i] = ResultParam{
			Name:  p.Name,
			Type:  p.Type,
			Value: p.Value,
		}
	}
	return out
}

const resultTimeLayout = "2006-01-02T15:04:05.000Z07:00"

// generateResult builds a PlaygroundResult from the workflow execution.
func generateResult(
	workflowPath string,
	runID string,
	startTime time.Time,
	elapsed time.Duration,
	exec *aether.WorkflowExecution,
) *PlaygroundResult {
	// Workflow-level outputs
	var wfOutputs []ResultParam
	if exec.Outputs != nil {
		wfOutputs = toResultParams(exec.Outputs.Parameters)
	}

	// Task records
	tasks := make([]ResultTask, 0, len(exec.Tasks))
	for _, t := range exec.Tasks {
		rt := ResultTask{
			RunID:        t.RunID,
			TaskName:     t.TaskName,
			Scope:        t.Scope,
			TemplateName: t.TemplateName,
			TemplateType: t.TemplateType,
		}

		if t.Status != nil {
			rt.Phase = *t.Status
		}
		if t.Message != nil {
			rt.Message = *t.Message
		}
		if t.RetryCount != nil {
			rt.RetryCount = *t.RetryCount
		}
		if t.Inputs != nil {
			rt.Inputs = toResultParams(t.Inputs.Parameters)
		}
		if t.Outputs != nil {
			rt.Outputs = toResultParams(t.Outputs.Parameters)
		}
		if t.Metrics != nil {
			// Metrics fields are already formatted strings from the engine.
			rt.StartedAt = t.Metrics.StartedAt
			rt.FinishedAt = t.Metrics.FinishedAt
			rt.Duration = t.Metrics.Duration
		}

		tasks = append(tasks, rt)
	}

	return &PlaygroundResult{
		RunID:     runID,
		Workflow:  workflowPath,
		Phase:     exec.Phase(),
		ElapsedMs: elapsed.Milliseconds(),
		StartTime: startTime.Format(resultTimeLayout),
		Outputs:   wfOutputs,
		Tasks:     tasks,
	}
}
