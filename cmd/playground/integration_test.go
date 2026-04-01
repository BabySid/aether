// integration_test.go - end-to-end integration tests for playground examples.
//
// For every *.json file in examples/ the test looks for a matching assertion
// file at examples/assertions/<name>.json. The assertion file declares the
// expected workflow phase, optional task count, and per-task expectations
// (phase + output parameter values).
//
// Assertion file schema:
//
//	{
//	  "skipIntegration": true,          // optional; skip this case in TestIntegration
//	  "expectPhase":     "Succeeded",   // required
//	  "expectTaskCount": 4,             // optional; total TaskRun count
//	  "expectTasks": [                  // optional; per-task expectations
//	    {
//	      "taskName":      "greet",     // matched by TaskRun.TaskName
//	      "expectPhase":   "Succeeded", // optional; defaults to any terminal
//	      "expectOutputs": [            // optional; subset match (other outputs ok)
//	        {"name": "message", "value": "Hello, Alice!"},
//	        {"name": "code",    "value": 200}
//	      ]
//	    }
//	  ]
//	}
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	aether "github.com/BabySid/aether"
	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/errsink"
	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/hook"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/vars"
)

// ─────────────────────────────────────────────────────────────────────────────
// assertion schema
// ─────────────────────────────────────────────────────────────────────────────

// OutputAssertion describes one expected output parameter.
type OutputAssertion struct {
	Name  string          `json:"name"`
	Value json.RawMessage `json:"value"` // expected JSON value
}

// TaskAssertion describes expectations for a single task run.
type TaskAssertion struct {
	TaskName      string            `json:"taskName"`
	ExpectPhase   string            `json:"expectPhase,omitempty"`
	ExpectOutputs []OutputAssertion `json:"expectOutputs,omitempty"`
}

// WorkflowAssertion is the top-level structure of an assertion file.
type WorkflowAssertion struct {
	// SkipIntegration, when true, causes TestIntegration to skip this example.
	// Use it for cases that require special orchestration (e.g. suspend+resume)
	// and are covered by a dedicated test function instead.
	SkipIntegration bool            `json:"skipIntegration,omitempty"`
	ExpectPhase     string          `json:"expectPhase"`
	ExpectTaskCount *int            `json:"expectTaskCount,omitempty"`
	ExpectTasks     []TaskAssertion `json:"expectTasks,omitempty"`

	// CronWorkflow-specific fields (used by TestCronIntegration).
	// CronTriggerCount is the number of times to fire the cron trigger.
	CronTriggerCount int `json:"cronTriggerCount,omitempty"`
	// ExpectRunCount is the expected number of WorkflowRuns after all triggers.
	ExpectRunCount *int `json:"expectRunCount,omitempty"`
	// ExpectRunPhase, if set, asserts the phase of the last created WorkflowRun.
	ExpectRunPhase string `json:"expectRunPhase,omitempty"`
	// ExpectCancelledCount is the expected number of Cancelled runs.
	ExpectCancelledCount *int `json:"expectCancelledCount,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// engine wiring helpers
// ─────────────────────────────────────────────────────────────────────────────

// engineBundle holds all components wired together for a single workflow run.
type engineBundle struct {
	eng      *aether.Engine
	memStore *MemoryStore
	brok     *LocalBroker
	finishCh chan struct{}
	cancel   context.CancelFunc
	ctx      context.Context
}

// newEngineBundle creates a fully-wired engine, broker, and store.
// watchInterval controls how often the timeout watcher scans; use a smaller
// value (e.g. 100ms) when testing task-level timeouts for faster detection.
// extraOpts are appended after the standard engine options, allowing tests to
// inject additional configuration such as WithVarsSource.
// The caller is responsible for calling eng.Start() and eng.Stop().
func newEngineBundle(t *testing.T, timeoutSec int, extraOpts ...any) *engineBundle {
	t.Helper()

	interval := 500 * time.Millisecond
	var engineOpts []aether.Option

	for _, opt := range extraOpts {
		switch v := opt.(type) {
		case time.Duration:
			interval = v
		case aether.Option:
			engineOpts = append(engineOpts, v)
		}
	}

	reg := executor.NewRegistry()
	if err := reg.Register(newEcho()); err != nil {
		t.Fatalf("register echo: %v", err)
	}

	memStore := NewMemoryStore()

	var eng *aether.Engine
	finishCh := make(chan struct{}, 64)

	brok := NewLocalBroker(
		reg,
		func(ctx context.Context, taskRunID string) {
			eng.OnTaskStarted(ctx, taskRunID)
		},
		func(ctx context.Context, result *broker.TaskResult) {
			eng.OnTaskCompleted(ctx, result)
			select {
			case finishCh <- struct{}{}:
			default:
			}
		},
	)

	baseOpts := []aether.Option{
		aether.WithStore(memStore),
		aether.WithIDGenerator(NewAtomicIDGen()),
		aether.WithExprEvaluator(NewSimpleEvaluator()),
		aether.WithTaskBroker(brok),
		aether.WithExecutorRegistry(reg),
		aether.WithTimeoutWatcher(newPollingWatcher(memStore, interval)),
	}

	var err error
	eng, err = aether.New(append(baseOpts, engineOpts...)...)
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	return &engineBundle{
		eng:      eng,
		memStore: memStore,
		brok:     brok,
		finishCh: finishCh,
		cancel:   cancel,
		ctx:      ctx,
	}
}

// waitTerminal polls until the workflow reaches a terminal phase or ctx expires.
func waitTerminal(t *testing.T, b *engineBundle, runID string) *aether.WorkflowExecution {
	t.Helper()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		exec, err := b.eng.Get(b.ctx, runID)
		if err != nil {
			t.Fatalf("get workflow: %v", err)
		}
		if exec.Status.IsTerminal() {
			return exec
		}
		select {
		case <-b.ctx.Done():
			return exec
		case <-b.finishCh:
			for len(b.finishCh) > 0 {
				<-b.finishCh
			}
		case <-ticker.C:
		}
	}
}

// waitTaskPhase polls until the named task reaches the expected phase or ctx expires.
// Returns the matching TaskExecution so the caller can use its RunID for Resume.
func waitTaskPhase(t *testing.T, b *engineBundle, runID string, taskName string, wantPhase model.Phase) aether.TaskExecution {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(10 * time.Second)
	for {
		exec, err := b.eng.Get(b.ctx, runID)
		if err != nil {
			t.Fatalf("get workflow: %v", err)
		}
		for _, tr := range exec.Tasks {
			if tr.TaskName == taskName && tr.Status == wantPhase {
				return tr
			}
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for task %q to reach phase %s", taskName, wantPhase)
			return aether.TaskExecution{}
		case <-b.ctx.Done():
			t.Fatalf("context done before task %q reached phase %s", taskName, wantPhase)
			return aether.TaskExecution{}
		case <-b.finishCh:
			for len(b.finishCh) > 0 {
				<-b.finishCh
			}
		case <-ticker.C:
		}
	}
}

func runWorkflow(t *testing.T, wfPath string, timeoutSec int) *aether.WorkflowExecution {
	t.Helper()

	b := newEngineBundle(t, timeoutSec)
	defer b.cancel()

	raw, err := os.ReadFile(wfPath)
	if err != nil {
		t.Fatalf("read %s: %v", wfPath, err)
	}
	var wf model.Workflow
	if err := json.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", wfPath, err)
	}

	if err := b.eng.Start(b.ctx); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	defer b.eng.Stop()

	runID, err := b.eng.Submit(b.ctx, &wf)
	if err != nil {
		t.Fatalf("submit workflow: %v", err)
	}

	return waitTerminal(t, b, runID)
}

// ─────────────────────────────────────────────────────────────────────────────
// assertion helpers
// ─────────────────────────────────────────────────────────────────────────────

// jsonEqual compares two json.RawMessage values semantically (ignoring whitespace).
func jsonEqual(a, b json.RawMessage) bool {
	var av, bv interface{}
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

// taskNames returns a display list of task names for error messages.
func taskNames(tasks []aether.TaskExecution) []string {
	names := make([]string, len(tasks))
	for i, t := range tasks {
		names[i] = t.TaskName
	}
	return names
}

// assertExecution applies a WorkflowAssertion to a finished execution.
func assertExecution(t *testing.T, exec *aether.WorkflowExecution, assert *WorkflowAssertion) {
	t.Helper()

	// 1. workflow phase
	if got := string(exec.Status); got != assert.ExpectPhase {
		t.Errorf("workflow phase: got %q, want %q", got, assert.ExpectPhase)
	}

	// 2. task count
	if assert.ExpectTaskCount != nil {
		if got := len(exec.Tasks); got != *assert.ExpectTaskCount {
			t.Errorf("task count: got %d, want %d", got, *assert.ExpectTaskCount)
		}
	}

	// 3. per-task expectations
	// Build index: taskName -> last TaskExecution (last wins for retries)
	taskByName := make(map[string]aether.TaskExecution)
	for _, task := range exec.Tasks {
		taskByName[task.TaskName] = task
	}

	for _, ta := range assert.ExpectTasks {
		task, found := taskByName[ta.TaskName]
		if !found {
			t.Errorf("task %q: not found (available: %v)", ta.TaskName, taskNames(exec.Tasks))
			continue
		}

		// 3a. task phase
		if ta.ExpectPhase != "" {
			if string(task.Status) != ta.ExpectPhase {
				t.Errorf("task %q phase: got %q, want %q", ta.TaskName, task.Status, ta.ExpectPhase)
			}
		}

		// 3b. output parameters (subset match)
		if len(ta.ExpectOutputs) > 0 {
			outByName := make(map[string]json.RawMessage)
			if task.Outputs != nil {
				for _, p := range task.Outputs.Parameters {
					outByName[p.Name] = p.Value
				}
			}
			for _, oa := range ta.ExpectOutputs {
				gotVal, exists := outByName[oa.Name]
				if !exists {
					t.Errorf("task %q output %q: not found", ta.TaskName, oa.Name)
					continue
				}
				if !jsonEqual(gotVal, oa.Value) {
					t.Errorf("task %q output %q: got %s, want %s",
						ta.TaskName, oa.Name, string(gotVal), string(oa.Value))
				}
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// table-driven integration test
// ─────────────────────────────────────────────────────────────────────────────

func TestIntegration(t *testing.T) {
	entries, err := filepath.Glob("examples/*.json")
	if err != nil || len(entries) == 0 {
		t.Fatal("no example files found in examples/")
	}

	for _, wfPath := range entries {
		wfPath := wfPath
		name := strings.TrimSuffix(filepath.Base(wfPath), ".json")
		assertPath := filepath.Join("examples", "assertions", name+".json")

		t.Run(name, func(t *testing.T) {
			// Load assertion file (required).
			assertRaw, err := os.ReadFile(assertPath)
			if err != nil {
				t.Fatalf("assertion file %s not found: %v", assertPath, err)
			}
			var assert WorkflowAssertion
			if err := json.Unmarshal(assertRaw, &assert); err != nil {
				t.Fatalf("parse assertion %s: %v", assertPath, err)
			}

			if assert.SkipIntegration {
				t.Skipf("skipped: requires dedicated test (suspend/resume or other special orchestration)")
			}

			exec := runWorkflow(t, wfPath, 30)
			assertExecution(t, exec, &assert)

			// Always log summary for visibility.
			t.Logf("workflow=%s phase=%s tasks=%d", name, exec.Status, len(exec.Tasks))
			for _, task := range exec.Tasks {
				phase := task.Status
				var outParts []string
				if task.Outputs != nil {
					for _, p := range task.Outputs.Parameters {
						outParts = append(outParts, fmt.Sprintf("%s=%s", p.Name, string(p.Value)))
					}
				}
				t.Logf("  task=%-30s phase=%-12s outputs=[%s]",
					task.TaskName, phase, strings.Join(outParts, ", "))
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestAwaitSuspend — dedicated test for the suspend/resume pattern.
//
// Workflow: prepare → await-approval (suspend=true) → finalize
//
// Steps:
//  1. Submit workflow; "prepare" runs and completes.
//  2. "await-approval" task starts and immediately suspends (PhaseRunning).
//  3. Test calls eng.Resume() injecting {approver: "test-user", __resumed: true}.
//  4. "await-approval" wakes up, detects __resumed marker, completes normally.
//  5. "finalize" runs and the workflow reaches Succeeded.
// ─────────────────────────────────────────────────────────────────────────────

func TestAwaitSuspend(t *testing.T) {
	const wfPath = "examples/11-await-suspend.json"

	raw, err := os.ReadFile(wfPath)
	if err != nil {
		t.Fatalf("read %s: %v", wfPath, err)
	}
	var wf model.Workflow
	if err := json.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", wfPath, err)
	}

	b := newEngineBundle(t, 30)
	defer b.cancel()

	if err := b.eng.Start(b.ctx); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	defer b.eng.Stop()

	runID, err := b.eng.Submit(b.ctx, &wf)
	if err != nil {
		t.Fatalf("submit workflow: %v", err)
	}
	t.Logf("submitted runID=%s", runID)

	// Step 1: wait for "await-approval" to reach PhaseSuspended.
	suspendedTask := waitTaskPhase(t, b, runID, "await-approval", model.PhaseSuspended)
	t.Logf("task %q suspended, taskRunID=%s", suspendedTask.TaskName, suspendedTask.RunID)

	// Step 2: resume with approver payload + __resumed marker.
	resumePayload := map[string]any{
		"approver":    "test-user",
		resumedMarker: true,
	}
	if err := b.eng.Resume(b.ctx, runID, suspendedTask.RunID, resumePayload); err != nil {
		t.Fatalf("resume task: %v", err)
	}
	t.Logf("resume sent")

	// Step 3: wait for workflow to complete.
	finalExec := waitTerminal(t, b, runID)
	t.Logf("workflow phase=%s tasks=%d", finalExec.Status, len(finalExec.Tasks))

	// Build assertion matching expected outcomes.
	// Note: resume payload keys (approver, __resumed) are echoed back as inputs.
	// The echo executor merges config outputs on top of echoed inputs, so:
	//   - "approved" comes from config outputs (value: true)
	//   - "approver" comes from the resume payload echoed as input
	taskCount := 4
	assertion := &WorkflowAssertion{
		ExpectPhase:     "Succeeded",
		ExpectTaskCount: &taskCount,
		ExpectTasks: []TaskAssertion{
			{
				TaskName:    "prepare",
				ExpectPhase: "Succeeded",
				ExpectOutputs: []OutputAssertion{
					{Name: "status", Value: json.RawMessage(`"ready"`)},
				},
			},
			{
				TaskName:    "await-approval",
				ExpectPhase: "Succeeded",
				ExpectOutputs: []OutputAssertion{
					{Name: "approved", Value: json.RawMessage(`true`)},
					{Name: "approver", Value: json.RawMessage(`"test-user"`)},
				},
			},
			{
				TaskName:    "finalize",
				ExpectPhase: "Succeeded",
				ExpectOutputs: []OutputAssertion{
					{Name: "result", Value: json.RawMessage(`"done"`)},
				},
			},
		},
	}
	assertExecution(t, finalExec, assertion)

	// Log all task outputs for visibility.
	for _, task := range finalExec.Tasks {
		phase := task.Status
		var outParts []string
		if task.Outputs != nil {
			for _, p := range task.Outputs.Parameters {
				outParts = append(outParts, fmt.Sprintf("%s=%s", p.Name, string(p.Value)))
			}
		}
		t.Logf("  task=%-30s phase=%-12s outputs=[%s]",
			task.TaskName, phase, strings.Join(outParts, ", "))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestTaskTimeout — dedicated test for task-level timeout + continueOn.timeout.
//
// Workflow: prepare → wait-external (suspend=true, timeout=1s, continueOn.timeout=true) → finalize
//
// Steps:
//  1. Submit workflow; "prepare" completes.
//  2. "wait-external" suspends and is never resumed — it times out after 1s.
//  3. continueOn.timeout=true allows the DAG to continue.
//  4. "finalize" runs and the workflow reaches Succeeded.
// ─────────────────────────────────────────────────────────────────────────────

func TestTaskTimeout(t *testing.T) {
	const wfPath = "examples/12-task-timeout.json"

	raw, err := os.ReadFile(wfPath)
	if err != nil {
		t.Fatalf("read %s: %v", wfPath, err)
	}
	var wf model.Workflow
	if err := json.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", wfPath, err)
	}

	// Use a fast watcher interval (100ms) so the 1s task timeout is detected promptly.
	b := newEngineBundle(t, 30, 100*time.Millisecond)
	defer b.cancel()

	if err := b.eng.Start(b.ctx); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	defer b.eng.Stop()

	runID, err := b.eng.Submit(b.ctx, &wf)
	if err != nil {
		t.Fatalf("submit workflow: %v", err)
	}
	t.Logf("submitted runID=%s", runID)

	// Wait for "wait-external" to enter PhaseSuspended.
	_ = waitTaskPhase(t, b, runID, "wait-external", model.PhaseSuspended)
	t.Logf("task wait-external suspended; waiting for timeout (~1s)...")

	// Do NOT resume — let the task timeout naturally.
	finalExec := waitTerminal(t, b, runID)
	t.Logf("workflow phase=%s tasks=%d", finalExec.Status, len(finalExec.Tasks))

	// The workflow ends in Succeeded: wait-external timed out (Timeout) but its
	// continueOn.timeout=true means the timeout is tolerated by the DAG aggregation.
	// finalize runs after wait-external because continueOn.timeout allows scheduling,
	// and the DAG phase is Succeeded because the Timeout is tolerated in aggregation.
	taskCount := 4
	assertion := &WorkflowAssertion{
		ExpectPhase:     "Succeeded",
		ExpectTaskCount: &taskCount,
		ExpectTasks: []TaskAssertion{
			{
				TaskName:    "prepare",
				ExpectPhase: "Succeeded",
				ExpectOutputs: []OutputAssertion{
					{Name: "status", Value: json.RawMessage(`"ready"`)},
				},
			},
			{
				TaskName:    "wait-external",
				ExpectPhase: "Timeout",
			},
			{
				TaskName:    "finalize",
				ExpectPhase: "Succeeded",
				ExpectOutputs: []OutputAssertion{
					{Name: "result", Value: json.RawMessage(`"completed-after-timeout"`)},
				},
			},
		},
	}
	assertExecution(t, finalExec, assertion)

	for _, task := range finalExec.Tasks {
		phase := task.Status
		var outParts []string
		if task.Outputs != nil {
			for _, p := range task.Outputs.Parameters {
				outParts = append(outParts, fmt.Sprintf("%s=%s", p.Name, string(p.Value)))
			}
		}
		t.Logf("  task=%-30s phase=%-12s outputs=[%s]",
			task.TaskName, phase, strings.Join(outParts, ", "))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestCancelWorkflow — dedicated test for workflow cancellation.
//
// Workflow: prepare → await (suspend=true) → finalize
//
// Steps:
//  1. Submit workflow; "prepare" completes.
//  2. "await" task suspends (PhaseSuspended).
//  3. Test calls eng.Cancel().
//  4. "await" transitions to PhaseCancelled, "finalize" never runs.
//  5. Workflow ends in PhaseCancelled.
// ─────────────────────────────────────────────────────────────────────────────

func TestCancelWorkflow(t *testing.T) {
	const wfPath = "examples/20-cancel-workflow.json"

	raw, err := os.ReadFile(wfPath)
	if err != nil {
		t.Fatalf("read %s: %v", wfPath, err)
	}
	var wf model.Workflow
	if err := json.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", wfPath, err)
	}

	b := newEngineBundle(t, 30)
	defer b.cancel()

	if err := b.eng.Start(b.ctx); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	defer b.eng.Stop()

	runID, err := b.eng.Submit(b.ctx, &wf)
	if err != nil {
		t.Fatalf("submit workflow: %v", err)
	}
	t.Logf("submitted runID=%s", runID)

	// Wait for "await" to reach PhaseSuspended.
	_ = waitTaskPhase(t, b, runID, "await", model.PhaseSuspended)
	t.Logf("task await suspended; cancelling workflow...")

	// Cancel the workflow.
	if err := b.eng.Cancel(b.ctx, runID); err != nil {
		t.Fatalf("cancel workflow: %v", err)
	}
	t.Logf("cancel sent")

	// Wait for workflow to reach terminal state.
	finalExec := waitTerminal(t, b, runID)
	t.Logf("workflow phase=%s tasks=%d", finalExec.Status, len(finalExec.Tasks))

	// Assert: workflow is Cancelled.
	if finalExec.Status != model.PhaseCancelled {
		t.Errorf("expected workflow phase %s, got %s", model.PhaseCancelled, finalExec.Status)
	}

	// Assert: prepare succeeded, await cancelled, finalize should not exist.
	taskByName := make(map[string]aether.TaskExecution)
	for _, task := range finalExec.Tasks {
		taskByName[task.TaskName] = task
	}

	if prepare, ok := taskByName["prepare"]; ok {
		if prepare.Status != model.PhaseSucceeded {
			t.Errorf("task prepare: expected %s, got %s", model.PhaseSucceeded, prepare.Status)
		}
	} else {
		t.Error("task prepare not found")
	}

	if await, ok := taskByName["await"]; ok {
		if await.Status != model.PhaseCancelled {
			t.Errorf("task await: expected %s, got %s", model.PhaseCancelled, await.Status)
		}
	} else {
		t.Error("task await not found")
	}

	// "finalize" should either not exist or be Cancelled (never started).
	if finalize, ok := taskByName["finalize"]; ok {
		if finalize.Status != model.PhaseCancelled {
			t.Errorf("task finalize: expected %s or absent, got %s", model.PhaseCancelled, finalize.Status)
		}
	}

	for _, task := range finalExec.Tasks {
		t.Logf("  task=%-30s phase=%-12s", task.TaskName, task.Status)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// DeploymentSource — user-defined vars.Source for deployment metadata.
//
// Demonstrates how to implement a custom namespace ("deploy.*") and register
// it at engine level via aether.WithVarsSource.
// ─────────────────────────────────────────────────────────────────────────────

// DeploymentSource is a custom vars.Source that exposes deployment metadata
// under the "deploy" namespace.
type DeploymentSource struct {
	Cluster string
	Region  string
}

func (d *DeploymentSource) Namespace() string { return "deploy" }

func (d *DeploymentSource) Vars() map[string]any {
	return map[string]any{
		"deploy.cluster": d.Cluster,
		"deploy.region":  d.Region,
	}
}

// Ensure DeploymentSource implements vars.Source at compile time.
var _ vars.Source = (*DeploymentSource)(nil)

// ─────────────────────────────────────────────────────────────────────────────
// TestCustomVars — end-to-end test for custom vars.Source.
//
// Workflow: 14-custom-vars.json
//   - Task "report-env" receives {{system.os}}, {{system.arch}}, {{deploy.cluster}},
//     {{deploy.region}} as arguments, resolved from two custom Sources.
//   - The echo executor echoes them back as output parameters.
//   - Test asserts the resolved values match the expected runtime values.
// ─────────────────────────────────────────────────────────────────────────────

func TestCustomVars(t *testing.T) {
	const wfPath = "examples/14-custom-vars.json"

	raw, err := os.ReadFile(wfPath)
	if err != nil {
		t.Fatalf("read %s: %v", wfPath, err)
	}
	var wf model.Workflow
	if err := json.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", wfPath, err)
	}

	// Wire engine with two custom Sources:
	//   - vars.SystemSource{}          → system.os, system.arch
	//   - DeploymentSource{...}        → deploy.cluster, deploy.region
	b := newEngineBundle(t, 30,
		aether.WithVarsSource(&vars.SystemSource{}),
		aether.WithVarsSource(&DeploymentSource{
			Cluster: "prod-cluster",
			Region:  "ap-east-1",
		}),
	)
	defer b.cancel()

	if err := b.eng.Start(b.ctx); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	defer b.eng.Stop()

	runID, err := b.eng.Submit(b.ctx, &wf)
	if err != nil {
		t.Fatalf("submit workflow: %v", err)
	}
	t.Logf("submitted runID=%s", runID)

	finalExec := waitTerminal(t, b, runID)
	t.Logf("workflow phase=%s tasks=%d", finalExec.Status, len(finalExec.Tasks))

	// Build assertion: os and arch come from runtime, cluster/region from DeploymentSource.
	osVal, _ := json.Marshal(runtime.GOOS)
	archVal, _ := json.Marshal(runtime.GOARCH)
	assertion := &WorkflowAssertion{
		ExpectPhase: "Succeeded",
		ExpectTasks: []TaskAssertion{
			{
				TaskName:    "report-env",
				ExpectPhase: "Succeeded",
				ExpectOutputs: []OutputAssertion{
					{Name: "os", Value: osVal},
					{Name: "arch", Value: archVal},
					{Name: "cluster", Value: json.RawMessage(`"prod-cluster"`)},
					{Name: "region", Value: json.RawMessage(`"ap-east-1"`)},
				},
			},
		},
	}
	assertExecution(t, finalExec, assertion)

	for _, task := range finalExec.Tasks {
		phase := task.Status
		var outParts []string
		if task.Outputs != nil {
			for _, p := range task.Outputs.Parameters {
				outParts = append(outParts, fmt.Sprintf("%s=%s", p.Name, string(p.Value)))
			}
		}
		t.Logf("  task=%-30s phase=%-12s outputs=[%s]",
			task.TaskName, phase, strings.Join(outParts, ", "))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestOSBranch — end-to-end test for OS-based DAG branching with custom vars.
//
// Workflow: 15-os-branch.json
//   - Task "detect-os" reads system.os via vars.SystemSource and outputs it.
//   - "run-on-mac" runs only when os == "darwin" (when condition).
//   - "run-on-linux" runs only when os == "linux" (when condition).
//   - Exactly one of the two branch tasks should Succeed; the other is Skipped.
// ─────────────────────────────────────────────────────────────────────────────

func TestOSBranch(t *testing.T) {
	const wfPath = "examples/15-os-branch.json"

	raw, err := os.ReadFile(wfPath)
	if err != nil {
		t.Fatalf("read %s: %v", wfPath, err)
	}
	var wf model.Workflow
	if err := json.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", wfPath, err)
	}

	// Wire engine with SystemSource so system.os is available.
	b := newEngineBundle(t, 30, aether.WithVarsSource(&vars.SystemSource{}))
	defer b.cancel()

	if err := b.eng.Start(b.ctx); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	defer b.eng.Stop()

	runID, err := b.eng.Submit(b.ctx, &wf)
	if err != nil {
		t.Fatalf("submit workflow: %v", err)
	}
	t.Logf("submitted runID=%s", runID)

	finalExec := waitTerminal(t, b, runID)
	t.Logf("workflow phase=%s tasks=%d", finalExec.Status, len(finalExec.Tasks))

	// Determine expected branch phases based on current runtime OS.
	macPhase, linuxPhase := "Skipped", "Skipped"
	switch runtime.GOOS {
	case "darwin":
		macPhase = "Succeeded"
	case "linux":
		linuxPhase = "Succeeded"
	}

	osVal, _ := json.Marshal(runtime.GOOS)
	assertion := &WorkflowAssertion{
		ExpectPhase: "Succeeded",
		ExpectTasks: []TaskAssertion{
			{
				TaskName:    "detect-os",
				ExpectPhase: "Succeeded",
				ExpectOutputs: []OutputAssertion{
					{Name: "os", Value: osVal},
				},
			},
			{
				TaskName:    "run-on-mac",
				ExpectPhase: macPhase,
			},
			{
				TaskName:    "run-on-linux",
				ExpectPhase: linuxPhase,
			},
		},
	}
	assertExecution(t, finalExec, assertion)

	for _, task := range finalExec.Tasks {
		phase := task.Status
		var outParts []string
		if task.Outputs != nil {
			for _, p := range task.Outputs.Parameters {
				outParts = append(outParts, fmt.Sprintf("%s=%s", p.Name, string(p.Value)))
			}
		}
		if len(outParts) > 0 {
			t.Logf("  task=%-30s phase=%-12s outputs=[%s]", task.TaskName, phase, strings.Join(outParts, ", "))
		} else {
			t.Logf("  task=%-30s phase=%-12s", task.TaskName, phase)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Hook integration test helpers
// ─────────────────────────────────────────────────────────────────────────────

// testNotifier captures hook events for assertion.
type testNotifier struct {
	mu     sync.Mutex
	events []*hook.Event
}

func (n *testNotifier) Notify(_ context.Context, event *hook.Event) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.events = append(n.events, event)
	return nil
}

func (n *testNotifier) Events() []*hook.Event {
	n.mu.Lock()
	defer n.mu.Unlock()
	cp := make([]*hook.Event, len(n.events))
	copy(cp, n.events)
	return cp
}

// hasEvent checks if at least one event matches the given scope and hook type.
func (n *testNotifier) hasEvent(scope hook.Scope, ht hook.Type) bool {
	for _, ev := range n.Events() {
		if ev.Scope == scope && ev.HookType == ht {
			return true
		}
	}
	return false
}

// countEvents returns the number of events matching the given scope and hook type.
func (n *testNotifier) countEvents(scope hook.Scope, ht hook.Type) int {
	count := 0
	for _, ev := range n.Events() {
		if ev.Scope == scope && ev.HookType == ht {
			count++
		}
	}
	return count
}

// testErrorSink captures errors reported to the ErrorSink.
type testErrorSink struct {
	mu     sync.Mutex
	errors []errsink.ErrorContext
}

func (s *testErrorSink) OnError(_ context.Context, _ error, ec errsink.ErrorContext) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors = append(s.errors, ec)
}

func (s *testErrorSink) Errors() []errsink.ErrorContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]errsink.ErrorContext, len(s.errors))
	copy(cp, s.errors)
	return cp
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHooksLifecycle — verifies workflow-level and task-level hooks fire
// during a normal successful run.
//
// Workflow: 24-hooks-lifecycle.json
//   - Workflow hooks: onStart, onSuccess, onExit
//   - Task hooks on step-a and step-b: onStart, onSuccess, onExit
//
// Expected events:
//   - Workflow: OnStart, OnSuccess, OnExit
//   - Task: OnStart x2, OnSuccess x2, OnExit x2
// ─────────────────────────────────────────────────────────────────────────────

func TestHooksLifecycle(t *testing.T) {
	const wfPath = "examples/24-hooks-lifecycle.json"

	raw, err := os.ReadFile(wfPath)
	if err != nil {
		t.Fatalf("read %s: %v", wfPath, err)
	}
	var wf model.Workflow
	if err := json.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", wfPath, err)
	}

	notifier := &testNotifier{}
	sink := &testErrorSink{}

	b := newEngineBundle(t, 30,
		aether.WithHookNotifier(notifier),
		aether.WithErrorSink(sink),
	)
	defer b.cancel()

	if err := b.eng.Start(b.ctx); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	defer b.eng.Stop()

	runID, err := b.eng.Submit(b.ctx, &wf)
	if err != nil {
		t.Fatalf("submit workflow: %v", err)
	}
	t.Logf("submitted runID=%s", runID)

	finalExec := waitTerminal(t, b, runID)
	if finalExec.Status != model.PhaseSucceeded {
		t.Fatalf("expected Succeeded, got %s", finalExec.Status)
	}

	// Verify workflow-level hooks
	for _, ht := range []hook.Type{hook.OnStart, hook.OnSuccess, hook.OnExit} {
		if !notifier.hasEvent(hook.ScopeWorkflow, ht) {
			t.Errorf("missing workflow hook %s", ht)
		}
	}

	// Verify task-level hooks: OnStart, OnSuccess, OnExit for each of 2 tasks
	for _, ht := range []hook.Type{hook.OnStart, hook.OnSuccess, hook.OnExit} {
		count := notifier.countEvents(hook.ScopeTask, ht)
		if count < 2 {
			t.Errorf("expected at least 2 task %s events, got %d", ht, count)
		}
	}

	// Verify event metadata: all events should carry the workflow run ID
	for _, ev := range notifier.Events() {
		if ev.WorkflowRunID == "" {
			t.Errorf("event %s/%s missing WorkflowRunID", ev.Scope, ev.HookType)
		}
		if ev.Scope == hook.ScopeTask && ev.TaskName == "" {
			t.Errorf("task event %s missing TaskName", ev.HookType)
		}
		if ev.Template != "hook-task" {
			t.Errorf("event %s/%s: expected template hook-task, got %s", ev.Scope, ev.HookType, ev.Template)
		}
	}

	// ErrorSink should have no critical errors
	for _, ec := range sink.Errors() {
		if ec.Severity >= errsink.SeverityError {
			t.Errorf("unexpected error: operation=%s severity=%d", ec.Operation, ec.Severity)
		}
	}

	t.Logf("captured %d hook events total", len(notifier.Events()))
	for _, ev := range notifier.Events() {
		t.Logf("  scope=%-10s type=%-12s workflow=%s task=%s", ev.Scope, ev.HookType, ev.WorkflowRunID, ev.TaskName)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHooksSuspendResume — verifies OnSuspend and OnResume hooks fire during
// suspend/resume lifecycle.
//
// Workflow: 25-hooks-suspend-resume.json
//   - Task "approval": hooks onStart, onSuspend, onResume, onSuccess, onExit
//
// Expected events (task scope):
//   - OnStart → OnSuspend → OnResume → OnSuccess → OnExit
// ─────────────────────────────────────────────────────────────────────────────

func TestHooksSuspendResume(t *testing.T) {
	const wfPath = "examples/25-hooks-suspend-resume.json"

	raw, err := os.ReadFile(wfPath)
	if err != nil {
		t.Fatalf("read %s: %v", wfPath, err)
	}
	var wf model.Workflow
	if err := json.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", wfPath, err)
	}

	notifier := &testNotifier{}
	b := newEngineBundle(t, 30, aether.WithHookNotifier(notifier))
	defer b.cancel()

	if err := b.eng.Start(b.ctx); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	defer b.eng.Stop()

	runID, err := b.eng.Submit(b.ctx, &wf)
	if err != nil {
		t.Fatalf("submit workflow: %v", err)
	}

	// Wait for task to suspend
	suspendedTask := waitTaskPhase(t, b, runID, "approval", model.PhaseSuspended)
	t.Logf("task %q suspended, taskRunID=%s", suspendedTask.TaskName, suspendedTask.RunID)

	// Verify OnSuspend fired
	if !notifier.hasEvent(hook.ScopeTask, hook.OnSuspend) {
		t.Error("missing task OnSuspend hook after suspend")
	}

	// Resume the task
	resumePayload := map[string]any{
		"approver":    "test-user",
		resumedMarker: true,
	}
	if err := b.eng.Resume(b.ctx, runID, suspendedTask.RunID, resumePayload); err != nil {
		t.Fatalf("resume task: %v", err)
	}

	// Wait for workflow to complete
	finalExec := waitTerminal(t, b, runID)
	if finalExec.Status != model.PhaseSucceeded {
		t.Fatalf("expected Succeeded, got %s", finalExec.Status)
	}

	// Verify the full lifecycle of task hooks
	for _, ht := range []hook.Type{hook.OnStart, hook.OnSuspend, hook.OnResume, hook.OnSuccess, hook.OnExit} {
		if !notifier.hasEvent(hook.ScopeTask, ht) {
			t.Errorf("missing task hook %s", ht)
		}
	}

	t.Logf("captured %d hook events total", len(notifier.Events()))
	for _, ev := range notifier.Events() {
		t.Logf("  scope=%-10s type=%-12s task=%s", ev.Scope, ev.HookType, ev.TaskName)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHooksCancel — verifies OnCancel and OnExit hooks fire when a workflow
// is cancelled.
//
// Workflow: 26-hooks-cancel.json
//   - Workflow hooks: onStart, onCancel, onExit
//   - Task "wait" hooks: onCancel, onExit
//
// Expected:
//   - Workflow: OnStart, OnCancel, OnExit
//   - Task "wait": OnCancel, OnExit (task-level cancel hooks)
// ─────────────────────────────────────────────────────────────────────────────

func TestHooksCancel(t *testing.T) {
	const wfPath = "examples/26-hooks-cancel.json"

	raw, err := os.ReadFile(wfPath)
	if err != nil {
		t.Fatalf("read %s: %v", wfPath, err)
	}
	var wf model.Workflow
	if err := json.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", wfPath, err)
	}

	notifier := &testNotifier{}
	b := newEngineBundle(t, 30, aether.WithHookNotifier(notifier))
	defer b.cancel()

	if err := b.eng.Start(b.ctx); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	defer b.eng.Stop()

	runID, err := b.eng.Submit(b.ctx, &wf)
	if err != nil {
		t.Fatalf("submit workflow: %v", err)
	}

	// Wait for "wait" task to suspend
	_ = waitTaskPhase(t, b, runID, "wait", model.PhaseSuspended)
	t.Logf("task wait suspended; cancelling workflow...")

	// Cancel the workflow
	if err := b.eng.Cancel(b.ctx, runID); err != nil {
		t.Fatalf("cancel workflow: %v", err)
	}

	// Wait for terminal state
	finalExec := waitTerminal(t, b, runID)
	if finalExec.Status != model.PhaseCancelled {
		t.Fatalf("expected Cancelled, got %s", finalExec.Status)
	}

	// Verify workflow-level cancel hooks
	if !notifier.hasEvent(hook.ScopeWorkflow, hook.OnStart) {
		t.Error("missing workflow OnStart hook")
	}
	if !notifier.hasEvent(hook.ScopeWorkflow, hook.OnCancel) {
		t.Error("missing workflow OnCancel hook")
	}
	if !notifier.hasEvent(hook.ScopeWorkflow, hook.OnExit) {
		t.Error("missing workflow OnExit hook")
	}

	// Verify task-level cancel hooks for "wait"
	found := false
	for _, ev := range notifier.Events() {
		if ev.Scope == hook.ScopeTask && ev.HookType == hook.OnCancel && ev.TaskName == "wait" {
			found = true
			break
		}
	}
	if !found {
		t.Error("missing task OnCancel hook for task 'wait'")
	}

	t.Logf("captured %d hook events total", len(notifier.Events()))
	for _, ev := range notifier.Events() {
		t.Logf("  scope=%-10s type=%-12s workflow=%s task=%s", ev.Scope, ev.HookType, ev.WorkflowRunID, ev.TaskName)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHooksInvalidTemplate — verifies that Submit rejects a workflow whose
// hook references a non-task template (DAG).
// ─────────────────────────────────────────────────────────────────────────────

func TestHooksInvalidTemplate(t *testing.T) {
	const wfPath = "examples/27-hooks-invalid-template.json"

	raw, err := os.ReadFile(wfPath)
	if err != nil {
		t.Fatalf("read %s: %v", wfPath, err)
	}
	var wf model.Workflow
	if err := json.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", wfPath, err)
	}

	b := newEngineBundle(t, 10)
	defer b.cancel()

	if err := b.eng.Start(b.ctx); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	defer b.eng.Stop()

	_, err = b.eng.Submit(b.ctx, &wf)
	if err == nil {
		t.Fatal("expected Submit to fail for hook referencing DAG template, got nil")
	}
	t.Logf("submit correctly rejected: %v", err)

	// Verify the error mentions the hook or template issue
	errStr := err.Error()
	if !strings.Contains(errStr, "hook") && !strings.Contains(errStr, "template") {
		t.Errorf("error should mention hook/template issue, got: %s", errStr)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestErrorSinkIntegration — verifies that the ErrorSink receives reports
// during normal operation without any critical errors.
// ─────────────────────────────────────────────────────────────────────────────

func TestErrorSinkIntegration(t *testing.T) {
	const wfPath = "examples/02-dag-linear.json"

	raw, err := os.ReadFile(wfPath)
	if err != nil {
		t.Fatalf("read %s: %v", wfPath, err)
	}
	var wf model.Workflow
	if err := json.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", wfPath, err)
	}

	sink := &testErrorSink{}
	b := newEngineBundle(t, 30, aether.WithErrorSink(sink))
	defer b.cancel()

	if err := b.eng.Start(b.ctx); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	defer b.eng.Stop()

	runID, err := b.eng.Submit(b.ctx, &wf)
	if err != nil {
		t.Fatalf("submit workflow: %v", err)
	}

	finalExec := waitTerminal(t, b, runID)
	if finalExec.Status != model.PhaseSucceeded {
		t.Fatalf("expected Succeeded, got %s", finalExec.Status)
	}

	// A successful workflow should have no Error/Critical severity reports
	for _, ec := range sink.Errors() {
		if ec.Severity >= errsink.SeverityError {
			t.Errorf("unexpected error report: operation=%s severity=%d wfRunID=%s taskRunID=%s",
				ec.Operation, ec.Severity, ec.WorkflowRunID, ec.TaskRunID)
		}
	}

	if len(sink.Errors()) > 0 {
		t.Logf("error sink received %d reports (all non-critical):", len(sink.Errors()))
		for _, ec := range sink.Errors() {
			t.Logf("  operation=%-25s severity=%d wfRunID=%s", ec.Operation, ec.Severity, ec.WorkflowRunID)
		}
	}
}
