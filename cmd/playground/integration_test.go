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
	"strings"
	"testing"
	"time"

	aether "github.com/BabySid/aether"
	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
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
// The caller is responsible for calling eng.Start() and eng.Stop().
func newEngineBundle(t *testing.T, timeoutSec int, watchInterval ...time.Duration) *engineBundle {
	t.Helper()

	interval := 500 * time.Millisecond
	if len(watchInterval) > 0 {
		interval = watchInterval[0]
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

	var err error
	eng, err = aether.New(
		aether.WithStore(memStore),
		aether.WithIDGenerator(NewAtomicIDGen()),
		aether.WithExprEvaluator(NewSimpleEvaluator()),
		aether.WithTaskBroker(brok),
		aether.WithExecutorRegistry(reg),
		aether.WithTimeoutWatcher(newPollingWatcher(memStore, interval)),
	)
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
		if exec.Phase().IsTerminal() {
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
// Returns the task's RunID so the caller can Resume it.
func waitTaskPhase(t *testing.T, b *engineBundle, runID string, taskName string, wantPhase model.Phase) *store.TaskRun {
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
			if tr.TaskName == taskName && tr.Status != nil && *tr.Status == wantPhase {
				return tr
			}
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for task %q to reach phase %s", taskName, wantPhase)
			return nil
		case <-b.ctx.Done():
			t.Fatalf("context done before task %q reached phase %s", taskName, wantPhase)
			return nil
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
func taskNames(tasks []*store.TaskRun) []string {
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
	if got := string(exec.Phase()); got != assert.ExpectPhase {
		t.Errorf("workflow phase: got %q, want %q", got, assert.ExpectPhase)
	}

	// 2. task count
	if assert.ExpectTaskCount != nil {
		if got := len(exec.Tasks); got != *assert.ExpectTaskCount {
			t.Errorf("task count: got %d, want %d", got, *assert.ExpectTaskCount)
		}
	}

	// 3. per-task expectations
	// Build index: taskName -> last TaskRun (last wins for retries)
	taskByName := make(map[string]*store.TaskRun)
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
			var gotPhase model.Phase
			if task.Status != nil {
				gotPhase = *task.Status
			}
			if string(gotPhase) != ta.ExpectPhase {
				t.Errorf("task %q phase: got %q, want %q", ta.TaskName, gotPhase, ta.ExpectPhase)
			}
		}

		// 3b. output parameters (subset match)
		if len(ta.ExpectOutputs) > 0 {
			// build output index
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
			t.Logf("workflow=%s phase=%s tasks=%d", name, exec.Phase(), len(exec.Tasks))
			for _, task := range exec.Tasks {
				phase := model.Phase("")
				if task.Status != nil {
					phase = *task.Status
				}
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

	// Step 1: wait for "await-approval" to reach PhaseRunning (suspended).
	suspendedTask := waitTaskPhase(t, b, runID, "await-approval", model.PhaseRunning)
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
	t.Logf("workflow phase=%s tasks=%d", finalExec.Phase(), len(finalExec.Tasks))

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
		phase := model.Phase("")
		if task.Status != nil {
			phase = *task.Status
		}
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

	// Wait for "wait-external" to enter PhaseRunning (suspended).
	_ = waitTaskPhase(t, b, runID, "wait-external", model.PhaseRunning)
	t.Logf("task wait-external suspended; waiting for timeout (~1s)...")

	// Do NOT resume — let the task timeout naturally.
	finalExec := waitTerminal(t, b, runID)
	t.Logf("workflow phase=%s tasks=%d", finalExec.Phase(), len(finalExec.Tasks))

	// The workflow ends in Error because wait-external timed out.
	// continueOn.timeout=true only allows downstream tasks to continue;
	// it does not change the DAG container's own terminal phase.
	taskCount := 4
	assertion := &WorkflowAssertion{
		ExpectPhase:     "Error",
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
		phase := model.Phase("")
		if task.Status != nil {
			phase = *task.Status
		}
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
