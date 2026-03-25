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
	ExpectPhase     string          `json:"expectPhase"`
	ExpectTaskCount *int            `json:"expectTaskCount,omitempty"`
	ExpectTasks     []TaskAssertion `json:"expectTasks,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// engine wiring helper
// ─────────────────────────────────────────────────────────────────────────────

func runWorkflow(t *testing.T, wfPath string, timeoutSec int) *aether.WorkflowExecution {
	t.Helper()

	raw, err := os.ReadFile(wfPath)
	if err != nil {
		t.Fatalf("read %s: %v", wfPath, err)
	}
	var wf model.Workflow
	if err := json.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", wfPath, err)
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

	eng, err = aether.New(
		aether.WithStore(memStore),
		aether.WithIDGenerator(NewAtomicIDGen()),
		aether.WithExprEvaluator(NewSimpleEvaluator()),
		aether.WithTaskBroker(brok),
		aether.WithExecutorRegistry(reg),
		aether.WithTimeoutWatcher(newPollingWatcher(memStore, 500*time.Millisecond)),
	)
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	if err := eng.Start(ctx); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	defer eng.Stop()

	runID, err := eng.Submit(ctx, &wf)
	if err != nil {
		t.Fatalf("submit workflow: %v", err)
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	var finalExec *aether.WorkflowExecution
loop:
	for {
		exec, getErr := eng.Get(ctx, runID)
		if getErr != nil {
			t.Fatalf("get workflow: %v", getErr)
		}
		if exec.Phase().IsTerminal() {
			finalExec = exec
			break loop
		}
		select {
		case <-ctx.Done():
			finalExec = exec
			break loop
		case <-finishCh:
			for len(finishCh) > 0 {
				<-finishCh
			}
		case <-ticker.C:
		}
	}

	return finalExec
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
