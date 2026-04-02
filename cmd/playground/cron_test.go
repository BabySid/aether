package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	aether "github.com/BabySid/aether"
	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/model"
)

// testScheduler is a minimal cron.Scheduler for testing.
// It stores callbacks and provides a Fire method to trigger them manually.
type testScheduler struct {
	mu        sync.Mutex
	callbacks map[string]func()
}

func newTestScheduler() *testScheduler {
	return &testScheduler{callbacks: make(map[string]func())}
}

func (s *testScheduler) Add(id, schedule, timezone string, callback func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callbacks[id] = callback
	return nil
}

func (s *testScheduler) Remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.callbacks, id)
}

// Fire manually triggers the callback for the given cronID.
func (s *testScheduler) Fire(id string) {
	s.mu.Lock()
	cb := s.callbacks[id]
	s.mu.Unlock()
	if cb != nil {
		cb()
	}
}

// Has returns whether a callback is registered for the given cronID.
func (s *testScheduler) Has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.callbacks[id]
	return ok
}

func (s *testScheduler) Start(_ context.Context) error { return nil }
func (s *testScheduler) Stop()                         {}

// newCronEngineBundle creates an engine with a testScheduler for CronWorkflow testing.
func newCronEngineBundle(t *testing.T) (*aether.Engine, *testScheduler, *MemoryStore) {
	t.Helper()

	reg := executor.NewRegistry()
	if err := reg.Register(newEcho()); err != nil {
		t.Fatalf("register echo: %v", err)
	}

	memStore := NewMemoryStore()
	sched := newTestScheduler()

	var eng *aether.Engine
	brok := NewLocalBroker(
		reg,
		func(ctx context.Context, taskRunID string) {
			eng.OnTaskStarted(ctx, taskRunID)
		},
		func(ctx context.Context, result *broker.TaskResult) {
			eng.OnTaskCompleted(ctx, result)
		},
	)

	var err error
	eng, err = aether.New(
		aether.WithStore(memStore),
		aether.WithIDGenerator(NewAtomicIDGen()),
		aether.WithExprEvaluator(NewSimpleEvaluator()),
		aether.WithTaskBroker(brok),
		aether.WithExecutorRegistry(reg),
		aether.WithTimeoutWatcher(newPollingWatcher(memStore, 500*time.Millisecond)),
		aether.WithCronScheduler(sched),
	)
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}
	return eng, sched, memStore
}

func validTestCronWorkflow() *model.CronWorkflow {
	return &model.CronWorkflow{
		APIVersion: "aether/v1",
		Kind:       "CronWorkflow",
		Metadata:   model.Metadata{Name: "test-cron"},
		Spec: model.CronWorkflowSpec{
			Schedule: "0 * * * *",
			WorkflowSpec: model.WorkflowSpec{
				Entrypoint: "main",
				Templates: []model.Template{
					{
						Task: &model.Task{
							Name:     "main",
							Executor: &model.Executor{Type: "echo"},
						},
					},
				},
			},
		},
	}
}

func TestCronWorkflow_SubmitAndGet(t *testing.T) {
	eng, _, _ := newCronEngineBundle(t)
	ctx := context.Background()

	cw := validTestCronWorkflow()
	cronID, err := eng.SubmitCronWorkflow(ctx, cw)
	if err != nil {
		t.Fatalf("SubmitCronWorkflow: %v", err)
	}
	if cronID == "" {
		t.Fatal("expected non-empty cronID")
	}

	exec, err := eng.GetCronWorkflow(ctx, cronID)
	if err != nil {
		t.Fatalf("GetCronWorkflow: %v", err)
	}
	if exec.ID != cronID {
		t.Errorf("ID = %q, want %q", exec.ID, cronID)
	}
	if len(exec.Runs) != 0 {
		t.Errorf("expected 0 runs before trigger, got %d", len(exec.Runs))
	}
}

func TestCronWorkflow_TriggerCreatesRun(t *testing.T) {
	eng, sched, _ := newCronEngineBundle(t)
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer eng.Stop()

	cw := validTestCronWorkflow()
	cronID, err := eng.SubmitCronWorkflow(ctx, cw)
	if err != nil {
		t.Fatalf("SubmitCronWorkflow: %v", err)
	}

	// Manually fire the cron trigger.
	sched.Fire(cronID)

	// Wait briefly for the workflow to be submitted and complete.
	time.Sleep(500 * time.Millisecond)

	exec, err := eng.GetCronWorkflow(ctx, cronID)
	if err != nil {
		t.Fatalf("GetCronWorkflow: %v", err)
	}
	if len(exec.Runs) != 1 {
		t.Fatalf("expected 1 run after trigger, got %d", len(exec.Runs))
	}
}

func TestCronWorkflow_Delete(t *testing.T) {
	eng, sched, _ := newCronEngineBundle(t)
	ctx := context.Background()

	cw := validTestCronWorkflow()
	cronID, err := eng.SubmitCronWorkflow(ctx, cw)
	if err != nil {
		t.Fatalf("SubmitCronWorkflow: %v", err)
	}

	if !sched.Has(cronID) {
		t.Fatal("expected scheduler to have cronID registered")
	}

	if err := eng.DeleteCronWorkflow(ctx, cronID); err != nil {
		t.Fatalf("DeleteCronWorkflow: %v", err)
	}

	if sched.Has(cronID) {
		t.Fatal("expected scheduler to NOT have cronID after delete")
	}

	_, err = eng.GetCronWorkflow(ctx, cronID)
	if err == nil {
		t.Fatal("expected error getting deleted CronWorkflow")
	}
}

func TestCronWorkflow_UpdateMutableFields(t *testing.T) {
	eng, sched, _ := newCronEngineBundle(t)
	ctx := context.Background()

	cw := validTestCronWorkflow()
	cronID, err := eng.SubmitCronWorkflow(ctx, cw)
	if err != nil {
		t.Fatalf("SubmitCronWorkflow: %v", err)
	}

	// Update schedule and timezone.
	updated := validTestCronWorkflow()
	updated.Spec.Schedule = "*/5 * * * *"
	updated.Spec.Timezone = "Asia/Shanghai"

	if err := eng.UpdateCronWorkflow(ctx, cronID, updated); err != nil {
		t.Fatalf("UpdateCronWorkflow: %v", err)
	}

	// The scheduler should still have the entry (re-registered).
	if !sched.Has(cronID) {
		t.Fatal("expected scheduler to still have cronID after update")
	}
}

func TestCronWorkflow_UpdateWorkflowSpecImmutable(t *testing.T) {
	eng, _, _ := newCronEngineBundle(t)
	ctx := context.Background()

	cw := validTestCronWorkflow()
	cronID, err := eng.SubmitCronWorkflow(ctx, cw)
	if err != nil {
		t.Fatalf("SubmitCronWorkflow: %v", err)
	}

	// Try to change WorkflowSpec.
	updated := validTestCronWorkflow()
	updated.Spec.WorkflowSpec.Entrypoint = "different"
	updated.Spec.WorkflowSpec.Templates = append(updated.Spec.WorkflowSpec.Templates, model.Template{
		Task: &model.Task{
			Name:     "different",
			Executor: &model.Executor{Type: "echo"},
		},
	})

	err = eng.UpdateCronWorkflow(ctx, cronID, updated)
	if err == nil {
		t.Fatal("expected error when changing WorkflowSpec")
	}
}

func TestCronWorkflow_SuspendSkipsTrigger(t *testing.T) {
	eng, sched, _ := newCronEngineBundle(t)
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer eng.Stop()

	cw := validTestCronWorkflow()
	cw.Spec.Suspend = true
	cronID, err := eng.SubmitCronWorkflow(ctx, cw)
	if err != nil {
		t.Fatalf("SubmitCronWorkflow: %v", err)
	}

	// Suspended CronWorkflow should not be registered with the scheduler.
	if sched.Has(cronID) {
		t.Fatal("suspended CronWorkflow should not be in scheduler")
	}
}

func TestCronWorkflow_ConcurrencyForbid(t *testing.T) {
	eng, sched, _ := newCronEngineBundle(t)
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer eng.Stop()

	// Use a workflow that suspends (so it stays active).
	cw := &model.CronWorkflow{
		APIVersion: "aether/v1",
		Kind:       "CronWorkflow",
		Metadata:   model.Metadata{Name: "forbid-cron"},
		Spec: model.CronWorkflowSpec{
			Schedule:          "0 * * * *",
			ConcurrencyPolicy: model.ConcurrencyForbid,
			WorkflowSpec: model.WorkflowSpec{
				Entrypoint: "main",
				Templates: []model.Template{
					{
						Task: &model.Task{
							Name:     "main",
							Executor: &model.Executor{Type: "echo"},
							Inputs: &model.Inputs{
								Parameters: []model.Parameter{
									{Name: "suspend", Value: json.RawMessage(`true`)},
								},
							},
						},
					},
				},
			},
		},
	}

	cronID, err := eng.SubmitCronWorkflow(ctx, cw)
	if err != nil {
		t.Fatalf("SubmitCronWorkflow: %v", err)
	}

	// First trigger: should create a run.
	sched.Fire(cronID)
	time.Sleep(300 * time.Millisecond)

	exec, _ := eng.GetCronWorkflow(ctx, cronID)
	if len(exec.Runs) != 1 {
		t.Fatalf("expected 1 run after first trigger, got %d", len(exec.Runs))
	}

	// Second trigger with Forbid: should NOT create another run because first is still active.
	sched.Fire(cronID)
	time.Sleep(300 * time.Millisecond)

	exec, _ = eng.GetCronWorkflow(ctx, cronID)
	if len(exec.Runs) != 1 {
		t.Fatalf("expected still 1 run with Forbid policy, got %d", len(exec.Runs))
	}
}

func TestCronWorkflow_ConcurrencyReplace(t *testing.T) {
	eng, sched, _ := newCronEngineBundle(t)
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer eng.Stop()

	// Use a workflow that suspends (so it stays active).
	cw := &model.CronWorkflow{
		APIVersion: "aether/v1",
		Kind:       "CronWorkflow",
		Metadata:   model.Metadata{Name: "replace-cron"},
		Spec: model.CronWorkflowSpec{
			Schedule:          "0 * * * *",
			ConcurrencyPolicy: model.ConcurrencyReplace,
			WorkflowSpec: model.WorkflowSpec{
				Entrypoint: "main",
				Templates: []model.Template{
					{
						Task: &model.Task{
							Name:     "main",
							Executor: &model.Executor{Type: "echo"},
							Inputs: &model.Inputs{
								Parameters: []model.Parameter{
									{Name: "suspend", Value: json.RawMessage(`true`)},
								},
							},
						},
					},
				},
			},
		},
	}

	cronID, err := eng.SubmitCronWorkflow(ctx, cw)
	if err != nil {
		t.Fatalf("SubmitCronWorkflow: %v", err)
	}

	// First trigger.
	sched.Fire(cronID)
	time.Sleep(300 * time.Millisecond)

	exec, _ := eng.GetCronWorkflow(ctx, cronID)
	if len(exec.Runs) != 1 {
		t.Fatalf("expected 1 run after first trigger, got %d", len(exec.Runs))
	}

	// Second trigger with Replace: should cancel the first and create a new one.
	sched.Fire(cronID)
	time.Sleep(300 * time.Millisecond)

	exec, _ = eng.GetCronWorkflow(ctx, cronID)
	if len(exec.Runs) != 2 {
		t.Fatalf("expected 2 runs with Replace policy, got %d", len(exec.Runs))
	}

	// The first run should be cancelled.
	cancelled := 0
	for _, run := range exec.Runs {
		if run.Status == model.PhaseCancelled {
			cancelled++
		}
	}
	if cancelled < 1 {
		t.Errorf("expected at least 1 cancelled run with Replace policy, got %d", cancelled)
	}
}

func TestCronWorkflow_ErrNotSupported(t *testing.T) {
	// Create an engine WITHOUT CronScheduler.
	reg := executor.NewRegistry()
	_ = reg.Register(newEcho())
	memStore := NewMemoryStore()

	var eng *aether.Engine
	brok := NewLocalBroker(
		reg,
		func(ctx context.Context, taskRunID string) { eng.OnTaskStarted(ctx, taskRunID) },
		func(ctx context.Context, result *broker.TaskResult) { eng.OnTaskCompleted(ctx, result) },
	)

	var err error
	eng, err = aether.New(
		aether.WithStore(memStore),
		aether.WithIDGenerator(NewAtomicIDGen()),
		aether.WithTaskBroker(brok),
		aether.WithExecutorRegistry(reg),
		aether.WithTimeoutWatcher(newPollingWatcher(memStore, 500*time.Millisecond)),
	)
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}

	ctx := context.Background()
	cw := validTestCronWorkflow()

	_, err = eng.SubmitCronWorkflow(ctx, cw)
	if err == nil {
		t.Fatal("expected ErrNotSupported")
	}

	_, err = eng.GetCronWorkflow(ctx, "any")
	if err == nil {
		t.Fatal("expected ErrNotSupported")
	}

	err = eng.UpdateCronWorkflow(ctx, "any", cw)
	if err == nil {
		t.Fatal("expected ErrNotSupported")
	}

	err = eng.DeleteCronWorkflow(ctx, "any")
	if err == nil {
		t.Fatal("expected ErrNotSupported")
	}
}

func TestCronWorkflow_NilCronWorkflow(t *testing.T) {
	eng, _, _ := newCronEngineBundle(t)
	ctx := context.Background()

	_, err := eng.SubmitCronWorkflow(ctx, nil)
	if err == nil {
		t.Fatal("expected error for nil CronWorkflow")
	}
}

func TestCronWorkflow_ValidationFailure(t *testing.T) {
	eng, _, _ := newCronEngineBundle(t)
	ctx := context.Background()

	cw := validTestCronWorkflow()
	cw.Spec.Schedule = "" // invalid
	_, err := eng.SubmitCronWorkflow(ctx, cw)
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestCronWorkflow_StartAtFuture(t *testing.T) {
	eng, sched, _ := newCronEngineBundle(t)
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer eng.Stop()

	cw := validTestCronWorkflow()
	cw.Spec.StartAt = time.Now().Add(1 * time.Hour).Format(time.RFC3339) // future
	cronID, err := eng.SubmitCronWorkflow(ctx, cw)
	if err != nil {
		t.Fatalf("SubmitCronWorkflow: %v", err)
	}

	// Trigger should be skipped since now < StartAt.
	sched.Fire(cronID)
	time.Sleep(200 * time.Millisecond)

	exec, _ := eng.GetCronWorkflow(ctx, cronID)
	if len(exec.Runs) != 0 {
		t.Fatalf("expected 0 runs (StartAt in future), got %d", len(exec.Runs))
	}
}

func TestCronWorkflow_EndAtPast(t *testing.T) {
	eng, sched, _ := newCronEngineBundle(t)
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer eng.Stop()

	cw := validTestCronWorkflow()
	cw.Spec.EndAt = time.Now().Add(-1 * time.Hour).Format(time.RFC3339) // past
	cronID, err := eng.SubmitCronWorkflow(ctx, cw)
	if err != nil {
		t.Fatalf("SubmitCronWorkflow: %v", err)
	}

	// Trigger should be skipped since now > EndAt, and entry removed from scheduler.
	sched.Fire(cronID)
	time.Sleep(200 * time.Millisecond)

	exec, _ := eng.GetCronWorkflow(ctx, cronID)
	if len(exec.Runs) != 0 {
		t.Fatalf("expected 0 runs (EndAt in past), got %d", len(exec.Runs))
	}

	if sched.Has(cronID) {
		t.Fatal("expected scheduler entry to be removed after EndAt exceeded")
	}
}

func TestCronWorkflow_UpdateSuspend(t *testing.T) {
	eng, sched, _ := newCronEngineBundle(t)
	ctx := context.Background()

	cw := validTestCronWorkflow()
	cronID, err := eng.SubmitCronWorkflow(ctx, cw)
	if err != nil {
		t.Fatalf("SubmitCronWorkflow: %v", err)
	}

	if !sched.Has(cronID) {
		t.Fatal("expected scheduler to have cronID")
	}

	// Suspend via update.
	updated := validTestCronWorkflow()
	updated.Spec.Suspend = true
	if err := eng.UpdateCronWorkflow(ctx, cronID, updated); err != nil {
		t.Fatalf("UpdateCronWorkflow: %v", err)
	}

	if sched.Has(cronID) {
		t.Fatal("expected scheduler to NOT have cronID after suspend")
	}
}

func TestCronWorkflow_HistoryCleanup(t *testing.T) {
	eng, sched, memStore := newCronEngineBundle(t)
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer eng.Stop()

	cw := validTestCronWorkflow()
	cw.Spec.SuccessfulJobsHistoryLimit = 2
	cw.Spec.FailedJobsHistoryLimit = 1
	cronID, err := eng.SubmitCronWorkflow(ctx, cw)
	if err != nil {
		t.Fatalf("SubmitCronWorkflow: %v", err)
	}

	// Fire 5 times — all succeed. Cleanup runs at trigger time against already-completed runs.
	// After the 5th trigger: runs 1-4 are completed, run 5 is in-flight.
	// Cleanup sees 4 succeeded, keeps newest 2, deletes 2 → total = 2 completed + 1 in-flight = 3.
	// We wait for run 5 to complete, so final count is 3 (2 kept + run 5).
	for range 5 {
		sched.Fire(cronID)
		time.Sleep(300 * time.Millisecond)
	}

	runs, err := memStore.ListWorkflowRunsByCronID(ctx, cronID)
	if err != nil {
		t.Fatalf("ListWorkflowRunsByCronID: %v", err)
	}

	if len(runs) != 3 {
		t.Fatalf("expected 3 runs after cleanup (limit=2, plus last in-flight), got %d", len(runs))
	}

	// Verify that deleted WorkflowRuns' TaskRuns are also cascade-deleted.
	// The surviving runs should still have their TaskRuns.
	for _, r := range runs {
		trs, err := memStore.ListTaskRuns(ctx, r.RunID)
		if err != nil {
			t.Fatalf("ListTaskRuns(%s): %v", r.RunID, err)
		}
		if len(trs) == 0 {
			t.Errorf("expected TaskRuns for surviving WorkflowRun %s, got 0", r.RunID)
		}
	}
}

func TestCronWorkflow_MultipleTriggers(t *testing.T) {
	eng, sched, _ := newCronEngineBundle(t)
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer eng.Stop()

	cw := validTestCronWorkflow()
	cronID, err := eng.SubmitCronWorkflow(ctx, cw)
	if err != nil {
		t.Fatalf("SubmitCronWorkflow: %v", err)
	}

	// Fire 3 times (default concurrency=Allow).
	for range 3 {
		sched.Fire(cronID)
		time.Sleep(300 * time.Millisecond)
	}

	exec, _ := eng.GetCronWorkflow(ctx, cronID)
	if len(exec.Runs) != 3 {
		t.Fatalf("expected 3 runs with Allow policy, got %d", len(exec.Runs))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestCronIntegration — data-driven integration test for CronWorkflow examples.
//
// For every CronWorkflow JSON in examples/ (kind == "CronWorkflow"), loads the
// corresponding assertion file and runs the test:
//  1. Submit the CronWorkflow.
//  2. Fire the trigger cronTriggerCount times.
//  3. Assert run count, run phase, cancelled count, and per-task expectations.
// ─────────────────────────────────────────────────────────────────────────────

func TestCronIntegration(t *testing.T) {
	entries, err := filepath.Glob("examples/*.json")
	if err != nil || len(entries) == 0 {
		t.Fatal("no example files found in examples/")
	}

	for _, cwPath := range entries {
		cwPath := cwPath
		name := strings.TrimSuffix(filepath.Base(cwPath), ".json")

		// Only process CronWorkflow files.
		raw, err := os.ReadFile(cwPath)
		if err != nil {
			t.Fatalf("read %s: %v", cwPath, err)
		}

		var peek struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(raw, &peek); err != nil {
			continue
		}
		if peek.Kind != "CronWorkflow" {
			continue
		}

		assertPath := filepath.Join("examples", "assertions", name+".json")
		t.Run(name, func(t *testing.T) {
			// Load assertion.
			assertRaw, err := os.ReadFile(assertPath)
			if err != nil {
				t.Fatalf("assertion file %s not found: %v", assertPath, err)
			}
			var assert WorkflowAssertion
			if err := json.Unmarshal(assertRaw, &assert); err != nil {
				t.Fatalf("parse assertion %s: %v", assertPath, err)
			}

			// Parse CronWorkflow.
			var cw model.CronWorkflow
			if err := json.Unmarshal(raw, &cw); err != nil {
				t.Fatalf("parse CronWorkflow %s: %v", cwPath, err)
			}

			eng, sched, _ := newCronEngineBundle(t)
			ctx := context.Background()
			if err := eng.Start(ctx); err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer eng.Stop()

			cronID, err := eng.SubmitCronWorkflow(ctx, &cw)
			if err != nil {
				t.Fatalf("SubmitCronWorkflow: %v", err)
			}
			t.Logf("submitted cronID=%s", cronID)

			// Fire triggers.
			triggerCount := assert.CronTriggerCount
			if triggerCount <= 0 {
				triggerCount = 1
			}
			for range triggerCount {
				sched.Fire(cronID)
				time.Sleep(300 * time.Millisecond)
			}

			// Fetch execution state.
			exec, err := eng.GetCronWorkflow(ctx, cronID)
			if err != nil {
				t.Fatalf("GetCronWorkflow: %v", err)
			}

			// Assert run count.
			if assert.ExpectRunCount != nil {
				if len(exec.Runs) != *assert.ExpectRunCount {
					t.Errorf("run count: got %d, want %d", len(exec.Runs), *assert.ExpectRunCount)
				}
			}

			// Assert last run phase.
			if assert.ExpectRunPhase != "" && len(exec.Runs) > 0 {
				lastRun := exec.Runs[len(exec.Runs)-1]
				if string(lastRun.Status) != assert.ExpectRunPhase {
					t.Errorf("last run phase: got %q, want %q", lastRun.Status, assert.ExpectRunPhase)
				}
			}

			// Assert cancelled count.
			if assert.ExpectCancelledCount != nil {
				cancelled := 0
				for _, run := range exec.Runs {
					if run.Status == model.PhaseCancelled {
						cancelled++
					}
				}
				if cancelled != *assert.ExpectCancelledCount {
					t.Errorf("cancelled count: got %d, want %d", cancelled, *assert.ExpectCancelledCount)
				}
			}

			// Assert per-task expectations on the last run (if any).
			if len(assert.ExpectTasks) > 0 && len(exec.Runs) > 0 {
				lastRun := exec.Runs[len(exec.Runs)-1]
				wfAssertion := &WorkflowAssertion{
					ExpectPhase: assert.ExpectRunPhase,
					ExpectTasks: assert.ExpectTasks,
				}
				assertExecution(t, &aether.WorkflowExecution{
					RunID:  lastRun.RunID,
					Status: lastRun.Status,
					Tasks:  lastRun.Tasks,
				}, wfAssertion)
			}

			// Log summary.
			t.Logf("cron=%s triggers=%d runs=%d", name, triggerCount, len(exec.Runs))
			for _, run := range exec.Runs {
				t.Logf("  run=%s phase=%s tasks=%d", run.RunID, run.Status, len(run.Tasks))
				for _, task := range run.Tasks {
					var outParts []string
					if task.Outputs != nil {
						for _, p := range task.Outputs.Parameters {
							outParts = append(outParts, fmt.Sprintf("%s=%s", p.Name, string(p.Value)))
						}
					}
					t.Logf("    task=%-20s phase=%-12s outputs=[%s]",
						task.TaskName, task.Status, strings.Join(outParts, ", "))
				}
			}
		})
	}
}
