package main

import (
	"context"
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
		func(ctx context.Context, taskRunID string) {
			eng.OnTaskStarted(ctx, taskRunID)
		},
		func(ctx context.Context, result *broker.TaskResult) {
			eng.OnTaskCompleted(ctx, result)
		},
	)
	w := NewLocalWorker("test-worker", brok, reg)
	brok.SetWorker(w)

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
	w.Start(context.Background(), 4)
	t.Cleanup(func() { _ = brok.Close() })
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

func TestCronWorkflow_ErrNotSupported(t *testing.T) {
	// Create an engine WITHOUT CronScheduler.
	reg := executor.NewRegistry()
	_ = reg.Register(newEcho())
	memStore := NewMemoryStore()

	var eng *aether.Engine
	brok := NewLocalBroker(
		func(ctx context.Context, taskRunID string) { eng.OnTaskStarted(ctx, taskRunID) },
		func(ctx context.Context, result *broker.TaskResult) { eng.OnTaskCompleted(ctx, result) },
	)
	w := NewLocalWorker("test-worker", brok, reg)
	brok.SetWorker(w)

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
	w.Start(context.Background(), 4)
	t.Cleanup(func() { _ = brok.Close() })

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
