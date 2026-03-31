package internal

import (
	"context"
	"testing"

	"github.com/BabySid/aether/errsink"
	"github.com/BabySid/aether/hook"
	"github.com/BabySid/aether/model"
)

type captureNotifier struct {
	events []*hook.Event
	err    error
}

func (c *captureNotifier) Notify(_ context.Context, event *hook.Event) error {
	c.events = append(c.events, event)
	return c.err
}

type captureSink struct {
	errors []errsink.ErrorContext
}

func (c *captureSink) OnError(_ context.Context, _ error, ec errsink.ErrorContext) {
	c.errors = append(c.errors, ec)
}

func TestNotifyHook_NilNotifier(t *testing.T) {
	// Should not panic
	NotifyHook(context.Background(), nil, nil, &hook.Event{})
}

func TestNotifyHook_NilEvent(t *testing.T) {
	n := &captureNotifier{}
	NotifyHook(context.Background(), n, nil, nil)
	if len(n.events) != 0 {
		t.Fatal("should not call Notify with nil event")
	}
}

func TestNotifyHook_ErrorReportedToSink(t *testing.T) {
	n := &captureNotifier{err: context.DeadlineExceeded}
	sink := &captureSink{}
	event := &hook.Event{
		HookType:      hook.OnSuccess,
		WorkflowRunID: "wf-1",
		TaskRunID:     "tr-1",
	}
	NotifyHook(context.Background(), n, sink, event)
	if len(sink.errors) != 1 {
		t.Fatalf("expected 1 error reported, got %d", len(sink.errors))
	}
	ec := sink.errors[0]
	if ec.WorkflowRunID != "wf-1" || ec.TaskRunID != "tr-1" {
		t.Fatalf("unexpected error context: %+v", ec)
	}
	if ec.Severity != errsink.SeverityWarning {
		t.Fatalf("expected SeverityWarning, got %d", ec.Severity)
	}
}

func TestNotifyHook_ErrorWithNilSink(t *testing.T) {
	n := &captureNotifier{err: context.DeadlineExceeded}
	// Should not panic when sink is nil
	NotifyHook(context.Background(), n, nil, &hook.Event{HookType: hook.OnError})
}

func TestFireWorkflowHooks_AllPhases(t *testing.T) {
	tests := []struct {
		phase    model.Phase
		hookType hook.Type
	}{
		{model.PhaseRunning, hook.OnStart},
		{model.PhaseSucceeded, hook.OnSuccess},
		{model.PhaseFailed, hook.OnFailure},
		{model.PhaseError, hook.OnError},
		{model.PhaseTimeout, hook.OnError},
		{model.PhaseCancelled, hook.OnCancel},
	}

	for _, tt := range tests {
		t.Run(string(tt.phase), func(t *testing.T) {
			n := &captureNotifier{}
			wf := &model.Workflow{
				Metadata: model.Metadata{Name: "test-wf"},
				Spec: model.WorkflowSpec{
					Hooks: &model.Hooks{
						OnStart:   &model.Hook{Template: "on-start"},
						OnSuccess: &model.Hook{Template: "on-success"},
						OnFailure: &model.Hook{Template: "on-failure"},
						OnError:   &model.Hook{Template: "on-error"},
						OnCancel:  &model.Hook{Template: "on-cancel"},
						OnExit:    &model.Hook{Template: "on-exit"},
					},
				},
			}
			FireWorkflowHooks(context.Background(), n, nil, wf, "wf-run-1", tt.phase)

			// Should fire the phase-specific hook
			found := false
			for _, ev := range n.events {
				if ev.HookType == tt.hookType {
					found = true
					if ev.Scope != hook.ScopeWorkflow {
						t.Errorf("expected ScopeWorkflow, got %s", ev.Scope)
					}
					if ev.WorkflowRunID != "wf-run-1" {
						t.Errorf("expected wf-run-1, got %s", ev.WorkflowRunID)
					}
				}
			}
			if !found {
				t.Errorf("expected hook %s for phase %s", tt.hookType, tt.phase)
			}

			// Terminal phases should also fire OnExit
			if tt.phase.IsTerminal() {
				exitFound := false
				for _, ev := range n.events {
					if ev.HookType == hook.OnExit {
						exitFound = true
					}
				}
				if !exitFound {
					t.Errorf("expected OnExit for terminal phase %s", tt.phase)
				}
			}
		})
	}
}

func TestFireWorkflowHooks_NilHooks(t *testing.T) {
	n := &captureNotifier{}
	wf := &model.Workflow{Spec: model.WorkflowSpec{}}
	FireWorkflowHooks(context.Background(), n, nil, wf, "wf-1", model.PhaseSucceeded)
	if len(n.events) != 0 {
		t.Fatal("should not fire hooks when Hooks is nil")
	}
}

func TestFireTaskHooks_AllPhases(t *testing.T) {
	tests := []struct {
		phase    model.Phase
		hookType hook.Type
	}{
		{model.PhaseRunning, hook.OnStart},
		{model.PhaseSucceeded, hook.OnSuccess},
		{model.PhaseFailed, hook.OnFailure},
		{model.PhaseError, hook.OnError},
		{model.PhaseTimeout, hook.OnError},
		{model.PhaseSuspended, hook.OnSuspend},
		{model.PhaseCancelled, hook.OnCancel},
	}

	for _, tt := range tests {
		t.Run(string(tt.phase), func(t *testing.T) {
			n := &captureNotifier{}
			task := &model.Task{
				Name: "my-task",
				Hooks: &model.Hooks{
					OnStart:   &model.Hook{Template: "on-start"},
					OnSuccess: &model.Hook{Template: "on-success"},
					OnFailure: &model.Hook{Template: "on-failure"},
					OnError:   &model.Hook{Template: "on-error"},
					OnSuspend: &model.Hook{Template: "on-suspend"},
					OnCancel:  &model.Hook{Template: "on-cancel"},
					OnExit:    &model.Hook{Template: "on-exit"},
				},
			}
			FireTaskHooks(context.Background(), n, nil, task, "wf-1", "tr-1", tt.phase)

			found := false
			for _, ev := range n.events {
				if ev.HookType == tt.hookType {
					found = true
					if ev.Scope != hook.ScopeTask {
						t.Errorf("expected ScopeTask, got %s", ev.Scope)
					}
					if ev.TaskRunID != "tr-1" || ev.TaskName != "my-task" {
						t.Errorf("unexpected event: %+v", ev)
					}
				}
			}
			if !found {
				t.Errorf("expected hook %s for phase %s", tt.hookType, tt.phase)
			}
		})
	}
}

func TestFireTaskHooks_NilTask(t *testing.T) {
	n := &captureNotifier{}
	FireTaskHooks(context.Background(), n, nil, nil, "wf-1", "tr-1", model.PhaseSucceeded)
	if len(n.events) != 0 {
		t.Fatal("should not fire hooks for nil task")
	}
}

func TestFireResumeHook(t *testing.T) {
	n := &captureNotifier{}
	task := &model.Task{
		Name: "approval",
		Hooks: &model.Hooks{
			OnResume: &model.Hook{Template: "notify-requester"},
		},
	}
	FireResumeHook(context.Background(), n, nil, task, "wf-1", "tr-1")
	if len(n.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(n.events))
	}
	ev := n.events[0]
	if ev.HookType != hook.OnResume {
		t.Errorf("expected OnResume, got %s", ev.HookType)
	}
	if ev.Template != "notify-requester" {
		t.Errorf("expected template notify-requester, got %s", ev.Template)
	}
}

func TestFireResumeHook_NoResumeHook(t *testing.T) {
	n := &captureNotifier{}
	task := &model.Task{
		Name:  "task",
		Hooks: &model.Hooks{OnSuccess: &model.Hook{Template: "x"}},
	}
	FireResumeHook(context.Background(), n, nil, task, "wf-1", "tr-1")
	if len(n.events) != 0 {
		t.Fatal("should not fire when OnResume is nil")
	}
}
