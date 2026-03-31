package hook

import (
	"context"
	"errors"
	"testing"
)

type mockNotifier struct {
	events []*Event
	err    error
}

func (m *mockNotifier) Notify(_ context.Context, event *Event) error {
	m.events = append(m.events, event)
	return m.err
}

func TestCompositeNotifier_FansOut(t *testing.T) {
	n1 := &mockNotifier{}
	n2 := &mockNotifier{}
	composite := CompositeNotifier{n1, n2}

	event := &Event{HookType: OnStart, WorkflowRunID: "wf-1", Template: "notify"}
	err := composite.Notify(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(n1.events) != 1 || len(n2.events) != 1 {
		t.Fatalf("expected both notifiers to receive event, got n1=%d n2=%d", len(n1.events), len(n2.events))
	}
}

func TestCompositeNotifier_JoinsErrors(t *testing.T) {
	err1 := errors.New("notifier-1 failed")
	err2 := errors.New("notifier-2 failed")
	n1 := &mockNotifier{err: err1}
	n2 := &mockNotifier{err: err2}
	composite := CompositeNotifier{n1, n2}

	event := &Event{HookType: OnSuccess, Template: "audit"}
	err := composite.Notify(context.Background(), event)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, err1) || !errors.Is(err, err2) {
		t.Fatalf("expected joined errors, got: %v", err)
	}
}

func TestCompositeNotifier_PartialFailure(t *testing.T) {
	failErr := errors.New("failed")
	n1 := &mockNotifier{err: failErr}
	n2 := &mockNotifier{}
	composite := CompositeNotifier{n1, n2}

	event := &Event{HookType: OnFailure}
	err := composite.Notify(context.Background(), event)
	if !errors.Is(err, failErr) {
		t.Fatalf("expected failErr, got: %v", err)
	}
	// n2 should still receive the event despite n1's failure
	if len(n2.events) != 1 {
		t.Fatalf("n2 should receive event even when n1 fails, got %d", len(n2.events))
	}
}

func TestCompositeNotifier_Empty(t *testing.T) {
	composite := CompositeNotifier{}
	err := composite.Notify(context.Background(), &Event{})
	if err != nil {
		t.Fatalf("empty composite should succeed, got: %v", err)
	}
}
