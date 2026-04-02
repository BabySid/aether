// polling_watcher.go — default timeout.Watcher implementation for standalone playground.
//
// PollingWatcher periodically scans the Store for TaskRuns and WorkflowRuns
// whose Deadline has passed, and emits TimeoutEvents to the Engine.
//
// This is an intentionally simple, single-process implementation.
// Production deployments should provide a custom timeout.Watcher via
// aether.WithTimeoutWatcher() — for example one backed by a distributed
// scheduler (Redis ZSET, DB cron with leader election, etc.).
package main

import (
	"context"
	"log"
	"time"

	"github.com/BabySid/aether/timeout"
)

// PollingWatcher scans the MemoryStore at a fixed interval and emits
// TimeoutEvents for any entity whose Deadline has elapsed.
type PollingWatcher struct {
	store    *MemoryStore
	interval time.Duration
	events   chan timeout.TimeoutEvent
	started  bool
	cancel   context.CancelFunc
}

// newPollingWatcher returns a PollingWatcher backed by the given MemoryStore.
// interval controls how often the store is scanned; a smaller value gives
// tighter timeout accuracy at the cost of more CPU.
func newPollingWatcher(s *MemoryStore, interval time.Duration) *PollingWatcher {
	return &PollingWatcher{
		store:    s,
		interval: interval,
		events:   make(chan timeout.TimeoutEvent, 256),
	}
}

// Start implements timeout.Watcher.
func (w *PollingWatcher) Start(ctx context.Context) error {
	if w.started {
		return nil
	}
	w.started = true
	innerCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	go w.loop(innerCtx)
	return nil
}

// Stop implements timeout.Watcher.
func (w *PollingWatcher) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

// Events implements timeout.Watcher.
func (w *PollingWatcher) Events() <-chan timeout.TimeoutEvent {
	return w.events
}

// loop runs until ctx is cancelled.
func (w *PollingWatcher) loop(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.scan(ctx)
		}
	}
}

// scan performs one pass over all active entities and emits events for expired ones.
func (w *PollingWatcher) scan(ctx context.Context) {
	now := time.Now()

	// Workflow-level timeouts
	wfRuns, err := w.store.ListActiveWorkflowRuns(ctx)
	if err != nil {
		log.Printf("[PollingWatcher] list active workflow runs: %v", err)
	} else {
		for _, wr := range wfRuns {
			if wr.Deadline != nil && now.After(*wr.Deadline) {
				w.emit(timeout.TimeoutEvent{Kind: timeout.KindWorkflow, RunID: wr.RunID})
			}
		}
	}

	// Task-level timeouts
	taskRuns, err := w.store.ListActiveTaskRuns(ctx)
	if err != nil {
		log.Printf("[PollingWatcher] list running task runs: %v", err)
	} else {
		for _, tr := range taskRuns {
			if tr.Deadline != nil && now.After(*tr.Deadline) {
				w.emit(timeout.TimeoutEvent{Kind: timeout.KindTask, RunID: tr.RunID})
			}
		}
	}
}

// emit sends ev to the events channel without blocking.
// Drops the event if the buffer is full (consumer too slow).
func (w *PollingWatcher) emit(ev timeout.TimeoutEvent) {
	select {
	case w.events <- ev:
	default:
		log.Printf("[PollingWatcher] event buffer full, dropping %+v", ev)
	}
}
