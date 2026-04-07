// local_broker.go — channel-based TaskBroker for standalone mode.
// Tasks are dispatched via an internal channel and consumed by LocalWorker goroutines.
package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/internal"
)

// LocalBroker routes tasks between the engine and local workers via a channel.
type LocalBroker struct {
	startHandler broker.StartHandler
	handler      broker.CompletionHandler

	mu       sync.Mutex
	closed   bool
	taskCh   chan *broker.TaskAssignment
	taskCtxs map[string]context.Context    // taskRunID → execution context (with timeout)
	cancels  map[string]context.CancelFunc // taskRunID → cancel func

	worker *LocalWorker
}

// NewLocalBroker creates a LocalBroker with the given callbacks.
func NewLocalBroker(startHandler broker.StartHandler, handler broker.CompletionHandler) *LocalBroker {
	return &LocalBroker{
		startHandler: startHandler,
		handler:      handler,
		taskCh:       make(chan *broker.TaskAssignment, 64),
		taskCtxs:     make(map[string]context.Context),
		cancels:      make(map[string]context.CancelFunc),
	}
}

// SetWorker associates the worker that will consume tasks from this broker.
func (b *LocalBroker) SetWorker(w *LocalWorker) {
	b.worker = w
}

func (b *LocalBroker) Dispatch(ctx context.Context, assignment *broker.TaskAssignment) error {
	taskCtx, cancel := context.WithCancel(ctx)
	if assignment.Timeout != "" {
		timeout, err := internal.ParseDuration(assignment.Timeout)
		if err == nil && timeout > 0 {
			cancel() // release the previous WithCancel before replacing
			taskCtx, cancel = context.WithTimeout(ctx, timeout)
		}
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		cancel()
		return fmt.Errorf("broker is closed")
	}
	b.taskCtxs[assignment.TaskRunID] = taskCtx
	b.cancels[assignment.TaskRunID] = cancel
	b.mu.Unlock()

	select {
	case b.taskCh <- assignment:
		return nil
	case <-ctx.Done():
		b.mu.Lock()
		delete(b.taskCtxs, assignment.TaskRunID)
		delete(b.cancels, assignment.TaskRunID)
		b.mu.Unlock()
		cancel()
		return ctx.Err()
	}
}

func (b *LocalBroker) Cancel(_ context.Context, taskRunID string) error {
	b.mu.Lock()
	cancel, ok := b.cancels[taskRunID]
	b.mu.Unlock()
	if ok {
		cancel()
	}
	return nil
}

func (b *LocalBroker) FetchTask(ctx context.Context, _ string) (*broker.TaskAssignment, error) {
	select {
	case assignment, ok := <-b.taskCh:
		if !ok {
			return nil, fmt.Errorf("broker closed")
		}
		return assignment, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TaskContext returns the execution context (with timeout) for a dispatched task.
func (b *LocalBroker) TaskContext(taskRunID string) context.Context {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.taskCtxs[taskRunID]
}

// CleanupTask removes the context and cancel entries for a completed task.
func (b *LocalBroker) CleanupTask(taskRunID string) {
	b.mu.Lock()
	delete(b.taskCtxs, taskRunID)
	delete(b.cancels, taskRunID)
	b.mu.Unlock()
}

func (b *LocalBroker) StartTask(ctx context.Context, taskRunID string, _ string) error {
	if b.startHandler != nil {
		b.startHandler(ctx, taskRunID)
	}
	return nil
}

func (b *LocalBroker) CompleteTask(ctx context.Context, result *broker.TaskResult) error {
	b.CleanupTask(result.TaskRunID)
	if b.handler != nil {
		b.handler(ctx, result)
	}
	return nil
}

func (b *LocalBroker) Close() error {
	b.mu.Lock()
	b.closed = true
	for _, cancel := range b.cancels {
		cancel()
	}
	b.mu.Unlock()

	close(b.taskCh)

	if b.worker != nil {
		b.worker.Stop()
	}
	return nil
}
