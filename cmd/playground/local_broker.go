// local_broker.go — goroutine-based TaskBroker for standalone mode.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/internal"
	"github.com/BabySid/aether/model"
)

// LocalBroker executes tasks in-process using goroutines.
type LocalBroker struct {
	registry     *executor.Registry
	startHandler broker.StartHandler
	handler      broker.CompletionHandler

	mu      sync.Mutex
	cancels map[uint64]context.CancelFunc
	wg      sync.WaitGroup
	closed  bool
}

// NewLocalBroker creates a LocalBroker wired with the given registry and callbacks.
func NewLocalBroker(reg *executor.Registry, startHandler broker.StartHandler, handler broker.CompletionHandler) *LocalBroker {
	return &LocalBroker{
		registry:     reg,
		startHandler: startHandler,
		handler:      handler,
		cancels:      make(map[uint64]context.CancelFunc),
	}
}

func (b *LocalBroker) Dispatch(ctx context.Context, assignment *broker.TaskAssignment) error {
	plugin, ok := b.registry.Get(assignment.ExecutorType)
	if !ok {
		if b.handler != nil {
			b.handler(ctx, &broker.TaskResult{
				TaskRunID: assignment.TaskRunID,
				Phase:     model.PhaseError,
				Message:   fmt.Sprintf("unknown executor type: %s", assignment.ExecutorType),
			})
		}
		return nil
	}

	taskCtx, cancel := context.WithCancel(ctx)
	if assignment.Timeout != "" {
		timeout, err := internal.ParseDuration(assignment.Timeout)
		if err == nil && timeout > 0 {
			taskCtx, cancel = context.WithTimeout(ctx, timeout)
		}
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		cancel()
		return fmt.Errorf("broker is closed")
	}
	b.cancels[assignment.TaskRunID] = cancel
	b.wg.Add(1)
	b.mu.Unlock()

	go func() {
		defer b.wg.Done()
		defer func() {
			b.mu.Lock()
			delete(b.cancels, assignment.TaskRunID)
			b.mu.Unlock()
			cancel()
		}()

		req := &executor.ExecuteRequest{
			TaskRunID: assignment.TaskRunID,
			Config:    assignment.ExecutorConfig,
		}
		if len(assignment.Inputs) > 0 {
			var inputs model.Inputs
			if err := json.Unmarshal(assignment.Inputs, &inputs); err == nil {
				req.Inputs = &inputs
			}
		}
		if len(assignment.Resources) > 0 {
			var resources model.Resources
			if err := json.Unmarshal(assignment.Resources, &resources); err == nil {
				req.Resources = &resources
			}
		}

		if b.startHandler != nil {
			b.startHandler(ctx, assignment.TaskRunID)
		}

		result, err := plugin.Execute(taskCtx, req)

		var taskResult broker.TaskResult
		taskResult.TaskRunID = assignment.TaskRunID

		if err != nil {
			if taskCtx.Err() != nil {
				taskResult.Phase = model.PhaseTimeout
				taskResult.Message = fmt.Sprintf("task timed out: %v", taskCtx.Err())
			} else {
				taskResult.Phase = model.PhaseError
				taskResult.Message = err.Error()
			}
		} else if result != nil {
			taskResult.Phase = result.Phase
			taskResult.Message = result.Msg
			taskResult.Outputs = result.Outputs
			now := time.Now().UTC().Format(time.RFC3339)
			if taskResult.Outputs == nil {
				taskResult.Outputs = &model.Outputs{}
			}
			if taskResult.Outputs.Metrics == nil {
				taskResult.Outputs.Metrics = &model.Metrics{}
			}
			taskResult.Outputs.Metrics.FinishedAt = now
		} else {
			taskResult.Phase = model.PhaseSucceeded
		}

		if b.handler != nil {
			b.handler(ctx, &taskResult)
		}
	}()

	return nil
}

func (b *LocalBroker) Cancel(_ context.Context, taskRunID uint64) error {
	b.mu.Lock()
	cancel, ok := b.cancels[taskRunID]
	b.mu.Unlock()
	if ok {
		cancel()
	}
	return nil
}

func (b *LocalBroker) FetchTask(ctx context.Context, _ string) (*broker.TaskAssignment, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *LocalBroker) Heartbeat(_ context.Context, _ uint64, _ string) error { return nil }

func (b *LocalBroker) StartTask(ctx context.Context, taskRunID uint64, _ string) error {
	if b.startHandler != nil {
		b.startHandler(ctx, taskRunID)
	}
	return nil
}

func (b *LocalBroker) CompleteTask(ctx context.Context, result *broker.TaskResult) error {
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
	b.wg.Wait()
	return nil
}
