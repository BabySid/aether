// Package local provides a goroutine-based TaskBroker for standalone mode.
//
// Tasks are executed in-process using goroutines and the executor.Registry.
// When a task finishes, the registered CompletionHandler is invoked directly.
//
// Worker-side methods (FetchTask, Heartbeat) are no-ops in local mode since
// there are no remote workers — the broker itself acts as the worker.
package local

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

// Broker executes tasks in-process using goroutines.
type Broker struct {
	registry     *executor.Registry
	startHandler broker.StartHandler
	handler      broker.CompletionHandler

	mu      sync.Mutex
	cancels map[uint64]context.CancelFunc // taskRunID → cancel
	wg      sync.WaitGroup
	closed  bool
}

// New creates a local Broker.
//
// reg is the executor registry for looking up executor plugins.
// startHandler is called immediately before execution begins — typically engine.OnTaskStarted
// captured via a closure. It signals the engine to transition ancestor containers to Running.
// handler is called when a task completes — typically engine.OnTaskCompleted
// captured via a closure.
func New(reg *executor.Registry, startHandler broker.StartHandler, handler broker.CompletionHandler) *Broker {
	return &Broker{
		registry:     reg,
		startHandler: startHandler,
		handler:      handler,
		cancels:      make(map[uint64]context.CancelFunc),
	}
}

// Dispatch starts a goroutine to execute the task.
// Applies timeout from the assignment and retry from template config.
// On completion, the registered CompletionHandler is invoked.
func (b *Broker) Dispatch(ctx context.Context, assignment *broker.TaskAssignment) error {
	plugin, ok := b.registry.Get(assignment.ExecutorType)
	if !ok {
		// Executor type not found — report error immediately via handler
		if b.handler != nil {
			b.handler(ctx, &broker.TaskResult{
				TaskRunID: assignment.TaskRunID,
				Phase:     model.PhaseError,
				Message:   fmt.Sprintf("unknown executor type: %s", assignment.ExecutorType),
			})
		}
		return nil
	}

	// Create a cancellable context for this task
	taskCtx, cancel := context.WithCancel(ctx)

	// Apply timeout if specified
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

		// Parse inputs if present
		if len(assignment.Inputs) > 0 {
			var inputs model.Inputs
			if err := json.Unmarshal(assignment.Inputs, &inputs); err == nil {
				req.Inputs = &inputs
			}
		}

		// Parse resources if present
		if len(assignment.Resources) > 0 {
			var resources model.Resources
			if err := json.Unmarshal(assignment.Resources, &resources); err == nil {
				req.Resources = &resources
			}
		}

		// Notify the engine that execution is about to begin.
		// This triggers Pending→Running transitions on ancestor containers and
		// the WorkflowRun before any actual computation takes place.
		if b.startHandler != nil {
			b.startHandler(ctx, assignment.TaskRunID)
		}

		// Execute (retry is handled at a higher level or by the plugin itself)
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

			// Record metrics
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

// Cancel sends a cancellation signal to a running task by cancelling its context.
func (b *Broker) Cancel(_ context.Context, taskRunID uint64) error {
	b.mu.Lock()
	cancel, ok := b.cancels[taskRunID]
	b.mu.Unlock()

	if ok {
		cancel()
	}
	return nil
}

// FetchTask is not applicable in local mode.
// Blocks until the context is done, since there are no remote workers.
func (b *Broker) FetchTask(ctx context.Context, _ string) (*broker.TaskAssignment, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// Heartbeat is a no-op in local mode.
func (b *Broker) Heartbeat(_ context.Context, _ uint64, _ string) error {
	return nil
}

// StartTask invokes the StartHandler directly.
// In local mode, this is typically called internally by the Dispatch goroutine
// immediately before execution. It can also be called externally if needed.
func (b *Broker) StartTask(ctx context.Context, taskRunID uint64, _ string) error {
	if b.startHandler != nil {
		b.startHandler(ctx, taskRunID)
	}
	return nil
}

// CompleteTask invokes the CompletionHandler directly.
// In local mode, this is typically called internally by the Dispatch goroutine.
// It can also be called externally (e.g., for Resume scenarios).
func (b *Broker) CompleteTask(ctx context.Context, result *broker.TaskResult) error {
	if b.handler != nil {
		b.handler(ctx, result)
	}
	return nil
}

// Close cancels all in-flight tasks and waits for them to finish.
func (b *Broker) Close() error {
	b.mu.Lock()
	b.closed = true
	for _, cancel := range b.cancels {
		cancel()
	}
	b.mu.Unlock()

	b.wg.Wait()
	return nil
}
