// local_broker.go — goroutine-based TaskBroker for standalone mode.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/internal"
	"github.com/BabySid/aether/model"
)

// ResumeFunc is the callback used by LocalBroker to trigger an automatic
// resume after a task returns ExecCodeSuspended with autoResumeAfter set.
// It mirrors aether.Engine.Resume: (ctx, workflowRunID, taskID, payload).
type ResumeFunc func(ctx context.Context, workflowRunID, taskID string, payload map[string]any) error

// LocalBroker executes tasks in-process using goroutines.
type LocalBroker struct {
	registry     *executor.Registry
	startHandler broker.StartHandler
	handler      broker.CompletionHandler
	// resumeFn, when set, is called by the auto-resume goroutine.
	resumeFn ResumeFunc

	mu      sync.Mutex
	cancels map[string]context.CancelFunc
	wg      sync.WaitGroup
	closed  bool
}

// NewLocalBroker creates a LocalBroker wired with the given registry and callbacks.
func NewLocalBroker(reg *executor.Registry, startHandler broker.StartHandler, handler broker.CompletionHandler) *LocalBroker {
	return &LocalBroker{
		registry:     reg,
		startHandler: startHandler,
		handler:      handler,
		cancels:      make(map[string]context.CancelFunc),
	}
}

// SetResumeFunc wires the auto-resume callback. Must be called before Submit.
func (b *LocalBroker) SetResumeFunc(fn ResumeFunc) {
	b.resumeFn = fn
}

func (b *LocalBroker) Dispatch(ctx context.Context, assignment *broker.TaskAssignment) error {
	plugin, ok := b.registry.Get(assignment.ExecutorType)
	if !ok {
		if b.handler != nil {
			b.handler(ctx, &broker.TaskResult{
				TaskRunID:     assignment.TaskRunID,
				WorkflowRunID: assignment.WorkflowRunID,
				ExecOutputs: &model.ExecOutputs{
					Code:    model.ExecCodeError,
					Message: fmt.Sprintf("unknown executor type: %s", assignment.ExecutorType),
				},
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
			TaskRunID:     assignment.TaskRunID,
			WorkflowRunID: assignment.WorkflowRunID,
			TaskName:      assignment.TaskName,
			TemplateName:  assignment.TemplateName,
			Config:        assignment.ExecutorConfig,
			Inputs:        assignment.Inputs,
			Resources:     assignment.Resources,
			Timeout:       assignment.Timeout,
			RetryCount:    assignment.RetryCount,
		}

		if b.startHandler != nil {
			b.startHandler(ctx, assignment.TaskRunID)
		}

		execOutputs, err := plugin.Execute(taskCtx, req)

		// --- ExecCode normalization ---
		// The executor sets Code for business outcomes (Succeeded/Suspended/Failed).
		// The broker translates system-level errors (timeout, panic) into ExecCode
		// so the engine can derive Phase uniformly from Code alone.
		//
		//   ctx.Err() != nil → ExecCodeTimeout
		//   err != nil       → ExecCodeError
		//   otherwise        → Code already set by executor (0/1/2)
		taskResult := &broker.TaskResult{
			TaskRunID:     assignment.TaskRunID,
			WorkflowRunID: assignment.WorkflowRunID,
			ExecOutputs:   execOutputs,
		}
		if taskResult.ExecOutputs == nil {
			taskResult.ExecOutputs = &model.ExecOutputs{}
		}

		if err != nil {
			if taskCtx.Err() != nil {
				taskResult.ExecOutputs.Code = model.ExecCodeTimeout
				taskResult.ExecOutputs.Message = fmt.Sprintf("task timed out: %v", taskCtx.Err())
			} else {
				taskResult.ExecOutputs.Code = model.ExecCodeError
				if taskResult.ExecOutputs.Message == "" {
					taskResult.ExecOutputs.Message = err.Error()
				}
			}
		}
		// No else needed: executor already set Code (ExecCodeSucceeded/Suspended/Failed).

		if b.handler != nil {
			b.handler(ctx, taskResult)
		}

		// Auto-resume: if the executor returned Suspended and the config declares
		// autoResumeAfter, spawn a goroutine that calls resumeFn after the delay.
		// This lets playground CLI demonstrate the full suspend→resume flow without
		// requiring external tooling or interactive input.
		if execOutputs != nil && execOutputs.Code == model.ExecCodeSuspended && b.resumeFn != nil {
			var cfg echoConfig
			if len(assignment.ExecutorConfig) > 0 {
				_ = json.Unmarshal(assignment.ExecutorConfig, &cfg)
			}
			if cfg.AutoResumeAfter != "" {
				delay, parseErr := internal.ParseDuration(cfg.AutoResumeAfter)
				if parseErr == nil && delay > 0 {
					wfRunID := assignment.WorkflowRunID
					taskRunID := assignment.TaskRunID
					taskName := assignment.TaskName
					resumeFn := b.resumeFn
					b.wg.Add(1)
					go func() {
						defer b.wg.Done()
						log.Printf("[echo] taskRunID=%s taskName=%s  auto-resume scheduled in %s",
							taskRunID, taskName, delay)
						select {
						case <-time.After(delay):
						case <-ctx.Done():
							return
						}
						log.Printf("[echo] taskRunID=%s taskName=%s  auto-resume firing",
							taskRunID, taskName)
						_ = resumeFn(ctx, wfRunID, taskRunID, map[string]any{
							resumedMarker: true,
						})
					}()
				}
			}
		}
	}()

	return nil
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
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *LocalBroker) Heartbeat(_ context.Context, _ string, _ string) error { return nil }

func (b *LocalBroker) StartTask(ctx context.Context, taskRunID string, _ string) error {
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
