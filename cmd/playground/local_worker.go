// local_worker.go — goroutine-based Worker that uses the standard
// FetchTask → StartTask → Execute → CompleteTask flow.
package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/model"
)

// LocalWorker runs worker goroutines that pull tasks from the broker,
// execute them via the executor registry, and report results back.
type LocalWorker struct {
	id       string
	broker   *LocalBroker
	registry *executor.Registry
	wg       sync.WaitGroup
}

// NewLocalWorker creates a LocalWorker wired to the given broker and registry.
func NewLocalWorker(id string, b *LocalBroker, reg *executor.Registry) *LocalWorker {
	return &LocalWorker{
		id:       id,
		broker:   b,
		registry: reg,
	}
}

// Start launches n worker goroutines. Each goroutine loops:
// FetchTask → StartTask → Execute → CompleteTask.
func (w *LocalWorker) Start(ctx context.Context, concurrency int) {
	for i := 0; i < concurrency; i++ {
		w.wg.Add(1)
		go w.run(ctx)
	}
}

// Stop waits for all worker goroutines to finish.
func (w *LocalWorker) Stop() {
	w.wg.Wait()
}

func (w *LocalWorker) run(ctx context.Context) {
	defer w.wg.Done()
	for {
		assignment, err := w.broker.FetchTask(ctx, w.id)
		if err != nil || assignment == nil {
			return
		}

		taskCtx := w.broker.TaskContext(assignment.TaskRunID)
		if taskCtx == nil {
			taskCtx = ctx
		}

		_ = w.broker.StartTask(ctx, assignment.TaskRunID, w.id)

		result := &broker.TaskResult{
			TaskRunID:     assignment.TaskRunID,
			WorkflowRunID: assignment.WorkflowRunID,
		}

		plugin, ok := w.registry.Get(assignment.ExecutorType)
		if !ok {
			result.ExecOutputs = &model.ExecOutputs{
				Code:    model.ExecCodeError,
				Message: fmt.Sprintf("unknown executor type: %s", assignment.ExecutorType),
			}
		} else {
			req := &executor.ExecuteRequest{
				TaskRunID:     assignment.TaskRunID,
				WorkflowRunID: assignment.WorkflowRunID,
				TaskName:      assignment.TaskName,
				TemplateName:  assignment.TemplateName,
				Inputs:        assignment.Inputs,
				Resources:     assignment.Resources,
				Timeout:       assignment.Timeout,
				RetryCount:    assignment.RetryCount,
			}
			execOutputs, execErr := plugin.Execute(taskCtx, req)
			result.ExecOutputs = normalizeExecOutputs(taskCtx, execOutputs, execErr)
		}

		_ = w.broker.CompleteTask(ctx, result)
	}
}

// normalizeExecOutputs translates system-level errors (timeout, panic) into
// ExecCode so the engine can derive Phase uniformly from Code alone.
func normalizeExecOutputs(taskCtx context.Context, outputs *model.ExecOutputs, err error) *model.ExecOutputs {
	if outputs == nil {
		outputs = &model.ExecOutputs{}
	}
	if err != nil {
		if taskCtx.Err() != nil {
			outputs.Code = model.ExecCodeTimeout
			outputs.Message = fmt.Sprintf("task timed out: %v", taskCtx.Err())
		} else {
			outputs.Code = model.ExecCodeError
			if outputs.Message == "" {
				outputs.Message = err.Error()
			}
		}
	}
	return outputs
}
