// Package broker defines the task lifecycle management abstraction for aether.
//
// TaskBroker is the single bridge between Engine (master) and Worker.
// It unifies task dispatch, cancellation, fetching, heartbeat, and completion
// into one interface. Implementations decide how tasks are distributed:
//   - local: goroutine + in-process executor
//   - distributed: message queue, Redis, HTTP, gRPC, etc.
//
// Implementations must be safe for concurrent use by multiple goroutines.
package broker

import (
	"context"
	"encoding/json"

	"github.com/BabySid/aether/model"
)

// TaskBroker manages the full lifecycle of task distribution between
// the Engine (master side) and Workers (execution side).
type TaskBroker interface {
	// --- Engine side ---

	// Dispatch submits a task for execution.
	// How and where the task runs is determined by the implementation:
	//   - local: starts a goroutine and executes in-process
	//   - distributed: enqueues to MQ / Redis / HTTP endpoint
	Dispatch(ctx context.Context, assignment *TaskAssignment) error

	// Cancel sends a cancellation signal to a running task.
	// Implementation decides how to propagate (context cancel / remote signal).
	Cancel(ctx context.Context, taskRunID uint64) error

	// --- Worker side ---

	// FetchTask pulls a pending task for execution (blocking / long-poll).
	// workerID identifies the caller for affinity / logging.
	// Returns (nil, context.DeadlineExceeded) or (nil, context.Canceled)
	// when the context expires before a task becomes available.
	FetchTask(ctx context.Context, workerID string) (*TaskAssignment, error)

	// Heartbeat reports that a worker is still alive and working on the task.
	// Implementations should treat this as idempotent.
	Heartbeat(ctx context.Context, taskRunID uint64, workerID string) error

	// CompleteTask reports the final execution result of a task.
	// Called by the worker after task execution finishes.
	// The implementation decides how to deliver this result to the engine:
	//   - local: directly invokes the CompletionHandler
	//   - distributed: publishes to MQ, the consumer calls engine.OnTaskCompleted
	CompleteTask(ctx context.Context, result *TaskResult) error

	// --- Lifecycle ---

	// Close releases broker resources and waits for in-flight tasks to drain.
	Close() error
}

// CompletionHandler is the callback invoked when a task finishes execution.
// Engine's OnTaskCompleted method satisfies this signature.
//
// This type is NOT part of the TaskBroker interface contract.
// It is a convenience type used by implementations (e.g., local broker)
// that need a direct callback mechanism.
type CompletionHandler func(ctx context.Context, result *TaskResult)

// TaskAssignment contains all information needed to execute a task.
// "Fat assignment": workers do not need to query the Store.
type TaskAssignment struct {
	TaskRunID      uint64
	WorkflowRunID  uint64
	TaskName       string
	TemplateName   string
	ExecutorType   string          // "script" / "function" / "await"
	ExecutorConfig json.RawMessage // executor.config raw JSON
	Inputs         json.RawMessage // task inputs raw JSON
	Timeout        string          // e.g. "30m"
	Resources      json.RawMessage // resource requirements
	Priority       int
}

// TaskResult holds the result of a completed task execution.
type TaskResult struct {
	TaskRunID uint64
	Phase     model.Phase
	Message   string
	Outputs   *model.Outputs
}
