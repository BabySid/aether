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

	"github.com/BabySid/aether/executor"
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
	Cancel(ctx context.Context, taskRunID string) error

	// --- Worker side ---

	// FetchTask pulls a pending task for execution (blocking / long-poll).
	// workerID identifies the caller for affinity / logging.
	// Returns (nil, context.DeadlineExceeded) or (nil, context.Canceled)
	// when the context expires before a task becomes available.
	FetchTask(ctx context.Context, workerID string) (*TaskAssignment, error)

	// StartTask reports that a worker has begun executing a task.
	// Must be called before any actual computation starts so that the engine
	// can transition the task (and its ancestor containers) from Pending to Running.
	// The implementation decides how to deliver this event to the engine:
	//   - local: directly invokes the StartHandler
	//   - distributed: publishes to MQ, the consumer calls engine.OnTaskStarted
	StartTask(ctx context.Context, taskRunID string, workerID string) error

	// Heartbeat reports that a worker is still alive and working on the task.
	// Implementations should treat this as idempotent.
	Heartbeat(ctx context.Context, taskRunID string, workerID string) error

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

// StartHandler is the callback invoked when a task begins execution.
// Engine's OnTaskStarted method satisfies this signature.
//
// This type is NOT part of the TaskBroker interface contract.
// It is a convenience type used by implementations (e.g., local broker)
// that need a direct callback mechanism.
type StartHandler func(ctx context.Context, taskRunID string)

// CompletionHandler is the callback invoked when a task finishes execution.
// Engine's OnTaskCompleted method satisfies this signature.
//
// This type is NOT part of the TaskBroker interface contract.
// It is a convenience type used by implementations (e.g., local broker)
// that need a direct callback mechanism.
type CompletionHandler func(ctx context.Context, result *TaskResult)

// TaskAssignment contains all information needed to execute a task.
// "Fat assignment": workers do not need to query the Store.
//
// Inputs and Resources use strong types: they follow a fixed schema known to the
// framework, and using *model.Inputs / *model.Resources eliminates manual
// marshal/unmarshal in every broker implementation.
type TaskAssignment struct {
	TaskRunID     string
	WorkflowRunID string
	TaskName      string
	TemplateName  string
	ExecutorType  string           // executor type identifier, e.g. "echo", "http", "shell"
	Inputs        *model.Inputs    // resolved task inputs (nil if none)
	Timeout       string           // e.g. "30m"
	Resources     *model.Resources // resource requirements (nil if none)
	Priority      int
	RetryCount    int // number of retries already consumed (0 = first attempt)
}

// WorkerRegistration is sent by a worker to the broker at startup.
// It carries the worker's identity, the executor types it can handle,
// and the full ExecutorSchema for each type so the master can populate
// its SchemaRegistry without a separate round-trip.
type WorkerRegistration struct {
	WorkerID      string                    // unique worker instance id
	ExecutorTypes []string                  // types this worker can handle
	Schemas       []executor.ExecutorSchema // full schemas for each type
	Tags          map[string]string         // optional routing labels
}

// TaskResult holds the result of a completed task execution.
// It is the message a worker sends to the engine when a task finishes.
//
// Design principles:
//   - WorkflowRunID mirrors TaskAssignment so the engine can locate the scope
//     without an extra store lookup.
//   - ExecOutputs.Code carries the execution outcome; the engine maps it to
//     Phase (single Phase writer).
//   - Phase and Metrics are NOT included: Phase is derived by the engine from
//     Code; Metrics (StartedAt/FinishedAt/Retries) are recorded by the engine
//     in OnTaskStarted / OnTaskCompleted.
type TaskResult struct {
	TaskRunID     string
	WorkflowRunID string // mirrors TaskAssignment.WorkflowRunID; avoids extra store lookup
	*model.ExecOutputs
}
