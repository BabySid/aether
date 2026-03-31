// Package store defines the storage interfaces and persistent models for aether.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/BabySid/aether/model"
)

// ErrNotFound indicates the requested resource does not exist.
var ErrNotFound = errors.New("not found")

// Store is the aggregated storage interface. It is the single source of truth for all state.
type Store interface {
	WorkflowRunStore
	TaskRunStore
	WorkflowTemplateStore
	SchemaStore
	Close() error
}

// WorkflowRunStore manages workflow run persistence.
type WorkflowRunStore interface {
	// CreateWorkflowRun stores the initial workflow run (immutable raw data).
	CreateWorkflowRun(ctx context.Context, run *WorkflowRun) error

	// GetWorkflowRun retrieves a workflow run by ID.
	GetWorkflowRun(ctx context.Context, runID string) (*WorkflowRun, error)

	// UpdateWorkflowRun persists mutable fields of a workflow run.
	// Only non-nil pointer fields in run are written; nil fields are left unchanged.
	// run.Token is passed to the implementation, which decides whether and how to use it
	// (e.g., optimistic version check, or ignored for single-threaded stores).
	// On success the returned WorkflowRun reflects the post-update state.
	UpdateWorkflowRun(ctx context.Context, run *WorkflowRun) (*WorkflowRun, error)

	// ListActiveWorkflowRuns returns all non-terminal workflow runs.
	// Intended for crash recovery on engine restart; not yet wired into the engine.
	ListActiveWorkflowRuns(ctx context.Context) ([]*WorkflowRun, error)
}

// TaskRunStore manages task run persistence.
type TaskRunStore interface {
	// CreateTaskRun persists a new task run.
	// The operation is idempotent by (workflowRunID, parentRunID, scope, taskName):
	// if a task run with the same composite key already exists, implementations MUST
	// return nil (treat it as a no-op) rather than creating a duplicate or returning
	// an error. The framework relies on this contract for concurrent-safe scheduling.
	//
	// Using Scope as part of the key is critical for loop iterations: all iterations
	// of a repeatCondition loop share the same taskName (the body template name), but
	// each has a unique Scope (e.g. "poll-job.loop[0]/", "poll-job.loop[1]/"), so
	// they are correctly distinguished without requiring an explicit iteration index field.
	CreateTaskRun(ctx context.Context, run *TaskRun) error

	// GetTaskRun retrieves a task run by ID.
	GetTaskRun(ctx context.Context, taskRunID string) (*TaskRun, error)

	// UpdateTaskRun persists mutable fields of a task run.
	// Only non-nil pointer fields in run are written; nil fields are left unchanged.
	// run.Token is passed to the implementation, which decides whether and how to use it
	// (e.g., optimistic version check, or ignored for single-threaded stores).
	// Immutable fields (RunID, WorkflowRunID, ParentRunID, Scope, TaskName, etc.) are always ignored.
	// On success the returned TaskRun reflects the post-update state.
	UpdateTaskRun(ctx context.Context, run *TaskRun) (*TaskRun, error)

	// ListTaskRuns returns all task runs for a given workflow run.
	ListTaskRuns(ctx context.Context, workflowRunID string) ([]*TaskRun, error)

	// ListTaskRunsByParent returns task runs that share the same parent container.
	// parentRunID="" returns top-level tasks (no parent container).
	// This is the core query for scoped/hierarchical DAG scheduling:
	// advanceScope uses it to find sibling tasks within the same scope.
	ListTaskRunsByParent(ctx context.Context, workflowRunID string, parentRunID string) ([]*TaskRun, error)

	// ListActiveTaskRuns returns all non-terminal task runs (Pending or Running) across
	// all workflow runs that have a Deadline set.
	// Used by the timeout watchdog to detect tasks that have exceeded their Deadline.
	// Note: Pending tasks are included because the Deadline is written at dispatch time,
	// before OnTaskStarted transitions the task to Running.
	ListActiveTaskRuns(ctx context.Context) ([]*TaskRun, error)
}

// WorkflowTemplateStore manages external workflow template loading.
type WorkflowTemplateStore interface {
	// GetWorkflowTemplate loads a WorkflowTemplate by namespace and name.
	GetWorkflowTemplate(ctx context.Context, namespace, name string) (*model.WorkflowTemplate, error)
}

// SchemaStore handles persistence of executor schemas.
// Business logic (version compatibility checks, active worker tracking) is handled
// in-memory by VersionedSchemaRegistry; this interface only provides raw schema CRUD,
// used to reload state after a master restart.
type SchemaStore interface {
	// UpsertSchema persists (inserts or overwrites) an executor's schema.
	// workerID identifies the worker instance that reported this schema, allowing
	// distinction between "active worker" and "orphan historical schema" after restart
	// (workerID="" indicates an orphan loaded during recovery).
	UpsertSchema(ctx context.Context, workerID string, schema model.ExecutorSchema) error

	// ListSchemas returns all persisted schemas and their associated workerIDs,
	// used to rebuild the in-memory VersionedSchemaRegistry at startup.
	ListSchemas(ctx context.Context) ([]SchemaRecord, error)

	// DeleteSchema removes schema records matching the given filters.
	// Both execType and workerID are optional filters (empty string means no restriction);
	// they are combined with AND semantics:
	//   DeleteSchema(ctx, "echo",   ""    ) → delete all records for echo (GC eviction)
	//   DeleteSchema(ctx, "",       "w-1" ) → delete all records for worker-1 (worker offline)
	//   DeleteSchema(ctx, "echo",   "w-1" ) → delete exactly the echo schema reported by worker-1
	DeleteSchema(ctx context.Context, execType, workerID string) error
}

// SchemaRecord is the persistence record unit of SchemaStore, carrying workerID metadata.
type SchemaRecord struct {
	WorkerID string
	Schema   model.ExecutorSchema
}

// --- Persistent models ---

// WorkflowRun represents a persisted workflow execution.
//
// Immutable fields (set at creation, never modified): RunID, Workflow, CreatedAt.
// Mutable fields use pointer types: nil means "do not modify this field" in UpdateWorkflowRun.
type WorkflowRun struct {
	// Immutable
	RunID     string
	Workflow  json.RawMessage // immutable raw workflow JSON
	CreatedAt time.Time

	// Mutable (nil = do not modify in UpdateWorkflowRun)
	Status  *model.Phase
	Message *string
	Outputs *model.Outputs
	Metrics *model.Metrics
	// Deadline is the absolute time after which the workflow is considered timed out.
	// Set by the Engine at Submit time from wf.Spec.Timeout; nil means no workflow-level deadline.
	Deadline *time.Time

	// Internal control
	// Token is an opaque uint64 write token. The framework passes it through unchanged;
	// the store implementation defines its semantics (e.g., monotonic version, seqno,
	// timestamp, or ignored entirely for single-threaded stores).
	Token     uint64
	UpdatedAt time.Time
}

// TaskRun represents a persisted task execution.
//
// The ParentRunID field forms a tree of TaskRuns that mirrors the template
// nesting structure (dag → task → dag → task → ...). This enables:
//   - Scoped scheduling: advanceScope only looks at sibling TaskRuns
//   - Variable isolation: BuildScopedEnv only exposes same-scope outputs
//   - Upward propagation: when all children complete, parent is finalized
//
// Immutable fields (set at creation, never modified):
//
//	RunID, WorkflowRunID, ParentRunID, Depth, Scope, TaskName,
//	TemplateName, TemplateType, CreatedAt.
//
// Mutable fields use pointer types: nil means "do not modify this field" in UpdateTaskRun.
type TaskRun struct {
	// Immutable
	RunID         string
	WorkflowRunID string
	ParentRunID   string // parent container TaskRun ID ("" = top-level scope)
	Depth         int    // tree depth (0 = top-level), created as parent.Depth + 1
	Scope         string // direct-parent path segment, e.g. "main-pipeline/" or "batch-review.loop[0]/"
	TaskName      string // task name within current scope (unique among siblings)
	TemplateName  string // referenced template name
	TemplateType  string // "dag" / "task" / "loop"
	CreatedAt     time.Time

	// Mutable (nil = do not modify in UpdateTaskRun)
	// Inputs holds the resolved task inputs. For suspended tasks it accumulates
	// successive Resume payloads merged on top of the original resolved inputs.
	Inputs     *model.Inputs
	Status     *model.Phase
	Message    *string
	Outputs    *model.Outputs
	Metrics    *model.Metrics
	RetryCount *int // number of retries already consumed (0 = first attempt, 1 = first retry, …)
	// Deadline is the absolute time after which the task is considered timed out.
	// Set by the Engine at dispatch time from the resolved assignment.Timeout; nil means no deadline.
	Deadline *time.Time

	// Internal control
	// Token is an opaque uint64 write token. The framework passes it through unchanged;
	// the store implementation defines its semantics (e.g., monotonic version, seqno,
	// timestamp, or ignored entirely for single-threaded stores).
	Token     uint64
	UpdatedAt time.Time
}
