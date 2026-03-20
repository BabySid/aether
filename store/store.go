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
	Close() error
}

// WorkflowRunStore manages workflow run persistence.
type WorkflowRunStore interface {
	// CreateWorkflowRun stores the initial workflow run (immutable raw data).
	CreateWorkflowRun(ctx context.Context, run *WorkflowRun) error

	// GetWorkflowRun retrieves a workflow run by ID.
	GetWorkflowRun(ctx context.Context, runID uint64) (*WorkflowRun, error)

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
	GetTaskRun(ctx context.Context, taskRunID uint64) (*TaskRun, error)

	// UpdateTaskRun persists mutable fields of a task run.
	// Only non-nil pointer fields in run are written; nil fields are left unchanged.
	// run.Token is passed to the implementation, which decides whether and how to use it
	// (e.g., optimistic version check, or ignored for single-threaded stores).
	// Immutable fields (RunID, WorkflowRunID, ParentRunID, Scope, TaskName, etc.) are always ignored.
	// On success the returned TaskRun reflects the post-update state.
	UpdateTaskRun(ctx context.Context, run *TaskRun) (*TaskRun, error)

	// ListTaskRuns returns all task runs for a given workflow run.
	ListTaskRuns(ctx context.Context, workflowRunID uint64) ([]*TaskRun, error)

	// ListTaskRunsByParent returns task runs that share the same parent container.
	// parentRunID=0 returns top-level tasks (no parent container).
	// This is the core query for scoped/hierarchical DAG scheduling:
	// advanceScope uses it to find sibling tasks within the same scope.
	ListTaskRunsByParent(ctx context.Context, workflowRunID uint64, parentRunID uint64) ([]*TaskRun, error)
}

// WorkflowTemplateStore manages external workflow template loading.
type WorkflowTemplateStore interface {
	// GetWorkflowTemplate loads a WorkflowTemplate by namespace and name.
	GetWorkflowTemplate(ctx context.Context, namespace, name string) (*model.WorkflowTemplate, error)
}

// --- Persistent models ---

// WorkflowRun represents a persisted workflow execution.
//
// Immutable fields (set at creation, never modified): RunID, Workflow, CreatedAt.
// Mutable fields use pointer types: nil means "do not modify this field" in UpdateWorkflowRun.
type WorkflowRun struct {
	// Immutable
	RunID     uint64
	Workflow  json.RawMessage // immutable raw workflow JSON
	CreatedAt time.Time

	// Mutable (nil = do not modify in UpdateWorkflowRun)
	Status  *model.Phase
	Message *string
	Outputs *model.Outputs
	Metrics *model.Metrics

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
//	TemplateName, TemplateType, Inputs, CreatedAt.
//
// Mutable fields use pointer types: nil means "do not modify this field" in UpdateTaskRun.
type TaskRun struct {
	// Immutable
	RunID         uint64
	WorkflowRunID uint64
	ParentRunID   uint64 // parent container TaskRun ID (0 = top-level scope)
	Depth         int    // tree depth (0 = top-level), created as parent.Depth + 1
	Scope         string // direct-parent path segment, e.g. "main-pipeline/" or "batch-review.loop[0]/"
	TaskName      string // task name within current scope (unique among siblings)
	TemplateName  string // referenced template name
	TemplateType  string // "dag" / "task" / "loop"
	Inputs        *model.Inputs
	CreatedAt     time.Time

	// Mutable (nil = do not modify in UpdateTaskRun)
	Status     *model.Phase
	Message    *string
	Outputs    *model.Outputs
	Metrics    *model.Metrics
	RetryCount *int // number of retries already consumed (0 = first attempt, 1 = first retry, …)

	// Internal control
	// Token is an opaque uint64 write token. The framework passes it through unchanged;
	// the store implementation defines its semantics (e.g., monotonic version, seqno,
	// timestamp, or ignored entirely for single-threaded stores).
	Token     uint64
	UpdatedAt time.Time
}
