// Package store defines the storage interfaces and persistent models for aether.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/BabySid/aether/model"
)

var (
	// ErrNotFound indicates the requested resource does not exist.
	ErrNotFound = errors.New("not found")

	// ErrCASConflict indicates a compare-and-swap conflict.
	ErrCASConflict = errors.New("cas conflict")

	// ErrAlreadyExists indicates the resource already exists.
	// Returned by CreateTaskRun when a task run with the same
	// (workflowRunID, parentRunID, taskName) already exists.
	// Callers should treat this as a no-op (idempotent create).
	ErrAlreadyExists = errors.New("already exists")
)

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

	// UpdateWorkflowRunStatus updates the workflow run status.
	UpdateWorkflowRunStatus(ctx context.Context, runID uint64, status model.Phase, msg string) error

	// UpdateWorkflowRunStatusCAS atomically updates status if current matches expected.
	UpdateWorkflowRunStatusCAS(ctx context.Context, runID uint64, expected, target model.Phase, msg string) (bool, error)

	// ListActiveWorkflowRuns returns all non-terminal workflow runs (for crash recovery).
	ListActiveWorkflowRuns(ctx context.Context) ([]*WorkflowRun, error)
}

// TaskRunStore manages task run persistence.
type TaskRunStore interface {
	// CreateTaskRun creates a single task run, idempotent by
	// (workflowRunID, parentRunID, taskName). If a task run with the same
	// composite key already exists, implementations MUST return ErrAlreadyExists
	// rather than creating a duplicate. Callers treat ErrAlreadyExists as a no-op.
	CreateTaskRun(ctx context.Context, run *TaskRun) error

	// BatchCreateTaskRuns creates multiple task runs. Deduplicates by (workflowRunID, parentRunID, taskName).
	BatchCreateTaskRuns(ctx context.Context, runs []*TaskRun) ([]*TaskRun, error)

	// GetTaskRun retrieves a task run by ID.
	GetTaskRun(ctx context.Context, taskRunID uint64) (*TaskRun, error)

	// UpdateTaskRun updates a task run's mutable fields (status, outputs, metrics).
	UpdateTaskRun(ctx context.Context, run *TaskRun) error

	// UpdateTaskRunCAS atomically updates a task run's status from expected to target.
	// Returns (true, nil) if the update succeeded, (false, nil) if the current status
	// did not match expected (i.e., another writer already transitioned it).
	// Used to guard idempotent state transitions such as Pending→Running and
	// Running→Succeeded to prevent duplicate processing under concurrent advanceScope calls.
	UpdateTaskRunCAS(ctx context.Context, taskRunID uint64, expected, target model.Phase, msg string) (bool, error)

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
type WorkflowRun struct {
	RunID     uint64
	Workflow  json.RawMessage // immutable raw workflow JSON
	Status    model.Phase
	Message   string
	Outputs   *model.Outputs
	Metrics   *model.Metrics
	CreatedAt time.Time
	UpdatedAt time.Time
}

// TaskRun represents a persisted task execution.
//
// The ParentRunID field forms a tree of TaskRuns that mirrors the template
// nesting structure (dag → task → dag → task → ...). This enables:
//   - Scoped scheduling: advanceScope only looks at sibling TaskRuns
//   - Variable isolation: BuildScopedEnv only exposes same-scope outputs
//   - Upward propagation: when all children complete, parent is finalized
type TaskRun struct {
	RunID         uint64
	WorkflowRunID uint64
	ParentRunID   uint64 // parent container TaskRun ID (0 = top-level scope)
	Depth         int    // tree depth (0 = top-level), created as parent.Depth + 1
	Scope         string // direct-parent path segment, e.g. "main-pipeline/" or "batch-review.loop[0]/"
	TaskName      string // task name within current scope (unique among siblings)
	TemplateName  string // referenced template name
	TemplateType  string // "dag" / "task" / "loop"
	Status        model.Phase
	Message       string
	Inputs        *model.Inputs
	Outputs       *model.Outputs
	Metrics       *model.Metrics
	Version       int64 // CAS optimistic lock version
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
