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
	// CreateTaskRun creates a single task run.
	CreateTaskRun(ctx context.Context, run *TaskRun) error

	// BatchCreateTaskRuns creates multiple task runs. Deduplicates by (workflowRunID, taskName).
	BatchCreateTaskRuns(ctx context.Context, runs []*TaskRun) ([]*TaskRun, error)

	// GetTaskRun retrieves a task run by ID.
	GetTaskRun(ctx context.Context, taskRunID uint64) (*TaskRun, error)

	// UpdateTaskRun updates a task run's mutable fields (status, outputs, metrics).
	UpdateTaskRun(ctx context.Context, run *TaskRun) error

	// ListTaskRuns returns all task runs for a given workflow run.
	ListTaskRuns(ctx context.Context, workflowRunID uint64) ([]*TaskRun, error)
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
type TaskRun struct {
	RunID         uint64
	WorkflowRunID uint64
	TaskName      string // unique within a workflow DAG
	Path          string // full path e.g. "main/sub-dag/task-a"
	TemplateName  string // referenced template name
	Status        model.Phase
	Message       string
	Inputs        *model.Inputs
	Outputs       *model.Outputs
	Metrics       *model.Metrics
	Version       int64 // CAS optimistic lock version
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
