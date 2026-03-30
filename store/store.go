// Package store defines the storage interfaces and persistent models for aether.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/model"
)

// ErrNotFound indicates the requested resource does not exist.
var ErrNotFound = errors.New("not found")

// Store is the aggregated storage interface. It is the single source of truth for all state.
type Store interface {
	WorkflowRunStore
	TaskRunStore
	WorkflowTemplateStore
	SchemaStore // 新增
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

// SchemaStore 负责 executor schema 的持久化。
// 业务逻辑（版本兼容检测、活跃 worker 追踪）由 VersionedSchemaRegistry 在内存中完成；
// 此接口只提供原始 schema 的 CRUD，供 master 重启后重新加载。
type SchemaStore interface {
	// UpsertSchema 持久化（新增或覆盖）一个 executor 的 schema。
	// workerID 标识上报该 schema 的 worker 实例，便于重启后区分"有活跃 worker"与
	// "历史 schema 孤本"（workerID="" 表示恢复加载时写入的孤本）。
	UpsertSchema(ctx context.Context, workerID string, schema executor.ExecutorSchema) error

	// ListSchemas 返回所有已持久化的 schema 及其关联的 workerID，
	// 供启动时重建内存 VersionedSchemaRegistry。
	ListSchemas(ctx context.Context) ([]SchemaRecord, error)

	// DeleteSchema 删除符合条件的 schema 记录。
	// execType 和 workerID 均为可选过滤条件（空串表示不限制），取交集匹配：
	//   DeleteSchema(ctx, "echo",   ""    ) → 删除 echo 的全部记录（GC 驱逐）
	//   DeleteSchema(ctx, "",       "w-1" ) → 删除 worker-1 的全部记录（worker 下线）
	//   DeleteSchema(ctx, "echo",   "w-1" ) → 精确删除 worker-1 上报的 echo schema
	DeleteSchema(ctx context.Context, execType, workerID string) error
}

// SchemaRecord 是 SchemaStore 的持久化记录单元，携带 workerID 元数据。
type SchemaRecord struct {
	WorkerID string
	Schema   executor.ExecutorSchema
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
