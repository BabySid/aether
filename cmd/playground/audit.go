// Package main — audit.go wraps store.Store and captures a full-state snapshot
// after every mutating operation (Create/Update). Snapshots drive the store
// timeline section of the HTML report and the per-step JSON dump files.
package main

import (
	"context"
	"sync"
	"time"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// SnapWorkflowRun is a deep-copied, JSON-serialisable view of a WorkflowRun.
type SnapWorkflowRun struct {
	RunID     uint64      `json:"runID"`
	Status    model.Phase `json:"status"`
	Message   string      `json:"message,omitempty"`
	Token     uint64      `json:"token"`
	UpdatedAt time.Time   `json:"updatedAt"`
	CreatedAt time.Time   `json:"createdAt"`
}

// SnapTaskRun is a deep-copied, JSON-serialisable view of a TaskRun.
type SnapTaskRun struct {
	RunID         uint64      `json:"runID"`
	WorkflowRunID uint64      `json:"workflowRunID"`
	ParentRunID   uint64      `json:"parentRunID,omitempty"`
	Depth         int         `json:"depth"`
	Scope         string      `json:"scope,omitempty"`
	TaskName      string      `json:"taskName"`
	TemplateName  string      `json:"templateName,omitempty"`
	TemplateType  string      `json:"templateType,omitempty"`
	Status        model.Phase `json:"status"`
	Message       string      `json:"message,omitempty"`
	RetryCount    int         `json:"retryCount,omitempty"`
	Token         uint64      `json:"token"`
	UpdatedAt     time.Time   `json:"updatedAt"`
	CreatedAt     time.Time   `json:"createdAt"`
}

// Snapshot captures the full store state after a single mutation.
type Snapshot struct {
	Seq          int               `json:"seq"`
	Time         time.Time         `json:"time"`
	Operation    string            `json:"operation"`
	EntityID     uint64            `json:"entityID"`
	WorkflowRuns []SnapWorkflowRun `json:"workflowRuns"`
	TaskRuns     []SnapTaskRun     `json:"taskRuns"`
}

// AuditStore wraps a store.Store and records a Snapshot after each mutation.
type AuditStore struct {
	inner store.Store

	mu        sync.Mutex
	snapshots []Snapshot
	wfRunIDs  []uint64 // all created workflow run IDs (for full snapshots)
}

func NewAuditStore(inner store.Store) *AuditStore {
	return &AuditStore{inner: inner}
}

func (a *AuditStore) Snapshots() []Snapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	cp := make([]Snapshot, len(a.snapshots))
	copy(cp, a.snapshots)
	return cp
}

// record takes a snapshot and appends it to the list.
// Must be called with a.mu held.
func (a *AuditStore) record(op string, entityID uint64) {
	ctx := context.Background()

	snap := Snapshot{
		Seq:       len(a.snapshots) + 1,
		Time:      time.Now(),
		Operation: op,
		EntityID:  entityID,
	}

	for _, wfID := range a.wfRunIDs {
		wfRun, err := a.inner.GetWorkflowRun(ctx, wfID)
		if err != nil {
			continue
		}
		sw := SnapWorkflowRun{
			RunID:     wfRun.RunID,
			Token:     wfRun.Token,
			UpdatedAt: wfRun.UpdatedAt,
			CreatedAt: wfRun.CreatedAt,
		}
		if wfRun.Status != nil {
			sw.Status = *wfRun.Status
		}
		if wfRun.Message != nil {
			sw.Message = *wfRun.Message
		}
		snap.WorkflowRuns = append(snap.WorkflowRuns, sw)

		trs, err := a.inner.ListTaskRuns(ctx, wfID)
		if err != nil {
			continue
		}
		for _, tr := range trs {
			st := SnapTaskRun{
				RunID:         tr.RunID,
				WorkflowRunID: tr.WorkflowRunID,
				ParentRunID:   tr.ParentRunID,
				Depth:         tr.Depth,
				Scope:         tr.Scope,
				TaskName:      tr.TaskName,
				TemplateName:  tr.TemplateName,
				TemplateType:  tr.TemplateType,
				Token:         tr.Token,
				UpdatedAt:     tr.UpdatedAt,
				CreatedAt:     tr.CreatedAt,
			}
			if tr.Status != nil {
				st.Status = *tr.Status
			}
			if tr.Message != nil {
				st.Message = *tr.Message
			}
			if tr.RetryCount != nil {
				st.RetryCount = *tr.RetryCount
			}
			snap.TaskRuns = append(snap.TaskRuns, st)
		}
	}

	a.snapshots = append(a.snapshots, snap)
}

// --- WorkflowRunStore ---

func (a *AuditStore) CreateWorkflowRun(ctx context.Context, run *store.WorkflowRun) error {
	if err := a.inner.CreateWorkflowRun(ctx, run); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.wfRunIDs = append(a.wfRunIDs, run.RunID)
	a.record("CreateWorkflowRun", run.RunID)
	return nil
}

func (a *AuditStore) GetWorkflowRun(ctx context.Context, runID uint64) (*store.WorkflowRun, error) {
	return a.inner.GetWorkflowRun(ctx, runID)
}

func (a *AuditStore) UpdateWorkflowRun(ctx context.Context, run *store.WorkflowRun) (*store.WorkflowRun, error) {
	result, err := a.inner.UpdateWorkflowRun(ctx, run)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.record("UpdateWorkflowRun", run.RunID)
	return result, nil
}

func (a *AuditStore) ListActiveWorkflowRuns(ctx context.Context) ([]*store.WorkflowRun, error) {
	return a.inner.ListActiveWorkflowRuns(ctx)
}

// --- TaskRunStore ---

func (a *AuditStore) CreateTaskRun(ctx context.Context, run *store.TaskRun) error {
	if err := a.inner.CreateTaskRun(ctx, run); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.record("CreateTaskRun", run.RunID)
	return nil
}

func (a *AuditStore) GetTaskRun(ctx context.Context, taskRunID uint64) (*store.TaskRun, error) {
	return a.inner.GetTaskRun(ctx, taskRunID)
}

func (a *AuditStore) UpdateTaskRun(ctx context.Context, run *store.TaskRun) (*store.TaskRun, error) {
	result, err := a.inner.UpdateTaskRun(ctx, run)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.record("UpdateTaskRun", run.RunID)
	return result, nil
}

func (a *AuditStore) ListTaskRuns(ctx context.Context, workflowRunID uint64) ([]*store.TaskRun, error) {
	return a.inner.ListTaskRuns(ctx, workflowRunID)
}

func (a *AuditStore) ListTaskRunsByParent(ctx context.Context, workflowRunID uint64, parentRunID uint64) ([]*store.TaskRun, error) {
	return a.inner.ListTaskRunsByParent(ctx, workflowRunID, parentRunID)
}

// --- WorkflowTemplateStore ---

func (a *AuditStore) GetWorkflowTemplate(ctx context.Context, namespace, name string) (*model.WorkflowTemplate, error) {
	return a.inner.GetWorkflowTemplate(ctx, namespace, name)
}

// --- Lifecycle ---

func (a *AuditStore) Close() error {
	return a.inner.Close()
}
