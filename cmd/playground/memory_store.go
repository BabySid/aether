// memory_store.go — in-memory Store implementation for standalone usage.
// It also records a full-state Snapshot after every mutating operation so that
// the HTML report's Snapshot Explorer can replay the store history step by step.
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// SnapWorkflowRun is a JSON-serialisable view of a WorkflowRun at a point in time.
type SnapWorkflowRun struct {
	RunID     string         `json:"runID"`
	Status    model.Phase    `json:"status"`
	Message   string         `json:"message,omitempty"`
	Outputs   *model.Outputs `json:"outputs,omitempty"`
	Token     uint64         `json:"token"`
	UpdatedAt time.Time      `json:"updatedAt"`
	CreatedAt time.Time      `json:"createdAt"`
}

// SnapTaskRun is a JSON-serialisable view of a TaskRun at a point in time.
type SnapTaskRun struct {
	RunID         string         `json:"runID"`
	WorkflowRunID string         `json:"workflowRunID"`
	ParentRunID   string         `json:"parentRunID,omitempty"`
	Depth         int            `json:"depth"`
	Scope         string         `json:"scope,omitempty"`
	TaskName      string         `json:"taskName"`
	TemplateName  string         `json:"templateName,omitempty"`
	TemplateType  string         `json:"templateType,omitempty"`
	Status        model.Phase    `json:"status"`
	Message       string         `json:"message,omitempty"`
	RetryCount    int            `json:"retryCount,omitempty"`
	Inputs        *model.Inputs  `json:"inputs,omitempty"`
	Outputs       *model.Outputs `json:"outputs,omitempty"`
	Token         uint64         `json:"token"`
	UpdatedAt     time.Time      `json:"updatedAt"`
	CreatedAt     time.Time      `json:"createdAt"`
}

// Snapshot captures the full store state after a single mutation.
type Snapshot struct {
	Seq          int               `json:"seq"`
	Time         time.Time         `json:"time"`
	Operation    string            `json:"operation"`
	EntityID     string            `json:"entityID"`
	WorkflowRuns []SnapWorkflowRun `json:"workflowRuns"`
	TaskRuns     []SnapTaskRun     `json:"taskRuns"`
}

func parentKey(workflowRunID, parentRunID string) string {
	return workflowRunID + ":" + parentRunID
}

// MemoryStore is an in-memory implementation of store.Store.
// It embeds snapshot recording so callers can retrieve the full mutation history
// via Snapshots() without any additional wrapper.
type MemoryStore struct {
	mu           sync.RWMutex
	workflowRuns map[string]*store.WorkflowRun
	taskRuns     map[string]*store.TaskRun
	taskIndex    map[string][]*store.TaskRun
	parentIndex  map[string][]*store.TaskRun
	templates    map[string]*model.WorkflowTemplate

	// snapshot history
	snapMu    sync.Mutex
	snapshots []Snapshot
	wfRunIDs  []string // all created workflow run IDs (for full snapshots)
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		workflowRuns: make(map[string]*store.WorkflowRun),
		taskRuns:     make(map[string]*store.TaskRun),
		taskIndex:    make(map[string][]*store.TaskRun),
		parentIndex:  make(map[string][]*store.TaskRun),
		templates:    make(map[string]*model.WorkflowTemplate),
	}
}

func (m *MemoryStore) Close() error { return nil }

// Snapshots returns a copy of the recorded mutation history.
func (m *MemoryStore) Snapshots() []Snapshot {
	m.snapMu.Lock()
	defer m.snapMu.Unlock()
	cp := make([]Snapshot, len(m.snapshots))
	copy(cp, m.snapshots)
	return cp
}

// record takes a full-state snapshot and appends it to the history.
// Must be called with snapMu NOT held; it acquires snapMu internally.
func (m *MemoryStore) record(op string, entityID string) {
	ctx := context.Background()

	snap := Snapshot{
		Seq:       len(m.snapshots) + 1,
		Time:      time.Now(),
		Operation: op,
		EntityID:  entityID,
	}

	for _, wfID := range m.wfRunIDs {
		wfRun, err := m.GetWorkflowRun(ctx, wfID)
		if err != nil {
			continue
		}
		sw := SnapWorkflowRun{
			RunID:     wfRun.RunID,
			Token:     wfRun.Token,
			UpdatedAt: wfRun.UpdatedAt,
			CreatedAt: wfRun.CreatedAt,
			Outputs:   wfRun.Outputs,
		}
		if wfRun.Status != nil {
			sw.Status = *wfRun.Status
		}
		if wfRun.Message != nil {
			sw.Message = *wfRun.Message
		}
		snap.WorkflowRuns = append(snap.WorkflowRuns, sw)

		trs, err := m.ListTaskRuns(ctx, wfID)
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
				Inputs:        tr.Inputs,
				Outputs:       tr.Outputs,
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

	m.snapMu.Lock()
	m.snapshots = append(m.snapshots, snap)
	m.snapMu.Unlock()
}

// --- WorkflowRunStore ---

func (m *MemoryStore) CreateWorkflowRun(_ context.Context, run *store.WorkflowRun) error {
	m.mu.Lock()
	if _, exists := m.workflowRuns[run.RunID]; exists {
		m.mu.Unlock()
		return fmt.Errorf("workflow run %s already exists", run.RunID)
	}
	now := time.Now()
	cp := *run
	cp.CreatedAt = now
	cp.UpdatedAt = now
	m.workflowRuns[run.RunID] = &cp
	m.wfRunIDs = append(m.wfRunIDs, run.RunID)
	m.mu.Unlock()

	m.record("CreateWorkflowRun", run.RunID)
	return nil
}

func (m *MemoryStore) GetWorkflowRun(_ context.Context, runID string) (*store.WorkflowRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	run, ok := m.workflowRuns[runID]
	if !ok {
		return nil, fmt.Errorf("workflow run %s: %w", runID, store.ErrNotFound)
	}
	cp := *run
	if run.Status != nil {
		s := *run.Status
		cp.Status = &s
	}
	if run.Message != nil {
		s := *run.Message
		cp.Message = &s
	}
	return &cp, nil
}

func (m *MemoryStore) UpdateWorkflowRun(_ context.Context, run *store.WorkflowRun) (*store.WorkflowRun, error) {
	m.mu.Lock()
	existing, ok := m.workflowRuns[run.RunID]
	if !ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("workflow run %s: %w", run.RunID, store.ErrNotFound)
	}
	if existing.Token != run.Token {
		m.mu.Unlock()
		return nil, fmt.Errorf("workflow run %s: token mismatch (expected %d, got %d)",
			run.RunID, existing.Token, run.Token)
	}
	if run.Status != nil {
		s := *run.Status
		existing.Status = &s
	}
	if run.Message != nil {
		s := *run.Message
		existing.Message = &s
	}
	if run.Outputs != nil {
		existing.Outputs = run.Outputs
	}
	if run.Metrics != nil {
		existing.Metrics = run.Metrics
	}
	if run.Deadline != nil {
		t := *run.Deadline
		existing.Deadline = &t
	}
	existing.Token++
	existing.UpdatedAt = time.Now()
	cp := *existing
	if existing.Status != nil {
		s := *existing.Status
		cp.Status = &s
	}
	if existing.Message != nil {
		s := *existing.Message
		cp.Message = &s
	}
	if existing.Deadline != nil {
		t := *existing.Deadline
		cp.Deadline = &t
	}
	m.mu.Unlock()

	m.record("UpdateWorkflowRun", run.RunID)
	return &cp, nil
}

func (m *MemoryStore) ListActiveWorkflowRuns(_ context.Context) ([]*store.WorkflowRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var active []*store.WorkflowRun
	for _, run := range m.workflowRuns {
		if run.Status == nil || !run.Status.IsTerminal() {
			cp := *run
			if run.Status != nil {
				s := *run.Status
				cp.Status = &s
			}
			active = append(active, &cp)
		}
	}
	return active, nil
}

// --- TaskRunStore ---

func (m *MemoryStore) CreateTaskRun(_ context.Context, run *store.TaskRun) error {
	m.mu.Lock()
	if m.taskExistsLocked(run.WorkflowRunID, run.ParentRunID, run.Scope, run.TaskName) {
		m.mu.Unlock()
		return nil
	}
	now := time.Now()
	cp := *run
	cp.CreatedAt = now
	cp.UpdatedAt = now
	if run.Status != nil {
		s := *run.Status
		cp.Status = &s
	}
	if run.Message != nil {
		s := *run.Message
		cp.Message = &s
	}
	if run.RetryCount != nil {
		rc := *run.RetryCount
		cp.RetryCount = &rc
	}
	m.taskRuns[run.RunID] = &cp
	m.taskIndex[run.WorkflowRunID] = append(m.taskIndex[run.WorkflowRunID], &cp)
	pk := parentKey(run.WorkflowRunID, run.ParentRunID)
	m.parentIndex[pk] = append(m.parentIndex[pk], &cp)
	m.mu.Unlock()

	m.record("CreateTaskRun", run.RunID)
	return nil
}

func (m *MemoryStore) GetTaskRun(_ context.Context, taskRunID string) (*store.TaskRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	run, ok := m.taskRuns[taskRunID]
	if !ok {
		return nil, fmt.Errorf("task run %s: %w", taskRunID, store.ErrNotFound)
	}
	return memDeepCopyTaskRun(run), nil
}

func (m *MemoryStore) UpdateTaskRun(_ context.Context, run *store.TaskRun) (*store.TaskRun, error) {
	m.mu.Lock()
	existing, ok := m.taskRuns[run.RunID]
	if !ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("task run %s: %w", run.RunID, store.ErrNotFound)
	}
	if existing.Token != run.Token {
		m.mu.Unlock()
		return nil, fmt.Errorf("task run %s: token mismatch (expected %d, got %d)",
			run.RunID, existing.Token, run.Token)
	}
	if run.Status != nil {
		s := *run.Status
		existing.Status = &s
	}
	if run.Message != nil {
		s := *run.Message
		existing.Message = &s
	}
	if run.Inputs != nil {
		existing.Inputs = run.Inputs
	}
	if run.Outputs != nil {
		existing.Outputs = run.Outputs
	}
	if run.Metrics != nil {
		existing.Metrics = run.Metrics
	}
	if run.RetryCount != nil {
		rc := *run.RetryCount
		existing.RetryCount = &rc
	}
	if run.Deadline != nil {
		t := *run.Deadline
		existing.Deadline = &t
	}
	existing.Token++
	existing.UpdatedAt = time.Now()
	for _, tr := range m.taskIndex[existing.WorkflowRunID] {
		if tr.RunID == run.RunID {
			tr.Token = existing.Token
			tr.UpdatedAt = existing.UpdatedAt
			break
		}
	}
	pk := parentKey(existing.WorkflowRunID, existing.ParentRunID)
	for _, tr := range m.parentIndex[pk] {
		if tr.RunID == run.RunID {
			tr.Token = existing.Token
			tr.UpdatedAt = existing.UpdatedAt
			break
		}
	}
	result := memDeepCopyTaskRun(existing)
	m.mu.Unlock()

	m.record("UpdateTaskRun", run.RunID)
	return result, nil
}

func (m *MemoryStore) ListTaskRuns(_ context.Context, workflowRunID string) ([]*store.TaskRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	runs := m.taskIndex[workflowRunID]
	result := make([]*store.TaskRun, len(runs))
	for i, r := range runs {
		result[i] = memDeepCopyTaskRun(r)
	}
	return result, nil
}

func (m *MemoryStore) ListActiveTaskRuns(_ context.Context) ([]*store.TaskRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*store.TaskRun
	for _, tr := range m.taskRuns {
		if tr.Deadline == nil {
			continue // no deadline set → not managed by watchdog
		}
		if tr.Status != nil && tr.Status.IsTerminal() {
			continue // already done
		}
		result = append(result, memDeepCopyTaskRun(tr))
	}
	return result, nil
}

func (m *MemoryStore) ListTaskRunsByParent(_ context.Context, workflowRunID string, parentRunID string) ([]*store.TaskRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pk := parentKey(workflowRunID, parentRunID)
	runs := m.parentIndex[pk]
	result := make([]*store.TaskRun, len(runs))
	for i, r := range runs {
		result[i] = memDeepCopyTaskRun(r)
	}
	return result, nil
}

func (m *MemoryStore) GetWorkflowTemplate(_ context.Context, namespace, name string) (*model.WorkflowTemplate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := namespace + "/" + name
	tmpl, ok := m.templates[key]
	if !ok {
		return nil, fmt.Errorf("workflow template %s: %w", key, store.ErrNotFound)
	}
	return tmpl, nil
}

func (m *MemoryStore) taskExistsLocked(workflowRunID, parentRunID string, scope, taskName string) bool {
	pk := parentKey(workflowRunID, parentRunID)
	for _, tr := range m.parentIndex[pk] {
		if tr.Scope == scope && tr.TaskName == taskName {
			return true
		}
	}
	return false
}

func memDeepCopyTaskRun(tr *store.TaskRun) *store.TaskRun {
	cp := *tr
	if tr.Status != nil {
		s := *tr.Status
		cp.Status = &s
	}
	if tr.Message != nil {
		s := *tr.Message
		cp.Message = &s
	}
	if tr.RetryCount != nil {
		rc := *tr.RetryCount
		cp.RetryCount = &rc
	}
	if tr.Deadline != nil {
		t := *tr.Deadline
		cp.Deadline = &t
	}
	return &cp
}
