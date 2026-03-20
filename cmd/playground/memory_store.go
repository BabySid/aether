// memory_store.go — in-memory Store implementation for standalone usage.
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

func parentKey(workflowRunID, parentRunID uint64) uint64 {
	return workflowRunID*1_000_000_000 + parentRunID
}

// MemoryStore is an in-memory implementation of store.Store.
type MemoryStore struct {
	mu           sync.RWMutex
	workflowRuns map[uint64]*store.WorkflowRun
	taskRuns     map[uint64]*store.TaskRun
	taskIndex    map[uint64][]*store.TaskRun
	parentIndex  map[uint64][]*store.TaskRun
	templates    map[string]*model.WorkflowTemplate
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		workflowRuns: make(map[uint64]*store.WorkflowRun),
		taskRuns:     make(map[uint64]*store.TaskRun),
		taskIndex:    make(map[uint64][]*store.TaskRun),
		parentIndex:  make(map[uint64][]*store.TaskRun),
		templates:    make(map[string]*model.WorkflowTemplate),
	}
}

func (m *MemoryStore) Close() error { return nil }

func (m *MemoryStore) CreateWorkflowRun(_ context.Context, run *store.WorkflowRun) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.workflowRuns[run.RunID]; exists {
		return fmt.Errorf("workflow run %d already exists", run.RunID)
	}
	now := time.Now()
	cp := *run
	cp.CreatedAt = now
	cp.UpdatedAt = now
	m.workflowRuns[run.RunID] = &cp
	return nil
}

func (m *MemoryStore) GetWorkflowRun(_ context.Context, runID uint64) (*store.WorkflowRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	run, ok := m.workflowRuns[runID]
	if !ok {
		return nil, fmt.Errorf("workflow run %d: %w", runID, store.ErrNotFound)
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
	defer m.mu.Unlock()
	existing, ok := m.workflowRuns[run.RunID]
	if !ok {
		return nil, fmt.Errorf("workflow run %d: %w", run.RunID, store.ErrNotFound)
	}
	if existing.Token != run.Token {
		return nil, fmt.Errorf("workflow run %d: token mismatch (expected %d, got %d)",
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

func (m *MemoryStore) CreateTaskRun(_ context.Context, run *store.TaskRun) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.taskExistsLocked(run.WorkflowRunID, run.ParentRunID, run.Scope, run.TaskName) {
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
	return nil
}

func (m *MemoryStore) GetTaskRun(_ context.Context, taskRunID uint64) (*store.TaskRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	run, ok := m.taskRuns[taskRunID]
	if !ok {
		return nil, fmt.Errorf("task run %d: %w", taskRunID, store.ErrNotFound)
	}
	return memDeepCopyTaskRun(run), nil
}

func (m *MemoryStore) UpdateTaskRun(_ context.Context, run *store.TaskRun) (*store.TaskRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, ok := m.taskRuns[run.RunID]
	if !ok {
		return nil, fmt.Errorf("task run %d: %w", run.RunID, store.ErrNotFound)
	}
	if existing.Token != run.Token {
		return nil, fmt.Errorf("task run %d: token mismatch (expected %d, got %d)",
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
	if run.RetryCount != nil {
		rc := *run.RetryCount
		existing.RetryCount = &rc
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
	return memDeepCopyTaskRun(existing), nil
}

func (m *MemoryStore) ListTaskRuns(_ context.Context, workflowRunID uint64) ([]*store.TaskRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	runs := m.taskIndex[workflowRunID]
	result := make([]*store.TaskRun, len(runs))
	for i, r := range runs {
		result[i] = memDeepCopyTaskRun(r)
	}
	return result, nil
}

func (m *MemoryStore) ListTaskRunsByParent(_ context.Context, workflowRunID uint64, parentRunID uint64) ([]*store.TaskRun, error) {
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

func (m *MemoryStore) taskExistsLocked(workflowRunID, parentRunID uint64, scope, taskName string) bool {
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
	return &cp
}
