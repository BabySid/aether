// Package memory provides an in-memory Store implementation for standalone usage.
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// parentKey builds the composite key for the parent index.
func parentKey(workflowRunID, parentRunID uint64) uint64 {
	// Simple composite: use workflow run ID shifted + parent run ID.
	// For in-memory usage this is sufficient. Production stores use SQL WHERE clauses.
	return workflowRunID*1_000_000_000 + parentRunID
}

// Store is an in-memory implementation of store.Store.
// Safe for concurrent use. Intended for standalone / testing scenarios.
type Store struct {
	mu           sync.RWMutex
	workflowRuns map[uint64]*store.WorkflowRun
	taskRuns     map[uint64]*store.TaskRun
	taskIndex    map[uint64][]*store.TaskRun        // workflowRunID -> task runs
	parentIndex  map[uint64][]*store.TaskRun        // parentKey(wfRunID, parentRunID) -> task runs
	templates    map[string]*model.WorkflowTemplate // "namespace/name" -> template
}

// New creates a new memory Store.
func New() *Store {
	return &Store{
		workflowRuns: make(map[uint64]*store.WorkflowRun),
		taskRuns:     make(map[uint64]*store.TaskRun),
		taskIndex:    make(map[uint64][]*store.TaskRun),
		parentIndex:  make(map[uint64][]*store.TaskRun),
		templates:    make(map[string]*model.WorkflowTemplate),
	}
}

// Close is a no-op for memory Store.
func (m *Store) Close() error {
	return nil
}

// --- WorkflowRunStore ---

func (m *Store) CreateWorkflowRun(_ context.Context, run *store.WorkflowRun) error {
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

func (m *Store) GetWorkflowRun(_ context.Context, runID uint64) (*store.WorkflowRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	run, ok := m.workflowRuns[runID]
	if !ok {
		return nil, fmt.Errorf("workflow run %d: %w", runID, store.ErrNotFound)
	}
	cp := *run
	return &cp, nil
}

func (m *Store) UpdateWorkflowRunStatus(_ context.Context, runID uint64, status model.Phase, msg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	run, ok := m.workflowRuns[runID]
	if !ok {
		return fmt.Errorf("workflow run %d: %w", runID, store.ErrNotFound)
	}
	run.Status = status
	run.Message = msg
	run.UpdatedAt = time.Now()
	return nil
}

func (m *Store) UpdateWorkflowRunStatusCAS(_ context.Context, runID uint64, expected, target model.Phase, msg string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	run, ok := m.workflowRuns[runID]
	if !ok {
		return false, fmt.Errorf("workflow run %d: %w", runID, store.ErrNotFound)
	}
	if run.Status != expected {
		return false, nil
	}
	run.Status = target
	run.Message = msg
	run.UpdatedAt = time.Now()
	return true, nil
}

func (m *Store) ListActiveWorkflowRuns(_ context.Context) ([]*store.WorkflowRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var active []*store.WorkflowRun
	for _, run := range m.workflowRuns {
		if !run.Status.IsTerminal() {
			cp := *run
			active = append(active, &cp)
		}
	}
	return active, nil
}

// --- TaskRunStore ---

func (m *Store) CreateTaskRun(_ context.Context, run *store.TaskRun) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Idempotency guard: (workflowRunID, parentRunID, taskName) must be unique within a scope.
	// Concurrent advanceScope calls may both attempt to create the same task; the second
	// call gets ErrAlreadyExists and treats it as a no-op.
	if m.taskExistsLocked(run.WorkflowRunID, run.ParentRunID, run.TaskName) {
		return store.ErrAlreadyExists
	}

	now := time.Now()
	cp := *run
	cp.CreatedAt = now
	cp.UpdatedAt = now
	m.taskRuns[run.RunID] = &cp
	m.taskIndex[run.WorkflowRunID] = append(m.taskIndex[run.WorkflowRunID], &cp)

	pk := parentKey(run.WorkflowRunID, run.ParentRunID)
	m.parentIndex[pk] = append(m.parentIndex[pk], &cp)

	return nil
}

func (m *Store) BatchCreateTaskRuns(_ context.Context, runs []*store.TaskRun) ([]*store.TaskRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	created := make([]*store.TaskRun, 0, len(runs))

	for _, run := range runs {
		// Dedup by (workflowRunID, parentRunID, taskName) — the natural unique key for a task
		// within its scope. Using workflowRunID+taskName alone is insufficient: the same
		// template name (e.g., loop body) can appear as sibling TaskRuns in different scopes.
		if m.taskExistsLocked(run.WorkflowRunID, run.ParentRunID, run.TaskName) {
			continue
		}

		cp := *run
		cp.CreatedAt = now
		cp.UpdatedAt = now
		m.taskRuns[run.RunID] = &cp
		m.taskIndex[run.WorkflowRunID] = append(m.taskIndex[run.WorkflowRunID], &cp)

		pk := parentKey(run.WorkflowRunID, run.ParentRunID)
		m.parentIndex[pk] = append(m.parentIndex[pk], &cp)

		retCp := cp
		created = append(created, &retCp)
	}

	return created, nil
}

func (m *Store) GetTaskRun(_ context.Context, taskRunID uint64) (*store.TaskRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	run, ok := m.taskRuns[taskRunID]
	if !ok {
		return nil, fmt.Errorf("task run %d: %w", taskRunID, store.ErrNotFound)
	}
	cp := *run
	return &cp, nil
}

func (m *Store) UpdateTaskRunCAS(_ context.Context, taskRunID uint64, expected, target model.Phase, msg string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.taskRuns[taskRunID]
	if !ok {
		return false, fmt.Errorf("task run %d: %w", taskRunID, store.ErrNotFound)
	}
	if existing.Status != expected {
		// Another writer already transitioned this task — idempotent no-op.
		return false, nil
	}

	existing.Status = target
	existing.Message = msg
	existing.Version++
	existing.UpdatedAt = time.Now()

	// Propagate to indexes.
	for _, tr := range m.taskIndex[existing.WorkflowRunID] {
		if tr.RunID == taskRunID {
			tr.Status = existing.Status
			tr.Message = existing.Message
			tr.Version = existing.Version
			tr.UpdatedAt = existing.UpdatedAt
			break
		}
	}
	pk := parentKey(existing.WorkflowRunID, existing.ParentRunID)
	for _, tr := range m.parentIndex[pk] {
		if tr.RunID == taskRunID {
			tr.Status = existing.Status
			tr.Message = existing.Message
			tr.Version = existing.Version
			tr.UpdatedAt = existing.UpdatedAt
			break
		}
	}
	return true, nil
}

func (m *Store) CompleteTaskRun(_ context.Context, taskRunID uint64, phase model.Phase, msg string, outputs *model.Outputs, metrics *model.Metrics) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.taskRuns[taskRunID]
	if !ok {
		return false, fmt.Errorf("task run %d: %w", taskRunID, store.ErrNotFound)
	}
	if existing.Status != model.PhaseRunning {
		// Duplicate callback or cancel/complete race — idempotent no-op.
		return false, nil
	}

	existing.Status = phase
	existing.Message = msg
	existing.Outputs = outputs
	existing.Metrics = metrics
	existing.Version++
	existing.UpdatedAt = time.Now()

	for _, tr := range m.taskIndex[existing.WorkflowRunID] {
		if tr.RunID == taskRunID {
			tr.Status = existing.Status
			tr.Message = existing.Message
			tr.Outputs = existing.Outputs
			tr.Metrics = existing.Metrics
			tr.Version = existing.Version
			tr.UpdatedAt = existing.UpdatedAt
			break
		}
	}
	pk := parentKey(existing.WorkflowRunID, existing.ParentRunID)
	for _, tr := range m.parentIndex[pk] {
		if tr.RunID == taskRunID {
			tr.Status = existing.Status
			tr.Message = existing.Message
			tr.Outputs = existing.Outputs
			tr.Metrics = existing.Metrics
			tr.Version = existing.Version
			tr.UpdatedAt = existing.UpdatedAt
			break
		}
	}
	return true, nil
}

func (m *Store) UpdateTaskRun(_ context.Context, run *store.TaskRun) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.taskRuns[run.RunID]
	if !ok {
		return fmt.Errorf("task run %d: %w", run.RunID, store.ErrNotFound)
	}

	existing.Status = run.Status
	existing.Message = run.Message
	existing.Outputs = run.Outputs
	existing.Metrics = run.Metrics
	existing.Version++
	existing.UpdatedAt = time.Now()

	// Also update the indexed copy in taskIndex
	for _, tr := range m.taskIndex[existing.WorkflowRunID] {
		if tr.RunID == run.RunID {
			tr.Status = existing.Status
			tr.Message = existing.Message
			tr.Outputs = existing.Outputs
			tr.Metrics = existing.Metrics
			tr.Version = existing.Version
			tr.UpdatedAt = existing.UpdatedAt
			break
		}
	}

	// Also update the indexed copy in parentIndex
	pk := parentKey(existing.WorkflowRunID, existing.ParentRunID)
	for _, tr := range m.parentIndex[pk] {
		if tr.RunID == run.RunID {
			tr.Status = existing.Status
			tr.Message = existing.Message
			tr.Outputs = existing.Outputs
			tr.Metrics = existing.Metrics
			tr.Version = existing.Version
			tr.UpdatedAt = existing.UpdatedAt
			break
		}
	}

	return nil
}

func (m *Store) ListTaskRuns(_ context.Context, workflowRunID uint64) ([]*store.TaskRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	runs := m.taskIndex[workflowRunID]
	result := make([]*store.TaskRun, len(runs))
	for i, r := range runs {
		cp := *r
		result[i] = &cp
	}
	return result, nil
}

func (m *Store) ListTaskRunsByParent(_ context.Context, workflowRunID uint64, parentRunID uint64) ([]*store.TaskRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pk := parentKey(workflowRunID, parentRunID)
	runs := m.parentIndex[pk]
	result := make([]*store.TaskRun, len(runs))
	for i, r := range runs {
		cp := *r
		result[i] = &cp
	}
	return result, nil
}

// --- WorkflowTemplateStore ---

func (m *Store) GetWorkflowTemplate(_ context.Context, namespace, name string) (*model.WorkflowTemplate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := namespace + "/" + name
	tmpl, ok := m.templates[key]
	if !ok {
		return nil, fmt.Errorf("workflow template %s: %w", key, store.ErrNotFound)
	}
	return tmpl, nil
}

// RegisterTemplate adds a WorkflowTemplate (for testing convenience).
func (m *Store) RegisterTemplate(tmpl *model.WorkflowTemplate) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ns := tmpl.Metadata.Namespace
	if ns == "" {
		ns = "default"
	}
	key := ns + "/" + tmpl.Metadata.Name
	m.templates[key] = tmpl
}

// taskExistsLocked checks if a task run exists for the given (workflowRunID, parentRunID, taskName) triple.
// Must be called with m.mu held.
func (m *Store) taskExistsLocked(workflowRunID, parentRunID uint64, taskName string) bool {
	pk := parentKey(workflowRunID, parentRunID)
	for _, tr := range m.parentIndex[pk] {
		if tr.TaskName == taskName {
			return true
		}
	}
	return false
}
