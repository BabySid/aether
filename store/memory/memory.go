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

// Store is an in-memory implementation of store.Store.
// Safe for concurrent use. Intended for standalone / testing scenarios.
type Store struct {
	mu           sync.RWMutex
	workflowRuns map[uint64]*store.WorkflowRun
	taskRuns     map[uint64]*store.TaskRun
	taskIndex    map[uint64][]*store.TaskRun        // workflowRunID -> task runs
	templates    map[string]*model.WorkflowTemplate // "namespace/name" -> template
}

// New creates a new memory Store.
func New() *Store {
	return &Store{
		workflowRuns: make(map[uint64]*store.WorkflowRun),
		taskRuns:     make(map[uint64]*store.TaskRun),
		taskIndex:    make(map[uint64][]*store.TaskRun),
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

	if _, exists := m.taskRuns[run.RunID]; exists {
		return fmt.Errorf("task run %d already exists", run.RunID)
	}

	now := time.Now()
	cp := *run
	cp.CreatedAt = now
	cp.UpdatedAt = now
	m.taskRuns[run.RunID] = &cp
	m.taskIndex[run.WorkflowRunID] = append(m.taskIndex[run.WorkflowRunID], &cp)
	return nil
}

func (m *Store) BatchCreateTaskRuns(_ context.Context, runs []*store.TaskRun) ([]*store.TaskRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	created := make([]*store.TaskRun, 0, len(runs))

	for _, run := range runs {
		// Dedup by (workflowRunID, taskName)
		if m.taskExistsLocked(run.WorkflowRunID, run.TaskName) {
			continue
		}

		cp := *run
		cp.CreatedAt = now
		cp.UpdatedAt = now
		m.taskRuns[run.RunID] = &cp
		m.taskIndex[run.WorkflowRunID] = append(m.taskIndex[run.WorkflowRunID], &cp)

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

	// Also update the indexed copy
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

// taskExistsLocked checks if a task run exists for the given workflow and task name.
// Must be called with m.mu held.
func (m *Store) taskExistsLocked(workflowRunID uint64, taskName string) bool {
	for _, tr := range m.taskIndex[workflowRunID] {
		if tr.TaskName == taskName {
			return true
		}
	}
	return false
}
