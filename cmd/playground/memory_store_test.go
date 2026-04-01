package main

import (
	"context"
	"testing"
	"time"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

func TestMemoryStore_DeleteWorkflowRun_CascadeTaskRuns(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryStore()

	// Create a WorkflowRun with two TaskRuns.
	runID := "wf-1"
	created := model.PhaseCreated
	if err := ms.CreateWorkflowRun(ctx, &store.WorkflowRun{
		RunID:  runID,
		Status: &created,
	}); err != nil {
		t.Fatalf("CreateWorkflowRun: %v", err)
	}

	for _, name := range []string{"task-a", "task-b"} {
		if err := ms.CreateTaskRun(ctx, &store.TaskRun{
			RunID:         runID + "-" + name,
			WorkflowRunID: runID,
			TaskName:      name,
			TemplateName:  name,
			TemplateType:  "task",
			Status:        &created,
		}); err != nil {
			t.Fatalf("CreateTaskRun(%s): %v", name, err)
		}
	}

	// Verify TaskRuns exist.
	trs, _ := ms.ListTaskRuns(ctx, runID)
	if len(trs) != 2 {
		t.Fatalf("expected 2 TaskRuns before delete, got %d", len(trs))
	}

	// Delete WorkflowRun — should cascade.
	if err := ms.DeleteWorkflowRun(ctx, runID); err != nil {
		t.Fatalf("DeleteWorkflowRun: %v", err)
	}

	// WorkflowRun gone.
	if _, err := ms.GetWorkflowRun(ctx, runID); err == nil {
		t.Fatal("expected ErrNotFound after delete")
	}

	// TaskRuns cascade-deleted.
	trs, _ = ms.ListTaskRuns(ctx, runID)
	if len(trs) != 0 {
		t.Fatalf("expected 0 TaskRuns after cascade delete, got %d", len(trs))
	}

	// ListTaskRunsByParent should also be clean.
	trs, _ = ms.ListTaskRunsByParent(ctx, runID, "")
	if len(trs) != 0 {
		t.Fatalf("expected 0 in parentIndex after cascade delete, got %d", len(trs))
	}
}

func TestMemoryStore_DeleteWorkflowRun_NotFound(t *testing.T) {
	ms := NewMemoryStore()
	err := ms.DeleteWorkflowRun(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected ErrNotFound")
	}
}

func TestMemoryStore_DeleteWorkflowRun_NoEffectOnOther(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryStore()
	created := model.PhaseCreated

	// Create two WorkflowRuns each with a TaskRun.
	for _, id := range []string{"wf-1", "wf-2"} {
		_ = ms.CreateWorkflowRun(ctx, &store.WorkflowRun{RunID: id, Status: &created})
		_ = ms.CreateTaskRun(ctx, &store.TaskRun{
			RunID:         id + "-task",
			WorkflowRunID: id,
			TaskName:      "main",
			TemplateName:  "main",
			TemplateType:  "task",
			Status:        &created,
			CreatedAt:     time.Now(),
		})
	}

	// Delete wf-1.
	if err := ms.DeleteWorkflowRun(ctx, "wf-1"); err != nil {
		t.Fatalf("DeleteWorkflowRun: %v", err)
	}

	// wf-2 should be untouched.
	if _, err := ms.GetWorkflowRun(ctx, "wf-2"); err != nil {
		t.Fatalf("wf-2 should still exist: %v", err)
	}
	trs, _ := ms.ListTaskRuns(ctx, "wf-2")
	if len(trs) != 1 {
		t.Fatalf("wf-2 should still have 1 TaskRun, got %d", len(trs))
	}
}
