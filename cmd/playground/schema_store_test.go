// schema_store_test.go — unit tests for MemoryStore's SchemaStore implementation.
package main

import (
	"context"
	"testing"

	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

func makeSchema(execType string) model.ExecutorSchema {
	return executor.SchemaOf[executor.DynamicOutputs, executor.DynamicOutputs](execType, "1.0", execType+" executor")
}

func TestSchemaStore_UpsertAndList(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryStore()

	s1 := makeSchema("echo")
	s2 := makeSchema("http")

	if err := ms.UpsertSchema(ctx, "worker-1", s1); err != nil {
		t.Fatalf("UpsertSchema echo: %v", err)
	}
	if err := ms.UpsertSchema(ctx, "worker-1", s2); err != nil {
		t.Fatalf("UpsertSchema http: %v", err)
	}

	records, err := ms.ListSchemas(ctx)
	if err != nil {
		t.Fatalf("ListSchemas: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("want 2 records, got %d", len(records))
	}

	types := make(map[string]string)
	for _, r := range records {
		types[r.Schema.Type] = r.WorkerID
	}
	if types["echo"] != "worker-1" {
		t.Errorf("echo: want workerID worker-1, got %s", types["echo"])
	}
	if types["http"] != "worker-1" {
		t.Errorf("http: want workerID worker-1, got %s", types["http"])
	}
}

func TestSchemaStore_Upsert_Overwrite(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryStore()

	s1 := model.ExecutorSchema{Type: "echo", Version: "1.0"}
	s2 := model.ExecutorSchema{Type: "echo", Version: "2.0"}

	_ = ms.UpsertSchema(ctx, "worker-1", s1)
	_ = ms.UpsertSchema(ctx, "worker-1", s2)

	records, _ := ms.ListSchemas(ctx)
	if len(records) != 1 {
		t.Fatalf("want 1 record after overwrite, got %d", len(records))
	}
	if records[0].Schema.Version != "2.0" {
		t.Errorf("want version 2.0 after overwrite, got %s", records[0].Schema.Version)
	}
}

func TestSchemaStore_DeleteByExecType(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryStore()

	_ = ms.UpsertSchema(ctx, "worker-1", makeSchema("echo"))
	_ = ms.UpsertSchema(ctx, "worker-1", makeSchema("http"))
	_ = ms.UpsertSchema(ctx, "worker-2", makeSchema("shell"))

	if err := ms.DeleteSchema(ctx, "echo", ""); err != nil {
		t.Fatalf("DeleteSchema: %v", err)
	}

	records, _ := ms.ListSchemas(ctx)
	for _, r := range records {
		if r.Schema.Type == "echo" {
			t.Error("echo should have been deleted")
		}
	}
	if len(records) != 2 {
		t.Fatalf("want 2 records after delete, got %d", len(records))
	}
}

func TestSchemaStore_DeleteByWorkerID(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryStore()

	_ = ms.UpsertSchema(ctx, "worker-1", makeSchema("echo"))
	_ = ms.UpsertSchema(ctx, "worker-1", makeSchema("http"))
	_ = ms.UpsertSchema(ctx, "worker-2", makeSchema("shell"))

	if err := ms.DeleteSchema(ctx, "", "worker-1"); err != nil {
		t.Fatalf("DeleteSchema: %v", err)
	}

	records, _ := ms.ListSchemas(ctx)
	if len(records) != 1 {
		t.Fatalf("want 1 record (worker-2/shell), got %d", len(records))
	}
	if records[0].WorkerID != "worker-2" || records[0].Schema.Type != "shell" {
		t.Errorf("unexpected record: %+v", records[0])
	}
}

func TestSchemaStore_DeleteExact(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryStore()

	_ = ms.UpsertSchema(ctx, "worker-1", makeSchema("echo"))
	_ = ms.UpsertSchema(ctx, "worker-2", makeSchema("echo"))

	// Precise delete: only worker-1's echo
	if err := ms.DeleteSchema(ctx, "echo", "worker-1"); err != nil {
		t.Fatalf("DeleteSchema: %v", err)
	}

	records, _ := ms.ListSchemas(ctx)
	if len(records) != 1 {
		t.Fatalf("want 1 record, got %d", len(records))
	}
	if records[0].WorkerID != "worker-2" {
		t.Errorf("want worker-2, got %s", records[0].WorkerID)
	}
}

func TestSchemaStore_DeleteAll(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryStore()

	_ = ms.UpsertSchema(ctx, "w1", makeSchema("echo"))
	_ = ms.UpsertSchema(ctx, "w2", makeSchema("http"))

	// Both empty filters → delete all
	if err := ms.DeleteSchema(ctx, "", ""); err != nil {
		t.Fatalf("DeleteSchema: %v", err)
	}

	records, _ := ms.ListSchemas(ctx)
	if len(records) != 0 {
		t.Fatalf("want 0 records, got %d", len(records))
	}
}

func TestSchemaStore_ListEmpty(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryStore()
	records, err := ms.ListSchemas(ctx)
	if err != nil {
		t.Fatalf("ListSchemas: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("want 0 records, got %d", len(records))
	}
}

func TestSchemaStore_MultipleWorkersSameType(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryStore()

	_ = ms.UpsertSchema(ctx, "worker-1", makeSchema("echo"))
	_ = ms.UpsertSchema(ctx, "worker-2", makeSchema("echo"))

	records, _ := ms.ListSchemas(ctx)
	if len(records) != 2 {
		t.Fatalf("want 2 records (different workers, same type), got %d", len(records))
	}
}

// TestSchemaStore_ImplementsInterface ensures *MemoryStore satisfies store.SchemaStore.
func TestSchemaStore_ImplementsInterface(t *testing.T) {
	var _ store.SchemaStore = (*MemoryStore)(nil)
}

// TestSchemaStore_ImplementsStoreInterface ensures *MemoryStore satisfies store.Store.
func TestSchemaStore_ImplementsStoreInterface(t *testing.T) {
	var _ store.Store = (*MemoryStore)(nil)
}
