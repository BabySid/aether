package binding

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// ─── CollectDAGOutputs ────────────────────────────────────

func TestCollectDAGOutputs_NilDecls(t *testing.T) {
	c := NewCollector(nil, nil)
	out, err := c.CollectDAGOutputs(context.Background(), nil, nil, EvalVars{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != nil {
		t.Fatal("expected nil outputs for nil decls")
	}
}

func TestCollectDAGOutputs_ResolvedFromChildren(t *testing.T) {
	phase := model.PhaseSucceeded
	children := []*store.TaskRun{
		{
			TaskName: "compute",
			Status:   &phase,
			Outputs: &model.Outputs{
				ExecOutputs: model.ExecOutputs{
					Parameters: []model.Parameter{
						{Name: "score", Value: rawJSON(0.95)},
					},
				},
			},
		},
	}
	decls := &model.Outputs{
		ExecOutputs: model.ExecOutputs{
			Parameters: []model.Parameter{
				{
					Name:      "final-score",
					ValueFrom: &model.ValueFrom{Parameter: "tasks.compute.outputs.parameters.score"},
				},
			},
		},
	}
	c := NewCollector(nil, nil)
	out, err := c.CollectDAGOutputs(context.Background(), decls, children, EvalVars{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil || len(out.Parameters) != 1 {
		t.Fatalf("expected 1 output parameter, got %v", out)
	}
	if out.Parameters[0].Name != "final-score" {
		t.Fatalf("unexpected param name: %s", out.Parameters[0].Name)
	}
	var score float64
	_ = json.Unmarshal(out.Parameters[0].Value, &score)
	if score != 0.95 {
		t.Fatalf("expected score=0.95, got %f", score)
	}
}

// Unresolvable parameters are skipped (best-effort); all skipped → nil returned.
func TestCollectDAGOutputs_UnresolvableSkipped(t *testing.T) {
	decls := &model.Outputs{
		ExecOutputs: model.ExecOutputs{
			Parameters: []model.Parameter{
				{
					Name:      "missing-output",
					ValueFrom: &model.ValueFrom{Parameter: "tasks.nonexistent.outputs.parameters.x"},
				},
			},
		},
	}
	c := NewCollector(nil, nil)
	out, err := c.CollectDAGOutputs(context.Background(), decls, nil, EvalVars{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != nil {
		t.Fatalf("expected nil when all outputs are unresolvable, got %v", out)
	}
}

// Static value (no valueFrom) is copied directly.
func TestCollectDAGOutputs_StaticValue(t *testing.T) {
	decls := &model.Outputs{
		ExecOutputs: model.ExecOutputs{
			Parameters: []model.Parameter{
				{Name: "static", Value: rawJSON("hello")},
			},
		},
	}
	c := NewCollector(nil, nil)
	out, err := c.CollectDAGOutputs(context.Background(), decls, nil, EvalVars{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil || len(out.Parameters) != 1 {
		t.Fatalf("expected 1 output, got %v", out)
	}
	var v string
	_ = json.Unmarshal(out.Parameters[0].Value, &v)
	if v != "hello" {
		t.Fatalf("expected hello, got %s", v)
	}
}

