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
	c := NewCollector(nil)
	out, err := c.CollectDAGOutputs(context.Background(), nil, nil, EvalEnv{})
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
	c := NewCollector(nil)
	out, err := c.CollectDAGOutputs(context.Background(), decls, children, EvalEnv{})
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
	c := NewCollector(nil)
	out, err := c.CollectDAGOutputs(context.Background(), decls, nil, EvalEnv{})
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
	c := NewCollector(nil)
	out, err := c.CollectDAGOutputs(context.Background(), decls, nil, EvalEnv{})
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

// ─── CollectLoopOutputs ───────────────────────────────────

func TestCollectLoopOutputs_Empty(t *testing.T) {
	c := NewCollector(nil)
	phase, msg, out := c.CollectLoopOutputs(nil, nil)
	if phase != model.PhaseSucceeded {
		t.Fatalf("expected Succeeded for empty results, got %s", phase)
	}
	if msg != "" || out != nil {
		t.Fatalf("expected empty msg and nil outputs, got msg=%q out=%v", msg, out)
	}
}

// First non-succeeded iteration causes failure propagation.
func TestCollectLoopOutputs_FailurePropagated(t *testing.T) {
	c := NewCollector(nil)
	results := []IterationResult{
		{Index: 0, Phase: model.PhaseSucceeded},
		{Index: 1, Phase: model.PhaseFailed, Message: "exec failed"},
	}
	phase, msg, _ := c.CollectLoopOutputs(results, nil)
	if phase != model.PhaseFailed {
		t.Fatalf("expected PhaseFailed, got %s", phase)
	}
	if msg == "" {
		t.Fatal("expected non-empty error message")
	}
}

// Strategy "last" picks the last iteration's outputs.
func TestCollectLoopOutputs_StrategyLast(t *testing.T) {
	c := NewCollector(nil)
	results := []IterationResult{
		{Index: 0, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			ExecOutputs: model.ExecOutputs{Parameters: []model.Parameter{{Name: "v", Value: rawJSON("first")}}},
		}},
		{Index: 1, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			ExecOutputs: model.ExecOutputs{Parameters: []model.Parameter{{Name: "v", Value: rawJSON("last")}}},
		}},
	}
	phase, _, out := c.CollectLoopOutputs(results, &model.Aggregate{Strategy: model.AggregateStrategyLast})
	if phase != model.PhaseSucceeded {
		t.Fatalf("expected Succeeded, got %s", phase)
	}
	if out == nil || len(out.Parameters) == 0 {
		t.Fatal("expected outputs")
	}
	var v string
	_ = json.Unmarshal(out.Parameters[0].Value, &v)
	if v != "last" {
		t.Fatalf("expected last, got %s", v)
	}
}

// Strategy "first" picks the first iteration's outputs.
func TestCollectLoopOutputs_StrategyFirst(t *testing.T) {
	c := NewCollector(nil)
	results := []IterationResult{
		{Index: 0, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			ExecOutputs: model.ExecOutputs{Parameters: []model.Parameter{{Name: "v", Value: rawJSON("first")}}},
		}},
		{Index: 1, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			ExecOutputs: model.ExecOutputs{Parameters: []model.Parameter{{Name: "v", Value: rawJSON("second")}}},
		}},
	}
	phase, _, out := c.CollectLoopOutputs(results, &model.Aggregate{Strategy: model.AggregateStrategyFirst})
	if phase != model.PhaseSucceeded {
		t.Fatalf("expected Succeeded, got %s", phase)
	}
	var v string
	_ = json.Unmarshal(out.Parameters[0].Value, &v)
	if v != "first" {
		t.Fatalf("expected first, got %s", v)
	}
}

// Strategy "list" collects all iterations into a JSON array.
func TestCollectLoopOutputs_StrategyList(t *testing.T) {
	c := NewCollector(nil)
	results := []IterationResult{
		{Index: 0, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			ExecOutputs: model.ExecOutputs{Parameters: []model.Parameter{{Name: "v", Value: rawJSON("a")}}},
		}},
		{Index: 1, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			ExecOutputs: model.ExecOutputs{Parameters: []model.Parameter{{Name: "v", Value: rawJSON("b")}}},
		}},
		{Index: 2, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			ExecOutputs: model.ExecOutputs{Parameters: []model.Parameter{{Name: "v", Value: rawJSON("c")}}},
		}},
	}
	phase, _, out := c.CollectLoopOutputs(results, &model.Aggregate{Strategy: model.AggregateStrategyList})
	if phase != model.PhaseSucceeded {
		t.Fatalf("expected Succeeded, got %s", phase)
	}
	if out == nil || len(out.Parameters) != 1 {
		t.Fatalf("expected 1 aggregated parameter")
	}
	var arr []string
	_ = json.Unmarshal(out.Parameters[0].Value, &arr)
	if len(arr) != 3 || arr[0] != "a" || arr[1] != "b" || arr[2] != "c" {
		t.Fatalf("unexpected list: %v", arr)
	}
}

// Strategy "list" respects the Parameters filter.
func TestCollectLoopOutputs_StrategyList_WithFilter(t *testing.T) {
	c := NewCollector(nil)
	results := []IterationResult{
		{Index: 0, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			ExecOutputs: model.ExecOutputs{Parameters: []model.Parameter{
				{Name: "a", Value: rawJSON(1)},
				{Name: "b", Value: rawJSON(2)},
			}},
		}},
	}
	phase, _, out := c.CollectLoopOutputs(results, &model.Aggregate{
		Strategy:   model.AggregateStrategyList,
		Parameters: []string{"a"},
	})
	if phase != model.PhaseSucceeded {
		t.Fatalf("unexpected phase: %s", phase)
	}
	if out == nil || len(out.Parameters) != 1 || out.Parameters[0].Name != "a" {
		t.Fatalf("expected only parameter 'a' in output")
	}
}

// Skipped iterations do not trigger failure.
func TestCollectLoopOutputs_SkippedIterationCounts(t *testing.T) {
	c := NewCollector(nil)
	results := []IterationResult{
		{Index: 0, Phase: model.PhaseSkipped},
		{Index: 1, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			ExecOutputs: model.ExecOutputs{Parameters: []model.Parameter{{Name: "v", Value: rawJSON("ok")}}},
		}},
	}
	phase, _, _ := c.CollectLoopOutputs(results, nil)
	if phase != model.PhaseSucceeded {
		t.Fatalf("expected Succeeded when all iterations succeeded/skipped, got %s", phase)
	}
}

// nil Aggregate defaults to strategy "last".
func TestCollectLoopOutputs_NilAggregate_DefaultsToLast(t *testing.T) {
	c := NewCollector(nil)
	results := []IterationResult{
		{Index: 0, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			ExecOutputs: model.ExecOutputs{Parameters: []model.Parameter{{Name: "v", Value: rawJSON("iter0")}}},
		}},
		{Index: 1, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			ExecOutputs: model.ExecOutputs{Parameters: []model.Parameter{{Name: "v", Value: rawJSON("iter1")}}},
		}},
	}
	phase, _, out := c.CollectLoopOutputs(results, nil)
	if phase != model.PhaseSucceeded {
		t.Fatalf("expected Succeeded")
	}
	var v string
	_ = json.Unmarshal(out.Parameters[0].Value, &v)
	if v != "iter1" {
		t.Fatalf("expected last iteration value 'iter1', got %s", v)
	}
}
