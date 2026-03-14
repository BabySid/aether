package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// ---- expandItems ----

func TestExpandItems_Scalar(t *testing.T) {
	items := []any{"a.txt", "b.txt", "c.txt"}
	result := expandItems(items, 0)
	if len(result) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result))
	}
	if result[0]["loop_iter.item"] != "a.txt" {
		t.Errorf("expected loop_iter.item=a.txt, got %v", result[0]["loop_iter.item"])
	}
	if result[1]["loop_iter.index"] != 1 {
		t.Errorf("expected loop_iter.index=1, got %v", result[1]["loop_iter.index"])
	}
}

func TestExpandItems_Object(t *testing.T) {
	items := []any{
		map[string]interface{}{"bucket": "s3://b", "key": "c.csv"},
	}
	result := expandItems(items, 0)
	if len(result) != 1 {
		t.Fatalf("expected 1 item, got %d", len(result))
	}
	if result[0]["bucket"] != "s3://b" {
		t.Errorf("expected bucket=s3://b, got %v", result[0]["bucket"])
	}
	if result[0]["key"] != "c.csv" {
		t.Errorf("expected key=c.csv, got %v", result[0]["key"])
	}
	// system key uses loop_iter. prefix — no collision with user object fields
	if result[0]["loop_iter.index"] != 0 {
		t.Errorf("expected loop_iter.index=0, got %v", result[0]["loop_iter.index"])
	}
	// loop_iter.item must NOT be set for object items
	if _, exists := result[0]["loop_iter.item"]; exists {
		t.Error("loop_iter.item should not be set for object items")
	}
}

func TestExpandItems_MaxIterations(t *testing.T) {
	items := []any{"a", "b", "c", "d", "e"}
	result := expandItems(items, 3)
	if len(result) != 3 {
		t.Errorf("expected 3 items (capped by maxIterations), got %d", len(result))
	}
}

func TestExpandItems_MaxIterationsZeroMeansUnlimited(t *testing.T) {
	items := []any{"a", "b", "c"}
	result := expandItems(items, 0)
	if len(result) != 3 {
		t.Errorf("maxIterations=0 should be unlimited, got %d", len(result))
	}
}

// ---- expandItemsFrom ----

func TestExpandItemsFrom_GoSlice(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return []interface{}{"x.txt", "y.txt"}, nil
	}}
	result, err := expandItemsFrom(context.Background(), "expr", eval, nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 items, got %d", len(result))
	}
	if result[0]["loop_iter.item"] != "x.txt" {
		t.Errorf("expected loop_iter.item=x.txt, got %v", result[0]["loop_iter.item"])
	}
}

func TestExpandItemsFrom_JSONString(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return `["alpha","beta","gamma"]`, nil
	}}
	result, err := expandItemsFrom(context.Background(), "expr", eval, nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result))
	}
	if result[2]["loop_iter.item"] != "gamma" {
		t.Errorf("expected loop_iter.item=gamma, got %v", result[2]["loop_iter.item"])
	}
}

func TestExpandItemsFrom_NilEvaluator(t *testing.T) {
	_, err := expandItemsFrom(context.Background(), "expr", nil, nil, 0)
	if err == nil {
		t.Error("expected error when eval is nil")
	}
}

func TestExpandItemsFrom_EvalError(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return nil, errors.New("eval failed")
	}}
	_, err := expandItemsFrom(context.Background(), "expr", eval, nil, 0)
	if err == nil {
		t.Error("expected error when evaluator returns error")
	}
}

func TestExpandItemsFrom_NonArrayResult(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return 42, nil // not a slice
	}}
	_, err := expandItemsFrom(context.Background(), "expr", eval, nil, 0)
	if err == nil {
		t.Error("expected error for non-array result")
	}
}

func TestExpandItemsFrom_InvalidJSONString(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return "not-a-json-array", nil
	}}
	_, err := expandItemsFrom(context.Background(), "expr", eval, nil, 0)
	if err == nil {
		t.Error("expected error for invalid JSON string")
	}
}

func TestExpandItemsFrom_MaxIterations(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return []interface{}{"a", "b", "c", "d", "e"}, nil
	}}
	result, err := expandItemsFrom(context.Background(), "expr", eval, nil, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("expected 2 items (capped), got %d", len(result))
	}
}

// ---- ExpandLoopIterations ----

func TestExpandLoopIterations_Items(t *testing.T) {
	loop := &model.Loop{
		Items: []any{"file1.csv", "file2.csv"},
	}
	result, err := ExpandLoopIterations(context.Background(), loop, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 iterations, got %d", len(result))
	}
	if result[0]["loop_iter.item"] != "file1.csv" {
		t.Errorf("expected loop_iter.item=file1.csv, got %v", result[0]["loop_iter.item"])
	}
}

func TestExpandLoopIterations_ItemsFrom(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return []interface{}{"item-a", "item-b"}, nil
	}}
	loop := &model.Loop{
		ItemsFrom: "tasks.list.outputs.parameters.files",
	}
	result, err := ExpandLoopIterations(context.Background(), loop, eval, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 iterations, got %d", len(result))
	}
}

func TestExpandLoopIterations_RepeatCondition_ReturnsNil(t *testing.T) {
	loop := &model.Loop{
		RepeatCondition: "iteration_index < 5",
	}
	result, err := ExpandLoopIterations(context.Background(), loop, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("repeatCondition mode should return nil, got %v", result)
	}
}

func TestExpandLoopIterations_MaxIterationsAppliedToItems(t *testing.T) {
	loop := &model.Loop{
		Items:         []any{"a", "b", "c", "d"},
		MaxIterations: 2,
	}
	result, err := ExpandLoopIterations(context.Background(), loop, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("expected 2 items (MaxIterations cap), got %d", len(result))
	}
}

// ---- AggregateResults ----

func TestAggregateResults_EmptyReturnsSucceeded(t *testing.T) {
	phase, msg, outputs := AggregateResults(nil, nil)
	if phase != model.PhaseSucceeded {
		t.Errorf("expected Succeeded for empty results, got %v", phase)
	}
	if msg != "" || outputs != nil {
		t.Errorf("expected empty msg and nil outputs, got msg=%q outputs=%v", msg, outputs)
	}
}

func TestAggregateResults_DefaultStrategyIsAll(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded},
		{Index: 1, Phase: model.PhaseFailed, Message: "oops"},
	}
	phase, msg, _ := AggregateResults(results, nil)
	if phase != model.PhaseFailed {
		t.Errorf("expected Failed (strategy all), got %v", phase)
	}
	if msg == "" {
		t.Error("expected non-empty message for failed iteration")
	}
}

// ---- aggregateAll ----

func TestAggregateAll_AllSucceeded_MergesOutputs(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{{Name: "result", Value: rawJSON("A")}},
		}},
		{Index: 1, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{{Name: "result", Value: rawJSON("B")}},
		}},
	}
	phase, msg, outputs := aggregateAll(results)
	if phase != model.PhaseSucceeded {
		t.Errorf("expected Succeeded, got %v", phase)
	}
	if msg != "" {
		t.Errorf("expected empty msg, got %q", msg)
	}
	if outputs == nil || len(outputs.Parameters) != 2 {
		t.Fatalf("expected 2 merged parameters, got %v", outputs)
	}
	names := map[string]bool{}
	for _, p := range outputs.Parameters {
		names[p.Name] = true
	}
	if !names["result_0"] || !names["result_1"] {
		t.Errorf("expected result_0 and result_1 keys, got %v", names)
	}
}

func TestAggregateAll_OneFailedShortCircuits(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded},
		{Index: 1, Phase: model.PhaseFailed, Message: "disk full"},
		{Index: 2, Phase: model.PhaseSucceeded},
	}
	phase, msg, _ := aggregateAll(results)
	if phase != model.PhaseFailed {
		t.Errorf("expected Failed, got %v", phase)
	}
	if msg == "" {
		t.Error("expected non-empty message")
	}
}

func TestAggregateAll_SkippedCountsAsSuccess(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded},
		{Index: 1, Phase: model.PhaseSkipped},
	}
	phase, _, _ := aggregateAll(results)
	if phase != model.PhaseSucceeded {
		t.Errorf("Skipped should be treated as success in aggregateAll, got %v", phase)
	}
}

func TestAggregateAll_NoOutputs(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded},
	}
	_, _, outputs := aggregateAll(results)
	if outputs != nil {
		t.Errorf("expected nil outputs when no iteration has outputs, got %v", outputs)
	}
}

// ---- aggregateFirstSuccess ----

func TestAggregateFirstSuccess_FirstSucceedWins(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseFailed},
		{Index: 1, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{{Name: "url", Value: rawJSON("https://example.com")}},
		}},
		{Index: 2, Phase: model.PhaseSucceeded},
	}
	phase, _, outputs := aggregateFirstSuccess(results)
	if phase != model.PhaseSucceeded {
		t.Errorf("expected Succeeded, got %v", phase)
	}
	if outputs == nil || len(outputs.Parameters) != 1 {
		t.Errorf("expected outputs from index 1, got %v", outputs)
	}
}

func TestAggregateFirstSuccess_NoSuccessReturnsFailed(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseFailed},
		{Index: 1, Phase: model.PhaseFailed},
	}
	phase, msg, _ := aggregateFirstSuccess(results)
	if phase != model.PhaseFailed {
		t.Errorf("expected Failed, got %v", phase)
	}
	if msg == "" {
		t.Error("expected non-empty failure message")
	}
}

func TestAggregateResults_FirstSuccess_Strategy(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{{Name: "x", Value: rawJSON(1)}},
		}},
	}
	phase, _, outputs := AggregateResults(results, &model.Aggregate{Strategy: "first_success"})
	if phase != model.PhaseSucceeded {
		t.Errorf("expected Succeeded, got %v", phase)
	}
	if outputs == nil {
		t.Error("expected outputs forwarded from first success")
	}
}

// ---- aggregateQuorum ----

func TestAggregateQuorum_Passes(t *testing.T) {
	// 3 out of 5 → 3 > 5/2=2 → Succeeded
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded},
		{Index: 1, Phase: model.PhaseFailed},
		{Index: 2, Phase: model.PhaseSucceeded},
		{Index: 3, Phase: model.PhaseFailed},
		{Index: 4, Phase: model.PhaseSucceeded},
	}
	phase, _, _ := aggregateQuorum(results)
	if phase != model.PhaseSucceeded {
		t.Errorf("expected Succeeded (quorum met), got %v", phase)
	}
}

func TestAggregateQuorum_Fails(t *testing.T) {
	// 2 out of 5 → 2 > 5/2=2 is false → Failed
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded},
		{Index: 1, Phase: model.PhaseFailed},
		{Index: 2, Phase: model.PhaseFailed},
		{Index: 3, Phase: model.PhaseFailed},
		{Index: 4, Phase: model.PhaseSucceeded},
	}
	phase, msg, _ := aggregateQuorum(results)
	if phase != model.PhaseFailed {
		t.Errorf("expected Failed (quorum not met), got %v", phase)
	}
	if msg == "" {
		t.Error("expected non-empty failure message")
	}
}

func TestAggregateResults_Quorum_Strategy(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded},
		{Index: 1, Phase: model.PhaseSucceeded},
		{Index: 2, Phase: model.PhaseFailed},
	}
	// 2 out of 3 → 2 > 3/2=1 → Succeeded
	phase, _, _ := AggregateResults(results, &model.Aggregate{Strategy: "quorum"})
	if phase != model.PhaseSucceeded {
		t.Errorf("expected Succeeded (quorum via AggregateResults), got %v", phase)
	}
}

// ---- BuildRepeatEnv ----

func TestBuildRepeatEnv_NilLastRun(t *testing.T) {
	env := BuildRepeatEnv(0, nil)
	if env["iteration_index"] != 0 {
		t.Errorf("expected iteration_index=0, got %v", env["iteration_index"])
	}
	if len(env) != 1 {
		t.Errorf("expected only iteration_index, got %v", env)
	}
}

func TestBuildRepeatEnv_WithLastRun(t *testing.T) {
	lastRun := &store.TaskRun{
		TaskName: "do-work",
		Status:   model.PhaseSucceeded,
		Outputs: &model.Outputs{
			Code: 0,
			Msg:  "ok",
			Parameters: []model.Parameter{
				{Name: "count", Value: rawJSON(3)},
			},
		},
	}
	env := BuildRepeatEnv(2, lastRun)

	if env["iteration_index"] != 2 {
		t.Errorf("expected iteration_index=2, got %v", env["iteration_index"])
	}
	if env["tasks.do-work.phase"] != string(model.PhaseSucceeded) {
		t.Errorf("expected phase key, got %v", env["tasks.do-work.phase"])
	}
	if env["tasks.do-work.code"] != 0 {
		t.Errorf("expected code=0, got %v", env["tasks.do-work.code"])
	}
	if env["tasks.do-work.outputs.parameters.count"] != float64(3) {
		t.Errorf("expected count=3, got %v", env["tasks.do-work.outputs.parameters.count"])
	}
}

// ---- EvalRepeatCondition ----

func TestEvalRepeatCondition_EmptyCondition(t *testing.T) {
	ok, err := EvalRepeatCondition(context.Background(), "", nil, nil)
	if err != nil || ok {
		t.Errorf("empty condition should return false, nil; got %v, %v", ok, err)
	}
}

func TestEvalRepeatCondition_NilEvaluator(t *testing.T) {
	ok, err := EvalRepeatCondition(context.Background(), "iteration_index < 5", nil, nil)
	if err != nil || ok {
		t.Errorf("nil evaluator should return false, nil; got %v, %v", ok, err)
	}
}

func TestEvalRepeatCondition_ReturnTrue(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return true, nil
	}}
	ok, err := EvalRepeatCondition(context.Background(), "iteration_index < 3", eval, map[string]any{"iteration_index": 1})
	if err != nil || !ok {
		t.Errorf("expected true, nil; got %v, %v", ok, err)
	}
}

func TestEvalRepeatCondition_ReturnFalse(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return false, nil
	}}
	ok, err := EvalRepeatCondition(context.Background(), "iteration_index < 3", eval, map[string]any{"iteration_index": 3})
	if err != nil || ok {
		t.Errorf("expected false, nil; got %v, %v", ok, err)
	}
}

func TestEvalRepeatCondition_ReturnStringTrue(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return "true", nil
	}}
	ok, err := EvalRepeatCondition(context.Background(), "x", eval, nil)
	if err != nil || !ok {
		t.Errorf("expected true from string 'true'; got %v, %v", ok, err)
	}
}

func TestEvalRepeatCondition_EvalError(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return nil, errors.New("syntax error")
	}}
	_, err := EvalRepeatCondition(context.Background(), "bad", eval, nil)
	if err == nil {
		t.Error("expected error from evaluator")
	}
}

func TestEvalRepeatCondition_NonBooleanResult(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return 42, nil
	}}
	_, err := EvalRepeatCondition(context.Background(), "x", eval, nil)
	if err == nil {
		t.Error("expected error for non-boolean result")
	}
}

func TestEvalRepeatCondition_IterationIndexInEnv(t *testing.T) {
	var capturedEnv map[string]any
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		capturedEnv = env
		return true, nil
	}}
	env := BuildRepeatEnv(5, nil)
	_, _ = EvalRepeatCondition(context.Background(), "iteration_index < 10", eval, env)
	if capturedEnv["iteration_index"] != 5 {
		t.Errorf("expected iteration_index=5 in env, got %v", capturedEnv["iteration_index"])
	}
}
