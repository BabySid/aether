package internal

import (
	"context"
	"encoding/json"
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
		map[string]any{"bucket": "s3://b", "key": "c.csv"},
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
		return []any{"x.txt", "y.txt"}, nil
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
		return []any{"a", "b", "c", "d", "e"}, nil
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
		return []any{"item-a", "item-b"}, nil
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
		RepeatCondition: "loop_iter.index < 5",
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

func TestAggregateResults_FailedIterationShortCircuits(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded},
		{Index: 1, Phase: model.PhaseFailed, Message: "oops"},
		{Index: 2, Phase: model.PhaseSucceeded},
	}
	phase, msg, _ := AggregateResults(results, nil)
	if phase != model.PhaseFailed {
		t.Errorf("expected Failed, got %v", phase)
	}
	if msg == "" {
		t.Error("expected non-empty message for failed iteration")
	}
}

func TestAggregateResults_SkippedCountsAsSuccess(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded},
		{Index: 1, Phase: model.PhaseSkipped},
	}
	phase, _, _ := AggregateResults(results, nil)
	if phase != model.PhaseSucceeded {
		t.Errorf("Skipped should be treated as success, got %v", phase)
	}
}

// ---- strategy: last (default) ----

func TestAggregateResults_Last_Default(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{{Name: "status", Value: rawJSON("first")}},
		}},
		{Index: 1, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{{Name: "status", Value: rawJSON("last")}},
		}},
	}
	phase, _, outputs := AggregateResults(results, nil) // nil → default "last"
	if phase != model.PhaseSucceeded {
		t.Errorf("expected Succeeded, got %v", phase)
	}
	if outputs == nil || len(outputs.Parameters) != 1 {
		t.Fatalf("expected 1 parameter, got %v", outputs)
	}
	var v string
	_ = json.Unmarshal(outputs.Parameters[0].Value, &v)
	if v != "last" {
		t.Errorf("expected last iteration value 'last', got %q", v)
	}
}

func TestAggregateResults_Last_WithFilter(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded},
		{Index: 1, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{
				{Name: "status", Value: rawJSON("ok")},
				{Name: "code", Value: rawJSON(0)},
			},
		}},
	}
	phase, _, outputs := AggregateResults(results, &model.Aggregate{
		Strategy:   model.AggregateStrategyLast,
		Parameters: []string{"status"},
	})
	if phase != model.PhaseSucceeded {
		t.Errorf("expected Succeeded, got %v", phase)
	}
	if outputs == nil || len(outputs.Parameters) != 1 || outputs.Parameters[0].Name != "status" {
		t.Errorf("expected only 'status' parameter, got %v", outputs)
	}
}

func TestAggregateResults_Last_NoOutputs(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded},
	}
	_, _, outputs := AggregateResults(results, &model.Aggregate{Strategy: model.AggregateStrategyLast})
	if outputs != nil {
		t.Errorf("expected nil outputs when last iteration has no outputs, got %v", outputs)
	}
}

// ---- strategy: first ----

func TestAggregateResults_First(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{{Name: "status", Value: rawJSON("first")}},
		}},
		{Index: 1, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{{Name: "status", Value: rawJSON("last")}},
		}},
	}
	phase, _, outputs := AggregateResults(results, &model.Aggregate{Strategy: model.AggregateStrategyFirst})
	if phase != model.PhaseSucceeded {
		t.Errorf("expected Succeeded, got %v", phase)
	}
	var v string
	_ = json.Unmarshal(outputs.Parameters[0].Value, &v)
	if v != "first" {
		t.Errorf("expected first iteration value 'first', got %q", v)
	}
}

// ---- strategy: list ----

func TestAggregateResults_List_AllParams(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{
				{Name: "status", Value: rawJSON("ok")},
				{Name: "code", Value: rawJSON(0)},
			},
		}},
		{Index: 1, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{
				{Name: "status", Value: rawJSON("fail")},
				{Name: "code", Value: rawJSON(1)},
			},
		}},
		{Index: 2, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{
				{Name: "status", Value: rawJSON("ok")},
				{Name: "code", Value: rawJSON(0)},
			},
		}},
	}
	phase, _, outputs := AggregateResults(results, &model.Aggregate{Strategy: model.AggregateStrategyList})
	if phase != model.PhaseSucceeded {
		t.Fatalf("expected Succeeded, got %v", phase)
	}
	if outputs == nil || len(outputs.Parameters) != 2 {
		t.Fatalf("expected 2 parameters (status, code), got %v", outputs)
	}
	paramMap := map[string]json.RawMessage{}
	for _, p := range outputs.Parameters {
		paramMap[p.Name] = p.Value
	}
	var statuses []string
	_ = json.Unmarshal(paramMap["status"], &statuses)
	if len(statuses) != 3 || statuses[0] != "ok" || statuses[1] != "fail" || statuses[2] != "ok" {
		t.Errorf("expected status=[ok,fail,ok], got %v", statuses)
	}
}

func TestAggregateResults_List_WithFilter(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{
				{Name: "status", Value: rawJSON("ok")},
				{Name: "code", Value: rawJSON(0)},
			},
		}},
		{Index: 1, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{
				{Name: "status", Value: rawJSON("fail")},
				{Name: "code", Value: rawJSON(1)},
			},
		}},
	}
	phase, _, outputs := AggregateResults(results, &model.Aggregate{
		Strategy:   model.AggregateStrategyList,
		Parameters: []string{"status"}, // only collect "status"
	})
	if phase != model.PhaseSucceeded {
		t.Fatalf("expected Succeeded, got %v", phase)
	}
	if outputs == nil || len(outputs.Parameters) != 1 || outputs.Parameters[0].Name != "status" {
		t.Errorf("expected only 'status' parameter, got %v", outputs)
	}
}

func TestAggregateResults_List_NoOutputs(t *testing.T) {
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded},
		{Index: 1, Phase: model.PhaseSucceeded},
	}
	_, _, outputs := AggregateResults(results, &model.Aggregate{Strategy: model.AggregateStrategyList})
	if outputs != nil {
		t.Errorf("expected nil outputs when no iteration has outputs, got %v", outputs)
	}
}

func TestAggregateResults_List_MissingParamPaddedWithNull(t *testing.T) {
	// iter[1] has no "code" → code array should be [0, null, 2]
	results := []LoopIterationResult{
		{Index: 0, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{
				{Name: "status", Value: rawJSON("ok")},
				{Name: "code", Value: rawJSON(0)},
			},
		}},
		{Index: 1, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{
				{Name: "status", Value: rawJSON("succ")},
				// no "code" — should be padded with null
			},
		}},
		{Index: 2, Phase: model.PhaseSucceeded, Outputs: &model.Outputs{
			Parameters: []model.Parameter{
				{Name: "status", Value: rawJSON("ok")},
				{Name: "code", Value: rawJSON(2)},
			},
		}},
	}
	phase, _, outputs := AggregateResults(results, &model.Aggregate{Strategy: model.AggregateStrategyList})
	if phase != model.PhaseSucceeded {
		t.Fatalf("expected Succeeded, got %v", phase)
	}
	if outputs == nil || len(outputs.Parameters) != 2 {
		t.Fatalf("expected 2 parameters, got %v", outputs)
	}
	paramMap := map[string]json.RawMessage{}
	for _, p := range outputs.Parameters {
		paramMap[p.Name] = p.Value
	}

	// status: ["ok","succ","ok"] — length 3
	var statuses []string
	if err := json.Unmarshal(paramMap["status"], &statuses); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if len(statuses) != 3 || statuses[0] != "ok" || statuses[1] != "succ" || statuses[2] != "ok" {
		t.Errorf("expected status=[ok,succ,ok], got %v", statuses)
	}

	// code: [0, null, 2] — length 3, index 1 must be null
	var codes []any
	if err := json.Unmarshal(paramMap["code"], &codes); err != nil {
		t.Fatalf("unmarshal code: %v", err)
	}
	if len(codes) != 3 {
		t.Fatalf("expected code array length 3, got %d", len(codes))
	}
	if codes[1] != nil {
		t.Errorf("expected code[1]=null, got %v", codes[1])
	}
}

// ---- BuildRepeatEnv ----

func TestBuildRepeatEnv_NilLastRun(t *testing.T) {
	env := BuildRepeatEnv(0, nil)
	if env["loop_iter.index"] != 0 {
		t.Errorf("expected loop_iter.index=0, got %v", env["loop_iter.index"])
	}
	if len(env) != 1 {
		t.Errorf("expected only loop_iter.index, got %v", env)
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

	if env["loop_iter.index"] != 2 {
		t.Errorf("expected loop_iter.index=2, got %v", env["loop_iter.index"])
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
	ok, err := EvalRepeatCondition(context.Background(), "loop_iter.index < 5", nil, nil)
	if err != nil || ok {
		t.Errorf("nil evaluator should return false, nil; got %v, %v", ok, err)
	}
}

func TestEvalRepeatCondition_ReturnTrue(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return true, nil
	}}
	ok, err := EvalRepeatCondition(context.Background(), "loop_iter.index < 3", eval, map[string]any{"loop_iter.index": 1})
	if err != nil || !ok {
		t.Errorf("expected true, nil; got %v, %v", ok, err)
	}
}

func TestEvalRepeatCondition_ReturnFalse(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return false, nil
	}}
	ok, err := EvalRepeatCondition(context.Background(), "loop_iter.index < 3", eval, map[string]any{"loop_iter.index": 3})
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
	_, _ = EvalRepeatCondition(context.Background(), "loop_iter.index < 10", eval, env)
	if capturedEnv["loop_iter.index"] != 5 {
		t.Errorf("expected loop_iter.index=5 in env, got %v", capturedEnv["loop_iter.index"])
	}
}
