package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

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
