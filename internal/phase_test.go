package internal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/model"
)

// mockEval is a configurable mock Evaluator.
type mockEval struct {
	fn func(expr string, env map[string]any) (any, error)
}

func (e *mockEval) Eval(_ context.Context, expr string, env map[string]any) (any, error) {
	return e.fn(expr, env)
}

// ---- CodeToPhase ----

func TestCodeToPhase(t *testing.T) {
	tests := []struct {
		code int
		want model.Phase
	}{
		{model.ExecCodeSucceeded, model.PhaseSucceeded},
		{model.ExecCodeSuspended, model.PhaseSuspended},
		{model.ExecCodeFailed, model.PhaseFailed},
		{model.ExecCodeError, model.PhaseError},
		{model.ExecCodeTimeout, model.PhaseTimeout},
		{999, model.PhaseError},  // unknown code → PhaseError
		{-1, model.PhaseError},   // negative code → PhaseError
	}
	for _, tt := range tests {
		got := CodeToPhase(tt.code)
		if got != tt.want {
			t.Errorf("CodeToPhase(%d) = %q, want %q", tt.code, got, tt.want)
		}
	}
}

// ---- EvalPhaseConditions ----

func TestEvalPhaseConditions_NilConditions(t *testing.T) {
	result := &broker.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeFailed, Message: "boom"},
	}
	phase := EvalPhaseConditions(context.Background(), nil, &mockEval{fn: func(string, map[string]any) (any, error) {
		t.Fatal("eval should not be called when conditions=nil")
		return nil, nil
	}}, result, nil)
	if phase != model.PhaseFailed {
		t.Errorf("expected PhaseFailed, got %q", phase)
	}
}

func TestEvalPhaseConditions_NilEvaluator(t *testing.T) {
	result := &broker.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeSucceeded},
	}
	conditions := &model.PhaseConditions{Failed: "true"}
	phase := EvalPhaseConditions(context.Background(), conditions, nil, result, nil)
	if phase != model.PhaseSucceeded {
		t.Errorf("expected PhaseSucceeded (base phase), got %q", phase)
	}
}

func TestEvalPhaseConditions_NilExecOutputs(t *testing.T) {
	result := &broker.TaskResult{ExecOutputs: nil}
	phase := EvalPhaseConditions(context.Background(), nil, nil, result, nil)
	// code=0 → PhaseSucceeded
	if phase != model.PhaseSucceeded {
		t.Errorf("expected PhaseSucceeded for nil ExecOutputs, got %q", phase)
	}
}

func TestEvalPhaseConditions_SucceededOverride(t *testing.T) {
	eval := &mockEval{fn: func(string, map[string]any) (any, error) { return true, nil }}
	result := &broker.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeFailed},
	}
	conditions := &model.PhaseConditions{Succeeded: "always-true"}
	phase := EvalPhaseConditions(context.Background(), conditions, eval, result, nil)
	if phase != model.PhaseSucceeded {
		t.Errorf("expected PhaseSucceeded override, got %q", phase)
	}
}

func TestEvalPhaseConditions_FailedOverride(t *testing.T) {
	eval := &mockEval{fn: func(expr string, _ map[string]any) (any, error) {
		if expr == "fail-cond" {
			return true, nil
		}
		return false, nil
	}}
	result := &broker.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeSucceeded},
	}
	conditions := &model.PhaseConditions{Succeeded: "succ-cond", Failed: "fail-cond"}
	phase := EvalPhaseConditions(context.Background(), conditions, eval, result, nil)
	// Succeeded is checked first but returns false → Failed returns true
	if phase != model.PhaseFailed {
		t.Errorf("expected PhaseFailed override, got %q", phase)
	}
}

func TestEvalPhaseConditions_ErrorOverride(t *testing.T) {
	eval := &mockEval{fn: func(expr string, _ map[string]any) (any, error) {
		if expr == "err-cond" {
			return true, nil
		}
		return false, nil
	}}
	result := &broker.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeSucceeded},
	}
	conditions := &model.PhaseConditions{Error: "err-cond"}
	phase := EvalPhaseConditions(context.Background(), conditions, eval, result, nil)
	if phase != model.PhaseError {
		t.Errorf("expected PhaseError override, got %q", phase)
	}
}

func TestEvalPhaseConditions_NoConditionMatch_FallsBackToBase(t *testing.T) {
	eval := &mockEval{fn: func(string, map[string]any) (any, error) { return false, nil }}
	result := &broker.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeTimeout},
	}
	conditions := &model.PhaseConditions{Succeeded: "x", Failed: "y", Error: "z"}
	phase := EvalPhaseConditions(context.Background(), conditions, eval, result, nil)
	if phase != model.PhaseTimeout {
		t.Errorf("expected PhaseTimeout fallback, got %q", phase)
	}
}

func TestEvalPhaseConditions_EvalError_TreatedAsFalse(t *testing.T) {
	eval := &mockEval{fn: func(string, map[string]any) (any, error) {
		return nil, errors.New("eval failed")
	}}
	result := &broker.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeFailed},
	}
	conditions := &model.PhaseConditions{Succeeded: "broken"}
	phase := EvalPhaseConditions(context.Background(), conditions, eval, result, nil)
	// eval error → condition treated as false → base phase returned
	if phase != model.PhaseFailed {
		t.Errorf("expected PhaseFailed fallback on eval error, got %q", phase)
	}
}

func TestEvalPhaseConditions_SucceededPriority(t *testing.T) {
	// All conditions return true — succeeded has highest priority
	eval := &mockEval{fn: func(string, map[string]any) (any, error) { return true, nil }}
	result := &broker.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeError},
	}
	conditions := &model.PhaseConditions{Succeeded: "a", Failed: "b", Error: "c"}
	phase := EvalPhaseConditions(context.Background(), conditions, eval, result, nil)
	if phase != model.PhaseSucceeded {
		t.Errorf("expected PhaseSucceeded (highest priority), got %q", phase)
	}
}

// ---- EvalPhaseConditions env ----

// captureEval is a mock Evaluator that captures the env passed to it.
type captureEval struct {
	env map[string]any
}

func (e *captureEval) Eval(_ context.Context, _ string, env map[string]any) (any, error) {
	e.env = env
	return true, nil
}

func TestEvalPhaseConditions_EnvContainsBaseFields(t *testing.T) {
	eval := &captureEval{}
	result := &broker.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeFailed, Message: "timeout upstream"},
	}
	conditions := &model.PhaseConditions{Succeeded: "check"}
	EvalPhaseConditions(context.Background(), conditions, eval, result, nil)

	if eval.env["phase"] != string(model.PhaseFailed) {
		t.Errorf("expected phase=%q, got %v", model.PhaseFailed, eval.env["phase"])
	}
	if eval.env["code"] != model.ExecCodeFailed {
		t.Errorf("expected code=%d, got %v", model.ExecCodeFailed, eval.env["code"])
	}
	if eval.env["msg"] != "timeout upstream" {
		t.Errorf("expected msg=%q, got %v", "timeout upstream", eval.env["msg"])
	}
}

func TestEvalPhaseConditions_OutputParamTypes(t *testing.T) {
	eval := &captureEval{}
	result := &broker.TaskResult{
		ExecOutputs: &model.ExecOutputs{
			Code:    0,
			Message: "ok",
			Parameters: []model.Parameter{
				{Name: "count", Value: json.RawMessage(`42`)},
				{Name: "ratio", Value: json.RawMessage(`3.14`)},
				{Name: "msg", Value: json.RawMessage(`"hello"`)},
				{Name: "flag", Value: json.RawMessage(`true`)},
			},
		},
	}
	conditions := &model.PhaseConditions{Succeeded: "true"}

	EvalPhaseConditions(context.Background(), conditions, eval, result, nil)

	tests := []struct {
		key      string
		wantType string
		wantVal  any
	}{
		{"outputs.parameters.count", "float64", float64(42)},
		{"outputs.parameters.ratio", "float64", float64(3.14)},
		{"outputs.parameters.msg", "string", "hello"},
		{"outputs.parameters.flag", "bool", true},
	}
	for _, tt := range tests {
		v, ok := eval.env[tt.key]
		if !ok {
			t.Errorf("env missing key %q", tt.key)
			continue
		}
		if v != tt.wantVal {
			t.Errorf("env[%q] = %v (%T), want %v (%s)", tt.key, v, v, tt.wantVal, tt.wantType)
		}
	}
}

// ---- unmarshalParam ----

func TestUnmarshalParam(t *testing.T) {
	tests := []struct {
		name string
		raw  json.RawMessage
		want any
	}{
		{"null", json.RawMessage(`null`), nil},
		{"empty", json.RawMessage(``), nil},
		{"number", json.RawMessage(`42`), float64(42)},
		{"string", json.RawMessage(`"hello"`), "hello"},
		{"bool", json.RawMessage(`true`), true},
	}
	for _, tt := range tests {
		got := unmarshalParam(tt.raw)
		if got != tt.want {
			t.Errorf("unmarshalParam(%s) = %v (%T), want %v", tt.name, got, got, tt.want)
		}
	}
}
