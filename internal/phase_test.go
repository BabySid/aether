package internal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/model"
)

// captureEval is a mock Evaluator that captures the env passed to it.
type captureEval struct {
	env map[string]any
}

func (e *captureEval) Eval(_ context.Context, _ string, env map[string]any) (any, error) {
	e.env = env
	return true, nil
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

	EvalPhaseConditions(context.Background(), conditions, eval, result)

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
