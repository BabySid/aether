package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// ---- mock helpers ----

// mockEvaluator is a simple expr.Evaluator stub.
type mockEvaluator struct {
	// fn is called with (expression, env) and returns (result, error).
	fn func(expr string, env map[string]any) (any, error)
}

func (m *mockEvaluator) Eval(_ context.Context, expr string, env map[string]any) (any, error) {
	return m.fn(expr, env)
}

// mockSecretStore is a simple secret.Store stub.
type mockSecretStore struct {
	secrets map[string]string // "name/key" → value
}

func (s *mockSecretStore) Get(_ context.Context, name, key string) (string, error) {
	k := name + "/" + key
	if v, ok := s.secrets[k]; ok {
		return v, nil
	}
	return "", fmt.Errorf("secret %s/%s not found", name, key)
}

// rawJSON is a convenience wrapper that converts a Go value to json.RawMessage.
func rawJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// ---- ResolveInputs ----

func TestResolveInputs_NilInputs(t *testing.T) {
	rc := &ResolveContext{}
	out, err := ResolveInputs(context.Background(), nil, rc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != nil {
		t.Fatalf("expected nil output, got %+v", out)
	}
}

func TestResolveInputs_ExplicitValue(t *testing.T) {
	inputs := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "foo", Value: rawJSON("bar")},
		},
	}
	rc := &ResolveContext{}

	out, err := ResolveInputs(context.Background(), inputs, rc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.Parameters) != 1 {
		t.Fatalf("expected 1 parameter, got %d", len(out.Parameters))
	}
	if string(out.Parameters[0].Value) != `"bar"` {
		t.Errorf("expected value %q, got %q", `"bar"`, out.Parameters[0].Value)
	}
}

func TestResolveInputs_DefaultFallback(t *testing.T) {
	inputs := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "env", Default: rawJSON("production")},
		},
	}
	rc := &ResolveContext{}

	out, err := ResolveInputs(context.Background(), inputs, rc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out.Parameters[0].Value) != `"production"` {
		t.Errorf("expected default %q, got %q", `"production"`, out.Parameters[0].Value)
	}
}

func TestResolveInputs_ArtifactsPassThrough(t *testing.T) {
	art := model.Artifact{Name: "my-artifact"}
	inputs := &model.Inputs{
		Artifacts: []model.Artifact{art},
	}
	rc := &ResolveContext{}

	out, err := ResolveInputs(context.Background(), inputs, rc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.Artifacts) != 1 || out.Artifacts[0].Name != "my-artifact" {
		t.Errorf("artifact not passed through: %+v", out.Artifacts)
	}
}

func TestResolveInputs_ParameterError(t *testing.T) {
	// valueFrom with no source and no default → error
	inputs := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "x", ValueFrom: &model.ValueFrom{}},
		},
	}
	rc := &ResolveContext{}

	_, err := ResolveInputs(context.Background(), inputs, rc)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ---- resolveParameter (via ResolveInputs) ----

func TestResolveParameter_NullValueTriggersResolution(t *testing.T) {
	// Value == "null" should be treated as unset → fall through to default.
	inputs := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "p", Value: json.RawMessage(`null`), Default: rawJSON(42)},
		},
	}
	rc := &ResolveContext{}

	out, err := ResolveInputs(context.Background(), inputs, rc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out.Parameters[0].Value) != `42` {
		t.Errorf("expected default 42, got %q", out.Parameters[0].Value)
	}
}

func TestResolveParameter_ValueFromFailsDefaultApplied(t *testing.T) {
	// valueFrom references a non-existent workflow arg; default should be used.
	inputs := &model.Inputs{
		Parameters: []model.Parameter{
			{
				Name:    "region",
				Default: rawJSON("us-east-1"),
				ValueFrom: &model.ValueFrom{
					Parameter: "non-existent-param",
				},
			},
		},
	}
	rc := &ResolveContext{WfArgs: &model.Arguments{}}

	out, err := ResolveInputs(context.Background(), inputs, rc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out.Parameters[0].Value) != `"us-east-1"` {
		t.Errorf("expected default %q, got %q", `"us-east-1"`, out.Parameters[0].Value)
	}
}

func TestResolveParameter_ValueFromFailsNoDefault(t *testing.T) {
	// valueFrom fails and no default → error propagated.
	inputs := &model.Inputs{
		Parameters: []model.Parameter{
			{
				Name:      "x",
				ValueFrom: &model.ValueFrom{Parameter: "missing"},
			},
		},
	}
	rc := &ResolveContext{WfArgs: &model.Arguments{}}

	_, err := ResolveInputs(context.Background(), inputs, rc)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ---- resolveFromPath ----

func TestResolveFromPath_HappyPath(t *testing.T) {
	taskRun := &store.TaskRun{
		TaskName: "step-a",
		Outputs: &model.Outputs{
			Parameters: []model.Parameter{
				{Name: "result", Value: rawJSON("hello")},
			},
		},
	}
	rc := &ResolveContext{TaskRuns: []*store.TaskRun{taskRun}}

	val, err := resolveFromPath("tasks.step-a.outputs.parameters.result", rc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(val) != `"hello"` {
		t.Errorf("expected %q, got %q", `"hello"`, val)
	}
}

func TestResolveFromPath_InvalidFormat(t *testing.T) {
	rc := &ResolveContext{}
	_, err := resolveFromPath("step-a.result", rc)
	if err == nil {
		t.Fatal("expected error for invalid path format")
	}
}

func TestResolveFromPath_TaskNotFound(t *testing.T) {
	rc := &ResolveContext{TaskRuns: []*store.TaskRun{}}
	_, err := resolveFromPath("tasks.missing.outputs.parameters.x", rc)
	if err == nil {
		t.Fatal("expected error for missing task")
	}
}

func TestResolveFromPath_ParamNotFound(t *testing.T) {
	taskRun := &store.TaskRun{
		TaskName: "step-a",
		Outputs:  &model.Outputs{},
	}
	rc := &ResolveContext{TaskRuns: []*store.TaskRun{taskRun}}

	_, err := resolveFromPath("tasks.step-a.outputs.parameters.no-such-param", rc)
	if err == nil {
		t.Fatal("expected error for missing parameter")
	}
}

func TestResolveFromPath_TaskOutputsNil(t *testing.T) {
	// Task exists but Outputs is nil; should not panic and should return error.
	taskRun := &store.TaskRun{
		TaskName: "step-a",
		Outputs:  nil,
	}
	rc := &ResolveContext{TaskRuns: []*store.TaskRun{taskRun}}

	_, err := resolveFromPath("tasks.step-a.outputs.parameters.x", rc)
	if err == nil {
		t.Fatal("expected error when outputs is nil")
	}
}

// ---- resolveFromParameter ----

func TestResolveFromParameter_ValuePresent(t *testing.T) {
	rc := &ResolveContext{
		WfArgs: &model.Arguments{
			Parameters: []model.Parameter{
				{Name: "env", Value: rawJSON("staging")},
			},
		},
	}

	val, err := resolveFromParameter("env", rc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(val) != `"staging"` {
		t.Errorf("expected %q, got %q", `"staging"`, val)
	}
}

func TestResolveFromParameter_DefaultPresent(t *testing.T) {
	rc := &ResolveContext{
		WfArgs: &model.Arguments{
			Parameters: []model.Parameter{
				{Name: "timeout", Default: rawJSON(30)},
			},
		},
	}

	val, err := resolveFromParameter("timeout", rc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(val) != `30` {
		t.Errorf("expected 30, got %q", val)
	}
}

func TestResolveFromParameter_NotFound(t *testing.T) {
	rc := &ResolveContext{
		WfArgs: &model.Arguments{
			Parameters: []model.Parameter{
				{Name: "other", Value: rawJSON("x")},
			},
		},
	}

	_, err := resolveFromParameter("missing", rc)
	if err == nil {
		t.Fatal("expected error for missing workflow argument")
	}
}

func TestResolveFromParameter_NilWfArgs(t *testing.T) {
	rc := &ResolveContext{WfArgs: nil}
	_, err := resolveFromParameter("any", rc)
	if err == nil {
		t.Fatal("expected error when WfArgs is nil")
	}
}

// ---- resolveFromExpression ----

func TestResolveFromExpression_Success(t *testing.T) {
	eval := &mockEvaluator{
		fn: func(expr string, env map[string]any) (any, error) {
			if expr == "1 + 1" {
				return 2, nil
			}
			return nil, fmt.Errorf("unknown expr")
		},
	}
	rc := &ResolveContext{Eval: eval}

	val, err := resolveFromExpression(context.Background(), "1 + 1", rc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(val) != `2` {
		t.Errorf("expected 2, got %q", val)
	}
}

func TestResolveFromExpression_NilEvaluator(t *testing.T) {
	rc := &ResolveContext{Eval: nil}
	_, err := resolveFromExpression(context.Background(), "1+1", rc)
	if err == nil {
		t.Fatal("expected error when evaluator is nil")
	}
}

func TestResolveFromExpression_EvalError(t *testing.T) {
	eval := &mockEvaluator{
		fn: func(expr string, env map[string]any) (any, error) {
			return nil, errors.New("syntax error")
		},
	}
	rc := &ResolveContext{Eval: eval}

	_, err := resolveFromExpression(context.Background(), "bad expr", rc)
	if err == nil {
		t.Fatal("expected error from evaluator")
	}
}

func TestResolveFromExpression_UsesTaskRunEnv(t *testing.T) {
	// Verifies that the task-run environment is built and passed to Eval.
	taskRun := &store.TaskRun{
		TaskName: "step-a",
		Outputs: &model.Outputs{
			Parameters: []model.Parameter{
				{Name: "score", Value: rawJSON(99)},
			},
		},
	}

	var capturedEnv map[string]any
	eval := &mockEvaluator{
		fn: func(expr string, env map[string]any) (any, error) {
			capturedEnv = env
			return "ok", nil
		},
	}
	rc := &ResolveContext{
		Eval:     eval,
		TaskRuns: []*store.TaskRun{taskRun},
	}

	_, err := resolveFromExpression(context.Background(), "tasks.step-a.outputs.parameters.score", rc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedEnv == nil {
		t.Fatal("env should not be nil")
	}
	// BuildTaskEnv produces flat dot-notation keys, e.g. "tasks.step-a.phase".
	wantKey := "tasks.step-a.outputs.parameters.score"
	if _, ok := capturedEnv[wantKey]; !ok {
		t.Errorf("env should contain key %q; got keys: %v", wantKey, capturedEnv)
	}
}

// ---- resolveFromSecret ----

func TestResolveFromSecret_Success(t *testing.T) {
	ss := &mockSecretStore{secrets: map[string]string{"db-creds/password": "s3cr3t"}}
	rc := &ResolveContext{SecretStore: ss}

	ref := &model.SecretKeyRef{Name: "db-creds", Key: "password"}
	val, err := resolveFromSecret(context.Background(), ref, rc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// json.Marshal("s3cr3t") produces "\"s3cr3t\""
	if string(val) != `"s3cr3t"` {
		t.Errorf("expected %q, got %q", `"s3cr3t"`, val)
	}
}

func TestResolveFromSecret_NilStore(t *testing.T) {
	rc := &ResolveContext{SecretStore: nil}
	ref := &model.SecretKeyRef{Name: "db-creds", Key: "password"}

	_, err := resolveFromSecret(context.Background(), ref, rc)
	if err == nil {
		t.Fatal("expected error when SecretStore is nil")
	}
}

func TestResolveFromSecret_NotFound(t *testing.T) {
	ss := &mockSecretStore{secrets: map[string]string{}}
	rc := &ResolveContext{SecretStore: ss}

	ref := &model.SecretKeyRef{Name: "no-secret", Key: "key"}
	_, err := resolveFromSecret(context.Background(), ref, rc)
	if err == nil {
		t.Fatal("expected error for missing secret")
	}
}

// ---- resolveValueFrom: no source configured ----

func TestResolveValueFrom_NoSourceConfigured(t *testing.T) {
	inputs := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "x", ValueFrom: &model.ValueFrom{}},
		},
	}
	rc := &ResolveContext{}

	_, err := ResolveInputs(context.Background(), inputs, rc)
	if err == nil {
		t.Fatal("expected error when valueFrom has no source")
	}
}
