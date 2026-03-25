package binding

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/BabySid/aether/model"
)

func TestBinder_Bind_NilDeclsNilArgs(t *testing.T) {
	b := NewBinder(nil, nil)
	result, err := b.Bind(context.Background(), nil, nil, EvalEnv{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Parameters) != 0 {
		t.Fatalf("expected 0 parameters, got %d", len(result.Parameters))
	}
}

// Priority 1: arg.Value (non-null) wins over decl.Value.
func TestBinder_Bind_ArgValueOverridesDecl(t *testing.T) {
	decls := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "p", Value: rawJSON("decl-value")},
		},
	}
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "p", Value: rawJSON("arg-value")},
		},
	}
	b := NewBinder(nil, nil)
	result, err := b.Bind(context.Background(), decls, args, EvalEnv{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got string
	_ = json.Unmarshal(result.Parameters[0].Value, &got)
	if got != "arg-value" {
		t.Fatalf("expected arg-value, got %s", got)
	}
}

// Priority 2: arg.ValueFrom.parameter resolves from env.
func TestBinder_Bind_ArgValueFromParameter(t *testing.T) {
	env := EvalEnv{"workflow.parameters.token": "wf-token"}
	decls := &model.Inputs{
		Parameters: []model.Parameter{{Name: "tok"}},
	}
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "tok", ValueFrom: &model.ValueFrom{Parameter: "workflow.parameters.token"}},
		},
	}
	b := NewBinder(nil, nil)
	result, err := b.Bind(context.Background(), decls, args, env)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got string
	_ = json.Unmarshal(result.Parameters[0].Value, &got)
	if got != "wf-token" {
		t.Fatalf("expected wf-token, got %s", got)
	}
}

// Priority 3: decl.Value used when no arg override.
func TestBinder_Bind_DeclValueFallback(t *testing.T) {
	decls := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "p", Value: rawJSON("decl-explicit")},
		},
	}
	b := NewBinder(nil, nil)
	result, err := b.Bind(context.Background(), decls, nil, EvalEnv{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got string
	_ = json.Unmarshal(result.Parameters[0].Value, &got)
	if got != "decl-explicit" {
		t.Fatalf("expected decl-explicit, got %s", got)
	}
}

// Priority 4: decl.Default as final fallback.
func TestBinder_Bind_DefaultFallback(t *testing.T) {
	decls := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "p", Default: rawJSON("fallback")},
		},
	}
	b := NewBinder(nil, nil)
	result, err := b.Bind(context.Background(), decls, nil, EvalEnv{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got string
	_ = json.Unmarshal(result.Parameters[0].Value, &got)
	if got != "fallback" {
		t.Fatalf("expected fallback, got %s", got)
	}
}

// Undeclared args are appended as pass-through.
func TestBinder_Bind_UndeclaredArgPassThrough(t *testing.T) {
	decls := &model.Inputs{
		Parameters: []model.Parameter{{Name: "declared"}},
	}
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "declared", Value: rawJSON("d-val")},
			{Name: "extra", Value: rawJSON("e-val")},
		},
	}
	b := NewBinder(nil, nil)
	result, err := b.Bind(context.Background(), decls, args, EvalEnv{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Parameters) != 2 {
		t.Fatalf("expected 2 parameters (declared + extra), got %d", len(result.Parameters))
	}
	names := map[string]bool{}
	for _, p := range result.Parameters {
		names[p.Name] = true
	}
	if !names["declared"] || !names["extra"] {
		t.Fatalf("unexpected parameter set: %v", names)
	}
}

// valueFrom.path looks up env by key.
func TestBinder_Bind_ArgValueFrom_PathLookup(t *testing.T) {
	env := EvalEnv{"tasks.step1.outputs.parameters.out": "step1-out"}
	decls := &model.Inputs{
		Parameters: []model.Parameter{{Name: "p"}},
	}
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "p", ValueFrom: &model.ValueFrom{Path: "tasks.step1.outputs.parameters.out"}},
		},
	}
	b := NewBinder(nil, nil)
	result, err := b.Bind(context.Background(), decls, args, env)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got string
	_ = json.Unmarshal(result.Parameters[0].Value, &got)
	if got != "step1-out" {
		t.Fatalf("expected step1-out, got %s", got)
	}
}

// valueFrom.parameter with legacy "workflow.arguments.parameters.*" alias is normalised.
func TestBinder_Bind_ArgValueFrom_LegacyAlias(t *testing.T) {
	env := EvalEnv{"workflow.parameters.env": "prod"}
	decls := &model.Inputs{
		Parameters: []model.Parameter{{Name: "e"}},
	}
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "e", ValueFrom: &model.ValueFrom{Parameter: "workflow.arguments.parameters.env"}},
		},
	}
	b := NewBinder(nil, nil)
	result, err := b.Bind(context.Background(), decls, args, env)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got string
	_ = json.Unmarshal(result.Parameters[0].Value, &got)
	if got != "prod" {
		t.Fatalf("expected prod, got %s", got)
	}
}

// valueFrom.expression invokes the evaluator.
func TestBinder_Bind_ArgValueFrom_Expression(t *testing.T) {
	eval := &fakeEvaluator{fn: func(expr string, _ map[string]any) (any, error) {
		return "expr-result", nil
	}}
	decls := &model.Inputs{
		Parameters: []model.Parameter{{Name: "p"}},
	}
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "p", ValueFrom: &model.ValueFrom{Expression: "1+1"}},
		},
	}
	b := NewBinder(eval, nil)
	result, err := b.Bind(context.Background(), decls, args, EvalEnv{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got string
	_ = json.Unmarshal(result.Parameters[0].Value, &got)
	if got != "expr-result" {
		t.Fatalf("expected expr-result, got %s", got)
	}
}

// valueFrom.expression: {{key}} placeholders are interpolated before eval.
func TestBinder_Bind_ArgValueFrom_ExpressionWithInterpolation(t *testing.T) {
	eval := &fakeEvaluator{fn: func(expr string, _ map[string]any) (any, error) {
		// After interpolation "{{x}}+1" becomes "42+1"; echo it back.
		return expr, nil
	}}
	env := EvalEnv{"x": "42"}
	decls := &model.Inputs{Parameters: []model.Parameter{{Name: "out"}}}
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "out", ValueFrom: &model.ValueFrom{Expression: "{{x}}+1"}},
		},
	}
	b := NewBinder(eval, nil)
	result, err := b.Bind(context.Background(), decls, args, env)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got string
	_ = json.Unmarshal(result.Parameters[0].Value, &got)
	if got != "42+1" {
		t.Fatalf("expected interpolated expression 42+1, got %s", got)
	}
}

// valueFrom.secretKeyRef fetches from the secret store.
func TestBinder_Bind_ArgValueFrom_SecretKeyRef(t *testing.T) {
	ss := &fakeSecretStore{secrets: map[string]string{"my-secret/api-key": "s3cr3t"}}
	decls := &model.Inputs{
		Parameters: []model.Parameter{{Name: "key"}},
	}
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "key", ValueFrom: &model.ValueFrom{SecretKeyRef: &model.SecretKeyRef{Name: "my-secret", Key: "api-key"}}},
		},
	}
	b := NewBinder(nil, ss)
	result, err := b.Bind(context.Background(), decls, args, EvalEnv{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got string
	_ = json.Unmarshal(result.Parameters[0].Value, &got)
	if got != "s3cr3t" {
		t.Fatalf("expected s3cr3t, got %s", got)
	}
}

// Unresolvable valueFrom falls back to decl.Default.
func TestBinder_Bind_ArgValueFrom_UnresolvableFallsToDefault(t *testing.T) {
	decls := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "p", Default: rawJSON("safe-default")},
		},
	}
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "p", ValueFrom: &model.ValueFrom{Path: "nonexistent.key"}},
		},
	}
	b := NewBinder(nil, nil)
	result, err := b.Bind(context.Background(), decls, args, EvalEnv{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got string
	_ = json.Unmarshal(result.Parameters[0].Value, &got)
	if got != "safe-default" {
		t.Fatalf("expected safe-default, got %s", got)
	}
}

// Template-level decl.ValueFrom is resolved when no call-site arg is provided.
func TestBinder_Bind_DeclValueFrom(t *testing.T) {
	env := EvalEnv{"inputs.parameters.region": "us-east-1"}
	decls := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "region", ValueFrom: &model.ValueFrom{Parameter: "inputs.parameters.region"}},
		},
	}
	b := NewBinder(nil, nil)
	result, err := b.Bind(context.Background(), decls, nil, env)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got string
	_ = json.Unmarshal(result.Parameters[0].Value, &got)
	if got != "us-east-1" {
		t.Fatalf("expected us-east-1, got %s", got)
	}
}

// Artifacts are passed through unchanged.
func TestBinder_Bind_ArtifactsPassThrough(t *testing.T) {
	decls := &model.Inputs{
		Artifacts: []model.Artifact{{Name: "my-artifact"}},
	}
	b := NewBinder(nil, nil)
	result, err := b.Bind(context.Background(), decls, nil, EvalEnv{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Artifacts) != 1 || result.Artifacts[0].Name != "my-artifact" {
		t.Fatalf("artifacts not passed through")
	}
}

// arg.Value = JSON "null" must not win over decl.Default.
func TestBinder_Bind_NullArgValueSkipped(t *testing.T) {
	decls := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "p", Default: rawJSON("default")},
		},
	}
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "p", Value: json.RawMessage("null")},
		},
	}
	b := NewBinder(nil, nil)
	result, err := b.Bind(context.Background(), decls, args, EvalEnv{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got string
	_ = json.Unmarshal(result.Parameters[0].Value, &got)
	if got != "default" {
		t.Fatalf("expected default, got %s", got)
	}
}

// Empty ValueFrom (no source fields) leaves the value empty without returning an error.
func TestBinder_Bind_ValueFrom_NoSource_ValueEmpty(t *testing.T) {
	decls := &model.Inputs{Parameters: []model.Parameter{{Name: "p"}}}
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "p", ValueFrom: &model.ValueFrom{}},
		},
	}
	b := NewBinder(nil, nil)
	result, err := b.Bind(context.Background(), decls, args, EvalEnv{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Parameters[0].Value) != 0 {
		t.Fatalf("expected empty value for empty ValueFrom with no default")
	}
}
