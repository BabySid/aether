package binding

import (
	"encoding/json"
	"testing"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
	"github.com/BabySid/aether/vars"
)

func TestNewVarBuilder_Empty(t *testing.T) {
	env := NewVarBuilder().Build()
	if len(env) != 0 {
		t.Fatalf("expected empty env, got %d entries", len(env))
	}
}

func TestVarBuilder_WithWorkflowArgs(t *testing.T) {
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "token", Value: rawJSON("abc123")},
			{Name: "count", Value: rawJSON(5)},
		},
	}
	env := NewVarBuilder().WithWorkflowArgs(args).Build()

	if v, ok := env["workflow.parameters.token"]; !ok || v != "abc123" {
		t.Fatalf("unexpected token: %v", v)
	}
	// JSON number unmarshal → float64
	if v, ok := env["workflow.parameters.count"]; !ok || v != float64(5) {
		t.Fatalf("unexpected count: %v (%T)", v, v)
	}
}

func TestVarBuilder_WithWorkflowArgs_FallsBackToDefault(t *testing.T) {
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "p", Default: rawJSON("default-val")},
		},
	}
	env := NewVarBuilder().WithWorkflowArgs(args).Build()
	if v := env["workflow.parameters.p"]; v != "default-val" {
		t.Fatalf("expected default value, got %v", v)
	}
}

func TestVarBuilder_WithWorkflowArgs_Nil(t *testing.T) {
	env := NewVarBuilder().WithWorkflowArgs(nil).Build()
	if len(env) != 0 {
		t.Fatalf("expected empty env for nil args, got %d entries", len(env))
	}
}

func TestVarBuilder_WithResolvedInputs(t *testing.T) {
	inputs := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "image", Value: rawJSON("ubuntu:22.04")},
		},
	}
	env := NewVarBuilder().WithResolvedInputs(inputs).Build()
	if v := env["inputs.parameters.image"]; v != "ubuntu:22.04" {
		t.Fatalf("expected image value, got %v", v)
	}
}

func TestVarBuilder_WithResolvedInputs_FallsBackToDefault(t *testing.T) {
	inputs := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "region", Default: rawJSON("ap-east-1")},
		},
	}
	env := NewVarBuilder().WithResolvedInputs(inputs).Build()
	if v := env["inputs.parameters.region"]; v != "ap-east-1" {
		t.Fatalf("expected default region, got %v", v)
	}
}

func TestVarBuilder_WithSiblingTaskRuns(t *testing.T) {
	phase := model.PhaseSucceeded
	runs := []*store.TaskRun{
		{
			TaskName: "step-a",
			Status:   &phase,
			Outputs: &model.Outputs{
				ExecOutputs: model.ExecOutputs{
					Code:    0,
					Message: "ok",
					Parameters: []model.Parameter{
						{Name: "result", Value: rawJSON("done")},
					},
				},
			},
		},
	}
	env := NewVarBuilder().WithSiblingTaskRuns(runs).Build()

	if v := env["tasks.step-a.phase"]; v != string(model.PhaseSucceeded) {
		t.Fatalf("unexpected phase: %v", v)
	}
	if v := env["tasks.step-a.code"]; v != 0 {
		t.Fatalf("unexpected code: %v", v)
	}
	if v := env["tasks.step-a.msg"]; v != "ok" {
		t.Fatalf("unexpected msg: %v", v)
	}
	if v := env["tasks.step-a.outputs.parameters.result"]; v != "done" {
		t.Fatalf("unexpected result param: %v", v)
	}
}

func TestVarBuilder_WithSiblingTaskRuns_NilStatus(t *testing.T) {
	runs := []*store.TaskRun{
		{TaskName: "pending-task", Status: nil},
	}
	env := NewVarBuilder().WithSiblingTaskRuns(runs).Build()
	if v := env["tasks.pending-task.phase"]; v != "" {
		t.Fatalf("expected empty phase string for nil status, got %v", v)
	}
}

func TestVarBuilder_WithLoopIteration_Scalar(t *testing.T) {
	env := NewVarBuilder().WithLoopIteration(2, "item-value").Build()
	if v := env["loop_iter.index"]; v != 2 {
		t.Fatalf("expected index=2, got %v", v)
	}
	if v := env["loop_iter.item"]; v != "item-value" {
		t.Fatalf("expected item=item-value, got %v", v)
	}
}

func TestVarBuilder_WithLoopIteration_Map(t *testing.T) {
	item := map[string]any{"host": "localhost", "port": 8080}
	env := NewVarBuilder().WithLoopIteration(0, item).Build()
	if v := env["loop_iter.host"]; v != "localhost" {
		t.Fatalf("expected host=localhost, got %v", v)
	}
	if v := env["loop_iter.port"]; v != 8080 {
		t.Fatalf("expected port=8080, got %v", v)
	}
	// "loop_iter.item" must NOT be set for map items
	if _, exists := env["loop_iter.item"]; exists {
		t.Fatal("loop_iter.item should not be set for map items")
	}
}

func TestVarBuilder_WithLoopIteration_NilItem(t *testing.T) {
	env := NewVarBuilder().WithLoopIteration(1, nil).Build()
	if _, exists := env["loop_iter.item"]; exists {
		t.Fatal("loop_iter.item should not be set for nil item")
	}
	if v := env["loop_iter.index"]; v != 1 {
		t.Fatalf("expected index=1, got %v", v)
	}
}

func TestVarBuilder_Build_IsSnapshot(t *testing.T) {
	b := NewVarBuilder()
	env1 := b.Build()

	// Mutate builder after first Build
	b.WithWorkflowArgs(&model.Arguments{
		Parameters: []model.Parameter{{Name: "x", Value: rawJSON("X")}},
	})
	env2 := b.Build()

	// env1 must be unaffected
	if _, exists := env1["workflow.parameters.x"]; exists {
		t.Fatal("Build should return an independent snapshot")
	}
	if _, exists := env2["workflow.parameters.x"]; !exists {
		t.Fatal("env2 should contain the new key")
	}
}

// ---- WithSource ----

// testSource is a custom vars.Source for testing.
type testSource struct {
	ns   string
	data map[string]any
}

func (s *testSource) Namespace() string    { return s.ns }
func (s *testSource) Vars() map[string]any { return s.data }

// verify testSource implements vars.Source
var _ vars.Source = (*testSource)(nil)

func TestVarBuilder_WithSource(t *testing.T) {
	src := &testSource{
		ns:   "custom",
		data: map[string]any{"custom.region": "us-east-1", "custom.env": "prod"},
	}
	env := NewVarBuilder().WithSource(src).Build()
	if v := env["custom.region"]; v != "us-east-1" {
		t.Errorf("expected us-east-1, got %v", v)
	}
	if v := env["custom.env"]; v != "prod" {
		t.Errorf("expected prod, got %v", v)
	}
}

func TestVarBuilder_WithSource_Nil(t *testing.T) {
	env := NewVarBuilder().WithSource(nil).Build()
	if len(env) != 0 {
		t.Fatalf("expected empty env for nil source, got %d entries", len(env))
	}
}

func TestVarBuilder_WithSource_SameNamespaceOverwrites(t *testing.T) {
	src1 := &testSource{ns: "ns", data: map[string]any{"ns.key": "first"}}
	src2 := &testSource{ns: "ns", data: map[string]any{"ns.key": "second"}}
	env := NewVarBuilder().WithSource(src1).WithSource(src2).Build()
	if v := env["ns.key"]; v != "second" {
		t.Errorf("expected later source to overwrite, got %v", v)
	}
}

// ---- Build(namespaces...) ----

func TestVarBuilder_Build_SelectiveNamespaces(t *testing.T) {
	srcA := &testSource{ns: "alpha", data: map[string]any{"alpha.x": 1}}
	srcB := &testSource{ns: "beta", data: map[string]any{"beta.y": 2}}
	srcC := &testSource{ns: "gamma", data: map[string]any{"gamma.z": 3}}

	b := NewVarBuilder().WithSource(srcA).WithSource(srcB).WithSource(srcC)

	// Only request alpha and gamma
	env := b.Build("alpha", "gamma")
	if v := env["alpha.x"]; v != 1 {
		t.Errorf("expected alpha.x=1, got %v", v)
	}
	if v := env["gamma.z"]; v != 3 {
		t.Errorf("expected gamma.z=3, got %v", v)
	}
	if _, exists := env["beta.y"]; exists {
		t.Error("beta namespace should not be included")
	}
}

func TestVarBuilder_Build_UnknownNamespace(t *testing.T) {
	src := &testSource{ns: "alpha", data: map[string]any{"alpha.x": 1}}
	env := NewVarBuilder().WithSource(src).Build("nonexistent")
	if len(env) != 0 {
		t.Fatalf("expected empty env for unknown namespace, got %d entries", len(env))
	}
}

func TestVarBuilder_Build_MixedBuiltinAndCustomSource(t *testing.T) {
	custom := &testSource{ns: "tenant", data: map[string]any{"tenant.id": "t-123"}}
	args := &model.Arguments{
		Parameters: []model.Parameter{{Name: "env", Value: json.RawMessage(`"staging"`)}},
	}
	env := NewVarBuilder().WithWorkflowArgs(args).WithSource(custom).Build()
	if v := env["workflow.parameters.env"]; v != "staging" {
		t.Errorf("expected staging, got %v", v)
	}
	if v := env["tenant.id"]; v != "t-123" {
		t.Errorf("expected t-123, got %v", v)
	}
}
