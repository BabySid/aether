package binding

import (
	"testing"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

func TestNewEnvBuilder_Empty(t *testing.T) {
	env := NewEnvBuilder().Build()
	if len(env) != 0 {
		t.Fatalf("expected empty env, got %d entries", len(env))
	}
}

func TestEnvBuilder_WithWorkflowArgs(t *testing.T) {
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "token", Value: rawJSON("abc123")},
			{Name: "count", Value: rawJSON(5)},
		},
	}
	env := NewEnvBuilder().WithWorkflowArgs(args).Build()

	if v, ok := env["workflow.parameters.token"]; !ok || v != "abc123" {
		t.Fatalf("unexpected token: %v", v)
	}
	// JSON number unmarshal → float64
	if v, ok := env["workflow.parameters.count"]; !ok || v != float64(5) {
		t.Fatalf("unexpected count: %v (%T)", v, v)
	}
}

func TestEnvBuilder_WithWorkflowArgs_FallsBackToDefault(t *testing.T) {
	args := &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "p", Default: rawJSON("default-val")},
		},
	}
	env := NewEnvBuilder().WithWorkflowArgs(args).Build()
	if v := env["workflow.parameters.p"]; v != "default-val" {
		t.Fatalf("expected default value, got %v", v)
	}
}

func TestEnvBuilder_WithWorkflowArgs_Nil(t *testing.T) {
	env := NewEnvBuilder().WithWorkflowArgs(nil).Build()
	if len(env) != 0 {
		t.Fatalf("expected empty env for nil args, got %d entries", len(env))
	}
}

func TestEnvBuilder_WithResolvedInputs(t *testing.T) {
	inputs := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "image", Value: rawJSON("ubuntu:22.04")},
		},
	}
	env := NewEnvBuilder().WithResolvedInputs(inputs).Build()
	if v := env["inputs.parameters.image"]; v != "ubuntu:22.04" {
		t.Fatalf("expected image value, got %v", v)
	}
}

func TestEnvBuilder_WithResolvedInputs_FallsBackToDefault(t *testing.T) {
	inputs := &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "region", Default: rawJSON("ap-east-1")},
		},
	}
	env := NewEnvBuilder().WithResolvedInputs(inputs).Build()
	if v := env["inputs.parameters.region"]; v != "ap-east-1" {
		t.Fatalf("expected default region, got %v", v)
	}
}

func TestEnvBuilder_WithSiblingTaskRuns(t *testing.T) {
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
	env := NewEnvBuilder().WithSiblingTaskRuns(runs).Build()

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

func TestEnvBuilder_WithSiblingTaskRuns_NilStatus(t *testing.T) {
	runs := []*store.TaskRun{
		{TaskName: "pending-task", Status: nil},
	}
	env := NewEnvBuilder().WithSiblingTaskRuns(runs).Build()
	if v := env["tasks.pending-task.phase"]; v != "" {
		t.Fatalf("expected empty phase string for nil status, got %v", v)
	}
}

func TestEnvBuilder_WithLoopIteration_Scalar(t *testing.T) {
	env := NewEnvBuilder().WithLoopIteration(2, "item-value").Build()
	if v := env["loop_iter.index"]; v != 2 {
		t.Fatalf("expected index=2, got %v", v)
	}
	if v := env["loop_iter.item"]; v != "item-value" {
		t.Fatalf("expected item=item-value, got %v", v)
	}
}

func TestEnvBuilder_WithLoopIteration_Map(t *testing.T) {
	item := map[string]any{"host": "localhost", "port": 8080}
	env := NewEnvBuilder().WithLoopIteration(0, item).Build()
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

func TestEnvBuilder_WithLoopIteration_NilItem(t *testing.T) {
	env := NewEnvBuilder().WithLoopIteration(1, nil).Build()
	if _, exists := env["loop_iter.item"]; exists {
		t.Fatal("loop_iter.item should not be set for nil item")
	}
	if v := env["loop_iter.index"]; v != 1 {
		t.Fatalf("expected index=1, got %v", v)
	}
}

func TestEnvBuilder_Build_IsSnapshot(t *testing.T) {
	b := NewEnvBuilder()
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
