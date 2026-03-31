package ivars_test

import (
	"encoding/json"
	"testing"

	ivars "github.com/BabySid/aether/internal/vars"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// rawJSON marshals v into JSON bytes for use in test Parameters.
func rawJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// ---------------------------------------------------------------------------
// WorkflowArgsSource
// ---------------------------------------------------------------------------

func TestWorkflowArgsSource_Namespace(t *testing.T) {
	p := &ivars.WorkflowArgsSource{}
	if p.Namespace() != "workflow" {
		t.Fatalf("expected namespace 'workflow', got %q", p.Namespace())
	}
}

func TestWorkflowArgsSource_Nil(t *testing.T) {
	p := &ivars.WorkflowArgsSource{Args: nil}
	if len(p.Vars()) != 0 {
		t.Fatal("expected nil/empty map for nil Args")
	}
}

func TestWorkflowArgsSource_Values(t *testing.T) {
	p := &ivars.WorkflowArgsSource{Args: &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "env", Value: rawJSON("prod")},
			{Name: "count", Value: rawJSON(3)},
		},
	}}
	m := p.Vars()
	if m["workflow.parameters.env"] != "prod" {
		t.Fatalf("expected prod, got %v", m["workflow.parameters.env"])
	}
	if m["workflow.parameters.count"] != float64(3) {
		t.Fatalf("expected 3.0, got %v", m["workflow.parameters.count"])
	}
}

func TestWorkflowArgsSource_FallbackToDefault(t *testing.T) {
	p := &ivars.WorkflowArgsSource{Args: &model.Arguments{
		Parameters: []model.Parameter{
			{Name: "region", Default: rawJSON("us-east-1")},
		},
	}}
	m := p.Vars()
	if m["workflow.parameters.region"] != "us-east-1" {
		t.Fatalf("expected us-east-1, got %v", m["workflow.parameters.region"])
	}
}

// ---------------------------------------------------------------------------
// ResolvedInputsSource
// ---------------------------------------------------------------------------

func TestResolvedInputsSource_Namespace(t *testing.T) {
	p := &ivars.ResolvedInputsSource{}
	if p.Namespace() != "inputs" {
		t.Fatalf("expected namespace 'inputs', got %q", p.Namespace())
	}
}

func TestResolvedInputsSource_Nil(t *testing.T) {
	p := &ivars.ResolvedInputsSource{Inputs: nil}
	if len(p.Vars()) != 0 {
		t.Fatal("expected nil/empty map for nil Inputs")
	}
}

func TestResolvedInputsSource_Values(t *testing.T) {
	p := &ivars.ResolvedInputsSource{Inputs: &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "image", Value: rawJSON("ubuntu:22.04")},
		},
	}}
	m := p.Vars()
	if m["inputs.parameters.image"] != "ubuntu:22.04" {
		t.Fatalf("expected ubuntu:22.04, got %v", m["inputs.parameters.image"])
	}
}

// ---------------------------------------------------------------------------
// SiblingTaskRunsSource
// ---------------------------------------------------------------------------

func TestSiblingTaskRunsSource_Namespace(t *testing.T) {
	p := &ivars.SiblingTaskRunsSource{}
	if p.Namespace() != "tasks" {
		t.Fatalf("expected namespace 'tasks', got %q", p.Namespace())
	}
}

func TestSiblingTaskRunsSource_Empty(t *testing.T) {
	p := &ivars.SiblingTaskRunsSource{Runs: nil}
	if len(p.Vars()) != 0 {
		t.Fatal("expected empty map for nil Runs")
	}
}

func TestSiblingTaskRunsSource_Values(t *testing.T) {
	phase := model.PhaseSucceeded
	p := &ivars.SiblingTaskRunsSource{Runs: []*store.TaskRun{
		{
			TaskName: "step-a",
			Status:   &phase,
			Outputs: &model.Outputs{
				ExecOutputs: model.ExecOutputs{
					Code:    0,
					Message: "done",
					Parameters: []model.Parameter{
						{Name: "result", Value: rawJSON("ok")},
					},
				},
			},
		},
	}}
	m := p.Vars()
	if m["tasks.step-a.phase"] != string(model.PhaseSucceeded) {
		t.Fatalf("unexpected phase: %v", m["tasks.step-a.phase"])
	}
	if m["tasks.step-a.code"] != 0 {
		t.Fatalf("unexpected code: %v", m["tasks.step-a.code"])
	}
	if m["tasks.step-a.msg"] != "done" {
		t.Fatalf("unexpected msg: %v", m["tasks.step-a.msg"])
	}
	if m["tasks.step-a.outputs.parameters.result"] != "ok" {
		t.Fatalf("unexpected result: %v", m["tasks.step-a.outputs.parameters.result"])
	}
}

func TestSiblingTaskRunsSource_NilStatus(t *testing.T) {
	p := &ivars.SiblingTaskRunsSource{Runs: []*store.TaskRun{
		{TaskName: "pending", Status: nil},
	}}
	m := p.Vars()
	if m["tasks.pending.phase"] != "" {
		t.Fatalf("expected empty phase for nil status, got %v", m["tasks.pending.phase"])
	}
}

// ---------------------------------------------------------------------------
// LoopIterationSource
// ---------------------------------------------------------------------------

func TestLoopIterationSource_Namespace(t *testing.T) {
	p := &ivars.LoopIterationSource{}
	if p.Namespace() != "loop_iter" {
		t.Fatalf("expected namespace 'loop_iter', got %q", p.Namespace())
	}
}

func TestLoopIterationSource_Scalar(t *testing.T) {
	p := &ivars.LoopIterationSource{Index: 2, Item: "file.txt"}
	m := p.Vars()
	if m["loop_iter.index"] != 2 {
		t.Fatalf("expected index=2, got %v", m["loop_iter.index"])
	}
	if m["loop_iter.item"] != "file.txt" {
		t.Fatalf("expected item=file.txt, got %v", m["loop_iter.item"])
	}
}

func TestLoopIterationSource_Map(t *testing.T) {
	item := map[string]any{"host": "localhost", "port": 8080}
	p := &ivars.LoopIterationSource{Index: 0, Item: item}
	m := p.Vars()
	if m["loop_iter.index"] != 0 {
		t.Fatalf("expected index=0, got %v", m["loop_iter.index"])
	}
	if m["loop_iter.host"] != "localhost" {
		t.Fatalf("expected host=localhost, got %v", m["loop_iter.host"])
	}
	if m["loop_iter.port"] != 8080 {
		t.Fatalf("expected port=8080, got %v", m["loop_iter.port"])
	}
	if _, exists := m["loop_iter.item"]; exists {
		t.Fatal("loop_iter.item should not be set for map items")
	}
}

func TestLoopIterationSource_NilItem(t *testing.T) {
	p := &ivars.LoopIterationSource{Index: 1, Item: nil}
	m := p.Vars()
	if m["loop_iter.index"] != 1 {
		t.Fatalf("expected index=1, got %v", m["loop_iter.index"])
	}
	if _, exists := m["loop_iter.item"]; exists {
		t.Fatal("loop_iter.item should not be set for nil item")
	}
}
