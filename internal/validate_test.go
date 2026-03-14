package internal

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/BabySid/aether/model"
)

// validWorkflow returns a minimal valid workflow for testing.
// Tests can modify fields before calling Validate.
func validWorkflow() *model.Workflow {
	return &model.Workflow{
		APIVersion: "graph/v1",
		Kind:       "Workflow",
		Metadata:   model.Metadata{Name: "test-wf"},
		Spec: model.WorkflowSpec{
			Entrypoint: "main",
			Templates: []model.Template{
				{
					DAG: &model.DAG{
						Name: "main",
						Tasks: []model.Task{
							{Name: "step1", Template: "exec-a"},
						},
					},
				},
				{
					Task: &model.Task{
						Name: "exec-a",
						Executor: &model.Executor{
							Type:   "script",
							Config: json.RawMessage(`{"runtime":"bash","source":"echo hello"}`),
						},
					},
				},
			},
		},
	}
}

// --- Workflow-level tests ---

func TestValidate_ValidWorkflow(t *testing.T) {
	wf := validWorkflow()
	if err := Validate(wf); err != nil {
		t.Fatalf("expected valid workflow to pass, got: %v", err)
	}
}

func TestValidate_UnsupportedAPIVersion(t *testing.T) {
	wf := validWorkflow()
	wf.APIVersion = "v2"
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for unsupported apiVersion")
	}
	assertContains(t, err.Error(), "unsupported apiVersion")
}

func TestValidate_UnsupportedKind(t *testing.T) {
	wf := validWorkflow()
	wf.Kind = "Job"
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for unsupported kind")
	}
	assertContains(t, err.Error(), "unsupported kind")
}

func TestValidate_AllSupportedKinds(t *testing.T) {
	for _, kind := range []string{"Workflow", "CronWorkflow", "WorkflowTemplate"} {
		wf := validWorkflow()
		wf.Kind = kind
		if err := Validate(wf); err != nil {
			t.Errorf("kind %q should be valid, got: %v", kind, err)
		}
	}
}

func TestValidate_EmptyMetadataName(t *testing.T) {
	wf := validWorkflow()
	wf.Metadata.Name = ""
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for empty metadata.name")
	}
	assertContains(t, err.Error(), "metadata.name is required")
}

func TestValidate_EmptyEntrypoint(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Entrypoint = ""
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for empty entrypoint")
	}
	assertContains(t, err.Error(), "spec.entrypoint is required")
}

func TestValidate_EntrypointNotFound(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Entrypoint = "nonexistent"
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for missing entrypoint template")
	}
	assertContains(t, err.Error(), "not found in templates")
}

func TestValidate_EmptyTemplates(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates = nil
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for empty templates")
	}
	assertContains(t, err.Error(), "spec.templates is required")
}

// --- Template-level tests ---

func TestValidate_EmptyTemplateName(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates = append(wf.Spec.Templates, model.Template{
		Task: &model.Task{Name: "", Executor: &model.Executor{Type: "script"}},
	})
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for empty template name")
	}
	assertContains(t, err.Error(), "name is required")
}

func TestValidate_TemplateMustHaveExactlyOneType(t *testing.T) {
	wf := validWorkflow()
	// A template with both DAG and Task set — count == 2, should fail
	wf.Spec.Templates = append(wf.Spec.Templates, model.Template{
		DAG: &model.DAG{
			Name:  "two-type",
			Tasks: []model.Task{{Name: "a", Template: "exec-a"}},
		},
		Task: &model.Task{Name: "two-type"},
	})
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for template with both dag and task")
	}
	assertContains(t, err.Error(), "must have exactly one of dag/executor/loop")
}

func TestValidate_TemplateBothDAGAndExecutor(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates = append(wf.Spec.Templates, model.Template{
		DAG: &model.DAG{
			Name:  "both-tmpl",
			Tasks: []model.Task{{Name: "a", Template: "exec-a"}},
		},
		Task: &model.Task{
			Name:     "both-tmpl",
			Executor: &model.Executor{Type: "script"},
		},
	})
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for template with both dag and executor")
	}
	assertContains(t, err.Error(), "must have exactly one of dag/executor/loop")
}

func TestValidate_TemplateBothDAGAndLoop(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates = append(wf.Spec.Templates, model.Template{
		DAG: &model.DAG{
			Name:  "dag-loop-tmpl",
			Tasks: []model.Task{{Name: "a", Template: "exec-a"}},
		},
		Loop: &model.Loop{Name: "dag-loop-tmpl", Body: "exec-a", Items: []any{1}},
	})
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for template with both dag and loop")
	}
	assertContains(t, err.Error(), "must have exactly one of dag/executor/loop")
}

func TestValidate_ExecutorTypeRequired(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[1] = model.Template{
		Task: &model.Task{
			Name:     "exec-a",
			Executor: &model.Executor{Type: ""},
		},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for empty executor.type")
	}
	assertContains(t, err.Error(), "executor.type is required")
}

// --- DAG tests ---

func TestValidate_DAGEmptyTasks(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[0].DAG.Tasks = nil
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for empty dag.tasks")
	}
	assertContains(t, err.Error(), "dag.tasks must not be empty")
}

func TestValidate_DAGTaskNameEmpty(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[0].DAG.Tasks = []model.Task{
		{Name: "", Template: "exec-a"},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for empty task name")
	}
	assertContains(t, err.Error(), "task.name is required")
}

func TestValidate_DuplicateTaskName(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[0].DAG.Tasks = []model.Task{
		{Name: "step1", Template: "exec-a"},
		{Name: "step1", Template: "exec-a"},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for duplicate task name")
	}
	assertContains(t, err.Error(), "duplicate task name")
}

func TestValidate_TaskTemplateRequired(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[0].DAG.Tasks = []model.Task{
		{Name: "step1", Template: ""},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for empty task.Template")
	}
	assertContains(t, err.Error(), "template is required")
}

func TestValidate_TaskTemplateNotFound(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[0].DAG.Tasks = []model.Task{
		{Name: "step1", Template: "nonexistent"},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for unknown template reference")
	}
	assertContains(t, err.Error(), "references unknown template")
}

func TestValidate_DependencyUnknownTask(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[0].DAG.Tasks = []model.Task{
		{Name: "step1", Template: "exec-a", Dependencies: []string{"ghost"}},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for dependency on unknown task")
	}
	assertContains(t, err.Error(), "depends on unknown task")
}

func TestValidate_DAGCycleDetected(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[0].DAG.Tasks = []model.Task{
		{Name: "a", Template: "exec-a", Dependencies: []string{"b"}},
		{Name: "b", Template: "exec-a", Dependencies: []string{"a"}},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for DAG cycle")
	}
	assertContains(t, err.Error(), "dag contains a cycle")
}

func TestValidate_MultiTaskDAG(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[0].DAG.Tasks = []model.Task{
		{Name: "a", Template: "exec-a"},
		{Name: "b", Template: "exec-a", Dependencies: []string{"a"}},
		{Name: "c", Template: "exec-a", Dependencies: []string{"a", "b"}},
	}
	if err := Validate(wf); err != nil {
		t.Fatalf("expected valid multi-task DAG, got: %v", err)
	}
}

func TestValidate_NestedDAGValid(t *testing.T) {
	wf := &model.Workflow{
		APIVersion: "graph/v1",
		Kind:       "Workflow",
		Metadata:   model.Metadata{Name: "nested-wf"},
		Spec: model.WorkflowSpec{
			Entrypoint: "main",
			Templates: []model.Template{
				{
					DAG: &model.DAG{
						Name: "main",
						Tasks: []model.Task{
							{Name: "step1", Template: "sub-dag"},
						},
					},
				},
				{
					DAG: &model.DAG{
						Name: "sub-dag",
						Tasks: []model.Task{
							{Name: "a", Template: "exec"},
							{Name: "b", Template: "exec", Dependencies: []string{"a"}},
						},
					},
				},
				{
					Task: &model.Task{
						Name:     "exec",
						Executor: &model.Executor{Type: "script"},
					},
				},
			},
		},
	}
	if err := Validate(wf); err != nil {
		t.Fatalf("expected nested DAG to pass validation, got: %v", err)
	}
}

func TestValidate_NestingDepthAtLimit(t *testing.T) {
	// Build a chain: dag-0 → dag-1 → ... → dag-(MaxNestingDepth-1) → leaf
	// Total depth = MaxNestingDepth, which should pass.
	templates := make([]model.Template, 0, MaxNestingDepth+1)
	for i := 0; i < MaxNestingDepth; i++ {
		next := fmt.Sprintf("dag-%d", i+1)
		if i == MaxNestingDepth-1 {
			next = "leaf"
		}
		templates = append(templates, model.Template{
			DAG: &model.DAG{
				Name:  fmt.Sprintf("dag-%d", i),
				Tasks: []model.Task{{Name: "step", Template: next}},
			},
		})
	}
	templates = append(templates, model.Template{
		Task: &model.Task{
			Name:     "leaf",
			Executor: &model.Executor{Type: "script"},
		},
	})

	wf := &model.Workflow{
		APIVersion: "graph/v1",
		Kind:       "Workflow",
		Metadata:   model.Metadata{Name: "deep-wf"},
		Spec: model.WorkflowSpec{
			Entrypoint: "dag-0",
			Templates:  templates,
		},
	}
	if err := Validate(wf); err != nil {
		t.Fatalf("expected nesting at limit to pass, got: %v", err)
	}
}

func TestValidate_NestingDepthExceedsLimit(t *testing.T) {
	// Build a chain: dag-0 → dag-1 → ... → dag-MaxNestingDepth → leaf
	// Total depth = MaxNestingDepth+1, which should fail.
	templates := make([]model.Template, 0, MaxNestingDepth+2)
	for i := 0; i <= MaxNestingDepth; i++ {
		next := fmt.Sprintf("dag-%d", i+1)
		if i == MaxNestingDepth {
			next = "leaf"
		}
		templates = append(templates, model.Template{
			DAG: &model.DAG{
				Name:  fmt.Sprintf("dag-%d", i),
				Tasks: []model.Task{{Name: "step", Template: next}},
			},
		})
	}
	templates = append(templates, model.Template{
		Task: &model.Task{
			Name:     "leaf",
			Executor: &model.Executor{Type: "script"},
		},
	})

	wf := &model.Workflow{
		APIVersion: "graph/v1",
		Kind:       "Workflow",
		Metadata:   model.Metadata{Name: "too-deep-wf"},
		Spec: model.WorkflowSpec{
			Entrypoint: "dag-0",
			Templates:  templates,
		},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for nesting depth exceeding limit")
	}
	assertContains(t, err.Error(), "nesting depth exceeds maximum")
}

func TestValidate_NestingDepthViaLoop(t *testing.T) {
	// dag-entry → loop-tmpl(body=inner-dag) → inner-dag(task→leaf)
	// depth chain: entry(0) → loop(1) → inner-dag(2) → leaf(3)
	wf := &model.Workflow{
		APIVersion: "graph/v1",
		Kind:       "Workflow",
		Metadata:   model.Metadata{Name: "loop-nest-wf"},
		Spec: model.WorkflowSpec{
			Entrypoint: "dag-entry",
			Templates: []model.Template{
				{
					DAG: &model.DAG{
						Name:  "dag-entry",
						Tasks: []model.Task{{Name: "step", Template: "loop-tmpl"}},
					},
				},
				{
					Loop: &model.Loop{
						Name:  "loop-tmpl",
						Body:  "inner-dag",
						Items: []any{1},
					},
				},
				{
					DAG: &model.DAG{
						Name:  "inner-dag",
						Tasks: []model.Task{{Name: "a", Template: "leaf"}},
					},
				},
				{
					Task: &model.Task{
						Name:     "leaf",
						Executor: &model.Executor{Type: "script"},
					},
				},
			},
		},
	}
	if err := Validate(wf); err != nil {
		t.Fatalf("expected loop nesting to pass, got: %v", err)
	}
}

// --- Loop tests ---

// loopWorkflow returns a workflow with a loop template as entrypoint.
func loopWorkflow(loop *model.Loop) *model.Workflow {
	loop.Name = "my-loop"
	return &model.Workflow{
		APIVersion: "graph/v1",
		Kind:       "Workflow",
		Metadata:   model.Metadata{Name: "loop-wf"},
		Spec: model.WorkflowSpec{
			Entrypoint: "my-loop",
			Templates: []model.Template{
				{Loop: loop},
				{
					Task: &model.Task{
						Name: "exec-a",
						Executor: &model.Executor{
							Type:   "function",
							Config: json.RawMessage(`{"name":"handler"}`),
						},
					},
				},
			},
		},
	}
}

func TestValidate_LoopWithItems(t *testing.T) {
	wf := loopWorkflow(&model.Loop{
		Body:  "exec-a",
		Items: []any{1, 2, 3},
	})
	if err := Validate(wf); err != nil {
		t.Fatalf("expected valid loop with items, got: %v", err)
	}
}

func TestValidate_LoopWithItemsConcurrency(t *testing.T) {
	wf := loopWorkflow(&model.Loop{
		Body:        "exec-a",
		Items:       []any{1, 2, 3},
		Concurrency: 2,
	})
	if err := Validate(wf); err != nil {
		t.Fatalf("expected valid loop with items+concurrency, got: %v", err)
	}
}

func TestValidate_LoopWithItemsFrom(t *testing.T) {
	wf := loopWorkflow(&model.Loop{
		Body:      "exec-a",
		ItemsFrom: "{{tasks.fetch.outputs.parameters.list}}",
	})
	if err := Validate(wf); err != nil {
		t.Fatalf("expected valid loop with itemsFrom, got: %v", err)
	}
}

func TestValidate_LoopWithRepeatCondition(t *testing.T) {
	wf := loopWorkflow(&model.Loop{
		Body:            "exec-a",
		RepeatCondition: "{{outputs.parameters.continue}} == true",
		MaxIterations:   10,
	})
	if err := Validate(wf); err != nil {
		t.Fatalf("expected valid loop with repeatCondition, got: %v", err)
	}
}

func TestValidate_LoopMissingBody(t *testing.T) {
	wf := loopWorkflow(&model.Loop{
		Body:  "",
		Items: []any{1},
	})
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for loop without body")
	}
	assertContains(t, err.Error(), "loop.body is required")
}

func TestValidate_LoopBodyUnknownTemplate(t *testing.T) {
	wf := loopWorkflow(&model.Loop{
		Body:  "nonexistent",
		Items: []any{1},
	})
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for loop body referencing unknown template")
	}
	assertContains(t, err.Error(), "loop.body references unknown template")
}

func TestValidate_LoopNoIterationMode(t *testing.T) {
	wf := loopWorkflow(&model.Loop{
		Body: "exec-a",
		// No repeatCondition, items, or itemsFrom
	})
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for loop with no iteration mode")
	}
	assertContains(t, err.Error(), "loop must specify one of repeatCondition/items/itemsFrom")
}

func TestValidate_LoopMultipleIterationModes(t *testing.T) {
	wf := loopWorkflow(&model.Loop{
		Body:      "exec-a",
		Items:     []any{1},
		ItemsFrom: "some-expr",
	})
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for loop with multiple iteration modes")
	}
	assertContains(t, err.Error(), "loop must specify only one of repeatCondition/items/itemsFrom")
}

func TestValidate_LoopRepeatConditionRequiresMaxIterations(t *testing.T) {
	wf := loopWorkflow(&model.Loop{
		Body:            "exec-a",
		RepeatCondition: "{{outputs.parameters.continue}} == true",
		// MaxIterations not set (0)
	})
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for repeatCondition without maxIterations")
	}
	assertContains(t, err.Error(), "loop.maxIterations is required when using repeatCondition")
}

func TestValidate_LoopRepeatConditionNoConcurrency(t *testing.T) {
	wf := loopWorkflow(&model.Loop{
		Body:            "exec-a",
		RepeatCondition: "{{outputs.parameters.continue}} == true",
		MaxIterations:   10,
		Concurrency:     3,
	})
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for repeatCondition with concurrency")
	}
	assertContains(t, err.Error(), "loop.concurrency is not allowed with repeatCondition")
}

func TestValidate_LoopAggregateStrategy_Valid(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates = append(wf.Spec.Templates,
		model.Template{Loop: &model.Loop{
			Name:      "my-loop",
			Items:     []any{"a"},
			Body:      "exec-a",
			Aggregate: &model.Aggregate{Strategy: model.AggregateStrategyAll},
		}},
	)
	if err := Validate(wf); err != nil {
		t.Fatalf("expected valid strategy to pass, got: %v", err)
	}
}

func TestValidate_LoopAggregateStrategy_Empty_Valid(t *testing.T) {
	// empty strategy means "all" (default); should pass
	wf := validWorkflow()
	wf.Spec.Templates = append(wf.Spec.Templates,
		model.Template{Loop: &model.Loop{
			Name:      "my-loop",
			Items:     []any{"a"},
			Body:      "exec-a",
			Aggregate: &model.Aggregate{},
		}},
	)
	if err := Validate(wf); err != nil {
		t.Fatalf("expected empty strategy to pass as default, got: %v", err)
	}
}

func TestValidate_LoopAggregateStrategy_Invalid(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates = append(wf.Spec.Templates,
		model.Template{Loop: &model.Loop{
			Name:      "my-loop",
			Items:     []any{"a"},
			Body:      "exec-a",
			Aggregate: &model.Aggregate{Strategy: "unknown-strategy"},
		}},
	)
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for unsupported aggregate strategy")
	}
	assertContains(t, err.Error(), "is not supported")
}

// --- Parameter name validation ---

func TestValidate_ParamName_Valid(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[1].Task.Inputs = &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "my-param"},
			{Name: "myparam123"},
			{Name: "ab"},
		},
	}
	if err := Validate(wf); err != nil {
		t.Fatalf("expected valid param names to pass, got: %v", err)
	}
}

func TestValidate_ParamName_DotRejected(t *testing.T) {
	// "loop_iter.index" fails DNS-1123: contains dot and underscore
	wf := validWorkflow()
	wf.Spec.Templates[1].Task.Inputs = &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "loop-iter.index"},
		},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for param name containing dot")
	}
	assertContains(t, err.Error(), "DNS-1123")
}

func TestValidate_ParamName_UnderscoreRejected(t *testing.T) {
	// underscore is not allowed by DNS-1123
	wf := validWorkflow()
	wf.Spec.Templates[1].Task.Inputs = &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "my_param"},
		},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for param name containing underscore")
	}
	assertContains(t, err.Error(), "DNS-1123")
}

func TestValidate_ParamName_UppercaseRejected(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[1].Task.Inputs = &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "MyParam"},
		},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for param name containing uppercase")
	}
	assertContains(t, err.Error(), "DNS-1123")
}

func TestValidate_ParamName_SpaceRejected(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[1].Task.Inputs = &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "my param"},
		},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for param name containing space")
	}
	assertContains(t, err.Error(), "DNS-1123")
}

func TestValidate_MetadataName_DNS1123(t *testing.T) {
	wf := validWorkflow()
	wf.Metadata.Name = "Invalid_Name"
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for metadata.name with underscore")
	}
	assertContains(t, err.Error(), "DNS-1123")
}

func TestValidate_TemplateName_DNS1123(t *testing.T) {
	wf := validWorkflow()
	// inject a template with an invalid name
	wf.Spec.Templates = append(wf.Spec.Templates, model.Template{
		Task: &model.Task{
			Name: "Bad_Template",
			Executor: &model.Executor{
				Type:   "script",
				Config: json.RawMessage(`{"runtime":"bash","source":"echo"}`),
			},
		},
	})
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for template name with underscore")
	}
	assertContains(t, err.Error(), "DNS-1123")
}

func TestValidate_TaskName_DNS1123(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[0].DAG.Tasks = append(wf.Spec.Templates[0].DAG.Tasks, model.Task{
		Name:     "bad_task",
		Template: "exec-a",
	})
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for DAG task name with underscore")
	}
	assertContains(t, err.Error(), "DNS-1123")
}

// --- Helpers ---

// assertContains checks if s contains substr.
func assertContains(t *testing.T, s, substr string) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Errorf("expected %q to contain %q", s, substr)
	}
}
