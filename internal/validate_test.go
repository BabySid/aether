package internal

import (
	"fmt"
	"strings"
	"testing"

	"github.com/BabySid/aether/model"
)

// validWorkflow returns a minimal valid workflow for testing.
// Tests can modify fields before calling Validate.
func validWorkflow() *model.Workflow {
	return &model.Workflow{
		APIVersion: "aether/v1",
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
							Type: "script",
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
	for _, kind := range []string{"Workflow", "CronWorkflow"} {
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
		t.Fatal("expected error for empty task.Template and nil executor")
	}
	assertContains(t, err.Error(), "either template or executor is required")
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
		APIVersion: "aether/v1",
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
		APIVersion: "aether/v1",
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
		APIVersion: "aether/v1",
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

func TestValidate_NestingDepthRespectsSpec(t *testing.T) {
	// Build a chain of depth 4: dag-0 → dag-1 → dag-2 → dag-3 → leaf
	// With spec.maxNestedDepth=3 this should fail (depth 4 > 3).
	templates := make([]model.Template, 0, 5)
	for i := 0; i < 4; i++ {
		next := fmt.Sprintf("dag-%d", i+1)
		if i == 3 {
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
		APIVersion: "aether/v1",
		Kind:       "Workflow",
		Metadata:   model.Metadata{Name: "spec-depth-wf"},
		Spec: model.WorkflowSpec{
			Entrypoint:     "dag-0",
			MaxNestedDepth: 3,
			Templates:      templates,
		},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for nesting depth exceeding spec.maxNestedDepth=3")
	}
	assertContains(t, err.Error(), "nesting depth exceeds maximum (3)")

	// Raising the limit to 4 should make it pass.
	wf.Spec.MaxNestedDepth = 4
	if err := Validate(wf); err != nil {
		t.Fatalf("expected nesting at spec limit to pass, got: %v", err)
	}
}

func TestValidate_NestingDepthViaLoop(t *testing.T) {
	// dag-entry → loop-tmpl(body=inner-dag) → inner-dag(task→leaf)
	// depth chain: entry(0) → loop(1) → inner-dag(2) → leaf(3)
	wf := &model.Workflow{
		APIVersion: "aether/v1",
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
		APIVersion: "aether/v1",
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
							Type: "function",
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

func TestValidate_LoopAggregateStrategy_Last(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates = append(wf.Spec.Templates,
		model.Template{Loop: &model.Loop{
			Name:      "my-loop",
			Items:     []any{"a"},
			Body:      "exec-a",
			Aggregate: &model.Aggregate{Strategy: model.AggregateStrategyLast},
		}},
	)
	if err := Validate(wf); err != nil {
		t.Fatalf("expected strategy 'last' to pass, got: %v", err)
	}
}

func TestValidate_LoopAggregateStrategy_First(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates = append(wf.Spec.Templates,
		model.Template{Loop: &model.Loop{
			Name:      "my-loop",
			Items:     []any{"a"},
			Body:      "exec-a",
			Aggregate: &model.Aggregate{Strategy: model.AggregateStrategyFirst},
		}},
	)
	if err := Validate(wf); err != nil {
		t.Fatalf("expected strategy 'first' to pass, got: %v", err)
	}
}

func TestValidate_LoopAggregateStrategy_List(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates = append(wf.Spec.Templates,
		model.Template{Loop: &model.Loop{
			Name:      "my-loop",
			Items:     []any{"a"},
			Body:      "exec-a",
			Aggregate: &model.Aggregate{Strategy: model.AggregateStrategyList},
		}},
	)
	if err := Validate(wf); err != nil {
		t.Fatalf("expected strategy 'list' to pass, got: %v", err)
	}
}

func TestValidate_LoopAggregateStrategy_Empty_Valid(t *testing.T) {
	// empty strategy defaults to "last"; should pass
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
				Type: "script",
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

func TestValidate_LoopInputParamName_Invalid(t *testing.T) {
	wf := loopWorkflow(&model.Loop{
		Body:  "exec-a",
		Items: []any{1, 2},
	})
	wf.Spec.Templates[0].Loop.Inputs = &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "my_param"},
		},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for loop input param name with underscore")
	}
	assertContains(t, err.Error(), "DNS-1123")
}

func TestValidate_LoopInputParamName_Valid(t *testing.T) {
	wf := loopWorkflow(&model.Loop{
		Body:  "exec-a",
		Items: []any{1, 2},
	})
	wf.Spec.Templates[0].Loop.Inputs = &model.Inputs{
		Parameters: []model.Parameter{
			{Name: "my-param"},
		},
	}
	if err := Validate(wf); err != nil {
		t.Fatalf("expected valid loop input param name, got: %v", err)
	}
}

func TestValidate_EntrypointReferencesUnknownTask(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[0].DAG.Entrypoints = "nonexistent"
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for entrypoint referencing unknown task")
	}
	assertContains(t, err.Error(), "unknown task")
}

func TestValidate_EntrypointReferencesValidTask(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[0].DAG.Entrypoints = wf.Spec.Templates[0].DAG.Tasks[0].Name
	if err := Validate(wf); err != nil {
		t.Fatalf("expected valid entrypoint, got: %v", err)
	}
}

func TestValidate_EntrypointMultiple_OneInvalid(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[0].DAG.Entrypoints = []any{
		wf.Spec.Templates[0].DAG.Tasks[0].Name,
		"ghost",
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error when one entrypoint references unknown task")
	}
	assertContains(t, err.Error(), "unknown task")
}

// --- Hook validation tests ---

func TestValidate_HookTemplateValid(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Hooks = &model.Hooks{
		OnStart:   &model.Hook{Template: "exec-a"},
		OnSuccess: &model.Hook{Template: "exec-a"},
	}
	if err := Validate(wf); err != nil {
		t.Fatalf("expected valid hook template, got: %v", err)
	}
}

func TestValidate_HookTemplateUnknown(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Hooks = &model.Hooks{
		OnFailure: &model.Hook{Template: "nonexistent"},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for hook referencing unknown template")
	}
	assertContains(t, err.Error(), "references unknown template")
}

func TestValidate_HookTemplateMustBeTask(t *testing.T) {
	wf := validWorkflow()
	// main is a DAG template — hooks should not reference it
	wf.Spec.Hooks = &model.Hooks{
		OnSuccess: &model.Hook{Template: "main"},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for hook referencing DAG template")
	}
	assertContains(t, err.Error(), "must be a task template")
}

func TestValidate_HookTemplateLoop(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates = append(wf.Spec.Templates, model.Template{
		Loop: &model.Loop{
			Name:  "my-loop",
			Body:  "exec-a",
			Items: []any{1},
		},
	})
	wf.Spec.Hooks = &model.Hooks{
		OnError: &model.Hook{Template: "my-loop"},
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for hook referencing Loop template")
	}
	assertContains(t, err.Error(), "must be a task template")
}

func TestValidate_TaskLevelHookValid(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[0].DAG.Tasks[0].Hooks = &model.Hooks{
		OnSuccess: &model.Hook{Template: "exec-a"},
		OnSuspend: &model.Hook{Template: "exec-a"},
		OnResume:  &model.Hook{Template: "exec-a"},
	}
	if err := Validate(wf); err != nil {
		t.Fatalf("expected valid task-level hooks, got: %v", err)
	}
}

func TestValidate_TaskLevelHookInvalid(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Templates[0].DAG.Tasks[0].Hooks = &model.Hooks{
		OnExit: &model.Hook{Template: "main"}, // DAG template
	}
	err := Validate(wf)
	if err == nil {
		t.Fatal("expected error for task hook referencing DAG template")
	}
	assertContains(t, err.Error(), "must be a task template")
}

func TestValidate_AllHookTypes(t *testing.T) {
	wf := validWorkflow()
	wf.Spec.Hooks = &model.Hooks{
		OnStart:   &model.Hook{Template: "exec-a"},
		OnSuccess: &model.Hook{Template: "exec-a"},
		OnFailure: &model.Hook{Template: "exec-a"},
		OnError:   &model.Hook{Template: "exec-a"},
		OnSuspend: &model.Hook{Template: "exec-a"},
		OnResume:  &model.Hook{Template: "exec-a"},
		OnCancel:  &model.Hook{Template: "exec-a"},
		OnExit:    &model.Hook{Template: "exec-a"},
	}
	if err := Validate(wf); err != nil {
		t.Fatalf("expected all hook types to pass validation, got: %v", err)
	}
}

// --- CronWorkflow validation tests ---

func validCronWorkflow() *model.CronWorkflow {
	return &model.CronWorkflow{
		APIVersion: "aether/v1",
		Kind:       "CronWorkflow",
		Metadata:   model.Metadata{Name: "test-cron"},
		Spec: model.CronWorkflowSpec{
			Schedule: "0 * * * *",
			Timezone: "UTC",
			WorkflowSpec: model.WorkflowSpec{
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
							Name:     "exec-a",
							Executor: &model.Executor{Type: "script"},
						},
					},
				},
			},
		},
	}
}

func TestValidateCronWorkflow_Valid(t *testing.T) {
	cw := validCronWorkflow()
	FillCronDefaults(cw)
	if err := ValidateCronWorkflow(cw); err != nil {
		t.Fatalf("expected valid CronWorkflow, got: %v", err)
	}
}

func TestValidateCronWorkflow_ScheduleMacros(t *testing.T) {
	for _, macro := range []string{"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly"} {
		cw := validCronWorkflow()
		cw.Spec.Schedule = macro
		FillCronDefaults(cw)
		if err := ValidateCronWorkflow(cw); err != nil {
			t.Errorf("macro %q should be valid, got: %v", macro, err)
		}
	}
}

func TestValidateCronWorkflow_UnsupportedAPIVersion(t *testing.T) {
	cw := validCronWorkflow()
	cw.APIVersion = "v2"
	err := ValidateCronWorkflow(cw)
	if err == nil {
		t.Fatal("expected error")
	}
	assertContains(t, err.Error(), "unsupported apiVersion")
}

func TestValidateCronWorkflow_WrongKind(t *testing.T) {
	cw := validCronWorkflow()
	cw.Kind = "Workflow"
	err := ValidateCronWorkflow(cw)
	if err == nil {
		t.Fatal("expected error")
	}
	assertContains(t, err.Error(), "expected CronWorkflow")
}

func TestValidateCronWorkflow_EmptyName(t *testing.T) {
	cw := validCronWorkflow()
	cw.Metadata.Name = ""
	err := ValidateCronWorkflow(cw)
	if err == nil {
		t.Fatal("expected error")
	}
	assertContains(t, err.Error(), "metadata.name is required")
}

func TestValidateCronWorkflow_InvalidName(t *testing.T) {
	cw := validCronWorkflow()
	cw.Metadata.Name = "My Cron"
	FillCronDefaults(cw)
	err := ValidateCronWorkflow(cw)
	if err == nil {
		t.Fatal("expected error")
	}
	assertContains(t, err.Error(), "DNS-1123")
}

func TestValidateCronWorkflow_EmptySchedule(t *testing.T) {
	cw := validCronWorkflow()
	cw.Spec.Schedule = ""
	FillCronDefaults(cw)
	err := ValidateCronWorkflow(cw)
	if err == nil {
		t.Fatal("expected error")
	}
	assertContains(t, err.Error(), "schedule is required")
}

func TestValidateCronWorkflow_InvalidSchedule(t *testing.T) {
	cw := validCronWorkflow()
	cw.Spec.Schedule = "not a cron"
	FillCronDefaults(cw)
	err := ValidateCronWorkflow(cw)
	if err == nil {
		t.Fatal("expected error")
	}
	assertContains(t, err.Error(), "5-field cron")
}

func TestValidateCronWorkflow_InvalidTimezone(t *testing.T) {
	cw := validCronWorkflow()
	cw.Spec.Timezone = "Invalid/Zone"
	FillCronDefaults(cw)
	err := ValidateCronWorkflow(cw)
	if err == nil {
		t.Fatal("expected error")
	}
	assertContains(t, err.Error(), "timezone")
}

func TestValidateCronWorkflow_ValidTimezone(t *testing.T) {
	cw := validCronWorkflow()
	cw.Spec.Timezone = "Asia/Shanghai"
	FillCronDefaults(cw)
	if err := ValidateCronWorkflow(cw); err != nil {
		t.Fatalf("Asia/Shanghai should be valid, got: %v", err)
	}
}

func TestValidateCronWorkflow_InvalidStartAt(t *testing.T) {
	cw := validCronWorkflow()
	cw.Spec.StartAt = "not-a-date"
	FillCronDefaults(cw)
	err := ValidateCronWorkflow(cw)
	if err == nil {
		t.Fatal("expected error")
	}
	assertContains(t, err.Error(), "startAt")
}

func TestValidateCronWorkflow_InvalidEndAt(t *testing.T) {
	cw := validCronWorkflow()
	cw.Spec.EndAt = "not-a-date"
	FillCronDefaults(cw)
	err := ValidateCronWorkflow(cw)
	if err == nil {
		t.Fatal("expected error")
	}
	assertContains(t, err.Error(), "endAt")
}

func TestValidateCronWorkflow_EndAtBeforeStartAt(t *testing.T) {
	cw := validCronWorkflow()
	cw.Spec.StartAt = "2030-01-02T00:00:00Z"
	cw.Spec.EndAt = "2030-01-01T00:00:00Z"
	FillCronDefaults(cw)
	err := ValidateCronWorkflow(cw)
	if err == nil {
		t.Fatal("expected error")
	}
	assertContains(t, err.Error(), "endAt must be after")
}

func TestValidateCronWorkflow_ValidStartAtEndAt(t *testing.T) {
	cw := validCronWorkflow()
	cw.Spec.StartAt = "2030-01-01T00:00:00Z"
	cw.Spec.EndAt = "2030-12-31T23:59:59Z"
	FillCronDefaults(cw)
	if err := ValidateCronWorkflow(cw); err != nil {
		t.Fatalf("expected valid, got: %v", err)
	}
}

func TestValidateCronWorkflow_InvalidConcurrencyPolicy(t *testing.T) {
	cw := validCronWorkflow()
	cw.Spec.ConcurrencyPolicy = "Invalid"
	FillCronDefaults(cw)
	err := ValidateCronWorkflow(cw)
	if err == nil {
		t.Fatal("expected error")
	}
	assertContains(t, err.Error(), "concurrencyPolicy")
}

func TestValidateCronWorkflow_ValidConcurrencyPolicies(t *testing.T) {
	for _, policy := range []string{model.ConcurrencyAllow, model.ConcurrencyForbid, model.ConcurrencyReplace} {
		cw := validCronWorkflow()
		cw.Spec.ConcurrencyPolicy = policy
		FillCronDefaults(cw)
		if err := ValidateCronWorkflow(cw); err != nil {
			t.Errorf("policy %q should be valid, got: %v", policy, err)
		}
	}
}

func TestValidateCronWorkflow_StartingDeadlineSecondsZero(t *testing.T) {
	cw := validCronWorkflow()
	zero := 0
	cw.Spec.StartingDeadlineSeconds = &zero
	FillCronDefaults(cw)
	err := ValidateCronWorkflow(cw)
	if err == nil {
		t.Fatal("expected error for StartingDeadlineSeconds=0")
	}
	assertContains(t, err.Error(), "startingDeadlineSeconds")
}

func TestValidateCronWorkflow_StartingDeadlineSecondsNegative(t *testing.T) {
	cw := validCronWorkflow()
	neg := -1
	cw.Spec.StartingDeadlineSeconds = &neg
	FillCronDefaults(cw)
	err := ValidateCronWorkflow(cw)
	if err == nil {
		t.Fatal("expected error")
	}
	assertContains(t, err.Error(), "startingDeadlineSeconds")
}

func TestValidateCronWorkflow_StartingDeadlineSecondsValid(t *testing.T) {
	cw := validCronWorkflow()
	val := 60
	cw.Spec.StartingDeadlineSeconds = &val
	FillCronDefaults(cw)
	if err := ValidateCronWorkflow(cw); err != nil {
		t.Fatalf("expected valid, got: %v", err)
	}
}

func TestValidateCronWorkflow_StartingDeadlineSecondsNil(t *testing.T) {
	cw := validCronWorkflow()
	cw.Spec.StartingDeadlineSeconds = nil
	FillCronDefaults(cw)
	if err := ValidateCronWorkflow(cw); err != nil {
		t.Fatalf("nil StartingDeadlineSeconds should be valid, got: %v", err)
	}
}

func TestValidateCronWorkflow_InvalidWorkflowSpec(t *testing.T) {
	cw := validCronWorkflow()
	cw.Spec.WorkflowSpec = model.WorkflowSpec{} // empty, missing entrypoint
	FillCronDefaults(cw)
	err := ValidateCronWorkflow(cw)
	if err == nil {
		t.Fatal("expected error for empty WorkflowSpec")
	}
	assertContains(t, err.Error(), "workflowSpec")
}

func TestValidateCronWorkflow_FiveFieldSchedule(t *testing.T) {
	tests := []struct {
		schedule string
		valid    bool
	}{
		{"*/5 * * * *", true},
		{"0 0 1 1 *", true},
		{"0 9 * * 1-5", true},
		{"0 * * *", false},       // 4 fields
		{"0 * * * * *", false},   // 6 fields
	}
	for _, tt := range tests {
		cw := validCronWorkflow()
		cw.Spec.Schedule = tt.schedule
		FillCronDefaults(cw)
		err := ValidateCronWorkflow(cw)
		if tt.valid && err != nil {
			t.Errorf("schedule %q should be valid, got: %v", tt.schedule, err)
		}
		if !tt.valid && err == nil {
			t.Errorf("schedule %q should be invalid", tt.schedule)
		}
	}
}

// --- Helpers ---

// assertContains checks if s contains substr.
func assertContains(t *testing.T, s, substr string) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Errorf("expected %q to contain %q", s, substr)
	}
}
