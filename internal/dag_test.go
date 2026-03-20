package internal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// phasePtr is a test helper to take the address of a Phase value.
func phasePtr(p model.Phase) *model.Phase { return &p }

// ---- HasCycle ----

func TestHasCycle_NoCycle(t *testing.T) {
	dag := &model.DAG{
		Tasks: []model.Task{
			{Name: "a"},
			{Name: "b", Dependencies: []string{"a"}},
			{Name: "c", Dependencies: []string{"b"}},
		},
	}
	if HasCycle(dag) {
		t.Error("expected no cycle")
	}
}

func TestHasCycle_DirectCycle(t *testing.T) {
	// a → b → a
	dag := &model.DAG{
		Tasks: []model.Task{
			{Name: "a", Dependencies: []string{"b"}},
			{Name: "b", Dependencies: []string{"a"}},
		},
	}
	if !HasCycle(dag) {
		t.Error("expected cycle to be detected")
	}
}

func TestHasCycle_SelfLoop(t *testing.T) {
	dag := &model.DAG{
		Tasks: []model.Task{
			{Name: "a", Dependencies: []string{"a"}},
		},
	}
	if !HasCycle(dag) {
		t.Error("expected self-loop cycle to be detected")
	}
}

func TestHasCycle_DiamondNoCycle(t *testing.T) {
	//   a
	//  / \
	// b   c
	//  \ /
	//   d
	dag := &model.DAG{
		Tasks: []model.Task{
			{Name: "a"},
			{Name: "b", Dependencies: []string{"a"}},
			{Name: "c", Dependencies: []string{"a"}},
			{Name: "d", Dependencies: []string{"b", "c"}},
		},
	}
	if HasCycle(dag) {
		t.Error("expected diamond DAG to have no cycle")
	}
}

func TestHasCycle_EmptyDAG(t *testing.T) {
	dag := &model.DAG{}
	if HasCycle(dag) {
		t.Error("empty DAG should have no cycle")
	}
}

// ---- FindTemplate ----

func TestFindTemplate_Found(t *testing.T) {
	wf := &model.Workflow{
		Spec: model.WorkflowSpec{
			Templates: []model.Template{
				{Task: &model.Task{Name: "step-a"}},
				{Task: &model.Task{Name: "step-b"}},
			},
		},
	}
	tmpl := FindTemplate(wf, "step-b")
	if tmpl == nil {
		t.Fatal("expected to find template step-b")
	}
	if tmpl.GetName() != "step-b" {
		t.Errorf("expected step-b, got %q", tmpl.GetName())
	}
}

func TestFindTemplate_NotFound(t *testing.T) {
	wf := &model.Workflow{
		Spec: model.WorkflowSpec{
			Templates: []model.Template{
				{Task: &model.Task{Name: "step-a"}},
			},
		},
	}
	tmpl := FindTemplate(wf, "missing")
	if tmpl != nil {
		t.Error("expected nil for missing template")
	}
}

func TestFindTemplate_Empty(t *testing.T) {
	wf := &model.Workflow{}
	if FindTemplate(wf, "any") != nil {
		t.Error("expected nil for empty template list")
	}
}

// ---- FindTask ----

func TestFindTask_Found(t *testing.T) {
	dag := &model.DAG{
		Tasks: []model.Task{
			{Name: "alpha"},
			{Name: "beta"},
		},
	}
	task := FindTask(dag, "beta")
	if task == nil || task.Name != "beta" {
		t.Errorf("expected task beta, got %v", task)
	}
}

func TestFindTask_NotFound(t *testing.T) {
	dag := &model.DAG{Tasks: []model.Task{{Name: "alpha"}}}
	if FindTask(dag, "gamma") != nil {
		t.Error("expected nil for missing task")
	}
}

// ---- computeReachable / extractEntrypointNames ----

func TestComputeReachable_NoEntrypoints(t *testing.T) {
	dag := &model.DAG{
		Tasks: []model.Task{
			{Name: "a"},
			{Name: "b", Dependencies: []string{"a"}},
		},
	}
	// nil entrypoints → nil reachable (all tasks allowed)
	r := computeReachable(dag, extractEntrypointNames(dag))
	if r != nil {
		t.Errorf("expected nil reachable when no entrypoints, got %v", r)
	}
}

func TestComputeReachable_SingleEntrypoint(t *testing.T) {
	// a → b → d
	// c        ← not reachable from a
	dag := &model.DAG{
		Entrypoints: "a",
		Tasks: []model.Task{
			{Name: "a"},
			{Name: "b", Dependencies: []string{"a"}},
			{Name: "c"},
			{Name: "d", Dependencies: []string{"b"}},
		},
	}
	r := computeReachable(dag, extractEntrypointNames(dag))
	for _, name := range []string{"a", "b", "d"} {
		if !r[name] {
			t.Errorf("expected %q to be reachable", name)
		}
	}
	if r["c"] {
		t.Error("expected c to NOT be reachable from entrypoint a")
	}
}

func TestComputeReachable_MultipleEntrypoints(t *testing.T) {
	// a → d
	// b        ← unreachable
	// c → e
	dag := &model.DAG{
		Entrypoints: []any{"a", "c"},
		Tasks: []model.Task{
			{Name: "a"},
			{Name: "b"},
			{Name: "c"},
			{Name: "d", Dependencies: []string{"a"}},
			{Name: "e", Dependencies: []string{"c"}},
		},
	}
	r := computeReachable(dag, extractEntrypointNames(dag))
	for _, name := range []string{"a", "c", "d", "e"} {
		if !r[name] {
			t.Errorf("expected %q to be reachable", name)
		}
	}
	if r["b"] {
		t.Error("expected b to NOT be reachable")
	}
}

// ---- FindReadyTasks ----

func TestFindReadyTasks_NoDeps(t *testing.T) {
	dag := &model.DAG{
		Tasks: []model.Task{
			{Name: "a"},
			{Name: "b", Dependencies: []string{"a"}},
		},
	}
	// No existing runs → only "a" is ready (no deps), "b" needs "a" done.
	ready := FindReadyTasks(dag, nil)
	if len(ready) != 1 || ready[0].Name != "a" {
		t.Errorf("expected [a], got %v", ready)
	}
}

func TestFindReadyTasks_DepSucceeded(t *testing.T) {
	dag := &model.DAG{
		Tasks: []model.Task{
			{Name: "a"},
			{Name: "b", Dependencies: []string{"a"}},
		},
	}
	existing := []*store.TaskRun{
		{TaskName: "a", Status: phasePtr(model.PhaseSucceeded)},
	}
	ready := FindReadyTasks(dag, existing)
	// "a" already has a run → skip; "b"'s dep is Succeeded → ready
	if len(ready) != 1 || ready[0].Name != "b" {
		t.Errorf("expected [b], got %v", ready)
	}
}

func TestFindReadyTasks_DepNotTerminal(t *testing.T) {
	dag := &model.DAG{
		Tasks: []model.Task{
			{Name: "a"},
			{Name: "b", Dependencies: []string{"a"}},
		},
	}
	existing := []*store.TaskRun{
		{TaskName: "a", Status: phasePtr(model.PhaseRunning)},
	}
	// "a" is still Running → "b" not ready
	ready := FindReadyTasks(dag, existing)
	if len(ready) != 0 {
		t.Errorf("expected no ready tasks, got %v", ready)
	}
}

func TestFindReadyTasks_DepFailedNoContinueOn(t *testing.T) {
	dag := &model.DAG{
		Tasks: []model.Task{
			{Name: "a"},
			{Name: "b", Dependencies: []string{"a"}},
		},
	}
	existing := []*store.TaskRun{
		{TaskName: "a", Status: phasePtr(model.PhaseFailed)},
	}
	// "a" failed with no continueOn → "b" not ready
	ready := FindReadyTasks(dag, existing)
	if len(ready) != 0 {
		t.Errorf("expected no ready tasks, got %v", ready)
	}
}

func TestFindReadyTasks_DepFailedWithUpstreamTaskContinueOn(t *testing.T) {
	dag := &model.DAG{
		Tasks: []model.Task{
			{Name: "a", ContinueOn: &model.ContinueOn{Failed: true}},
			{Name: "b", Dependencies: []string{"a"}},
		},
	}
	existing := []*store.TaskRun{
		{TaskName: "a", Status: phasePtr(model.PhaseFailed)},
	}
	ready := FindReadyTasks(dag, existing)
	if len(ready) != 1 || ready[0].Name != "b" {
		t.Errorf("expected [b], got %v", ready)
	}
}

func TestFindReadyTasks_DepFailedWithDAGContinueOn(t *testing.T) {
	dag := &model.DAG{
		ContinueOn: &model.ContinueOn{Failed: true},
		Tasks: []model.Task{
			{Name: "a"},
			{Name: "b", Dependencies: []string{"a"}},
		},
	}
	existing := []*store.TaskRun{
		{TaskName: "a", Status: phasePtr(model.PhaseFailed)},
	}
	ready := FindReadyTasks(dag, existing)
	if len(ready) != 1 || ready[0].Name != "b" {
		t.Errorf("expected [b] via DAG continueOn, got %v", ready)
	}
}

func TestFindReadyTasks_EntrypointsExcludeUnreachable(t *testing.T) {
	// Entrypoints = [a, c]; b has no deps but is NOT reachable → must be skipped
	dag := &model.DAG{
		Entrypoints: []any{"a", "c"},
		Tasks: []model.Task{
			{Name: "a"},
			{Name: "b"}, // no deps, but not reachable from entrypoints
			{Name: "c"},
			{Name: "d", Dependencies: []string{"a"}},
		},
	}
	ready := FindReadyTasks(dag, nil)
	names := make(map[string]bool)
	for _, r := range ready {
		names[r.Name] = true
	}
	if !names["a"] || !names["c"] {
		t.Errorf("expected a and c to be ready, got %v", ready)
	}
	if names["b"] {
		t.Error("expected b to be excluded (not reachable from entrypoints)")
	}
	if names["d"] {
		t.Error("expected d to not be ready yet (dep a not done)")
	}
}

func TestFindReadyTasks_EntrypointsDownstreamReady(t *testing.T) {
	// After a succeeds, d should become ready; b still excluded
	dag := &model.DAG{
		Entrypoints: []any{"a", "c"},
		Tasks: []model.Task{
			{Name: "a"},
			{Name: "b"},
			{Name: "c"},
			{Name: "d", Dependencies: []string{"a"}},
		},
	}
	existing := []*store.TaskRun{
		{TaskName: "a", Status: phasePtr(model.PhaseSucceeded)},
	}
	ready := FindReadyTasks(dag, existing)
	names := make(map[string]bool)
	for _, r := range ready {
		names[r.Name] = true
	}
	if !names["c"] || !names["d"] {
		t.Errorf("expected c and d to be ready, got %v", ready)
	}
	if names["b"] {
		t.Error("b must remain excluded even after a succeeds")
	}
}

func TestFindReadyTasks_AlreadyCreated(t *testing.T) {
	dag := &model.DAG{
		Tasks: []model.Task{{Name: "a"}},
	}
	existing := []*store.TaskRun{
		{TaskName: "a", Status: phasePtr(model.PhaseSucceeded)},
	}
	// "a" already has a run → nothing to create
	ready := FindReadyTasks(dag, existing)
	if len(ready) != 0 {
		t.Errorf("expected no new ready tasks, got %v", ready)
	}
}

// ---- EvalWhenCondition ----

func TestEvalWhenCondition_EmptyWhen(t *testing.T) {
	ok, err := EvalWhenCondition(context.Background(), "", nil, nil)
	if err != nil || !ok {
		t.Errorf("empty when should return true, nil; got %v, %v", ok, err)
	}
}

func TestEvalWhenCondition_NilEvaluator(t *testing.T) {
	// Non-empty when with nil evaluator → treat as true
	ok, err := EvalWhenCondition(context.Background(), "some-expr", nil, nil)
	if err != nil || !ok {
		t.Errorf("nil evaluator should return true, nil; got %v, %v", ok, err)
	}
}

func TestEvalWhenCondition_ReturnsBoolTrue(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return true, nil
	}}
	ok, err := EvalWhenCondition(context.Background(), "1==1", eval, nil)
	if err != nil || !ok {
		t.Errorf("expected true, nil; got %v, %v", ok, err)
	}
}

func TestEvalWhenCondition_ReturnsBoolFalse(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return false, nil
	}}
	ok, err := EvalWhenCondition(context.Background(), "1==2", eval, nil)
	if err != nil || ok {
		t.Errorf("expected false, nil; got %v, %v", ok, err)
	}
}

func TestEvalWhenCondition_ReturnsStringTrue(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return "true", nil
	}}
	ok, err := EvalWhenCondition(context.Background(), "x", eval, nil)
	if err != nil || !ok {
		t.Errorf("expected true from string 'true'; got %v, %v", ok, err)
	}
}

func TestEvalWhenCondition_ReturnsStringFalse(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return "false", nil
	}}
	ok, err := EvalWhenCondition(context.Background(), "x", eval, nil)
	if err != nil || ok {
		t.Errorf("expected false from string 'false'; got %v, %v", ok, err)
	}
}

func TestEvalWhenCondition_EvalError(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return nil, errors.New("syntax error")
	}}
	_, err := EvalWhenCondition(context.Background(), "bad", eval, nil)
	if err == nil {
		t.Error("expected error from evaluator")
	}
}

func TestEvalWhenCondition_NonBooleanResult(t *testing.T) {
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return 42, nil // integer, not bool or string
	}}
	_, err := EvalWhenCondition(context.Background(), "x", eval, nil)
	if err == nil {
		t.Error("expected error for non-boolean result")
	}
}

func TestEvalWhenCondition_UsesTaskRunEnv(t *testing.T) {
	taskRun := &store.TaskRun{
		TaskName: "checker",
		Status:   phasePtr(model.PhaseSucceeded),
	}
	var capturedEnv map[string]any
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		capturedEnv = env
		return true, nil
	}}
	_, _ = EvalWhenCondition(context.Background(), "tasks.checker.phase == 'Succeeded'", eval, []*store.TaskRun{taskRun})
	if capturedEnv == nil {
		t.Fatal("env should not be nil")
	}
	if capturedEnv["tasks.checker.phase"] != string(model.PhaseSucceeded) {
		t.Errorf("expected phase key in env, got %v", capturedEnv)
	}
}

// ---- BuildTaskEnv ----

func TestBuildTaskEnv_EmptyRuns(t *testing.T) {
	env := BuildTaskEnv(nil)
	if len(env) != 0 {
		t.Errorf("expected empty env, got %v", env)
	}
}

func TestBuildTaskEnv_PhaseKey(t *testing.T) {
	runs := []*store.TaskRun{
		{TaskName: "step-a", Status: phasePtr(model.PhaseSucceeded)},
	}
	env := BuildTaskEnv(runs)
	if env["tasks.step-a.phase"] != string(model.PhaseSucceeded) {
		t.Errorf("expected phase key, got %v", env)
	}
}

func TestBuildTaskEnv_OutputParameters(t *testing.T) {
	runs := []*store.TaskRun{
		{
			TaskName: "step-a",
			Status:   phasePtr(model.PhaseSucceeded),
			Outputs: &model.Outputs{
				Code: 0,
				Msg:  "ok",
				Parameters: []model.Parameter{
					{Name: "score", Value: rawJSON(95)},
				},
			},
		},
	}
	env := BuildTaskEnv(runs)

	if env["tasks.step-a.code"] != 0 {
		t.Errorf("expected code=0, got %v", env["tasks.step-a.code"])
	}
	if env["tasks.step-a.msg"] != "ok" {
		t.Errorf("expected msg=ok, got %v", env["tasks.step-a.msg"])
	}
	if env["tasks.step-a.outputs.parameters.score"] != float64(95) {
		t.Errorf("expected score=95, got %v", env["tasks.step-a.outputs.parameters.score"])
	}
}

func TestBuildTaskEnv_MultipleRuns(t *testing.T) {
	runs := []*store.TaskRun{
		{TaskName: "a", Status: phasePtr(model.PhaseSucceeded)},
		{TaskName: "b", Status: phasePtr(model.PhaseFailed)},
	}
	env := BuildTaskEnv(runs)
	if env["tasks.a.phase"] != string(model.PhaseSucceeded) {
		t.Error("missing a.phase")
	}
	if env["tasks.b.phase"] != string(model.PhaseFailed) {
		t.Error("missing b.phase")
	}
}

// ---- BuildTaskAssignment ----

func makeWorkflow(priority int) *model.Workflow {
	return &model.Workflow{
		Spec: model.WorkflowSpec{Priority: priority},
	}
}

// minExec returns a minimal executor for use in tests that don't focus on executor fields.
func minExec() *model.Executor { return &model.Executor{Type: "script"} }

func TestBuildTaskAssignment_NoExecutorReturnsError(t *testing.T) {
	tr := &store.TaskRun{RunID: 1, TaskName: "t"}
	taskDecl := &model.Task{Name: "t"} // no executor
	wf := makeWorkflow(0)

	_, err := BuildTaskAssignment(1, tr, taskDecl, nil, wf)
	if err == nil {
		t.Error("expected error when task definition has no executor")
	}
}

func TestBuildTaskAssignment_Basic(t *testing.T) {
	tr := &store.TaskRun{RunID: 10, TaskName: "my-task", TemplateName: "my-tmpl"}
	taskDecl := &model.Task{Name: "my-tmpl", Executor: minExec()}
	wf := makeWorkflow(5)

	a, err := BuildTaskAssignment(100, tr, taskDecl, nil, wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.TaskRunID != 10 {
		t.Errorf("expected TaskRunID=10, got %d", a.TaskRunID)
	}
	if a.WorkflowRunID != 100 {
		t.Errorf("expected WorkflowRunID=100, got %d", a.WorkflowRunID)
	}
	if a.TaskName != "my-task" {
		t.Errorf("expected TaskName=my-task, got %q", a.TaskName)
	}
	if a.Priority != 5 {
		t.Errorf("expected Priority=5, got %d", a.Priority)
	}
}

func TestBuildTaskAssignment_ExecutorInfo(t *testing.T) {
	tr := &store.TaskRun{RunID: 1, TaskName: "t"}
	taskDecl := &model.Task{
		Name:     "t",
		Executor: &model.Executor{Type: "function", Config: json.RawMessage(`{"fn":"main"}`)},
	}
	wf := makeWorkflow(0)

	a, err := BuildTaskAssignment(1, tr, taskDecl, nil, wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.ExecutorType != "function" {
		t.Errorf("expected executor type function, got %q", a.ExecutorType)
	}
}

func TestBuildTaskAssignment_TimeoutTemplateLevel(t *testing.T) {
	tr := &store.TaskRun{RunID: 1, TaskName: "t"}
	taskDecl := &model.Task{Name: "t", Timeout: "10m", Executor: minExec()}
	wf := makeWorkflow(0)

	a, err := BuildTaskAssignment(1, tr, taskDecl, nil, wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.Timeout != "10m" {
		t.Errorf("expected timeout=10m, got %q", a.Timeout)
	}
}

func TestBuildTaskAssignment_TimeoutTaskOverridesTemplate(t *testing.T) {
	tr := &store.TaskRun{RunID: 1, TaskName: "t"}
	taskDecl := &model.Task{Name: "t", Timeout: "10m", Executor: minExec()}
	taskCall := &model.Task{Name: "t", Timeout: "30s"}
	wf := makeWorkflow(0)

	a, err := BuildTaskAssignment(1, tr, taskDecl, taskCall, wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.Timeout != "30s" {
		t.Errorf("expected callSite timeout 30s to override definition 10m, got %q", a.Timeout)
	}
}

func TestBuildTaskAssignment_TaskNilDoesNotPanic(t *testing.T) {
	tr := &store.TaskRun{RunID: 1, TaskName: "t"}
	taskDecl := &model.Task{Name: "t", Timeout: "5m", Executor: minExec()}
	wf := makeWorkflow(0)

	a, err := BuildTaskAssignment(1, tr, taskDecl, nil, wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.Timeout != "5m" {
		t.Errorf("expected definition timeout 5m when taskCall is nil, got %q", a.Timeout)
	}
}

func TestBuildTaskAssignment_InputsMerged(t *testing.T) {
	tr := &store.TaskRun{RunID: 1, TaskName: "t"}
	taskDecl := &model.Task{
		Name:     "t",
		Executor: minExec(),
		Inputs: &model.Inputs{
			Parameters: []model.Parameter{
				{Name: "region", Default: rawJSON("us-east-1")},
				{Name: "env", Default: rawJSON("dev")},
			},
		},
	}
	taskCall := &model.Task{
		Name: "t",
		Arguments: &model.Arguments{
			Parameters: []model.Parameter{
				{Name: "env", Value: rawJSON("prod")}, // override
			},
		},
	}
	wf := makeWorkflow(0)

	a, err := BuildTaskAssignment(1, tr, taskDecl, taskCall, wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(a.Inputs) == 0 {
		t.Fatal("expected inputs to be set")
	}

	var inputs model.Inputs
	if err := json.Unmarshal(a.Inputs, &inputs); err != nil {
		t.Fatalf("failed to unmarshal inputs: %v", err)
	}
	paramMap := make(map[string]json.RawMessage)
	for _, p := range inputs.Parameters {
		paramMap[p.Name] = p.Value
	}
	// "env" should be overridden to "prod"
	if string(paramMap["env"]) != `"prod"` {
		t.Errorf("expected env=prod, got %q", paramMap["env"])
	}
	// "region" keeps default (nil value from merge)
	if _, ok := paramMap["region"]; !ok {
		t.Error("expected region key to be present")
	}
}

func TestBuildTaskAssignment_Resources(t *testing.T) {
	tr := &store.TaskRun{RunID: 1, TaskName: "t"}
	taskDecl := &model.Task{
		Name:      "t",
		Executor:  minExec(),
		Resources: &model.Resources{Memory: "512Mi"},
	}
	wf := makeWorkflow(0)

	a, err := BuildTaskAssignment(1, tr, taskDecl, nil, wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(a.Resources) == 0 {
		t.Fatal("expected resources to be set")
	}

	var res model.Resources
	if err := json.Unmarshal(a.Resources, &res); err != nil {
		t.Fatalf("unmarshal resources: %v", err)
	}
	if res.Memory != "512Mi" {
		t.Errorf("expected Memory=512Mi, got %q", res.Memory)
	}
}

// ---- mergeContinueOn (via isDependencySatisfied indirectly tested above;
//      test directly via FindReadyTasks with mixed policies) ----

func TestFindReadyTasks_MixedContinueOnUpstreamOverridesDAG(t *testing.T) {
	// DAG-level: failed=false; upstream task a: failed=true → upstream task wins
	dag := &model.DAG{
		ContinueOn: &model.ContinueOn{Failed: false},
		Tasks: []model.Task{
			{Name: "a", ContinueOn: &model.ContinueOn{Failed: true}},
			{Name: "b", Dependencies: []string{"a"}},
		},
	}
	existing := []*store.TaskRun{
		{TaskName: "a", Status: phasePtr(model.PhaseFailed)},
	}
	ready := FindReadyTasks(dag, existing)
	if len(ready) != 1 || ready[0].Name != "b" {
		t.Errorf("expected upstream task-level continueOn to allow b; got %v", ready)
	}
}

func TestFindReadyTasks_TimeoutContinueOn(t *testing.T) {
	// ContinueOn.timeout is on the upstream task a
	dag := &model.DAG{
		Tasks: []model.Task{
			{Name: "a", ContinueOn: &model.ContinueOn{Timeout: true}},
			{Name: "b", Dependencies: []string{"a"}},
		},
	}
	existing := []*store.TaskRun{
		{TaskName: "a", Status: phasePtr(model.PhaseTimeout)},
	}
	ready := FindReadyTasks(dag, existing)
	if len(ready) != 1 || ready[0].Name != "b" {
		t.Errorf("expected b to be ready after timeout with upstream continueOn.timeout=true; got %v", ready)
	}
}
