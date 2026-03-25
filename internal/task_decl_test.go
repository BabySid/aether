package internal

import (
	"testing"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

// buildWF builds a *model.Workflow from the given templates.
func buildWF(templates ...model.Template) *model.Workflow {
	return &model.Workflow{Spec: model.WorkflowSpec{Templates: templates}}
}

// tmplTask builds a task-type Template with the given name and optional executor.
func tmplTask(name string, executor *model.Executor) model.Template {
	return model.Template{Task: &model.Task{Name: name, Executor: executor}}
}

// tmplDAG builds a DAG-type Template with the given name and tasks.
func tmplDAG(name string, tasks ...model.Task) model.Template {
	return model.Template{DAG: &model.DAG{Name: name, Tasks: tasks}}
}

// tmplLoop builds a Loop-type Template with the given name.
func tmplLoop(name string) model.Template {
	return model.Template{Loop: &model.Loop{Name: name}}
}

// trFor builds a TaskRun with the given templateName, taskName, and parentRunID.
func trFor(templateName, taskName, parentRunID string) *store.TaskRun {
	return &store.TaskRun{RunID: "tr-1", TemplateName: templateName, TaskName: taskName, ParentRunID: parentRunID}
}

// ptrFor builds a parent TaskRun with the given runID and templateName.
func ptrFor(runID, templateName string) *store.TaskRun {
	return &store.TaskRun{RunID: runID, TemplateName: templateName}
}

// ─── ResolveTaskDecl tests ────────────────────────────────────────────────────

func TestResolveTaskDecl_NamedTemplate(t *testing.T) {
	exec := &model.Executor{Type: "script"}
	wf := buildWF(tmplTask("my-task", exec))
	tr := trFor("my-task", "step1", "")

	taskDecl, taskCall, isLoop := ResolveTaskDecl(wf, tr, nil)

	if taskDecl == nil {
		t.Fatal("expected taskDecl from named template")
	}
	if taskDecl.Executor != exec {
		t.Fatal("expected executor from named template")
	}
	if taskCall != nil {
		t.Fatal("expected nil taskCall (no parent)")
	}
	if isLoop {
		t.Fatal("expected isLoopIteration=false")
	}
}

func TestResolveTaskDecl_InlineExecutorInDAG(t *testing.T) {
	// No named template; executor declared inline on the DAG task node.
	exec := &model.Executor{Type: "inline"}
	dagTask := model.Task{Name: "step1", Executor: exec}
	wf := buildWF(tmplDAG("main-dag", dagTask))

	tr := trFor("", "step1", "parent-1")
	pTR := ptrFor("parent-1", "main-dag")

	taskDecl, taskCall, isLoop := ResolveTaskDecl(wf, tr, pTR)

	if taskDecl == nil {
		t.Fatal("expected taskDecl from inline executor")
	}
	if taskDecl.Executor != exec {
		t.Fatal("expected inline executor")
	}
	if taskCall == nil || taskCall.Name != "step1" {
		t.Fatal("expected taskCall from DAG task node")
	}
	if isLoop {
		t.Fatal("expected isLoopIteration=false")
	}
}

func TestResolveTaskDecl_NamedTemplateWinsOverInline(t *testing.T) {
	// Both named template and inline executor on DAG node; named template wins for taskDecl.
	namedExec := &model.Executor{Type: "named"}
	inlineExec := &model.Executor{Type: "inline"}
	dagTask := model.Task{Name: "step1", Executor: inlineExec}
	wf := buildWF(
		tmplTask("step-tmpl", namedExec),
		tmplDAG("main-dag", dagTask),
	)

	tr := trFor("step-tmpl", "step1", "parent-1")
	pTR := ptrFor("parent-1", "main-dag")

	taskDecl, taskCall, _ := ResolveTaskDecl(wf, tr, pTR)

	if taskDecl == nil || taskDecl.Executor != namedExec {
		t.Fatal("named template should win for taskDecl")
	}
	if taskCall == nil {
		t.Fatal("taskCall should be set from DAG task node")
	}
}

func TestResolveTaskDecl_LoopParent(t *testing.T) {
	wf := buildWF(
		tmplTask("iter-tmpl", &model.Executor{Type: "script"}),
		tmplLoop("my-loop"),
	)

	tr := trFor("iter-tmpl", "iter[0]", "loop-run-1")
	pTR := ptrFor("loop-run-1", "my-loop")

	taskDecl, taskCall, isLoop := ResolveTaskDecl(wf, tr, pTR)

	if taskDecl == nil {
		t.Fatal("expected taskDecl from named template")
	}
	if taskCall != nil {
		t.Fatal("loop iterations have no taskCall")
	}
	if !isLoop {
		t.Fatal("expected isLoopIteration=true")
	}
}

func TestResolveTaskDecl_NilParentTR(t *testing.T) {
	exec := &model.Executor{Type: "script"}
	wf := buildWF(tmplTask("top-task", exec))
	tr := trFor("top-task", "step1", "")

	taskDecl, taskCall, isLoop := ResolveTaskDecl(wf, tr, nil)

	if taskDecl == nil {
		t.Fatal("expected taskDecl")
	}
	if taskCall != nil {
		t.Fatal("expected nil taskCall for top-level task")
	}
	if isLoop {
		t.Fatal("expected isLoopIteration=false")
	}
}

func TestResolveTaskDecl_TemplateNotFound(t *testing.T) {
	wf := buildWF() // empty templates
	tr := trFor("missing-tmpl", "step1", "")

	taskDecl, taskCall, isLoop := ResolveTaskDecl(wf, tr, nil)

	if taskDecl != nil {
		t.Fatal("expected nil taskDecl when template not found")
	}
	if taskCall != nil {
		t.Fatal("expected nil taskCall")
	}
	if isLoop {
		t.Fatal("expected isLoopIteration=false")
	}
}

func TestResolveTaskDecl_DAGTaskNotFound(t *testing.T) {
	// Parent DAG exists but has no task matching tr.TaskName.
	wf := buildWF(tmplDAG("main-dag")) // no tasks
	tr := trFor("", "ghost-task", "parent-1")
	pTR := ptrFor("parent-1", "main-dag")

	taskDecl, taskCall, isLoop := ResolveTaskDecl(wf, tr, pTR)

	if taskDecl != nil {
		t.Fatal("expected nil taskDecl")
	}
	if taskCall != nil {
		t.Fatal("expected nil taskCall")
	}
	if isLoop {
		t.Fatal("expected isLoopIteration=false")
	}
}

func TestResolveTaskDecl_DAGNodeWithoutInlineExecutor(t *testing.T) {
	// DAG task has NO inline executor — taskDecl stays from named template only.
	namedExec := &model.Executor{Type: "named"}
	dagTask := model.Task{Name: "step1"} // no Executor field
	wf := buildWF(
		tmplTask("step-tmpl", namedExec),
		tmplDAG("main-dag", dagTask),
	)

	tr := trFor("step-tmpl", "step1", "parent-1")
	pTR := ptrFor("parent-1", "main-dag")

	taskDecl, taskCall, _ := ResolveTaskDecl(wf, tr, pTR)

	if taskDecl == nil || taskDecl.Executor != namedExec {
		t.Fatal("taskDecl should come from named template")
	}
	if taskCall == nil {
		t.Fatal("taskCall should be set from DAG node")
	}
}
