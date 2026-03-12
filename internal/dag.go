package internal

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/expr"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// HasCycle detects cycles in a DAG using DFS.
func HasCycle(dag *model.DAG) bool {
	adj := make(map[string][]string)
	for _, task := range dag.Tasks {
		for _, dep := range task.Dependencies {
			adj[dep] = append(adj[dep], task.Name)
		}
	}

	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int)

	var dfs func(node string) bool
	dfs = func(node string) bool {
		color[node] = gray
		for _, next := range adj[node] {
			switch color[next] {
			case gray:
				return true
			case white:
				if dfs(next) {
					return true
				}
			}
		}
		color[node] = black
		return false
	}

	for _, task := range dag.Tasks {
		if color[task.Name] == white {
			if dfs(task.Name) {
				return true
			}
		}
	}
	return false
}

// FindTemplate finds a template by name in the workflow.
func FindTemplate(wf *model.Workflow, name string) *model.Template {
	for i := range wf.Spec.Templates {
		if wf.Spec.Templates[i].GetName() == name {
			return &wf.Spec.Templates[i]
		}
	}
	return nil
}

// FindRootTasks returns tasks with no dependencies (entry points of the DAG).
func FindRootTasks(dag *model.DAG) []model.Task {
	roots := make([]model.Task, 0)

	if dag.Entrypoints != nil {
		switch ep := dag.Entrypoints.(type) {
		case string:
			for _, task := range dag.Tasks {
				if task.Name == ep {
					roots = append(roots, task)
					break
				}
			}
			return roots
		case []any:
			epNames := make(map[string]bool)
			for _, v := range ep {
				if s, ok := v.(string); ok {
					epNames[s] = true
				}
			}
			for _, task := range dag.Tasks {
				if epNames[task.Name] {
					roots = append(roots, task)
				}
			}
			return roots
		}
	}

	for _, task := range dag.Tasks {
		if len(task.Dependencies) == 0 {
			roots = append(roots, task)
		}
	}
	return roots
}

// FindReadyTasks finds tasks whose dependencies are all satisfied.
// Takes into account continueOn policies at both task and DAG level.
// Only returns tasks that haven't been created yet (not in existing task runs).
func FindReadyTasks(dag *model.DAG, existingRuns []*store.TaskRun) []model.Task {
	taskStatus := make(map[string]model.Phase)
	for _, tr := range existingRuns {
		taskStatus[tr.TaskName] = tr.Status
	}

	ready := make([]model.Task, 0)
	for _, task := range dag.Tasks {
		// Skip tasks that already have a TaskRun
		if _, exists := taskStatus[task.Name]; exists {
			continue
		}

		allDepsReady := true
		for _, dep := range task.Dependencies {
			depStatus, exists := taskStatus[dep]
			if !exists || !depStatus.IsTerminal() {
				allDepsReady = false
				break
			}
			if !isDependencySatisfied(depStatus, task.ContinueOn, dag.ContinueOn) {
				allDepsReady = false
				break
			}
		}

		if allDepsReady {
			ready = append(ready, task)
		}
	}
	return ready
}

// isDependencySatisfied checks whether a dependency's terminal status
// is acceptable for the dependent task to proceed.
//
// A dependency is satisfied if:
//   - Succeeded or Skipped (always OK)
//   - Failed and continueOn.failed is true
//   - Error and continueOn.error is true
//   - Timeout and continueOn.timeout is true
//
// continueOn is resolved by merging task-level and DAG-level policies (task takes precedence).
func isDependencySatisfied(depStatus model.Phase, taskCO, dagCO *model.ContinueOn) bool {
	if depStatus == model.PhaseSucceeded || depStatus == model.PhaseSkipped {
		return true
	}

	co := mergeContinueOn(taskCO, dagCO)
	if co == nil {
		return false
	}

	switch depStatus {
	case model.PhaseFailed:
		return co.Failed
	case model.PhaseError:
		return co.Error
	case model.PhaseTimeout:
		return co.Timeout
	default:
		return false
	}
}

// mergeContinueOn merges task-level and DAG-level continueOn policies.
// Task-level fields take precedence; DAG-level is the fallback.
func mergeContinueOn(taskCO, dagCO *model.ContinueOn) *model.ContinueOn {
	if taskCO == nil && dagCO == nil {
		return nil
	}
	if taskCO == nil {
		return dagCO
	}
	if dagCO == nil {
		return taskCO
	}
	// Merge: task-level takes precedence via OR (task || dag)
	return &model.ContinueOn{
		Failed:  taskCO.Failed || dagCO.Failed,
		Error:   taskCO.Error || dagCO.Error,
		Timeout: taskCO.Timeout || dagCO.Timeout,
	}
}

// EvalWhenCondition evaluates a task's "when" expression.
// Returns true if the task should execute, false if it should be skipped.
// If eval is nil or when is empty, returns true (always execute).
func EvalWhenCondition(ctx context.Context, when string, eval expr.Evaluator, taskRuns []*store.TaskRun) (bool, error) {
	if when == "" {
		return true, nil
	}
	if eval == nil {
		// No evaluator — cannot evaluate, treat as always true
		return true, nil
	}

	env := BuildTaskEnv(taskRuns)
	result, err := eval.Eval(ctx, when, env)
	if err != nil {
		return false, fmt.Errorf("eval when %q: %w", when, err)
	}

	switch v := result.(type) {
	case bool:
		return v, nil
	case string:
		return v == "true", nil
	default:
		return false, fmt.Errorf("when expression %q returned non-boolean: %v", when, result)
	}
}

// BuildTaskEnv builds an environment map from existing task runs for expression evaluation.
// Keys follow the pattern:
//
//	tasks.<taskName>.phase    → "Succeeded" / "Failed" / ...
//	tasks.<taskName>.code     → "0"
//	tasks.<taskName>.msg      → "error message"
func BuildTaskEnv(taskRuns []*store.TaskRun) map[string]any {
	env := make(map[string]any)
	for _, tr := range taskRuns {
		prefix := "tasks." + tr.TaskName
		env[prefix+".phase"] = string(tr.Status)
		if tr.Outputs != nil {
			env[prefix+".code"] = tr.Outputs.Code
			env[prefix+".msg"] = tr.Outputs.Msg
			for _, p := range tr.Outputs.Parameters {
				var val any
				if err := json.Unmarshal(p.Value, &val); err == nil {
					env[prefix+".outputs.parameters."+p.Name] = val
				} else {
					env[prefix+".outputs.parameters."+p.Name] = string(p.Value)
				}
			}
		}
	}
	return env
}

// BuildTaskAssignment creates a TaskAssignment from a TaskRun, its template, and the task definition.
// Merges task.arguments into template inputs for the "fat assignment".
//
// Preconditions (guaranteed by Validate at Submit time):
//   - tmpl is non-nil (task.Template is required and references a valid template)
//   - task is non-nil (TaskRun names match DAG task names)
func BuildTaskAssignment(workflowRunID uint64, tr *store.TaskRun, tmpl *model.Template, task *model.Task, wf *model.Workflow) *broker.TaskAssignment {
	assignment := &broker.TaskAssignment{
		TaskRunID:     tr.RunID,
		WorkflowRunID: workflowRunID,
		TaskName:      tr.TaskName,
		TemplateName:  tr.TemplateName,
		Priority:      wf.Spec.Priority,
	}

	if exec := tmpl.GetExecutor(); exec != nil {
		assignment.ExecutorType = exec.Type
		assignment.ExecutorConfig = exec.Config
	}

	// Timeout: task-level overrides template-level
	timeout := tmpl.GetTimeout()
	if task.Timeout != "" {
		timeout = task.Timeout
	}
	assignment.Timeout = timeout

	// Inputs: start with template inputs, merge task arguments on top
	inputs := resolveInputs(tmpl.GetInputs(), task)
	if inputs != nil {
		inputsJSON, _ := json.Marshal(inputs)
		assignment.Inputs = inputsJSON
	}

	if res := tmpl.GetResources(); res != nil {
		resourcesJSON, _ := json.Marshal(res)
		assignment.Resources = resourcesJSON
	}

	return assignment
}

// resolveInputs merges template inputs with task arguments.
// Task arguments override template input defaults.
func resolveInputs(templateInputs *model.Inputs, task *model.Task) *model.Inputs {
	if templateInputs == nil && (task == nil || task.Arguments == nil) {
		return nil
	}

	result := &model.Inputs{}

	// Start with template inputs
	if templateInputs != nil {
		result.Parameters = append(result.Parameters, templateInputs.Parameters...)
		result.Artifacts = append(result.Artifacts, templateInputs.Artifacts...)
	}

	// Override with task arguments
	if task != nil && task.Arguments != nil {
		// Override parameters by name
		for _, arg := range task.Arguments.Parameters {
			found := false
			for i := range result.Parameters {
				if result.Parameters[i].Name == arg.Name {
					result.Parameters[i].Value = arg.Value
					found = true
					break
				}
			}
			if !found {
				result.Parameters = append(result.Parameters, arg)
			}
		}
		// Override artifacts by name
		for _, arg := range task.Arguments.Artifacts {
			found := false
			for i := range result.Artifacts {
				if result.Artifacts[i].Name == arg.Name {
					result.Artifacts[i] = arg
					found = true
					break
				}
			}
			if !found {
				result.Artifacts = append(result.Artifacts, arg)
			}
		}
	}

	return result
}

// FindTask finds a task by name within a DAG.
func FindTask(dag *model.DAG, name string) *model.Task {
	for i := range dag.Tasks {
		if dag.Tasks[i].Name == name {
			return &dag.Tasks[i]
		}
	}
	return nil
}
