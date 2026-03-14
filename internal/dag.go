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

// extractEntrypointNames returns the set of task names declared in dag.Entrypoints.
// Returns nil if no Entrypoints is specified.
func extractEntrypointNames(dag *model.DAG) map[string]bool {
	if dag.Entrypoints == nil {
		return nil
	}
	switch ep := dag.Entrypoints.(type) {
	case string:
		return map[string]bool{ep: true}
	case []any:
		names := make(map[string]bool, len(ep))
		for _, v := range ep {
			if s, ok := v.(string); ok {
				names[s] = true
			}
		}
		return names
	}
	return nil
}

// computeReachable returns the set of all task names reachable from the given
// entrypoint names by following the DAG edges forward (entrypoint → downstream).
// If entrypoints is nil, all tasks are considered reachable.
func computeReachable(dag *model.DAG, entrypoints map[string]bool) map[string]bool {
	if entrypoints == nil {
		return nil // nil means "all tasks"
	}

	// Build forward adjacency: dep → tasks that depend on dep
	fwd := make(map[string][]string)
	for _, task := range dag.Tasks {
		for _, dep := range task.Dependencies {
			fwd[dep] = append(fwd[dep], task.Name)
		}
	}

	reachable := make(map[string]bool)
	queue := make([]string, 0, len(entrypoints))
	for name := range entrypoints {
		if !reachable[name] {
			reachable[name] = true
			queue = append(queue, name)
		}
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range fwd[cur] {
			if !reachable[next] {
				reachable[next] = true
				queue = append(queue, next)
			}
		}
	}
	return reachable
}

// FindReadyTasks finds tasks whose dependencies are all satisfied.
// ContinueOn is read from the upstream (dependency) task: if a task declares continueOn,
// it allows its downstream tasks to proceed even when it ends in a non-success state.
// Only returns tasks that haven't been created yet (not in existing task runs).
// When dag.Entrypoints is set, only tasks reachable from those entrypoints are considered.
func FindReadyTasks(dag *model.DAG, existingRuns []*store.TaskRun) []model.Task {
	taskStatus := make(map[string]model.Phase)
	for _, tr := range existingRuns {
		taskStatus[tr.TaskName] = tr.Status
	}

	// Build a name→task index for quick dep lookup
	taskByName := make(map[string]*model.Task, len(dag.Tasks))
	for i := range dag.Tasks {
		taskByName[dag.Tasks[i].Name] = &dag.Tasks[i]
	}

	reachable := computeReachable(dag, extractEntrypointNames(dag))

	ready := make([]model.Task, 0)
	for _, task := range dag.Tasks {
		// Skip tasks outside the reachable subgraph (only when entrypoints are set)
		if reachable != nil && !reachable[task.Name] {
			continue
		}

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
			// ContinueOn is declared on the upstream dep task
			var depTaskCO *model.ContinueOn
			if depTask, ok := taskByName[dep]; ok {
				depTaskCO = depTask.ContinueOn
			}
			if !isDependencySatisfied(depStatus, depTaskCO, dag.ContinueOn) {
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
// is acceptable for downstream tasks to proceed.
//
// A dependency is satisfied if:
//   - Succeeded or Skipped (always OK)
//   - Failed and depTask.continueOn.failed is true
//   - Error and depTask.continueOn.error is true
//   - Timeout and depTask.continueOn.timeout is true
//
// depTaskCO is the ContinueOn of the upstream (dependency) task.
// dagCO is the DAG-level default; depTask-level takes precedence.
func isDependencySatisfied(depStatus model.Phase, depTaskCO, dagCO *model.ContinueOn) bool {
	if depStatus == model.PhaseSucceeded || depStatus == model.PhaseSkipped {
		return true
	}

	co := mergeContinueOn(depTaskCO, dagCO)
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

// BuildTaskAssignment assembles a self-contained TaskAssignment that is passed to broker.Dispatch.
//
// Call timing: tr is still in Pending state when this is called. This function only merges
// information — it does not trigger any state transition. The state change Pending → Running
// happens later, when the worker calls broker.StartTask after receiving the assignment.
//
// Three information sources are merged:
//
//   - tr    (TaskRun)  : runtime IDs (TaskRunID, WorkflowRunID), task/template name
//   - tmpl  (Template) : definition defaults — executor, inputs, timeout, resources
//   - task  (DAG node) : call-site overrides — task.Arguments overrides tmpl inputs defaults;
//     task.Timeout overrides tmpl.Timeout
//
// Example — template defines timeout=10m and env=dev; DAG node overrides timeout=30s and env=prod:
//
//	tmpl.inputs    = [{name:"region", default:"us-east-1"}, {name:"env", default:"dev"}]
//	tmpl.timeout   = "10m"
//	task.arguments = [{name:"env", value:"prod"}]
//	task.timeout   = "30s"
//
//	→ assignment.Timeout = "30s"                                        (task overrides tmpl)
//	→ assignment.Inputs  = [{name:"region", default:"us-east-1"},       (tmpl default kept)
//	                         {name:"env",    value:"prod"}]              (task override wins)
//
// task may be nil when:
//   - ParentRunID == 0 (top-level task): no parent DAG node to look up.
//   - Parent is a Loop: iterations are dispatched directly without a DAG task node.
//
// When task is nil, only template-level defaults (timeout, inputs, resources) apply.
func BuildTaskAssignment(workflowRunID uint64, tr *store.TaskRun, tmpl *model.Template, task *model.Task, wf *model.Workflow) (*broker.TaskAssignment, error) {
	exec := tmpl.GetExecutor()
	if exec == nil {
		return nil, fmt.Errorf("template %q has no executor: cannot build assignment for task %q", tmpl.GetName(), tr.TaskName)
	}

	assignment := &broker.TaskAssignment{
		TaskRunID:      tr.RunID,
		WorkflowRunID:  workflowRunID,
		TaskName:       tr.TaskName,
		TemplateName:   tr.TemplateName,
		Priority:       wf.Spec.Priority,
		ExecutorType:   exec.Type,
		ExecutorConfig: exec.Config,
	}

	// Timeout: task-level overrides template-level (task may be nil)
	timeout := tmpl.GetTimeout()
	if task != nil && task.Timeout != "" {
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

	return assignment, nil
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
