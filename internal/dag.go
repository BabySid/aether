package internal

import (
	"context"
	"fmt"

	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/expr"
	ivars "github.com/BabySid/aether/internal/vars"
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
		if tr.Status != nil {
			taskStatus[tr.TaskName] = *tr.Status
		}
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

	co := MergeContinueOn(depTaskCO, dagCO)
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

// MergeContinueOn returns the effective continueOn policy for a task.
// If the task declares its own continueOn, it is used as-is (full override).
// Otherwise the DAG-level default is used as a fallback.
func MergeContinueOn(taskCO, dagCO *model.ContinueOn) *model.ContinueOn {
	if taskCO != nil {
		return taskCO
	}
	return dagCO
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
	src := &ivars.SiblingTaskRunsSource{Runs: taskRuns}
	return src.Vars()
}

// BuildTaskAssignment assembles a self-contained TaskAssignment that is passed to broker.Dispatch.
//
// Call timing: tr is still in Pending state when this is called. This function only merges
// information — it does not trigger any state transition. The state change Pending → Running
// happens later, when the worker calls broker.StartTask after receiving the assignment.
//
// Two information sources are merged:
//
//   - taskDecl      : definition layer — executor, inputs declaration, timeout default, resources.
//     Either the named template's Task field (template-ref mode) or the inline DAG task
//     node itself when it carries an executor (inline-executor mode).
//   - taskCall : call-site layer — arguments and timeout override supplied by the DAG
//     task node that references the template.  Nil when the task is a top-level entry or
//     a loop iteration (no DAG task node exists at the call site).
//
// Example — taskDecl defines timeout=10m and env=dev; taskCall overrides timeout=30s and env=prod:
//
//	taskDecl.inputs      = [{name:"region", default:"us-east-1"}, {name:"env", default:"dev"}]
//	taskDecl.timeout     = "10m"
//	taskCall.arguments = [{name:"env", value:"prod"}]
//	taskCall.timeout   = "30s"
//
//	→ assignment.Timeout = "30s"                                           (callSite overrides def)
//	→ assignment.Inputs  = [{name:"region", default:"us-east-1"},          (def default kept)
//	                         {name:"env",    value:"prod"}]                 (callSite override wins)
//
// taskCall may be nil when:
//   - ParentRunID == 0 (top-level task): no parent DAG node to look up.
//   - Parent is a Loop: iterations are dispatched directly without a DAG task node.
//
// When taskCall is nil, only definition-level defaults (timeout, inputs, resources) apply.
// BuildTaskAssignment assembles a TaskAssignment skeleton (executor, timeout, resources).
//
// Inputs are intentionally excluded: callers must resolve and set assignment.Inputs
// separately via binding.Binder.Bind() to ensure valueFrom and template variables are
// expanded before the assignment is dispatched to the broker.
func BuildTaskAssignment(workflowRunID string, tr *store.TaskRun, taskDecl *model.Task, taskCall *model.Task, wf *model.Workflow) (*broker.TaskAssignment, error) {
	if taskDecl == nil {
		return nil, fmt.Errorf("no task definition for task %q: cannot build assignment", tr.TaskName)
	}
	exec := taskDecl.Executor
	if exec == nil {
		return nil, fmt.Errorf("task definition %q has no executor: cannot build assignment for task %q", taskDecl.Name, tr.TaskName)
	}

	retryCount := 0
	if tr.RetryCount != nil {
		retryCount = *tr.RetryCount
	}
	assignment := &broker.TaskAssignment{
		TaskRunID:     tr.RunID,
		WorkflowRunID: workflowRunID,
		TaskName:      tr.TaskName,
		TemplateName:  tr.TemplateName,
		Priority:      wf.Spec.Priority,
		ExecutorType:  exec.Type,
		RetryCount:    retryCount,
	}

	// Timeout: callSite-level overrides definition-level (taskCall may be nil)
	timeout := taskDecl.Timeout
	if taskCall != nil && taskCall.Timeout != "" {
		timeout = taskCall.Timeout
	}
	assignment.Timeout = timeout

	// Resources: from definition only
	assignment.Resources = taskDecl.Resources

	return assignment, nil
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
