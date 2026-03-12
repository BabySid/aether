package aether

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BabySid/aether/internal"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// advanceScope is the core iterative scheduling function.
// It processes the scope identified by startParentRunID, then walks up the scope
// tree until the workflow is finalized or a scope is still in progress.
//
// The iterative up-walk replaces the previous advanceScope ↔ propagateUp mutual
// tail recursion: instead of propagateUp calling advanceScope(parent), we simply
// set parentRunID = parentTR.ParentRunID and continue the loop.
//
// # Scope tree model
//
// Every DAG/Loop container is a scope boundary. The tree mirrors template nesting:
//
//	parentRunID=0  (virtual workflow root)
//	└── main-dag   (RunID=10, type=DAG)  ← scope 10
//	    ├── fetch  (RunID=11, type=Task)
//	    └── notify (RunID=12, type=Task)
//
// advanceScope(startParentRunID=0) processes the root scope, activating "main-dag".
// advanceScope(startParentRunID=10) processes the DAG scope, creating and dispatching
// "fetch" and "notify".
//
// When "fetch" completes, OnTaskCompleted calls advanceScope(startParentRunID=10).
// If all siblings in scope 10 are terminal, the loop marks "main-dag" done,
// sets parentRunID=0, and continues to the root scope where finalizeWorkflow is called.
func (e *Engine) advanceScope(ctx context.Context, workflowRunID uint64, wf *model.Workflow, startParentRunID uint64) error {
	parentRunID := startParentRunID

	for {
		// 1. Load all TaskRuns in this scope (siblings = same parentRunID).
		siblings, err := e.store.ListTaskRunsByParent(ctx, workflowRunID, parentRunID)
		if err != nil {
			return err
		}

		// 2. Activate any Pending TaskRuns in this scope.
		// Containers (DAG/Loop) transition to Running and recurse into their child scope.
		// Leaf tasks (type=task) are dispatched to the Broker directly.
		for _, sib := range siblings {
			if sib.Status != model.PhasePending {
				continue
			}
			if err := e.activateTaskRun(ctx, workflowRunID, wf, sib); err != nil {
				return err
			}
		}

		// 3. If this scope is owned by a DAG container, check whether more tasks are
		// now ready (i.e., all their declared dependencies are terminal).
		// Skipped for parentRunID=0 (root scope) because the root has no DAG.
		if parentRunID != 0 {
			if err := e.createReadyTasks(ctx, workflowRunID, wf, parentRunID, siblings); err != nil {
				return err
			}
		}

		// 4. Re-read siblings after createReadyTasks, which may have added new TaskRuns
		// (newly ready tasks) or changed statuses (skipped tasks).
		siblings, err = e.store.ListTaskRunsByParent(ctx, workflowRunID, parentRunID)
		if err != nil {
			return err
		}

		// 5. If any sibling is still non-terminal, this scope is still in progress.
		// allTerminal returns false for an empty list (no tasks yet = not done).
		if !allTerminal(siblings) {
			return nil
		}

		// 6. All siblings are terminal — this scope is complete.
		// Root scope (parentRunID=0): finalize the entire workflow and stop.
		if parentRunID == 0 {
			e.finalizeWorkflow(ctx, workflowRunID, siblings, wf)
			return nil
		}

		// Non-root scope: aggregate children into the parent container's result,
		// then walk up one level (replacing the propagateUp tail call with a loop step).
		parentTR, err := e.store.GetTaskRun(ctx, parentRunID)
		if err != nil {
			return err
		}
		phase, msg := aggregatePhase(siblings)
		parentTR.Status = phase
		parentTR.Message = msg
		if err := e.store.UpdateTaskRun(ctx, parentTR); err != nil {
			return err
		}

		// Move up to the parent's parent scope and re-evaluate in the next iteration.
		parentRunID = parentTR.ParentRunID
	}
}

// activateTaskRun transitions a Pending TaskRun to its active state based on TemplateType.
//
// The three template types have fundamentally different activation paths:
//
//   - DAG (container): immediately enters Running and opens a child scope.
//     Its children (tasks within the DAG) are created and dispatched by advanceScope.
//     DAG itself never reaches a Broker — it's done when all its children are terminal.
//
//   - Task (leaf): dispatched directly to the Broker for execution.
//     The Broker runs the executor and calls back OnTaskCompleted when done.
//     Does NOT enter Running here — the Broker manages that transition.
//
//   - Loop (container): immediately enters Running and starts a loop controller.
//     The controller is responsible for expanding items / evaluating repeatCondition
//     and spawning iteration TaskRuns.
func (e *Engine) activateTaskRun(ctx context.Context, workflowRunID uint64, wf *model.Workflow, tr *store.TaskRun) error {
	switch tr.TemplateType {
	case model.TemplateTypeDAG:
		// Mark Running before entering child scope so that the parent scope's
		// allTerminal check correctly sees this node as in-progress.
		tr.Status = model.PhaseRunning
		if err := e.store.UpdateTaskRun(ctx, tr); err != nil {
			return err
		}
		// tr.RunID becomes the startParentRunID for the child scope.
		return e.advanceScope(ctx, workflowRunID, wf, tr.RunID)

	case model.TemplateTypeTask:
		// Leaf task: build a TaskAssignment (executor + resolved inputs) and hand off
		// to the Broker. Status transitions to Running inside the Broker/Executor.
		return e.dispatchLeafTask(ctx, workflowRunID, wf, tr)

	case model.TemplateTypeLoop:
		// Mark Running before starting the loop controller.
		tr.Status = model.PhaseRunning
		if err := e.store.UpdateTaskRun(ctx, tr); err != nil {
			return err
		}
		// TODO: implement startLoopController — expand items/evaluate repeatCondition,
		// spawn iteration TaskRuns, and wire up completion aggregation.
		return nil

	default:
		return fmt.Errorf("unknown template type %q for task %q", tr.TemplateType, tr.TaskName)
	}
}

// createReadyTasks finds DAG tasks whose dependencies are all satisfied and no
// TaskRun exists yet, then creates and activates TaskRuns for them.
//
// The function loops instead of recursing: when tasks are skipped (their "when"
// condition evaluated to false), the loop refreshes siblings and re-evaluates
// readiness in the next iteration. This avoids unbounded recursion for DAGs with
// many skip-chains while keeping the eager-unblocking behavior.
//
// # When-condition evaluation
//
// Each ready task is evaluated against its "when" expression using sibling
// TaskRun outputs as the expression context:
//
//	task "notify":
//	  when: "{{tasks.review.outputs.parameters.decision}} == 'approve'"
//	  template: "send-approval"
//
// If the expression evaluates to false (or evaluation errors), the task is
// Skipped immediately (a Skipped TaskRun is persisted so downstream tasks
// can still proceed).
//
// # Why loop after skipping
//
// A skipped task is terminal. Its dependents may now be unblocked:
//
//	A ──► B (when: ..., skipped)
//	           └──► C (depends on B)
//
// After B is skipped, C's dependency on B is satisfied. The loop refreshes
// siblings and re-evaluates so C gets created without waiting for the next
// external event.
func (e *Engine) createReadyTasks(ctx context.Context, workflowRunID uint64, wf *model.Workflow, parentRunID uint64, siblings []*store.TaskRun) error {
	// parentTR and tmpl.DAG don't change across loop iterations — resolve once.
	parentTR, err := e.store.GetTaskRun(ctx, parentRunID)
	if err != nil {
		return err
	}
	tmpl := internal.FindTemplate(wf, parentTR.TemplateName)
	if tmpl == nil || tmpl.DAG == nil {
		// Parent is not a DAG (e.g., a Loop scope is handled elsewhere).
		return nil
	}

	for {
		// FindReadyTasks returns DAG tasks whose every dependency has a terminal
		// TaskRun in siblings, and which have no TaskRun of their own yet.
		readyTasks := internal.FindReadyTasks(tmpl.DAG, siblings)
		if len(readyTasks) == 0 {
			return nil
		}

		// Evaluate "when" conditions and partition into execute vs skip.
		// siblings provides the expression evaluation context (sibling outputs).
		var toExecute []model.Task
		var toSkip []model.Task
		for _, task := range readyTasks {
			if task.When == "" {
				toExecute = append(toExecute, task)
				continue
			}
			shouldRun, err := internal.EvalWhenCondition(ctx, task.When, e.exprEvaluator, siblings)
			if err != nil {
				// Treat evaluation errors as "skip" to avoid blocking the DAG.
				toSkip = append(toSkip, task)
				continue
			}
			if shouldRun {
				toExecute = append(toExecute, task)
			} else {
				toSkip = append(toSkip, task)
			}
		}

		// Persist Skipped TaskRuns immediately (they count as terminal for downstream deps).
		for _, task := range toSkip {
			taskTmpl := internal.FindTemplate(wf, task.Template)
			templateType := ""
			if taskTmpl != nil {
				templateType = internal.ResolveTemplateType(taskTmpl)
			}
			skippedRun := &store.TaskRun{
				RunID:         e.idGen.Generate(),
				WorkflowRunID: workflowRunID,
				ParentRunID:   parentRunID,
				Depth:         parentTR.Depth + 1,
				TaskName:      task.Name,
				TemplateName:  task.Template,
				TemplateType:  templateType,
				Status:        model.PhaseSkipped,
				Message:       fmt.Sprintf("when condition %q evaluated to false", task.When),
			}
			if err := e.store.CreateTaskRun(ctx, skippedRun); err != nil {
				return err
			}
		}

		// Create Pending TaskRuns for tasks that should execute, then activate immediately.
		for _, task := range toExecute {
			taskTmpl := internal.FindTemplate(wf, task.Template)
			templateType := ""
			if taskTmpl != nil {
				templateType = internal.ResolveTemplateType(taskTmpl)
			}
			newRun := &store.TaskRun{
				RunID:         e.idGen.Generate(),
				WorkflowRunID: workflowRunID,
				ParentRunID:   parentRunID,
				Depth:         parentTR.Depth + 1,
				TaskName:      task.Name,
				TemplateName:  task.Template,
				TemplateType:  templateType,
				Status:        model.PhasePending,
			}
			if err := e.store.CreateTaskRun(ctx, newRun); err != nil {
				return err
			}
			// Activate synchronously: containers enter Running + recurse; leaf tasks dispatch.
			if err := e.activateTaskRun(ctx, workflowRunID, wf, newRun); err != nil {
				return err
			}
		}

		// If no tasks were skipped in this round, downstream deps cannot have changed.
		if len(toSkip) == 0 {
			return nil
		}

		// Some tasks were skipped — their dependents may be newly ready.
		// Refresh siblings (which now include the freshly-skipped TaskRuns) and loop.
		siblings, err = e.store.ListTaskRunsByParent(ctx, workflowRunID, parentRunID)
		if err != nil {
			return err
		}
	}
}

// dispatchLeafTask builds and dispatches a TaskAssignment for a leaf task (templateType=task).
//
// Executing a task requires merging two independent sources of information:
//
//  1. Task Template (definition layer) — stored in wf.spec.templates[]:
//     executor type/config, inputs declaration, default timeout, resources.
//
//  2. DAG Task Node (call-site layer) — stored in parent DAG's tasks[]:
//     arguments (actual parameter values), per-call timeout override.
//
// Example workflow shape:
//
//	templates:
//	  main (DAG):
//	    tasks:
//	      - name: "fetch",  template: "http-fetch"
//	      - name: "notify", template: "ding-alert", dependencies: ["fetch"]
//	        arguments: { msg: "{{tasks.fetch.outputs.parameters.result}}" }
//	  http-fetch (Task): executor=script, outputs=[result]
//	  ding-alert  (Task): executor=function(sendDing), inputs=[msg]
//
// When the "notify" TaskRun is dispatched, this function:
//  1. Looks up the "ding-alert" template → gets executor=sendDing
//  2. Looks up "notify" in parent DAG "main" → gets arguments.msg expression
//  3. Builds assignment merging both: executor + merged inputs
//  4. Resolves "{{tasks.fetch.outputs.parameters.result}}" against sibling TaskRuns
//  5. Dispatches the fully-resolved assignment to the Broker
func (e *Engine) dispatchLeafTask(ctx context.Context, workflowRunID uint64, wf *model.Workflow, tr *store.TaskRun) error {
	// Step 1: Look up the task template definition (executor, inputs declaration, resources, etc.)
	// tr.TemplateName is just a string reference; the actual definition lives in wf.spec.templates[].
	taskTmpl := internal.FindTemplate(wf, tr.TemplateName)
	if taskTmpl == nil {
		return fmt.Errorf("template %q not found for task %q", tr.TemplateName, tr.TaskName)
	}

	// Step 2: Look up the call-site task node in the parent DAG.
	//
	// The template defines *what* inputs it accepts; the DAG task node defines *what values*
	// to pass (arguments). These are intentionally separate:
	//   - template "ding-alert" declares: inputs: [{ name: "channel" }]
	//   - DAG task "notify" supplies:     arguments: [{ name: "channel", value: "ops-group" }]
	//
	// We check parentTmpl.DAG != nil because the parent container might be a Loop instead of
	// a DAG. Loop iterations are dispatched by DispatchIterations (loop.go) which injects
	// iteration parameters directly — no DAG task node to look up here.
	//
	// When ParentRunID == 0 (top-level task), task remains nil and only template defaults apply.
	var task *model.Task
	if tr.ParentRunID != 0 {
		parentTR, err := e.store.GetTaskRun(ctx, tr.ParentRunID)
		if err == nil {
			parentTmpl := internal.FindTemplate(wf, parentTR.TemplateName)
			if parentTmpl != nil && parentTmpl.DAG != nil {
				task = internal.FindTask(parentTmpl.DAG, tr.TaskName)
			}
		}
	}

	// Step 3: Merge template definition + call-site arguments into a TaskAssignment.
	// Merging rules (BuildTaskAssignment):
	//   - executor:  from taskTmpl.GetExecutor()
	//   - timeout:   task.Timeout (call-site) overrides taskTmpl.GetTimeout() (template default)
	//   - inputs:    template inputs declaration merged with task arguments (arguments win on conflict)
	//   - resources: from taskTmpl.GetResources()
	assignment := internal.BuildTaskAssignment(workflowRunID, tr, taskTmpl, task, wf)

	// Step 4: Resolve dynamic input expressions.
	//
	// Input parameter values may contain expressions referencing sibling task outputs, e.g.:
	//   inputs:
	//     parameters:
	//       - name: "reviewResult"
	//         valueFrom:
	//           expression: "{{tasks.review.outputs.parameters.decision}}"
	//
	// siblingRuns provides the execution context: all TaskRuns sharing the same parent scope,
	// so the expression evaluator can read outputs from already-completed sibling tasks.
	// wf.Spec.Arguments provides workflow-level parameter values (the outermost scope).
	if taskTmpl.GetInputs() != nil {
		siblingRuns, _ := e.store.ListTaskRunsByParent(ctx, workflowRunID, tr.ParentRunID)
		rc := &internal.ResolveContext{
			Eval:        e.exprEvaluator,
			SecretStore: e.secretStore,
			TaskRuns:    siblingRuns, // sibling outputs available for expression binding
			WfArgs:      wf.Spec.Arguments,
		}
		resolvedInputs, err := internal.ResolveInputs(ctx, taskTmpl.GetInputs(), rc)
		if err == nil && resolvedInputs != nil {
			inputsJSON, _ := json.Marshal(resolvedInputs)
			assignment.Inputs = inputsJSON
		}
	}

	// Step 5: Dispatch the fully-resolved assignment to the task broker for execution.
	return e.taskBroker.Dispatch(ctx, assignment)
}

// finalizeWorkflow marks the workflow as complete based on top-level TaskRun results.
func (e *Engine) finalizeWorkflow(ctx context.Context, workflowRunID uint64, topLevelRuns []*store.TaskRun, wf *model.Workflow) {
	phase, msg := aggregatePhase(topLevelRuns)
	_ = e.store.UpdateWorkflowRunStatus(ctx, workflowRunID, phase, msg)

	if wf != nil {
		internal.FireWorkflowHooks(ctx, e.hookNotifier, wf, workflowRunID, phase)
	}
}

// aggregatePhase determines the final phase from a set of terminal TaskRuns.
func aggregatePhase(taskRuns []*store.TaskRun) (model.Phase, string) {
	hasFailure := false
	hasError := false

	for _, tr := range taskRuns {
		if tr.Status == model.PhaseFailed {
			hasFailure = true
		}
		if tr.Status == model.PhaseError || tr.Status == model.PhaseTimeout {
			hasError = true
		}
	}

	switch {
	case hasError:
		return model.PhaseError, "one or more tasks errored"
	case hasFailure:
		return model.PhaseFailed, "one or more tasks failed"
	default:
		return model.PhaseSucceeded, ""
	}
}

// allTerminal returns true if all TaskRuns are in a terminal state.
func allTerminal(taskRuns []*store.TaskRun) bool {
	if len(taskRuns) == 0 {
		return false
	}
	for _, tr := range taskRuns {
		if !tr.Status.IsTerminal() {
			return false
		}
	}
	return true
}
