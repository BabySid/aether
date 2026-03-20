package aether

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BabySid/aether/internal"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

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
			} else if task.Executor != nil {
				// Inline executor on the DAG task node — treat as a leaf task.
				templateType = model.TemplateTypeTask
			}
			skippedPhase := model.PhaseSkipped
			skipMsg := fmt.Sprintf("when condition %q evaluated to false", task.When)
			skippedRun := &store.TaskRun{
				RunID:         e.idGen.Generate(),
				WorkflowRunID: workflowRunID,
				ParentRunID:   parentRunID,
				Depth:         parentTR.Depth + 1,
				Scope:         parentTR.TaskName + "/",
				TaskName:      task.Name,
				TemplateName:  task.Template,
				TemplateType:  templateType,
				Status:        &skippedPhase,
				Message:       &skipMsg,
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
			} else if task.Executor != nil {
				// Inline executor on the DAG task node — treat as a leaf task.
				templateType = model.TemplateTypeTask
			}
			pendingPhase := model.PhasePending
			newRun := &store.TaskRun{
				RunID:         e.idGen.Generate(),
				WorkflowRunID: workflowRunID,
				ParentRunID:   parentRunID,
				Depth:         parentTR.Depth + 1,
				Scope:         parentTR.TaskName + "/",
				TaskName:      task.Name,
				TemplateName:  task.Template,
				TemplateType:  templateType,
				Status:        &pendingPhase,
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
	// Step 1: Resolve the definition task (taskDecl) and call-site task (taskCall).
	//
	// A leaf task is either:
	//   (a) template-ref mode:   tr.TemplateName points to a named template whose .Task field
	//       carries the executor/inputs/resources/timeout declaration.
	//   (b) inline-executor mode: the DAG task node itself carries the executor — there is no
	//       separate template entry in wf.spec.templates.
	//
	// taskDecl     = the Task definition (executor, inputs declaration, resources, timeout default)
	// taskCall = the DAG task node supplying arguments/timeout override; nil for top-level
	//               tasks and loop iterations.
	var taskDecl *model.Task
	var taskCall *model.Task
	isLoopIteration := false

	// Named template reference: look up wf.spec.templates by name.
	if tr.TemplateName != "" {
		taskTmpl := internal.FindTemplate(wf, tr.TemplateName)
		if taskTmpl != nil {
			// Named template must be a Task-type template.
			taskDecl = taskTmpl.Task
		}
	}

	// Resolve parent context: call-site arguments (DAG mode) or loop iteration flag.
	if tr.ParentRunID != 0 {
		parentTR, err := e.store.GetTaskRun(ctx, tr.ParentRunID)
		if err != nil {
			return fmt.Errorf("get parent TaskRun %d for task %q: %w", tr.ParentRunID, tr.TaskName, err)
		}
		parentTmpl := internal.FindTemplate(wf, parentTR.TemplateName)
		if parentTmpl != nil && parentTmpl.DAG != nil {
			dagTask := internal.FindTask(parentTmpl.DAG, tr.TaskName)
			if dagTask != nil {
				// taskCall always provides call-site arguments / timeout override.
				taskCall = dagTask
				// Inline-executor mode: the DAG task node IS the definition when no named
				// template was resolved above.
				if taskDecl == nil && dagTask.Executor != nil {
					taskDecl = dagTask
				}
			}
		} else if parentTmpl != nil && parentTmpl.Loop != nil {
			isLoopIteration = true
		}
	}

	if taskDecl == nil {
		return fmt.Errorf("template %q not found for task %q", tr.TemplateName, tr.TaskName)
	}

	// Step 2: Merge definition + call-site into a TaskAssignment.
	//   - executor:      from taskDecl.Executor
	//   - timeout:       taskCall.Timeout overrides taskDecl.Timeout
	//   - inputs:        taskDecl.Inputs merged with taskCall.Arguments (callSite wins)
	//   - resources:     from taskDecl.Resources
	assignment, err := internal.BuildTaskAssignment(workflowRunID, tr, taskDecl, taskCall, wf)
	if err != nil {
		return fmt.Errorf("build task assignment for %q: %w", tr.TaskName, err)
	}

	// Step 3: Resolve inputs.
	//
	// For loop iterations, iteration parameters (loop_iter.index, item fields) were pre-stored in
	// tr.Inputs by startLoopController. Use them directly — no expression resolution needed.
	//
	// For DAG tasks, input values may contain expressions referencing sibling outputs, e.g.:
	//   inputs.parameters[*].valueFrom.expression: "{{tasks.review.outputs.parameters.decision}}"
	// siblingRuns provides the evaluation context; wf.Spec.Arguments provides workflow-level args.
	if isLoopIteration {
		if tr.Inputs != nil {
			inputsJSON, _ := json.Marshal(tr.Inputs)
			assignment.Inputs = inputsJSON
		}
	} else if taskDecl.Inputs != nil {
		siblingRuns, err := e.store.ListTaskRunsByParent(ctx, workflowRunID, tr.ParentRunID)
		if err != nil {
			return fmt.Errorf("list siblings for task %q input resolution: %w", tr.TaskName, err)
		}
		rc := &internal.ResolveContext{
			Eval:        e.exprEvaluator,
			SecretStore: e.secretStore,
			TaskRuns:    siblingRuns, // sibling outputs available for expression binding
			WfArgs:      wf.Spec.Arguments,
		}
		resolvedInputs, err := internal.ResolveInputs(ctx, taskDecl.Inputs, rc)
		if err != nil {
			return fmt.Errorf("resolve inputs for task %q: %w", tr.TaskName, err)
		}
		if resolvedInputs != nil {
			inputsJSON, _ := json.Marshal(resolvedInputs)
			assignment.Inputs = inputsJSON
		}
	}

	// Step 4: Dispatch the fully-resolved assignment to the task broker for execution.
	// Ancestor containers (DAG/Loop) and the WorkflowRun will transition from Pending
	// to Running via OnTaskStarted, which is called by the broker/worker when execution
	// actually begins — not here at dispatch time.
	return e.taskBroker.Dispatch(ctx, assignment)
}
