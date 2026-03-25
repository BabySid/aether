package aether

import (
	"context"
	"fmt"
	"time"

	"github.com/BabySid/aether/internal"
	"github.com/BabySid/aether/internal/binding"
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
func (e *Engine) createReadyTasks(ctx context.Context, workflowRunID string, wf *model.Workflow, parentTR *store.TaskRun, siblings []*store.TaskRun) error {
	// parentTR is passed in by the caller (advanceScope already has it); no extra GetTaskRun needed.
	parentRunID := parentTR.RunID
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
		var sibErr error
		siblings, sibErr = e.store.ListTaskRunsByParent(ctx, workflowRunID, parentRunID)
		if sibErr != nil {
			return sibErr
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
func (e *Engine) dispatchLeafTask(ctx context.Context, workflowRunID string, wf *model.Workflow, tr *store.TaskRun) error {
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
	// Resolve taskDecl (definition) and taskCall (call-site arguments).
	// parentTR is fetched here rather than inside ResolveTaskDecl so that any
	// store error can be wrapped with task-specific context.
	var parentTR *store.TaskRun
	if tr.ParentRunID != "" {
		var err error
		parentTR, err = e.store.GetTaskRun(ctx, tr.ParentRunID)
		if err != nil {
			return fmt.Errorf("get parent TaskRun %q for task %q: %w", tr.ParentRunID, tr.TaskName, err)
		}
	}
	taskDecl, taskCall, isLoopIteration := internal.ResolveTaskDecl(wf, tr, parentTR)

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

	// Step 3: Resolve inputs via binding.Binder.
	//
	// For loop iterations, iteration parameters (loop_iter.index, item fields) were pre-stored
	// in tr.Inputs by startLoopController. Use them directly — no further resolution needed.
	//
	// For all other tasks (DAG tasks, top-level tasks), build an EvalEnv from workflow args and
	// sibling TaskRun outputs, then let Binder merge taskDecl.Inputs with taskCall.Arguments and
	// resolve any valueFrom references (expression, parameter, path, secretKeyRef).
	var callSiteArgs *model.Arguments
	if taskCall != nil {
		callSiteArgs = taskCall.Arguments
	}

	if isLoopIteration {
		// Iteration params are already in tr.Inputs; forward as-is.
		assignment.Inputs = tr.Inputs
	} else {
		siblingRuns, err := e.store.ListTaskRunsByParent(ctx, workflowRunID, tr.ParentRunID)
		if err != nil {
			return fmt.Errorf("list siblings for task %q input resolution: %w", tr.TaskName, err)
		}
		env := binding.NewEnvBuilder().
			WithWorkflowArgs(wf.Spec.Arguments).
			WithSiblingTaskRuns(siblingRuns).
			Build()
		binder := binding.NewBinder(e.exprEvaluator, e.secretStore)
		resolvedInputs, err := binder.Bind(ctx, taskDecl.Inputs, callSiteArgs, env)
		if err != nil {
			return fmt.Errorf("resolve inputs for task %q: %w", tr.TaskName, err)
		}
		if resolvedInputs != nil && len(resolvedInputs.Parameters) > 0 {
			assignment.Inputs = resolvedInputs
		}
	}

	// Step 4: Persist resolved inputs + deadline in a single UpdateTaskRun call before
	// dispatching. Writing the Deadline here (before Dispatch) ensures the timeout
	// watchdog can detect it even if the Broker/Executor crashes right after Dispatch.
	var updateInputs *model.Inputs
	if assignment.Inputs != nil {
		updateInputs = assignment.Inputs
	}
	var updateDeadline *time.Time
	if assignment.Timeout != "" {
		if d, parseErr := internal.ParseDuration(assignment.Timeout); parseErr == nil && d > 0 {
			dl := time.Now().Add(d)
			updateDeadline = &dl
		}
	}
	if updateInputs != nil || updateDeadline != nil {
		updated, _ := e.store.UpdateTaskRun(ctx, &store.TaskRun{
			RunID:    tr.RunID,
			Token:    tr.Token,
			Inputs:   updateInputs,
			Deadline: updateDeadline,
		})
		// Use the returned token for any subsequent writes; fall back to original tr if update failed.
		if updated != nil {
			tr = updated
		}
	}

	// Step 5: Dispatch the fully-resolved assignment to the task broker for execution.
	// Ancestor containers (DAG/Loop) and the WorkflowRun will transition from Pending
	// to Running via OnTaskStarted, which is called by the broker/worker when execution
	// actually begins — not here at dispatch time.
	return e.taskBroker.Dispatch(ctx, assignment)
}
