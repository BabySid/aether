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

// createEligibleTasks finds DAG tasks whose dependencies are all satisfied and no
// TaskRun exists yet, then creates TaskRuns (in PhaseCreated) for them so
// advanceScope can activate them in the next iteration.
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
func (e *Engine) createEligibleTasks(ctx context.Context, workflowRunID string, wf *model.Workflow, parentTR *store.TaskRun, siblings []*store.TaskRun) error {
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

		// Create Created TaskRuns for tasks that should execute, then activate immediately.
		for _, task := range toExecute {
			taskTmpl := internal.FindTemplate(wf, task.Template)
			templateType := ""
			if taskTmpl != nil {
				templateType = internal.ResolveTemplateType(taskTmpl)
			} else if task.Executor != nil {
				// Inline executor on the DAG task node — treat as a leaf task.
				templateType = model.TemplateTypeTask
			}
			pendingPhase := model.PhaseCreated
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
	// For all other tasks (DAG tasks, top-level tasks), build an EvalVars from workflow args and
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
		var parentInputs *model.Inputs
		if parentTR != nil {
			parentInputs = parentTR.Inputs
		}
		env := e.newVarBuilder().
			WithWorkflowArgs(wf.Spec.Arguments).
			WithResolvedInputs(parentInputs).
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

	// Step 4: Atomically transition the leaf task from Created → Ready, persisting
	// resolved inputs and deadline in the same write.
	//
	// This serves as a dispatch-claim guard: if two concurrent advanceScope calls
	// both see this task as Created (e.g. one from trySpawnNextIterations and one
	// from a sibling's completion), only the first UpdateTaskRun wins (token check);
	// the second sees a mismatch and returns nil without re-dispatching.
	//
	// Writing the Deadline here (before Broker.Dispatch) also ensures the timeout
	// watchdog can detect it even if the broker goroutine crashes immediately after.
	ready := model.PhaseReady
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
	updated, claimErr := e.store.UpdateTaskRun(ctx, &store.TaskRun{
		RunID:    tr.RunID,
		Token:    tr.Token,
		Status:   &ready,
		Inputs:   updateInputs,
		Deadline: updateDeadline,
	})
	if claimErr != nil {
		// Token mismatch: another advanceScope path already claimed and dispatched this task.
		return nil
	}
	tr = updated

	// Step 5: Mark ancestor containers and the WorkflowRun as Ready now that a leaf task
	// has been committed for dispatch. They will transition to Running when OnTaskStarted fires.
	if err := e.markAncestorsReady(ctx, tr.WorkflowRunID, tr.ParentRunID); err != nil {
		return err
	}

	// Step 6: Dispatch the fully-resolved assignment to the task broker for execution.
	// Ancestor containers and the WorkflowRun will transition from Ready to Running
	// via OnTaskStarted, which is called by the broker/worker when execution actually begins.
	return e.taskBroker.Dispatch(ctx, assignment)
}

// resolveDAGInputs resolves the call-site arguments into the DAG container's declared inputs
// and persists the result into the DAG's TaskRun.Inputs.
//
// This mirrors resolveLoopInputs: without this step, child tasks that reference
// "inputs.parameters.*" in their arguments.valueFrom would always see empty values,
// because dispatchLeafTask builds the EvalVars from parentTR.Inputs.
//
// The persisted inputs are read back by dispatchLeafTask via parentTR.Inputs, so that
// sub-tasks can resolve expressions like:
//
//	valueFrom: { parameter: "inputs.parameters.userid" }
func (e *Engine) resolveDAGInputs(ctx context.Context, workflowRunID string, wf *model.Workflow, dagTR *store.TaskRun) (*model.Inputs, error) {
	dagTmpl := internal.FindTemplate(wf, dagTR.TemplateName)
	if dagTmpl == nil || dagTmpl.DAG == nil {
		return nil, nil
	}
	// No inputs declared on this DAG template — nothing to resolve.
	if dagTmpl.DAG.Inputs == nil || len(dagTmpl.DAG.Inputs.Parameters) == 0 {
		return nil, nil
	}

	// Find call-site Arguments from the parent DAG task node (if any).
	var callSiteArgs *model.Arguments
	if dagTR.ParentRunID != "" {
		parentTR, err := e.store.GetTaskRun(ctx, dagTR.ParentRunID)
		if err != nil {
			return nil, fmt.Errorf("get parent for dag %q: %w", dagTR.TaskName, err)
		}
		parentTmpl := internal.FindTemplate(wf, parentTR.TemplateName)
		if parentTmpl != nil && parentTmpl.DAG != nil {
			if dagTask := internal.FindTask(parentTmpl.DAG, dagTR.TaskName); dagTask != nil {
				callSiteArgs = dagTask.Arguments
			}
		}
	}

	// Build EvalVars: workflow-level args + sibling outputs.
	siblingRuns, err := e.store.ListTaskRunsByParent(ctx, workflowRunID, dagTR.ParentRunID)
	if err != nil {
		return nil, fmt.Errorf("list siblings for dag input binding %q: %w", dagTR.TaskName, err)
	}
	env := e.newVarBuilder().
		WithWorkflowArgs(wf.Spec.Arguments).
		WithSiblingTaskRuns(siblingRuns).
		Build()

	binder := binding.NewBinder(e.exprEvaluator, e.secretStore)
	return binder.Bind(ctx, dagTmpl.DAG.Inputs, callSiteArgs, env)
}
