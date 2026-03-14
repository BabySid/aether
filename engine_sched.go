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
// If all siblings in scope 10 are terminal, the loop aggregates their results into
// "main-dag", sets parentRunID=0, and continues upward to the root scope where
// finalizeWorkflow is called.
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
			if sib.Status == model.PhasePending {
				if err := e.activateTaskRun(ctx, workflowRunID, wf, sib); err != nil {
					return err
				}
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

		// 5. Root scope (parentRunID=0): finalize the workflow if all top-level tasks are
		// terminal, otherwise wait.
		if parentRunID == 0 {
			if allTerminal(siblings) {
				e.finalizeWorkflow(ctx, workflowRunID, siblings, wf)
			}
			return nil
		}

		// 6. Non-root scope: load the parent container (DAG or Loop) once and handle its
		// advancement logic. Loop and DAG have distinct continuation rules:
		//
		//   Loop scope:
		//     - Concurrency refill: when an iteration finishes, eagerly spawn the next one
		//       so active slots stay at concurrency limit (items/itemsFrom mode).
		//     - If not all iterations are terminal yet, stay in progress.
		//     - repeatCondition advance: once all current iterations are terminal, re-evaluate
		//       the condition and spawn the next iteration (or stop and aggregate).
		//
		//   DAG scope:
		//     - If any sibling is still non-terminal, stay in progress.
		//     - All terminal: aggregate children and walk up one level.
		parentTR, err := e.store.GetTaskRun(ctx, parentRunID)
		if err != nil {
			return err
		}

		if parentTR.TemplateType == model.TemplateTypeLoop {
			// Concurrency refill: spawn pending iterations to fill free slots, even if
			// other iterations are still running. This must happen before allTerminal check
			// so that iter[N] is created immediately when iter[0] finishes, not deferred
			// until all others are also done.
			spawned, err := e.trySpawnNextIterations(ctx, workflowRunID, wf, parentTR, siblings)
			if err != nil {
				return err
			}
			if spawned {
				return nil
			}

			// If any iteration is still running, the loop scope is still in progress.
			if !allTerminal(siblings) {
				return nil
			}

			// All iterations terminal: check whether to spawn the next repeatCondition
			// iteration or proceed to aggregation.
			advanced, err := e.tryAdvanceRepeatLoop(ctx, workflowRunID, wf, parentTR, siblings)
			if err != nil {
				return err
			}
			if advanced {
				return nil
			}
		} else {
			// DAG scope: simply wait for all tasks to finish.
			if !allTerminal(siblings) {
				return nil
			}
		}

		// 7. All children are terminal and no further iterations will be spawned.
		// Aggregate children's results into the parent container's phase and walk up.
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
//   - DAG (container): stays Pending and opens a child scope via advanceScope.
//     It transitions to Running only when its first leaf task is dispatched
//     (markAncestorsRunning walks up the chain at that point).
//     DAG itself never reaches a Broker — it's done when all its children are terminal.
//
//   - Task (leaf): dispatched directly to the Broker for execution.
//     The Broker runs the executor and calls back OnTaskCompleted when done.
//     Status transitions to Running inside the Broker/Executor, not here.
//
//   - Loop (container): stays Pending and starts a loop controller.
//     It transitions to Running only when its first iteration task is dispatched
//     (markAncestorsRunning walks up the chain at that point).
//     The controller is responsible for expanding items / evaluating repeatCondition
//     and spawning iteration TaskRuns.
func (e *Engine) activateTaskRun(ctx context.Context, workflowRunID uint64, wf *model.Workflow, tr *store.TaskRun) error {
	switch tr.TemplateType {
	case model.TemplateTypeDAG:
		// DAG stays Pending here. It will transition to Running when its first
		// leaf task is dispatched (via markAncestorsRunning in dispatchLeafTask).
		// The parent scope's allTerminal check is unaffected because Pending is
		// not a terminal state per Phase.IsTerminal().
		//
		// tr.RunID becomes the startParentRunID for the child scope.
		return e.advanceScope(ctx, workflowRunID, wf, tr.RunID)

	case model.TemplateTypeTask:
		// Leaf task: build a TaskAssignment (executor + resolved inputs) and hand off
		// to the Broker. Status transitions to Running inside the Broker/Executor.
		return e.dispatchLeafTask(ctx, workflowRunID, wf, tr)

	case model.TemplateTypeLoop:
		// Loop stays Pending here, mirroring the DAG behaviour.
		// It transitions to Running only when its first iteration task is dispatched
		// (markAncestorsRunning walks up the chain at that point).
		return e.startLoopController(ctx, workflowRunID, wf, tr)

	default:
		return fmt.Errorf("unknown template type %q for task %q", tr.TemplateType, tr.TaskName)
	}
}

// startLoopController initialises a Loop container and creates the first batch of
// iteration TaskRuns.
//
// Three loop modes are handled differently:
//
//   - items / itemsFrom: the full iteration list is expanded upfront. When concurrency
//     is set, only the first min(concurrency, total) iterations are created now; the rest
//     are created lazily by trySpawnNextIterations as active slots become free.
//
//   - repeatCondition: serial mode — only iteration 0 is created here. After it finishes,
//     tryAdvanceRepeatLoop evaluates the condition and spawns the next iteration (or stops).
//
// Example loop template:
//
//	templates:
//	  process-files (Loop): items=["a.txt","b.txt","c.txt"], concurrency=2, body="handle-file"
//	  handle-file   (Task): executor=script
//
// When "process-files" is activated, startLoopController:
//  1. Expands items → [{item:"a.txt",item_index:0}, {item:"b.txt",item_index:1}, {item:"c.txt",item_index:2}]
//  2. Creates TaskRuns for iterations 0 and 1 only (concurrency=2)
//  3. Calls advanceScope(loopTR.RunID) → dispatches iterations 0 and 1 to the broker
//  4. When iteration 0 finishes, trySpawnNextIterations creates and dispatches iteration 2
//
// Each iteration TaskRun has ParentRunID = loopTR.RunID, so ListTaskRunsByParent returns
// all iterations as siblings. Iteration inputs (item_index, item fields) are pre-stored in
// TaskRun.Inputs and forwarded to the broker without further expression resolution.
//
// Zero-iteration loops are finalized as Succeeded immediately without entering advanceScope.
func (e *Engine) startLoopController(ctx context.Context, workflowRunID uint64, wf *model.Workflow, loopTR *store.TaskRun) error {
	// 1. Resolve loop and body templates.
	loopTmpl := internal.FindTemplate(wf, loopTR.TemplateName)
	if loopTmpl == nil || loopTmpl.Loop == nil {
		return fmt.Errorf("loop template %q not found for task %q", loopTR.TemplateName, loopTR.TaskName)
	}
	loop := loopTmpl.Loop

	bodyTmpl := internal.FindTemplate(wf, loop.Body)
	if bodyTmpl == nil {
		return fmt.Errorf("loop body template %q not found for loop %q", loop.Body, loopTR.TemplateName)
	}
	bodyTemplateType := internal.ResolveTemplateType(bodyTmpl)

	// repeatCondition mode: serial, one iteration at a time.
	// Spawn the first iteration now; subsequent ones are launched in tryAdvanceRepeatLoop
	// after each iteration completes.
	if loop.RepeatCondition != "" {
		return e.spawnRepeatIteration(ctx, workflowRunID, wf, loopTR, 0)
	}

	// 2. Expand iterations; use sibling task outputs as the expression environment.
	siblingRuns, err := e.store.ListTaskRunsByParent(ctx, workflowRunID, loopTR.ParentRunID)
	if err != nil {
		return fmt.Errorf("list siblings for loop %q: %w", loopTR.TemplateName, err)
	}
	env := internal.BuildTaskEnv(siblingRuns)
	iterations, err := internal.ExpandLoopIterations(ctx, loop, e.exprEvaluator, env)
	if err != nil {
		return fmt.Errorf("expand loop %q: %w", loopTR.TemplateName, err)
	}

	// 3. Zero iterations: mark the loop as succeeded immediately.
	if len(iterations) == 0 {
		loopTR.Status = model.PhaseSucceeded
		loopTR.Message = "loop had no iterations"
		return e.store.UpdateTaskRun(ctx, loopTR)
	}

	// 4. Create one child TaskRun per iteration (Pending, ParentRunID = loopTR.RunID).
	// When concurrency is set, only the first min(concurrency, total) iterations are
	// created now. As each completes, trySpawnNextIterations refills the active slot.
	// Iteration parameters are serialised into TaskRun.Inputs so that dispatchLeafTask
	// can forward them to the broker without re-resolution.
	numToCreate := len(iterations)
	if loop.Concurrency > 0 && loop.Concurrency < numToCreate {
		numToCreate = loop.Concurrency
	}
	for i := 0; i < numToCreate; i++ {
		if err := e.createIterationRun(ctx, workflowRunID, loopTR, loop.Body, bodyTemplateType, i, iterations[i]); err != nil {
			return err
		}
	}

	// 5. Advance the loop's child scope to activate / dispatch the iteration TaskRuns.
	return e.advanceScope(ctx, workflowRunID, wf, loopTR.RunID)
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
				Scope:         parentTR.TaskName + "/",
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
				Scope:         parentTR.TaskName + "/",
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

	// Step 2: Look up the call-site task node in the parent DAG (if applicable).
	//
	// The template defines *what* inputs it accepts; the DAG task node defines *what values*
	// to pass (arguments). These are intentionally separate:
	//   - template "ding-alert" declares: inputs: [{ name: "channel" }]
	//   - DAG task "notify" supplies:     arguments: [{ name: "channel", value: "ops-group" }]
	//
	// task is nil when:
	//   - ParentRunID == 0 (top-level task): no parent DAG node to look up.
	//   - Parent is a Loop: iteration parameters are pre-stored in tr.Inputs by
	//     startLoopController; there is no DAG task node for loop iterations.
	var task *model.Task
	isLoopIteration := false
	if tr.ParentRunID != 0 {
		parentTR, err := e.store.GetTaskRun(ctx, tr.ParentRunID)
		if err != nil {
			return fmt.Errorf("get parent TaskRun %d for task %q: %w", tr.ParentRunID, tr.TaskName, err)
		}
		parentTmpl := internal.FindTemplate(wf, parentTR.TemplateName)
		if parentTmpl != nil && parentTmpl.DAG != nil {
			task = internal.FindTask(parentTmpl.DAG, tr.TaskName)
		} else if parentTmpl != nil && parentTmpl.Loop != nil {
			isLoopIteration = true
		}
	}

	// Step 3: Merge template definition + call-site arguments into a TaskAssignment.
	// Merging rules (BuildTaskAssignment):
	//   - executor:  from taskTmpl.GetExecutor()
	//   - timeout:   task.Timeout (call-site) overrides taskTmpl.GetTimeout() (template default)
	//   - inputs:    template inputs declaration merged with task arguments (arguments win on conflict)
	//   - resources: from taskTmpl.GetResources()
	assignment, err := internal.BuildTaskAssignment(workflowRunID, tr, taskTmpl, task, wf)
	if err != nil {
		return fmt.Errorf("build task assignment for %q: %w", tr.TaskName, err)
	}

	// Step 4: Resolve inputs.
	//
	// For loop iterations, iteration parameters (item_index, item fields) were pre-stored in
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
	} else if taskTmpl.GetInputs() != nil {
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
		resolvedInputs, err := internal.ResolveInputs(ctx, taskTmpl.GetInputs(), rc)
		if err != nil {
			return fmt.Errorf("resolve inputs for task %q: %w", tr.TaskName, err)
		}
		if resolvedInputs != nil {
			inputsJSON, _ := json.Marshal(resolvedInputs)
			assignment.Inputs = inputsJSON
		}
	}

	// Step 5: Dispatch the fully-resolved assignment to the task broker for execution.
	// Ancestor containers (DAG/Loop) and the WorkflowRun will transition from Pending
	// to Running via OnTaskStarted, which is called by the broker/worker when execution
	// actually begins — not here at dispatch time.
	return e.taskBroker.Dispatch(ctx, assignment)
}

// markAncestorsRunning walks up the ParentRunID chain starting from parentRunID,
// transitioning every Pending ancestor TaskRun to Running.
// Once the chain reaches the root (parentRunID=0), it also promotes the
// WorkflowRun from Pending to Running if it hasn't been already.
//
// This is called immediately after a leaf task is dispatched so that the semantic
// "Running" state of DAG containers and the WorkflowRun accurately reflects the
// moment real work begins, rather than the moment a container node was activated.
func (e *Engine) markAncestorsRunning(ctx context.Context, workflowRunID uint64, parentRunID uint64) error {
	for parentRunID != 0 {
		ancestor, err := e.store.GetTaskRun(ctx, parentRunID)
		if err != nil {
			return err
		}
		if ancestor.Status != model.PhasePending {
			// Already Running (or terminal) — no need to update further up the chain.
			break
		}
		ancestor.Status = model.PhaseRunning
		if err := e.store.UpdateTaskRun(ctx, ancestor); err != nil {
			return err
		}
		parentRunID = ancestor.ParentRunID
	}

	// Promote the WorkflowRun from Pending to Running (idempotent via CAS).
	if _, err := e.store.UpdateWorkflowRunStatusCAS(ctx, workflowRunID, model.PhasePending, model.PhaseRunning, ""); err != nil {
		return err
	}
	return nil
}

// finalizeWorkflow marks the workflow as complete based on top-level TaskRun results.
//
// Uses CAS (Running → terminal) to guard against double-finalization: in a local
// synchronous broker, Dispatch calls OnTaskCompleted inline, which calls advanceScope
// and may reach finalizeWorkflow before the outer advanceScope call returns. The CAS
// ensures that only the first finalization succeeds and fires workflow-level hooks;
// subsequent calls are no-ops because the WorkflowRun is no longer Running.
func (e *Engine) finalizeWorkflow(ctx context.Context, workflowRunID uint64, topLevelRuns []*store.TaskRun, wf *model.Workflow) {
	phase, msg := aggregatePhase(topLevelRuns)
	updated, err := e.store.UpdateWorkflowRunStatusCAS(ctx, workflowRunID, model.PhaseRunning, phase, msg)
	if err != nil || !updated {
		// Either a store error or the workflow was already finalized — either way, stop.
		return
	}

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

// spawnRepeatIteration creates and activates the body TaskRun for one iteration of a
// repeatCondition loop.
//
// Called in two places:
//   - startLoopController: spawns iteration 0 when the loop container is first activated.
//   - tryAdvanceRepeatLoop: spawns iteration N after iteration N-1 completes and the
//     condition still evaluates to true.
//
// Example repeatCondition loop:
//
//	templates:
//	  poll-job (Loop): repeatCondition="tasks.check.outputs.parameters.status != 'done'",
//	                   maxIterations=20, body="check"
//	  check    (Task): executor=script, outputs=[status]
//
// Iteration 0 is created by startLoopController. When it finishes:
//   - tryAdvanceRepeatLoop builds env={iteration_index:0, tasks.check.phase:"Succeeded", ...}
//   - evaluates "tasks.check.outputs.parameters.status != 'done'"
//   - if true → calls spawnRepeatIteration(loopTR, 1) → creates iteration-1 TaskRun, advances scope
//   - if false → returns advanced=false → advanceScope aggregates all children and finalizes loop
//
// All iteration TaskRuns share the same ParentRunID (loopTR.RunID), so ListTaskRunsByParent
// returns them all as siblings. iterIndex is encoded in the Scope field for traceability.
func (e *Engine) spawnRepeatIteration(ctx context.Context, workflowRunID uint64, wf *model.Workflow, loopTR *store.TaskRun, iterIndex int) error {
	loopTmpl := internal.FindTemplate(wf, loopTR.TemplateName)
	if loopTmpl == nil || loopTmpl.Loop == nil {
		return fmt.Errorf("loop template %q not found", loopTR.TemplateName)
	}
	loop := loopTmpl.Loop

	bodyTmpl := internal.FindTemplate(wf, loop.Body)
	if bodyTmpl == nil {
		return fmt.Errorf("loop body template %q not found", loop.Body)
	}
	bodyTemplateType := internal.ResolveTemplateType(bodyTmpl)

	iterScope := fmt.Sprintf("%s.loop[%d]/", loopTR.TaskName, iterIndex)
	iterRun := &store.TaskRun{
		RunID:         e.idGen.Generate(),
		WorkflowRunID: workflowRunID,
		ParentRunID:   loopTR.RunID,
		Depth:         loopTR.Depth + 1,
		Scope:         iterScope,
		TaskName:      loop.Body,
		TemplateName:  loop.Body,
		TemplateType:  bodyTemplateType,
		Status:        model.PhasePending,
	}
	if err := e.store.CreateTaskRun(ctx, iterRun); err != nil {
		return fmt.Errorf("create repeat iteration %d: %w", iterIndex, err)
	}
	return e.advanceScope(ctx, workflowRunID, wf, loopTR.RunID)
}

// tryAdvanceRepeatLoop decides whether a repeatCondition loop should continue after
// all current children are terminal.
//
// Called from advanceScope (inside the TemplateTypeLoop guard) when all children of
// a Loop container are terminal. It re-evaluates the repeatCondition using the last
// iteration's outputs as the expression context:
//
//	env = { iteration_index: N-1,
//	        tasks.<body>.phase: "Succeeded",
//	        tasks.<body>.outputs.parameters.<name>: <value>, ... }
//
// Example:
//
//	loop "retry-fetch": repeatCondition="tasks.fetch.phase != 'Succeeded'", maxIterations=5
//
//	After iteration 0 finishes with phase=Failed:
//	  env["tasks.fetch.phase"] = "Failed"
//	  condition evaluates to true  → spawnRepeatIteration(1) → returns (true, nil)
//
//	After iteration 1 finishes with phase=Succeeded:
//	  condition evaluates to false → returns (false, nil) → advanceScope aggregates + finalizes
//
// Return values:
//
//	(true,  nil) — new iteration spawned; scope is still in progress.
//	(false, nil) — not a repeatCondition loop, loop is done, or maxIterations reached.
//	(false, err) — condition evaluation or iteration creation failed.
func (e *Engine) tryAdvanceRepeatLoop(ctx context.Context, workflowRunID uint64, wf *model.Workflow, parentTR *store.TaskRun, children []*store.TaskRun) (bool, error) {
	if parentTR.TemplateType != model.TemplateTypeLoop {
		return false, nil
	}
	loopTmpl := internal.FindTemplate(wf, parentTR.TemplateName)
	if loopTmpl == nil || loopTmpl.Loop == nil || loopTmpl.Loop.RepeatCondition == "" {
		return false, nil
	}
	loop := loopTmpl.Loop

	// Determine the next iteration index from the count of completed iterations.
	nextIndex := len(children)

	// Safety guard: respect maxIterations.
	if loop.MaxIterations > 0 && nextIndex >= loop.MaxIterations {
		return false, nil
	}

	// Find the most recent iteration (last child) to use as the expression context.
	var lastRun *store.TaskRun
	for _, c := range children {
		if lastRun == nil || c.RunID > lastRun.RunID {
			lastRun = c
		}
	}

	env := internal.BuildRepeatEnv(nextIndex-1, lastRun)
	shouldContinue, err := internal.EvalRepeatCondition(ctx, loop.RepeatCondition, e.exprEvaluator, env)
	if err != nil {
		return false, fmt.Errorf("repeatCondition loop %q: %w", parentTR.TaskName, err)
	}
	if !shouldContinue {
		return false, nil
	}

	if err := e.spawnRepeatIteration(ctx, workflowRunID, wf, parentTR, nextIndex); err != nil {
		return false, err
	}
	return true, nil
}

// createIterationRun creates a single Pending child TaskRun for one iteration of an
// items/itemsFrom loop.
//
// Called by startLoopController (initial batch) and trySpawnNextIterations (concurrency
// refill). iterParams (e.g. {item:"a.txt", item_index:0}) are serialised into
// TaskRun.Inputs so dispatchLeafTask can forward them to the broker verbatim without
// re-running expression resolution.
//
// The Scope field is set to "<loopTaskName>.loop[<iterIndex>]/" for traceability.
func (e *Engine) createIterationRun(ctx context.Context, workflowRunID uint64, loopTR *store.TaskRun, bodyName, bodyTemplateType string, iterIndex int, iterParams map[string]any) error {
	iterScope := fmt.Sprintf("%s.loop[%d]/", loopTR.TaskName, iterIndex)

	var iterInputs *model.Inputs
	if len(iterParams) > 0 {
		params := make([]model.Parameter, 0, len(iterParams))
		for k, v := range iterParams {
			valJSON, _ := json.Marshal(v)
			params = append(params, model.Parameter{Name: k, Value: valJSON})
		}
		iterInputs = &model.Inputs{Parameters: params}
	}

	iterRun := &store.TaskRun{
		RunID:         e.idGen.Generate(),
		WorkflowRunID: workflowRunID,
		ParentRunID:   loopTR.RunID,
		Depth:         loopTR.Depth + 1,
		Scope:         iterScope,
		TaskName:      bodyName,
		TemplateName:  bodyName,
		TemplateType:  bodyTemplateType,
		Status:        model.PhasePending,
		Inputs:        iterInputs,
	}
	if err := e.store.CreateTaskRun(ctx, iterRun); err != nil {
		return fmt.Errorf("create iteration %s: %w", iterScope+bodyName, err)
	}
	return nil
}

// trySpawnNextIterations refills active iteration slots for concurrency-limited
// items/itemsFrom loops.
//
// Called from advanceScope (inside the TemplateTypeLoop guard) after tryAdvanceRepeatLoop
// returns false. It fires whenever any child iteration finishes, and keeps at most
// loop.Concurrency iterations active at a time.
//
// Example:
//
//	loop "batch": items=[0..9], concurrency=3, body="process"
//
//	startLoopController creates iterations 0,1,2.
//	When iteration 0 finishes:
//	  createdCount=3, activeCount=2 (1,2 still running), availableSlots=1
//	  → creates iteration 3, advances scope
//	When iteration 1 finishes:
//	  createdCount=4, activeCount=2 (2,3 still running), availableSlots=1
//	  → creates iteration 4, advances scope
//	... until all 10 iterations are created and complete.
//
// The full iteration list is re-expanded on each call (cheap, stateless). If itemsFrom
// is dynamic its expression is re-evaluated, but the result is expected to be stable
// across a single workflow run.
//
// Return values:
//
//	(true,  nil) — at least one new iteration was spawned.
//	(false, nil) — not a concurrency-limited items/itemsFrom loop, or all iterations created.
//	(false, err) — expansion or iteration creation failed.
func (e *Engine) trySpawnNextIterations(ctx context.Context, workflowRunID uint64, wf *model.Workflow, parentTR *store.TaskRun, children []*store.TaskRun) (bool, error) {
	if parentTR.TemplateType != model.TemplateTypeLoop {
		return false, nil
	}
	loopTmpl := internal.FindTemplate(wf, parentTR.TemplateName)
	if loopTmpl == nil || loopTmpl.Loop == nil {
		return false, nil
	}
	loop := loopTmpl.Loop

	// Only applies to items/itemsFrom modes with a concurrency limit.
	if loop.Concurrency <= 0 || loop.RepeatCondition != "" {
		return false, nil
	}

	// Re-expand iterations to know the total count and each item's params.
	siblingRuns, err := e.store.ListTaskRunsByParent(ctx, workflowRunID, parentTR.ParentRunID)
	if err != nil {
		return false, fmt.Errorf("list siblings for loop %q concurrency advance: %w", parentTR.TemplateName, err)
	}
	env := internal.BuildTaskEnv(siblingRuns)
	iterations, err := internal.ExpandLoopIterations(ctx, loop, e.exprEvaluator, env)
	if err != nil {
		return false, fmt.Errorf("expand loop %q for concurrency advance: %w", parentTR.TemplateName, err)
	}
	totalIter := len(iterations)
	if totalIter == 0 {
		return false, nil
	}

	// Count already-created iterations (children already have TaskRuns).
	createdCount := len(children)
	if createdCount >= totalIter {
		// All iterations have been created; nothing left to spawn.
		return false, nil
	}

	// Count currently active (non-terminal) iterations.
	activeCount := 0
	for _, c := range children {
		if !c.Status.IsTerminal() {
			activeCount++
		}
	}

	// How many new slots are available?
	availableSlots := loop.Concurrency - activeCount
	if availableSlots <= 0 {
		return false, nil
	}

	bodyTmpl := internal.FindTemplate(wf, loop.Body)
	if bodyTmpl == nil {
		return false, fmt.Errorf("loop body template %q not found", loop.Body)
	}
	bodyTemplateType := internal.ResolveTemplateType(bodyTmpl)

	spawned := false
	for slot := 0; slot < availableSlots && createdCount < totalIter; slot++ {
		if err := e.createIterationRun(ctx, workflowRunID, parentTR, loop.Body, bodyTemplateType, createdCount, iterations[createdCount]); err != nil {
			return false, err
		}
		createdCount++
		spawned = true
	}

	if spawned {
		// Activate the freshly-created Pending iterations.
		if err := e.advanceScope(ctx, workflowRunID, wf, parentTR.RunID); err != nil {
			return false, err
		}
	}
	return spawned, nil
}
