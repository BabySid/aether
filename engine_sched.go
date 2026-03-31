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

// advanceScope is the core iterative scheduling function.
// It processes the scope identified by startParentRunID, then walks up the scope
// tree until the workflow is finalized or a scope is still in progress.
//
// # Scope tree model
//
// Every DAG/Loop container is a scope boundary. The tree mirrors template nesting:
//
// DAG example:
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
//
// Loop example:
//
//	parentRunID=0      (virtual workflow root)
//	└── process-files  (RunID=20, type=Loop)  ← scope 20
//	    ├── handle-file[0]  (RunID=21, type=Task)
//	    ├── handle-file[1]  (RunID=22, type=Task)
//	    └── handle-file[2]  (RunID=23, type=Task)
//
// advanceScope(startParentRunID=0) activates "process-files" (Loop container).
// startLoopController expands items and creates iteration TaskRuns under scope 20.
// advanceScope(startParentRunID=20) dispatches iteration 0 and 1 (concurrency=2).
//
// When iteration 0 completes, OnTaskCompleted calls advanceScope(startParentRunID=20).
// trySpawnNextIterations detects a free slot and creates iteration 2.
// Once all iterations are terminal, results are aggregated into "process-files"
// and advanceScope walks up to parentRunID=0 to finalize the workflow.
func (e *Engine) advanceScope(ctx context.Context, workflowRunID string, wf *model.Workflow, startParentRunID string) error {
	parentRunID := startParentRunID

	for {
		// 1. Load all TaskRuns in this scope (siblings = same parentRunID).
		siblings, err := e.store.ListTaskRunsByParent(ctx, workflowRunID, parentRunID)
		if err != nil {
			return err
		}

		// 2. Activate any Created TaskRuns in this scope.
		// Containers (DAG/Loop) open their child scope; leaf tasks are claimed Ready and dispatched.
		// Leaf tasks (type=task) are dispatched to the Broker directly.
		for _, sib := range siblings {
			if sib.Status != nil && *sib.Status == model.PhaseCreated {
				if err := e.activateTaskRun(ctx, workflowRunID, wf, sib); err != nil {
					return err
				}
			}
		}

		// 3. If this scope is owned by a DAG container, check whether more tasks are
		// now ready (i.e., all their declared dependencies are terminal).
		// Skipped for parentRunID="" (root scope) because the root has no DAG.
		//
		// Example — DAG "main": fetch → notify → alert
		//   advanceScope is called after "fetch" completes (Succeeded).
		//   At step 2, siblings=[fetch:Succeeded], no Created tasks to activate.
		//   Step 3 calls createEligibleTasks:
		//     - "notify" depends on "fetch" → fetch is terminal → notify is eligible → CreateTaskRun(notify,Created)
		//     - "alert"  depends on "notify" → notify not yet terminal → not eligible yet
		//   After step 3: siblings=[fetch:Succeeded] (stale — notify was just added to the store)
		// 3. Fetch the parent container once (if this is a non-root scope) so that
		// createEligibleTasks can skip its own GetTaskRun, and Step 6 can reuse the same
		// value without a second round-trip to the store.
		var currentParentTR *store.TaskRun
		if parentRunID != "" {
			var fetchErr error
			currentParentTR, fetchErr = e.store.GetTaskRun(ctx, parentRunID)
			if fetchErr != nil {
				return fetchErr
			}
			if err := e.createEligibleTasks(ctx, workflowRunID, wf, currentParentTR, siblings); err != nil {
				return err
			}
		}

		// 4. Re-read siblings after createEligibleTasks, which may have added new TaskRuns
		// (newly eligible tasks) or changed statuses (skipped tasks).
		//
		// Continuing the example above:
		//   Before re-read: siblings=[fetch:Succeeded]           ← stale, misses "notify"
		//   After  re-read: siblings=[fetch:Succeeded, notify:Created]  ← fresh
		//   The loop's next iteration (step 2) will then activate notify:Created → dispatch it.
		siblings, err = e.store.ListTaskRunsByParent(ctx, workflowRunID, parentRunID)
		if err != nil {
			return err
		}

		// 5. Root scope (parentRunID=""): finalize the workflow if all top-level tasks are
		// terminal, otherwise wait.
		//
		// Example — workflow with a single top-level DAG "main":
		//   advanceScope(parentRunID="") is called when "main" (DAG container) becomes Succeeded.
		//   siblings=[main:Succeeded] → allTerminal=true → finalizeWorkflow sets WF to Succeeded.
		//
		//   If "main" is still Running (some tasks inside are not yet terminal):
		//   siblings=[main:Running] → allTerminal=false → return nil (wait for next event).
		if parentRunID == "" {
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
		// currentParentTR was fetched in Step 3 above (same loop iteration, parentRunID unchanged).
		parentTR := currentParentTR

		if parentTR.TemplateType == model.TemplateTypeLoop {
			// Concurrency refill: spawn Created iterations to fill free slots, even if
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
		// Use Get + UpdateTaskRun(Running → terminal) guarded by Token to prevent
		// concurrent advanceScope calls from double-finalizing the container.
		//
		// For DAG containers, aggregation is continueOn-aware: child tasks that
		// failed/errored/timed-out but whose phase is "tolerated" by their merged
		// continueOn policy (task-level OR dag-level fallback) are treated as
		// non-failures for the purpose of the parent DAG's phase.
		//
		// Loop containers use plain aggregatePhase: iterations are independent and
		// have no dependency graph, so continueOn has no aggregation semantics there.
		// (If a loop body is itself a DAG with continueOn, its internal aggregation
		// already produces the correct phase before the loop sees it.)
		var phase model.Phase
		var msg string
		if parentTR.TemplateType == model.TemplateTypeDAG {
			tmpl := internal.FindTemplate(wf, parentTR.TemplateName)
			var dag *model.DAG
			if tmpl != nil {
				dag = tmpl.DAG
			}
			phase, msg = aggregatePhaseDAG(siblings, dag)
		} else {
			phase, msg = aggregatePhase(siblings)
		}
		tr, err := e.store.GetTaskRun(ctx, parentTR.RunID)
		if err != nil {
			return err
		}
		// Guard: only finalize if still Running (another advanceScope may have already done it).
		if tr.Status == nil || *tr.Status != model.PhaseRunning {
			return nil
		}

		// For DAG/Loop containers, collect container-level outputs so downstream tasks
		// and the workflow itself can reference them via valueFrom.
		var containerOutputs *model.Outputs
		switch parentTR.TemplateType {
		case model.TemplateTypeDAG:
			// Resolve dag.outputs.parameters valueFrom references from children.
			tmpl := internal.FindTemplate(wf, parentTR.TemplateName)
			if tmpl != nil && tmpl.DAG != nil && tmpl.DAG.Outputs != nil {
				env := e.newVarBuilder().
					WithWorkflowArgs(wf.Spec.Arguments).
					WithSiblingTaskRuns(siblings).
					Build()
				collector := binding.NewCollector(e.exprEvaluator)
				collected, _ := collector.CollectDAGOutputs(ctx, tmpl.DAG.Outputs, siblings, env)
				if collected != nil {
					containerOutputs = &model.Outputs{
						Phase:       phase,
						ExecOutputs: model.ExecOutputs{Parameters: collected.Parameters},
					}
				}
			}
		case model.TemplateTypeLoop:
			// Aggregate loop iteration outputs according to the loop's aggregate strategy.
			tmpl := internal.FindTemplate(wf, parentTR.TemplateName)
			if tmpl != nil && tmpl.Loop != nil {
				results := make([]internal.LoopIterationResult, 0, len(siblings))
				for i, sib := range siblings {
					r := internal.LoopIterationResult{Index: i, Outputs: sib.Outputs}
					if sib.Status != nil {
						r.Phase = *sib.Status
					}
					if sib.Message != nil {
						r.Message = *sib.Message
					}
					results = append(results, r)
				}
				_, _, aggregated := internal.AggregateResults(results, tmpl.Loop.Aggregate)
				if aggregated != nil {
					containerOutputs = &model.Outputs{
						Phase:       phase,
						ExecOutputs: model.ExecOutputs{Parameters: aggregated.Parameters},
					}
				}
			}
		}

		containerMetrics := internal.ComputeMetricsFinish(tr.Metrics)

		_, err = e.store.UpdateTaskRun(ctx, &store.TaskRun{
			RunID:   tr.RunID,
			Token:   tr.Token,
			Status:  &phase,
			Message: &msg,
			Outputs: containerOutputs,
			Metrics: containerMetrics,
		})
		if err != nil {
			// Token mismatch: another advanceScope already finalized this container — stop here.
			return nil
		}

		// Move up to the parent's parent scope and re-evaluate in the next iteration.
		parentRunID = parentTR.ParentRunID
	}
}

// activateTaskRun transitions a Created TaskRun to its active state based on TemplateType.
//
// The three template types have fundamentally different activation paths:
//
//   - DAG (container): stays Created and opens a child scope via advanceScope.
//     It transitions to Ready/Running only when its first leaf task is dispatched
//     (markAncestorsReady + markAncestorsRunning walk up the chain at that point).
//     DAG itself never reaches a Broker — it's done when all its children are terminal.
//
//   - Task (leaf): claimed as Ready (Created → Ready via token guard) then dispatched
//     to the Broker for execution. Status transitions to Running when OnTaskStarted fires.
//
//   - Loop (container): stays Created and starts a loop controller.
//     It transitions to Ready/Running only when its first iteration task is dispatched
//     (markAncestorsReady + markAncestorsRunning walk up the chain at that point).
//     The controller is responsible for expanding items / evaluating repeatCondition
//     and spawning iteration TaskRuns.
func (e *Engine) activateTaskRun(ctx context.Context, workflowRunID string, wf *model.Workflow, tr *store.TaskRun) error {
	switch tr.TemplateType {
	case model.TemplateTypeDAG:
		// Resolve call-site arguments into DAG inputs before entering the scope, so that
		// child tasks can resolve "inputs.parameters.*" expressions via parentTR.Inputs.
		// This mirrors the Loop path (resolveLoopInputs + UpdateTaskRun).
		resolvedDAGInputs, err := e.resolveDAGInputs(ctx, workflowRunID, wf, tr)
		if err != nil {
			return fmt.Errorf("resolve dag inputs for %q: %w", tr.TaskName, err)
		}
		if resolvedDAGInputs != nil {
			if updated, updateErr := e.store.UpdateTaskRun(ctx, &store.TaskRun{
				RunID:  tr.RunID,
				Token:  tr.Token,
				Inputs: resolvedDAGInputs,
			}); updateErr == nil && updated != nil {
				tr = updated
			}
		}
		// DAG stays Created here. It will transition to Ready/Running when its first
		// leaf task is dispatched (via markAncestorsReady + markAncestorsRunning in dispatchLeafTask).
		// The parent scope's allTerminal check is unaffected because Created is
		// not a terminal state per Phase.IsTerminal().
		//
		// tr.RunID becomes the startParentRunID for the child scope.
		return e.advanceScope(ctx, workflowRunID, wf, tr.RunID)

	case model.TemplateTypeTask:
		// Leaf task: build a TaskAssignment (executor + resolved inputs) and hand off
		// to the Broker. Status transitions to Running inside the Broker/Executor.
		return e.dispatchLeafTask(ctx, workflowRunID, wf, tr)

	case model.TemplateTypeLoop:
		// Loop stays Created here, mirroring the DAG behaviour.
		// It transitions to Ready/Running only when its first iteration task is dispatched
		// (markAncestorsReady + markAncestorsRunning walk up the chain at that point).
		//
		// Resolve call-site arguments into loop inputs before starting the controller
		// so that startLoopController can build the EvalVars with WithResolvedInputs and
		// expressions like "{{inputs.parameters.content-list}}" in itemsFrom resolve correctly.
		// Persist resolved inputs so trySpawnNextIterations can re-expand the item list
		// using the same resolved values when spawning additional iterations later.
		resolvedLoopInputs, err := e.resolveLoopInputs(ctx, workflowRunID, wf, tr)
		if err != nil {
			return fmt.Errorf("resolve loop inputs for %q: %w", tr.TaskName, err)
		}
		if resolvedLoopInputs != nil {
			// Use the returned TaskRun directly — no need for a separate GetTaskRun re-fetch.
			if updated, updateErr := e.store.UpdateTaskRun(ctx, &store.TaskRun{
				RunID:  tr.RunID,
				Token:  tr.Token,
				Inputs: resolvedLoopInputs,
			}); updateErr == nil && updated != nil {
				tr = updated
			}
		}
		return e.startLoopController(ctx, workflowRunID, wf, tr, resolvedLoopInputs)

	default:
		return fmt.Errorf("unknown template type %q for task %q", tr.TemplateType, tr.TaskName)
	}
}

// markAncestorsReady walks up the ParentRunID chain starting from parentRunID,
// transitioning every Created ancestor container TaskRun to Ready.
// Once the chain reaches the root (parentRunID=""), it also promotes the
// WorkflowRun from Created to Ready.
//
// Called from dispatchLeafTask immediately after the leaf task transitions
// Created → Ready, so ancestor containers reflect "at least one child leaf
// has been committed for dispatch" before the broker goroutine starts.
func (e *Engine) markAncestorsReady(ctx context.Context, workflowRunID string, parentRunID string) error {
	for parentRunID != "" {
		ancestor, err := e.store.GetTaskRun(ctx, parentRunID)
		if err != nil {
			return err
		}
		// Only transition Created → Ready. If already Ready/Running/terminal, stop walking.
		if ancestor.Status == nil || *ancestor.Status != model.PhaseCreated {
			break
		}
		ready := model.PhaseReady
		empty := ""
		_, err = e.store.UpdateTaskRun(ctx, &store.TaskRun{
			RunID:   ancestor.RunID,
			Token:   ancestor.Token,
			Status:  &ready,
			Message: &empty,
		})
		if err != nil {
			// Token mismatch: another goroutine already transitioned this ancestor — stop walking.
			break
		}
		parentRunID = ancestor.ParentRunID
	}

	// Promote the WorkflowRun from Created to Ready.
	wfRun, err := e.store.GetWorkflowRun(ctx, workflowRunID)
	if err != nil {
		return err
	}
	if wfRun.Status == nil || *wfRun.Status != model.PhaseCreated {
		return nil
	}
	ready := model.PhaseReady
	empty := ""
	_, err = e.store.UpdateWorkflowRun(ctx, &store.WorkflowRun{
		RunID:   wfRun.RunID,
		Token:   wfRun.Token,
		Status:  &ready,
		Message: &empty,
	})
	_ = err
	return nil
}

// markAncestorsRunning walks up the ParentRunID chain starting from parentRunID,
// transitioning every Ready ancestor TaskRun to Running and recording StartedAt.
// Once the chain reaches the root (parentRunID=""), it also promotes the
// WorkflowRun from Ready to Running if it hasn't been already.
//
// Called from OnTaskStarted when a leaf task transitions Ready → Running,
// so container ancestors and the WorkflowRun reflect the moment real execution begins.
func (e *Engine) markAncestorsRunning(ctx context.Context, workflowRunID string, parentRunID string) error {
	startedAt := time.Now().UTC().Format(time.RFC3339)

	for parentRunID != "" {
		ancestor, err := e.store.GetTaskRun(ctx, parentRunID)
		if err != nil {
			return err
		}
		// Only transition Ready → Running. If already Running or terminal, stop walking.
		if ancestor.Status == nil || *ancestor.Status != model.PhaseReady {
			break
		}
		running := model.PhaseRunning
		empty := ""
		_, err = e.store.UpdateTaskRun(ctx, &store.TaskRun{
			RunID:   ancestor.RunID,
			Token:   ancestor.Token,
			Status:  &running,
			Message: &empty,
			Metrics: &model.Metrics{StartedAt: startedAt},
		})
		if err != nil {
			// Token mismatch: another goroutine already transitioned this ancestor — stop walking.
			break
		}
		parentRunID = ancestor.ParentRunID
	}

	// Promote the WorkflowRun from Ready to Running (idempotent via Token).
	wfRun, err := e.store.GetWorkflowRun(ctx, workflowRunID)
	if err != nil {
		return err
	}
	if wfRun.Status == nil || *wfRun.Status != model.PhaseReady {
		return nil
	}
	running := model.PhaseRunning
	empty := ""
	_, err = e.store.UpdateWorkflowRun(ctx, &store.WorkflowRun{
		RunID:   wfRun.RunID,
		Token:   wfRun.Token,
		Status:  &running,
		Message: &empty,
		Metrics: &model.Metrics{StartedAt: startedAt},
	})
	// Token mismatch or already Running — either is fine, not an error.
	_ = err
	return nil
}

// finalizeWorkflow marks the workflow as complete based on top-level TaskRun results.
//
// Uses Get + UpdateWorkflowRun(Running → terminal) guarded by Token to prevent
// double-finalization: in a local synchronous broker, Dispatch calls OnTaskCompleted
// inline, which calls advanceScope and may reach finalizeWorkflow before the outer
// advanceScope call returns. The Token ensures that only the first finalization
// succeeds and fires workflow-level hooks; subsequent calls are no-ops because
// the WorkflowRun is no longer Running.
func (e *Engine) finalizeWorkflow(ctx context.Context, workflowRunID string, topLevelRuns []*store.TaskRun, wf *model.Workflow) {
	phase, msg := aggregatePhase(topLevelRuns)

	wfRun, err := e.store.GetWorkflowRun(ctx, workflowRunID)
	if err != nil {
		return
	}
	// Guard: only finalize if still Running.
	if wfRun.Status == nil || *wfRun.Status != model.PhaseRunning {
		return
	}
	// Compute FinishedAt/Duration from the StartedAt recorded at workflow start.
	wfMetrics := internal.ComputeMetricsFinish(wfRun.Metrics)

	// Collect workflow-level outputs from the entrypoint DAG container.
	// topLevelRuns contains the single entrypoint container (e.g. main DAG).
	// Its resolved Outputs (populated by advanceScope) become the workflow outputs.
	var wfOutputs *model.Outputs
	for _, tr := range topLevelRuns {
		if tr.Outputs != nil && len(tr.Outputs.Parameters) > 0 {
			wfOutputs = tr.Outputs
			break
		}
	}

	_, err = e.store.UpdateWorkflowRun(ctx, &store.WorkflowRun{
		RunID:   wfRun.RunID,
		Token:   wfRun.Token,
		Status:  &phase,
		Message: &msg,
		Metrics: wfMetrics,
		Outputs: wfOutputs,
	})
	if err != nil {
		// Token mismatch: already finalized by another path — stop.
		return
	}

	if wf != nil {
		internal.FireWorkflowHooks(ctx, e.hookNotifier, wf, workflowRunID, phase)
	}
}

// aggregatePhase determines the final phase from a set of terminal TaskRuns.
//
// Priority order (highest to lowest):
//  1. Cancelled — any cancelled sibling means the scope was abandoned.
//  2. Error     — system/infrastructure errors (includes Timeout).
//  3. Failed    — business-level failure.
//  4. Succeeded — all siblings completed normally (includes Skipped).
func aggregatePhase(taskRuns []*store.TaskRun) (model.Phase, string) {
	hasCancelled := false
	hasError := false
	hasFailure := false

	for _, tr := range taskRuns {
		if tr.Status == nil {
			continue
		}
		switch *tr.Status {
		case model.PhaseCancelled:
			hasCancelled = true
		case model.PhaseError, model.PhaseTimeout:
			hasError = true
		case model.PhaseFailed:
			hasFailure = true
		}
	}

	switch {
	case hasCancelled:
		return model.PhaseCancelled, "one or more tasks were cancelled"
	case hasError:
		return model.PhaseError, "one or more tasks errored"
	case hasFailure:
		return model.PhaseFailed, "one or more tasks failed"
	default:
		return model.PhaseSucceeded, ""
	}
}

// aggregatePhaseDAG is like aggregatePhase but respects the DAG's continueOn policy.
//
// Each child TaskRun is matched against its call-site task node in the DAG to
// obtain a merged continueOn (task-level takes precedence over DAG-level default).
// If a child's terminal phase is "tolerated" by its merged continueOn, it is treated
// as a non-failure for the purpose of determining the parent DAG's aggregated phase.
//
// Example: step-b has continueOn.failed=true and ends in Failed.
//   - Without this function: DAG phase = Failed (step-b's failure propagates).
//   - With this function:    DAG phase = Succeeded (step-b's failure is tolerated).
//
// Cancelled tasks are never tolerated — cancellation always propagates.
func aggregatePhaseDAG(taskRuns []*store.TaskRun, dag *model.DAG) (model.Phase, string) {
	if dag == nil {
		// Fallback: no DAG definition, use plain aggregation.
		return aggregatePhase(taskRuns)
	}

	// Build a quick-lookup map: taskName → call-site Task node for continueOn access.
	taskNodeByName := make(map[string]*model.Task, len(dag.Tasks))
	for i := range dag.Tasks {
		taskNodeByName[dag.Tasks[i].Name] = &dag.Tasks[i]
	}

	hasCancelled := false
	hasError := false
	hasFailure := false

	for _, tr := range taskRuns {
		if tr.Status == nil {
			continue
		}
		phase := *tr.Status
		switch phase {
		case model.PhaseCancelled:
			// Cancellation always propagates regardless of continueOn.
			hasCancelled = true
		case model.PhaseError, model.PhaseTimeout, model.PhaseFailed:
			// Check whether this task's merged continueOn tolerates this phase.
			var taskCO *model.ContinueOn
			if node, ok := taskNodeByName[tr.TaskName]; ok {
				taskCO = node.ContinueOn
			}
			co := internal.MergeContinueOn(taskCO, dag.ContinueOn)
			tolerated := false
			if co != nil {
				switch phase {
				case model.PhaseFailed:
					tolerated = co.Failed
				case model.PhaseError:
					tolerated = co.Error
				case model.PhaseTimeout:
					tolerated = co.Timeout
				}
			}
			if !tolerated {
				if phase == model.PhaseFailed {
					hasFailure = true
				} else {
					hasError = true
				}
			}
		}
	}

	switch {
	case hasCancelled:
		return model.PhaseCancelled, "one or more tasks were cancelled"
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
		if tr.Status == nil || !tr.Status.IsTerminal() {
			return false
		}
	}
	return true
}
