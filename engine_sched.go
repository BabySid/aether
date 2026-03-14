package aether

import (
	"context"
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
