package aether

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BabySid/aether/internal"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// scheduleNext is the core scheduling logic, shared by Resume and OnTaskCompleted.
// It loads the workflow, computes ready tasks, evaluates when conditions,
// creates TaskRuns, and dispatches them.
func (e *Engine) scheduleNext(ctx context.Context, workflowRunID uint64) error {
	// 1. Load workflow
	run, err := e.store.GetWorkflowRun(ctx, workflowRunID)
	if err != nil {
		return err
	}

	// If workflow is already terminal, nothing to do
	if run.Status.IsTerminal() {
		return nil
	}

	var wf model.Workflow
	if err := json.Unmarshal(run.Workflow, &wf); err != nil {
		return err
	}
	internal.FillDefaults(&wf)

	tmpl := internal.FindTemplate(&wf, wf.Spec.Entrypoint)
	if tmpl == nil || tmpl.DAG == nil {
		return nil
	}

	// 2. Compute next ready tasks
	allTaskRuns, err := e.store.ListTaskRuns(ctx, workflowRunID)
	if err != nil {
		return err
	}

	readyTasks := internal.FindReadyTasks(tmpl.DAG, allTaskRuns)

	// 3. Evaluate "when" conditions and partition into execute vs skip
	var toExecute []model.Task
	var toSkip []model.Task
	for _, task := range readyTasks {
		if task.When == "" {
			toExecute = append(toExecute, task)
			continue
		}
		shouldRun, err := internal.EvalWhenCondition(ctx, task.When, e.exprEvaluator, allTaskRuns)
		if err != nil {
			toSkip = append(toSkip, task)
			continue
		}
		if shouldRun {
			toExecute = append(toExecute, task)
		} else {
			toSkip = append(toSkip, task)
		}
	}

	// 4. Create and persist skipped TaskRuns
	for _, task := range toSkip {
		skippedRun := &store.TaskRun{
			RunID:         e.idGen.Generate(),
			WorkflowRunID: workflowRunID,
			TaskName:      task.Name,
			Path:          task.Name,
			TemplateName:  task.Template,
			Status:        model.PhaseSkipped,
			Message:       fmt.Sprintf("when condition %q evaluated to false", task.When),
		}
		_, _ = e.store.BatchCreateTaskRuns(ctx, []*store.TaskRun{skippedRun})
	}

	// 5. Create and dispatch executable TaskRuns
	if len(toExecute) > 0 {
		if err := e.dispatchTasks(ctx, workflowRunID, toExecute, tmpl.DAG, &wf, allTaskRuns); err != nil {
			return err
		}
	}

	// 6. If we skipped some tasks, re-advance to handle their dependents
	if len(toSkip) > 0 {
		return e.scheduleNext(ctx, workflowRunID)
	}

	// 7. If nothing was dispatched and nothing was skipped, try to finalize
	if len(toExecute) == 0 {
		allTaskRuns, _ = e.store.ListTaskRuns(ctx, workflowRunID)
		e.tryFinalizeWorkflow(ctx, workflowRunID, allTaskRuns, &wf)
	}

	return nil
}

// dispatchTasks creates TaskRuns for the given tasks, resolves parameters, and dispatches them.
func (e *Engine) dispatchTasks(ctx context.Context, workflowRunID uint64, tasks []model.Task, dag *model.DAG, wf *model.Workflow, existingRuns []*store.TaskRun) error {
	newTaskRuns := make([]*store.TaskRun, 0, len(tasks))
	for _, task := range tasks {
		newTaskRuns = append(newTaskRuns, &store.TaskRun{
			RunID:         e.idGen.Generate(),
			WorkflowRunID: workflowRunID,
			TaskName:      task.Name,
			Path:          task.Name,
			TemplateName:  task.Template,
			Status:        model.PhasePending,
		})
	}

	created, err := e.store.BatchCreateTaskRuns(ctx, newTaskRuns)
	if err != nil {
		return fmt.Errorf("aether: create task runs: %w", err)
	}

	// Build resolve context for parameter resolution
	rc := &internal.ResolveContext{
		Eval:        e.exprEvaluator,
		SecretStore: e.secretStore,
		TaskRuns:    existingRuns,
		WfArgs:      wf.Spec.Arguments,
	}

	for _, cr := range created {
		taskTmpl := internal.FindTemplate(wf, cr.TemplateName)
		task := internal.FindTask(dag, cr.TaskName)

		// Check if this is a loop template — handle specially
		if taskTmpl != nil && taskTmpl.Loop != nil {
			if err := e.handleLoopTemplate(ctx, workflowRunID, cr.TaskName, taskTmpl, wf, existingRuns); err != nil {
				// Mark task as error
				cr.Status = model.PhaseError
				cr.Message = fmt.Sprintf("loop expansion failed: %v", err)
				_ = e.store.UpdateTaskRun(ctx, cr)
			}
			continue
		}

		// Resolve inputs with parameter binding
		assignment := internal.BuildTaskAssignment(workflowRunID, cr, taskTmpl, task, wf)
		if taskTmpl != nil && taskTmpl.Inputs != nil {
			resolvedInputs, err := internal.ResolveInputs(ctx, taskTmpl.Inputs, rc)
			if err == nil && resolvedInputs != nil {
				inputsJSON, _ := json.Marshal(resolvedInputs)
				assignment.Inputs = inputsJSON
			}
		}

		if err := e.taskBroker.Dispatch(ctx, assignment); err != nil {
			return fmt.Errorf("aether: dispatch task %q: %w", cr.TaskName, err)
		}
	}

	return nil
}

// handleLoopTemplate expands and dispatches a loop template's iterations.
func (e *Engine) handleLoopTemplate(ctx context.Context, workflowRunID uint64, taskName string, tmpl *model.Template, wf *model.Workflow, existingRuns []*store.TaskRun) error {
	if tmpl.Loop == nil {
		return nil
	}

	bodyTmpl := internal.FindTemplate(wf, tmpl.Loop.Body)
	if bodyTmpl == nil {
		return fmt.Errorf("loop body template %q not found", tmpl.Loop.Body)
	}

	// Build environment for expression evaluation
	env := internal.BuildTaskEnv(existingRuns)

	// Expand iterations
	iterations, err := internal.ExpandLoopIterations(ctx, tmpl.Loop, e.exprEvaluator, env)
	if err != nil {
		return fmt.Errorf("expand loop: %w", err)
	}

	if iterations == nil {
		// repeatCondition mode — not yet supported as a full dispatch loop
		// For now, treat as single iteration
		iterations = []map[string]any{{"item_index": 0}}
	}

	ld := &internal.LoopDispatcher{
		Store:     e.store,
		Broker:    e.taskBroker,
		Eval:      e.exprEvaluator,
		IDGen:     e.idGen.Generate,
		Workflow:  wf,
		BodyTempl: bodyTmpl,
	}

	concurrency := tmpl.Loop.Concurrency
	_, err = ld.DispatchIterations(ctx, workflowRunID, taskName, iterations, concurrency)
	return err
}

// tryFinalizeWorkflow checks if all tasks are terminal and updates the workflow status.
func (e *Engine) tryFinalizeWorkflow(ctx context.Context, workflowRunID uint64, taskRuns []*store.TaskRun, wf *model.Workflow) {
	allTerminal := true
	hasFailure := false
	hasError := false

	for _, tr := range taskRuns {
		if !tr.Status.IsTerminal() {
			allTerminal = false
			break
		}
		if tr.Status == model.PhaseFailed {
			hasFailure = true
		}
		if tr.Status == model.PhaseError || tr.Status == model.PhaseTimeout {
			hasError = true
		}
	}

	if !allTerminal {
		return
	}

	var finalPhase model.Phase
	var msg string
	switch {
	case hasError:
		finalPhase = model.PhaseError
		msg = "one or more tasks errored"
	case hasFailure:
		finalPhase = model.PhaseFailed
		msg = "one or more tasks failed"
	default:
		finalPhase = model.PhaseSucceeded
		msg = ""
	}

	_ = e.store.UpdateWorkflowRunStatus(ctx, workflowRunID, finalPhase, msg)

	// Fire workflow completion hooks
	if wf != nil {
		internal.FireWorkflowHooks(ctx, e.hookNotifier, wf, workflowRunID, finalPhase)
	}
}
