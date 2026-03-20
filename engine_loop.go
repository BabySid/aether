package aether

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BabySid/aether/internal"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

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
//  1. Expands items → [{loop_iter.item:"a.txt",loop_iter.index:0}, ...]
//  2. Creates TaskRuns for iterations 0 and 1 only (concurrency=2)
//  3. Calls advanceScope(loopTR.RunID) → dispatches iterations 0 and 1 to the broker
//  4. When iteration 0 finishes, trySpawnNextIterations creates and dispatches iteration 2
//
// Each iteration TaskRun has ParentRunID = loopTR.RunID, so ListTaskRunsByParent returns
// all iterations as siblings. Iteration inputs (loop_iter.index, item fields) are pre-stored
// in TaskRun.Inputs and forwarded to the broker without further expression resolution.
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
		// Expansion failed (e.g. expression error). Mark the loop as Error so the
		// parent scope can detect a terminal state and progress rather than hanging.
		errPhase := model.PhaseError
		errMsg := fmt.Sprintf("expand loop %q: %v", loopTR.TemplateName, err)
		_, _ = e.store.UpdateTaskRun(ctx, &store.TaskRun{
			RunID:   loopTR.RunID,
			Token:   loopTR.Token,
			Status:  &errPhase,
			Message: &errMsg,
		})
		return nil
	}

	// 3. Zero iterations: mark the loop as succeeded immediately.
	// The loop is still Pending at this point (never dispatched a leaf task),
	// so we update it directly without a token guard — no concurrent writer can
	// have transitioned it yet.
	if len(iterations) == 0 {
		succeeded := model.PhaseSucceeded
		noIterMsg := "loop had no iterations"
		_, err = e.store.UpdateTaskRun(ctx, &store.TaskRun{
			RunID:   loopTR.RunID,
			Token:   loopTR.Token,
			Status:  &succeeded,
			Message: &noIterMsg,
		})
		return err
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
//   - tryAdvanceRepeatLoop builds env={loop_iter.index:0, tasks.check.phase:"Succeeded", ...}
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
	pendingPhase := model.PhasePending
	iterRun := &store.TaskRun{
		RunID:         e.idGen.Generate(),
		WorkflowRunID: workflowRunID,
		ParentRunID:   loopTR.RunID,
		Depth:         loopTR.Depth + 1,
		Scope:         iterScope,
		TaskName:      loop.Body,
		TemplateName:  loop.Body,
		TemplateType:  bodyTemplateType,
		Status:        &pendingPhase,
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
//	env = { loop_iter.index: N-1,
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

	// Find the last completed iteration by its deterministic Scope value.
	// repeatCondition is serial, so the previous iteration's scope is always
	// "<loopTaskName>.loop[<nextIndex-1>]/". Using Scope avoids relying on RunID
	// ordering, which is an implementation detail of the ID generator.
	lastScope := fmt.Sprintf("%s.loop[%d]/", parentTR.TaskName, nextIndex-1)
	var lastRun *store.TaskRun
	for _, c := range children {
		if c.Scope == lastScope {
			lastRun = c
			break
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
// refill). iterParams (e.g. {loop_iter.item:"a.txt", loop_iter.index:0}) are serialised
// into TaskRun.Inputs so dispatchLeafTask can forward them to the broker verbatim without
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

	iterPending := model.PhasePending
	iterRun := &store.TaskRun{
		RunID:         e.idGen.Generate(),
		WorkflowRunID: workflowRunID,
		ParentRunID:   loopTR.RunID,
		Depth:         loopTR.Depth + 1,
		Scope:         iterScope,
		TaskName:      bodyName,
		TemplateName:  bodyName,
		TemplateType:  bodyTemplateType,
		Status:        &iterPending,
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
		if c.Status == nil || !c.Status.IsTerminal() {
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
