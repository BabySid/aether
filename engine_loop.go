package aether

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BabySid/aether/errsink"
	"github.com/BabySid/aether/idgen"
	"github.com/BabySid/aether/internal"
	"github.com/BabySid/aether/internal/binding"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// resolveLoopInputs binds the call-site arguments to the loop template's declared inputs.
//
// Returns the resolved *model.Inputs that startLoopController uses to build the EvalVars
// via binding.WithResolvedInputs(...), making expressions like
// itemsFrom: "{{inputs.parameters.content-list}}" resolvable.
//
// Note: Inputs is immutable in the store (set at creation), so resolved inputs are passed
// in-memory rather than persisted.
func (e *Engine) resolveLoopInputs(ctx context.Context, workflowRunID string, wf *model.Workflow, loopTR *store.TaskRun) (*model.Inputs, error) {
	loopTmpl := internal.FindTemplate(wf, loopTR.TemplateName)
	if loopTmpl == nil || loopTmpl.Loop == nil {
		return nil, nil
	}

	// Find call-site Arguments from the parent DAG task node (if any).
	var callSiteArgs *model.Arguments
	if loopTR.ParentRunID != "" {
		parentTR, err := e.store.GetTaskRun(ctx, loopTR.ParentRunID)
		if err != nil {
			return nil, fmt.Errorf("get parent for loop %q: %w", loopTR.TaskName, err)
		}
		parentTmpl := internal.FindTemplate(wf, parentTR.TemplateName)
		if parentTmpl != nil && parentTmpl.DAG != nil {
			if dagTask := internal.FindTask(parentTmpl.DAG, loopTR.TaskName); dagTask != nil {
				callSiteArgs = dagTask.Arguments
			}
		}
	}

	// Build EvalVars: workflow-level args + sibling outputs.
	siblingRuns, err := e.store.ListTaskRunsByParent(ctx, workflowRunID, loopTR.ParentRunID)
	if err != nil {
		return nil, fmt.Errorf("list siblings for loop input binding %q: %w", loopTR.TaskName, err)
	}
	env := e.newVarBuilder().
		WithWorkflowArgs(wf.Spec.Arguments).
		WithSiblingTaskRuns(siblingRuns).
		Build()

	binder := binding.NewBinder(e.exprEvaluator, e.secretStore, e.errorSink)
	return binder.Bind(ctx, loopTmpl.Loop.Inputs, callSiteArgs, env)
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
// startLoopController initialises a Loop container and creates the first batch of
// iteration TaskRuns.
//
// resolvedInputs is the result of resolveLoopInputs: call-site arguments merged with
// the loop template's declared inputs. It is passed in-memory (not persisted) because
// TaskRun.Inputs is immutable after creation.
func (e *Engine) startLoopController(ctx context.Context, workflowRunID string, wf *model.Workflow, loopTR *store.TaskRun, resolvedInputs *model.Inputs) error {
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

	// 2. Expand iterations.
	// Build EvalVars from workflow args + sibling task outputs + loop's resolved inputs
	// so that itemsFrom expressions like "{{inputs.parameters.content-list}}" resolve correctly.
	siblingRuns, err := e.store.ListTaskRunsByParent(ctx, workflowRunID, loopTR.ParentRunID)
	if err != nil {
		return fmt.Errorf("list siblings for loop %q: %w", loopTR.TemplateName, err)
	}
	env := e.newVarBuilder().
		WithWorkflowArgs(wf.Spec.Arguments).
		WithResolvedInputs(resolvedInputs).
		WithSiblingTaskRuns(siblingRuns).
		Build()
	iterations, err := internal.ExpandLoopIterations(ctx, loop, e.exprEvaluator, env)
	if err != nil {
		// Expansion failed (e.g. expression error). Report to ErrorSink, then mark
		// the loop as Error so the parent scope can detect a terminal state.
		e.reportError(ctx, err, errsink.ErrorContext{
			WorkflowRunID: workflowRunID, TaskRunID: loopTR.RunID,
			Operation: "startLoopController.expandLoop", Severity: errsink.SeverityError,
		})
		errPhase := model.PhaseError
		errMsg := fmt.Sprintf("expand loop %q: %v", loopTR.TemplateName, err)
		if _, updateErr := e.store.UpdateTaskRun(ctx, &store.TaskRun{
			RunID:   loopTR.RunID,
			Token:   loopTR.Token,
			Status:  &errPhase,
			Message: &errMsg,
		}); updateErr != nil {
			e.reportError(ctx, updateErr, errsink.ErrorContext{
				WorkflowRunID: workflowRunID, TaskRunID: loopTR.RunID,
				Operation: "startLoopController.markError", Severity: errsink.SeverityError,
			})
		}
		return nil
	}

	// 3. Zero iterations: mark the loop as succeeded immediately.
	// The loop is still Created at this point (never dispatched a leaf task),
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

	// 4. Create one child TaskRun per iteration (Created, ParentRunID = loopTR.RunID).
	// When concurrency is set, only the first min(concurrency, total) iterations are
	// created now. As each completes, trySpawnNextIterations refills the active slot.
	// Iteration parameters are serialised into TaskRun.Inputs so that dispatchLeafTask
	// can forward them to the broker without re-resolution.
	numToCreate := len(iterations)
	if loop.Concurrency > 0 && loop.Concurrency < numToCreate {
		numToCreate = loop.Concurrency
	}
	for i := 0; i < numToCreate; i++ {
		if err := e.createIterationRun(ctx, workflowRunID, wf, loopTR, loop, loop.Body, bodyTemplateType, i, iterations[i]); err != nil {
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
func (e *Engine) spawnRepeatIteration(ctx context.Context, workflowRunID string, wf *model.Workflow, loopTR *store.TaskRun, iterIndex int) error {
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
	pendingPhase := model.PhaseCreated
	iterRun := &store.TaskRun{
		RunID: e.idGen.Generate(idgen.Context{
			WorkflowRunID: workflowRunID,
			WorkflowKind:  wf.Kind,
			TaskName:      loop.Body,
			TemplateName:  loop.Body,
		}),
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
func (e *Engine) tryAdvanceRepeatLoop(ctx context.Context, workflowRunID string, wf *model.Workflow, parentTR *store.TaskRun, children []*store.TaskRun) (bool, error) {
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

// createIterationRun creates a single Created child TaskRun for one iteration of an
// items/itemsFrom loop.
//
// Called by startLoopController (initial batch) and trySpawnNextIterations (concurrency
// refill). iterParams (e.g. {loop_iter.item:"a.txt", loop_iter.index:0}) are serialised
// into TaskRun.Inputs so dispatchLeafTask can forward them to the broker verbatim without
// re-running expression resolution.
//
// If the loop template has an Arguments block, each argument is resolved against the
// current iteration env (which exposes iterator.item and iterator.index aliases) and
// merged into the TaskRun.Inputs. This allows loop arguments to:
//   - Map iteration values to body-template input names via {{iterator.item}} etc.
//   - Pass static values (e.g. outputs declarations) through to body tasks.
//
// The Scope field is set to "<loopTaskName>.loop[<iterIndex>]/" for traceability.
func (e *Engine) createIterationRun(ctx context.Context, workflowRunID string, wf *model.Workflow, loopTR *store.TaskRun, loop *model.Loop, bodyName, bodyTemplateType string, iterIndex int, iterParams map[string]any) error {
	iterScope := fmt.Sprintf("%s.loop[%d]/", loopTR.TaskName, iterIndex)

	// Build the iteration env: loop_iter.* keys + iterator.* aliases.
	// The iterator.* aliases let loop arguments reference {{iterator.item}} and
	// {{iterator.index}} in their value expressions, which is the user-facing syntax.
	iterEnv := make(map[string]any, len(iterParams)+2)
	for k, v := range iterParams {
		iterEnv[k] = v
	}
	if item, ok := iterParams["loop_iter.item"]; ok {
		iterEnv["iterator.item"] = item
	}
	if idx, ok := iterParams["loop_iter.index"]; ok {
		iterEnv["iterator.index"] = idx
	}

	// Start with the raw iteration params (loop_iter.*).
	params := make([]model.Parameter, 0, len(iterParams))
	for k, v := range iterParams {
		valJSON, marshalErr := json.Marshal(v)
		if marshalErr != nil {
			return fmt.Errorf("marshal iteration param %q: %w", k, marshalErr)
		}
		params = append(params, model.Parameter{Name: k, Value: valJSON})
	}

	// Resolve loop arguments and append to params.
	// Arguments whose value is a {{...}} expression are interpolated against iterEnv.
	// Static arguments (plain values, arrays, objects) are passed through unchanged.
	if loop != nil && loop.Arguments != nil {
		for _, arg := range loop.Arguments.Parameters {
			resolved := resolveIterArg(arg, iterEnv)
			params = append(params, resolved)
		}
	}

	var iterInputs *model.Inputs
	if len(params) > 0 {
		iterInputs = &model.Inputs{Parameters: params}
	}

	iterPending := model.PhaseCreated
	iterRun := &store.TaskRun{
		RunID: e.idGen.Generate(idgen.Context{
			WorkflowRunID: workflowRunID,
			WorkflowKind:  wf.Kind,
			TaskName:      bodyName,
			TemplateName:  bodyName,
		}),
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

// resolveIterArg resolves a single loop argument parameter against the current
// iteration's env (which contains iterator.item, iterator.index, and loop_iter.* keys).
//
// If the argument value is a JSON string containing a {{...}} placeholder, it is
// interpolated against the env. Non-string values (arrays, objects, numbers, booleans)
// are forwarded as-is because they cannot contain placeholder expressions.
//
// This enables loop arguments like:
//
//	{"name": "filename", "value": "{{iterator.item}}"}   → resolved to item value
//	{"name": "index",    "value": "{{iterator.index}}"}  → resolved to index value
//	{"name": "outputs",  "value": [...]}                  → passed through unchanged
func resolveIterArg(arg model.Parameter, iterEnv map[string]any) model.Parameter {
	out := model.Parameter{Name: arg.Name, Type: arg.Type}

	if len(arg.Value) == 0 {
		return out
	}

	// If the raw JSON value is a quoted string, try {{...}} interpolation.
	var s string
	if json.Unmarshal(arg.Value, &s) == nil {
		// It's a JSON string — interpolate placeholders.
		resolved, interpolated := binding.Interpolate(s, iterEnv)
		if interpolated {
			b, _ := json.Marshal(resolved)
			out.Value = b
		} else {
			out.Value = arg.Value
		}
		return out
	}

	// Non-string JSON value (array, object, number, bool) — pass through unchanged.
	out.Value = arg.Value
	return out
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
func (e *Engine) trySpawnNextIterations(ctx context.Context, workflowRunID string, wf *model.Workflow, parentTR *store.TaskRun, children []*store.TaskRun) (bool, error) {
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
	// Use the same EvalVars approach: workflow args + loop resolved inputs + sibling outputs.
	siblingRuns, err := e.store.ListTaskRunsByParent(ctx, workflowRunID, parentTR.ParentRunID)
	if err != nil {
		return false, fmt.Errorf("list siblings for loop %q concurrency advance: %w", parentTR.TemplateName, err)
	}
	env := e.newVarBuilder().
		WithWorkflowArgs(wf.Spec.Arguments).
		WithResolvedInputs(parentTR.Inputs).
		WithSiblingTaskRuns(siblingRuns).
		Build()
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
		if err := e.createIterationRun(ctx, workflowRunID, wf, parentTR, loop, loop.Body, bodyTemplateType, createdCount, iterations[createdCount]); err != nil {
			return false, err
		}
		createdCount++
		spawned = true
	}

	if spawned {
		// Activate the freshly-created iterations (Created state).
		if err := e.advanceScope(ctx, workflowRunID, wf, parentTR.RunID); err != nil {
			return false, err
		}
	}
	return spawned, nil
}
