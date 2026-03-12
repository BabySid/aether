package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/expr"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// LoopContext holds the state for a loop template execution.
type LoopContext struct {
	WorkflowRunID uint64
	LoopTaskName  string // parent task name
	Loop          *model.Loop
	BodyTemplate  *model.Template
	Workflow      *model.Workflow
}

// ExpandLoopIterations expands a loop definition into a list of iteration items.
// Each item is a map[string]any representing one iteration's parameters.
//
// Supports three modes:
//   - items: explicit list of items
//   - itemsFrom: dynamic list from expression evaluation
//   - repeatCondition: condition-based loop (returns nil; handled differently)
func ExpandLoopIterations(ctx context.Context, loop *model.Loop, eval expr.Evaluator, env map[string]any) ([]map[string]any, error) {
	// Mode 1: items — explicit list
	if len(loop.Items) > 0 {
		return expandItems(loop.Items, loop.MaxIterations), nil
	}

	// Mode 2: itemsFrom — dynamic list from expression
	if loop.ItemsFrom != "" {
		return expandItemsFrom(ctx, loop.ItemsFrom, eval, env, loop.MaxIterations)
	}

	// Mode 3: repeatCondition — handled by LoopRunner, not expanded upfront
	return nil, nil
}

// expandItems converts the raw items ([]any) into a list of parameter maps.
// Each item represents one iteration; objects are flattened, scalars use "item" key.
func expandItems(items []any, maxIterations int) []map[string]any {
	result := make([]map[string]any, 0, len(items))
	for i, item := range items {
		if maxIterations > 0 && i >= maxIterations {
			break
		}
		params := make(map[string]any)
		switch v := item.(type) {
		case map[string]interface{}:
			// Object items are flattened into the params map
			for k, vv := range v {
				params[k] = vv
			}
		default:
			// Scalar items use "item" key
			params["item"] = v
		}
		params["item_index"] = i
		result = append(result, params)
	}
	return result
}

// expandItemsFrom evaluates an expression to get a dynamic list.
func expandItemsFrom(ctx context.Context, expression string, eval expr.Evaluator, env map[string]any, maxIterations int) ([]map[string]any, error) {
	if eval == nil {
		return nil, fmt.Errorf("itemsFrom requires an ExprEvaluator but none is configured")
	}

	raw, err := eval.Eval(ctx, expression, env)
	if err != nil {
		return nil, fmt.Errorf("eval itemsFrom %q: %w", expression, err)
	}

	// Try to convert the result to a list
	var items []interface{}
	switch v := raw.(type) {
	case []interface{}:
		items = v
	case string:
		// Try parsing as JSON array
		if err := json.Unmarshal([]byte(v), &items); err != nil {
			return nil, fmt.Errorf("itemsFrom %q returned non-array string", expression)
		}
	default:
		return nil, fmt.Errorf("itemsFrom %q returned non-array type: %T", expression, raw)
	}

	result := make([]map[string]any, 0, len(items))
	for i, item := range items {
		if maxIterations > 0 && i >= maxIterations {
			break
		}
		params := make(map[string]any)
		switch v := item.(type) {
		case map[string]interface{}:
			for k, vv := range v {
				params[k] = vv
			}
		default:
			params["item"] = v
		}
		params["item_index"] = i
		result = append(result, params)
	}
	return result, nil
}

// LoopIterationResult holds the result of one loop iteration.
type LoopIterationResult struct {
	Index   int
	Phase   model.Phase
	Message string
	Outputs *model.Outputs
}

// AggregateResults aggregates loop iteration results according to the strategy.
func AggregateResults(results []LoopIterationResult, aggregate *model.Aggregate) (model.Phase, string, *model.Outputs) {
	if len(results) == 0 {
		return model.PhaseSucceeded, "", nil
	}

	strategy := "all"
	if aggregate != nil && aggregate.Strategy != "" {
		strategy = aggregate.Strategy
	}

	switch strategy {
	case "first_success":
		return aggregateFirstSuccess(results)
	case "quorum":
		return aggregateQuorum(results)
	default: // "all"
		return aggregateAll(results)
	}
}

// aggregateAll: all must succeed for the loop to succeed.
func aggregateAll(results []LoopIterationResult) (model.Phase, string, *model.Outputs) {
	var allParams []model.Parameter
	for _, r := range results {
		if r.Phase != model.PhaseSucceeded && r.Phase != model.PhaseSkipped {
			return r.Phase, fmt.Sprintf("iteration %d: %s", r.Index, r.Message), nil
		}
		if r.Outputs != nil {
			for _, p := range r.Outputs.Parameters {
				allParams = append(allParams, model.Parameter{
					Name:  fmt.Sprintf("%s_%d", p.Name, r.Index),
					Value: p.Value,
				})
			}
		}
	}
	var outputs *model.Outputs
	if len(allParams) > 0 {
		outputs = &model.Outputs{
			Phase:      model.PhaseSucceeded,
			Parameters: allParams,
		}
	}
	return model.PhaseSucceeded, "", outputs
}

// aggregateFirstSuccess: succeed as soon as one iteration succeeds.
func aggregateFirstSuccess(results []LoopIterationResult) (model.Phase, string, *model.Outputs) {
	for _, r := range results {
		if r.Phase == model.PhaseSucceeded {
			return model.PhaseSucceeded, "", r.Outputs
		}
	}
	return model.PhaseFailed, "no iteration succeeded", nil
}

// aggregateQuorum: succeed if more than half succeed.
func aggregateQuorum(results []LoopIterationResult) (model.Phase, string, *model.Outputs) {
	succeeded := 0
	for _, r := range results {
		if r.Phase == model.PhaseSucceeded {
			succeeded++
		}
	}
	if succeeded > len(results)/2 {
		return model.PhaseSucceeded, "", nil
	}
	return model.PhaseFailed, fmt.Sprintf("quorum not met: %d/%d succeeded", succeeded, len(results)), nil
}

// RunLoopItems executes loop iterations using the items/itemsFrom mode.
// Dispatches iterations with concurrency control.
// Returns when all iterations complete.
//
// This is called by the Engine when it encounters a loop template.
// Each iteration creates a child TaskRun and dispatches it.
type LoopDispatcher struct {
	Store     store.Store
	Broker    broker.TaskBroker
	Eval      expr.Evaluator
	IDGen     func() uint64
	Workflow  *model.Workflow
	BodyTempl *model.Template
}

// DispatchIterations creates and dispatches TaskRuns for each loop iteration.
// Returns the iteration TaskRun IDs.
func (ld *LoopDispatcher) DispatchIterations(
	ctx context.Context,
	workflowRunID uint64,
	parentTaskName string,
	iterations []map[string]any,
	concurrency int,
) ([]uint64, error) {
	if concurrency <= 0 {
		concurrency = len(iterations) // unlimited
	}

	taskRunIDs := make([]uint64, 0, len(iterations))
	sem := make(chan struct{}, concurrency)
	var mu sync.Mutex
	var firstErr error

	var wg sync.WaitGroup
	for i, iterParams := range iterations {
		iterName := fmt.Sprintf("%s[%d]", parentTaskName, i)

		// Create iteration TaskRun
		iterRunID := ld.IDGen()
		iterRun := &store.TaskRun{
			RunID:         iterRunID,
			WorkflowRunID: workflowRunID,
			TaskName:      iterName,
			Path:          iterName,
			TemplateName:  ld.BodyTempl.GetName(),
			Status:        model.PhasePending,
		}
		if _, err := ld.Store.BatchCreateTaskRuns(ctx, []*store.TaskRun{iterRun}); err != nil {
			return nil, fmt.Errorf("create iteration task run %s: %w", iterName, err)
		}

		mu.Lock()
		taskRunIDs = append(taskRunIDs, iterRunID)
		mu.Unlock()

		// Build inputs from iteration params
		var inputs *model.Inputs
		if len(iterParams) > 0 {
			params := make([]model.Parameter, 0, len(iterParams))
			for k, v := range iterParams {
				valJSON, _ := json.Marshal(v)
				params = append(params, model.Parameter{
					Name:  k,
					Value: valJSON,
				})
			}
			inputs = &model.Inputs{Parameters: params}
		}

		// Build assignment
		assignment := &broker.TaskAssignment{
			TaskRunID:     iterRunID,
			WorkflowRunID: workflowRunID,
			TaskName:      iterName,
			TemplateName:  ld.BodyTempl.GetName(),
			Priority:      ld.Workflow.Spec.Priority,
		}
		if exec := ld.BodyTempl.GetExecutor(); exec != nil {
			assignment.ExecutorType = exec.Type
			assignment.ExecutorConfig = exec.Config
		}
		if timeout := ld.BodyTempl.GetTimeout(); timeout != "" {
			assignment.Timeout = timeout
		}
		if inputs != nil {
			inputsJSON, _ := json.Marshal(inputs)
			assignment.Inputs = inputsJSON
		}
		if res := ld.BodyTempl.GetResources(); res != nil {
			resourcesJSON, _ := json.Marshal(res)
			assignment.Resources = resourcesJSON
		}

		// Dispatch with concurrency control
		wg.Add(1)
		sem <- struct{}{}
		go func(a *broker.TaskAssignment) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := ld.Broker.Dispatch(ctx, a); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(assignment)
	}

	wg.Wait()
	return taskRunIDs, firstErr
}
