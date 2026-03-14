package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/BabySid/aether/expr"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// ExpandLoopIterations expands a loop definition into a list of iteration items.
// Each item is a map[string]any representing one iteration's parameters that will
// be pre-stored in TaskRun.Inputs by startLoopController and forwarded verbatim to
// the broker by dispatchLeafTask — no further expression resolution is needed.
//
// Supports three modes:
//
//   - items:           explicit list (scalar or object); expanded immediately.
//   - itemsFrom:       expression evaluated at run time; result must be a JSON array.
//   - repeatCondition: condition-based loop; returns nil (engine drives iteration
//     one-by-one via tryAdvanceRepeatLoop, not pre-expanded).
//
// Example
//
//	loop "run-loop": items=["report_jan.csv","report_feb.csv","report_mar.csv"]
//
//	ExpandLoopIterations returns:
//	  [{item:"report_jan.csv", item_index:0},
//	   {item:"report_feb.csv", item_index:1},
//	   {item:"report_mar.csv", item_index:2}]
//
// Called by startLoopController when a Loop container is first activated, before
// any child TaskRun is created.
//
// Return values:
//
//	([]map, nil)  — iteration list; may be empty if maxIterations=0 and items=[].
//	(nil,   nil)  — repeatCondition mode; caller handles iteration lifecycle.
//	(nil,   err)  — itemsFrom expression evaluation or JSON parsing failed.
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
//
// Mapping rules:
//   - Object item  (map[string]any): all key/value pairs are flattened into params.
//   - Scalar item  (string/int/…):   stored under the reserved key "item".
//   - item_index is always injected (0-based).
//
// Example:
//
//	items = ["a.txt", {bucket:"s3://b", key:"c.csv"}]
//
//	Iteration 0 → {loop_iter.item:"a.txt",  loop_iter.index:0}
//	Iteration 1 → {bucket:"s3://b", key:"c.csv", loop_iter.index:1}
//
// maxIterations > 0 caps the expansion; 0 means unlimited.
//
// System-reserved keys use the "loop_iter." prefix so they can never collide with
// user-defined object fields, which are restricted to [a-zA-Z0-9_-] by validate.go.
func expandItems(items []any, maxIterations int) []map[string]any {
	result := make([]map[string]any, 0, len(items))
	for i, item := range items {
		if maxIterations > 0 && i >= maxIterations {
			break
		}
		params := make(map[string]any)
		switch v := item.(type) {
		case map[string]any:
			// Object items are flattened into the params map
			maps.Copy(params, v)
		default:
			// Scalar items use the reserved "loop_iter.item" key
			params["loop_iter.item"] = v
		}
		params["loop_iter.index"] = i
		result = append(result, params)
	}
	return result
}

// expandItemsFrom evaluates an expression to obtain a dynamic item list and
// then delegates to the same per-item mapping logic used by expandItems.
//
// The expression is evaluated against env (which contains workflow arguments and
// previously completed task outputs) and must resolve to one of:
//   - []any  — direct Go slice from the evaluator
//   - string         — a JSON-encoded array (e.g. output of a preceding task)
//
// Example (itemsFrom loop):
//
//	loop "process-results":
//	  itemsFrom: "tasks.list-files.outputs.parameters.files"
//	  body: "process-file"
//
//	env["tasks.list-files.outputs.parameters.files"] = `["a.txt","b.txt","c.txt"]`
//
//	expandItemsFrom evaluates the expression → JSON string
//	  → json.Unmarshal → []any{"a.txt","b.txt","c.txt"}
//	  → [{loop_iter.item:"a.txt", loop_iter.index:0}, {loop_iter.item:"b.txt", loop_iter.index:1}, ...]
//
// Called exclusively by ExpandLoopIterations when loop.ItemsFrom != "".
// Returns an error if eval is nil, the expression fails, or the result is not an array.
func expandItemsFrom(ctx context.Context, expression string, eval expr.Evaluator, env map[string]any, maxIterations int) ([]map[string]any, error) {
	if eval == nil {
		return nil, fmt.Errorf("itemsFrom requires an ExprEvaluator but none is configured")
	}

	raw, err := eval.Eval(ctx, expression, env)
	if err != nil {
		return nil, fmt.Errorf("eval itemsFrom %q: %w", expression, err)
	}

	// Try to convert the result to a list
	var items []any
	switch v := raw.(type) {
	case []any:
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
		case map[string]any:
			maps.Copy(params, v)
		default:
			params["loop_iter.item"] = v
		}
		params["loop_iter.index"] = i
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

// AggregateResults aggregates the terminal results of all loop iterations into a
// single (phase, message, outputs) triple according to the aggregate strategy.
//
// All strategies first verify that every iteration succeeded (or was skipped);
// the first non-success iteration short-circuits with its phase/message.
//
// Strategies (default: "last"):
//
//	"last"  — use the last iteration's outputs as-is.
//	"first" — use the first iteration's outputs as-is.
//	"list"  — collect each declared parameter across all iterations into a JSON array.
//
// The Aggregate.Parameters field lists which output parameter names to include.
// If Parameters is empty, all output parameters are included.
//
// Example loop: 3 iterations, body outputs {status, code}.
//
//	strategy:"last",  parameters:[]         → {status:"ok",  code:0}     (iter[2] output)
//	strategy:"first", parameters:["status"] → {status:"ok"}              (iter[0].status only)
//	strategy:"list",  parameters:["status"] → {status:["ok","fail","ok"]} (all iters, index-ordered)
//	strategy:"list",  parameters:[]         → {status:["ok","fail","ok"], code:[0,1,0]}
//
// Called by advanceScope after allTerminal=true and tryAdvanceRepeatLoop returns false.
func AggregateResults(results []LoopIterationResult, aggregate *model.Aggregate) (model.Phase, string, *model.Outputs) {
	if len(results) == 0 {
		return model.PhaseSucceeded, "", nil
	}

	// All strategies require every iteration to succeed first.
	for _, r := range results {
		if r.Phase != model.PhaseSucceeded && r.Phase != model.PhaseSkipped {
			return r.Phase, fmt.Sprintf("iteration %d: %s", r.Index, r.Message), nil
		}
	}

	strategy := model.AggregateStrategyLast
	if aggregate != nil && aggregate.Strategy != "" {
		strategy = aggregate.Strategy
	}
	var paramFilter map[string]bool
	if aggregate != nil && len(aggregate.Parameters) > 0 {
		paramFilter = make(map[string]bool, len(aggregate.Parameters))
		for _, p := range aggregate.Parameters {
			paramFilter[p] = true
		}
	}

	switch strategy {
	case model.AggregateStrategyFirst:
		return aggregatePickOne(results, 0, paramFilter)
	case model.AggregateStrategyList:
		return aggregateList(results, paramFilter)
	default: // AggregateStrategyLast
		return aggregatePickOne(results, len(results)-1, paramFilter)
	}
}

// aggregatePickOne returns the outputs of the iteration at position idx (0=first, N-1=last),
// filtered by paramFilter (nil means include all parameters).
func aggregatePickOne(results []LoopIterationResult, idx int, paramFilter map[string]bool) (model.Phase, string, *model.Outputs) {
	r := results[idx]
	if r.Outputs == nil || len(r.Outputs.Parameters) == 0 {
		return model.PhaseSucceeded, "", nil
	}
	params := filterParams(r.Outputs.Parameters, paramFilter)
	if len(params) == 0 {
		return model.PhaseSucceeded, "", nil
	}
	return model.PhaseSucceeded, "", &model.Outputs{Phase: model.PhaseSucceeded, Parameters: params}
}

// aggregateList collects the values of each parameter across all iterations into
// an ordered JSON array (by iteration index). Missing values are padded with null
// so that all arrays have equal length (len(results)), enabling index-based alignment.
//
// Only parameters in paramFilter are included; if paramFilter is nil, all parameters
// found in any iteration are collected.
//
// Example (3 iterations, paramFilter=nil, iter[1] has no "code"):
//
//	iter[0]: {status:"ok",   code:0}
//	iter[1]: {status:"succ"}
//	iter[2]: {status:"ok",   code:2}
//
//	→ {status:["ok","succ","ok"], code:[0,null,2]}
//
// All arrays are guaranteed to have length == len(results).
func aggregateList(results []LoopIterationResult, paramFilter map[string]bool) (model.Phase, string, *model.Outputs) {
	// Pass 1: discover all parameter names in insertion order.
	order := make([]string, 0)
	seen := make(map[string]bool)
	for _, r := range results {
		if r.Outputs == nil {
			continue
		}
		for _, p := range r.Outputs.Parameters {
			if paramFilter != nil && !paramFilter[p.Name] {
				continue
			}
			if !seen[p.Name] {
				seen[p.Name] = true
				order = append(order, p.Name)
			}
		}
	}

	if len(order) == 0 {
		return model.PhaseSucceeded, "", nil
	}

	// Pass 2: for each iteration and each known parameter, collect value or null.
	collected := make(map[string][]json.RawMessage, len(order))
	for _, name := range order {
		collected[name] = make([]json.RawMessage, len(results))
		for i := range collected[name] {
			collected[name][i] = json.RawMessage("null")
		}
	}
	for i, r := range results {
		if r.Outputs == nil {
			continue
		}
		for _, p := range r.Outputs.Parameters {
			if _, ok := collected[p.Name]; ok {
				collected[p.Name][i] = p.Value
			}
		}
	}

	params := make([]model.Parameter, 0, len(order))
	for _, name := range order {
		arrJSON, err := json.Marshal(collected[name])
		if err != nil {
			return model.PhaseFailed, fmt.Sprintf("aggregate list: marshal %q: %v", name, err), nil
		}
		params = append(params, model.Parameter{Name: name, Value: arrJSON})
	}
	return model.PhaseSucceeded, "", &model.Outputs{Phase: model.PhaseSucceeded, Parameters: params}
}

// filterParams returns only the parameters whose names are in paramFilter.
// If paramFilter is nil, all parameters are returned unchanged.
func filterParams(params []model.Parameter, paramFilter map[string]bool) []model.Parameter {
	if paramFilter == nil {
		return params
	}
	out := make([]model.Parameter, 0, len(params))
	for _, p := range params {
		if paramFilter[p.Name] {
			out = append(out, p)
		}
	}
	return out
}

// BuildRepeatEnv constructs the expression evaluation environment used by
// EvalRepeatCondition to decide whether a repeatCondition loop should continue.
//
// The environment exposes the just-finished iteration's result so that the condition
// expression can reference task phase, exit code, message, and output parameters.
//
// Available keys:
//
//	loop_iter.index                                → 0-based index of the finished iteration
//	tasks.<bodyName>.phase                         → phase string (e.g. "Succeeded", "Failed")
//	tasks.<bodyName>.code                          → executor exit code (if Outputs != nil)
//	tasks.<bodyName>.msg                           → executor message (if Outputs != nil)
//	tasks.<bodyName>.outputs.parameters.<name>     → deserialized output parameter value
//
// Example (repeatCondition loop, iteration 2 just finished):
//
//	loop "retry-fetch": body="fetch", repeatCondition="tasks.fetch.phase != 'Succeeded'"
//
//	lastRun = {TaskName:"fetch", Status:"Failed",
//	           Outputs:{Code:1, Msg:"timeout", Parameters:[{status,"pending"}]}}
//
//	BuildRepeatEnv(2, lastRun) →
//	  {
//	    "loop_iter.index":                          2,
//	    "tasks.fetch.phase":                        "Failed",
//	    "tasks.fetch.code":                         1,
//	    "tasks.fetch.msg":                          "timeout",
//	    "tasks.fetch.outputs.parameters.status":    "pending",
//	  }
//
// Called by tryAdvanceRepeatLoop immediately before EvalRepeatCondition.
// iterIndex is the 0-based index of the iteration that just completed (nextIndex - 1).
func BuildRepeatEnv(iterIndex int, lastRun *store.TaskRun) map[string]any {
	env := map[string]any{
		"loop_iter.index": iterIndex,
	}
	if lastRun == nil {
		return env
	}
	prefix := "tasks." + lastRun.TaskName
	env[prefix+".phase"] = string(lastRun.Status)
	if lastRun.Outputs != nil {
		env[prefix+".code"] = lastRun.Outputs.Code
		env[prefix+".msg"] = lastRun.Outputs.Msg
		for _, p := range lastRun.Outputs.Parameters {
			var val any
			if err := json.Unmarshal(p.Value, &val); err == nil {
				env[prefix+".outputs.parameters."+p.Name] = val
			} else {
				env[prefix+".outputs.parameters."+p.Name] = string(p.Value)
			}
		}
	}
	return env
}

// EvalRepeatCondition evaluates the repeatCondition expression against env and
// returns true if the loop should spawn another iteration.
//
// The condition is a boolean expression string supported by the configured
// expr.Evaluator (e.g. simple/cel). The evaluator may return a bool or a
// string "true"/"false"; any other type is an error.
//
// Example:
//
//	condition = "tasks.fetch.phase != 'Succeeded'"
//	env       = {"tasks.fetch.phase": "Failed", "loop_iter.index": 0}
//
//	→ true  (loop continues, spawnRepeatIteration is called for index 1)
//
//	condition = "tasks.fetch.phase != 'Succeeded'"
//	env       = {"tasks.fetch.phase": "Succeeded", "loop_iter.index": 1}
//
//	→ false (loop is done, advanceScope aggregates results and finalizes the container)
//
// Special cases:
//   - condition == ""  → false (no condition configured, stop immediately)
//   - eval == nil      → false (no evaluator, conservative stop)
//
// Called by tryAdvanceRepeatLoop after all current children reach a terminal state.
func EvalRepeatCondition(ctx context.Context, condition string, eval expr.Evaluator, env map[string]any) (bool, error) {
	if condition == "" {
		return false, nil
	}
	if eval == nil {
		return false, nil
	}
	result, err := eval.Eval(ctx, condition, env)
	if err != nil {
		return false, fmt.Errorf("eval repeatCondition %q: %w", condition, err)
	}
	switch v := result.(type) {
	case bool:
		return v, nil
	case string:
		return v == "true", nil
	default:
		return false, fmt.Errorf("repeatCondition %q returned non-boolean: %v", condition, result)
	}
}
