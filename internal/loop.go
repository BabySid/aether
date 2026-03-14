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
// single (phase, message, outputs) triple, according to the loop's aggregate strategy.
//
// Strategies:
//
//	"all"           (default) — all iterations must succeed; outputs are merged with
//	                             suffix "_<index>" to avoid name collisions.
//	"first_success"           — succeed as soon as any one iteration succeeds; returns
//	                             that iteration's outputs directly.
//	"quorum"                  — succeed if strictly more than half of iterations succeed.
//
// Example (3-item loop, strategy "all"):
//
//	results = [{Index:0, Phase:Succeeded, Outputs:{status_0:"ok"}},
//	           {Index:1, Phase:Succeeded, Outputs:{status_1:"ok"}},
//	           {Index:2, Phase:Failed,    Message:"timeout"}]
//
//	→ (Failed, "iteration 2: timeout", nil)
//
// Example (3-item loop, all succeeded, strategy "all"):
//
//	results = [{Index:0, Phase:Succeeded, Outputs:{Parameters:[{status,"ok"}]}}, ...]
//
//	→ (Succeeded, "", &Outputs{Parameters:[{status_0,"ok"},{status_1,"ok"},{status_2,"ok"}]})
//
// Called by advanceScope after allTerminal=true and tryAdvanceRepeatLoop returns false,
// to compute the final phase/outputs of the Loop container TaskRun.
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

// aggregateAll requires every iteration to be in a terminal-success state (Succeeded
// or Skipped). The first non-success iteration short-circuits with its phase/message.
// Successful iteration outputs are merged with a "_<index>" suffix on each parameter name.
//
// Example:
//
//	results = [{Index:0, Phase:Succeeded, Outputs:{Parameters:[{Name:"result", Value:"A"}]}},
//	           {Index:1, Phase:Succeeded, Outputs:{Parameters:[{Name:"result", Value:"B"}]}}]
//
//	→ (Succeeded, "", &Outputs{Parameters:[{result_0,"A"}, {result_1,"B"}]})
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

// aggregateFirstSuccess returns success the moment it finds the first iteration with
// phase=Succeeded, forwarding that iteration's Outputs directly.
// If no iteration succeeded the loop itself fails.
//
// Typical use case: fan-out search — spawn N workers in parallel, accept the first
// winner, ignore the rest.
//
// Example:
//
//	results = [{Index:0, Phase:Failed},
//	           {Index:1, Phase:Succeeded, Outputs:{Parameters:[{Name:"url", Value:"https://..."}]}},
//	           {Index:2, Phase:Succeeded, ...}]
//
//	→ (Succeeded, "", &Outputs{Parameters:[{url,"https://..."}]})  ← first success wins
func aggregateFirstSuccess(results []LoopIterationResult) (model.Phase, string, *model.Outputs) {
	for _, r := range results {
		if r.Phase == model.PhaseSucceeded {
			return model.PhaseSucceeded, "", r.Outputs
		}
	}
	return model.PhaseFailed, "no iteration succeeded", nil
}

// aggregateQuorum succeeds if strictly more than half of the iterations succeeded.
// No outputs are merged; only the phase/message are determined.
//
// Example (5 iterations, 3 succeed):
//
//	succeeded=3, total=5 → 3 > 5/2=2 → (Succeeded, "", nil)
//
// Example (5 iterations, 2 succeed):
//
//	succeeded=2, total=5 → 2 > 5/2=2 is false → (Failed, "quorum not met: 2/5 succeeded", nil)
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

// BuildRepeatEnv constructs the expression evaluation environment used by
// EvalRepeatCondition to decide whether a repeatCondition loop should continue.
//
// The environment exposes the just-finished iteration's result so that the condition
// expression can reference task phase, exit code, message, and output parameters.
//
// Available keys:
//
//	iteration_index                                → 0-based index of the finished iteration
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
//	    "iteration_index":                          2,
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
		"iteration_index": iterIndex,
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
//	env       = {"tasks.fetch.phase": "Failed", "iteration_index": 0}
//
//	→ true  (loop continues, spawnRepeatIteration is called for index 1)
//
//	condition = "tasks.fetch.phase != 'Succeeded'"
//	env       = {"tasks.fetch.phase": "Succeeded", "iteration_index": 1}
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
