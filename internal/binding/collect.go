package binding

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BabySid/aether/expr"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
	"github.com/BabySid/aether/vars"
)

// Collector assembles container-level outputs from child task run results.
//
// Two strategies are supported:
//   - CollectDAGOutputs: reads declared valueFrom references for a DAG container
//     and resolves each one from an EvalVars built from the child task runs.
//   - CollectLoopOutputs: delegates to the loop aggregation strategy (last/first/list).
type Collector struct {
	eval expr.Evaluator
}

// NewCollector returns a Collector backed by the given expression evaluator.
// eval may be nil if none of the DAG output declarations use valueFrom.expression.
func NewCollector(eval expr.Evaluator) *Collector {
	return &Collector{eval: eval}
}

// CollectDAGOutputs resolves each output parameter declared in decls by looking
// up its valueFrom in an EvalVars built from children.
//
// A declared output parameter must have a valueFrom that points to a child task:
//
//	valueFrom.parameter: "tasks.<name>.outputs.parameters.<param>"
//
// Parameters that cannot be resolved are skipped (value left empty) rather than
// failing the entire collection, because partial outputs are better than none.
func (c *Collector) CollectDAGOutputs(
	ctx context.Context,
	decls *model.Outputs,
	children []*store.TaskRun,
	env EvalVars,
) (*model.Outputs, error) {
	if decls == nil || len(decls.Parameters) == 0 {
		return nil, nil
	}

	// Augment env with children if caller didn't already include them
	// (safe to re-add; SiblingTaskRunsProvider just overwrites same keys)
	resolvedEnv := make(EvalVars, len(env))
	for k, v := range env {
		resolvedEnv[k] = v
	}
	for k, v := range (&vars.SiblingTaskRunsSource{Runs: children}).Vars() {
		resolvedEnv[k] = v
	}

	binder := NewBinder(c.eval, nil)

	out := &model.Outputs{}
	for _, decl := range decls.Parameters {
		p := model.Parameter{Name: decl.Name, Type: decl.Type}
		if decl.ValueFrom != nil {
			raw, err := binder.resolveValueFrom(ctx, decl.ValueFrom, resolvedEnv)
			if err != nil {
				// Best-effort: skip unresolvable outputs rather than failing
				continue
			}
			p.Value = raw
		} else if len(decl.Value) > 0 {
			p.Value = decl.Value
		}
		out.Parameters = append(out.Parameters, p)
	}

	if len(out.Parameters) == 0 {
		return nil, nil
	}
	return out, nil
}

// IterationResult is the result of a single loop iteration.
// It is an alias for internal.LoopIterationResult so callers can use either package.
type IterationResult struct {
	Index   int
	Phase   model.Phase
	Message string
	Outputs *model.Outputs
}

// CollectLoopOutputs aggregates loop iteration outputs according to the aggregate strategy.
//
// Strategies:
//
//	"last"  (default) — use the last iteration's outputs as-is
//	"first"           — use the first iteration's outputs as-is
//	"list"            — each declared parameter becomes a JSON array across all iterations
//
// If any iteration is non-Succeeded/non-Skipped, the loop is considered failed and the
// first failing iteration's phase and message are returned.
func (c *Collector) CollectLoopOutputs(
	results []IterationResult,
	aggregate *model.Aggregate,
) (model.Phase, string, *model.Outputs) {
	if len(results) == 0 {
		return model.PhaseSucceeded, "", nil
	}

	// All strategies require every iteration to have succeeded or been skipped.
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
		for _, name := range aggregate.Parameters {
			paramFilter[name] = true
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

// aggregatePickOne returns the outputs of the iteration at idx, filtered by paramFilter.
func aggregatePickOne(results []IterationResult, idx int, paramFilter map[string]bool) (model.Phase, string, *model.Outputs) {
	r := results[idx]
	if r.Outputs == nil || len(r.Outputs.Parameters) == 0 {
		return model.PhaseSucceeded, "", r.Outputs
	}
	if len(paramFilter) == 0 {
		return model.PhaseSucceeded, "", r.Outputs
	}
	// Filter to requested parameters
	out := &model.Outputs{}
	for _, p := range r.Outputs.Parameters {
		if paramFilter[p.Name] {
			out.Parameters = append(out.Parameters, p)
		}
	}
	return model.PhaseSucceeded, "", out
}

// aggregateList collects each parameter across all iterations into a JSON array.
func aggregateList(results []IterationResult, paramFilter map[string]bool) (model.Phase, string, *model.Outputs) {
	// Gather all parameter names (preserve first-seen order)
	seen := make(map[string]bool)
	var paramNames []string
	for _, r := range results {
		if r.Outputs == nil {
			continue
		}
		for _, p := range r.Outputs.Parameters {
			if len(paramFilter) > 0 && !paramFilter[p.Name] {
				continue
			}
			if !seen[p.Name] {
				seen[p.Name] = true
				paramNames = append(paramNames, p.Name)
			}
		}
	}

	if len(paramNames) == 0 {
		return model.PhaseSucceeded, "", nil
	}

	// Build per-parameter value lists
	lists := make(map[string][]any, len(paramNames))
	for _, r := range results {
		byName := make(map[string]model.Parameter)
		if r.Outputs != nil {
			for _, p := range r.Outputs.Parameters {
				byName[p.Name] = p
			}
		}
		for _, name := range paramNames {
			if p, ok := byName[name]; ok {
				lists[name] = append(lists[name], unmarshalAny(p.Value))
			} else {
				lists[name] = append(lists[name], nil)
			}
		}
	}

	out := &model.Outputs{}
	for _, name := range paramNames {
		raw, err := marshalJSON(lists[name])
		if err != nil {
			continue
		}
		out.Parameters = append(out.Parameters, model.Parameter{Name: name, Value: raw})
	}

	return model.PhaseSucceeded, "", out
}

// marshalJSON marshals v to a JSON RawMessage byte slice.
func marshalJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}
