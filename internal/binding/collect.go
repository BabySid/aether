package binding

import (
	"context"

	"github.com/BabySid/aether/expr"
	ivars "github.com/BabySid/aether/internal/vars"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// Collector assembles container-level outputs from child task run results.
//
// CollectDAGOutputs reads declared valueFrom references for a DAG container
// and resolves each one from an EvalVars built from the child task runs.
//
// Loop output aggregation is handled by internal.AggregateResults in loop.go.
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
	for k, v := range (&ivars.SiblingTaskRunsSource{Runs: children}).Vars() {
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

