// Package expr defines the expression evaluator abstraction for aether.
package expr

import "context"

// Evaluator evaluates expressions (when, repeatCondition, phaseConditions, valueFrom.expression).
type Evaluator interface {
	// Eval evaluates an expression with the given environment variables.
	Eval(ctx context.Context, expr string, env map[string]any) (any, error)
}
