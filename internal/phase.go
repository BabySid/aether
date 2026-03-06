package internal

import (
	"context"
	"fmt"

	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/expr"
	"github.com/BabySid/aether/model"
)

// EvalPhaseConditions evaluates phaseConditions to determine the final task phase.
// If phaseConditions is nil, the result phase is returned as-is.
//
// PhaseConditions allows users to override the task phase based on custom expressions.
// For example, a task that "fails" at the executor level might be considered "succeeded"
// based on output analysis.
func EvalPhaseConditions(
	ctx context.Context,
	conditions *model.PhaseConditions,
	eval expr.Evaluator,
	result *executor.ExecuteResult,
) model.Phase {
	if conditions == nil || eval == nil {
		return result.Phase
	}

	// Build environment from result
	env := map[string]any{
		"phase": string(result.Phase),
		"code":  result.Code,
		"msg":   result.Msg,
	}
	if result.Outputs != nil {
		for _, p := range result.Outputs.Parameters {
			env["outputs.parameters."+p.Name] = string(p.Value)
		}
	}

	// Evaluate conditions in priority order: succeeded > failed > error
	if conditions.Succeeded != "" {
		if evalBool(ctx, eval, conditions.Succeeded, env) {
			return model.PhaseSucceeded
		}
	}
	if conditions.Failed != "" {
		if evalBool(ctx, eval, conditions.Failed, env) {
			return model.PhaseFailed
		}
	}
	if conditions.Error != "" {
		if evalBool(ctx, eval, conditions.Error, env) {
			return model.PhaseError
		}
	}

	// No condition matched — return original phase
	return result.Phase
}

// evalBool evaluates an expression and returns true if the result is truthy.
func evalBool(ctx context.Context, eval expr.Evaluator, expression string, env map[string]any) bool {
	result, err := eval.Eval(ctx, expression, env)
	if err != nil {
		return false
	}
	switch v := result.(type) {
	case bool:
		return v
	case string:
		return v == "true"
	default:
		return fmt.Sprintf("%v", v) == "true"
	}
}
