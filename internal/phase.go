package internal

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/errsink"
	"github.com/BabySid/aether/expr"
	"github.com/BabySid/aether/model"
)

// CodeToPhase maps an ExecOutputs.Code to its canonical Phase.
// This is the single authoritative Code→Phase mapping in the engine layer.
//
//	ExecCodeSucceeded → PhaseSucceeded
//	ExecCodeSuspended → PhaseSuspended (await pattern)
//	ExecCodeFailed    → PhaseFailed    (business failure)
//	ExecCodeError     → PhaseError     (system/framework-level error)
//	ExecCodeTimeout   → PhaseTimeout   (task exceeded its deadline)
//
// Any unrecognised code is treated as PhaseError (fail-safe).
func CodeToPhase(code int) model.Phase {
	switch code {
	case model.ExecCodeSucceeded:
		return model.PhaseSucceeded
	case model.ExecCodeSuspended:
		return model.PhaseSuspended
	case model.ExecCodeFailed:
		return model.PhaseFailed
	case model.ExecCodeError:
		return model.PhaseError
	case model.ExecCodeTimeout:
		return model.PhaseTimeout
	default:
		return model.PhaseError
	}
}

// EvalErrorContext carries the ErrorSink and identifiers needed to report
// expression evaluation failures. This avoids bloating function signatures
// with individual sink/workflowRunID/taskRunID parameters.
type EvalErrorContext struct {
	Sink          errsink.ErrorSink
	WorkflowRunID string
	TaskRunID     string
}

// EvalPhaseConditions evaluates phaseConditions to determine the final task phase.
// If phaseConditions is nil, the phase derived from result.ExecOutputs.Code is returned as-is.
//
// PhaseConditions allows users to override the task phase based on custom expressions.
// For example, a task that "fails" at the executor level might be considered "succeeded"
// based on output analysis.
func EvalPhaseConditions(
	ctx context.Context,
	conditions *model.PhaseConditions,
	eval expr.Evaluator,
	result *broker.TaskResult,
	errCtx *EvalErrorContext,
) model.Phase {
	// Derive the base phase from Code (single source of truth).
	var code int
	var msg string
	if result.ExecOutputs != nil {
		code = result.ExecOutputs.Code
		msg = result.ExecOutputs.Message
	}
	basePhase := CodeToPhase(code)

	if conditions == nil || eval == nil {
		return basePhase
	}

	// Build evaluation environment from result.
	env := map[string]any{
		"phase": string(basePhase),
		"code":  code,
		"msg":   msg,
	}
	if result.ExecOutputs != nil {
		for _, p := range result.ExecOutputs.Parameters {
			env["outputs.parameters."+p.Name] = unmarshalParam(p.Value)
		}
	}

	// Evaluate conditions in priority order: succeeded > failed > error
	if conditions.Succeeded != "" {
		if evalBool(ctx, eval, conditions.Succeeded, env, errCtx) {
			return model.PhaseSucceeded
		}
	}
	if conditions.Failed != "" {
		if evalBool(ctx, eval, conditions.Failed, env, errCtx) {
			return model.PhaseFailed
		}
	}
	if conditions.Error != "" {
		if evalBool(ctx, eval, conditions.Error, env, errCtx) {
			return model.PhaseError
		}
	}

	// No condition matched — return base phase derived from Code.
	return basePhase
}

// unmarshalParam decodes a json.RawMessage into its native Go type
// (float64 for numbers, string for strings, bool for booleans, etc.)
// so that expression evaluation operates on real types instead of raw strings.
func unmarshalParam(raw json.RawMessage) any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	return v
}

// evalBool evaluates an expression and returns true if the result is truthy.
// Evaluation errors are reported to the ErrorSink (if provided) and treated as false.
func evalBool(ctx context.Context, eval expr.Evaluator, expression string, env map[string]any, errCtx *EvalErrorContext) bool {
	result, err := eval.Eval(ctx, expression, env)
	if err != nil {
		if errCtx != nil && errCtx.Sink != nil {
			errCtx.Sink.OnError(ctx, err, errsink.ErrorContext{
				WorkflowRunID: errCtx.WorkflowRunID,
				TaskRunID:     errCtx.TaskRunID,
				Operation:     "evalPhaseConditions",
				Severity:      errsink.SeverityWarning,
			})
		}
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
