// resolve.go — parameter value resolution
//
// Responsibility: before a task is dispatched, bind concrete values to every
// unresolved parameter in inputs.parameters and return a fully-bound Inputs copy.
//
// Resolution priority (resolveParameter):
//  1. Explicit value (non-empty and not JSON null) → used as-is, no resolution
//  2. valueFrom → resolved from one of four sources (see below)
//     - if valueFrom fails and a default exists, silently fall back to default
//     - if valueFrom fails and no default, return error
//  3. default → fallback
//  4. none of the above → value stays empty (validated downstream)
//
// Four valueFrom sources (resolveValueFrom):
//   - path         read from a completed task's outputs
//     format: tasks.<taskName>.outputs.parameters.<paramName>
//   - parameter    look up a workflow-level argument by name (WfArgs)
//   - expression   evaluate via ExprEvaluator; env is built by BuildTaskEnv
//   - secretKeyRef fetch from SecretStore by name+key
//
// Call site: engine_sched.go dispatchLeafTask calls ResolveInputs before
// handing a TaskAssignment to the broker.
package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/BabySid/aether/expr"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/secret"
	"github.com/BabySid/aether/store"
)

// ResolveContext holds the dependencies needed for parameter resolution.
type ResolveContext struct {
	Eval        expr.Evaluator
	SecretStore secret.Store
	TaskRuns    []*store.TaskRun
	WfArgs      *model.Arguments // workflow-level arguments
}

// ResolveInputs resolves all parameter values in the inputs,
// applying valueFrom and default value logic.
func ResolveInputs(ctx context.Context, inputs *model.Inputs, rc *ResolveContext) (*model.Inputs, error) {
	if inputs == nil {
		return nil, nil
	}

	resolved := &model.Inputs{
		Parameters: make([]model.Parameter, len(inputs.Parameters)),
		Artifacts:  inputs.Artifacts, // Artifacts pass through (resolved separately)
	}
	copy(resolved.Parameters, inputs.Parameters)

	for i := range resolved.Parameters {
		p := &resolved.Parameters[i]
		if err := resolveParameter(ctx, p, rc); err != nil {
			return nil, fmt.Errorf("resolve parameter %q: %w", p.Name, err)
		}
	}

	return resolved, nil
}

// resolveParameter resolves a single parameter's value.
// Priority: value (explicit) > valueFrom > default.
func resolveParameter(ctx context.Context, p *model.Parameter, rc *ResolveContext) error {
	// If value is already set (non-null, non-empty), keep it
	if len(p.Value) > 0 && string(p.Value) != "null" {
		return nil
	}

	// Try valueFrom
	if p.ValueFrom != nil {
		val, err := resolveValueFrom(ctx, p.ValueFrom, rc)
		if err != nil {
			// Fall through to default if valueFrom fails
			if len(p.Default) > 0 {
				p.Value = p.Default
				return nil
			}
			return err
		}
		p.Value = val
		return nil
	}

	// Use default if no value
	if len(p.Default) > 0 {
		p.Value = p.Default
	}

	return nil
}

// resolveValueFrom resolves a parameter value from a ValueFrom source.
func resolveValueFrom(ctx context.Context, vf *model.ValueFrom, rc *ResolveContext) (json.RawMessage, error) {
	// 1. path — resolve from task outputs by path
	if vf.Path != "" {
		return resolveFromPath(vf.Path, rc)
	}

	// 2. parameter — resolve from workflow arguments
	if vf.Parameter != "" {
		return resolveFromParameter(vf.Parameter, rc)
	}

	// 3. expression — evaluate via ExprEvaluator
	if vf.Expression != "" {
		return resolveFromExpression(ctx, vf.Expression, rc)
	}

	// 4. secretKeyRef — resolve from SecretStore
	if vf.SecretKeyRef != nil {
		return resolveFromSecret(ctx, vf.SecretKeyRef, rc)
	}

	return nil, fmt.Errorf("valueFrom has no source configured")
}

// resolveFromPath resolves a value from task output path.
// Path format: "tasks.<taskName>.outputs.parameters.<paramName>"
func resolveFromPath(path string, rc *ResolveContext) (json.RawMessage, error) {
	parts := strings.Split(path, ".")
	if len(parts) < 5 || parts[0] != "tasks" {
		return nil, fmt.Errorf("invalid path: %q (expected tasks.<name>.outputs.parameters.<param>)", path)
	}

	taskName := parts[1]
	// parts[2] = "outputs"
	// parts[3] = "parameters"
	paramName := parts[4]

	for _, tr := range rc.TaskRuns {
		if tr.TaskName == taskName && tr.Outputs != nil {
			for _, p := range tr.Outputs.Parameters {
				if p.Name == paramName {
					return p.Value, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("path %q: task %q output parameter %q not found", path, taskName, paramName)
}

// resolveFromParameter resolves a value from workflow-level arguments.
//
// The name may be a bare parameter name (e.g. "username") or the full
// dotted path used in valueFrom.parameter references:
//
//	"workflow.arguments.parameters.username"
//
// Both forms are accepted; the prefix is stripped before lookup.
func resolveFromParameter(name string, rc *ResolveContext) (json.RawMessage, error) {
	const prefix = "workflow.arguments.parameters."
	bare := name
	if strings.HasPrefix(name, prefix) {
		bare = name[len(prefix):]
	}

	if rc.WfArgs != nil {
		for _, p := range rc.WfArgs.Parameters {
			if p.Name == bare {
				if len(p.Value) > 0 {
					return p.Value, nil
				}
				if len(p.Default) > 0 {
					return p.Default, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("workflow argument %q not found", name)
}

// resolveFromExpression evaluates an expression to resolve a value.
//
// The expression can reference sibling task outputs via dot-notation variables.
// For example, given a completed task "score-task" that output {"result": 92}:
//
//	expression: `tasks.score-task.outputs.parameters.result > 80 ? "pass" : "fail"`
//
// Because expr.Evaluator is stateless, we must first build an env map that
// exposes the current-scope TaskRuns as flat variables:
//
//	BuildTaskEnv(rc.TaskRuns) →
//	  "tasks.score-task.phase"                          = "Succeeded"
//	  "tasks.score-task.outputs.parameters.result"      = 92
//
// The evaluator then resolves the expression against that env.
func resolveFromExpression(ctx context.Context, expression string, rc *ResolveContext) (json.RawMessage, error) {
	if rc.Eval == nil {
		return nil, fmt.Errorf("expression requires ExprEvaluator but none is configured")
	}

	env := BuildTaskEnv(rc.TaskRuns)
	result, err := rc.Eval.Eval(ctx, expression, env)
	if err != nil {
		return nil, fmt.Errorf("eval expression %q: %w", expression, err)
	}

	return json.Marshal(result)
}

// resolveFromSecret resolves a value from the SecretStore.
func resolveFromSecret(ctx context.Context, ref *model.SecretKeyRef, rc *ResolveContext) (json.RawMessage, error) {
	if rc.SecretStore == nil {
		return nil, fmt.Errorf("secretKeyRef requires SecretStore but none is configured")
	}

	val, err := rc.SecretStore.Get(ctx, ref.Name, ref.Key)
	if err != nil {
		return nil, fmt.Errorf("get secret %s/%s: %w", ref.Name, ref.Key, err)
	}

	return json.Marshal(val)
}
