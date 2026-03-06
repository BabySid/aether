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
func resolveFromParameter(name string, rc *ResolveContext) (json.RawMessage, error) {
	if rc.WfArgs != nil {
		for _, p := range rc.WfArgs.Parameters {
			if p.Name == name {
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
