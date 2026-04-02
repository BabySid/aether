package binding

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/BabySid/aether/errsink"
	"github.com/BabySid/aether/expr"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/secret"
)

// Binder merges template-declared inputs with call-site arguments and resolves
// all valueFrom references through an EvalVars.
//
// Binding priority (per parameter, independent):
//
//  1. arguments[name].Value (non-empty, non-null) — used directly
//  2. arguments[name].ValueFrom                   — resolved from env
//  3. decls[name].Default                         — fallback
//  4. (nothing)                                   — value stays empty
//
// Parameters that appear in arguments but not in decls are appended as-is
// (undeclared pass-through), enabling callers to inject extra values without
// modifying the template schema.
type Binder struct {
	eval        expr.Evaluator
	secretStore secret.Provider
	sink        errsink.ErrorSink
}

// NewBinder returns a Binder. eval, secretStore, and sink may be nil; they are
// only required when resolving valueFrom.expression, valueFrom.secretKeyRef,
// or reporting resolution warnings respectively.
func NewBinder(eval expr.Evaluator, secretStore secret.Provider, sink errsink.ErrorSink) *Binder {
	return &Binder{eval: eval, secretStore: secretStore, sink: sink}
}

// Bind returns a fully-bound copy of the declared inputs.
//
//   - decls     is the template's Inputs declaration (name/type/default).  May be nil.
//   - arguments is the call-site parameter overrides (value/valueFrom).      May be nil.
//   - env       is the current EvalVars snapshot used for valueFrom resolution.
func (b *Binder) Bind(
	ctx context.Context,
	decls *model.Inputs,
	arguments *model.Arguments,
	env EvalVars,
) (*model.Inputs, error) {
	// Index arguments by name for O(1) lookup
	argIndex := make(map[string]model.Parameter)
	if arguments != nil {
		for _, a := range arguments.Parameters {
			argIndex[a.Name] = a
		}
	}

	// Index decls by name so we can track which have been processed
	declIndex := make(map[string]model.Parameter)
	if decls != nil {
		for _, d := range decls.Parameters {
			declIndex[d.Name] = d
		}
	}

	result := &model.Inputs{}

	// Artifacts pass through unchanged (artifact binding is out of scope here)
	if decls != nil {
		result.Artifacts = decls.Artifacts
	}

	// Process declared parameters
	if decls != nil {
		for _, d := range decls.Parameters {
			bound, err := b.bindOne(ctx, d, argIndex[d.Name], env)
			if err != nil {
				return nil, fmt.Errorf("bind parameter %q: %w", d.Name, err)
			}
			result.Parameters = append(result.Parameters, bound)
		}
	}

	// Append undeclared arguments (pass-through)
	if arguments != nil {
		for _, a := range arguments.Parameters {
			if _, declared := declIndex[a.Name]; declared {
				continue // already processed above
			}
			bound, err := b.bindOne(ctx, model.Parameter{Name: a.Name}, a, env)
			if err != nil {
				return nil, fmt.Errorf("bind parameter %q: %w", a.Name, err)
			}
			result.Parameters = append(result.Parameters, bound)
		}
	}

	return result, nil
}

// bindOne resolves a single parameter value.
// decl is the template declaration (provides name/type/default).
// arg  is the call-site argument (provides value/valueFrom override).
func (b *Binder) bindOne(ctx context.Context, decl model.Parameter, arg model.Parameter, env EvalVars) (model.Parameter, error) {
	out := model.Parameter{
		Name:        decl.Name,
		Type:        decl.Type,
		Description: decl.Description,
		Enum:        decl.Enum,
		Default:     decl.Default,
	}

	// Priority 1: argument explicit value
	if len(arg.Value) > 0 && string(arg.Value) != "null" {
		out.Value = arg.Value
		return out, nil
	}

	// Priority 2: argument valueFrom
	if arg.ValueFrom != nil {
		val, err := b.resolveValueFrom(ctx, arg.ValueFrom, env)
		if err != nil {
			// Report the resolution failure to ErrorSink for observability.
			if b.sink != nil {
				b.sink.OnError(ctx, err, errsink.ErrorContext{
					Operation: "bind.resolveValueFrom." + decl.Name,
					Severity:  errsink.SeverityWarning,
				})
			}
			// Fall through to default on resolution failure.
			if len(decl.Default) > 0 {
				out.Value = decl.Default
				return out, nil
			}
			// Per design spec, unresolvable valueFrom → leave value empty.
			// Downstream validators or executors decide how to handle missing values.
			// We do not propagate the error here because a task with a missing input
			// should still be dispatched (e.g. playground/noop), and the executor can
			// report a failure if the value is truly required.
			return out, nil
		}
		out.Value = val
		return out, nil
	}

	// Priority 3: decl explicit value (rare: template-level default value)
	if len(decl.Value) > 0 && string(decl.Value) != "null" {
		out.Value = decl.Value
		return out, nil
	}

	// Priority 3.5: decl valueFrom — template-declared default resolution
	// (e.g. an input parameter whose default source is a workflow argument or
	// sibling task output, declared directly on the template input, not at call-site)
	if decl.ValueFrom != nil {
		val, err := b.resolveValueFrom(ctx, decl.ValueFrom, env)
		if err != nil {
			if b.sink != nil {
				b.sink.OnError(ctx, err, errsink.ErrorContext{
					Operation: "bind.resolveValueFrom." + decl.Name,
					Severity:  errsink.SeverityWarning,
				})
			}
			// Fall through to default on resolution failure.
			if len(decl.Default) > 0 {
				out.Value = decl.Default
				return out, nil
			}
			return out, nil
		}
		out.Value = val
		return out, nil
	}

	// Priority 4: decl default
	if len(decl.Default) > 0 {
		out.Value = decl.Default
	}

	return out, nil
}

// resolveValueFrom resolves a ValueFrom against the provided EvalVars.
//
// All path-style lookups go through the env map (populated by VarBuilder),
// so this function has no direct dependency on store.TaskRun or model.Arguments.
//
// Routing:
//
//	path         → env lookup (keys like "tasks.<n>.outputs.parameters.<p>")
//	parameter    → env lookup (keys like "workflow.parameters.<n>", "inputs.parameters.<n>",
//	               or "tasks.<n>.outputs.parameters.<p>" — determined by the key prefix present in env)
//	expression   → Interpolate(expr, env) then eval
//	secretKeyRef → secretStore.Get
func (b *Binder) resolveValueFrom(ctx context.Context, vf *model.ValueFrom, env EvalVars) (json.RawMessage, error) {
	// 1. path
	if vf.Path != "" {
		return lookupEnv(vf.Path, env)
	}

	// 2. parameter — supports multiple reference formats:
	//    "workflow.parameters.<name>"
	//    "workflow.arguments.parameters.<name>"   (legacy alias)
	//    "inputs.parameters.<name>"
	//    "tasks.<name>.outputs.parameters.<p>"
	if vf.Parameter != "" {
		key := normalizeParameterRef(vf.Parameter)
		return lookupEnv(key, env)
	}

	// 3. expression — interpolate {{...}} first, then evaluate
	if vf.Expression != "" {
		return b.resolveExpression(ctx, vf.Expression, env)
	}

	// 4. secretKeyRef
	if vf.SecretKeyRef != nil {
		return b.resolveSecret(ctx, vf.SecretKeyRef)
	}

	return nil, fmt.Errorf("valueFrom has no source configured")
}

// normalizeParameterRef converts legacy "workflow.arguments.parameters.<name>"
// references to the canonical "workflow.parameters.<name>" form, so both formats
// resolve via the same EvalVars key.
func normalizeParameterRef(ref string) string {
	const legacyPrefix = "workflow.arguments.parameters."
	const canonicalPrefix = "workflow.parameters."
	if strings.HasPrefix(ref, legacyPrefix) {
		return canonicalPrefix + ref[len(legacyPrefix):]
	}
	return ref
}

// lookupEnv fetches key from env and marshals it to JSON.
func lookupEnv(key string, env EvalVars) (json.RawMessage, error) {
	val, ok := env[key]
	if !ok {
		return nil, fmt.Errorf("key %q not found in eval env", key)
	}
	if val == nil {
		return json.RawMessage("null"), nil
	}
	raw, err := json.Marshal(val)
	if err != nil {
		return nil, fmt.Errorf("marshal env[%q]: %w", key, err)
	}
	return raw, nil
}

// resolveExpression interpolates {{...}} placeholders in expr, then evaluates it.
func (b *Binder) resolveExpression(ctx context.Context, expression string, env EvalVars) (json.RawMessage, error) {
	if b.eval == nil {
		return nil, fmt.Errorf("expression requires ExprEvaluator but none is configured")
	}

	// Interpolate template variables first
	interpolated := InterpolateString(expression, env)

	result, err := b.eval.Eval(ctx, interpolated, env)
	if err != nil {
		return nil, fmt.Errorf("eval expression %q: %w", expression, err)
	}
	return json.Marshal(result)
}

// resolveSecret fetches a secret value from the SecretStore.
func (b *Binder) resolveSecret(ctx context.Context, ref *model.SecretKeyRef) (json.RawMessage, error) {
	if b.secretStore == nil {
		return nil, fmt.Errorf("secretKeyRef requires SecretStore but none is configured")
	}
	val, err := b.secretStore.Get(ctx, ref.Name, ref.Key)
	if err != nil {
		return nil, fmt.Errorf("get secret %s/%s: %w", ref.Name, ref.Key, err)
	}
	return json.Marshal(val)
}
