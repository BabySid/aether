package internal

import (
	"encoding/json"

	"github.com/BabySid/aether/model"
)

// MergeParameters merges src into dst; src keys win on conflict.
// Returns a new slice; dst and src are not modified.
func MergeParameters(dst, src []model.Parameter) []model.Parameter {
	if len(src) == 0 {
		return dst
	}
	idx := make(map[string]int, len(dst))
	result := make([]model.Parameter, len(dst))
	copy(result, dst)
	for i, p := range result {
		idx[p.Name] = i
	}
	for _, p := range src {
		if i, found := idx[p.Name]; found {
			result[i] = p // src wins
		} else {
			idx[p.Name] = len(result)
			result = append(result, p)
		}
	}
	return result
}

// MergeInputsWithPayload creates a new *model.Inputs with existing parameters
// merged with payload. payload keys win on conflict (last-writer-wins).
func MergeInputsWithPayload(existing *model.Inputs, payload map[string]any) *model.Inputs {
	payloadParams := make([]model.Parameter, 0, len(payload))
	for k, v := range payload {
		valJSON, _ := json.Marshal(v)
		payloadParams = append(payloadParams, model.Parameter{Name: k, Value: valJSON})
	}
	var base []model.Parameter
	if existing != nil {
		base = existing.Parameters
	}
	return &model.Inputs{Parameters: MergeParameters(base, payloadParams)}
}

// MergeOutputsWithDecl filters and fills executor output parameters according to
// the template-declared outputs. The declared parameter list acts as a whitelist:
// only parameters explicitly declared in decl are included in the result.
//
// Rules:
//   - When decl is nil or declares no parameters → actual is returned unchanged
//     (no filtering; executor outputs are preserved as-is).
//   - Parameters declared in decl AND produced by the executor → executor value wins.
//   - Parameters declared in decl with empty/null executor value → filled by the
//     declared static value (if non-empty).
//   - Parameters produced by the executor but NOT declared in decl → excluded
//     (whitelist filtering enforces protocol semantics).
//   - Parameters declared in decl but absent from both executor output and the
//     declared static value → excluded from the result (nothing to emit).
//
// Returns a new *model.Outputs; actual is not modified.
func MergeOutputsWithDecl(actual *model.Outputs, decl *model.Outputs) *model.Outputs {
	if decl == nil || len(decl.Parameters) == 0 {
		return actual
	}
	if actual == nil {
		actual = &model.Outputs{}
	}

	// Build a lookup of actual (executor-produced) parameters.
	actualIdx := make(map[string]model.Parameter, len(actual.ExecOutputs.Parameters))
	for _, p := range actual.ExecOutputs.Parameters {
		actualIdx[p.Name] = p
	}

	// Iterate declared parameters as the whitelist; actual params not in decl are dropped.
	filtered := make([]model.Parameter, 0, len(decl.ExecOutputs.Parameters))
	for _, declP := range decl.ExecOutputs.Parameters {
		if actualP, found := actualIdx[declP.Name]; found {
			// Executor produced this param — use its value.
			// If executor value is empty/null, fall back to declared static value.
			if (len(actualP.Value) == 0 || string(actualP.Value) == "null") &&
				len(declP.Value) > 0 && string(declP.Value) != "null" {
				actualP.Value = declP.Value
			}
			filtered = append(filtered, actualP)
		} else {
			// Executor did not produce this param — use declared static value as fallback.
			// Skip if the declared value is also empty/null (nothing to emit).
			if len(declP.Value) == 0 || string(declP.Value) == "null" {
				continue
			}
			filtered = append(filtered, model.Parameter{
				Name:        declP.Name,
				Type:        declP.Type,
				Description: declP.Description,
				Value:       declP.Value,
			})
		}
	}

	result := *actual // shallow copy of the Outputs struct
	result.ExecOutputs.Parameters = filtered
	return &result
}
