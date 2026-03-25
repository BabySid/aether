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

// MergeOutputsWithDecl merges template-declared output parameter values into
// actual executor outputs. The actual executor value always wins; the declared
// value is used only as a fallback when the executor left the parameter absent
// or with an empty/null value.
//
// Rules:
//   - Parameters present in actual with a non-empty value → kept as-is.
//   - Parameters present in actual with an empty/null value → filled by decl value (if non-empty).
//   - Parameters absent in actual → appended from decl (preserving Name/Type/Description/Value).
//   - Declared parameters with an empty or null value are skipped entirely.
//
// Returns a new *model.Outputs; actual is not modified.
// If decl is nil or has no parameters with values, actual is returned unchanged.
func MergeOutputsWithDecl(actual *model.Outputs, decl *model.Outputs) *model.Outputs {
	if decl == nil || len(decl.Parameters) == 0 {
		return actual
	}
	if actual == nil {
		actual = &model.Outputs{}
	}
	// Work on a shallow copy of the Parameters slice so we don't mutate the original.
	merged := make([]model.Parameter, len(actual.ExecOutputs.Parameters))
	copy(merged, actual.ExecOutputs.Parameters)

	// Build index over the copy.
	actualIdx := make(map[string]int, len(merged))
	for i, p := range merged {
		actualIdx[p.Name] = i
	}

	for _, declP := range decl.ExecOutputs.Parameters {
		if len(declP.Value) == 0 || string(declP.Value) == "null" {
			continue
		}
		if i, found := actualIdx[declP.Name]; found {
			// Fill only when executor left the value empty or null.
			if len(merged[i].Value) == 0 || string(merged[i].Value) == "null" {
				merged[i].Value = declP.Value
			}
		} else {
			// Parameter not produced by the executor — append the declared entry.
			actualIdx[declP.Name] = len(merged)
			merged = append(merged, model.Parameter{
				Name:        declP.Name,
				Type:        declP.Type,
				Description: declP.Description,
				Value:       declP.Value,
			})
		}
	}

	result := *actual // shallow copy of the Outputs struct
	result.ExecOutputs.Parameters = merged
	return &result
}
