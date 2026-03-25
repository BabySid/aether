package internal

import (
	"encoding/json"
	"testing"

	"github.com/BabySid/aether/model"
)

// helper: build a Parameter with a JSON-marshalled value.
func param(name string, value any) model.Parameter {
	v, _ := json.Marshal(value)
	return model.Parameter{Name: name, Value: v}
}

// helper: extract the string value of a named parameter from a slice.
func paramValue(params []model.Parameter, name string) any {
	for _, p := range params {
		if p.Name == name {
			var v any
			_ = json.Unmarshal(p.Value, &v)
			return v
		}
	}
	return nil
}

// ─── MergeParameters ────────────────────────────────────────────────────────

func TestMergeParameters_EmptySrc(t *testing.T) {
	dst := []model.Parameter{param("a", 1)}
	result := MergeParameters(dst, nil)
	if len(result) != 1 || paramValue(result, "a") != float64(1) {
		t.Fatalf("expected unchanged dst, got %v", result)
	}
}

func TestMergeParameters_EmptyDst(t *testing.T) {
	src := []model.Parameter{param("x", "hello")}
	result := MergeParameters(nil, src)
	if len(result) != 1 || paramValue(result, "x") != "hello" {
		t.Fatalf("expected src appended, got %v", result)
	}
}

func TestMergeParameters_SrcWinsOnConflict(t *testing.T) {
	dst := []model.Parameter{param("k", "old")}
	src := []model.Parameter{param("k", "new")}
	result := MergeParameters(dst, src)
	if len(result) != 1 {
		t.Fatalf("expected 1 parameter, got %d", len(result))
	}
	if paramValue(result, "k") != "new" {
		t.Fatalf("expected src value 'new', got %v", paramValue(result, "k"))
	}
}

func TestMergeParameters_NoMutation(t *testing.T) {
	dst := []model.Parameter{param("a", 1)}
	src := []model.Parameter{param("a", 2), param("b", 3)}
	_ = MergeParameters(dst, src)
	// dst must remain unchanged
	if paramValue(dst, "a") != float64(1) {
		t.Fatal("dst was mutated")
	}
}

func TestMergeParameters_Append(t *testing.T) {
	dst := []model.Parameter{param("a", 1)}
	src := []model.Parameter{param("b", 2)}
	result := MergeParameters(dst, src)
	if len(result) != 2 {
		t.Fatalf("expected 2 parameters, got %d", len(result))
	}
	if paramValue(result, "a") != float64(1) {
		t.Fatalf("expected a=1, got %v", paramValue(result, "a"))
	}
	if paramValue(result, "b") != float64(2) {
		t.Fatalf("expected b=2, got %v", paramValue(result, "b"))
	}
}

func TestMergeParameters_MultipleConflictsAndAppends(t *testing.T) {
	dst := []model.Parameter{param("a", 1), param("b", 2)}
	src := []model.Parameter{param("b", 99), param("c", 3)}
	result := MergeParameters(dst, src)
	if len(result) != 3 {
		t.Fatalf("expected 3 parameters, got %d", len(result))
	}
	if paramValue(result, "a") != float64(1) {
		t.Fatalf("a should be 1")
	}
	if paramValue(result, "b") != float64(99) {
		t.Fatalf("b should be 99 (src wins)")
	}
	if paramValue(result, "c") != float64(3) {
		t.Fatalf("c should be 3")
	}
}

// ─── MergeInputsWithPayload ──────────────────────────────────────────────────

func TestMergeInputsWithPayload_NilExisting(t *testing.T) {
	result := MergeInputsWithPayload(nil, map[string]any{"k": "v"})
	if result == nil {
		t.Fatal("expected non-nil Inputs")
	}
	if paramValue(result.Parameters, "k") != "v" {
		t.Fatalf("expected k=v, got %v", paramValue(result.Parameters, "k"))
	}
}

func TestMergeInputsWithPayload_EmptyPayload(t *testing.T) {
	existing := &model.Inputs{Parameters: []model.Parameter{param("x", 42)}}
	result := MergeInputsWithPayload(existing, nil)
	if len(result.Parameters) != 1 || paramValue(result.Parameters, "x") != float64(42) {
		t.Fatalf("expected unchanged parameters, got %v", result.Parameters)
	}
}

func TestMergeInputsWithPayload_PayloadWins(t *testing.T) {
	existing := &model.Inputs{Parameters: []model.Parameter{param("decision", "pending")}}
	result := MergeInputsWithPayload(existing, map[string]any{"decision": "approve"})
	if len(result.Parameters) != 1 {
		t.Fatalf("expected 1 parameter, got %d", len(result.Parameters))
	}
	if paramValue(result.Parameters, "decision") != "approve" {
		t.Fatalf("payload should win: expected 'approve', got %v", paramValue(result.Parameters, "decision"))
	}
}

func TestMergeInputsWithPayload_AppendNew(t *testing.T) {
	existing := &model.Inputs{Parameters: []model.Parameter{param("a", 1)}}
	result := MergeInputsWithPayload(existing, map[string]any{"b": 2})
	if len(result.Parameters) != 2 {
		t.Fatalf("expected 2 parameters, got %d", len(result.Parameters))
	}
	if paramValue(result.Parameters, "a") != float64(1) {
		t.Fatal("existing param 'a' should be preserved")
	}
	if paramValue(result.Parameters, "b") != float64(2) {
		t.Fatal("new payload param 'b' should be appended")
	}
}

func TestMergeInputsWithPayload_NoMutationOfExisting(t *testing.T) {
	existing := &model.Inputs{Parameters: []model.Parameter{param("x", "original")}}
	_ = MergeInputsWithPayload(existing, map[string]any{"x": "changed"})
	if paramValue(existing.Parameters, "x") != "original" {
		t.Fatal("existing Inputs was mutated")
	}
}

// ─── MergeOutputsWithDecl ────────────────────────────────────────────────────

func outputs(params ...model.Parameter) *model.Outputs {
	return &model.Outputs{ExecOutputs: model.ExecOutputs{Parameters: params}}
}

func TestMergeOutputsWithDecl_NilDecl(t *testing.T) {
	actual := outputs(param("a", 1))
	result := MergeOutputsWithDecl(actual, nil)
	if result != actual {
		t.Fatal("expected same pointer when decl is nil")
	}
}

func TestMergeOutputsWithDecl_NilActual(t *testing.T) {
	decl := outputs(param("a", 42))
	result := MergeOutputsWithDecl(nil, decl)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if paramValue(result.Parameters, "a") != float64(42) {
		t.Fatalf("expected a=42, got %v", paramValue(result.Parameters, "a"))
	}
}

func TestMergeOutputsWithDecl_ActualWinsOnConflict(t *testing.T) {
	actual := outputs(param("score", 99))
	decl := outputs(param("score", 0))
	result := MergeOutputsWithDecl(actual, decl)
	if paramValue(result.Parameters, "score") != float64(99) {
		t.Fatalf("actual should win: expected 99, got %v", paramValue(result.Parameters, "score"))
	}
}

func TestMergeOutputsWithDecl_FillEmptyActualValue(t *testing.T) {
	// Actual has the parameter but with a null/empty value.
	actualParams := []model.Parameter{{Name: "result", Value: nil}}
	actual := &model.Outputs{ExecOutputs: model.ExecOutputs{Parameters: actualParams}}
	decl := outputs(param("result", "default"))
	result := MergeOutputsWithDecl(actual, decl)
	if paramValue(result.Parameters, "result") != "default" {
		t.Fatalf("decl should fill empty actual: got %v", paramValue(result.Parameters, "result"))
	}
}

func TestMergeOutputsWithDecl_DeclOnlyFallback(t *testing.T) {
	// Executor did not produce "y" at all; decl declares it with a static value.
	// Whitelist: only "y" is declared, so only "y" appears in result.
	// "x" (executor-only, not declared) is excluded.
	actual := outputs(param("x", 1))
	decl := outputs(param("y", 2))
	result := MergeOutputsWithDecl(actual, decl)
	if len(result.Parameters) != 1 {
		t.Fatalf("expected 1 param (whitelist), got %d", len(result.Parameters))
	}
	if paramValue(result.Parameters, "y") != float64(2) {
		t.Fatalf("y should come from decl fallback, got %v", paramValue(result.Parameters, "y"))
	}
	if paramValue(result.Parameters, "x") != nil {
		t.Fatal("x is not declared — must be excluded by whitelist")
	}
}

func TestMergeOutputsWithDecl_SkipDeclNullValues(t *testing.T) {
	// Whitelist semantics: decl declares "b" with null value.
	// "b" is skipped (null declared value, executor also didn't produce it).
	// "a" is excluded because it's not in the whitelist.
	// Result: 0 parameters.
	actual := outputs(param("a", 1))
	declParams := []model.Parameter{{Name: "b", Value: nil}}
	decl := &model.Outputs{ExecOutputs: model.ExecOutputs{Parameters: declParams}}
	result := MergeOutputsWithDecl(actual, decl)
	if len(result.Parameters) != 0 {
		t.Fatalf("null-value decl param + undeclared 'a' should yield 0 params, got %d", len(result.Parameters))
	}
}

func TestMergeOutputsWithDecl_NoMutationOfActual(t *testing.T) {
	actual := outputs(param("k", "orig"))
	decl := outputs(param("k", "fill"), param("new", "x"))
	_ = MergeOutputsWithDecl(actual, decl)
	// actual.Parameters must not be changed.
	if len(actual.Parameters) != 1 {
		t.Fatal("actual.Parameters was mutated (length changed)")
	}
	if paramValue(actual.Parameters, "k") != "orig" {
		t.Fatal("actual parameter value was mutated")
	}
}

func TestMergeOutputsWithDecl_WhitelistFiltersUndeclaredParams(t *testing.T) {
	// Executor produced "summary", "ok", "raw", "count".
	// Decl only declares "summary" and "ok".
	// Result must contain only "summary" and "ok".
	actual := outputs(param("raw", "data"), param("count", 5), param("summary", "done"), param("ok", true))
	decl := outputs(param("summary", ""), param("ok", false))
	result := MergeOutputsWithDecl(actual, decl)
	if len(result.Parameters) != 2 {
		t.Fatalf("expected 2 whitelisted params, got %d: %v", len(result.Parameters), result.Parameters)
	}
	if paramValue(result.Parameters, "summary") != "done" {
		t.Fatalf("summary should keep executor value 'done', got %v", paramValue(result.Parameters, "summary"))
	}
	if paramValue(result.Parameters, "ok") != true {
		t.Fatalf("ok should keep executor value true, got %v", paramValue(result.Parameters, "ok"))
	}
	if paramValue(result.Parameters, "raw") != nil {
		t.Fatal("raw is not declared — must be excluded")
	}
	if paramValue(result.Parameters, "count") != nil {
		t.Fatal("count is not declared — must be excluded")
	}
}

func TestMergeOutputsWithDecl_PreservesOtherOutputFields(t *testing.T) {
	// Only "b" is declared — "a" (executor-only) should be excluded.
	actual := &model.Outputs{
		ExecOutputs: model.ExecOutputs{Parameters: []model.Parameter{param("a", 1)}},
	}
	decl := outputs(param("b", 2))
	result := MergeOutputsWithDecl(actual, decl)
	// "a" not declared → excluded; "b" not in executor but has decl value → included
	if len(result.Parameters) != 1 {
		t.Fatalf("expected 1 param (only declared 'b'), got %d", len(result.Parameters))
	}
	if paramValue(result.Parameters, "b") != float64(2) {
		t.Fatalf("expected b=2 from decl fallback, got %v", paramValue(result.Parameters, "b"))
	}
}
