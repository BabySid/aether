package executor

import (
	"encoding/json"
	"testing"

	"github.com/BabySid/aether/model"
)

// ─────────────────────────────────────────────────────────────────────────────
// OutputFrom
// ─────────────────────────────────────────────────────────────────────────────

type simpleOutput struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mixedOutput struct {
	StatusCode int            `json:"status_code"`
	Body       any            `json:"body"`
	Headers    map[string]any `json:"headers"`
}

type skipOutput struct {
	Visible string `json:"visible"`
	Hidden  string `json:"-"`
}

type noTagSimple struct {
	Alpha int
	Beta  string
}

type embeddedBase struct {
	Base string `json:"base"`
}

type embeddedOutput struct {
	embeddedBase        // anonymous — should trigger error
	Extra        string `json:"extra"`
}

func rawJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func TestOutputFrom_Basic(t *testing.T) {
	out, err := OutputFrom(simpleOutput{Code: 0, Message: "ok"})
	if err != nil {
		t.Fatalf("OutputFrom: %v", err)
	}
	if len(out.Parameters) != 2 {
		t.Fatalf("want 2 params, got %d", len(out.Parameters))
	}
	paramVal := func(name string) any {
		for _, p := range out.Parameters {
			if p.Name == name {
				var v any
				_ = json.Unmarshal(p.Value, &v)
				return v
			}
		}
		return nil
	}
	if paramVal("code") != float64(0) {
		t.Errorf("code: want 0, got %v", paramVal("code"))
	}
	if paramVal("message") != "ok" {
		t.Errorf("message: want ok, got %v", paramVal("message"))
	}
}

func TestOutputFrom_FieldNamesFromJSONTag(t *testing.T) {
	out, err := OutputFrom(mixedOutput{StatusCode: 200, Body: "hello", Headers: map[string]any{"X-Foo": "bar"}})
	if err != nil {
		t.Fatalf("OutputFrom: %v", err)
	}
	names := make(map[string]bool)
	for _, p := range out.Parameters {
		names[p.Name] = true
	}
	for _, want := range []string{"status_code", "body", "headers"} {
		if !names[want] {
			t.Errorf("missing parameter %q", want)
		}
	}
}

func TestOutputFrom_SkipDashField(t *testing.T) {
	out, err := OutputFrom(skipOutput{Visible: "yes", Hidden: "no"})
	if err != nil {
		t.Fatalf("OutputFrom: %v", err)
	}
	if len(out.Parameters) != 1 {
		t.Fatalf("want 1 param, got %d", len(out.Parameters))
	}
	if out.Parameters[0].Name != "visible" {
		t.Errorf("want visible, got %s", out.Parameters[0].Name)
	}
}

func TestOutputFrom_NoTagFallsBackToFieldName(t *testing.T) {
	out, err := OutputFrom(noTagSimple{Alpha: 42, Beta: "hi"})
	if err != nil {
		t.Fatalf("OutputFrom: %v", err)
	}
	if len(out.Parameters) != 2 {
		t.Fatalf("want 2 params, got %d", len(out.Parameters))
	}
	if out.Parameters[0].Name != "Alpha" {
		t.Errorf("want Alpha, got %s", out.Parameters[0].Name)
	}
}

func TestOutputFrom_EmbeddedFieldReturnsError(t *testing.T) {
	_, err := OutputFrom(embeddedOutput{embeddedBase: embeddedBase{Base: "x"}, Extra: "y"})
	if err == nil {
		t.Error("expected error for embedded anonymous field, got nil")
	}
}

func TestOutputFrom_NonStructReturnsError(t *testing.T) {
	_, err := OutputFrom("not a struct")
	if err == nil {
		t.Error("expected error for non-struct, got nil")
	}
}

func TestOutputFrom_ValueSerialised(t *testing.T) {
	out, _ := OutputFrom(simpleOutput{Code: 42, Message: "done"})
	for _, p := range out.Parameters {
		if p.Name == "code" {
			var v float64
			if err := json.Unmarshal(p.Value, &v); err != nil {
				t.Fatalf("unmarshal code: %v", err)
			}
			if v != 42 {
				t.Errorf("code: want 42, got %v", v)
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// BindInputs
// ─────────────────────────────────────────────────────────────────────────────

type bindTarget struct {
	URL    string `json:"url"`
	Method string `json:"method"`
	Count  int    `json:"count"`
}

func makeInputs(pairs ...any) *model.Inputs {
	var params []model.Parameter
	for i := 0; i+1 < len(pairs); i += 2 {
		name := pairs[i].(string)
		raw, _ := json.Marshal(pairs[i+1])
		params = append(params, model.Parameter{Name: name, Value: raw})
	}
	return &model.Inputs{Parameters: params}
}

func TestBindInputs_Basic(t *testing.T) {
	inputs := makeInputs("url", "https://example.com", "method", "GET", "count", 5)
	var dst bindTarget
	if err := BindInputs(inputs, &dst); err != nil {
		t.Fatalf("BindInputs: %v", err)
	}
	if dst.URL != "https://example.com" {
		t.Errorf("URL: want https://example.com, got %s", dst.URL)
	}
	if dst.Method != "GET" {
		t.Errorf("Method: want GET, got %s", dst.Method)
	}
	if dst.Count != 5 {
		t.Errorf("Count: want 5, got %d", dst.Count)
	}
}

func TestBindInputs_NilInputs(t *testing.T) {
	var dst bindTarget
	if err := BindInputs(nil, &dst); err != nil {
		t.Fatalf("BindInputs with nil inputs: %v", err)
	}
	// dst should be zero-value
	if dst.URL != "" || dst.Method != "" || dst.Count != 0 {
		t.Error("expected zero dst for nil inputs")
	}
}

func TestBindInputs_PartialInputs(t *testing.T) {
	inputs := makeInputs("url", "https://api.io")
	var dst bindTarget
	if err := BindInputs(inputs, &dst); err != nil {
		t.Fatalf("BindInputs: %v", err)
	}
	if dst.URL != "https://api.io" {
		t.Errorf("URL: want https://api.io, got %s", dst.URL)
	}
	if dst.Method != "" {
		t.Errorf("Method: expected empty (not in inputs), got %s", dst.Method)
	}
}

func TestBindInputs_ExtraInputsIgnored(t *testing.T) {
	inputs := makeInputs("url", "https://x.io", "unknown_field", "ignored")
	var dst bindTarget
	if err := BindInputs(inputs, &dst); err != nil {
		t.Fatalf("BindInputs: %v", err)
	}
	if dst.URL != "https://x.io" {
		t.Errorf("URL: want https://x.io, got %s", dst.URL)
	}
}

func TestBindInputs_RoundTripWithOutputFrom(t *testing.T) {
	// OutputFrom → Parameters → BindInputs should produce identical values.
	type roundTrip struct {
		Name   string `json:"name"`
		Score  int    `json:"score"`
		Active bool   `json:"active"`
	}
	original := roundTrip{Name: "alice", Score: 99, Active: true}
	exec, err := OutputFrom(original)
	if err != nil {
		t.Fatalf("OutputFrom: %v", err)
	}
	inputs := &model.Inputs{Parameters: exec.Parameters}
	var dst roundTrip
	if err := BindInputs(inputs, &dst); err != nil {
		t.Fatalf("BindInputs: %v", err)
	}
	if dst.Name != original.Name {
		t.Errorf("Name: want %s, got %s", original.Name, dst.Name)
	}
	if dst.Score != original.Score {
		t.Errorf("Score: want %d, got %d", original.Score, dst.Score)
	}
	if dst.Active != original.Active {
		t.Errorf("Active: want %v, got %v", original.Active, dst.Active)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// SchemaOf + OutputFrom consistency
// ─────────────────────────────────────────────────────────────────────────────

// TestSchemaOutputNamesMatchOutputFrom verifies that the field names produced
// by OutputFrom match exactly the parameter names declared in Schema().Outputs.
func TestSchemaOutputNamesMatchOutputFrom(t *testing.T) {
	type httpOut struct {
		StatusCode int    `json:"status_code"`
		Body       string `json:"body"`
	}

	type httpPlugin struct{}
	_ = &httpPlugin{}

	schema := SchemaOf[DynamicOutputs, httpOut]("http", "1.0", "")
	exec, err := OutputFrom(httpOut{StatusCode: 200, Body: "hello"})
	if err != nil {
		t.Fatalf("OutputFrom: %v", err)
	}

	schemaNames := make(map[string]bool)
	for _, p := range schema.Outputs.Parameters {
		schemaNames[p.Name] = true
	}
	for _, p := range exec.Parameters {
		if !schemaNames[p.Name] {
			t.Errorf("OutputFrom produced field %q not declared in Schema().Outputs", p.Name)
		}
	}
}

