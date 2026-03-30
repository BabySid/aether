package executor

import (
	"context"
	"testing"

	"github.com/BabySid/aether/model"
)

// ─────────────────────────────────────────────────────────────────────────────
// test fixtures
// ─────────────────────────────────────────────────────────────────────────────

type httpOutput struct {
	StatusCode int            `json:"status_code" desc:"HTTP response status code"`
	Body       any            `json:"body"        desc:"Parsed response body"`
	Headers    map[string]any `json:"headers"     desc:"Response headers"`
}

type shellOutput struct {
	ExitCode int    `json:"exit_code" desc:"Process exit code"`
	Stdout   string `json:"stdout"    desc:"Standard output"`
	Stderr   string `json:"stderr"    desc:"Standard error"`
}

type noTagOutput struct {
	Code    int
	Message string
}

type skipFieldOutput struct {
	Keep   string `json:"keep"`
	Hidden string `json:"-"`
}

type omitEmptyOutput struct {
	Name  string `json:"name,omitempty"`
	Score int    `json:"score,omitempty"`
}

type configStruct struct {
	URL    string `json:"url"    desc:"Request URL"`
	Method string `json:"method" desc:"HTTP method"`
}

// shellPlugin is a minimal Plugin backed by shellOutput for registry tests.
type shellPlugin struct{}

func (s *shellPlugin) Type() string { return "shell" }
func (s *shellPlugin) Schema() ExecutorSchema {
	return SchemaOf[DynamicOutputs, shellOutput]("shell", "1.0", "Shell executor")
}

func (s *shellPlugin) Execute(_ context.Context, _ *ExecuteRequest) (*model.ExecOutputs, error) {
	return nil, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// SchemaOf — static outputs
// ─────────────────────────────────────────────────────────────────────────────

func TestSchemaOf_StaticOutputs(t *testing.T) {
	s := SchemaOf[DynamicOutputs, httpOutput]("http", "1.0", "HTTP executor")

	if s.Type != "http" {
		t.Errorf("Type: want http, got %s", s.Type)
	}
	if s.Version != "1.0" {
		t.Errorf("Version: want 1.0, got %s", s.Version)
	}
	if s.Description != "HTTP executor" {
		t.Errorf("Description mismatch")
	}
	if s.Inputs != nil {
		t.Errorf("Inputs: expected nil for DynamicOutputs config, got non-nil")
	}
	if s.Outputs == nil {
		t.Fatal("Outputs: expected non-nil for httpOutput, got nil")
	}
	if len(s.Outputs.Parameters) != 3 {
		t.Fatalf("Outputs.Parameters: want 3, got %d", len(s.Outputs.Parameters))
	}
}

func TestSchemaOf_OutputFieldNames(t *testing.T) {
	s := SchemaOf[DynamicOutputs, httpOutput]("http", "1.0", "")

	want := []struct {
		name string
		typ  string
		desc string
	}{
		{"status_code", "int", "HTTP response status code"},
		{"body", "any", "Parsed response body"},
		{"headers", "object", "Response headers"},
	}
	for i, w := range want {
		p := s.Outputs.Parameters[i]
		if p.Name != w.name {
			t.Errorf("[%d] Name: want %s, got %s", i, w.name, p.Name)
		}
		if p.Type != w.typ {
			t.Errorf("[%d] Type: want %s, got %s", i, w.typ, p.Type)
		}
		if p.Description != w.desc {
			t.Errorf("[%d] Description: want %q, got %q", i, w.desc, p.Description)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// SchemaOf — dynamic outputs
// ─────────────────────────────────────────────────────────────────────────────

func TestSchemaOf_DynamicOutputs(t *testing.T) {
	s := SchemaOf[DynamicOutputs, DynamicOutputs]("echo", "1.0", "Echo executor")
	if s.Outputs != nil {
		t.Errorf("DynamicOutputs: want nil Outputs, got non-nil")
	}
	if s.Inputs != nil {
		t.Errorf("DynamicOutputs config: want nil Inputs, got non-nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// SchemaOf — config inputs
// ─────────────────────────────────────────────────────────────────────────────

func TestSchemaOf_WithConfigInputs(t *testing.T) {
	s := SchemaOf[configStruct, DynamicOutputs]("http", "1.0", "")
	if s.Inputs == nil {
		t.Fatal("Inputs: expected non-nil for configStruct")
	}
	if len(s.Inputs.Parameters) != 2 {
		t.Fatalf("Inputs.Parameters: want 2, got %d", len(s.Inputs.Parameters))
	}
	if s.Inputs.Parameters[0].Name != "url" {
		t.Errorf("param[0].Name: want url, got %s", s.Inputs.Parameters[0].Name)
	}
	if s.Inputs.Parameters[0].Description != "Request URL" {
		t.Errorf("param[0].Description mismatch: got %s", s.Inputs.Parameters[0].Description)
	}
	if s.Inputs.Parameters[1].Name != "method" {
		t.Errorf("param[1].Name: want method, got %s", s.Inputs.Parameters[1].Name)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// SchemaOf — tag edge cases
// ─────────────────────────────────────────────────────────────────────────────

func TestSchemaOf_NoTagOutput_FallsBackToFieldName(t *testing.T) {
	s := SchemaOf[DynamicOutputs, noTagOutput]("noop", "1.0", "")
	if s.Outputs == nil {
		t.Fatal("Outputs: expected non-nil")
	}
	if len(s.Outputs.Parameters) != 2 {
		t.Fatalf("want 2 params, got %d", len(s.Outputs.Parameters))
	}
	if s.Outputs.Parameters[0].Name != "Code" {
		t.Errorf("want Code, got %s", s.Outputs.Parameters[0].Name)
	}
	if s.Outputs.Parameters[1].Name != "Message" {
		t.Errorf("want Message, got %s", s.Outputs.Parameters[1].Name)
	}
}

func TestSchemaOf_SkipDashField(t *testing.T) {
	s := SchemaOf[DynamicOutputs, skipFieldOutput]("noop", "1.0", "")
	if s.Outputs == nil {
		t.Fatal("Outputs: expected non-nil")
	}
	if len(s.Outputs.Parameters) != 1 {
		t.Fatalf("want 1 param (Hidden skipped), got %d", len(s.Outputs.Parameters))
	}
	if s.Outputs.Parameters[0].Name != "keep" {
		t.Errorf("want keep, got %s", s.Outputs.Parameters[0].Name)
	}
}

func TestSchemaOf_OmitEmptyTagParsed(t *testing.T) {
	s := SchemaOf[DynamicOutputs, omitEmptyOutput]("noop", "1.0", "")
	if s.Outputs == nil {
		t.Fatal("Outputs: expected non-nil")
	}
	if len(s.Outputs.Parameters) != 2 {
		t.Fatalf("want 2 params, got %d", len(s.Outputs.Parameters))
	}
	if s.Outputs.Parameters[0].Name != "name" {
		t.Errorf("want name (omitempty stripped), got %s", s.Outputs.Parameters[0].Name)
	}
	if s.Outputs.Parameters[1].Name != "score" {
		t.Errorf("want score, got %s", s.Outputs.Parameters[1].Name)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// SchemaOf — type mapping
// ─────────────────────────────────────────────────────────────────────────────

func TestSchemaOf_TypeMapping(t *testing.T) {
	s := SchemaOf[DynamicOutputs, shellOutput]("shell", "1.0", "")
	params := s.Outputs.Parameters
	// exit_code → int, stdout → string, stderr → string
	typeFor := func(name string) string {
		for _, p := range params {
			if p.Name == name {
				return p.Type
			}
		}
		return ""
	}
	if typeFor("exit_code") != "int" {
		t.Errorf("exit_code: want int, got %s", typeFor("exit_code"))
	}
	if typeFor("stdout") != "string" {
		t.Errorf("stdout: want string, got %s", typeFor("stdout"))
	}
	if typeFor("stderr") != "string" {
		t.Errorf("stderr: want string, got %s", typeFor("stderr"))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// SchemaOf — panic on non-struct
// ─────────────────────────────────────────────────────────────────────────────

func TestSchemaOf_PanicsOnNonStructConfig(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for non-struct C, got none")
		}
	}()
	SchemaOf[string, DynamicOutputs]("bad", "1.0", "")
}

func TestSchemaOf_PanicsOnNonStructOutput(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for non-struct O, got none")
		}
	}()
	SchemaOf[DynamicOutputs, int]("bad", "1.0", "")
}

// ─────────────────────────────────────────────────────────────────────────────
// parseJSONTagName
// ─────────────────────────────────────────────────────────────────────────────

func TestParseJSONTagName(t *testing.T) {
	cases := []struct{ tag, want string }{
		{"status_code", "status_code"},
		{"status_code,omitempty", "status_code"},
		{",omitempty", ""},
		{"", ""},
		{"-", "-"},
	}
	for _, c := range cases {
		got := parseJSONTagName(c.tag)
		if got != c.want {
			t.Errorf("parseJSONTagName(%q): want %q, got %q", c.tag, c.want, got)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Registry — schema caching
// ─────────────────────────────────────────────────────────────────────────────

func TestRegistry_SchemaCached(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(&shellPlugin{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, ok := reg.GetSchema("shell")
	if !ok {
		t.Fatal("GetSchema: expected schema to be cached")
	}
	if got.Type != "shell" {
		t.Errorf("Type: want shell, got %s", got.Type)
	}
	if got.Outputs == nil {
		t.Fatal("Outputs: expected non-nil")
	}
	if len(got.Outputs.Parameters) != 3 {
		t.Fatalf("Outputs.Parameters: want 3, got %d", len(got.Outputs.Parameters))
	}
}

func TestRegistry_GetSchema_NotFound(t *testing.T) {
	reg := NewRegistry()
	_, ok := reg.GetSchema("missing")
	if ok {
		t.Error("expected ok=false for missing schema")
	}
}

func TestRegistry_Schemas(t *testing.T) {
	reg := NewRegistry()
	_ = reg.Register(&shellPlugin{})
	schemas := reg.Schemas()
	if len(schemas) != 1 {
		t.Fatalf("want 1 schema, got %d", len(schemas))
	}
	if schemas[0].Type != "shell" {
		t.Errorf("want shell, got %s", schemas[0].Type)
	}
}

func TestRegistry_DuplicateRegister(t *testing.T) {
	reg := NewRegistry()
	_ = reg.Register(&shellPlugin{})
	err := reg.Register(&shellPlugin{})
	if err == nil {
		t.Error("expected error on duplicate register")
	}
}
