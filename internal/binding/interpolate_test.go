package binding

import "testing"

func TestInterpolate_NoPlaceholder(t *testing.T) {
	env := EvalVars{"x": 42}
	val, ok := Interpolate("hello world", env)
	if ok {
		t.Fatal("expected wasInterpolated=false for string without placeholders")
	}
	if val != "hello world" {
		t.Fatalf("expected %q, got %v", "hello world", val)
	}
}

func TestInterpolate_SingleToken_TypePreserved(t *testing.T) {
	env := EvalVars{"num": 99}
	val, ok := Interpolate("{{num}}", env)
	if !ok {
		t.Fatal("expected wasInterpolated=true")
	}
	if val != 99 {
		t.Fatalf("expected int 99, got %v (%T)", val, val)
	}
}

func TestInterpolate_SingleToken_StringValue(t *testing.T) {
	env := EvalVars{"greeting": "hello"}
	val, ok := Interpolate("{{greeting}}", env)
	if !ok {
		t.Fatal("expected wasInterpolated=true")
	}
	if val != "hello" {
		t.Fatalf("expected %q, got %v", "hello", val)
	}
}

func TestInterpolate_SingleToken_KeyNotFound(t *testing.T) {
	env := EvalVars{}
	val, ok := Interpolate("{{missing}}", env)
	// Key not found: original string returned unchanged, wasInterpolated=false
	if ok {
		t.Fatal("expected wasInterpolated=false when key not found")
	}
	if val != "{{missing}}" {
		t.Fatalf("expected original placeholder, got %v", val)
	}
}

func TestInterpolate_MultiToken(t *testing.T) {
	env := EvalVars{"a": "foo", "b": "bar"}
	val, ok := Interpolate("prefix-{{a}}-{{b}}-suffix", env)
	if !ok {
		t.Fatal("expected wasInterpolated=true")
	}
	if val != "prefix-foo-bar-suffix" {
		t.Fatalf("unexpected result: %v", val)
	}
}

func TestInterpolate_MultiToken_MissingKeyPreserved(t *testing.T) {
	env := EvalVars{"a": "A"}
	val, ok := Interpolate("{{a}}-{{missing}}", env)
	if !ok {
		t.Fatal("expected wasInterpolated=true because {{a}} was replaced")
	}
	if val != "A-{{missing}}" {
		t.Fatalf("unexpected result: %v", val)
	}
}

func TestInterpolate_UnclosedBrace(t *testing.T) {
	env := EvalVars{"x": "X"}
	// No closing }} → no interpolation, must not panic
	_, ok := Interpolate("{{x", env)
	if ok {
		t.Fatal("expected wasInterpolated=false for unclosed brace")
	}
}

func TestInterpolateString_NonStringValue(t *testing.T) {
	env := EvalVars{"n": 3.14}
	s := InterpolateString("{{n}}", env)
	// fmt.Sprint(3.14) → "3.14"
	if s != "3.14" {
		t.Fatalf("expected %q, got %q", "3.14", s)
	}
}

func TestInterpolateString_NoPlaceholder(t *testing.T) {
	s := InterpolateString("plain text", EvalVars{})
	if s != "plain text" {
		t.Fatalf("expected %q, got %q", "plain text", s)
	}
}
