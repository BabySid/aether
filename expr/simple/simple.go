// Package simple provides a basic expression evaluator that supports == and != comparisons.
package simple

import (
	"context"
	"fmt"
	"strings"
)

// Evaluator is a simple expression evaluator supporting basic comparisons.
type Evaluator struct{}

// New creates a simple Evaluator.
func New() *Evaluator {
	return &Evaluator{}
}

// Eval evaluates simple expressions: "a == b", "a != b", and literal "true"/"false".
func (e *Evaluator) Eval(_ context.Context, expr string, env map[string]any) (any, error) {
	expr = strings.TrimSpace(expr)

	// Literal booleans
	if expr == "true" {
		return true, nil
	}
	if expr == "false" {
		return false, nil
	}

	// ==
	if parts := strings.SplitN(expr, "==", 2); len(parts) == 2 {
		left := resolveValue(strings.TrimSpace(parts[0]), env)
		right := resolveValue(strings.TrimSpace(parts[1]), env)
		return fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right), nil
	}

	// !=
	if parts := strings.SplitN(expr, "!=", 2); len(parts) == 2 {
		left := resolveValue(strings.TrimSpace(parts[0]), env)
		right := resolveValue(strings.TrimSpace(parts[1]), env)
		return fmt.Sprintf("%v", left) != fmt.Sprintf("%v", right), nil
	}

	return nil, fmt.Errorf("unsupported expression: %s", expr)
}

// resolveValue resolves a token — if it's a known env key, return the env value; otherwise return as literal.
func resolveValue(token string, env map[string]any) any {
	// Strip surrounding quotes
	if len(token) >= 2 && token[0] == '"' && token[len(token)-1] == '"' {
		return token[1 : len(token)-1]
	}
	if v, ok := env[token]; ok {
		return v
	}
	return token
}
