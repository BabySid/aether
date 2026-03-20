// simple_eval.go — basic expression evaluator supporting == and != comparisons.
package main

import (
	"context"
	"fmt"
	"strings"
)

// SimpleEvaluator evaluates simple expressions: "a == b", "a != b", "true", "false".
type SimpleEvaluator struct{}

func NewSimpleEvaluator() *SimpleEvaluator { return &SimpleEvaluator{} }

func (e *SimpleEvaluator) Eval(_ context.Context, expr string, env map[string]any) (any, error) {
	expr = strings.TrimSpace(expr)
	if expr == "true" {
		return true, nil
	}
	if expr == "false" {
		return false, nil
	}
	if parts := strings.SplitN(expr, "==", 2); len(parts) == 2 {
		left := simpleResolve(strings.TrimSpace(parts[0]), env)
		right := simpleResolve(strings.TrimSpace(parts[1]), env)
		return fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right), nil
	}
	if parts := strings.SplitN(expr, "!=", 2); len(parts) == 2 {
		left := simpleResolve(strings.TrimSpace(parts[0]), env)
		right := simpleResolve(strings.TrimSpace(parts[1]), env)
		return fmt.Sprintf("%v", left) != fmt.Sprintf("%v", right), nil
	}
	return nil, fmt.Errorf("unsupported expression: %s", expr)
}

func simpleResolve(token string, env map[string]any) any {
	if len(token) >= 2 && token[0] == '\'' && token[len(token)-1] == '\'' {
		return token[1 : len(token)-1]
	}
	if len(token) >= 2 && token[0] == '"' && token[len(token)-1] == '"' {
		return token[1 : len(token)-1]
	}
	if v, ok := env[token]; ok {
		return v
	}
	return token
}
