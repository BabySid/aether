package binding

import (
	"context"
	"encoding/json"
	"errors"
)

// ─────────────────────────────────────────────────────────
// Shared test helpers
// ─────────────────────────────────────────────────────────

// fakeEvaluator implements expr.Evaluator for tests.
type fakeEvaluator struct {
	fn func(expr string, env map[string]any) (any, error)
}

func (f *fakeEvaluator) Eval(_ context.Context, expr string, env map[string]any) (any, error) {
	return f.fn(expr, env)
}

// fakeSecretStore implements secret.Provider for tests.
type fakeSecretStore struct {
	secrets map[string]string // key = "name/key"
}

func (f *fakeSecretStore) Get(_ context.Context, name, key string) (string, error) {
	k := name + "/" + key
	if v, ok := f.secrets[k]; ok {
		return v, nil
	}
	return "", errors.New("secret not found: " + k)
}

// rawJSON marshals v to json.RawMessage; panics on error.
func rawJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
