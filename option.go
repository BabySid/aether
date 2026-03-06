package aether

import (
	"github.com/BabySid/aether/artifact"
	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/expr"
	"github.com/BabySid/aether/hook"
	"github.com/BabySid/aether/idgen"
	"github.com/BabySid/aether/secret"
	"github.com/BabySid/aether/store"
)

// Option configures an Engine instance.
type Option func(*Engine)

// WithStore sets the state store (required).
func WithStore(s store.Store) Option {
	return func(e *Engine) {
		e.store = s
	}
}

// WithExecutor registers an executor plugin.
// Call multiple times to register multiple executor types.
func WithExecutor(plugin executor.Plugin) Option {
	return func(e *Engine) {
		if e.executorReg == nil {
			e.executorReg = executor.NewRegistry()
		}
		_ = e.executorReg.Register(plugin)
	}
}

// WithExprEvaluator sets the expression evaluator (optional).
func WithExprEvaluator(eval expr.Evaluator) Option {
	return func(e *Engine) {
		e.exprEvaluator = eval
	}
}

// WithIDGenerator sets the ID generator (required).
func WithIDGenerator(gen idgen.Generator) Option {
	return func(e *Engine) {
		e.idGen = gen
	}
}

// WithTaskBroker sets the task broker (required).
// TaskBroker is the single bridge between Engine and Worker,
// handling task dispatch, cancellation, fetching, and completion.
func WithTaskBroker(b broker.TaskBroker) Option {
	return func(e *Engine) {
		e.taskBroker = b
	}
}

// WithArtifactStore sets the artifact store (optional).
func WithArtifactStore(a artifact.Store) Option {
	return func(e *Engine) {
		e.artifactStore = a
	}
}

// WithSecretStore sets the secret store (optional).
func WithSecretStore(s secret.Store) Option {
	return func(e *Engine) {
		e.secretStore = s
	}
}

// WithHookNotifier sets the hook notifier (optional).
func WithHookNotifier(h hook.Notifier) Option {
	return func(e *Engine) {
		e.hookNotifier = h
	}
}
