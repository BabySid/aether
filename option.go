package aether

import (
	"github.com/BabySid/aether/artifact"
	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/cron"
	"github.com/BabySid/aether/errsink"
	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/expr"
	"github.com/BabySid/aether/hook"
	"github.com/BabySid/aether/idgen"
	"github.com/BabySid/aether/secret"
	"github.com/BabySid/aether/store"
	"github.com/BabySid/aether/timeout"
	"github.com/BabySid/aether/vars"
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

// WithExecutorRegistry sets a pre-built executor registry.
// Use this when you already have an *executor.Registry (e.g. shared with a broker)
// and want to avoid registering each plugin twice.
func WithExecutorRegistry(reg *executor.Registry) Option {
	return func(e *Engine) {
		e.executorReg = reg
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

// WithErrorSink sets the error observation sink (optional).
// When configured, the engine reports internal errors (hook failures, store
// errors in void callbacks, critical-path failures) to the sink for external
// monitoring and alerting. The engine's scheduling behaviour is identical
// regardless of whether an ErrorSink is configured.
func WithErrorSink(s errsink.ErrorSink) Option {
	return func(e *Engine) {
		e.errorSink = s
	}
}

// WithTimeoutWatcher sets the timeout watchdog (optional).
// When configured, Engine.Start() will begin the watchdog loop that detects
// tasks and workflows that have exceeded their deadlines.
func WithTimeoutWatcher(w timeout.Watcher) Option {
	return func(e *Engine) {
		e.timeoutWatcher = w
	}
}

// WithVarsSource registers a global Source at the engine level (optional).
// Call multiple times to register multiple providers.
//
// Engine-level providers are injected into every VarBuilder at the start of each
// variable resolution call, making their variables available in all workflow templates.
// They have lower priority than per-call providers: if a per-call provider (e.g.
// vars.WorkflowArgsSource) produces the same key, the per-call value wins.
//
// This option is intended for providers whose data is stable across workflow runs,
// such as vars.SystemSource (exposes system.os and system.arch).
//
// Example:
//
//	engine, _ := aether.New(
//	    aether.WithVarsSource(&vars.SystemSource{}),
//	)
func WithVarsSource(p vars.Source) Option {
	return func(e *Engine) {
		if p != nil {
			e.varsSources = append(e.varsSources, p)
		}
	}
}

// WithCronScheduler sets the cron scheduling backend (optional).
// When configured, CronWorkflow methods (SubmitCronWorkflow, GetCronWorkflow, etc.)
// become available. Without it, those methods return ErrNotSupported.
func WithCronScheduler(s cron.Scheduler) Option {
	return func(e *Engine) {
		e.cronScheduler = s
	}
}
