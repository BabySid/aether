// retry.go — task-level retry policy resolution and evaluation.
//
// Retry is only supported on leaf tasks (executor type). DAG and Loop containers
// do not retry directly — they rely on their child tasks' retry policies.
//
// # Retry policy lookup
//
// ResolveRetryPolicy looks up the call-site task node inside the parent DAG
// template to find the task-level Retry config. There is no workflow-level
// retry fallback by design: a workflow-level retry would be ambiguous (which
// tasks should be retried? all? only the failed ones?) and the Expression field
// cannot meaningfully reference a specific task's outputs at that scope.
//
// # ShouldRetry logic
//
//  1. If retry is nil or Limit == 0 → no retry.
//  2. If phase is Succeeded or Skipped → no retry (task completed normally or was intentionally skipped).
//  3. If phase is Failed → no retry by default (business-level failure; executor explicitly returned
//     Failed to signal a known, deterministic outcome — retrying would likely yield the same result).
//     Users who want to retry on Failed must set Expression explicitly, e.g.:
//     expression: "tasks.<name>.phase != 'Succeeded'"
//  4. If RetryCount >= Limit → retry budget exhausted.
//  5. If Expression is empty → retry on Error and Timeout only.
//  6. If Expression is set → evaluate it; retry only if it returns true (overrides the default
//     phase filter, giving users full control).
//
// The expression environment is built from the task's own execution result
// using BuildTaskEnv([]*store.TaskRun{tr}), exposing:
//
//	tasks.<name>.phase                        — e.g. "Failed", "Error", "Timeout"
//	tasks.<name>.code                         — exit code integer
//	tasks.<name>.msg                          — error message string
//	tasks.<name>.outputs.parameters.<param>   — output parameter value
package internal

import (
	"context"
	"fmt"

	"github.com/BabySid/aether/expr"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// ResolveRetryPolicy returns the retry policy for the given task node.
//
// It looks up the call-site task by (parentTemplateName, taskName) inside the
// parent DAG template. Returns nil if:
//   - the parent template is not found
//   - the parent template has no DAG
//   - the task node is not found within the DAG
//   - the task node has no Retry configured
func ResolveRetryPolicy(wf *model.Workflow, parentTemplateName, taskName string) *model.Retry {
	tmpl := FindTemplate(wf, parentTemplateName)
	if tmpl == nil || tmpl.DAG == nil {
		return nil
	}
	task := FindTask(tmpl.DAG, taskName)
	if task == nil {
		return nil
	}
	return task.Retry
}

// ShouldRetry returns true if the task should be retried based on its retry
// policy and the result of the last execution attempt.
//
// Default behaviour (no Expression):
//   - Succeeded / Skipped / Failed → no retry
//   - Error / Timeout → retry (up to Limit)
//
// With Expression set, the expression overrides the default phase filter and
// has full control over whether to retry. eval may be nil; in that case a
// non-empty Expression is treated as "always retry" (graceful degradation,
// consistent with EvalWhenCondition behaviour).
func ShouldRetry(ctx context.Context, tr *store.TaskRun, retry *model.Retry, eval expr.Evaluator) (bool, error) {
	if retry == nil || retry.Limit <= 0 {
		return false, nil
	}

	// Succeeded and Skipped are not failure phases — no retry needed.
	if tr.Status == nil || *tr.Status == model.PhaseSucceeded || *tr.Status == model.PhaseSkipped {
		return false, nil
	}

	// Retry budget exhausted.
	if tr.RetryCount != nil && *tr.RetryCount >= retry.Limit {
		return false, nil
	}

	// No custom expression: apply default phase filter.
	// Failed is a business-level failure — the executor explicitly returned Failed
	// to signal a known, deterministic outcome. Retrying would likely yield the
	// same result, so we exclude it from default retry.
	// Only Error (system error) and Timeout are retried by default.
	if retry.Expression == "" {
		return *tr.Status == model.PhaseError || *tr.Status == model.PhaseTimeout, nil
	}

	// Custom expression: evaluate against the task's own execution result.
	// If no evaluator is configured, fall back to unconditional retry.
	if eval == nil {
		return true, nil
	}

	env := BuildTaskEnv([]*store.TaskRun{tr})
	result, err := eval.Eval(ctx, retry.Expression, env)
	if err != nil {
		return false, fmt.Errorf("retry expression %q: %w", retry.Expression, err)
	}

	switch v := result.(type) {
	case bool:
		return v, nil
	case string:
		return v == "true", nil
	default:
		return false, fmt.Errorf("retry expression must return bool or string, got %T", v)
	}
}
