package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// ---- helpers ----

func makeWorkflowWithRetry(taskRetry *model.Retry) *model.Workflow {
	return &model.Workflow{
		Spec: model.WorkflowSpec{
			Entrypoint: "main",
			Templates: []model.Template{
				{
					DAG: &model.DAG{
						Name: "main",
						Tasks: []model.Task{
							{
								Name:  "fetch",
								Retry: taskRetry,
								Executor: &model.Executor{
									Type: "function",
								},
							},
						},
					},
				},
			},
		},
	}
}

func makeTaskRun(name string, status model.Phase, retryCount int) *store.TaskRun {
	return &store.TaskRun{
		RunID:      1,
		TaskName:   name,
		Status:     &status,
		RetryCount: &retryCount,
	}
}

// ---- ResolveRetryPolicy ----

func TestResolveRetryPolicy_NilRetry(t *testing.T) {
	wf := makeWorkflowWithRetry(nil)
	policy := ResolveRetryPolicy(wf, "main", "fetch")
	if policy != nil {
		t.Errorf("expected nil retry policy, got %+v", policy)
	}
}

func TestResolveRetryPolicy_WithRetry(t *testing.T) {
	retry := &model.Retry{Limit: 3}
	wf := makeWorkflowWithRetry(retry)
	policy := ResolveRetryPolicy(wf, "main", "fetch")
	if policy == nil {
		t.Fatal("expected non-nil retry policy")
	}
	if policy.Limit != 3 {
		t.Errorf("expected limit=3, got %d", policy.Limit)
	}
}

func TestResolveRetryPolicy_TemplateNotFound(t *testing.T) {
	wf := makeWorkflowWithRetry(&model.Retry{Limit: 2})
	policy := ResolveRetryPolicy(wf, "nonexistent", "fetch")
	if policy != nil {
		t.Errorf("expected nil for unknown template, got %+v", policy)
	}
}

func TestResolveRetryPolicy_TaskNotFound(t *testing.T) {
	wf := makeWorkflowWithRetry(&model.Retry{Limit: 2})
	policy := ResolveRetryPolicy(wf, "main", "nonexistent-task")
	if policy != nil {
		t.Errorf("expected nil for unknown task, got %+v", policy)
	}
}

func TestResolveRetryPolicy_NonDAGTemplate(t *testing.T) {
	wf := &model.Workflow{
		Spec: model.WorkflowSpec{
			Entrypoint: "single",
			Templates: []model.Template{
				{
					// task template (no DAG)
					Task: &model.Task{
						Name: "single",
						Executor: &model.Executor{
							Type: "function",
						},
					},
				},
			},
		},
	}
	policy := ResolveRetryPolicy(wf, "single", "single")
	if policy != nil {
		t.Errorf("expected nil for non-DAG template, got %+v", policy)
	}
}

// ---- ShouldRetry ----

func TestShouldRetry_NilPolicy(t *testing.T) {
	tr := makeTaskRun("fetch", model.PhaseFailed, 0)
	ok, err := ShouldRetry(context.Background(), tr, nil, nil)
	if err != nil || ok {
		t.Errorf("expected (false, nil), got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_ZeroLimit(t *testing.T) {
	tr := makeTaskRun("fetch", model.PhaseFailed, 0)
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 0}, nil)
	if err != nil || ok {
		t.Errorf("expected (false, nil), got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_Succeeded(t *testing.T) {
	tr := makeTaskRun("fetch", model.PhaseSucceeded, 0)
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 3}, nil)
	if err != nil || ok {
		t.Errorf("Succeeded phase should not retry, got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_Skipped(t *testing.T) {
	tr := makeTaskRun("fetch", model.PhaseSkipped, 0)
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 3}, nil)
	if err != nil || ok {
		t.Errorf("Skipped phase should not retry, got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_BudgetExhausted(t *testing.T) {
	tr := makeTaskRun("fetch", model.PhaseFailed, 3)
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 3}, nil)
	if err != nil || ok {
		t.Errorf("exhausted budget should not retry, got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_FailedNoRetryByDefault(t *testing.T) {
	// Failed is a business-level failure — no retry by default (no expression set).
	tr := makeTaskRun("fetch", model.PhaseFailed, 0)
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 3}, nil)
	if err != nil || ok {
		t.Errorf("Failed phase should not retry by default, got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_ErrorWithinLimit(t *testing.T) {
	tr := makeTaskRun("fetch", model.PhaseError, 0)
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 2}, nil)
	if err != nil || !ok {
		t.Errorf("expected (true, nil) for Error phase, got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_TimeoutWithinLimit(t *testing.T) {
	tr := makeTaskRun("fetch", model.PhaseTimeout, 0)
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 1}, nil)
	if err != nil || !ok {
		t.Errorf("expected (true, nil) for Timeout phase, got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_FailedWithExpressionTrue(t *testing.T) {
	// Expression can override default and retry on Failed.
	tr := makeTaskRun("fetch", model.PhaseFailed, 0)
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return true, nil
	}}
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 3, Expression: "tasks.fetch.phase != 'Succeeded'"}, eval)
	if err != nil || !ok {
		t.Errorf("expected (true, nil) when expression overrides Failed default, got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_ExpressionTrue(t *testing.T) {
	tr := makeTaskRun("fetch", model.PhaseFailed, 0)
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return true, nil
	}}
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 3, Expression: "tasks.fetch.phase == 'Failed'"}, eval)
	if err != nil || !ok {
		t.Errorf("expected (true, nil) when expression returns true, got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_ExpressionFalse(t *testing.T) {
	tr := makeTaskRun("fetch", model.PhaseFailed, 0)
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return false, nil
	}}
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 3, Expression: "tasks.fetch.phase == 'Error'"}, eval)
	if err != nil || ok {
		t.Errorf("expected (false, nil) when expression returns false, got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_ExpressionStringTrue(t *testing.T) {
	tr := makeTaskRun("fetch", model.PhaseError, 0)
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return "true", nil
	}}
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 2, Expression: "some-expr"}, eval)
	if err != nil || !ok {
		t.Errorf("expected (true, nil) for string 'true', got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_ExpressionStringFalse(t *testing.T) {
	tr := makeTaskRun("fetch", model.PhaseError, 0)
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return "false", nil
	}}
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 2, Expression: "some-expr"}, eval)
	if err != nil || ok {
		t.Errorf("expected (false, nil) for string 'false', got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_ExpressionError(t *testing.T) {
	tr := makeTaskRun("fetch", model.PhaseFailed, 0)
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return nil, errors.New("syntax error")
	}}
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 3, Expression: "bad-expr"}, eval)
	if err == nil || ok {
		t.Errorf("expected (false, err) when expression errors, got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_ExpressionNonBool(t *testing.T) {
	tr := makeTaskRun("fetch", model.PhaseFailed, 0)
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		return 42, nil // unexpected type
	}}
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 3, Expression: "some-expr"}, eval)
	if err == nil || ok {
		t.Errorf("expected (false, err) for non-bool result, got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_ExpressionNilEvaluator(t *testing.T) {
	// When expression is set but no evaluator is configured, degrade to unconditional retry.
	tr := makeTaskRun("fetch", model.PhaseFailed, 0)
	ok, err := ShouldRetry(context.Background(), tr, &model.Retry{Limit: 3, Expression: "some-expr"}, nil)
	if err != nil || !ok {
		t.Errorf("expected (true, nil) when eval=nil (graceful degradation), got (%v, %v)", ok, err)
	}
}

func TestShouldRetry_ExpressionReceivesTaskEnv(t *testing.T) {
	// Verify that the expression receives the task's phase in env.
	statusFailed := model.PhaseFailed
	retryZero := 0
	tr := &store.TaskRun{
		RunID:      1,
		TaskName:   "fetch",
		Status:     &statusFailed,
		RetryCount: &retryZero,
	}
	var capturedEnv map[string]any
	eval := &mockEvaluator{fn: func(expr string, env map[string]any) (any, error) {
		capturedEnv = env
		return true, nil
	}}
	_, _ = ShouldRetry(context.Background(), tr, &model.Retry{Limit: 3, Expression: "tasks.fetch.phase == 'Failed'"}, eval)

	if capturedEnv == nil {
		t.Fatal("expected env to be captured")
	}
	if capturedEnv["tasks.fetch.phase"] != string(model.PhaseFailed) {
		t.Errorf("expected tasks.fetch.phase=%q, got %v", model.PhaseFailed, capturedEnv["tasks.fetch.phase"])
	}
}
