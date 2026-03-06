package internal

import (
	"context"
	"math"
	"time"

	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/model"
)

// RetryExecutor wraps an executor.Plugin with retry logic.
// It respects the Retry policy from the template/task configuration.
type RetryExecutor struct {
	Plugin executor.Plugin
	Retry  *model.Retry
}

// ExecuteWithRetry runs the plugin with retry logic.
// Returns the final result after all retries are exhausted or success is achieved.
func ExecuteWithRetry(ctx context.Context, plugin executor.Plugin, req *executor.ExecuteRequest, retry *model.Retry) (*executor.ExecuteResult, error, int) {
	if retry == nil || retry.Limit <= 0 {
		result, err := plugin.Execute(ctx, req)
		return result, err, 0
	}

	var lastResult *executor.ExecuteResult
	var lastErr error
	retries := 0

	for attempt := 0; attempt <= retry.Limit; attempt++ {
		result, err := plugin.Execute(ctx, req)

		if err == nil && result != nil && result.Phase == model.PhaseSucceeded {
			return result, nil, retries
		}

		lastResult = result
		lastErr = err
		retries = attempt

		// Check if we should retry based on expression
		// (expression evaluation would require ExprEvaluator, skip for now if not set)
		if attempt < retry.Limit {
			// Wait with backoff before next attempt
			delay := computeBackoff(retry.Backoff, attempt)
			if delay > 0 {
				select {
				case <-ctx.Done():
					return lastResult, ctx.Err(), retries
				case <-time.After(delay):
				}
			}
		}
	}

	return lastResult, lastErr, retries
}

// computeBackoff calculates the delay for a retry attempt using exponential backoff.
func computeBackoff(backoff *model.Backoff, attempt int) time.Duration {
	if backoff == nil {
		return 0
	}

	baseDuration, err := ParseDuration(backoff.Duration)
	if err != nil || baseDuration == 0 {
		return time.Second // fallback 1s
	}

	factor := backoff.Factor
	if factor <= 0 {
		factor = 2.0
	}

	delay := float64(baseDuration) * math.Pow(factor, float64(attempt))

	if backoff.MaxDuration != "" {
		maxDuration, err := ParseDuration(backoff.MaxDuration)
		if err == nil && maxDuration > 0 && time.Duration(delay) > maxDuration {
			delay = float64(maxDuration)
		}
	}

	return time.Duration(delay)
}
