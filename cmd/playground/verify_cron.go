// verify_cron.go — CronWorkflow assertion structures and verification logic.
package main

import (
	"fmt"

	aether "github.com/BabySid/aether"
	"github.com/BabySid/aether/model"
)

// CronAssertion is the top-level structure of an assertion file for CronWorkflow.
type CronAssertion struct {
	CronTriggerCount     int             `json:"cronTriggerCount,omitempty"`
	ExpectRunCount       *int            `json:"expectRunCount,omitempty"`
	ExpectRunPhase       string          `json:"expectRunPhase,omitempty"`
	ExpectCancelledCount *int            `json:"expectCancelledCount,omitempty"`
	ExpectTasks          []TaskAssertion `json:"expectTasks,omitempty"`
}

// VerifyCron checks a CronWorkflowExecution against the assertion and returns
// a list of failure descriptions. An empty slice means all checks passed.
func VerifyCron(exec *aether.CronWorkflowExecution, assert *CronAssertion) []string {
	var failures []string
	fail := func(format string, args ...any) {
		failures = append(failures, fmt.Sprintf(format, args...))
	}

	// 1. run count
	if assert.ExpectRunCount != nil {
		if len(exec.Runs) != *assert.ExpectRunCount {
			fail("run count: got %d, want %d", len(exec.Runs), *assert.ExpectRunCount)
		}
	}

	// 2. last run phase
	if assert.ExpectRunPhase != "" && len(exec.Runs) > 0 {
		lastRun := exec.Runs[len(exec.Runs)-1]
		if string(lastRun.Status) != assert.ExpectRunPhase {
			fail("last run phase: got %q, want %q", lastRun.Status, assert.ExpectRunPhase)
		}
	}

	// 3. cancelled count
	if assert.ExpectCancelledCount != nil {
		cancelled := 0
		for _, run := range exec.Runs {
			if run.Status == model.PhaseCancelled {
				cancelled++
			}
		}
		if cancelled != *assert.ExpectCancelledCount {
			fail("cancelled count: got %d, want %d", cancelled, *assert.ExpectCancelledCount)
		}
	}

	// 4. per-task expectations on the last run
	if len(assert.ExpectTasks) > 0 && len(exec.Runs) > 0 {
		lastRun := exec.Runs[len(exec.Runs)-1]
		verifyTasks(&failures, lastRun.Tasks, assert.ExpectTasks)
	}

	return failures
}
