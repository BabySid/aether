package aether

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/BabySid/aether/errsink"
	"github.com/BabySid/aether/internal"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// maxMissedSchedules is the upper bound on missed cron triggers. If more schedules
// are missed, the CronWorkflow sets a MissedSchedule condition and skips.
const maxMissedSchedules = 100

// triggerCronRun is the cron callback. It implements the trigger flow from the spec.
func (e *Engine) triggerCronRun(cronID string) {
	ctx := context.Background()

	// 1. Load CronWorkflow record.
	record, err := e.store.GetCronWorkflow(ctx, cronID)
	if err != nil {
		e.reportError(ctx, err, errsink.ErrorContext{
			Operation: "cronTrigger.getCronWorkflow", Severity: errsink.SeverityWarning,
		})
		return
	}

	var cw model.CronWorkflow
	if err := json.Unmarshal(record.CronWorkflow, &cw); err != nil {
		e.reportError(ctx, err, errsink.ErrorContext{
			Operation: "cronTrigger.unmarshal", Severity: errsink.SeverityError,
		})
		return
	}
	internal.FillCronDefaults(&cw)

	now := time.Now()

	// 2. Check Suspend.
	if cw.Spec.Suspend {
		return
	}

	// 3. Check now < StartAt.
	if cw.Spec.StartAt != "" {
		if startAt, err := time.Parse(time.RFC3339, cw.Spec.StartAt); err == nil && now.Before(startAt) {
			return
		}
	}

	// 4. Check now > EndAt → auto-suspend.
	if cw.Spec.EndAt != "" {
		if endAt, err := time.Parse(time.RFC3339, cw.Spec.EndAt); err == nil && now.After(endAt) {
			e.cronScheduler.Remove(cronID)
			return
		}
	}

	// 5. Missed schedule check.
	if record.Status != nil && record.Status.LastScheduleTime != nil {
		missedCount := countMissedSchedules(cw.Spec.Schedule, cw.Spec.Timezone, *record.Status.LastScheduleTime, now)
		if missedCount > maxMissedSchedules {
			e.reportError(ctx, fmt.Errorf("missed %d schedules (>%d), skipping", missedCount, maxMissedSchedules), errsink.ErrorContext{
				Operation: "cronTrigger.missedSchedule", Severity: errsink.SeverityWarning,
			})
			return
		}
	}

	// 6. Check StartingDeadlineSeconds.
	if cw.Spec.StartingDeadlineSeconds != nil {
		deadline := time.Duration(*cw.Spec.StartingDeadlineSeconds) * time.Second
		if record.Status != nil && record.Status.LastScheduleTime != nil {
			elapsed := now.Sub(*record.Status.LastScheduleTime)
			if elapsed > deadline {
				return
			}
		}
	}

	// 7. ConcurrencyPolicy.
	runs, err := e.store.ListWorkflowRunsByCronID(ctx, cronID)
	if err != nil {
		e.reportError(ctx, err, errsink.ErrorContext{
			Operation: "cronTrigger.listRuns", Severity: errsink.SeverityWarning,
		})
		return
	}
	activeRuns := filterActiveRuns(runs)

	switch cw.Spec.ConcurrencyPolicy {
	case model.ConcurrencyForbid:
		if len(activeRuns) > 0 {
			return
		}
	case model.ConcurrencyReplace:
		for _, r := range activeRuns {
			if cancelErr := e.Cancel(ctx, r.RunID); cancelErr != nil {
				e.reportError(ctx, cancelErr, errsink.ErrorContext{
					WorkflowRunID: r.RunID,
					Operation:     "cronTrigger.cancelReplace", Severity: errsink.SeverityWarning,
				})
			}
		}
	}
	// ConcurrencyAllow: continue regardless.

	// 8. Construct Workflow and submit.
	wf := &model.Workflow{
		APIVersion: cw.APIVersion,
		Kind:       "Workflow",
		Metadata:   cw.Metadata,
		Spec:       cw.Spec.WorkflowSpec,
	}
	runID, submitErr := e.submitInternal(ctx, wf, cronID)
	if submitErr != nil {
		e.reportError(ctx, submitErr, errsink.ErrorContext{
			Operation: "cronTrigger.submit", Severity: errsink.SeverityError,
		})
		return
	}

	// 9. Update LastScheduleTime and LastSubmittedRunID.
	e.updateCronStatus(ctx, cronID, now, runID)

	// 10. History cleanup.
	e.cleanupCronHistory(ctx, cronID, cw.Spec.SuccessfulJobsHistoryLimit, cw.Spec.FailedJobsHistoryLimit)
}

// reloadCronWorkflows re-registers all non-suspended CronWorkflows with the scheduler.
// Called during Engine.Start() for crash recovery.
func (e *Engine) reloadCronWorkflows(ctx context.Context) error {
	records, err := e.store.ListCronWorkflows(ctx)
	if err != nil {
		return fmt.Errorf("list CronWorkflows: %w", err)
	}

	for _, record := range records {
		var cw model.CronWorkflow
		if err := json.Unmarshal(record.CronWorkflow, &cw); err != nil {
			continue
		}
		internal.FillCronDefaults(&cw)
		if cw.Spec.Suspend {
			continue
		}
		cronID := record.CronID
		if err := e.cronScheduler.Add(cronID, cw.Spec.Schedule, cw.Spec.Timezone, func() {
			e.triggerCronRun(cronID)
		}); err != nil {
			e.reportError(ctx, err, errsink.ErrorContext{
				Operation: "reloadCronWorkflows.add", Severity: errsink.SeverityWarning,
			})
		}
	}
	return nil
}

// countMissedSchedules counts how many cron triggers were missed between lastSchedule and now.
// This is a simplified implementation that estimates based on the schedule string.
func countMissedSchedules(schedule, timezone string, lastSchedule, now time.Time) int {
	interval := estimateCronInterval(schedule)
	if interval <= 0 {
		return 0
	}
	elapsed := now.Sub(lastSchedule)
	if elapsed <= 0 {
		return 0
	}
	return int(elapsed / interval)
}

// estimateCronInterval provides a rough estimate of the cron interval duration.
// For precise scheduling, the cron.Scheduler implementation handles exact timing.
func estimateCronInterval(schedule string) time.Duration {
	schedule = strings.TrimSpace(schedule)

	// Handle macros.
	switch schedule {
	case "@yearly", "@annually":
		return 365 * 24 * time.Hour
	case "@monthly":
		return 30 * 24 * time.Hour
	case "@weekly":
		return 7 * 24 * time.Hour
	case "@daily", "@midnight":
		return 24 * time.Hour
	case "@hourly":
		return time.Hour
	}

	// For 5-field cron, use a simple heuristic based on the minute field.
	fields := strings.Fields(schedule)
	if len(fields) != 5 {
		return 0
	}

	// Check minute field for */N pattern.
	if strings.HasPrefix(fields[0], "*/") {
		var n int
		if _, err := fmt.Sscanf(fields[0], "*/%d", &n); err == nil && n > 0 {
			return time.Duration(n) * time.Minute
		}
	}

	// If minute is a specific number and hour is *, it's roughly hourly.
	if fields[1] == "*" {
		return time.Hour
	}

	// If both minute and hour are specific, it's roughly daily.
	return 24 * time.Hour
}

// filterActiveRuns returns non-terminal workflow runs.
func filterActiveRuns(runs []*store.WorkflowRun) []*store.WorkflowRun {
	var active []*store.WorkflowRun
	for _, r := range runs {
		if r.Status == nil || !r.Status.IsTerminal() {
			active = append(active, r)
		}
	}
	return active
}

// updateCronStatus updates the LastScheduleTime and LastSubmittedRunID of a CronWorkflow.
func (e *Engine) updateCronStatus(ctx context.Context, cronID string, t time.Time, runID string) {
	record, err := e.store.GetCronWorkflow(ctx, cronID)
	if err != nil {
		return
	}
	if record.Status == nil {
		record.Status = &store.CronWorkflowStatus{}
	}
	record.Status.LastScheduleTime = &t
	record.Status.LastSubmittedRunID = runID
	if err := e.store.UpdateCronWorkflow(ctx, record); err != nil {
		e.reportError(ctx, err, errsink.ErrorContext{
			Operation: "updateCronStatus", Severity: errsink.SeverityWarning,
		})
	}
}

// cleanupCronHistory removes old completed WorkflowRuns beyond the history limits.
func (e *Engine) cleanupCronHistory(ctx context.Context, cronID string, successLimit, failedLimit int) {
	runs, err := e.store.ListWorkflowRunsByCronID(ctx, cronID)
	if err != nil {
		return
	}

	var succeeded, failed []*store.WorkflowRun
	for _, r := range runs {
		if r.Status == nil || !r.Status.IsTerminal() {
			continue
		}
		switch *r.Status {
		case model.PhaseSucceeded:
			succeeded = append(succeeded, r)
		case model.PhaseFailed, model.PhaseError, model.PhaseTimeout:
			failed = append(failed, r)
		}
	}

	// Sort by creation time descending (newest first).
	sort.Slice(succeeded, func(i, j int) bool { return succeeded[i].CreatedAt.After(succeeded[j].CreatedAt) })
	sort.Slice(failed, func(i, j int) bool { return failed[i].CreatedAt.After(failed[j].CreatedAt) })

	// Delete excess runs beyond the limit.
	// DeleteWorkflowRun cascade-deletes associated TaskRuns.
	for i := successLimit; i < len(succeeded); i++ {
		if err := e.store.DeleteWorkflowRun(ctx, succeeded[i].RunID); err != nil {
			e.reportError(ctx, err, errsink.ErrorContext{
				WorkflowRunID: succeeded[i].RunID,
				Operation:     "cleanupCronHistory.deleteSucceeded", Severity: errsink.SeverityWarning,
			})
		}
	}
	for i := failedLimit; i < len(failed); i++ {
		if err := e.store.DeleteWorkflowRun(ctx, failed[i].RunID); err != nil {
			e.reportError(ctx, err, errsink.ErrorContext{
				WorkflowRunID: failed[i].RunID,
				Operation:     "cleanupCronHistory.deleteFailed", Severity: errsink.SeverityWarning,
			})
		}
	}
}
