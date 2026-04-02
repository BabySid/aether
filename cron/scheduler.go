// Package cron defines the scheduling abstraction for CronWorkflow.
package cron

import "context"

// Scheduler manages cron-based scheduling entries.
// Lifecycle is managed by the Engine: Engine.Start calls Scheduler.Start,
// and Engine.Stop calls Scheduler.Stop.
type Scheduler interface {
	// Start launches the scheduler's background services.
	// Returns immediately; background work runs until Stop is called.
	Start(ctx context.Context) error

	// Stop shuts down the scheduler's background services.
	// It is safe to call Stop multiple times; only the first call takes effect.
	Stop()

	// Add registers a cron entry. callback is invoked on each schedule match.
	// timezone is an IANA timezone string (e.g. "UTC", "Asia/Shanghai").
	Add(id string, schedule string, timezone string, callback func()) error

	// Remove unregisters a cron entry. No-op if id is not found.
	Remove(id string)
}
