// Package cron defines the scheduling abstraction for CronWorkflow.
package cron

// Scheduler manages cron-based scheduling entries.
// Lifecycle is managed by the Engine; if the implementation also implements
// io.Closer, Engine.Stop() will call Close().
type Scheduler interface {
	// Add registers a cron entry. callback is invoked on each schedule match.
	// timezone is an IANA timezone string (e.g. "UTC", "Asia/Shanghai").
	Add(id string, schedule string, timezone string, callback func()) error

	// Remove unregisters a cron entry. No-op if id is not found.
	Remove(id string)
}
