// Package idgen defines the ID generation abstraction for aether.
package idgen

// Generator generates globally unique string IDs for workflow runs and task runs.
// Using string IDs decouples the framework from any specific numeric scheme and
// allows implementations to use UUIDs, Snowflake IDs, or monotonic counters.
type Generator interface {
	// Generate returns a new unique ID as a string.
	Generate() string
}
