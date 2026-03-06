// Package idgen defines the ID generation abstraction for aether.
package idgen

// Generator generates globally unique uint64 IDs for workflow runs and task runs.
type Generator interface {
	// Generate returns a new unique ID.
	Generate() uint64
}
