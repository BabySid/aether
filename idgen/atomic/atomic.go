// Package atomic provides a simple atomic-increment ID generator.
package atomic

import "sync/atomic"

// Generator generates monotonically increasing uint64 IDs.
type Generator struct {
	counter atomic.Uint64
}

// New creates an atomic Generator.
func New() *Generator {
	return &Generator{}
}

// Generate returns the next ID (starting from 1).
func (g *Generator) Generate() uint64 {
	return g.counter.Add(1)
}
