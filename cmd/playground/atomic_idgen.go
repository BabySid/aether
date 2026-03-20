// atomic_idgen.go — monotonically-increasing uint64 ID generator.
package main

import "sync/atomic"

// AtomicIDGen generates monotonically increasing uint64 IDs.
type AtomicIDGen struct {
	counter atomic.Uint64
}

func NewAtomicIDGen() *AtomicIDGen { return &AtomicIDGen{} }

// Generate returns the next ID (starting from 1).
func (g *AtomicIDGen) Generate() uint64 { return g.counter.Add(1) }
