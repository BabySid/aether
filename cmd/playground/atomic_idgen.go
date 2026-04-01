// atomic_idgen.go — monotonically-increasing string ID generator.
package main

import (
	"fmt"
	"sync/atomic"

	"github.com/BabySid/aether/idgen"
)

// AtomicIDGen generates monotonically increasing string IDs.
type AtomicIDGen struct {
	counter atomic.Uint64
}

func NewAtomicIDGen() *AtomicIDGen { return &AtomicIDGen{} }

// Generate returns the next ID as a string (starting from "1").
func (g *AtomicIDGen) Generate(_ idgen.Context) string {
	return fmt.Sprintf("%d", g.counter.Add(1))
}
