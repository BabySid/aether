// noop.go — a catch-all executor plugin that immediately returns Succeeded.
// Registered for the types "function", "script", "await", and "noop" so that
// any workflow JSON can be run without real business logic.
package main

import (
	"context"

	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/model"
)

// NoopExecutor succeeds immediately for any task, regardless of executor type.
type NoopExecutor struct {
	typ string
}

func newNoop(typ string) *NoopExecutor { return &NoopExecutor{typ: typ} }

func (n *NoopExecutor) Type() string { return n.typ }

func (n *NoopExecutor) Execute(_ context.Context, req *executor.ExecuteRequest) (*executor.ExecuteResult, error) {
	return &executor.ExecuteResult{
		Phase: model.PhaseSucceeded,
		Msg:   "noop: task completed successfully",
		Outputs: &model.Outputs{
			Phase: model.PhaseSucceeded,
		},
	}, nil
}
