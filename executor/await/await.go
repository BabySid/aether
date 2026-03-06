// Package await provides a built-in executor for human-in-the-loop / external wait tasks.
//
// The await executor immediately transitions the task to Running phase.
// The task stays Running until it is explicitly resumed via Engine.Resume().
//
// Workflow spec config:
//
//	{"message": "Waiting for manual approval"}
package await

import (
	"context"
	"encoding/json"

	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/model"
)

// Executor handles await-type tasks.
type Executor struct{}

// New creates an await Executor.
func New() *Executor {
	return &Executor{}
}

// Type returns "await".
func (e *Executor) Type() string {
	return "await"
}

// Execute immediately returns Running phase.
// The task will remain Running until Engine.Resume() is called externally.
func (e *Executor) Execute(_ context.Context, req *executor.ExecuteRequest) (*executor.ExecuteResult, error) {
	var cfg model.AwaitConfig
	if len(req.Config) > 0 {
		_ = json.Unmarshal(req.Config, &cfg)
	}

	return &executor.ExecuteResult{
		Phase: model.PhaseRunning,
		Msg:   cfg.Message,
	}, nil
}
