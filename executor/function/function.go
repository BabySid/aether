// Package function provides a built-in executor that invokes registered Go functions.
//
// Usage:
//
//	funcExec := function.New()
//	funcExec.Register("myFunc", myHandler)
//	engine, _ := aether.New(aether.WithExecutor(funcExec), ...)
//
// The executor config in the workflow spec should be:
//
//	{"name": "myFunc", "arguments": {"key": "value"}}
package function

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/model"
)

// Handler is a Go function that can be invoked by the function executor.
// It receives a context and a request, and returns a result or an error.
type Handler func(ctx context.Context, req *Request) (*Response, error)

// Request is the input to a function handler.
type Request struct {
	TaskRunID uint64
	Name      string
	Arguments map[string]any
	Inputs    *model.Inputs
}

// Response is the output from a function handler.
type Response struct {
	Phase   model.Phase
	Code    int
	Msg     string
	Outputs *model.Outputs
}

// Executor invokes registered Go functions.
type Executor struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// New creates a function Executor.
func New() *Executor {
	return &Executor{
		handlers: make(map[string]Handler),
	}
}

// Register adds a named function handler.
func (e *Executor) Register(name string, handler Handler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[name] = handler
}

// Type returns "function".
func (e *Executor) Type() string {
	return "function"
}

// Execute parses the FunctionConfig, looks up the handler, and invokes it.
func (e *Executor) Execute(ctx context.Context, req *executor.ExecuteRequest) (*executor.ExecuteResult, error) {
	var cfg model.FunctionConfig
	if err := json.Unmarshal(req.Config, &cfg); err != nil {
		return nil, fmt.Errorf("function executor: invalid config: %w", err)
	}

	e.mu.RLock()
	handler, ok := e.handlers[cfg.Name]
	e.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("function executor: handler %q not registered", cfg.Name)
	}

	// Parse arguments from config
	var args map[string]any
	if len(cfg.Arguments) > 0 {
		_ = json.Unmarshal(cfg.Arguments, &args)
	}

	funcReq := &Request{
		TaskRunID: req.TaskRunID,
		Name:      cfg.Name,
		Arguments: args,
		Inputs:    req.Inputs,
	}

	resp, err := handler(ctx, funcReq)
	if err != nil {
		return &executor.ExecuteResult{
			Phase: model.PhaseError,
			Msg:   err.Error(),
		}, nil
	}

	if resp == nil {
		return &executor.ExecuteResult{
			Phase: model.PhaseSucceeded,
		}, nil
	}

	return &executor.ExecuteResult{
		Phase:   resp.Phase,
		Code:    resp.Code,
		Msg:     resp.Msg,
		Outputs: resp.Outputs,
	}, nil
}
