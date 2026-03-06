// Package script provides a built-in executor that runs scripts via os/exec.
//
// The script executor spawns a child process to execute inline scripts.
// The runtime field specifies the interpreter (e.g., "bash", "python3", "sh").
//
// Workflow spec config:
//
//	{"runtime": "bash", "source": "echo hello world"}
//
// Security note: This executor runs arbitrary commands on the host.
// Use with caution and proper sandboxing in production.
package script

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/model"
)

// Executor runs scripts via child processes.
type Executor struct {
	// AllowedRuntimes restricts which runtimes can be used.
	// If empty, all runtimes are allowed.
	AllowedRuntimes map[string]bool
}

// New creates a script Executor.
func New() *Executor {
	return &Executor{}
}

// NewWithAllowedRuntimes creates a script Executor that only allows specific runtimes.
func NewWithAllowedRuntimes(runtimes ...string) *Executor {
	allowed := make(map[string]bool, len(runtimes))
	for _, r := range runtimes {
		allowed[r] = true
	}
	return &Executor{AllowedRuntimes: allowed}
}

// Type returns "script".
func (e *Executor) Type() string {
	return "script"
}

// Execute parses the ScriptConfig and runs the script in a child process.
func (e *Executor) Execute(ctx context.Context, req *executor.ExecuteRequest) (*executor.ExecuteResult, error) {
	var cfg model.ScriptConfig
	if err := json.Unmarshal(req.Config, &cfg); err != nil {
		return nil, fmt.Errorf("script executor: invalid config: %w", err)
	}

	if cfg.Runtime == "" {
		return nil, fmt.Errorf("script executor: runtime is required")
	}
	if cfg.Source == "" {
		return nil, fmt.Errorf("script executor: source is required")
	}

	// Check allowed runtimes
	if len(e.AllowedRuntimes) > 0 && !e.AllowedRuntimes[cfg.Runtime] {
		return nil, fmt.Errorf("script executor: runtime %q is not allowed", cfg.Runtime)
	}

	// Build command: runtime -c "source"
	// Common pattern: bash -c "script", python3 -c "script"
	flag := "-c"
	if cfg.Runtime == "python3" || cfg.Runtime == "python" {
		flag = "-c"
	}

	cmd := exec.CommandContext(ctx, cfg.Runtime, flag, cfg.Source)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Pass input parameters as environment variables
	if req.Inputs != nil {
		for _, p := range req.Inputs.Parameters {
			var val string
			if err := json.Unmarshal(p.Value, &val); err != nil {
				val = string(p.Value)
			}
			envKey := fmt.Sprintf("AETHER_INPUT_%s", strings.ToUpper(p.Name))
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", envKey, val))
		}
	}

	err := cmd.Run()

	result := &executor.ExecuteResult{}
	if err != nil {
		if ctx.Err() != nil {
			// Context cancelled (timeout or cancel)
			result.Phase = model.PhaseError
			result.Msg = fmt.Sprintf("script cancelled: %v", ctx.Err())
		} else {
			result.Phase = model.PhaseFailed
			result.Code = cmd.ProcessState.ExitCode()
			result.Msg = strings.TrimSpace(stderr.String())
			if result.Msg == "" {
				result.Msg = err.Error()
			}
		}
	} else {
		result.Phase = model.PhaseSucceeded
		result.Code = 0
		// Capture stdout as output parameter
		output := strings.TrimSpace(stdout.String())
		if output != "" {
			outputJSON, _ := json.Marshal(output)
			result.Outputs = &model.Outputs{
				Phase: model.PhaseSucceeded,
				Parameters: []model.Parameter{
					{Name: "stdout", Value: outputJSON},
				},
			}
		}
	}

	return result, nil
}
