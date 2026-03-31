// Command playground executes a workflow JSON file locally.
//
// Usage:
//
//	playground -workflow path/to/workflow.json [-out report.html] [-result result.json] [-timeout 60]
//
// Flags:
//
//	-workflow   Path to the workflow JSON file (required).
//	-out        Output HTML report path (optional; omit or set "" to skip HTML generation).
//	-result     Output machine-readable JSON result path (optional).
//	-timeout    Maximum seconds to wait for the workflow to finish (default: 60).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	aether "github.com/BabySid/aether"
	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/vars"
)

func main() {
	wfPath := flag.String("workflow", "", "Path to workflow JSON file (required)")
	outPath := flag.String("out", "", "Output HTML report path (optional; empty = skip)")
	resultPath := flag.String("result", "", "Output machine-readable JSON result path (optional)")
	timeoutSec := flag.Int("timeout", 5, "Max seconds to wait for workflow completion")
	flag.Parse()

	if *wfPath == "" {
		fmt.Fprintln(os.Stderr, "error: -workflow is required")
		flag.Usage()
		os.Exit(1)
	}

	// --- 1. Load workflow JSON ---
	raw, err := os.ReadFile(*wfPath)
	if err != nil {
		log.Fatalf("read workflow: %v", err)
	}
	var wf model.Workflow
	if err := json.Unmarshal(raw, &wf); err != nil {
		log.Fatalf("parse workflow JSON: %v", err)
	}

	// --- 2. Build executor registry ---
	// "echo" executor uses the built-in EchoExecutor.
	reg := executor.NewRegistry()
	_ = reg.Register(newEcho())

	// --- 3. Build store ---
	memStore := NewMemoryStore()

	// --- 4. Wire engine + broker ---
	// The broker needs engine callbacks; the engine needs the broker.
	// We capture *Engine in a var and reference it lazily in closures.
	// This is safe: task execution only starts after eng.Submit(), which is
	// called after the engine is fully constructed.
	var eng *aether.Engine
	finishCh := make(chan struct{}, 64)

	brok := NewLocalBroker(
		reg,
		func(ctx context.Context, taskRunID string) {
			eng.OnTaskStarted(ctx, taskRunID)
		},
		func(ctx context.Context, result *broker.TaskResult) {
			eng.OnTaskCompleted(ctx, result)
			select {
			case finishCh <- struct{}{}:
			default:
			}
		},
	)
	// Wire auto-resume: allows echo tasks with autoResumeAfter to call
	// eng.Resume() automatically after a delay, demonstrating suspend/resume
	// in a single CLI run without external tooling.
	brok.SetResumeFunc(func(ctx context.Context, workflowRunID, taskRunID string, payload map[string]any) error {
		return eng.Resume(ctx, workflowRunID, taskRunID, payload)
	})

	eng, err = aether.New(
		aether.WithStore(memStore),
		aether.WithIDGenerator(NewAtomicIDGen()),
		aether.WithExprEvaluator(NewSimpleEvaluator()),
		aether.WithTaskBroker(brok),
		aether.WithExecutorRegistry(reg),
		aether.WithTimeoutWatcher(newPollingWatcher(memStore, 2*time.Second)),
		// Built-in VarsSources: system.* namespace (os, arch, hostname, …)
		// is always available so workflows can reference {{system.os}} etc.
		aether.WithVarsSource(&vars.SystemSource{}),
	)
	if err != nil {
		log.Fatalf("create engine: %v", err)
	}

	// --- 5. Submit workflow ---
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutSec)*time.Second)
	defer cancel()

	if startErr := eng.Start(ctx); startErr != nil {
		log.Fatalf("start engine: %v", startErr)
	}
	defer eng.Stop()

	startTime := time.Now()
	runID, err := eng.Submit(ctx, &wf)
	if err != nil {
		log.Fatalf("submit workflow: %v", err)
	}
	log.Printf("workflow submitted: runID=%s", runID)

	// --- 6. Wait for workflow to reach terminal state ---
	var finalExec *aether.WorkflowExecution
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

loop:
	for {
		exec, getErr := eng.Get(ctx, runID)
		if getErr != nil {
			log.Fatalf("get workflow: %v", getErr)
		}
		if exec.Phase().IsTerminal() {
			finalExec = exec
			break loop
		}
		select {
		case <-ctx.Done():
			log.Printf("timeout waiting for workflow; last phase: %s", exec.Phase())
			finalExec = exec
			break loop
		case <-finishCh:
			for len(finishCh) > 0 {
				<-finishCh
			}
		case <-ticker.C:
			// fallback poll
		}
	}

	elapsed := time.Since(startTime)
	log.Printf("workflow finished: phase=%s elapsed=%s progress=%s",
		finalExec.Phase(), elapsed.Round(time.Millisecond), finalExec.Progress)

	// --- 7. Collect snapshots ---
	snapshots := memStore.Snapshots()

	// --- 8. Generate HTML report (optional) ---
	if *outPath != "" {
		report := generateHTMLReport(ReportData{
			WorkflowPath: *wfPath,
			RunID:        runID,
			StartTime:    startTime,
			Elapsed:      elapsed,
			Execution:    finalExec,
			Snapshots:    snapshots,
			RawWorkflow:  raw,
		})
		if err := os.WriteFile(*outPath, []byte(report), 0o644); err != nil {
			log.Fatalf("write HTML report: %v", err)
		}
		log.Printf("HTML report written to %s", *outPath)
	}

	// --- 9. Write machine-readable JSON result (optional) ---
	if *resultPath != "" {
		result := generateResult(*wfPath, runID, startTime, elapsed, finalExec)
		resultJSON, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			log.Fatalf("marshal result JSON: %v", err)
		}
		if err := os.WriteFile(*resultPath, resultJSON, 0o644); err != nil {
			log.Fatalf("write result JSON: %v", err)
		}
		log.Printf("result JSON written to %s", *resultPath)
	}
}
