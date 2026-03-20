// Command playground executes a workflow JSON file locally and produces an HTML report.
//
// Usage:
//
//	playground -workflow path/to/workflow.json [-out report.html] [-timeout 60]
//
// Flags:
//
//	-workflow   Path to the workflow JSON file (required).
//	-out        Output HTML report path (default: aether-report.html).
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
)

func main() {
	wfPath := flag.String("workflow", "", "Path to workflow JSON file (required)")
	outPath := flag.String("out", "aether-report.html", "Output HTML report path")
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
	// All executor types are mocked with NoopExecutor so that any workflow JSON
	// can run without real business logic or external dependencies.
	reg := executor.NewRegistry()
	_ = reg.Register(newNoop("function"))
	_ = reg.Register(newNoop("script"))
	_ = reg.Register(newNoop("await"))
	_ = reg.Register(newNoop("noop"))

	// --- 3. Build store with audit wrapper ---
	auditStore := NewAuditStore(NewMemoryStore())

	// --- 4. Wire engine + broker ---
	// The broker needs engine callbacks; the engine needs the broker.
	// We capture *Engine in a var and reference it lazily in closures.
	// This is safe: task execution only starts after eng.Submit(), which is
	// called after the engine is fully constructed.
	var eng *aether.Engine
	finishCh := make(chan struct{}, 64)

	brok := NewLocalBroker(
		reg,
		func(ctx context.Context, taskRunID uint64) {
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

	eng, err = aether.New(
		aether.WithStore(auditStore),
		aether.WithIDGenerator(NewAtomicIDGen()),
		aether.WithExprEvaluator(NewSimpleEvaluator()),
		aether.WithTaskBroker(brok),
		aether.WithExecutor(newNoop("function")),
		aether.WithExecutor(newNoop("script")),
		aether.WithExecutor(newNoop("await")),
		aether.WithExecutor(newNoop("noop")),
	)
	if err != nil {
		log.Fatalf("create engine: %v", err)
	}

	// --- 5. Submit workflow ---
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutSec)*time.Second)
	defer cancel()

	startTime := time.Now()
	runID, err := eng.Submit(ctx, &wf)
	if err != nil {
		log.Fatalf("submit workflow: %v", err)
	}
	log.Printf("workflow submitted: runID=%d", runID)

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
		if exec.Phase.IsTerminal() {
			finalExec = exec
			break loop
		}
		select {
		case <-ctx.Done():
			log.Printf("timeout waiting for workflow; last phase: %s", exec.Phase)
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
		finalExec.Phase, elapsed.Round(time.Millisecond), finalExec.Progress)

	// --- 7. Collect snapshots ---
	snapshots := auditStore.Snapshots()

	// --- 8. Generate HTML report ---
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
		log.Fatalf("write report: %v", err)
	}
	log.Printf("report written to %s", *outPath)
}
