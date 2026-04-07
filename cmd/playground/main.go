// Command playground executes a workflow or CronWorkflow JSON file locally.
//
// Usage:
//
//	playground -workflow path/to/workflow.json [-out report.html] [-result result.json] [-verify assertions.json] [-timeout 60]
//
// Flags:
//
//	-workflow   Path to the workflow/CronWorkflow JSON file (required).
//	-out        Output HTML report path (optional; omit or set "" to skip HTML generation).
//	-result     Output machine-readable JSON result path (optional).
//	-verify     Assertion JSON file path for automated verification (optional).
//	-timeout    Maximum seconds to wait for the workflow to finish (default: 5).
//
// The CLI auto-detects the resource kind from the JSON "kind" field:
//   - "Workflow" (or absent) → runs as a normal Workflow.
//   - "CronWorkflow" → submits a CronWorkflow, fires triggers, and verifies.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	aether "github.com/BabySid/aether"
	"github.com/BabySid/aether/broker"
	"github.com/BabySid/aether/executor"
	"github.com/BabySid/aether/internal"
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/vars"
)

// DeploymentSource is a custom vars.Source that exposes deployment metadata
// under the "deploy" namespace. Registered by default in playground so that
// example 14-custom-vars can be verified without external tooling.
type DeploymentSource struct {
	Cluster string
	Region  string
}

func (d *DeploymentSource) Namespace() string { return "deploy" }

func (d *DeploymentSource) Vars() map[string]any {
	return map[string]any{
		"deploy.cluster": d.Cluster,
		"deploy.region":  d.Region,
	}
}

var _ vars.Source = (*DeploymentSource)(nil)

// immediateScheduler is a cron.Scheduler that stores callbacks for manual
// triggering via Fire(). Used by the playground CLI for CronWorkflow testing.
type immediateScheduler struct {
	mu        sync.Mutex
	callbacks map[string]func()
}

func newImmediateScheduler() *immediateScheduler {
	return &immediateScheduler{callbacks: make(map[string]func())}
}

func (s *immediateScheduler) Start(_ context.Context) error { return nil }
func (s *immediateScheduler) Stop()                         {}

func (s *immediateScheduler) Add(id, _, _ string, callback func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callbacks[id] = callback
	return nil
}

func (s *immediateScheduler) Remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.callbacks, id)
}

func (s *immediateScheduler) Fire(id string) {
	s.mu.Lock()
	cb := s.callbacks[id]
	s.mu.Unlock()
	if cb != nil {
		cb()
	}
}

func main() {
	wfPath := flag.String("workflow", "", "Path to workflow JSON file (required)")
	outPath := flag.String("out", "", "Output HTML report path (optional; empty = skip)")
	resultPath := flag.String("result", "", "Output machine-readable JSON result path (optional)")
	verifyPath := flag.String("verify", "", "Assertion JSON file for automated verification (optional)")
	timeoutSec := flag.Int("timeout", 5, "Max seconds to wait for workflow completion")
	flag.Parse()

	if *wfPath == "" {
		fmt.Fprintln(os.Stderr, "error: -workflow is required")
		flag.Usage()
		os.Exit(1)
	}

	// --- 1. Load JSON and detect kind ---
	raw, err := os.ReadFile(*wfPath)
	if err != nil {
		log.Fatalf("read workflow: %v", err)
	}

	var peek struct {
		Kind string `json:"kind"`
	}
	_ = json.Unmarshal(raw, &peek)

	// --- 2. Load assertion (if -verify) ---
	var wfAssertion *WorkflowAssertion
	var cronAssertion *CronAssertion
	if *verifyPath != "" {
		assertRaw, err := os.ReadFile(*verifyPath)
		if err != nil {
			log.Fatalf("read assertion file: %v", err)
		}
		if peek.Kind == "CronWorkflow" {
			var ca CronAssertion
			if err := json.Unmarshal(assertRaw, &ca); err != nil {
				log.Fatalf("parse cron assertion: %v", err)
			}
			cronAssertion = &ca
		} else {
			var wa WorkflowAssertion
			if err := json.Unmarshal(assertRaw, &wa); err != nil {
				log.Fatalf("parse assertion: %v", err)
			}
			wfAssertion = &wa
		}
	}

	if peek.Kind == "CronWorkflow" {
		runCronWorkflow(raw, cronAssertion, *timeoutSec)
	} else {
		runWorkflow(raw, *wfPath, *outPath, *resultPath, wfAssertion, *timeoutSec)
	}
}

// runWorkflow handles the normal Workflow path.
func runWorkflow(raw []byte, wfPath, outPath, resultPath string, assertion *WorkflowAssertion, timeoutSec int) {
	var wf model.Workflow
	if err := json.Unmarshal(raw, &wf); err != nil {
		log.Fatalf("parse workflow JSON: %v", err)
	}

	eng, memStore, finishCh, ctx, cancel := buildEngine(timeoutSec, nil)
	defer cancel()
	defer eng.Stop()

	startTime := time.Now()

	// Handle expectSubmitError
	runID, err := eng.Submit(ctx, &wf)
	if assertion != nil && assertion.ExpectSubmitError != "" {
		if err == nil {
			log.Fatalf("FAIL: expected Submit error containing %q, but Submit succeeded", assertion.ExpectSubmitError)
		}
		if !strings.Contains(err.Error(), assertion.ExpectSubmitError) {
			log.Fatalf("FAIL: Submit error %q does not contain %q", err.Error(), assertion.ExpectSubmitError)
		}
		log.Printf("PASS: Submit correctly rejected: %v", err)
		return
	}
	if err != nil {
		log.Fatalf("submit workflow: %v", err)
	}
	log.Printf("workflow submitted: runID=%s", runID)

	// Start test orchestration (auto-resume/auto-cancel) if in verify mode
	if assertion != nil {
		startOrchestration(ctx, eng, runID, assertion.AutoResumeAfter, assertion.AutoCancelAfter)
	}

	// Wait for workflow to reach terminal state
	finalExec := waitForTerminal(ctx, eng, runID, finishCh)
	elapsed := time.Since(startTime)
	log.Printf("workflow finished: phase=%s elapsed=%s progress=%s",
		finalExec.Status, elapsed.Round(time.Millisecond), finalExec.Progress)

	// Generate HTML report (optional)
	if outPath != "" {
		snapshots := memStore.Snapshots()
		report := generateHTMLReport(ReportData{
			WorkflowPath: wfPath,
			RunID:        runID,
			StartTime:    startTime,
			Elapsed:      elapsed,
			Execution:    finalExec,
			Snapshots:    snapshots,
			RawWorkflow:  raw,
		})
		if err := os.WriteFile(outPath, []byte(report), 0o644); err != nil {
			log.Fatalf("write HTML report: %v", err)
		}
		log.Printf("HTML report written to %s", outPath)
	}

	// Write machine-readable JSON result (optional)
	if resultPath != "" {
		result := generateResult(wfPath, runID, startTime, elapsed, finalExec)
		resultJSON, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			log.Fatalf("marshal result JSON: %v", err)
		}
		if err := os.WriteFile(resultPath, resultJSON, 0o644); err != nil {
			log.Fatalf("write result JSON: %v", err)
		}
		log.Printf("result JSON written to %s", resultPath)
	}

	// Verify (optional)
	if assertion != nil {
		failures := Verify(finalExec, assertion)
		if len(failures) > 0 {
			for _, f := range failures {
				log.Printf("FAIL: %s", f)
			}
			os.Exit(1)
		}
		log.Printf("PASS: all assertions matched")
	}
}

// runCronWorkflow handles the CronWorkflow path.
func runCronWorkflow(raw []byte, assertion *CronAssertion, timeoutSec int) {
	var cw model.CronWorkflow
	if err := json.Unmarshal(raw, &cw); err != nil {
		log.Fatalf("parse CronWorkflow JSON: %v", err)
	}

	sched := newImmediateScheduler()
	eng, _, _, ctx, cancel := buildEngine(timeoutSec, sched)
	defer cancel()
	defer eng.Stop()

	cronID, err := eng.SubmitCronWorkflow(ctx, &cw)
	if err != nil {
		log.Fatalf("submit CronWorkflow: %v", err)
	}
	log.Printf("CronWorkflow submitted: cronID=%s", cronID)

	// Fire triggers
	triggerCount := 1
	if assertion != nil && assertion.CronTriggerCount > 0 {
		triggerCount = assertion.CronTriggerCount
	}
	for i := range triggerCount {
		sched.Fire(cronID)
		if i < triggerCount-1 {
			time.Sleep(300 * time.Millisecond)
		}
	}
	// Wait for runs to complete
	time.Sleep(500 * time.Millisecond)

	exec, err := eng.GetCronWorkflow(ctx, cronID)
	if err != nil {
		log.Fatalf("get CronWorkflow: %v", err)
	}

	log.Printf("CronWorkflow finished: runs=%d", len(exec.Runs))
	for _, run := range exec.Runs {
		log.Printf("  run=%s phase=%s tasks=%d", run.RunID, run.Status, len(run.Tasks))
	}

	// Verify (optional)
	if assertion != nil {
		failures := VerifyCron(exec, assertion)
		if len(failures) > 0 {
			for _, f := range failures {
				log.Printf("FAIL: %s", f)
			}
			os.Exit(1)
		}
		log.Printf("PASS: all assertions matched")
	}
}

// buildEngine creates a fully-wired engine with all playground components.
// If sched is non-nil, it is registered as the CronScheduler.
func buildEngine(timeoutSec int, sched *immediateScheduler) (*aether.Engine, *MemoryStore, chan struct{}, context.Context, context.CancelFunc) {
	reg := executor.NewRegistry()
	_ = reg.Register(newEcho())

	memStore := NewMemoryStore()

	var eng *aether.Engine
	finishCh := make(chan struct{}, 64)

	brok := NewLocalBroker(
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
	worker := NewLocalWorker("local-worker", brok, reg)
	brok.SetWorker(worker)

	opts := []aether.Option{
		aether.WithStore(memStore),
		aether.WithIDGenerator(NewAtomicIDGen()),
		aether.WithExprEvaluator(NewSimpleEvaluator()),
		aether.WithTaskBroker(brok),
		aether.WithExecutorRegistry(reg),
		aether.WithTimeoutWatcher(newPollingWatcher(memStore, 500*time.Millisecond)),
		aether.WithVarsSource(&vars.SystemSource{}),
		aether.WithVarsSource(&DeploymentSource{
			Cluster: "prod-cluster",
			Region:  "ap-east-1",
		}),
	}
	if sched != nil {
		opts = append(opts, aether.WithCronScheduler(sched))
	}

	var err error
	eng, err = aether.New(opts...)
	if err != nil {
		log.Fatalf("create engine: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	if startErr := eng.Start(ctx); startErr != nil {
		log.Fatalf("start engine: %v", startErr)
	}
	worker.Start(ctx, 4)

	return eng, memStore, finishCh, ctx, cancel
}

// waitForTerminal polls until the workflow reaches a terminal phase or ctx expires.
func waitForTerminal(ctx context.Context, eng *aether.Engine, runID string, finishCh chan struct{}) *aether.WorkflowExecution {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		exec, err := eng.Get(ctx, runID)
		if err != nil {
			log.Fatalf("get workflow: %v", err)
		}
		if exec.Status.IsTerminal() {
			return exec
		}
		select {
		case <-ctx.Done():
			log.Printf("timeout waiting for workflow; last phase: %s", exec.Status)
			return exec
		case <-finishCh:
			for len(finishCh) > 0 {
				<-finishCh
			}
		case <-ticker.C:
		}
	}
}

// startOrchestration sets up auto-resume or auto-cancel for -verify mode.
// It monitors the workflow for suspended tasks and fires the appropriate action.
func startOrchestration(ctx context.Context, eng *aether.Engine, runID string, autoResumeAfter, autoCancelAfter string) {
	if autoResumeAfter == "" && autoCancelAfter == "" {
		return
	}

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		fired := false
		for !fired {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			exec, err := eng.Get(ctx, runID)
			if err != nil || exec.Status.IsTerminal() {
				return
			}

			for _, task := range exec.Tasks {
				if task.Status != model.PhaseSuspended {
					continue
				}
				fired = true

				if autoResumeAfter != "" {
					delay, parseErr := internal.ParseDuration(autoResumeAfter)
					if parseErr != nil {
						log.Printf("invalid autoResumeAfter %q: %v", autoResumeAfter, parseErr)
						return
					}
					taskRunID := task.RunID
					go func() {
						select {
						case <-time.After(delay):
						case <-ctx.Done():
							return
						}
						log.Printf("[orchestrate] auto-resume firing for task %s", taskRunID)
						_ = eng.Resume(ctx, runID, taskRunID, map[string]any{
							resumedMarker: true,
						})
					}()
				}

				if autoCancelAfter != "" {
					delay, parseErr := internal.ParseDuration(autoCancelAfter)
					if parseErr != nil {
						log.Printf("invalid autoCancelAfter %q: %v", autoCancelAfter, parseErr)
						return
					}
					go func() {
						select {
						case <-time.After(delay):
						case <-ctx.Done():
							return
						}
						log.Printf("[orchestrate] auto-cancel firing for workflow %s", runID)
						_ = eng.Cancel(ctx, runID)
					}()
				}

				break // only handle the first suspended task
			}
		}
	}()
}
