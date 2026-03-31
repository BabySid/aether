# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Aether is a pluggable, graph-based workflow engine implementing the "Graph Workflow Protocol" (`aether/v1`). It provides composable templates, DAG orchestration, conditional branching, loop primitives, pluggable executors, retry & timeout policies, and suspend/resume (await). Written in Go 1.24 with **zero external dependencies**.

## Design Philosophy

### 1. Protocol-First, Not Framework-First

Aether defines a **declarative protocol** (`apiVersion: aether/v1`) rather than an imperative framework. Workflows are expressed as JSON/struct documents with typed resources (`Workflow`, `CronWorkflow`, `WorkflowTemplate`), following the Kubernetes resource model. The engine is a runtime that interprets these documents — not a library callers extend by subclassing or embedding.

This means the workflow definition is the contract. The engine can be swapped, replicated, or versioned independently of the workflows it runs.

### 2. Engine as Pure Scheduler — Never Executor

The engine **schedules and coordinates** but never executes task logic itself. All execution is delegated to pluggable `executor.Plugin` implementations via the `broker.TaskBroker`. The engine's only job is to:

- Determine which tasks are ready to run (DAG dependency satisfaction, loop iteration control)
- Dispatch them to workers via the broker
- React to completions and advance the scope tree

This separation means workers can run in-process, in separate goroutines, or on remote machines — the engine is topology-agnostic.

### 3. Fat Task Assignments — Workers Never Query the Store

`broker.TaskAssignment` carries **everything** a worker needs to execute a task (inputs, secrets, artifacts, executor type, schema). Workers are fully self-contained and never need to call back into the `store.Store`. This eliminates a class of distributed coupling problems and makes workers simple, testable, and independently deployable.

### 4. Scope-Tree as Recursive Composition

TaskRuns form a **parent-child tree** via `ParentRunID`. Every DAG and Loop container is a "scope". Templates are composable: a DAG can contain tasks that reference Loop templates, which contain tasks that reference inner DAGs, arbitrarily deep (bounded by `maxNestedDepth`, default 10).

`advanceScope()` processes one scope at a time, walking upward to parent scopes until the workflow reaches a terminal state. This recursive design means the same scheduling logic handles any nesting depth without special cases.

### 5. Phase Is Engine-Owned

Executors return an integer `ExecCode`. The engine is the **sole writer** of `Phase` values (`Created → Ready → Running → Succeeded|Failed|Error|Timeout|Skipped|Cancelled`). Users can influence phase mapping via `phaseConditions` expressions, but can never set `PhaseSkipped` or `PhaseCancelled` directly — those are exclusively engine semantics (skipped = `when` condition false; cancelled = user-initiated stop).

This keeps the state machine coherent and prevents executors from expressing engine-level concerns.

### 6. Optimistic Concurrency as the Consistency Model

All mutable state updates use a **Token-based optimistic locking** pattern (`store.WorkflowRun.Token`, `store.TaskRun.Token`). Concurrent writers (multiple engine instances, cancel races, duplicate callbacks) compete via the store's token check — the first writer wins, subsequent ones are silently dropped. No distributed locks, no transactions beyond single-record CAS.

This makes the engine safe to run as multiple replicas and keeps the `store.Store` interface simple to implement.

### 7. Hexagonal Architecture — Dependencies Flow Inward

The core engine (`package aether`) depends only on **interfaces**. All implementations are injected via functional options (`option.go`). The ports are each in their own package with minimal surface area:

- `store.Store` — state persistence
- `broker.TaskBroker` — task dispatch/cancel/fetch/complete
- `executor.Plugin` — task execution logic
- `expr.Evaluator` — expression evaluation (`when`, `repeatCondition`, `phaseConditions`)
- `hook.Notifier` — lifecycle notifications
- `idgen.Generator` — unique ID generation
- `artifact.Store` — artifact upload/download
- `secret.Store` — secret retrieval
- `timeout.Watcher` — deadline expiry detection
- `vars.Source` — variable injection (e.g. `system.os`, `system.arch`)

Zero external dependencies is a corollary of this: the core engine never imports third-party packages, so every interface implementation is either provided by the caller or included as an optional adapter.

### 8. Declarative Binding — Inputs Flow, Not Side Effects

Parameter values flow through the workflow via **declarative binding**: `{{inputs.parameters.name}}` interpolation and expression evaluation. Tasks cannot reach sideways into sibling task state; they can only receive values explicitly wired through `arguments`. This makes data flow visible in the workflow document and testable without running the engine.

### 9. Suspend/Resume as First-Class Primitive

Executors can return `ExecCodeSuspended` to pause mid-execution. The task stays in `PhaseRunning`; the engine accumulates partial outputs on each `Resume()` call (last-writer-wins merge). This models human-approval gates, external callback patterns, and long-running interactive tasks without polling.

---

## Build & Test Commands

```bash
# Run all tests
go test ./...

# Run a single test
go test ./internal/ -run TestValidate

# Run integration tests (uses example workflows in cmd/playground/examples/)
go test ./cmd/playground/

# Build the playground CLI
go build -o playground ./cmd/playground/

# Run a workflow through the playground
./playground -workflow cmd/playground/examples/single-task.json [-out report.html] [-result result.json] [-timeout 60]
```

No Makefile or external build tooling — standard `go` commands only.

## Architecture

### Core Engine Files

- `engine.go` — `Engine` struct: `New()`, `Submit()`, `Get()`, `Resume()`, `Cancel()`, `Start()`/`Stop()`
- `engine_sched.go` — `advanceScope()`: the core iterative scheduling loop that walks the scope tree
- `engine_dag.go` — DAG scheduling: `createEligibleTasks()`, `dispatchLeafTask()`, `resolveDAGInputs()`
- `engine_loop.go` — Loop scheduling: `startLoopController()`, `trySpawnNextIterations()`, `resolveLoopInputs()`

### Three Template Types

- **`dag`** — directed acyclic graph of tasks (with `dependencies` edges)
- **`task`** — leaf executor invocation
- **`loop`** — iteration via `items`/`itemsFrom`/`repeatCondition`

### Key Design Patterns

**Scope-tree scheduling**: TaskRuns form a tree via `ParentRunID`. Each DAG/Loop container is a "scope". `advanceScope()` processes a scope then walks upward to parent scopes until the workflow finalizes.

**Optimistic concurrency**: All mutable state updates use a `Token`-based optimistic locking pattern. The store decides token semantics (version counter, timestamp, etc.).

**Phase is engine-owned**: Executors return an `ExecCode` (integer). The engine maps codes to `Phase` values, optionally overridden by user-defined `PhaseConditions` expressions.

**Fat task assignments**: `broker.TaskAssignment` carries all information needed to execute a task. Workers never query the Store directly.

**Suspend/resume (await)**: Executors can return `ExecCodeSuspended`. The task stays in `PhaseRunning` with partial outputs. `Engine.Resume()` merges new payload and re-dispatches.

### Model Layer (`model/`)

- `Phase` state machine: `Created → Ready → Running → (Succeeded|Failed|Error|Timeout|Skipped|Cancelled)`
- Names follow DNS-1123 label rules (lowercase alphanumeric + hyphens, max 63 chars)
- Maximum template nesting depth: 10 (configurable via `spec.maxNestedDepth`)

### Internal Helpers (`internal/`)

- `dag.go` — `FindReadyTasks`, `FindTemplate`, `FindTask`
- `binding/` — parameter resolution: interpolation, expression evaluation, environment building
- `validate.go` — structural/semantic workflow validation
- `retry.go`, `timeout.go`, `merge.go`, `phase.go`, `hooks.go`, `defaults.go`, `task_decl.go`

### Testing Patterns

- Standard table-driven tests in `internal/` and `executor/`
- Integration tests in `cmd/playground/`: data-driven, reads `examples/*.json` workflows and matches against `examples/assertions/*.json` for expected phase, task count, and per-task outputs
- Example workflows cover: single tasks, linear/parallel DAGs, when-conditions, continue-on-failure, static loops, loop aggregation, nested DAGs, retry, workflow arguments, suspend/resume, timeouts, and DAG inputs

### Playground CLI (`cmd/playground/`)

The only executable. Wires in-memory implementations of all interfaces: `MemoryStore`, `LocalBroker`, `AtomicIDGen`, `SimpleEvaluator`, `PollingWatcher`, `EchoExecutor`. Used for integration testing and experimentation.
