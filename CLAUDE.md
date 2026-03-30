# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Aether is a pluggable, graph-based workflow engine implementing the "Graph Workflow Protocol" (`aether/v1`). It provides composable templates, DAG orchestration, conditional branching, loop primitives, pluggable executors, retry & timeout policies, and suspend/resume (await). Written in Go 1.24 with **zero external dependencies**.

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

### Hexagonal / Ports-and-Adapters

The core engine (`package aether`) depends only on interfaces. All implementations are injected via functional options (`option.go`):

**Port interfaces** (each in its own package):
- `store.Store` — state persistence (workflow runs, task runs, templates, schemas)
- `broker.TaskBroker` — task dispatch/cancel/fetch/complete between engine and workers
- `executor.Plugin` — task execution logic (e.g., echo, http, shell)
- `expr.Evaluator` — expression evaluation for `when`, `repeatCondition`, `phaseConditions`
- `hook.Notifier` — lifecycle hook notifications
- `idgen.Generator` — unique ID generation
- `artifact.Store` — artifact upload/download
- `secret.Store` — secret retrieval
- `timeout.Watcher` — deadline expiry detection

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
