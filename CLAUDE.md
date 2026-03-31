# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Aether is a pluggable, graph-based workflow engine implementing the "Graph Workflow Protocol" (`aether/v1`). It provides composable templates, DAG orchestration, conditional branching, loop primitives, pluggable executors, retry & timeout policies, and suspend/resume (await).

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

### 6. Hexagonal Architecture — Dependencies Flow Inward

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

### 7. Declarative Binding — Inputs Flow, Not Side Effects

Parameter values flow through the workflow via **declarative binding**: `{{inputs.parameters.name}}` interpolation and expression evaluation. Tasks cannot reach sideways into sibling task state; they can only receive values explicitly wired through `arguments`. This makes data flow visible in the workflow document and testable without running the engine.

### 8. Suspend/Resume as First-Class Primitive

Executors can return `ExecCodeSuspended` to pause mid-execution. The task transitions to `PhaseSuspended`; the engine accumulates partial outputs on each `Resume()` call (last-writer-wins merge). This models human-approval gates, external callback patterns, and long-running interactive tasks without polling.

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

**Protocol and interfaces as the two load-bearing pillars**: The framework advances through two complementary mechanisms — a declarative protocol (the workflow document) and a set of Go interfaces (the extension points). The protocol is the blueprint: it expresses the intended execution structure without prescribing how it is carried out. The interfaces are the seams: they let every infrastructure concern (state, dispatch, evaluation, secrets, …) be swapped without touching the engine. Neither pillar alone is sufficient — the protocol defines *what*, the interfaces define *how-to-plug-in*.

**Environment-agnostic by design**: The framework makes no assumptions about the deployment topology. A workflow runs identically whether the engine is embedded in a single process, spread across microservices, or deployed as multiple replicas. There is no built-in notion of "local" vs "remote" — that distinction belongs to the `broker.TaskBroker` and `store.Store` implementations provided by the caller.

**Engine is the minimal public API**: `Engine` is the only struct exposed to callers. Its surface area is intentionally small — `Submit`, `Get`, `Resume`, `Cancel`, `Start`, `Stop`, and the two task lifecycle callbacks. Every capability is opt-in via functional options. The guiding rule: *expose less, couple less*. A smaller API means fewer assumptions baked in and lower integration cost for adopters.

**`internal/` is the implementation boundary**: All scheduling algorithms, binding logic, validation, retry/timeout mechanics, and DAG traversal live under `internal/`. Outside of `internal/`, every package (except the top-level `aether` engine package) is a pure interface layer — it defines a contract and nothing else. This boundary is enforced by the Go compiler: no external consumer can import `internal/` directly, keeping implementation details from leaking into the public contract.

**Scope-tree scheduling**: TaskRuns form a tree via `ParentRunID`. Each DAG/Loop container is a "scope". `advanceScope()` processes a scope then walks upward to parent scopes until the workflow finalizes.

**Phase is engine-owned**: Executors return an `ExecCode` (integer). The engine maps codes to `Phase` values, optionally overridden by user-defined `PhaseConditions` expressions. `PhaseSkipped` and `PhaseCancelled` are set exclusively by the engine.

**Fat task assignments**: `broker.TaskAssignment` carries all information needed to execute a task. Workers never query the Store directly.

**Declarative binding**: Parameter values flow via `{{inputs.parameters.name}}` interpolation. Tasks receive values only through explicitly wired `arguments` — no sideways access to sibling state.

**Suspend/resume (await)**: Executors can return `ExecCodeSuspended`. The task transitions to `PhaseSuspended` with partial outputs. `Engine.Resume()` merges new payload and re-dispatches.

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
