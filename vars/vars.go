// Package vars defines the Source extension point for aether workflow variable namespaces.
//
// A Source contributes a flat key→value map to the workflow evaluation environment (EvalVars).
// Variables can then be referenced as {{namespace.key}} placeholders in workflow templates.
//
// # Built-in sources
//
// The engine provides four internal per-run value objects covering the standard
// variable namespaces (workflow, inputs, tasks, loop_iter). These live in
// internal/vars and are wired automatically by the binding layer.
//
// One built-in global source exposes runtime metadata:
//
//   - SystemSource — "system.{os,arch}"
//
// # Custom sources
//
// Implement Source to add custom variable namespaces.
//
// ## Static value object (per-run data injected at construction)
//
//	type TenantSource struct {
//	    TenantID string
//	    Tier     string
//	}
//	func (s *TenantSource) Namespace() string { return "tenant" }
//	func (s *TenantSource) Vars() map[string]any {
//	    return map[string]any{
//	        "tenant.id":   s.TenantID,
//	        "tenant.tier": s.Tier,
//	    }
//	}
//
//	engine, _ := aether.New(
//	    aether.WithVarsSource(&TenantSource{TenantID: "acme", Tier: "enterprise"}),
//	)
//
// ## Dynamic / real-time computation
//
// Vars() is called each time the evaluation environment is built, so it can
// return values computed at call time — not at registration time.
// This is useful for timestamps, counters, or any data that must reflect the
// state of the world at the moment a task's inputs are resolved.
//
//	// ClockSource exposes the current wall-clock time under the "now" namespace.
//	// Because Vars() is evaluated lazily, every task that references {{now.*}}
//	// receives the time at which its inputs are resolved, not when the engine started.
//	type ClockSource struct{}
//
//	func (s *ClockSource) Namespace() string { return "now" }
//	func (s *ClockSource) Vars() map[string]any {
//	    t := time.Now().UTC()
//	    return map[string]any{
//	        "now.unix":      t.Unix(),                        // e.g. 1711857600
//	        "now.rfc3339":   t.Format(time.RFC3339),          // e.g. "2026-03-31T04:00:00Z"
//	        "now.date":      t.Format("2006-01-02"),          // e.g. "2026-03-31"
//	        "now.year":      t.Year(),                        // e.g. 2026
//	        "now.month":     int(t.Month()),                  // e.g. 3
//	        "now.day":       t.Day(),                         // e.g. 31
//	    }
//	}
//
//	engine, _ := aether.New(
//	    aether.WithVarsSource(&ClockSource{}),
//	)
//
// With this registered, any workflow template can reference:
//
//	{"name": "report-date", "value": "{{now.date}}"}
//	{"name": "run-at",      "value": "{{now.rfc3339}}"}
//
// # Lifecycle
//
// Sources have two lifecycle patterns:
//
//  1. Global singleton: stateless, registered once with aether.WithVarsSource.
//     Vars() may still compute dynamic values on each call (e.g. ClockSource,
//     SystemSource). Safe to share across all workflow runs.
//
//  2. Per-run value object: carries run-specific data injected at construction.
//     The engine creates a new instance per execution context. The built-in
//     internal sources (WorkflowArgsSource, SiblingTaskRunsSource, etc.) follow
//     this pattern and serve as reference implementations.
package vars

import "runtime"

// Source is the extension point for adding custom variable namespaces to
// the workflow evaluation environment.
//
// Each source owns one logical namespace. Vars() returns a flat map of
// fully-qualified key→value pairs (e.g. {"system.os": "linux"}).
// Keys should share the source's namespace prefix to avoid collisions.
//
// See package-level documentation for lifecycle patterns and examples.
type Source interface {
	// Namespace returns a short identifier for this source (e.g. "system", "tenant").
	// Used for documentation and debugging; does not restrict the key space of Vars().
	Namespace() string

	// Vars returns the flat key→value map contributed by this source.
	// Called once per evaluation context build. The map must not be mutated after return.
	Vars() map[string]any
}

// SystemSource is a built-in global Source that exposes runtime environment
// information under the "system" namespace.
//
// Namespace: "system"
// Keys produced:
//
//	"system.os"   — operating system name (e.g. "linux", "darwin", "windows")
//	"system.arch" — CPU architecture (e.g. "amd64", "arm64")
//
// Stateless singleton: safe to share across workflow runs. Register at engine level:
//
//	engine, _ := aether.New(
//	    aether.WithVarsSource(&vars.SystemSource{}),
//	)
type SystemSource struct{}

func (s *SystemSource) Namespace() string { return "system" }

func (s *SystemSource) Vars() map[string]any {
	return map[string]any{
		"system.os":   runtime.GOOS,
		"system.arch": runtime.GOARCH,
	}
}
