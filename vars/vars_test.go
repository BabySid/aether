package vars_test

import (
	"runtime"
	"testing"

	"github.com/BabySid/aether/vars"
)

// ---------------------------------------------------------------------------
// SystemSource
// ---------------------------------------------------------------------------

func TestSystemSource_Namespace(t *testing.T) {
	p := &vars.SystemSource{}
	if p.Namespace() != "system" {
		t.Fatalf("expected namespace 'system', got %q", p.Namespace())
	}
}

func TestSystemSource_Provide(t *testing.T) {
	p := &vars.SystemSource{}
	m := p.Vars()
	if m["system.os"] != runtime.GOOS {
		t.Fatalf("expected system.os=%q, got %v", runtime.GOOS, m["system.os"])
	}
	if m["system.arch"] != runtime.GOARCH {
		t.Fatalf("expected system.arch=%q, got %v", runtime.GOARCH, m["system.arch"])
	}
}

// ---------------------------------------------------------------------------
// Custom provider integration via Source interface
// ---------------------------------------------------------------------------

// tenantProvider is a test implementation of vars.Source.
type tenantProvider struct {
	tenantID string
	tier     string
}

func (p *tenantProvider) Namespace() string { return "tenant" }
func (p *tenantProvider) Vars() map[string]any {
	return map[string]any{
		"tenant.id":   p.tenantID,
		"tenant.tier": p.tier,
	}
}

func TestCustomProvider_ImplementsInterface(t *testing.T) {
	var _ vars.Source = &tenantProvider{}
}

func TestCustomProvider_Provide(t *testing.T) {
	p := &tenantProvider{tenantID: "acme", tier: "enterprise"}
	m := p.Vars()
	if m["tenant.id"] != "acme" {
		t.Fatalf("expected tenant.id=acme, got %v", m["tenant.id"])
	}
	if m["tenant.tier"] != "enterprise" {
		t.Fatalf("expected tenant.tier=enterprise, got %v", m["tenant.tier"])
	}
}
