package internal

import (
	"strings"
	"testing"
	"time"

	"github.com/BabySid/aether/model"
)

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		// Empty string
		{"", 0, false},

		// Standard Go durations (delegated to time.ParseDuration)
		{"500ms", 500 * time.Millisecond, false},
		{"30s", 30 * time.Second, false},
		{"5m", 5 * time.Minute, false},
		{"1h", time.Hour, false},
		{"1h30m", 90 * time.Minute, false},

		// "d" suffix (days) — extended by ParseDuration
		{"1d", 24 * time.Hour, false},
		{"7d", 7 * 24 * time.Hour, false},

		// Invalid inputs
		{"-1s", 0, true}, // negative duration is not meaningful for a timeout
		{"abc", 0, true},
		{"5x", 0, true},
		{"1 d", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := ParseDuration(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseDuration(%q) error = %v, wantErr %v", tc.input, err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("ParseDuration(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestComputeMetricsFinish_NilExisting(t *testing.T) {
	// RFC3339 has second precision; truncate before to seconds so the
	// parsed FinishedAt (also second precision) is never less than before.
	before := time.Now().UTC().Truncate(time.Second)
	m := ComputeMetricsFinish(nil)
	after := time.Now().UTC()

	if m == nil {
		t.Fatal("expected non-nil Metrics")
	}
	if m.FinishedAt == "" {
		t.Error("expected FinishedAt to be set")
	}
	// FinishedAt should parse successfully and be between before/after
	ft, err := time.Parse(time.RFC3339, m.FinishedAt)
	if err != nil {
		t.Fatalf("FinishedAt parse error: %v", err)
	}
	if ft.Before(before) || ft.After(after.Add(time.Second)) {
		t.Errorf("FinishedAt %v out of expected range [%v, %v]", ft, before, after)
	}
	if m.StartedAt != "" {
		t.Errorf("expected empty StartedAt, got %q", m.StartedAt)
	}
	if m.Duration != "" {
		t.Errorf("expected empty Duration, got %q", m.Duration)
	}
	if m.Retries != 0 {
		t.Errorf("expected Retries=0, got %d", m.Retries)
	}
}

func TestComputeMetricsFinish_EmptyStartedAt(t *testing.T) {
	existing := &model.Metrics{StartedAt: "", Retries: 2}
	m := ComputeMetricsFinish(existing)

	if m.FinishedAt == "" {
		t.Error("expected FinishedAt to be set")
	}
	if m.StartedAt != "" {
		t.Errorf("expected empty StartedAt propagated, got %q", m.StartedAt)
	}
	if m.Duration != "" {
		t.Errorf("expected empty Duration when StartedAt is empty, got %q", m.Duration)
	}
	if m.Retries != 2 {
		t.Errorf("expected Retries=2, got %d", m.Retries)
	}
}

func TestComputeMetricsFinish_WithValidStartedAt(t *testing.T) {
	startedAt := time.Now().UTC().Add(-500 * time.Millisecond)
	existing := &model.Metrics{
		StartedAt: startedAt.Format(time.RFC3339),
		Retries:   3,
	}
	m := ComputeMetricsFinish(existing)

	if m.StartedAt != existing.StartedAt {
		t.Errorf("StartedAt not propagated: got %q, want %q", m.StartedAt, existing.StartedAt)
	}
	if m.FinishedAt == "" {
		t.Error("expected FinishedAt to be set")
	}
	if m.Duration == "" {
		t.Error("expected Duration to be non-empty")
	}
	// Duration should be positive and contain "ms" or "s"
	if !strings.Contains(m.Duration, "ms") && !strings.Contains(m.Duration, "s") {
		t.Errorf("unexpected Duration format: %q", m.Duration)
	}
	if m.Retries != 3 {
		t.Errorf("expected Retries=3, got %d", m.Retries)
	}
}

func TestComputeMetricsFinish_InvalidStartedAt(t *testing.T) {
	existing := &model.Metrics{StartedAt: "not-a-time"}
	// should not panic; Duration should be empty
	m := ComputeMetricsFinish(existing)

	if m == nil {
		t.Fatal("expected non-nil Metrics")
	}
	if m.StartedAt != "not-a-time" {
		t.Errorf("StartedAt not propagated: got %q", m.StartedAt)
	}
	if m.Duration != "" {
		t.Errorf("expected empty Duration on parse failure, got %q", m.Duration)
	}
}
