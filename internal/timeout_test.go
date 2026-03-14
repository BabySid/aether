package internal

import (
	"testing"
	"time"
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
