package internal

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// durationRegex matches durations like "30s", "5m", "1h", "1h30m", "500ms".
var durationRegex = regexp.MustCompile(`^(\d+)(ms|s|m|h|d)$`)

// ParseDuration parses a duration string (e.g., "30s", "5m", "1h", "2d").
// Extends Go's time.ParseDuration with support for "d" (days).
// Returns 0 if the input is empty.
func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}

	// Try standard Go duration first
	if d, err := time.ParseDuration(s); err == nil {
		if d < 0 {
			return 0, fmt.Errorf("invalid duration: %q (must be non-negative)", s)
		}
		return d, nil
	}

	// Handle "d" suffix for days
	m := durationRegex.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("invalid duration: %q", s)
	}

	val, _ := strconv.Atoi(m[1])
	switch m[2] {
	case "d":
		return time.Duration(val) * 24 * time.Hour, nil
	case "h":
		return time.Duration(val) * time.Hour, nil
	case "m":
		return time.Duration(val) * time.Minute, nil
	case "s":
		return time.Duration(val) * time.Second, nil
	case "ms":
		return time.Duration(val) * time.Millisecond, nil
	default:
		return 0, fmt.Errorf("invalid duration unit: %q", m[2])
	}
}
