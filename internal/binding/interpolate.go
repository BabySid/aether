package binding

import (
	"fmt"
	"strings"
)

// Interpolate replaces {{key}} placeholders in s with values from env.
//
// Rules:
//   - If the entire string is a single {{key}} and env[key] is a non-string type,
//     the original Go value is returned directly (type preserved).
//   - Otherwise every {{key}} occurrence is replaced with fmt.Sprint(env[key]).
//     Keys not found in env are left as-is (not an error).
//
// Return values:
//
//	(resolvedValue any, wasInterpolated bool)
//
// wasInterpolated is true when at least one placeholder was substituted.
func Interpolate(s string, env EvalEnv) (any, bool) {
	// Fast path: no template markers at all
	if !strings.Contains(s, "{{") {
		return s, false
	}

	// Single-token case: entire string is exactly "{{key}}"
	if strings.HasPrefix(s, "{{") && strings.HasSuffix(s, "}}") {
		inner := s[2 : len(s)-2]
		// Only treat as single token if there's no inner {{ }}
		if !strings.Contains(inner, "{{") && !strings.Contains(inner, "}}") {
			key := strings.TrimSpace(inner)
			if val, ok := env[key]; ok {
				return val, true
			}
			// Key not found — return original string unchanged
			return s, false
		}
	}

	// Multi-token or mixed case: replace all {{key}} with their string representations
	result, replaced := replacePlaceholders(s, env)
	return result, replaced
}

// InterpolateString is a convenience wrapper that always returns a string.
// It is suitable for contexts that only accept string values (e.g., log messages,
// executor config fields that are always strings).
func InterpolateString(s string, env EvalEnv) string {
	v, _ := Interpolate(s, env)
	if str, ok := v.(string); ok {
		return str
	}
	return fmt.Sprint(v)
}

// replacePlaceholders scans s for all {{key}} tokens and replaces each with
// fmt.Sprint(env[key]). Returns the substituted string and whether any
// substitution was made.
func replacePlaceholders(s string, env EvalEnv) (string, bool) {
	var sb strings.Builder
	replaced := false
	remaining := s
	for {
		start := strings.Index(remaining, "{{")
		if start == -1 {
			sb.WriteString(remaining)
			break
		}
		end := strings.Index(remaining[start:], "}}")
		if end == -1 {
			// Unclosed placeholder — write the rest as-is
			sb.WriteString(remaining)
			break
		}
		end += start // absolute index of "}}" start

		// Write text before the placeholder
		sb.WriteString(remaining[:start])

		key := strings.TrimSpace(remaining[start+2 : end])
		if val, ok := env[key]; ok {
			sb.WriteString(fmt.Sprint(val))
			replaced = true
		} else {
			// Key not found — preserve the original placeholder
			sb.WriteString(remaining[start : end+2])
		}
		remaining = remaining[end+2:]
	}
	return sb.String(), replaced
}
