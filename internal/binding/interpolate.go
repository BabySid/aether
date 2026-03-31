package binding

import (
	"fmt"
	"strings"
)

// ExtractNamespaces scans s for all {{namespace.key}} placeholders and returns
// the deduplicated list of namespace prefixes (the part before the first ".").
//
// Placeholders without a "." (e.g. "{{barekey}}") are ignored — they have no namespace.
//
// Example:
//
//	ExtractNamespaces("os={{system.os}} tenant={{tenant.name}} idx={{loop_iter.index}}")
//	// → ["system", "tenant", "loop_iter"]
//
// Use the result to call VarBuilder.Build(namespaces...) for on-demand evaluation,
// so that Sources whose namespace is not referenced in s are not triggered.
func ExtractNamespaces(s string) []string {
	seen := make(map[string]bool)
	var result []string
	remaining := s
	for {
		start := strings.Index(remaining, "{{")
		if start == -1 {
			break
		}
		end := strings.Index(remaining[start:], "}}")
		if end == -1 {
			break
		}
		key := strings.TrimSpace(remaining[start+2 : start+end])
		if dot := strings.Index(key, "."); dot > 0 {
			ns := key[:dot]
			if !seen[ns] {
				seen[ns] = true
				result = append(result, ns)
			}
		}
		remaining = remaining[start+end+2:]
	}
	return result
}

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
func Interpolate(s string, env EvalVars) (any, bool) {
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
func InterpolateString(s string, env EvalVars) string {
	v, _ := Interpolate(s, env)
	if str, ok := v.(string); ok {
		return str
	}
	return fmt.Sprint(v)
}

// replacePlaceholders scans s for all {{key}} tokens and replaces each with
// fmt.Sprint(env[key]). Returns the substituted string and whether any
// substitution was made.
func replacePlaceholders(s string, env EvalVars) (string, bool) {
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
