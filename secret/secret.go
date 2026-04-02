// Package secret defines the secret storage abstraction for aether.
package secret

import "context"

// Provider retrieves secrets by name and key (optional).
type Provider interface {
	// Get retrieves a secret value.
	Get(ctx context.Context, name string, key string) (string, error)
}
