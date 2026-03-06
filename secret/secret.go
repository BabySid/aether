// Package secret defines the secret storage abstraction for aether.
package secret

import "context"

// Store retrieves secrets by name and key (optional).
type Store interface {
	// Get retrieves a secret value.
	Get(ctx context.Context, name string, key string) (string, error)
}
