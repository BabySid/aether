// Package artifact defines the artifact storage abstraction for aether.
package artifact

import (
	"context"
	"encoding/json"
	"io"
)

// Store manages artifact upload and download (optional).
type Store interface {
	// Type returns the storage type identifier (e.g. "oss", "local", "http").
	Type() string

	// Upload uploads an artifact.
	Upload(ctx context.Context, config json.RawMessage, data io.Reader) error

	// Download downloads an artifact.
	Download(ctx context.Context, config json.RawMessage) (io.ReadCloser, error)
}
