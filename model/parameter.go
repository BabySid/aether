package model

import "encoding/json"

// Parameter defines a workflow or task parameter.
type Parameter struct {
	Name        string            `json:"name"`
	Type        string            `json:"type,omitempty"` // string, int, float, bool, json, list
	Value       json.RawMessage   `json:"value,omitempty"`
	Default     json.RawMessage   `json:"default,omitempty"`
	Description string            `json:"description,omitempty"`
	Enum        []json.RawMessage `json:"enum,omitempty"`
	ValueFrom   *ValueFrom        `json:"valueFrom,omitempty"`
}

// ValueFrom specifies a source for a parameter value.
type ValueFrom struct {
	Path         string        `json:"path,omitempty"`
	Parameter    string        `json:"parameter,omitempty"`
	Expression   string        `json:"expression,omitempty"`
	SecretKeyRef *SecretKeyRef `json:"secretKeyRef,omitempty"`
}

// SecretKeyRef references a key in a secret store.
type SecretKeyRef struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// Artifact defines an input/output artifact.
type Artifact struct {
	Name    string          `json:"name"`
	From    string          `json:"from,omitempty"`
	Source  *ArtifactSource `json:"source,omitempty"`
	Archive *Archive        `json:"archive,omitempty"`
}

// ArtifactSource specifies the storage backend for an artifact.
type ArtifactSource struct {
	Type   string          `json:"type"`   // oss, local, http
	Config json.RawMessage `json:"config"` // ossSourceConfig | localSourceConfig | httpSourceConfig
}

// Archive defines the archive format for an artifact.
// TODO: reserved for artifact upload/download feature; not yet used in the engine.
type Archive struct {
	Type string `json:"type,omitempty"` // none, tar, tar.gz, zip
}

// OSSSourceConfig is the configuration for OSS artifact storage.
// TODO: reserved for artifact upload/download feature; not yet used in the engine.
type OSSSourceConfig struct {
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	Endpoint string `json:"endpoint"`
}

// LocalSourceConfig is the configuration for local artifact storage.
// TODO: reserved for artifact upload/download feature; not yet used in the engine.
type LocalSourceConfig struct {
	Path string `json:"path"`
}

// HTTPSourceConfig is the configuration for HTTP artifact storage.
// TODO: reserved for artifact upload/download feature; not yet used in the engine.
type HTTPSourceConfig struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
}
