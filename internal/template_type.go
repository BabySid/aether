package internal

import "github.com/BabySid/aether/model"

// ResolveTemplateType determines the template type from its set fields.
// Exactly one of DAG/Executor/Loop must be set (enforced by Validate).
func ResolveTemplateType(tmpl *model.Template) string {
	switch {
	case tmpl.DAG != nil:
		return model.TemplateTypeDAG
	case tmpl.Task != nil:
		return model.TemplateTypeTask
	case tmpl.Loop != nil:
		return model.TemplateTypeLoop
	default:
		return ""
	}
}
