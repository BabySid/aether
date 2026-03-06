// Package internal contains internal helper functions for the aether engine.
package internal

import "github.com/BabySid/aether/model"

// FillDefaults applies default values to a Workflow.
func FillDefaults(wf *model.Workflow) {
	if wf.Metadata.Namespace == "" {
		wf.Metadata.Namespace = "default"
	}
	if wf.Spec.Timeout == "" {
		wf.Spec.Timeout = "1h"
	}
	if wf.Spec.Priority == 0 {
		wf.Spec.Priority = 500
	}
	if wf.Spec.MaxNestedDepth == 0 {
		wf.Spec.MaxNestedDepth = 3
	}
}
