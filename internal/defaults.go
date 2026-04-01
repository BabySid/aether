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

// FillCronDefaults applies default values to a CronWorkflow.
func FillCronDefaults(cw *model.CronWorkflow) {
	if cw.Metadata.Namespace == "" {
		cw.Metadata.Namespace = "default"
	}
	if cw.Spec.Timezone == "" {
		cw.Spec.Timezone = "UTC"
	}
	if cw.Spec.ConcurrencyPolicy == "" {
		cw.Spec.ConcurrencyPolicy = model.ConcurrencyAllow
	}
	if cw.Spec.SuccessfulJobsHistoryLimit == 0 {
		cw.Spec.SuccessfulJobsHistoryLimit = 3
	}
	if cw.Spec.FailedJobsHistoryLimit == 0 {
		cw.Spec.FailedJobsHistoryLimit = 1
	}

	// Fill defaults on the embedded WorkflowSpec.
	tmpWf := &model.Workflow{Spec: cw.Spec.WorkflowSpec}
	FillDefaults(tmpWf)
	cw.Spec.WorkflowSpec = tmpWf.Spec
}
