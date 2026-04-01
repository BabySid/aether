package internal

import (
	"testing"

	"github.com/BabySid/aether/model"
)

func TestFillDefaults_AllEmpty(t *testing.T) {
	wf := &model.Workflow{}
	FillDefaults(wf)

	if wf.Metadata.Namespace != "default" {
		t.Errorf("Namespace = %q, want %q", wf.Metadata.Namespace, "default")
	}
	if wf.Spec.Timeout != "1h" {
		t.Errorf("Timeout = %q, want %q", wf.Spec.Timeout, "1h")
	}
	if wf.Spec.Priority != 500 {
		t.Errorf("Priority = %d, want %d", wf.Spec.Priority, 500)
	}
	if wf.Spec.MaxNestedDepth != 3 {
		t.Errorf("MaxNestedDepth = %d, want %d", wf.Spec.MaxNestedDepth, 3)
	}
}

func TestFillDefaults_PreservesExisting(t *testing.T) {
	wf := &model.Workflow{
		Metadata: model.Metadata{Namespace: "prod"},
		Spec: model.WorkflowSpec{
			Timeout:        "30m",
			Priority:       100,
			MaxNestedDepth: 5,
		},
	}
	FillDefaults(wf)

	if wf.Metadata.Namespace != "prod" {
		t.Errorf("Namespace = %q, want %q", wf.Metadata.Namespace, "prod")
	}
	if wf.Spec.Timeout != "30m" {
		t.Errorf("Timeout = %q, want %q", wf.Spec.Timeout, "30m")
	}
	if wf.Spec.Priority != 100 {
		t.Errorf("Priority = %d, want %d", wf.Spec.Priority, 100)
	}
	if wf.Spec.MaxNestedDepth != 5 {
		t.Errorf("MaxNestedDepth = %d, want %d", wf.Spec.MaxNestedDepth, 5)
	}
}

func TestFillDefaults_PartialOverride(t *testing.T) {
	wf := &model.Workflow{
		Metadata: model.Metadata{Namespace: "staging"},
		Spec: model.WorkflowSpec{
			Priority: 800,
			// Timeout and MaxNestedDepth left at zero values
		},
	}
	FillDefaults(wf)

	if wf.Metadata.Namespace != "staging" {
		t.Errorf("Namespace = %q, want %q", wf.Metadata.Namespace, "staging")
	}
	if wf.Spec.Timeout != "1h" {
		t.Errorf("Timeout = %q, want default %q", wf.Spec.Timeout, "1h")
	}
	if wf.Spec.Priority != 800 {
		t.Errorf("Priority = %d, want %d", wf.Spec.Priority, 800)
	}
	if wf.Spec.MaxNestedDepth != 3 {
		t.Errorf("MaxNestedDepth = %d, want default %d", wf.Spec.MaxNestedDepth, 3)
	}
}

// --- CronWorkflow defaults ---

func TestFillCronDefaults_AllEmpty(t *testing.T) {
	cw := &model.CronWorkflow{
		Spec: model.CronWorkflowSpec{
			WorkflowSpec: model.WorkflowSpec{
				Entrypoint: "main",
				Templates:  []model.Template{{Task: &model.Task{Name: "main", Executor: &model.Executor{Type: "echo"}}}},
			},
		},
	}
	FillCronDefaults(cw)

	if cw.Metadata.Namespace != "default" {
		t.Errorf("Namespace = %q, want %q", cw.Metadata.Namespace, "default")
	}
	if cw.Spec.Timezone != "UTC" {
		t.Errorf("Timezone = %q, want %q", cw.Spec.Timezone, "UTC")
	}
	if cw.Spec.ConcurrencyPolicy != model.ConcurrencyAllow {
		t.Errorf("ConcurrencyPolicy = %q, want %q", cw.Spec.ConcurrencyPolicy, model.ConcurrencyAllow)
	}
	if cw.Spec.SuccessfulJobsHistoryLimit != 3 {
		t.Errorf("SuccessfulJobsHistoryLimit = %d, want %d", cw.Spec.SuccessfulJobsHistoryLimit, 3)
	}
	if cw.Spec.FailedJobsHistoryLimit != 1 {
		t.Errorf("FailedJobsHistoryLimit = %d, want %d", cw.Spec.FailedJobsHistoryLimit, 1)
	}
	// WorkflowSpec defaults should also be filled.
	if cw.Spec.WorkflowSpec.Timeout != "1h" {
		t.Errorf("WorkflowSpec.Timeout = %q, want %q", cw.Spec.WorkflowSpec.Timeout, "1h")
	}
	if cw.Spec.WorkflowSpec.Priority != 500 {
		t.Errorf("WorkflowSpec.Priority = %d, want %d", cw.Spec.WorkflowSpec.Priority, 500)
	}
}

func TestFillCronDefaults_PreservesExisting(t *testing.T) {
	cw := &model.CronWorkflow{
		Metadata: model.Metadata{Namespace: "prod"},
		Spec: model.CronWorkflowSpec{
			Timezone:                   "Asia/Shanghai",
			ConcurrencyPolicy:          model.ConcurrencyForbid,
			SuccessfulJobsHistoryLimit: 5,
			FailedJobsHistoryLimit:     2,
			WorkflowSpec: model.WorkflowSpec{
				Timeout:  "30m",
				Priority: 100,
			},
		},
	}
	FillCronDefaults(cw)

	if cw.Metadata.Namespace != "prod" {
		t.Errorf("Namespace = %q, want %q", cw.Metadata.Namespace, "prod")
	}
	if cw.Spec.Timezone != "Asia/Shanghai" {
		t.Errorf("Timezone = %q, want %q", cw.Spec.Timezone, "Asia/Shanghai")
	}
	if cw.Spec.ConcurrencyPolicy != model.ConcurrencyForbid {
		t.Errorf("ConcurrencyPolicy = %q, want %q", cw.Spec.ConcurrencyPolicy, model.ConcurrencyForbid)
	}
	if cw.Spec.SuccessfulJobsHistoryLimit != 5 {
		t.Errorf("SuccessfulJobsHistoryLimit = %d, want %d", cw.Spec.SuccessfulJobsHistoryLimit, 5)
	}
	if cw.Spec.FailedJobsHistoryLimit != 2 {
		t.Errorf("FailedJobsHistoryLimit = %d, want %d", cw.Spec.FailedJobsHistoryLimit, 2)
	}
	if cw.Spec.WorkflowSpec.Timeout != "30m" {
		t.Errorf("WorkflowSpec.Timeout = %q, want %q", cw.Spec.WorkflowSpec.Timeout, "30m")
	}
}
