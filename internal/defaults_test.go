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
