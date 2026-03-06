package internal

import (
	"context"
	"fmt"

	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// ResolveWorkflowTemplateRef resolves a workflowTemplateRef by loading
// the referenced WorkflowTemplate from the Store and merging its spec
// into the workflow.
//
// If the workflow has no templateRef, this is a no-op.
// After resolution, the workflow will have templates and entrypoint set.
func ResolveWorkflowTemplateRef(ctx context.Context, wf *model.Workflow, s store.Store) error {
	ref := wf.Spec.WorkflowTemplateRef
	if ref == nil {
		return nil
	}

	namespace := ref.Namespace
	if namespace == "" {
		namespace = wf.Metadata.Namespace
		if namespace == "" {
			namespace = "default"
		}
	}

	tmpl, err := s.GetWorkflowTemplate(ctx, namespace, ref.Name)
	if err != nil {
		return fmt.Errorf("resolve workflowTemplateRef %s/%s: %w", namespace, ref.Name, err)
	}

	// Merge template spec into workflow
	// Template provides: templates, entrypoint, timeout, retry, hooks
	// Workflow-level values override template values where set
	if wf.Spec.Entrypoint == "" {
		wf.Spec.Entrypoint = tmpl.Spec.Entrypoint
	}
	if len(wf.Spec.Templates) == 0 {
		wf.Spec.Templates = tmpl.Spec.Templates
	}
	if wf.Spec.Timeout == "" {
		wf.Spec.Timeout = tmpl.Spec.Timeout
	}
	if wf.Spec.Retry == nil {
		wf.Spec.Retry = tmpl.Spec.Retry
	}
	if wf.Spec.Priority == 0 {
		wf.Spec.Priority = tmpl.Spec.Priority
	}
	if wf.Spec.MaxNestedDepth == 0 {
		wf.Spec.MaxNestedDepth = tmpl.Spec.MaxNestedDepth
	}
	if wf.Spec.Hooks == nil {
		wf.Spec.Hooks = tmpl.Spec.Hooks
	}

	// Merge arguments: workflow args override template args
	if tmpl.Spec.Arguments != nil {
		if wf.Spec.Arguments == nil {
			wf.Spec.Arguments = tmpl.Spec.Arguments
		} else {
			wf.Spec.Arguments = mergeArguments(tmpl.Spec.Arguments, wf.Spec.Arguments)
		}
	}

	// Clear the ref since it's been resolved
	wf.Spec.WorkflowTemplateRef = nil

	return nil
}

// mergeArguments merges base arguments with override arguments.
// Override parameters take precedence over base parameters by name.
func mergeArguments(base, override *model.Arguments) *model.Arguments {
	if base == nil {
		return override
	}
	if override == nil {
		return base
	}

	result := &model.Arguments{}

	// Start with base parameters
	paramMap := make(map[string]int) // name → index
	for _, p := range base.Parameters {
		paramMap[p.Name] = len(result.Parameters)
		result.Parameters = append(result.Parameters, p)
	}
	// Override with workflow parameters
	for _, p := range override.Parameters {
		if idx, ok := paramMap[p.Name]; ok {
			result.Parameters[idx] = p
		} else {
			result.Parameters = append(result.Parameters, p)
		}
	}

	// Artifacts: similar merge
	artifactMap := make(map[string]int)
	for _, a := range base.Artifacts {
		artifactMap[a.Name] = len(result.Artifacts)
		result.Artifacts = append(result.Artifacts, a)
	}
	for _, a := range override.Artifacts {
		if idx, ok := artifactMap[a.Name]; ok {
			result.Artifacts[idx] = a
		} else {
			result.Artifacts = append(result.Artifacts, a)
		}
	}

	return result
}
