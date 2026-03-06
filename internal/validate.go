package internal

import (
	"fmt"

	"github.com/BabySid/aether/model"
)

// Validate performs structural and semantic validation on a Workflow.
func Validate(wf *model.Workflow) error {
	if wf.APIVersion != "graph/v1" {
		return fmt.Errorf("unsupported apiVersion: %s", wf.APIVersion)
	}
	if wf.Kind != "Workflow" && wf.Kind != "CronWorkflow" && wf.Kind != "WorkflowTemplate" {
		return fmt.Errorf("unsupported kind: %s", wf.Kind)
	}
	if wf.Metadata.Name == "" {
		return fmt.Errorf("metadata.name is required")
	}

	if wf.Spec.WorkflowTemplateRef == nil {
		if wf.Spec.Entrypoint == "" {
			return fmt.Errorf("spec.entrypoint is required when templates are inline")
		}
		if len(wf.Spec.Templates) == 0 {
			return fmt.Errorf("spec.templates is required when workflowTemplateRef is not set")
		}
		if FindTemplate(wf, wf.Spec.Entrypoint) == nil {
			return fmt.Errorf("entrypoint template %q not found in templates", wf.Spec.Entrypoint)
		}
	}

	for i := range wf.Spec.Templates {
		tmpl := &wf.Spec.Templates[i]
		if tmpl.Name == "" {
			return fmt.Errorf("template[%d].name is required", i)
		}
		count := 0
		if tmpl.DAG != nil {
			count++
		}
		if tmpl.Executor != nil {
			count++
		}
		if tmpl.Loop != nil {
			count++
		}
		if count != 1 {
			return fmt.Errorf("template %q must have exactly one of dag/executor/loop", tmpl.Name)
		}

		if tmpl.DAG != nil {
			if err := validateDAG(wf, tmpl.DAG); err != nil {
				return fmt.Errorf("template %q: %w", tmpl.Name, err)
			}
		}
	}

	return nil
}

// validateDAG validates a DAG definition.
func validateDAG(wf *model.Workflow, dag *model.DAG) error {
	if len(dag.Tasks) == 0 {
		return fmt.Errorf("dag.tasks must not be empty")
	}

	taskNames := make(map[string]bool, len(dag.Tasks))
	for _, task := range dag.Tasks {
		if task.Name == "" {
			return fmt.Errorf("task.name is required")
		}
		if taskNames[task.Name] {
			return fmt.Errorf("duplicate task name %q", task.Name)
		}
		taskNames[task.Name] = true
	}

	for _, task := range dag.Tasks {
		for _, dep := range task.Dependencies {
			if !taskNames[dep] {
				return fmt.Errorf("task %q depends on unknown task %q", task.Name, dep)
			}
		}
		if task.Template != "" && FindTemplate(wf, task.Template) == nil {
			return fmt.Errorf("task %q references unknown template %q", task.Name, task.Template)
		}
	}

	if HasCycle(dag) {
		return fmt.Errorf("dag contains a cycle")
	}

	return nil
}
