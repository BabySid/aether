package internal

import (
	"fmt"

	"github.com/BabySid/aether/model"
)

// MaxNestingDepth is the maximum allowed static template nesting depth.
const MaxNestingDepth = 10

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
		name := tmpl.GetName()
		if name == "" {
			return fmt.Errorf("template[%d].name is required", i)
		}

		// Exactly one of dag/task/loop must be set
		count := 0
		if tmpl.DAG != nil {
			count++
		}
		if tmpl.Task != nil {
			count++
		}
		if tmpl.Loop != nil {
			count++
		}
		if count != 1 {
			return fmt.Errorf("template %q must have exactly one of dag/executor/loop", name)
		}

		if tmpl.DAG != nil {
			if err := validateDAG(wf, tmpl.DAG); err != nil {
				return fmt.Errorf("template %q: %w", name, err)
			}
		}

		if tmpl.Task != nil {
			if tmpl.Task.Executor != nil && tmpl.Task.Executor.Type == "" {
				return fmt.Errorf("template %q: executor.type is required", name)
			}
		}

		if tmpl.Loop != nil {
			if err := validateLoop(wf, tmpl.Loop); err != nil {
				return fmt.Errorf("template %q: %w", name, err)
			}
		}
	}

	// Static nesting depth check from entrypoint
	if wf.Spec.WorkflowTemplateRef == nil {
		if err := validateNestingDepth(wf, wf.Spec.Entrypoint, 0); err != nil {
			return err
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
		if task.Template == "" {
			return fmt.Errorf("task %q: template is required", task.Name)
		}
		if FindTemplate(wf, task.Template) == nil {
			return fmt.Errorf("task %q references unknown template %q", task.Name, task.Template)
		}
	}

	if HasCycle(dag) {
		return fmt.Errorf("dag contains a cycle")
	}

	return nil
}

// validateLoop validates a Loop definition.
func validateLoop(wf *model.Workflow, loop *model.Loop) error {
	// body is required
	if loop.Body == "" {
		return fmt.Errorf("loop.body is required")
	}

	// body must reference an existing template
	bodyTmpl := FindTemplate(wf, loop.Body)
	if bodyTmpl == nil {
		return fmt.Errorf("loop.body references unknown template %q", loop.Body)
	}

	// Exactly one iteration mode: repeatCondition | items | itemsFrom
	modeCount := 0
	if loop.RepeatCondition != "" {
		modeCount++
	}
	if len(loop.Items) > 0 {
		modeCount++
	}
	if loop.ItemsFrom != "" {
		modeCount++
	}
	if modeCount == 0 {
		return fmt.Errorf("loop must specify one of repeatCondition/items/itemsFrom")
	}
	if modeCount > 1 {
		return fmt.Errorf("loop must specify only one of repeatCondition/items/itemsFrom")
	}

	// repeatCondition requires maxIterations as safety bound
	if loop.RepeatCondition != "" && loop.MaxIterations <= 0 {
		return fmt.Errorf("loop.maxIterations is required when using repeatCondition")
	}

	// concurrency only applies to items/itemsFrom mode
	if loop.Concurrency > 0 && loop.RepeatCondition != "" {
		return fmt.Errorf("loop.concurrency is not allowed with repeatCondition (serial only)")
	}

	return nil
}

// validateNestingDepth walks the template reference tree and rejects
// workflows whose static nesting exceeds MaxNestingDepth.
func validateNestingDepth(wf *model.Workflow, tmplName string, depth int) error {
	if depth > MaxNestingDepth {
		return fmt.Errorf("template nesting depth exceeds maximum (%d)", MaxNestingDepth)
	}

	tmpl := FindTemplate(wf, tmplName)
	if tmpl == nil {
		// Unknown template errors are already reported by other validators.
		return nil
	}

	if tmpl.DAG != nil {
		for _, task := range tmpl.DAG.Tasks {
			if err := validateNestingDepth(wf, task.Template, depth+1); err != nil {
				return err
			}
		}
	}

	if tmpl.Loop != nil && tmpl.Loop.Body != "" {
		if err := validateNestingDepth(wf, tmpl.Loop.Body, depth+1); err != nil {
			return err
		}
	}

	// Executor is a leaf node — no further nesting.
	return nil
}
