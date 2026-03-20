package internal

import (
	"fmt"
	"regexp"

	"github.com/BabySid/aether/model"
)

// nameRe matches DNS-1123 label names as used throughout the schema:
//
//	^[a-z0-9]([-a-z0-9]*[a-z0-9])?$   maxLength=63
//
// Applied uniformly to: metadata.name, template names, DAG task names, and
// parameter names. Dots are reserved as the system namespace separator
// (e.g. "loop_iter.index") and therefore disallowed in user-defined names.
var nameRe = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

const nameMaxLength = 63

// validateName returns an error if name does not conform to the DNS-1123 rule.
func validateName(kind, name string) error {
	if len(name) > nameMaxLength {
		return fmt.Errorf("%s %q is too long: max %d characters", kind, name, nameMaxLength)
	}
	if !nameRe.MatchString(name) {
		return fmt.Errorf("%s %q is invalid: must match DNS-1123 (^[a-z0-9]([-a-z0-9]*[a-z0-9])?$)", kind, name)
	}
	return nil
}

// MaxNestingDepth is the maximum allowed static template nesting depth.
const MaxNestingDepth = 10

// Validate performs structural and semantic validation on a Workflow.
func Validate(wf *model.Workflow) error {
	if wf.APIVersion != "aether/v1" {
		return fmt.Errorf("unsupported apiVersion: %s", wf.APIVersion)
	}
	if wf.Kind != "Workflow" && wf.Kind != "CronWorkflow" && wf.Kind != "WorkflowTemplate" {
		return fmt.Errorf("unsupported kind: %s", wf.Kind)
	}
	if wf.Metadata.Name == "" {
		return fmt.Errorf("metadata.name is required")
	}
	if err := validateName("metadata.name", wf.Metadata.Name); err != nil {
		return err
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
		if err := validateName("template name", name); err != nil {
			return err
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
			if err := validateInputParamNames(name, tmpl.GetInputs()); err != nil {
				return err
			}
		}

		if tmpl.DAG != nil {
			if err := validateInputParamNames(name, tmpl.GetInputs()); err != nil {
				return err
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
		if err := validateName("task name", task.Name); err != nil {
			return err
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
		// A DAG task must have either an inline executor OR a template reference.
		if task.Template == "" && task.Executor == nil {
			return fmt.Errorf("task %q: either template or executor is required", task.Name)
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

// validateInputParamNames checks that every parameter name in inputs follows the
// DNS-1123 rule. Dots are reserved for system-injected keys (e.g. loop_iter.index)
// and are therefore disallowed in user-defined parameter names.
func validateInputParamNames(tmplName string, inputs *model.Inputs) error {
	if inputs == nil {
		return nil
	}
	for _, p := range inputs.Parameters {
		if err := validateName("parameter name", p.Name); err != nil {
			return fmt.Errorf("template %q inputs: %w", tmplName, err)
		}
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

	// validate aggregate strategy if specified
	if loop.Aggregate != nil && loop.Aggregate.Strategy != "" {
		switch loop.Aggregate.Strategy {
		case model.AggregateStrategyLast, model.AggregateStrategyFirst, model.AggregateStrategyList:
			// valid
		default:
			return fmt.Errorf("loop.aggregate.strategy %q is not supported; valid values: last, first, list",
				loop.Aggregate.Strategy)
		}
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
