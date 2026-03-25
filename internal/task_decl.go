package internal

import (
	"github.com/BabySid/aether/model"
	"github.com/BabySid/aether/store"
)

// ResolveTaskDecl looks up the task definition (*model.Task) and call-site node
// for a given TaskRun using the workflow template graph.
//
// Return values:
//
//   - taskDecl        — the definition (executor, inputs declaration, resources,
//     timeout default). Comes from the named template (tr.TemplateName) or, for
//     inline-executor tasks, from the DAG task node in the parent template.
//
//   - taskCall        — the DAG task node at the call site supplying arguments and
//     timeout override. nil for top-level tasks and loop iterations.
//
//   - isLoopIteration — true when the parent template is a Loop; iteration params
//     are already stored in tr.Inputs so no further binding is needed.
//
// parentTR is the TaskRun of the parent container; it may be nil for top-level
// tasks. The function is a pure template-graph lookup — it does not access the store.
func ResolveTaskDecl(wf *model.Workflow, tr *store.TaskRun, parentTR *store.TaskRun) (*model.Task, *model.Task, bool) {
	var taskDecl *model.Task
	var taskCall *model.Task
	var isLoopIteration bool

	// Step 1: named template reference.
	if tr.TemplateName != "" {
		taskTmpl := FindTemplate(wf, tr.TemplateName)
		if taskTmpl != nil {
			taskDecl = taskTmpl.Task
		}
	}

	// Step 2: resolve call-site from parent DAG or detect loop iteration.
	if parentTR != nil {
		parentTmpl := FindTemplate(wf, parentTR.TemplateName)
		if parentTmpl != nil && parentTmpl.DAG != nil {
			dagTask := FindTask(parentTmpl.DAG, tr.TaskName)
			if dagTask != nil {
				// taskCall always provides call-site arguments / timeout override.
				taskCall = dagTask
				// Inline-executor mode: the DAG task node IS the definition when no
				// named template was resolved above.
				if taskDecl == nil && dagTask.Executor != nil {
					taskDecl = dagTask
				}
			}
		} else if parentTmpl != nil && parentTmpl.Loop != nil {
			isLoopIteration = true
		}
	}
	return taskDecl, taskCall, isLoopIteration
}
