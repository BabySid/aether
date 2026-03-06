package aether

import "github.com/BabySid/aether/model"

// WorkflowExecution is the return type of Engine.Get.
type WorkflowExecution struct {
	WorkflowID uint64
	Phase      model.Phase
	Code       int
	Msg        string
	Progress   string
	Outputs    *model.Outputs
	Metrics    *model.Metrics
	Tasks      []TaskExecution
}

// TaskExecution represents a task's execution status within a workflow.
type TaskExecution struct {
	TaskID   uint64
	Name     string
	Path     string
	Template string
	Phase    model.Phase
	Metrics  *model.Metrics
}
