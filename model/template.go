package model

// Template is a pure union container. Exactly one of DAG, Task, or Loop must be set.
// name, inputs, and outputs are owned by each sub-type.
type Template struct {
	DAG  *DAG  `json:"dag,omitempty"`
	Task *Task `json:"task,omitempty"`
	Loop *Loop `json:"loop,omitempty"`
}

// GetName returns the template name from whichever sub-type is set.
func (t *Template) GetName() string {
	if t.DAG != nil {
		return t.DAG.Name
	}
	if t.Task != nil {
		return t.Task.Name
	}
	if t.Loop != nil {
		return t.Loop.Name
	}
	return ""
}

// GetInputs returns the inputs from whichever sub-type is set.
func (t *Template) GetInputs() *Inputs {
	if t.DAG != nil {
		return t.DAG.Inputs
	}
	if t.Task != nil {
		return t.Task.Inputs
	}
	if t.Loop != nil {
		return t.Loop.Inputs
	}
	return nil
}

// GetOutputs returns the outputs from whichever sub-type is set.
func (t *Template) GetOutputs() *Outputs {
	if t.DAG != nil {
		return t.DAG.Outputs
	}
	if t.Task != nil {
		return t.Task.Outputs
	}
	if t.Loop != nil {
		return t.Loop.Outputs
	}
	return nil
}

// GetExecutor returns the executor if this is a Task template, nil otherwise.
func (t *Template) GetExecutor() *Executor {
	if t.Task != nil {
		return t.Task.Executor
	}
	return nil
}

// GetTimeout returns the timeout from whichever sub-type is set.
func (t *Template) GetTimeout() string {
	if t.DAG != nil {
		return t.DAG.Timeout
	}
	if t.Task != nil {
		return t.Task.Timeout
	}
	if t.Loop != nil {
		return t.Loop.Timeout
	}
	return ""
}

// GetResources returns the resources if this is a Task template, nil otherwise.
func (t *Template) GetResources() *Resources {
	if t.Task != nil {
		return t.Task.Resources
	}
	return nil
}
