package model

// Template defines a reusable workflow template. Exactly one of DAG, Executor, or Loop must be set.
type Template struct {
	Name            string           `json:"name"`
	Inputs          *Inputs          `json:"inputs,omitempty"`
	Outputs         *Outputs         `json:"outputs,omitempty"`
	PhaseConditions *PhaseConditions `json:"phaseConditions,omitempty"`
	DAG             *DAG             `json:"dag,omitempty"`
	Executor        *Executor        `json:"executor,omitempty"`
	Loop            *Loop            `json:"loop,omitempty"`
	Timeout         string           `json:"timeout,omitempty"`
	Retry           *Retry           `json:"retry,omitempty"`
	Resources       *Resources       `json:"resources,omitempty"`
}
