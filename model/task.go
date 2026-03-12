package model

// Task defines a single task node in a DAG (call site), or a standalone Task template.
// When used as a standalone template (template.task), name/inputs/outputs/executor carry
// the template definition. When used inside dag.tasks[], name is the DAG-local node name
// and template/executor/arguments are the call-site fields.
type Task struct {
	Name            string           `json:"name"`
	Inputs          *Inputs          `json:"inputs,omitempty"`
	Template        string           `json:"template,omitempty"`
	Executor        *Executor        `json:"executor,omitempty"`
	Dependencies    []string         `json:"dependencies,omitempty"`
	Arguments       *Arguments       `json:"arguments,omitempty"`
	When            string           `json:"when,omitempty"`
	Timeout         string           `json:"timeout,omitempty"`
	Retry           *Retry           `json:"retry,omitempty"`
	ContinueOn      *ContinueOn      `json:"continueOn,omitempty"`
	Resources       *Resources       `json:"resources,omitempty"`
	PhaseConditions *PhaseConditions `json:"phaseConditions,omitempty"`
	Hooks           *Hooks           `json:"hooks,omitempty"`
	Outputs         *Outputs         `json:"outputs,omitempty"`
}

// DAG defines a directed acyclic graph of tasks. When used as a template, it carries
// its own name, inputs, and outputs.
type DAG struct {
	Name            string           `json:"name"`
	Inputs          *Inputs          `json:"inputs,omitempty"`
	Outputs         *Outputs         `json:"outputs,omitempty"`
	Entrypoints     any              `json:"entrypoints,omitempty"` // string or []string
	Tasks           []Task           `json:"tasks"`
	ContinueOn      *ContinueOn      `json:"continueOn,omitempty"`
	PhaseConditions *PhaseConditions `json:"phaseConditions,omitempty"`
	Timeout         string           `json:"timeout,omitempty"`
}

// Loop defines a loop construct (repeatCondition, items, or itemsFrom). When used as a
// template, it carries its own name, inputs, and outputs.
type Loop struct {
	Name            string           `json:"name"`
	Inputs          *Inputs          `json:"inputs,omitempty"`
	Outputs         *Outputs         `json:"outputs,omitempty"`
	RepeatCondition string           `json:"repeatCondition,omitempty"`
	Items           []any            `json:"items,omitempty"`
	ItemsFrom       string           `json:"itemsFrom,omitempty"`
	Concurrency     int              `json:"concurrency,omitempty"`
	MaxIterations   int              `json:"maxIterations,omitempty"`
	Body            string           `json:"body,omitempty"` // template name
	Arguments       *Arguments       `json:"arguments,omitempty"`
	Aggregate       *Aggregate       `json:"aggregate,omitempty"`
	PhaseConditions *PhaseConditions `json:"phaseConditions,omitempty"`
	Timeout         string           `json:"timeout,omitempty"`
}

// Aggregate defines how loop iteration results are combined.
type Aggregate struct {
	Strategy   string   `json:"strategy,omitempty"` // last, first, list, merge
	Parameters []string `json:"parameters,omitempty"`
}
