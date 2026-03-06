package model

// Task defines a single task node in a DAG.
type Task struct {
	Name         string      `json:"name"`
	Template     string      `json:"template"`
	Dependencies []string    `json:"dependencies,omitempty"`
	Arguments    *Arguments  `json:"arguments,omitempty"`
	When         string      `json:"when,omitempty"`
	Timeout      string      `json:"timeout,omitempty"`
	ContinueOn   *ContinueOn `json:"continueOn,omitempty"`
	Hooks        *Hooks      `json:"hooks,omitempty"`
}

// DAG defines a directed acyclic graph of tasks.
type DAG struct {
	Entrypoints interface{} `json:"entrypoints,omitempty"` // string or []string
	Tasks       []Task      `json:"tasks"`
	ContinueOn  *ContinueOn `json:"continueOn,omitempty"`
}

// Loop defines a loop construct (repeat, items, or itemsFrom).
type Loop struct {
	RepeatCondition string          `json:"repeatCondition,omitempty"`
	Items           [][]interface{} `json:"items,omitempty"`
	ItemsFrom       string          `json:"itemsFrom,omitempty"`
	Concurrency     int             `json:"concurrency,omitempty"`
	MaxIterations   int             `json:"maxIterations,omitempty"`
	Body            string          `json:"body,omitempty"` // template name
	Aggregate       *Aggregate      `json:"aggregate,omitempty"`
}

// Aggregate defines how loop iteration results are combined.
type Aggregate struct {
	Strategy   string      `json:"strategy,omitempty"` // all, first_success, quorum, custom
	Parameters []Parameter `json:"parameters,omitempty"`
}
