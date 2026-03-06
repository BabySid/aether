package model

// Hooks defines lifecycle hook handlers.
type Hooks struct {
	OnStart   *Hook `json:"onStart,omitempty"`
	OnSuccess *Hook `json:"onSuccess,omitempty"`
	OnFailure *Hook `json:"onFailure,omitempty"`
	OnError   *Hook `json:"onError,omitempty"`
	OnExit    *Hook `json:"onExit,omitempty"`
}

// Hook references a template to execute at a lifecycle point.
type Hook struct {
	Template string `json:"template"`
}
