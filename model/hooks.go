package model

// Hooks defines lifecycle hook handlers.
//
// Workflow-level hooks: OnStart, OnSuccess, OnFailure, OnError, OnCancel, OnExit.
// Task-level hooks add: OnSuspend, OnResume (meaningful only for suspendable tasks).
type Hooks struct {
	OnStart   *Hook `json:"onStart,omitempty"`
	OnSuccess *Hook `json:"onSuccess,omitempty"`
	OnFailure *Hook `json:"onFailure,omitempty"`
	OnError   *Hook `json:"onError,omitempty"`
	OnSuspend *Hook `json:"onSuspend,omitempty"`
	OnResume  *Hook `json:"onResume,omitempty"`
	OnCancel  *Hook `json:"onCancel,omitempty"`
	OnExit    *Hook `json:"onExit,omitempty"`
}

// Hook references a template to execute at a lifecycle point.
type Hook struct {
	Template string `json:"template"`
}
