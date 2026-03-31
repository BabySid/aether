package internal

import (
	"testing"

	"github.com/BabySid/aether/model"
)

func TestResolveTemplateType(t *testing.T) {
	tests := []struct {
		name string
		tmpl *model.Template
		want string
	}{
		{
			name: "DAG",
			tmpl: &model.Template{DAG: &model.DAG{Name: "main"}},
			want: model.TemplateTypeDAG,
		},
		{
			name: "Task",
			tmpl: &model.Template{Task: &model.Task{Name: "run"}},
			want: model.TemplateTypeTask,
		},
		{
			name: "Loop",
			tmpl: &model.Template{Loop: &model.Loop{Name: "iter"}},
			want: model.TemplateTypeLoop,
		},
		{
			name: "Empty",
			tmpl: &model.Template{},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveTemplateType(tt.tmpl)
			if got != tt.want {
				t.Errorf("ResolveTemplateType() = %q, want %q", got, tt.want)
			}
		})
	}
}
