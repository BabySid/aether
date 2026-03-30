package executor

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/BabySid/aether/model"
)

// OutputFrom converts an output struct to *model.ExecOutputs by reflecting its fields.
// The json tag on each field becomes the parameter.Name; the field value is JSON-serialised.
// Fields tagged with json:"-" are skipped.
//
// 约束：output 必须是扁平 struct（flat struct），不支持嵌入 struct（anonymous field）、
// 指针字段或嵌套 struct。所有字段应为基础类型、string、[]T、map[string]T 或 any。
// 这一约束确保 OutputFrom 与 deriveParamSchema 的反射逻辑保持一致。
//
// Example:
//
//	return executor.OutputFrom(HttpOutput{StatusCode: 200, Body: body})
func OutputFrom(output any) (*model.ExecOutputs, error) {
	t := reflect.TypeOf(output)
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("OutputFrom: expected struct, got %s", t.Kind())
	}
	v := reflect.ValueOf(output)
	var params []model.Parameter
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Anonymous {
			return nil, fmt.Errorf("OutputFrom: embedded (anonymous) fields not supported: %s", f.Name)
		}
		jsonTag := f.Tag.Get("json")
		if jsonTag == "-" {
			continue
		}
		name := parseJSONTagName(jsonTag)
		if name == "" {
			name = f.Name
		}
		raw, err := json.Marshal(v.Field(i).Interface())
		if err != nil {
			return nil, fmt.Errorf("OutputFrom: field %s: %w", name, err)
		}
		params = append(params, model.Parameter{Name: name, Value: raw})
	}
	return &model.ExecOutputs{Parameters: params}, nil
}

// BindInputs maps model.Inputs.Parameters into dst by matching each parameter's Name
// against the json tag of the corresponding struct field.
// Phase 2 extension — not required for core Output binding.
//
// 注意：model.Parameter.Value 的类型决定了此处的处理方式：
//   - 若 Value 是 json.RawMessage → 直接使用，无需再次 Marshal
//   - 若 Value 是 any（interface{}）→ 需要 Marshal 再 Unmarshal
//
// 当前实现假设 Value 为 json.RawMessage（与 OutputFrom 产出一致）。
func BindInputs(inputs *model.Inputs, dst any) error {
	if inputs == nil {
		return nil
	}
	index := make(map[string]json.RawMessage, len(inputs.Parameters))
	for _, p := range inputs.Parameters {
		// p.Value 已是 json.RawMessage（[]byte），直接使用
		index[p.Name] = p.Value
	}
	t := reflect.TypeOf(dst).Elem()
	v := reflect.ValueOf(dst).Elem()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := parseJSONTagName(f.Tag.Get("json"))
		if name == "" || name == "-" {
			continue
		}
		if raw, ok := index[name]; ok {
			ptr := v.Field(i).Addr().Interface()
			if err := json.Unmarshal(raw, ptr); err != nil {
				return fmt.Errorf("BindInputs: field %s: %w", name, err)
			}
		}
	}
	return nil
}
