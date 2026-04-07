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
// Constraint: output must be a flat struct — no embedded (anonymous) fields, pointer fields,
// or nested structs. All fields should be primitive types, string, []T, map[string]T, or any.
// This constraint ensures OutputFrom and deriveParamSchema share consistent reflection logic.
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
// Note: the handling depends on the type of model.Parameter.Value:
//   - If Value is json.RawMessage → use directly, no need to re-Marshal
//   - If Value is any (interface{}) → must Marshal then Unmarshal
//
// The current implementation assumes Value is json.RawMessage (consistent with OutputFrom output).
func BindInputs(inputs *model.Inputs, dst any) error {
	if inputs == nil {
		return nil
	}
	rv := reflect.ValueOf(dst)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("BindInputs: dst must be a non-nil pointer, got %T", dst)
	}
	if rv.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("BindInputs: dst must point to a struct, got pointer to %s", rv.Elem().Kind())
	}
	index := make(map[string]json.RawMessage, len(inputs.Parameters))
	for _, p := range inputs.Parameters {
		// p.Value is already json.RawMessage ([]byte), use directly
		index[p.Name] = p.Value
	}
	t := rv.Elem().Type()
	v := rv.Elem()
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
