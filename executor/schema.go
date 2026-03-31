package executor

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/BabySid/aether/model"
)

// DynamicOutputs is a sentinel type for executors whose outputs are runtime-determined.
// Usage: executor.SchemaOf[MyConfig, executor.DynamicOutputs](...)
type DynamicOutputs struct{}

// SchemaOf derives an ExecutorSchema by reflecting on the Config and Output struct types.
//
//	C = config struct type (must be a struct; panics otherwise)
//	O = output struct type; use executor.DynamicOutputs for dynamic-output executors
func SchemaOf[C, O any](execType, version, description string) ExecutorSchema {
	assertStructType[C]()
	assertStructType[O]() // no-op for DynamicOutputs (empty struct)

	return ExecutorSchema{
		Type:        execType,
		Version:     version,
		Description: description,
		Inputs:      deriveInputs[C](),  // nil when C has no exported fields
		Outputs:     deriveOutputs[O](), // nil when O = DynamicOutputs
	}
}

// assertStructType panics if T is not a struct type.
func assertStructType[T any]() {
	var zero T
	t := reflect.TypeOf(zero)
	if t != nil && t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("SchemaOf: type parameter must be a struct, got %s", t.Kind()))
	}
}

// deriveInputs reflects C and builds a *model.Inputs with one Parameter per exported field.
// json tag → parameter.Name; desc tag → parameter.Description; type inferred from Go kind.
// Returns nil when C has no exported fields.
func deriveInputs[C any]() *model.Inputs {
	params := reflectToParams[C]()
	if len(params) == 0 {
		return nil
	}
	return &model.Inputs{Parameters: params}
}

// deriveOutputs reflects O and builds a *model.ExecOutputs with one Parameter per exported field.
// Returns nil when O is DynamicOutputs.
func deriveOutputs[O any]() *model.ExecOutputs {
	var zero O
	t := reflect.TypeOf(zero)
	if t == nil || t == reflect.TypeOf(DynamicOutputs{}) {
		return nil
	}
	params := reflectToParams[O]()
	if len(params) == 0 {
		return nil
	}
	return &model.ExecOutputs{Parameters: params}
}

// reflectToParams converts a struct type T into []model.Parameter using json/desc tags.
func reflectToParams[T any]() []model.Parameter {
	var zero T
	t := reflect.TypeOf(zero)
	if t == nil {
		return nil
	}
	var result []model.Parameter
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		jsonTag := f.Tag.Get("json")
		if jsonTag == "-" {
			continue
		}
		name := parseJSONTagName(jsonTag) // "status_code,omitempty" → "status_code"
		if name == "" {
			name = f.Name // fallback to Go field name when no json tag
		}
		result = append(result, model.Parameter{
			Name:        name,
			Type:        goTypeToParamType(f.Type),
			Description: f.Tag.Get("desc"),
		})
	}
	return result
}

// parseJSONTagName extracts the field name from a json struct tag value.
// e.g. "status_code,omitempty" → "status_code"
func parseJSONTagName(tag string) string {
	if idx := strings.Index(tag, ","); idx != -1 {
		return tag[:idx]
	}
	return tag
}

// goTypeToParamType maps a Go reflect.Type to a model.Parameter type string.
func goTypeToParamType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "int"
	case reflect.Float32, reflect.Float64:
		return "float"
	case reflect.Bool:
		return "bool"
	case reflect.Slice, reflect.Array:
		return "array"
	case reflect.Map, reflect.Struct:
		return "object"
	case reflect.Interface:
		return "any" // interface{} / any
	default:
		return "any"
	}
}
