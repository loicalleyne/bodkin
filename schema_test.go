package bodkin

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/stretchr/testify/assert"
)

func TestSchemaInference_SimpleTypes(t *testing.T) {
	jsonInput := `{"int_field": 42,"string_field": "hello","bool_field": true,"float_field": 3.14}`

	b := NewBodkin()

	err := b.Unify(jsonInput)
	assert.NoError(t, err)

	schema, err := b.Schema()
	assert.NoError(t, err)

	expectedFields := []arrow.Field{
		{Name: "int_field", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "string_field", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "bool_field", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "float_field", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}

	compareSchemas(t, expectedFields, schema.Fields())
}

func TestSchemaInference_DeeplyNestedStructTypes(t *testing.T) {
	jsonInput := `{
        "level1": {
            "level2": {
                "level3": {
                    "int_field": 42,
                    "string_field": "nested"
                }
            }
        }
    }`

	b := NewBodkin()

	err := b.Unify(jsonInput)
	assert.NoError(t, err)

	schema, err := b.Schema()
	assert.NoError(t, err)

	expectedFields := []arrow.Field{
		{
			Name: "level1",
			Type: arrow.StructOf(
				arrow.Field{
					Name: "level2",
					Type: arrow.StructOf(
						arrow.Field{Name: "level3", Type: arrow.StructOf(
							arrow.Field{Name: "int_field", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
							arrow.Field{Name: "string_field", Type: arrow.BinaryTypes.String, Nullable: true},
						), Nullable: true},
					),
					Nullable: true,
				},
			),
			Nullable: true,
		},
	}

	compareSchemas(t, expectedFields, schema.Fields())
}

func TestSchemaInference_UnionTypes(t *testing.T) {
	jsonInput := `{
        "union_field": [true,42,"string",null,{"inner":"thing"}]
    }`

	b := NewBodkin(WithCheckForUnion(), WithUseVariantForUnions())

	err := b.Unify(jsonInput)
	assert.NoError(t, err)

	schema, err := b.Schema()
	assert.NoError(t, err)

	expectedFields := []arrow.Field{
		{
			Name:     "union_field",
			Type:     arrow.ListOf(extensions.NewDefaultVariantType()),
			Nullable: true,
		},
	}

	compareSchemas(t, expectedFields, schema.Fields())
}

func TestSchemaInference_DeeplyNestedMixedTypes(t *testing.T) {
	jsonInput := `{
        "level1": {
            "list_field": [
                {
                    "nested_struct": {
                        "list_field2": [{
                            "key1": "value1",
                            "key2": "value2"
                        }]
                    }
                }
            ]
        }
    }`

	b := NewBodkin()

	err := b.Unify(jsonInput)
	assert.NoError(t, err)

	schema, err := b.Schema()
	assert.NoError(t, err)

	expectedFields := []arrow.Field{
		{
			Name: "level1",
			Type: arrow.StructOf(
				arrow.Field{
					Name: "list_field",
					Type: arrow.ListOf(
						arrow.StructOf(
							arrow.Field{
								Name: "nested_struct",
								Type: arrow.StructOf(
									arrow.Field{
										Name: "list_field2",
										Type: arrow.ListOf(
											arrow.StructOf(
												arrow.Field{Name: "key1", Type: arrow.BinaryTypes.String, Nullable: true},
												arrow.Field{Name: "key2", Type: arrow.BinaryTypes.String, Nullable: true},
											),
										),
										Nullable: true,
									},
								),
								Nullable: true,
							},
						),
					),
					Nullable: true,
				},
			),
			Nullable: true,
		},
	}

	compareSchemas(t, expectedFields, schema.Fields())
}

func compareSchemas(t *testing.T, expected, actual []arrow.Field) {
	assert.Equal(t, len(expected), len(actual), "Schemas have different number of fields")

	expectedMap := make(map[string]arrow.Field)
	for _, field := range expected {
		expectedMap[field.Name] = field
	}

	for _, actualField := range actual {
		expectedField, exists := expectedMap[actualField.Name]
		assert.True(t, exists, "Field %s is missing in the expected schema", actualField.Name)
		assert.Equal(t, expectedField.Nullable, actualField.Nullable, "Field %s has a different nullability", actualField.Name)

		// Compare field types, including nested types
		compareFieldTypes(t, expectedField.Type, actualField.Type, actualField.Name)
	}
}

func compareFieldTypes(t *testing.T, expectedType, actualType arrow.DataType, fieldName string) {
	assert.Equal(t, expectedType.ID(), actualType.ID(), "Field %s has a different type ID", fieldName)

	switch expected := expectedType.(type) {
	case *arrow.StructType:
		actual, ok := actualType.(*arrow.StructType)
		assert.True(t, ok, "Field %s is expected to be a StructType but is not", fieldName)
		compareSchemas(t, expected.Fields(), actual.Fields())
	case *arrow.ListType:
		actual, ok := actualType.(*arrow.ListType)
		assert.True(t, ok, "Field %s is expected to be a ListType but is not", fieldName)
		compareFieldTypes(t, expected.Elem(), actual.Elem(), fieldName+".<list>")
	case *arrow.MapType:
		actual, ok := actualType.(*arrow.MapType)
		assert.True(t, ok, "Field %s is expected to be a MapType but is not", fieldName)
		compareFieldTypes(t, expected.KeyType(), actual.KeyType(), fieldName+".<map_key>")
		compareFieldTypes(t, expected.ItemType(), actual.ItemType(), fieldName+".<map_value>")
	// Add more cases for other complex types if needed
	default:
		// For primitive types, the type ID comparison above is sufficient
	}
}
