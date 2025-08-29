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

func TestSchemaInference_JSONArray(t *testing.T) {
	jsonInput := `{
        "array_field": [1, 2, 3, 4, 5],
		"array_field2": []
    }`

	jsonInput2 := `{
		"array_field2": [42, 43, 44]
    }`
	b := NewBodkin()

	err := b.Unify(jsonInput)
	assert.NoError(t, err)

	err = b.Unify(jsonInput2)
	assert.NoError(t, err)

	schema, err := b.Schema()
	assert.NoError(t, err)

	expectedFields := []arrow.Field{
		{
			Name:     "array_field",
			Type:     arrow.ListOf(arrow.PrimitiveTypes.Int64),
			Nullable: true,
		},
		{
			Name:     "array_field2",
			Type:     arrow.ListOf(arrow.PrimitiveTypes.Int64),
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

func TestUnify_MergeNestedStructFields(t *testing.T) {
	// Initial JSON data
	initialData := `{
        "level1": {
            "nested_field": {
                "field1": "value1",
				"nested_field2": {
                	"field3": "value1",
					"nested_field3": {
						"field5": "value1"
					}
            	}
            }
        }
    }`

	// New JSON data with additional nested fields
	newData := `{
        "level1": {
            "nested_field": {
                "field2": 42,
				"nested_field2": {
					"field4": 42,
					"nested_field3": {
						"field6": "value1"
					}
				}
            }
        }
    }`

	// Create a Bodkin instance
	b := NewBodkin()

	// Unify the initial data
	err := b.Unify(initialData)
	assert.NoError(t, err)

	// Unify the new data
	err = b.Unify(newData)
	assert.NoError(t, err)

	// Retrieve the schema
	schema, err := b.Schema()
	assert.NoError(t, err)

	// Define the expected schema
	expectedFields := []arrow.Field{
		{
			Name: "level1",
			Type: arrow.StructOf(
				arrow.Field{
					Name: "nested_field",
					Type: arrow.StructOf(
						arrow.Field{Name: "field1", Type: arrow.BinaryTypes.String, Nullable: true},
						arrow.Field{Name: "field2", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
						arrow.Field{
							Name: "nested_field2",
							Type: arrow.StructOf(
								arrow.Field{Name: "field3", Type: arrow.BinaryTypes.String, Nullable: true},
								arrow.Field{Name: "field4", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
								arrow.Field{
									Name: "nested_field3",
									Type: arrow.StructOf(
										arrow.Field{Name: "field5", Type: arrow.BinaryTypes.String, Nullable: true},
										arrow.Field{Name: "field6", Type: arrow.BinaryTypes.String, Nullable: true},
									),
									Nullable: true,
								},
							),
							Nullable: true,
						},
					),
					Nullable: true,
				},
			),
			Nullable: true,
		},
	}

	// Validate the schema
	compareSchemas(t, expectedFields, schema.Fields())
}

func TestUnify_MergeListStructFields(t *testing.T) {
	// Initial JSON data
	initialData := `{
		"list_field": [
			{
				"nested_field": {
					"field1": "value1"
				}
			}
		]
	}`

	// New JSON data with additional nested fields
	newData := `{
		"list_field": [
			{
				"nested_field": {
					"field2": 42
				}
			}
		]
	}`

	// Create a Bodkin instance
	b := NewBodkin()

	// Unify the initial data
	err := b.Unify(initialData)
	assert.NoError(t, err)

	// Unify the new data
	err = b.Unify(newData)
	assert.NoError(t, err)

	// Retrieve the schema
	schema, err := b.Schema()
	assert.NoError(t, err)

	// Define the expected schema
	expectedFields := []arrow.Field{
		{
			Name: "list_field",
			Type: arrow.ListOf(
				arrow.StructOf(
					arrow.Field{
						Name: "nested_field",
						Type: arrow.StructOf(
							arrow.Field{Name: "field1", Type: arrow.BinaryTypes.String, Nullable: true},
							arrow.Field{Name: "field2", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
						),
						Nullable: true,
					},
				),
			),
			Nullable: true,
		},
	}

	// Validate the schema
	compareSchemas(t, expectedFields, schema.Fields())
}

func TestUnify_MergeNestedListStructFields(t *testing.T) {
	// Initial JSON data
	initialData := `{
        "level1": {
            "nested_field": {
                "field1": "value1",
				"list_field": [
					{
						"nested_field2": {
							"field3": "value1",
							"nested_field3": {
								"field5": "value1"
							}
						}	
				}
				]
            }
        }
    }`

	// New JSON data with additional nested fields
	newData := `{
        "level1": {
            "nested_field": {
                "field2": 42,
				"list_field": [
					{
						"nested_field2": {
							"field4": 42,
							"nested_field3": {
								"field6": "value1"
							}
						}
					}
					
				]
            }
        }
    }`

	// Create a Bodkin instance
	b := NewBodkin()

	// Unify the initial data
	err := b.Unify(initialData)
	assert.NoError(t, err)

	// Unify the new data
	err = b.Unify(newData)
	assert.NoError(t, err)

	// Retrieve the schema
	schema, err := b.Schema()
	assert.NoError(t, err)

	// Define the expected schema
	expectedFields := []arrow.Field{
		{
			Name: "level1",
			Type: arrow.StructOf(
				arrow.Field{
					Name: "nested_field",
					Type: arrow.StructOf(
						arrow.Field{Name: "field1", Type: arrow.BinaryTypes.String, Nullable: true},
						arrow.Field{Name: "field2", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
						arrow.Field{
							Name: "list_field",
							Type: arrow.ListOf(
								arrow.StructOf(
									arrow.Field{Name: "nested_field2", Type: arrow.StructOf(
										arrow.Field{Name: "field3", Type: arrow.BinaryTypes.String, Nullable: true},
										arrow.Field{Name: "field4", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
										arrow.Field{
											Name: "nested_field3",
											Type: arrow.StructOf(
												arrow.Field{Name: "field5", Type: arrow.BinaryTypes.String, Nullable: true},
												arrow.Field{Name: "field6", Type: arrow.BinaryTypes.String, Nullable: true},
											),
											Nullable: true,
										},
									), Nullable: true},
								),
							),
							Nullable: true,
						},
					),
					Nullable: true,
				},
			),
			Nullable: true,
		},
	}

	// Validate the schema
	compareSchemas(t, expectedFields, schema.Fields())
}

func TestSchemaInference_Matrix(t *testing.T) {
	jsonInput := `{
        "matrix": [
            [1, 2, 3],
            [4, 5, 6],
            [7, 8, 9]
        ]
    }`

	b := NewBodkin()

	// Unify the JSON input
	err := b.Unify(jsonInput)
	assert.NoError(t, err)

	// Retrieve the schema
	schema, err := b.Schema()
	assert.NoError(t, err)

	// Define the expected schema
	expectedFields := []arrow.Field{
		{
			Name: "matrix",
			Type: arrow.ListOf(
				arrow.ListOf(arrow.PrimitiveTypes.Int64),
			),
			Nullable: true,
		},
	}

	// Validate the schema
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
