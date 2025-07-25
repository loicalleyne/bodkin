package bodkin

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/stretchr/testify/assert"
)

func TestSchemaInference_SimpleTypes(t *testing.T) {
	jsonInput := `{
        "int_field": 42,
        "string_field": "hello",
        "bool_field": true,
        "float_field": 3.14
    }`

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

	assert.Equal(t, expectedFields, schema.Fields())
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

	assert.Equal(t, expectedFields, schema.Fields())
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

	assert.Equal(t, expectedFields, schema.Fields())
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

	assert.Equal(t, expectedFields, schema.Fields())
}
