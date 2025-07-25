package bodkin

import (
	"bytes"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

func TestWithIOReaderAndUnifyScan(t *testing.T) {
	// Sample JSON data
	data := `{"field1": "value1", "field2": 42}
	{"field3": 867.5609, "field4": [{"key": "value"}]}`
	reader := bytes.NewReader([]byte(data))

	// Create a Bodkin instance
	b := NewBodkin(WithIOReader(reader, '\n'))

	// Call UnifyScan
	err := b.UnifyScan()
	if err != nil {
		t.Fatalf("UnifyScan failed: %v", err)
	}

	schema, err := b.Schema()
	assert.NoError(t, err)

	expectedFields := []arrow.Field{
		{Name: "field1", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "field2", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "field3", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "field4", Type: arrow.ListOf(arrow.StructOf(
			arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true},
		)), Nullable: true},
	}
	// Validate results
	compareSchemas(t, expectedFields, schema.Fields())
}

func TestWithIOReaderAndUnifyScanWithDelimiter(t *testing.T) {
	// Sample JSON data with custom delimiter
	data := `{"field1": "value1", "field2": 42};{"field3": 867.5609, "field4": [{"key": "value"}]}`
	reader := bytes.NewReader([]byte(data))

	// Create a Bodkin instance with custom delimiter
	b := NewBodkin(WithIOReader(reader, ';'))

	// Call UnifyScan
	err := b.UnifyScan()
	if err != nil {
		t.Fatalf("UnifyScan failed: %v", err)
	}

	schema, err := b.Schema()
	assert.NoError(t, err)

	expectedFields := []arrow.Field{
		{Name: "field1", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "field2", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "field3", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "field4", Type: arrow.ListOf(arrow.StructOf(
			arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true},
		)), Nullable: true},
	}
	// Validate results
	compareSchemas(t, expectedFields, schema.Fields())
}
func TestWithIOReaderAndUnifyScanWithEmptyData(t *testing.T) {
	// Sample empty JSON data
	data := ""
	reader := bytes.NewReader([]byte(data))

	// Create a Bodkin instance
	b := NewBodkin(WithIOReader(reader, '\n'))

	// Call UnifyScan
	err := b.UnifyScan()
	assert.Equal(t, err.Error(), "invalid input : invalid input : EOF")

	_, err = b.Schema()
	assert.Equal(t, err.Error(), "bodkin not initialised")
}

func TestWithIOReaderAndUnifyScanWithInvalidData(t *testing.T) {
	// Sample invalid JSON data
	data := `{"field1": "value1", "field2": 42, {"field3": 867.5609, "field4": [{"key": "value"}]}`
	reader := bytes.NewReader([]byte(data))

	// Create a Bodkin instance
	b := NewBodkin(WithIOReader(reader, '\n'))

	// Call UnifyScan
	err := b.UnifyScan()
	if err == nil {
		t.Fatal("Expected UnifyScan to fail with invalid data, but it succeeded")
	}

	// Validate that the error is related to parsing
	assert.Contains(t, err.Error(), "invalid input : json: cannot unmarshal object into Go value of type string")
}

func TestWithInferTimeUnitsAndSchema(t *testing.T) {
	// Sample JSON data with time, date, and timestamp formats
	data := `{"time_field": "12:34:56","time_field2": "12:34:56.789","date_field": "2025-07-25","timestamp_field": "2025-07-25T12:34:56.789"}`

	// Create a Bodkin instance with time unit inference
	b := NewBodkin(WithInferTimeUnits())

	// Call Unify
	err := b.Unify(data)
	assert.NoError(t, err)

	// Retrieve the schema
	schema, err := b.Schema()
	assert.NoError(t, err, "Failed to retrieve schema")

	// Define the expected schema
	expectedFields := []arrow.Field{
		{Name: "time_field", Type: arrow.FixedWidthTypes.Time64ns, Nullable: true},
		{Name: "time_field2", Type: arrow.FixedWidthTypes.Time64ns, Nullable: true},
		{Name: "date_field", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "timestamp_field", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
	}

	// Validate the schema
	compareSchemas(t, expectedFields, schema.Fields())
}

func TestWithQuotedValuesAreStringsAndSchema(t *testing.T) {
	// Sample JSON data with quoted and unquoted values
	data := `{"field1": "\"quoted_string\"","field2": 42,"field3": "\"12345\""}`
	reader := bytes.NewReader([]byte(data))

	// Create a Bodkin instance with quoted values treated as strings
	b := NewBodkin(WithIOReader(reader, '\n'), WithQuotedValuesAreStrings())

	// Call UnifyScan
	err := b.UnifyScan()
	assert.NoError(t, err, "UnifyScan failed")

	schema, err := b.Schema()
	assert.NoError(t, err, "Failed to retrieve schema")

	// Define the expected schema
	expectedFields := []arrow.Field{
		{Name: "field1", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "field2", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "field3", Type: arrow.BinaryTypes.String, Nullable: true},
	}

	// Validate the schema
	compareSchemas(t, expectedFields, schema.Fields())
}

func TestUnifyAtPath(t *testing.T) {
	// Initial JSON data
	initialData := `{"level1": {"field1": "value1"}}`
	// New JSON data to merge at a specific path
	newData := `{"field2": 42}`

	// Create a Bodkin instance
	b := NewBodkin()

	// Unify the initial data
	err := b.Unify(initialData)
	assert.NoError(t, err)

	// Unify the new data at a specific path
	err = b.UnifyAtPath(newData, "$.level1")
	assert.NoError(t, err)

	// Retrieve the schema
	schema, err := b.Schema()
	assert.NoError(t, err)

	// Define the expected schema
	expectedFields := []arrow.Field{
		{
			Name: "level1",
			Type: arrow.StructOf(
				arrow.Field{Name: "field1", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "field2", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			),
			Nullable: true,
		},
	}

	// Validate the schema
	compareSchemas(t, expectedFields, schema.Fields())
}

func TestExportAndImportSchemaFile(t *testing.T) {
	// Sample JSON data
	data := `{"field1": "value1", "field2": 42}`

	// Create a Bodkin instance
	b := NewBodkin()

	// Unify the data
	err := b.Unify(data)
	assert.NoError(t, err)

	// Export the schema to a file
	exportPath := "test_schema.arrow"
	err = b.ExportSchemaFile(exportPath)
	assert.NoError(t, err)
	defer os.Remove(exportPath)

	// Import the schema from the file
	importedSchema, err := b.ImportSchemaFile(exportPath)
	assert.NoError(t, err)

	// Retrieve the original schema
	originalSchema, err := b.Schema()
	assert.NoError(t, err)

	// Validate that the imported schema matches the original schema
	assert.True(t, originalSchema.Equal(importedSchema), "Imported schema does not match the original schema")
}

func TestExportAndImportSchemaBytes(t *testing.T) {
	// Sample JSON data
	data := `{"field1": "value1", "field2": 42}`

	// Create a Bodkin instance
	b := NewBodkin()

	// Unify the data
	err := b.Unify(data)
	assert.NoError(t, err)

	// Export the schema as bytes
	schemaBytes, err := b.ExportSchemaBytes()
	assert.NoError(t, err)

	// Import the schema from bytes
	importedSchema, err := b.ImportSchemaBytes(schemaBytes)
	assert.NoError(t, err)

	// Retrieve the original schema
	originalSchema, err := b.Schema()
	assert.NoError(t, err)

	// Validate that the imported schema matches the original schema
	assert.True(t, originalSchema.Equal(importedSchema), "Imported schema does not match the original schema")
}

func TestLastSchema(t *testing.T) {
	// Sample JSON data
	data1 := `{"field1": "value1"}`
	data2 := `{"field2": 42}`

	// Create a Bodkin instance
	b := NewBodkin()

	// Unify the first data
	err := b.Unify(data1)
	assert.NoError(t, err)

	// Unify the second data
	err = b.Unify(data2)
	assert.NoError(t, err)

	// Retrieve the last schema
	lastSchema, err := b.LastSchema()
	assert.NoError(t, err)

	// Define the expected schema for the last input
	expectedFields := []arrow.Field{
		{Name: "field2", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}

	// Validate the last schema
	compareSchemas(t, expectedFields, lastSchema.Fields())
}

func TestUnifyWithInvalidInput(t *testing.T) {
	// Invalid JSON data
	data := `{"field1": "value1", "field2": [}`

	// Create a Bodkin instance
	b := NewBodkin()

	// Attempt to unify the invalid data
	err := b.Unify(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid input")
}

func TestUnifyAtPathWithInvalidPath(t *testing.T) {
	// Initial JSON data
	initialData := `{"level1": {"field1": "value1"}}`
	// New JSON data to merge at a non-existent path
	newData := `{"field2": 42}`

	// Create a Bodkin instance
	b := NewBodkin()

	// Unify the initial data
	err := b.Unify(initialData)
	assert.NoError(t, err)

	// Attempt to unify the new data at a non-existent path
	err = b.UnifyAtPath(newData, "$.nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "path not found")
}
