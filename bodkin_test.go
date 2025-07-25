package bodkin

import (
	"bytes"
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
	assert.Equal(t, expectedFields, schema.Fields())
}
