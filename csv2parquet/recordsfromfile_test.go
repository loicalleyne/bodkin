package json2parquet

import (
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func dummySchema() *arrow.Schema {
	// Minimal schema for testing
	fields := []arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32},
	}
	return arrow.NewSchema(fields, nil)
}

func TestRecordsFromFile_Basic(t *testing.T) {
	input := "test_input.csv"
	output := "test_output.parquet"
	defer os.Remove(input)
	defer os.Remove(output)
	csvData := "name,age\nAlice,30\nBob,25\n"
	err := os.WriteFile(input, []byte(csvData), 0644)
	if err != nil {
		t.Fatalf("failed to write input file: %v", err)
	}
	schema := dummySchema()
	n, err := RecordsFromFile(input, output, true, schema, nil)
	if err != nil {
		t.Fatalf("RecordsFromFile error: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 records, got %d", n)
	}
	if _, err := os.Stat(output); err != nil {
		t.Errorf("output file not created: %v", err)
	}
}

func TestRecordsFromFile_EmptyInput(t *testing.T) {
	input := "test_empty.csv"
	output := "test_empty.parquet"
	defer os.Remove(input)
	defer os.Remove(output)
	err := os.WriteFile(input, []byte(""), 0644)
	if err != nil {
		t.Fatalf("failed to write input file: %v", err)
	}
	schema := dummySchema()
	n, err := RecordsFromFile(input, output, true, schema, nil)
	if err == nil && n != 0 {
		t.Error("expected error or zero records for empty input")
	}
}

func TestRecordsFromFile_NoHeader(t *testing.T) {
	input := "test_noheader.csv"
	output := "test_noheader.parquet"
	defer os.Remove(input)
	defer os.Remove(output)
	csvData := "Alice,30\nBob,25\n"
	err := os.WriteFile(input, []byte(csvData), 0644)
	if err != nil {
		t.Fatalf("failed to write input file: %v", err)
	}
	fields := []arrow.Field{
		{Name: "column_1", Type: arrow.BinaryTypes.String},
		{Name: "column_2", Type: arrow.PrimitiveTypes.Int32},
	}
	schema := arrow.NewSchema(fields, nil)
	n, err := RecordsFromFile(input, output, false, schema, nil)
	if err != nil {
		t.Fatalf("RecordsFromFile (no header) error: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 records, got %d", n)
	}
	if _, err := os.Stat(output); err != nil {
		t.Errorf("output file not created: %v", err)
	}
}
