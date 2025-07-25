package json2parquet

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/loicalleyne/bodkin"
)

func TestFromReader(t *testing.T) {
	data := `{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}`
	reader := bytes.NewReader([]byte(data))

	opts := []bodkin.Option{bodkin.WithMaxCount(2)}
	schema, count, err := FromReader(reader, opts...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if schema == nil {
		t.Fatal("expected schema, got nil")
	}

	if count != 2 {
		t.Fatalf("expected count 2, got %d", count)
	}
}

func TestSchemaFromFile(t *testing.T) {
	file, err := os.CreateTemp("", "test.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	data := `{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}`
	_, err = file.WriteString(data)
	if err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	file.Close()

	opts := []bodkin.Option{bodkin.WithMaxCount(2)}
	schema, count, err := SchemaFromFile(file.Name(), opts...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if schema == nil {
		t.Fatal("expected schema, got nil")
	}

	if count != 2 {
		t.Fatalf("expected count 2, got %d", count)
	}
}

func TestRecordsFromFile(t *testing.T) {
	inputFile, err := os.CreateTemp("", "input.json")
	if err != nil {
		t.Fatalf("failed to create temp input file: %v", err)
	}
	defer os.Remove(inputFile.Name())

	outputFile, err := os.CreateTemp("", "output.parquet")
	if err != nil {
		t.Fatalf("failed to create temp output file: %v", err)
	}
	defer os.Remove(outputFile.Name())

	data := `{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}`
	_, err = inputFile.WriteString(data)
	if err != nil {
		t.Fatalf("failed to write to temp input file: %v", err)
	}
	inputFile.Close()

	opts := []bodkin.Option{bodkin.WithMaxCount(2)}
	schema, _, err := SchemaFromFile(inputFile.Name(), opts...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count, err := RecordsFromFile(inputFile.Name(), outputFile.Name(), schema, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if count != 2 {
		t.Fatalf("expected count 2, got %d", count)
	}
}

func TestFromReader_Error(t *testing.T) {
	reader := bytes.NewReader([]byte(`invalid json`))

	opts := []bodkin.Option{}
	_, _, err := FromReader(reader, opts...)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSchemaFromFile_FileNotFound(t *testing.T) {
	_, _, err := SchemaFromFile("nonexistent.json")
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected file not found error, got %v", err)
	}
}
