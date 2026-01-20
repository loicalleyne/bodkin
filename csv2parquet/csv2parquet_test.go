package json2parquet

import (
	"bytes"
	"encoding/csv"
	"testing"
)

func TestFromReader_WithHeader(t *testing.T) {
	data := "name,age\nAlice,30\nBob,25\n"
	r := csv.NewReader(bytes.NewBufferString(data))
	schema, count, err := FromReader(*r, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
	if schema == nil {
		t.Fatal("expected non-nil schema")
	}
	fields := schema.Fields()
	if len(fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(fields))
	}
	if fields[0].Name != "name" || fields[1].Name != "age" {
		t.Errorf("unexpected field names: %+v", fields)
	}
}

func TestFromReader_WithoutHeader(t *testing.T) {
	data := "Alice,30\nBob,25\n"
	r := csv.NewReader(bytes.NewBufferString(data))
	schema, count, err := FromReader(*r, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
	if schema == nil {
		t.Fatal("expected non-nil schema")
	}
	fields := schema.Fields()
	if len(fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(fields))
	}
	if fields[0].Name != "column_1" || fields[1].Name != "column_2" {
		t.Errorf("unexpected field names: %+v", fields)
	}
}

func TestFromReader_Empty(t *testing.T) {
	data := ""
	r := csv.NewReader(bytes.NewBufferString(data))
	_, count, err := FromReader(*r, false)
	if err == nil && count != 0 {
		t.Error("expected error or zero count for empty input")
	}
}
