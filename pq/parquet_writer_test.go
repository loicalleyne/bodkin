package pq

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestNewParquetWriter(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}
	sc := arrow.NewSchema(fields, nil)

	tempFile := "test.parquet"
	defer os.Remove(tempFile)

	pw, pqschema, err := NewParquetWriter(sc, DefaultWrtp, tempFile)
	if err != nil {
		t.Fatalf("failed to create ParquetWriter: %v", err)
	}
	defer pw.Close()

	if pqschema == nil {
		t.Error("expected non-nil parquet schema")
	}
}

func TestParquetWriter_Write(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}
	sc := arrow.NewSchema(fields, nil)

	tempFile := "test_write.parquet"
	defer os.Remove(tempFile)

	pw, _, err := NewParquetWriter(sc, DefaultWrtp, tempFile)
	if err != nil {
		t.Fatalf("failed to create ParquetWriter: %v", err)
	}
	defer pw.Close()

	record := map[string]interface{}{
		"id":   1,
		"name": "test",
	}
	jsonData, _ := json.Marshal(record)

	if err := pw.Write(jsonData); err != nil {
		t.Fatalf("failed to write record: %v", err)
	}

	if pw.RecordCount() != 1 {
		t.Errorf("expected record count to be 1, got %d", pw.RecordCount())
	}
}

func TestParquetWriter_WriteRecord(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}
	sc := arrow.NewSchema(fields, nil)

	tempFile := "test_write_record.parquet"
	defer os.Remove(tempFile)

	pw, _, err := NewParquetWriter(sc, DefaultWrtp, tempFile)
	if err != nil {
		t.Fatalf("failed to create ParquetWriter: %v", err)
	}
	defer pw.Close()

	recbld := array.NewRecordBuilder(mem, sc)
	defer recbld.Release()

	recbld.Field(0).(*array.Int64Builder).Append(1)
	recbld.Field(1).(*array.StringBuilder).Append("test")

	rec := recbld.NewRecord()
	defer rec.Release()

	if err := pw.WriteRecord(rec); err != nil {
		t.Fatalf("failed to write record: %v", err)
	}

	if pw.RecordCount() != 1 {
		t.Errorf("expected record count to be 1, got %d", pw.RecordCount())
	}
}

func TestParquetWriter_Close(t *testing.T) {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}
	sc := arrow.NewSchema(fields, nil)

	tempFile := "test_close.parquet"
	defer os.Remove(tempFile)

	pw, _, err := NewParquetWriter(sc, DefaultWrtp, tempFile)
	if err != nil {
		t.Fatalf("failed to create ParquetWriter: %v", err)
	}

	if err := pw.Close(); err != nil {
		t.Fatalf("failed to close ParquetWriter: %v", err)
	}
}
