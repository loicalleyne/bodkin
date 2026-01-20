package json2parquet

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	arrowcsv "github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/loicalleyne/bodkin"
	"github.com/loicalleyne/bodkin/pq"
)

func FromReader(r csv.Reader, hasHeader bool, opts ...bodkin.Option) (*arrow.Schema, int, error) {
	var err error
	csvRow := make(map[string]interface{})
	var header []string
	u := bodkin.NewBodkin(opts...)
	// iterate through opts, looking for WithCSVHeader

	if hasHeader {
		header, err = r.Read()
		if err != nil {
			return nil, 0, err
		}
		for _, colName := range header {
			csvRow[colName] = nil
		}
	} else {
		// if no header, we need to read the first row to determine number of columns
		firstRow, err := r.Read()
		if err != nil {
			return nil, 0, err
		}
		for i, v := range firstRow {
			colName := fmt.Sprintf("column_%d", i+1)
			header = append(header, colName)
			csvRow[colName] = v
		}
		u.Unify(csvRow)
		for k := range csvRow {
			csvRow[k] = nil
		}
	}
	for err != io.EOF {
		row, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, err
		}
		for i, colName := range header {
			csvRow[colName] = row[i]
		}
		u.Unify(csvRow)
		if u.Count() > u.MaxCount() {
			break
		}
		for k := range csvRow {
			csvRow[k] = nil
		}
	}
	schema, err := u.Schema()
	if err != nil {
		return nil, u.Count(), err
	}
	// reorder the schema fields according to header
	var fields []arrow.Field
	for _, colName := range header {
		field, ok := schema.FieldsByName(colName)
		if !ok {
			return nil, u.Count(), fmt.Errorf("expected field %s not found in schema", colName)
		}
		if len(field) != 0 {
			fields = append(fields, field[0])
		}
	}
	schema = arrow.NewSchema(fields, nil)
	return schema, u.Count(), err
}

func RecordsFromFile(inputFile, outputFile string, hasHeader bool, schema *arrow.Schema, munger func(io.Reader, io.Writer) error, opts ...parquet.WriterProperty) (int, error) {
	n := 0
	f, err := os.Open(inputFile)
	if err != nil {
		return 0, err
	}
	defer func() {
		if r := recover(); r != nil {
			// fmt.Printf("recover: %+v\n", r)
			fmt.Println("Records read before data error:", n)
		}
	}()
	defer f.Close()
	var prp *parquet.WriterProperties = pq.DefaultWrtp
	if len(opts) != 0 {
		prp = parquet.NewWriterProperties(opts...)
	}
	pw, _, err := pq.NewParquetWriter(schema, prp, outputFile)
	if err != nil {
		return 0, err
	}
	defer pw.Close()

	var r io.Reader
	var rdr *arrowcsv.Reader
	chunk := 1024
	munger = nil
	r = bufio.NewReaderSize(f, 1024*1024*128)
	if munger != nil {
		pr, pwr := io.Pipe()

		go func() {
			// close the writer, so the reader knows there's no more data
			defer pwr.Close()
			munger(r, pwr)
		}()
		rdr = arrowcsv.NewReader(pr, schema, arrowcsv.WithHeader(hasHeader), arrowcsv.WithChunk(chunk))
	} else {
		rdr = arrowcsv.NewReader(r, schema, arrowcsv.WithHeader(hasHeader), arrowcsv.WithChunk(chunk))
	}

	defer rdr.Release()

	for rdr.Next() {
		rec := rdr.RecordBatch()
		err1 := pw.WriteRecord(rec)
		if err != nil {
			err = errors.Join(err, fmt.Errorf("failed to write parquet record: %v", err1))
		}
		n = n + int(rec.NumRows())
	}
	if err := rdr.Err(); err != nil {
		return n, err
	}
	err = pw.Close()
	if err != nil {
		return n, err
	}
	return n, err
}
