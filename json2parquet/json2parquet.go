package json2parquet

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/loicalleyne/bodkin"
	"github.com/loicalleyne/bodkin/pq"
)

func FromReader(r io.Reader, opts ...bodkin.Option) (*arrow.Schema, int, error) {
	var err error
	s := bufio.NewScanner(r)
	u := bodkin.NewBodkin(opts...)
	for s.Scan() {
		u.Unify(s.Bytes())
		if u.Count() > u.MaxCount() {
			break
		}
	}
	schema, err := u.Schema()
	if err != nil {
		return nil, u.Count(), err
	}
	return schema, u.Count(), err
}

func SchemaFromFile(inputFile string, opts ...bodkin.Option) (*arrow.Schema, int, error) {
	f, err := os.Open(inputFile)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1024*32)
	return FromReader(r, opts...)
}

func RecordsFromFile(inputFile, outputFile string, schema *arrow.Schema, munger func(io.Reader, io.Writer) error, opts ...parquet.WriterProperty) (int, error) {
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
	var rdr *array.JSONReader
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
		rdr = array.NewJSONReader(pr, schema, array.WithChunk(chunk))
	} else {
		rdr = array.NewJSONReader(r, schema, array.WithChunk(chunk))
	}

	defer rdr.Release()

	for rdr.Next() {
		rec := rdr.Record()
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
