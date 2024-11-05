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
)

func FromReader(r io.Reader, opts ...bodkin.Option) (*arrow.Schema, int, error) {
	var err, errBundle error
	s := bufio.NewScanner(r)
	var u *bodkin.Bodkin
	var i int
	if s.Scan() {
		u, err = bodkin.NewBodkin(s.Bytes(), opts...)
		if err != nil {
			errBundle = errors.Join(errBundle, err)
		}
		i++
	} else {
		return nil, i, bodkin.ErrInvalidInput
	}
	for s.Scan() {
		u.Unify(s.Bytes())
		i++
		if i > 10000 {
			break
		}
	}
	schema, err := u.Schema()
	if schema == nil {
		if err != nil {
			errBundle = errors.Join(errBundle, err)
		}
		return nil, i, errBundle
	}
	return schema, i, errBundle
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
			fmt.Println(err)
			fmt.Println("Records:", n)
		}
	}()
	defer f.Close()
	var prp *parquet.WriterProperties = defaultWrtp
	if len(opts) != 0 {
		prp = parquet.NewWriterProperties(opts...)
	}
	pw, _, err := NewParquetWriter(schema, prp, outputFile)
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
		n = n + chunk
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