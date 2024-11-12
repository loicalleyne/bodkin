// Package reader contains helpers for reading data and loading to Arrow.
package reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	json "github.com/goccy/go-json"
)

type DataSource int

const (
	DataSourceGo DataSource = iota
	DataSourceJSON
	DataSourceAvro
)

// Option configures an Avro reader/writer.
type (
	Option func(config)
	config *DataReader
)

type DataReader struct {
	rr               io.Reader
	refs             int64
	source           DataSource
	schema           *arrow.Schema
	bld              *array.RecordBuilder
	mem              memory.Allocator
	bldMap           *fieldPos
	ldr              *dataLoader
	cur              arrow.Record
	new              arrow.Record
	readerCtx        context.Context
	readCancel       func()
	err              error
	anyChan          chan any
	recChan          chan arrow.Record
	bldDone          chan struct{}
	chunk            int
	inputBufferSize  int
	recordBufferSize int
}

func (r *DataReader) Read(a any) error {
	m, err := InputMap(a)
	if err != nil {
		r.err = errors.Join(r.err, err)
		return err
	}
	r.anyChan <- m
	return nil
}

func (r *DataReader) ReadOne(a any, ldr int) {
	var err error
	defer func() {
		if rc := recover(); rc != nil {
			fmt.Println(rc, err)
		}
	}()
	m, err := InputMap(a)
	if err != nil {
		r.err = errors.Join(r.err, err)
	}
	var v []byte
	v, err = json.Marshal(m)
	if err != nil {
		panic(err)
	}
	switch ldr {
	case 0:
		d := json.NewDecoder(bytes.NewReader(v))
		d.UseNumber()
		err = d.Decode(r.bld)
		if err != nil {
			panic(err)
		}
	case 1:
		err = r.ldr.loadDatum(m)
		if err != nil {
			r.err = err
		}
	}

	r.new = r.bld.NewRecord()
}

func (r *DataReader) Flush() {
	r.readCancel()
}

func (r *DataReader) Next() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.new != nil {
		r.cur = r.new
	}
	if r.err != nil {
		return false
	}

	return r.cur != nil
}

// Next returns whether a Record can be received from the converted record queue.
// The user should check Err() after call to Next that return false to check
// if an error took place.
// func (r *DataReader) Next() bool {
// 	if r.cur != nil {
// 		r.cur.Release()
// 		r.cur = nil
// 	}
// 	fmt.Printf("next: %v:%v\n%v\n", len(r.recChan), len(r.anyChan), r.readerCtx.Err())
// 	if r.readerCtx.Err() != nil {
// 		fmt.Printf("if ctx\n")
// 		if len(r.recChan) > 0 {
// 			r.cur = <-r.recChan
// 		}
// 		if r.err != nil {
// 			return false
// 		}
// 		return r.cur != nil
// 	}
// 	select {
// 	case r.cur = <-r.recChan:
// 		fmt.Printf("r.cur = <-r.recChan\n")
// 	case <-r.bldDone:
// 		fmt.Printf("next: bldDone\n")
// 		if len(r.recChan) > 0 {
// 			r.cur = <-r.recChan
// 		}
// 		return r.cur != nil
// 	}
// 	if r.err != nil {
// 		return false
// 	}

// 	return r.cur != nil
// }

func (r *DataReader) DataSource() DataSource { return r.source }
func (r *DataReader) Schema() *arrow.Schema  { return r.schema }

// Err returns the last error encountered during the reading of data.
func (r *DataReader) Err() error { return r.err }

// Record returns the current Arrow record.
// It is valid until the next call to Next.
func (r *DataReader) Record() arrow.Record { return r.cur }

func NewReader(schema *arrow.Schema, source DataSource, opts ...Option) (*DataReader, error) {
	switch source {
	case DataSourceGo, DataSourceJSON, DataSourceAvro:
		break
	default:
		source = DataSourceGo
	}
	r := &DataReader{
		source:           source,
		schema:           schema,
		mem:              memory.DefaultAllocator,
		inputBufferSize:  100,
		recordBufferSize: 100,
		chunk:            0,
	}
	for _, opt := range opts {
		opt(r)
	}

	r.anyChan = make(chan any, r.inputBufferSize)
	r.recChan = make(chan arrow.Record, r.recordBufferSize)
	r.bldDone = make(chan struct{})
	r.readerCtx, r.readCancel = context.WithCancel(context.Background())
	r.bld = array.NewRecordBuilder(memory.DefaultAllocator, schema)
	r.bldMap = newFieldPos()
	r.bldMap.isStruct = true
	r.source = source
	r.ldr = newDataLoader()
	for idx, fb := range r.bld.Fields() {
		mapFieldBuilders(fb, schema.Field(idx), r.bldMap)
	}
	r.ldr.drawTree(r.bldMap)
	go r.recordFactory()
	return r, nil
}

// WithAllocator specifies the Arrow memory allocator used while building records.
func WithAllocator(mem memory.Allocator) Option {
	return func(cfg config) {
		cfg.mem = mem
	}
}

// WithChunk specifies the chunk size used while reading data to Arrow records.
//
// If n is zero or 1, no chunking will take place and the reader will create
// one record per row.
// If n is greater than 1, chunks of n rows will be read.
func WithChunk(n int) Option {
	return func(cfg config) {
		cfg.chunk = n
	}
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (r *DataReader) Retain() {
	atomic.AddInt64(&r.refs, 1)
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (r *DataReader) Release() {
	// debug.Assert(atomic.LoadInt64(&r.refs) > 0, "too many releases")

	if atomic.AddInt64(&r.refs, -1) == 0 {
		if r.cur != nil {
			r.cur.Release()
		}
	}
}

// func (r *DataReader) ReadFile() {
// 	defer close(r.anyChan)

// 	for r.r.HasNext() {
// 		select {
// 		case <-r.readerCtx.Done():
// 			r.err = fmt.Errorf("avro decoding cancelled, %d records read", r.avroDatumCount)
// 			return
// 		default:
// 			var datum any
// 			m, err := reader.InputMap()
// 			if err != nil {
// 				if errors.Is(err, io.EOF) {
// 					r.err = nil
// 					return
// 				}
// 				r.err = err
// 				return
// 			}
// 			r.anyChan <- datum
// 			r.anyDatumCount++
// 		}
// 	}
// }

func (r *DataReader) recordFactory() {
	defer close(r.recChan)
	recChunk := 0
	switch {
	case r.chunk < 1:
		for data := range r.anyChan {
			fmt.Println(data)
			err := r.ldr.loadDatum(data)
			if err != nil {
				r.err = err
				continue
			}
			r.recChan <- r.bld.NewRecord()
		}
		r.bldDone <- struct{}{}
	case r.chunk >= 1:
		for data := range r.anyChan {
			if recChunk == 0 {
				r.bld.Reserve(r.chunk)
			}
			err := r.ldr.loadDatum(data)
			if err != nil {
				r.err = err
				return
			}
			recChunk++
			if recChunk >= r.chunk {
				r.recChan <- r.bld.NewRecord()
				recChunk = 0
			}
		}
		if recChunk != 0 {
			r.recChan <- r.bld.NewRecord()
		}
		r.bldDone <- struct{}{}
	}
}
