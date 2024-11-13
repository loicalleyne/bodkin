// Package reader contains helpers for reading data and loading to Arrow.
package reader

import (
	"bufio"
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
	sf               bufio.SplitFunc
	sc               *bufio.Scanner
	refs             int64
	source           DataSource
	schema           *arrow.Schema
	bld              *array.RecordBuilder
	mem              memory.Allocator
	bldMap           *fieldPos
	ldr              *dataLoader
	cur              arrow.Record
	readerCtx        context.Context
	readCancel       func()
	err              error
	anyChan          chan any
	recChan          chan arrow.Record
	recReq           chan struct{}
	bldDone          chan struct{}
	jsonDecode       bool
	chunk            int
	inputBufferSize  int
	recordBufferSize int
	countInput       int
}

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
		inputBufferSize:  1024 * 64,
		recordBufferSize: 1024 * 64,
		chunk:            0,
	}
	for _, opt := range opts {
		opt(r)
	}

	r.anyChan = make(chan any, r.inputBufferSize)
	r.recChan = make(chan arrow.Record, r.recordBufferSize)
	r.bldDone = make(chan struct{})
	r.recReq = make(chan struct{}, 100)
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

func (r *DataReader) ReadToRecord(a any) (arrow.Record, error) {
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

	switch r.jsonDecode {
	case true:
		var v []byte
		v, err = json.Marshal(m)
		if err != nil {
			r.err = err
			return nil, err
		}
		d := json.NewDecoder(bytes.NewReader(v))
		d.UseNumber()
		err = d.Decode(r.bld)
		if err != nil {
			return nil, err
		}
	default:
		err = r.ldr.loadDatum(m)
		if err != nil {
			return nil, err
		}
	}

	return r.bld.NewRecord(), nil
}

func (r *DataReader) Schema() *arrow.Schema { return r.schema }

// Err returns the last error encountered during the reading of data.
func (r *DataReader) Err() error { return r.err }

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
