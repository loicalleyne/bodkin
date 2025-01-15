// Package reader contains helpers for reading data and loading to Arrow.
package reader

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
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
const (
	Manual int = iota
	Scanner
)
const DefaultDelimiter byte = byte('\n')

// Option configures an Avro reader/writer.
type (
	Option func(config)
	config *DataReader
)

type DataReader struct {
	rr               io.Reader
	br               *bufio.Reader
	delim            byte
	refs             int64
	source           DataSource
	schema           *arrow.Schema
	bld              *array.RecordBuilder
	mem              memory.Allocator
	opts             []Option
	bldMap           *fieldPos
	ldr              *dataLoader
	cur              arrow.Record
	curBatch         []arrow.Record
	readerCtx        context.Context
	readCancel       func()
	err              error
	anyChan          chan any
	recChan          chan arrow.Record
	recReq           chan struct{}
	bldDone          chan struct{}
	inputLock        atomic.Int32
	factoryLock      atomic.Int32
	wg               sync.WaitGroup
	jsonDecode       bool
	chunk            int
	inputCount       int
	inputBufferSize  int
	recordBufferSize int
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
		delim:            DefaultDelimiter,
		opts:             opts,
	}
	for _, opt := range opts {
		opt(r)
	}

	r.anyChan = make(chan any, r.inputBufferSize)
	r.recChan = make(chan arrow.Record, r.recordBufferSize)
	r.bldDone = make(chan struct{})
	r.recReq = make(chan struct{}, 100)
	r.readerCtx, r.readCancel = context.WithCancel(context.Background())

	if r.rr != nil {
		r.wg.Add(1)
		go r.decode2Chan()
	}
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
	r.wg.Add(1)
	return r, nil
}

// ReadToRecord decodes a datum directly to an arrow.Record. The record
// should be released by the user when done with it.
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

// NextBatch returns whether a []arrow.Record of a specified size can be received
// from the converted record queue. Will still return true if the queue channel is closed and
// last batch of records available < batch size specified.
// The user should check Err() after a call to NextBatch that returned false to check
// if an error took place.
func (r *DataReader) NextBatch(batchSize int) bool {
	if batchSize < 1 {
		batchSize = 1
	}
	if len(r.curBatch) != 0 {
		for _, rec := range r.curBatch {
			rec.Release()
		}
		r.curBatch = []arrow.Record{}
	}
	r.wg.Wait()

	for len(r.curBatch) <= batchSize {
		select {
		case rec, ok := <-r.recChan:
			if !ok && rec == nil {
				if len(r.curBatch) > 0 {
					goto jump
				}
				return false
			}
			if rec != nil {
				r.curBatch = append(r.curBatch, rec)
			}
		case <-r.bldDone:
			if len(r.recChan) > 0 {
				rec := <-r.recChan
				r.curBatch = append(r.curBatch, rec)
			}
		case <-r.readerCtx.Done():
			return false
		}
	}

jump:
	if r.err != nil {
		return false
	}

	return len(r.curBatch) > 0
}

// Next returns whether a Record can be received from the converted record queue.
// The user should check Err() after a call to Next that returned false to check
// if an error took place.
func (r *DataReader) Next() bool {
	var ok bool
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	r.wg.Wait()
	select {
	case r.cur, ok = <-r.recChan:
		if !ok && r.cur == nil {
			return false
		}
	case <-r.bldDone:
		if len(r.recChan) > 0 {
			r.cur = <-r.recChan
		}
	case <-r.readerCtx.Done():
		return false
	}
	if r.err != nil {
		return false
	}

	return r.cur != nil
}

func (r *DataReader) Mode() int {
	switch r.rr {
	case nil:
		return Manual
	default:
		return Scanner
	}
}

func (r *DataReader) Count() int             { return r.inputCount }
func (r *DataReader) ResetCount()            { r.inputCount = 0 }
func (r *DataReader) InputBufferSize() int   { return r.inputBufferSize }
func (r *DataReader) RecBufferSize() int     { return r.recordBufferSize }
func (r *DataReader) DataSource() DataSource { return r.source }
func (r *DataReader) Opts() []Option         { return r.opts }

// Record returns the current Arrow record.
// It is valid until the next call to Next.
func (r *DataReader) Record() arrow.Record { return r.cur }

// Record returns the current Arrow record batch.
// It is valid until the next call to NextBatch.
func (r *DataReader) RecordBatch() []arrow.Record { return r.curBatch }
func (r *DataReader) Schema() *arrow.Schema       { return r.schema }

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

// Peek returns the length of the input data and Arrow Record queues.
func (r *DataReader) Peek() (int, int) {
	return len(r.anyChan), len(r.recChan)
}

// Cancel cancels the Reader's io.Reader scan to Arrow.
func (r *DataReader) Cancel() {
	r.readCancel()
}

// Read loads one datum.
// If the Reader has an io.Reader, Read is a no-op.
func (r *DataReader) Read(a any) error {
	if r.rr != nil {
		return nil
	}
	var err error
	defer func() error {
		if rc := recover(); rc != nil {
			r.err = errors.Join(r.err, fmt.Errorf("panic %v", rc))
		}
		return r.err
	}()
	m, err := InputMap(a)
	if err != nil {
		r.err = errors.Join(r.err, err)
		return err
	}
	r.anyChan <- m
	r.inputCount++
	return nil
}

// Reset resets a Reader to its initial state.
func (r *DataReader) Reset() {
	r.readCancel()
	r.anyChan = make(chan any, r.inputBufferSize)
	r.recChan = make(chan arrow.Record, r.recordBufferSize)
	r.bldDone = make(chan struct{})
	r.inputCount = 0

	// DataReader has an io.Reader
	if r.rr != nil {
		r.br.Reset(r.rr)
		go r.decode2Chan()
		r.wg.Add(1)
	}
	go r.recordFactory()
	r.wg.Add(1)
}
