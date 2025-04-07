package reader

import (
	"bufio"
	"context"
	"io"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// WithAllocator specifies the Arrow memory allocator used while building records.
func WithAllocator(mem memory.Allocator) Option {
	return func(cfg config) {
		cfg.mem = mem
	}
}

// WithJSONDecoder specifies whether to use goccy/json-go as the Bodkin Reader's decoder.
// The default is the Bodkin DataLoader, a linked list of builders which reduces recursive lookups
// in maps when loading data.
func WithJSONDecoder() Option {
	return func(cfg config) {
		cfg.jsonDecode = true
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

// WithContext specifies the context used while reading data to Arrow records.
// Calling reader.Cancel() will cancel the context and stop reading data.
func WithContext(ctx context.Context) Option {
	return func(cfg config) {
		cfg.readerCtx, cfg.readCancel = context.WithCancel(ctx)
	}
}

// WithIOReader provides an io.Reader to Bodkin Reader, along with a delimiter
// to use to split datum in the data stream. Default delimiter '\n' if delimiter
// is not provided.
func WithIOReader(r io.Reader, delim byte) Option {
	return func(cfg config) {
		cfg.rr = r
		cfg.br = bufio.NewReaderSize(cfg.rr, 1024*1024*16)
		if delim != DefaultDelimiter {
			cfg.delim = delim
		}
	}
}

// WithInputBufferSize specifies the Bodkin Reader's input buffer size.
func WithInputBufferSize(n int) Option {
	return func(cfg config) {
		cfg.inputBufferSize = n
	}
}

// WithRecordBufferSize specifies the Bodkin Reader's record buffer size.
func WithRecordBufferSize(n int) Option {
	return func(cfg config) {
		cfg.recordBufferSize = n
	}
}
