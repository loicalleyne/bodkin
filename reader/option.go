package reader

import (
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
