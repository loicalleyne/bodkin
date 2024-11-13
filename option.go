package bodkin

import (
	"bufio"
	"io"
)

// WithInferTimeUnits() enables scanning input string values for time, date and timestamp types.
//
// Times use a format of HH:MM or HH:MM:SS[.zzz] where the fractions of a second cannot
// exceed the precision allowed by the time unit, otherwise unmarshalling will error.
//
// Dates use YYYY-MM-DD format.
//
// Timestamps use RFC3339Nano format except without a timezone, all of the following are valid:
//
//		YYYY-MM-DD
//		YYYY-MM-DD[T]HH
//		YYYY-MM-DD[T]HH:MM
//	 YYYY-MM-DD[T]HH:MM:SS[.zzzzzzzzzz]
func WithInferTimeUnits() Option {
	return func(cfg config) {
		cfg.inferTimeUnits = true
	}
}

// WithTypeConversion enables upgrading the column types to fix compatibilty conflicts.
func WithTypeConversion() Option {
	return func(cfg config) {
		cfg.typeConversion = true
	}
}

// WithTypeConversion enables upgrading the column types to fix compatibilty conflicts.
func WithQuotedValuesAreStrings() Option {
	return func(cfg config) {
		cfg.quotedValuesAreStrings = true
	}
}

// WithMaxCount enables capping the number of Unify evaluations.
func WithMaxCount(i int) Option {
	return func(cfg config) {
		cfg.maxCount = i
	}
}

// WithIOReader provides an io.Reader for a Bodkin to use with UnifyScan().
// A bufio.SplitFunc can optionally be provided, otherwise the default
// ScanLines will be used.
func WithIOReader(r io.Reader, sf bufio.SplitFunc) Option {
	return func(cfg config) {
		cfg.rr = r
		if sf != nil {
			cfg.sf = sf
		}
	}
}
