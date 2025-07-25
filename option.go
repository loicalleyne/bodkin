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

// WithCheckForUnion enables checking for list element Union types in the input data.
func WithCheckForUnion() Option {
	return func(cfg config) {
		cfg.checkForUnion = true
	}
}

// WithUseVariantForUnions enables using the Variant type as list element type
// for lists containing Union types in the input data.
func WithUseVariantForUnions() Option {
	return func(cfg config) {
		cfg.useVariantForUnions = true
	}
}

// WithQuotedValuesAreStrings enables handling quoted values as strings.
func WithQuotedValuesAreStrings() Option {
	return func(cfg config) {
		cfg.quotedValuesAreStrings = true
	}
}

// WithMaxCount enables capping the number records to use in Unify evaluations.
func WithMaxCount(i int) Option {
	return func(cfg config) {
		cfg.maxCount = i
	}
}

// WithIOReader provides an io.Reader for a Bodkin to use with UnifyScan(), along
// with a delimiter to use to split datum in the data stream.
// Default delimiter '\n' if delimiter is not provided.
func WithIOReader(r io.Reader, delim byte) Option {
	return func(cfg config) {
		cfg.rr = r
		cfg.br = bufio.NewReaderSize(cfg.rr, 1024*16)
		switch delim {
		case '\n':
			cfg.delim = '\n'
		default:
			cfg.delim = delim
		}
	}
}
