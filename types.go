package bodkin

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// goType2Arrow maps a Go type to an Arrow DataType.
func goType2Arrow(f *fieldPos, gt any) arrow.DataType {
	var dt arrow.DataType
	switch t := gt.(type) {
	case []any:
		return goType2Arrow(f, t[0])
	case json.Number:
		if _, err := t.Int64(); err == nil {
			f.arrowType = arrow.INT64
			dt = arrow.PrimitiveTypes.Int64
		} else {
			f.arrowType = arrow.FLOAT64
			dt = arrow.PrimitiveTypes.Float64
		}
	case time.Time:
		f.arrowType = arrow.TIMESTAMP
		dt = arrow.FixedWidthTypes.Timestamp_us
		// either 32 or 64 bits
	case int:
		f.arrowType = arrow.INT64
		dt = arrow.PrimitiveTypes.Int64
	// the set of all signed  8-bit integers (-128 to 127)
	case int8:
		f.arrowType = arrow.INT8
		dt = arrow.PrimitiveTypes.Int8
	// the set of all signed 16-bit integers (-32768 to 32767)
	case int16:
		f.arrowType = arrow.INT16
		dt = arrow.PrimitiveTypes.Int16
	// the set of all signed 32-bit integers (-2147483648 to 2147483647)
	case int32:
		f.arrowType = arrow.INT32
		dt = arrow.PrimitiveTypes.Int32
	// the set of all signed 64-bit integers (-9223372036854775808 to 9223372036854775807)
	case int64:
		f.arrowType = arrow.INT64
		dt = arrow.PrimitiveTypes.Int64
	// either 32 or 64 bits
	case uint:
		f.arrowType = arrow.UINT64
		dt = arrow.PrimitiveTypes.Uint64
	// the set of all unsigned  8-bit integers (0 to 255)
	case uint8:
		f.arrowType = arrow.UINT8
		dt = arrow.PrimitiveTypes.Uint8
	// the set of all unsigned 16-bit integers (0 to 65535)
	case uint16:
		f.arrowType = arrow.UINT16
		dt = arrow.PrimitiveTypes.Uint16
	// the set of all unsigned 32-bit integers (0 to 4294967295)
	case uint32:
		f.arrowType = arrow.UINT32
		dt = arrow.PrimitiveTypes.Uint32
	// the set of all unsigned 64-bit integers (0 to 18446744073709551615)
	case uint64:
		f.arrowType = arrow.UINT64
		dt = arrow.PrimitiveTypes.Uint64
	// the set of all IEEE-754 32-bit floating-point numbers
	case float32:
		f.arrowType = arrow.FLOAT32
		dt = arrow.PrimitiveTypes.Float32
	// the set of all IEEE-754 64-bit floating-point numbers
	case float64:
		f.arrowType = arrow.FLOAT64
		dt = arrow.PrimitiveTypes.Float64
	case bool:
		f.arrowType = arrow.BOOL
		dt = arrow.FixedWidthTypes.Boolean
	case string:
		if f.owner.inferTimeUnits {
			for _, r := range timestampMatchers {
				if r.MatchString(t) {
					f.arrowType = arrow.TIMESTAMP
					return arrow.FixedWidthTypes.Timestamp_us
				}
			}
			if dateMatcher.MatchString(t) {
				f.arrowType = arrow.DATE32
				return arrow.FixedWidthTypes.Date32
			}
			if timeMatcher.MatchString(t) {
				f.arrowType = arrow.TIME64
				return arrow.FixedWidthTypes.Time64ns
			}
		}
		if !f.owner.quotedValuesAreStrings {
			if slices.Contains(boolMatcher, t) {
				f.arrowType = arrow.BOOL
				return arrow.FixedWidthTypes.Boolean
			}
			if integerMatcher.MatchString(t) {
				f.arrowType = arrow.INT64
				return arrow.PrimitiveTypes.Int64
			}
			if floatMatcher.MatchString(t) {
				f.arrowType = arrow.FLOAT64
				return arrow.PrimitiveTypes.Float64
			}
		}
		f.arrowType = arrow.STRING
		dt = arrow.BinaryTypes.String
	case []byte:
		f.arrowType = arrow.BINARY
		dt = arrow.BinaryTypes.Binary
	// the set of all complex numbers with float32 real and imaginary parts
	case complex64:
		// TO-DO
	// the set of all complex numbers with float64 real and imaginary parts
	case complex128:
		// TO-DO
	case nil:
		f.arrowType = arrow.NULL
		f.err = fmt.Errorf("%v : %v", ErrUndefinedFieldType, f.namePath())
		dt = arrow.BinaryTypes.Binary
	}
	return dt
}

func arrowTypeID2Type(f *fieldPos, t arrow.Type) arrow.DataType {
	var dt arrow.DataType
	switch t {
	// BOOL is a 1 bit, LSB bit-packed ordering
	case arrow.BOOL:
		dt = arrow.FixedWidthTypes.Boolean
	// the set of all signed  8-bit integers (-128 to 127)
	case arrow.INT8:
		dt = arrow.PrimitiveTypes.Int8
	// the set of all unsigned  8-bit integers (0 to 255)
	case arrow.UINT8:
		dt = arrow.PrimitiveTypes.Uint8
	// the set of all signed 16-bit integers (-32768 to 32767)
	case arrow.INT16:
		dt = arrow.PrimitiveTypes.Int16
	// the set of all unsigned 16-bit integers (0 to 65535)
	case arrow.UINT16:
		dt = arrow.PrimitiveTypes.Uint16
	// the set of all signed 32-bit integers (-2147483648 to 2147483647)
	case arrow.INT32:
		dt = arrow.PrimitiveTypes.Int32
	// the set of all unsigned 32-bit integers (0 to 4294967295)
	case arrow.UINT32:
		dt = arrow.PrimitiveTypes.Uint32
	// the set of all signed 64-bit integers (-9223372036854775808 to 9223372036854775807)
	case arrow.INT64:
		dt = arrow.PrimitiveTypes.Int64
	// the set of all unsigned 64-bit integers (0 to 18446744073709551615)
	case arrow.UINT64:
		dt = arrow.PrimitiveTypes.Uint64
	// the set of all IEEE-754 32-bit floating-point numbers
	case arrow.FLOAT32:
		dt = arrow.PrimitiveTypes.Float32
	// the set of all IEEE-754 64-bit floating-point numbers
	case arrow.FLOAT64:
		dt = arrow.PrimitiveTypes.Float64
	// TIMESTAMP is an exact timestamp encoded with int64 since UNIX epoch
	case arrow.TIMESTAMP:
		dt = arrow.FixedWidthTypes.Timestamp_us
	// DATE32 is int32 days since the UNIX epoch
	case arrow.DATE32:
		dt = arrow.FixedWidthTypes.Date32
	// TIME64 is a signed 64-bit integer, representing either microseconds or
	// nanoseconds since midnight
	case arrow.TIME64:
		dt = arrow.FixedWidthTypes.Time64ns
	// STRING is a UTF8 variable-length string
	case arrow.STRING:
		dt = arrow.BinaryTypes.String
	// BINARY is a Variable-length byte type (no guarantee of UTF8-ness)
	case arrow.BINARY:
		dt = arrow.BinaryTypes.Binary
	// NULL type having no physical storage
	case arrow.NULL:
		dt = arrow.BinaryTypes.Binary
	case arrow.STRUCT:
		var fields []arrow.Field
		for _, c := range f.children {
			fields = append(fields, c.field)
		}
		return arrow.StructOf(fields...)
	case arrow.LIST:
		var fields []arrow.Field
		for _, c := range f.children {
			fields = append(fields, c.field)
		}
		return arrow.StructOf(fields...)
	}
	return dt
}
