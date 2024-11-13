package reader

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type dataLoader struct {
	idx, depth int32
	list       *fieldPos
	item       *fieldPos
	mapField   *fieldPos
	mapKey     *fieldPos
	mapValue   *fieldPos
	fields     []*fieldPos
	children   []*dataLoader
}

var (
	ErrNullStructData = errors.New("null struct data")
)

func newDataLoader() *dataLoader { return &dataLoader{idx: 0, depth: 0} }

// drawTree takes the tree of field builders produced by mapFieldBuilders()
// and produces another tree structure and aggregates fields whose values can
// be retrieved from a `map[string]any` into a slice of builders, and creates a hierarchy to
// deal with nested types (lists and maps).
func (d *dataLoader) drawTree(field *fieldPos) {
	for _, f := range field.children() {
		if f.isList || f.isMap {
			if f.isList {
				c := d.newListChild(f)
				if !f.childrens[0].isList {
					c.item = f.childrens[0]
					c.drawTree(f.childrens[0])
				} else {
					c.drawTree(f.childrens[0].childrens[0])
				}
			}
			if f.isMap {
				c := d.newMapChild(f)
				if !arrow.IsNested(f.childrens[1].builder.Type().ID()) {
					c.mapKey = f.childrens[0]
					c.mapValue = f.childrens[1]
				} else {
					c.mapKey = f.childrens[0]
					m := c.newChild()
					m.mapValue = f.childrens[1]
					m.drawTree(f.childrens[1])
				}
			}
		} else {
			d.fields = append(d.fields, f)
			if len(f.children()) > 0 {
				d.drawTree(f)
			}
		}
	}
}

// loadDatum loads data to the schema fields' builder functions.
// Since array.StructBuilder.AppendNull() will recursively append null to all of the
// struct's fields, in the case of nil being passed to a struct's builderFunc it will
// return a ErrNullStructData error to signal that all its sub-fields can be skipped.
func (d *dataLoader) loadDatum(data any) error {
	if d.list == nil && d.mapField == nil {
		if d.mapValue != nil {
			d.mapValue.appendFunc(data)
		}
		var NullParent *fieldPos
		for _, f := range d.fields {
			if f.parent == NullParent {
				continue
			}
			if d.mapValue == nil {
				err := f.appendFunc(f.getValue(data))
				if err != nil {
					if err == ErrNullStructData {
						NullParent = f
						continue
					}
					return err
				}
			} else {
				switch dt := data.(type) {
				case nil:
					err := f.appendFunc(dt)
					if err != nil {
						if err == ErrNullStructData {
							NullParent = f
							continue
						}
						return err
					}
				case []any:
					if len(d.children) < 1 {
						for _, e := range dt {
							err := f.appendFunc(e)
							if err != nil {
								if err == ErrNullStructData {
									NullParent = f
									continue
								}
								return err
							}
						}
					} else {
						for _, e := range dt {
							d.children[0].loadDatum(e)
						}
					}
				case map[string]any:
					err := f.appendFunc(f.getValue(dt))
					if err != nil {
						if err == ErrNullStructData {
							NullParent = f
							continue
						}
						return err
					}
				}

			}
		}
		for _, c := range d.children {
			if c.list != nil {
				c.loadDatum(c.list.getValue(data))
			}
			if c.mapField != nil {
				switch dt := data.(type) {
				case nil:
					c.loadDatum(dt)
				case map[string]any:
					c.loadDatum(c.mapField.getValue(dt))
				default:
					c.loadDatum(c.mapField.getValue(data))
				}
			}
		}
	} else {
		if d.list != nil {
			switch dt := data.(type) {
			case nil:
				d.list.appendFunc(dt)
			case []any:
				d.list.appendFunc(dt)
				for _, e := range dt {
					if d.item != nil {
						d.item.appendFunc(e)
					}
					var NullParent *fieldPos
					for _, f := range d.fields {
						if f.parent == NullParent {
							continue
						}
						err := f.appendFunc(f.getValue(e))
						if err != nil {
							if err == ErrNullStructData {
								NullParent = f
								continue
							}
							return err
						}
					}
					for _, c := range d.children {
						if c.list != nil {
							c.loadDatum(c.list.getValue(e))
						}
						if c.mapField != nil {
							c.loadDatum(c.mapField.getValue(e))
						}
					}
				}
			case map[string]any:
				d.list.appendFunc(dt)  //
				for _, e := range dt { //
					if d.item != nil {
						d.item.appendFunc(e)
					}
					var NullParent *fieldPos
					for _, f := range d.fields {
						if f.parent == NullParent {
							continue
						}
						err := f.appendFunc(f.getValue(e))
						if err != nil {
							if err == ErrNullStructData {
								NullParent = f
								continue
							}
							return err
						}
					}
					for _, c := range d.children {
						c.loadDatum(c.list.getValue(e))
					}
				}
			default:
				d.list.appendFunc(data)
				d.item.appendFunc(dt)
			}
		}
		if d.mapField != nil {
			switch dt := data.(type) {
			case nil:
				d.mapField.appendFunc(dt)
			case map[string]any:
				d.mapField.appendFunc(dt)
				for k, v := range dt {
					d.mapKey.appendFunc(k)
					if d.mapValue != nil {
						d.mapValue.appendFunc(v)
					} else {
						d.children[0].loadDatum(v)
					}
				}
			}
		}
	}
	return nil
}

func (d *dataLoader) newChild() *dataLoader {
	var child *dataLoader = &dataLoader{
		depth: d.depth + 1,
	}
	d.children = append(d.children, child)
	return child
}

func (d *dataLoader) newListChild(list *fieldPos) *dataLoader {
	var child *dataLoader = &dataLoader{
		list:  list,
		item:  list.childrens[0],
		depth: d.depth + 1,
	}
	d.children = append(d.children, child)
	return child
}

func (d *dataLoader) newMapChild(mapField *fieldPos) *dataLoader {
	var child *dataLoader = &dataLoader{
		mapField: mapField,
		depth:    d.depth + 1,
	}
	d.children = append(d.children, child)
	return child
}

type fieldPos struct {
	parent       *fieldPos
	fieldName    string
	builder      array.Builder
	source       DataSource
	path         []string
	isList       bool
	isItem       bool
	isStruct     bool
	isMap        bool
	typeName     string
	appendFunc   func(val interface{}) error
	metadatas    arrow.Metadata
	childrens    []*fieldPos
	index, depth int32
}

func newFieldPos() *fieldPos { return &fieldPos{index: -1} }

func (f *fieldPos) children() []*fieldPos { return f.childrens }

func (f *fieldPos) newChild(childName string, childBuilder array.Builder, meta arrow.Metadata) *fieldPos {
	var child fieldPos = fieldPos{
		parent:    f,
		source:    f.source,
		fieldName: childName,
		builder:   childBuilder,
		metadatas: meta,
		index:     int32(len(f.childrens)),
		depth:     f.depth + 1,
	}
	if f.isList {
		child.isItem = true
	}
	child.path = child.buildNamePath()
	f.childrens = append(f.childrens, &child)
	return &child
}

func (f *fieldPos) buildNamePath() []string {
	var path []string

	cur := f
	for i := f.depth - 1; i >= 0; i-- {
		if cur.fieldName != "item" {
			path = append([]string{cur.fieldName}, path...)
		} else {
			break
		}

		if !cur.parent.isMap {
			cur = cur.parent
		}
	}
	if f.parent.parent != nil && f.parent.parent.isList {
		var listPath []string
		for i := len(path) - 1; i >= 0; i-- {
			if path[i] != "elem" {
				listPath = append([]string{path[i]}, listPath...)
			} else {
				return listPath
			}
		}
	}
	if f.parent != nil && f.parent.fieldName == "item" {
		var listPath []string
		for i := len(path) - 1; i >= 0; i-- {
			if path[i] != "item" {
				listPath = append([]string{path[i]}, listPath...)
			} else {
				return listPath
			}
		}
	}
	// avro/arrow Maps ?
	// if f.parent != nil && f.parent.fieldName == "value" {
	// 	for i := len(path) - 1; i >= 0; i-- {
	// 		if path[i] != "value" {
	// 			listPath = append([]string{path[i]}, listPath...)
	// 		} else {
	// 			return listPath
	// 		}
	// 	}
	// }
	return path
}

// NamePath returns a slice of keys making up the path to the field
func (f *fieldPos) namePath() []string { return f.path }

// GetValue retrieves the value from the map[string]any
// by following the field's key path
func (f *fieldPos) getValue(m any) any {
	if _, ok := m.(map[string]any); !ok {
		return m
	}
	for _, key := range f.namePath() {
		valueMap, ok := m.(map[string]any)
		if !ok {
			if key == "item" {
				return m
			}
			return nil
		}
		m, ok = valueMap[key]
		if !ok {
			return nil
		}
	}
	return m
}

// Data is loaded to Arrow arrays using the following type mapping:
//
//	Avro					Go    			Arrow
//	null					nil				Null
//	boolean					bool			Boolean
//	bytes					[]byte			Binary
//	float					float32			Float32
//	double					float64			Float64
//	long					int64			Int64
//	int						int32  			Int32
//	string					string			String
//	array					[]interface{}	List
//	enum					string			Dictionary
//	fixed					[]byte			FixedSizeBinary
//	map and record			map[string]any	Struct
//
// mapFieldBuilders builds a tree of field builders matching the Arrow schema
func mapFieldBuilders(b array.Builder, field arrow.Field, parent *fieldPos) {
	f := parent.newChild(field.Name, b, field.Metadata)
	switch bt := b.(type) {
	case *array.BinaryBuilder:
		f.appendFunc = func(data interface{}) error {
			appendBinaryData(bt, data, f.source)
			return nil
		}
	case *array.BinaryDictionaryBuilder:
		// has metadata for Avro enum symbols
		f.appendFunc = func(data interface{}) error {
			appendBinaryDictData(bt, data, f.source)
			return nil
		}
		// add Avro enum symbols to builder
		sb := array.NewStringBuilder(memory.DefaultAllocator)
		for _, v := range field.Metadata.Values() {
			sb.Append(v)
		}
		sa := sb.NewStringArray()
		bt.InsertStringDictValues(sa)
	case *array.BooleanBuilder:
		f.appendFunc = func(data interface{}) error {
			appendBoolData(bt, data, f.source)
			return nil
		}
	case *array.Date32Builder:
		f.appendFunc = func(data interface{}) error {
			appendDate32Data(bt, data, f.source)
			return nil
		}
	case *array.Decimal128Builder:
		f.appendFunc = func(data interface{}) error {
			err := appendDecimal128Data(bt, data, f.source)
			if err != nil {
				return err
			}
			return nil
		}
	case *array.Decimal256Builder:
		f.appendFunc = func(data interface{}) error {
			err := appendDecimal256Data(bt, data, f.source)
			if err != nil {
				return err
			}
			return nil
		}
	case *extensions.UUIDBuilder:
		f.appendFunc = func(data interface{}) error {
			switch dt := data.(type) {
			case nil:
				bt.AppendNull()
			case string:
				err := bt.AppendValueFromString(dt)
				if err != nil {
					return err
				}
			case []byte:
				err := bt.AppendValueFromString(string(dt))
				if err != nil {
					return err
				}
			}
			return nil
		}
	case *array.FixedSizeBinaryBuilder:
		f.appendFunc = func(data interface{}) error {
			appendFixedSizeBinaryData(bt, data, f.source)
			return nil
		}
	case *array.Float32Builder:
		f.appendFunc = func(data interface{}) error {
			appendFloat32Data(bt, data, f.source)
			return nil
		}
	case *array.Float64Builder:
		f.appendFunc = func(data interface{}) error {
			appendFloat64Data(bt, data, f.source)
			return nil
		}
	case *array.Int32Builder:
		f.appendFunc = func(data interface{}) error {
			appendInt32Data(bt, data, f.source)
			return nil
		}
	case *array.Int64Builder:
		f.appendFunc = func(data interface{}) error {
			appendInt64Data(bt, data, f.source)
			return nil
		}
	case *array.LargeListBuilder:
		vb := bt.ValueBuilder()
		f.isList = true
		mapFieldBuilders(vb, field.Type.(*arrow.LargeListType).ElemField(), f)
		f.appendFunc = func(data interface{}) error {
			switch dt := data.(type) {
			case nil:
				bt.AppendNull()
			case []interface{}:
				if len(dt) == 0 {
					bt.AppendEmptyValue()
				} else {
					bt.Append(true)
				}
			default:
				bt.Append(true)
			}
			return nil
		}
	case *array.ListBuilder:
		vb := bt.ValueBuilder()
		f.isList = true
		mapFieldBuilders(vb, field.Type.(*arrow.ListType).ElemField(), f)
		f.appendFunc = func(data interface{}) error {
			switch dt := data.(type) {
			case nil:
				bt.AppendNull()
			case []interface{}:
				if len(dt) == 0 {
					bt.AppendEmptyValue()
				} else {
					bt.Append(true)
				}
			default:
				bt.Append(true)
			}
			return nil
		}
	case *array.MapBuilder:
		// has metadata for objects in values
		f.isMap = true
		kb := bt.KeyBuilder()
		ib := bt.ItemBuilder()
		mapFieldBuilders(kb, field.Type.(*arrow.MapType).KeyField(), f)
		mapFieldBuilders(ib, field.Type.(*arrow.MapType).ItemField(), f)
		f.appendFunc = func(data interface{}) error {
			switch data.(type) {
			case nil:
				bt.AppendNull()
			default:
				bt.Append(true)
			}
			return nil
		}
	case *array.MonthDayNanoIntervalBuilder:
		f.appendFunc = func(data interface{}) error {
			appendDurationData(bt, data, f.source)
			return nil
		}
	case *array.StringBuilder:
		f.appendFunc = func(data interface{}) error {
			appendStringData(bt, data, f.source)
			return nil
		}
	case *array.StructBuilder:
		// has metadata for Avro Union named types
		f.typeName, _ = field.Metadata.GetValue("typeName")
		f.isStruct = true
		// create children
		for i, p := range field.Type.(*arrow.StructType).Fields() {
			mapFieldBuilders(bt.FieldBuilder(i), p, f)
		}
		f.appendFunc = func(data interface{}) error {
			switch data.(type) {
			case nil:
				bt.AppendNull()
				return ErrNullStructData
			default:
				bt.Append(true)
			}
			return nil
		}
	case *array.Time32Builder:
		f.appendFunc = func(data interface{}) error {
			appendTime32Data(bt, data, f.source)
			return nil
		}
	case *array.Time64Builder:
		f.appendFunc = func(data interface{}) error {
			appendTime64Data(bt, data, f.source)
			return nil
		}
	case *array.TimestampBuilder:
		f.appendFunc = func(data interface{}) error {
			appendTimestampData(bt, data, f.source)
			return nil
		}
	}
}

func appendBinaryData(b *array.BinaryBuilder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case []byte:
		b.Append(dt)
	case map[string]any:
		if source == DataSourceAvro {
			switch ct := dt["bytes"].(type) {
			case nil:
				b.AppendNull()
			default:
				b.Append(ct.([]byte))
			}
		}
	default:
		b.Append(fmt.Append([]byte{}, data))
	}
}

func appendBinaryDictData(b *array.BinaryDictionaryBuilder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case string:
		b.AppendString(dt)
	case map[string]any:
		if source == DataSourceAvro {
			switch v := dt["string"].(type) {
			case nil:
				b.AppendNull()
			case string:
				b.AppendString(v)
			}
		}
	}
}

func appendBoolData(b *array.BooleanBuilder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case bool:
		b.Append(dt)
	case map[string]any:
		if source == DataSourceAvro {
			switch v := dt["boolean"].(type) {
			case nil:
				b.AppendNull()
			case bool:
				b.Append(v)
			}
		}
	}
}

func appendDate32Data(b *array.Date32Builder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case json.Number:
		// TO-DO
	case string:
		date, _ := time.Parse(time.DateOnly, dt)
		b.Append(arrow.Date32FromTime(date))
	case time.Time:
		b.Append(arrow.Date32FromTime(dt))
	case int32:
		b.Append(arrow.Date32(dt))
	case map[string]any:
		if source == DataSourceAvro {
			switch v := dt["int"].(type) {
			case nil:
				b.AppendNull()
			case int32:
				b.Append(arrow.Date32(v))
			}
		}
	}
}

func appendDecimal128Data(b *array.Decimal128Builder, data any, source DataSource) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case []byte:
		// TO-DO
		if source == DataSourceAvro {
			buf := bytes.NewBuffer(dt)
			if len(dt) <= 38 {
				var intData int64
				err := binary.Read(buf, binary.BigEndian, &intData)
				if err != nil {
					return err
				}
				b.Append(decimal128.FromI64(intData))
			} else {
				var bigIntData big.Int
				b.Append(decimal128.FromBigInt(bigIntData.SetBytes(buf.Bytes())))
			}
		}
	case map[string]any:
		if source == DataSourceAvro {
			buf := bytes.NewBuffer(dt["bytes"].([]byte))
			if len(dt["bytes"].([]byte)) <= 38 {
				var intData int64
				err := binary.Read(buf, binary.BigEndian, &intData)
				if err != nil {
					return err
				}
				b.Append(decimal128.FromI64(intData))
			} else {
				var bigIntData big.Int
				b.Append(decimal128.FromBigInt(bigIntData.SetBytes(buf.Bytes())))
			}
		}
	}
	return nil
}

func appendDecimal256Data(b *array.Decimal256Builder, data any, source DataSource) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case []byte:
		// TO-DO
		if source == DataSourceAvro {
			var bigIntData big.Int
			buf := bytes.NewBuffer(dt)
			b.Append(decimal256.FromBigInt(bigIntData.SetBytes(buf.Bytes())))
		}
	case map[string]any:
		if source == DataSourceAvro {
			var bigIntData big.Int
			buf := bytes.NewBuffer(dt["bytes"].([]byte))
			b.Append(decimal256.FromBigInt(bigIntData.SetBytes(buf.Bytes())))
		}
	}
	return nil
}

// Avro duration logical type annotates Avro fixed type of size 12, which stores three little-endian
// unsigned integers that represent durations at different granularities of time. The first stores
// a number in months, the second stores a number in days, and the third stores a number in milliseconds.
//
// https://pkg.go.dev/time#Duration
// Go time.Duration int64
// A Duration represents the elapsed time between two instants as an int64 nanosecond count.
// The representation limits the largest representable duration to approximately 290 years.
func appendDurationData(b *array.MonthDayNanoIntervalBuilder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case []byte:
		// TO-DO
		if source == DataSourceAvro {
			dur := new(arrow.MonthDayNanoInterval)
			dur.Months = int32(binary.LittleEndian.Uint16(dt[:3]))
			dur.Days = int32(binary.LittleEndian.Uint16(dt[4:7]))
			dur.Nanoseconds = int64(binary.LittleEndian.Uint32(dt[8:]) * 1000000)
			b.Append(*dur)
		}
	case map[string]any:
		if source == DataSourceAvro {
			switch dtb := dt["bytes"].(type) {
			case nil:
				b.AppendNull()
			case []byte:
				dur := new(arrow.MonthDayNanoInterval)
				dur.Months = int32(binary.LittleEndian.Uint16(dtb[:3]))
				dur.Days = int32(binary.LittleEndian.Uint16(dtb[4:7]))
				dur.Nanoseconds = int64(binary.LittleEndian.Uint32(dtb[8:]) * 1000000)
				b.Append(*dur)
			}
		}
	}
}

func appendFixedSizeBinaryData(b *array.FixedSizeBinaryBuilder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case []byte:
		b.Append(dt)
	case map[string]any:
		if source == DataSourceAvro {
			switch v := dt["bytes"].(type) {
			case nil:
				b.AppendNull()
			case []byte:
				b.Append(v)
			}
		}
	}
}

func appendFloat32Data(b *array.Float32Builder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case float32:
		b.Append(dt)
	case json.Number:
		f, _ := dt.Float64()
		b.Append(float32(f))
	case string:
		i, _ := strconv.ParseFloat(dt, 32)
		b.Append(float32(i))
	case map[string]any:
		if source == DataSourceAvro {
			switch v := dt["float"].(type) {
			case nil:
				b.AppendNull()
			case float32:
				b.Append(v)
			}
		}
	}
}

func appendFloat64Data(b *array.Float64Builder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case float64:
		b.Append(dt)
	case json.Number:
		f, _ := dt.Float64()
		b.Append(f)
	case string:
		i, _ := strconv.ParseFloat(dt, 64)
		b.Append(i)
	case map[string]any:
		if source == DataSourceAvro {
			switch v := dt["double"].(type) {
			case nil:
				b.AppendNull()
			case float64:
				b.Append(v)
			}
		}
	}
}

func appendInt8Data(b *array.Int8Builder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case int:
		b.Append(int8(dt))
	case int8:
		b.Append(dt)
	case json.Number:
		i, _ := dt.Int64()
		b.Append(int8(i))
	case string:
		i, _ := strconv.ParseInt(dt, 10, 8)
		b.Append(int8(i))
	case map[string]any:

	}
}

func appendInt16Data(b *array.Int16Builder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case int:
		b.Append(int16(dt))
	case int16:
		b.Append(dt)
	case json.Number:
		i, _ := dt.Int64()
		b.Append(int16(i))
	case string:
		i, _ := strconv.ParseInt(dt, 10, 16)
		b.Append(int16(i))
	case map[string]any:

	}
}

func appendInt32Data(b *array.Int32Builder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case int:
		b.Append(int32(dt))
	case int32:
		b.Append(dt)
	case json.Number:
		i, _ := dt.Int64()
		b.Append(int32(i))
	case string:
		i, _ := strconv.ParseInt(dt, 10, 32)
		b.Append(int32(i))
	case map[string]any:

	}
}

func appendInt64Data(b *array.Int64Builder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case int:
		b.Append(int64(dt))
	case int64:
		b.Append(dt)
	case string:
		i, _ := strconv.ParseInt(dt, 10, 64)
		b.Append(i)
	case json.Number:
		i, _ := dt.Int64()
		b.Append(i)
	case map[string]any:
		if source == DataSourceAvro {
			switch v := dt["long"].(type) {
			case nil:
				b.AppendNull()
			case int:
				b.Append(int64(v))
			case int64:
				b.Append(v)
			}
		}
	}
}

func appendStringData(b *array.StringBuilder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case string:
		b.Append(dt)
	case map[string]any:
		if source == DataSourceAvro {
			switch v := dt["string"].(type) {
			case nil:
				b.AppendNull()
			case string:
				b.Append(v)
			}
		}
	default:
		b.Append(fmt.Sprint(data))
	}
}

func appendTime32Data(b *array.Time32Builder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case string:
		t, _ := arrow.Time32FromString(dt, arrow.Microsecond)
		b.Append(t)
	case int32:
		b.Append(arrow.Time32(dt))
	case map[string]any:
		if source == DataSourceAvro {
			switch v := dt["int"].(type) {
			case nil:
				b.AppendNull()
			case int32:
				b.Append(arrow.Time32(v))
			}
		}
	}
}

func appendTime64Data(b *array.Time64Builder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case string:
		t, _ := arrow.Time64FromString(dt, arrow.Microsecond)
		b.Append(t)
	case int64:
		b.Append(arrow.Time64(dt))
	case map[string]any:
		if source == DataSourceAvro {
			switch v := dt["long"].(type) {
			case nil:
				b.AppendNull()
			case int64:
				b.Append(arrow.Time64(v))
			}
		}
	}
}

func appendTimestampData(b *array.TimestampBuilder, data any, source DataSource) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case json.Number:
		epochSeconds, _ := dt.Int64()
		t, _ := arrow.TimestampFromTime(time.Unix(epochSeconds, 0), arrow.Microsecond)
		b.Append(t)
	case string:
		t, _ := arrow.TimestampFromString(dt, arrow.Microsecond)
		b.Append(t)
	case time.Time:
		t, _ := arrow.TimestampFromTime(dt, arrow.Microsecond)
		b.Append(t)
	case int64:
		b.Append(arrow.Timestamp(dt))
	case map[string]any:
		switch v := dt["long"].(type) {
		case nil:
			b.AppendNull()
		case int64:
			b.Append(arrow.Timestamp(v))
		}
	}
}
