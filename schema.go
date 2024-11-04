package bodkin

import (
	"errors"
	"fmt"
	"regexp"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type fieldPos struct {
	root         *fieldPos
	parent       *fieldPos
	owner        *Bodkin
	builder      array.Builder
	name         string
	path         []string
	isList       bool
	isItem       bool
	isStruct     bool
	isMap        bool
	typeName     string
	field        arrow.Field
	children     []*fieldPos
	childmap     map[string]*fieldPos
	appendFunc   func(val interface{}) error
	metadatas    arrow.Metadata
	index, depth int32
	err          error
}

// Schema evaluation/evolution errors.
var (
	ErrUndefinedInput            = errors.New("nil input")
	ErrInvalidInput              = errors.New("invalid input")
	ErrNoLatestSchema            = errors.New("no second input has been provided")
	ErrUndefinedFieldType        = errors.New("could not determine type of unpopulated field")
	ErrUndefinedArrayElementType = errors.New("could not determine element type of empty array")
	ErrNotAnUpgradableType       = errors.New("is not an upgradable type")
	ErrPathNotFound              = errors.New("path not found")
	ErrFieldTypeChanged          = errors.New("changed")
	ErrFieldAdded                = errors.New("added")
)

// UpgradableTypes are scalar types that can be upgraded to a more flexible type.
var UpgradableTypes []arrow.Type = []arrow.Type{arrow.INT8,
	arrow.INT16,
	arrow.INT32,
	arrow.INT64,
	arrow.DATE32,
	arrow.TIME64,
	arrow.TIMESTAMP,
	arrow.STRING,
}

// Regular expressions and variables for type inference.
var (
	timestampMatchers []*regexp.Regexp
	dateMatcher       *regexp.Regexp
	timeMatcher       *regexp.Regexp
	integerMatcher    *regexp.Regexp
	floatMatcher      *regexp.Regexp
	boolMatcher       []string
)

func init() {
	registerTsMatchers()
	registerQuotedStringValueMatchers()
}

func registerTsMatchers() {
	dateMatcher = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	timeMatcher = regexp.MustCompile(`^\d{1,2}:\d{1,2}:\d{1,2}(\.\d{1,6})?$`)
	timestampMatchers = append(timestampMatchers,
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`), // ISO 8601
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`), // RFC 3339 with space instead of T
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`),                            // Datetime format with dashes
		regexp.MustCompile(`^\d{4}-\d{1,2}-\d{1,2}[T ]\d{1,2}:\d{1,2}:\d{1,2}(\.\d{1,6})? *(([+-]\d{1,2}(:\d{1,2})?)|Z|UTC)?$`))
}

func registerQuotedStringValueMatchers() {
	integerMatcher = regexp.MustCompile(`^[-+]?\d+$`)
	floatMatcher = regexp.MustCompile(`^[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?$`)
	boolMatcher = append(boolMatcher, "true", "false")
}

func newFieldPos(b *Bodkin) *fieldPos {
	f := new(fieldPos)
	f.owner = b
	f.index = -1
	f.root = f
	f.childmap = make(map[string]*fieldPos)
	f.children = make([]*fieldPos, 0)
	return f
}

func (f *fieldPos) assignChild(child *fieldPos) {
	f.children = append(f.children, child)
	f.childmap[child.name] = child
}

func (f *fieldPos) child(index int) (*fieldPos, error) {
	if index < len(f.children) {
		return f.children[index], nil
	}
	return nil, fmt.Errorf("%v child index %d not found", f.namePath(), index)
}

func (f *fieldPos) error() error             { return f.err }
func (f *fieldPos) metadata() arrow.Metadata { return f.field.Metadata }

func (f *fieldPos) newChild(childName string) *fieldPos {
	var child fieldPos = fieldPos{
		root:   f.root,
		parent: f,
		owner:  f.owner,
		name:   childName,
		index:  int32(len(f.children)),
		depth:  f.depth + 1,
	}
	child.path = child.namePath()
	child.childmap = make(map[string]*fieldPos)
	return &child
}

func (f *fieldPos) mapChildren() {
	for i, c := range f.children {
		f.childmap[c.name] = f.children[i]
	}
}

// getPath returns a field found at a defined path, otherwise returns ErrPathNotFound.
func (f *fieldPos) getPath(path []string) (*fieldPos, error) {
	if len(path) == 0 { // degenerate input
		return nil, fmt.Errorf("getPath needs at least one key")
	}
	if node, ok := f.childmap[path[0]]; !ok {
		return nil, ErrPathNotFound
	} else if len(path) == 1 { // we've reached the final key
		return node, nil
	} else { // 1+ more keys
		return node.getPath(path[1:])
	}
}

// namePath returns a slice of keys making up the path to the field
func (f *fieldPos) namePath() []string {
	if len(f.path) == 0 {
		var path []string
		cur := f
		for i := f.depth - 1; i >= 0; i-- {
			path = append([]string{cur.name}, path...)
			cur = cur.parent
		}
		return path
	}
	return f.path
}

// namePath returns the path to the field in json dot notation
func (f *fieldPos) dotPath() string {
	var path string = "$"
	for i, p := range f.path {
		path = path + p
		if i+1 != len(f.path) {
			path = path + "."
		}
	}
	return path
}

// getValue retrieves the value from the map[string]any
// by following the field's key path
func (f *fieldPos) getValue(m map[string]any) any {
	var value any = m
	for _, key := range f.namePath() {
		valueMap, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value, ok = valueMap[key]
		if !ok {
			return nil
		}
	}
	return value
}

// graft grafts a new field into the schema tree
func (f *fieldPos) graft(n *fieldPos) {
	graft := f.newChild(n.name)
	graft.field = n.field
	graft.children = append(graft.children, n.children...)
	graft.mapChildren()
	f.assignChild(graft)
	f.owner.changes = errors.Join(f.owner.changes, fmt.Errorf("%w %v : %v", ErrFieldAdded, graft.dotPath(), graft.field.Type.String()))
	if f.field.Type.ID() == arrow.STRUCT {
		gf := f.field.Type.(*arrow.StructType)
		var nf []arrow.Field
		nf = append(nf, gf.Fields()...)
		nf = append(nf, graft.field)
		f.field = arrow.Field{Name: graft.name, Type: arrow.StructOf(nf...), Nullable: true}
		if (f.parent != nil) && f.parent.field.Type.ID() == arrow.LIST {
			f.parent.field = arrow.Field{Name: f.parent.name, Type: arrow.ListOf(f.field.Type.(*arrow.StructType)), Nullable: true}
		}
	}
}

// Only scalar types in UpgradableTypes[] can be upgraded
func (o *fieldPos) upgradeType(n *fieldPos, t arrow.Type) error {
	if !slices.Contains(UpgradableTypes, n.field.Type.ID()) {
		return fmt.Errorf("%v %v", n.field.Type.Name(), ErrNotAnUpgradableType.Error())
	}
	oldType := o.field.Type.String()
	switch t {
	case arrow.FLOAT64:
		o.field = arrow.Field{Name: o.name, Type: arrow.PrimitiveTypes.Float64, Nullable: true}
	case arrow.STRING:
		o.field = arrow.Field{Name: o.name, Type: arrow.BinaryTypes.String, Nullable: true}
	case arrow.TIMESTAMP:
		o.field = arrow.Field{Name: o.name, Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: true}
	}
	switch o.parent.field.Type.ID() {
	case arrow.LIST:
		o.parent.field = arrow.Field{Name: o.parent.name, Type: arrow.ListOf(n.field.Type), Nullable: true}
	case arrow.STRUCT:
		var fields []arrow.Field
		for _, c := range o.parent.children {
			fields = append(fields, c.field)
		}
		o.parent.field = arrow.Field{Name: o.parent.name, Type: arrow.StructOf(fields...), Nullable: true}
	}
	o.owner.changes = errors.Join(o.owner.changes, fmt.Errorf("%w %v : from %v to %v", ErrFieldTypeChanged, o.dotPath(), oldType, o.field.Type.String()))
	return nil
}

func errWrap(f *fieldPos) error {
	var err error
	if f.err != nil {
		err = errors.Join(f.err)
	}
	if len(f.children) > 0 {
		for _, field := range f.children {
			err = errors.Join(err, errWrap(field))
		}
	}
	return err
}

// mapToArrow traverses a map[string]any and creates a fieldPos tree from
// which an Arrow schema can be generated.
func mapToArrow(f *fieldPos, m map[string]any) {
	for k, v := range m {
		child := f.newChild(k)
		switch t := v.(type) {
		case map[string]any:
			mapToArrow(child, t)
			var fields []arrow.Field
			for _, c := range child.children {
				fields = append(fields, c.field)
			}
			if len(child.children) != 0 {
				child.field = arrow.Field{Name: k, Type: arrow.StructOf(fields...), Nullable: true}
				f.assignChild(child)
			}

		case []any:
			if len(t) <= 0 {
				f.err = errors.Join(f.err, fmt.Errorf("%v : %v", ErrUndefinedArrayElementType, child.namePath()))
			} else {
				et := sliceElemType(child, t)
				child.field = arrow.Field{Name: k, Type: arrow.ListOf(et), Nullable: true}
				f.assignChild(child)
			}
		case nil:
			f.err = errors.Join(f.err, fmt.Errorf("%v : %v", ErrUndefinedFieldType, child.namePath()))
		default:
			child.field = arrow.Field{Name: k, Type: goType2Arrow(child, v), Nullable: true}
			f.assignChild(child)
		}
	}
	var fields []arrow.Field
	for _, c := range f.children {
		fields = append(fields, c.field)
	}
	f.field = arrow.Field{Name: f.name, Type: arrow.StructOf(fields...), Nullable: true}
}

// sliceElemType evaluates the slice type and returns an Arrow DataType
// to be used in building an Arrow Field.
func sliceElemType(f *fieldPos, v []any) arrow.DataType {
	switch ft := v[0].(type) {
	case map[string]any:
		child := f.newChild(f.name + ".elem")
		mapToArrow(child, ft)
		var fields []arrow.Field
		for _, c := range child.children {
			fields = append(fields, c.field)
		}
		f.assignChild(child)
		return arrow.StructOf(fields...)
	case []any:
		if len(ft) < 1 {
			f.err = errors.Join(f.err, fmt.Errorf("%v : %v", ErrUndefinedArrayElementType, f.namePath()))
			return arrow.GetExtensionType("skip")
		}
		child := f.newChild(f.name + ".elem")
		et := sliceElemType(child, v[0].([]any))
		f.assignChild(child)
		return arrow.ListOf(et)
	default:
		return goType2Arrow(f, v)
	}
	return nil
}

// goType2Arrow maps a Go type to an Arrow DataType.
func goType2Arrow(f *fieldPos, gt any) arrow.DataType {
	var dt arrow.DataType
	switch t := gt.(type) {
	case []any:
		return goType2Arrow(f, t[0])
	// either 32 or 64 bits
	case int:
		dt = arrow.PrimitiveTypes.Int64
	// the set of all signed  8-bit integers (-128 to 127)
	case int8:
		dt = arrow.PrimitiveTypes.Int8
	// the set of all signed 16-bit integers (-32768 to 32767)
	case int16:
		dt = arrow.PrimitiveTypes.Int16
	// the set of all signed 32-bit integers (-2147483648 to 2147483647)
	case int32:
		dt = arrow.PrimitiveTypes.Int32
	// the set of all signed 64-bit integers (-9223372036854775808 to 9223372036854775807)
	case int64:
		dt = arrow.PrimitiveTypes.Int64
	// either 32 or 64 bits
	case uint:
		dt = arrow.PrimitiveTypes.Uint64
	// the set of all unsigned  8-bit integers (0 to 255)
	case uint8:
		dt = arrow.PrimitiveTypes.Uint8
	// the set of all unsigned 16-bit integers (0 to 65535)
	case uint16:
		dt = arrow.PrimitiveTypes.Uint16
	// the set of all unsigned 32-bit integers (0 to 4294967295)
	case uint32:
		dt = arrow.PrimitiveTypes.Uint32
	// the set of all unsigned 64-bit integers (0 to 18446744073709551615)
	case uint64:
		dt = arrow.PrimitiveTypes.Uint64
	// the set of all IEEE-754 32-bit floating-point numbers
	case float32:
		dt = arrow.PrimitiveTypes.Float32
	// the set of all IEEE-754 64-bit floating-point numbers
	case float64:
		dt = arrow.PrimitiveTypes.Float64
	case bool:
		dt = arrow.FixedWidthTypes.Boolean
	case string:
		if f.owner.inferTimeUnits {
			for _, r := range timestampMatchers {
				if r.MatchString(t) {
					return arrow.FixedWidthTypes.Timestamp_us
				}
			}
			if dateMatcher.MatchString(t) {
				return arrow.FixedWidthTypes.Date32
			}
			if timeMatcher.MatchString(t) {
				return arrow.FixedWidthTypes.Time64ns
			}
		}
		if !f.owner.quotedValuesAreStrings {
			if slices.Contains(boolMatcher, t) {
				return arrow.FixedWidthTypes.Boolean
			}
			if integerMatcher.MatchString(t) {
				return arrow.PrimitiveTypes.Int64
			}
			if floatMatcher.MatchString(t) {
				return arrow.PrimitiveTypes.Float64
			}
		}
		dt = arrow.BinaryTypes.String
	case []byte:
		dt = arrow.BinaryTypes.Binary
	// the set of all complex numbers with float32 real and imaginary parts
	case complex64:
		// TO-DO
	// the set of all complex numbers with float64 real and imaginary parts
	case complex128:
		// TO-DO
	case nil:
		f.err = fmt.Errorf("%v : %v", ErrUndefinedFieldType, f.namePath())
		dt = arrow.BinaryTypes.Binary
	}
	return dt
}
