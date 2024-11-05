// Package bodkin is a Go library for generating schemas and decoding generic map values and native Go structures to Apache Arrow.
// The goal is to provide a useful toolkit to make it easier to use Arrow, and by extension Parquet with data whose shape
// is evolving  or not strictly defined.
package bodkin

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/go-viper/mapstructure/v2"
	json "github.com/goccy/go-json"
	omap "github.com/wk8/go-ordered-map/v2"
)

// Option configures a Bodkin
type (
	Option func(config)
	config *Bodkin
)

// Field represents an element in the input data.
type Field struct {
	Dotpath string     `json:"dotpath"`
	Type    arrow.Type `json:"arrow_type"`
	// Number of child fields if a nested type
	Childen int `json:"children,omitempty"`
	// Evaluation failure reason
	Issue error `json:"issue,omitempty"`
}

const (
	unknown int = 0
	known   int = 1
)

// Bodkin is a collection of field paths, describing the columns of a structured input(s).
type Bodkin struct {
	original               *fieldPos
	old                    *fieldPos
	new                    *fieldPos
	knownFields            *omap.OrderedMap[string, *fieldPos]
	untypedFields          *omap.OrderedMap[string, *fieldPos]
	unificationCount       int
	inferTimeUnits         bool
	quotedValuesAreStrings bool
	typeConversion         bool
	err                    error
	changes                error
}

// NewBodkin returns a new Bodkin value from a structured input.
// Input must be a json byte slice or string, a Go struct with exported fields or map[string]any.
// Any uppopulated fields, empty objects or empty slices in JSON or map[string]any inputs are skipped as their
// types cannot be evaluated and converted.
func NewBodkin(a any, opts ...Option) (*Bodkin, error) {
	m, err := InputMap(a)
	if err != nil {
		return nil, err
	}
	return newBodkin(m, opts...)
}

func newBodkin(m map[string]any, opts ...Option) (*Bodkin, error) {
	b := &Bodkin{}
	for _, opt := range opts {
		opt(b)
	}

	// Ordered map of known fields, keys are field dotpaths.
	b.knownFields = omap.New[string, *fieldPos]()
	b.untypedFields = omap.New[string, *fieldPos]()
	// Keep an immutable copy of the initial evaluation.
	g := newFieldPos(b)
	mapToArrow(g, m)
	b.original = g
	_, err := b.OriginSchema()
	// Identical to above except this one can be mutated with Unify.
	f := newFieldPos(b)
	mapToArrow(f, m)
	b.old = f

	return b, err
}

// InputMap takes structured input data and attempts to decode it to
// map[string]any. Input data can be json in string or []byte, or any other
// Go data type which can be decoded by [MapStructure/v2].
// [MapStructure/v2]: github.com/go-viper/mapstructure/v2
func InputMap(a any) (map[string]any, error) {
	m := map[string]any{}
	switch input := a.(type) {
	case nil:
		return nil, ErrUndefinedInput
	case map[string]any:
		return input, nil
	case []byte:
		r := bytes.NewReader(input)
		d := json.NewDecoder(r)
		d.UseNumber()
		err := d.Decode(&m)
		if err != nil {
			return nil, fmt.Errorf("%v : %v", ErrInvalidInput, err)
		}
	case string:
		r := bytes.NewReader([]byte(input))
		d := json.NewDecoder(r)
		d.UseNumber()
		err := d.Decode(&m)
		if err != nil {
			return nil, fmt.Errorf("%v : %v", ErrInvalidInput, err)
		}
	default:
		err := mapstructure.Decode(a, &m)
		if err != nil {
			return nil, fmt.Errorf("%v : %v", ErrInvalidInput, err)
		}
	}
	return m, nil
}

// Returns count of evaluated field paths.
func (u *Bodkin) CountPaths() int {
	return u.knownFields.Len()
}

// Returns count of unevaluated field paths.
func (u *Bodkin) CountPending() int {
	return u.untypedFields.Len()
}

// Err returns a []Field that could not be evaluated to date.
func (u *Bodkin) Err() []Field {
	fp := u.sortMapKeysDesc(unknown)
	var paths []Field = make([]Field, len(fp))
	for i, p := range fp {
		f, _ := u.untypedFields.Get(p)
		d := Field{Dotpath: f.dotPath(), Type: f.arrowType}
		switch f.arrowType {
		case arrow.STRUCT:
			d.Issue = fmt.Errorf("struct : %vs", ErrUndefinedFieldType)
		case arrow.LIST:
			d.Issue = fmt.Errorf("list : %v", ErrUndefinedArrayElementType)
		default:
			d.Issue = fmt.Errorf("%w", ErrUndefinedFieldType)
		}
		paths[i] = d
	}
	return paths
}

// Changes returns a list of field additions and field type conversions done
// in the lifetime of the Bodkin object.
func (u *Bodkin) Changes() error { return u.changes }

// Count returns the number of datum evaluated for schema to date.
func (u *Bodkin) Count() int { return u.unificationCount }

// Paths returns a slice of dotpaths of fields successfully evaluated to date.
func (u *Bodkin) Paths() []Field {
	fp := u.sortMapKeysDesc(known)
	var paths []Field = make([]Field, len(fp))
	for i, p := range fp {
		f, ok := u.knownFields.Get(p)
		if !ok {
			continue
		}
		d := Field{Dotpath: f.dotPath(), Type: f.arrowType}
		switch f.arrowType {
		case arrow.STRUCT:
			d.Childen = len(f.children)
		}
		paths[i] = d
	}
	return paths
}

// Unify merges structured input's column definition with the previously input's schema.
// Any uppopulated fields, empty objects or empty slices in JSON input are skipped.
func (u *Bodkin) Unify(a any) {
	m, err := InputMap(a)
	if err != nil {
		u.err = fmt.Errorf("%v : %v", ErrInvalidInput, err)
		return
	}

	f := newFieldPos(u)
	mapToArrow(f, m)
	u.new = f
	for _, field := range u.new.children {
		u.merge(field, nil)
	}
	u.unificationCount++
}

// Unify merges structured input's column definition with the previously input's schema,
// using a specified valid path as the root. An error is returned if the mergeAt path is
// not found.
// Any uppopulated fields, empty objects or empty slices in JSON input are skipped.
func (u *Bodkin) UnifyAtPath(a any, mergeAt string) error {
	mergePath := make([]string, 0)
	if !(len(mergeAt) == 0 || mergeAt == "$") {
		mergePath = strings.Split(strings.TrimPrefix(mergeAt, "$"), ".")
	}
	if _, ok := u.knownFields.Get(mergeAt); !ok {
		return fmt.Errorf("unitfyatpath %s : %v", mergeAt, ErrPathNotFound)
	}

	m, err := InputMap(a)
	if err != nil {
		u.err = fmt.Errorf("%v : %v", ErrInvalidInput, err)
		return fmt.Errorf("%v : %v", ErrInvalidInput, err)
	}

	f := newFieldPos(u)
	mapToArrow(f, m)
	u.new = f
	for _, field := range u.new.children {
		u.merge(field, mergePath)
	}
	u.unificationCount++
	return nil
}

// Schema returns the original Arrow schema generated from the structure/types of
// the initial input, and a panic recovery error if the schema could not be created.
func (u *Bodkin) OriginSchema() (*arrow.Schema, error) {
	var s *arrow.Schema
	defer func(s *arrow.Schema) (*arrow.Schema, error) {
		if pErr := recover(); pErr != nil {
			return nil, fmt.Errorf("schema problem: %v", pErr)
		}
		return s, nil
	}(s)
	var fields []arrow.Field
	for _, c := range u.original.children {
		fields = append(fields, c.field)
	}
	s = arrow.NewSchema(fields, nil)
	return s, nil
}

// Schema returns the current merged Arrow schema generated from the structure/types of
// the input(s), and a panic recovery error if the schema could not be created.
func (u *Bodkin) Schema() (*arrow.Schema, error) {
	var s *arrow.Schema
	defer func(s *arrow.Schema) (*arrow.Schema, error) {
		if pErr := recover(); pErr != nil {
			return nil, fmt.Errorf("schema problem: %v", pErr)
		}
		return s, nil
	}(s)
	var fields []arrow.Field
	for _, c := range u.old.children {
		fields = append(fields, c.field)
	}
	s = arrow.NewSchema(fields, nil)
	return s, nil
}

// LastSchema returns the Arrow schema generated from the structure/types of
// the most recent input. Any uppopulated fields, empty objects or empty slices are skipped.
// ErrNoLatestSchema if Unify() has never been called. A panic recovery error is returned
// if the schema could not be created.
func (u *Bodkin) LastSchema() (*arrow.Schema, error) {
	var s *arrow.Schema
	defer func(s *arrow.Schema) (*arrow.Schema, error) {
		if pErr := recover(); pErr != nil {
			return nil, fmt.Errorf("schema problem: %v", pErr)
		}
		return s, nil
	}(s)
	if u.new == nil {
		return nil, ErrNoLatestSchema
	}
	var fields []arrow.Field
	for _, c := range u.new.children {
		fields = append(fields, c.field)
	}
	s = arrow.NewSchema(fields, nil)
	return s, nil
}

// merge merges a new or changed field into the unified schema.
// Conflicting TIME, DATE, TIMESTAMP types are upgraded to STRING.
// DATE can upgrade to TIMESTAMP.
// INTEGER can upgrade to FLOAT.
func (u *Bodkin) merge(n *fieldPos, mergeAt []string) {
	var nPath, nParentPath []string
	if len(mergeAt) > 0 {
		nPath = slices.Concat(mergeAt, n.path)
		nParentPath = slices.Concat(mergeAt, n.parent.path)
	} else {
		nPath = n.path
		nParentPath = n.parent.path
	}
	if kin, err := u.old.getPath(nPath); err == ErrPathNotFound {
		// root graft
		if n.root == n.parent {
			u.old.root.graft(n)
		} else {
			// branch graft
			b, _ := u.old.getPath(nParentPath)
			b.graft(n)
		}
	} else {
		if u.typeConversion && (!kin.field.Equal(n.field) && kin.field.Type.ID() != n.field.Type.ID()) {
			switch kin.field.Type.ID() {
			case arrow.NULL:
				break
			case arrow.STRING:
				break
			case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
				switch n.field.Type.ID() {
				case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
					err := kin.upgradeType(n, arrow.FLOAT64)
					if err != nil {
						kin.err = errors.Join(kin.err, err)
					}
				default:
					err := kin.upgradeType(n, arrow.STRING)
					if err != nil {
						kin.err = errors.Join(kin.err, err)
					}
				}
			case arrow.FLOAT16:
				switch n.field.Type.ID() {
				case arrow.FLOAT32:
					err := kin.upgradeType(n, arrow.FLOAT32)
					if err != nil {
						kin.err = errors.Join(kin.err, err)
					}
				case arrow.FLOAT64:
					err := kin.upgradeType(n, arrow.FLOAT64)
					if err != nil {
						kin.err = errors.Join(kin.err, err)
					}
				default:
					err := kin.upgradeType(n, arrow.STRING)
					if err != nil {
						kin.err = errors.Join(kin.err, err)
					}
				}
			case arrow.FLOAT32:
				switch n.field.Type.ID() {
				case arrow.FLOAT64:
					err := kin.upgradeType(n, arrow.FLOAT64)
					if err != nil {
						kin.err = errors.Join(kin.err, err)
					}
				default:
					err := kin.upgradeType(n, arrow.STRING)
					if err != nil {
						kin.err = errors.Join(kin.err, err)
					}
				}
			case arrow.FLOAT64:
				switch n.field.Type.ID() {
				case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64, arrow.FLOAT16, arrow.FLOAT32:
					break
				default:
					err := kin.upgradeType(n, arrow.STRING)
					if err != nil {
						kin.err = errors.Join(kin.err, err)
					}
				}
			case arrow.TIMESTAMP:
				switch n.field.Type.ID() {
				case arrow.TIME64:
					err := kin.upgradeType(n, arrow.STRING)
					if err != nil {
						kin.err = errors.Join(kin.err, err)
					}
				}
			case arrow.DATE32:
				switch n.field.Type.ID() {
				case arrow.TIMESTAMP:
					err := kin.upgradeType(n, arrow.TIMESTAMP)
					if err != nil {
						kin.err = errors.Join(kin.err, err)
					}
				// case arrow.TIME64:
				default:
					err := kin.upgradeType(n, arrow.STRING)
					if err != nil {
						kin.err = errors.Join(kin.err, err)
					}
				}
			case arrow.TIME64:
				switch n.field.Type.ID() {
				case arrow.DATE32, arrow.TIMESTAMP:
					err := kin.upgradeType(n, arrow.STRING)
					if err != nil {
						kin.err = errors.Join(kin.err, err)
					}
				}
			}
		}
		for _, v := range n.childmap {
			u.merge(v, mergeAt)
		}
	}
}

func (u *Bodkin) sortMapKeysDesc(k int) []string {
	var m *omap.OrderedMap[string, *fieldPos]
	var sortedPaths, paths []string
	switch k {
	case known:
		sortedPaths = make([]string, u.knownFields.Len())
		paths = make([]string, u.knownFields.Len())
		m = u.knownFields
	case unknown:
		sortedPaths = make([]string, u.untypedFields.Len())
		paths = make([]string, u.untypedFields.Len())
		m = u.untypedFields
	default:
		return sortedPaths
	}
	if m.Len() == 0 {
		return sortedPaths
	}
	i := 0
	for pair := m.Newest(); pair != nil; pair = pair.Prev() {
		paths[i] = pair.Key
		i++
	}
	maxDepth := 0
	for _, p := range paths {
		pathDepth := strings.Count(p, ".")
		if pathDepth > maxDepth {
			maxDepth = pathDepth
		}
	}
	sortIndex := 0
	for maxDepth >= 0 {
		for _, p := range paths {
			pathDepth := strings.Count(p, ".")
			if pathDepth == maxDepth {
				sortedPaths[sortIndex] = p
				sortIndex++
			}
		}
		maxDepth--
	}
	return sortedPaths
}
