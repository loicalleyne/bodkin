package bodkin

import (
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/go-viper/mapstructure/v2"
	"github.com/goccy/go-json"
)

type (
	Option func(config)
	config *Bodkin
)

// Bodkin is a collection of field paths, describing the columns of a structured input(s).
type Bodkin struct {
	original       *fieldPos
	old            *fieldPos
	new            *fieldPos
	inferTimeUnits bool
	typeConversion bool
	err            error
	changes        error
}

// NewBodkin returns a new Bodkin value from a structured input.
// Input must be a json byte slice or string, a struct with exported fields or map[string]any.
// Any uppopulated fields, empty objects or empty slices in the input are skipped as their
// types cannot be evaluated and converted.
func NewBodkin(a any, opts ...Option) (*Bodkin, error) {
	m := map[string]interface{}{}
	switch input := a.(type) {
	case nil:
		return nil, ErrUndefinedInput
	case map[string]any:
		return newBodkin(input, opts...)
	case []byte:
		err := json.Unmarshal(input, &m)
		if err != nil {
			return nil, fmt.Errorf("%v : %v", ErrInvalidInput, err)
		}
	case string:
		err := json.Unmarshal([]byte(input), &m)
		if err != nil {
			return nil, fmt.Errorf("%v : %v", ErrInvalidInput, err)
		}
	default:
		err := mapstructure.Decode(a, &m)
		if err != nil {
			return nil, fmt.Errorf("%v : %v", ErrInvalidInput, err)
		}
	}
	return newBodkin(m, opts...)
}

func newBodkin(m map[string]any, opts ...Option) (*Bodkin, error) {
	b := &Bodkin{}
	f := newFieldPos(b)
	for _, opt := range opts {
		opt(b)
	}
	mapToArrow(f, m)
	b.old = f

	g := newFieldPos(b)
	mapToArrow(g, m)
	b.original = g
	return b, errWrap(f)
}

// Err returns the last error encountered during the unification of input schemas.
func (u *Bodkin) Err() error { return u.err }

// Changes returns a list of field additions and field type conversions done
// in the lifetime of the Bodkin object.
func (u *Bodkin) Changes() error { return u.changes }

// WithInferTimeUnits() enables scanning input string values for time, date and timestamp types.
//
// Times use a format of HH:MM or HH:MM:SS[.zzz] where the fractions of a second cannot
// exceed the precision allowed by the time unit, otherwise unmarshalling will error.
//
// # Dates use YYYY-MM-DD format
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

// Unify merges structured input's column definition with the previously input's schema.
// Any uppopulated fields, empty objects or empty slices in the input are skipped.
func (u *Bodkin) Unify(a any) {
	m := map[string]interface{}{}
	switch input := a.(type) {
	case nil:
		u.err = ErrUndefinedInput
	case []byte:
		err := json.Unmarshal(input, &m)
		if err != nil {
			u.err = fmt.Errorf("%v : %v", ErrInvalidInput, err)
			return
		}
	case string:
		err := json.Unmarshal([]byte(input), &m)
		if err != nil {
			u.err = fmt.Errorf("%v : %v", ErrInvalidInput, err)
			return
		}
	case map[string]any:
		f := newFieldPos(u)
		mapToArrow(f, m)
		u.new = f
		for _, field := range u.new.children {
			u.merge(field)
		}
	default:
		err := mapstructure.Decode(a, &m)
		if err != nil {
			u.err = fmt.Errorf("%v : %v", ErrInvalidInput, err)
			return
		}
	}
	f := newFieldPos(u)
	mapToArrow(f, m)
	u.new = f
	for _, field := range u.new.children {
		u.merge(field)
	}
}

// Schema returns the original Arrow schema generated from the structure/types of
// the initial input, and wrapped errors indicating which fields could not be evaluated.
func (u *Bodkin) OriginSchema() (*arrow.Schema, error) {
	var fields []arrow.Field
	for _, c := range u.original.children {
		fields = append(fields, c.field)
	}
	err := errWrap(u.original)
	return arrow.NewSchema(fields, nil), err
}

// Schema returns the current merged Arrow schema generated from the structure/types of
// the input(s), and wrapped errors indicating which fields could not be evaluated.
func (u *Bodkin) Schema() (*arrow.Schema, error) {
	var fields []arrow.Field
	for _, c := range u.old.children {
		fields = append(fields, c.field)
	}
	err := errWrap(u.old)
	return arrow.NewSchema(fields, nil), err
}

// LastSchema returns the Arrow schema generated from the structure/types of
// the most recent input. Any uppopulated fields, empty objects or empty slices are skipped.
// ErrNoLatestSchema if Unify() has never been called.
func (u *Bodkin) LastSchema() (*arrow.Schema, error) {
	if u.new == nil {
		return nil, ErrNoLatestSchema
	}
	var fields []arrow.Field
	for _, c := range u.new.children {
		fields = append(fields, c.field)
	}
	err := errWrap(u.new)
	return arrow.NewSchema(fields, nil), err
}

// merge merges a new or changed field into the unified schema.
// Conflicting TIME, DATE, TIMESTAMP types are upgraded to STRING.
// DATE can upgrade to TIMESTAMP.
// INTEGER can upgrade to FLOAT.
func (u *Bodkin) merge(n *fieldPos) {
	if kin, err := u.old.getPath(n.path); err == ErrPathNotFound {
		// root graft
		if n.root == n.parent {
			u.old.root.graft(n)
		} else {
			// branch graft
			b, _ := u.old.getPath(n.parent.path)
			b.graft(n)
		}
	} else {
		if u.typeConversion && (!kin.field.Equal(n.field) && kin.field.Type.ID() != n.field.Type.ID()) {
			switch kin.field.Type.ID() {
			case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
				switch n.field.Type.ID() {
				case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
					err := kin.upgradeType(n, arrow.FLOAT64)
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
				case arrow.TIME64:
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
			u.merge(v)
		}
	}
}
