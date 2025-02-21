package reader

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/go-viper/mapstructure/v2"
	json "github.com/goccy/go-json"
)

var (
	ErrUndefinedInput = errors.New("nil input")
	ErrInvalidInput   = errors.New("invalid input")
)

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
		ms := New(&EncoderConfig{EncodeHook: mapstructure.RecursiveStructToMapHookFunc()})
		enc, err := ms.Encode(a)
		if err != nil {
			return nil, fmt.Errorf("Error decoding to map[string]interface{}: %v", err)
		}
		return enc.(map[string]any), nil
	}
	return m, nil
}
