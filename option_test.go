package bodkin

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithInferTimeUnits(t *testing.T) {
	b := NewBodkin(WithInferTimeUnits())
	assert.True(t, b.inferTimeUnits, "WithInferTimeUnits should enable inferTimeUnits")
}

func TestWithTypeConversion(t *testing.T) {
	b := NewBodkin(WithTypeConversion())
	assert.True(t, b.typeConversion, "WithTypeConversion should enable typeConversion")
}

func TestWithQuotedValuesAreStrings(t *testing.T) {
	b := NewBodkin(WithQuotedValuesAreStrings())
	assert.True(t, b.quotedValuesAreStrings, "WithQuotedValuesAreStrings should enable quotedValuesAreStrings")
}

func TestWithMaxCount(t *testing.T) {
	maxCount := 100
	b := NewBodkin(WithMaxCount(maxCount))
	assert.Equal(t, maxCount, b.maxCount, "WithMaxCount should set maxCount to the provided value")
}

func TestWithCheckForUnion(t *testing.T) {
	b := NewBodkin(WithCheckForUnion())
	assert.True(t, b.checkForUnion, "WithCheckForUnion should enable checkForUnion")
}

func TestWithUseVariantForUnions(t *testing.T) {
	b := NewBodkin(WithUseVariantForUnions())
	assert.True(t, b.useVariantForUnions, "WithUseVariantForUnions should enable useVariantForUnions")
}

func TestWithIOReader(t *testing.T) {
	data := "record1\nrecord2\nrecord3"
	reader := bytes.NewReader([]byte(data))
	b := NewBodkin(WithIOReader(reader, '\n'))

	assert.NotNil(t, b.rr, "WithIOReader should set the io.Reader")
	assert.NotNil(t, b.br, "WithIOReader should set the bufio.Reader")
	assert.Equal(t, byte('\n'), b.delim, "WithIOReader should set the correct delimiter")
}
