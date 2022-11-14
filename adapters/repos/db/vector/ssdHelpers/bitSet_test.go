package ssdhelpers_test

import (
	"testing"

	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	"github.com/stretchr/testify/assert"
)

func TestBitSetClean(t *testing.T) {
	bs := ssdhelpers.NewBitSet(5)
	x := uint64(2)
	bs.Add(x)
	assert.True(t, bs.Contains(x))
	bs.Clean()
	assert.False(t, bs.Contains(x))
}

func TestBitSetAddAndContains(t *testing.T) {
	bs := ssdhelpers.NewBitSet(5)
	x := uint64(2)
	assert.False(t, bs.Contains(x))
	bs.Add(x)
	assert.True(t, bs.Contains(x))
}

func TestBitSetContainsAndAdd(t *testing.T) {
	bs := ssdhelpers.NewBitSet(5)
	x := uint64(2)
	assert.False(t, bs.Contains(x))
	assert.False(t, bs.ContainsAndAdd(x))
	assert.True(t, bs.ContainsAndAdd(x))
	assert.True(t, bs.Contains(x))
}

func TestBitSetContainsAndRemove(t *testing.T) {
	bs := ssdhelpers.NewBitSet(5)
	x := uint64(2)
	assert.False(t, bs.Contains(x))
	bs.Add(x)
	assert.True(t, bs.Contains(x))
	assert.True(t, bs.ContainsAndRemove(x))
	assert.False(t, bs.ContainsAndRemove(x))
	assert.False(t, bs.Contains(x))
}

func TestFullBitSetSize(t *testing.T) {
	fbs := ssdhelpers.NewFullBitSet(5)
	assert.True(t, fbs.Size() == 0)
	fbs.Add(1)
	assert.True(t, fbs.Size() == 1)
	fbs.Add(1)
	assert.True(t, fbs.Size() == 1)
	fbs.Add(3)
	assert.True(t, fbs.Size() == 2)
}

func TestFullBitSetElements(t *testing.T) {
	fbs := ssdhelpers.NewFullBitSet(5)
	assert.EqualValues(t, fbs.Elements(), []uint64{})
	fbs.Add(1)
	assert.EqualValues(t, fbs.Elements(), []uint64{1})
	fbs.Add(1)
	assert.EqualValues(t, fbs.Elements(), []uint64{1})
	fbs.Add(3)
	assert.EqualValues(t, fbs.Elements(), []uint64{1, 3})
}
