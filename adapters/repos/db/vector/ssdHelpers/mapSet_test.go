package ssdhelpers_test

import (
	"testing"

	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	"github.com/stretchr/testify/assert"
)

func TestMapSetIntersect(t *testing.T) {
	ms := ssdhelpers.NewMapSet()
	ms.Add(1)
	ms.Add(2)
	ms.Add(3)
	assert.EqualValues(t, ms.Intersect([]uint64{2, 4, 3}).Elements(), []uint64{2, 3})
}

func TestMapSetContains(t *testing.T) {
	ms := ssdhelpers.NewMapSet()
	ms.Add(1)
	ms.Add(2)
	ms.Add(3)
	assert.True(t, ms.Contains(1))
	assert.True(t, ms.Contains(2))
	assert.True(t, ms.Contains(3))
	assert.False(t, ms.Contains(4))
	assert.False(t, ms.Contains(5))
}

func TestMapSetDiffFro(t *testing.T) {
	ms := ssdhelpers.NewMapSet()
	ms.Add(1)
	ms.Add(2)
	ms.Add(3)
	assert.EqualValues(t, ms.DiffFrom([]uint64{2, 4, 3}).Elements(), []uint64{4})
}
