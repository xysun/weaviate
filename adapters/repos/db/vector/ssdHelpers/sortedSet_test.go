package ssdhelpers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortedSetAddKeepsSorted(t *testing.T) {
	vectors := [][]float32{
		{1, 2},
		{10, 20},
		{100, 200},
		{150, 300},
	}
	ss := NewSortedSet(3, func(ctx context.Context, id uint64) ([]float32, error) { return vectors[id], nil }, L2, []float32{0, 0}, len(vectors))
	ss.Add(3)
	ss.Add(2)
	ss.Add(1)
	elements, _ := ss.Elements(3)
	assert.EqualValues(t, elements, []uint64{1, 2, 3})
}

func TestSortedSetAddRangeKeepsSorted(t *testing.T) {
	vectors := [][]float32{
		{1, 2},
		{10, 20},
		{100, 200},
		{150, 300},
	}
	ss := NewSortedSet(3, func(ctx context.Context, id uint64) ([]float32, error) { return vectors[id], nil }, L2, []float32{0, 0}, len(vectors))
	ss.AddRange([]uint64{3, 2, 1})
	elements, _ := ss.Elements(3)
	assert.EqualValues(t, elements, []uint64{1, 2, 3})
}

func TestSortedSetNotVisitedReturnsCorrect(t *testing.T) {
	vectors := [][]float32{
		{1, 2},
		{10, 20},
		{100, 200},
		{150, 300},
	}
	ss := NewSortedSet(3, func(ctx context.Context, id uint64) ([]float32, error) { return vectors[id], nil }, L2, []float32{0, 0}, len(vectors))
	ss.AddRange([]uint64{3, 2, 1})
	ss.Top()
	assert.True(t, ss.NotVisited())
	ss.Top()
	assert.True(t, ss.NotVisited())
	ss.Top()
	assert.False(t, ss.NotVisited())
}

func TestSortedSetTopReturnsCorrect(t *testing.T) {
	vectors := [][]float32{
		{1, 2},
		{10, 20},
		{100, 200},
		{150, 300},
	}
	ss := NewSortedSet(3, func(ctx context.Context, id uint64) ([]float32, error) { return vectors[id], nil }, L2, []float32{0, 0}, len(vectors))
	ss.AddRange([]uint64{3, 2, 1})
	f, _ := ss.Top()
	assert.Equal(t, f, uint64(1))
	f, _ = ss.Top()
	assert.Equal(t, f, uint64(2))
	f, _ = ss.Top()
	assert.Equal(t, f, uint64(3))
}

func TestSortedSetResortIsCorrectWhenLower(t *testing.T) {
	vectors := [][]float32{
		{1, 2},
		{10, 20},
		{100, 200},
		{150, 300},
	}
	ss := NewSortedSet(3, func(ctx context.Context, id uint64) ([]float32, error) { return vectors[id], nil }, L2, []float32{0, 0}, len(vectors))
	ss.AddRange([]uint64{3, 2, 1})
	ss.ReSort(1, []float32{5, 10})
	f, _ := ss.Top()
	assert.Equal(t, f, uint64(2))
	f, _ = ss.Top()
	assert.Equal(t, f, uint64(1))
	f, _ = ss.Top()
	assert.Equal(t, f, uint64(3))
}

func TestSortedSetResortIsCorrectWhenHigher(t *testing.T) {
	vectors := [][]float32{
		{1, 2},
		{10, 20},
		{100, 200},
		{150, 300},
	}
	ss := NewSortedSet(3, func(ctx context.Context, id uint64) ([]float32, error) { return vectors[id], nil }, L2, []float32{0, 0}, len(vectors))
	ss.AddRange([]uint64{3, 2, 1})
	ss.ReSort(1, []float32{200, 400})
	f, _ := ss.Top()
	assert.Equal(t, f, uint64(1))
	f, _ = ss.Top()
	assert.Equal(t, f, uint64(3))
	f, _ = ss.Top()
	assert.Equal(t, f, uint64(2))
}

func TestSortedSetKeepsBounded(t *testing.T) {
	vectors := [][]float32{
		{1, 2},
		{10, 20},
		{100, 200},
		{150, 300},
	}
	ss := NewSortedSet(3, func(ctx context.Context, id uint64) ([]float32, error) { return vectors[id], nil }, L2, []float32{0, 0}, len(vectors))
	ss.AddRange([]uint64{3, 0, 2, 1})
	f, _ := ss.Top()
	assert.Equal(t, f, uint64(0))
	f, _ = ss.Top()
	assert.Equal(t, f, uint64(1))
	f, _ = ss.Top()
	assert.Equal(t, f, uint64(2))
	assert.False(t, ss.NotVisited())
}
