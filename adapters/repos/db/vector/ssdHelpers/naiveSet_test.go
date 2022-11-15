package ssdhelpers_test

import (
	"context"
	"testing"

	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	"github.com/stretchr/testify/assert"
)

func TestNaiveSetDoesNotAddCenter(t *testing.T) {
	p := uint64(0)
	vectors := [][]float32{
		{0, 10, 0, 12, 34},
		{0, 12, 0, 12, 34},
		{0, 10, 4, 12, 34},
		{0, 10, 0, 15, 34},
		{0, 10, 0, 12, 31},
	}
	visitedSet := ssdhelpers.NewNaiveSet(
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[id], nil
		},
		ssdhelpers.L2,
		5,
	)
	visitedSet.ReCenter(p)

	visitedSet.Add(0)
	visitedSet.Add(1)
	visitedSet.AddRange([]uint64{0, 1, 3, 2})
	assert.True(t, visitedSet.Size() == 3)
	for _, x := range visitedSet.GetItems() {
		assert.True(t, x.GetIndex() != p)
	}
}

func TestNaiveSetDoesSortTheData(t *testing.T) {
	p := uint64(0)
	vectors := [][]float32{
		{0, 10, 0, 12, 34},
		{0, 12, 0, 12, 34}, // 0
		{0, 10, 4, 12, 34}, // 2
		{0, 10, 0, 15, 34}, // 1
		{0, 10, 0, 12, 39}, // 3
	}
	sorted := []uint64{1, 3, 2, 4}
	visitedSet := ssdhelpers.NewNaiveSet(
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[id], nil
		},
		ssdhelpers.L2,
		5,
	)
	visitedSet.ReCenter(p)

	visitedSet.AddRange([]uint64{0, 1, 2, 3, 4})
	assert.True(t, visitedSet.Size() == 4)
	for i := 0; i < visitedSet.Size(); i++ {
		x := visitedSet.Pop()
		assert.True(t, x.GetIndex() == sorted[i])
	}
}

func TestNaiveSetPopJumpsOverRemoved(t *testing.T) {
	p := uint64(0)
	vectors := [][]float32{
		{0, 10, 0, 12, 34},
		{0, 12, 0, 12, 34}, // 0
		{0, 10, 4, 12, 34}, // 2
		{0, 10, 0, 15, 34}, // 1
		{0, 10, 0, 12, 39}, // 3
	}
	sorted := []uint64{1, 3, 4}
	visitedSet := ssdhelpers.NewNaiveSet(
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[id], nil
		},
		ssdhelpers.L2,
		5,
	)
	visitedSet.ReCenter(p)

	visitedSet.AddRange([]uint64{0, 1, 2, 3, 4})
	visitedSet.Remove(visitedSet.GetItems()[2])
	assert.True(t, visitedSet.Size() == 3)
	for i := 0; i < visitedSet.Size(); i++ {
		x := visitedSet.Pop()
		assert.True(t, x.GetIndex() == sorted[i])
	}
}

func TestNaiveSetIteratorJumpsOverRemoved(t *testing.T) {
	p := uint64(0)
	vectors := [][]float32{
		{0, 10, 0, 12, 34},
		{0, 12, 0, 12, 34}, // 0
		{0, 10, 4, 12, 34}, // 2
		{0, 10, 0, 15, 34}, // 1
		{0, 10, 0, 12, 39}, // 3
	}
	sorted := []uint64{1, 3, 4}
	visitedSet := ssdhelpers.NewNaiveSet(
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[id], nil
		},
		ssdhelpers.L2,
		5,
	)
	visitedSet.ReCenter(p)

	visitedSet.AddRange([]uint64{0, 1, 2, 3, 4})
	visitedSet.Remove(visitedSet.GetItems()[2])
	assert.True(t, visitedSet.Size() == 3)

	k := 0
	for _, x := range visitedSet.GetItems() {
		if visitedSet.SkipOn(x) {
			continue
		}
		assert.True(t, x.GetIndex() == sorted[k])
		k++
	}
}

func TestNaiveSetIteratorJumpsOverRemovedOnLoop(t *testing.T) {
	p := uint64(0)
	vectors := [][]float32{
		{0, 10, 0, 12, 34},
		{0, 12, 0, 12, 34}, // 0
		{0, 10, 4, 12, 34}, // 2
		{0, 10, 0, 15, 34}, // 1
		{0, 10, 0, 12, 39}, // 3
	}
	sorted := []uint64{1, 3, 4}
	visitedSet := ssdhelpers.NewNaiveSet(
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[id], nil
		},
		ssdhelpers.L2,
		5,
	)
	visitedSet.ReCenter(p)

	visitedSet.AddRange([]uint64{0, 1, 2, 3, 4})

	assert.True(t, visitedSet.Size() == 4)

	k := 0
	for _, x := range visitedSet.GetItems() {
		if visitedSet.SkipOn(x) {
			continue
		}
		visitedSet.Remove(visitedSet.GetItems()[2])
		assert.True(t, x.GetIndex() == sorted[k])
		k++
	}
}

func TestNaiveSetIgnoresWhenRemovingAlreadyPoped(t *testing.T) {
	p := uint64(0)
	vectors := [][]float32{
		{0, 10, 0, 12, 34},
		{0, 12, 0, 12, 34}, // 0
		{0, 10, 4, 12, 34}, // 2
		{0, 10, 0, 15, 34}, // 1
		{0, 10, 0, 12, 39}, // 3
	}
	visitedSet := ssdhelpers.NewNaiveSet(
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[id], nil
		},
		ssdhelpers.L2,
		5,
	)
	visitedSet.ReCenter(p)

	visitedSet.AddRange([]uint64{0, 1, 2, 3, 4})
	assert.True(t, visitedSet.Size() == 4)
	x := visitedSet.Pop()
	visitedSet.Remove(x)
	assert.True(t, visitedSet.Size() == 3)
}
