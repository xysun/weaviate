package ssdhelpers_test

import (
	"context"
	"fmt"
	"testing"

	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	"github.com/stretchr/testify/assert"
)

func TestKMeans(t *testing.T) {
	vectors := [4][]float32{
		{0, 5},
		{0.1, 4.9},
		{0.01, 5.1},
		{10.1, 7},
	}
	kmeans := ssdhelpers.New(
		2,
		ssdhelpers.L2,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		4,
	)
	kmeans.Partition()
	assert.True(t, kmeans.Nearest(vectors[0]) == kmeans.Nearest(vectors[1]))
	assert.True(t, kmeans.Nearest(vectors[0]) == kmeans.Nearest(vectors[2]))
	assert.True(t, kmeans.Nearest(vectors[0]) != kmeans.Nearest(vectors[3]))
	fmt.Println(kmeans.Nearest(vectors[0]))
	fmt.Println(kmeans.Nearest(vectors[1]))
	fmt.Println(kmeans.Nearest(vectors[2]))
	fmt.Println(kmeans.Nearest(vectors[3]))
}

func TestKMeansNNearest(t *testing.T) {
	vectors := [6][]float32{
		{0, 5},
		{0.1, 4.9},
		{0.01, 5.1},
		{10.1, 7},
		{5.1, 2},
		{5.0, 2.1},
	}
	kmeans := ssdhelpers.New(
		3,
		ssdhelpers.L2,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		6,
	)
	kmeans.Partition()
	centers := make([][]uint64, 6)
	for i := range centers {
		centers[i] = kmeans.NNearest(vectors[i], 2)
	}
	assert.True(t, centers[0][0] == centers[1][0])
	assert.True(t, centers[0][0] == centers[2][0])
	assert.True(t, centers[4][0] == centers[5][0])

	assert.True(t, centers[0][1] == centers[1][1])
	assert.True(t, centers[0][1] == centers[2][1])
	assert.True(t, centers[4][1] == centers[5][1])

	assert.True(t, centers[0][1] == centers[4][0])
	assert.True(t, centers[0][1] == centers[5][0])
	assert.True(t, centers[4][1] == centers[1][0])
	assert.True(t, centers[5][1] == centers[1][0])

	fmt.Println(centers[0])
	fmt.Println(centers[1])
	fmt.Println(centers[2])
	fmt.Println(centers[3])
	fmt.Println(centers[4])
	fmt.Println(centers[5])
}
