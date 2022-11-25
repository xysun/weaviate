package ssdhelpers_test

import (
	"context"
	"testing"

	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	"github.com/stretchr/testify/assert"
)

func TestKMeansNNearest(t *testing.T) {
	vectors := [6][]float32{
		{0, 5},
		{0.1, 4.9},
		{0.01, 5.1},
		{10.1, 7},
		{5.1, 2},
		{5.0, 2.1},
	}
	kmeans := ssdhelpers.NewKMeans(
		3,
		ssdhelpers.L2,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		6,
		2,
	)
	kmeans.Fit()
	centers := make([]uint64, 6)
	for i := range centers {
		centers[i] = kmeans.Nearest(vectors[i])
	}
	for v := range vectors {
		min := ssdhelpers.L2(vectors[v], kmeans.Centroid(byte(centers[v])))
		for c := range centers {
			assert.True(t, ssdhelpers.L2(vectors[v], kmeans.Centroid(byte(centers[c]))) >= min)
		}
	}
}
