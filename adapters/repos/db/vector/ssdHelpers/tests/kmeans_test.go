//go:build kmeansTest
// +build kmeansTest

package ssdhelpers_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/fogleman/gg"

	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	testinghelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/testingHelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func drawClusters(scale int, w int, h int, vectors [][]float32, centroids [][]float32, clusters []int, name string) {
	dc := gg.NewContext(w*scale, h*scale)
	dc.SetRGB(1, 1, 1)
	dc.Clear()
	dc.SetRGBA(0.3, 0.3, 0.3, 1)
	dc.SetLineWidth(5)
	for i, v := range vectors {
		dc.DrawLine(
			float64(scale)*float64(v[0]),
			float64(scale)*float64(v[1]),
			float64(scale)*float64(centroids[clusters[i]][0]),
			float64(scale)*float64(centroids[clusters[i]][1]))
		dc.Stroke()
	}
	dc.SavePNG(name)
}

func TestGraphCentroids(t *testing.T) {
	vectors := [][]float32{
		{0, 5},
		{0.1, 4.9},
		{0.01, 5.1},
		{10.1, 7},
		{5.1, 2},
		{5.0, 2.1},
	}
	centroids := [][]float32{
		{0.15, 4.95},
		{0.036666665, 5},
		{6.7333336, 3.7},
	}
	cc := []int{1, 0, 1, 2, 2, 2}
	drawClusters(100, 11, 11, vectors, centroids, cc, "../../diskAnn/testdata/kmeans.png")
}

func TestDistances(t *testing.T) {
	vectors := [6][]float32{
		{0, 5},
		{0.1, 4.9},
		{0.01, 5.1},
		{10.1, 7},
		{5.1, 2},
		{5.0, 2.1},
	}
	centroids := [][]float32{
		{0.1, 4.9},
		{0.036666665, 5},
		{6.7333336, 3.7},
	}
	cc := []int{1, 0, 1, 2, 2, 2}
	for k, v := range vectors {
		var min float32 = math.MaxFloat32
		minIndex := 0
		for i, c := range centroids {
			d := ssdhelpers.L2(v, c)
			if d < min {
				min = d
				minIndex = i
			}
		}
		if minIndex != cc[k] {
			fmt.Println(k, minIndex)
		} else {
			fmt.Println(k)
		}
	}
}

func TestKMeansWithBadCenters(t *testing.T) {
	vectors := [6][]float32{
		{0, 5},
		{0.1, 4.9},
		{0.01, 5.1},
		{10.1, 7},
		{5.1, 2},
		{5.0, 2.1},
	}
	centroids := [][]float32{
		{0.1, 4.9},
		{0.036666665, 5},
		{6.7333336, 3.7},
	}
	kmeans := ssdhelpers.NewKMeansWithCenters(
		3,
		ssdhelpers.L2,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		6,
		2,
		centroids,
	)
	kmeans.Partition()
	fmt.Println(kmeans.Centroid(0))
	fmt.Println(kmeans.Centroid(1))
	fmt.Println(kmeans.Centroid(2))
	centers := make([][]uint64, 6)
	for i := range centers {
		centers[i] = kmeans.NNearest(vectors[i], 2)
	}
	assert.EqualValues(t, centers[0], centers[1])
	assert.EqualValues(t, centers[0], centers[2])
	assert.EqualValues(t, centers[4], centers[5])

	fmt.Println(centers[0])
	fmt.Println(centers[1])
	fmt.Println(centers[2])
	fmt.Println(centers[3])
	fmt.Println(centers[4])
	fmt.Println(centers[5])
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
	kmeans := ssdhelpers.NewKMeans(
		3,
		ssdhelpers.L2,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		6,
		2,
	)
	kmeans.Partition()
	fmt.Println(kmeans.Centroid(0))
	fmt.Println(kmeans.Centroid(1))
	fmt.Println(kmeans.Centroid(2))
	centers := make([][]uint64, 6)
	for i := range centers {
		centers[i] = kmeans.NNearest(vectors[i], 2)
	}
	assert.EqualValues(t, centers[0], centers[1])
	assert.EqualValues(t, centers[0], centers[2])
	assert.EqualValues(t, centers[4], centers[5])

	fmt.Println(centers[0])
	fmt.Println(centers[1])
	fmt.Println(centers[2])
	fmt.Println(centers[3])
	fmt.Println(centers[4])
	fmt.Println(centers[5])
}

func TestWithSift1M(t *testing.T) {
	vectors_size := 100000
	k := 40
	vectors := testinghelpers.ReadSiftVecsFrom("../../diskAnn/testdata/sift/sift_learn.fvecs", vectors_size)
	kmeans := ssdhelpers.New(
		k,
		ssdhelpers.L2,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		vectors_size,
		128,
	)
	kmeans.Partition()
	centersHits := make([]int, kmeans.K)
	for _, v := range vectors {
		centersHits[kmeans.Nearest(v)]++
	}
	for _, c := range centersHits {
		require.True(t, c > 0)
	}
}
