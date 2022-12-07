package ssdhelpers

import (
	"context"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type DistanceFunction func([]float32, []float32) float32
type (
	VectorForID      func(ctx context.Context, id uint64) ([]float32, error)
	MultiVectorForID func(ctx context.Context, ids []uint64) ([][]float32, error)
)

type DistanceProvider interface {
	Distance(vec1, vec2 []float32) float32
	Aggregate(d1, d2 float32) float32
}

type L2DistanceProvider struct {
	distancer distancer.L2SquaredProvider
}

func NewL2DistanceProvider() *L2DistanceProvider {
	return &L2DistanceProvider{
		distancer: distancer.NewL2SquaredProvider(),
	}
}

func (dp L2DistanceProvider) Distance(x, y []float32) float32 {
	d, _, _ := dp.distancer.SingleDist(x, y)
	return d
}

func (dp L2DistanceProvider) Aggregate(d1, d2 float32) float32 {
	return d1 + d2
}

type CosineDistanceProvider struct {
	distancer distancer.CosineDistanceProvider
}

func NewCosineDistanceProvider() *CosineDistanceProvider {
	return &CosineDistanceProvider{
		distancer: distancer.NewCosineDistanceProvider(),
	}
}

func (dp CosineDistanceProvider) Distance(x, y []float32) float32 {
	d, _, _ := dp.distancer.SingleDist(x, y)
	return d
}

func (dp CosineDistanceProvider) Aggregate(d1, d2 float32) float32 {
	return 1 - (2 - d1 - d2)
}

func Contains(elements []uint64, x uint64) bool {
	for _, e := range elements {
		if e == x {
			return true
		}
	}
	return false
}
