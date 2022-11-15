package ssdhelpers

import (
	"context"
)

type DistanceFunction func([]float32, []float32) float32
type (
	VectorForID      func(ctx context.Context, id uint64) ([]float32, error)
	MultiVectorForID func(ctx context.Context, ids []uint64) ([][]float32, error)
)

func L2(x []float32, y []float32) float32 {
	d := float32(0.0)
	for i := range x {
		diff := x[i] - y[i]
		d += diff * diff
	}
	return d
}

func Contains(elements []uint64, x uint64) bool {
	for _, e := range elements {
		if e == x {
			return true
		}
	}
	return false
}
