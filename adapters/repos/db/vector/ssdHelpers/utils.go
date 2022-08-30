package ssdhelpers

import (
	"context"
	"math"
	"runtime"
	"sync"
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

type Action func(workerId uint64, taskIndex uint64, mutex *sync.Mutex)

func Concurrently(n uint64, action Action) {
	n64 := float64(n)
	workerCount := runtime.GOMAXPROCS(0)
	mutex := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	split := uint64(math.Ceil(n64 / float64(workerCount)))
	for worker := uint64(0); worker < uint64(workerCount); worker++ {
		wg.Add(1)
		go func(workerID uint64) {
			defer wg.Done()
			for i := workerID * split; i < uint64(math.Min(float64((workerID+1)*split), n64)); i++ {
				action(workerID, i, mutex)
			}
		}(worker)
	}
	wg.Wait()
}

func Contains(elements []uint64, x uint64) bool {
	for _, e := range elements {
		if e == x {
			return true
		}
	}
	return false
}
