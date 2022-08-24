package hnsw

import (
	"context"
	"sync/atomic"
)

type atomicVectors []atomic.Pointer[[]float32]

type atomicVectorCache struct {
	store atomic.Pointer[atomicVectors]
}

func newAtomicVectorCache() *atomicVectorCache {
	a := &atomicVectorCache{}
	initialStore := make(atomicVectors, initialSize)
	a.store.Store(&initialStore)
	return a
}

func (a *atomicVectorCache) preload(id uint64, vec []float32) {
	vecs := a.store.Load()
	(*vecs)[id].Store(&vec)
}

func (a *atomicVectorCache) get(ctx context.Context, id uint64) ([]float32, error) {
	vecs := a.store.Load()
	return *(*vecs)[id].Load(), nil
}
