package hnsw

import (
	"context"
	"sync/atomic"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type atomicVectors []atomic.Pointer[[]float32]

type atomicVectorCache struct {
	store           atomic.Pointer[atomicVectors]
	vectorForID     VectorForID
	normalizeOnRead bool
}

func newAtomicVectorCache(
	vecForID VectorForID, normalize bool,
) *atomicVectorCache {
	a := &atomicVectorCache{
		vectorForID:     vecForID,
		normalizeOnRead: normalize,
	}
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
	vecPtr := (*vecs)[id].Load()
	if vecPtr == nil {
		return a.handleCacheMiss(ctx, id)
	}

	return *vecPtr, nil
}

func (a *atomicVectorCache) multiGet(ctx context.Context, ids []uint64) ([][]float32, error) {
	out := make([][]float32, len(ids))
	vecs := a.store.Load()
	for i, id := range ids {
		vecPtr := (*vecs)[id].Load()
		if vecPtr == nil {
			vec, err := a.handleCacheMiss(ctx, id)
			if err != nil {
				return nil, err
			}
			out[i] = vec
		} else {
			out[i] = *vecPtr
		}
	}

	return out, nil
}

func (a *atomicVectorCache) handleCacheMiss(ctx context.Context, id uint64) ([]float32, error) {
	vec, err := a.vectorForID(ctx, id)
	if err != nil {
		return nil, err
	}

	if a.normalizeOnRead {
		vec = distancer.Normalize(vec)
	}

	// atomic.AddInt64(&n.count, 1)
	vecs := a.store.Load()
	(*vecs)[id].Store(&vec)

	return vec, nil
}

func (a *atomicVectorCache) len() int32 {
	vecs := a.store.Load()
	return int32(len(*vecs))
}

// grow prefers thread-safety over accuracy. Keep in mind that this is a cache
// and vectors are immutable. Thus, a cache miss is not a big problem; it only
// comes with a performance penality of having to fetch the entry that we
// missed during copying.
//
// All access is atomic and therefore thread-safe. The reason for "inaccuracy"
// is because the iteration over the old cache is not an atomic operation (only
// its individual parts are). As a result, someone could write into element
// 100, when we have already iterated all the way to element 200. We would then
// miss this write and not copy it into the new cache. As outlined above, this
// is fine because it only leads to a cache miss.
//
// There is also nothing that makes sure only one grow operation is happening
// at the same time, i.e. there is no mutual exclusion. Since the pointer swap
// is atomic parallel growths would not be an issue from a thread-safety
// perspective. From a logical perspective, they might, but since the caller
// already holds a lock, we can run under the assumption that this is mitigated.
func (a *atomicVectorCache) grow(node uint64) {
	newSize := node + minimumIndexGrowthDelta
	newCache := make(atomicVectors, newSize)

	existing := a.store.Load()
	for i := range *existing {
		newCache[i].Store((*existing)[i].Load())
	}

	oldCache := a.store.Swap(&newCache)

	// clean up old to prevent memory leaks
	for i := range *existing {
		(*oldCache)[i].Store(nil)
	}
}

func (a *atomicVectorCache) countVectors() int64 {
	panic("not implemented yet")
}

func (a *atomicVectorCache) prefetch(id uint64) {
	// do nothing
}

func (a *atomicVectorCache) drop() {
	panic("not implemented yet")
}

func (a *atomicVectorCache) updateMaxSize(size int64) {
	panic("not implemented yet")
}

func (a *atomicVectorCache) copyMaxSize() int64 {
	return -1
}

func (a *atomicVectorCache) delete(ctx context.Context, id uint64) {
	panic("not implemented yet")
}
