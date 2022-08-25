package hnsw

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAtomicVectorCache(t *testing.T) {
	vecs := genVectors(50, 32)

	cache := newAtomicVectorCache()
	for i, vec := range vecs {
		cache.preload(uint64(i), vec)
	}

	for i, vec := range vecs {
		retrieved, _ := cache.get(context.Background(), uint64(i))
		assert.Equal(t, vec, retrieved)
	}
}

func TestAtomicVectorCacheGrowing(t *testing.T) {
	vecs := genVectors(2, 32)

	cache := newAtomicVectorCache()
	assert.Equal(t, int(initialSize), int(cache.len()))

	posBeforeGrow := uint64(initialSize - 1)
	cache.preload(posBeforeGrow, vecs[0])

	posAfterGrow := posBeforeGrow + 1000
	cache.grow(posAfterGrow)
	assert.Greater(t, int(cache.len()), int(posAfterGrow))

	cache.preload(posAfterGrow, vecs[1])

	retrieved0, _ := cache.get(context.Background(), uint64(posBeforeGrow))
	assert.Equal(t, vecs[0], retrieved0)

	retrieved1, _ := cache.get(context.Background(), uint64(posAfterGrow))
	assert.Equal(t, vecs[1], retrieved1)
}
