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
