package hnsw

import (
	"context"
	"math/rand"
	"sync"
	"testing"
)

var (
	dim         = 32
	count       = 25_000
	readThreads = 10
)

func BenchmarkCacheImport_Atomic(b *testing.B) {
	vectors := genVectors(count, dim)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cache := newAtomicVectorCache()
		wg := &sync.WaitGroup{}
		for id, vec := range vectors {
			wg.Add(1)
			id := id
			vec := vec
			go func() {
				cache.preload(uint64(id), vec)
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func BenchmarkCacheImport_ShardedLock(b *testing.B) {
	vectors := genVectors(count, dim)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cache := newShardedLockCache(nil, 1e9, nil, false)
		wg := &sync.WaitGroup{}
		for id, vec := range vectors {
			wg.Add(1)
			id := id
			vec := vec
			go func() {
				cache.preload(uint64(id), vec)
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func BenchmarkCacheLoad_ShardedLock(b *testing.B) {
	vectors := genVectors(count, dim)

	b.ResetTimer()
	cache := newShardedLockCache(nil, 1e9, nil, false)
	for id, vec := range vectors {
		cache.preload(uint64(id), vec)
	}

	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		wg := &sync.WaitGroup{}
		for tc := 0; tc < readThreads; tc++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range vectors {
					v, err := cache.get(ctx, uint64(j))
					_, _ = v, err
				}
			}()
		}
		wg.Wait()
	}
}

func BenchmarkCacheLoad_Atomic(b *testing.B) {
	vectors := genVectors(count, dim)

	b.ResetTimer()
	cache := newAtomicVectorCache()
	for id, vec := range vectors {
		cache.preload(uint64(id), vec)
	}

	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		wg := &sync.WaitGroup{}
		for tc := 0; tc < readThreads; tc++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range vectors {
					v, err := cache.get(ctx, uint64(j))
					_, _ = v, err
				}
			}()
		}
		wg.Wait()
	}
}

func genVectors(count, dim int) [][]float32 {
	out := make([][]float32, count)
	for i := range out {
		out[i] = make([]float32, dim)
		for j := range out[i] {
			out[i][j] = rand.Float32()
		}

	}

	return out
}
