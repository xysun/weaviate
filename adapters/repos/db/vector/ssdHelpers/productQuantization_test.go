package ssdhelpers_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	testinghelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/testingHelpers"
	"github.com/stretchr/testify/assert"
)

func compare(x []byte, y []byte) bool {
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

func TestPQ(t *testing.T) {
	dimensions := 128
	vectors_size := 100000
	queries_size := 100
	k := 100
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size, "../diskAnn")
	pq := ssdhelpers.NewProductQunatizer(
		32,
		256,
		ssdhelpers.L2,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		dimensions,
		vectors_size,
	)
	before := time.Now()
	pq.Fit()
	fmt.Println("time elapse:", time.Since(before))
	before = time.Now()
	encoded := make([][]byte, vectors_size)
	testinghelpers.Concurrently(uint64(vectors_size), func(_ uint64, i uint64, _ *sync.Mutex) {
		encoded[i] = pq.Encode(vectors[i])
	})
	fmt.Println("time elapse:", time.Since(before))
	collisions := 0
	testinghelpers.Concurrently(uint64(len(encoded)-1), func(_ uint64, i uint64, _ *sync.Mutex) {
		for j := int(i) + 1; j < len(encoded); j++ {
			if compare(encoded[i], encoded[j]) {
				collisions++
				fmt.Println(collisions)
			}
		}
	})
	fmt.Println(collisions)
	fmt.Println("=============")
	s := ssdhelpers.NewSortedSet(
		k,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		ssdhelpers.L2,
		nil,
		vectors_size,
	)
	s.SetPQ(encoded, pq)
	var relevant uint64
	for _, query := range queries {
		pq.CenterAt(query)
		truth := testinghelpers.BruteForce(vectors, query, k, ssdhelpers.L2)
		s.ReCenter(query, false)
		for v := range vectors {
			s.AddPQVector(uint64(v), nil, nil)
		}
		results, _ := s.Elements(k)
		relevant += testinghelpers.MatchesInLists(truth, results)
	}
	recall := float32(relevant) / float32(k*queries_size)
	fmt.Println(recall)
	assert.True(t, recall > 0.8)
}
