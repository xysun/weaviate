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
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size, "../testdata")
	/*
		Tiles
			100K ->
				time elapse: 156.957375ms
				time elapse: 1.418848333s
				=============
				0.9749
			1M
				time elapse: 1.561379542s
				time elapse: 14.285373917s
				=============
				0.97

		KMeans
			100K
				64 ->
				time elapse: 52.578619209s
				time elapse: 26.147346166s
				============
				0.9297

				128 ->
				time elapse: 1m40.145045833s
				time elapse: 36.7864395s
				=============
				0.9987

			1M
				64 ->
				time elapse: 8m27.160292875s
				time elapse: 4m19.166638083s
				=============
				0.9089
	*/
	pq := ssdhelpers.NewProductQuantizer(
		32,
		256,
		ssdhelpers.L2,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		dimensions,
		vectors_size,
		ssdhelpers.UseTileEncoder,
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
