package ssdhelpers_test

import (
	"context"
	"fmt"
	"math/rand"
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
	rand.Seed(0)
	dimensions := 128
	vectors_size := 1000000
	queries_size := 100
	k := 100
	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)

	pq := ssdhelpers.NewProductQuantizer(
		128,
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
	for i := 0; i < vectors_size; i++ {
		encoded[i] = pq.Encode(vectors[i])
	}
	fmt.Println("time elapse:", time.Since(before))
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
	assert.True(t, recall > 0.65)
}

/*
func TestPQDistance(t *testing.T) {
	rand.Seed(0)
	dimensions := 128
	vectors_size := 1000
	queries_size := 100

	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	pq := ssdhelpers.NewProductQuantizer(
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

	encoded := make([][]byte, vectors_size)
	for i := 0; i < vectors_size; i++ {
		encoded[i] = pq.Encode(vectors[i])
	}

	for _, q := range queries {
		pq.CenterAt(q)
		for _, e2 := range encoded {
			assert.Equal(t, ssdhelpers.L2(q, pq.Decode(e2)), pq.Distance(e2))
			break
		}

	}
}
*/
