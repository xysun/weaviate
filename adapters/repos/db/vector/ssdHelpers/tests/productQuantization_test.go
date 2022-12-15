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

func TestPQGist(t *testing.T) {
	dimensions := 960
	vectors_size := 100000
	queries_size := 100
	k := 100
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size, dimensions, "gist", "../../diskAnn/testdata")
	/*
		GIST
		Tiles
			100K ->
				time elapse: 2.261131583s
				time elapse: 12.721803958s
				=============
				8b -> 0.9881
				6b -> 0.9579
				4b -> 0.8532
		KMeans
			100K
				60 ->
					time elapse: 3m31.137677125s
					time elapse: 1m20.983001584s
					=============
					0.5224
				120 ->
					time elapse: 4m3.915850625s
					time elapse: 1m31.835826791s
					=============
					0.6817
				240 ->
					time elapse: 6m40.595989708s
					time elapse: 2m33.470047459s
					=============
					0.845
				480 ->
					time elapse: 8m47.254897s
					time elapse: 3m15.953190708s
					=============
					0.9514
				960 ->
					time elapse: 5m37.086638208s
					time elapse: 4m30.193578834s
					=============
					0.9893
		SIFT
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
		ssdhelpers.NewL2DistanceProvider(),
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
	fmt.Println("=============")
	s := ssdhelpers.NewSortedSet(
		k,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		ssdhelpers.NewL2DistanceProvider(),
		nil,
	)
	s.SetPQ(encoded, pq)
	var relevant uint64
	for _, query := range queries {
		pq.CenterAt(query)
		truth := testinghelpers.BruteForce(vectors, query, k, ssdhelpers.NewL2DistanceProvider().Distance)
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

func TestPQSift(t *testing.T) {
	dimensions := 128
	vectors_size := 100000
	queries_size := 100
	k := 100
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size, dimensions, "sift", "../../diskAnn/testdata")
	/*
		SIFT
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
		128,
		256,
		ssdhelpers.NewL2DistanceProvider(),
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
	fmt.Println("=============")
	s := ssdhelpers.NewSortedSet(
		k,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		ssdhelpers.NewL2DistanceProvider(),
		nil,
	)
	s.SetPQ(encoded, pq)
	var relevant uint64
	for _, query := range queries {
		pq.CenterAt(query)
		truth := testinghelpers.BruteForce(vectors, query, k, ssdhelpers.NewL2DistanceProvider().Distance)
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
