//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build benchmarkPQ
// +build benchmarkPQ

package hnsw

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	//"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecall(t *testing.T) {
	// efConstruction := 64
	// ef := 64
	// maxNeighbors := 32

	var vectors [][]float32
	var queries [][]float32
	var truths [][]uint64
	//var vectorIndex *hnsw

	t.Run("generate random vectors", func(t *testing.T) {
		vectorsJSON, err := ioutil.ReadFile("recall_vectors.json")
		require.Nil(t, err)
		err = json.Unmarshal(vectorsJSON, &vectors)
		require.Nil(t, err)

		queriesJSON, err := ioutil.ReadFile("recall_queries.json")
		require.Nil(t, err)
		err = json.Unmarshal(queriesJSON, &queries)
		require.Nil(t, err)

		truthsJSON, err := ioutil.ReadFile("recall_truths.json")
		require.Nil(t, err)
		err = json.Unmarshal(truthsJSON, &truths)
		require.Nil(t, err)
	})

	t.Run("test product quantization", func(t *testing.T) {

		var pq productQuantizer
		pq.m = 8
		pq.Ks = 64
		pq.fit(vectors)

		require.Equal(t, pq.m, len(pq.codewords))
		require.Equal(t, pq.Ks, len(pq.codewords[0]))
		require.Equal(t, int(len(vectors[0])/pq.m), len(pq.codewords[0][0]))

		encoded := pq.encode(vectors[0])
		//fmt.Println(encoded)

		require.Equal(t, len(encoded), pq.m)

		samples := 10

		quantizedVectors := make([][]float32, samples)

		for i := 0; i < samples; i++ {
			quantizedVectors[i] = pq.encode(vectors[i])
		}

		fmt.Printf("Distances for Vector %d:\n", 0)
		distances := pq.search(vectors[0], quantizedVectors)

		// next step is recall testing
		_ = distances

	})

}

func matchesInLists(control []uint64, results []uint64) int {
	desired := map[uint64]struct{}{}
	for _, relevant := range control {
		desired[relevant] = struct{}{}
	}

	var matches int
	for _, candidate := range results {
		_, ok := desired[candidate]
		if ok {
			matches++
		}
	}

	return matches
}

func containsDuplicates(in []uint64) bool {
	seen := map[uint64]struct{}{}

	for _, value := range in {
		if _, ok := seen[value]; ok {
			return true
		}
		seen[value] = struct{}{}
	}

	return false
}
