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

package diskAnn_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/diskAnn"
	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	testinghelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/testingHelpers"
	"github.com/stretchr/testify/assert"
)

func TestRecall(t *testing.T) {
	rand.Seed(0)
	dimensions := 128
	vectors_size := 10000
	queries_size := 1000
	before := time.Now()
	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, 128)
	fmt.Printf("generating data took %s\n", time.Since(before))

	before = time.Now()
	index, _ := diskAnn.New(diskAnn.Config{
		VectorForIDThunk: nil,
		Distance:         ssdhelpers.L2,
	},
		diskAnn.UserConfig{
			R:                  32,
			L:                  50,
			Alpha:              1.2,
			VectorsSize:        uint64(0),
			ClustersSize:       40,
			ClusterOverlapping: 2,
			Dimensions:         dimensions,
			C:                  0,
			Path:               "",
			Segments:           128,
			Centroids:          255,
		})
	index.BuildIndex()
	for id := 0; id < vectors_size; id++ {
		index.Add(uint64(id), vectors[id])
	}
	index.SwitchGraphToDisk("testdata/test.praph", 64, 255, 0)

	k := 10
	L := []int{4, 5, 10}
	truths := testinghelpers.BuildTruths(queries_size, vectors_size, queries, vectors, k, ssdhelpers.L2)
	for _, l := range L {
		l = l * k
		index.SetL(l)
		var relevant uint64
		var retrieved int

		var querying time.Duration = 0
		for i := 0; i < len(queries); i++ {
			before = time.Now()
			results, _, _ := index.SearchByVector(queries[i], k, nil)
			querying += time.Since(before)
			retrieved += k
			relevant += testinghelpers.MatchesInLists(truths[i], results)
		}

		recall := float32(relevant) / float32(retrieved)
		latency := float32(querying.Microseconds()) / float32(queries_size)
		assert.True(t, recall > 0.9)
		assert.True(t, latency < 700)
	}
}
