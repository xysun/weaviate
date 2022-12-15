package hnswPq_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnswPq"
	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	testinghelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/testingHelpers"
	"github.com/stretchr/testify/assert"
)

/*
Building the index took 3m17.0460215s
0.9639 3877.69
*/
func TestHnswPqGist(t *testing.T) {
	fmt.Println("Gist")
	efConstruction := 256
	ef := 256
	maxNeighbors := 64
	dimensions := 128
	vectors_size := 10000
	queries_size := 100
	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size, dimensions, "sift", "../diskAnn/testdata")
	k := 100
	truths := testinghelpers.BuildTruths(queries_size, vectors_size, queries, vectors, k, ssdhelpers.NewCosineDistanceProvider().Distance, "../diskAnn/testdata")
	//testinghelpers.Normalize(vectors)
	//testinghelpers.Normalize(queries)
	fmt.Printf("generating data took %s\n", time.Since(before))

	before = time.Now()
	index := hnswPq.NewHnswPq(
		hnsw.Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewL2SquaredProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
		}, hnsw.UserConfig{
			MaxConnections: maxNeighbors,
			EFConstruction: efConstruction,
			EF:             ef,
		},
		ssdhelpers.PQConfig{
			Segments:  dimensions,
			Centroids: 256,
			Distance:  ssdhelpers.NewL2DistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			Dimensions:  dimensions,
			EncoderType: ssdhelpers.UseTileEncoder,
		},
	)
	for id := 0; id < vectors_size; id++ {
		index.Add(uint64(id), vectors[id])
		if id%1000 == 0 {
			fmt.Println(id, time.Since(before))
		}
	}
	fmt.Printf("Building the index took %s\n", time.Since(before))

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

	/*
		0.9935 25701.44

		0.9946 25937.66
	*/
	recall := float32(relevant) / float32(retrieved)
	latency := float32(querying.Microseconds()) / float32(queries_size)
	assert.True(t, recall > 0.099)
	assert.True(t, latency < 22700)
	fmt.Println(recall, latency)
}

/*
Time to compress: 10m15.779157958s
Building the index took 16m26.500126375s
128
0.981 7959.25
256
0.985 12602.53
512
0.984 20773.9

Building the index took 18m46.250776s
128
0.98 7550.67

256
0.981 12850.95

512
0.981 20695.24
*/
func TestHnswPqSift(t *testing.T) {
	fmt.Println("Sift1MPQKMeans 10K/1K")
	efConstruction := 64
	ef := 32
	maxNeighbors := 32
	dimensions := 128
	vectors_size := 1000000
	queries_size := 100
	switch_at := vectors_size
	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size, dimensions, "sift", "../diskAnn/testdata")
	k := 10
	truths := testinghelpers.BuildTruths(queries_size, vectors_size, queries, vectors, k, ssdhelpers.NewL2DistanceProvider().Distance, "../diskAnn/testdata")
	fmt.Printf("generating data took %s\n", time.Since(before))

	uc := hnsw.UserConfig{
		MaxConnections: maxNeighbors,
		EFConstruction: efConstruction,
		EF:             ef,
		Compressed:     false,
	}
	init := time.Now()
	index := hnswPq.NewHnswPq(
		hnsw.Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewL2SquaredProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
		}, uc,
		ssdhelpers.PQConfig{
			Size:      switch_at,
			Segments:  dimensions,
			Centroids: 256,
			Distance:  ssdhelpers.NewL2DistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			Dimensions:  dimensions,
			EncoderType: ssdhelpers.UseKMeansEncoder,
		},
	)
	testinghelpers.Concurrently(uint64(switch_at), func(_, id uint64, _ *sync.Mutex) {
		index.Add(uint64(id), vectors[id])
		if id%1000 == 0 {
			fmt.Println(id, time.Since(before))
		}
	})
	before = time.Now()
	uc.Compressed = true
	index.Compress() /*should have configuration.compressed = true*/
	fmt.Printf("Time to compress: %s", time.Since(before))
	fmt.Println()
	testinghelpers.Concurrently(uint64(vectors_size-switch_at), func(_, id uint64, _ *sync.Mutex) {
		idx := switch_at + int(id)
		index.Add(uint64(idx), vectors[idx])
		if id%1000 == 0 {
			fmt.Println(idx, time.Since(before))
		}
	})
	fmt.Printf("Building the index took %s\n", time.Since(init))

	for _, currentEF := range []int{32, 64, 128, 256, 512} {
		uc.EF = currentEF
		index.UpdateUserConfig(uc)
		fmt.Println(currentEF)
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

		/*
			0.9935 25701.44

			0.9946 25937.66
		*/
		recall := float32(relevant) / float32(retrieved)
		latency := float32(querying.Microseconds()) / float32(queries_size)
		assert.True(t, recall > 0.099)
		assert.True(t, latency < 22700)
		fmt.Println(recall, latency)
		fmt.Println()
	}

}

/*
10K 0.99
100K 0.987
*/
func TestHnswSiftEncodeDecode(t *testing.T) {
	fmt.Println("Sift-distorted 10K")
	efConstruction := 256
	ef := 256
	maxNeighbors := 64
	dimensions := 128
	vectors_size := 10000
	queries_size := 100
	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size, dimensions, "sift", "../diskAnn/testdata")
	k := 10
	truths := testinghelpers.BuildTruths(queries_size, vectors_size, queries, vectors, k, ssdhelpers.NewL2DistanceProvider().Distance, "../diskAnn/testdata")
	fmt.Printf("generating data took %s\n", time.Since(before))
	pqCfg := ssdhelpers.PQConfig{
		Segments:  dimensions,
		Centroids: 256,
		Distance:  ssdhelpers.NewL2DistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		Dimensions:  dimensions,
		EncoderType: ssdhelpers.UseKMeansEncoder,
	}
	pq := ssdhelpers.NewProductQuantizer(pqCfg.Segments, pqCfg.Segments, pqCfg.Distance, pqCfg.VectorForIDThunk, pqCfg.Dimensions, vectors_size, pqCfg.EncoderType)
	pq.Fit()
	for i := range vectors {
		vectors[i] = pq.Decode(pq.Encode(vectors[i]))
	}

	before = time.Now()
	index, _ := hnsw.New(
		hnsw.Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewL2SquaredProvider(),
			VectorForIDThunk: func(_ context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
		}, hnsw.UserConfig{
			MaxConnections: maxNeighbors,
			EFConstruction: efConstruction,
			EF:             ef,
		},
	)
	for id := 0; id < vectors_size; id++ {
		index.Add(uint64(id), vectors[id])
		if id%10000 == 0 {
			fmt.Println(id, time.Since(before))
		}
	}
	fmt.Printf("Building the index took %s\n", time.Since(before))

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

	/*
		0.9935 25701.44

		0.9946 25937.66
	*/
	recall := float32(relevant) / float32(retrieved)
	latency := float32(querying.Microseconds()) / float32(queries_size)
	assert.True(t, recall > 0.099)
	assert.True(t, latency < 22700)
	fmt.Println(recall, latency)
}

/*
Building the index took 5m50.075975333s
128
0.995 2766.86

256
0.999 4374.68

512
0.997 7815.32
*/
func TestHnswSift(t *testing.T) {
	fmt.Println("Sift1M")
	efConstruction := 64
	ef := 32
	maxNeighbors := 32
	dimensions := 128
	vectors_size := 1000000
	queries_size := 100
	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size, dimensions, "sift", "../diskAnn/testdata")
	k := 10
	truths := testinghelpers.BuildTruths(queries_size, vectors_size, queries, vectors, k, ssdhelpers.NewL2DistanceProvider().Distance, "../diskAnn/testdata")
	fmt.Printf("generating data took %s\n", time.Since(before))

	uc := hnsw.UserConfig{
		MaxConnections: maxNeighbors,
		EFConstruction: efConstruction,
		EF:             ef,
		Compressed:     false,
	}
	before = time.Now()
	index, _ := hnsw.New(
		hnsw.Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewL2SquaredProvider(),
			VectorForIDThunk: func(_ context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
		}, uc,
	)
	testinghelpers.Concurrently(uint64(vectors_size), func(_, id uint64, _ *sync.Mutex) {
		index.Add(uint64(id), vectors[id])
		if id%1000 == 0 {
			fmt.Println(id, time.Since(before))
		}
	})
	fmt.Printf("Building the index took %s\n", time.Since(before))

	for _, currentEF := range []int{32, 64, 128, 256, 512} {
		uc.EF = currentEF
		index.UpdateUserConfig(uc)
		fmt.Println(currentEF)

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

		/*
			0.9935 25701.44

			0.9946 25937.66
		*/
		recall := float32(relevant) / float32(retrieved)
		latency := float32(querying.Microseconds()) / float32(queries_size)
		assert.True(t, recall > 0.099)
		assert.True(t, latency < 22700)
		fmt.Println(recall, latency)
		fmt.Println()
	}
}

func TestChartsLoadLatencyTime(t *testing.T) {
	results := make(map[string][][]float32, 0)
	results["HNSW (efC: 128, ef: 128, maxN: 64, indexing: 5m50.075975333s)"] = [][]float32{
		{2766.86, 0.995},
		{4374.68, 0.999},
		{7815.32, 0.997},
	}
	results["HNSW (efC: 64, ef: 32, maxN: 32, indexing: )"] = [][]float32{
		{764.07, 0.919},
		{1201.92, 0.965},
		{2013.61, 0.990},
		{6254.97, 0.997},
	}
	results["HNSW+PQ (Fit: 100%, efC: 128, ef: 128, maxN: 64, indexing: 16m26.500126375s)"] = [][]float32{
		{7959.25, 0.981},
		{12602.53, 0.985},
		{20773.9, 0.984},
	}
	results["HNSW+PQ (Fit: 10%, efC: 128, ef: 128, maxN: 64, indexing: 16m26.500126375s)"] = [][]float32{
		{7550.67, 0.980},
		{12850.95, 0.981},
		{20695.24, 0.981},
	}
	testinghelpers.ChartData("Sift1M L2", "Latency (micro seconds) Vs Recall", results, "../diskAnn/testdata/hnswpq.html")
}
