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

package diskAnn

import (
	"context"
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"time"

	"testing"

	"github.com/pkg/errors"
	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	testinghelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/testingHelpers"
)

/*func TestClusteredVamana(t *testing.T) {
	R := 4
	L := 10
	dimensions := 2
	vectors_size := 1000
	vectors := generate_vecs(vectors_size, dimensions)
	w := 1024
	index, _ := New(Config{
		R:     R,
		L:     L,
		Alpha: float32(1.1),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		VectorsSize:        uint64(vectors_size),
		Distance:           ssdhelpers.L2,
		ClustersSize:       40,
		ClusterOverlapping: 2,
	})
	index.BuildIndex()

	index2, _ := New(Config{
		R:     R,
		L:     L,
		Alpha: float32(1.2),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		VectorsSize:        uint64(vectors_size),
		Distance:           ssdhelpers.L2,
		ClustersSize:       40,
		ClusterOverlapping: 2,
	})
	index2.BuildIndexSharded()

	testinghelpers.Normalize(vectors, w)
	testinghelpers.PlotGraphHighLighted("vamana_test.png", index.edges, vectors, w, w, index.s_index, 5)
	testinghelpers.PlotGraphHighLighted("vamana_test_sharded.png", index2.edges, vectors, w, w, index2.s_index, 5)
}*/

func loadQueries(queries_size int) [][]float32 {
	f, err := os.Open("./sift/sift_queries.gob")
	if err != nil {
		panic(errors.Wrap(err, "Could not open truths file"))
	}
	defer f.Close()

	queries := make([][]float32, queries_size)
	cDec := gob.NewDecoder(f)
	err = cDec.Decode(&queries)
	if err != nil {
		panic(errors.Wrap(err, "Could not decode truths"))
	}
	return queries
}

func TestBigDataVamana(t *testing.T) {
	rand.Seed(0)
	dimensions := 128
	vectors_size := 100000
	queries_size := 1000
	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, dimensions, queries_size)
	if vectors == nil {
		panic("Error generating vectors")
	}
	fmt.Printf("generating data took %s\n", time.Since(before))

	paramsRs := []int{32, 70}
	paramsLs := []int{50, 125}
	results := make(map[string][][]float32, 0)
	for paramIndex := range paramsRs {
		paramR := paramsRs[paramIndex]
		paramL := paramsLs[paramIndex]
		paramAlpha := float32(1.2)
		before = time.Now()
		index := BuildVamana(
			paramR,
			paramL,
			paramAlpha,
			func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			uint64(vectors_size),
			ssdhelpers.L2,
			"./data",
		)
		fmt.Printf("Index built in: %s\n", time.Since(before))
		//Ks := [3]int{1, 10, 100}
		Ks := [2]int{1, 10}
		fmt.Println("K\tL\trecall\t\tquerying")
		L := [10]int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}

		for _, k := range Ks {
			//testinghelpers.BuildTruths(queries_size, queries, vectors, k, ssdhelpers.L2)
			truths := testinghelpers.LoadTruths(queries_size, k)
			data := make([][]float32, len(L))
			for i, l := range L {
				index.config.L = l
				var relevant uint64
				var retrieved int

				var querying time.Duration = 0
				for i := 0; i < len(queries); i++ {
					before = time.Now()
					results := index.SearchByVector(queries[i], k)
					querying += time.Since(before)
					retrieved += k
					relevant += testinghelpers.MatchesInLists(truths[i], results)
				}

				recall := float32(relevant) / float32(retrieved)
				queryingTime := float32(querying.Microseconds()) / 1000
				data[i] = []float32{queryingTime, recall}
				fmt.Printf("%d\t%s\t%f\n", k, querying/1000, recall)
			}
			results[fmt.Sprintf("Vamana - K: %d (R: %d, L: %d, alpha:%.1f)", k, paramR, paramL, paramAlpha)] = data
		}
	}
	testinghelpers.ChartData("Recall Vs Latency", "", results, "index.html")
}

/*
func TestBigDataHNSW(t *testing.T) {
	rand.Seed(0)
	dimensions := 128
	vectors_size := 100000
	queries_size := 1000
	k := 100
	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, dimensions, queries_size)
	if vectors == nil {
		panic("Error generating vectors")
	}
	//testinghelpers.BuildTruths(queries_size, queries, vectors, k, euclidean_distance)
	truths := testinghelpers.LoadTruths(queries_size, k)
	fmt.Printf("generating data took %s\n", time.Since(before))

	efc := 128
	ef := 64
	maxN := 32

	index, _ := hnsw.New(hnsw.Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "recallbenchmark",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
	}, hnsw.UserConfig{
		MaxConnections: maxN,
		EFConstruction: efc,
		EF:             ef,
	})

	vectorIndex := index

	workerCount := runtime.GOMAXPROCS(0)
	jobsForWorker := make([][][]float32, workerCount)

	before = time.Now()
	for i, vec := range vectors {
		workerID := i % workerCount
		jobsForWorker[workerID] = append(jobsForWorker[workerID], vec)
	}

	wg := &sync.WaitGroup{}
	for workerID, jobs := range jobsForWorker {
		wg.Add(1)
		go func(workerID int, myJobs [][]float32) {
			defer wg.Done()
			for i, vec := range myJobs {
				originalIndex := (i * workerCount) + workerID
				err := vectorIndex.Add(uint64(originalIndex), vec)
				require.Nil(t, err)
			}
		}(workerID, jobs)
	}

	wg.Wait()
	indexing := time.Since(before)
	fmt.Printf("Indexing done in: %s\n", indexing)
	efs := []int{8, 16, 32, 64, 128, 256}
	fmt.Println("ef	recall	querying")
	for _, efSearch := range efs {
		index.UpdateUserConfig(hnsw.UserConfig{
			MaxConnections: maxN,
			EFConstruction: efc,
			EF:             efSearch,
		})
		var relevant uint64
		var retrieved int

		var querying time.Duration = 0
		before = time.Now()
		for i := 0; i < len(queries); i++ {
			before = time.Now()
			results, _, err := vectorIndex.SearchByVector(queries[i], k, nil)
			querying += time.Since(before)
			require.Nil(t, err)

			retrieved += k
			relevant += matchesInLists(truths[i], results)
		}

		recall := float32(relevant) / float32(retrieved)
		fmt.Printf("%d	%f	%s\n", efSearch, recall, querying/1000)
	}
}*/

func TestCharts(t *testing.T) {
	results := make(map[string][][]float32, 0)
	results["Vamana-100K (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{163.928, 0.100000},
		{298.392, 0.200020},
		{441.883, 0.300110},
		{612.648, 0.400230},
		{809.438, 0.500330},
		{1065.917, 0.600460},
		{1291.523, 0.700660},
		{1559.368, 0.800850},
		{1921.146, 0.900580},
		{2281.075, 0.982600},
		{2671.63, 0.985780},
		{3187.38, 0.988130},
		{3622.974, 0.990240},
	}
	results["Vamana-100K (R: 70, L: 125, alpha:1.2)"] = [][]float32{
		{302.028, 0.100000},
		{456.726, 0.200020},
		{691.125, 0.300120},
		{922.246, 0.400250},
		{1192.283, 0.500330},
		{1514.109, 0.600470},
		{1936.631, 0.700670},
		{2276.221, 0.800880},
		{2680.889, 0.900970},
		{3207.606, 0.997720},
		{3726.338, 0.998250},
		{4194.549, 0.998570},
		{4743.362, 0.998920},
	}
	results["HNSW-100K (efC: 512, ef: 256, maxN: 128)"] = [][]float32{
		{633.732, 0.983720},
		{603.067, 0.983720},
		{556.734, 0.983720},
		{489.457, 0.983720},
		{745.091, 0.990590},
		{1087.982, 0.998640},
	}
	results["HNSW-100K (efC: 512, ef: 256, maxN: 128)"] = [][]float32{
		{755.518, 0.996870},
		{653.413, 0.996870},
		{648.089, 0.996870},
		{643.147, 0.996870},
		{780.003, 0.998520},
		{1338.566, 0.999820},
	}
	testinghelpers.ChartData("100-Recall@100 vs Latency", "", results, "line-100.html")

	results = make(map[string][][]float32, 0)
	results["Vamana-100K (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{161.431, 0.979200},
		{299.493, 0.990300},
		{443.038, 0.993000},
		{613.695, 0.994300},
		{817.2, 0.995300},
		{1031.923, 0.996600},
		{1286.471, 0.997200},
		{1537.991, 0.997700},
		{1977.028, 0.997900},
		{2382.66, 0.998400},
		{2650.609, 0.998700},
		{3096.479, 0.998800},
		{3572.708, 0.998700},
	}
	results["Vamana-100K (R: 70, L: 125, alpha:1.2)"] = [][]float32{
		{247.432, 0.992600},
		{447.583, 0.997300},
		{679.018, 0.998600},
		{928.812, 0.999100},
		{1229.798, 0.999200},
		{1498.787, 0.999500},
		{1866.398, 0.999500},
		{2217.243, 0.999700},
		{2593.244, 0.999400},
		{3046.202, 0.999900},
		{3512.017, 0.999800},
		{4183.827, 0.999700},
		{4663.706, 0.999600},
	}
	results["HNSW-100K (efC: 512, ef: 256, maxN: 128)"] = [][]float32{
		{123.772, 0.960200},
		{126.898, 0.974600},
		{203.975, 0.989800},
		{345.065, 0.997000},
		{634.041, 0.999200},
		{1079.155, 0.999700},
	}
	results["HNSW-100K (efC: 512, ef: 256, maxN: 128)"] = [][]float32{
		{121.867, 0.992700},
		{163.182, 0.995000},
		{299.105, 0.998500},
		{469.748, 0.999500},
		{770.672, 0.999600},
		{1315.154, 0.999800},
	}
	testinghelpers.ChartData("Recall vs Latency", "", results, "line-10.html")
}
