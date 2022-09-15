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

func generate_vecs(size int, dimensions int) [][]float32 {
	vectors := make([][]float32, 0, size)
	for i := 0; i < size; i++ {
		v := make([]float32, 0, dimensions)
		for j := 0; j < dimensions; j++ {
			v = append(v, rand.Float32())
		}
		vectors = append(vectors, v)
	}
	return vectors
}

/*
func TestClusteredVamana(t *testing.T) {
	R := 4
	L := 10
	dimensions := 2
	vectors_size := 1000
	vectors := generate_vecs(vectors_size, dimensions)
	w := 1024
	index := testinghelpers.BuildVamana(
		R,
		L,
		1.2,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		uint64(vectors_size),
		ssdhelpers.L2,
		"./data",
	)

	testinghelpers.Normalize(vectors, w)
	testinghelpers.PlotGraph("vamana_flat_test.png", index.GetGraph(), vectors, w, w)
	testinghelpers.PlotGraphHighLightedBold("vamana_3_test.png", index.GetGraph(), vectors, w, w, index.GetEntry(), 3)
	testinghelpers.PlotGraphHighLightedBold("vamana_6_test.png", index.GetGraph(), vectors, w, w, index.GetEntry(), 6)
	testinghelpers.PlotGraphHighLightedBold("vamana_9_test.png", index.GetGraph(), vectors, w, w, index.GetEntry(), 9)
}
*/
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

	paramsRs := []int{35}
	paramsLs := []int{60}
	alphas := []float32{1.2}
	results := make(map[string][][]float32, 0)
	for _, paramAlpha := range alphas {
		for paramIndex := range paramsRs {
			paramR := paramsRs[paramIndex]
			paramL := paramsLs[paramIndex]
			before = time.Now()
			index := testinghelpers.BuildVamana(
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
			Ks := []int{10, 100}
			L := []int{1, 2, 3, 4, 5, 10}
			for _, k := range Ks {
				fmt.Println("K\tL\trecall\t\tquerying")
				truths := testinghelpers.BuildTruths(queries_size, queries, vectors, k, ssdhelpers.L2)
				data := make([][]float32, len(L))
				for i, l := range L {
					l = l * k
					index.SetL(l)
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
					fmt.Printf("{%f,%f},\n", float32(querying.Microseconds())/float32(1000), recall)
				}
				results[fmt.Sprintf("Vamana - K: %d (R: %d, L: %d, alpha:%.1f)", k, paramR, paramL, paramAlpha)] = data
			}
		}
	}
	testinghelpers.ChartData("Recall Vs Latency", "", results, "index.html")
}

/*func TestBits(t *testing.T) {
	keysSource := make([]byte, 12500)
	keys := bitarray.NewBufferFromByteSlice(keysSource)
	keys.PutBitAt(3, 1)
	keys.PutBitAt(100, 1)
	values := []int{1, 3, 10, 100, 900}
	founds := []bool{false, true, false, true, false}
	for i := range values {
		b := keysSource[values[i]/8]
		index := byte(math.Pow(2, 8-float64(values[i]%8)-1))
		fmt.Printf("%08b", b)
		fmt.Println()
		fmt.Printf("%08b", index)
		fmt.Println()
		fmt.Printf("%08b", b&index)
		fmt.Println()
		require.Equal(t, b&index != 0, founds[i])
	}
}*/

/*
func TestBigDataMicrosoftVamana(t *testing.T) {
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

	paramsRs := []int{32}
	paramsLs := []int{50}
	alphas := []float32{1.2}
	results := make(map[string][][]float32, 0)

	for _, paramAlpha := range alphas {
		for paramIndex := range paramsRs {
			paramR := paramsRs[paramIndex]
			paramL := paramsLs[paramIndex]
			before = time.Now()
			index, _ := diskAnn.New(diskAnn.Config{
				R:     paramR,
				L:     paramL,
				Alpha: paramAlpha,
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[int(id)], nil
				},
				VectorsSize:        uint64(vectors_size),
				Distance:           ssdhelpers.L2,
				ClustersSize:       40,
				ClusterOverlapping: 2,
			})
			index.GraphFromDumpFile("data/index_sift_learn_R32_L50_A1.2.dump")
			fmt.Printf("Index built in: %s\n", time.Since(before))
			Ks := []int{10, 100}
			L := []int{10, 20, 30, 40, 50, 100, 200, 300, 400, 500, 1000}
			for _, k := range Ks {
				fmt.Println("K\tL\trecall\t\tquerying")
				truths := testinghelpers.BuildTruths(queries_size, queries, vectors, k, ssdhelpers.L2)
				data := make([][]float32, len(L))
				for i, l := range L {
					index.SetL(l)
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
					fmt.Printf("{%f,%f},\n", float32(querying.Microseconds())/float32(1000), recall)
				}
				results[fmt.Sprintf("Vamana - K: %d (R: %d, L: %d, alpha:%.1f)", k, paramR, paramL, paramAlpha)] = data
			}
		}
	}
	testinghelpers.ChartData("Recall Vs Latency", "", results, "index.html")
}*/
/*
func TestBigDataVamanaSharded(t *testing.T) {
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
		index, _ := diskAnn.New(diskAnn.Config{
			R:     paramR,
			L:     paramL,
			Alpha: paramAlpha,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			VectorsSize:        uint64(vectors_size),
			Distance:           ssdhelpers.L2,
			ClustersSize:       40,
			ClusterOverlapping: 2,
		})
		index.BuildIndexSharded()
		fmt.Printf("Index built in: %s\n", time.Since(before))
		//Ks := [3]int{1, 10, 100}
		Ks := []int{10}
		fmt.Println("K\tL\trecall\t\tquerying")
		L := []int{10, 20, 30, 40, 50, 100}

		for _, k := range Ks {
			truths := testinghelpers.BuildTruths(queries_size, queries, vectors, k, ssdhelpers.L2)
			data := make([][]float32, len(L))
			for i, l := range L {
				index.SetL(l)
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
*/
/*
func TestBigDataHNSW(t *testing.T) {
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

	efc := 512
	ef := 256
	maxN := 128

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
	efs := []int{8, 16, 32, 64, 128, 256, 512}
	fmt.Println("ef	recall	querying")
	Ks := []int{10, 100}

	fmt.Printf("Index built in: %s\n", time.Since(before))
	for _, k := range Ks {
		truths := testinghelpers.BuildTruths(queries_size, queries, vectors, k, ssdhelpers.L2)
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
				relevant += testinghelpers.MatchesInLists(truths[i], results)
			}

			recall := float32(relevant) / float32(retrieved)
			fmt.Printf("{%f,%f},\n", float32(querying.Microseconds())/float32(1000), recall)
		}
	}
}*/

func TestCharts(t *testing.T) {
	results := make(map[string][][]float32, 0)
	results["Vamana-K10 (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{122.360001, 0.976800},
		{198.046997, 0.988700},
		{265.140015, 0.991200},
		{319.872986, 0.993100},
		{398.997009, 0.994000},
		{650.624023, 0.998300},
	}
	results["Vamana-K10 (R: 70, L: 125, alpha:1.2)"] = [][]float32{
		{187.643997, 0.990800},
		{325.036011, 0.997300},
		{439.959991, 0.998800},
		{534.960022, 0.999200},
		{631.174988, 0.999300},
		{1038.689941, 0.999800},
	}
	results["HNSW-K10 (efC: 512, ef: 256, maxN: 128)"] = [][]float32{
		{131.358994, 0.992600},
		{185.324005, 0.994800},
		{315.315002, 0.998300},
		{541.302979, 0.999300},
		{932.544983, 0.999800},
		{1650.616943, 0.999700},
		{2925.180908, 0.999600},
	}
	results["Vamana Microsoft K10 (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{74.86, 0.9500},
		{114.28, 0.9761},
		{151.50, 0.9852},
		{188.41, 0.9900},
		{223.06, 0.9927},
		{374.78, 0.9978},
	}
	results["Vamana Microsoft K10 (R: 70, L: 125, alpha:1.2)"] = [][]float32{
		{129.23, 0.9830},
		{202.26, 0.9947},
		{257.37, 0.9977},
		{324.01, 0.9988},
		{371.81, 0.9993},
		{635.79, 0.9999},
	}
	//testinghelpers.ChartData("Recall vs Latency", "", results, "line-10.html")

	//results = make(map[string][][]float32, 0)
	results["Vamana-100K (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{623.234009, 0.981700},
		{1126.630981, 0.996450},
		{1538.332031, 0.998550},
		{1922.119019, 0.999310},
		{2254.335938, 0.999620},
		{3950.362061, 0.999930},
	}
	results["Vamana-100K (R: 70, L: 125, alpha:1.2)"] = [][]float32{
		{1033.817017, 0.997660},
		{1727.614990, 0.999730},
		{2239.555908, 0.999910},
		{2699.837891, 0.999960},
		{3219.499023, 0.999970},
		{5116.891113, 0.999970},
	}
	results["HNSW-100K (efC: 512, ef: 256, maxN: 128)"] = [][]float32{
		{762.625977, 0.996920},
		{928.812988, 0.998500},
		{1637.084961, 0.999830},
		{2735.382080, 0.999950},
	}
	results["Vamana Microsoft K100 (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{451.03, 0.9795},
		{735.23, 0.9955},
		{991.78, 0.9984},
		{1229.70, 0.9993},
		{1456.79, 0.9996},
		{2483.10, 0.9999},
	}
	results["Vamana Microsoft K100 (R: 70, L: 125, alpha:1.2)"] = [][]float32{
		{754.86, 0.9969},
		{1150.70, 0.9996},
		{1510.88, 0.9999},
		{1875.45, 0.9999},
		{2178.63, 0.9999},
		{3550.24, 1.0000},
	}
	testinghelpers.ChartData("Recall vs Latency", "", results, "line-10-100.html")
}
