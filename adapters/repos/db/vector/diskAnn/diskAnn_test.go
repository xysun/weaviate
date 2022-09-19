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
	"runtime"
	"sync"
	"time"

	"testing"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	testinghelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/testingHelpers"
	"github.com/stretchr/testify/require"
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

/*
func TestBigDataVamana(t *testing.T) {
	rand.Seed(0)
	dimensions := 128
	vectors_size := 1000000
	queries_size := 1000
	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, dimensions, queries_size)
	if vectors == nil {
		panic("Error generating vectors")
	}
	fmt.Printf("generating data took %s\n", time.Since(before))

	paramsRs := []int{32, 70}
	paramsLs := []int{50, 125}
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
			//index.SwitchGraphToDiskWithBinary("data/graphs/")
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
					adds, hits, truncs := 0, 0, 0
					for i := 0; i < len(queries); i++ {
						before = time.Now()
						results := index.SearchByVector(queries[i], k)
						a, h, t := index.Stats()
						adds += a
						hits += h
						truncs += t
						querying += time.Since(before)
						retrieved += k
						relevant += testinghelpers.MatchesInLists(truths[i], results)
					}

					recall := float32(relevant) / float32(retrieved)
					queryingTime := float32(querying.Microseconds()) / 1000
					data[i] = []float32{queryingTime, recall}
					fmt.Printf("{%f,%f},\n", float32(querying.Microseconds())/float32(1000), recall)
					fmt.Printf("%d nodes visited. %f rate of hits. %f rate of truncs\n", adds, float32(hits)/float32(adds), float32(truncs)/float32(adds))
				}
				results[fmt.Sprintf("Vamana - K: %d (R: %d, L: %d, alpha:%.1f)", k, paramR, paramL, paramAlpha)] = data
			}
		}
	}
	testinghelpers.ChartData("Recall Vs Latency", "", results, "index.html")
}*/

func TestBigDataHNSW(t *testing.T) {
	rand.Seed(0)
	dimensions := 128
	vectors_size := 1000000
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
}

func TestChartsLocally(t *testing.T) {
	results := make(map[string][][]float32, 0)
	results["Vamana-K10 (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{76.458000, 0.976800},
		{114.477997, 0.988700},
		{148.255997, 0.991200},
		{182.470001, 0.993100},
		{219.266006, 0.994000},
		{427.165009, 0.998300},
	}
	results["1M.Vamana-K10 DISK (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{167.820007, 0.731900},
		{257.269989, 0.856200},
		{363.251007, 0.908600},
		{406.160004, 0.937400},
		{487.117004, 0.954200},
		{851.056030, 0.982600},
	}
	results["1M.Vamana-K100 DISK (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{812.659973, 0.937950},
		{1439.386963, 0.980260},
		{2040.751953, 0.990690},
		{2628.572998, 0.995160},
		{3202.738037, 0.997030},
		{5751.185059, 0.999360},
	}
	results["1M.Vamana-K10 (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{186.328003, 0.731900},
		{216.947998, 0.856200},
		{284.690002, 0.908600},
		{347.972992, 0.937400},
		{412.359009, 0.954200},
		{721.843018, 0.982600},
	}
	results["1M.Vamana-K100 (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{721.943970, 0.937950},
		{1275.021973, 0.980260},
		{1780.459961, 0.990690},
		{2247.707031, 0.995160},
		{2735.287109, 0.997030},
		{4829.875000, 0.999360},
	}
	results["Vamana-K10 DISK (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{89.390999, 0.976900},
		{135.847000, 0.988700},
		{180.169006, 0.991200},
		{255.389999, 0.993100},
		{260.484985, 0.994000},
		{495.574005, 0.998300},
	}
	results["1M.Vamana-K15 (R: 35, L: 60, alpha:1.2)"] = [][]float32{
		{190.666000, 0.802333},
		{298.526001, 0.905867},
		{400.162994, 0.943467},
		{506.438995, 0.962333},
		{614.580994, 0.974067},
		{1065.156982, 0.991800},
	}
	results["1M.Vamana-K15 DISK (R: 35, L: 60, alpha:1.2)"] = [][]float32{
		{223.477005, 0.802333},
		{362.477997, 0.905867},
		{451.963013, 0.943467},
		{574.362976, 0.962333},
		{701.539978, 0.974067},
		{1212.021973, 0.991800},
	}
	results["1M.Vamana-K10 DISK (R: 50, L: 125, alpha:1.2)"] = [][]float32{
		{260.501007, 0.849900},
		{422.815002, 0.938100},
		{542.075989, 0.969100},
		{669.241028, 0.981500},
		{799.059998, 0.988600},
		{1441.484009, 0.997200},
	}
	results["1M.Vamana-K100 DISK (R: 50, L: 125, alpha:1.2)"] = [][]float32{
		{1420.660034, 0.984760},
		{2451.729980, 0.997360},
		{3282.085938, 0.999100},
		{4132.586914, 0.999570},
		{5065.460938, 0.999730},
		{8682.079102, 0.999880},
	}
	results["1M.Vamana-K10 (R: 50, L: 125, alpha:1.2)"] = [][]float32{
		{238.514008, 0.849900},
		{401.175995, 0.938100},
		{486.173004, 0.969100},
		{603.901978, 0.981500},
		{712.685974, 0.988600},
		{1238.598022, 0.997200},
	}
	results["1M.Vamana-K100 (R: 50, L: 125, alpha:1.2)"] = [][]float32{
		{1247.623047, 0.984760},
		{2132.617920, 0.997360},
		{2922.035889, 0.999100},
		{3645.793945, 0.999570},
		{4339.534180, 0.999730},
		{7477.674805, 0.999880},
	}
	results["1M.HNSW-K10"] = [][]float32{
		{286.450989, 0.818700},
		{367.898010, 0.891400},
		{596.833984, 0.960200},
		{1010.138977, 0.989400},
		{1806.718994, 0.997400},
		{3131.382080, 0.999200},
		{5428.570801, 0.999100},
	}
	results["1M.HNSW-K100"] = [][]float32{
		{1538.776001, 0.977220},
		{1614.432983, 0.977220},
		{1484.279053, 0.977220},
		{1473.899048, 0.977220},
		{1758.787964, 0.986950},
		{3275.989990, 0.997930},
		{5649.340820, 0.999690},
	}
	testinghelpers.ChartData("Recall vs Latency", "", results, "local-10.html")
}

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
