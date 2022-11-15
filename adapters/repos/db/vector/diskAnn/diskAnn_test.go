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
	vectors_size := 100000
	queries_size := 1000
	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size)
	fmt.Printf("generating data took %s\n", time.Since(before))

	before = time.Now()
	index := diskAnn.BuildVamana(
		32,
		50,
		10,
		1.2,
		1,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		0,
		uint64(vectors_size),
		ssdhelpers.L2,
		"./data",
		dimensions,
		64,
		255,
	)
	index.BuildIndex()
	for id := 0; id < vectors_size; id++ {
		index.Add(uint64(id), vectors[id])
		if id%10000 == 0 {
			fmt.Println(id, time.Since(before))
		}
	}
	index.SwitchGraphToDisk("data/test.praph", 64, 255)
	fmt.Printf("Building the index took %s\n", time.Since(before))

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
		assert.True(t, recall > 0.99)
		assert.True(t, latency < 700)
		fmt.Println(recall, latency)
	}
}

/*
func generate_vecs(size int, dimensions int, width int) [][]float32 {
	vectors := make([][]float32, 0, size)
	for i := 0; i < size; i++ {
		v := make([]float32, 0, dimensions)
		for j := 0; j < dimensions; j++ {
			v = append(v, float32(width)*rand.Float32())
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

func TestBigDataVamana(t *testing.T) {
	rand.Seed(0)
	dimensions := 128
	vectors_size := 10000
	queries_size := 1000
	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size)

	//var vectors [][]float32 = nil
	//queries := testinghelpers.ReadQueries(dimensions, queries_size)

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
			index := diskAnn.BuildVamana(
				paramR,
				paramL,
				10000,
				paramAlpha,
				1,
				func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[int(id)], nil
				},
				uint64(0),
				uint64(vectors_size),
				ssdhelpers.L2,
				"./data",
				dimensions,
				64,
				255,
			)

			index.SetVectors(vectors)
			index.BuildIndex()

			fmt.Printf("Index built in: %s\n", time.Since(before))
			Ks := []int{10}
			L := []int{1, 2, 3, 4, 5, 10}
			for _, k := range Ks {
				fmt.Println("K\tL\trecall\t\tquerying")
				truths := testinghelpers.BuildTruths(queries_size, vectors_size, queries, vectors, k, ssdhelpers.L2)
				data := make([][]float32, len(L))
				for i, l := range L {
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

func TestVamanaAdd(t *testing.T) {
	fmt.Println("Vamana Add")
	rand.Seed(0)
	dimensions := 128
	vectors_size := 10000
	queries_size := 1000
	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size)
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
			index := diskAnn.BuildVamana(
				paramR,
				paramL,
				10,
				paramAlpha,
				1,
				func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[int(id)], nil
				},
				0,
				uint64(vectors_size),
				ssdhelpers.L2,
				"./data",
				dimensions,
				64,
				255,
			)
			index.BuildIndex()
			switchAt := vectors_size
			for id := 0; id < switchAt; id++ {
				index.Add(uint64(id), vectors[id])
				if id%1000 == 0 {
					fmt.Println(id, time.Since(before))
				}
			}
			fmt.Println("Moving index to disk")
			index.SwitchGraphToDisk("data/test.praph", 64, 255)
			for id := switchAt; id < len(vectors); id++ {
				index.Add(uint64(id), vectors[id])
				if id%10000 == 0 {
					fmt.Println(id, time.Since(before))
				}
			}

			fmt.Printf("Index built in: %s\n", time.Since(before))
			Ks := []int{10}
			L := []int{1, 2, 3, 4, 5, 10}
			for _, k := range Ks {
				fmt.Println("K\tL\trecall\t\tquerying")
				truths := testinghelpers.BuildTruths(queries_size, vectors_size, queries, vectors, k, ssdhelpers.L2)
				data := make([][]float32, len(L))
				for i, l := range L {
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
					queryingTime := float32(querying.Microseconds()) / float32(queries_size)
					data[i] = []float32{queryingTime, recall}
					fmt.Printf("{%f,%f},\n", queryingTime, recall)
				}
				results[fmt.Sprintf("Vamana - K: %d (R: %d, L: %d, alpha:%.1f)", k, paramR, paramL, paramAlpha)] = data
			}
		}
	}
	testinghelpers.ChartData("Recall Vs Latency", "", results, "index.html")
}

func TestChartsRestrictedMemory(t *testing.T) {
	results := make(map[string][][]float32, 0)
	results["Disk.2GB.1M.Vamana-K10(32,50)"] = [][]float32{
		{958.698975, 0.699500},
		{890.101990, 0.844000},
		{945.184998, 0.891800},
		{1014.202026, 0.916600},
		{1133.063965, 0.931000},
		{2724.816895, 0.957900},
	}
	results["Disk.2GB.1M.Vamana-K100(32,50)"] = [][]float32{
		{1150.121948, 0.871120},
		{3620.985107, 0.968070},
		{3391.447998, 0.972440},
		{3595.562988, 0.974460},
		{4122.500977, 0.974390},
		{7248.335938, 0.974750},
	}
	results["Disk.2GB.1M.Vamana-K10(50,125)"] = [][]float32{
		{1009.401001, 0.772700},
		{932.911011, 0.908700},
		{1053.316040, 0.939700},
		{1177.852051, 0.952700},
		{1249.359985, 0.959800},
		{3003.294922, 0.969100},
	}
	results["Disk.2GB.1M.Vamana-K100(50,125)"] = [][]float32{
		{1420.853027, 0.889250},
		{4558.092773, 0.980460},
		{4426.825195, 0.981100},
		{4676.366211, 0.981700},
		{5181.709961, 0.981190},
		{9026.903320, 0.981180},
	}
	results["Disk.1GB.1M.Vamana-K10(32,50)"] = [][]float32{
		{935.927979, 0.699500},
		{889.596008, 0.844000},
		{1007.203003, 0.891800},
		{1104.713989, 0.916600},
		{1314.854004, 0.931000},
		{3068.589111, 0.957900},
	}
	results["Disk.1GB.1M.Vamana-K100(32,50)"] = [][]float32{
		{1472.922974, 0.871120},
		{5393.076172, 0.968070},
		{7859.795898, 0.972440},
		{7809.741211, 0.974460},
		{8225.103516, 0.974390},
		{17993.191406, 0.974750},
	}
	results["Disk.1GB.1M.Vamana-K10(50,125)"] = [][]float32{
		{1052.354004, 0.772700},
		{995.994019, 0.908700},
		{1095.563965, 0.939700},
		{1184.362061, 0.952700},
		{1285.172974, 0.959800},
		{3175.681885, 0.969100},
	}
	results["Disk.1GB.1M.Vamana-K100(50,125)"] = [][]float32{
		{1412.458008, 0.889250},
		{4831.715820, 0.980460},
		{8369.609375, 0.981100},
		{10225.232422, 0.981700},
		{13136.087891, 0.981190},
		{25727.447266, 0.981180},
	}

	results["Memory.2GB.1M.Vamana-K10(32,50)"] = [][]float32{
		{166.973007, 0.732300},
		{259.264008, 0.852400},
		{340.123993, 0.906000},
		{399.195007, 0.935100},
		{472.648010, 0.952000},
		{827.448975, 0.982600},
	}
	results["Memory.2GB.1M.Vamana-K100(32,50)"] = [][]float32{
		{837.482971, 0.938120},
		{1458.152954, 0.980280},
		{2023.520020, 0.990530},
		{2573.649902, 0.994850},
		{3150.767090, 0.996970},
		{5741.962891, 0.999270},
	}
	results["Memory.2GB.1M.Vamana-K10(50,125)"] = [][]float32{
		{231.764999, 0.819800},
		{346.630005, 0.919600},
		{458.490997, 0.956300},
		{579.164001, 0.973000},
		{695.955017, 0.980500},
		{1182.359009, 0.994500},
	}
	results["Memory.2GB.1M.Vamana-K100(50,125)"] = [][]float32{
		{1146.046997, 0.975550},
		{1996.694946, 0.994800},
		{2747.449951, 0.998030},
		{3474.520020, 0.999060},
		{4247.509766, 0.999410},
		{7222.888184, 0.999790},
	}
	results["Memory.1GB.1M.Vamana-K10(32,50)"] = [][]float32{
		{4714.607910, 0.732300},
		{1050.614014, 0.852400},
		{875.713013, 0.906000},
		{774.049988, 0.935100},
		{857.372986, 0.952000},
		{1660.234009, 0.982600},
	}
	results["Memory.1GB.1M.Vamana-K100(32,50)"] = [][]float32{
		{837.515015, 0.938120},
		{1937.188965, 0.980280},
		{2323.085938, 0.990530},
		{2755.389893, 0.994850},
		{3156.101074, 0.996970},
		{5737.317871, 0.999270},
	}

	results["Disk.64GB.1M.Vamana-K10(32,50)"] = [][]float32{
		{524.119019, 0.699500},
		{505.002991, 0.844000},
		{586.656006, 0.891800},
		{705.440002, 0.916600},
		{741.166016, 0.931000},
	}
	results["Disk.64GB.1M.Vamana-K100(32,50)"] = [][]float32{
		{933.088013, 0.871120},
		{1708.057983, 0.968070},
		{2139.823975, 0.972440},
		{2672.676025, 0.974460},
		{3225.975098, 0.974390},
		{5846.970215, 0.974750},
	}
	results["Disk.64GB.1M.Vamana-K10(50,125)"] = [][]float32{
		{415.709015, 0.772700},
		{538.377991, 0.908700},
		{947.497009, 0.939700},
		{1023.247009, 0.952700},
		{940.630981, 0.959800},
		{1666.770020, 0.969100},
	}
	results["Disk.64GB.1M.Vamana-K100(50,125)"] = [][]float32{
		{1189.365967, 0.889250},
		{2363.684082, 0.980460},
		{3025.362061, 0.981100},
		{4080.071045, 0.981700},
		{4859.623047, 0.981190},
		{7256.229980, 0.981180},
	}

	results["Memory.64GB.1M.Vamana-K10(32,50)"] = [][]float32{
		{148.899002, 0.732300},
		{210.649994, 0.852400},
		{278.335999, 0.906000},
		{338.220001, 0.935100},
		{404.811005, 0.952000},
		{712.327026, 0.982600},
	}
	results["Memory.64GB.1M.Vamana-K100(32,50)"] = [][]float32{
		{703.241028, 0.938120},
		{1240.748047, 0.980280},
		{1736.916992, 0.990530},
		{2219.573975, 0.994850},
		{2712.528076, 0.996970},
		{5059.583008, 0.999270},
	}
	results["Memory.64GB.1M.Vamana-K10(50,125)"] = [][]float32{
		{204.182999, 0.819800},
		{315.498993, 0.919600},
		{397.196991, 0.956300},
		{486.312988, 0.973000},
		{585.039978, 0.980500},
		{1016.247009, 0.994500},
	}
	results["Memory.64GB.1M.Vamana-K100(50,125)"] = [][]float32{
		{1017.151978, 0.975550},
		{1801.399048, 0.994800},
		{2465.585938, 0.998030},
		{3105.491943, 0.999060},
		{3777.148926, 0.999410},
		{6629.953125, 0.999790},
	}
	testinghelpers.ChartData("Recall vs Latency (restricted memory)", "", results, "local-memory.html")
}

func initClass(url string) {
	payload := strings.NewReader(`{
        "class": "Article",
        "description": "A written text, for example a news article or blog post",
        "properties": [
            {
            "dataType": [
                "string"
            ],
            "description": "Title of the article",
            "name": "title"
            },
            {
            "dataType": [
                "text"
            ],
            "description": "The content of the article",
            "name": "content"
            }
        ],
        "vectorIndexType": "vamana",
        "vectorIndexConfig": {
          "radius": 50,
		  "list": 125,
          "path": "vamana",
          "segments": 64,
          "centroids": 255,
          "dimensions": 128,
		  "vectorIndexCache": 10
        }
    }`)

	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/%s", url, "schema"), payload)
	req.Header.Add("content-type", "application/json")
	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()

	body, _ := ioutil.ReadAll(res.Body)

	fmt.Println(string(body))
}

func sendVector(url string, vector []float32) string {
	j, _ := json.Marshal(vector)
	payload := strings.NewReader(fmt.Sprintf("%s%s%s",
		`{
			"class": "Article",
			"vector": `, j,
		`}`))

	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/%s", url, "objects"), payload)
	req.Header.Add("content-type", "application/json")
	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()

	body, _ := ioutil.ReadAll(res.Body)

	return string(body)
}

func sendSwitchToDisk(url string) {
	payload := strings.NewReader(`{
        "class": "Article",
		"description": "A written text, for example a news article or blog post",
        "properties": [
            {
            "dataType": [
                "string"
            ],
            "description": "Title of the article",
            "name": "title"
            },
            {
            "dataType": [
                "text"
            ],
            "description": "The content of the article",
            "name": "content"
            }
        ],
        "vectorIndexType": "vamana",
        "vectorIndexConfig": {
          "disk": true
        }
    }`)

	req, _ := http.NewRequest("PUT", fmt.Sprintf("%s/%s", url, "schema/Article"), payload)
	req.Header.Add("content-type", "application/json")
	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()

	body, _ := ioutil.ReadAll(res.Body)

	fmt.Println(string(body))
}

func TestFixtures(t *testing.T) {
	url := "http://localhost:8080/v1"

	initClass(url)

	vectors_size := 1000000
	queries_size := 1000
	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size)
	fmt.Println(queries[0])
	fmt.Printf("generating data took %s\n", time.Since(before))
	switchAt := 100001
	before = time.Now()
	for id := 0; id < switchAt; id++ {
		sendVector(url, vectors[id])
		if id%10000 == 0 {
			fmt.Println(id, time.Since(before))
		}
	}
	fmt.Printf("sending data (in memory) took %s\n", time.Since(before))
	before = time.Now()
	sendSwitchToDisk(url)
	fmt.Printf("switching to disk took %s\n", time.Since(before))
	before = time.Now()
	for id := switchAt; id < len(vectors); id++ {
		sendVector(url, vectors[id])
		if id%10000 == 0 {
			fmt.Println(id, time.Since(before))
		}
	}
	fmt.Printf("sending data (on disk) took %s\n", time.Since(before))
}

func TestChartsDisk(t *testing.T) {
	results := make(map[string][][]float32, 0)

	results["Disk.64GB.1M.Vamana-K10(32,50)"] = [][]float32{
		{524.119019, 0.699500},
		{505.002991, 0.844000},
		{586.656006, 0.891800},
		{705.440002, 0.916600},
		{741.166016, 0.931000},
	}
	results["Disk.64GB.1M.Vamana-K100(32,50)"] = [][]float32{
		{933.088013, 0.871120},
		{1708.057983, 0.968070},
		{2139.823975, 0.972440},
		{2672.676025, 0.974460},
		{3225.975098, 0.974390},
		{5846.970215, 0.974750},
	}
	results["Disk.64GB.1M.Vamana-K10(50,125)"] = [][]float32{
		{415.709015, 0.772700},
		{538.377991, 0.908700},
		{947.497009, 0.939700},
		{1023.247009, 0.952700},
		{940.630981, 0.959800},
		{1666.770020, 0.969100},
	}
	results["Disk.64GB.1M.Vamana-K100(50,125)"] = [][]float32{
		{1189.365967, 0.889250},
		{2363.684082, 0.980460},
		{3025.362061, 0.981100},
		{4080.071045, 0.981700},
		{4859.623047, 0.981190},
		{7256.229980, 0.981180},
	}

	results["Disk.64GB.1M.Vamana-K10(32,50, threshold=10000)"] = [][]float32{
		{603.606995, 0.882200},
		{768.734985, 0.931900},
		{879.161011, 0.943500},
		{991.455017, 0.950100},
		{1065.780029, 0.956300},
		{1510.677002, 0.963300},
	}
	results["Disk.64GB.1M.Vamana-K100(32,50, threshold=10000)"] = [][]float32{
		{1502.506958, 0.900370},
		{2218.360107, 0.971370},
		{2646.385986, 0.973430},
		{3303.989990, 0.974700},
		{3640.055908, 0.974450},
		{6395.688965, 0.974740},
	}
	results["Disk.64GB.1M.Vamana-K10(50,125, threshold=10000)"] = [][]float32{
		{2695.073975, 0.908200},
		{1559.921997, 0.957800},
		{1571.666992, 0.964700},
		{2082.822021, 0.968200},
		{2426.698975, 0.969300},
		{2879.564941, 0.971600},
	}
	results["Disk.64GB.1M.Vamana-K100(50,125, threshold=10000)"] = [][]float32{
		{2333.837891, 0.903680},
		{3651.132080, 0.980980},
		{5187.723145, 0.981080},
		{5390.121094, 0.981640},
		{4536.372070, 0.981200},
		{7607.637207, 0.981180},
	}

	results["Disk.64GB.1M.Vamana-K10(32,50, threshold=15000)"] = [][]float32{
		{1185.734985, 0.920800},
		{1484.084961, 0.955900},
		{1750.005981, 0.960200},
		{1953.381958, 0.965000},
		{2090.652100, 0.964900},
		{2797.216064, 0.967500},
	}
	results["Disk.64GB.1M.Vamana-K100(32,50, threshold=15000)"] = [][]float32{
		{2800.680908, 0.919020},
		{3672.093994, 0.973810},
		{4434.779785, 0.974070},
		{5002.068848, 0.974980},
		{5717.979980, 0.974620},
		{9949.947266, 0.974760},
	}
	results["Disk.64GB.1M.Vamana-K10(50,125, threshold=15000)"] = [][]float32{
		{4093.736084, 0.926100},
		{2723.572998, 0.969400},
		{3927.895020, 0.971400},
		{3736.239014, 0.972800},
		{3505.020996, 0.972400},
		{3523.193115, 0.973300},
	}
	results["Disk.64GB.1M.Vamana-K100(50,125, threshold=15000)"] = [][]float32{
		{3597.377930, 0.912250},
		{5632.202148, 0.981360},
		{6353.460938, 0.981030},
		{5622.622070, 0.981690},
		{6221.294922, 0.981210},
		{9096.052734, 0.981180},
	}

	results["Disk.64GB.1M.Vamana-K10(32,50, threshold=5000)"] = [][]float32{
		{441.346985, 0.819600},
		{1002.466003, 0.893900},
		{770.200989, 0.917800},
		{668.468994, 0.930200},
		{1011.739990, 0.942300},
		{1647.251953, 0.959700},
	}
	results["Disk.64GB.1M.Vamana-K100(32,50, threshold=5000)"] = [][]float32{
		{1109.219971, 0.881540},
		{1984.061035, 0.968940},
		{2545.472900, 0.972710},
		{2926.709961, 0.974510},
		{3473.985107, 0.974440},
		{6289.577148, 0.974750},
	}
	results["Disk.64GB.1M.Vamana-K10(50,125, threshold=5000)"] = [][]float32{
		{1148.510986, 0.866000},
		{956.859985, 0.934900},
		{965.244995, 0.952100},
		{1066.087036, 0.961100},
		{1084.676025, 0.963700},
		{2549.709961, 0.970100},
	}
	results["Disk.64GB.1M.Vamana-K100(50,125, threshold=5000)"] = [][]float32{
		{2005.084961, 0.895960},
		{3545.111084, 0.980830},
		{5041.490234, 0.981100},
		{4937.492188, 0.981660},
		{5336.137207, 0.981190},
		{7525.289062, 0.981180},
	}

	results["Memory.64GB.1M.Vamana-K10(32,50)"] = [][]float32{
		{148.899002, 0.732300},
		{210.649994, 0.852400},
		{278.335999, 0.906000},
		{338.220001, 0.935100},
		{404.811005, 0.952000},
		{712.327026, 0.982600},
	}
	results["Memory.64GB.1M.Vamana-K100(32,50)"] = [][]float32{
		{703.241028, 0.938120},
		{1240.748047, 0.980280},
		{1736.916992, 0.990530},
		{2219.573975, 0.994850},
		{2712.528076, 0.996970},
		{5059.583008, 0.999270},
	} /*
		results["Memory.64GB.1M.Vamana-K10(50,125)"] = [][]float32{
			{204.182999, 0.819800},
			{315.498993, 0.919600},
			{397.196991, 0.956300},
			{486.312988, 0.973000},
			{585.039978, 0.980500},
			{1016.247009, 0.994500},
		}
		results["Memory.64GB.1M.Vamana-K100(50,125)"] = [][]float32{
			{1017.151978, 0.975550},
			{1801.399048, 0.994800},
			{2465.585938, 0.998030},
			{3105.491943, 0.999060},
			{3777.148926, 0.999410},
			{6629.953125, 0.999790},
		}
	testinghelpers.ChartData("Recall vs Latency (restricted memory)", "", results, "local-threshold.html")
}

func TestBigDataHNSW(t *testing.T) {
	rand.Seed(0)
	vectors_size := 10000
	queries_size := 1000
	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size)
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
	fmt.Println(workerCount)

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
	Ks := []int{10}

	fmt.Printf("Index built in: %s\n", time.Since(before))
	for _, k := range Ks {
		truths := testinghelpers.BuildTruths(queries_size, vectors_size, queries, vectors, k, ssdhelpers.L2)
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
	results["1M.Vamana-K10 (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{140.128006, 0.731900},
		{205.098007, 0.856200},
		{271.778015, 0.908600},
		{382.928009, 0.937400},
		{392.040009, 0.954200},
		{706.038025, 0.982600},
	}
	results["1M.Vamana-K10 DISK(m=32) (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{214.785004, 0.731900},
		{335.592987, 0.856200},
		{435.425995, 0.908600},
		{525.208984, 0.937400},
		{616.948975, 0.954200},
		{1066.010010, 0.982600},
		{1890.437988, 0.994000},
		{2640.787109, 0.996900},
		{3430.990967, 0.997800},
		{4033.707031, 0.998100},
	}
	results["1M.Vamana-K100 (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{742.294983, 0.937950},
		{1284.087036, 0.980260},
		{1710.437012, 0.990690},
		{2233.701904, 0.995160},
		{2710.023926, 0.997030},
		{4901.999023, 0.999360},
	}
	results["1M.Vamana-K100 DISK(m=32) (R: 32, L: 50, alpha:1.2)"] = [][]float32{
		{1111.874023, 0.937950},
		{2007.961060, 0.980260},
		{2688.414062, 0.990690},
		{3424.183105, 0.995160},
		{4358.198242, 0.997030},
		{7563.219238, 0.999360},
		{13339.875000, 0.999820},
		{19001.220703, 0.999850},
		{24828.673828, 0.999880},
		{31713.359375, 0.999890},
	}
	results["1M.Vamana-K10 (R: 50, L: 125, alpha:1.2)"] = [][]float32{
		{228.947006, 0.849900},
		{352.932007, 0.938100},
		{470.498993, 0.969100},
		{578.221008, 0.981500},
		{685.950989, 0.988600},
		{1233.038940, 0.997200},
	}
	results["1M.Vamana-K10 DISK(m=32) (R: 50, L: 125, alpha:1.2)"] = [][]float32{
		{369.119995, 0.849900},
		{558.973999, 0.938100},
		{760.851013, 0.969100},
		{964.197021, 0.981500},
		{1144.796021, 0.988600},
		{1886.180054, 0.997200},
		{3494.604980, 0.999200},
		{4676.979004, 0.999400},
		{5871.167969, 0.999400},
		{6872.892090, 0.999400},
	}
	results["1M.Vamana-K100 (R: 50, L: 125, alpha:1.2)"] = [][]float32{
		{1236.319946, 0.984760},
		{2215.521973, 0.997360},
		{2850.907959, 0.999100},
		{3603.865967, 0.999570},
		{4287.932129, 0.999730},
		{7401.032227, 0.999880},
	}
	results["1M.Vamana-K100 DISK(m=32) (R: 50, L: 125, alpha:1.2)"] = [][]float32{
		{1939.338013, 0.984760},
		{3287.219971, 0.997360},
		{4758.981934, 0.999100},
		{5873.312012, 0.999570},
		{6931.570801, 0.999730},
		{12284.960938, 0.999880},
		{21644.734375, 0.999890},
		{30160.632812, 0.999890},
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

func TestChartsHighlighted(t *testing.T) {
	rand.Seed(0)
	dimensions := 2
	vectors_size := 1000
	width := 1024
	before := time.Now()
	vectors := generate_vecs(vectors_size, dimensions, width)
	if vectors == nil {
		panic("Error generating vectors")
	}
	fmt.Printf("generating data took %s\n", time.Since(before))

	paramR := 4
	paramL := 8
	before = time.Now()
	index := diskAnn.BuildVamana(
		paramR,
		paramL,
		1.2,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		uint64(vectors_size),

		ssdhelpers.L2,
		"./data",
	)
	index.BuildIndex()
	testinghelpers.PlotGraph("Vamana_0.png", index.GetGraph(), vectors, width, width)
	testinghelpers.PlotGraphHighLightedBold("Vamana_3.png", index.GetGraph(), vectors, width, width, index.GetEntry(), 3)
	testinghelpers.PlotGraphHighLightedBold("Vamana_6.png", index.GetGraph(), vectors, width, width, index.GetEntry(), 6)
	testinghelpers.PlotGraphHighLightedBold("Vamana_9.png", index.GetGraph(), vectors, width, width, index.GetEntry(), 9)
}
*/
