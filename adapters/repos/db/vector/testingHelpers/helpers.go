package testinghelpers

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"os"
	"sort"

	"github.com/fogleman/gg"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/go-echarts/go-echarts/v2/types"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/diskAnn"
	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
)

type DistanceFunction func([]float32, []float32) float32

func int32FromBytes(bytes []byte) int {
	return int(binary.LittleEndian.Uint32(bytes))
}

func float32FromBytes(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	float := math.Float32frombits(bits)
	return float
}

func readSiftFloat(file string, maxObjects int) [][]float32 {

	f, err := os.Open("sift/" + file)
	if err != nil {
		panic(errors.Wrap(err, "Could not open SIFT file"))
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		panic(errors.Wrap(err, "Could not get SIFT file properties"))
	}
	fileSize := fi.Size()
	if fileSize < 1000000 {
		panic("The file is only " + fmt.Sprint(fileSize) + " bytes long. Did you forgot to install git lfs?")
	}

	// The sift data is a binary file containing floating point vectors
	// For each entry, the first 4 bytes is the length of the vector (in number of floats, not in bytes)
	// which is followed by the vector data with vector length * 4 bytes.
	// |-length-vec1 (4bytes)-|-Vec1-data-(4*length-vector-1 bytes)-|-length-vec2 (4bytes)-|-Vec2-data-(4*length-vector-2 bytes)-|
	// The vector length needs to be converted from bytes to int
	// The vector data needs to be converted from bytes to float
	// Note that the vector entries are of type float but are integer numbers eg 2.0
	bytesPerF := 4
	vectorLengthFloat := 128
	objects := make([][]float32, maxObjects)
	vectorBytes := make([]byte, bytesPerF+vectorLengthFloat*bytesPerF)
	for i := 0; i >= 0; i++ {
		_, err = f.Read(vectorBytes)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		if int32FromBytes(vectorBytes[0:bytesPerF]) != vectorLengthFloat {
			panic("Each vector must have 128 entries.")
		}
		vectorFloat := []float32{}
		for j := 0; j < vectorLengthFloat; j++ {
			start := (j + 1) * bytesPerF // first bytesPerF are length of vector
			vectorFloat = append(vectorFloat, float32FromBytes(vectorBytes[start:start+bytesPerF]))
		}
		objects[i] = vectorFloat

		if i >= maxObjects-1 {
			break
		}
	}

	return objects
}

func ReadVecs(size int, dimensions int, queriesSize int) ([][]float32, [][]float32) {
	fmt.Printf("generating %d vectors...", size+queriesSize)
	vectors := readSiftFloat("sift_learn.fvecs", size)
	queries := readSiftFloat("sift_query.fvecs", queriesSize)
	fmt.Printf(" done\n")
	return vectors, queries
}

func bruteForce(vectors [][]float32, query []float32, k int, distance DistanceFunction) []uint64 {
	type distanceAndIndex struct {
		distance float32
		index    uint64
	}

	distances := make([]distanceAndIndex, len(vectors))

	for i, vec := range vectors {
		dist := distance(query, vec)
		distances[i] = distanceAndIndex{
			index:    uint64(i),
			distance: dist,
		}
	}

	sort.Slice(distances, func(a, b int) bool {
		return distances[a].distance < distances[b].distance
	})

	if len(distances) < k {
		k = len(distances)
	}

	out := make([]uint64, k)
	for i := 0; i < k; i++ {
		out[i] = distances[i].index
	}

	return out
}

func BuildTruths(queries_size int, queries [][]float32, vectors [][]float32, k int, distance DistanceFunction) [][]uint64 {
	fileName := fmt.Sprintf("./sift/sift_truths%d.gob", k)
	truths := make([][]uint64, queries_size)

	if _, err := os.Stat(fileName); err == nil {
		return loadTruths(fileName, queries_size, k)
	}

	for i, query := range queries {
		truths[i] = bruteForce(vectors, query, k, distance)
	}

	f, err := os.Create(fileName)
	if err != nil {
		panic(errors.Wrap(err, "Could not open file"))
	}

	defer f.Close()
	enc := gob.NewEncoder(f)
	err = enc.Encode(truths)
	if err != nil {
		panic(errors.Wrap(err, "Could not encode truths"))
	}
	return truths
}

func loadTruths(fileName string, queries_size int, k int) [][]uint64 {

	f, err := os.Open(fileName)
	if err != nil {
		panic(errors.Wrap(err, "Could not open truths file"))
	}
	defer f.Close()

	truths := make([][]uint64, queries_size)
	cDec := gob.NewDecoder(f)
	err = cDec.Decode(&truths)
	if err != nil {
		panic(errors.Wrap(err, "Could not decode truths"))
	}
	return truths
}

func PlotGraph(name string, edges [][]uint64, vectors [][]float32, w int, h int) {
	dc := gg.NewContext(w, h)
	dc.SetRGB(1, 1, 1)
	dc.Clear()
	dc.SetRGBA(0.3, 0.3, 0.3, 1)
	dc.SetLineWidth(1)
	for i := range edges {
		x := vectors[i]
		for j := range edges[i] {
			dc.DrawLine(float64(x[0]), float64(x[1]), float64(vectors[edges[i][j]][0]), float64(vectors[edges[i][j]][1]))
			dc.Stroke()
		}
	}
	dc.SavePNG(name)
}

func PlotGraphHighLighted(name string, edges [][]uint64, vectors [][]float32, w int, h int, entry uint64, levels int) {
	dc := gg.NewContext(w, h)
	dc.SetRGB(1, 1, 1)
	dc.Clear()
	dc.SetRGBA(0.3, 0.3, 0.3, 1)
	dc.SetLineWidth(1)
	l := make([][]uint64, levels)
	l[0] = []uint64{entry}
	for i := 1; i < levels; i++ {
		for _, x := range l[i-1] {
			for _, outNeighbor := range edges[x] {
				l[i] = append(l[i], outNeighbor)
			}
		}
	}
	colors := [][]float64{
		{1, 0, 0, 1},
		{0, 1, 0, 1},
		{0, 0, 1, 1},
		{1, 0, 1, 1},
		{0, 1, 1, 1},
	}
	for i := range edges {
		x := vectors[i]
		for j := range edges[i] {
			found := false
			for k := range l {
				if ssdhelpers.Contains(l[k], uint64(i)) {
					dc.SetRGBA(colors[k][0], colors[k][1], colors[k][2], colors[k][3])
					dc.SetLineWidth(float64(2))
					found = true
					break
				}
			}
			if !found {
				dc.SetRGBA(0.5, 0.5, 0.5, 1)
				dc.SetLineWidth(0.1)
			}
			dc.DrawLine(float64(x[0]), float64(x[1]), float64(vectors[edges[i][j]][0]), float64(vectors[edges[i][j]][1]))
			dc.Stroke()
		}
	}
	dc.SavePNG(name)
}

func Normalize(vectors [][]float32, w int) {
	size := len(vectors[0])
	min := make([]float32, size)
	max := make([]float32, size)
	for i := 0; i < size; i++ {
		min[i] = math.MaxFloat32
		max[i] = -math.MaxFloat32
	}
	for x := range vectors {
		for i := 0; i < size; i++ {
			if min[i] > vectors[x][i] {
				min[i] = vectors[x][i]
			}
			if max[i] < vectors[x][i] {
				max[i] = vectors[x][i]
			}
		}
	}
	for x := range vectors {
		for i := 0; i < size; i++ {
			vectors[x][i] = float32(w) * (vectors[x][i] - min[i]) / (max[i] - min[i])
		}
	}
}

func ChartData(title string, subTitle string, data map[string][][]float32, path string) {
	// create a new line instance
	line := charts.NewLine()

	// set some global options like Title/Legend/ToolTip or anything else
	line.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{
			Theme: types.ThemeInfographic,
		}),
		charts.WithTitleOpts(opts.Title{
			Title:    title,
			Subtitle: subTitle,
		}),
		charts.WithYAxisOpts(opts.YAxis{
			Min: "dataMin",
		}),
		charts.WithLegendOpts(opts.Legend{
			Show:   true,
			Align:  "right",
			Orient: "vertical",
			Right:  "10%",
			Bottom: "20%",
		}),
	)

	for key, value := range data {
		line.AddSeries(key, GenerateLineItemsFromArray(value))
	}
	line.SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))
	f, _ := os.Create(path)
	_ = line.Render(f)
}

func GenerateLineItemsFromArray(array [][]float32) []opts.LineData {
	items := make([]opts.LineData, len(array))
	for i, x := range array {
		items[i] = opts.LineData{Value: x}
	}
	return items
}

func MatchesInLists(control []uint64, results []uint64) uint64 {
	desired := map[uint64]struct{}{}
	for _, relevant := range control {
		desired[relevant] = struct{}{}
	}

	var matches uint64
	for _, candidate := range results {
		_, ok := desired[candidate]
		if ok {
			matches++
		}
	}

	return matches
}

func BuildVamana(R int, L int, alpha float32, VectorForIDThunk ssdhelpers.VectorForID, vectorsSize uint64, distance ssdhelpers.DistanceFunction, path string) *diskAnn.Vamana {
	completePath := fmt.Sprintf("%s/vamana-r%d-l%d-a%.1f", path, R, L, alpha)
	if _, err := os.Stat(completePath); err == nil {
		return diskAnn.VamanaFromDisk(completePath, VectorForIDThunk, distance)
	}
	os.Mkdir(completePath, os.ModePerm)

	index, _ := diskAnn.New(diskAnn.Config{
		R:                  R,
		L:                  L,
		Alpha:              alpha,
		VectorForIDThunk:   VectorForIDThunk,
		VectorsSize:        vectorsSize,
		Distance:           distance,
		ClustersSize:       40,
		ClusterOverlapping: 2,
	})

	index.BuildIndex()
	index.ToDisk(completePath)
	return index
}

func BuildVamanaSharded(R int, L int, alpha float32, VectorForIDThunk ssdhelpers.VectorForID, vectorsSize uint64, distance ssdhelpers.DistanceFunction, path string) *diskAnn.Vamana {
	completePath := fmt.Sprintf("%s/vamana-r%d-l%d-a%.1f", path, R, L, alpha)
	if _, err := os.Stat(completePath); err == nil {
		return diskAnn.VamanaFromDisk(completePath, VectorForIDThunk, distance)
	}

	index, _ := diskAnn.New(diskAnn.Config{
		R:                  R,
		L:                  L,
		Alpha:              alpha,
		VectorForIDThunk:   VectorForIDThunk,
		VectorsSize:        vectorsSize,
		Distance:           distance,
		ClustersSize:       40,
		ClusterOverlapping: 2,
	})

	index.BuildIndexSharded()
	index.ToDisk(completePath)
	return index
}
