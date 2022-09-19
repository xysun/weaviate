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
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
)

type Vamana struct {
	config Config // configuration

	s_index      uint64     // entry point
	edges        [][]uint64 // edges on the graph
	set          ssdhelpers.Set
	outNeighbors func(uint64) ([]uint64, []float32)
	graphID      string
	graphFile    *os.File
}

const ConfigFileName = "cfg.gob"
const EntryFileName = "entry.gob"
const GraphFileName = "graph.gob"

func New(config Config) (*Vamana, error) {
	index := &Vamana{
		config: config,
	}
	index.set = *ssdhelpers.NewSet(config.L, config.VectorForIDThunk, config.Distance, nil, int(config.VectorsSize))
	index.outNeighbors = index.outNeighborsFromMemory
	return index, nil
}

func BuildVamana(R int, L int, alpha float32, VectorForIDThunk ssdhelpers.VectorForID, vectorsSize uint64, distance ssdhelpers.DistanceFunction, path string) *Vamana {
	completePath := fmt.Sprintf("%s/%d.vamana-r%d-l%d-a%.1f", path, vectorsSize, R, L, alpha)
	if _, err := os.Stat(completePath); err == nil {
		return VamanaFromDisk(completePath, VectorForIDThunk, distance)
	}

	index, _ := New(Config{
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

func (v *Vamana) BuildIndexSharded() {
	if v.config.ClustersSize == 1 {
		v.BuildIndex()
		return
	}

	cluster := ssdhelpers.New(v.config.ClustersSize, v.config.Distance, v.config.VectorForIDThunk, int(v.config.VectorsSize))
	cluster.Partition()
	shards := make([][]uint64, v.config.ClustersSize)
	for i := 0; i < int(v.config.VectorsSize); i++ {
		i64 := uint64(i)
		vec, _ := v.config.VectorForIDThunk(context.Background(), i64)
		c := cluster.NNearest(vec, v.config.ClusterOverlapping)
		for j := 0; j < v.config.ClusterOverlapping; j++ {
			shards[c[j]] = append(shards[c[j]], i64)
		}
	}

	vectorForIDThunk := v.config.VectorForIDThunk
	vectorsSize := v.config.VectorsSize
	shardedGraphs := make([][][]uint64, v.config.ClustersSize)

	ssdhelpers.Concurrently(uint64(len(shards)), func(workerId, taskIndex uint64, mutex *sync.Mutex) {
		config := Config{
			R:     v.config.R,
			L:     v.config.L,
			Alpha: v.config.Alpha,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectorForIDThunk(ctx, shards[taskIndex][id])
			},
			VectorsSize:        uint64(len(shards[taskIndex])),
			Distance:           v.config.Distance,
			ClustersSize:       v.config.ClustersSize,
			ClusterOverlapping: v.config.ClusterOverlapping,
		}

		index, _ := New(config)
		index.BuildIndex()
		shardedGraphs[taskIndex] = index.edges
	})

	v.config.VectorForIDThunk = vectorForIDThunk
	v.config.VectorsSize = vectorsSize
	v.s_index = v.medoid()
	v.edges = make([][]uint64, v.config.VectorsSize)
	for shardIndex, shard := range shards {
		for connectionIndex, connection := range shardedGraphs[shardIndex] {
			for _, outNeighbor := range connection {
				mappedOutNeighbor := shard[outNeighbor]
				if !ssdhelpers.Contains(v.edges[shard[connectionIndex]], mappedOutNeighbor) {
					v.edges[shard[connectionIndex]] = append(v.edges[shard[connectionIndex]], mappedOutNeighbor)
				}
			}
		}
	}
	for edgeIndex := range v.edges {
		if len(v.edges[edgeIndex]) > v.config.R {
			if len(v.edges[edgeIndex]) > v.config.R {
				rand.Shuffle(len(v.edges[edgeIndex]), func(x int, y int) {
					temp := v.edges[edgeIndex][x]
					v.edges[edgeIndex][x] = v.edges[edgeIndex][y]
					v.edges[edgeIndex][y] = temp
				})
				//Meet the R constrain after merging
				//Take a random subset with the appropriate size. Implementation idea from Microsoft reference code
				v.edges[edgeIndex] = v.edges[edgeIndex][:v.config.R]
			}
		}
	}
}

func (v *Vamana) BuildIndex() {
	v.edges = v.makeRandomGraph()
	v.s_index = v.medoid()
	alpha := v.config.Alpha
	v.config.Alpha = 1
	v.pass() //Not sure yet what did they mean in the paper with two passes... Two passes is exactly the same as only the last pass to the best of my knowledge.
	v.config.Alpha = alpha
	v.pass()
}

func (v *Vamana) GetGraph() [][]uint64 {
	return v.edges
}

func (v *Vamana) GetEntry() uint64 {
	return v.s_index
}

func (v *Vamana) SetL(L int) {
	v.config.L = L
	v.set = *ssdhelpers.NewSet(L, v.config.VectorForIDThunk, v.config.Distance, nil, int(v.config.VectorsSize))
}

func (v *Vamana) SearchByVector(query []float32, k int) []uint64 {
	return v.greedySearchQuery(query, k)
}

func (v *Vamana) ToDisk(path string) {
	fConfig, err := os.Create(fmt.Sprintf("%s/%s", path, ConfigFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not create config file"))
	}
	fEntry, err := os.Create(fmt.Sprintf("%s/%s", path, EntryFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not create entry point file"))
	}
	fGraph, err := os.Create(fmt.Sprintf("%s/%s", path, GraphFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not create graph file"))
	}
	defer fConfig.Close()
	defer fEntry.Close()
	defer fGraph.Close()

	cEnc := gob.NewEncoder(fConfig)
	err = cEnc.Encode(v.config)
	if err != nil {
		panic(errors.Wrap(err, "Could not encode config"))
	}

	eEnc := gob.NewEncoder(fEntry)
	err = eEnc.Encode(v.s_index)
	if err != nil {
		panic(errors.Wrap(err, "Could not encode entry point"))
	}

	gEnc := gob.NewEncoder(fGraph)
	err = gEnc.Encode(v.edges)
	if err != nil {
		panic(errors.Wrap(err, "Could not encode graph"))
	}
}

func (v *Vamana) GraphFromDumpFile(filePath string) {
	f, err := os.Open(filePath)
	if err != nil {
		panic(errors.Wrap(err, "Unable to read input file "+filePath))
	}
	defer f.Close()
	csvReader := csv.NewReader(f)
	csvReader.FieldsPerRecord = -1
	records, err := csvReader.ReadAll()
	if err != nil {
		panic(errors.Wrap(err, "Unable to parse file as CSV for "+filePath))
	}
	v.edges = make([][]uint64, v.config.VectorsSize)
	for r, row := range records {
		v.edges[r] = make([]uint64, len(row)-1)
		for j, element := range row {
			if j == len(row)-1 {
				break
			}
			v.edges[r][j] = str2uint64(element)
		}
	}
}

func str2uint64(str string) uint64 {
	str = strings.Trim(str, " ")
	i, _ := strconv.ParseInt(str, 10, 64)
	return uint64(i)
}

func VamanaFromDisk(path string, VectorForIDThunk ssdhelpers.VectorForID, distance ssdhelpers.DistanceFunction) *Vamana {
	fConfig, err := os.Open(fmt.Sprintf("%s/%s", path, ConfigFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not open config file"))
	}
	fEntry, err := os.Open(fmt.Sprintf("%s/%s", path, EntryFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not open entry point file"))
	}
	fGraph, err := os.Open(fmt.Sprintf("%s/%s", path, GraphFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not open graph file"))
	}
	defer fConfig.Close()
	defer fEntry.Close()
	defer fGraph.Close()

	var config Config
	cDec := gob.NewDecoder(fConfig)
	err = cDec.Decode(&config)
	config.Dimensions = 128
	if err != nil {
		panic(errors.Wrap(err, "Could not decode config"))
	}

	index, err := New(config)

	eDec := gob.NewDecoder(fEntry)
	err = eDec.Decode(&index.s_index)
	if err != nil {
		panic(errors.Wrap(err, "Could not decode config"))
	}

	gDec := gob.NewDecoder(fGraph)
	err = gDec.Decode(&index.edges)
	if err != nil {
		panic(errors.Wrap(err, "Could not decode config"))
	}
	index.config.VectorForIDThunk = VectorForIDThunk
	index.config.Distance = distance
	return index
}

func (v *Vamana) pass() {
	random_order := permutation(int(v.config.VectorsSize))
	for i := range random_order {
		x := random_order[i]
		x64 := uint64(x)
		q, err := v.config.VectorForIDThunk(context.Background(), x64)
		if err != nil {
			panic(errors.Wrap(err, fmt.Sprintf("Could not fetch vector with id %d", x64)))
		}
		_, visited := v.greedySearch(q, 1)
		v.robustPrune(x64, visited)
		n_out_i := v.edges[x]
		for j := range n_out_i {
			n_out_j := append(v.edges[n_out_i[j]], x64)
			if len(n_out_j) > v.config.R {
				v.robustPrune(n_out_i[j], n_out_j)
			} else {
				v.edges[n_out_i[j]] = n_out_j
			}
		}
	}
}

func min(x uint64, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

func (v *Vamana) makeRandomGraph() [][]uint64 {
	edges := make([][]uint64, v.config.VectorsSize)
	ssdhelpers.Concurrently(v.config.VectorsSize, func(workerID uint64, i uint64, mutex *sync.Mutex) {
		edges[i] = make([]uint64, v.config.R)
		for j := 0; j < v.config.R; j++ {
			edges[i][j] = rand.Uint64() % (v.config.VectorsSize - 1)
			if edges[i][j] >= i { //avoid connecting with itself
				edges[i][j]++
			}
		}
	})
	return edges
}

func (v *Vamana) medoid() uint64 {
	var min_dist float32 = math.MaxFloat32
	min_index := uint64(0)

	mean := make([]float32, v.config.VectorsSize)
	for i := uint64(0); i < v.config.VectorsSize; i++ {
		x, err := v.config.VectorForIDThunk(context.Background(), i)
		if err != nil {
			panic(errors.Wrap(err, fmt.Sprintf("Could not fetch vector with id %d", i)))
		}
		for j := 0; j < len(x); j++ {
			mean[j] += x[j]
		}
	}
	for j := 0; j < len(mean); j++ {
		mean[j] /= float32(v.config.VectorsSize)
	}

	//ToDo: Not really helping like this
	ssdhelpers.Concurrently(v.config.VectorsSize, func(workerID uint64, i uint64, mutex *sync.Mutex) {
		x, err := v.config.VectorForIDThunk(context.Background(), i)
		if err != nil {
			panic(errors.Wrap(err, fmt.Sprintf("Could not fetch vector with id %d", i)))
		}
		dist := v.config.Distance(x, mean)
		mutex.Lock()
		if dist < min_dist {
			min_dist = dist
			min_index = uint64(i)
		}
		mutex.Unlock()
	})
	return min_index
}

func permutation(n int) []int {
	permutation := make([]int, n)
	for i := range permutation {
		permutation[i] = i
	}
	for i := 0; i < 2*n; i++ {
		x := rand.Intn(n)
		y := rand.Intn(n)
		z := permutation[x]
		permutation[x] = permutation[y]
		permutation[y] = z
	}
	return permutation
}

func (v *Vamana) greedySearch(x []float32, k int) ([]uint64, []uint64) {
	v.set.ReCenter(x, k)
	v.set.Add(v.s_index)
	allVisited := []uint64{v.s_index}
	for v.set.NotVisited() {
		nn := v.set.Top()
		v.set.AddRange(v.edges[nn])
		allVisited = append(allVisited, nn)
	}
	return v.set.Elements(k), allVisited
}

func (v *Vamana) greedySearchQuery(x []float32, k int) []uint64 {
	v.set.ReCenter(x, k)
	v.set.Add(v.s_index)
	for v.set.NotVisited() {
		neighbours, _ := v.outNeighbors(v.set.Top())
		v.set.AddRange(neighbours)
	}
	return v.set.Elements(k)
}

func (v *Vamana) outNeighborsFromMemory(x uint64) ([]uint64, []float32) {
	vector, _ := v.config.VectorForIDThunk(context.Background(), x)
	return v.edges[x], vector
}

func (v *Vamana) OutNeighborsFromDiskWithBinary(x uint64) ([]uint64, []float32) {
	return ssdhelpers.ReadGraphRowWithBinary(v.graphFile, x, v.config.R, v.config.Dimensions)
}

func (v *Vamana) OutNeighborsFromDisk(x uint64) ([]uint64, []float32) {
	panic("Not implemented yet...")
}

func (v *Vamana) SwitchGraphToDisk(path string) {
	v.graphID = path + uuid.New().String() + ".graph"
	ssdhelpers.DumpGraphToDisk(v.graphID, v.edges, v.config.R, v.config.VectorForIDThunk)
	v.outNeighbors = v.OutNeighborsFromDisk
	v.edges = nil
	v.graphFile, _ = os.Open(v.graphID)
}

func (v *Vamana) SwitchGraphToDiskWithBinary(path string) {
	v.graphID = path + uuid.New().String() + ".graph"
	ssdhelpers.DumpGraphToDiskWithBinary(v.graphID, v.edges, v.config.R, v.config.VectorForIDThunk, v.config.Dimensions)
	v.outNeighbors = v.OutNeighborsFromDiskWithBinary
	v.edges = nil
	v.graphFile, _ = os.Open(v.graphID)
}

func elementsFromMap(set map[uint64]struct{}) []uint64 {
	res := make([]uint64, len(set))
	i := 0
	for x := range set {
		res[i] = x
		i++
	}
	return res
}

func (v *Vamana) robustPrune(p uint64, visited []uint64) {
	visitedSet := NewSet2()
	visitedSet.AddRange(visited).AddRange(v.edges[p]).Remove(p)
	qP, err := v.config.VectorForIDThunk(context.Background(), p)
	if err != nil {
		panic(err)
	}
	out := ssdhelpers.NewFullBitSet(int(v.config.VectorsSize))
	for visitedSet.Size() > 0 {
		pMin := v.closest(qP, visitedSet)
		out.Add(pMin.index)
		qPMin, err := v.config.VectorForIDThunk(context.Background(), pMin.index)
		if err != nil {
			panic(errors.Wrap(err, fmt.Sprintf("Could not fetch vector with id %d", pMin.index)))
		}
		if out.Size() == v.config.R {
			break
		}

		for _, x := range visitedSet.items {
			qX, err := v.config.VectorForIDThunk(context.Background(), x.index)
			if err != nil {
				panic(errors.Wrap(err, fmt.Sprintf("Could not fetch vector with id %d", x.index)))
			}
			if (v.config.Alpha * v.config.Distance(qPMin, qX)) <= x.distance {
				visitedSet.Remove(x.index)
			}
		}
	}
	v.edges[p] = out.Elements()
}

func (v *Vamana) closest(x []float32, set *Set2) *IndexAndDistance {
	var min float32 = math.MaxFloat32
	var indice *IndexAndDistance = nil
	for _, element := range set.items {
		distance := element.distance
		if distance == 0 {
			qi, err := v.config.VectorForIDThunk(context.Background(), element.index)
			if err != nil {
				panic(errors.Wrap(err, fmt.Sprintf("Could not fetch vector with id %d", element.index)))
			}
			distance = v.config.Distance(qi, x)
			element.distance = distance
		}
		if min > distance {
			min = distance
			indice = element
		}
	}
	return indice
}
