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

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	"github.com/semi-technologies/weaviate/entities/schema"
)

type Stats struct {
	Hops int
}

type Vertex struct {
	Id     uint64
	Vector []float32
}

type VamanaData struct {
	SIndex          uint64 // entry point
	GraphID         string
	CachedEdges     map[uint64]*ssdhelpers.VectorWithNeighbors
	EncondedVectors [][]byte
	OnDisk          bool
	Vertices        []Vertex
	Mean            []float32
}

type Vamana struct {
	config     Config // configuration
	userConfig UserConfig
	data       VamanaData

	cachedBitMap     *ssdhelpers.BitSet
	edges            [][]uint64 // edges on the graph
	set              ssdhelpers.Set
	graphFile        *os.File
	pq               *ssdhelpers.ProductQuantizer
	outNeighbors     func(uint64) ([]uint64, []float32)
	addRange         func([]uint64)
	beamSearchHolder func(*Vamana, []uint64, func([]uint64, ...uint64) []uint64) []uint64
}

const ConfigFileName = "cfg.gob"
const DataFileName = "data.gob"
const GraphFileName = "graph.gob"

func New(config Config, userConfig UserConfig) (*Vamana, error) {
	index := &Vamana{
		config:     config,
		userConfig: userConfig,
	}
	index.set = *ssdhelpers.NewSet(userConfig.L, config.VectorForIDThunk, config.Distance, nil, int(userConfig.VectorsSize))
	index.outNeighbors = index.outNeighborsFromMemory
	index.addRange = index.addRangeVectors
	index.beamSearchHolder = secuentialBeamSearch
	return index, nil
}

func BuildVamana(R int, L int, alpha float32, VectorForIDThunk ssdhelpers.VectorForID, vectorsSize uint64, distance ssdhelpers.DistanceFunction, path string) *Vamana {
	completePath := fmt.Sprintf("%s/%d.vamana-r%d-l%d-a%.1f", path, vectorsSize, R, L, alpha)
	if _, err := os.Stat(completePath); err == nil {
		return VamanaFromDisk(completePath, VectorForIDThunk, distance)
	}

	index, _ := New(Config{
		VectorForIDThunk: VectorForIDThunk,
		Distance:         distance,
	}, UserConfig{
		R:                  R,
		L:                  L,
		Alpha:              alpha,
		ClustersSize:       40,
		ClusterOverlapping: 2,
		VectorsSize:        vectorsSize,
	})

	os.Mkdir(path, os.ModePerm)

	index.BuildIndex()
	index.ToDisk(completePath)
	index.beamSearchHolder = secuentialBeamSearch
	return index
}

func minInt(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

func (v *Vamana) SetCacheSize(size int) {
	v.userConfig.C = minInt(size, int(v.userConfig.VectorsSize))
	v.userConfig.OriginalCacheSize = size
}

func (v *Vamana) SetBeamSize(size int) {
	v.userConfig.BeamSize = size
}

func (v *Vamana) BuildIndexSharded() {
	if v.userConfig.ClustersSize == 1 {
		v.BuildIndex()
		return
	}

	cluster := ssdhelpers.New(v.userConfig.ClustersSize, v.config.Distance, v.config.VectorForIDThunk, int(v.userConfig.VectorsSize), v.userConfig.Dimensions)
	cluster.Partition()
	shards := make([][]uint64, v.userConfig.ClustersSize)
	for i := 0; i < int(v.userConfig.VectorsSize); i++ {
		i64 := uint64(i)
		vec, _ := v.config.VectorForIDThunk(context.Background(), i64)
		c := cluster.NNearest(vec, v.userConfig.ClusterOverlapping)
		for j := 0; j < v.userConfig.ClusterOverlapping; j++ {
			shards[c[j]] = append(shards[c[j]], i64)
		}
	}

	vectorForIDThunk := v.config.VectorForIDThunk
	vectorsSize := v.userConfig.VectorsSize
	shardedGraphs := make([][][]uint64, v.userConfig.ClustersSize)

	ssdhelpers.Concurrently(uint64(len(shards)), func(_, taskIndex uint64, _ *sync.Mutex) {
		config := Config{
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectorForIDThunk(ctx, shards[taskIndex][id])
			},
			Distance: v.config.Distance,
		}
		userConfig := UserConfig{
			R:                  v.userConfig.R,
			L:                  v.userConfig.L,
			Alpha:              v.userConfig.Alpha,
			ClustersSize:       v.userConfig.ClustersSize,
			ClusterOverlapping: v.userConfig.ClusterOverlapping,
			VectorsSize:        uint64(len(shards[taskIndex])),
		}

		index, _ := New(config, userConfig)
		index.BuildIndex()
		shardedGraphs[taskIndex] = index.edges
	})

	v.config.VectorForIDThunk = vectorForIDThunk
	v.userConfig.VectorsSize = vectorsSize
	v.data.SIndex = v.medoid()
	v.edges = make([][]uint64, v.userConfig.VectorsSize)
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
		if len(v.edges[edgeIndex]) > v.userConfig.R {
			if len(v.edges[edgeIndex]) > v.userConfig.R {
				rand.Shuffle(len(v.edges[edgeIndex]), func(x int, y int) {
					temp := v.edges[edgeIndex][x]
					v.edges[edgeIndex][x] = v.edges[edgeIndex][y]
					v.edges[edgeIndex][y] = temp
				})
				//Meet the R constrain after merging
				//Take a random subset with the appropriate size. Implementation idea from Microsoft reference code
				v.edges[edgeIndex] = v.edges[edgeIndex][:v.userConfig.R]
			}
		}
	}
}

func (v *Vamana) BuildIndex() {
	v.data.Mean = make([]float32, v.userConfig.Dimensions)
	v.SetL(v.userConfig.L)
	v.edges = v.makeRandomGraph()
	v.data.SIndex = v.medoid()
	v.pass()
}

func (v *Vamana) GetGraph() [][]uint64 {
	return v.edges
}

func (v *Vamana) GetEntry() uint64 {
	return v.data.SIndex
}

func (v *Vamana) SetL(L int) {
	v.userConfig.L = L
	v.set = *ssdhelpers.NewSet(L, v.config.VectorForIDThunk, v.config.Distance, nil, int(v.userConfig.VectorsSize))
	v.set.SetPQ(v.data.EncondedVectors, v.pq)
}

func (v *Vamana) SearchByVector(query []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	// ids := make([]uint64, 2)
	// ids[0] = 0
	// ids[1] = 1

	distances := make([]float32, 2)
	distances[0] = 0.1
	distances[1] = 0.1
	return v.greedySearchQuery(query, k), distances, nil
	//return v.greedySearchQuery(query, k), nil, nil
}

func (v *Vamana) ToDisk(path string) {
	fConfig, err := os.Create(fmt.Sprintf("%s/%s", path, ConfigFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not create config file"))
	}
	fData, err := os.Create(fmt.Sprintf("%s/%s", path, DataFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not create entry point file"))
	}
	fGraph, err := os.Create(fmt.Sprintf("%s/%s", path, GraphFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not create graph file"))
	}
	defer fConfig.Close()
	defer fData.Close()
	defer fGraph.Close()

	cEnc := gob.NewEncoder(fConfig)
	err = cEnc.Encode(v.config)
	if err != nil {
		panic(errors.Wrap(err, "Could not encode config"))
	}

	dEnc := gob.NewEncoder(fData)
	err = dEnc.Encode(v.data)
	if err != nil {
		panic(errors.Wrap(err, "Could not encode data"))
	}

	gEnc := gob.NewEncoder(fGraph)
	err = gEnc.Encode(v.edges)
	if err != nil {
		panic(errors.Wrap(err, "Could not encode graph"))
	}

	v.pq.ToDisk(path)
	v.cachedBitMap.ToDisk(path)
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
	v.edges = make([][]uint64, v.userConfig.VectorsSize)
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
	fData, err := os.Open(fmt.Sprintf("%s/%s", path, DataFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not open entry point file"))
	}
	fGraph, err := os.Open(fmt.Sprintf("%s/%s", path, GraphFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not open graph file"))
	}
	defer fConfig.Close()
	defer fData.Close()
	defer fGraph.Close()

	var userConfig UserConfig
	cDec := gob.NewDecoder(fConfig)
	err = cDec.Decode(&userConfig)
	userConfig.Dimensions = 128
	if err != nil {
		panic(errors.Wrap(err, "Could not decode config"))
	}

	index, err := New(Config{}, userConfig)

	dDec := gob.NewDecoder(fData)
	err = dDec.Decode(&index.data)
	if err != nil {
		panic(errors.Wrap(err, "Could not decode data"))
	}

	gDec := gob.NewDecoder(fGraph)
	err = gDec.Decode(&index.edges)
	if err != nil {
		panic(errors.Wrap(err, "Could not decode edges"))
	}
	index.config.VectorForIDThunk = VectorForIDThunk
	index.config.Distance = distance
	if index.data.OnDisk && index.userConfig.BeamSize > 1 {
		index.beamSearchHolder = initBeamSearch
	} else {
		index.beamSearchHolder = secuentialBeamSearch
	}
	index.pq = ssdhelpers.PQFromDisk(path, VectorForIDThunk, distance)
	index.cachedBitMap = ssdhelpers.BitSetFromDisk(path)
	if index.data.OnDisk {
		index.outNeighbors = index.OutNeighborsFromDisk
		index.addRange = index.addRangePQ
		index.graphFile, _ = os.Open(index.data.GraphID)
	} else {
		index.outNeighbors = index.outNeighborsFromMemory
		index.addRange = index.addRangeVectors
	}
	return index
}

func (v *Vamana) pass() {
	random_order := permutation(int(v.userConfig.VectorsSize))
	for i := range random_order {
		x := random_order[i]
		x64 := uint64(x)
		q, err := v.config.VectorForIDThunk(context.Background(), x64)
		fmt.Println(q)
		if err != nil {
			panic(errors.Wrap(err, fmt.Sprintf("Could not fetch vector with id %d", x64)))
		}
		_, visited := v.greedySearchWithVisited(q, 1)
		v.robustPrune(x64, visited)
		n_out_i := v.edges[x]
		for j := range n_out_i {
			n_out_j := append(v.edges[n_out_i[j]], x64)
			if len(n_out_j) > v.userConfig.R {
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
	edges := make([][]uint64, v.userConfig.VectorsSize)
	ssdhelpers.Concurrently(v.userConfig.VectorsSize, func(_ uint64, i uint64, _ *sync.Mutex) {
		edges[i] = make([]uint64, v.userConfig.R)
		for j := 0; j < v.userConfig.R; j++ {
			edges[i][j] = rand.Uint64() % (v.userConfig.VectorsSize - 1)
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

	mean := make([]float32, v.userConfig.Dimensions)
	for i := uint64(0); i < v.userConfig.VectorsSize; i++ {
		x, err := v.config.VectorForIDThunk(context.Background(), i)
		if err != nil {
			panic(errors.Wrap(err, fmt.Sprintf("Could not fetch vector with id %d", i)))
		}
		for j := 0; j < len(x); j++ {
			mean[j] += x[j]
		}
	}
	for j := 0; j < len(mean); j++ {
		mean[j] /= float32(v.userConfig.VectorsSize)
	}

	//ToDo: Not really helping like this
	ssdhelpers.Concurrently(v.userConfig.VectorsSize, func(_ uint64, i uint64, mutex *sync.Mutex) {
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

func (v *Vamana) greedySearch(x []float32, k int, allVisited []uint64, updateVisited func([]uint64, ...uint64) []uint64) ([]uint64, []uint64) {
	v.set.ReCenter(x, v.data.OnDisk)
	if v.data.OnDisk {
		v.set.AddPQVector(v.data.SIndex, v.data.CachedEdges, v.cachedBitMap)
	} else {
		v.set.Add(v.data.SIndex)
	}

	//allVisited := []uint64{v.data.SIndex}
	for v.set.NotVisited() {
		allVisited = v.beamSearchHolder(v, allVisited, updateVisited)
	}
	if v.data.OnDisk && v.userConfig.BeamSize > 1 {
		v.beamSearchHolder = initBeamSearch
	}
	return v.set.Elements(k), allVisited
}

func (v *Vamana) greedySearchWithVisited(x []float32, k int) ([]uint64, []uint64) {
	return v.greedySearch(x, k, []uint64{v.data.SIndex}, func(source []uint64, elements ...uint64) []uint64 {
		return append(source, elements...)
	})
}

func (v *Vamana) greedySearchQuery(x []float32, k int) []uint64 {
	res, _ := v.greedySearch(x, k, nil, func(source []uint64, elements ...uint64) []uint64 {
		return nil
	})
	return res
}

func (v *Vamana) addRangeVectors(elements []uint64) {
	v.set.AddRange(elements)
}

func (v *Vamana) addRangePQ(elements []uint64) {
	v.set.AddRangePQ(elements, v.data.CachedEdges, v.cachedBitMap)
}

func initBeamSearch(v *Vamana, visited []uint64, updateVisited func([]uint64, ...uint64) []uint64) []uint64 {
	newVisited := secuentialBeamSearch(v, visited, updateVisited)
	v.beamSearchHolder = beamSearch
	return newVisited
}

func beamSearch(v *Vamana, visited []uint64, updateVisited func([]uint64, ...uint64) []uint64) []uint64 {
	tops, indexes := v.set.TopN(v.userConfig.BeamSize)
	neighbours := make([][]uint64, v.userConfig.BeamSize)
	vectors := make([][]float32, v.userConfig.BeamSize)
	ssdhelpers.Concurrently(uint64(len(tops)), func(_, i uint64, _ *sync.Mutex) {
		neighbours[i], vectors[i] = v.outNeighbors(tops[i])
	})
	for i := range indexes {
		if vectors[i] != nil {
			v.set.ReSort(indexes[i], vectors[i])
		}
		v.addRange(neighbours[i])
		visited = updateVisited(visited, neighbours[i]...)
	}
	return visited
}

func secuentialBeamSearch(v *Vamana, visited []uint64, updateVisited func([]uint64, ...uint64) []uint64) []uint64 {
	top, index := v.set.Top()
	neighbours, vector := v.outNeighbors(top)
	if vector != nil {
		v.set.ReSort(index, vector)
	}
	v.addRange(neighbours)
	visited = updateVisited(visited, neighbours...)
	return visited
}

func (v *Vamana) outNeighborsFromMemory(x uint64) ([]uint64, []float32) {
	return v.edges[x], nil
}

func (v *Vamana) VectorFromDisk(x uint64) []float32 {
	cached, found := v.data.CachedEdges[x]
	if found {
		return cached.Vector
	}
	_, vector := ssdhelpers.ReadGraphRowWithBinary(v.graphFile, x, v.userConfig.R, v.userConfig.Dimensions)
	return vector
}

func (v *Vamana) OutNeighborsFromDisk(x uint64) ([]uint64, []float32) {
	cached, found := v.data.CachedEdges[x]
	if found {
		return cached.OutNeighbors, nil
	}
	return ssdhelpers.ReadGraphRowWithBinary(v.graphFile, x, v.userConfig.R, v.userConfig.Dimensions)
}

func (v *Vamana) addToCacheRecursively(hops int, elements []uint64) {
	if hops <= 0 {
		return
	}

	newElements := make([]uint64, 0)
	for _, x := range elements {
		if hops <= 0 {
			return
		}
		found := v.cachedBitMap.ContainsAndAdd(x)
		if found {
			continue
		}
		hops--

		vec, _ := v.config.VectorForIDThunk(context.Background(), uint64(x))
		v.data.CachedEdges[x] = &ssdhelpers.VectorWithNeighbors{
			Vector:       vec,
			OutNeighbors: v.edges[x],
		}
		for _, n := range v.edges[x] {
			newElements = append(newElements, n)
		}
	}
	v.addToCacheRecursively(hops, newElements)
}

func (v *Vamana) SwitchGraphToDisk(path string, segments int, centroids int) {
	v.data.GraphID = path
	ssdhelpers.DumpGraphToDiskWithBinary(v.data.GraphID, v.edges, v.userConfig.R, v.config.VectorForIDThunk, v.userConfig.Dimensions)
	v.outNeighbors = v.OutNeighborsFromDisk
	v.data.CachedEdges = make(map[uint64]*ssdhelpers.VectorWithNeighbors, v.userConfig.C)
	v.cachedBitMap = ssdhelpers.NewBitSet(int(v.userConfig.VectorsSize))
	v.addToCacheRecursively(v.userConfig.C, []uint64{v.data.SIndex})
	v.edges = nil
	v.graphFile, _ = os.Open(v.data.GraphID)
	v.data.EncondedVectors = v.encondeVectors(segments, centroids)
	v.set.SetPQ(v.data.EncondedVectors, v.pq)
	v.addRange = v.addRangePQ
	v.data.OnDisk = true
	if v.userConfig.BeamSize > 1 {
		v.beamSearchHolder = initBeamSearch
	}
	v.config.VectorForIDThunk = func(_ context.Context, id uint64) ([]float32, error) {
		return v.VectorFromDisk(id), nil
	}
}

func (v *Vamana) encondeVectors(segments int, centroids int) [][]byte {
	v.pq = ssdhelpers.NewProductQunatizer(segments, centroids, v.config.Distance, v.config.VectorForIDThunk, v.userConfig.Dimensions, int(v.userConfig.VectorsSize))
	v.pq.Fit()
	enconded := make([][]byte, v.userConfig.VectorsSize)
	ssdhelpers.Concurrently(v.userConfig.VectorsSize, func(_ uint64, vIndex uint64, _ *sync.Mutex) {
		found := v.cachedBitMap.Contains(vIndex)
		if found {
			enconded[vIndex] = nil
			return
		}
		x, _ := v.config.VectorForIDThunk(context.Background(), vIndex)
		enconded[vIndex] = v.pq.Encode(x)
	})
	return enconded
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

func (v *Vamana) robustPrune(p uint64, visited []uint64) []uint64 {
	visitedSet := NewSet2()
	outneighbors, _ := v.outNeighbors(p)
	visitedSet.AddRange(visited).AddRange(outneighbors).Remove(p)
	qP, err := v.config.VectorForIDThunk(context.Background(), p)
	if err != nil {
		panic(err)
	}
	out := ssdhelpers.NewFullBitSet(int(v.userConfig.VectorsSize))
	for visitedSet.Size() > 0 {
		pMin := v.closest(qP, visitedSet)
		out.Add(pMin.index)
		qPMin, err := v.config.VectorForIDThunk(context.Background(), pMin.index)
		if err != nil {
			panic(errors.Wrap(err, fmt.Sprintf("Could not fetch vector with id %d", pMin.index)))
		}
		if out.Size() == v.userConfig.R {
			break
		}

		for _, x := range visitedSet.items {
			qX, err := v.config.VectorForIDThunk(context.Background(), x.index)
			if err != nil {
				panic(errors.Wrap(err, fmt.Sprintf("Could not fetch vector with id %d", x.index)))
			}
			if (v.userConfig.Alpha * v.config.Distance(qPMin, qX)) <= x.distance {
				visitedSet.Remove(x.index)
			}
		}
	}

	elements := out.Elements()
	v.updateOutNeighbors(p, elements)
	return elements
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

func (v *Vamana) addOutNeighbor(id uint64, neighbor uint64) {
	if v.data.OnDisk {
		cached, found := v.data.CachedEdges[id]
		if found {
			cached.OutNeighbors = append(cached.OutNeighbors, neighbor)
			if len(cached.OutNeighbors) > v.userConfig.R {
				v.robustPrune(id, cached.OutNeighbors)
			}
			return
		}
		outneighbors, vector := ssdhelpers.ReadGraphRowWithBinary(v.graphFile, id, v.userConfig.R, v.userConfig.Dimensions)
		outneighbors = append(outneighbors, neighbor)
		if len(outneighbors) > v.userConfig.R {
			v.robustPrune(id, outneighbors)
			return
		}
		ssdhelpers.WriteRowToGraphWithBinary(v.graphFile, v.userConfig.VectorsSize, v.userConfig.R, v.userConfig.Dimensions, vector, outneighbors)
	}

	v.edges[id] = append(v.edges[id], neighbor)
	if len(v.edges[id]) > v.userConfig.R {
		v.robustPrune(id, v.edges[id])
	}
}

func (v *Vamana) addVectorAndOutNeighbors(id uint64, vector []float32, outneighbors []uint64) {
	v.userConfig.VectorsSize++
	if v.data.OnDisk {
		if v.userConfig.C < v.userConfig.OriginalCacheSize {
			v.data.CachedEdges[v.userConfig.VectorsSize-1] = &ssdhelpers.VectorWithNeighbors{Vector: vector, OutNeighbors: outneighbors}
			v.userConfig.C++
			return
		}

		ssdhelpers.WriteRowToGraphWithBinary(v.graphFile, v.userConfig.VectorsSize, v.userConfig.R, v.userConfig.Dimensions, vector, outneighbors)
		return
	}

	v.data.Vertices = append(v.data.Vertices, Vertex{Id: id, Vector: vector})
	v.edges = append(v.edges, outneighbors)
}

func (v *Vamana) updateOutNeighbors(id uint64, outneighbors []uint64) {
	if v.data.OnDisk {
		cached, found := v.data.CachedEdges[id]
		if found {
			cached.OutNeighbors = outneighbors
			return
		}

		ssdhelpers.WriteOutNeighborsToGraphWithBinary(v.graphFile, id, v.userConfig.R, v.userConfig.Dimensions, outneighbors)
		return
	}
	v.edges[id] = outneighbors
}

func (v *Vamana) updateEntryPointAfterAdd(vector []float32) {
	size := float32(v.userConfig.VectorsSize)
	for i := range v.data.Mean {
		v.data.Mean[i] = (v.data.Mean[i]*(size-1) + vector[i]) / size
	}
	v.data.SIndex = v.greedySearchQuery(v.data.Mean, 1)[0]
}

func (v *Vamana) Add(id uint64, vector []float32) error {
	v.SetL(v.userConfig.L)
	//ToDo: should use position and not id...
	v.addVectorAndOutNeighbors(id, vector, make([]uint64, 0))
	_, visited := v.greedySearchWithVisited(vector, 1)
	v.robustPrune(id, visited)
	out, _ := v.outNeighbors(id)
	for _, x := range out {
		v.addOutNeighbor(x, id)
	}
	v.updateEntryPointAfterAdd(vector)
	return nil
}

func (i *Vamana) Delete(id uint64) error {
	// silently ignore
	return nil
}

func (i *Vamana) SearchByVectorDistance(vector []float32, dist float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, errors.Errorf("cannot vector-search on a class not vector-indexed")
}

func (i *Vamana) UpdateUserConfig(updated schema.VectorIndexConfig) error {
	i.config.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		return i.data.Vertices[id].Vector, nil
	}
	i.BuildIndex()
	return nil
}

func (i *Vamana) Drop(context.Context) error {
	// silently ignore
	return nil
}

func (i *Vamana) Flush() error {
	return nil
}

func (i *Vamana) Shutdown(context.Context) error {
	return nil
}

func (i *Vamana) PauseMaintenance(context.Context) error {
	return nil
}

func (i *Vamana) SwitchCommitLogs(context.Context) error {
	return nil
}

func (i *Vamana) ListFiles(context.Context) ([]string, error) {
	return nil, nil
}

func (i *Vamana) ResumeMaintenance(context.Context) error {
	return nil
}
