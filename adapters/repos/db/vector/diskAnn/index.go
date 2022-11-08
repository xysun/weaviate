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
	"bytes"
	"context"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
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

type VamanaData struct {
	SIndex          uint64 // entry point
	GraphID         string
	CachedEdges     map[uint64]*ssdhelpers.VectorWithNeighbors
	EncondedVectors [][]byte
	OnDisk          bool
	Ids             []uint64
	Vectors         [][]float32
	Mean            []float32

	//ToDo: Remove this fast please...
	tempId  uint64
	tempVec []float32
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
	getOutNeighbors  func(uint64) ([]uint64, []float32)
	setOutNeighbors  func(uint64, []uint64, []float32)
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
	index.getOutNeighbors = index.outNeighborsFromMemory
	index.setOutNeighbors = index.outNeighborsToMemory
	index.addRange = index.addRangeVectors
	index.beamSearchHolder = secuentialBeamSearch
	return index, nil
}

func buildVamana(R int, L int, C int, alpha float32, beamSize int, VectorForIDThunk ssdhelpers.VectorForID, vectorsSize uint64, distance ssdhelpers.DistanceFunction, completePath string, dimensions int, toDisk bool, segments int, centroids int) *Vamana {
	if _, err := os.Stat(completePath); err == nil {
		index := VamanaFromDisk(completePath, VectorForIDThunk, distance)
		index.SetCacheSize(C)
		index.SetBeamSize(beamSize)
		return index
	}
	err := os.Mkdir(completePath, os.ModePerm)
	if err != nil {
		panic(err)
	}
	index, _ := New(Config{
		VectorForIDThunk: VectorForIDThunk,
		Distance:         distance,
	},
		UserConfig{
			R:                  R,
			L:                  L,
			Alpha:              alpha,
			VectorsSize:        vectorsSize,
			ClustersSize:       40,
			ClusterOverlapping: 2,
			Dimensions:         dimensions,
			C:                  0,
			BeamSize:           beamSize,
			Path:               completePath,
			Segments:           segments,
			Centroids:          centroids,
		})
	index.config.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		if id == index.data.tempId {
			return index.data.tempVec, nil
		}
		return index.data.Vectors[id], nil
	}
	index.SetCacheSize(C)
	index.BuildIndex()
	if toDisk {
		index.SwitchGraphToDisk(fmt.Sprintf("%s.graph", completePath), segments, centroids)
	}
	index.ToDisk(completePath)
	return index
}

func (v *Vamana) SetCacheSize(size int) {
	v.userConfig.OriginalCacheSize = size
	v.userConfig.C = minInt(size, int(v.userConfig.VectorsSize))
}

func BuildVamana(R int, L int, C int, alpha float32, beamSize int, VectorForIDThunk ssdhelpers.VectorForID, vectorsSize uint64, distance ssdhelpers.DistanceFunction, path string, dimensions int, segments int, centroids int) *Vamana {
	completePath := fmt.Sprintf("%s/%d.vamana-r%d-l%d-a%.1f", path, vectorsSize, R, L, alpha)
	return buildVamana(R, L, C, alpha, beamSize, VectorForIDThunk, vectorsSize, distance, completePath, dimensions, false, segments, centroids)
}

func BuildDiskVamana(R int, L int, C int, alpha float32, beamSize int, VectorForIDThunk ssdhelpers.VectorForID, vectorsSize uint64, distance ssdhelpers.DistanceFunction, path string, dimensions int, segments int, centroids int) *Vamana {
	noDiskPath := fmt.Sprintf("%s/%d.vamana-r%d-l%d-a%.1f", path, vectorsSize, R, L, alpha)
	completePath := fmt.Sprintf("%s/Disk.%d.vamana-r%d-l%d-a%.1f", path, vectorsSize, R, L, alpha)
	if _, err := os.Stat(completePath); err == nil {
		index := VamanaFromDisk(completePath, VectorForIDThunk, distance)
		return index
	}
	if _, err := os.Stat(noDiskPath); err == nil {
		index := VamanaFromDisk(noDiskPath, VectorForIDThunk, distance)
		index.SwitchGraphToDisk(fmt.Sprintf("%s.graph", completePath), segments, centroids)
		os.Mkdir(completePath, 0o777)
		index.ToDisk(completePath)
		return index
	}
	return buildVamana(R, L, C, alpha, beamSize, VectorForIDThunk, vectorsSize, distance, completePath, dimensions, true, segments, centroids)
}

func (v *Vamana) SetBeamSize(size int) {
	v.userConfig.BeamSize = size
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
	ids, distances := v.greedySearchQuery(query, k)
	if v.data.Ids != nil {
		for i := 0; i < len(ids); i++ {
			ids[i] = v.data.Ids[ids[i]]
		}
	}
	return ids, distances, nil
}

func (v *Vamana) ToDisk(path string) {
	completePath := fmt.Sprintf("%s/%s", path, ConfigFileName)
	cmd := exec.Command("ls data")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Run()
	fConfig, err := os.Create(completePath)
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
	err = cEnc.Encode(v.userConfig)
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
		index.getOutNeighbors = index.OutNeighborsFromDisk
		index.setOutNeighbors = index.OutNeighborsToDisk
		index.addRange = index.addRangePQ
		index.graphFile, _ = os.Open(index.data.GraphID)
	} else {
		index.getOutNeighbors = index.outNeighborsFromMemory
		index.setOutNeighbors = index.OutNeighborsToDisk
		index.addRange = index.addRangeVectors
	}
	return index
}

func (v *Vamana) pass() {
	random_order := permutation(int(v.userConfig.VectorsSize))
	for i := range random_order {
		x := random_order[i]
		x64 := uint64(x)
		q := v.getVector(x64)
		_, visited, _ := v.greedySearchWithVisited(q, 1)
		elements := v.robustPrune(x64, visited)
		v.edges[x64] = elements
		n_out_i := v.edges[x]
		for j := range n_out_i {
			n_out_j := append(v.edges[n_out_i[j]], x64)
			if len(n_out_j) > v.userConfig.R {
				elements := v.robustPrune(n_out_i[j], n_out_j)
				v.edges[n_out_i[j]] = elements
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

func minInt(x int, y int) int {
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
		x := v.getVector(i)
		for j := 0; j < len(x); j++ {
			mean[j] += x[j]
		}
	}
	for j := 0; j < len(mean); j++ {
		mean[j] /= float32(v.userConfig.VectorsSize)
	}

	//ToDo: Not really helping like this
	ssdhelpers.Concurrently(v.userConfig.VectorsSize, func(_ uint64, i uint64, mutex *sync.Mutex) {
		x := v.getVector(i)
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

func (v *Vamana) greedySearch(x []float32, k int, allVisited []uint64, updateVisited func([]uint64, ...uint64) []uint64) ([]uint64, []uint64, []float32) {
	if v.userConfig.VectorsSize == 0 {
		return []uint64{}, []uint64{}, []float32{}
	}
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
	indices, distances := v.set.Elements(k)
	return indices, allVisited, distances
}

func (v *Vamana) greedySearchWithVisited(x []float32, k int) ([]uint64, []uint64, []float32) {
	return v.greedySearch(x, k, []uint64{v.data.SIndex}, func(source []uint64, elements ...uint64) []uint64 {
		return append(source, elements...)
	})
}

func (v *Vamana) greedySearchQuery(x []float32, k int) ([]uint64, []float32) {
	res, _, distances := v.greedySearch(x, k, nil, func(source []uint64, elements ...uint64) []uint64 {
		return nil
	})
	return res, distances
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
		neighbours[i], vectors[i] = v.getOutNeighbors(tops[i])
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
	neighbours, vector := v.getOutNeighbors(top)
	if vector != nil {
		v.set.ReSort(index, vector)
	}
	v.addRange(neighbours)
	visited = updateVisited(visited, neighbours...)
	return visited
}

func (v *Vamana) outNeighborsFromMemory(x uint64) ([]uint64, []float32) {
	if x >= v.userConfig.VectorsSize {
		return []uint64{}, nil
	}
	return v.edges[x], nil
}

func (v *Vamana) outNeighborsToMemory(x uint64, outneighbors []uint64, _ []float32) {
	v.edges[x] = outneighbors
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
	if x >= v.userConfig.VectorsSize {
		return []uint64{}, nil
	}
	cached, found := v.data.CachedEdges[x]
	if found {
		return cached.OutNeighbors, nil
	}
	return ssdhelpers.ReadGraphRowWithBinary(v.graphFile, x, v.userConfig.R, v.userConfig.Dimensions)
}

func (v *Vamana) OutNeighborsToDisk(x uint64, outneighbors []uint64, vector []float32) {
	cached, found := v.data.CachedEdges[x]
	if found {
		cached.OutNeighbors = outneighbors
		return
	}
	ssdhelpers.WriteRowToGraphWithBinary(v.graphFile, v.userConfig.VectorsSize, v.userConfig.R, v.userConfig.Dimensions, vector, outneighbors)
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

		vec := v.getVector(uint64(x))
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
	v.getOutNeighbors = v.OutNeighborsFromDisk
	v.setOutNeighbors = v.OutNeighborsToDisk
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
		if id == v.data.tempId {
			return v.data.tempVec, nil
		}
		return v.VectorFromDisk(id), nil
	}
	v.data.Vectors = nil
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
		x := v.getVector(vIndex)
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

func (v *Vamana) getVector(id uint64) []float32 {
	vector, err := v.config.VectorForIDThunk(context.Background(), id)
	if err != nil {
		panic(errors.Wrap(err, fmt.Sprintf("Could not fetch vector with id %d", id)))
	}
	return vector
}

func (v *Vamana) robustPrune(p uint64, visited []uint64) []uint64 {
	visitedSet := NewSet2()
	outneighbors, _ := v.getOutNeighbors(p)
	visitedSet.AddRange(visited).AddRange(outneighbors).Remove(p)
	qP := v.getVector(p)
	out := ssdhelpers.NewFullBitSet(int(v.userConfig.VectorsSize))
	for visitedSet.Size() > 0 {
		pMin := v.closest(qP, visitedSet)
		out.Add(pMin.index)
		qPMin := v.getVector(pMin.index)
		if out.Size() == v.userConfig.R {
			break
		}

		for _, x := range visitedSet.items {
			qX := v.getVector(x.index)
			if (v.userConfig.Alpha * v.config.Distance(qPMin, qX)) <= x.distance {
				visitedSet.Remove(x.index)
			}
		}
	}

	return out.Elements()
}

func (v *Vamana) closest(x []float32, set *Set2) *IndexAndDistance {
	var min float32 = math.MaxFloat32
	var indice *IndexAndDistance = nil
	for _, element := range set.items {
		distance := element.distance
		if distance == 0 {
			qi := v.getVector(element.index)
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
	outneighbors, vector := v.getOutNeighbors(id)
	outneighbors = append(outneighbors, neighbor)
	if len(outneighbors) > v.userConfig.R {
		outneighbors = v.robustPrune(id, outneighbors)
	}
	v.setOutNeighbors(id, outneighbors, vector)
}

func (v *Vamana) addVectorAndOutNeighbors(id uint64, vector []float32, outneighbors []uint64) {
	v.userConfig.VectorsSize++
	if v.data.Ids != nil {
		v.data.Ids = append(v.data.Ids, id)
	}

	if v.data.OnDisk {
		if v.userConfig.C < v.userConfig.OriginalCacheSize {
			v.data.CachedEdges[v.userConfig.VectorsSize-1] = &ssdhelpers.VectorWithNeighbors{Vector: vector, OutNeighbors: outneighbors}
			v.userConfig.C++
			return
		}

		ssdhelpers.WriteRowToGraphWithBinary(v.graphFile, v.userConfig.VectorsSize, v.userConfig.R, v.userConfig.Dimensions, vector, outneighbors)
		return
	}

	v.edges = append(v.edges, outneighbors)
	v.data.Vectors = append(v.data.Vectors, vector)
}

func (v *Vamana) updateEntryPointAfterAdd(vector []float32) {
	size := float32(v.userConfig.VectorsSize)
	for i := range v.data.Mean {
		v.data.Mean[i] = (v.data.Mean[i]*(size-1) + vector[i]) / size
	}
	v.SetL(3)
	indexes, _ := v.greedySearchQuery(v.data.Mean, 1)
	v.SetL(v.userConfig.L)
	v.data.SIndex = indexes[0]
}

func (v *Vamana) Add(id uint64, vector []float32) error {
	v.SetL(v.userConfig.L)
	v.data.tempId = id
	v.data.tempVec = vector
	//ToDo: should use position and not id...
	_, visited, _ := v.greedySearchWithVisited(vector, 1)
	outneighbors := v.robustPrune(id, visited)
	v.data.tempId = math.MaxUint64
	v.addVectorAndOutNeighbors(id, vector, outneighbors)
	for _, x := range outneighbors {
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
	vamanaUserConfig, ok := updated.(UserConfig)
	if !ok {
		return errors.Errorf("vamana vector index: config is not diskAnn.UserConfig: %T", updated)
	}
	if !i.data.OnDisk && vamanaUserConfig.OnDisk {
		i.SwitchGraphToDisk(fmt.Sprintf("%s.graph", i.userConfig.Path), i.userConfig.Segments, i.userConfig.Centroids)
		i.ToDisk(i.userConfig.Path)
	}
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
