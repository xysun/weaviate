package ssdhelpers

import (
	"context"
	"math"
)

type SortedSet struct {
	bitSet          *BitSet
	items           []IndexAndDistance
	vectorForID     VectorForID
	distance        DistanceFunction
	center          []float32
	capacity        int
	firstIndex      int
	last            int
	encondedVectors [][]byte
	pq              *ProductQuantizer
}

type VectorWithNeighbors struct {
	Vector       []float32
	OutNeighbors []uint64
}

type IndexAndDistance struct {
	index      uint64
	distance   float32
	pqDistance float32
	visited    bool
	vector     []float32
}

func (r *IndexAndDistance) GetIndex() uint64 {
	return r.index
}

func (r *IndexAndDistance) GetDistance() float32 {
	return r.distance
}

func (r *IndexAndDistance) SetDistance(distance float32) {
	r.distance = distance
}

func (r *IndexAndDistance) GetVector() []float32 {
	return r.vector
}

func NewSortedSet(capacity int, vectorForID VectorForID, distance DistanceFunction, center []float32, vectorSize int) *SortedSet {
	s := SortedSet{
		items:       make([]IndexAndDistance, capacity),
		vectorForID: vectorForID,
		distance:    distance,
		center:      center,
		capacity:    capacity,
		firstIndex:  0,
		last:        capacity - 1,
		bitSet:      NewBitSet(vectorSize),
	}
	for i := range s.items {
		s.items[i].distance = math.MaxFloat32
	}
	return &s
}

func (s *SortedSet) ReCenter(center []float32, onDisk bool) {
	s.center = center
	s.bitSet.Clean()
	for i := range s.items {
		s.items[i].distance = math.MaxFloat32
		s.items[i].pqDistance = math.MaxFloat32
	}
	if s.pq != nil {
		s.pq.CenterAt(center)
	}
}

func distanceForVector(s *SortedSet, x uint64) float32 {
	vec, _ := s.vectorForID(context.Background(), x)
	return s.distance(vec, s.center)
}

func distanceForPQVector(s *SortedSet, x uint64) float32 {
	vec := s.encondedVectors[x]
	return s.pq.Distance(vec)
}

func (s *SortedSet) add(x uint64, distancer func(s *SortedSet, x uint64) float32) bool {
	if s.bitSet.ContainsAndAdd(x) {
		return false
	}
	dist := distancer(s, x)
	if s.items[s.last].distance <= dist {
		return false
	}
	data := IndexAndDistance{
		index:      x,
		distance:   dist,
		pqDistance: dist,
		visited:    false,
	}

	index := s.insert(data)
	if index < s.firstIndex {
		s.firstIndex = index
	}
	return true
}

func (s *SortedSet) AddPQVector(item uint64, cache map[uint64]*VectorWithNeighbors, bitSet *BitSet) bool {
	if bitSet == nil {
		return s.add(item, distanceForPQVector)
	}
	found := bitSet.Contains(item)
	if found {
		vector := cache[item]
		return s.add(item, func(s *SortedSet, x uint64) float32 {
			return s.distance(vector.Vector, s.center)
		})
	}
	return s.add(item, distanceForPQVector)
}

func (s *SortedSet) Add(x uint64) bool {
	return s.add(x, distanceForVector)
}

func (s *SortedSet) insert(data IndexAndDistance) int {
	left := 0
	right := s.last

	if s.items[left].distance >= data.distance {
		copy(s.items[1:], s.items[:s.last])
		s.items[left] = data
		return left
	}

	for right > 1 && left < right-1 {
		mid := (left + right) / 2
		if s.items[mid].distance > data.distance {
			right = mid
		} else {
			left = mid
		}
	}
	for left > 0 {
		if s.items[left].distance < data.distance {
			break
		}
		if s.items[left].index == data.index {
			return s.capacity
		}
		left--
	}
	copy(s.items[right+1:], s.items[right:])
	s.items[right] = data
	return right
}

func (s *SortedSet) AddRange(indices []uint64) {
	for _, item := range indices {
		s.Add(item)
	}
}

func (s *SortedSet) AddRangePQ(indices []uint64, cache map[uint64]*VectorWithNeighbors, bitSet *BitSet) {
	for _, item := range indices {
		found := bitSet.Contains(item)
		if found {
			vector := cache[item]
			s.add(item, func(s *SortedSet, x uint64) float32 {
				return s.distance(vector.Vector, s.center)
			})
		}
		s.AddPQVector(item, cache, bitSet)
	}
}

func (s *SortedSet) SetPQ(encondedVectors [][]byte, pq *ProductQuantizer) {
	s.encondedVectors = encondedVectors
	s.pq = pq
}

func (s *SortedSet) NotVisited() bool {
	return s.firstIndex < s.capacity
}

func (s *SortedSet) Top() (uint64, int) {
	if s.firstIndex < s.capacity {
		s.items[s.firstIndex].visited = true
		lastFirst := s.firstIndex
		x := s.items[s.firstIndex].index
		for s.firstIndex < s.capacity && s.items[s.firstIndex].visited {
			s.firstIndex++
		}
		return x, lastFirst
	}
	return math.MaxUint64, -1
}

func (s *SortedSet) TopN(n int) ([]uint64, []int) {
	tops, indexes := make([]uint64, 0, n), make([]int, 0, n)
	for i := 0; i < n; i++ {
		top, index := s.Top()
		tops = append(tops, top)
		indexes = append(indexes, index)
		if !s.NotVisited() {
			break
		}
	}
	return tops, indexes
}

func (s *SortedSet) ReSort(i int, vector []float32) {
	s.items[i].distance = s.distance(vector, s.center)
	if i > 0 && s.items[i].distance < s.items[i-1].distance {
		j := i - 1
		for j >= 0 && s.items[i].distance < s.items[j].distance {
			j--
		}
		if i-j == 1 {
			s.items[i], s.items[j] = s.items[j], s.items[i]
			return
		}
		data := s.items[i]
		copy(s.items[j+2:i+1], s.items[j+1:i])
		s.items[j+1] = data
	} else if i < len(s.items)-1 && s.items[i].distance > s.items[i+1].distance {
		j := i + 1
		for j >= 0 && s.items[i].distance < s.items[j].distance {
			j++
		}
		if j >= s.firstIndex && i+1 <= s.firstIndex {
			s.firstIndex--
		}
		if j-i == 1 {
			s.items[i], s.items[j] = s.items[j], s.items[i]
			return
		}
		data := s.items[i]
		copy(s.items[i:j-1], s.items[i+1:j])
		s.items[j-1] = data
	}
}

func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

func (s *SortedSet) Elements(k int) ([]uint64, []float32) {
	size := min(s.capacity, k)

	indices := make([]uint64, 0, size)
	distances := make([]float32, 0, size)
	for i := 0; i < size; i++ {
		indices = append(indices, s.items[i].index)
		distances = append(distances, s.items[i].distance)
	}
	return indices, distances
}
