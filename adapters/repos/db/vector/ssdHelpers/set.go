package ssdhelpers

import (
	"context"
	"math"
)

type Set struct {
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
	index    uint64
	distance float32
	visited  bool
}

func NewSet(capacity int, vectorForID VectorForID, distance DistanceFunction, center []float32, vectorSize int) *Set {
	s := Set{
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

func max(x int, y int) int {
	if x < y {
		return y
	}
	return x
}

func (s *Set) ReCenter(center []float32) {
	s.center = center
	s.bitSet.Clean()
	for i := range s.items {
		s.items[i].distance = math.MaxFloat32
	}
	if s.pq != nil {
		s.pq.CenterAt(center)
	}
}

func distanceForVector(s *Set, x uint64) float32 {
	vec, _ := s.vectorForID(context.Background(), x)
	return s.distance(vec, s.center)
}

func distanceForPQVector(s *Set, x uint64) float32 {
	vec := s.encondedVectors[x]
	return s.pq.Distance(vec)
}

func (s *Set) add(x uint64, distancer func(s *Set, x uint64) float32) bool {
	if s.bitSet.ContainsAndAdd(x) {
		return false
	}
	dist := distancer(s, x)
	if s.items[s.last].distance <= dist {
		return false
	}
	data := IndexAndDistance{
		index:    x,
		distance: dist,
		visited:  false,
	}

	index := s.insert(data)
	if index < s.firstIndex {
		s.firstIndex = index
	}
	return true
}

func (s *Set) AddPQVector(x uint64) bool {
	return s.add(x, distanceForPQVector)
}

func (s *Set) Add(x uint64) bool {
	return s.add(x, distanceForVector)
}

func (s *Set) insert(data IndexAndDistance) int {
	left := 0
	right := s.last

	if s.items[left].distance >= data.distance {
		copy(s.items[1:], s.items[:s.last-1])
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

func (s *Set) AddRange(indices []uint64) {
	for _, item := range indices {
		s.Add(item)
	}
}

func (s *Set) AddRangePQ(indices []uint64, cache map[uint64]*VectorWithNeighbors, bitSet *BitSet) {
	for _, item := range indices {
		found := bitSet.Contains(item)
		if found {
			vector, _ := cache[item]
			s.add(item, func(s *Set, x uint64) float32 {
				return s.distance(vector.Vector, s.center)
			})
		}
		s.AddPQVector(item)
	}
}

func (s *Set) SetPQ(encondedVectors [][]byte, pq *ProductQuantizer) {
	s.encondedVectors = encondedVectors
	s.pq = pq
}

func (s *Set) NotVisited() bool {
	return s.firstIndex < s.capacity-1
}

func (s *Set) Top() (uint64, int) {
	s.items[s.firstIndex].visited = true
	lastFirst := s.firstIndex
	x := s.items[s.firstIndex].index
	for s.firstIndex < s.capacity && s.items[s.firstIndex].visited {
		s.firstIndex++
	}
	return x, lastFirst
}

func (s *Set) TopN(n int) ([]uint64, []int) {
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

func (s *Set) ReSort(i int, vector []float32) {
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

func (s *Set) Elements(k int) []uint64 {
	size := min(s.capacity, k)
	res := make([]uint64, 0, size)
	for i := 0; i < size; i++ {
		res = append(res, s.items[i].index)
	}
	return res
}
