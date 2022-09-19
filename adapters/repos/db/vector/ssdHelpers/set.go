package ssdhelpers

import (
	"context"
	"math"
)

type Set struct {
	bitSet      *BitSet
	items       []IndexAndDistance
	vectorForID VectorForID
	distance    DistanceFunction
	center      []float32
	capacity    int
	firstIndex  int
	last        int
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

func (s *Set) ReCenter(center []float32, k int) {
	s.center = center
	s.bitSet.Clean()
	for i := range s.items {
		s.items[i].distance = math.MaxFloat32
	}
}

func (s *Set) Add(x uint64) bool {
	if s.bitSet.ContainsAndAdd(x) {
		return false
	}
	vec, _ := s.vectorForID(context.Background(), x)
	dist := s.distance(vec, s.center)
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

func (s *Set) NotVisited() bool {
	return s.firstIndex < s.capacity-1
}

func (s *Set) Top() uint64 {
	s.items[s.firstIndex].visited = true
	x := s.items[s.firstIndex].index
	for s.firstIndex < s.capacity && s.items[s.firstIndex].visited {
		s.firstIndex++
	}
	return x
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
