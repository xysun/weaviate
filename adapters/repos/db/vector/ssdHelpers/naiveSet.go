package ssdhelpers

import "context"

type NaiveSet struct {
	items       []*IndexAndDistance
	center      uint64
	x           []float32
	vectorForID VectorForID
	distance    DistanceFunction
	current     int
	bitSet      *BitSet
	size        int
}

func NewNaiveSet(center uint64, vectorForID VectorForID, distance DistanceFunction, capacity int) *NaiveSet {
	set := &NaiveSet{
		items:       make([]*IndexAndDistance, 0),
		center:      center,
		vectorForID: vectorForID,
		distance:    distance,
		current:     0,
		bitSet:      NewBitSet(capacity),
		size:        0,
	}
	set.x, _ = vectorForID(context.Background(), center)

	return set
}

func (s *NaiveSet) Add(x uint64) *NaiveSet {
	if s.center == x {
		return s
	}
	if s.bitSet.ContainsAndAdd(x) {
		return s
	}

	vector, _ := s.vectorForID(context.Background(), x)
	item := &IndexAndDistance{
		index:    x,
		distance: s.distance(s.x, vector),
		visited:  false,
	}
	s.size++
	if s.size == 1 {
		s.items = append(s.items, item)
		return s
	}

	s.insert(item)
	return s
}

func (s *NaiveSet) insert(data *IndexAndDistance) {
	left := s.current
	right := len(s.items)

	if s.items[left].distance >= data.distance {
		s.items = append([]*IndexAndDistance{data}, s.items...)
		return
	}

	for right > 1 && left < right-1 {
		mid := (left + right) / 2
		if s.items[mid].distance > data.distance {
			right = mid
		} else {
			left = mid
		}
	}

	s.items = append(s.items, &IndexAndDistance{})
	copy(s.items[right+1:], s.items[right:])
	s.items[right] = data
}

func (s *NaiveSet) GetItems() []*IndexAndDistance {
	return s.items
}

func (s *NaiveSet) SkipOn(x *IndexAndDistance) bool {
	return x.visited
}

func (s *NaiveSet) AddRange(others []uint64) *NaiveSet {
	for _, item := range others {
		s.Add(item)
	}
	return s
}

func (s *NaiveSet) Remove(x *IndexAndDistance) {
	x.visited = true
	s.size--
}

func (s *NaiveSet) Pop() *IndexAndDistance {
	for s.items[s.current].visited {
		s.current++
	}

	x := s.items[s.current]
	s.current++
	s.size--
	return x
}

func (s *NaiveSet) Size() int {
	return s.size
}
