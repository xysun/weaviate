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

func NewNaiveSet(vectorForID VectorForID, distance DistanceFunction, capacity int) *NaiveSet {
	set := &NaiveSet{
		vectorForID: vectorForID,
		distance:    distance,
		bitSet:      NewBitSet(capacity),
	}
	return set
}

func (s *NaiveSet) ReCenter(center uint64) {
	s.items = make([]*IndexAndDistance, 0)
	s.center = center
	s.current = 0
	s.size = 0
	s.x, _ = s.vectorForID(context.Background(), center)
	s.bitSet.Clean()
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
		vector:   vector,
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
	right := len(s.items) - 1

	if s.items[right].distance <= data.distance {
		s.items = append(s.items, data)
		return
	}

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
	if x.visited {
		return
	}
	x.visited = true
	s.size--
}

func (s *NaiveSet) Pop() *IndexAndDistance {
	for s.items[s.current].visited {
		s.current++
	}

	x := s.items[s.current]
	x.visited = true
	s.current++
	s.size--
	return x
}

func (s *NaiveSet) Size() int {
	return s.size
}

func (s *NaiveSet) RemoveIf(filter func(*IndexAndDistance) bool) {
	for i := s.current; i < s.size; i++ {
		x := s.items[i]
		if x.visited {
			continue
		}
		if filter(x) {
			s.Remove(x)
		}
	}
}
