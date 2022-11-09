package diskAnn

type NaiveSet struct {
	items map[uint64]*IndexAndDistance
}

type IndexAndDistance struct {
	index    uint64
	distance float32
}

func NewNaiveSet() *NaiveSet {
	set := &NaiveSet{
		items: make(map[uint64]*IndexAndDistance, 0),
	}
	return set
}

func (s *NaiveSet) Add(x uint64) *NaiveSet {
	s.items[x] = &IndexAndDistance{
		index:    x,
		distance: 0,
	}
	return s
}

func (s *NaiveSet) AddRange(others []uint64) *NaiveSet {
	for _, item := range others {
		s.Add(item)
	}
	return s
}

func (s *NaiveSet) Remove(x uint64) *NaiveSet {
	delete(s.items, x)
	return s
}

func (s *NaiveSet) RemoveRange(others []uint64) *NaiveSet {
	for _, item := range others {
		s.Remove(item)
	}
	return s
}

func (s *NaiveSet) Size() int {
	return len(s.items)
}
