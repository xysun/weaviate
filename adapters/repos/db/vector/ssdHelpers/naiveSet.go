package ssdhelpers

type NaiveSet struct {
	items map[uint64]*IndexAndDistance
	//	bitSet *BitSet
}

func NewNaiveSet(size int) *NaiveSet {
	set := &NaiveSet{
		items: make(map[uint64]*IndexAndDistance, 0),
		//	bitSet: NewBitSet(size),
	}
	return set
}

func (s *NaiveSet) Add(x uint64) *NaiveSet {
	//	if s.bitSet.ContainsAndAdd(x) {
	//		return s
	//	}

	s.items[x] = &IndexAndDistance{
		index:    x,
		distance: 0,
	}
	return s
}

func (s *NaiveSet) GetItems() map[uint64]*IndexAndDistance {
	return s.items
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
