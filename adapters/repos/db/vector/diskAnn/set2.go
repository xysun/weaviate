package diskAnn

type Set2 struct {
	items map[uint64]*IndexAndDistance
}

type IndexAndDistance struct {
	index    uint64
	distance float32
	visited  bool
}

func NewSet2() *Set2 {
	set := &Set2{
		items: make(map[uint64]*IndexAndDistance, 0),
	}
	return set
}

func (s *Set2) Add(x uint64) *Set2 {
	s.items[x] = &IndexAndDistance{
		index:    x,
		visited:  false,
		distance: 0,
	}
	return s
}

func (s *Set2) NotVisited() bool {
	for _, element := range s.items {
		if !element.visited {
			return true
		}
	}
	return false
}

func (s *Set2) AddRange(others []uint64) *Set2 {
	for _, item := range others {
		s.Add(item)
	}
	return s
}

func (s *Set2) Remove(x uint64) *Set2 {
	delete(s.items, x)
	return s
}

func (s *Set2) RemoveRange(others []uint64) *Set2 {
	for _, item := range others {
		s.Remove(item)
	}
	return s
}

func (s *Set2) Size() int {
	return len(s.items)
}
