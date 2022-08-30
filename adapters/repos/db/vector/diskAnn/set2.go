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

func (s *Set2) Wrap(items []IndexAndDistance) *Set2 {
	s.items = make(map[uint64]*IndexAndDistance, len(items))
	for _, x := range items {
		s.items[x.index] = &IndexAndDistance{
			index:    x.index,
			visited:  x.visited,
			distance: x.distance,
		}
	}
	return s
}

func (s *Set2) Contains(x uint64) bool {
	_, found := s.items[x]
	return found
}

func (s *Set2) Add(x uint64) *Set2 {
	if s.Contains(x) {
		return s
	}
	s.items[x] = &IndexAndDistance{
		index:    x,
		visited:  false,
		distance: 0,
	}
	return s
}

func (s *Set2) NotVisited() int {
	res := make([]IndexAndDistance, len(s.items))
	i := 0
	for _, element := range s.items {
		if element.visited {
			continue
		}
		res[i] = *element
		i++
	}
	return i
}

func (s *Set2) Visit(x uint64) {
	element, found := s.items[x]
	if !found {
		return
	}
	element.visited = true
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

func (s *Set2) RemoveFromStruct(x IndexAndDistance) *Set2 {
	return s.Remove(x.index)
}

func (s *Set2) Elements() []IndexAndDistance {
	res := make([]IndexAndDistance, len(s.items))
	i := 0
	for _, element := range s.items {
		res[i] = *element
		i++
	}
	return res[:i]
}
