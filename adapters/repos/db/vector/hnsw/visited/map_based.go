package visited

type MapBasedVisitedList map[uint64]struct{}

func NewMapBased(entrySize int) MapBasedVisitedList {
	return make(MapBasedVisitedList, entrySize)
}

func (m MapBasedVisitedList) Visit(id uint64) {
	m[id] = struct{}{}
}

func (m MapBasedVisitedList) Visited(id uint64) bool {
	_, ok := m[id]
	return ok
}
