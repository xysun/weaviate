package ssdhelpers

type MapSet struct {
	items map[uint64]struct{}
}

func NewMapSet() *MapSet {
	return &MapSet{
		items: make(map[uint64]struct{}),
	}
}

func (ms *MapSet) Add(x uint64) {
	ms.items[x] = struct{}{}
}

func (ms *MapSet) Contains(x uint64) bool {
	_, found := ms.items[x]
	return found
}

func (ms *MapSet) Intersect(elements []uint64) *MapSet {
	results := NewMapSet()
	for _, x := range elements {
		_, found := ms.items[x]
		if !found {
			continue
		}
		results.Add(x)
	}
	return results
}

func (ms *MapSet) Size() int {
	return len(ms.items)
}

func (ms *MapSet) Elements() []uint64 {
	results := make([]uint64, 0, ms.Size())
	for x := range ms.items {
		results = append(results, x)
	}
	return results
}

func (ms *MapSet) DiffFrom(elements []uint64) *MapSet {
	results := NewMapSet()
	for _, x := range elements {
		if ms.Contains(x) {
			continue
		}
		results.Add(x)
	}
	return results
}
