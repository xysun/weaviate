package ssdhelpers

type BitSet struct {
	keys []byte
	nils []byte
}

func NewBitSet(capacity int) *BitSet {
	return &BitSet{
		keys: make([]byte, capacity/8+1),
		nils: make([]byte, capacity/8+1),
	}
}

func (s *BitSet) Clean() {
	copy(s.keys, s.nils)
}

func (s *BitSet) ContainsAndAdd(x uint64) bool {
	b := s.keys[x/8]
	mask := byte(1 << (8 - (x % 8) - 1))
	if b&mask != 0 {
		return true
	}
	s.keys[x/8] = b | mask
	return false
}

func (s *BitSet) ContainsAndRemove(x uint64) bool {
	b := s.keys[x/8]
	mask := byte(1 << (8 - (x % 8) - 1))
	if b&mask == 0 {
		return false
	}
	s.keys[x/8] = b & ^mask
	return true
}

func (s *BitSet) Contains(x uint64) bool {
	b := s.keys[x/8]
	mask := byte(1 << (8 - (x % 8) - 1))
	return b&mask == 0
}

func (s *BitSet) Add(x uint64) {
	b := s.keys[x/8]
	mask := byte(1 << (8 - (x % 8) - 1))
	s.keys[x/8] = b | mask
}

type FullBitSet struct {
	bitSet *BitSet
	items  []uint64
}

func NewFullBitSet(capacity int) *FullBitSet {
	return &FullBitSet{
		bitSet: NewBitSet(capacity),
		items:  make([]uint64, 0),
	}
}

func (s *FullBitSet) Clean() {
	s.bitSet.Clean()
	s.items = make([]uint64, 0)
}

func (s *FullBitSet) Contains(x uint64) bool {
	return s.bitSet.Contains(x)
}

func (s *FullBitSet) Add(x uint64) {
	if s.bitSet.ContainsAndAdd(x) {
		return
	}
	s.items = append(s.items, x)
}

func (s *FullBitSet) Remove(x uint64) {
	if s.bitSet.ContainsAndRemove(x) {
		s.items = remove(s.items, x)
	}
}

func remove(items []uint64, x uint64) []uint64 {
	index := 0
	for index < len(items) && items[index] != x {
		index++
	}
	if index >= len(items) {
		return items
	}
	items[index] = items[len(items)-1]
	return items[:len(items)-1]
}

func (s *FullBitSet) Elements() []uint64 {
	return s.items
}

func (s *FullBitSet) Size() int {
	return len(s.items)
}

type SortedSetElement struct {
	Item  uint64
	Score float32
}

type FullSortedBitSet struct {
	bitSet *BitSet
	items  []SortedSetElement
}

func NewFullSortedBitSet(capacity int) *FullSortedBitSet {
	return &FullSortedBitSet{
		bitSet: NewBitSet(capacity),
		items:  make([]SortedSetElement, 0),
	}
}

func (s *FullSortedBitSet) FullSortedBitSet() {
	s.bitSet.Clean()
	s.items = make([]SortedSetElement, 0)
}

func (s *FullSortedBitSet) Contains(x uint64) bool {
	return s.bitSet.Contains(x)
}

func (s *FullSortedBitSet) find(x SortedSetElement) int {
	left := 0
	right := len(s.items) - 1

	if s.items[left].Score >= x.Score {
		return 0
	}

	for right > 1 && left < right-1 {
		mid := (left + right) / 2
		if s.items[mid].Score > x.Score {
			right = mid
		} else {
			left = mid
		}
	}
	for left > 0 {
		if s.items[left].Score < x.Score {
			break
		}
		left--
	}
	return right
}

func (s *FullSortedBitSet) Add(x SortedSetElement) {
	if s.bitSet.ContainsAndAdd(x.Item) {
		return
	}

	if len(s.items) == 0 {
		s.items = append(s.items, x)
		return
	}
	index := s.find(x)
	s.items = append(s.items, x)
	if s.items[index].Score < x.Score {
		index++
	}
	copy(s.items[index+1:], s.items[index:])
	s.items[index] = x
}

func (s *FullSortedBitSet) Remove(x SortedSetElement) {
	if s.bitSet.ContainsAndRemove(x.Item) {
		s.items = removeAt(s.items, s.find(x))
		return
	}
}

func removeAt(items []SortedSetElement, index int) []SortedSetElement {
	copy(items[index:], items[index+1:])
	return items[:len(items)-1]
}

func (s *FullSortedBitSet) First() SortedSetElement {
	return s.items[0]
}

func (s *FullSortedBitSet) Elements() []uint64 {
	res := make([]uint64, 0, len(s.items))
	for _, x := range s.items {
		res = append(res, x.Item)
	}
	return res
}

func (s *FullSortedBitSet) SortedElements() []SortedSetElement {
	return s.items
}

func (s *FullSortedBitSet) Size() int {
	return len(s.items)
}
