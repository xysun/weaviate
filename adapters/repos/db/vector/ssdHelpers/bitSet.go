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

type MapSet struct {
	keys map[uint64]struct{}
}

func NewMapSet(capacity int) *MapSet {
	return &MapSet{
		keys: make(map[uint64]struct{}, capacity),
	}
}

func (s *MapSet) ContainsAndAdd(x uint64) bool {
	_, found := s.keys[x]
	if found {
		return true
	}
	s.keys[x] = struct{}{}
	return false
}
