package ssdhelpers

import (
	"encoding/gob"
	"fmt"
	"os"

	"github.com/pkg/errors"
)

type BitSet struct {
	keys []byte
	nils []byte
}

const BitSetDataFileName = "bitset.gob"

func NewBitSet(capacity int) *BitSet {
	return &BitSet{
		keys: make([]byte, capacity/8+1),
		nils: make([]byte, capacity/8+1),
	}
}

func (s *BitSet) ToDisk(path string) {
	if s == nil {
		return
	}
	fData, err := os.Create(fmt.Sprintf("%s/%s", path, BitSetDataFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not create bitset file"))
	}
	defer fData.Close()

	dEnc := gob.NewEncoder(fData)
	err = dEnc.Encode(s.keys)
	if err != nil {
		panic(errors.Wrap(err, "Could not encode bitset"))
	}
}

func BitSetFromDisk(path string) *BitSet {
	fData, err := os.Open(fmt.Sprintf("%s/%s", path, BitSetDataFileName))
	if err != nil {
		return nil
	}
	defer fData.Close()

	var data []byte
	dDec := gob.NewDecoder(fData)
	err = dDec.Decode(&data)
	if err != nil {
		panic(errors.Wrap(err, "Could not decode data"))
	}
	s := NewBitSet((len(data) - 1) * 8)
	s.keys = data
	return s
}

func (s *BitSet) Clean() {
	copy(s.keys, s.nils)
}

func (s *BitSet) maskFor(x uint64) byte {
	mask := byte(1 << (8 - (x % 8) - 1))
	return mask
}

func (s *BitSet) contains(x uint64) (bool, byte, byte) {
	b := s.keys[x/8]
	mask := s.maskFor(x)
	return b&mask != 0, b, mask
}

func (s *BitSet) ContainsAndAdd(x uint64) bool {
	found, b, mask := s.contains(x)
	if found {
		return true
	}
	s.keys[x/8] = b | mask
	return false
}

func (s *BitSet) ContainsAndRemove(x uint64) bool {
	found, b, mask := s.contains(x)
	if !found {
		return false
	}
	s.keys[x/8] = b & ^mask
	return true
}

func (s *BitSet) Contains(x uint64) bool {
	found, _, _ := s.contains(x)
	return found
}

func (s *BitSet) Add(x uint64) {
	b := s.keys[x/8]
	mask := s.maskFor(x)
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

func (s *FullBitSet) Elements() []uint64 {
	return s.items
}

func (s *FullBitSet) Size() int {
	return len(s.items)
}
