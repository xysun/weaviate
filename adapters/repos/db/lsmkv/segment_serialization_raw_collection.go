package lsmkv

import (
	"encoding/binary"
)

// segmentCollectionNodeRaw is an experimental version of segmentCollectionNode
// which tries to keep the values slice as raw data. This should allow for more
// efficient passing arond on very large entries, such as when there are
// filters that match millions of documents in the inverted index
type segmentCollectionNodeRaw struct {
	values     []byte
	primaryKey []byte
	offset     int
}

func (s *segmentCollectionNodeRaw) FromBytes(in []byte) error {
	s.offset = 0 // reset in case it's been used before

	valuesLen := binary.LittleEndian.Uint64(in[s.offset : s.offset+8])
	s.offset += 8

	valuesStart := s.offset
	for i := 0; i < int(valuesLen); i++ {
		s.offset += 1 // account for the tombstone byte

		valueLen := binary.LittleEndian.Uint64(in[s.offset : s.offset+8])
		s.offset += 8
		s.offset += int(valueLen)
	}
	valuesEnd := s.offset

	s.values = in[valuesStart:valuesEnd]

	keyLen := binary.LittleEndian.Uint32(in[s.offset : s.offset+4])
	s.offset += 4
	s.primaryKey = in[s.offset : s.offset+int(keyLen)]
	s.offset += int(keyLen)

	return nil
}

// valuesIterator takes in a raw values []byte as we can get it from
// segmentCollectionNodeRaw, and iterates over it
type valuesIterator struct {
	in     []byte
	offset int
}

func newValuesIterator(in []byte) *valuesIterator {
	return &valuesIterator{
		in:     in,
		offset: 0,
	}
}

// Iterate returns the value, tombstone combination. If the value is nil, the
// Iterator has reached its end.
func (vi *valuesIterator) Iterate() ([]byte, bool) {
	if vi.offset == len(vi.in) {
		return nil, false
	}

	tombstone := vi.in[vi.offset] == 0x01
	vi.offset += 1

	valueLen := binary.LittleEndian.Uint64(vi.in[vi.offset : vi.offset+8])
	vi.offset += 8

	value := vi.in[vi.offset : vi.offset+int(valueLen)]
	vi.offset += int(valueLen)

	return value, tombstone
}
