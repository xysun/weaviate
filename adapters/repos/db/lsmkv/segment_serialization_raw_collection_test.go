package lsmkv

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Benchmark_CollectionNode_Raw(b *testing.B) {
	nodeBytes, err := dummyNode(1e6, 20)
	require.Nil(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := &segmentCollectionNodeRaw{}
		n.FromBytes(nodeBytes)
	}
}

func Benchmark_CollectionNode_Classic(b *testing.B) {
	nodeBytes, err := dummyNode(1e6, 20)
	require.Nil(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParseCollectionNode(bytes.NewReader(nodeBytes))
	}
}

func dummyNode(count int, sizePerValue int) ([]byte, error) {
	n := segmentCollectionNode{
		primaryKey: []byte{0x1, 0x2, 0x3, 0x1, 0x2, 0x3, 0x1, 0x2, 0x3, 0x1, 0x2, 0x3},
		values:     make([]value, count),
	}

	for i := range n.values {
		n.values[i].value = make([]byte, sizePerValue)
		if i%3 == 0 {
			n.values[i].tombstone = true
		}
	}

	buf := &bytes.Buffer{}

	if _, err := n.KeyIndexAndWriteTo(buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func Test_CollectionNodeRawAndClassicProduceIdenticalResults(t *testing.T) {
	valueCount := int(1e6)
	nodeBytes, err := dummyNode(valueCount, 20)
	require.Nil(t, err)

	classic, err := ParseCollectionNode(bytes.NewReader(nodeBytes))
	require.Nil(t, err)

	raw := &segmentCollectionNodeRaw{}
	err = raw.FromBytes(nodeBytes)
	require.Nil(t, err)

	it := newValuesIterator(raw.values)

	i := 0
	for val, ts := it.Iterate(); val != nil; val, ts = it.Iterate() {
		assert.Equal(t, classic.values[i].value, val, fmt.Sprintf("iteration %d", i))
		assert.Equal(t, classic.values[i].tombstone, ts, fmt.Sprintf("iteration %d", i))
		i++
	}

	assert.Equal(t, valueCount, i)
}
