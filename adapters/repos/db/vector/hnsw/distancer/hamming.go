package distancer

import (
	"math/bits"

	"github.com/pkg/errors"
)

type HammingProvider struct{}

func NewHammingProvider() HammingProvider {
	return HammingProvider{}
}

var hammingImplementation = func(a, b []byte) int {
	sum := 0
	for i := range a {
		sum += bits.OnesCount8(a[i] ^ b[i])
	}

	return sum
}

func (d HammingProvider) SingleDist(a, b []byte) (int, bool, error) {
	if len(a) != len(b) {
		return 0, false, errors.Errorf("vector lengths don't match: %d vs %d",
			len(a), len(b))
	}

	dist := hammingImplementation(a, b)

	return dist, true, nil
}

func (d HammingProvider) Type() string {
	return "cosine-dot"
}

// func (d HammingProvider) New(a []byte) Distancer {
// 	return &Hamming{a: a}
// }
