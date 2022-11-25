//go:build distributionTest
// +build distributionTest

package ssdhelpers_test

import (
	"os"
	"testing"

	"github.com/aybabtme/uniplot/histogram"
	testinghelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/testingHelpers"
)

func TestSIFTDist(t *testing.T) {
	size := 10000
	index := 0
	vectors := testinghelpers.ReadSiftVecsFrom("../../diskAnn/testdata/sift/sift_learn.fvecs", size)
	probe := make([]float64, 0, size)
	for i := 0; i < size; i++ {
		probe = append(probe, float64(vectors[i][index]))
	}
	hist := histogram.Hist(20, probe)
	histogram.Fprint(os.Stdout, hist, histogram.Linear(5))
}
