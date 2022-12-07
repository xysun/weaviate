package diskAnn_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aybabtme/uniplot/histogram"
	testinghelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/testingHelpers"
)

func TestDistributions(t *testing.T) {
	vectors_size := 1000000
	queries_size := 10
	dimensions := 960
	before := time.Now()
	vectors, _ := testinghelpers.ReadVecs(vectors_size, queries_size, dimensions, "gist", "../testdata")
	testinghelpers.Normalize(vectors)
	fmt.Printf("generating data took %s\n", time.Since(before))
	for i := 0; i < 100; i++ {
		fmt.Println("********")
		segmented := make([]float64, 0)
		for c := 0; c < vectors_size; c++ {
			segmented = append(segmented, float64(vectors[c][i]))
		}
		hist := histogram.Hist(60, segmented)
		histogram.Fprint(os.Stdout, hist, histogram.Linear(5))
	}
}
