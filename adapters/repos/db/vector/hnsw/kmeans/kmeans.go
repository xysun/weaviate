// Derived from MIT licensed https://github.com/muesli/kmeans

package kmeans

import (
	"fmt"
	"math/rand"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/clusters"
)

type Kmeans struct {
	deltaThreshold     float32
	iterationThreshold int
}

func New() Kmeans {
	return Kmeans{
		deltaThreshold:     0.001,
		iterationThreshold: 12,
	}
}

func (m Kmeans) Partition(dataset clusters.Observations, k int) (clusters.Clusters, error) {
	if k > len(dataset) {
		return clusters.Clusters{}, fmt.Errorf("the size of the data set must at least equal k")
	}

	cc, err := clusters.New(k, dataset)
	if err != nil {
		return cc, err
	}

	points := make([]int, len(dataset))
	changes := 1

	for i := 0; changes > 0; i++ {
		changes = 0
		cc.Reset()

		for p, point := range dataset {
			ci := cc.Nearest(point)
			cc[ci].Append(point)
			if points[p] != ci {
				points[p] = ci
				changes++
			}
		}

		for ci := 0; ci < len(cc); ci++ {
			if len(cc[ci].Observations) == 0 {
				// During the iterations, if any of the cluster centers has no
				// data points associated with it, assign a random data point
				// to it.
				// Also see: http://user.ceng.metu.edu.tr/~tcan/ceng465_f1314/Schedule/KMeansEmpty.html
				var ri int
				for {
					// find a cluster with at least two data points, otherwise
					// we're just emptying one cluster to fill another
					ri = rand.Intn(len(dataset)) //nolint:gosec // rand.Intn is good enough for this
					if len(cc[points[ri]].Observations) > 1 {
						break
					}
				}
				cc[ci].Append(dataset[ri])
				points[ri] = ci

				// Ensure that we always see at least one more iteration after
				// randomly assigning a data point to a cluster
				changes = len(dataset)
			}
		}

		if changes > 0 {
			cc.Recenter()
		}
		if i == m.iterationThreshold ||
			changes < int(float32(len(dataset))*m.deltaThreshold) {
			// fmt.Println("Aborting:", changes, int(float32(len(dataset))*m.TerminationThreshold))
			break
		}
	}

	return cc, nil
}
