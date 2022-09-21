package hnsw

import (
	"fmt"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/clusters"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/kmeans"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
)

type productQuantizer struct {
	Ks        int
	m         int
	codewords [][][]float32
}

func (pq *productQuantizer) fit(xs [][]float32) {

	if len(xs) == 0 {
		panic("xs must not be empty")
	}

	D := len(xs[0])
	m := pq.m
	Ks := pq.Ks

	if D%m != 0 {
		panic("dimension must be a multiple of m")
	}

	Ds := D / m

	codewords := make([][][]float32, m)

	for i := 0; i < m; i++ {

		fmt.Printf("Training subspace %d/%d\n", i, m)

		// TODO: Replace Coordinates objects with slice in kmeans
		var subspace clusters.Observations

		for j := 0; j < len(xs); j++ {
			subspace = append(subspace, clusters.Coordinates(xs[j][i*Ds:(i+1)*Ds]))
		}

		km := kmeans.New()
		clusters, err := km.Partition(subspace, Ks)

		if err != nil {
			panic(err)
		}

		codewords[i] = make([][]float32, Ks)

		for k, c := range clusters {
			// fmt.Printf("Centered at x: %.2f y: %.2f z: %.2f, ... \n", c.Center[0], c.Center[1], c.Center[3])
			// fmt.Printf("Matching data points: %d\n\n", len(c.Observations))
			codewords[i][k] = make([]float32, Ds)
			for l := 0; l < Ds; l++ {
				codewords[i][k][l] = c.Center[l]
			}
		}

	}

	pq.codewords = codewords
}

func (pq *productQuantizer) encode(vec []float32) []float32 {

	// Encodes a single vector into a product quantizer code.

	codes := make([]float32, pq.m)

	D := len(vec) // TODO change to pull this from struct
	Ds := D / pq.m

	provider := distancer.L2SquaredProvider{}

	for i := 0; i < pq.m; i++ {
		// fmt.Printf("Encoding subspace %d/%d\n", i, pq.m)

		vec_sub := vec[i*Ds : (i+1)*Ds]

		medoid_queue := priorityqueue.NewMin(1)

		for k := 0; k < len(pq.codewords[i]); k++ {

			dist, _, err := provider.SingleDist(vec_sub, pq.codewords[i][k])
			if err != nil {
				panic(err)
			}
			// fmt.Printf("Medoid %d distance = %f\n", k, dist)
			medoid_queue.Insert(uint64(k), dist)
		}

		best_medoid := medoid_queue.Pop()

		// fmt.Printf("Best medoid distance: %f\n", best_medoid.Dist)
		// leaving type as float32 for consistency
		codes[i] = float32(best_medoid.ID)

	}

	return codes
}

func (pq *productQuantizer) search(vec []float32, quantizedVectors [][]float32) []float32 {

	// Searches for the k nearest neighbors of a single vector.

	distance_table := make([][]float32, pq.m)

	D := len(vec) // TODO change to pull this from struct
	Ds := D / pq.m

	fmt.Printf("Creating distance table\n")

	provider := distancer.L2SquaredProvider{}

	// Build distance table
	for i := 0; i < pq.m; i++ {
		vec_sub := vec[i*Ds : (i+1)*Ds]
		distance_table[i] = make([]float32, len(pq.codewords[i]))
		for j := 0; j < pq.Ks; j++ {
			dist, _, err := provider.SingleDist(vec_sub, pq.codewords[i][j])
			if err != nil {
				panic(err)
			}
			distance_table[i][j] = dist
		}
	}

	// Lookup partial distances
	// This needs to be integrated with HNSW, and Vamana

	distances := make([]float32, len(quantizedVectors))

	for q := 0; q < len(quantizedVectors); q++ {
		distances[q] = 0
		for i := 0; i < pq.m; i++ {
			// fmt.Println(distance_table[i])
			// fmt.Println(int(quantizedVectors[q][i]))

			// note int conversion here as I'm storing quantizedVectors  also as float32
			// I'm doing this so we can use same distance functions
			distances[q] += distance_table[i][int(quantizedVectors[q][i])]
		}

		fmt.Printf("Vector %d distance: %f\n", q, distances[q])
	}

	return distances

}
