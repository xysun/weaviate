package ssdhelpers

import (
	"context"
	"encoding/gob"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync"

	"github.com/pkg/errors"
)

type Encoder int

const (
	UseTileEncoder   Encoder = 0
	UseKMeansEncoder         = 1
)

type ProductQuantizer struct {
	ks               int
	m                int
	ds               int
	distance         DistanceFunction
	vectorForIDThunk VectorForID
	dimensions       int
	dataSize         int
	kms              []NoopEncoder
	center           []float32
	distances        [][]float32
	encoderType      Encoder
}

type PQData struct {
	Ks          int
	M           int
	Dimensions  int
	DataSize    int
	EncoderType Encoder
}

type NoopEncoder interface {
	ToDisk(path string, id int)
	Encode(x []float32) byte
	Centroid(b byte) []float32
	Add(x float32)
	Fit() error
}

const PQDataFileName = "pq.gob"

func NewProductQuantizer(segments int, centroids int, distance DistanceFunction, vectorForIDThunk VectorForID, dimensions int, dataSize int, encoderType Encoder) *ProductQuantizer {
	if dataSize == 0 {
		panic("data must not be empty")
	}
	if dimensions%segments != 0 {
		panic("dimension must be a multiple of m")
	}
	pq := &ProductQuantizer{
		ks:               centroids,
		m:                segments,
		ds:               dimensions / segments,
		distance:         distance,
		vectorForIDThunk: vectorForIDThunk,
		dimensions:       dimensions,
		dataSize:         dataSize,
		encoderType:      encoderType,
	}
	return pq
}

func (pq *ProductQuantizer) ToDisk(path string) {
	if pq == nil {
		return
	}
	fData, err := os.Create(fmt.Sprintf("%s/%s", path, PQDataFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not create kmeans file"))
	}
	defer fData.Close()

	dEnc := gob.NewEncoder(fData)
	err = dEnc.Encode(PQData{
		Ks:          pq.ks,
		M:           pq.m,
		Dimensions:  pq.dimensions,
		DataSize:    pq.dataSize,
		EncoderType: pq.encoderType,
	})
	if err != nil {
		panic(errors.Wrap(err, "Could not encode pq"))
	}
	for id, km := range pq.kms {
		km.ToDisk(path, id)
	}
}

func PQFromDisk(path string, VectorForIDThunk VectorForID, distance DistanceFunction) *ProductQuantizer {
	fData, err := os.Open(fmt.Sprintf("%s/%s", path, PQDataFileName))
	if err != nil {
		return nil
	}
	defer fData.Close()

	data := PQData{}
	dDec := gob.NewDecoder(fData)
	err = dDec.Decode(&data)
	if err != nil {
		panic(errors.Wrap(err, "Could not decode data"))
	}
	pq := NewProductQuantizer(data.M, data.Ks, distance, VectorForIDThunk, data.Dimensions, data.DataSize, data.EncoderType)
	switch data.EncoderType {
	case UseKMeansEncoder:
		pq.kms = make([]NoopEncoder, pq.m)
		for id := range pq.kms {
			pq.kms[id] = KMeansFromDisk(path, id, VectorForIDThunk, distance)
		}
	case UseTileEncoder:
		pq.kms = make([]NoopEncoder, pq.m)
		for id := range pq.kms {
			pq.kms[id] = TileEncoderFromDisk(path, id)
		}
	}
	return pq
}

func (pq *ProductQuantizer) extractSegment(i int, v []float32) []float32 {
	return v[i*pq.ds : (i+1)*pq.ds]
}

type Action func(workerId uint64, taskIndex uint64, mutex *sync.Mutex)

func concurrently(n uint64, action Action) {
	n64 := float64(n)
	workerCount := runtime.GOMAXPROCS(0)
	mutex := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	split := uint64(math.Ceil(n64 / float64(workerCount)))
	for worker := uint64(0); worker < uint64(workerCount); worker++ {
		wg.Add(1)
		go func(workerID uint64) {
			defer wg.Done()
			for i := workerID * split; i < uint64(math.Min(float64((workerID+1)*split), n64)); i++ {
				action(workerID, i, mutex)
			}
		}(worker)
	}
	wg.Wait()
}

func (pq *ProductQuantizer) Fit() {
	switch pq.encoderType {
	case UseTileEncoder:
		pq.kms = make([]NoopEncoder, pq.m)
		concurrently(uint64(pq.m), func(_ uint64, i uint64, _ *sync.Mutex) {
			pq.kms[i] = NewTileEncoder(8)
			for j := 0; j < pq.dataSize; j++ {
				vec, _ := pq.vectorForIDThunk(context.Background(), uint64(j))
				pq.kms[i].Add(vec[i])
			}
		})
		return
	case UseKMeansEncoder:
		pq.kms = make([]NoopEncoder, pq.m)
		concurrently(uint64(pq.m), func(_ uint64, i uint64, _ *sync.Mutex) {
			pq.kms[i] = NewKMeans(
				pq.ks,
				pq.distance,
				func(ctx context.Context, id uint64) ([]float32, error) {
					v, e := pq.vectorForIDThunk(ctx, id)
					return pq.extractSegment(int(i), v), e
				},
				pq.dataSize,
				pq.ds)
			err := pq.kms[i].Fit()
			if err != nil {
				panic(err)
			}
		})
	}
	/*for i := 0; i < 1; i++ {
		fmt.Println("********")
		centers := make([]float64, 0)
		for c := 0; c < pq.ks; c++ {
			centers = append(centers, float64(pq.kms[i].Centroid(byte(c))[0]))
		}
		hist := histogram.Hist(20, centers)
		histogram.Fprint(os.Stdout, hist, histogram.Linear(5))
	}*/
}

func (pq *ProductQuantizer) Encode(vec []float32) []byte {
	codes := make([]byte, pq.m)
	for i := 0; i < pq.m; i++ {
		codes[i] = byte(pq.kms[i].Encode(pq.extractSegment(i, vec)))
	}
	return codes
}

func (pq *ProductQuantizer) Decode(code []byte) []float32 {
	vec := make([]float32, 0, len(pq.center))
	for i, b := range code {
		vec = append(vec, pq.kms[i].Centroid(b)...)
	}
	return vec
}

func (pq *ProductQuantizer) CenterAt(vec []float32) {
	pq.center = vec
	pq.distances = make([][]float32, pq.m)
	for m := 0; m < pq.m; m++ {
		pq.distances[m] = make([]float32, pq.ks)
	}
}

func (pq *ProductQuantizer) Distance(encoded []byte) float32 {
	dist := float32(0.0)
	for i, b := range encoded {
		d := pq.distances[i][b]
		if d == 0 {
			d = pq.distance(pq.extractSegment(i, pq.center), pq.kms[i].Centroid(b))
			pq.distances[i][b] = d
		}
		dist += d
	}
	return dist
}

/*
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
			di^st, _, err := provider.SingleDist(vec_sub, pq.codewords[i][j])
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

}*/
