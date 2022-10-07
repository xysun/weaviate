package ssdhelpers

import (
	"context"
	"encoding/gob"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"

	"github.com/pkg/errors"
)

type KMeans struct {
	K                  int
	DeltaThreshold     float32
	IterationThreshold int
	Distance           DistanceFunction
	VectorForIDThunk   VectorForID
	centers            [][]float32
	dimensions         int
	dataSize           int
}

type KMeansData struct {
	K        int
	Centers  [][]float32
	DataSize int
}

const DataFileName = "kmeans.gob"

func New(k int, distance DistanceFunction, vectorForIdThunk VectorForID, dataSize int, dimensions int) *KMeans {
	return &KMeans{
		K:                  k,
		DeltaThreshold:     0.1,
		IterationThreshold: 1000,
		Distance:           distance,
		VectorForIDThunk:   vectorForIdThunk,
		dimensions:         dimensions,
		dataSize:           dataSize,
	}
}

func (m *KMeans) ToDisk(path string, id int) {
	fData, err := os.Create(fmt.Sprintf("%s/%d.%s", path, id, DataFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not create kmeans file"))
	}
	defer fData.Close()

	dEnc := gob.NewEncoder(fData)
	err = dEnc.Encode(KMeansData{
		K:        m.K,
		Centers:  m.centers,
		DataSize: m.dataSize,
	})
	if err != nil {
		panic(errors.Wrap(err, "Could not encode kmeans"))
	}
}

func KMeansFromDisk(path string, id int, VectorForIDThunk VectorForID, distance DistanceFunction) *KMeans {
	fData, err := os.Open(fmt.Sprintf("%s/%d.%s", path, id, DataFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not open kmeans file"))
	}
	defer fData.Close()

	data := KMeansData{}
	dDec := gob.NewDecoder(fData)
	err = dDec.Decode(&data)
	if err != nil {
		panic(errors.Wrap(err, "Could not decode data"))
	}
	kmeans := New(data.K, distance, VectorForIDThunk, data.DataSize, 0)
	kmeans.centers = data.Centers
	return kmeans
}

func (m *KMeans) Nearest(point []float32) uint64 {
	return m.NNearest(point, 1)[0]
}

func (m *KMeans) NNearest(point []float32, n int) []uint64 {
	mins := make([]uint64, n)
	minD := make([]float32, n)
	for i := range mins {
		mins[i] = 0
		minD[i] = math.MaxFloat32
	}
	for i, c := range m.centers {
		distance := m.Distance(point, c)
		j := 0
		for (j < n) && minD[j] < distance {
			j++
		}
		if j < n {
			for l := n - 1; l >= j+1; l-- {
				mins[l] = mins[l-1]
				minD[l] = minD[l-1]
			}
			minD[j] = distance
			mins[j] = uint64(i)
		}
	}
	return mins
}

func (m *KMeans) Partition() (*KMeans, error) { //init centers using min/max per dimension
	k64 := uint64(m.K)
	Concurrently(k64, func(workerID uint64, i uint64, mutex *sync.Mutex) {
		var p []float32
		for j := 0; j < m.dimensions; j++ {
			p = append(p, rand.Float32())
		}
		mutex.Lock()
		m.centers = append(m.centers, p)
		mutex.Unlock()
	})

	points := make([]uint64, m.dataSize)
	changes := 1

	for i := 0; changes > 0; i++ {
		changes = 0
		cc := make([][]uint64, m.K)
		for j := range cc {
			cc[j] = make([]uint64, 0)
		}

		Concurrently(uint64(m.dataSize), func(workerId, p uint64, mutex *sync.Mutex) {
			point, _ := m.VectorForIDThunk(context.Background(), uint64(p))
			ci := m.Nearest(point)
			mutex.Lock()
			cc[ci] = append(cc[ci], uint64(p))
			mutex.Unlock()
			if points[p] != ci {
				points[p] = ci
				changes++
			}
		})

		Concurrently(k64, func(workerID uint64, ci uint64, mutex *sync.Mutex) {
			if len(cc[ci]) == 0 {
				var ri int
				for {
					ri = rand.Intn(m.dataSize)
					if len(cc[points[ri]]) > 1 {
						break
					}
				}
				cc[ci] = append(cc[ci], uint64(ri))
				points[ri] = ci
				changes = m.dataSize
			}
		})

		if changes > 0 {
			Concurrently(k64, func(workerID uint64, i uint64, mutex *sync.Mutex) {
				m.centers[i] = make([]float32, m.dimensions)
				for j := range m.centers[i] {
					m.centers[i][j] = 0
				}
				size := len(cc[i])
				for _, ci := range cc[i] {
					v := m.getPoint(ci)
					for j := 0; j < m.dimensions; j++ {
						m.centers[i][j] += v[j]
					}
				}
				for j := 0; j < m.dimensions; j++ {
					m.centers[i][j] /= float32(size)
				}
			})
		}
		if i == m.IterationThreshold ||
			changes < int(float32(m.dataSize)*m.DeltaThreshold) {
			break
		}
	}

	return m, nil
}

func (m *KMeans) Center(point []float32) []float32 {
	return m.centers[m.Nearest(point)]
}

func (m *KMeans) Centroid(i uint64) []float32 {
	return m.centers[i]
}

func (m *KMeans) getPoint(index uint64) []float32 {
	v, _ := m.VectorForIDThunk(context.Background(), index)
	return v
}
