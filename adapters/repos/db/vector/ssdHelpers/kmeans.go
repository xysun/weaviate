package ssdhelpers

import (
	"context"
	"encoding/gob"
	"fmt"
	"math"
	"math/rand"
	"os"

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

	data KMeansPartitionData
}

type KMeansPartitionData struct {
	changes      int
	points       []uint64
	cc           [][]uint64
	maxDistances []float32
	maxPoints    [][]float32
}
type KMeansData struct {
	K        int
	Centers  [][]float32
	DataSize int
}

const DataFileName = "kmeans.gob"

func NewKMeans(k int, distance DistanceFunction, vectorForIdThunk VectorForID, dataSize int, dimensions int) *KMeans {
	kMeans := &KMeans{
		K:                  k,
		DeltaThreshold:     0.0001,
		IterationThreshold: 10000,
		Distance:           distance,
		VectorForIDThunk:   vectorForIdThunk,
		dimensions:         dimensions,
		dataSize:           dataSize,
	}
	kMeans.initCenters()
	return kMeans
}

func NewKMeansWithCenters(k int, distance DistanceFunction, vectorForIdThunk VectorForID, dataSize int, dimensions int, centers [][]float32) *KMeans {
	kMeans := NewKMeans(k, distance, vectorForIdThunk, dataSize, dimensions)
	kMeans.setCenters(centers)
	return kMeans
}

func (m *KMeans) ToDisk(path string, id int) {
	if m == nil {
		return
	}
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
		return nil
	}
	defer fData.Close()

	data := KMeansData{}
	dDec := gob.NewDecoder(fData)
	err = dDec.Decode(&data)
	if err != nil {
		panic(errors.Wrap(err, "Could not decode data"))
	}
	kmeans := NewKMeans(data.K, distance, VectorForIDThunk, data.DataSize, 0)
	kmeans.centers = data.Centers
	return kmeans
}

func (m *KMeans) Nearest(point []float32) uint64 {
	return m.NNearest(point, 1)[0]
}

func (m *KMeans) nNearest(point []float32, n int) ([]uint64, []float32) {
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
	return mins, minD
}

func (m *KMeans) NNearest(point []float32, n int) []uint64 {
	nearest, _ := m.nNearest(point, n)
	return nearest
}

func (m *KMeans) setCenters(centers [][]float32) {
	// ToDo: check size
	m.centers = centers
}

func (m *KMeans) initCenters() {
	for i := 0; i < m.K; i++ {
		var p []float32
		for j := 0; j < m.dimensions; j++ {
			p = append(p, rand.Float32())
		}
		m.centers = append(m.centers, p)
	}
}

func (m *KMeans) recluster() {
	for p := 0; p < m.dataSize; p++ {
		point, _ := m.VectorForIDThunk(context.Background(), uint64(p))
		cis, dis := m.nNearest(point, 1)
		ci, di := cis[0], dis[0]
		m.data.cc[ci] = append(m.data.cc[ci], uint64(p))
		if di > m.data.maxDistances[ci] {
			m.data.maxDistances[ci] = di
			m.data.maxPoints[ci] = point
		}
		if m.data.points[p] != ci {
			m.data.points[p] = ci
			m.data.changes++
		}
	}
}

func (m *KMeans) resortOnEmptySets() {
	k64 := uint64(m.K)
	for ci := uint64(0); ci < k64; ci++ {
		if len(m.data.cc[ci]) == 0 {
			var ri int
			for {
				ri = rand.Intn(m.dataSize)
				if len(m.data.cc[m.data.points[ri]]) > 1 {
					break
				}
			}
			m.data.cc[ci] = append(m.data.cc[ci], uint64(ri))
			m.data.points[ri] = ci
			m.data.changes = m.dataSize
		}
	}
}

func (m *KMeans) recalcCenters() {
	for index := 0; index < m.K; index++ {
		m.centers[index] = make([]float32, m.dimensions)
		for j := range m.centers[index] {
			m.centers[index][j] = 0
		}
		size := len(m.data.cc[index])
		for _, ci := range m.data.cc[index] {
			v := m.getPoint(ci)
			for j := 0; j < m.dimensions; j++ {
				m.centers[index][j] += v[j]
			}
		}
		for j := 0; j < m.dimensions; j++ {
			m.centers[index][j] /= float32(size)
		}
	}
}

func (m *KMeans) stopCondition(iterations int) bool {
	return iterations == m.IterationThreshold ||
		m.data.changes < int(float32(m.dataSize)*m.DeltaThreshold)
}

func (m *KMeans) spreadCenters() {
	maxIndex := 0
	minIndex := 0
	var minDistance float32 = math.MaxFloat32

	for ci := 0; ci < m.K; ci++ {
		if m.data.maxDistances[maxIndex] < m.data.maxDistances[ci] {
			maxIndex = ci
		}
		for co := ci + 1; co < m.K; co++ {
			distance := m.Distance(m.centers[ci], m.centers[co])
			if distance < minDistance {
				minIndex = ci
				minDistance = distance
			}
		}
	}
	if minDistance < m.data.maxDistances[maxIndex] {
		m.data.changes = m.dataSize
		m.centers[minIndex] = m.data.maxPoints[maxIndex]
	}
}

func (m *KMeans) Partition() (*KMeans, error) { // init centers using min/max per dimension
	m.data.points = make([]uint64, m.dataSize)
	m.data.changes = 1

	for i := 0; m.data.changes > 0; i++ {
		m.data.changes = 0
		m.data.cc = make([][]uint64, m.K)
		m.data.maxDistances = make([]float32, m.K)
		m.data.maxPoints = make([][]float32, m.K)
		for j := range m.data.cc {
			m.data.cc[j] = make([]uint64, 0)
		}

		m.recluster()
		m.resortOnEmptySets()
		if m.data.changes > 0 {
			m.recalcCenters()
		}

		/*if m.data.changes == 0 {
			m.spreadCenters()
		}*/

		if m.stopCondition(i) {
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
