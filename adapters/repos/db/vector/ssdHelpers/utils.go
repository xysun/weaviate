package ssdhelpers

import (
	"context"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type DistanceFunction func([]float32, []float32) float32
type (
	VectorForID      func(ctx context.Context, id uint64) ([]float32, error)
	MultiVectorForID func(ctx context.Context, ids []uint64) ([][]float32, error)
)

type DistanceProvider interface {
	Distance(vec1, vec2 []float32) float32
	Aggregate(d1, d2 float32) float32
}

type L2DistanceProvider struct {
	distancer distancer.L2SquaredProvider
}

func NewL2DistanceProvider() *L2DistanceProvider {
	return &L2DistanceProvider{
		distancer: distancer.NewL2SquaredProvider(),
	}
}

func (dp L2DistanceProvider) Distance(x, y []float32) float32 {
	d, _, _ := dp.distancer.SingleDist(x, y)
	return d
}

func (dp L2DistanceProvider) Aggregate(d1, d2 float32) float32 {
	return d1 + d2
}

type CosineDistanceProvider struct {
	distancer distancer.CosineDistanceProvider
}

func NewCosineDistanceProvider() *CosineDistanceProvider {
	return &CosineDistanceProvider{
		distancer: distancer.NewCosineDistanceProvider(),
	}
}

func (dp CosineDistanceProvider) Distance(x, y []float32) float32 {
	d, _, _ := dp.distancer.SingleDist(x, y)
	return d
}

func (dp CosineDistanceProvider) Aggregate(d1, d2 float32) float32 {
	return 1 - (2 - d1 - d2)
}

type PQDistanceProvider struct {
	pq         *ProductQuantizer
	distancer  DistanceProvider
	dimensions int
	typeTxt    string
}

func NewCosinePQDistanceProvider(pq *ProductQuantizer) PQDistanceProvider {
	return PQDistanceProvider{
		pq:        pq,
		distancer: NewCosineDistanceProvider(),
		typeTxt:   "pq-cosine-dot",
	}
}

func NewL2PQDistanceProvider(pq *ProductQuantizer) PQDistanceProvider {
	return PQDistanceProvider{
		pq:        pq,
		distancer: NewL2DistanceProvider(),
		typeTxt:   "pq-l2-squared",
	}
}

func (dp PQDistanceProvider) DistanceBetweenNodes(x, y []byte) (float32, bool, error) {
	return dp.pq.DistanceBetweenNodes(x, y), true, nil
}

func (dp PQDistanceProvider) DistanceBetweenNodeAndVector(x []float32, y []byte) (float32, bool, error) {
	return dp.pq.DistanceBetweenNodeAndVector(x, y), true, nil
}

func (dp PQDistanceProvider) SingleDist(x, y []float32) (float32, bool, error) {
	return dp.distancer.Distance(x, y), true, nil
}

func (d PQDistanceProvider) Type() string {
	return d.typeTxt
}

func (d PQDistanceProvider) New(a []float32) distancer.Distancer {
	lut := d.pq.CenterAt(a)
	return &PQDistancer{
		x:         a,
		distancer: d.distancer,
		pq:        d.pq,
		lut:       lut,
	}
}

type PQDistancer struct {
	x         []float32
	distancer DistanceProvider
	pq        *ProductQuantizer
	lut       *DistanceLookUpTable
}

func (d *PQDistancer) Distance(x []float32) (float32, bool, error) {
	return d.distancer.Distance(d.x, x), true, nil
}

func (d *PQDistancer) DistanceToNode(x []byte) (float32, bool, error) {
	return d.pq.Distance(x, d.lut), true, nil
}

func Contains(elements []uint64, x uint64) bool {
	for _, e := range elements {
		if e == x {
			return true
		}
	}
	return false
}

func Float32sFromBytes(data []byte, results []float32) {
	float32sFromBytes(data, results)
}

func BytesFromFloat32s(source []float32, data []byte) {
	bytesFromFloat32s(source, data)
}

type L2PQDistanceProvider struct {
	pq         *ProductQuantizer
	distancer  *L2DistanceProvider
	dimensions int
}
