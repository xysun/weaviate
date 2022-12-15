package hnswPq

import (
	"context"
	"sync"

	"github.com/semi-technologies/weaviate/adapters/repos/db"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	testinghelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/testingHelpers"
	"github.com/semi-technologies/weaviate/entities/schema"
)

type HnswPq struct {
	hnsw    db.VectorIndex
	pq      *ssdhelpers.ProductQuantizer
	vectors [][]float32
	encoded [][]byte
	uc      hnsw.UserConfig
}

func NewHnswPq(cfg hnsw.Config, uc hnsw.UserConfig, pqCfg ssdhelpers.PQConfig) *HnswPq {
	pq := ssdhelpers.NewProductQuantizer(pqCfg.Segments, pqCfg.Segments, pqCfg.Distance, pqCfg.VectorForIDThunk, pqCfg.Dimensions, pqCfg.Size, pqCfg.EncoderType)
	cfg.DistanceProvider = ssdhelpers.NewL2PQDistanceProvider(pq)
	index := &HnswPq{
		pq: pq,
	}
	uc.VectorCacheMaxObjects = 0
	cfg.VectorBytes = index.encodedVector
	uc.Compressed = false
	index.uc = uc
	index.hnsw, _ = hnsw.New(cfg, uc)
	index.vectors = make([][]float32, pqCfg.Size)
	return index
}

func (i *HnswPq) encodedVector(id uint64) []byte {
	return i.encoded[id]
}

func (i *HnswPq) Compress() {
	i.pq.Fit()
	i.encoded = make([][]byte, 1000000)
	testinghelpers.Concurrently(uint64(len(i.vectors)), func(_, index uint64, _ *sync.Mutex) {
		encoded := i.pq.Encode(i.vectors[index])
		i.encoded[index] = encoded
	})
	i.uc.Compressed = true
	i.hnsw.UpdateUserConfig(i.uc)
	i.vectors = nil
}

func (i *HnswPq) add(id uint64, vector []float32) error {
	i.vectors[id] = vector
	i.hnsw.Add(id, vector)
	return nil
}

func (i *HnswPq) addCompressed(id uint64, vector []float32) error {
	encoded := i.pq.Encode(vector)
	i.encoded[id] = encoded
	return i.hnsw.Add(id, vector)
}

func (i *HnswPq) Add(id uint64, vector []float32) error {
	// no compression (concurrently)
	//  10000/  10000         -> 1.0000  1649.51 ~2.8s
	// 100000/ 100000         -> 0.9996  2863.35 ~40s
	//1000000/1000000         -> 0.9970  5185.84 11m38.317806375s

	//  10000/  10000 (reuse) -> 0.9946  3959.35 14.724077042s
	// 100000/ 100000         -> 0.9916  7261.51 17m13.529016167s
	//1000000/1000000         -> 0.9890 12045.14 4h42m36.46814325s
	//1000000/ 100000         -> 0.9878 11646.39 4h43m37.376247458s
	//1000000/ 100000 (reuse) -> 0.9872 12590.88 3h35m27.724821625s
	if !i.uc.Compressed {
		return i.add(id, vector)
	}

	return i.addCompressed(id, vector)
}

func (i *HnswPq) Delete(id uint64) error {
	return i.hnsw.Delete(id)
}

func (i *HnswPq) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	return i.hnsw.SearchByVector(vector, k, allow)
}

func (i *HnswPq) SearchByVectorDistance(vector []float32, dist float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	return i.hnsw.SearchByVectorDistance(vector, dist, maxLimit, allow)
}

func (i *HnswPq) UpdateUserConfig(updated schema.VectorIndexConfig) error {
	//handle updates... for now just compress
	return i.hnsw.UpdateUserConfig(updated)
}

func (i *HnswPq) Drop(ctx context.Context) error {
	return i.hnsw.Drop(ctx)
}

func (i *HnswPq) Flush() error {
	return i.hnsw.Flush()
}

func (i *HnswPq) Shutdown(ctx context.Context) error {
	return i.hnsw.Shutdown(ctx)
}

func (i *HnswPq) PauseMaintenance(ctx context.Context) error {
	return i.hnsw.PauseMaintenance(ctx)
}

func (i *HnswPq) SwitchCommitLogs(ctx context.Context) error {
	return i.hnsw.SwitchCommitLogs(ctx)
}

func (i *HnswPq) ListFiles(ctx context.Context) ([]string, error) {
	return i.hnsw.ListFiles(ctx)
}

func (i *HnswPq) ResumeMaintenance(ctx context.Context) error {
	return i.hnsw.ResumeMaintenance(ctx)
}
