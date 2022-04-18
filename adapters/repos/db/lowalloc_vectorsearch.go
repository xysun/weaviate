package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (db *DB) PreallocatedVectorSearch(ctx context.Context, params traverser.GetParams,
	results *search.RawResults) error {

	if params.SearchVector == nil {
		return errors.Errorf("must contain search vector")
	}

	totalLimit, err := db.getTotalLimit(params.Pagination, params.AdditionalProperties)
	if err != nil {
		return errors.Wrapf(err, "invalid pagination params")
	}

	idx := db.GetIndex(schema.ClassName(params.ClassName))
	if idx == nil {
		return fmt.Errorf("tried to browse non-existing index for %s", params.ClassName)
	}

	targetDist := extractDistanceFromParams(params)

	shardNames := idx.getSchema.ShardingState(idx.Config.ClassName.String()).
		AllPhysicalShards()

	if len(shardNames) != 1 {
		return errors.Errorf("limited to exactly one shard for now")
	}

	shardName := shardNames[0]
	local := idx.getSchema.
		ShardingState(idx.Config.ClassName.String()).
		IsShardLocal(shardName)

	if !local {
		return errors.Errorf("remote shards not supported yet")
	}

	shard := idx.Shards[shardName]
	if err := shard.preallocatedObjectVectorSearch(ctx, params, results,
		totalLimit, targetDist); err != nil {
		return err
	}

	return nil
}

func (s *Shard) preallocatedObjectVectorSearch(ctx context.Context,
	params traverser.GetParams, results *search.RawResults, totalLimit int,
	targetDist float32) error {

	if params.Filters != nil {
		return errors.Errorf("filtered search not supported yet")
	}

	if totalLimit < 0 {
		return errors.Errorf("limit must be explicitly provided")
	}

	ids, dists, err := s.vectorIndex.SearchByVector(params.SearchVector, totalLimit, nil)
	if err != nil {
		return err
	}

	if len(ids) == 0 {
		return nil
	}

	_ = dists

	if err := s.rawObjectsByDocID(results, ids); err != nil {
		return err
	}

	return nil
}

func (s *Shard) rawObjectsByDocID(results *search.RawResults, ids []uint64,
) error {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return errors.Errorf("objects bucket not found")
	}

	for _, id := range ids {
		keyBuf := bytes.NewBuffer(nil)
		binary.Write(keyBuf, binary.LittleEndian, &id)
		docIDBytes := keyBuf.Bytes()
		res, err := bucket.GetBySecondary(0, docIDBytes)
		if err != nil {
			return err
		}

		if res == nil {
			continue
		}

		results.Add(res)
	}

	return nil
}
