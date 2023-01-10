//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package scaler

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// ErrUnresolvedName cannot resolve the host address of a node
var ErrUnresolvedName = errors.New("cannot resolve node name")

type Scaler struct {
	// the scaleOutManager needs to read and updated the sharding state of a
	// class. It can access it through the schemaManager
	schemaManager SchemaManager

	// get information about which nodes are in the cluster
	clusterState clusterState

	backerUpper BackerUpper

	nodes client

	logger logrus.FieldLogger

	persistenceRoot string
}

type clusterState interface { // TODO cluster
	// AllNames() returns all the node names (not the hostnames!) including the
	// local one
	AllNames() []string
	LocalName() string
	NodeHostname(name string) (string, bool)
}

type BackerUpper interface {
	// ShardsBackup returns class backup descriptor for a list of shards
	ShardsBackup(_ context.Context, id, class string, shards []string) (backup.ClassDescriptor, error)
	// ReleaseBackup releases the backup with the specified id
	ReleaseBackup(ctx context.Context, id, className string) error
}

func New(clusterState clusterState, backerUpper BackerUpper,
	nodeClient client, logger logrus.FieldLogger, persistenceRoot string,
) *Scaler {
	return &Scaler{
		clusterState:    clusterState,
		backerUpper:     backerUpper,
		nodes:           nodeClient,
		logger:          logger,
		persistenceRoot: persistenceRoot,
	}
}

type SchemaManager interface {
	ShardingState(class string) *sharding.State
}

func (s *Scaler) SetSchemaManager(sm SchemaManager) {
	s.schemaManager = sm
}

// Scale scales in/out
// it returns the updated sharding state if successful. The caller must then
// make sure to broadcast that state to all nodes as part of the "update"
// transaction.
func (s *Scaler) Scale(ctx context.Context, className string,
	updated sharding.Config, prevReplFactor, newReplFactor int64,
) (*sharding.State, error) {
	// First identify what the sharding state was before this change. This is
	// mainly to be able to compare the diff later, so we know where we need to
	// make changes
	ssBefore := s.schemaManager.ShardingState(className)
	if ssBefore == nil {
		return nil, errors.Errorf("no sharding state for class %q", className)
	}
	if newReplFactor > prevReplFactor {
		return s.scaleOut(ctx, className, ssBefore, updated, newReplFactor)
	}

	if newReplFactor < prevReplFactor {
		return s.scaleIn(ctx, className, updated)
	}

	return nil, nil
}

// scaleOut is a relatively primitive implementation that takes a shard and
// copies it onto another node. Then it returns the new and updated sharding
// state where the node-association is updated to point to all nodes where the
// shard can be found.
//
// This implementation is meant as a temporary one and should probably be
// replaced with something more sophisticated. The main issues are:
//
//   - Everything is synchronous. This blocks the users request until all data
//     is copied which is not great.
//   - Everything is sequential. A lot of the things could probably happen in
//     parallel
//
// Follow the in-line comments to see how this implementation achieves scaling
// out
func (s *Scaler) scaleOut(ctx context.Context, className string, ssBefore *sharding.State,
	updated sharding.Config, replFactor int64,
) (*sharding.State, error) {
	// Create a deep copy of the old sharding state, so we can start building the
	// updated state. Because this is a deep copy we don't risk leaking our
	// changes to anyone else. We can return the changes in the end where the
	// caller can then make sure to broadcast the new state to the cluster.
	ssAfter := ssBefore.DeepCopy()
	ssAfter.Config = updated

	// Identify all shards of the class and adjust the replicas. After this is
	// done, the affected shards now belong to more nodes than they did before.
	for name, shard := range ssAfter.Physical {
		shard.AdjustReplicas(int(replFactor), s.clusterState)
		ssAfter.Physical[name] = shard
	}
	lDist, nodeDist := distributions(ssBefore, &ssAfter)
	// However, so far we have only updated config, now we also need to actually
	// copy files.
	g, ctx := errgroup.WithContext(ctx)
	// resolve hosts beforehand
	nodes := nodeDist.nodes()
	hosts, err := hosts(nodes, s.clusterState)
	if err != nil {
		return nil, err
	}
	for i, node := range nodes {
		dist := nodeDist[node]
		i := i
		g.Go(func() error {
			err := s.nodes.IncreaseReplicationFactor(ctx, hosts[i], className, dist)
			if err != nil {
				return fmt.Errorf("increase replication factor for class %q on node %q: %w", className, nodes[i], err)
			}
			return nil
		})
	}

	g.Go(func() error {
		if err := s.LocalScaleOut(ctx, className, lDist); err != nil {
			return fmt.Errorf("increase local replication factor: %w", err)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Finally, return sharding state back to schema manager. The schema manager
	// will then broadcast this updated state to the cluster. This is essentially
	// what will take the new replication shards live: On the new nodes, if
	// traffic is incoming, IsShardLocal() would have returned false before. But
	// now that a copy of the local shard is present it will return true and
	// serve the traffic.
	return &ssAfter, nil
}

// LocalScaleOut iterates over every shard that this class has. This is the
// meat&bones of this implementation. For each shard, we're roughly doing the
// following:
//   - Create shards backup, so the shards are safe to copy
//   - Figure out the copy targets (i.e. each node that is part of the after
//     state, but wasn't part of the before state yet)
//   - Create an empty shard on the target node
//   - Copy over all files from the backup
//   - ReInit the shard to recognize the copied files
//   - Release the single-shard backup
func (s *Scaler) LocalScaleOut(ctx context.Context,
	className string, dist ShardDist,
) error {
	if len(dist) < 1 {
		return nil
	}
	// Create backup of the sin
	bakID := fmt.Sprintf("_internal_scaler_%s", uuid.New().String()) // todo better name
	bak, err := s.backerUpper.ShardsBackup(ctx, bakID, className, dist.shards())
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}

	defer func() {
		err := s.backerUpper.ReleaseBackup(context.Background(), bakID, className)
		if err != nil {
			s.logger.WithField("scaler", "releaseBackup").WithField("class", className).Error(err)
		}
	}()
	rsync := newRSync(s.nodes, s.clusterState, s.persistenceRoot)
	return rsync.Push(ctx, bak.Shards, dist, className)
}

func (s *Scaler) scaleIn(ctx context.Context, className string,
	updated sharding.Config,
) (*sharding.State, error) {
	return nil, errors.Errorf("scaling in (reducing replica count) not supported yet")
}
