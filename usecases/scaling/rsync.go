package scaling

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/semi-technologies/weaviate/entities/backup"
	"golang.org/x/sync/errgroup"
)

type nodeClient interface {
	PutFile(ctx context.Context, hostName, indexName,
		shardName, fileName string, payload io.ReadSeekCloser) error

	// CreateShard creates an empty shard on the remote node.
	// This is required in order to sync files to a specific shard on the remote node.
	CreateShard(ctx context.Context,
		hostName, indexName, shardName string) error

	// ReInitShard re-initialized new shard after all files has been synced to the remote node
	// Otherwise, it would not recognize the files when
	// serving traffic later.
	ReInitShard(ctx context.Context,
		hostName, indexName, shardName string) error
	IncreaseReplicationFactor(ctx context.Context, host, class string, dist ShardDist) error
}

// rsync synchronizes shards with remote nodes
type rsync struct {
	nodes           nodeClient
	clusterState    clusterState
	persistenceRoot string
}

func newRSync(nodes nodeClient, cluster clusterState, rootPath string) *rsync {
	return &rsync{nodes: nodes, clusterState: cluster, persistenceRoot: rootPath}
}

func (r rsync) Push(ctx context.Context, shardsBackups []backup.ShardDescriptor, dist ShardDist, className string) error {
	var g errgroup.Group
	for _, desc := range shardsBackups {
		shardName := desc.Name
		additions := dist[shardName]
		desc := desc
		g.Go(func() error {
			return r.syncShard(ctx, className, desc, additions)
		})

	}
	return g.Wait()
}

func (r *rsync) syncShard(ctx context.Context, className string, desc backup.ShardDescriptor, nodes []string) error {
	// Iterate over the new target nodes and copy files
	for _, targetNode := range nodes {
		if err := r.CreateShard(ctx, targetNode, className, desc.Name); err != nil {
			return fmt.Errorf("create new shard on remote node: %w", err)
		}

		// Transfer each file that's part of the backup.
		for _, file := range desc.Files {
			err := r.PutFile(ctx, file, targetNode, className, desc.Name)
			if err != nil {
				return fmt.Errorf("copy files to remote node: %w", err)
			}
		}

		// Transfer shard metadata files
		err := r.PutFile(ctx, desc.ShardVersionPath, targetNode, className, desc.Name)
		if err != nil {
			return fmt.Errorf("copy shard version to remote node: %w", err)
		}

		err = r.PutFile(ctx, desc.DocIDCounterPath, targetNode, className, desc.Name)
		if err != nil {
			return fmt.Errorf("copy index counter to remote node: %w", err)
		}

		err = r.PutFile(ctx, desc.PropLengthTrackerPath, targetNode, className, desc.Name)
		if err != nil {
			return fmt.Errorf("copy prop length tracker to remote node: %w", err)
		}

		// Now that all files are on the remote node's new shard, the shard needs
		// to be reinitialized. Otherwise, it would not recognize the files when
		// serving traffic later.
		if err := r.ReInitShard(ctx, targetNode, className, desc.Name); err != nil {
			return fmt.Errorf("create new shard on remote node: %w", err)
		}
	}
	return nil
}

func (r *rsync) PutFile(ctx context.Context, sourceFileName string,
	targetNode, className, shardName string,
) error {
	absPath := filepath.Join(r.persistenceRoot, sourceFileName)

	hostname, ok := r.clusterState.NodeHostname(targetNode)
	if !ok {
		return fmt.Errorf("resolve hostname for node %q", targetNode)
	}

	f, err := os.Open(absPath)
	if err != nil {
		return fmt.Errorf("open file %q for reading: %w", absPath, err)
	}

	return r.nodes.PutFile(ctx, hostname, className, shardName, sourceFileName, f)
}

func (r *rsync) CreateShard(ctx context.Context,
	targetNode, className, shardName string,
) error {
	hostname, ok := r.clusterState.NodeHostname(targetNode)
	if !ok {
		return fmt.Errorf("resolve hostname for node %q", targetNode)
	}

	return r.nodes.CreateShard(ctx, hostname, className, shardName)
}

func (r *rsync) ReInitShard(ctx context.Context,
	targetNode, className, shardName string,
) error {
	hostname, ok := r.clusterState.NodeHostname(targetNode)
	if !ok {
		return fmt.Errorf("resolve hostname for node %q", targetNode)
	}

	return r.nodes.ReInitShard(ctx, hostname, className, shardName)
}
