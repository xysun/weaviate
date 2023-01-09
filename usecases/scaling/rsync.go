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
	client          nodeClient
	clusterState    clusterState
	persistenceRoot string
}

func newRSync(nodes nodeClient, cluster clusterState, rootPath string) *rsync {
	return &rsync{client: nodes, clusterState: cluster, persistenceRoot: rootPath}
}

func (r rsync) Push(ctx context.Context, shardsBackups []backup.ShardDescriptor, dist ShardDist, className string) error {
	var g errgroup.Group
	for _, desc := range shardsBackups {
		shardName := desc.Name
		additions := dist[shardName]
		desc := desc
		g.Go(func() error {
			return r.PushShard(ctx, className, desc, additions)
		})

	}
	return g.Wait()
}

func (r *rsync) PushShard(ctx context.Context, className string, desc backup.ShardDescriptor, nodes []string) error {
	// Iterate over the new target nodes and copy files
	for _, targetNode := range nodes {
		host, ok := r.clusterState.NodeHostname(targetNode)
		if !ok {
			return fmt.Errorf("%w: %q", ErrUnresolvedName, targetNode)
		}
		if err := r.client.CreateShard(ctx, host, className, desc.Name); err != nil {
			return fmt.Errorf("create new shard on remote node: %w", err)
		}

		// Transfer each file that's part of the backup.
		for _, file := range desc.Files {
			err := r.PutFile(ctx, file, host, className, desc.Name)
			if err != nil {
				return fmt.Errorf("copy files to remote node: %w", err)
			}
		}

		// Transfer shard metadata files
		err := r.PutFile(ctx, desc.ShardVersionPath, host, className, desc.Name)
		if err != nil {
			return fmt.Errorf("copy shard version to remote node: %w", err)
		}

		err = r.PutFile(ctx, desc.DocIDCounterPath, host, className, desc.Name)
		if err != nil {
			return fmt.Errorf("copy index counter to remote node: %w", err)
		}

		err = r.PutFile(ctx, desc.PropLengthTrackerPath, host, className, desc.Name)
		if err != nil {
			return fmt.Errorf("copy prop length tracker to remote node: %w", err)
		}

		// Now that all files are on the remote node's new shard, the shard needs
		// to be reinitialized. Otherwise, it would not recognize the files when
		// serving traffic later.
		if err := r.client.ReInitShard(ctx, host, className, desc.Name); err != nil {
			return fmt.Errorf("create new shard on remote node: %w", err)
		}
	}
	return nil
}

func (r *rsync) PutFile(ctx context.Context, sourceFileName string,
	hostname, className, shardName string,
) error {
	absPath := filepath.Join(r.persistenceRoot, sourceFileName)
	f, err := os.Open(absPath)
	if err != nil {
		return fmt.Errorf("open file %q for reading: %w", absPath, err)
	}

	return r.client.PutFile(ctx, hostname, className, shardName, sourceFileName, f)
}
