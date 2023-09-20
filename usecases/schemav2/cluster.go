package schemav2

import (
	"fmt"

	"github.com/hashicorp/raft"
)

type Cluster struct {
	*raft.Raft
}

func NewCluster(raft *raft.Raft) Cluster {
	return Cluster{raft}
}

func (h Cluster) Join(id, addr string, voter bool) error {
	if h.Raft.State() != raft.Leader {
		return fmt.Errorf("node %v is not the leader %v", id, raft.Leader)
	}
	cfg := h.Raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return fmt.Errorf("get raft config: %w", err)
	}
	var fut raft.IndexFuture
	if voter {
		fut = h.Raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
		if err := fut.Error(); err != nil {
			return fmt.Errorf("add voter: %w", err)
		}
	} else {
		fut = h.Raft.AddNonvoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
		if err := fut.Error(); err != nil {
			return fmt.Errorf("add non voter: %w", err)
		}
	}
	return nil
}

func (h Cluster) Remove(id string) error {
	if h.Raft.State() != raft.Leader {
		return fmt.Errorf("node %v is not the leader %v", id, raft.Leader)
	}
	cfg := h.Raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return fmt.Errorf("get raft config: %w", err)
	}

	fut := h.Raft.RemoveServer(raft.ServerID(id), 0, 0)
	if err := fut.Error(); err != nil {
		return fmt.Errorf("add voter: %w", err)
	}
	return nil
}

func (h Cluster) Stats() map[string]string {
	return h.Raft.Stats()
}
