package schemav2

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	// tcpMaxPool controls how many connections we will pool
	tcpMaxPool = 3

	// tcpTimeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	tcpTimeout = 10 * time.Second

	raftDBName         = "raft.db"
	logCacheCapacity   = 128
	nRetainedSnapShots = 1
)

type Config struct {
	WorkDir  string // raft working directory
	NodeID   string
	Host     string
	RaftPort string
}

type Candidate struct {
	ID       string
	Address  string
	NonVoter bool
}

func (f *fsm) Open() error {
	fmt.Println("bootstrapping started")

	if err := os.Mkdir(f.raftDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", f.raftDir, err)
	}

	// log store
	logStore, err := raftbolt.NewBoltStore(filepath.Join(f.raftDir, raftDBName))
	if err != nil {
		return fmt.Errorf("raft: bolt db: %w", err)
	}
	// log cache
	logCache, err := raft.NewLogCache(logCacheCapacity, logStore)
	if err != nil {
		return fmt.Errorf("raft: log cache: %w", err)
	}
	// file snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(f.raftDir, nRetainedSnapShots, os.Stdout)
	if err != nil {
		return fmt.Errorf("raft: file snapshot store: %w", err)
	}

	// tcp transport
	address := fmt.Sprintf("%s:%s", f.host, f.raftPort)
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return fmt.Errorf("net.ResolveTCPAddr address=%v error=%w", address, err)
	}

	transport, err := raft.NewTCPTransport(address, tcpAddr, tcpMaxPool, tcpTimeout, os.Stdout)
	if err != nil {
		return fmt.Errorf("raft.NewTCPTransport  address=%v tcpAddress=%v maxPool=%v timeOut=%v: %w", address, tcpAddr, tcpMaxPool, tcpTimeout, err)
	}
	log.Printf("raft.NewTCPTransport  address=%v tcpAddress=%v maxPool=%v timeOut=%v\n", address, tcpAddr, tcpMaxPool, tcpTimeout)

	// raft node
	raftNodeConfig := raft.DefaultConfig()
	raftNodeConfig.LocalID = raft.ServerID(f.nodeID)
	raftNodeConfig.SnapshotThreshold = 100
	raftNode, err := raft.NewRaft(raftNodeConfig, f, logCache, logStore, snapshotStore, transport)
	if err != nil {
		return fmt.Errorf("raft.NewRaft %v %w", address, err)
	}

	// cluster
	clusterConfig := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(f.nodeID),
				Address: transport.LocalAddr(),
			},
		},
	}
	raftNode.BootstrapCluster(clusterConfig)


	fmt.Printf("bootstrapping done, %v\n", f)
	return nil
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (f *fsm) Apply(l *raft.Log) interface{} {
	if l.Type != raft.LogCommand {
		log.Printf("%v is not a log command\n", l.Type)
		return nil
	}
	cmd := Request{}
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		log.Printf("apply: unmarshal command %v\n", err)
		return nil
	}
	log.Printf("apply: op=%v key=%v value=%v", cmd.Operation, cmd.Class, cmd.Value)

	switch cmd.Operation {
	case CMD_ADD_CLASS:
		req := cmd.Value.(RequestAddClass)
		return Response{
			Error: f.addClass(req.Class, req.State),
		}
	case CMD_UPDATE_CLASS:
		req := cmd.Value.(RequestUpdateClass)
		return Response{
			Error: f.updateClass(req.Class, req.State),
		}
	case CMD_DELETE_CLASS:
		f.deleteClass(cmd.Class)
		return Response{}

	case CMD_ADD_PROPERTY:
		req := cmd.Value.(RequestAddProperty)
		return Response{
			Error: f.addProperty(cmd.Class, req.Property),
		}
	case CMD_ADD_TENANT:
		req := cmd.Value.(map[string]sharding.Physical)
		return Response{
			Error: f.addTenants(cmd.Class, req),
		}
	case CMD_UPDATE_TENANT:
		_, err := f.updateTenants(cmd.Class, cmd.Value.([]TenantUpdate))
		return Response{
			Error: err,
		}
	case CMD_DELETE_TENANT:
		names := cmd.Value.([]string)
		return Response{
			Error: f.deleteTenants(cmd.Class, names),
		}
	default:
		log.Println("unknown command ", cmd)
	}
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	log.Println("persisting snapshot")
	f.RLock()
	defer f.RUnlock()
	return f.Classes, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	log.Println("restoring snapshot")
	f.Lock()
	defer f.Unlock()
	f.Classes.Restore(rc)
	return nil
}
