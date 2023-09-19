package schemav2

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type CMD int16

const (
	CMD_ADD_CLASS CMD = iota + 1
	CMD_UPDATE_CLASS
	CMD_DELETE_CLASS
	CMD_ADD_PROPERTY
)

const (
	CMD_ADD_TENANT CMD = iota + 16
	CMD_UPDATE_TENANT
	CMD_DELETE_TENANT
)

var (
	errClassNotFound         = errors.New("class not found")
	errShardNotFound         = errors.New("shard not found")
	//errUnexpectedRequestType = errors.New("unexpected request type")
)

type snapshot map[string]*metaClass

type fsm struct {
	sync.RWMutex
	Classes snapshot `json:"classes"`
}

type metaClass struct {
	Class    models.Class
	Sharding sharding.State
}

func NewFSM(nClasses int) fsm {
	return fsm{
		Classes: make(snapshot, nClasses),
	}
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (f *fsm) Apply(l *raft.Log) interface{} {
	if l.Type != raft.LogCommand {
		log.Println("%v is not a log command", l.Type)
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

func (f *fsm) addClass(cls models.Class, ss sharding.State) error {
	f.Lock()
	defer f.Unlock()
	info := f.Classes[cls.Class]
	if info == nil {
		return errClassNotFound
	}
	f.Classes[cls.Class] = &metaClass{cls, ss}
	return nil
}

func (f *fsm) updateClass(u *models.Class, ss *sharding.State) error {
	f.Lock()
	defer f.Unlock()

	info := f.Classes[u.Class]
	if info == nil {
		return errClassNotFound
	}
	if u != nil {
		info.Class = *u
	}
	if ss != nil {
		info.Sharding = *ss
	}

	return nil
}

func (f *fsm) deleteClass(name string) {
	f.Lock()
	defer f.Unlock()
	delete(f.Classes, name)
}

func (f *fsm) addProperty(class string, p models.Property) error {
	f.Lock()
	defer f.Unlock()

	info := f.Classes[class]
	if info == nil {
		return errClassNotFound
	}

	// update all at once to prevent race condition with concurrent readers
	src := info.Class.Properties
	dest := make([]*models.Property, len(src)+1)
	copy(dest, src)
	dest[len(src)] = &p
	info.Class.Properties = dest
	return nil
}

func (f *fsm) addTenants(class string, shards map[string]sharding.Physical) error {
	f.Lock()
	defer f.Unlock()

	info := f.Classes[class]
	if info == nil {
		return errClassNotFound
	}
	for k, v := range shards {
		info.Sharding.Physical[k] = v
	}
	return nil
}

func (f *fsm) deleteTenants(class string, shards []string) error {
	f.Lock()
	defer f.Unlock()

	info := f.Classes[class]
	if info == nil {
		return errClassNotFound
	}
	for _, name := range shards {
		info.Sharding.DeletePartition(name)
	}
	return nil
}

func (f *fsm) updateTenants(class string, us []TenantUpdate) (n int, err error) {
	f.Lock()
	defer f.Unlock()

	info := f.Classes[class]
	if info == nil {
		return 0, errClassNotFound
	}
	missingShards := []string{}
	ps := info.Sharding.Physical
	for _, u := range us {
		p, ok := ps[u.Name]
		if !ok {
			missingShards = append(missingShards, u.Name)
			continue
		}
		if p.ActivityStatus() == u.Status {
			continue
		}
		copy := p.DeepCopy()
		copy.Status = u.Status
		ps[u.Name] = copy
		n++
	}
	if len(missingShards) > 0 {
		err = fmt.Errorf("%w: %v", errShardNotFound, missingShards)
	}
	return
}

type TenantUpdate struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

type Request struct {
	Operation CMD
	Class     string
	Value     interface{}
}

type Response struct {
	Error error
	Data  interface{}
}

var _ raft.FSM = &fsm{}
