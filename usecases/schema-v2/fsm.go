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

var (
	errClassNotFound = errors.New("class not found")
	errShardNotFound = errors.New("shard not found")
)

type fsm struct {
	sync.RWMutex
	Classes map[string]*metaClass `json:"classes"`
}

type metaClass struct {
	Class    models.Class
	Sharding sharding.State
}

func NewFSM(nClasses int) fsm {
	return fsm{
		Classes: make(map[string]*metaClass, nClasses),
	}
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	log.Println("restoring snapshot")

	defer func() {
		if err2 := rc.Close(); err2 != nil {
			log.Printf("restore snapshot: close reader: %v\n", err2)
		}
	}()
	// TODO: restore class embedded sub types (see repo.schema.store)
	m := make(map[string]*metaClass, 32)
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return fmt.Errorf("restore snapshot: decode json: %v", err)
	}
	f.Classes = m
	return nil
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
		return Response{
			Error: f.addClass(models.Class{}, sharding.State{}),
		}
	case CMD_UPDATE_CLASS:
		return Response{
			Error: f.updateClass(nil, nil),
		}
	case CMD_DELETE_CLASS:
		f.deleteClass(cmd.Class)
		return Response{}

	case CMD_ADD_PROPERTY:
		return Response{
			Error: f.addProperty(cmd.Class, nil),
		}
	case CMD_ADD_TENANT:
		return Response{
			Error: f.addTenants(cmd.Class, nil),
		}
	case CMD_UPDATE_TENANT:
		_, err := f.updateTenants(cmd.Class, nil)
		return Response{
			Error: err,
		}
	case CMD_DELETE_TENANT:
		return Response{
			Error: f.deleteTenants(cmd.Class, nil),
		}
	default:
		log.Println("unknown command ", cmd)
	}
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return f, nil
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (f *fsm) Persist(sink raft.SnapshotSink) (err error) {
	log.Println("persisting snapshot")
	f.Lock()
	defer f.Unlock()
	defer sink.Close()

	err = json.NewEncoder(sink).Encode(f.Classes)

	// should we cal cancel if err != nil
	log.Println("persisting snapshot done", err)
	return err
}

// Release is invoked when we are finished with the snapshot.
func (*fsm) Release() {
	log.Println("snapshot has been successfully created")
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

func (f *fsm) addProperty(class string, p *models.Property) error {
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
	dest[len(src)] = p
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

var _ raft.FSM = &fsm{}
