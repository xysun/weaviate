package schemav2

import (
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/hashicorp/raft"
)

func (s snapshot) Restore(rc io.ReadCloser) error {
	log.Println("restoring snapshot")

	defer func() {
		if err2 := rc.Close(); err2 != nil {
			log.Printf("restore snapshot: close reader: %v\n", err2)
		}
	}()

	//
	// TODO: restore class embedded sub types (see repo.schema.store)
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return fmt.Errorf("restore snapshot: decode json: %v", err)
	}

	return nil
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s snapshot) Persist(sink raft.SnapshotSink) (err error) {
	defer sink.Close()

	err = json.NewEncoder(sink).Encode(s)

	// should we cal cancel if err != nil
	log.Println("persisting snapshot done", err)
	return err
}

// Release is invoked when we are finished with the snapshot.
func (s snapshot) Release() {
	log.Println("snapshot has been successfully created")
}
