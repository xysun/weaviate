package hnsw

import (
	"context"
	"encoding/binary"
	"log"
	"os"
	"path"

	"github.com/pkg/errors"
)

func (h *hnsw) buildDiskSnapshot() error {
	snapshotPath := path.Join(h.rootPath, h.id+ ".disksnapshot")

	ok, err := Exists(snapshotPath)
	if err != nil {
		return err
	}

	if ok {
		log.Print("snapshot exists not doing anything")
	}

	log.Print("snapshot does not exist, building now")


	if err := h.writeDiskSnapshotMetadata(snapshotPath); err != nil {
		return err
	}

	for i, node := range h.nodes {
		if node == nil {
			continue
		}
		// ignore locks
		// ignore higher levels
		cons := node.connections[0]

		log.Printf("node %d with connections %v", i, cons)

	}

	return nil
}

func Exists(name string) (bool, error) {
    _, err := os.Stat(name)
    if err == nil {
        return true, nil
    }
    if errors.Is(err, os.ErrNotExist) {
        return false, nil
    }
    return false, err
}

func (h *hnsw) writeDiskSnapshotMetadata(rootPath string) error {

	maxConn := uint32(h.maximumConnectionsLayerZero)
	log.Printf("max connections: %d", maxConn)
	vec, err := h.vectorForID(context.Background(), 0)
	if err != nil{
		return err
	}

	dims := uint32(len(vec))
	log.Printf("vector dims: %d", dims)

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], maxConn)
	binary.LittleEndian.PutUint32(buf[4:8], dims)
	log.Printf("buf %v", buf)

	f, err := os.Create(rootPath + ".metadata")
	if err != nil {
		return err
	}

	f.Write(buf)
	return f.Close()
}
