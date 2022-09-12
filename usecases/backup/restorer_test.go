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

package backup

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// ErrAny represent a random error
var ErrAny = errors.New("any error")

func TestRestoreStatus(t *testing.T) {
	t.Parallel()
	var (
		backendType = "s3"
		id          = "1234"
		m           = createManager(nil, nil, nil)
		ctx         = context.Background()
		starTime    = time.Now().UTC()
		path        = "bucket/backups/123"
	)
	// initial state
	_, err := m.RestorationStatus(ctx, nil, backendType, id)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("must return an error if backup doesn't exist")
	}
	// active state
	m.restorer.lastStatus.reqStat = reqStat{
		Starttime: starTime,
		ID:        id,
		Status:    backup.Transferring,
		path:      path,
	}
	st, err := m.RestorationStatus(ctx, nil, backendType, id)
	if err != nil {
		t.Errorf("get active status: %v", err)
	}
	expected := RestoreStatus{Path: path, StartedAt: starTime, Status: backup.Transferring}
	if expected != st {
		t.Errorf("get active status: got=%v want=%v", st, expected)
	}
	// cached status
	m.restorer.lastStatus.reset()
	st.CompletedAt = starTime
	m.restoreStatusMap.Store("s3/"+id, st)
	st, err = m.RestorationStatus(ctx, nil, backendType, id)
	if err != nil {
		t.Errorf("fetch status from map: %v", err)
	}
	expected.CompletedAt = starTime
	if expected != st {
		t.Errorf("fetch status from map got=%v want=%v", st, expected)
	}
}

func TestRestoreRequestValidation(t *testing.T) {
	var (
		cls         = "MyClass"
		backendName = "s3"
		rawbytes    = []byte("hello")
		id          = "1234"
		timept      = time.Now().UTC()
		m           = createManager(nil, nil, nil)
		ctx         = context.Background()
		path        = "bucket/backups"
		req         = &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{cls},
			Exclude: []string{},
		}
	)
	meta := backup.BackupDescriptor{
		ID:            id,
		StartedAt:     timept,
		Version:       "1",
		ServerVersion: "1",
		Status:        string(backup.Success),
		Classes: []backup.ClassDescriptor{{
			Name: cls, Schema: rawbytes, ShardingState: rawbytes,
		}},
	}

	t.Run("NonEmptyIncludeAndExclude", func(t *testing.T) {
		_, err := m.Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{cls},
			Exclude: []string{cls},
		})
		assert.NotNil(t, err)
	})
	t.Run("BackendFailure", func(t *testing.T) { //  backend provider fails
		backend := &fakeBackend{}
		m2 := createManager(nil, backend, ErrAny)
		_, err := m2.Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{cls},
			Exclude: []string{},
		})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), backendName)
	})

	t.Run("GetMetdataFile", func(t *testing.T) {
		backend := &fakeBackend{}
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(nil, ErrAny)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, backend, nil)
		_, err := m2.Restore(ctx, nil, req)
		if err == nil || !strings.Contains(err.Error(), "find") {
			t.Errorf("must return an error if it fails to get meta data: %v", err)
		}
		// meta data not found
		backend = &fakeBackend{}
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(nil, backup.ErrNotFound{})
		m3 := createManager(nil, backend, nil)

		_, err = m3.Restore(ctx, nil, req)
		if _, ok := err.(backup.ErrNotFound); !ok {
			t.Errorf("must return an error if meta data doesn't exist: %v", err)
		}
	})

	t.Run("FailedBackup", func(t *testing.T) {
		backend := &fakeBackend{}
		bytes := marshalMeta(backup.BackupDescriptor{ID: id, Status: string(backup.Failed)})
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, backend, nil)
		_, err := m2.Restore(ctx, nil, req)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), backup.Failed)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})
	t.Run("CorruptedBackupFile", func(t *testing.T) {
		backend := &fakeBackend{}
		bytes := marshalMeta(backup.BackupDescriptor{ID: id, Status: string(backup.Success)})
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, backend, nil)
		_, err := m2.Restore(ctx, nil, req)
		assert.NotNil(t, err)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Contains(t, err.Error(), "corrupted")
	})
	t.Run("WrongBackupFile", func(t *testing.T) {
		backend := &fakeBackend{}
		bytes := marshalMeta(backup.BackupDescriptor{ID: "123", Status: string(backup.Success)})
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, backend, nil)
		_, err := m2.Restore(ctx, nil, req)
		assert.NotNil(t, err)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Contains(t, err.Error(), "wrong backup file")
	})

	t.Run("UknownClass", func(t *testing.T) {
		backend := &fakeBackend{}
		bytes := marshalMeta(meta)
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, backend, nil)
		_, err := m2.Restore(ctx, nil, &BackupRequest{ID: id, Include: []string{"unknown"}})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "unknown")
	})

	t.Run("EmptyResultClassList", func(t *testing.T) { //  backup was successful but class list is empty
		backend := &fakeBackend{}
		bytes := marshalMeta(meta)
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, backend, nil)
		_, err := m2.Restore(ctx, nil, &BackupRequest{ID: id, Exclude: []string{cls}})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "empty")
	})
	t.Run("ClassAlreadyExists", func(t *testing.T) { //  one class exists already in DB
		backend := &fakeBackend{}
		sourcer := &fakeSourcer{}
		sourcer.On("ClassExists", cls).Return(true)
		bytes := marshalMeta(meta)
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(sourcer, backend, nil)
		_, err := m2.Restore(ctx, nil, &BackupRequest{ID: id})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), cls)
		uerr := backup.ErrUnprocessable{}
		if !errors.As(err, &uerr) {
			t.Errorf("error want=%v got=%v", uerr, err)
		}
	})
}

func TestManagerRestoreBackup(t *testing.T) {
	var (
		cls         = "DemoClass"
		backendName = "gcs"
		backupID    = "1"
		rawbytes    = []byte("hello")
		timept      = time.Now().UTC()
		ctx         = context.Background()
		path        = "dst/path"
	)
	meta := backup.BackupDescriptor{
		ID:            backupID,
		StartedAt:     timept,
		Version:       "1",
		ServerVersion: "1",
		Status:        string(backup.Success),
		Classes: []backup.ClassDescriptor{{
			Name: cls, Schema: rawbytes, ShardingState: rawbytes,
		}},
	}

	t.Run("AnotherBackupIsInProgress", func(t *testing.T) {
		req1 := BackupRequest{
			ID:      backupID,
			Include: []string{cls},
			Backend: backendName,
		}
		backend := &fakeBackend{}
		sourcer := &fakeSourcer{}
		sourcer.On("ClassExists", cls).Return(false)
		bytes := marshalMeta(meta)
		backend.On("GetObject", ctx, backupID, MetaDataFilename).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		// simulate work by delaying return of SourceDataPath()
		backend.On("SourceDataPath").Return(t.TempDir()).Run(func(args mock.Arguments) { time.Sleep(time.Hour) })
		m2 := createManager(sourcer, backend, nil)
		_, err := m2.Restore(ctx, nil, &BackupRequest{ID: backupID})
		assert.Nil(t, err)
		m := createManager(sourcer, backend, nil)
		resp1, err := m.Restore(ctx, nil, &req1)
		assert.Nil(t, err)
		status1 := string(backup.Started)
		want1 := &models.BackupRestoreResponse{
			Backend: backendName,
			Classes: req1.Include,
			ID:      backupID,
			Status:  &status1,
			Path:    path,
		}
		assert.Equal(t, resp1, want1)

		resp2, err := m.Restore(ctx, nil, &req1)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "already in progress")
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Nil(t, resp2)
	})

	// t.Run("fails when init meta fails", func(t *testing.T) {
	// 	classes := []string{cls}

	// 	sourcer := &fakeSourcer{}
	// 	sourcer.On("Backupable", ctx, classes).Return(nil)
	// 	backend := &fakeBackend{}
	// 	backend.On("HomeDir", mock.Anything).Return(path)
	// 	backend.On("GetObject", ctx, backupID, MetaDataFilename).Return(nil, backup.NewErrNotFound(errors.New("not found")))
	// 	backend.On("Initialize", ctx, backupID).Return(errors.New("init meta failed"))
	// 	bm := createManager(sourcer, backend, nil)

	// 	meta, err := bm.Backup(ctx, nil, &BackupRequest{
	// 		Backend: backendName,
	// 		ID:      backupID,
	// 		Include: classes,
	// 	})

	// 	assert.Nil(t, meta)
	// 	assert.NotNil(t, err)
	// 	assert.Contains(t, err.Error(), "init")
	// 	assert.IsType(t, backup.ErrUnprocessable{}, err)
	// })
}

func marshalMeta(m backup.BackupDescriptor) []byte {
	bytes, _ := json.MarshalIndent(m, "", "")
	return bytes
}
