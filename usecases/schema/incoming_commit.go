//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
)

func (m *Manager) handleCommit(ctx context.Context, tx *cluster.Transaction) error {
	switch tx.Type {
	case AddClass:
		return m.handleAddClassCommit(ctx, tx)
	case AddProperty:
		return m.handleAddPropertyCommit(ctx, tx)
	case DeleteClass:
		return m.handleDeleteClassCommit(ctx, tx)
	case UpdateClass:
		return m.handleUpdateClassCommit(ctx, tx)
	case AddPartitions:
		return m.handleAddPartitionsCommit(ctx, tx)
	case DeleteTenants:
		return m.handleDeletePartitionsCommit(ctx, tx)
	default:
		return errors.Errorf("unrecognized commit type %q", tx.Type)
	}
}

func (m *Manager) handleTxResponse(ctx context.Context,
	tx *cluster.Transaction,
) error {
	switch tx.Type {
	case ReadSchema:
		tx.Payload = ReadSchemaPayload{
			Schema: &m.state,
		}
		return nil
	// TODO
	default:
		// silently ignore. Not all types support responses
		return nil
	}
}

func (m *Manager) handleAddClassCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	pl, ok := tx.Payload.(AddClassPayload)
	if !ok {
		m.Unlock()
		return errors.Errorf("expected commit payload to be AddClassPayload, but got %T",
			tx.Payload)
	}

	err := m.handleAddClassCommitAndParse(ctx, &pl)
	m.Unlock()
	if err != nil {
		return err
	}
	// call to migrator needs to be outside the lock that is set in addClass
	return m.migrator.AddClass(ctx, pl.Class, pl.State)
}

func (m *Manager) handleAddClassCommitAndParse(ctx context.Context, pl *AddClassPayload) error {
	err := m.parseShardingConfig(ctx, pl.Class)
	if err != nil {
		return err
	}

	err = m.parseVectorIndexConfig(ctx, pl.Class)
	if err != nil {
		return err
	}

	pl.State.SetLocalName(m.clusterState.LocalName())
	return m.addClassApplyChanges(ctx, pl.Class, pl.State)
}

func (m *Manager) handleAddPropertyCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	pl, ok := tx.Payload.(AddPropertyPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be AddPropertyPayload, but got %T",
			tx.Payload)
	}

	return m.addClassPropertyApplyChanges(ctx, pl.ClassName, pl.Property)
}

func (m *Manager) handleDeleteClassCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	pl, ok := tx.Payload.(DeleteClassPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be DeleteClassPayload, but got %T",
			tx.Payload)
	}

	return m.deleteClassApplyChanges(ctx, pl.ClassName, pl.Force)
}

func (m *Manager) handleUpdateClassCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	pl, ok := tx.Payload.(UpdateClassPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be UpdateClassPayload, but got %T",
			tx.Payload)
	}

	if err := m.parseVectorIndexConfig(ctx, pl.Class); err != nil {
		return err
	}

	if err := m.parseShardingConfig(ctx, pl.Class); err != nil {
		return err
	}

	return m.updateClassApplyChanges(ctx, pl.ClassName, pl.Class, pl.State)
}

func (m *Manager) handleAddPartitionsCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	req, ok := tx.Payload.(AddPartitionsPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be AddPartitions, but got %T",
			tx.Payload)
	}
	cls, st := m.getClassByName(req.ClassName), m.ShardingState(req.ClassName)
	if cls == nil || st == nil {
		return fmt.Errorf("class %q: %w", req.ClassName, ErrNotFound)
	}

	return m.onAddPartitions(ctx, st, cls, req)
}

func (m *Manager) handleDeletePartitionsCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	req, ok := tx.Payload.(DeleteTenantsPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be DeletePartitions, but got %T",
			tx.Payload)
	}
	cls := m.getClassByName(req.ClassName)
	if cls == nil {
		m.logger.WithField("action", "delete_tenants").
			WithField("class", req.ClassName).Warn("class not found")
		return nil
	}

	return m.onDeletePartitions(ctx, cls, req)
}
