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

package objects

import (
	"context"
	"fmt"
	"github.com/semi-technologies/weaviate/usecases/objects/validation"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/entities/errorcompounder"
	"github.com/semi-technologies/weaviate/entities/models"
)

// AddObjects Class Instances in batch to the connected DB
func (b *BatchManager) AddObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, fields []*string,
) (BatchObjects, error) {
	err := b.authorizer.Authorize(principal, "create", "batch/objects")
	if err != nil {
		return nil, err
	}

	unlock, err := b.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	before := time.Now()
	b.metrics.BatchInc()
	defer b.metrics.BatchOp("total_uc_level", before.UnixNano())
	defer b.metrics.BatchDec()

	return b.addObjects(ctx, principal, objects, fields)
}

func (b *BatchManager) addObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, fields []*string,
) (BatchObjects, error) {
	beforePreProcessing := time.Now()
	if err := b.validateObjectForm(objects); err != nil {
		return nil, NewErrInvalidUserInput("invalid param 'objects': %v", err)
	}

	batchObjects := b.validateObjectsConcurrently(ctx, principal, objects, fields)
	b.metrics.BatchOp("total_preprocessing", beforePreProcessing.UnixNano())

	var (
		res BatchObjects
		err error
	)

	beforePersistence := time.Now()
	defer b.metrics.BatchOp("total_persistence_level", beforePersistence.UnixNano())
	if res, err = b.vectorRepo.BatchPutObjects(ctx, batchObjects); err != nil {
		return nil, NewErrInternal("batch objects: %#v", err)
	}

	return res, nil
}

func (b *BatchManager) validateObjectForm(objects []*models.Object) error {
	if len(objects) == 0 {
		return fmt.Errorf("cannot be empty, need at least one object for batching")
	}

	return nil
}

func (b *BatchManager) validateObjectsConcurrently(ctx context.Context, principal *models.Principal,
	objects []*models.Object, fields []*string,
) BatchObjects {
	fieldsToKeep := determineResponseFields(fields)
	c := make(chan BatchObjects, len(objects))

	b.validateObjects(ctx, principal, objects, &c, fieldsToKeep)

	close(c)
	return objectsChanToSlice(c, len(objects))
}

func (b *BatchManager) validateObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, resultsC *chan BatchObjects,
	fieldsToKeep map[string]struct{},
) {
	ec := &errorcompounder.ErrorCompounder{}
	wg := new(sync.WaitGroup)

	for _, object := range objects {
		wg.Add(1)
		go b.parseObject(ctx, principal, wg, object, fieldsToKeep, ec)
	}
	wg.Wait()

	err := b.modulesProvider.UpdateVectors(ctx, objects, b.findObject, b.logger)
	ec.Add(err)

	batchObjects := BatchObjects{}
	for i, object := range objects {
		batchObjects = append(batchObjects, BatchObject{
			OriginalIndex: i,
			Err:           err,
			Object:        object,
			UUID:          object.ID,
			Vector:        object.Vector,
		})
	}
	*resultsC <- batchObjects
}

func (b *BatchManager) parseObject(ctx context.Context, principal *models.Principal, wg *sync.WaitGroup, object *models.Object,
	fieldsToKeep map[string]struct{}, ec *errorcompounder.ErrorCompounder,
) {
	defer wg.Done()

	var id strfmt.UUID
	// Auto Schema
	err := b.autoSchemaManager.autoSchema(ctx, principal, object)
	ec.Add(err)

	if object.ID == "" {
		// Generate UUID for the new object
		uid, err := generateUUID()
		id = uid
		ec.Add(err)
	} else {
		if _, err := uuid.Parse(object.ID.String()); err != nil {
			ec.Add(err)
		}
		id = object.ID
	}

	// Validate schema given in body with the weaviate schema
	s, err := b.schemaManager.GetSchema(principal)
	ec.Add(err)

	// Create Action object
	obj := &models.Object{}
	obj.LastUpdateTimeUnix = 0
	obj.ID = id
	obj.Vector = object.Vector

	if _, ok := fieldsToKeep["class"]; ok {
		obj.Class = object.Class
	}
	if _, ok := fieldsToKeep["properties"]; ok {
		obj.Properties = object.Properties
	}

	if obj.Properties == nil {
		obj.Properties = map[string]interface{}{}
	}
	now := unixNow()
	if _, ok := fieldsToKeep["creationTimeUnix"]; ok {
		obj.CreationTimeUnix = now
	}
	if _, ok := fieldsToKeep["lastUpdateTimeUnix"]; ok {
		obj.LastUpdateTimeUnix = now
	}

	err = validation.New(s, b.vectorRepo.Exists, b.config).Object(ctx, object)
	ec.Add(err)
}

func objectsChanToSlice(c chan BatchObjects, i int) BatchObjects {
	result := make([]BatchObject, i)
	for objectsPerClass := range c {
		for _, object := range objectsPerClass {
			result[object.OriginalIndex] = object
		}
	}
	return result
}

func unixNow() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
