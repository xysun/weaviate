/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

package schema

import (
	"context"
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
)

// AddActionProperty to an existing Action
func (m *Manager) AddActionProperty(ctx context.Context, class string, property *models.SemanticSchemaClassProperty) error {
	return m.addClassProperty(ctx, class, property, kind.Action)
}

// AddThingProperty to an existing Thing
func (m *Manager) AddThingProperty(ctx context.Context, class string, property *models.SemanticSchemaClassProperty) error {
	return m.addClassProperty(ctx, class, property, kind.Thing)
}

func (m *Manager) addClassProperty(ctx context.Context, className string,
	prop *models.SemanticSchemaClassProperty, k kind.Kind) error {
	label := fmt.Sprintf("schema/%s/property", k.Name())
	finish := meassure(m.metrics.UseCase.WithLabelValues("add", label))
	defer finish()

	finish = meassure(m.metrics.Locking.WithLabelValues("schema", "add", label))
	unlock, err := m.locks.LockSchema()
	if err != nil {
		return err
	}
	defer unlock()
	finish()

	finish = meassure(m.metrics.Validation.WithLabelValues("add", label))
	semanticSchema := m.state.SchemaFor(k)
	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	err = m.validateCanAddProperty(prop, class)
	if err != nil {
		return err
	}
	finish()

	finish = meassure(m.metrics.Connector.WithLabelValues("add", label))
	defer finish()
	class.Properties = append(class.Properties, prop)
	err = m.saveSchema(ctx)
	if err != nil {
		return err
	}

	return m.migrator.AddProperty(ctx, k, className, prop)
}

func (m *Manager) validateCanAddProperty(property *models.SemanticSchemaClassProperty, class *models.SemanticSchemaClass) error {
	// Verify format of property.
	_, err := schema.ValidatePropertyName(property.Name)
	if err != nil {
		return err
	}

	// First check if there is a name clash.
	err = validatePropertyNameUniqueness(property.Name, class)
	if err != nil {
		return err
	}

	err = m.validatePropertyNameOrKeywordsCorrect(class.Class, property.Name, property.Keywords)
	if err != nil {
		return err
	}

	// Validate data type of property.
	schema := m.GetSchema()
	_, err = (&schema).FindPropertyDataType(property.DataType)
	if err != nil {
		return fmt.Errorf("Data type of property '%s' is invalid; %v", property.Name, err)
	}

	if err = m.validateNetworkCrossRefs(property.DataType); err != nil {
		return fmt.Errorf("Data type of property '%s' is invalid; %v", property.Name, err)
	}

	// all is fine!
	return nil
}
