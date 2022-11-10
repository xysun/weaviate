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

package vectorizer

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/fatih/camelcase"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/modules/text2vec-openai/ent"
)

type Vectorizer struct {
	client Client
}

func New(client Client) *Vectorizer {
	return &Vectorizer{
		client: client,
	}
}

type Client interface {
	Vectorize(ctx context.Context, input string,
		config ent.VectorizationConfig) (*ent.VectorizationResult, error)
	VectorizeInputs(ctx context.Context, inputs []string,
		config ent.VectorizationConfig) ([]*ent.VectorizationResult, error)
	VectorizeQuery(ctx context.Context, input string,
		config ent.VectorizationConfig) (*ent.VectorizationResult, error)
}

// IndexCheck returns whether a property of a class should be indexed
type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizePropertyName(propertyName string) bool
	VectorizeClassName() bool
	Model() string
	Type() string
}

func sortStringKeys(schema_map map[string]interface{}) []string {
	keys := make([]string, 0, len(schema_map))
	for k := range schema_map {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (v *Vectorizer) Objects(ctx context.Context, objects []*models.Object,
	settings ClassSettings,
) error {
	return v.objects(ctx, objects, settings)
}

func appendPropIfText(icheck ClassSettings, list *[]string, propName string,
	value interface{},
) {
	valueString, ok := value.(string)
	if ok {
		if icheck.VectorizePropertyName(propName) {
			// use prop and value
			*list = append(*list, strings.ToLower(
				fmt.Sprintf("%s %s", camelCaseToLower(propName), valueString)))
		} else {
			*list = append(*list, strings.ToLower(valueString))
		}
	}
}

func (v *Vectorizer) generateCorpi(className string, schema interface{}, settings ClassSettings) string {
	var corpi []string

	if settings.VectorizeClassName() {
		corpi = append(corpi, camelCaseToLower(className))
	}

	if schema != nil {
		schemamap := schema.(map[string]interface{})
		for _, prop := range sortStringKeys(schemamap) {
			if !settings.PropertyIndexed(prop) {
				continue
			}

			if asSlice, ok := schemamap[prop].([]interface{}); ok {
				for _, elem := range asSlice {
					appendPropIfText(settings, &corpi, prop, elem)
				}
			} else {
				appendPropIfText(settings, &corpi, prop, schemamap[prop])
			}
		}
	}

	if len(corpi) == 0 {
		// fall back to using the class name
		corpi = append(corpi, camelCaseToLower(className))
	}

	return strings.Join(corpi, " ")
}

func (v *Vectorizer) objects(ctx context.Context, objects []*models.Object, settings ClassSettings) error {
	inputs := make([]string, len(objects))
	for i, object := range objects {
		inputs[i] = v.generateCorpi(object.Class, object.Properties, settings)
	}

	now := time.Now()
	res, err := v.client.VectorizeInputs(ctx, inputs, ent.VectorizationConfig{
		Type:  settings.Type(),
		Model: settings.Model(),
	})
	fmt.Printf("OpenAI call time: %v\n", time.Since(now))

	for i, v := range res {
		objects[i].Vector = v.Vector
	}

	return err
}

func camelCaseToLower(in string) string {
	parts := camelcase.Split(in)
	var sb strings.Builder
	for i, part := range parts {
		if part == " " {
			continue
		}

		if i > 0 {
			sb.WriteString(" ")
		}

		sb.WriteString(strings.ToLower(part))
	}

	return sb.String()
}
