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

package diskAnn

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	"github.com/semi-technologies/weaviate/entities/schema"
)

type Config struct {
	VectorForIDThunk ssdhelpers.VectorForID // to retrieve vectors
	Distance         ssdhelpers.DistanceFunction
}

type UserConfig struct {
	R                  int     `json:"radius"`
	L                  int     `json:"list"`
	Alpha              float32 `json:"alpha"`
	ClustersSize       int
	ClusterOverlapping int
	C                  int `json:"c"`
	OriginalCacheSize  int
	BeamSize           int
	Dimensions         int    `json:"dimensions"`
	VectorsSize        uint64 `json:"size"`
	Segments           int    `json:"segments"`
	Centroids          int    `json:"centroids"`
	Path               string `json:"path"`
	OnDisk             bool   `json:"disk"`
}

func (config UserConfig) IndexType() string {
	return "vamana"
}

func NewUserConfig() UserConfig {
	return UserConfig{
		R:                  32,
		L:                  125,
		Alpha:              1.2,
		ClustersSize:       40,
		ClusterOverlapping: 2,
		C:                  10000,
		BeamSize:           1,
		Dimensions:         4,
		VectorsSize:        0,
		Centroids:          255,
		Segments:           4,
		Path:               "",
		OnDisk:             false,
	}
}

func ParseUserConfig(input interface{}) (schema.VectorIndexConfig, error) {
	uc := NewUserConfig()

	asMap, ok := input.(map[string]interface{})
	if !ok || asMap == nil {
		return uc, fmt.Errorf("input must be a non-nil map")
	}

	if err := optionalIntFromMap(asMap, "radius", func(v int) {
		uc.R = v
	}); err != nil {
		return uc, err
	}

	if err := optionalIntFromMap(asMap, "list", func(v int) {
		uc.L = v
	}); err != nil {
		return uc, err
	}

	if err := optionalFloatFromMap(asMap, "alpha", func(v float32) {
		uc.Alpha = v
	}); err != nil {
		return uc, err
	}

	if err := optionalIntFromMap(asMap, "vectorIndexCache", func(v int) {
		uc.C = v
	}); err != nil {
		return uc, err
	}

	if err := optionalIntFromMap(asMap, "dimensions", func(v int) {
		uc.Dimensions = v
	}); err != nil {
		return uc, err
	}

	if err := optionalIntFromMap(asMap, "size", func(v int) {
		uc.VectorsSize = uint64(v)
	}); err != nil {
		return uc, err
	}

	if err := optionalIntFromMap(asMap, "segments", func(v int) {
		uc.Segments = v
	}); err != nil {
		return uc, err
	}

	if err := optionalIntFromMap(asMap, "segments", func(v int) {
		uc.Segments = v
	}); err != nil {
		return uc, err
	}

	if err := optionalStringFromMap(asMap, "path", func(v string) {
		uc.Path = v
	}); err != nil {
		return uc, err
	}

	if err := optionalBoolFromMap(asMap, "disk", func(v bool) {
		uc.OnDisk = v
	}); err != nil {
		return uc, err
	}

	return uc, nil
}

func optionalStringFromMap(in map[string]interface{}, name string,
	setFn func(v string),
) error {
	value, ok := in[name]
	if !ok {
		return nil
	}

	asString, ok := value.(string)
	if !ok {
		return nil
	}

	setFn(asString)
	return nil
}

func optionalIntFromMap(in map[string]interface{}, name string,
	setFn func(v int),
) error {
	value, ok := in[name]
	if !ok {
		return nil
	}

	var asInt64 int64
	var err error

	// depending on whether we get the results from disk or from the REST API,
	// numbers may be represented slightly differently
	switch typed := value.(type) {
	case json.Number:
		asInt64, err = typed.Int64()
	case float64:
		asInt64 = int64(typed)
	}
	if err != nil {
		return errors.Wrapf(err, "%s", name)
	}

	setFn(int(asInt64))
	return nil
}

func optionalBoolFromMap(in map[string]interface{}, name string,
	setFn func(v bool),
) error {
	value, ok := in[name]
	if !ok {
		return nil
	}

	asBool, ok := value.(bool)
	if !ok {
		return nil
	}

	setFn(asBool)
	return nil
}

func optionalFloatFromMap(in map[string]interface{}, name string,
	setFn func(v float32),
) error {
	value, ok := in[name]
	if !ok {
		return nil
	}

	var asFloat float64
	var err error

	// depending on whether we get the results from disk or from the REST API,
	// numbers may be represented slightly differently
	switch typed := value.(type) {
	case json.Number:
		asFloat, err = typed.Float64()
	case float64:
		asFloat = float64(typed)
	}
	if err != nil {
		return errors.Wrapf(err, "%s", name)
	}

	setFn(float32(asFloat))
	return nil
}
