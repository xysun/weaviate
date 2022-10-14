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
	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
)

type Config struct {
	VectorForIDThunk ssdhelpers.VectorForID // to retrieve vectors
	Distance         ssdhelpers.DistanceFunction
}

type UserConfig struct {
	R                  int     // degree bound
	L                  int     // search list size
	Alpha              float32 // to decide on the range of long connections
	ClustersSize       int
	ClusterOverlapping int
	C                  int
	OriginalCacheSize  int
	BeamSize           int
	Dimensions         int
	VectorsSize        uint64 // size of the dataset
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
	}
}
