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

package hnsw

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

func (h *hnsw) selectNeighborsHeuristic(input *priorityqueue.Queue,
	max int, denyList helpers.AllowList,
) error {
	if input.Len() < max {
		return nil
	}

	// TODO, if this solution stays we might need something with fewer allocs
	ids := make([]uint64, input.Len())
	var vecs [][]float32

	closestFirst := h.pools.pqHeuristic.GetMin(input.Len())
	i := uint64(0)
	for input.Len() > 0 {
		elem := input.Pop()
		closestFirst.Insert(elem.ID, i, elem.Dist)
		ids[i] = elem.ID
		i++
	}

	vecs, errs := h.multiVectorForID(context.TODO(), ids)

	returnList := h.pools.pqItemSlice.Get().([]priorityqueue.ItemWithIndex)

	for closestFirst.Len() > 0 && len(returnList) < max {
		curr := closestFirst.Pop()
		if denyList != nil && denyList.Contains(curr.ID) {
			continue
		}
		distToQuery := curr.Dist

		currVec := vecs[curr.Index]
		if err := errs[curr.Index]; err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID)
				continue
			} else {
				// not a typed error, we can recover from, return with err
				return errors.Wrapf(err,
					"unrecoverable error for docID %d", curr.ID)
			}
		}
		good := true
		for _, item := range returnList {
			peerDist, _, _ := h.distancerProvider.SingleDist(currVec,
				vecs[item.Index])

			if peerDist < distToQuery {
				good = false
				break
			}
		}

		if good {
			returnList = append(returnList, curr)
		}

	}

	h.pools.pqHeuristic.Put(closestFirst)

	for _, retElem := range returnList {
		input.Insert(retElem.ID, retElem.Dist)
	}

	// rewind and return to pool
	returnList = returnList[:0]

	// nolint:staticcheck
	h.pools.pqItemSlice.Put(returnList)

	return nil
}

func (h *hnsw) selectNeighborsHeuristic2(nodeid uint64, input *priorityqueue.Queue,
	max int, denyList helpers.AllowList,
) error {
	if input.Len() < max {
		return nil
	}

	// TODO, if this solution stays we might need something with fewer allocs
	ids := make([]uint64, input.Len())
	var vecs [][]float32

	closestFirst := h.pools.pqHeuristic.GetMin(input.Len())
	i := uint64(0)
	for input.Len() > 0 {
		elem := input.Pop()
		closestFirst.Insert(elem.ID, i, elem.Dist)
		ids[i] = elem.ID
		i++
	}

	fmt.Printf("  ==> [%v] heuristic closest ids [%v]\n", nodeid, closestFirst.IDs())
	fmt.Printf("  ==> [%v] heuristic closest dists [%v]\n", nodeid, closestFirst.Dists())

	vecs, errs := h.multiVectorForID(context.TODO(), ids)

	returnList := h.pools.pqItemSlice.Get().([]priorityqueue.ItemWithIndex)

	for closestFirst.Len() > 0 && len(returnList) < max {
		curr := closestFirst.Pop()
		if denyList != nil && denyList.Contains(curr.ID) {
			continue
		}
		distToQuery := curr.Dist

		currVec := vecs[curr.Index]
		if err := errs[curr.Index]; err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID)
				continue
			} else {
				// not a typed error, we can recover from, return with err
				return errors.Wrapf(err,
					"unrecoverable error for docID %d", curr.ID)
			}
		}
		good := true
		for _, item := range returnList {
			peerDist, _, _ := h.distancerProvider.SingleDist(currVec,
				vecs[item.Index])

			fmt.Printf("  ==> [%v] heuristic dist curr [%v][%v] vs item [%v][%v] vs peer [%v]\n", nodeid, curr.ID, curr.Dist, item.ID, item.Dist, peerDist)
			if peerDist < distToQuery {
				good = false
				break
			}
		}

		if good {
			fmt.Printf("  ==> [%v] heuristic append return with [%v]\n", nodeid, curr.ID)
			returnList = append(returnList, curr)
		} else {
			fmt.Printf("  ==> [%v] heuristic reject with [%v]\n", nodeid, curr.ID)
		}
	}

	h.pools.pqHeuristic.Put(closestFirst)

	for _, retElem := range returnList {
		input.Insert(retElem.ID, retElem.Dist)
	}
	fmt.Printf("  ==> [%v] heuristic appended [%v]\n", nodeid, input.Len())
	// rewind and return to pool
	returnList = returnList[:0]

	// nolint:staticcheck
	h.pools.pqItemSlice.Put(returnList)

	return nil
}
