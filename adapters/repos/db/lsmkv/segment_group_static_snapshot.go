package lsmkv

import (
	"fmt"

	"github.com/pkg/errors"
)

type SGStaticSnapshot struct {
	segments []*segment
}

func NewSGStaticSnapshotFromSG(sg *SegmentGroup) *SGStaticSnapshot {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	out := &SGStaticSnapshot{
		segments: make([]*segment, len(sg.segments)),
	}
	copy(out.segments, sg.segments)

	return out
}

func (sg *SGStaticSnapshot) makeExistsOnLower(nextSegmentIndex int) existsOnLowerSegmentsFn {
	return func(key []byte) (bool, error) {
		if nextSegmentIndex == 0 {
			// this is already the lowest possible segment, we can guarantee that
			// any key in this segment is previously unseen.
			return false, nil
		}

		v, err := sg.getWithUpperSegmentBoundary(key, nextSegmentIndex-1)
		if err != nil {
			return false, errors.Wrapf(err, "check exists on segments lower than %d",
				nextSegmentIndex)
		}

		return v != nil, nil
	}
}

func (sg *SGStaticSnapshot) getWithUpperSegmentBoundary(key []byte, topMostSegment int) ([]byte, error) {
	// assumes "replace" strategy

	// start with latest and exit as soon as something is found, thus making sure
	// the latest takes presence
	for i := topMostSegment; i >= 0; i-- {
		v, err := sg.segments[i].get(key)
		if err != nil {
			if err == NotFound {
				continue
			}

			if err == Deleted {
				return nil, nil
			}

			panic(fmt.Sprintf("unsupported error in segmentGroup.get(): %v", err))
		}

		return v, nil
	}

	return nil, nil
}
