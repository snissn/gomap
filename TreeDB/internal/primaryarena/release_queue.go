package primaryarena

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// ReleaseQueue contains finite physical retirement work for one owner. It has
// no publication ancestry or lookup authority. All descendants inherit the
// originating queue when the last actual incoming edge is dropped.
type ReleaseQueue struct {
	owner     *Arena
	bankHead  uint64
	groupHead *Group
}

func (a *Arena) NewReleaseQueue(w *iterator.OrdinalScanWork) (*ReleaseQueue, bool, error) {
	bytes := uint64(unsafe.Sizeof(ReleaseQueue{}))
	if !reserve(w, 1, bytes) {
		return nil, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counters.Bytes += bytes
	return &ReleaseQueue{owner: a}, true, nil
}

// ProgressOrdinaryRelease spends bounded actual work only on ordinary custody.
// Accepted native owners have separate queues, progressed by their own Finish.
func (a *Arena) ProgressOrdinaryRelease(steps int, w *iterator.OrdinalScanWork) (bool, error) {
	for i := 0; i < steps; i++ {
		bytes := 2 * uint64(unsafe.Sizeof(ReleaseQueue{}))
		if !reserve(w, 1, bytes) {
			return false, nil
		}
		a.mu.Lock()
		empty := a.ordinaryRelease.bankHead == 0 && a.ordinaryRelease.groupHead == nil
		a.counters.Bytes += bytes
		a.mu.Unlock()
		if empty {
			return true, nil
		}
		ready, progress, err := a.ReleaseStep(w)
		if err != nil || !ready {
			return false, err
		}
		if !progress {
			return true, nil
		}
	}
	return true, nil
}
