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

// InitializeReleaseQueue binds caller-embedded ordinary capture custody.
// Snapshot allocation admission already includes this embedded queue.
func (a *Arena) InitializeReleaseQueue(q *ReleaseQueue) {
	*q = ReleaseQueue{owner: a}
}

// PreserveOrdinaryReleaseQueue transfers only residual failed ordinary cleanup
// to the SAME arena's existing queue. Native owners never call this helper.
func (a *Arena) PreserveOrdinaryReleaseQueue(q *ReleaseQueue) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if q == nil || q.owner != a {
		return ErrStale
	}
	if q.bankHead != 0 {
		tail := a.slot(Namespace + q.bankHead)
		if tail == nil {
			return ErrStale
		}
		for tail.next != 0 {
			tail = a.slot(Namespace + tail.next)
			if tail == nil {
				return ErrStale
			}
		}
		tail.next = a.ordinaryRelease.bankHead
		a.ordinaryRelease.bankHead = q.bankHead
		q.bankHead = 0
	}
	if q.groupHead != nil {
		tail := q.groupHead
		for tail.releaseNext != nil {
			tail = tail.releaseNext
		}
		tail.releaseNext = a.ordinaryRelease.groupHead
		a.ordinaryRelease.groupHead = q.groupHead
		q.groupHead = nil
	}
	return nil
}
