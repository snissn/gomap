package caching

import (
	"errors"
	"sync"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
)

var errCOWReadBatchSize = errors.New("treedb: successor batch requires equal lengths and at most32 probes")

type cowReadBatchStage struct {
	best      pointSuccessorCandidate
	inspected cowSuccessorInspection
	err       error
	fallback  bool
}

// SeekGEVersionRanges is optional on the existing qualified cut. It stages at
// most32 owned metadata winners, then releases its traversal workspace BEFORE
// any complete-cut materialization or scalar/merge fallback can reacquire it.
// There is no cache/cursor lifetime beyond this synchronous operation.
func (s *Snapshot) SeekGEVersionRanges(starts, ends [][]byte, results []mvcckey.VersionRangeResult) error {
	if s == nil {
		return backenddb.ErrClosed
	}
	if len(starts) != len(ends) || len(starts) != len(results) || len(starts) > mvcckey.MaxReadBatch {
		return errCOWReadBatchSize
	}
	if err := s.beginRead(); err != nil {
		return err
	}
	defer s.endRead()
	if s.cowCut == nil {
		return ErrCOWUnsupported
	}
	for i := range results {
		results[i] = mvcckey.VersionRangeResult{}
	}
	if len(starts) == 0 {
		return nil
	}
	// Empty basis and unknown immutable sources keep their maintained route.
	empty, err := s.cowCut.basis.snapshot.OwnedUserRootEmpty()
	if err != nil {
		return err
	}
	eligible := !empty
	for _, source := range s.rootIterator.immutables {
		if _, ok := source.(memtable.SuccessorTable); !ok {
			eligible = false
			break
		}
	}
	if !eligible {
		s.cowReadBatchScalar(starts, ends, results)
		return nil
	}
	// This finite call-local staging uses the original COW budget. It neither
	// retains a new physical owner nor competes with decoder/pool authority.
	lease, err := s.cowCache.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof([mvcckey.MaxReadBatch]cowReadBatchStage{}))))
	if err != nil {
		s.cowReadBatchScalar(starts, ends, results)
		return nil
	}
	defer lease.Close()
	var staged [mvcckey.MaxReadBatch]cowReadBatchStage
	s.cowReadMu.Lock()
	// Existing cut-local readers keep priority; otherwise borrow one original
	// basis/slot nonblockingly. No new per-query read workspace is introduced.
	var mu *sync.Mutex
	var owner **cowReadWorkspace
	basis := s.cowCut.basis
	if s.cowReader != nil && s.cowReader.point != nil {
		s.cowReadMu.Unlock()
		lease.Close()
		s.cowReadBatchScalar(starts, ends, results)
		return nil
	}
	if basis.readMu.TryLock() {
		mu, owner = &basis.readMu, &basis.reader
	} else {
		for i := range basis.readSlots {
			slot := &basis.readSlots[i]
			if slot.mu.TryLock() {
				mu, owner = &slot.mu, &slot.reader
				break
			}
		}
	}
	if mu == nil {
		s.cowReadMu.Unlock()
		lease.Close()
		s.cowReadBatchScalar(starts, ends, results)
		return nil
	}
	fresh := *owner == nil
	w, err := basis.workspaceLocked(owner, s.cowCache.budget, s.cowCache.readLimits())
	if err == nil {
		_, err = w.pointScratch()
	}
	if err != nil {
		cowReadHandoffLocked(mu, owner, fresh, false, err)
		mu.Unlock()
		s.cowReadMu.Unlock()
		if errors.Is(err, memtable.ErrCOWCapacity) {
			lease.Close()
			s.cowReadBatchScalar(starts, ends, results)
			return nil
		}
		return err
	}
	w.point.successor.BeginLeafBatch()
	handoff := false
	var releaseErr error
	for i := range starts {
		stage := &staged[i]
		stage.best, stage.err = s.cowWorkspacePhysicalSuccessorLocked(w, starts[i], ends[i], &stage.inspected)
		if errors.Is(stage.err, memtable.ErrCOWCapacity) && stage.inspected.file != nil {
			// Original scalar handoff: metadata succeeded; the complete-cut owner can
			// materialize using this exact inspection after fresh scratch refunds.
			stage.err = nil
		} else if stage.err == errCOWPhysicalDictionary || errors.Is(stage.err, memtable.ErrCOWCapacity) {
			// Only established dictionary/admission eligibility permits fallback.
			stage.fallback = true
		}
		handoff = handoff || cowSuccessorNeedsCutLocalValue(stage.best)
		if stage.err != nil {
			if stage.err != errCOWPhysicalDictionary {
				releaseErr = stage.err
			}
			// Errors invalidate reusable page state. Never hide a real read error by
			// retrying it; other independent inputs still retain their own outcomes.
			w.point.successor.EndLeafBatch()
			w.point.successor.BeginLeafBatch()
		}
	}
	w.point.successor.EndLeafBatch()
	freshOwner := cowReadHandoffLocked(mu, owner, fresh, handoff, releaseErr)
	mu.Unlock()
	// No traversal slot remains locked from here. The exact original cut owns
	// every staged pointer/inspection; outputs were copied before page reuse.
	for i := range starts {
		stage, result := &staged[i], &results[i]
		if stage.fallback || stage.best.flags&node.FlagTombstone != 0 {
			continue
		}
		if stage.err != nil {
			result.Err = stage.err
			continue
		}
		best := &stage.best
		if best.found && best.flags&node.FlagPointer != 0 && !best.valueOwned {
			best.value, result.Err = s.cowSuccessorValueLocked(*best, stage.inspected)
			if errors.Is(result.Err, memtable.ErrCOWCapacity) && freshOwner.refund() {
				best.value, result.Err = s.cowSuccessorValueLocked(*best, stage.inspected)
			}
		}
		if result.Err == nil {
			result.Key, result.Value, result.Found = best.key, best.value, best.found
		}
	}
	s.cowReadMu.Unlock()
	for i := range starts {
		stage := &staged[i]
		if stage.fallback {
			// The maintained scalar route retains its own dictionary/capacity policy.
			r := &results[i]
			r.Key, r.Value, r.Found, r.Err = s.cowSeekGEAdmitted(starts[i], ends[i])
		} else if stage.err == nil && stage.best.flags&node.FlagTombstone != 0 {
			r := &results[i]
			r.Key, r.Value, r.Found, r.Err = s.cowSeekGEMergeAdmitted(starts[i], ends[i])
		}
	}
	return nil
}

func (s *Snapshot) cowReadBatchScalar(starts, ends [][]byte, results []mvcckey.VersionRangeResult) {
	for i := range starts {
		r := &results[i]
		r.Key, r.Value, r.Found, r.Err = s.cowSeekGEAdmitted(starts[i], ends[i])
	}
}
