package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"unsafe"
)

// Both the ordered frontier and this state are embedded in the original Set's
// producer-admitted allocation. They carry no independent file/lease authority.
type nativeMVCCSetReleaseState struct {
	phase         uint8
	nativeOwned   bool
	next          int
	current       *File
	failure       nativeMVCCSetReleaseFailure
	failed        bool
	fileRefund    fileProducerReleaseWork
	managerRefund ManagerWrapperReleaseWork
	managerBound  bool
}
type nativeMVCCSetReleaseFailure struct{ causes [17]error }

func (e *nativeMVCCSetReleaseFailure) Error() string   { return "value-log original Set cleanup failed" }
func (e *nativeMVCCSetReleaseFailure) Unwrap() []error { return e.causes[:] }

func (m *Manager) ReleaseNativeMVCCSetStep(set *Set, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, ErrNativeMVCCFileWork
	}
	return m.releaseNativeMVCCSetStep(set, true, w)
}
func (m *Manager) releaseNativeMVCCSetStep(set *Set, native bool, w *iterator.OrdinalScanWork) (bool, error) {
	if set == nil {
		return true, nil
	}
	if m == nil || !set.retentionKnown || set.retentionShape.FileCount > len(set.nativeFiles) {
		return false, rootpublication.ErrResourceOwnership
	}
	set.releaseMu.Lock()
	defer set.releaseMu.Unlock()
	state := &set.nativeRelease
	if state.phase == 3 {
		if state.failed {
			return true, &state.failure
		}
		return true, nil
	}
	if !native && state.nativeOwned {
		return false, ErrNativeMVCCFileWork
	}
	if state.phase == 0 {
		if set.RefCount.Load() == 1 && !state.managerBound {
			ready, err := state.managerRefund.bindSetWrapper(m, w)
			if !ready || err != nil {
				return false, err
			}
			state.managerBound = true
		}
		if !reserveManagerWrapperWork(w, 1, 2*uint64(unsafe.Sizeof(*set))) {
			return false, nil
		}
		refs := set.RefCount.Add(-1)
		if refs < 0 {
			return false, rootpublication.ErrResourceOwnership
		}
		if refs > 0 {
			return true, nil
		}
		state.nativeOwned = native
		state.phase = 1
	}
	for state.next < set.retentionShape.FileCount {
		if state.phase == 1 {
			if !reserveManagerWrapperWork(w, 1, 2*uint64(unsafe.Sizeof(*state))+2*uint64(unsafe.Sizeof(File{}))+128) {
				return false, nil
			}
			f := set.nativeFiles[state.next]
			if f == nil {
				return false, rootpublication.ErrResourceOwnership
			}
			m.mu.Lock()
			// SAME manager membership exclusion claims physical callback ownership
			// before publishing the actual zero-reference edge to ordinary walkers.
			refs := f.RefCount.Load()
			if refs <= 0 {
				m.mu.Unlock()
				return false, rootpublication.ErrResourceOwnership
			}
			if refs == 1 && (m.closing || f.IsZombie.Load()) && native {
				f.nativeCallbacks.Store(true)
			}
			refs = f.RefCount.Add(-1)
			state.current = f
			state.phase = 2
			m.mu.Unlock()
			if refs != 0 {
				state.phase = 4
			}
		}
		if state.phase == 2 {
			f := state.current
			m.mu.RLock()
			closing := m.closing
			zombie := f.IsZombie.Load()
			m.mu.RUnlock()
			if closing {
				ready, err := m.finishNativeMVCCTerminalStep(f, native, w)
				if !ready {
					return false, err
				}
				state.failure.causes[state.next] = err
				state.failed = state.failed || err != nil
			} else if zombie {
				// Namespace retirement must continue its SAME original deletion owner.
				// Until its work-bearing filesystem stages are bound, retain this exact
				// callback in the original Set/File instead of an ordinary retry/fallback.
				if native {
					return false, ErrNativeMVCCFileWork
				}
				err := m.deleteZombieFile(f)
				state.failure.causes[state.next] = err
				state.failed = state.failed || err != nil
			}
			state.phase = 4
		}
		if state.phase == 4 {
			if !reserveManagerWrapperWork(w, 1, 2*uint64(unsafe.Sizeof(*state))+uint64(unsafe.Sizeof(File{}))+32) {
				return false, nil
			}
			f := state.current
			// The original last Set has no readable borrowers. Remove its physical
			// aliases before any successful containing producer contribution refund.
			delete(set.Files, f.ID)
			set.nativeFiles[state.next] = nil
			state.current = nil
			state.fileRefund.file = f
			state.phase = 5
		}
		if state.phase == 5 {
			ready, err := state.fileRefund.releaseStep(native, w)
			if !ready || err != nil {
				return false, err
			}
			// The external control has no physical aliases after actual completion.
			// Its next use starts only after that completed original removal.
			if !reserveManagerWrapperWork(w, 1, 2*uint64(unsafe.Sizeof(state.fileRefund))+32) {
				return false, nil
			}
			state.fileRefund = fileProducerReleaseWork{}
			state.next++
			state.phase = 1
		}
	}
	if state.phase != 6 {
		if !reserveManagerWrapperWork(w, 1, 2*uint64(unsafe.Sizeof(*state))+uint64(unsafe.Sizeof(set.Files))) {
			return false, nil
		}
		set.Files = nil
		state.phase = 6
	}
	ready, err := state.managerRefund.releaseStep(native, w)
	if !ready || err != nil {
		return false, err
	}
	if !reserveManagerWrapperWork(w, 1, 2*uint64(unsafe.Sizeof(*state))+uint64(unsafe.Sizeof(set.Files))) {
		return false, nil
	}
	set.Files = nil
	state.phase = 3
	if state.failed {
		return true, &state.failure
	}
	return true, nil
}

type nativeMVCCFileTerminalState struct {
	phase uint8
	cause error
}

// Continue the original terminal Manager/File membership, physical close and
// final parent outcome. A native-owned callback is never drained by the
// ordinary terminal walker, which retains its original map discovery edge.
func (m *Manager) finishNativeMVCCTerminalStep(f *File, native bool, w *iterator.OrdinalScanWork) (bool, error) {
	if m == nil || f == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	f.terminalMu.Lock()
	defer f.terminalMu.Unlock()
	state := &f.nativeTerminal
	if state.phase == 4 || f.terminalAttempted && state.phase == 0 {
		return true, f.terminalErr
	}
	if !native && f.nativeCallbacks.Load() {
		return false, ErrNativeMVCCFileWork
	}
	if state.phase == 0 {
		if !reserveManagerWrapperWork(w, 1, 2*uint64(unsafe.Sizeof(*f))+2*uint64(unsafe.Sizeof(*m))+128) {
			return false, nil
		}
		m.mu.Lock()
		if m.files[f.ID] != f || f.RefCount.Load() != 0 {
			m.mu.Unlock()
			return true, nil
		}
		f.terminalAttempted = true
		state.cause = m.unobserveStableFileLocked(f)
		state.phase = 1
		m.mu.Unlock()
	}
	if state.phase == 1 {
		ready, err := f.closeOriginalStep(native, w)
		if !ready {
			return false, err
		}
		state.cause = errors.Join(state.cause, err)
		state.phase = 2
	}
	if state.phase == 2 {
		if !reserveManagerWrapperWork(w, 1, 2*uint64(unsafe.Sizeof(*f))+retirementParentRetentionEnvelope+128) {
			return false, nil
		}
		// The actual final parent Close remains outside Manager membership locks.
		state.cause = errors.Join(state.cause, f.closeOriginalRetirementParent())
		state.phase = 3
	}
	if state.phase == 3 {
		if !reserveManagerWrapperWork(w, 1, 2*uint64(unsafe.Sizeof(*f))+2*uint64(unsafe.Sizeof(*m))+128) {
			return false, nil
		}
		m.mu.Lock()
		if state.cause != nil {
			f.terminalErr = state.cause
			m.retainOriginalFileFailureLocked(f)
		} else if m.files[f.ID] == f {
			delete(m.files, f.ID)
			f.producerDetached.Store(true)
			if f.IsZombie.Load() {
				m.retiredCount--
			}
		}
		state.phase = 4
		f.nativeCallbacks.Store(false)
		if !m.closeRunning {
			m.refundTerminalMetadataLocked()
		}
		err := m.closeErr
		m.mu.Unlock()
		// Set alias scrub completes before File containing metadata is refunded by
		// releaseNativeMVCCSetStep; the ordinary walker still performs its final refund.
		return true, err
	}
	return false, nil
}
