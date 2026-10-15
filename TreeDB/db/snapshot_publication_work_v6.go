package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"unsafe"
)

// Headers have their own allocation lifetime. They escape into current readers
// and therefore must never point into the refundable install descriptor.
func (install *rootPublicationVisibleInstallV1) prepareActivationHeadersV6(w *iterator.OrdinalScanWork) (bool, error) {
	if install.activationState == nil {
		if w != nil && !w.Reserve(1, uint64(unsafe.Sizeof(DBState{}))) {
			return false, nil
		}
		install.activationState = &DBState{
			primaryRoot: install.primaryRoot, CommitSeq: install.next.CommitSeq,
			RootPageID: install.next.UserRootPageID, SystemRootPageID: install.next.SystemRootPageID,
			AppliedCommandLSN: install.next.AppliedCommandLSN, MaxEntryRevision: page.EntryRevision(install.next.MaxEntryRevision),
			ValueLogSet: install.valueLogSet, LeafGenerations: install.leafGenerationView,
		}
	}
	if install.activationSnapshot == nil {
		if w != nil && !w.Reserve(1, uint64(unsafe.Sizeof(snapshotView{}))) {
			return false, nil
		}
		install.activationSnapshot = &snapshotView{}
	}
	return true, nil
}

// This is a stack-only scope under the original publication/DB serialization.
// Native admission holds the original pin table while pricing the actual
// bounded table walk. No iterator, pin lock or table lock survives a return.
type snapshotPublicationScopeV6 struct {
	db          *DB
	state       *DBState
	pins        leafGenerationStaleTransitionV6
	pruneLocked bool
	native      bool
}

func (db *DB) beginSnapshotPublicationV6(state *DBState, w *iterator.OrdinalScanWork) (snapshotPublicationScopeV6, bool) {
	scope := snapshotPublicationScopeV6{db: db, state: state, native: w != nil}
	old := db.snapshotViewRO.Load()
	if old != nil && old.state != nil && old.state.LeafGenerations != nil && old.state.LeafGenerations != state.LeafGenerations {
		var ready bool
		scope.pins, ready = old.state.LeafGenerations.PinSet.beginStaleTransitionV6(&db.leafGenerationPins, w)
		if !ready {
			return snapshotPublicationScopeV6{}, false
		}
	}
	if w != nil && state.LeafGenerations != nil && db.snapshotAcquireInFlight() == 0 {
		tracker := &db.leafGenerationPins
		tracker.mu.Lock()
		// Native uses allocation-free linear membership. Admit both the table
		// slots and every possible active-ID comparison before any deletion.
		records := uint64(len(tracker.refs)) * (1 + uint64(len(state.LeafGenerations.GenerationOrder)))
		bytes := uint64(len(tracker.refs))*uint64(unsafe.Sizeof((*leafGenerationPinRef)(nil))) + records*8
		if !w.Reserve(records, bytes) {
			tracker.mu.Unlock()
			scope.pins.finish(false)
			return snapshotPublicationScopeV6{}, false
		}
		scope.pruneLocked = true
	}
	return scope, true
}
func (scope *snapshotPublicationScopeV6) commitPins() { scope.pins.finish(true) }
func (scope *snapshotPublicationScopeV6) finish(activated bool) {
	scope.pins.finish(false)
	if scope.pruneLocked {
		tracker := &scope.db.leafGenerationPins
		if activated && scope.db.snapshotAcquireInFlight() == 0 {
			active := scope.state.LeafGenerations.GenerationOrder
			for id, ref := range tracker.refs {
				if ref == nil {
					delete(tracker.refs, id)
					continue
				}
				found := false
				for _, candidate := range active {
					if candidate != 0 && candidate == id {
						found = true
						break
					}
				}
				if !found && ref.count.Load() == 0 {
					delete(tracker.refs, id)
				}
			}
		}
		tracker.mu.Unlock()
	} else if activated && !scope.native && scope.state.LeafGenerations != nil && scope.db.snapshotAcquireInFlight() == 0 {
		scope.db.leafGenerationPins.pruneInactiveGenerationIDs(scope.state.LeafGenerations.GenerationOrder)
	}
	*scope = snapshotPublicationScopeV6{}
}

// Ordinary and native publication populate the SAME independent snapshot
// header and preserve negative coverage and system-root epoch semantics.
// Old-reader pins are committed by the caller immediately before this store.
func (db *DB) publishSnapshotViewWithHeaderV6(header *snapshotView, idx *indexGen, state *DBState, vm *valuelog.Manager, coverage *negativeRootCoverage) {
	old := db.snapshotViewRO.Load()
	var negativeFilter *tree.NegativeFilter
	if old != nil && old.idx == idx && old.state != nil {
		if old.state.CommitSeq == state.CommitSeq && old.state.RootPageID == state.RootPageID {
			negativeFilter = old.negativeFilter
		}
		if coverage != nil && coverage.matches(old, idx, state) {
			negativeFilter = coverage.filter
		}
	}
	coherentRootChanged := old == nil || old.idx == nil || old.state == nil || old.idx.id != idx.id || old.state.SystemRootPageID != state.SystemRootPageID
	publishEpoch := db.systemRootPublishEpoch.Load()
	if coherentRootChanged {
		publishEpoch = db.systemRootPublishEpoch.Add(1)
	}
	*header = snapshotView{negativeFilter: negativeFilter, idx: idx, state: state, vlogManager: vm, systemRootPublishEpoch: publishEpoch}
	db.snapshotViewRO.Store(header)
}
