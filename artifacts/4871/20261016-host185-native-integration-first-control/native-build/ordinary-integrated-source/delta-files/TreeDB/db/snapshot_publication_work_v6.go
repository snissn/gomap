package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/tree"
	"unsafe"
)

// Published state/view headers are original GC-managed outputs. Their actual
// allocation and initialization traffic is admitted before allocator adoption;
// no private header or pointer-bearing temporary survives a refusal or yield.
func admitActivationHeadersV6(w *iterator.OrdinalScanWork) bool {
	return w == nil || w.Reserve(2, uint64(unsafe.Sizeof(DBState{}))+uint64(unsafe.Sizeof(snapshotView{})))
}

// This is a stack-only scope under the original publication/DB serialization.
// Native admission holds the original pin table while pricing the actual
// bounded table walk. No iterator, pin lock or table lock survives a return.
type snapshotPublicationScopeV6 struct {
	db          *DB
	generations *leafGenerationView
	pins        leafGenerationStaleTransitionV6
	pruneLocked bool
	native      bool
}

func (db *DB) beginSnapshotPublicationV6(generations *leafGenerationView, w *iterator.OrdinalScanWork) (snapshotPublicationScopeV6, bool) {
	scope := snapshotPublicationScopeV6{db: db, generations: generations, native: w != nil}
	old := db.snapshotViewRO.Load()
	if old != nil && old.state != nil && old.state.LeafGenerations != nil && old.state.LeafGenerations != generations {
		var ready bool
		scope.pins, ready = old.state.LeafGenerations.PinSet.beginStaleTransitionV6(&db.leafGenerationPins, w)
		if !ready {
			return snapshotPublicationScopeV6{}, false
		}
	}
	if w != nil && generations != nil && db.snapshotAcquireInFlight() == 0 {
		tracker := &db.leafGenerationPins
		tracker.mu.Lock()
		// Native uses allocation-free linear membership. Admit both the table
		// slots and every possible active-ID comparison before any deletion.
		records := uint64(len(tracker.refs)) * (1 + uint64(len(generations.GenerationOrder)))
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
			active := scope.generations.GenerationOrder
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
	} else if activated && !scope.native && scope.generations != nil && scope.db.snapshotAcquireInFlight() == 0 {
		scope.db.leafGenerationPins.pruneInactiveGenerationIDs(scope.generations.GenerationOrder)
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
