package db

import (
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"sync"
	"unsafe"
)

// primaryBaseReadLeaseV5 owns a genuine beginRead pin. Plain Snapshot fields
// remain immutable until explicit pin release, even after Close marks it closed.
// ReleasePinV5 returns last-read retirement to the owner without finalization.
type primaryBaseReadLeaseV5 struct {
	mu                sync.Mutex
	snapshot          *Snapshot
	pinned            bool
	wrapperGeneration uint64
	generation        *freelist.FreelistGenerationV1
	root              *primaryStateRootV5
}

// Header size is a constant descriptor; it reads no Snapshot state.
func (lease *primaryBaseReadLeaseV5) BaseReadHeaderSizeV5() uint64 {
	return 2*uint64(unsafe.Sizeof(primaryBaseReadLeaseV5{})) + 2*uint64(unsafe.Sizeof(Snapshot{}))
}
func (lease *primaryBaseReadLeaseV5) BaseGenerationV5() *freelist.FreelistGenerationV1 {
	if lease == nil {
		return nil
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return lease.generation
}
func (lease *primaryBaseReadLeaseV5) aliveLocked() bool {
	return lease.pinned && lease.snapshot != nil && !lease.snapshot.closed.Load() && lease.snapshot.generation.Load() == lease.wrapperGeneration && lease.snapshot.registryID != 0 && lease.snapshot.primaryRoot == lease.root
}
func (lease *primaryBaseReadLeaseV5) BaseReadAliveV5() bool {
	if lease == nil {
		return false
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return lease.aliveLocked()
}
func (lease *primaryBaseReadLeaseV5) BaseReadPageV5(id uint64, dst []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if lease == nil {
		return false, primaryarena.ErrStale
	}
	if w != nil && !w.Reserve(1, 2*uint64(unsafe.Sizeof(*lease))+2*uint64(unsafe.Sizeof(Snapshot{}))) {
		return false, nil
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if !lease.aliveLocked() {
		return false, primaryarena.ErrStale
	}
	image, ready, e := lease.snapshot.idx.pager.GetWithWork(id, w)
	if e != nil || !ready {
		return ready, e
	}
	if len(dst) != page.PageSize || len(image) != page.PageSize {
		return false, primaryarena.ErrFormat
	}
	copy(dst, image)
	return true, nil
}

// ReleasePinV5 detaches only the retained read edge. A nonnil result is actual
// last-read closed Snapshot debt retained by the caller until retirement ends.
func (lease *primaryBaseReadLeaseV5) ReleasePinV5(w *iterator.OrdinalScanWork) (bool, *Snapshot) {
	if lease == nil {
		return true, nil
	}
	if w != nil && !w.Reserve(1, 2*uint64(unsafe.Sizeof(*lease))+2*uint64(unsafe.Sizeof(Snapshot{}))) {
		return false, nil
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if !lease.pinned {
		return true, nil
	}
	snapshot := lease.snapshot
	// Keep the owner pin until every other transient read has completed. This
	// prevents another reader from becoming the last read and running ordinary
	// finalization after the native owner detached its pin.
	state := snapshot.readState.Load()
	if state&^snapshotReadClosedBit != 1 {
		return false, nil
	}
	lease.pinned = false
	if snapshot.readState.Add(^uint64(0)) == snapshotReadClosedBit {
		return true, snapshot
	}
	return true, nil
}

// preparePrimaryBaseKeyProofV5 is called with writer and durable-publication
// admission held. It binds an already owned snapshot to the allocator's actual
// fully published generation before immutable proof preparation can yield.
func (db *DB) preparePrimaryBaseKeyProofV5(snapshot *Snapshot, key []byte, revision page.EntryRevision, w *iterator.OrdinalScanWork) (*primaryarena.BaseKeyProofV5, *primaryBaseReadLeaseV5, bool, error) {
	if w != nil && !w.Reserve(1, 2*uint64(unsafe.Sizeof(Snapshot{}))+2*uint64(unsafe.Sizeof(primaryBaseReadLeaseV5{}))) {
		return nil, nil, false, nil
	}
	if snapshot == nil {
		return nil, nil, false, primaryarena.ErrStale
	}
	if e := snapshot.beginRead(); e != nil {
		return nil, nil, false, primaryarena.ErrStale
	}
	lease := &primaryBaseReadLeaseV5{snapshot: snapshot, pinned: true, wrapperGeneration: snapshot.generation.Load()}
	proof, ready, e := db.preparePrimaryBaseKeyProofWithReadLeaseV6(lease, key, revision, w)
	return proof, lease, ready, e
}

func (db *DB) preparePrimaryBaseKeyProofWithReadLeaseV6(lease *primaryBaseReadLeaseV5, key []byte, revision page.EntryRevision, w *iterator.OrdinalScanWork) (*primaryarena.BaseKeyProofV5, bool, error) {
	if w != nil && !w.Reserve(2, 2*uint64(unsafe.Sizeof(primaryBaseReadLeaseV5{}))+2*uint64(unsafe.Sizeof(Snapshot{}))+2*uint64(unsafe.Sizeof(primaryDurableRuntimeV5{}))) {
		return nil, false, nil
	}
	if lease == nil {
		return nil, false, primaryarena.ErrStale
	}
	lease.mu.Lock()
	locked := true
	defer func() {
		if locked {
			lease.mu.Unlock()
		}
	}()
	snapshot := lease.snapshot
	if !lease.pinned || snapshot == nil || snapshot.generation.Load() != lease.wrapperGeneration {
		return nil, false, primaryarena.ErrStale
	}

	if snapshot == nil || snapshot.closed.Load() || snapshot.idx == nil || snapshot.idx != db.idx.Load() || snapshot.primaryRoot == nil || db.durableRoot.primary == nil || db.durableRoot.primary.current == nil || snapshot.primaryRoot.arena != db.durableRoot.primary.current.arena || snapshot.primaryRoot.ref != db.durableRoot.primary.current.ref || snapshot.primaryRoot.dataCommit != db.durableRoot.primary.current.dataCommit || snapshot.state == nil || snapshot.state.RootPageID != db.durableRoot.primary.current.ref.PageID {
		return nil, false, primaryarena.ErrStale
	}
	generation, e := snapshot.idx.allocator.PublishedSnapshotGenerationV1(db.durableRoot.record.Freelist)
	if e != nil || generation.CommitSeq() != snapshot.primaryRoot.dataCommit {
		return nil, false, primaryarena.ErrStale
	}
	lease.generation, lease.root = generation, snapshot.primaryRoot
	if w != nil && !w.Reserve(1, 4*page.PageSize) {
		return nil, false, nil
	}
	image, ready, e := snapshot.idx.primary.Pager().GetWithWork(primaryarena.Local(snapshot.primaryRoot.ref.PageID), w)
	if !ready || e != nil {
		return nil, false, e
	}
	directory, e := node.DecodePrimaryDirectory(image)
	if e != nil {
		return nil, false, e
	}
	base, sequence := directory.Base()
	lease.mu.Unlock()
	locked = false
	proof, ready, e := primaryarena.PrepareBaseKeyProofV5(lease, base, sequence, key, revision, w)
	return proof, ready, e
}
