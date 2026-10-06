package caching

import (
	"errors"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
)

// A successful final physical batch proves coverage of exactly this captured
// prefix. If successor capture refuses, this owner keeps the entire prefix on
// its old basis. The next coordinator pass retries only the handoff.
type cowFlushHandoff struct {
	cut      *cowReadCut
	lease    *memtable.COWExternalLease
	accepted bool
}

func (c *cowCache) captureBackendBasis(provider backendSnapshotProvider) (*cowBackendBasis, error) {
	bounded, ok := provider.(interface {
		AcquireSnapshotWithAllocationAdmission(func(backenddb.SnapshotAllocationSizes) error) (*backenddb.Snapshot, error)
	})
	if !ok {
		return nil, ErrCOWUnsupported
	}
	// Admit the callback and both captured reservation/count cells before creation.
	scratch, err := c.budget.AcquireExternal(memtable.COWAllocationCharge(4*uint64(unsafe.Sizeof(uintptr(0)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof((*memtable.COWExternalLease)(nil)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(int(0)))))
	if err != nil {
		return nil, err
	}
	defer scratch.Close()
	var lease *memtable.COWExternalLease
	var count int
	snapshot, err := bounded.AcquireSnapshotWithAllocationAdmission(func(s backenddb.SnapshotAllocationSizes) error {
		if s.ValueLog.MapHint > c.budget.Limits().MaxResources || s.ValueLog.FileCount > c.budget.Limits().MaxResources {
			return memtable.ErrCOWCapacity
		}
		count = s.ValueLog.FileCount
		var err error
		lease, err = c.budget.AcquireExternal(cowBasisCharge(s))
		return err
	})
	if err != nil {
		lease.Close()
		if err == backenddb.ErrSnapshotCapacity {
			err = memtable.ErrCOWCapacity
		}
		return nil, err
	}
	pins := make([]*rootpublication.IdentityPin, count)
	if err = snapshot.PinValueLogReadFiles(pins); err != nil {
		_ = snapshot.Close()
		lease.Close()
		return nil, err
	}
	return newCOWBackendBasis(snapshot, lease, pins), nil
}

// installCOWBasis preserves every newer mutable and frozen source. For a flush,
// the exact accepted captured prefix alone is removed. A maintenance refresh
// supplies nil coverage and preserves every source on the equivalent new tree.
// Allocation is admitted before construction; cutMu only performs the swap.
func (c *cowCache) installCOWBasis(basis *cowBackendBasis, covered *cowReadCut) (*cowReadCut, error) {
	c.writerMu.Lock()
	defer c.writerMu.Unlock()
	old := c.cut
	if old == nil {
		return nil, backenddb.ErrClosed
	}
	n := 0
	if covered != nil {
		n = len(covered.frozen)
		if old.basis != covered.basis || len(old.frozen) < n {
			return nil, backenddb.ErrRootPublicationBasisMismatch
		}
		for i := 0; i < n; i++ {
			if old.frozen[i].root != covered.frozen[i].root {
				return nil, backenddb.ErrRootPublicationBasisMismatch
			}
		}
	}
	lease, err := c.budget.AcquireExternal(cowCutCharge(len(old.shards), len(old.frozen)-n))
	if err != nil {
		return nil, err
	}
	next := &cowReadCut{refs: 1, cache: c, lease: lease, basis: basis, shards: make([]cowTable, len(old.shards)), frozen: make([]cowTable, len(old.frozen)-n)}
	basis.retain()
	copy(next.shards, old.shards)
	copy(next.frozen, old.frozen[n:])
	for i := range next.shards {
		if !next.shards[i].root.Retain() {
			panic("lost COW handoff mutable")
		}
	}
	for i := range next.frozen {
		if !next.frozen[i].root.Retain() {
			panic("lost COW handoff frozen")
		}
	}
	next.buildDomains()
	c.cutMu.Lock()
	c.cut = next
	c.activeCuts.Add(1)
	old.refs--
	last := old.refs == 0
	c.cutMu.Unlock()
	if last {
		return old, nil
	}
	return nil, nil
}

func (db *DB) completeCOWHandoff() error {
	c := db.cow
	h := c.handoff
	if h == nil {
		return nil
	}
	basis, err := c.captureBackendBasis(db.backend.(backendSnapshotProvider))
	if err != nil {
		return err
	}
	old, err := c.installCOWBasis(basis, h.cut)
	basis.release()
	if err != nil {
		return err
	}
	c.handoff = nil
	c.handoffs.Add(1)
	if old != nil {
		old.drain()
	}
	c.releaseCut(h.cut)
	h.lease.Close()
	return nil
}

// flushCOWFrozen is called with flushMu held. It streams immutable metadata
// through the existing physical batch/build-group authority; values and pointer
// decoders are never materialized by the flush planner.
func (db *DB) flushCOWFrozen(syncFlush bool, publish *checkpointCommandWALPublish) error {
	c := db.cow
	if c.handoff != nil {
		if !c.handoff.accepted {
			return errors.New("COW unfinished publication")
		}
		if err := db.completeCOWHandoff(); err != nil {
			return err
		}
	}
	cut := c.retainCut()
	if cut == nil {
		return backenddb.ErrClosed
	}
	if len(cut.frozen) == 0 {
		c.releaseCut(cut)
		return nil
	}
	n := len(cut.frozen)
	chunkCap := db.flushBackendMaxEntries
	if chunkCap <= 0 {
		chunkCap = 1024
	}
	// The COW stream stays bounded even when legacy options disable chunking.
	if chunkCap > 1024 {
		chunkCap = 1024
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowFlushHandoff{}))) + memtable.COWAllocationCharge(uint64(chunkCap)*uint64(unsafe.Sizeof(batch.Entry{})))
	lease, err := c.budget.AcquireExternal(charge)
	if err != nil {
		c.releaseCut(cut)
		return err
	}
	h := &cowFlushHandoff{cut: cut, lease: lease}
	backend, ok := db.backend.(interface {
		BeginRootPublicationBuildGroupFromSnapshot(*backenddb.Snapshot) (*backenddb.RootPublicationBuildGroup, error)
	})
	if !ok {
		c.releaseCut(cut)
		lease.Close()
		return ErrCOWUnsupported
	}
	group, err := backend.BeginRootPublicationBuildGroupFromSnapshot(cut.basis.snapshot)
	if err != nil {
		c.releaseCut(cut)
		lease.Close()
		return err
	}
	ops := make([]batch.Entry, 0, chunkCap)
	// Sources apply oldest to newest within the same private build group. This
	// preserves tombstones and precedence without a second materialized merge.
	for i := range cut.frozen {
		it := cut.frozen[i].NewIterator(nil, nil)
		metadata, ok := it.(iterator.RevisionUnsafeIterator)
		if !ok {
			_ = it.Close()
			err = ErrCOWUnsupported
			break
		}
		for it.Valid() {
			value, ptr, flags, revision := metadata.UnsafeEntryWithRevision()
			op := batch.Entry{Type: batch.OpPut, Key: it.(iterator.UnsafeIterator).UnsafeKey(), Value: value, ValuePtr: ptr, IsPtr: flags&node.FlagPointer != 0, Revision: revision}
			if flags&node.FlagTombstone != 0 {
				op.Type = batch.OpDelete
				op.Value = nil
			}
			ops = append(ops, op)
			it.Next()
			if len(ops) == chunkCap || !it.Valid() {
				if err = it.Error(); err != nil {
					break
				}
				last := i == n-1 && !it.Valid()
				if err = db.writeCanonicalFlushRunOpsChunk(ops, syncFlush, publish, group, last, flushCollectionBackground); err != nil {
					break
				}
				ops = ops[:0]
			}
		}
		if err == nil {
			err = it.Error()
		}
		_ = it.Close()
		if err != nil {
			break
		}
	}
	err = errors.Join(err, group.Close())
	// Acceptance is the backend publication receipt, independent of a later
	// durability or cleanup error. Preserve the exact covered prefix on its old
	// basis so retries never reapply an already accepted group.
	if group.Accepted() {
		h.accepted = true
		c.handoff = h
		if err != nil {
			return err
		}
		return db.completeCOWHandoff()
	}
	c.releaseCut(cut)
	lease.Close()
	return err
}
