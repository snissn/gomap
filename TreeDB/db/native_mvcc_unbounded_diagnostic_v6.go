package db

import (
	"bytes"
	"context"
	"errors"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// NativeMVCCDiagnosticAwaitV6 drains only already-visible publication debt.
// It never flushes the cache or changes the user tree.
func (db *DB) NativeMVCCDiagnosticAwaitV6() error {
	db.mu.RLock()
	seq := db.meta.CommitSeq
	db.mu.RUnlock()
	return db.rootPublication.coordinator.WaitThrough(context.Background(), seq)
}

// PublishNativeMVCCUnboundedDiagnosticV6 transfers the SAME native constructor
// into the original BuildGroup/candidate/publisher. It is a synchronous,
// explicitly unmetered captured-prefix/materialized-DATA diagnostic, not an
// implementation or cost prediction of the bounded prepared-output ABI.
func (c *NativeMVCCCurrentDeleteV6) PublishNativeMVCCUnboundedDiagnosticV6(basis *Snapshot, input iterator.UnsafeIterator, delta *batch.Batch) (accepted bool, retErr error) {
	if c == nil || c.phase != 3 || c.transaction == nil || c.db == nil || input == nil || delta == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	txn := c.transaction
	if err := txn.EnableNativeUnboundedDiagnosticV6(); err != nil {
		return false, err
	}
	group, err := c.db.BeginRootPublicationBuildGroupFromSnapshot(basis)
	if err != nil {
		return false, err
	}
	defer func() { retErr = errors.Join(retErr, group.Close()) }()
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.idx != c.snapshot.idx || c.snapshot.primaryRoot == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	// Transfer private construction custody before any allocation/effect.
	group.primaryCurrent = txn
	c.transaction = nil
	c.phase = 4
	w := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	cut := c.db.mvccAdmission.Begin()
	defer cut.Abort()
	ready, err := cut.ObservePrune(c.request, c.record.Key, w)
	if !ready || err != nil {
		return false, err
	}
	// Validate against the SAME retained composite winner immediately before
	// applying its physical delete. Do not omit the overlay row and resurrect
	// an older disk incarnation.
	input.Seek(c.record.Key)
	if !input.Valid() || !bytes.Equal(input.UnsafeKey(), c.record.Key) {
		return false, rootpublication.ErrDurableRootOwnership
	}
	v, p, f, r := iterator.UnsafeEntryWithRevision(input)
	if r != c.record.Revision || p != c.record.ValuePtr || f != c.record.Flags || (f&node.FlagPointer == 0 && !bytes.Equal(v, c.record.Value)) {
		return false, rootpublication.ErrDurableRootOwnership
	}
	if err = input.Error(); err != nil {
		return false, err
	}
	if err = delta.SetOps([]batch.Entry{{Type: batch.OpDelete, Key: bytes.Clone(c.record.Key), Revision: c.record.Revision}}); err != nil {
		return false, err
	}
	group.pinBatchValueLogSegmentsLocked(delta)
	z := group.idx.zipper.CloneWithAllocator(group.tracker)
	var oldPointerRefs zipper.PointerRefCounts
	var oldEntriesRemoved uint64
	base, sequence, retired, metrics, err := z.ApplyMaterializedPrimaryDeltaV6(group.currentRoot, delta, &oldPointerRefs, &oldEntriesRemoved)
	if err != nil {
		return false, err
	}
	entries, ranges := delta.ApplyPlan()
	refs, err := c.db.buildValueLogRefDelta(group.idx.pager, group.currentRoot, group.baseSeq, entries, ranges, &oldPointerRefs, oldEntriesRemoved, true)
	if err != nil {
		return false, err
	}
	group.mergeValueLogRefDeltaLocked(refs)
	releaseValueLogRefDelta(refs)
	group.retired = append(group.retired, retired...)
	mergeOrderedRootPublishMetrics(&group.metrics, metrics)
	group.recordVacuumMutationLocked(entries, ranges)
	directory := make([]byte, page.PageSize)
	if err = node.EncodePrimaryDirectory(directory, txn.PrimaryConstructionReferenceV6().PageID, sequence, base, nil); err != nil {
		return false, err
	}
	ready, err = txn.SealPrimaryConstructionV6(directory, [1]*primaryarena.Group{}, w)
	if !ready || err != nil {
		return false, err
	}
	group.currentRoot = txn.PrimaryConstructionReferenceV6().PageID
	if coverage := group.negativeCoverage; coverage != nil {
		for i := range entries {
			coverage.filter.Add(entries[i].Key)
		}
		coverage.nextRoot = group.currentRoot
	}
	group.maxEntryRevision = page.EntryRevision(c.db.meta.MaxEntryRevision)
	for _, entry := range entries {
		if entry.Revision > group.maxEntryRevision {
			group.maxEntryRevision = entry.Revision
		}
	}
	retErr = group.finalizeLocked(&Batch{db: c.db, physicalOnly: true}, true)
	accepted = group.accepted
	if accepted {
		cut.Commit()
	}
	// The transaction is now either coordinator-owned or retained by the SAME
	// BuildGroup cleanup. No disposing alias may remain in the public cursor.
	txn = nil
	if retErr == nil {
		c.phase = 6
		c.db = nil
		c.snapshot = nil
		c.source = nil
		c.record = node.LeafEntry{}
	}
	return accepted, retErr
}
