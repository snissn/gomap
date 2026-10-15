package db

import (
	"bytes"
	"context"
	"errors"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
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
	records := c.records
	if len(records) == 0 {
		records = []node.LeafEntry{c.record}
	}
	// All entries are validated under the SAME fresh canonical cut before any
	// DATA mutation. A conflicting current winner refuses the whole batch.
	for i := range records {
		record := &records[i]
		ready, err := cut.ObservePrune(c.request, record.Key, w)
		if !ready || err != nil {
			return false, err
		}
		input.Seek(record.Key)
		if !input.Valid() || !bytes.Equal(input.UnsafeKey(), record.Key) {
			return false, rootpublication.ErrDurableRootOwnership
		}
		v, p, f, r := iterator.UnsafeEntryWithRevision(input)
		if r != record.Revision || p != record.ValuePtr || f != record.Flags || (f&node.FlagPointer == 0 && !bytes.Equal(v, record.Value)) {
			return false, rootpublication.ErrDurableRootOwnership
		}
		if err = input.Error(); err != nil {
			return false, err
		}
	}
	for i := range records {
		record := &records[i]
		if err = delta.SetOps([]batch.Entry{{Type: batch.OpDelete, Key: bytes.Clone(record.Key), Revision: record.Revision}}); err != nil {
			return false, err
		}
	}
	group.pinBatchValueLogSegmentsLocked(delta)
	z := group.idx.zipper.CloneWithAllocator(group.tracker)
	var oldPointerRefs zipper.PointerRefCounts
	var oldEntriesRemoved uint64
	var base node.PrimaryOperand
	var sequence uint64
	var retired []uint64
	var metrics adaptive.Metrics
	if c.output != nil {
		var refs *zipper.PointerRefCounts
		var ready bool
		base, refs, oldEntriesRemoved, ready = c.output.Result()
		if !ready {
			return false, ErrNativeMVCCSnapshotWork
		}
		oldPointerRefs = *refs
		sequence = c.outputSequence
		for i := 0; i < c.outputInput.RetiredCount; i++ {
			retired = append(retired, c.outputInput.Retired[i])
		}
		// SAME original tracker takes physical custody BEFORE producer aliases clear.
		c.output.VisitPages(func(allocated, old uint64) {
			if allocated != 0 {
				group.tracker.pages = append(group.tracker.pages, allocated)
			}
			if old != 0 {
				retired = append(retired, old)
			}
		})
		if !primaryarena.IsPage(group.currentRoot) {
			retired = append(retired, group.currentRoot)
		}
		owner, charge := c.output.Complete()
		c.output = nil
		refs = nil
		owner.RemovePending(charge)
	} else {
		base, sequence, retired, metrics, err = z.ApplyMaterializedPrimaryDeltaV6(group.currentRoot, delta, &oldPointerRefs, &oldEntriesRemoved)
		if err != nil {
			return false, err
		}
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
	ready, err := txn.SealPrimaryConstructionV6(directory, [1]*primaryarena.Group{}, w)
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
		c.records = nil
	}
	return accepted, retErr
}
