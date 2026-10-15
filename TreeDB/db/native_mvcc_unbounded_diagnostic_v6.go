package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
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
// explicitly unmetered O(current tree) reconstruction diagnostic, not an
// implementation or cost prediction of the bounded prepared-output ABI.
func (c *NativeMVCCCurrentDeleteV6) PublishNativeMVCCUnboundedDiagnosticV6(basis *Snapshot, input iterator.UnsafeIterator) (accepted bool, retErr error) {
	if c == nil || c.phase != 3 || c.transaction == nil || c.db == nil || input == nil {
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
	filtered := nativeDiagnosticSurvivorsV6{UnsafeIterator: input, selected: c.record}
	filtered.skip()
	root, err := bulk.BuildWithOptions(&filtered, group.tracker, group.idx.pager, bulk.BuildOptions{EntryRevisions: true, LeafPrefixCompression: true, PackedValuePtr: true})
	if err != nil {
		return false, err
	}
	if err = filtered.Error(); err != nil {
		return false, err
	}
	if !filtered.removed {
		return false, rootpublication.ErrDurableRootOwnership
	}
	image, err := group.idx.pager.Get(root)
	if err != nil {
		return false, err
	}
	directory := make([]byte, page.PageSize)
	if err = node.EncodePrimaryDirectory(directory, txn.PrimaryConstructionReferenceV6().PageID, group.baseSeq+1, node.PrimaryOperand{Ref: page.PageChildRef(root), Digest: sha256.Sum256(image)}, nil); err != nil {
		return false, err
	}
	ready, err = txn.SealPrimaryConstructionV6(directory, [1]*primaryarena.Group{}, w)
	if !ready || err != nil {
		return false, err
	}
	group.currentRoot = txn.PrimaryConstructionReferenceV6().PageID
	group.maxEntryRevision = page.EntryRevision(c.db.meta.MaxEntryRevision)
	if filtered.maxRevision > group.maxEntryRevision {
		group.maxEntryRevision = filtered.maxRevision
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

type nativeDiagnosticSurvivorsV6 struct {
	iterator.UnsafeIterator
	selected    node.LeafEntry
	removed     bool
	failure     error
	maxRevision page.EntryRevision
}

func (it *nativeDiagnosticSurvivorsV6) skip() {
	for it.UnsafeIterator.Valid() {
		v, p, f, r := iterator.UnsafeEntryWithRevision(it.UnsafeIterator)
		if r > it.maxRevision {
			it.maxRevision = r
		}
		if !bytes.Equal(it.UnsafeIterator.UnsafeKey(), it.selected.Key) {
			return
		}
		if it.removed || r != it.selected.Revision || p != it.selected.ValuePtr || f != it.selected.Flags || (f&node.FlagPointer == 0 && !bytes.Equal(v, it.selected.Value)) {
			it.failure = rootpublication.ErrDurableRootOwnership
			return
		}
		it.removed = true
		it.UnsafeIterator.Next()
	}
}
func (it *nativeDiagnosticSurvivorsV6) Next() { it.UnsafeIterator.Next(); it.skip() }
func (it *nativeDiagnosticSurvivorsV6) Valid() bool {
	return it.failure == nil && it.UnsafeIterator.Valid()
}
func (it *nativeDiagnosticSurvivorsV6) Error() error {
	return errors.Join(it.failure, it.UnsafeIterator.Error())
}
func (it *nativeDiagnosticSurvivorsV6) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	return iterator.UnsafeEntryWithRevision(it.UnsafeIterator)
}
