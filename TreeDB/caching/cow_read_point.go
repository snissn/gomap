package caching

import (
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// cowGetVersionedAppend owns one synchronous read of the current complete cut.
// Unlike an exported Snapshot, no handle or borrowed value survives this call.
// The original cut pins all sources; existing basis/cache scratch remains admitted
// and serialized by its original owner. Only caller-owned output escapes.
func (db *DB) cowGetVersionedAppend(key, dst []byte, missingIsNil bool) (out []byte, revision page.EntryRevision, err error) {
	c := db.cow
	c.captureCalls.Add(1) // Preserve the public point-read capture counter.
	if !c.beginRead() {
		return dst, 0, backenddb.ErrClosed
	}
	defer c.endRead()
	cut := c.retainCut()
	if cut == nil {
		return dst, 0, backenddb.ErrClosed
	}
	var snapshot *Snapshot
	var spare *cowReadCut
	defer func() {
		// Get normalizes a miss before joining cleanup, as the retained public
		// snapshot path does. Append calls preserve dst and ErrKeyNotFound.
		if missingIsNil && err == nil && out == nil {
			out = []byte{}
		}
		if missingIsNil && err == tree.ErrKeyNotFound {
			out, err = nil, nil
		}
		if snapshot != nil {
			err = joinCOWCleanupError(err, snapshot.Close())
		} else {
			err = joinCOWCleanupError(err, c.releaseCut(cut))
		}
		if spare != nil {
			err = joinCOWCleanupError(err, c.releaseCut(spare))
		}
	}()
	key = normalizeRawKVPointKey(key)
	value, ptr, flags, revision, found := cut.domains[db.shardIndex(key)].getEntryWithRevision(key)
	entry := node.LeafEntry{ValuePtr: ptr, Flags: flags, Revision: revision}
	var fresh cowFreshReadOwner
	if found {
		if flags&node.FlagTombstone != 0 {
			return dst, revision, tree.ErrKeyNotFound
		}
		if flags&node.FlagPointer == 0 {
			return append(dst, normalizeRawKVValue(value)...), revision, nil
		}
	} else {
		var handled bool
		out, entry, handled, err = cut.basis.appendInline(db, key, dst, &fresh)
		if handled {
			return out, entry.Revision, err
		}
	}

	// Pointer/dictionary materialization keeps the original admitted Snapshot
	// workspace and dependency policy. Transfer this exact cut; do not
	// recapture a later publication or recursively enter the cache read gate.
	if fresh.reader != nil {
		// Admission consumes retained even on refusal. Keep one spare reference
		// to THIS cut so refund/retry cannot recapture a later publication.
		c.cutMu.Lock()
		cut.refs++
		c.cutMu.Unlock()
		spare = cut
	}
	retained := cut
	cut = nil
	snapshot, err = db.snapshotFromCOWCut(retained)
	if errors.Is(err, memtable.ErrCOWCapacity) && fresh.refund() {
		retained, spare = spare, nil
		snapshot, err = db.snapshotFromCOWCut(retained)
	}
	if err != nil {
		return dst, 0, err
	}
	if spare != nil {
		retained, spare = spare, nil
		if releaseErr := c.releaseCut(retained); releaseErr != nil {
			return dst, 0, releaseErr
		}
	}
	if entry.Flags&node.FlagPointer != 0 {
		// The exact pointer metadata is stable under this retained cut. Reuse
		// the original exact-cut workspace with owned copy-out, without traversing
		// the same backend again.
		snapshot.cowReadMu.Lock()
		defer snapshot.cowReadMu.Unlock()
		out, revision, err := snapshot.cowAppendPointEntryLocked(entry, dst)
		if errors.Is(err, memtable.ErrCOWCapacity) && fresh.refund() {
			out, revision, err = snapshot.cowAppendPointEntryLocked(entry, dst)
		}
		if err != nil {
			return dst, revision, err
		}
		return out, revision, nil
	}
	out, revision, err = snapshot.cowOwnedPointAppendOpen(key, dst)
	if errors.Is(err, memtable.ErrCOWCapacity) && fresh.refund() {
		out, revision, err = snapshot.cowOwnedPointAppendOpen(key, dst)
	}
	return out, revision, err
}

// appendInline finishes copy-out before releasing basis.readMu. It never exposes
// traversal scratch or changes inspection, checksum, leaf-cache or codec policy.
// Pointers and captured-only dictionary providers retain the original
// cut-local materializer; capable dictionary leaves finish this traversal.
func (b *cowBackendBasis) appendInline(db *DB, key, dst []byte, freshOwner *cowFreshReadOwner) (out []byte, result node.LeafEntry, handled bool, err error) {
	b.readMu.Lock()
	defer b.readMu.Unlock()
	fresh := b.reader == nil
	defer func() {
		// Pointer/dictionary metadata has detached page aliases. Preserve a new
		// owner for reuse unless its original cut actually refuses admission.
		*freshOwner = cowReadHandoffLocked(&b.readMu, &b.reader, fresh, !handled, err)
	}()
	w, err := b.physicalWorkspaceLocked(db.cow.budget, db.cow.readLimits())
	if err != nil {
		return dst, node.LeafEntry{}, true, err
	}
	point, err := w.pointScratch()
	if err != nil {
		return dst, node.LeafEntry{}, true, err
	}
	var independent *cowCacheDictionaryRead
	if provider, ok := db.dictStore.(cowDictionaryReadProvider); ok {
		definition := cowCacheDictionaryRead{provider: provider, cache: db.cow}
		independent = &definition
	}
	readLeaf := func(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
		return b.readPhysicalLeafWithDictionary(db.cow, w, ptr, dst, false, false, independent)
	}
	entry, err := b.snapshot.GetEntryExactWithFixedScratch(key, point.key[:], point.leaf[:], readLeaf)
	_, dictionaryCapacity := err.(*cowPhysicalDictionaryCapacity)
	if err == errCOWPhysicalDictionary || dictionaryCapacity {
		// Leave this lock before the original exact-cut Snapshot handoff can
		// refund fresh traversal storage and retry its dictionary admission.
		return dst, node.LeafEntry{}, false, nil
	}
	if err != nil {
		return dst, node.LeafEntry{}, true, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		entry.Key, entry.Value = nil, nil
		return dst, entry, true, tree.ErrKeyNotFound
	}
	if entry.Flags&node.FlagPointer != 0 {
		entry.Key, entry.Value = nil, nil
		return dst, entry, false, nil
	}
	out = append(dst, entry.Value...)
	entry.Key, entry.Value = nil, nil
	return out, entry, true, nil
}
