package caching

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
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
	}()
	key = normalizeRawKVPointKey(key)
	value, ptr, flags, revision, found := cut.domains[db.shardIndex(key)].getEntryWithRevision(key)
	entry := node.LeafEntry{ValuePtr: ptr, Flags: flags, Revision: revision}
	if found {
		if flags&node.FlagTombstone != 0 {
			return dst, revision, tree.ErrKeyNotFound
		}
		if flags&node.FlagPointer == 0 {
			return append(dst, normalizeRawKVValue(value)...), revision, nil
		}
	} else {
		var handled bool
		out, entry, handled, err = cut.basis.appendInline(db, key, dst)
		if handled {
			return out, entry.Revision, err
		}
	}

	// Pointer/dictionary materialization keeps the original admitted Snapshot
	// workspace and dependency policy. Transfer this exact cut once; do not
	// recapture a later publication or recursively enter the cache read gate.
	retained := cut
	cut = nil
	snapshot, err = db.snapshotFromCOWCut(retained)
	if err != nil {
		return dst, 0, err
	}
	if entry.Flags&node.FlagPointer != 0 {
		// The exact pointer metadata is stable under this retained cut. Reuse
		// the original materializer without traversing the same backend again.
		snapshot.cowReadMu.Lock()
		defer snapshot.cowReadMu.Unlock()
		value, revision, err := snapshot.cowValueFromEntryLocked(entry)
		if err != nil {
			return dst, revision, err
		}
		return append(dst, value...), revision, nil
	}
	return snapshot.cowGetVersionedAppendOpen(key, dst)
}

// appendInline finishes copy-out before releasing basis.readMu. It never exposes
// traversal scratch or changes inspection, checksum, leaf-cache or codec policy.
// Pointers and dictionary leaves use the original cut-local materializer.
func (b *cowBackendBasis) appendInline(db *DB, key, dst []byte) ([]byte, node.LeafEntry, bool, error) {
	b.readMu.Lock()
	defer b.readMu.Unlock()
	w, err := b.physicalWorkspaceLocked(db.cow.budget, db.cow.readLimits())
	if err != nil {
		return dst, node.LeafEntry{}, true, err
	}
	point, err := w.pointScratch()
	if err != nil {
		return dst, node.LeafEntry{}, true, err
	}
	readLeaf := func(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
		return b.readPhysicalLeafLocked(db.cow, ptr, dst, false)
	}
	entry, err := b.snapshot.GetEntryExactWithFixedScratch(key, point.key[:], point.leaf[:], readLeaf)
	if err == errCOWPhysicalDictionary {
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
	out := append(dst, entry.Value...)
	entry.Key, entry.Value = nil, nil
	return out, entry, true, nil
}
