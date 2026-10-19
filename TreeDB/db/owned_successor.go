package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// SeekGEOwnedAtRoot is the synchronous cache-backend successor capability. A
// nonzero root must belong to the caller's retained published-root closure;
// zero selects this capture's current root. It includes physical tombstones.
// Returned key/value bytes are owned, including materialized pointer values.
// No Snapshot, reader, iterator or scratch alias escapes the private capture.
func (db *DB) SeekGEOwnedAtRoot(rootID uint64, start, end []byte) (entry node.LeafEntry, found bool, err error) {
	r, err := db.acquireOneShotReadOrErr()
	if err != nil {
		return node.LeafEntry{}, false, err
	}
	// End the synchronous tree/reader scope before Close can retire its backing.
	entry, found, err = r.seekGEOwnedAtRoot(rootID, start, end)
	if closeErr := r.close(); closeErr != nil {
		return node.LeafEntry{}, false, errors.Join(err, closeErr)
	}
	return entry, found, err
}

func (r *oneShotRead) seekGEOwnedAtRoot(rootID uint64, start, end []byte) (node.LeafEntry, bool, error) {
	s := &r.snapshot
	// A by-value tree preserves PRIMARY's captured namespace/bounds for the
	// current root. Alternate roots do not enter Snapshot.rootTrees: oneShotRead
	// cleanup deliberately has no exported alternate-root cache to retire.
	captured := s.tree
	if rootID != 0 && rootID != s.state.RootPageID {
		captured = tree.Tree{}
		captured.Reset(s.idx.pager, &s.reader, rootID)
	}
	entry, found, err := captured.SeekGEWithFixedScratch(start, end, r.successorKey[:], r.successorLeaf[:], &r.successor, r.readSuccessorLeaf)
	if err != nil || !found {
		return node.LeafEntry{}, false, err
	}
	entry.Key = append([]byte(nil), entry.Key...)
	if entry.Flags&node.FlagTombstone != 0 {
		entry.Value = nil
	} else if entry.Flags&node.FlagPointer != 0 {
		// This is the same captured valueReader append path used by Tree.GetAppend.
		// Materialize before Close even if the cache later selects a newer candidate.
		entry.Value, err = s.reader.ReadUnsafeAppend(entry.ValuePtr, []byte{})
		if err != nil {
			return node.LeafEntry{}, false, err
		}
		if entry.Value == nil {
			entry.Value = []byte{}
		}
	} else {
		entry.Value = append([]byte(nil), entry.Value...)
	}
	return entry, true, nil
}

func (r *oneShotRead) readSuccessorLeaf(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	data, usedDst, err := r.snapshot.reader.ReadLeafLogPageUnsafeTo(ptr, dst)
	if err != nil {
		return nil, err
	}
	if len(data) != page.PageSize {
		return nil, node.ErrCorruptedNode
	}
	if !usedDst {
		// The normal reader may return a captured mmap/cache view. The fixed-scratch
		// tree requires this exact destination; it still validates the leaf checksum.
		copy(dst[:page.PageSize], data)
	}
	return dst[:page.PageSize:page.PageSize], nil
}
