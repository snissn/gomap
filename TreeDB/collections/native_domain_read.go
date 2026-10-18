package collections

import (
	"bytes"
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// nativeDomainRead borrows a domain and the exact enclosing complete cut's
// Snapshot. Its containing read scope must retain BOTH owners through all
// operations and borrowed result use, and scrub this value before releasing
// either. It grants no independent view/pin/source or pointer-resolution
// authority. Public read-scope activation is separate from this metadata merge.
type nativeDomainRead struct {
	domain  *nativeDomainState
	base    backenddb.SnapshotRootReader
	hasBase bool
}

// nativeDomainReadScratch belongs to the caller's preadmitted read scope. A
// scratch instance cannot serve overlapping operations or outstanding entries.
type nativeDomainReadScratch struct {
	key, leaf [page.PageSize]byte
	successor tree.OwnedSuccessorScratch
}

func newNativeDomainRead(domain *nativeDomainState, basis *backenddb.Snapshot) (nativeDomainRead, error) {
	if domain == nil || domain.refs.Load() <= 0 {
		return nativeDomainRead{}, backenddb.ErrNativeSemanticUnsupported
	}
	base, err := basis.NativeReaderAtRoot(domain.baseRoot)
	if err != nil && !(domain.baseRoot == 0 && errors.Is(err, tree.ErrKeyNotFound)) {
		return nativeDomainRead{}, err
	}
	return nativeDomainRead{domain: domain, base: base, hasBase: domain.baseRoot != 0}, nil
}

func nativeDomainRecordEntry(row memtable.COWRecord) node.LeafEntry {
	return node.LeafEntry{Key: nativeBorrowedStringBytes(row.Key), Value: nativeBorrowedStringBytes(row.Value), ValuePtr: row.Ptr, Flags: row.Flags, Revision: row.Revision}
}

// getEntry preserves tombstones and pointer metadata. Only the enclosing
// captured merge suppresses deletions and resolves values using its exact
// source resources. An absent newer source never suppresses an older source.
func (r *nativeDomainRead) getEntry(key []byte, scratch *nativeDomainReadScratch, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (node.LeafEntry, error) {
	if r == nil || r.domain == nil || scratch == nil {
		return node.LeafEntry{}, backenddb.ErrNativeSemanticUnsupported
	}
	if r.domain.pending != nil {
		if row, ok := r.domain.pending.Get(key); ok {
			return nativeDomainRecordEntry(row), nil
		}
	}
	if row, ok := r.domain.captured.get(key); ok {
		return nativeDomainRecordEntry(row), nil
	}
	if !r.hasBase {
		return node.LeafEntry{}, tree.ErrKeyNotFound
	}
	return r.base.GetEntryExactWithFixedScratch(key, scratch.key[:], scratch.leaf[:], readLeaf)
}

// seekGE merges one candidate per original finite physical source. Sources are
// visited newest first; equal-key candidates therefore preserve the newest
// final effect, including a tombstone. It creates no cursor, source array,
// key copy or catalog walk. The base read runs last because it borrows scratch.
func (r *nativeDomainRead) seekGE(start, end []byte, scratch *nativeDomainReadScratch, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (node.LeafEntry, bool, error) {
	if r == nil || r.domain == nil || scratch == nil {
		return node.LeafEntry{}, false, backenddb.ErrNativeSemanticUnsupported
	}
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return node.LeafEntry{}, false, nil
	}
	var best node.LeafEntry
	found := false
	if r.domain.pending != nil {
		if row, ok := r.domain.pending.SeekGE(start, end); ok {
			best, found = nativeDomainRecordEntry(row), true
		}
	}
	for source := r.domain.captured; source != nil; source = source.older {
		if row, ok := source.source.root.SeekGE(start, end); ok {
			if !found || bytes.Compare(nativeBorrowedStringBytes(row.Key), best.Key) < 0 {
				best, found = nativeDomainRecordEntry(row), true
			}
		}
	}
	if r.hasBase {
		entry, ok, err := r.base.SeekGEEntryWithFixedScratch(start, end, scratch.key[:], scratch.leaf[:], &scratch.successor, readLeaf)
		if err != nil {
			return node.LeafEntry{}, false, err
		}
		if ok && (!found || bytes.Compare(entry.Key, best.Key) < 0) {
			best, found = entry, true
		}
	}
	return best, found, nil
}
