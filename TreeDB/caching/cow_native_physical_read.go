package caching

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// nativeEntryAtRoot shares only physical decoding/traversal, never logical
// visibility. The caller's original cut retains this basis through copy-out.
func (b *cowBackendBasis) nativeEntryAtRoot(budget *memtable.COWBudget, root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	b.readMu.Lock()
	defer b.readMu.Unlock()
	limit := budget.Limits().MaxInFlightBytes
	w, err := b.physicalWorkspaceLocked(budget, valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit})
	if err != nil {
		return node.LeafEntry{}, err
	}
	point, err := w.pointScratch()
	if err != nil {
		return node.LeafEntry{}, err
	}
	reader, err := b.snapshot.NativeReaderAtRoot(root)
	if err != nil {
		return node.LeafEntry{}, err
	}
	entry, err := reader.GetEntryExactWithFixedScratch(key, point.key[:], point.leaf[:], w.readLeaf)
	if err != nil {
		return node.LeafEntry{}, err
	}
	// End every page alias before unlocking. Caller input and returned output are
	// independent of the admitted workspace; pointer values retain no page bytes.
	entry.Key = nil
	value := entry.Value
	entry.Value = nil
	if copyValue && entry.Flags&(node.FlagPointer|node.FlagTombstone) == 0 {
		entry.Value = make([]byte, len(value))
		copy(entry.Value, value)
	}
	return entry, nil
}

func (c *NativeSemanticCapture) NativeEntryAtRoot(root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	if c == nil || c.cut == nil || c.cache == nil {
		return node.LeafEntry{}, backenddb.ErrClosed
	}
	return c.cut.basis.nativeEntryAtRoot(c.cache.budget, root, key, copyValue)
}
func (tx *nativeConditionalTxn) NativeEntryAtRoot(root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	if tx == nil || tx.ConditionalTxn == nil {
		return node.LeafEntry{}, backenddb.ErrConditionalTxnClosed
	}
	if err := tx.ensureOpen(); err != nil {
		return node.LeafEntry{}, err
	}
	return tx.snap.cowCut.basis.nativeEntryAtRoot(tx.db.cow.budget, root, key, copyValue)
}

// nativeValueAtPointer borrows the exact basis file and the SAME admitted
// dictionary-free decoder used by raw value reads. No pointer/file/caller scope
// is retained by the workspace. Copy-out and retained-output admission complete
// while readMu still excludes every other scratch borrower.
func (b *cowBackendBasis) nativeValueAtPointer(budget *memtable.COWBudget, ptr page.ValuePtr, admitOutput func(uint64) error) ([]byte, error) {
	b.readMu.Lock()
	defer b.readMu.Unlock()
	limit := budget.Limits().MaxInFlightBytes
	w, err := b.physicalWorkspaceLocked(budget, valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit})
	if err != nil {
		return nil, err
	}
	f, _, err := b.snapshot.PinnedValueLogFile(ptr.FileID)
	if err != nil {
		return nil, err
	}
	shape, err := valuelog.InspectCOWRecord(f, ptr, w.limits)
	if err != nil {
		return nil, err
	}
	if shape.Compressed && shape.DictID != 0 {
		return nil, valuelog.ErrCOWReadUnsupported
	}
	value, err := w.readInspected(f, ptr, shape, false, nil)
	if err != nil {
		return nil, err
	}
	if admitOutput != nil {
		if err = admitOutput(memtable.COWAllocationCharge(uint64(len(value)))); err != nil {
			return nil, err
		}
	}
	owned := make([]byte, len(value))
	copy(owned, value)
	return owned, nil
}

func (c *NativeSemanticCapture) NativeValueAtPointer(ptr page.ValuePtr, admitOutput func(uint64) error) ([]byte, error) {
	if c == nil || c.cut == nil || c.cache == nil {
		return nil, backenddb.ErrClosed
	}
	return c.cut.basis.nativeValueAtPointer(c.cache.budget, ptr, admitOutput)
}

func (tx *nativeConditionalTxn) NativeValueAtPointer(ptr page.ValuePtr, admitOutput func(uint64) error) ([]byte, error) {
	if tx == nil || tx.ConditionalTxn == nil {
		return nil, backenddb.ErrConditionalTxnClosed
	}
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	return tx.snap.cowCut.basis.nativeValueAtPointer(tx.db.cow.budget, ptr, admitOutput)
}
