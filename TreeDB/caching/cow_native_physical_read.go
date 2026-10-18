package caching

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
)

// nativeEntryAtRoot shares only physical decoding/traversal, never logical
// visibility. The caller's original cut retains this basis through copy-out.
func (b *cowBackendBasis) nativeEntryAtRoot(budget *memtable.COWBudget, root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	b.readMu.Lock()
	defer b.readMu.Unlock()
	if b.reader == nil {
		limit := budget.Limits().MaxInFlightBytes
		w, err := admitCOWReadWorkspace(budget, valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit})
		if err != nil {
			return node.LeafEntry{}, err
		}
		w.basis = b.snapshot
		b.reader = w
	}
	w := b.reader
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
