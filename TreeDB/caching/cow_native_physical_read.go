package caching

import (
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// nativeEntryAtRoot shares only physical decoding/traversal, never logical
// visibility. The caller's original cut retains this basis through copy-out.
func (b *cowBackendBasis) nativeEntryAtRoot(cache *cowCache, provider cowDictionaryReadProvider, root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	limit := cache.budget.Limits().MaxInFlightBytes
	limits := valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit}
	mu, owner, _ := b.tryReadWorkspace(0)
	opportunistic := mu != nil && owner != &b.reader
	if mu == nil {
		b.readMu.Lock()
		mu, owner = &b.readMu, &b.reader
	}
	for {
		fresh := *owner == nil
		w, err := b.workspaceLocked(owner, cache.budget, limits)
		var point *cowPointScratch
		if err == nil {
			point, err = w.pointScratch()
		}
		if err == nil {
			// From this point a real traversal error is final, even if its error value
			// also represents a read capacity or closed-resource refusal.
			entry, readErr := b.nativeEntryWithWorkspace(cache, provider, w, point, root, key, copyValue)
			mu.Unlock()
			return entry, readErr
		}
		// End the partial owner before its receipts refund and before primary retry.
		w, point = nil, nil
		if opportunistic {
			cowReadHandoffLocked(mu, owner, fresh, false, err)
		}
		mu.Unlock()
		if !opportunistic || !(errors.Is(err, memtable.ErrCOWCapacity) || errors.Is(err, memtable.ErrCOWClosed)) {
			return node.LeafEntry{}, err
		}
		b.readMu.Lock()
		mu, owner, opportunistic = &b.readMu, &b.reader, false
	}
}

// The selected owner remains locked through exact-root traversal and copy-out.
// Scratch borrows the original basis only; the provider is never retained.
func (b *cowBackendBasis) nativeEntryWithWorkspace(cache *cowCache, provider cowDictionaryReadProvider, w *cowReadWorkspace, point *cowPointScratch, root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	reader, err := b.snapshot.NativeReaderAtRoot(root)
	if err != nil {
		return node.LeafEntry{}, err
	}
	entry, err := reader.GetEntryExactWithFixedScratch(key, point.key[:], point.leaf[:], func(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
		return b.readNativePhysicalLeafWithWorkspace(cache, provider, w, ptr, dst)
	})
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
	return c.cut.basis.nativeEntryAtRoot(c.cache, c.dictionary, root, key, copyValue)
}
func (tx *nativeConditionalTxn) NativeEntryAtRoot(root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	if tx == nil || tx.ConditionalTxn == nil {
		return node.LeafEntry{}, backenddb.ErrConditionalTxnClosed
	}
	if err := tx.ensureOpen(); err != nil {
		return node.LeafEntry{}, err
	}
	provider, _ := tx.db.dictStore.(cowDictionaryReadProvider)
	return tx.snap.cowCut.basis.nativeEntryAtRoot(tx.db.cow, provider, root, key, copyValue)
}

// nativeLeafPage retains the primary traversal lock and cache decoder owner.
// ReadCOWLeafPage and verified cache hits copy into caller dst before these locks
// end; no shared scratch or file authority escapes.
func (b *cowBackendBasis) nativeLeafPage(cache *cowCache, provider cowDictionaryReadProvider, ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	b.readMu.Lock()
	defer b.readMu.Unlock()
	limit := cache.budget.Limits().MaxInFlightBytes
	if _, err := b.physicalWorkspaceLocked(cache.budget, valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit}); err != nil {
		return nil, err
	}
	return b.readNativePhysicalLeafLocked(cache, provider, ptr, dst)
}

func (c *NativeSemanticCapture) NativeLeafPage(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	if c == nil || c.cut == nil || c.cache == nil {
		return nil, backenddb.ErrClosed
	}
	return c.cut.basis.nativeLeafPage(c.cache, c.dictionary, ptr, dst)
}

func (tx *nativeConditionalTxn) NativeLeafPage(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	if tx == nil || tx.ConditionalTxn == nil {
		return nil, backenddb.ErrConditionalTxnClosed
	}
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	provider, _ := tx.db.dictStore.(cowDictionaryReadProvider)
	return tx.snap.cowCut.basis.nativeLeafPage(tx.db.cow, provider, ptr, dst)
}

// The provider is a synchronous capability from the original capture/txn.
// readPhysicalLeafWithDictionary inspects its exact pinned file before any hit;
// the cache decoder independently owns admitted definition bytes on a miss.
func (b *cowBackendBasis) readNativePhysicalLeafLocked(cache *cowCache, provider cowDictionaryReadProvider, ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	return b.readNativePhysicalLeafWithWorkspace(cache, provider, b.reader, ptr, dst)
}

func (b *cowBackendBasis) readNativePhysicalLeafWithWorkspace(cache *cowCache, provider cowDictionaryReadProvider, w *cowReadWorkspace, ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	var independent *cowCacheDictionaryRead
	if provider != nil {
		independent = &cowCacheDictionaryRead{provider: provider, cache: cache}
	}
	return b.readPhysicalLeafWithDictionary(cache, w, ptr, dst, true, false, independent)
}

// tryNativeBasisValue uses only the existing four basis owners. A selected
// owner is held through inspection, preparation, verified read and owned copy.
// false means no payload read occurred and the original shared route must run.
func (b *cowBackendBasis) tryNativeBasisValue(cache *cowCache, ptr page.ValuePtr, admitOutput func(uint64) error) (handled bool, value []byte, err error) {
	mu, owner, _ := b.tryReadWorkspace(0)
	if mu == nil {
		return false, nil, nil
	}
	defer mu.Unlock()
	fresh := *owner == nil
	limit := cache.budget.Limits().MaxInFlightBytes
	w, err := b.workspaceLocked(owner, cache.budget, valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit})
	if err != nil {
		if errors.Is(err, memtable.ErrCOWCapacity) || errors.Is(err, memtable.ErrCOWClosed) {
			return false, nil, nil
		}
		return true, nil, err
	}
	// An eligibility-only admission must not linger after an inspection error
	// or valid unsupported shape. Existing healthy scratch stays reusable.
	prepared := false
	defer func() {
		if fresh && !prepared && w != nil {
			b.retireReadWorkspaceLocked(owner, w)
		}
	}()
	f, _, err := b.snapshot.PinnedValueLogFile(ptr.FileID)
	if err != nil {
		return true, nil, err
	}
	shape, err := valuelog.InspectCOWRecord(f, ptr, w.limits)
	if err != nil {
		return true, nil, err
	}
	if shape.Compressed && (shape.DictID != 0 || (shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4)) {
		return false, nil, nil
	}
	// This is the sole additional fallback boundary. Preparation admits only
	// scratch: it reads no payload and cannot invoke the output callback. A
	// refusal may have replaced codecs or grown arrays, so retire the complete
	// exact selected owner, including any pre-existing point scratch.
	if _, _, _, err = w.prepareRead(shape, shape.ValueBytes, nil); err != nil {
		if errors.Is(err, memtable.ErrCOWCapacity) || errors.Is(err, memtable.ErrCOWClosed) {
			failed := w
			w = nil
			b.retireReadWorkspaceLocked(owner, failed)
			return false, nil, nil
		}
		return true, nil, err
	}
	prepared = true
	// Preparation repeats only matching, fitting checks in readInspected. All
	// errors from this actual checked read are terminal, regardless of sentinel.
	borrowed, err := w.readInspected(f, ptr, shape, false, nil, nil)
	if err != nil {
		return true, nil, err
	}
	if admitOutput != nil {
		if err := admitOutput(memtable.COWAllocationCharge(uint64(len(borrowed)))); err != nil {
			return true, nil, err
		}
	}
	owned := make([]byte, len(borrowed))
	copy(owned, borrowed)
	return true, owned, nil
}

// nativeValueAtPointer borrows the exact retained basis file. Eligible raw and
// dictionary-free Snappy/LZ4 values can lazily admit an idle basis workspace;
// the original shared decoder owns every remaining route. Each route finishes
// verification, retained-output admission and independent copy under its mutex.
func (b *cowBackendBasis) nativeValueAtPointer(cache *cowCache, provider cowDictionaryReadProvider, ptr page.ValuePtr, admitOutput func(uint64) error) ([]byte, error) {
	if handled, value, err := b.tryNativeBasisValue(cache, ptr, admitOutput); handled {
		return value, err
	}
	cache.physicalMu.Lock()
	defer cache.physicalMu.Unlock()
	limit := cache.budget.Limits().MaxInFlightBytes
	w, err := cache.physicalWorkspaceLocked(valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit})
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
	var definition *cowCacheDictionaryRead
	if shape.Compressed && shape.DictID != 0 {
		if provider == nil {
			return nil, valuelog.ErrCOWReadUnsupported
		}
		definition = &cowCacheDictionaryRead{provider: provider, cache: cache}
	}
	value, err := w.readInspected(f, ptr, shape, false, nil, definition)
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
	return c.cut.basis.nativeValueAtPointer(c.cache, c.dictionary, ptr, admitOutput)
}

func (tx *nativeConditionalTxn) NativeValueAtPointer(ptr page.ValuePtr, admitOutput func(uint64) error) ([]byte, error) {
	if tx == nil || tx.ConditionalTxn == nil {
		return nil, backenddb.ErrConditionalTxnClosed
	}
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	provider, _ := tx.db.dictStore.(cowDictionaryReadProvider)
	return tx.snap.cowCut.basis.nativeValueAtPointer(tx.db.cow, provider, ptr, admitOutput)
}
