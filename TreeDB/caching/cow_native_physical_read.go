package caching

import (
	"bytes"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// nativeEntryAtRoot shares only physical decoding/traversal, never logical
// visibility. The caller's original cut retains this basis through copy-out.
func (b *cowBackendBasis) nativeEntryAtRoot(cache *cowCache, provider cowDictionaryReadProvider, root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	return b.nativeEntryAtRootInto(cache, provider, root, key, copyValue, nil, false)
}

func (b *cowBackendBasis) nativeEntryAtRootAdmitted(cache *cowCache, provider cowDictionaryReadProvider, root uint64, key []byte, admitOutput func(uint64) error) (node.LeafEntry, error) {
	return b.nativeEntryAtRootWithAdmit(cache, provider, root, key, true, nil, false, admitOutput)
}

func (b *cowBackendBasis) nativeEntryAtRootInto(cache *cowCache, provider cowDictionaryReadProvider, root uint64, key []byte, copyValue bool, dst []byte, useDst bool) (node.LeafEntry, error) {
	return b.nativeEntryAtRootWithAdmit(cache, provider, root, key, copyValue, dst, useDst, nil)
}

func (b *cowBackendBasis) nativeEntryAtRootWithAdmit(cache *cowCache, provider cowDictionaryReadProvider, root uint64, key []byte, copyValue bool, dst []byte, useDst bool, admitOutput func(uint64) error) (node.LeafEntry, error) {
	limit := cache.budget.Limits().MaxInFlightBytes
	limits := valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit}
	if mu, owner, ok := b.tryLockReadLane(cowReadStripe(key)); ok {
		defer mu.Unlock()
		fresh := *owner == nil
		w, err := b.workspaceLocked(owner, cache.budget, limits)
		if err != nil {
			return node.LeafEntry{}, err
		}
		entry, err := b.readNativeEntryWithWorkspaceAdmitted(cache, w, provider, root, key, copyValue, dst, useDst, admitOutput)
		if err != nil && fresh && *owner == w && w.point == nil && len(w.codecs) == 0 && w.allocations == nil {
			*owner = nil
			w.close()
		}
		return entry, err
	}
	// All basis read lanes are busy: admit a bounded per-call temporary
	// workspace so concurrent readers never serialize.
	w, err := admitCOWReadWorkspace(cache.budget, limits)
	if err != nil {
		return node.LeafEntry{}, err
	}
	w.basis = b.snapshot
	defer w.close()
	return b.readNativeEntryWithWorkspaceAdmitted(cache, w, provider, root, key, copyValue, dst, useDst, admitOutput)
}

func (b *cowBackendBasis) readNativeEntryWithWorkspace(cache *cowCache, w *cowReadWorkspace, provider cowDictionaryReadProvider, root uint64, key []byte, copyValue bool, dst []byte, useDst bool) (node.LeafEntry, error) {
	return b.readNativeEntryWithWorkspaceAdmitted(cache, w, provider, root, key, copyValue, dst, useDst, nil)
}

func (w *cowReadWorkspace) cachedLeafReadScratch() []byte {
	if w == nil {
		return nil
	}
	if len(w.codecs) > 0 && w.codecs[0] != nil && cap(w.codecs[0].input) > 0 {
		return w.codecs[0].input[:cap(w.codecs[0].input)]
	}
	if cap(w.input) > 0 {
		return w.input[:cap(w.input)]
	}
	return nil
}

func (b *cowBackendBasis) readNativeEntryWithWorkspaceAdmitted(cache *cowCache, w *cowReadWorkspace, provider cowDictionaryReadProvider, root uint64, key []byte, copyValue bool, dst []byte, useDst bool, admitOutput func(uint64) error) (node.LeafEntry, error) {
	point, err := w.pointScratch()
	if err != nil {
		return node.LeafEntry{}, err
	}
	var independent *cowCacheDictionaryRead
	if provider != nil {
		independent = &cowCacheDictionaryRead{provider: provider, cache: cache}
	}
	point.leafGen++
	var out node.LeafEntry
	// Borrow verified cached leaf pages under a read lock (zero-copy, zero-pread)
	// with a single snapshot read pin while preserving dictionary capability,
	// COWReadLimits, and leafPageCache accounting; any miss or policy mismatch
	// falls back to full inspection.
	err = b.snapshot.GetEntryExactAtRootWithPinnedCOWLeafView(
		root, key, point.key[:], point.leaf[:], w.cachedLeafReadScratch(), w.limits,
		func(shape valuelog.COWRecordShape) bool {
			if shape.DictID != 0 && (independent == nil || independent.provider == nil || independent.cache != cache) {
				return false
			}
			if shape.Compressed && shape.DictID == 0 && shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4 {
				return false
			}
			return true
		},
		func(ptr page.LeafLogPtr, leafDst []byte, record backenddb.PinnedCOWLeafRead) ([]byte, tree.LeafLogPageReadState, error) {
			return readInspectedPhysicalLeaf(cache, w, ptr, leafDst, true, true, independent, &record)
		},
		func(entry node.LeafEntry) error {
			// End every page alias before unlocking. Caller input and returned output are
			// independent of the admitted workspace; pointer values retain no page bytes.
			entry.Key = nil
			value := entry.Value
			entry.Value = nil
			if copyValue && entry.Flags&(node.FlagPointer|node.FlagTombstone) == 0 {
				if useDst {
					entry.Value = append(dst[:0], value...)
				} else {
					if admitOutput != nil {
						if err := admitOutput(memtable.COWAllocationCharge(uint64(len(value)))); err != nil {
							return err
						}
					}
					entry.Value = make([]byte, len(value))
					copy(entry.Value, value)
				}
			}
			out = entry
			return nil
		},
	)
	if err != nil {
		return node.LeafEntry{}, err
	}
	return out, nil
}

func (c *cowReadCut) NativeEntryAtRoot(root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	if c == nil || c.basis == nil || c.cache == nil {
		return node.LeafEntry{}, backenddb.ErrClosed
	}
	return c.basis.nativeEntryAtRoot(c.cache, c.dictionaryProvider(), root, key, copyValue)
}

func (c *cowReadCut) NativeEntryAtRootInto(root uint64, key, dst []byte) (node.LeafEntry, error) {
	if c == nil || c.basis == nil || c.cache == nil {
		return node.LeafEntry{}, backenddb.ErrClosed
	}
	return c.basis.nativeEntryAtRootInto(c.cache, c.dictionaryProvider(), root, key, true, dst, dst != nil)
}

func (c *cowReadCut) NativeValueAtPointer(ptr page.ValuePtr, admitOutput func(uint64) error) ([]byte, error) {
	if c == nil || c.basis == nil || c.cache == nil {
		return nil, backenddb.ErrClosed
	}
	return c.basis.nativeValueAtPointer(c.cache, c.dictionaryProvider(), ptr, admitOutput)
}

func (c *cowReadCut) NativeValueAtPointerInto(ptr page.ValuePtr, dst []byte, admitOutput func(uint64) error) ([]byte, error) {
	if c == nil || c.basis == nil || c.cache == nil {
		return nil, backenddb.ErrClosed
	}
	return c.basis.nativeValueAtPointerInto(c.cache, c.dictionaryProvider(), ptr, dst, dst != nil, admitOutput)
}

func (c *NativeSemanticCapture) NativeEntryAtRoot(root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	if c == nil || c.cut == nil || c.cache == nil {
		return node.LeafEntry{}, backenddb.ErrClosed
	}
	return c.cut.basis.nativeEntryAtRoot(c.cache, c.dictionary, root, key, copyValue)
}

func (c *NativeSemanticCapture) NativeEntryAtRootAdmitted(root uint64, key []byte, admitOutput func(uint64) error) (node.LeafEntry, error) {
	if c == nil || c.cut == nil || c.cache == nil {
		return node.LeafEntry{}, backenddb.ErrClosed
	}
	return c.cut.basis.nativeEntryAtRootAdmitted(c.cache, c.dictionary, root, key, admitOutput)
}

func (c *NativeSemanticCapture) NativeEntryAtRootInto(root uint64, key, dst []byte) (node.LeafEntry, error) {
	if c == nil || c.cut == nil || c.cache == nil {
		return node.LeafEntry{}, backenddb.ErrClosed
	}
	return c.cut.basis.nativeEntryAtRootInto(c.cache, c.dictionary, root, key, true, dst, dst != nil)
}

func (tx *nativeConditionalTxn) NativeEntryAtRoot(root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	if tx == nil || tx.nativeClosed {
		return node.LeafEntry{}, backenddb.ErrConditionalTxnClosed
	}
	if err := tx.ensureOpen(); err != nil {
		return node.LeafEntry{}, err
	}
	provider, _ := tx.db.dictStore.(cowDictionaryReadProvider)
	return tx.snap.cowCut.basis.nativeEntryAtRoot(tx.db.cow, provider, root, key, copyValue)
}

func (tx *nativeConditionalTxn) NativeEntryAtRootAdmitted(root uint64, key []byte, admitOutput func(uint64) error) (node.LeafEntry, error) {
	if tx == nil || tx.nativeClosed {
		return node.LeafEntry{}, backenddb.ErrConditionalTxnClosed
	}
	if err := tx.ensureOpen(); err != nil {
		return node.LeafEntry{}, err
	}
	provider, _ := tx.db.dictStore.(cowDictionaryReadProvider)
	return tx.snap.cowCut.basis.nativeEntryAtRootAdmitted(tx.db.cow, provider, root, key, admitOutput)
}

// nativeLeafPage uses lane 0 (basis.readMu / basis.reader) when uncontended to
// preserve workspace reuse invariants, and falls back to a bounded per-call
// stack workspace when contended so concurrent leaf readers never serialize or
// retain extra ExternalLeases.
func (b *cowBackendBasis) nativeLeafPage(cache *cowCache, provider cowDictionaryReadProvider, ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	limit := cache.budget.Limits().MaxInFlightBytes
	limits := valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit}
	if b.readMu.TryLock() {
		defer b.readMu.Unlock()
		fresh := b.reader == nil
		w, err := b.physicalWorkspaceLocked(cache.budget, limits)
		if err != nil {
			return nil, err
		}
		data, err := b.readNativePhysicalLeafWithWorkspace(cache, w, provider, ptr, dst)
		if err != nil && fresh && b.reader == w && w.point == nil && len(w.codecs) == 0 && w.allocations == nil {
			b.reader = nil
			w.close()
		}
		return data, err
	}
	// Lane 0 is busy: admit a bounded per-call temporary workspace on the stack
	// and refund its lease immediately after copy-out into caller dst.
	lease, err := cache.budget.AcquireExternalValue(cowReadWorkspaceInspectionCharge())
	if err != nil {
		return nil, err
	}
	w := cowReadWorkspace{basis: b.snapshot, budget: cache.budget, limits: limits}
	defer func() {
		w.closeValueScratch()
		w.basis, w.budget = nil, nil
		lease.Close()
	}()
	return b.readNativePhysicalLeafWithWorkspace(cache, &w, provider, ptr, dst)
}

// nativeSeekLiveKeyAtRoot performs a non-blocking settled SeekGE skipping
// physical tombstones using a warm basis read lane (or bounded temporary
// fallback) and returns an owned copy of the winning key.
func (b *cowBackendBasis) nativeSeekLiveKeyAtRoot(cache *cowCache, provider cowDictionaryReadProvider, root uint64, start, end []byte) ([]byte, bool, error) {
	limit := cache.budget.Limits().MaxInFlightBytes
	limits := valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit}
	if mu, owner, ok := b.tryLockReadLane(cowReadStripe(start)); ok {
		defer mu.Unlock()
		fresh := *owner == nil
		w, err := b.workspaceLocked(owner, cache.budget, limits)
		if err != nil {
			return nil, false, err
		}
		key, found, err := b.seekLiveKeyWithWorkspace(cache, w, provider, root, start, end)
		if err != nil && fresh && *owner == w && w.point == nil && len(w.codecs) == 0 && w.allocations == nil {
			*owner = nil
			w.close()
		}
		return key, found, err
	}
	w, err := admitCOWReadWorkspace(cache.budget, limits)
	if err != nil {
		return nil, false, err
	}
	w.basis = b.snapshot
	defer w.close()
	return b.seekLiveKeyWithWorkspace(cache, w, provider, root, start, end)
}

func (b *cowBackendBasis) seekLiveKeyWithWorkspace(cache *cowCache, w *cowReadWorkspace, provider cowDictionaryReadProvider, root uint64, start, end []byte) ([]byte, bool, error) {
	point, err := w.pointScratch()
	if err != nil {
		return nil, false, err
	}
	reader, err := b.snapshot.NativeReaderAtRoot(root)
	if err != nil {
		return nil, false, err
	}
	var independent *cowCacheDictionaryRead
	if provider != nil {
		independent = &cowCacheDictionaryRead{provider: provider, cache: cache}
	}
	checkShape := func(shape valuelog.COWRecordShape) bool {
		if shape.DictID != 0 && (independent == nil || independent.provider == nil || independent.cache != cache) {
			return false
		}
		if shape.Compressed && shape.DictID == 0 && shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4 {
			return false
		}
		return true
	}
	readLeaf := func(ptr page.LeafLogPtr, leafDst []byte) ([]byte, error) {
		if data, hit, err := b.snapshot.CopyVerifiedCOWLeafCached(ptr, w.limits, checkShape, leafDst); err != nil || hit {
			return data, err
		}
		return b.readNativePhysicalLeafWithWorkspace(cache, w, provider, ptr, leafDst)
	}
	curStart := start
	var nextStartBuf [page.PageSize + 1]byte
	for {
		point.leafGen++
		entry, found, err := reader.SeekGEEntryWithFixedScratch(curStart, end, point.key[:], point.leaf[:], &point.successor, readLeaf)
		if err != nil || !found {
			return nil, false, err
		}
		if entry.Flags&node.FlagTombstone == 0 {
			return bytes.Clone(entry.Key), true, nil
		}
		if len(entry.Key) >= len(nextStartBuf) {
			return nil, false, memtable.ErrCOWCapacity
		}
		n := copy(nextStartBuf[:], entry.Key)
		nextStartBuf[n] = 0
		curStart = nextStartBuf[:n+1]
	}
}

func (c *NativeSemanticCapture) NativeSeekLiveKeyAtRoot(root uint64, start, end []byte) ([]byte, bool, error) {
	if c == nil || c.cut == nil || c.cache == nil {
		return nil, false, backenddb.ErrClosed
	}
	return c.cut.basis.nativeSeekLiveKeyAtRoot(c.cache, c.dictionary, root, start, end)
}

// nativeScanLiveKeysAtRoot streams live keys in [start, end) from a settled
// physical root using a warm lane's reusable OwnedPointerProjectionWorkspace.
// visit MUST be an internal, non-blocking, non-reentrant collector that copies
// any bytes it retains; arbitrary user callbacks MUST NOT run inside visit.
func (b *cowBackendBasis) nativeScanLiveKeysAtRoot(cache *cowCache, provider cowDictionaryReadProvider, root uint64, start, end []byte, visit func(key []byte) (bool, error)) error {
	limit := cache.budget.Limits().MaxInFlightBytes
	limits := valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit}
	if mu, owner, ok := b.tryLockReadLane(cowReadStripe(start)); ok {
		defer mu.Unlock()
		fresh := *owner == nil
		w, err := b.workspaceLocked(owner, cache.budget, limits)
		if err != nil {
			return err
		}
		err = b.scanLiveKeysWithWorkspace(cache, w, provider, root, start, end, visit)
		if err != nil && fresh && *owner == w && w.point == nil && w.projectionLease == nil && len(w.codecs) == 0 && w.allocations == nil {
			*owner = nil
			w.close()
		}
		return err
	}
	w, err := admitCOWReadWorkspace(cache.budget, limits)
	if err != nil {
		return err
	}
	w.basis = b.snapshot
	defer w.close()
	return b.scanLiveKeysWithWorkspace(cache, w, provider, root, start, end, visit)
}

type cowNativeScanLiveKeysState struct {
	reader      backenddb.SnapshotRootReader
	b           *cowBackendBasis
	cache       *cowCache
	w           *cowReadWorkspace
	provider    cowDictionaryReadProvider
	independent cowCacheDictionaryRead
	hasDict     bool

	checkShapeFn      func(valuelog.COWRecordShape) bool
	readLeafFn        func(page.LeafLogPtr, []byte) ([]byte, error)
	admitProjectionFn func(uint64) error
}

var cowNativeScanLiveKeysStatePool = sync.Pool{
	New: func() any {
		s := new(cowNativeScanLiveKeysState)
		s.checkShapeFn = s.checkShape
		s.readLeafFn = s.readLeaf
		s.admitProjectionFn = s.admitProjection
		return s
	},
}

func (s *cowNativeScanLiveKeysState) checkShape(shape valuelog.COWRecordShape) bool {
	if shape.DictID != 0 && (!s.hasDict || s.independent.provider == nil || s.independent.cache != s.cache) {
		return false
	}
	if shape.Compressed && shape.DictID == 0 && shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4 {
		return false
	}
	return true
}

func (s *cowNativeScanLiveKeysState) readLeaf(ptr page.LeafLogPtr, leafDst []byte) ([]byte, error) {
	if data, hit, err := s.b.snapshot.CopyVerifiedCOWLeafCached(ptr, s.w.limits, s.checkShapeFn, leafDst); err != nil || hit {
		return data, err
	}
	return s.b.readNativePhysicalLeafWithWorkspace(s.cache, s.w, s.provider, ptr, leafDst)
}

func (s *cowNativeScanLiveKeysState) admitProjection(bytes uint64) error {
	return s.w.admitProjection(bytes)
}

func (s *cowNativeScanLiveKeysState) reset() {
	s.reader = backenddb.SnapshotRootReader{}
	s.b = nil
	s.cache = nil
	s.w = nil
	s.provider = nil
	s.independent = cowCacheDictionaryRead{}
	s.hasDict = false
}

func (b *cowBackendBasis) scanLiveKeysWithWorkspace(cache *cowCache, w *cowReadWorkspace, provider cowDictionaryReadProvider, root uint64, start, end []byte, visit func(key []byte) (bool, error)) error {
	s := cowNativeScanLiveKeysStatePool.Get().(*cowNativeScanLiveKeysState)
	var err error
	s.reader, err = b.snapshot.NativeReaderAtRoot(root)
	if err != nil {
		s.reset()
		cowNativeScanLiveKeysStatePool.Put(s)
		return err
	}
	s.b = b
	s.cache = cache
	s.w = w
	s.provider = provider
	if provider != nil {
		s.independent = cowCacheDictionaryRead{provider: provider, cache: cache}
		s.hasDict = true
	}
	it, err := s.reader.OwnedPointerProjectionIteratorWithWorkspace(start, end, s.readLeafFn, &w.projection, s.admitProjectionFn)
	if err != nil {
		s.reset()
		cowNativeScanLiveKeysStatePool.Put(s)
		return err
	}
	defer func() {
		_ = it.Close()
		w.projection.Reset()
		s.reset()
		cowNativeScanLiveKeysStatePool.Put(s)
	}()
	for ; it.Valid(); it.Next() {
		_, _, flags, _ := iterator.UnsafeEntryWithRevision(it)
		if err := it.Error(); err != nil {
			return err
		}
		if flags&node.FlagTombstone != 0 {
			continue
		}
		cont, err := visit(it.UnsafeKey())
		if err != nil || !cont {
			return err
		}
	}
	return it.Error()
}

func (c *NativeSemanticCapture) NativeScanLiveKeysAtRoot(root uint64, start, end []byte, visit func(key []byte) (bool, error)) error {
	if c == nil || c.cut == nil || c.cache == nil {
		return backenddb.ErrClosed
	}
	return c.cut.basis.nativeScanLiveKeysAtRoot(c.cache, c.dictionary, root, start, end, visit)
}

func (c *cowReadCut) NativeScanLiveKeysAtRoot(root uint64, start, end []byte, visit func(key []byte) (bool, error)) error {
	if c == nil || c.basis == nil || c.cache == nil {
		return backenddb.ErrClosed
	}
	return c.basis.nativeScanLiveKeysAtRoot(c.cache, c.dictionaryProvider(), root, start, end, visit)
}

func (c *NativeSemanticCapture) NativeLeafPage(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	if c == nil || c.cut == nil || c.cache == nil {
		return nil, backenddb.ErrClosed
	}
	return c.cut.basis.nativeLeafPage(c.cache, c.dictionary, ptr, dst)
}

func (tx *nativeConditionalTxn) NativeLeafPage(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	if tx == nil || tx.nativeClosed {
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
func (b *cowBackendBasis) readNativePhysicalLeafWithWorkspace(cache *cowCache, w *cowReadWorkspace, provider cowDictionaryReadProvider, ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	var independent *cowCacheDictionaryRead
	if provider != nil {
		independent = &cowCacheDictionaryRead{provider: provider, cache: cache}
	}
	return b.readPhysicalLeafWithDictionary(cache, w, ptr, dst, true, false, independent)
}

// nativeValueAtPointer borrows the exact retained basis file and one of the
// original cache's independently owned decoders. No basis/file/caller scope is
// retained by scratch. Copy-out and retained-output admission finish before the
// lane lock ends.
func (b *cowBackendBasis) nativeValueAtPointer(cache *cowCache, provider cowDictionaryReadProvider, ptr page.ValuePtr, admitOutput func(uint64) error) ([]byte, error) {
	return b.nativeValueAtPointerInto(cache, provider, ptr, nil, false, admitOutput)
}

func cowValuePtrStripe(ptr page.ValuePtr) uint32 {
	x := uint64(ptr.FileID)<<32 ^ ptr.Offset ^ uint64(page.ValuePtrSubIndex(ptr))
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	return uint32(x)
}

func cowNativeValueCheckShapeNoProvider(shape valuelog.COWRecordShape) bool {
	return !(shape.Compressed && shape.DictID != 0)
}

func (b *cowBackendBasis) nativeValueAtPointerInto(cache *cowCache, provider cowDictionaryReadProvider, ptr page.ValuePtr, dst []byte, useDst bool, admitOutput func(uint64) error) ([]byte, error) {
	limit := cache.budget.Limits().MaxInFlightBytes
	limits := valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit}
	// Acquire lane 0 first (preserving single-goroutine lane-0 affinity), then
	// spill to warm/admittable physicalSlots for ALL value-log records (both
	// block-compressed and dictionary-compressed) so concurrent readers reuse
	// pre-admitted decoder workspaces without hammering COWBudget.mu or mheap.
	if mu, slotReader, ok := cache.tryLockPhysicalLane(cowValuePtrStripe(ptr)); ok {
		defer mu.Unlock()
		fresh := *slotReader == nil
		w, err := cache.physicalWorkspaceSlotLocked(slotReader, limits)
		if err != nil {
			return nil, err
		}
		owned, err := b.readNativeValueWithWorkspace(cache, w, provider, ptr, dst, useDst, admitOutput)
		if err != nil && fresh && *slotReader == w && len(w.codecs) == 0 && w.allocations == nil {
			*slotReader = nil
			w.close()
		}
		return owned, err
	}
	// All retained physical lanes are busy (or budget headroom is tight for
	// admitting a new spillover slot): admit a bounded per-call temporary
	// workspace on the stack and refund its lease immediately after copy-out.
	inspectLease, err := cache.budget.AcquireExternalValue(cowReadWorkspaceInspectionCharge())
	if err != nil {
		return nil, err
	}
	w := cowReadWorkspace{budget: cache.budget, limits: limits}
	defer func() {
		w.closeValueScratch()
		w.budget = nil
		inspectLease.Close()
	}()
	return b.readNativeValueWithWorkspace(cache, &w, provider, ptr, dst, useDst, admitOutput)
}

func (b *cowBackendBasis) readNativeValueWithWorkspace(cache *cowCache, w *cowReadWorkspace, provider cowDictionaryReadProvider, ptr page.ValuePtr, dst []byte, useDst bool, admitOutput func(uint64) error) ([]byte, error) {
	f, _, err := b.snapshot.PinnedValueLogSegment(ptr.FileID)
	if err != nil {
		return nil, err
	}
	if page.ValuePtrIsGrouped(ptr) {
		var checkShape func(valuelog.COWRecordShape) bool
		if provider == nil {
			checkShape = cowNativeValueCheckShapeNoProvider
		}
		if admitOutput == nil {
			if owned, hit := f.CopyVerifiedCOWGroupedValueCached(ptr, w.limits, checkShape, dst, useDst, false); hit {
				return owned, nil
			}
		} else if cap(w.output) > 0 {
			// Copy verified bytes into this lane's already-admitted workspace
			// output under slot.mu.RLock(), then release the slot lock before
			// invoking admitOutput and allocating caller output. This avoids
			// lock-order inversion with COWBudget.mu, unlock/relock races, and
			// premature admission on fallback errors.
			if val, hit := f.CopyVerifiedCOWGroupedValueCached(ptr, w.limits, checkShape, w.output[:0], true, true); hit {
				if err := admitOutput(memtable.COWAllocationCharge(uint64(len(val)))); err != nil {
					return nil, err
				}
				if useDst {
					return append(dst[:0], val...), nil
				}
				owned := make([]byte, len(val))
				copy(owned, val)
				return owned, nil
			}
		}
	}
	shape, err := f.InspectCOWRecord(ptr, w.limits)
	if err != nil {
		return nil, err
	}
	return b.readInspectedNativeValueWithWorkspace(cache, w, provider, f, ptr, shape, dst, useDst, admitOutput)
}

func (b *cowBackendBasis) readInspectedNativeValueWithWorkspace(cache *cowCache, w *cowReadWorkspace, provider cowDictionaryReadProvider, f *valuelog.File, ptr page.ValuePtr, shape valuelog.COWRecordShape, dst []byte, useDst bool, admitOutput func(uint64) error) ([]byte, error) {
	var definition *cowCacheDictionaryRead
	if shape.Compressed && shape.DictID != 0 {
		if provider == nil {
			return nil, valuelog.ErrCOWReadUnsupported
		}
		definition = &cowCacheDictionaryRead{provider: provider, cache: cache}
	}
	if useDst && admitOutput == nil {
		return w.appendInspectedAndCache(f, ptr, shape, dst[:0], definition)
	}
	value, err := w.readInspectedAndCache(f, ptr, shape, definition)
	if err != nil {
		return nil, err
	}
	if admitOutput != nil {
		if err = admitOutput(memtable.COWAllocationCharge(uint64(len(value)))); err != nil {
			return nil, err
		}
	}
	if useDst {
		return append(dst[:0], value...), nil
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

func (c *NativeSemanticCapture) NativeValueAtPointerInto(ptr page.ValuePtr, dst []byte, admitOutput func(uint64) error) ([]byte, error) {
	if c == nil || c.cut == nil || c.cache == nil {
		return nil, backenddb.ErrClosed
	}
	return c.cut.basis.nativeValueAtPointerInto(c.cache, c.dictionary, ptr, dst, dst != nil, admitOutput)
}

func (tx *nativeConditionalTxn) NativeValueAtPointer(ptr page.ValuePtr, admitOutput func(uint64) error) ([]byte, error) {
	if tx == nil || tx.nativeClosed {
		return nil, backenddb.ErrConditionalTxnClosed
	}
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	provider, _ := tx.db.dictStore.(cowDictionaryReadProvider)
	return tx.snap.cowCut.basis.nativeValueAtPointer(tx.db.cow, provider, ptr, admitOutput)
}
