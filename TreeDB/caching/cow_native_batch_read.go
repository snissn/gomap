package caching

import (
	"errors"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type cowNativeBatchValueSpan struct {
	off int
	n   int
}

type cowNativeBatchReadState struct {
	reader      backenddb.SnapshotRootReader
	cache       *cowCache
	w           *cowReadWorkspace
	independent cowCacheDictionaryRead
	hasDict     bool
	keys        [][]byte
	dst         []node.LeafEntry
	found       []bool
	copyValues  bool
	admitOutput func(uint64) error
	point       *cowPointScratch
	spans       []cowNativeBatchValueSpan
	inlineSpans [17]cowNativeBatchValueSpan
	slab        []byte
	curIdx      int

	readLeafFn   func(page.LeafLogPtr, []byte, backenddb.PinnedCOWLeafRead) ([]byte, tree.LeafLogPageReadState, error)
	checkShapeFn func(valuelog.COWRecordShape) bool
	consumeFn    func(node.LeafEntry) error
	runFn        func(read func(key, keyScratch, leafScratch []byte, consume func(node.LeafEntry) error) error) error
}

var cowNativeBatchReadStatePool = sync.Pool{
	New: func() any {
		s := new(cowNativeBatchReadState)
		s.readLeafFn = s.readLeaf
		s.checkShapeFn = s.checkShape
		s.consumeFn = s.consume
		s.runFn = s.run
		return s
	},
}

func (s *cowNativeBatchReadState) readLeaf(ptr page.LeafLogPtr, leafDst []byte, record backenddb.PinnedCOWLeafRead) ([]byte, tree.LeafLogPageReadState, error) {
	var independent *cowCacheDictionaryRead
	if s.hasDict {
		independent = &s.independent
	}
	return readInspectedPhysicalLeaf(s.cache, s.w, ptr, leafDst, true, true, independent, &record)
}

func (s *cowNativeBatchReadState) checkShape(shape valuelog.COWRecordShape) bool {
	if shape.DictID != 0 && (!s.hasDict || s.independent.provider == nil || s.independent.cache != s.cache) {
		return false
	}
	if shape.Compressed && shape.DictID == 0 && shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4 {
		return false
	}
	return true
}

func (s *cowNativeBatchReadState) consume(entry node.LeafEntry) error {
	// End every page/scratch alias before releasing the borrowed view.
	entry.Key = nil
	i := s.curIdx
	s.found[i] = true
	if s.copyValues && entry.Flags&(node.FlagPointer|node.FlagTombstone) == 0 {
		if len(entry.Value) == 0 {
			// Preserve non-nil empty slice parity with NativeEntryAtRoot.
			entry.Value = []byte{}
		} else if s.admitOutput != nil {
			n := len(entry.Value)
			if cap(s.slab)-len(s.slab) < n {
				rem := len(s.keys) - i
				est := n
				if rem > 1 && n <= 4096 {
					est = n * min(rem, 32)
				}
				if admitErr := s.admitOutput(memtable.COWAllocationCharge(uint64(est))); admitErr != nil {
					if est == n || !errors.Is(admitErr, memtable.ErrCOWCapacity) {
						return admitErr
					}
					est = n
					if admitErr = s.admitOutput(memtable.COWAllocationCharge(uint64(est))); admitErr != nil {
						return admitErr
					}
				}
				s.slab = make([]byte, 0, est)
			}
			off := len(s.slab)
			s.slab = append(s.slab, entry.Value...)
			entry.Value = s.slab[off : off+n : off+n]
		} else {
			if s.slab == nil {
				rem := len(s.keys) - i
				est := len(entry.Value)
				if rem > 1 && len(entry.Value) <= 4096 {
					est = len(entry.Value) * min(rem, 32)
				}
				s.slab = make([]byte, 0, est)
			}
			off := len(s.slab)
			n := len(entry.Value)
			s.slab = append(s.slab, entry.Value...)
			entry.Value = nil
			s.spans[i] = cowNativeBatchValueSpan{off: off, n: n}
		}
	} else {
		entry.Value = nil
	}
	s.dst[i] = entry
	return nil
}

func (s *cowNativeBatchReadState) run(read func(key, keyScratch, leafScratch []byte, consume func(node.LeafEntry) error) error) error {
	for i, key := range s.keys {
		s.curIdx = i
		s.dst[i] = node.LeafEntry{}
		s.found[i] = false
		s.point.leafGen++
		readErr := read(key, s.point.key[:], s.point.leaf[:], s.consumeFn)
		if errors.Is(readErr, tree.ErrKeyNotFound) {
			continue
		}
		if readErr != nil {
			return readErr
		}
	}
	return nil
}

func (s *cowNativeBatchReadState) reset() {
	s.reader = backenddb.SnapshotRootReader{}
	s.cache = nil
	s.w = nil
	s.independent = cowCacheDictionaryRead{}
	s.hasDict = false
	s.keys = nil
	s.dst = nil
	s.found = nil
	s.copyValues = false
	s.admitOutput = nil
	s.point = nil
	s.spans = nil
	s.slab = nil
	s.curIdx = 0
}

// NativeEntriesAtRootAdmitted performs a batched point lookup on a conditional
// transaction's opening physical root under one warm read lane and one snapshot
// read pin, admitting copied inline values through admitOutput before allocation.
func (tx *nativeConditionalTxn) NativeEntriesAtRootAdmitted(root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, admitOutput func(uint64) error) error {
	if tx == nil || tx.nativeClosed {
		return backenddb.ErrConditionalTxnClosed
	}
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if len(dst) != len(keys) || len(found) != len(keys) {
		return errors.New("caching: NativeEntriesAtRootAdmitted slice length mismatch")
	}
	if len(keys) == 0 {
		return nil
	}
	provider, _ := tx.db.dictStore.(cowDictionaryReadProvider)
	_, err := tx.snap.cowCut.basis.nativeEntriesAtRootWithAdmit(tx.db.cow, provider, root, keys, dst, found, true, nil, admitOutput)
	return err
}

// NativeEntriesAtRoot performs a batched point lookup against a settled
// physical root under a single warm read lane and single snapshot read pin.
// When copyValues is true, inline values are packed into one contiguous slab
// and materialized with exact capacity bounds after the pinned read completes.
func (c *NativeSemanticCapture) NativeEntriesAtRoot(root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, copyValues bool) error {
	if c == nil || c.cut == nil || c.cache == nil {
		return backenddb.ErrClosed
	}
	if len(dst) != len(keys) || len(found) != len(keys) {
		return errors.New("caching: NativeEntriesAtRoot slice length mismatch")
	}
	if len(keys) == 0 {
		return nil
	}
	_, err := c.cut.basis.nativeEntriesAtRootWithAdmit(c.cache, c.dictionary, root, keys, dst, found, copyValues, nil, nil)
	return err
}

// NativeEntriesAtRootInto performs a batched point lookup against a settled
// physical root under a single warm read lane and single snapshot read pin,
// packing copied inline values into slab[:0] (growing if needed) and returning
// the resulting slab so callers with borrowed-slice semantics can pool and reuse
// it across queries.
func (c *NativeSemanticCapture) NativeEntriesAtRootInto(root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, slab []byte) ([]byte, error) {
	if c == nil || c.cut == nil || c.cache == nil {
		return slab, backenddb.ErrClosed
	}
	if len(dst) != len(keys) || len(found) != len(keys) {
		return slab, errors.New("caching: NativeEntriesAtRootInto slice length mismatch")
	}
	if len(keys) == 0 {
		return slab, nil
	}
	return c.cut.basis.nativeEntriesAtRootWithAdmit(c.cache, c.dictionary, root, keys, dst, found, true, slab, nil)
}

// NativeEntriesAtRootInto performs a batched point lookup directly against a
// borrowed live cut's settled physical root, reusing slab[:0] for inline values.
func (c *cowReadCut) NativeEntriesAtRootInto(root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, slab []byte) ([]byte, error) {
	if c == nil || c.basis == nil || c.cache == nil {
		return slab, backenddb.ErrClosed
	}
	if len(dst) != len(keys) || len(found) != len(keys) {
		return slab, errors.New("caching: NativeEntriesAtRootInto slice length mismatch")
	}
	if len(keys) == 0 {
		return slab, nil
	}
	return c.basis.nativeEntriesAtRootWithAdmit(c.cache, c.dictionaryProvider(), root, keys, dst, found, true, slab, nil)
}

func (b *cowBackendBasis) nativeEntriesAtRootWithAdmit(cache *cowCache, provider cowDictionaryReadProvider, root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, copyValues bool, slab []byte, admitOutput func(uint64) error) ([]byte, error) {
	limit := cache.budget.Limits().MaxInFlightBytes
	limits := valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit}
	if mu, owner, ok := b.tryLockReadLane(cowReadStripe(keys[0])); ok {
		defer mu.Unlock()
		fresh := *owner == nil
		w, err := b.workspaceLocked(owner, cache.budget, limits)
		if err != nil {
			return slab, err
		}
		outSlab, err := b.readNativeEntriesWithWorkspaceAdmitted(cache, w, provider, root, keys, dst, found, copyValues, slab, admitOutput)
		if err != nil && fresh && *owner == w && w.point == nil && w.projectionLease == nil && len(w.codecs) == 0 && w.allocations == nil {
			*owner = nil
			w.close()
		}
		return outSlab, err
	}
	// All basis read lanes are busy: admit a bounded per-call temporary
	// workspace so concurrent readers never serialize.
	w, err := admitCOWReadWorkspace(cache.budget, limits)
	if err != nil {
		return slab, err
	}
	w.basis = b.snapshot
	defer w.close()
	return b.readNativeEntriesWithWorkspaceAdmitted(cache, w, provider, root, keys, dst, found, copyValues, slab, admitOutput)
}

func (b *cowBackendBasis) readNativeEntriesWithWorkspaceAdmitted(cache *cowCache, w *cowReadWorkspace, provider cowDictionaryReadProvider, root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, copyValues bool, reuseSlab []byte, admitOutput func(uint64) error) ([]byte, error) {
	point, err := w.pointScratch()
	if err != nil {
		return reuseSlab, err
	}
	s := cowNativeBatchReadStatePool.Get().(*cowNativeBatchReadState)
	s.reader, err = b.snapshot.NativeReaderAtRoot(root)
	if err != nil {
		s.reset()
		cowNativeBatchReadStatePool.Put(s)
		return reuseSlab, err
	}
	s.cache = cache
	s.w = w
	if provider != nil {
		s.independent = cowCacheDictionaryRead{provider: provider, cache: cache}
		s.hasDict = true
	}
	s.keys = keys
	s.dst = dst
	s.found = found
	s.copyValues = copyValues
	s.admitOutput = admitOutput
	s.point = point
	if copyValues && admitOutput == nil {
		if len(keys) <= len(s.inlineSpans) {
			s.spans = s.inlineSpans[:len(keys)]
			clear(s.spans)
		} else {
			s.spans = make([]cowNativeBatchValueSpan, len(keys))
		}
		if cap(reuseSlab) > 0 {
			s.slab = reuseSlab[:0]
		}
	}

	err = s.reader.WithPinnedCOWPointReadView(w.limits, s.checkShapeFn, s.readLeafFn, s.runFn)
	outSlab := s.slab
	if err != nil {
		clear(dst)
		clear(found)
		s.reset()
		cowNativeBatchReadStatePool.Put(s)
		return outSlab, err
	}
	if copyValues && admitOutput == nil && len(outSlab) > 0 {
		for i, sp := range s.spans {
			if sp.n > 0 {
				dst[i].Value = outSlab[sp.off : sp.off+sp.n : sp.off+sp.n]
			}
		}
	}
	s.reset()
	cowNativeBatchReadStatePool.Put(s)
	return outSlab, nil
}
