package db

import (
	"bytes"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type valueReader struct {
	vlogs                  tree.SlabReader
	outerLeafMode          string
	modeInit               bool
	outerLeafEnabled       bool
	fenceLookupEnabled     bool
	skipOuterLeafChecksums bool
	cache                  *outerLeafBlockCache
	keyCache               *outerLeafKeyCache
	fenceDecodeLeases      *outerLeafFenceDecodeLeaseSet
}

type staticFenceKeysLease struct {
	// keys may alias shared cache-backed slices; callers must treat them as
	// immutable views.
	keys  [][]byte
	inUse bool
}

var staticFenceKeysLeasePool = sync.Pool{
	New: func() any {
		return &staticFenceKeysLease{}
	},
}

func acquireStaticFenceKeysLease(keys [][]byte) *staticFenceKeysLease {
	lease := staticFenceKeysLeasePool.Get().(*staticFenceKeysLease)
	lease.keys = keys
	lease.inUse = true
	return lease
}

func (l *staticFenceKeysLease) Keys() [][]byte {
	if l == nil || !l.inUse {
		return nil
	}
	return l.keys
}

func (l *staticFenceKeysLease) Release() {
	if l == nil || !l.inUse {
		return
	}
	l.keys = nil
	l.inUse = false
	staticFenceKeysLeasePool.Put(l)
}

const outerLeafLookupScratchMaxRetain = 1 << 20
const outerLeafFenceDecodeScratchMaxRetain = 64 << 20
const outerLeafFenceDecodeLeaseSetMinSize = 8
const outerLeafFenceDecodeLeaseSetMaxSize = 64

var outerLeafLookupScratchPool sync.Pool
var outerLeafLookupScratchEntryPool = sync.Pool{
	New: func() any {
		return &outerLeafScratchEntry{}
	},
}
var outerLeafFenceDecodeScratchPool sync.Pool
var outerLeafFenceDecodeScratchEntryPool = sync.Pool{
	New: func() any {
		return &outerLeafScratchEntry{}
	},
}
var outerLeafFenceDecodedBlockPool sync.Pool
var outerLeafFenceDecodeSharedLeases *outerLeafFenceDecodeLeaseSet
var outerLeafFenceDecodeSharedLeasesOnce sync.Once
var outerLeafFenceDecodeContextPool = sync.Pool{
	New: func() any {
		return &outerLeafFenceDecodeContext{}
	},
}

type outerLeafScratchEntry struct {
	buf []byte
}

type outerLeafFenceDecodeContext struct {
	scratch []byte
	block   *outerleaf.DecodedBlock
}

type outerLeafFenceDecodeLeaseSet struct {
	pool sync.Pool
}

func outerLeafFenceDecodeSharedLeaseSet() *outerLeafFenceDecodeLeaseSet {
	if outerLeafFenceDecodeSharedLeases != nil {
		return outerLeafFenceDecodeSharedLeases
	}
	outerLeafFenceDecodeSharedLeasesOnce.Do(initOuterLeafFenceDecodeSharedLeases)
	return outerLeafFenceDecodeSharedLeases
}

func initOuterLeafFenceDecodeSharedLeases() {
	outerLeafFenceDecodeSharedLeases = newOuterLeafFenceDecodeLeaseSet(0)
}

func newOuterLeafFenceDecodeLeaseSet(size int) *outerLeafFenceDecodeLeaseSet {
	if size <= 0 {
		size = outerLeafFenceDecodeLeaseSetPreferredSize()
	}
	set := &outerLeafFenceDecodeLeaseSet{}
	set.pool.New = func() any {
		return acquireOuterLeafFenceDecodeContext()
	}
	for i := 0; i < size; i++ {
		set.pool.Put(acquireOuterLeafFenceDecodeContext())
	}
	return set
}

func outerLeafFenceDecodeLeaseSetPreferredSize() int {
	n := runtime.GOMAXPROCS(0)
	if n < outerLeafFenceDecodeLeaseSetMinSize {
		return outerLeafFenceDecodeLeaseSetMinSize
	}
	if n > outerLeafFenceDecodeLeaseSetMaxSize {
		return outerLeafFenceDecodeLeaseSetMaxSize
	}
	return n
}

func (s *outerLeafFenceDecodeLeaseSet) acquire() *outerLeafFenceDecodeContext {
	if s == nil {
		return nil
	}
	if v := s.pool.Get(); v != nil {
		if ctx, ok := v.(*outerLeafFenceDecodeContext); ok && ctx != nil {
			return ctx
		}
	}
	return acquireOuterLeafFenceDecodeContext()
}

func (s *outerLeafFenceDecodeLeaseSet) release(ctx *outerLeafFenceDecodeContext) {
	if s == nil || ctx == nil {
		return
	}
	s.pool.Put(ctx)
}

func (s *outerLeafFenceDecodeLeaseSet) close() {
	_ = s
}

func acquireOuterLeafFenceDecodeContext() *outerLeafFenceDecodeContext {
	return outerLeafFenceDecodeContextPool.Get().(*outerLeafFenceDecodeContext)
}

func releaseOuterLeafFenceDecodeContext(ctx *outerLeafFenceDecodeContext) {
	if ctx == nil {
		return
	}
	scratch := ctx.scratch
	block := ctx.block
	ctx.scratch = nil
	ctx.block = nil
	if block != nil {
		block.Release()
		outerLeafFenceDecodedBlockPut(block)
	}
	outerLeafFenceDecodeScratchPut(scratch)
	outerLeafFenceDecodeContextPool.Put(ctx)
}

func outerLeafLookupScratchGet() []byte {
	if v := outerLeafLookupScratchPool.Get(); v != nil {
		if entry, ok := v.(*outerLeafScratchEntry); ok && entry != nil {
			b := entry.buf
			entry.buf = nil
			outerLeafLookupScratchEntryPool.Put(entry)
			return b[:0]
		}
	}
	return nil
}

func outerLeafLookupScratchPut(b []byte) {
	if cap(b) == 0 || cap(b) > outerLeafLookupScratchMaxRetain {
		return
	}
	entry := outerLeafLookupScratchEntryPool.Get().(*outerLeafScratchEntry)
	entry.buf = b[:0]
	outerLeafLookupScratchPool.Put(entry)
}

func outerLeafFenceDecodeScratchGet() []byte {
	if v := outerLeafFenceDecodeScratchPool.Get(); v != nil {
		if entry, ok := v.(*outerLeafScratchEntry); ok && entry != nil {
			b := entry.buf
			entry.buf = nil
			outerLeafFenceDecodeScratchEntryPool.Put(entry)
			return b[:0]
		}
	}
	return nil
}

func outerLeafFenceDecodeScratchPut(b []byte) {
	if cap(b) == 0 || cap(b) > outerLeafFenceDecodeScratchMaxRetain {
		return
	}
	entry := outerLeafFenceDecodeScratchEntryPool.Get().(*outerLeafScratchEntry)
	entry.buf = b[:0]
	outerLeafFenceDecodeScratchPool.Put(entry)
}

func outerLeafFenceDecodedBlockGet() *outerleaf.DecodedBlock {
	if v := outerLeafFenceDecodedBlockPool.Get(); v != nil {
		if b, ok := v.(*outerleaf.DecodedBlock); ok && b != nil {
			return b
		}
	}
	return &outerleaf.DecodedBlock{}
}

func outerLeafFenceDecodedBlockPut(b *outerleaf.DecodedBlock) {
	if b == nil {
		return
	}
	outerLeafFenceDecodedBlockPool.Put(b)
}

type fenceBlockDecodeLease struct {
	block    *outerleaf.DecodedBlock
	scratch  []byte
	released bool
}

func (l *fenceBlockDecodeLease) Release() {
	if l == nil || l.released {
		return
	}
	l.released = true
	if l.block != nil {
		l.block.Release()
		outerLeafFenceDecodedBlockPut(l.block)
		l.block = nil
	}
	outerLeafFenceDecodeScratchPut(l.scratch)
	l.scratch = nil
}

type unsafeAppendReader interface {
	ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error)
}

type unsafeAppendBatchReader interface {
	ReadUnsafeAppendBatch(ptrs []page.ValuePtr, dst [][]byte) ([][]byte, error)
}

// ValueReaderForState returns a reader that resolves value-log pointers.
func ValueReaderForState(state *DBState) tree.SlabReader {
	if state == nil {
		return nil
	}
	return newValueReader(state.ValueLogSet, "", false, nil, nil)
}

func newValueReader(vlogs tree.SlabReader, mode string, skipOuterLeafChecksums bool, cache *outerLeafBlockCache, keyCache *outerLeafKeyCache) valueReader {
	r := valueReader{
		vlogs:                  vlogs,
		skipOuterLeafChecksums: skipOuterLeafChecksums,
		cache:                  cache,
		keyCache:               keyCache,
	}
	r.setOuterLeafMode(mode)
	if r.fenceLookupEnabled {
		r.fenceDecodeLeases = outerLeafFenceDecodeSharedLeaseSet()
	}
	return r
}

func (r *valueReader) releaseDecodeContext() {
	if r == nil || r.fenceDecodeLeases == nil {
		return
	}
	if r.fenceDecodeLeases != outerLeafFenceDecodeSharedLeases {
		r.fenceDecodeLeases.close()
	}
	r.fenceDecodeLeases = nil
}

func (r *valueReader) setOuterLeafMode(mode string) {
	mode = strings.TrimSpace(mode)
	r.outerLeafMode = mode
	r.outerLeafEnabled = outerleaf.ModeEnabled(mode)
	r.fenceLookupEnabled = mode == outerleaf.ModeV2FencePtr
	r.modeInit = true
}

func (r valueReader) outerLeafModeEnabled() bool {
	if r.modeInit {
		return r.outerLeafEnabled
	}
	return outerleaf.ModeEnabled(strings.TrimSpace(r.outerLeafMode))
}

func (r valueReader) fenceLookupModeEnabled() bool {
	if r.modeInit {
		return r.fenceLookupEnabled
	}
	return strings.TrimSpace(r.outerLeafMode) == outerleaf.ModeV2FencePtr
}

func (r valueReader) KeyAwareEnabled() bool {
	return r.outerLeafModeEnabled()
}

func (r valueReader) FenceLookupEnabled() bool {
	return r.fenceLookupModeEnabled()
}

func (r valueReader) FencePointerLikelyBlock(ptr page.ValuePtr) bool {
	return page.ValuePtrIsFenceOuter(ptr)
}

func (r valueReader) withoutOuterLeafCaches() valueReader {
	r.cache = nil
	r.keyCache = nil
	return r
}

func (r valueReader) decodeValue(ptr page.ValuePtr, raw []byte, unsafe bool) ([]byte, error) {
	if !r.outerLeafModeEnabled() {
		return raw, nil
	}
	if !outerleaf.HasMagic(raw) {
		// Fence mode may still surface direct value-log pointers for singleton
		// oversized entries.
		return raw, nil
	}
	if r.cache == nil {
		val, found, err := r.decodeOuterLeafValueForKeyNoCache(raw, nil, unsafe)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("value reader: outer-leaf first-entry lookup miss ptr=%+v", ptr)
		}
		return val, nil
	}
	block, err := r.outerLeafBlock(ptr, raw)
	if err != nil {
		return nil, err
	}
	val, err := block.FirstValue()
	if err == nil {
		return val, nil
	}
	if err != nil && err != outerleaf.ErrBlobRefEntry {
		return nil, err
	}
	entry, found, lookupErr := block.EntryForKey(nil)
	if lookupErr != nil {
		return nil, lookupErr
	}
	if !found {
		return nil, fmt.Errorf("value reader: outer-leaf first-entry lookup miss ptr=%+v", ptr)
	}
	return r.resolveLookupResult(entry, unsafe)
}

func (r valueReader) decodeValueForKey(ptr page.ValuePtr, key, raw []byte) ([]byte, error) {
	decoded, found, err := r.decodeValueForKeyFound(ptr, key, raw, false)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("value reader: outer-leaf key lookup miss ptr=%+v key=%x", ptr, key)
	}
	return decoded, nil
}

func (r valueReader) decodeValueForKeyUnsafe(ptr page.ValuePtr, key, raw []byte) ([]byte, error) {
	decoded, found, err := r.decodeValueForKeyFound(ptr, key, raw, true)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("value reader: outer-leaf key lookup miss ptr=%+v key=%x", ptr, key)
	}
	return decoded, nil
}

func (r valueReader) decodeValueForKeyFound(ptr page.ValuePtr, key, raw []byte, unsafe bool) ([]byte, bool, error) {
	if !r.outerLeafModeEnabled() {
		return raw, true, nil
	}
	if !outerleaf.HasMagic(raw) {
		// Fence mode may still surface direct value-log pointers for singleton
		// oversized entries.
		return raw, true, nil
	}
	if r.cache == nil {
		if leases := r.fenceDecodeLeases; leases != nil {
			if ctx := leases.acquire(); ctx != nil {
				val, found, err := r.decodeOuterLeafValueForKeyNoCacheWithLeaseCtx(raw, key, unsafe, ctx)
				leases.release(ctx)
				return val, found, err
			}
		}
		return r.decodeOuterLeafValueForKeyNoCache(raw, key, unsafe)
	}
	cacheKey := newOuterLeafBlockKey(ptr)
	if block := r.cache.get(cacheKey); block != nil {
		entry, found, err := block.EntryForKey(key)
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, nil
		}
		val, err := r.resolveLookupResult(entry, unsafe)
		if err != nil {
			return nil, false, err
		}
		return val, true, nil
	}
	if leases := r.fenceDecodeLeases; leases != nil {
		if ctx := leases.acquire(); ctx != nil {
			val, found, err := r.decodeValueForKeyFoundWithLeaseCtx(cacheKey, key, raw, unsafe, ctx)
			leases.release(ctx)
			return val, found, err
		}
	}
	verifyChecksums := !r.skipOuterLeafChecksums
	scratch := outerLeafFenceDecodeScratchGet()
	decodedBlock := outerLeafFenceDecodedBlockGet()
	block, nextScratch, err := outerleaf.DecodeBlockLeaseWithScratchAndVerify(raw, scratch, decodedBlock, verifyChecksums)
	if err != nil {
		outerLeafFenceDecodedBlockPut(decodedBlock)
		outerLeafFenceDecodeScratchPut(nextScratch)
		return nil, false, err
	}
	if block == nil {
		outerLeafFenceDecodedBlockPut(decodedBlock)
		outerLeafFenceDecodeScratchPut(nextScratch)
		return nil, false, nil
	}
	releaseAfterLookup := true
	if block.FirstKind() != outerleaf.EntryKindBlobRef {
		nextScratch = block.ReclaimTransferredScratchForRelease(nextScratch)
		r.cache.put(cacheKey, block)
		releaseAfterLookup = false
	}
	if releaseAfterLookup {
		defer func() {
			block.Release()
			outerLeafFenceDecodedBlockPut(decodedBlock)
			outerLeafFenceDecodeScratchPut(nextScratch)
		}()
	}
	var (
		entry outerleaf.LookupResult
		found bool
	)
	if releaseAfterLookup {
		entry, found, err = block.EntryForKeyNoRestartKeys(key)
	} else {
		entry, found, err = block.EntryForKey(key)
	}
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	val, err := r.resolveLookupResult(entry, unsafe)
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (r valueReader) decodeValueForKeyFoundWithLeaseCtx(cacheKey outerLeafBlockKey, key, raw []byte, unsafe bool, ctx *outerLeafFenceDecodeContext) ([]byte, bool, error) {
	verifyChecksums := !r.skipOuterLeafChecksums
	scratch := ctx.scratch
	decodedBlock := ctx.block
	if decodedBlock == nil {
		decodedBlock = outerLeafFenceDecodedBlockGet()
		ctx.block = decodedBlock
	}
	block, nextScratch, err := outerleaf.DecodeBlockLeaseWithScratchAndVerify(raw, scratch, decodedBlock, verifyChecksums)
	if err != nil {
		ctx.scratch = nextScratch
		return nil, false, err
	}
	if block == nil {
		ctx.scratch = nextScratch
		return nil, false, nil
	}
	if block.FirstKind() != outerleaf.EntryKindBlobRef {
		nextScratch = block.ReclaimTransferredScratchForRelease(nextScratch)
		ctx.scratch = nextScratch
		ctx.block = outerLeafFenceDecodedBlockGet()
		r.cache.put(cacheKey, block)
		entry, found, err := block.EntryForKey(key)
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, nil
		}
		val, err := r.resolveLookupResult(entry, unsafe)
		if err != nil {
			return nil, false, err
		}
		return val, true, nil
	}
	entry, found, err := block.EntryForKeyNoRestartKeys(key)
	block.Release()
	ctx.block = block
	ctx.scratch = nextScratch
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	val, err := r.resolveLookupResult(entry, unsafe)
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (r valueReader) decodeValueForKeyFoundAppend(ptr page.ValuePtr, key, raw, dst []byte) ([]byte, bool, error) {
	if !r.outerLeafModeEnabled() {
		return append(dst[:0], raw...), true, nil
	}
	if !outerleaf.HasMagic(raw) {
		return append(dst[:0], raw...), true, nil
	}
	if r.cache == nil {
		if leases := r.fenceDecodeLeases; leases != nil {
			if ctx := leases.acquire(); ctx != nil {
				val, found, err := r.decodeOuterLeafAppendForKeyNoCacheWithLeaseCtx(raw, key, dst, ctx)
				leases.release(ctx)
				return val, found, err
			}
		}
		return r.decodeOuterLeafAppendForKeyNoCache(raw, key, dst)
	}
	cacheKey := newOuterLeafBlockKey(ptr)
	if block := r.cache.get(cacheKey); block != nil {
		entry, found, err := block.EntryForKey(key)
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, nil
		}
		out, err := r.resolveLookupResultAppend(entry, dst)
		if err != nil {
			return nil, false, err
		}
		return out, true, nil
	}
	if leases := r.fenceDecodeLeases; leases != nil {
		if ctx := leases.acquire(); ctx != nil {
			val, found, err := r.decodeValueForKeyFoundAppendWithLeaseCtx(cacheKey, key, raw, dst, ctx)
			leases.release(ctx)
			return val, found, err
		}
	}
	verifyChecksums := !r.skipOuterLeafChecksums
	scratch := outerLeafFenceDecodeScratchGet()
	decodedBlock := outerLeafFenceDecodedBlockGet()
	block, nextScratch, err := outerleaf.DecodeBlockLeaseWithScratchAndVerify(raw, scratch, decodedBlock, verifyChecksums)
	if err != nil {
		outerLeafFenceDecodedBlockPut(decodedBlock)
		outerLeafFenceDecodeScratchPut(nextScratch)
		return nil, false, err
	}
	if block == nil {
		outerLeafFenceDecodedBlockPut(decodedBlock)
		outerLeafFenceDecodeScratchPut(nextScratch)
		return nil, false, nil
	}
	releaseAfterLookup := true
	if block.FirstKind() != outerleaf.EntryKindBlobRef {
		nextScratch = block.ReclaimTransferredScratchForRelease(nextScratch)
		r.cache.put(cacheKey, block)
		releaseAfterLookup = false
	}
	if releaseAfterLookup {
		defer func() {
			block.Release()
			outerLeafFenceDecodedBlockPut(decodedBlock)
			outerLeafFenceDecodeScratchPut(nextScratch)
		}()
	}
	var (
		entry outerleaf.LookupResult
		found bool
	)
	if releaseAfterLookup {
		entry, found, err = block.EntryForKeyNoRestartKeys(key)
	} else {
		entry, found, err = block.EntryForKey(key)
	}
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	out, err := r.resolveLookupResultAppend(entry, dst)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil
}

func (r valueReader) decodeValueForKeyFoundAppendWithLeaseCtx(cacheKey outerLeafBlockKey, key, raw, dst []byte, ctx *outerLeafFenceDecodeContext) ([]byte, bool, error) {
	verifyChecksums := !r.skipOuterLeafChecksums
	scratch := ctx.scratch
	decodedBlock := ctx.block
	if decodedBlock == nil {
		decodedBlock = outerLeafFenceDecodedBlockGet()
		ctx.block = decodedBlock
	}
	block, nextScratch, err := outerleaf.DecodeBlockLeaseWithScratchAndVerify(raw, scratch, decodedBlock, verifyChecksums)
	if err != nil {
		ctx.scratch = nextScratch
		return nil, false, err
	}
	if block == nil {
		ctx.scratch = nextScratch
		return nil, false, nil
	}
	if block.FirstKind() != outerleaf.EntryKindBlobRef {
		nextScratch = block.ReclaimTransferredScratchForRelease(nextScratch)
		ctx.scratch = nextScratch
		ctx.block = outerLeafFenceDecodedBlockGet()
		r.cache.put(cacheKey, block)
		entry, found, err := block.EntryForKey(key)
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, nil
		}
		out, err := r.resolveLookupResultAppend(entry, dst)
		if err != nil {
			return nil, false, err
		}
		return out, true, nil
	}
	entry, found, err := block.EntryForKeyNoRestartKeys(key)
	block.Release()
	ctx.block = block
	ctx.scratch = nextScratch
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	out, err := r.resolveLookupResultAppend(entry, dst)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil
}

func (r valueReader) decodeOuterLeafValueForKeyNoCacheWithLeaseCtx(raw, key []byte, unsafe bool, ctx *outerLeafFenceDecodeContext) ([]byte, bool, error) {
	verifyChecksums := !r.skipOuterLeafChecksums
	scratch := ctx.scratch
	entry, ok, found, outScratch, err := outerleaf.DecodeEntryForKeyWithVerify(raw, key, scratch, verifyChecksums)
	nextScratch := outScratch
	if cap(nextScratch) == 0 {
		nextScratch = scratch
	}
	ctx.scratch = nextScratch
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return raw, true, nil
	}
	if !found {
		return nil, false, nil
	}
	val, err := r.resolveLookupResult(entry, unsafe)
	if err != nil {
		return nil, false, err
	}
	if entry.Kind == outerleaf.EntryKindInline {
		val = append([]byte(nil), val...)
	}
	return val, true, nil
}

func (r valueReader) decodeOuterLeafAppendForKeyNoCacheWithLeaseCtx(raw, key, dst []byte, ctx *outerLeafFenceDecodeContext) ([]byte, bool, error) {
	verifyChecksums := !r.skipOuterLeafChecksums
	scratch := ctx.scratch
	entry, ok, found, outScratch, err := outerleaf.DecodeEntryForKeyWithVerify(raw, key, scratch, verifyChecksums)
	nextScratch := outScratch
	if cap(nextScratch) == 0 {
		nextScratch = scratch
	}
	ctx.scratch = nextScratch
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return raw, true, nil
	}
	if !found {
		return nil, false, nil
	}
	out, err := r.resolveLookupResultAppend(entry, dst)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil
}

func (r valueReader) decodeOuterLeafValueForKeyNoCache(raw, key []byte, unsafe bool) ([]byte, bool, error) {
	verifyChecksums := !r.skipOuterLeafChecksums
	var scratch []byte
	if !unsafe {
		scratch = outerLeafLookupScratchGet()
	}
	entry, ok, found, outScratch, err := outerleaf.DecodeEntryForKeyWithVerify(raw, key, scratch, verifyChecksums)
	if unsafe {
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return raw, true, nil
		}
		if !found {
			return nil, false, nil
		}
		val, err := r.resolveLookupResult(entry, true)
		if err != nil {
			return nil, false, err
		}
		return val, true, nil
	}

	recycle := outScratch
	if cap(recycle) == 0 {
		recycle = scratch
	}
	if err != nil {
		outerLeafLookupScratchPut(recycle)
		return nil, false, err
	}
	if !ok {
		outerLeafLookupScratchPut(recycle)
		return raw, true, nil
	}
	if !found {
		outerLeafLookupScratchPut(recycle)
		return nil, false, nil
	}
	val, err := r.resolveLookupResult(entry, unsafe)
	if err != nil {
		outerLeafLookupScratchPut(recycle)
		return nil, false, err
	}
	if entry.Kind == outerleaf.EntryKindInline {
		val = append([]byte(nil), val...)
	}
	outerLeafLookupScratchPut(recycle)
	return val, true, nil
}

func (r valueReader) resolveLookupResult(entry outerleaf.LookupResult, unsafe bool) ([]byte, error) {
	switch entry.Kind {
	case outerleaf.EntryKindInline:
		return entry.Value, nil
	case outerleaf.EntryKindBlobRef:
		if !page.IsValueLogFileID(entry.BlobPtr.FileID) {
			return nil, fmt.Errorf("value reader: invalid nested blob pointer file %d", entry.BlobPtr.FileID)
		}
		if unsafe {
			return r.readRawUnsafe(entry.BlobPtr)
		}
		return r.readRaw(entry.BlobPtr)
	default:
		return nil, fmt.Errorf("value reader: unsupported outer-leaf entry kind %d", entry.Kind)
	}
}

func (r valueReader) resolveLookupResultAppend(entry outerleaf.LookupResult, dst []byte) ([]byte, error) {
	switch entry.Kind {
	case outerleaf.EntryKindInline:
		if len(entry.Value) == 0 {
			return dst[:0], nil
		}
		var out []byte
		if cap(dst) >= len(entry.Value) {
			out = dst[:len(entry.Value)]
		} else {
			out = make([]byte, len(entry.Value))
		}
		copy(out, entry.Value)
		return out, nil
	case outerleaf.EntryKindBlobRef:
		if !page.IsValueLogFileID(entry.BlobPtr.FileID) {
			return nil, fmt.Errorf("value reader: invalid nested blob pointer file %d", entry.BlobPtr.FileID)
		}
		if app, ok := r.vlogs.(unsafeAppendReader); ok {
			return app.ReadUnsafeAppend(entry.BlobPtr, dst[:0])
		}
		val, err := r.readRawUnsafe(entry.BlobPtr)
		if err != nil {
			return nil, err
		}
		if len(val) == 0 {
			return dst[:0], nil
		}
		var out []byte
		if cap(dst) >= len(val) {
			out = dst[:len(val)]
		} else {
			out = make([]byte, len(val))
		}
		copy(out, val)
		return out, nil
	default:
		return nil, fmt.Errorf("value reader: unsupported outer-leaf entry kind %d", entry.Kind)
	}
}

func (r valueReader) decodeOuterLeafAppendForKeyNoCache(raw, key, dst []byte) ([]byte, bool, error) {
	if !outerleaf.HasMagic(raw) {
		return raw, true, nil
	}

	verifyChecksums := !r.skipOuterLeafChecksums
	scratch := outerLeafLookupScratchGet()
	entry, ok, found, outScratch, err := outerleaf.DecodeEntryForKeyWithVerify(raw, key, scratch, verifyChecksums)
	recycle := outScratch
	if cap(recycle) == 0 {
		recycle = scratch
	}
	if err != nil {
		outerLeafLookupScratchPut(recycle)
		return nil, false, err
	}
	if !ok {
		outerLeafLookupScratchPut(recycle)
		return raw, true, nil
	}
	if !found {
		outerLeafLookupScratchPut(recycle)
		return nil, false, nil
	}
	out, err := r.resolveLookupResultAppend(entry, dst)
	if err != nil {
		outerLeafLookupScratchPut(recycle)
		return nil, false, err
	}
	outerLeafLookupScratchPut(recycle)
	return out, true, nil
}

func (r valueReader) ReadUnsafeFenceForKey(ptr page.ValuePtr, key []byte) ([]byte, bool, error) {
	if !r.fenceLookupModeEnabled() {
		return nil, false, nil
	}
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		entry, found, err := block.EntryForKey(key)
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, nil
		}
		val, err := r.resolveLookupResult(entry, true)
		if err != nil {
			return nil, false, err
		}
		return val, true, nil
	}
	raw, err := r.readRawUnsafe(ptr)
	if err != nil {
		return nil, false, err
	}
	if !outerleaf.HasMagic(raw) {
		return nil, false, nil
	}
	return r.decodeValueForKeyFound(ptr, key, raw, true)
}

func (r valueReader) ReadUnsafeFenceAppendForKey(ptr page.ValuePtr, key []byte, dst []byte) ([]byte, bool, error) {
	if !r.fenceLookupModeEnabled() {
		return dst, false, nil
	}
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		entry, found, err := block.EntryForKey(key)
		if err != nil {
			return nil, false, err
		}
		if !found {
			return dst, false, nil
		}
		out, err := r.resolveLookupResultAppend(entry, dst)
		if err != nil {
			return nil, false, err
		}
		return out, true, nil
	}
	raw, err := r.readRawUnsafe(ptr)
	if err != nil {
		return nil, false, err
	}
	if !outerleaf.HasMagic(raw) {
		return dst, false, nil
	}
	return r.decodeValueForKeyFoundAppend(ptr, key, raw, dst)
}

func (r valueReader) ReadUnsafeFenceBlock(ptr page.ValuePtr) ([]tree.FenceBlockEntry, bool, error) {
	return r.ReadUnsafeFenceBlockInto(ptr, nil)
}

func (r valueReader) ReadUnsafeFenceBlockLeaseInto(ptr page.ValuePtr, dst []tree.FenceBlockEntry) ([]tree.FenceBlockEntry, tree.FenceBlockLease, bool, error) {
	if !r.fenceLookupModeEnabled() {
		return nil, nil, false, nil
	}
	block := r.cachedOuterLeafBlock(ptr)
	var lease tree.FenceBlockLease
	if block == nil {
		raw, err := r.readRawUnsafe(ptr)
		if err != nil {
			return nil, nil, true, err
		}
		if !outerleaf.HasMagic(raw) {
			return nil, nil, false, nil
		}
		scratch := outerLeafFenceDecodeScratchGet()
		decodedBlock := outerLeafFenceDecodedBlockGet()
		decoded, nextScratch, decErr := outerleaf.DecodeBlockLeaseWithScratchAndVerify(raw, scratch, decodedBlock, !r.skipOuterLeafChecksums)
		if decErr != nil {
			outerLeafFenceDecodedBlockPut(decodedBlock)
			outerLeafFenceDecodeScratchPut(nextScratch)
			return nil, nil, true, decErr
		}
		block = decoded
		lease = &fenceBlockDecodeLease{
			block:   decoded,
			scratch: nextScratch,
		}
	}
	if cap(dst) < block.EntryCount() {
		dst = make([]tree.FenceBlockEntry, 0, block.EntryCount())
	} else {
		dst = dst[:0]
	}
	err := block.VisitTypedEntries(func(key []byte, kind outerleaf.EntryKind, value []byte, blobPtr page.ValuePtr) error {
		val, err := r.resolveLookupResult(outerleaf.LookupResult{
			Kind:    kind,
			Value:   value,
			BlobPtr: blobPtr,
		}, true)
		if err != nil {
			return err
		}
		dst = append(dst, tree.FenceBlockEntry{Key: key, Value: val})
		return nil
	})
	if err != nil {
		if lease != nil {
			lease.Release()
		}
		return nil, nil, true, err
	}
	return dst, lease, true, nil
}

func (r valueReader) ReadUnsafeFenceBlockInto(ptr page.ValuePtr, dst []tree.FenceBlockEntry) ([]tree.FenceBlockEntry, bool, error) {
	if !r.fenceLookupModeEnabled() {
		return nil, false, nil
	}
	block := r.cachedOuterLeafBlock(ptr)
	if block == nil {
		raw, err := r.readRawUnsafe(ptr)
		if err != nil {
			return nil, true, err
		}
		if !outerleaf.HasMagic(raw) {
			return nil, false, nil
		}
		decoded, decErr := r.outerLeafBlock(ptr, raw)
		if decErr != nil {
			return nil, true, decErr
		}
		block = decoded
	}
	if cap(dst) < block.EntryCount() {
		dst = make([]tree.FenceBlockEntry, 0, block.EntryCount())
	} else {
		dst = dst[:0]
	}
	err := block.VisitTypedEntries(func(key []byte, kind outerleaf.EntryKind, value []byte, blobPtr page.ValuePtr) error {
		val, err := r.resolveLookupResult(outerleaf.LookupResult{
			Kind:    kind,
			Value:   value,
			BlobPtr: blobPtr,
		}, true)
		if err != nil {
			return err
		}
		dst = append(dst, tree.FenceBlockEntry{Key: key, Value: val})
		return nil
	})
	if err != nil {
		return nil, true, err
	}
	return dst, true, nil
}

func (r valueReader) ReadUnsafeFenceBlockKeys(ptr page.ValuePtr) ([][]byte, bool, error) {
	if !r.fenceLookupModeEnabled() {
		return nil, false, nil
	}
	if keys := r.cachedOuterLeafKeys(ptr); keys != nil {
		return keys, true, nil
	}
	block := r.cachedOuterLeafBlock(ptr)
	if block != nil {
		keys, err := block.Keys(nil)
		if err != nil {
			return nil, true, err
		}
		r.cacheOuterLeafKeys(ptr, keys)
		return keys, true, nil
	}
	raw, err := r.readRawUnsafe(ptr)
	if err != nil {
		return nil, true, err
	}
	if !outerleaf.HasMagic(raw) {
		return nil, false, nil
	}
	keys, decErr := outerleaf.DecodeKeysWithVerify(raw, !r.skipOuterLeafChecksums)
	if decErr != nil {
		return nil, true, decErr
	}
	r.cacheOuterLeafKeys(ptr, keys)
	return keys, true, nil
}

func (r valueReader) ReadUnsafeFenceBlockKeysRangeLease(ptr page.ValuePtr, lower []byte, upper []byte) (tree.FenceKeysLease, bool, error) {
	if !r.fenceLookupModeEnabled() {
		return nil, false, nil
	}
	if len(lower) > 0 && len(upper) > 0 && bytes.Compare(lower, upper) >= 0 {
		return nil, true, nil
	}
	if cachedKeys := r.cachedOuterLeafKeys(ptr); cachedKeys != nil {
		return acquireStaticFenceKeysLease(sliceFenceKeysRange(cachedKeys, lower, upper)), true, nil
	}
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		keys, err := block.KeysRange(nil, lower, upper)
		if err != nil {
			return nil, true, err
		}
		return acquireStaticFenceKeysLease(keys), true, nil
	}
	raw, err := r.readRawUnsafe(ptr)
	if err != nil {
		return nil, true, err
	}
	if !outerleaf.HasMagic(raw) {
		return nil, false, nil
	}
	lease, decErr := outerleaf.DecodeKeysRangeLeaseWithVerify(raw, lower, upper, !r.skipOuterLeafChecksums)
	if decErr != nil {
		return nil, true, decErr
	}
	if lease == nil {
		return nil, true, nil
	}
	return lease, true, nil
}

func (r valueReader) ReadUnsafeFenceBlockKeysRange(ptr page.ValuePtr, lower []byte, upper []byte) ([][]byte, bool, error) {
	if !r.fenceLookupModeEnabled() {
		return nil, false, nil
	}
	if len(lower) == 0 && len(upper) == 0 {
		return r.ReadUnsafeFenceBlockKeys(ptr)
	}
	if len(lower) > 0 && len(upper) > 0 && bytes.Compare(lower, upper) >= 0 {
		return nil, true, nil
	}
	if cachedKeys := r.cachedOuterLeafKeys(ptr); cachedKeys != nil {
		return sliceFenceKeysRange(cachedKeys, lower, upper), true, nil
	}
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		keys, err := block.KeysRange(nil, lower, upper)
		if err != nil {
			return nil, true, err
		}
		return keys, true, nil
	}
	raw, err := r.readRawUnsafe(ptr)
	if err != nil {
		return nil, true, err
	}
	if !outerleaf.HasMagic(raw) {
		return nil, false, nil
	}
	keys, decErr := outerleaf.DecodeKeysRangeWithVerify(raw, lower, upper, !r.skipOuterLeafChecksums)
	if decErr != nil {
		return nil, true, decErr
	}
	return keys, true, nil
}

func (r valueReader) ReadUnsafeFenceBlockSeek(ptr page.ValuePtr, key []byte) (pos int, below bool, above bool, keys [][]byte, ok bool, err error) {
	if !r.fenceLookupModeEnabled() {
		return 0, false, false, nil, false, nil
	}
	if cachedKeys := r.cachedOuterLeafKeys(ptr); cachedKeys != nil {
		lower, isBelow, isAbove := classifyFenceKeys(cachedKeys, key)
		if isBelow || isAbove {
			return lower, isBelow, isAbove, nil, true, nil
		}
		return lower, false, false, cachedKeys, true, nil
	}
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		lower, isBelow, isAbove, lowerErr := block.LowerBound(key)
		if lowerErr != nil {
			return 0, false, false, nil, true, lowerErr
		}
		if isBelow || isAbove {
			return lower, isBelow, isAbove, nil, true, nil
		}
		blockKeys, keyErr := block.Keys(nil)
		if keyErr != nil {
			return 0, false, false, nil, true, keyErr
		}
		r.cacheOuterLeafKeys(ptr, blockKeys)
		return lower, false, false, blockKeys, true, nil
	}

	raw, readErr := r.readRawUnsafe(ptr)
	if readErr != nil {
		return 0, false, false, nil, true, readErr
	}
	if !outerleaf.HasMagic(raw) {
		return 0, false, false, nil, false, nil
	}
	lower, isBelow, isAbove, blockKeys, decErr := outerleaf.DecodeLowerBoundAndKeysOnMatchWithVerify(raw, key, !r.skipOuterLeafChecksums)
	if decErr != nil {
		return 0, false, false, nil, true, decErr
	}
	if isBelow || isAbove {
		return lower, isBelow, isAbove, nil, true, nil
	}
	r.cacheOuterLeafKeys(ptr, blockKeys)
	return lower, false, false, blockKeys, true, nil
}

func (r valueReader) ReadUnsafeFenceBlockSeekLease(ptr page.ValuePtr, key []byte) (pos int, below bool, above bool, lease tree.FenceKeysLease, ok bool, err error) {
	if !r.fenceLookupModeEnabled() {
		return 0, false, false, nil, false, nil
	}
	if cachedKeys := r.cachedOuterLeafKeys(ptr); cachedKeys != nil {
		lower, isBelow, isAbove := classifyFenceKeys(cachedKeys, key)
		if isBelow || isAbove {
			return lower, isBelow, isAbove, nil, true, nil
		}
		return lower, false, false, acquireStaticFenceKeysLease(cachedKeys), true, nil
	}
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		lower, isBelow, isAbove, lowerErr := block.LowerBound(key)
		if lowerErr != nil {
			return 0, false, false, nil, true, lowerErr
		}
		if isBelow || isAbove {
			return lower, isBelow, isAbove, nil, true, nil
		}
		blockKeys, keyErr := block.Keys(nil)
		if keyErr != nil {
			return 0, false, false, nil, true, keyErr
		}
		r.cacheOuterLeafKeys(ptr, blockKeys)
		return lower, false, false, acquireStaticFenceKeysLease(blockKeys), true, nil
	}

	raw, readErr := r.readRawUnsafe(ptr)
	if readErr != nil {
		return 0, false, false, nil, true, readErr
	}
	if !outerleaf.HasMagic(raw) {
		return 0, false, false, nil, false, nil
	}
	lower, isBelow, isAbove, keyLease, decErr := outerleaf.DecodeLowerBoundAndKeysOnMatchLeaseWithVerify(raw, key, !r.skipOuterLeafChecksums)
	if decErr != nil {
		return 0, false, false, nil, true, decErr
	}
	if isBelow || isAbove {
		if keyLease != nil {
			keyLease.Release()
		}
		return lower, isBelow, isAbove, nil, true, nil
	}
	if keyLease != nil {
		r.cacheOuterLeafKeys(ptr, cloneFenceKeys(keyLease.Keys()))
	}
	return lower, false, false, keyLease, true, nil
}

func classifyFenceKeys(keys [][]byte, key []byte) (pos int, below bool, above bool) {
	if len(keys) == 0 {
		return 0, false, true
	}
	if len(key) == 0 {
		return 0, false, false
	}
	pos = sort.Search(len(keys), func(i int) bool {
		return bytes.Compare(keys[i], key) >= 0
	})
	if pos == 0 && bytes.Compare(key, keys[0]) < 0 {
		return 0, true, false
	}
	if pos >= len(keys) {
		return len(keys), false, true
	}
	return pos, false, false
}

func sliceFenceKeysRange(keys [][]byte, lower []byte, upper []byte) [][]byte {
	if len(keys) == 0 {
		return nil
	}
	start := 0
	if len(lower) > 0 {
		start = sort.Search(len(keys), func(i int) bool {
			return bytes.Compare(keys[i], lower) >= 0
		})
	}
	if start >= len(keys) {
		return nil
	}
	end := len(keys)
	if len(upper) > 0 {
		end = sort.Search(len(keys), func(i int) bool {
			return bytes.Compare(keys[i], upper) >= 0
		})
	}
	if end <= start {
		return nil
	}
	return keys[start:end]
}

func cloneFenceKeys(keys [][]byte) [][]byte {
	if len(keys) == 0 {
		return nil
	}
	cloned := make([][]byte, len(keys))
	for i := range keys {
		if len(keys[i]) == 0 {
			cloned[i] = []byte{}
			continue
		}
		k := make([]byte, len(keys[i]))
		copy(k, keys[i])
		cloned[i] = k
	}
	return cloned
}

func (r valueReader) readRaw(ptr page.ValuePtr) ([]byte, error) {
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil, fmt.Errorf("expected value log pointer, got file %d", ptr.FileID)
	}
	if r.vlogs == nil {
		return nil, fmt.Errorf("value log reader unavailable for file %d", ptr.FileID)
	}
	return r.vlogs.Read(ptr)
}

func (r valueReader) readRawUnsafe(ptr page.ValuePtr) ([]byte, error) {
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil, fmt.Errorf("expected value log pointer, got file %d", ptr.FileID)
	}
	if r.vlogs == nil {
		return nil, fmt.Errorf("value log reader unavailable for file %d", ptr.FileID)
	}
	return r.vlogs.ReadUnsafe(ptr)
}

func (r valueReader) Read(ptr page.ValuePtr) ([]byte, error) {
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		entry, found, err := block.EntryForKey(nil)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("value reader: outer-leaf first-entry lookup miss ptr=%+v", ptr)
		}
		return r.resolveLookupResult(entry, false)
	}
	raw, err := r.readRaw(ptr)
	if err != nil {
		return nil, err
	}
	return r.decodeValue(ptr, raw, false)
}

func (r valueReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		entry, found, err := block.EntryForKey(nil)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("value reader: outer-leaf first-entry lookup miss ptr=%+v", ptr)
		}
		return r.resolveLookupResult(entry, true)
	}
	raw, err := r.readRawUnsafe(ptr)
	if err != nil {
		return nil, err
	}
	return r.decodeValue(ptr, raw, true)
}

func (r valueReader) ReadForKey(ptr page.ValuePtr, key []byte) ([]byte, error) {
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		entry, found, err := block.EntryForKey(key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("value reader: outer-leaf key lookup miss ptr=%+v key=%x", ptr, key)
		}
		return r.resolveLookupResult(entry, false)
	}
	raw, err := r.readRaw(ptr)
	if err != nil {
		return nil, err
	}
	return r.decodeValueForKey(ptr, key, raw)
}

func (r valueReader) ReadUnsafeForKey(ptr page.ValuePtr, key []byte) ([]byte, error) {
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		entry, found, err := block.EntryForKey(key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("value reader: outer-leaf key lookup miss ptr=%+v key=%x", ptr, key)
		}
		return r.resolveLookupResult(entry, true)
	}
	raw, err := r.readRawUnsafe(ptr)
	if err != nil {
		return nil, err
	}
	return r.decodeValueForKeyUnsafe(ptr, key, raw)
}

func (r valueReader) ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	return r.ReadUnsafeAppendForKey(ptr, nil, dst)
}

func (r valueReader) ReadUnsafeAppendForKey(ptr page.ValuePtr, key []byte, dst []byte) ([]byte, error) {
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil, fmt.Errorf("expected value log pointer, got file %d", ptr.FileID)
	}
	if r.vlogs == nil {
		return nil, fmt.Errorf("value log reader unavailable for file %d", ptr.FileID)
	}
	if !r.outerLeafModeEnabled() {
		if app, ok := r.vlogs.(unsafeAppendReader); ok {
			return app.ReadUnsafeAppend(ptr, dst[:0])
		}
		val, err := r.vlogs.ReadUnsafe(ptr)
		if err != nil {
			return nil, err
		}
		return append(dst[:0], val...), nil
	}
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		entry, found, err := block.EntryForKey(key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("value reader: outer-leaf key lookup miss ptr=%+v key=%x", ptr, key)
		}
		return r.resolveLookupResultAppend(entry, dst)
	}
	if app, ok := r.vlogs.(unsafeAppendReader); ok {
		raw, err := app.ReadUnsafeAppend(ptr, dst[:0])
		if err != nil {
			return nil, err
		}
		out, found, err := r.decodeOuterLeafAppendForKeyNoCache(raw, key, dst)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("value reader: outer-leaf key lookup miss ptr=%+v key=%x", ptr, key)
		}
		return out, nil
	}
	val, err := r.vlogs.ReadUnsafe(ptr)
	if err != nil {
		return nil, err
	}
	decoded, err := r.decodeValueForKeyUnsafe(ptr, key, val)
	if err != nil {
		return nil, err
	}
	dst = append(dst[:0], decoded...)
	return dst, nil
}

func (r valueReader) ReadUnsafeAppendBatch(ptrs []page.ValuePtr, dst [][]byte) ([][]byte, error) {
	if len(ptrs) == 0 {
		return dst[:0], nil
	}
	if !r.outerLeafModeEnabled() {
		if app, ok := r.vlogs.(unsafeAppendBatchReader); ok {
			return app.ReadUnsafeAppendBatch(ptrs, dst)
		}
	}
	if app, ok := r.vlogs.(unsafeAppendBatchReader); ok {
		out, err := app.ReadUnsafeAppendBatch(ptrs, dst)
		if err != nil {
			return nil, err
		}
		decoder := r.withoutOuterLeafCaches()
		for i := range out {
			target := []byte(nil)
			if i < len(dst) {
				target = dst[i][:0]
			}
			decoded, found, decErr := decoder.decodeOuterLeafAppendForKeyNoCache(out[i], nil, target)
			if decErr != nil {
				return nil, decErr
			}
			if !found {
				return nil, fmt.Errorf("value reader: outer-leaf key lookup miss ptr=%+v", ptrs[i])
			}
			if i < len(dst) {
				dst[i] = decoded
				out[i] = dst[i]
				continue
			}
			out[i] = decoded
		}
		return out, nil
	}
	if cap(dst) < len(ptrs) {
		dst = make([][]byte, len(ptrs))
	} else {
		dst = dst[:len(ptrs)]
	}
	for i, ptr := range ptrs {
		var err error
		dst[i], err = r.ReadUnsafeAppend(ptr, dst[i][:0])
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func (r valueReader) ReadUnsafeAppendBatchForKeys(ptrs []page.ValuePtr, keys [][]byte, dst [][]byte) ([][]byte, error) {
	if len(ptrs) != len(keys) {
		return nil, fmt.Errorf("value reader: ptr/key batch mismatch %d/%d", len(ptrs), len(keys))
	}
	if len(ptrs) == 0 {
		return dst[:0], nil
	}
	if app, ok := r.vlogs.(unsafeAppendBatchReader); ok {
		out, err := app.ReadUnsafeAppendBatch(ptrs, dst)
		if err != nil {
			return nil, err
		}
		decoder := r.withoutOuterLeafCaches()
		for i := range out {
			target := []byte(nil)
			if i < len(dst) {
				target = dst[i][:0]
			}
			decoded, found, decErr := decoder.decodeOuterLeafAppendForKeyNoCache(out[i], keys[i], target)
			if decErr != nil {
				return nil, decErr
			}
			if !found {
				return nil, fmt.Errorf("value reader: outer-leaf key lookup miss ptr=%+v key=%x", ptrs[i], keys[i])
			}
			if i < len(dst) {
				dst[i] = decoded
				out[i] = dst[i]
				continue
			}
			out[i] = decoded
		}
		return out, nil
	}
	if cap(dst) < len(ptrs) {
		dst = make([][]byte, len(ptrs))
	} else {
		dst = dst[:len(ptrs)]
	}
	for i := range ptrs {
		var err error
		dst[i], err = r.ReadUnsafeAppendForKey(ptrs[i], keys[i], dst[i][:0])
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func (r valueReader) outerLeafBlock(ptr page.ValuePtr, raw []byte) (*outerleaf.DecodedBlock, error) {
	verifyChecksums := !r.skipOuterLeafChecksums
	if r.cache == nil {
		return outerleaf.DecodeBlockWithVerify(raw, nil, verifyChecksums)
	}
	key := newOuterLeafBlockKey(ptr)
	if block := r.cache.get(key); block != nil {
		return block, nil
	}
	block, err := outerleaf.DecodeBlockWithVerify(raw, nil, verifyChecksums)
	if err != nil {
		return nil, err
	}
	// Blob-ref-dominant fence blocks (large-value mode) exhibit low temporal
	// locality and high cache churn in prefix scans; skipping cache admission for
	// those blocks avoids lock/churn overhead while retaining cache benefits for
	// inline-heavy blocks.
	if block.FirstKind() != outerleaf.EntryKindBlobRef {
		r.cache.put(key, block)
	}
	return block, nil
}

func (r valueReader) cachedOuterLeafBlock(ptr page.ValuePtr) *outerleaf.DecodedBlock {
	if !r.outerLeafModeEnabled() || r.cache == nil {
		return nil
	}
	return r.cache.get(newOuterLeafBlockKey(ptr))
}

func (r valueReader) cachedOuterLeafKeys(ptr page.ValuePtr) [][]byte {
	if !r.fenceLookupModeEnabled() || r.keyCache == nil {
		return nil
	}
	return r.keyCache.get(newOuterLeafBlockKey(ptr))
}

func (r valueReader) cacheOuterLeafKeys(ptr page.ValuePtr, keys [][]byte) {
	if !r.fenceLookupModeEnabled() || r.keyCache == nil || len(keys) == 0 {
		return
	}
	r.keyCache.put(newOuterLeafBlockKey(ptr), keys)
}
