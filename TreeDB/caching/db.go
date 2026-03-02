package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/template"
	"github.com/snissn/gomap/TreeDB/tree"
)

var errDBClosing = errors.New("cachingdb: db closing")

var ErrKeyEmpty = fmt.Errorf("key cannot be empty")
var ErrValueNil = fmt.Errorf("value cannot be nil")
var ErrBatchClosed = fmt.Errorf("batch has been written or closed")
var ErrUnsafeOptions = fmt.Errorf("unsafe options require AllowUnsafe")
var ErrMemtableFull = fmt.Errorf("memtable full")
var errWALClosed = errors.New("cachingdb: wal writer closed")
var errWALUnavailable = errors.New("cachingdb: wal unavailable")

var iteratorDebugEnabled atomic.Bool

var valueLogEligiblePool sync.Pool                          // stores []int
var valueLogRecordPool sync.Pool                            // stores []valuelog.Record
var valueLogKeyPool sync.Pool                               // stores [][]byte
var valueLogPtrPool sync.Pool                               // stores []page.ValuePtr
var outerLeafEntryPool sync.Pool                            // stores []outerleaf.Entry
var batchArenaPools [batchArenaClassCount]sync.Pool         // stores []byte
var batchArenaLeasePool sync.Pool                           // stores *batchArenaLease
var batchEntrySliceRefPool sync.Pool                        // stores *batchEntrySliceRef
var outerLeafArenaPools [outerLeafArenaClassCount]sync.Pool // stores []byte
var outerLeafArenaLeaseMu sync.Mutex
var outerLeafArenaLeases [outerLeafArenaClassCount][][]byte
var outerLeafBlobRefScratchPool sync.Pool
var outerLeafEncoderPool sync.Pool // stores *outerleaf.Encoder
var valueLogPreparedBodyPool sync.Pool
var valueLogPreparedFramesPool sync.Pool     // stores []preparedDictFrame
var valueLogDictPrepareResultsPool sync.Pool // stores chan vlogDictPrepareResult
var valueLogKeyLeaseMu sync.Mutex
var valueLogKeyLeases [][][]byte

const (
	maxValueLogKeyLeaseCount = 64
	maxValueLogKeyLeaseCap   = 1 << 20
	batchArenaMinShift       = 12
	batchArenaMaxShift       = 22
	batchArenaClassCount     = batchArenaMaxShift - batchArenaMinShift + 1
	outerLeafArenaMinShift   = 12
	outerLeafArenaMaxShift   = 24
	outerLeafArenaClassCount = outerLeafArenaMaxShift - outerLeafArenaMinShift + 1
	maxOuterLeafArenaLeases  = 64
)

type vlogPreparedFrameBody struct {
	buf []byte
}

type batchEntrySliceRef struct {
	entries []batch.Entry
}

func getBatchEntrySliceRef(entries []batch.Entry) *batchEntrySliceRef {
	if v := batchEntrySliceRefPool.Get(); v != nil {
		if ref, ok := v.(*batchEntrySliceRef); ok {
			ref.entries = entries
			return ref
		}
	}
	return &batchEntrySliceRef{entries: entries}
}

func putBatchEntrySliceRef(ref *batchEntrySliceRef) {
	if ref == nil {
		return
	}
	ref.entries = nil
	batchEntrySliceRefPool.Put(ref)
}

func getValueLogEligible(capacity int) []int {
	if capacity < 0 {
		capacity = 0
	}
	if v := valueLogEligiblePool.Get(); v != nil {
		if s, ok := v.([]int); ok {
			if cap(s) >= capacity {
				return s[:0]
			}
		}
	}
	return make([]int, 0, capacity)
}

func putValueLogEligible(s []int) {
	if s == nil {
		return
	}
	// Avoid retaining huge slices in the pool.
	if cap(s) > 1<<20 {
		return
	}
	valueLogEligiblePool.Put(s[:0])
}

func getValueLogRecords(n int) []valuelog.Record {
	if n < 0 {
		n = 0
	}
	if v := valueLogRecordPool.Get(); v != nil {
		if s, ok := v.([]valuelog.Record); ok {
			if cap(s) >= n {
				return s[:n]
			}
		}
	}
	return make([]valuelog.Record, n)
}

func getValueLogRecordsCap(capacity int) []valuelog.Record {
	if capacity < 0 {
		capacity = 0
	}
	if v := valueLogRecordPool.Get(); v != nil {
		if s, ok := v.([]valuelog.Record); ok {
			if cap(s) >= capacity {
				return s[:0]
			}
		}
	}
	return make([]valuelog.Record, 0, capacity)
}

func putValueLogRecords(s []valuelog.Record) {
	if s == nil {
		return
	}
	for i := range s {
		s[i] = valuelog.Record{}
	}
	// Avoid retaining huge slices in the pool.
	if cap(s) > 1<<20 {
		return
	}
	valueLogRecordPool.Put(s[:0])
}

func clearValueLogRecordValues(s []valuelog.Record) {
	for i := range s {
		// Drop value references before pooling to avoid retaining large backing
		// arrays when callers provide subslices/views.
		s[i].Value = nil
	}
}

func putValueLogRecordsNoClear(s []valuelog.Record) {
	if s == nil {
		return
	}
	// Avoid O(cap) clearing work for oversized slices that we intentionally
	// drop instead of returning to the pool.
	if cap(s) > 1<<20 {
		return
	}
	records := s
	if cap(records) > len(records) {
		records = records[:cap(records)]
	}
	clearValueLogRecordValues(records)
	valueLogRecordPool.Put(s[:0])
}

func getOuterLeafEntriesCap(capacity int) []outerleaf.Entry {
	if capacity < 0 {
		capacity = 0
	}
	if v := outerLeafEntryPool.Get(); v != nil {
		if s, ok := v.([]outerleaf.Entry); ok && cap(s) >= capacity {
			return s[:0]
		}
	}
	return make([]outerleaf.Entry, 0, capacity)
}

func putOuterLeafEntries(s []outerleaf.Entry) {
	if s == nil {
		return
	}
	if cap(s) > 1<<15 {
		return
	}
	entries := s
	if cap(entries) > len(entries) {
		entries = entries[:cap(entries)]
	}
	for i := range entries {
		entries[i] = outerleaf.Entry{}
	}
	outerLeafEntryPool.Put(s[:0])
}

func outerLeafArenaClassForLen(capacity int) (idx int, classCap int, ok bool) {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > maxOuterLeafArenaPoolCap {
		return 0, 0, false
	}
	minCap := 1 << outerLeafArenaMinShift
	if capacity <= minCap {
		return 0, minCap, true
	}
	classCap = 1 << uint(bits.Len(uint(capacity-1)))
	if classCap < minCap {
		classCap = minCap
	}
	if classCap > maxOuterLeafArenaPoolCap {
		return 0, 0, false
	}
	shift := bits.Len(uint(classCap)) - 1
	idx = shift - outerLeafArenaMinShift
	if idx < 0 || idx >= outerLeafArenaClassCount {
		return 0, 0, false
	}
	return idx, classCap, true
}

func outerLeafArenaClassForCap(capacity int) (idx int, ok bool) {
	minCap := 1 << outerLeafArenaMinShift
	if capacity < minCap || capacity > maxOuterLeafArenaPoolCap {
		return 0, false
	}
	if capacity&(capacity-1) != 0 {
		return 0, false
	}
	shift := bits.TrailingZeros(uint(capacity))
	idx = shift - outerLeafArenaMinShift
	if idx < 0 || idx >= outerLeafArenaClassCount {
		return 0, false
	}
	return idx, true
}

func outerLeafArenaMaxReuseCap(capacity int) int {
	if capacity <= 0 {
		return 1 << outerLeafArenaMinShift
	}
	// Clamp before multiplication to avoid potential integer overflow.
	if capacity > maxOuterLeafArenaPoolCap/8 {
		return maxOuterLeafArenaPoolCap
	}
	maxCap := capacity * 8
	if maxCap < 1<<20 {
		maxCap = 1 << 20
	}
	if maxCap > maxOuterLeafArenaPoolCap {
		maxCap = maxOuterLeafArenaPoolCap
	}
	return maxCap
}

func batchArenaClassForLen(capacity int) (idx int, classCap int, ok bool) {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > batchCopyArenaMaxRetain {
		return 0, 0, false
	}
	minCap := 1 << batchArenaMinShift
	if capacity <= minCap {
		return 0, minCap, true
	}
	classCap = 1 << uint(bits.Len(uint(capacity-1)))
	if classCap < minCap {
		classCap = minCap
	}
	if classCap > batchCopyArenaMaxRetain {
		return 0, 0, false
	}
	shift := bits.Len(uint(classCap)) - 1
	idx = shift - batchArenaMinShift
	if idx < 0 || idx >= batchArenaClassCount {
		return 0, 0, false
	}
	return idx, classCap, true
}

func batchArenaClassForCap(capacity int) (idx int, ok bool) {
	minCap := 1 << batchArenaMinShift
	if capacity < minCap || capacity > batchCopyArenaMaxRetain {
		return 0, false
	}
	if capacity&(capacity-1) != 0 {
		return 0, false
	}
	shift := bits.TrailingZeros(uint(capacity))
	idx = shift - batchArenaMinShift
	if idx < 0 || idx >= batchArenaClassCount {
		return 0, false
	}
	return idx, true
}

func getBatchArena(capacity int) []byte {
	if capacity < 0 {
		capacity = 0
	}
	idx, classCap, ok := batchArenaClassForLen(capacity)
	if !ok {
		return make([]byte, 0, capacity)
	}
	if v := batchArenaPools[idx].Get(); v != nil {
		if buf, ok := v.([]byte); ok {
			if cap(buf) == classCap {
				return buf[:0]
			}
		}
	}
	return make([]byte, 0, classCap)
}

func putBatchArena(buf []byte) {
	if buf == nil {
		return
	}
	if cap(buf) > batchCopyArenaMaxRetain {
		return
	}
	idx, ok := batchArenaClassForCap(cap(buf))
	if !ok {
		return
	}
	batchArenaPools[idx].Put(buf[:0])
}

func putBatchArenas(chunks [][]byte) {
	for i := range chunks {
		if chunks[i] != nil {
			putBatchArena(chunks[i])
			chunks[i] = nil
		}
	}
}

func getOuterLeafArena(capacity int) []byte {
	if capacity < 0 {
		capacity = 0
	}
	idx, classCap, ok := outerLeafArenaClassForLen(capacity)
	if !ok {
		return make([]byte, 0, capacity)
	}
	maxReuseCap := outerLeafArenaMaxReuseCap(capacity)
	maxIdx, _, maxOK := outerLeafArenaClassForLen(maxReuseCap)
	if !maxOK {
		maxIdx = idx
	}

	outerLeafArenaLeaseMu.Lock()
	for bucket := idx; bucket <= maxIdx; bucket++ {
		leases := outerLeafArenaLeases[bucket]
		if n := len(leases); n > 0 {
			buf := leases[n-1]
			outerLeafArenaLeases[bucket][n-1] = nil
			outerLeafArenaLeases[bucket] = leases[:n-1]
			outerLeafArenaLeaseMu.Unlock()
			if cap(buf) >= capacity {
				return buf[:0]
			}
			if capIdx, ok := outerLeafArenaClassForCap(cap(buf)); ok {
				outerLeafArenaPools[capIdx].Put(buf[:0])
			}
			return make([]byte, 0, classCap)
		}
	}
	outerLeafArenaLeaseMu.Unlock()
	for bucket := idx; bucket <= maxIdx; bucket++ {
		if v := outerLeafArenaPools[bucket].Get(); v != nil {
			if s, ok := v.([]byte); ok && cap(s) >= capacity && cap(s) <= maxReuseCap {
				return s[:0]
			}
		}
	}
	return make([]byte, 0, classCap)
}

func putOuterLeafArena(buf []byte) {
	if buf == nil {
		return
	}
	if cap(buf) > maxOuterLeafArenaPoolCap {
		return
	}
	idx, ok := outerLeafArenaClassForCap(cap(buf))
	if !ok {
		return
	}
	buf = buf[:0]
	outerLeafArenaLeaseMu.Lock()
	if len(outerLeafArenaLeases[idx]) < maxOuterLeafArenaLeases {
		outerLeafArenaLeases[idx] = append(outerLeafArenaLeases[idx], buf)
		outerLeafArenaLeaseMu.Unlock()
		return
	}
	outerLeafArenaLeaseMu.Unlock()
	outerLeafArenaPools[idx].Put(buf)
}

func getOuterLeafBlobRefScratch() *[outerLeafBlobRefStackScratchCap]byte {
	if v := outerLeafBlobRefScratchPool.Get(); v != nil {
		if s, ok := v.(*[outerLeafBlobRefStackScratchCap]byte); ok && s != nil {
			return s
		}
	}
	return new([outerLeafBlobRefStackScratchCap]byte)
}

func putOuterLeafBlobRefScratch(buf *[outerLeafBlobRefStackScratchCap]byte) {
	if buf == nil {
		return
	}
	outerLeafBlobRefScratchPool.Put(buf)
}

func getOuterLeafEncoder() *outerleaf.Encoder {
	if v := outerLeafEncoderPool.Get(); v != nil {
		if e, ok := v.(*outerleaf.Encoder); ok && e != nil {
			return e
		}
	}
	return &outerleaf.Encoder{}
}

func putOuterLeafEncoder(e *outerleaf.Encoder) {
	if e == nil {
		return
	}
	e.Trim(maxOuterLeafEncoderRawScratchCap, maxOuterLeafEncoderEncScratchCap, maxOuterLeafEncoderRestartsCap)
	outerLeafEncoderPool.Put(e)
}

func getValueLogPtrs(n int) []page.ValuePtr {
	if n < 0 {
		n = 0
	}
	if v := valueLogPtrPool.Get(); v != nil {
		if s, ok := v.([]page.ValuePtr); ok {
			if cap(s) >= n {
				return s[:n]
			}
		}
	}
	return make([]page.ValuePtr, n)
}

func getValueLogPtrsCap(capacity int) []page.ValuePtr {
	if capacity < 0 {
		capacity = 0
	}
	if v := valueLogPtrPool.Get(); v != nil {
		if s, ok := v.([]page.ValuePtr); ok {
			maxCap := capacity * 2
			if maxCap < 256 {
				maxCap = 256
			}
			if cap(s) >= capacity && cap(s) <= maxCap {
				return s[:0]
			}
		}
	}
	return make([]page.ValuePtr, 0, capacity)
}

func putValueLogPtrs(s []page.ValuePtr) {
	if s == nil {
		return
	}
	clear(s)
	// Avoid retaining huge slices in the pool.
	if cap(s) > 1<<20 {
		return
	}
	valueLogPtrPool.Put(s[:0])
}

func putValueLogPtrsNoClear(s []page.ValuePtr) {
	if s == nil {
		return
	}
	// page.ValuePtr contains no pointer fields, so we can safely skip element
	// clearing in hot paths to reduce memclr overhead.
	if cap(s) > 1<<20 {
		return
	}
	valueLogPtrPool.Put(s[:0])
}

func getValueLogKeys(capacity int) [][]byte {
	if capacity < 0 {
		capacity = 0
	}
	valueLogKeyLeaseMu.Lock()
	for i := len(valueLogKeyLeases) - 1; i >= 0; i-- {
		s := valueLogKeyLeases[i]
		if cap(s) < capacity {
			continue
		}
		last := len(valueLogKeyLeases) - 1
		valueLogKeyLeases[i] = valueLogKeyLeases[last]
		valueLogKeyLeases[last] = nil
		valueLogKeyLeases = valueLogKeyLeases[:last]
		valueLogKeyLeaseMu.Unlock()
		return s[:0]
	}
	valueLogKeyLeaseMu.Unlock()
	if v := valueLogKeyPool.Get(); v != nil {
		if s, ok := v.([][]byte); ok {
			if cap(s) >= capacity {
				return s[:0]
			}
		}
	}
	return make([][]byte, 0, capacity)
}

func putValueLogKeys(s [][]byte) {
	if s == nil {
		return
	}
	clear(s)
	// Avoid retaining huge slices in the pool.
	if cap(s) > maxValueLogKeyLeaseCap {
		return
	}
	valueLogKeyLeaseMu.Lock()
	if len(valueLogKeyLeases) < maxValueLogKeyLeaseCount {
		valueLogKeyLeases = append(valueLogKeyLeases, s[:0])
		valueLogKeyLeaseMu.Unlock()
		return
	}
	valueLogKeyLeaseMu.Unlock()
	valueLogKeyPool.Put(s[:0])
}

func getVlogPreparedFrameBody() *vlogPreparedFrameBody {
	if v := valueLogPreparedBodyPool.Get(); v != nil {
		if body, ok := v.(*vlogPreparedFrameBody); ok {
			return body
		}
	}
	return &vlogPreparedFrameBody{}
}

func putVlogPreparedFrameBody(body *vlogPreparedFrameBody) {
	if body == nil {
		return
	}
	if cap(body.buf) > maxVlogPreparedBodyPoolCap {
		body.buf = nil
		return
	}
	body.buf = body.buf[:0]
	valueLogPreparedBodyPool.Put(body)
}

func getVlogPreparedFrames(n int) []preparedDictFrame {
	if n < 0 {
		n = 0
	}
	if v := valueLogPreparedFramesPool.Get(); v != nil {
		if s, ok := v.([]preparedDictFrame); ok {
			if cap(s) >= n {
				return s[:n]
			}
		}
	}
	return make([]preparedDictFrame, n)
}

func putVlogPreparedFrames(frames []preparedDictFrame) {
	if frames == nil {
		return
	}
	clear(frames)
	if cap(frames) > maxVlogPreparedFramesPoolCap {
		return
	}
	valueLogPreparedFramesPool.Put(frames[:0])
}

func getVlogDictPrepareResults(capacity int) chan vlogDictPrepareResult {
	if capacity < 1 {
		capacity = 1
	}
	if v := valueLogDictPrepareResultsPool.Get(); v != nil {
		if ch, ok := v.(chan vlogDictPrepareResult); ok {
			maxCap := capacity * 2
			if maxCap < 256 {
				maxCap = 256
			}
			if len(ch) == 0 && cap(ch) >= capacity && cap(ch) <= maxCap {
				return ch
			}
		}
	}
	return make(chan vlogDictPrepareResult, capacity)
}

func putVlogDictPrepareResults(ch chan vlogDictPrepareResult) {
	if ch == nil {
		return
	}
	for {
		select {
		case <-ch:
		default:
			if cap(ch) > maxVlogDictPrepareResultsPoolCap {
				return
			}
			valueLogDictPrepareResultsPool.Put(ch)
			return
		}
	}
}

const (
	envDebugFlushPointers = "TREEDB_DEBUG_FLUSH_PTRS"
	envDebugFlushTiming   = "TREEDB_DEBUG_FLUSH_TIMING"
	// When enabled, checkpoint verifies strict v1_leaflog directory invariants:
	// fence-anchor pointers only, monotonic anchors, and non-overlapping decoded ranges.
	envDebugV1LeafLogInvariants = "TREEDB_DEBUG_V1_LEAFLOG_INVARIANTS"
	// When enabled together with TREEDB_DEBUG_V1_LEAFLOG_INVARIANTS, invariant
	// violations panic instead of returning an error.
	envDebugV1LeafLogInvariantsPanic = "TREEDB_DEBUG_V1_LEAFLOG_INVARIANTS_PANIC"

	minMemtablePrealloc              = 64 * 1024
	maxMemtablePrealloc              = 256 << 20
	adaptiveMinWrites                = 1024
	adaptiveSequentialWritePct       = 0.85
	adaptiveRangeIteratorPct         = 0.40
	adaptiveOverwriteWritePct        = 0.25
	adaptiveWarmupBytes              = 16 * 1024 * 1024
	maxMemtableBytesPerShard         = int64(3 << 30)
	maxOuterLeafArenaPoolCap         = 16 << 20
	outerLeafBlobRefStackScratchCap  = 256
	maxOuterLeafEncoderRawScratchCap = 2 << 20
	maxOuterLeafEncoderEncScratchCap = 2 << 20
	maxOuterLeafEncoderRestartsCap   = 1 << 15
	maxVlogPreparedBodyPoolCap       = 8 << 20
	maxVlogPreparedFramesPoolCap     = 1 << 14
	maxVlogDictPrepareResultsPoolCap = 1 << 14
)

// SetIteratorDebug toggles attaching debug metadata to iterators returned by
// CachingDB.Iterator. It is intended for benchmarking/diagnostics.
func SetIteratorDebug(enabled bool) {
	iteratorDebugEnabled.Store(enabled)
}

func envBool(name string) bool {
	v, ok := os.LookupEnv(name)
	if !ok {
		return false
	}
	v = strings.TrimSpace(strings.ToLower(v))
	if v == "" {
		return true
	}
	switch v {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	}
	if n, err := strconv.Atoi(v); err == nil {
		return n != 0
	}
	return false
}

func (db *DB) maybeValidateV1LeafLogInvariants() error {
	if db == nil || !db.debugV1LeafLogInvariants {
		return nil
	}
	mode := strings.TrimSpace(db.indexOuterLeafMode)
	if mode != backenddb.IndexOuterLeafModeV1LeafLog && mode != backenddb.IndexOuterLeafModeV1LeafLogRoute {
		return nil
	}
	err := db.validateV1LeafLogDirectoryInvariants()
	if err != nil && db.debugV1LeafLogInvariantsPanic {
		panic(err)
	}
	return err
}

func (db *DB) validateV1LeafLogDirectoryInvariants() error {
	if db == nil {
		return nil
	}
	mode := strings.TrimSpace(db.indexOuterLeafMode)
	if mode != backenddb.IndexOuterLeafModeV1LeafLog && mode != backenddb.IndexOuterLeafModeV1LeafLogRoute {
		return nil
	}
	routeMode := mode == backenddb.IndexOuterLeafModeV1LeafLogRoute
	if db.valueLogReader == nil {
		return errors.New("cachingdb: missing value-log reader for v1_leaflog invariant validation")
	}
	type pointerProjectionIter interface {
		IteratorWithOptions(start, end []byte, opts backenddb.IteratorOptions) (iterator.UnsafeIterator, error)
	}
	provider, ok := db.backend.(pointerProjectionIter)
	if !ok {
		return nil
	}
	it, err := provider.IteratorWithOptions(nil, nil, backenddb.IteratorOptions{
		Mode:              backenddb.IteratorModePointerProjection,
		IncludeTombstones: true,
	})
	if err != nil {
		return err
	}
	if it == nil {
		return nil
	}
	defer func() { _ = it.Close() }()

	type anchorRange struct {
		anchor []byte
		min    []byte
		max    []byte
	}
	rangeByPtr := make(map[page.ValuePtr]anchorRange, 256)
	var prev *anchorRange
	for it.Valid() {
		k := it.UnsafeKey()
		_, ptr, flags := it.UnsafeEntry()
		it.Next()
		if routeMode && flags&node.FlagTombstone == 0 && flags&node.FlagPointer == 0 {
			return fmt.Errorf("cachingdb: v1_leaflog invariant violation: route mode persisted non-pointer row key=%q flags=%#x", k, flags)
		}
		if flags&node.FlagPointer == 0 || flags&node.FlagTombstone != 0 {
			continue
		}
		basePtr := ptr
		if routeMode {
			if page.ValuePtrIsFenceOuter(ptr) {
				return fmt.Errorf("cachingdb: v1_leaflog invariant violation: route mode anchor should be plain pointer key=%q ptr=%+v", k, ptr)
			}
		} else {
			basePtr = page.ValuePtrClearFenceOuter(ptr)
		}
		rng, decoded := rangeByPtr[basePtr]
		if !decoded {
			raw, err := db.valueLogReader.Read(basePtr)
			if err != nil {
				return fmt.Errorf("cachingdb: v1_leaflog invariant read failed key=%q ptr=%+v: %w", k, ptr, err)
			}
			if !outerleaf.HasMagic(raw) {
				return fmt.Errorf("cachingdb: v1_leaflog invariant violation: pointer does not reference outer-leaf payload key=%q ptr=%+v", k, ptr)
			}
			keys, err := outerleaf.DecodeKeysWithVerify(raw, true)
			if err != nil {
				return fmt.Errorf("cachingdb: v1_leaflog invariant decode failed key=%q ptr=%+v: %w", k, ptr, err)
			}
			if len(keys) == 0 {
				return fmt.Errorf("cachingdb: v1_leaflog invariant violation: empty outer-leaf payload key=%q ptr=%+v", k, ptr)
			}
			for i := 1; i < len(keys); i++ {
				if bytes.Compare(keys[i-1], keys[i]) >= 0 {
					return fmt.Errorf("cachingdb: v1_leaflog invariant violation: non-monotonic payload keys ptr=%+v", ptr)
				}
			}
			rng = anchorRange{
				anchor: append([]byte(nil), keys[0]...),
				min:    append([]byte(nil), keys[0]...),
				max:    append([]byte(nil), keys[len(keys)-1]...),
			}
			rangeByPtr[basePtr] = rng
		} else if routeMode {
			return fmt.Errorf("cachingdb: v1_leaflog invariant violation: duplicate block pointer rows key=%q ptr=%+v first_anchor=%q", k, ptr, rng.anchor)
		}
		if bytes.Compare(k, rng.min) < 0 || bytes.Compare(k, rng.max) > 0 {
			return fmt.Errorf("cachingdb: v1_leaflog invariant violation: anchor key outside decoded range key=%q range=[%q,%q] ptr=%+v", k, rng.min, rng.max, ptr)
		}
		if prev != nil {
			if bytes.Compare(prev.anchor, k) >= 0 {
				return fmt.Errorf("cachingdb: v1_leaflog invariant violation: non-increasing anchor order prev=%q curr=%q", prev.anchor, k)
			}
		}
		next := rng
		prev = &next
	}
	if err := it.Error(); err != nil {
		return err
	}
	return nil
}

func (db *DB) syncDirBestEffort(dir string) {
	if dir == "" || runtime.GOOS == "windows" {
		return
	}
	f, err := os.Open(dir)
	if err != nil {
		db.reportError(fmt.Errorf("cachingdb: failed to open dir %q for sync: %w", dir, err))
		return
	}
	if err := f.Sync(); err != nil {
		db.reportError(fmt.Errorf("cachingdb: failed to sync dir %q: %w", dir, err))
	}
	if err := f.Close(); err != nil {
		db.reportError(fmt.Errorf("cachingdb: failed to close dir %q after sync: %w", dir, err))
	}
}

func (db *DB) removeFileRetry(path string) error {
	var err error
	for i := 0; i < 20; i++ {
		err = os.Remove(path)
		if err == nil || os.IsNotExist(err) {
			return nil
		}
		if runtime.GOOS != "windows" {
			return err
		}
		// Windows: Retry with exponential backoff up to ~1s total
		time.Sleep(time.Duration(i+1) * 5 * time.Millisecond)
	}
	return err
}

func warnInsecureDir(dir string, notify func(error)) {
	if dir == "" || notify == nil || runtime.GOOS == "windows" {
		return
	}
	info, err := os.Lstat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		notify(fmt.Errorf("cachingdb: failed to stat dir %q: %w", dir, err))
		return
	}
	if info.Mode()&os.ModeSymlink != 0 {
		notify(fmt.Errorf("cachingdb: dir %q is a symlink; verify target permissions", dir))
		info, err = os.Stat(dir)
		if err != nil {
			notify(fmt.Errorf("cachingdb: failed to stat symlink target %q: %w", dir, err))
			return
		}
	}
	if !info.IsDir() {
		notify(fmt.Errorf("cachingdb: path %q is not a directory", dir))
		return
	}
	perms := info.Mode().Perm()
	if perms&0o002 != 0 {
		notify(fmt.Errorf("cachingdb: dir %q is world-writable (mode %o)", dir, perms))
	} else if perms&0o020 != 0 {
		notify(fmt.Errorf("cachingdb: dir %q is group-writable (mode %o)", dir, perms))
	}
}

func memtableCapacity(flushThreshold int64) int {
	if flushThreshold <= 0 {
		return 0
	}
	capBytes := flushThreshold + flushThreshold/4 // +25% to cover skiplist overhead.
	if capBytes < minMemtablePrealloc {
		capBytes = minMemtablePrealloc
	}
	if capBytes > maxMemtablePrealloc {
		capBytes = maxMemtablePrealloc
	}
	maxInt := int64(int(^uint(0) >> 1))
	if capBytes > maxInt {
		capBytes = maxInt
	}
	return int(capBytes)
}

func normalizeShardCount(n int) int {
	if n < 1 {
		return 1
	}
	// Round down to a power of two.
	v := 1
	for v<<1 <= n {
		v <<= 1
	}
	return v
}

func defaultMemtableShards() int {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		n = 1
	}
	n *= 2
	if n > 16 {
		n = 16
	}
	return normalizeShardCount(n)
}

func shardCapacity(totalCap, shards int) int {
	if shards <= 1 {
		return totalCap
	}
	if totalCap <= 0 {
		return 1
	}
	cap := totalCap / shards
	if cap <= 0 {
		cap = 1
	}
	return cap
}

func (db *DB) valueLogEnabled() bool {
	return true
}

func (db *DB) splitValueLogEnabled() bool {
	return true
}

func (db *DB) valueLogThresholdForKey(key []byte) int {
	if db == nil {
		return page.DefaultInlineThreshold
	}
	return backenddb.ResolveInlineThresholdForKey(db.valueLogThreshold, key, db.valueLogDomainThresholds)
}

func (db *DB) shouldWriteViaValueLogForKeyValue(key, value []byte) bool {
	if db == nil {
		return false
	}
	switch strings.TrimSpace(db.indexOuterLeafMode) {
	case backenddb.IndexOuterLeafModeV1LeafLogRoute:
		// Route mode uses directory anchors with value-log payload blocks even for
		// values that are below the user-configured inline threshold.
		return true
	default:
		return db.forceValueLogPointers || len(value) > db.valueLogThresholdForKey(key)
	}
}

func (db *DB) outerLeafV2Enabled() bool {
	return db != nil && outerleaf.ModeEnabled(db.indexOuterLeafMode)
}

func (db *DB) outerLeafFenceV2Enabled() bool {
	return db != nil && strings.TrimSpace(db.indexOuterLeafMode) == backenddb.IndexOuterLeafModeV2FencePtr
}

func (db *DB) outerLeafAnchorModeEnabled() bool {
	if db == nil {
		return false
	}
	switch strings.TrimSpace(db.indexOuterLeafMode) {
	case backenddb.IndexOuterLeafModeV2FencePtr, backenddb.IndexOuterLeafModeV1LeafLog, backenddb.IndexOuterLeafModeV1LeafLogRoute:
		return true
	default:
		return false
	}
}

func normalizeValueLogWALFenceMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", string(backenddb.ValueLogWALFenceModeRIDJoin):
		return string(backenddb.ValueLogWALFenceModeRIDJoin)
	case string(backenddb.ValueLogWALFenceModeSimpleInline):
		return string(backenddb.ValueLogWALFenceModeSimpleInline)
	default:
		return strings.ToLower(strings.TrimSpace(mode))
	}
}

const defaultOuterLeafBlobThresholdMin = 16 << 10
const defaultOuterLeafFenceBlockTargetBytes = 4 << 10
const outerLeafFenceAdaptiveLZ4MaxAvgValueBytes = 64

func (db *DB) effectiveOuterLeafBlobThresholdBytes() int {
	if db == nil {
		return defaultOuterLeafBlobThresholdMin
	}
	if db.outerLeafBlobThresholdBytes > 0 {
		return db.outerLeafBlobThresholdBytes
	}
	return defaultOuterLeafBlobThresholdMin
}

func (db *DB) oversizedOuterLeafValue(value []byte) bool {
	if db == nil || !db.outerLeafFenceV2Enabled() {
		return false
	}
	return len(value) >= db.effectiveOuterLeafBlobThresholdBytes()
}

func (db *DB) fenceRIDJoinHybridEnabled() bool {
	return db != nil &&
		db.outerLeafFenceV2Enabled() &&
		!db.disableJournal &&
		db.valueLogWALFenceMode == string(backenddb.ValueLogWALFenceModeRIDJoin)
}

func (db *DB) selectOuterLeafBlockCodec(totalValueBytes, entryCount int) uint8 {
	if db == nil {
		return uint8(backenddb.ValueLogBlockSnappy)
	}
	codec := db.outerLeafBlockCodec
	if codec != uint8(backenddb.ValueLogBlockSnappy) || !db.outerLeafFenceV2Enabled() || entryCount <= 0 {
		return codec
	}
	avgValueBytes := totalValueBytes / entryCount
	if avgValueBytes <= outerLeafFenceAdaptiveLZ4MaxAvgValueBytes {
		return uint8(backenddb.ValueLogBlockLZ4)
	}
	return codec
}

func (db *DB) encodeOuterLeafValue(key, value []byte) ([]byte, error) {
	if !db.outerLeafV2Enabled() {
		return value, nil
	}
	codec := db.selectOuterLeafBlockCodec(len(value), 1)
	return outerleaf.EncodeSingle(nil, key, value, codec, db.outerLeafBlockRestart)
}

func (db *DB) encodeOuterLeafBlobRef(dst, key []byte, ptr page.ValuePtr) ([]byte, error) {
	if !db.outerLeafV2Enabled() {
		return nil, fmt.Errorf("cachingdb: blob-ref outer-leaf encoding requires v2 mode")
	}
	enc := getOuterLeafEncoder()
	defer putOuterLeafEncoder(enc)
	var single [1]outerleaf.TypedEntry
	single[0].Key = key
	single[0].Kind = outerleaf.EntryKindBlobRef
	single[0].BlobPtr = ptr
	codec := db.selectOuterLeafBlockCodec(0, 1)
	return enc.EncodeTypedEntries(dst, single[:], codec, db.outerLeafBlockRestart)
}

func (db *DB) decodeOuterLeafValue(key, value []byte) ([]byte, error) {
	if !db.outerLeafV2Enabled() {
		return value, nil
	}
	entry, ok, found, _, err := outerleaf.DecodeEntryForKey(value, key, nil)
	if err != nil {
		return nil, err
	}
	if !ok {
		return value, nil
	}
	if !found {
		return nil, fmt.Errorf("cachingdb: outer-leaf key lookup miss")
	}
	if entry.Kind == outerleaf.EntryKindBlobRef {
		if db.valueLogReader == nil {
			return nil, fmt.Errorf("cachingdb: blob-ref entry requires value-log reader")
		}
		return db.valueLogReader.Read(entry.BlobPtr)
	}
	return entry.Value, nil
}

func allOuterLeafRecordValues(records []valuelog.Record) bool {
	if len(records) == 0 {
		return false
	}
	for i := range records {
		if !outerleaf.HasMagic(records[i].Value) {
			return false
		}
	}
	return true
}

type outerLeafRecordGroup struct {
	start int
	end   int
}

const outerLeafEncodedHeaderBytes = 22

func ensureOuterLeafArenaCap(buf []byte, need int) []byte {
	if need <= 0 || cap(buf)-len(buf) >= need {
		return buf
	}
	want := len(buf) + need
	newCap := cap(buf) * 2
	if newCap < want {
		newCap = want
	}
	grown := make([]byte, len(buf), newCap)
	copy(grown, buf)
	return grown
}

func appendOuterLeafRecordGroup(db *DB, encoder *outerleaf.Encoder, entries []outerleaf.Entry, groupStart int, records []valuelog.Record, groups []outerLeafRecordGroup, arena []byte, maxEncodedHint int) ([]valuelog.Record, []outerLeafRecordGroup, []byte, error) {
	if len(entries) == 0 {
		return records, groups, arena, nil
	}
	if maxEncodedHint < outerLeafEncodedHeaderBytes+32 {
		maxEncodedHint = outerLeafEncodedHeaderBytes + 32
	}
	arena = ensureOuterLeafArenaCap(arena, maxEncodedHint)
	base := len(arena)
	dst := arena[base:base]

	var (
		payload []byte
		err     error
	)
	totalValueBytes := 0
	for i := range entries {
		totalValueBytes += len(entries[i].Value)
	}
	codec := db.selectOuterLeafBlockCodec(totalValueBytes, len(entries))
	if encoder != nil {
		payload, err = encoder.EncodeEntriesAssumeSorted(dst, entries, codec, db.outerLeafBlockRestart)
	} else {
		payload, err = outerleaf.EncodeEntriesAssumeSorted(dst, entries, codec, db.outerLeafBlockRestart)
	}
	if err != nil {
		return nil, nil, nil, err
	}
	arena = arena[:base+len(payload)]
	payload = arena[base : base+len(payload)]
	records = append(records, valuelog.Record{Value: payload})
	groups = append(groups, outerLeafRecordGroup{start: groupStart, end: groupStart + len(entries)})
	return records, groups, arena, nil
}

func appendOuterLeafTypedRecordGroup(db *DB, encoder *outerleaf.Encoder, entries []outerleaf.TypedEntry, groupStart int, records []valuelog.Record, groups []outerLeafRecordGroup, arena []byte, maxEncodedHint int) ([]valuelog.Record, []outerLeafRecordGroup, []byte, error) {
	if len(entries) == 0 {
		return records, groups, arena, nil
	}
	if maxEncodedHint < outerLeafEncodedHeaderBytes+32 {
		maxEncodedHint = outerLeafEncodedHeaderBytes + 32
	}
	arena = ensureOuterLeafArenaCap(arena, maxEncodedHint)
	base := len(arena)
	dst := arena[base:base]

	var (
		payload []byte
		err     error
	)
	totalValueBytes := 0
	for i := range entries {
		if entries[i].Kind == outerleaf.EntryKindInline {
			totalValueBytes += len(entries[i].Value)
		}
	}
	codec := db.selectOuterLeafBlockCodec(totalValueBytes, len(entries))
	if encoder != nil {
		payload, err = encoder.EncodeTypedEntriesAssumeSorted(dst, entries, codec, db.outerLeafBlockRestart)
	} else {
		payload, err = outerleaf.EncodeTypedEntriesAssumeSorted(dst, entries, codec, db.outerLeafBlockRestart)
	}
	if err != nil {
		return nil, nil, nil, err
	}
	arena = arena[:base+len(payload)]
	payload = arena[base : base+len(payload)]
	records = append(records, valuelog.Record{Value: payload})
	groups = append(groups, outerLeafRecordGroup{start: groupStart, end: groupStart + len(entries)})
	return records, groups, arena, nil
}

func (db *DB) writeRouteOuterLeafValueRecords(lane *lane, dictID uint64, keys [][]byte, values [][]byte, durability journalDurability) ([]page.ValuePtr, []outerLeafRecordGroup, error) {
	if len(keys) != len(values) {
		return nil, nil, fmt.Errorf("cachingdb: outer-leaf key/value length mismatch %d/%d", len(keys), len(values))
	}
	if len(keys) == 0 {
		return nil, nil, nil
	}
	if lane == nil {
		return nil, nil, errWALUnavailable
	}

	recordsCap := len(keys)
	groupsCap := len(keys)
	entriesCap := 8

	targetBytes := db.outerLeafBlockTargetBytes
	if targetBytes <= 0 {
		targetBytes = outerleaf.NormalizeBlockTargetBytes(0)
	}

	if len(keys) > 1 {
		sample := len(keys)
		if sample > 8 {
			sample = 8
		}
		avgEntryEstimate := 1
		if sample > 0 {
			total := 0
			for i := 0; i < sample; i++ {
				total += len(keys[i]) + len(values[i]) + 16
			}
			avgEntryEstimate = total / sample
			if avgEntryEstimate <= 0 {
				avgEntryEstimate = 1
			}
		}
		entriesPerBlock := targetBytes / avgEntryEstimate
		if entriesPerBlock < 1 {
			entriesPerBlock = 1
		}
		entriesCap = entriesPerBlock
		if entriesCap < 8 {
			entriesCap = 8
		}
		if entriesCap > len(keys) {
			entriesCap = len(keys)
		}
		recordsCap = (len(keys) + entriesPerBlock - 1) / entriesPerBlock
		if recordsCap < 1 {
			recordsCap = 1
		}
		if recordsCap > len(keys) {
			recordsCap = len(keys)
		}
		groupsCap = recordsCap
	}

	records := getValueLogRecordsCap(recordsCap)
	defer putValueLogRecordsNoClear(records)
	groups := make([]outerLeafRecordGroup, 0, groupsCap)

	entries := make([]outerleaf.TypedEntry, 0, entriesCap)
	nestedIdxs := make([]int, 0, entriesCap/2)
	outerEncoder := getOuterLeafEncoder()
	defer putOuterLeafEncoder(outerEncoder)

	arenaCap := outerLeafEncodedHeaderBytes + len(keys)*8
	for i := range keys {
		arenaCap += len(keys[i])
		arenaCap += len(values[i])
	}
	arena := getOuterLeafArena(arenaCap)
	defer putOuterLeafArena(arena)

	estimated := 0
	groupStart := 0
	var prevKey []byte

	flushRouteGroup := func() error {
		if len(entries) == 0 {
			return nil
		}
		nestedRecords := getValueLogRecordsCap(len(nestedIdxs))
		defer putValueLogRecordsNoClear(nestedRecords)
		if len(nestedIdxs) > 0 {
			startRID := db.nextRID.Add(uint64(len(nestedIdxs))) - uint64(len(nestedIdxs)) + 1
			for _, nestedPos := range nestedIdxs {
				nestedRecords = append(nestedRecords, valuelog.Record{
					RID:   startRID + uint64(len(nestedRecords)),
					Value: entries[nestedPos].Value,
				})
			}
			nestedPtrs, err := db.appendValueLog(lane, dictID, nil, nestedRecords, durability)
			if err != nil {
				return err
			}
			defer putValueLogPtrs(nestedPtrs)
			if len(nestedPtrs) != len(nestedIdxs) {
				return fmt.Errorf("cachingdb: route nested pointer count mismatch expected=%d got=%d", len(nestedIdxs), len(nestedPtrs))
			}
			for i, nestedPos := range nestedIdxs {
				entries[nestedPos].BlobPtr = nestedPtrs[i]
				entries[nestedPos].Value = nil
			}
		}

		var err error
		records, groups, arena, err = appendOuterLeafTypedRecordGroup(db, outerEncoder, entries, groupStart, records, groups, arena, estimated+32)
		if err != nil {
			return err
		}
		entries = entries[:0]
		nestedIdxs = nestedIdxs[:0]
		estimated = 0
		prevKey = nil
		return nil
	}

	for i := range keys {
		key := keys[i]
		value := values[i]
		valueShouldBeNested := len(value) > db.valueLogThresholdForKey(key)
		entryEstimate := len(key) + len(value) + 16
		if valueShouldBeNested {
			// Nested values are represented as pointer entries and do not grow
			// in-blob payloads with raw value bytes.
			entryEstimate = len(key) + 16
		}

		if len(entries) > 0 {
			if bytes.Compare(prevKey, key) >= 0 {
				// Preserve correctness for non-monotonic batches by cutting blocks.
				if err := flushRouteGroup(); err != nil {
					return nil, nil, err
				}
				groupStart = i
			} else if estimated+entryEstimate > targetBytes {
				if err := flushRouteGroup(); err != nil {
					return nil, nil, err
				}
				groupStart = i
			}
		}

		entry := outerleaf.TypedEntry{
			Key: key,
		}
		if valueShouldBeNested {
			entry.Kind = outerleaf.EntryKindBlobRef
			entry.Value = value
		} else {
			entry.Kind = outerleaf.EntryKindInline
			entry.Value = value
		}
		if valueShouldBeNested {
			nestedIdxs = append(nestedIdxs, len(entries))
		}
		if len(entries) == 0 {
			groupStart = i
		}
		entries = append(entries, entry)
		estimated += entryEstimate
		prevKey = key
	}
	if len(entries) > 0 {
		if err := flushRouteGroup(); err != nil {
			return nil, nil, err
		}
	}

	if len(records) == 0 {
		return nil, nil, nil
	}
	startRID := db.nextRID.Add(uint64(len(records))) - uint64(len(records)) + 1
	for i := range records {
		records[i].RID = startRID + uint64(i)
	}
	ptrs, err := db.appendValueLog(lane, dictID, nil, records, durability)
	if err != nil {
		return nil, nil, err
	}
	if len(ptrs) != len(records) {
		putValueLogPtrs(ptrs)
		return nil, nil, fmt.Errorf("cachingdb: outer route value-log returned %d ptrs for %d records", len(ptrs), len(records))
	}
	return ptrs, groups, nil
}

// buildOuterLeafValueRecords encodes key/value pairs into value-log records.
// The returned groups map each encoded record back to a contiguous source range
// [start,end).
func (db *DB) buildOuterLeafValueRecords(keys [][]byte, values [][]byte) ([]valuelog.Record, []outerLeafRecordGroup, []byte, error) {
	if len(keys) != len(values) {
		return nil, nil, nil, fmt.Errorf("cachingdb: outer-leaf key/value length mismatch %d/%d", len(keys), len(values))
	}
	if len(keys) == 0 {
		return nil, nil, nil, nil
	}
	recordsCap := len(keys)
	groupsCap := len(keys)
	entriesCap := 8

	if db.outerLeafV2Enabled() && len(keys) > 1 {
		targetBytes := db.outerLeafBlockTargetBytes
		if targetBytes <= 0 {
			targetBytes = outerleaf.NormalizeBlockTargetBytes(0)
		}
		sample := len(keys)
		if sample > 8 {
			sample = 8
		}
		avgEntryEstimate := 1
		if sample > 0 {
			total := 0
			for i := 0; i < sample; i++ {
				total += len(keys[i]) + len(values[i]) + 16
			}
			avgEntryEstimate = total / sample
			if avgEntryEstimate <= 0 {
				avgEntryEstimate = 1
			}
		}
		entriesPerBlock := targetBytes / avgEntryEstimate
		if entriesPerBlock < 1 {
			entriesPerBlock = 1
		}
		entriesCap = entriesPerBlock
		if entriesCap < 8 {
			entriesCap = 8
		}
		if entriesCap > len(keys) {
			entriesCap = len(keys)
		}
		recordsCap = (len(keys) + entriesPerBlock - 1) / entriesPerBlock
		if recordsCap < 1 {
			recordsCap = 1
		}
		if recordsCap > len(keys) {
			recordsCap = len(keys)
		}
		groupsCap = recordsCap
	}

	records := getValueLogRecordsCap(recordsCap)
	groups := make([]outerLeafRecordGroup, 0, groupsCap)

	if !db.outerLeafV2Enabled() || len(keys) == 1 {
		for i := range keys {
			payload, err := db.encodeOuterLeafValue(keys[i], values[i])
			if err != nil {
				return nil, nil, nil, err
			}
			records = append(records, valuelog.Record{Value: payload})
			groups = append(groups, outerLeafRecordGroup{start: i, end: i + 1})
		}
		return records, groups, nil, nil
	}

	targetBytes := db.outerLeafBlockTargetBytes
	if targetBytes <= 0 {
		targetBytes = outerleaf.NormalizeBlockTargetBytes(0)
	}

	entries := getOuterLeafEntriesCap(entriesCap)
	defer putOuterLeafEntries(entries)
	outerEncoder := getOuterLeafEncoder()
	defer putOuterLeafEncoder(outerEncoder)
	arenaCap := outerLeafEncodedHeaderBytes + len(keys)*8
	for i := range keys {
		arenaCap += len(keys[i]) + len(values[i])
	}
	arena := getOuterLeafArena(arenaCap)
	releaseArena := true
	defer func() {
		if releaseArena {
			putOuterLeafArena(arena)
		}
	}()
	estimated := 0
	var prevKey []byte
	groupStart := 0

	for i := range keys {
		key := keys[i]
		value := values[i]
		entryEstimate := len(key) + len(value) + 16
		if len(entries) > 0 {
			if bytes.Compare(prevKey, key) >= 0 {
				// Preserve correctness for non-monotonic batches by cutting blocks.
				var err error
				records, groups, arena, err = appendOuterLeafRecordGroup(db, outerEncoder, entries, groupStart, records, groups, arena, estimated+32)
				if err != nil {
					return nil, nil, nil, err
				}
				entries = entries[:0]
				estimated = 0
				prevKey = nil
			} else if estimated+entryEstimate > targetBytes {
				var err error
				records, groups, arena, err = appendOuterLeafRecordGroup(db, outerEncoder, entries, groupStart, records, groups, arena, estimated+32)
				if err != nil {
					return nil, nil, nil, err
				}
				entries = entries[:0]
				estimated = 0
				prevKey = nil
			}
		}
		if len(entries) == 0 {
			groupStart = i
		}
		entries = append(entries, outerleaf.Entry{Key: key, Value: value})
		estimated += entryEstimate
		prevKey = key
	}
	if len(entries) > 0 {
		var err error
		records, groups, arena, err = appendOuterLeafRecordGroup(db, outerEncoder, entries, groupStart, records, groups, arena, estimated+32)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	releaseArena = false
	return records, groups, arena, nil
}

func fallbackAutoVlogWriteMode(mode vlogCompressionMode, writeMode vlogCompressionWriteMode) vlogCompressionWriteMode {
	if mode == vlogCompressionAuto && writeMode == vlogWriteDict {
		return vlogWriteBlock
	}
	return writeMode
}

func lookupVlogDictBytes(dictID uint64, singleDictID uint64, singleDict []byte, dictByID map[uint64][]byte) []byte {
	if dictID == 0 {
		return nil
	}
	if dictByID == nil {
		if dictID == singleDictID {
			return singleDict
		}
		return nil
	}
	return dictByID[dictID]
}

func (db *DB) deferredValueLogEnabled() bool {
	// Routing-anchor outer-leaf modes benefit from flush-time regrouping so
	// singleton writes can coalesce into larger outer blocks before value-log
	// append, reducing index pointer fanout.
	//
	// WAL-on v2_fenceptr mode:
	//   - simple_inline: deferred for all pointer-eligible values
	//   - rid_join: deferred for non-oversized pointer-eligible values (C1),
	//     while oversized values (C2) may still take immediate pointer paths.
	if db == nil {
		return false
	}
	if !db.outerLeafAnchorModeEnabled() || !db.valueLogEnabled() || !db.allowValueLogPointers() {
		return false
	}
	return true
}

func (db *DB) deferredValueLogNeedsSingleLaneRegroup() bool {
	// Deferred fence materialization currently requires single-lane regrouping
	// to preserve stable fence collapse ordering (including WAL-on simple_inline).
	return db != nil && db.deferredValueLogEnabled()
}

func (db *DB) allowFencePointerCollapse() bool {
	// Collapse is correctness-coupled with anchor promotion and is only allowed in
	// deferred fence mode where flush regrouping is single-lane.
	return db != nil && db.deferredValueLogEnabled() && db.supportsFenceAnchorPromotion()
}

func (db *DB) allowMutableFencePointerCollapse() bool {
	// Mutable-path deferred pointer rewrite is intentionally left uncollapsed.
	// In-place collapse before flush-time anchor promotion can orphan siblings.
	return false
}

func (db *DB) supportsFenceAnchorPromotion() bool {
	if db == nil || db.valueLogReader == nil || db.backend == nil {
		return false
	}
	type snapshotProvider interface {
		AcquireSnapshot() *backenddb.Snapshot
	}
	_, ok := db.backend.(snapshotProvider)
	return ok
}

// sumKeyValueBytes returns the total bytes across paired key/value entries.
// If keys and values differ in length, only the shared prefix is counted.
func sumKeyValueBytes(keys, values [][]byte) uint64 {
	if len(keys) == 0 || len(values) == 0 {
		return 0
	}
	if len(keys) > len(values) {
		keys = keys[:len(values)]
	} else if len(values) > len(keys) {
		values = values[:len(keys)]
	}
	var total uint64
	for i := 0; i < len(keys); i++ {
		total += uint64(len(keys[i]) + len(values[i]))
	}
	return total
}

func (db *DB) recordDeferredFenceEnqueued(keys int, bytes uint64) {
	if db == nil || keys <= 0 {
		return
	}
	db.deferredFenceEnqueuedKeys.Add(uint64(keys))
	if bytes > 0 {
		db.deferredFenceEnqueuedBytes.Add(bytes)
	}
}

func (db *DB) recordDeferredFenceMaterialized(keys int, bytes uint64) {
	if db == nil || keys <= 0 {
		return
	}
	// Keep legacy counters for backwards-compatible dashboards.
	db.deferredFenceCandidateKeys.Add(uint64(keys))
	db.deferredFenceMaterializedKeys.Add(uint64(keys))
	if bytes > 0 {
		db.deferredFenceMaterializedBytes.Add(bytes)
	}
}

func (db *DB) shouldFlushDeferredValueLog(writeMode vlogCompressionWriteMode, records []valuelog.Record) bool {
	if !db.deferredValueLogEnabled() {
		return false
	}
	// For v2 fence-pointer write paths, keep deferred appends buffered across
	// calls; readers flush on demand via flushValueLogForPtr.
	if db.outerLeafFenceV2Enabled() {
		if allOuterLeafRecordValues(records) {
			return false
		}
		// WAL-on rid_join oversized writes (C2) emit a raw blob record followed by
		// an outer-leaf blob-ref record. Keep the blob append buffered too so the
		// pair does not force an extra write syscall per Set/Batch item.
		if db.fenceRIDJoinHybridEnabled() && len(records) == 1 && db.oversizedOuterLeafValue(records[0].Value) {
			return false
		}
	}
	return true
}

func (db *DB) shouldFlushDeferredValueLogValue(writeMode vlogCompressionWriteMode, value []byte) bool {
	if !db.deferredValueLogEnabled() {
		return false
	}
	if db.outerLeafFenceV2Enabled() {
		if outerleaf.HasMagic(value) {
			return false
		}
		if db.fenceRIDJoinHybridEnabled() && db.oversizedOuterLeafValue(value) {
			return false
		}
	}
	return true
}

func (db *DB) walUsesValueLog() bool {
	return false
}

func (db *DB) needsVlogAutotuneTiming() bool {
	if db == nil {
		return false
	}
	if db.valueLogAutotuneOptions.Mode != valuelog.AutotuneOff {
		return true
	}
	return vlogAutotuneMetricsEnabled.Load()
}

func (db *DB) pickLane(sync bool, preferred int) (*lane, error) {
	if db == nil || len(db.lanes) == 0 {
		return nil, errWALUnavailable
	}
	if !sync && preferred >= 0 && preferred < len(db.lanes) {
		l := &db.lanes[preferred]
		if !l.syncing.Load() {
			return l, nil
		}
	}

	db.laneMu.Lock()
	defer db.laneMu.Unlock()
	for {
		select {
		case <-db.closeCh:
			return nil, errWALClosed
		default:
		}

		if preferred >= 0 && preferred < len(db.lanes) {
			l := &db.lanes[preferred]
			if !l.syncing.Load() {
				if sync {
					l.syncing.Store(true)
				}
				return l, nil
			}
			// If preferred lane is busy, we could wait or fallback.
			// To maintain strict lane-affinity, we wait.
			db.laneCond.Wait()
			continue
		}

		start := db.nextLane
		for i := 0; i < len(db.lanes); i++ {
			idx := (start + i) % len(db.lanes)
			l := &db.lanes[idx]
			if l.syncing.Load() {
				continue
			}
			db.nextLane = (idx + 1) % len(db.lanes)
			if sync {
				l.syncing.Store(true)
			}
			return l, nil
		}
		db.laneCond.Wait()
	}
}

func (db *DB) releaseLaneSync(l *lane) {
	if l == nil {
		return
	}
	if !l.syncing.CompareAndSwap(true, false) {
		return
	}
	db.laneMu.Lock()
	db.laneCond.Broadcast()
	db.laneMu.Unlock()
}

func (db *DB) currentValueLogPath(l *lane) string {
	if l == nil {
		return ""
	}
	if db.splitValueLogEnabled() {
		l.vlogMu.Lock()
		path := l.vlogPath
		l.vlogMu.Unlock()
		return path
	}
	l.walMu.Lock()
	path := l.walPath
	l.walMu.Unlock()
	return path
}

func (db *DB) currentValueLogSeq(l *lane) int {
	if l == nil {
		return 0
	}
	if db.splitValueLogEnabled() {
		l.vlogMu.Lock()
		seq := l.vlogSeq
		l.vlogMu.Unlock()
		return seq
	}
	l.walMu.Lock()
	seq := l.walSeq
	l.walMu.Unlock()
	return seq
}

func (db *DB) currentWALPaths() []string {
	if db == nil || db.disableJournal {
		return nil
	}
	paths := make([]string, 0, len(db.lanes))
	for i := range db.lanes {
		l := &db.lanes[i]
		l.walMu.Lock()
		if l.walPath != "" {
			paths = append(paths, l.walPath)
		}
		l.walMu.Unlock()
	}
	return paths
}

func (db *DB) currentValueLogPaths() []string {
	if db == nil || !db.valueLogEnabled() {
		return nil
	}
	paths := make([]string, 0, len(db.lanes))
	for i := range db.lanes {
		l := &db.lanes[i]
		if db.splitValueLogEnabled() {
			l.vlogMu.Lock()
			if l.vlogPath != "" {
				paths = append(paths, l.vlogPath)
			}
			l.vlogMu.Unlock()
			continue
		}
		l.walMu.Lock()
		if l.walPath != "" {
			paths = append(paths, l.walPath)
		}
		l.walMu.Unlock()
	}
	return paths
}

func (db *DB) deferValueLogOps(ops []batch.Entry, sync bool) ([]batch.Entry, error) {
	if db == nil || len(ops) == 0 || !db.deferredValueLogEnabled() {
		return ops, nil
	}
	if !db.allowValueLogPointers() {
		return ops, nil
	}
	fenceMode := db.outerLeafAnchorModeEnabled()
	collapseFence := fenceMode && db.allowMutableFencePointerCollapse()
	routeAnchorMode := strings.TrimSpace(db.indexOuterLeafMode) == backenddb.IndexOuterLeafModeV1LeafLogRoute

	eligible := getValueLogEligible(len(ops))
	defer putValueLogEligible(eligible)
	for i := range ops {
		op := &ops[i]
		if op.Type != batch.OpPut || op.IsPtr {
			continue
		}
		if !db.shouldWriteViaValueLogForKeyValue(op.Key, op.Value) {
			continue
		}
		eligible = append(eligible, i)
	}
	if len(eligible) == 0 {
		return ops, nil
	}

	lane, err := db.pickLane(false, -1)
	if err != nil {
		return nil, err
	}

	// Best-effort: use the current dict when available.
	dictID := uint64(0)
	if db.dictStore != nil {
		if id, err := db.currentDictID(context.Background()); err == nil {
			dictID = id
		}
	}

	keys := getValueLogKeys(len(eligible))
	defer putValueLogKeys(keys)
	values := getValueLogKeys(len(eligible))
	defer putValueLogKeys(values)
	for _, idx := range eligible {
		op := &ops[idx]
		keys = append(keys, op.Key)
		values = append(values, op.Value)
	}
	// For deferred value-log appends, unsynced writes intentionally use "none"
	// value-log flush durability, and synced writes use "sync" value-log flush
	// durability. Note: these modes control value-log flush behavior here, not
	// journal/WAL durability.
	durability := journalDurabilityNone
	if sync {
		durability = journalDurabilitySync
	}

	var ptrs []page.ValuePtr
	var groups []outerLeafRecordGroup
	var records []valuelog.Record
	var outerArena []byte
	routeMode := strings.TrimSpace(db.indexOuterLeafMode) == backenddb.IndexOuterLeafModeV1LeafLogRoute
	if routeMode {
		ptrs, groups, err = db.writeRouteOuterLeafValueRecords(lane, dictID, keys, values, durability)
		if err != nil {
			return nil, err
		}
	} else {
		records, groups, outerArena, err = db.buildOuterLeafValueRecords(keys, values)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			return ops, nil
		}
		startRID := db.nextRID.Add(uint64(len(records))) - uint64(len(records)) + 1
		for i := range records {
			records[i].RID = startRID + uint64(i)
		}
		ptrs, err = db.appendValueLog(lane, dictID, nil, records, durability)
		if err != nil {
			return nil, err
		}
		if len(ptrs) != len(records) {
			putValueLogPtrs(ptrs)
			putValueLogRecordsNoClear(records)
			putOuterLeafArena(outerArena)
			return nil, fmt.Errorf("cachingdb: deferred value-log returned %d ptrs for %d records", len(ptrs), len(records))
		}
	}
	defer func() {
		putValueLogPtrs(ptrs)
		if !routeMode {
			putValueLogRecordsNoClear(records)
			putOuterLeafArena(outerArena)
		}
	}()

	var eligibleByIdx []bool
	if collapseFence {
		eligibleByIdx = make([]bool, len(ops))
		for _, idx := range eligible {
			if idx >= 0 && idx < len(eligibleByIdx) {
				eligibleByIdx[idx] = true
			}
		}
	}

	for i := range groups {
		ptr := ptrs[i]
		if collapseFence {
			if !routeAnchorMode {
				ptr = page.ValuePtrMarkFenceOuterCollapsed(ptr)
			}
		}
		group := groups[i]
		if group.start < 0 || group.end < group.start || group.end > len(eligible) {
			return nil, errors.New("cachingdb: deferred value-log group out of range")
		}
		if collapseFence {
			if group.start < group.end {
				idx := eligible[group.start]
				op := &ops[idx]
				op.ValuePtr = ptr
				op.IsPtr = true
				op.Value = nil
			}
			continue
		}
		for srcPos := group.start; srcPos < group.end; srcPos++ {
			idx := eligible[srcPos]
			op := &ops[idx]
			op.ValuePtr = ptr
			op.IsPtr = true
			op.Value = nil
		}
	}
	if collapseFence && len(eligibleByIdx) > 0 {
		dst := 0
		for i := range ops {
			if eligibleByIdx[i] && !ops[i].IsPtr {
				// Fence mode keeps one key per grouped block.
				continue
			}
			if dst != i {
				ops[dst] = ops[i]
			}
			dst++
		}
		// Clear the discarded tail to avoid retaining large key/value
		// references in pooled entry slices.
		for i := dst; i < len(ops); i++ {
			var zero batch.Entry
			ops[i] = zero
		}
		ops = ops[:dst]
	}
	retainPath := db.currentValueLogPath(lane)
	if retainPath != "" {
		db.markValueLogRetain(retainPath)
	}
	if durability == journalDurabilityNone {
		db.backendReadVlogDirtySeq.Add(1)
	}
	return ops, nil
}

type deferredFenceCollapseState struct {
	lastPtr page.ValuePtr
	havePtr bool
	prevKey []byte
}

type fencePendingMutationKind uint8

const (
	fencePendingMutationNone fencePendingMutationKind = iota
	fencePendingMutationDelete
	fencePendingMutationSetInline
	fencePendingMutationSetPointer
)

type fencePendingMutation struct {
	kind fencePendingMutationKind
	ptr  page.ValuePtr
}

func valuePtrSameRecord(a, b page.ValuePtr) bool {
	a = page.ValuePtrClearFenceOuter(a)
	b = page.ValuePtrClearFenceOuter(b)
	if a.FileID != b.FileID || a.Offset != b.Offset {
		return false
	}
	if page.ValuePtrIsGrouped(a) != page.ValuePtrIsGrouped(b) {
		return false
	}
	if page.ValuePtrRecordLength(a) != page.ValuePtrRecordLength(b) {
		return false
	}
	if page.ValuePtrIsGrouped(a) {
		return page.ValuePtrSubIndex(a) == page.ValuePtrSubIndex(b)
	}
	return true
}

func (m fencePendingMutation) replacesFenceAnchor(oldPtr page.ValuePtr) bool {
	switch m.kind {
	case fencePendingMutationDelete, fencePendingMutationSetInline:
		return true
	case fencePendingMutationSetPointer:
		// Treat pointer rewrites as anchor replacements conservatively.
		// Value-log segments can be recycled, so (file,offset,length,subindex)
		// equality is not a stable cross-checkpoint identity.
		return true
	default:
		return false
	}
}

func (m fencePendingMutation) keepsFencePointer(oldPtr page.ValuePtr) bool {
	return false
}

func fencePendingMutationFromMemEntry(ptr page.ValuePtr, flags byte, found bool) (fencePendingMutation, bool) {
	if !found {
		return fencePendingMutation{}, false
	}
	if flags&node.FlagTombstone != 0 {
		return fencePendingMutation{kind: fencePendingMutationDelete}, true
	}
	if flags&node.FlagPointer != 0 {
		return fencePendingMutation{kind: fencePendingMutationSetPointer, ptr: ptr}, true
	}
	return fencePendingMutation{kind: fencePendingMutationSetInline}, true
}

type fenceMutationLookupFn func(key []byte) (fencePendingMutation, bool)

func fenceMutationLookupForMem(mem memtable.Table) fenceMutationLookupFn {
	if mem == nil {
		return nil
	}
	return func(key []byte) (fencePendingMutation, bool) {
		_, ptr, flags, found := mem.GetEntry(key)
		return fencePendingMutationFromMemEntry(ptr, flags, found)
	}
}

func fenceMutationLookupForUnits(units []flushUnit) fenceMutationLookupFn {
	if len(units) == 0 {
		return nil
	}
	if len(units) == 1 {
		mem := units[0].mem
		if mem == nil {
			return nil
		}
		return func(key []byte) (fencePendingMutation, bool) {
			_, ptr, flags, found := mem.GetEntry(key)
			return fencePendingMutationFromMemEntry(ptr, flags, found)
		}
	}
	const fenceMutationLookupIndexMinEntries = 4096
	totalLen := 0
	for i := range units {
		if units[i].memLen > 0 {
			totalLen += units[i].memLen
		}
	}
	if totalLen >= fenceMutationLookupIndexMinEntries {
		type indexedMutation struct {
			key []byte
			mut fencePendingMutation
		}
		index := make(map[uint64][]indexedMutation, totalLen)
		built := true
		for i := range units {
			mem := units[i].mem
			if mem == nil || units[i].memLen == 0 {
				continue
			}
			it := mem.NewIterator(nil, nil)
			for it.Valid() {
				key := it.UnsafeKey()
				_, ptr, flags := it.UnsafeEntry()
				mut, ok := fencePendingMutationFromMemEntry(ptr, flags, true)
				if ok {
					h := xxhash.Sum64(key)
					bucket := index[h]
					updated := false
					for j := range bucket {
						if bytes.Equal(bucket[j].key, key) {
							bucket[j].mut = mut
							updated = true
							break
						}
					}
					if !updated {
						keyCopy := append([]byte(nil), key...)
						bucket = append(bucket, indexedMutation{key: keyCopy, mut: mut})
					}
					index[h] = bucket
				}
				it.Next()
			}
			if err := it.Error(); err != nil {
				built = false
			}
			if err := it.Close(); err != nil {
				built = false
			}
			if !built {
				break
			}
		}
		if built {
			return func(key []byte) (fencePendingMutation, bool) {
				bucket := index[xxhash.Sum64(key)]
				for i := range bucket {
					if bytes.Equal(bucket[i].key, key) {
						return bucket[i].mut, true
					}
				}
				return fencePendingMutation{}, false
			}
		}
	}
	return func(key []byte) (fencePendingMutation, bool) {
		for i := len(units) - 1; i >= 0; i-- {
			if units[i].mem == nil {
				continue
			}
			_, ptr, flags, found := units[i].mem.GetEntry(key)
			if !found {
				continue
			}
			return fencePendingMutationFromMemEntry(ptr, flags, true)
		}
		return fencePendingMutation{}, false
	}
}

type fenceAnchorPromoter struct {
	db *DB

	lookup fenceMutationLookupFn

	snapshot      *backenddb.Snapshot
	snapshotReady bool

	keysByPtr map[page.ValuePtr][][]byte
	promoted  map[string]page.ValuePtr

	rangeLoaded  bool
	rangeKnown   bool
	backendRange keyRange

	backendEmptyChecked bool
	backendEmpty        bool

	deleteRematerializedBySource map[page.ValuePtr]struct{}

	overlapRewriteBySource map[page.ValuePtr]*fenceSourceRewritePlan
	overlapRewriteOrder    []page.ValuePtr
	rewrittenSourceAlias   map[page.ValuePtr]fenceSourceRewriteAlias
	lastOverlapPlan        *fenceSourceRewritePlan
	lastOverlapSourcePtr   page.ValuePtr

	routeCursorReady    bool
	routeCursor         iterator.UnsafeIterator
	routeCursorCurrent  []byte
	routeCursorPtr      page.ValuePtr
	routeCursorHaveCur  bool
	routeCursorNext     []byte
	routeCursorNextPtr  page.ValuePtr
	routeCursorHaveNext bool
	routeCursorLastKey  []byte
	routeCursorHaveLast bool
}

type fenceSourcePendingSet struct {
	key   []byte
	value []byte
}

type fenceSourceRewritePlan struct {
	sourceKey []byte
	sourcePtr page.ValuePtr
	sets      []fenceSourcePendingSet
	setIndex  map[string]int
	// sourceKeys caches decoded source payload keys for membership checks.
	sourceKeys [][]byte
	// sourceKeyCount tracks the decoded key cardinality from sourcePtr so we can
	// safely detect full-source rewrites (all keys in source block mutated).
	sourceKeyCount int
}

type fenceSourceRewriteAlias struct {
	sourceKey []byte
	sourcePtr page.ValuePtr
}

func newFenceAnchorPromoter(db *DB, lookup fenceMutationLookupFn) *fenceAnchorPromoter {
	if db == nil {
		return nil
	}
	return &fenceAnchorPromoter{
		db:     db,
		lookup: lookup,
	}
}

func (p *fenceAnchorPromoter) close() {
	if p == nil || p.snapshot == nil {
	} else {
		_ = p.snapshot.Close()
		p.snapshot = nil
	}
	if p.routeCursor != nil {
		_ = p.routeCursor.Close()
		p.routeCursor = nil
	}
	p.routeCursorReady = false
	p.routeCursorHaveCur = false
	p.routeCursorHaveNext = false
	p.routeCursorHaveLast = false
}

func (p *fenceAnchorPromoter) snapshotView() *backenddb.Snapshot {
	if p == nil || p.db == nil {
		return nil
	}
	if p.snapshotReady {
		return p.snapshot
	}
	p.snapshotReady = true
	type snapshotProvider interface {
		AcquireSnapshot() *backenddb.Snapshot
	}
	provider, ok := p.db.backend.(snapshotProvider)
	if !ok {
		return nil
	}
	p.snapshot = provider.AcquireSnapshot()
	return p.snapshot
}

func (p *fenceAnchorPromoter) v1LeafLogMode() bool {
	if p == nil || p.db == nil {
		return false
	}
	mode := strings.TrimSpace(p.db.indexOuterLeafMode)
	return mode == backenddb.IndexOuterLeafModeV1LeafLog || mode == backenddb.IndexOuterLeafModeV1LeafLogRoute
}

func (p *fenceAnchorPromoter) v1LeafLogRouteMode() bool {
	if p == nil || p.db == nil {
		return false
	}
	return strings.TrimSpace(p.db.indexOuterLeafMode) == backenddb.IndexOuterLeafModeV1LeafLogRoute
}

func (p *fenceAnchorPromoter) encodeAnchorPointer(basePtr page.ValuePtr) page.ValuePtr {
	if p == nil {
		return basePtr
	}
	if p.v1LeafLogRouteMode() {
		return page.ValuePtrClearFenceOuter(basePtr)
	}
	return page.ValuePtrMarkFenceOuterCollapsed(basePtr)
}

func (p *fenceAnchorPromoter) pointerActsAsAnchor(ptr page.ValuePtr) bool {
	if p == nil {
		return page.ValuePtrIsFenceOuter(ptr)
	}
	if p.v1LeafLogRouteMode() {
		// Route mode stores plain anchor pointers (no fence marker bit).
		return true
	}
	return page.ValuePtrIsFenceOuter(ptr)
}

func (plan *fenceSourceRewritePlan) setValue(key, value []byte) {
	if plan == nil || len(key) == 0 {
		return
	}
	if plan.setIndex == nil {
		plan.setIndex = make(map[string]int, 8)
	}
	keyStr := string(key)
	valCopy := append([]byte(nil), value...)
	if idx, ok := plan.setIndex[keyStr]; ok {
		plan.sets[idx].value = valCopy
		return
	}
	keyCopy := append([]byte(nil), key...)
	plan.setIndex[keyStr] = len(plan.sets)
	plan.sets = append(plan.sets, fenceSourcePendingSet{
		key:   keyCopy,
		value: valCopy,
	})
}

func (plan *fenceSourceRewritePlan) lookupValue(key []byte) ([]byte, bool) {
	if plan == nil || len(key) == 0 || len(plan.sets) == 0 {
		return nil, false
	}
	idx, ok := plan.setIndex[string(key)]
	if !ok || idx < 0 || idx >= len(plan.sets) {
		return nil, false
	}
	return plan.sets[idx].value, true
}

func (plan *fenceSourceRewritePlan) sourceFullyCovered() bool {
	if plan == nil || plan.sourceKeyCount < 0 {
		return false
	}
	return len(plan.sets) >= plan.sourceKeyCount
}

func (plan *fenceSourceRewritePlan) containsKey(key []byte) bool {
	if plan == nil || len(key) == 0 || len(plan.sourceKeys) == 0 {
		return false
	}
	lo, hi := 0, len(plan.sourceKeys)
	for lo < hi {
		mid := lo + (hi-lo)/2
		cmp := bytes.Compare(plan.sourceKeys[mid], key)
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(plan.sourceKeys) && bytes.Equal(plan.sourceKeys[lo], key)
}

func (plan *fenceSourceRewritePlan) withinSourceRange(key []byte) bool {
	if plan == nil || len(key) == 0 || len(plan.sourceKeys) == 0 {
		return false
	}
	first := plan.sourceKeys[0]
	last := plan.sourceKeys[len(plan.sourceKeys)-1]
	return bytes.Compare(key, first) >= 0 && bytes.Compare(key, last) <= 0
}

func (p *fenceAnchorPromoter) clearOverlapRewritePlans() {
	if p == nil {
		return
	}
	p.overlapRewriteBySource = nil
	p.overlapRewriteOrder = p.overlapRewriteOrder[:0]
	p.lastOverlapPlan = nil
	p.lastOverlapSourcePtr = page.ValuePtr{}
}

func (p *fenceAnchorPromoter) routeCursorLoadNext() error {
	if p == nil || p.routeCursor == nil {
		p.routeCursorHaveNext = false
		return nil
	}
	for p.routeCursor.Valid() {
		k := p.routeCursor.UnsafeKey()
		_, ptr, flags := p.routeCursor.UnsafeEntry()
		p.routeCursor.Next()
		if flags&node.FlagPointer == 0 || flags&node.FlagTombstone != 0 {
			continue
		}
		if !p.pointerActsAsAnchor(ptr) {
			continue
		}
		p.routeCursorNext = append(p.routeCursorNext[:0], k...)
		p.routeCursorNextPtr = page.ValuePtrClearFenceOuter(ptr)
		p.routeCursorHaveNext = true
		return nil
	}
	if err := p.routeCursor.Error(); err != nil {
		return err
	}
	p.routeCursorHaveNext = false
	return nil
}

func (p *fenceAnchorPromoter) lookupRouteSourceMonotonic(key []byte) ([]byte, page.ValuePtr, bool, error) {
	if p == nil || !p.v1LeafLogRouteMode() || len(key) == 0 {
		return nil, page.ValuePtr{}, false, nil
	}
	if p.routeCursorHaveLast && bytes.Compare(key, p.routeCursorLastKey) < 0 {
		// Non-monotonic keys: fallback to standard lookup path.
		return nil, page.ValuePtr{}, false, nil
	}
	if !p.routeCursorReady {
		type iteratorProvider interface {
			IteratorWithOptions(start, end []byte, opts backenddb.IteratorOptions) (iterator.UnsafeIterator, error)
		}
		provider, ok := p.db.backend.(iteratorProvider)
		if !ok {
			return nil, page.ValuePtr{}, false, nil
		}
		it, err := provider.IteratorWithOptions(nil, nil, backenddb.IteratorOptions{
			Mode:              backenddb.IteratorModePointerProjection,
			IncludeTombstones: true,
		})
		if err != nil {
			return nil, page.ValuePtr{}, false, err
		}
		p.routeCursor = it
		p.routeCursorReady = true
		if err := p.routeCursorLoadNext(); err != nil {
			return nil, page.ValuePtr{}, false, err
		}
	}
	for p.routeCursorHaveNext && bytes.Compare(p.routeCursorNext, key) <= 0 {
		p.routeCursorCurrent = append(p.routeCursorCurrent[:0], p.routeCursorNext...)
		p.routeCursorPtr = p.routeCursorNextPtr
		p.routeCursorHaveCur = true
		if err := p.routeCursorLoadNext(); err != nil {
			return nil, page.ValuePtr{}, false, err
		}
	}
	p.routeCursorLastKey = append(p.routeCursorLastKey[:0], key...)
	p.routeCursorHaveLast = true
	if !p.routeCursorHaveCur {
		return nil, page.ValuePtr{}, false, nil
	}
	return append([]byte(nil), p.routeCursorCurrent...), p.routeCursorPtr, true, nil
}

func (p *fenceAnchorPromoter) overlapRewriteSourceFullyCovered(sourcePtr page.ValuePtr) bool {
	if p == nil || sourcePtr == (page.ValuePtr{}) {
		return false
	}
	if p.overlapRewriteBySource == nil {
		return false
	}
	plan := p.overlapRewriteBySource[sourcePtr]
	return plan != nil && plan.sourceFullyCovered()
}

func (p *fenceAnchorPromoter) remapRewrittenSource(sourceKey []byte, sourcePtr page.ValuePtr) ([]byte, page.ValuePtr) {
	if p == nil || len(sourceKey) == 0 {
		return sourceKey, sourcePtr
	}
	for hop := 0; hop < 8; hop++ {
		if p.rewrittenSourceAlias == nil {
			break
		}
		alias, ok := p.rewrittenSourceAlias[sourcePtr]
		if !ok || len(alias.sourceKey) == 0 {
			break
		}
		sourceKey = alias.sourceKey
		sourcePtr = alias.sourcePtr
	}
	return sourceKey, sourcePtr
}

func (p *fenceAnchorPromoter) recordRewrittenSourceAlias(oldSourcePtr page.ValuePtr, newSourceKey []byte, newSourcePtr page.ValuePtr) {
	if p == nil || len(newSourceKey) == 0 {
		return
	}
	if p.rewrittenSourceAlias == nil {
		p.rewrittenSourceAlias = make(map[page.ValuePtr]fenceSourceRewriteAlias, 8)
	}
	p.rewrittenSourceAlias[oldSourcePtr] = fenceSourceRewriteAlias{
		sourceKey: append([]byte(nil), newSourceKey...),
		sourcePtr: page.ValuePtrClearFenceOuter(newSourcePtr),
	}
}

func (p *fenceAnchorPromoter) lookupPredecessorFenceAnchor(key []byte) ([]byte, page.ValuePtr, bool, error) {
	if p == nil || p.db == nil || len(key) == 0 {
		return nil, page.ValuePtr{}, false, nil
	}
	type reverseIteratorProvider interface {
		ReverseIteratorWithOptions(start, end []byte, opts backenddb.IteratorOptions) (iterator.UnsafeIterator, error)
	}
	provider, ok := p.db.backend.(reverseIteratorProvider)
	if !ok {
		return nil, page.ValuePtr{}, false, nil
	}
	it, err := provider.ReverseIteratorWithOptions(nil, key, backenddb.IteratorOptions{
		Mode:              backenddb.IteratorModePointerProjection,
		IncludeTombstones: true,
	})
	if err != nil {
		return nil, page.ValuePtr{}, false, err
	}
	if it == nil {
		return nil, page.ValuePtr{}, false, nil
	}
	defer func() { _ = it.Close() }()

	const maxScan = 4096
	scanned := 0
	for it.Valid() {
		if scanned >= maxScan {
			break
		}
		scanned++
		k := it.UnsafeKey()
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer == 0 || flags&node.FlagTombstone != 0 {
			it.Next()
			continue
		}
		if !p.pointerActsAsAnchor(ptr) {
			it.Next()
			continue
		}
		return append([]byte(nil), k...), page.ValuePtrClearFenceOuter(ptr), true, nil
	}
	if err := it.Error(); err != nil {
		return nil, page.ValuePtr{}, false, err
	}
	return nil, page.ValuePtr{}, false, nil
}

func (p *fenceAnchorPromoter) lookupFenceSourceForMutation(key []byte) ([]byte, page.ValuePtr, bool, error) {
	if p == nil || len(key) == 0 {
		return nil, page.ValuePtr{}, false, nil
	}
	snap := p.snapshotView()
	if snap == nil {
		return nil, page.ValuePtr{}, false, nil
	}
	sourceKey, sourcePtr, found, err := snap.LookupFencePointerOrigin(key)
	if err != nil {
		return nil, page.ValuePtr{}, false, err
	}
	if found && len(sourceKey) > 0 {
		sourceKey, sourcePtr = p.remapRewrittenSource(sourceKey, page.ValuePtrClearFenceOuter(sourcePtr))
		return sourceKey, sourcePtr, true, nil
	}
	// In strict route mode, exact persisted pointer rows are themselves anchor
	// directory entries. Use the exact pointer row as rewrite source so updates
	// replace the anchored payload block instead of leaving stale exact rows.
	if exact, err := snap.GetEntryExact(key); err == nil {
		if !p.v1LeafLogRouteMode() {
			return nil, page.ValuePtr{}, false, nil
		}
		if exact.Flags&node.FlagPointer == 0 {
			return nil, page.ValuePtr{}, false, nil
		}
		sourceKey := append([]byte(nil), key...)
		sourcePtr := page.ValuePtrClearFenceOuter(exact.ValuePtr)
		sourceKey, sourcePtr = p.remapRewrittenSource(sourceKey, sourcePtr)
		return sourceKey, sourcePtr, true, nil
	} else if err != nil && !errors.Is(err, tree.ErrKeyNotFound) {
		return nil, page.ValuePtr{}, false, err
	}
	sourceKey, sourcePtr, found, err = p.lookupPredecessorFenceAnchor(key)
	if err != nil || !found || len(sourceKey) == 0 {
		return sourceKey, sourcePtr, found, err
	}
	sourceKey, sourcePtr = p.remapRewrittenSource(sourceKey, sourcePtr)
	return sourceKey, sourcePtr, true, nil
}

func (p *fenceAnchorPromoter) queueV1LeafLogOverlapRewrite(key, value []byte) (queued bool, sourcePtr page.ValuePtr, foundSource bool, err error) {
	if p == nil || !p.v1LeafLogMode() || len(key) == 0 {
		return false, page.ValuePtr{}, false, nil
	}
	// Fast path for sorted/clustered workloads: consecutive keys often belong to
	// the same source payload block. Reuse the last decoded plan directly.
	if p.lastOverlapPlan != nil && p.lastOverlapPlan.containsKey(key) {
		p.lastOverlapPlan.setValue(key, value)
		return true, p.lastOverlapSourcePtr, true, nil
	}
	sourceKey, sourcePtr, found, err := p.lookupRouteSourceMonotonic(key)
	if err != nil {
		return false, page.ValuePtr{}, false, err
	}
	if !found || len(sourceKey) == 0 {
		sourceKey, sourcePtr, found, err = p.lookupFenceSourceForMutation(key)
		if err != nil {
			return false, page.ValuePtr{}, false, err
		}
		if !found || len(sourceKey) == 0 {
			return false, page.ValuePtr{}, false, nil
		}
	}
	sourceKey, sourcePtr = p.remapRewrittenSource(sourceKey, page.ValuePtrClearFenceOuter(sourcePtr))
	if p.overlapRewriteBySource == nil {
		p.overlapRewriteBySource = make(map[page.ValuePtr]*fenceSourceRewritePlan, 8)
	}
	plan, ok := p.overlapRewriteBySource[sourcePtr]
	if !ok {
		plan = &fenceSourceRewritePlan{
			sourceKey: append([]byte(nil), sourceKey...),
			sourcePtr: sourcePtr,
			setIndex:  make(map[string]int, 8),
			// Decode lazily on first enqueue; keep -1 as unknown sentinel.
			sourceKeyCount: -1,
		}
		p.overlapRewriteBySource[sourcePtr] = plan
		p.overlapRewriteOrder = append(p.overlapRewriteOrder, sourcePtr)
	}
	if plan.sourceKeyCount < 0 {
		keys, err := p.decodeFenceKeys(sourcePtr)
		if err != nil {
			return false, sourcePtr, true, err
		}
		plan.sourceKeyCount = len(keys)
		plan.sourceKeys = keys
	}
	// Queue overlap rewrites when key is in-source OR falls inside the source
	// key range (in-range inserts). Keys outside the source range must fall back
	// to direct unresolved publishing to avoid rewriting unrelated blocks.
	if !plan.containsKey(key) && !plan.withinSourceRange(key) {
		return false, sourcePtr, true, nil
	}
	plan.setValue(key, value)
	p.lastOverlapPlan = plan
	p.lastOverlapSourcePtr = sourcePtr
	return true, sourcePtr, true, nil
}

func (p *fenceAnchorPromoter) materializeRewriteBlobRefs(entries []outerleaf.TypedEntry, lane *lane, dictID uint64) error {
	if p == nil || p.db == nil || len(entries) == 0 {
		return nil
	}
	pending := make([]int, 0, len(entries))
	records := getValueLogRecordsCap(len(entries))
	defer putValueLogRecordsNoClear(records)
	for i := range entries {
		if entries[i].Kind != outerleaf.EntryKindBlobRef {
			continue
		}
		if entries[i].BlobPtr != (page.ValuePtr{}) || len(entries[i].Value) == 0 {
			continue
		}
		pending = append(pending, i)
		records = append(records, valuelog.Record{Value: entries[i].Value})
	}
	if len(records) == 0 {
		return nil
	}
	if lane == nil {
		return errors.New("cachingdb: missing lane for overlap rewrite blob refs")
	}
	startRID := p.db.nextRID.Add(uint64(len(records))) - uint64(len(records)) + 1
	for i := range records {
		records[i].RID = startRID + uint64(i)
	}
	ptrs, err := p.db.appendValueLog(lane, dictID, nil, records, journalDurabilityFlush)
	if err != nil {
		return err
	}
	defer putValueLogPtrs(ptrs)
	if len(ptrs) != len(pending) {
		return fmt.Errorf("cachingdb: overlap rewrite blob-ref pointer mismatch expected=%d got=%d", len(pending), len(ptrs))
	}
	for i, pos := range pending {
		entries[pos].BlobPtr = ptrs[i]
		entries[pos].Value = nil
	}
	return nil
}

func (p *fenceAnchorPromoter) rewriteSourceFromPlan(plan *fenceSourceRewritePlan, lane *lane, dictID uint64) ([]byte, []byte, bool, error) {
	if p == nil || p.db == nil || plan == nil || len(plan.sourceKey) == 0 {
		return nil, nil, false, nil
	}
	if plan.sourceFullyCovered() {
		if len(plan.sets) == 0 {
			return nil, nil, true, nil
		}
		out := make([]outerleaf.TypedEntry, 0, len(plan.sets))
		for i := range plan.sets {
			set := plan.sets[i]
			keyCopy := append([]byte(nil), set.key...)
			valCopy := append([]byte(nil), set.value...)
			kind := outerleaf.EntryKindInline
			if len(valCopy) > p.db.valueLogThresholdForKey(keyCopy) {
				kind = outerleaf.EntryKindBlobRef
			}
			out = append(out, outerleaf.TypedEntry{Key: keyCopy, Kind: kind, Value: valCopy})
		}
		sort.Slice(out, func(i, j int) bool {
			return bytes.Compare(out[i].Key, out[j].Key) < 0
		})
		if err := p.materializeRewriteBlobRefs(out, lane, dictID); err != nil {
			return nil, nil, false, err
		}
		totalInlineBytes := 0
		for i := range out {
			if out[i].Kind == outerleaf.EntryKindInline {
				totalInlineBytes += len(out[i].Value)
			}
		}
		enc := getOuterLeafEncoder()
		codec := p.db.selectOuterLeafBlockCodec(totalInlineBytes, len(out))
		payload, err := enc.EncodeTypedEntriesAssumeSorted(nil, out, codec, p.db.outerLeafBlockRestart)
		putOuterLeafEncoder(enc)
		if err != nil {
			return nil, nil, false, err
		}
		return append([]byte(nil), out[0].Key...), payload, false, nil
	}
	if p.db.valueLogReader == nil {
		return nil, nil, false, errors.New("cachingdb: missing value-log reader for overlap rewrite")
	}
	raw, err := p.db.valueLogReader.Read(plan.sourcePtr)
	if err != nil {
		return nil, nil, false, err
	}
	if !outerleaf.HasMagic(raw) {
		return nil, nil, false, fmt.Errorf("cachingdb: overlap rewrite source not outer-leaf payload key=%q ptr=%+v", plan.sourceKey, plan.sourcePtr)
	}
	decoded, err := outerleaf.DecodeBlockLeaseWithVerify(raw, true)
	if err != nil {
		return nil, nil, false, err
	}
	if decoded == nil {
		return nil, nil, false, nil
	}
	defer decoded.Release()

	typed, err := decoded.TypedEntries(nil)
	if err != nil {
		return nil, nil, false, err
	}
	if len(typed) == 0 && len(plan.sets) == 0 {
		return nil, nil, false, nil
	}

	out := make([]outerleaf.TypedEntry, 0, len(typed)+len(plan.sets))
	seen := make(map[string]struct{}, len(typed)+len(plan.sets))
	for i := range typed {
		ent := typed[i]
		keyStr := string(ent.Key)
		seen[keyStr] = struct{}{}
		// Apply queued set mutations directly from the overlap rewrite plan.
		// This keeps set-only rewrites correct even when global mutation lookup
		// is intentionally deferred/lazy for hot-path performance.
		if val, ok := plan.lookupValue(ent.Key); ok {
			keyCopy := append([]byte(nil), ent.Key...)
			valCopy := append([]byte(nil), val...)
			kind := outerleaf.EntryKindInline
			if len(valCopy) > p.db.valueLogThresholdForKey(keyCopy) {
				kind = outerleaf.EntryKindBlobRef
			}
			out = append(out, outerleaf.TypedEntry{Key: keyCopy, Kind: kind, Value: valCopy})
			continue
		}
		if p.lookup != nil {
			if mut, ok := p.lookup(ent.Key); ok {
				switch mut.kind {
				case fencePendingMutationDelete:
					continue
				}
			}
		}

		keyCopy := append([]byte(nil), ent.Key...)
		next := outerleaf.TypedEntry{
			Key:     keyCopy,
			Kind:    ent.Kind,
			BlobPtr: ent.BlobPtr,
		}
		if ent.Kind == outerleaf.EntryKindInline && len(ent.Value) > 0 {
			next.Value = append([]byte(nil), ent.Value...)
		}
		out = append(out, next)
	}

	for i := range plan.sets {
		set := plan.sets[i]
		if _, ok := seen[string(set.key)]; ok {
			continue
		}
		keyCopy := append([]byte(nil), set.key...)
		valCopy := append([]byte(nil), set.value...)
		kind := outerleaf.EntryKindInline
		if len(valCopy) > p.db.valueLogThresholdForKey(keyCopy) {
			kind = outerleaf.EntryKindBlobRef
		}
		out = append(out, outerleaf.TypedEntry{Key: keyCopy, Kind: kind, Value: valCopy})
	}

	if len(out) == 0 {
		return nil, nil, true, nil
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Key, out[j].Key) < 0
	})
	if err := p.materializeRewriteBlobRefs(out, lane, dictID); err != nil {
		return nil, nil, false, err
	}
	totalInlineBytes := 0
	for i := range out {
		if out[i].Kind == outerleaf.EntryKindInline {
			totalInlineBytes += len(out[i].Value)
		}
	}

	enc := getOuterLeafEncoder()
	codec := p.db.selectOuterLeafBlockCodec(totalInlineBytes, len(out))
	payload, err := enc.EncodeTypedEntriesAssumeSorted(nil, out, codec, p.db.outerLeafBlockRestart)
	putOuterLeafEncoder(enc)
	if err != nil {
		return nil, nil, false, err
	}
	return append([]byte(nil), out[0].Key...), payload, false, nil
}

func (p *fenceAnchorPromoter) flushQueuedV1LeafLogOverlapRewrites(emitPointer func(key []byte, ptr page.ValuePtr) error, emitDelete func(key []byte) error) error {
	if p == nil || !p.v1LeafLogMode() {
		return nil
	}
	if len(p.overlapRewriteOrder) == 0 {
		return nil
	}
	if emitPointer == nil || emitDelete == nil {
		return errors.New("cachingdb: overlap rewrite emit callbacks are required")
	}

	order := p.overlapRewriteOrder
	sort.Slice(order, func(i, j int) bool {
		left := p.overlapRewriteBySource[order[i]]
		right := p.overlapRewriteBySource[order[j]]
		if left == nil || right == nil {
			return i < j
		}
		return bytes.Compare(left.sourceKey, right.sourceKey) < 0
	})

	type overlapRewritePending struct {
		plan      *fenceSourceRewritePlan
		anchorKey []byte
	}
	pending := make([]overlapRewritePending, 0, len(order))
	records := make([]valuelog.Record, 0, len(order))
	preferredLane := -1
	if p.db.deferredValueLogNeedsSingleLaneRegroup() {
		preferredLane = 0
	}
	lane, err := p.db.pickLane(false, preferredLane)
	if err != nil {
		return err
	}
	dictID := uint64(0)
	if p.db.dictStore != nil {
		if id, err := p.db.currentDictID(context.Background()); err == nil {
			dictID = id
		}
	}

	for _, sourcePtr := range order {
		plan := p.overlapRewriteBySource[sourcePtr]
		if plan == nil {
			continue
		}
		anchorKey, payload, removedAll, err := p.rewriteSourceFromPlan(plan, lane, dictID)
		if err != nil {
			return err
		}
		if removedAll {
			if err := emitDelete(plan.sourceKey); err != nil {
				return err
			}
			continue
		}
		if len(anchorKey) == 0 {
			continue
		}
		pending = append(pending, overlapRewritePending{plan: plan, anchorKey: anchorKey})
		records = append(records, valuelog.Record{Value: payload})
	}

	if len(records) > 0 {
		startRID := p.db.nextRID.Add(uint64(len(records))) - uint64(len(records)) + 1
		for i := range records {
			records[i].RID = startRID + uint64(i)
		}

		ptrs, err := p.db.appendValueLog(lane, dictID, nil, records, journalDurabilityFlush)
		if err != nil {
			return err
		}
		if len(ptrs) != len(pending) {
			putValueLogPtrs(ptrs)
			return fmt.Errorf("cachingdb: overlap rewrite returned %d pointers for %d records", len(ptrs), len(pending))
		}
		for i := range pending {
			rewrittenPtr := p.encodeAnchorPointer(ptrs[i])
			if err := p.emitUniquePointer(pending[i].anchorKey, rewrittenPtr, emitPointer); err != nil {
				putValueLogPtrs(ptrs)
				return err
			}
			p.recordRewrittenSourceAlias(pending[i].plan.sourcePtr, pending[i].anchorKey, rewrittenPtr)
			if p.v1LeafLogRouteMode() {
				// Route mode is directory-only: purge any stale exact sibling rows
				// from the rewritten source block so exact-key reads cannot shadow
				// newer anchor coverage after overlap rewrites.
				sourceKeys, err := p.decodeFenceKeys(pending[i].plan.sourcePtr)
				if err != nil {
					putValueLogPtrs(ptrs)
					return err
				}
				for _, oldKey := range sourceKeys {
					if len(oldKey) == 0 || bytes.Equal(oldKey, pending[i].anchorKey) {
						continue
					}
					if err := emitDelete(oldKey); err != nil {
						putValueLogPtrs(ptrs)
						return err
					}
				}
			}
			if !bytes.Equal(pending[i].anchorKey, pending[i].plan.sourceKey) {
				if err := emitDelete(pending[i].plan.sourceKey); err != nil {
					putValueLogPtrs(ptrs)
					return err
				}
			}
		}
		putValueLogPtrs(ptrs)
		retainPath := p.db.currentValueLogPath(lane)
		if retainPath != "" {
			p.db.markValueLogRetain(retainPath)
		}
	}
	p.clearOverlapRewritePlans()
	return nil
}

func (p *fenceAnchorPromoter) decodeFenceKeys(ptr page.ValuePtr) ([][]byte, error) {
	if p == nil || p.db == nil {
		return nil, nil
	}
	if p.keysByPtr != nil {
		if keys, ok := p.keysByPtr[ptr]; ok {
			return keys, nil
		}
	}
	if p.db.valueLogReader == nil {
		return nil, errors.New("cachingdb: missing value-log reader for fence-anchor promotion")
	}
	raw, err := p.db.valueLogReader.Read(ptr)
	if err != nil {
		return nil, err
	}
	if !outerleaf.HasMagic(raw) {
		return nil, nil
	}
	keys, err := outerleaf.DecodeKeysWithVerify(raw, true)
	if err != nil {
		return nil, err
	}
	if p.keysByPtr == nil {
		p.keysByPtr = make(map[page.ValuePtr][][]byte)
	}
	p.keysByPtr[ptr] = keys
	return keys, nil
}

func (p *fenceAnchorPromoter) findPromotion(ptr page.ValuePtr, anchorKey []byte) ([]byte, bool, error) {
	keys, err := p.decodeFenceKeys(ptr)
	if err != nil {
		return nil, false, err
	}
	if len(keys) == 0 {
		return nil, false, nil
	}
	anchorIdx := -1
	for i := range keys {
		key := keys[i]
		if bytes.Equal(key, anchorKey) {
			anchorIdx = i
			break
		}
	}
	if anchorIdx < 0 {
		return nil, false, nil
	}
	snap := p.snapshotView()
	for i := anchorIdx + 1; i < len(keys); i++ {
		key := keys[i]
		if p.lookup != nil {
			if pending, ok := p.lookup(key); ok {
				if pending.keepsFencePointer(ptr) {
					return nil, false, nil
				}
				continue
			}
		}
		// Never clobber exact persisted entries. Tree reads prefer exact leaf
		// entries over fence fallback, so promotion must skip keys that already
		// have an exact entry (unless it already keeps the same pointer record).
		if snap != nil {
			exact, err := snap.GetEntryExact(key)
			if err == nil {
				if exact.Flags&node.FlagPointer != 0 && valuePtrSameRecord(exact.ValuePtr, ptr) {
					return nil, false, nil
				}
				continue
			}
			if !errors.Is(err, tree.ErrKeyNotFound) {
				return nil, false, err
			}

			// Candidate promotion keys must currently resolve from the same fence
			// pointer record; otherwise promotion could clobber newer fallback
			// coverage from another anchor.
			resolvedPtr, found, err := snap.LookupFencePointerSource(key)
			if err != nil {
				return nil, false, err
			}
			if !found || !valuePtrSameRecord(resolvedPtr, ptr) {
				continue
			}
		}
		return append([]byte(nil), key...), true, nil
	}
	return nil, false, nil
}

func (p *fenceAnchorPromoter) emitUniquePointer(key []byte, ptr page.ValuePtr, emitPointer func(key []byte, ptr page.ValuePtr) error) error {
	if p == nil || len(key) == 0 || emitPointer == nil {
		return nil
	}
	if p.promoted == nil {
		p.promoted = make(map[string]page.ValuePtr)
	}
	keyStr := string(key)
	if existing, ok := p.promoted[keyStr]; ok && existing == ptr {
		return nil
	}
	if err := emitPointer(key, ptr); err != nil {
		return err
	}
	p.promoted[keyStr] = ptr
	return nil
}

func (p *fenceAnchorPromoter) keyResolvesFromSource(snap *backenddb.Snapshot, key []byte, sourcePtr page.ValuePtr) (exact bool, marked bool, resolved bool, err error) {
	if snap == nil || len(key) == 0 {
		return false, false, false, nil
	}
	entry, err := snap.GetEntryExact(key)
	if err == nil {
		if entry.Flags&node.FlagPointer == 0 || !valuePtrSameRecord(entry.ValuePtr, sourcePtr) {
			return true, false, false, nil
		}
		return true, p.pointerActsAsAnchor(entry.ValuePtr), true, nil
	}
	if !errors.Is(err, tree.ErrKeyNotFound) {
		return false, false, false, err
	}
	resolvedPtr, found, err := snap.LookupFencePointerSource(key)
	if err != nil {
		return false, false, false, err
	}
	if !found || !valuePtrSameRecord(resolvedPtr, sourcePtr) {
		return false, false, false, nil
	}
	return false, false, true, nil
}

func (p *fenceAnchorPromoter) rewriteFallbackSourceBlock(sourceKey, mutationKey []byte, sourcePtr page.ValuePtr) ([]byte, page.ValuePtr, bool, error) {
	if p == nil || p.db == nil || len(sourceKey) == 0 || len(mutationKey) == 0 {
		return nil, page.ValuePtr{}, false, nil
	}
	if p.db.valueLogReader == nil {
		return nil, page.ValuePtr{}, false, errors.New("cachingdb: missing value-log reader for fallback rewrite")
	}
	raw, err := p.db.valueLogReader.Read(sourcePtr)
	if err != nil {
		return nil, page.ValuePtr{}, false, err
	}
	if !outerleaf.HasMagic(raw) {
		// Direct/non-outer payload: keep legacy rematerialization path.
		return nil, page.ValuePtr{}, false, nil
	}
	decoded, err := outerleaf.DecodeBlockLeaseWithVerify(raw, true)
	if err != nil {
		return nil, page.ValuePtr{}, false, err
	}
	if decoded == nil {
		return nil, page.ValuePtr{}, false, nil
	}
	defer decoded.Release()

	typed, err := decoded.TypedEntries(nil)
	if err != nil {
		return nil, page.ValuePtr{}, false, err
	}
	if len(typed) == 0 {
		return nil, page.ValuePtr{}, false, nil
	}

	kept := make([]outerleaf.TypedEntry, 0, len(typed))
	removed := false
	totalInlineBytes := 0
	sourceKept := false
	for i := range typed {
		ent := typed[i]
		if bytes.Equal(ent.Key, mutationKey) {
			removed = true
			continue
		}
		if p.lookup != nil {
			if mut, ok := p.lookup(ent.Key); ok && mut.kind != fencePendingMutationNone {
				// Pending writes/deletes in the current flush batch own this key.
				// Exclude it from the rewritten fallback block to avoid emitting
				// stale overlap rows that can violate iterator parity.
				removed = true
				continue
			}
		}
		keyCopy := append([]byte(nil), ent.Key...)
		next := outerleaf.TypedEntry{
			Key:     keyCopy,
			Kind:    ent.Kind,
			BlobPtr: ent.BlobPtr,
		}
		if bytes.Equal(ent.Key, sourceKey) {
			sourceKept = true
		}
		if ent.Kind == outerleaf.EntryKindInline && len(ent.Value) > 0 {
			next.Value = append([]byte(nil), ent.Value...)
			totalInlineBytes += len(ent.Value)
		}
		kept = append(kept, next)
	}
	if !removed {
		return nil, page.ValuePtr{}, false, nil
	}
	if len(kept) == 0 {
		// Entire block removed; caller should rely on pending delete op.
		return nil, page.ValuePtr{}, true, nil
	}

	emitKey := sourceKey
	if !sourceKept || bytes.Equal(mutationKey, sourceKey) {
		emitKey = kept[0].Key
	}
	if len(emitKey) == 0 {
		return nil, page.ValuePtr{}, false, errors.New("cachingdb: fallback rewrite produced empty anchor key")
	}

	enc := getOuterLeafEncoder()
	codec := p.db.selectOuterLeafBlockCodec(totalInlineBytes, len(kept))
	payload, err := enc.EncodeTypedEntriesAssumeSorted(nil, kept, codec, p.db.outerLeafBlockRestart)
	putOuterLeafEncoder(enc)
	if err != nil {
		return nil, page.ValuePtr{}, false, err
	}

	preferredLane := -1
	if p.db.deferredValueLogNeedsSingleLaneRegroup() {
		preferredLane = 0
	}
	lane, err := p.db.pickLane(false, preferredLane)
	if err != nil {
		return nil, page.ValuePtr{}, false, err
	}
	dictID := uint64(0)
	if p.db.dictStore != nil {
		if id, err := p.db.currentDictID(context.Background()); err == nil {
			dictID = id
		}
	}
	rid := p.db.nextRID.Add(1)
	var recs [1]valuelog.Record
	recs[0] = valuelog.Record{RID: rid, Value: payload}
	ptrs, err := p.db.appendValueLog(lane, dictID, nil, recs[:], journalDurabilityFlush)
	if err != nil {
		return nil, page.ValuePtr{}, false, err
	}
	if len(ptrs) != 1 {
		putValueLogPtrs(ptrs)
		return nil, page.ValuePtr{}, false, fmt.Errorf("cachingdb: fallback rewrite returned %d pointers", len(ptrs))
	}
	basePtr := ptrs[0]
	putValueLogPtrs(ptrs)

	retainPath := p.db.currentValueLogPath(lane)
	if retainPath != "" {
		p.db.markValueLogRetain(retainPath)
	}
	return append([]byte(nil), emitKey...), p.encodeAnchorPointer(basePtr), true, nil
}

type fenceProjectionIteratorProvider interface {
	IteratorWithOptions(start, end []byte, opts backenddb.IteratorOptions) (iterator.UnsafeIterator, error)
}

// rematerializeFallbackSingleCandidate is a bounded fast path for
// v1_leaflog's single-predecessor fence lookup semantics. It avoids per-key
// LookupFencePointerSource calls by walking a bounded pointer-projection range.
func (p *fenceAnchorPromoter) rematerializeFallbackSingleCandidate(sourceKey, mutationKey []byte, sourcePtr page.ValuePtr, keys [][]byte, emitPointer func(key []byte, ptr page.ValuePtr) error) (handled bool, err error) {
	if p == nil || p.db == nil || len(keys) == 0 || emitPointer == nil {
		return false, nil
	}
	mode := strings.TrimSpace(p.db.indexOuterLeafMode)
	if mode != backenddb.IndexOuterLeafModeV1LeafLog && mode != backenddb.IndexOuterLeafModeV1LeafLogRoute {
		return false, nil
	}
	provider, ok := p.db.backend.(fenceProjectionIteratorProvider)
	if !ok {
		return false, nil
	}
	sourceIdx := -1
	for i := range keys {
		if bytes.Equal(keys[i], sourceKey) {
			sourceIdx = i
			break
		}
	}
	// Fast path is intentionally conservative: only run when sourceKey is the
	// leading key in the decoded block.
	if sourceIdx != 0 {
		return false, nil
	}
	handled = true
	opts := backenddb.IteratorOptions{
		Mode:              backenddb.IteratorModePointerProjection,
		IncludeTombstones: true,
	}
	lastKey := keys[len(keys)-1]
	if len(lastKey) == 0 {
		return false, nil
	}
	// Bound iterator work to keys <= lastKey.
	end := make([]byte, 0, len(lastKey)+1)
	end = append(end, lastKey...)
	end = append(end, 0)

	it, err := provider.IteratorWithOptions(keys[0], end, opts)
	if err != nil {
		return handled, err
	}
	if it == nil {
		return false, nil
	}
	defer func() {
		if closeErr := it.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	var (
		haveCurr  bool
		currKey   []byte
		currPtr   page.ValuePtr
		currFlags byte
	)
	loadCurr := func() error {
		if !it.Valid() {
			haveCurr = false
			currKey = nil
			currPtr = page.ValuePtr{}
			currFlags = 0
			return nil
		}
		currKey = it.UnsafeKey()
		_, currPtr, currFlags = it.UnsafeEntry()
		if err := it.Error(); err != nil {
			return err
		}
		haveCurr = true
		return nil
	}
	if err := loadCurr(); err != nil {
		return handled, err
	}

	exactPtr := page.ValuePtrClearFenceOuter(sourcePtr)
	var (
		havePrev  bool
		prevPtr   page.ValuePtr
		prevFlags byte
	)
	for i := range keys {
		k := keys[i]
		if len(k) == 0 {
			continue
		}
		if p.lookup != nil {
			if _, ok := p.lookup(k); ok {
				continue
			}
		}
		if bytes.Equal(k, mutationKey) {
			continue
		}
		for haveCurr && bytes.Compare(currKey, k) < 0 {
			prevPtr = currPtr
			prevFlags = currFlags
			havePrev = true
			it.Next()
			if err := loadCurr(); err != nil {
				return handled, err
			}
		}
		exact := false
		marked := false
		resolved := false
		if haveCurr && bytes.Equal(currKey, k) {
			exact = true
			if currFlags&node.FlagPointer != 0 && valuePtrSameRecord(currPtr, sourcePtr) {
				resolved = true
				marked = p.pointerActsAsAnchor(currPtr)
			}
			prevPtr = currPtr
			prevFlags = currFlags
			havePrev = true
			it.Next()
			if err := loadCurr(); err != nil {
				return handled, err
			}
		} else if havePrev &&
			prevFlags&node.FlagPointer != 0 &&
			prevFlags&node.FlagTombstone == 0 &&
			p.pointerActsAsAnchor(prevPtr) &&
			valuePtrSameRecord(prevPtr, sourcePtr) {
			resolved = true
		}
		if !resolved {
			continue
		}
		if exact && !marked {
			continue
		}
		if err := p.emitUniquePointer(k, exactPtr, emitPointer); err != nil {
			return handled, err
		}
	}
	if err := it.Error(); err != nil {
		return handled, err
	}
	return handled, nil
}

func (p *fenceAnchorPromoter) maybeRematerializeFallbackMutation(mutationKey []byte, snap *backenddb.Snapshot, emitPointer func(key []byte, ptr page.ValuePtr) error, emitDelete func(key []byte) error, allowExactFallback bool) error {
	if p == nil || snap == nil || len(mutationKey) == 0 || emitPointer == nil || emitDelete == nil {
		return nil
	}
	if p.v1LeafLogMode() {
		p.db.routeLeaflogFallbackDirectAttempts.Add(1)
	}
	sourceKey, sourcePtr, found, err := p.lookupFenceSourceForMutation(mutationKey)
	if err != nil || !found || len(sourceKey) == 0 {
		return err
	}
	sourcePtr = page.ValuePtrClearFenceOuter(sourcePtr)
	if p.deleteRematerializedBySource != nil {
		if _, ok := p.deleteRematerializedBySource[sourcePtr]; ok {
			return nil
		}
	}
	keys, err := p.decodeFenceKeys(sourcePtr)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	mutationIdx := -1
	for i := range keys {
		if bytes.Equal(keys[i], mutationKey) {
			mutationIdx = i
			break
		}
	}
	if mutationIdx < 0 {
		return nil
	}
	mode := strings.TrimSpace(p.db.indexOuterLeafMode)
	if mode == backenddb.IndexOuterLeafModeV1LeafLog || mode == backenddb.IndexOuterLeafModeV1LeafLogRoute {
		hasNonDeleteMutation := false
		if p.lookup != nil {
			for i := range keys {
				mut, ok := p.lookup(keys[i])
				if !ok {
					continue
				}
				if mut.kind != fencePendingMutationDelete && mut.kind != fencePendingMutationNone {
					hasNonDeleteMutation = true
					break
				}
			}
		}
		if hasNonDeleteMutation {
			// Mixed delete+set overlap is normally rewritten by the queued
			// source-block COW path during deferred pointer group publish.
			// Only skip direct rewrite when a rewrite plan for this source has
			// actually been queued; otherwise we can drop the source anchor without
			// publishing a replacement block.
			if p.overlapRewriteBySource != nil {
				if _, queued := p.overlapRewriteBySource[sourcePtr]; queued {
					return nil
				}
			}
		}
		emitKey, rewrittenPtr, rewritten, err := p.rewriteFallbackSourceBlock(sourceKey, mutationKey, sourcePtr)
		if err != nil {
			return err
		}
		if rewritten {
			if p.v1LeafLogMode() {
				p.db.routeLeaflogFallbackDirectRewrites.Add(1)
			}
			if len(emitKey) > 0 {
				if err := p.emitUniquePointer(emitKey, rewrittenPtr, emitPointer); err != nil {
					return err
				}
			}
			if len(sourceKey) > 0 && (len(emitKey) == 0 || !bytes.Equal(emitKey, sourceKey)) {
				if err := emitDelete(sourceKey); err != nil {
					return err
				}
			}
			if p.deleteRematerializedBySource == nil {
				p.deleteRematerializedBySource = make(map[page.ValuePtr]struct{}, 8)
			}
			p.deleteRematerializedBySource[sourcePtr] = struct{}{}
			return nil
		}
		if mode == backenddb.IndexOuterLeafModeV1LeafLogRoute {
			// Route mode must never persist per-key exact fallback rows.
			// When block rewrite cannot proceed, fail instead of rematerializing.
			return fmt.Errorf("cachingdb: v1_leaflog_route requires fallback block rewrite for key %q", mutationKey)
		}
		if !allowExactFallback {
			return fmt.Errorf("cachingdb: v1_leaflog requires fallback block rewrite for key %q", mutationKey)
		}
	}
	if handled, err := p.rematerializeFallbackSingleCandidate(sourceKey, mutationKey, sourcePtr, keys, emitPointer); err != nil {
		return err
	} else if handled {
		if p.deleteRematerializedBySource == nil {
			p.deleteRematerializedBySource = make(map[page.ValuePtr]struct{}, 8)
		}
		p.deleteRematerializedBySource[sourcePtr] = struct{}{}
		return nil
	}
	exactPtr := page.ValuePtrClearFenceOuter(sourcePtr)
	for i := range keys {
		k := keys[i]
		if len(k) == 0 {
			continue
		}
		if p.lookup != nil {
			if _, ok := p.lookup(k); ok {
				continue
			}
		}
		if bytes.Equal(k, mutationKey) {
			continue
		}
		exact, marked, resolved, err := p.keyResolvesFromSource(snap, k, sourcePtr)
		if err != nil {
			return err
		}
		if !resolved {
			continue
		}
		// After deleting a fallback-only key, rewrite surviving keys that still
		// resolve from this source to exact pointers. This removes overlapping
		// fallback expansion and preserves one-row-per-logical-key iteration.
		if exact && !marked {
			continue
		}
		if err := p.emitUniquePointer(k, exactPtr, emitPointer); err != nil {
			return err
		}
	}
	if p.deleteRematerializedBySource == nil {
		p.deleteRematerializedBySource = make(map[page.ValuePtr]struct{}, 8)
	}
	p.deleteRematerializedBySource[sourcePtr] = struct{}{}
	return nil
}

func (p *fenceAnchorPromoter) maybePromoteAnchor(key []byte, pending fencePendingMutation, emitPointer func(key []byte, ptr page.ValuePtr) error, emitDelete func(key []byte) error) error {
	if p == nil || p.db == nil || len(key) == 0 || emitPointer == nil || emitDelete == nil {
		return nil
	}
	if pending.kind == fencePendingMutationNone {
		return nil
	}
	if !p.keyCouldExistInBackend(key) {
		return nil
	}
	snap := p.snapshotView()
	if snap == nil {
		return nil
	}
	entry, err := snap.GetEntryExact(key)
	if err != nil {
		if errors.Is(err, tree.ErrKeyNotFound) {
			if pending.kind == fencePendingMutationDelete {
				mode := strings.TrimSpace(p.db.indexOuterLeafMode)
				allowExactFallback := mode != backenddb.IndexOuterLeafModeV1LeafLog && mode != backenddb.IndexOuterLeafModeV1LeafLogRoute
				return p.maybeRematerializeFallbackMutation(key, snap, emitPointer, emitDelete, allowExactFallback)
			}
			return nil
		}
		return err
	}
	if entry.Flags&node.FlagPointer == 0 {
		return nil
	}
	if !pending.replacesFenceAnchor(entry.ValuePtr) {
		return nil
	}
	promoteKey, shouldEmit, err := p.findPromotion(entry.ValuePtr, key)
	if err != nil {
		return err
	}
	if !shouldEmit || len(promoteKey) == 0 {
		return nil
	}
	return p.emitUniquePointer(promoteKey, entry.ValuePtr, emitPointer)
}

func (p *fenceAnchorPromoter) loadBackendRange() {
	if p == nil || p.rangeLoaded || p.db == nil {
		return
	}
	p.rangeLoaded = true
	_ = p.db.ensureBackendRange()
	p.db.mu.RLock()
	p.rangeKnown = p.db.backendRangeKnown
	p.backendRange = p.db.backendRange
	p.db.mu.RUnlock()
}

func (p *fenceAnchorPromoter) keyCouldExistInBackend(key []byte) bool {
	if p == nil || len(key) == 0 {
		return false
	}
	p.loadBackendRange()
	if !p.rangeKnown {
		return true
	}
	if !p.backendRange.valid {
		return !p.backendDefinitelyEmpty()
	}
	return bytes.Compare(key, p.backendRange.min) >= 0 && bytes.Compare(key, p.backendRange.max) <= 0
}

func (p *fenceAnchorPromoter) backendDefinitelyEmpty() bool {
	if p == nil {
		return false
	}
	if p.backendEmptyChecked {
		return p.backendEmpty
	}
	p.backendEmptyChecked = true
	if p.db == nil || p.db.backend == nil {
		p.backendEmpty = false
		return false
	}
	iter, err := p.db.backend.Iterator(nil, nil)
	if err != nil || iter == nil {
		p.backendEmpty = false
		return false
	}
	empty := !iter.Valid()
	_ = iter.Close()
	p.backendEmpty = empty
	return empty
}

func reserveBackendBatchOps(backendBatch batch.Interface, n int) {
	if backendBatch == nil || n <= 0 {
		return
	}
	type reserveHint interface {
		Reserve(int)
	}
	if r, ok := backendBatch.(reserveHint); ok {
		r.Reserve(n)
	}
}

func (db *DB) flushDeferredValueLogMemtable(
	iter iterator.UnsafeIterator,
	backendBatch batch.Interface,
	memLen int,
	sync bool,
	laneID int,
	mutationLookup fenceMutationLookupFn,
	fenceState *deferredFenceCollapseState,
) (int, error) {
	if db == nil {
		return 0, nil
	}
	if iter == nil {
		return 0, nil
	}
	if backendBatch == nil {
		return 0, errors.New("cachingdb: missing backend batch")
	}
	allowPointers := db.allowValueLogPointers()

	type (
		setViewer interface {
			SetView(key, value []byte) error
		}
		deleteViewer interface {
			DeleteView(key []byte) error
		}
		ptrSetter interface {
			SetPointer(key []byte, ptr page.ValuePtr) error
		}
		ptrSetterView interface {
			SetPointerView(key []byte, ptr page.ValuePtr) error
		}
	)
	sv, _ := backendBatch.(setViewer)
	dv, _ := backendBatch.(deleteViewer)
	psv, _ := backendBatch.(ptrSetterView)
	ps, _ := backendBatch.(ptrSetter)
	reserveHint := memLen
	if db != nil && db.flushBackendMaxEntries > 0 && reserveHint > db.flushBackendMaxEntries {
		reserveHint = db.flushBackendMaxEntries
	}
	reserveBackendBatchOps(backendBatch, reserveHint)

	var ptrKeys [][]byte
	var ptrVals [][]byte
	defer func() {
		putValueLogKeys(ptrKeys)
		putValueLogKeys(ptrVals)
	}()
	fenceMode := db.outerLeafAnchorModeEnabled()
	collapseFence := fenceMode && db.allowFencePointerCollapse()
	routeAnchorMode := strings.TrimSpace(db.indexOuterLeafMode) == backenddb.IndexOuterLeafModeV1LeafLogRoute
	state := fenceState
	if state == nil {
		state = &deferredFenceCollapseState{}
	}
	var promoter *fenceAnchorPromoter
	if collapseFence {
		promoter = newFenceAnchorPromoter(db, mutationLookup)
		defer promoter.close()
	}
	if collapseFence && routeAnchorMode && promoter == nil {
		return 0, errors.New("cachingdb: v1_leaflog_route requires anchor promoter for strict directory publish")
	}
	emittedOps := 0
	emitPromotionPointer := func(key []byte, ptr page.ValuePtr) error {
		if psv != nil {
			if err := psv.SetPointerView(key, ptr); err != nil {
				return err
			}
			emittedOps++
			return nil
		}
		if ps != nil {
			if err := ps.SetPointer(key, ptr); err != nil {
				return err
			}
			emittedOps++
			return nil
		}
		return errors.New("cachingdb: backend batch missing SetPointer")
	}
	emitPromotionDelete := func(key []byte) error {
		if dv != nil {
			if err := dv.DeleteView(key); err != nil {
				return err
			}
			emittedOps++
			return nil
		}
		if err := backendBatch.Delete(key); err != nil {
			return err
		}
		emittedOps++
		return nil
	}
	vlogLane := (*lane)(nil)
	var (
		dictID      uint64
		dictIDReady bool
	)
	durability := journalDurabilityNone
	if sync {
		durability = journalDurabilitySync
	}
	ensureVlogLane := func() error {
		if vlogLane != nil {
			return nil
		}
		l, err := db.pickLane(false, laneID)
		if err != nil {
			return err
		}
		vlogLane = l
		if !dictIDReady {
			if db.dictStore != nil {
				if id, err := db.currentDictID(context.Background()); err == nil {
					dictID = id
				}
			}
			dictIDReady = true
		}
		return nil
	}

	for iter.Valid() {
		key := iter.UnsafeKey()
		if collapseFence && len(state.prevKey) > 0 && bytes.Compare(key, state.prevKey) <= 0 {
			// Fence collapse is only valid on ascending key runs.
			state.havePtr = false
		}
		if iter.IsDeleted() {
			if promoter != nil {
				if err := promoter.maybePromoteAnchor(key, fencePendingMutation{kind: fencePendingMutationDelete}, emitPromotionPointer, emitPromotionDelete); err != nil {
					return 0, err
				}
			}
			var err error
			if dv != nil {
				err = dv.DeleteView(key)
			} else {
				err = backendBatch.Delete(key)
			}
			if err != nil {
				return 0, err
			}
			emittedOps++
			state.havePtr = false
			state.prevKey = append(state.prevKey[:0], key...)
			iter.Next()
			continue
		}

		val, ptr, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			if promoter != nil {
				if err := promoter.maybePromoteAnchor(key, fencePendingMutation{kind: fencePendingMutationSetPointer, ptr: ptr}, emitPromotionPointer, emitPromotionDelete); err != nil {
					return 0, err
				}
			}
			emitPtr := true
			if collapseFence {
				if state.havePtr && valuePtrSameRecord(ptr, state.lastPtr) {
					emitPtr = false
				} else {
					state.lastPtr = ptr
					state.havePtr = true
				}
			}
			if emitPtr {
				if psv != nil {
					if err := psv.SetPointerView(key, ptr); err != nil {
						return 0, err
					}
				} else if ps != nil {
					if err := ps.SetPointer(key, ptr); err != nil {
						return 0, err
					}
				} else {
					return 0, errors.New("cachingdb: backend batch missing SetPointer")
				}
				emittedOps++
			}
			state.prevKey = append(state.prevKey[:0], key...)
			iter.Next()
			continue
		}
		if val == nil && db.memtableValueLogPointers {
			return 0, errors.New("cachingdb: flush missing value-log ptr for key")
		}
		if promoter != nil {
			if err := promoter.maybePromoteAnchor(key, fencePendingMutation{kind: fencePendingMutationSetInline}, emitPromotionPointer, emitPromotionDelete); err != nil {
				return 0, err
			}
		}

		if allowPointers && db.shouldWriteViaValueLogForKeyValue(key, val) {
			if ptrKeys == nil {
				hint := memLen
				if hint <= 0 {
					hint = 16
				}
				if hint > db.flushBackendInitEntries {
					hint = db.flushBackendInitEntries
				}
				ptrKeys = getValueLogKeys(hint)
				ptrVals = getValueLogKeys(hint)
			}
			keyCopy := append([]byte(nil), key...)
			valCopy := append([]byte(nil), val...)
			ptrKeys = append(ptrKeys, keyCopy)
			ptrVals = append(ptrVals, valCopy)
		} else {
			var err error
			if sv != nil {
				err = sv.SetView(key, val)
			} else {
				err = backendBatch.Set(key, val)
			}
			if err != nil {
				return 0, err
			}
			emittedOps++
		}
		state.prevKey = append(state.prevKey[:0], key...)
		iter.Next()
	}

	if len(ptrKeys) == 0 {
		return emittedOps, nil
	}
	if !allowPointers {
		return emittedOps, nil
	}
	if len(ptrKeys) != len(ptrVals) {
		return 0, errors.New("cachingdb: internal deferred value-log mismatch")
	}
	candidateBytes := sumKeyValueBytes(ptrKeys, ptrVals)
	if err := ensureVlogLane(); err != nil {
		return 0, err
	}
	var (
		ptrs       []page.ValuePtr
		records    []valuelog.Record
		groups     []outerLeafRecordGroup
		outerArena []byte
		err        error
	)
	routeMode := routeAnchorMode
	if routeMode {
		ptrs, groups, err = db.writeRouteOuterLeafValueRecords(vlogLane, dictID, ptrKeys, ptrVals, durability)
	} else {
		records, groups, outerArena, err = db.buildOuterLeafValueRecords(ptrKeys, ptrVals)
	}
	if err != nil {
		return 0, err
	}
	if routeMode && len(ptrs) == 0 {
		if len(groups) > 0 {
			return 0, errors.New("cachingdb: v1_leaflog_route emitted empty route record set")
		}
		return emittedOps, nil
	}
	if !routeMode && len(records) == 0 {
		putValueLogRecordsNoClear(records)
		putOuterLeafArena(outerArena)
		return emittedOps, nil
	}
	if !routeMode {
		defer putValueLogRecordsNoClear(records)
		defer putOuterLeafArena(outerArena)

		startRID := db.nextRID.Add(uint64(len(records))) - uint64(len(records)) + 1
		for i := range records {
			records[i].RID = startRID + uint64(i)
		}

		vlogPtrs, err := db.appendValueLog(vlogLane, dictID, nil, records, durability)
		if err != nil {
			return 0, err
		}
		if len(vlogPtrs) != len(records) {
			putValueLogPtrs(vlogPtrs)
			return 0, fmt.Errorf("cachingdb: deferred value-log returned %d ptrs for %d records", len(vlogPtrs), len(records))
		}
		ptrs = vlogPtrs
	}
	if len(ptrs) == 0 {
		return emittedOps, nil
	}
	defer putValueLogPtrs(ptrs)
	if fenceMode {
		db.recordDeferredFenceMaterialized(len(ptrKeys), candidateBytes)
	}

	emitQueuedPointer := func(key []byte, ptr page.ValuePtr) error {
		if psv != nil {
			if err := psv.SetPointerView(key, ptr); err != nil {
				return err
			}
			emittedOps++
			return nil
		}
		if ps != nil {
			if err := ps.SetPointer(key, ptr); err != nil {
				return err
			}
			emittedOps++
			return nil
		}
		return errors.New("cachingdb: backend batch missing SetPointer")
	}
	emitQueuedDelete := func(key []byte) error {
		if dv != nil {
			if err := dv.DeleteView(key); err != nil {
				return err
			}
			emittedOps++
			return nil
		}
		if err := backendBatch.Delete(key); err != nil {
			return err
		}
		emittedOps++
		return nil
	}

	emittedGroups := uint64(0)
	for i := range groups {
		basePtr := ptrs[i]
		ptr := basePtr
		exactPtr := page.ValuePtrClearFenceOuter(basePtr)
		if collapseFence {
			if !routeAnchorMode {
				ptr = page.ValuePtrMarkFenceOuterCollapsed(basePtr)
			}
		}
		group := groups[i]
		emitWholeGroup := false
		if group.start < 0 || group.end < group.start || group.end > len(ptrKeys) {
			return 0, errors.New("cachingdb: deferred value-log group out of range")
		}
		if collapseFence {
			if group.start >= group.end {
				continue
			}
			emittedGroups++
			overlap := false
			if promoter != nil {
				for srcPos := group.start; srcPos < group.end; srcPos++ {
					if promoter.keyCouldExistInBackend(ptrKeys[srcPos]) {
						overlap = true
						break
					}
				}
			}
			if overlap && promoter != nil && promoter.v1LeafLogMode() {
				emitKey := func(key []byte, emitPtr page.ValuePtr) error {
					if psv != nil {
						if err := psv.SetPointerView(key, emitPtr); err != nil {
							return err
						}
					} else if ps != nil {
						if err := ps.SetPointer(key, emitPtr); err != nil {
							return err
						}
					} else {
						return errors.New("cachingdb: backend batch missing SetPointer")
					}
					emittedOps++
					return nil
				}

				unresolved := getValueLogEligible(group.end - group.start)
				queuedCount := 0
				for srcPos := group.start; srcPos < group.end; srcPos++ {
					queued, _, _, err := promoter.queueV1LeafLogOverlapRewrite(ptrKeys[srcPos], ptrVals[srcPos])
					if err != nil {
						putValueLogEligible(unresolved)
						return 0, err
					}
					if queued {
						queuedCount++
						continue
					}
					unresolved = append(unresolved, srcPos)
				}
				if queuedCount > 0 {
					if routeAnchorMode {
						if len(unresolved) > 0 {
							// Route mode keeps overlap handling in rewrite anchors
							// only; emit one canonical anchor row at payload min.
							// unresolved is built in-group key order, so the first
							// unresolved position is the payload-min anchor key.
							anchorPos := unresolved[0]
							if err := emitKey(ptrKeys[anchorPos], ptr); err != nil {
								putValueLogEligible(unresolved)
								return 0, err
							}
							for _, srcPos := range unresolved[1:] {
								if err := emitPromotionDelete(ptrKeys[srcPos]); err != nil {
									putValueLogEligible(unresolved)
									return 0, err
								}
							}
						}
					} else {
						for _, srcPos := range unresolved {
							if err := emitKey(ptrKeys[srcPos], exactPtr); err != nil {
								putValueLogEligible(unresolved)
								return 0, err
							}
						}
					}
					putValueLogEligible(unresolved)
					continue
				}
				if len(unresolved) > 0 {
					emitWholeGroup = true
				} else {
					putValueLogEligible(unresolved)
					continue
				}
				putValueLogEligible(unresolved)
				if !emitWholeGroup {
					if routeAnchorMode && group.end-group.start > 1 {
						for srcPos := group.start + 1; srcPos < group.end; srcPos++ {
							if err := emitPromotionDelete(ptrKeys[srcPos]); err != nil {
								return 0, err
							}
						}
					}
					group.end = group.start + 1
				}
			} else if overlap {
				if routeAnchorMode {
					return 0, errors.New("cachingdb: v1_leaflog_route overlap requires promoter rewrite path")
				}
				emitWholeGroup = true
			}
		}
		// collapseFence block ends here
		for srcPos := group.start; srcPos < group.end; srcPos++ {
			key := ptrKeys[srcPos]
			if collapseFence && emitWholeGroup && routeAnchorMode {
				// In route mode (strict directory contract), emit only one
				// anchor per new outer-leaf block; do not emit exact per-key siblings.
				if srcPos != group.start {
					if err := emitPromotionDelete(key); err != nil {
						return 0, err
					}
					continue
				}
			}
			outPtr := ptr
			if collapseFence && emitWholeGroup && !routeAnchorMode {
				// Legacy v1_leaflog keeps collapsed exact siblings during overlap rewrites.
				outPtr = exactPtr
			}
			if psv != nil {
				if err := psv.SetPointerView(key, outPtr); err != nil {
					return 0, err
				}
			} else if ps != nil {
				if err := ps.SetPointer(key, outPtr); err != nil {
					return 0, err
				}
			} else {
				return 0, errors.New("cachingdb: backend batch missing SetPointer")
			}
			emittedOps++
		}
	}
	if collapseFence && promoter != nil && promoter.v1LeafLogMode() {
		if err := promoter.flushQueuedV1LeafLogOverlapRewrites(emitQueuedPointer, emitQueuedDelete); err != nil {
			return 0, err
		}
	}
	if collapseFence && emittedGroups > 0 {
		db.deferredFenceEmittedGroups.Add(emittedGroups)
	}

	retainPath := db.currentValueLogPath(vlogLane)
	if retainPath != "" {
		db.markValueLogRetain(retainPath)
	}
	return emittedOps, nil
}

func (db *DB) flushDeferredValueLogUnits(units []flushUnit, backendBatch batch.Interface, sync bool, laneID int) (int, error) {
	if db == nil || len(units) == 0 {
		return 0, nil
	}
	if backendBatch == nil {
		return 0, errors.New("cachingdb: missing backend batch")
	}

	type (
		setViewer interface {
			SetView(key, value []byte) error
		}
		deleteViewer interface {
			DeleteView(key []byte) error
		}
		ptrSetter interface {
			SetPointer(key []byte, ptr page.ValuePtr) error
		}
		ptrSetterView interface {
			SetPointerView(key []byte, ptr page.ValuePtr) error
		}
	)
	sv, _ := backendBatch.(setViewer)
	dv, _ := backendBatch.(deleteViewer)
	psv, _ := backendBatch.(ptrSetterView)
	ps, _ := backendBatch.(ptrSetter)

	chunkCap := db.flushBuildChunkCap
	if chunkCap <= 0 {
		chunkCap = 8192
	}
	const maxDeferredInlineGroupKeys = 32768
	unitRuns := getUnitRuns(len(units))
	defer func() {
		for i := range unitRuns {
			for _, run := range unitRuns[i] {
				putEntrySlice(run)
			}
			putEntryRuns(unitRuns[i])
		}
		putUnitRuns(unitRuns)
	}()
	for i := range units {
		runs, _, err := buildOpRuns(units[i].mem, chunkCap)
		if err != nil {
			return 0, err
		}
		unitRuns[i] = runs
	}

	heap := getOpMergeHeap(len(unitRuns))
	defer func() { putOpMergeHeap(heap) }()
	for i := range unitRuns {
		if len(unitRuns[i]) == 0 {
			continue
		}
		it := newOpRunIter(unitRuns[i])
		if !it.Valid() {
			continue
		}
		priority := len(unitRuns) - 1 - i
		heap = append(heap, opMergeItem{iter: it, priority: priority, key: it.Key()})
	}
	for i := len(heap)/2 - 1; i >= 0; i-- {
		(&heap).down(i, len(heap))
	}

	allowPointers := db.allowValueLogPointers()
	fenceMode := db.outerLeafAnchorModeEnabled()
	collapseFence := fenceMode && db.allowFencePointerCollapse()
	routeAnchorMode := strings.TrimSpace(db.indexOuterLeafMode) == backenddb.IndexOuterLeafModeV1LeafLogRoute
	durability := journalDurabilityNone
	if sync {
		durability = journalDurabilitySync
	}
	ptrCap := estimateUnitRunEntries(unitRuns, chunkCap)
	reserveHint := ptrCap
	if db != nil && db.flushBackendMaxEntries > 0 && reserveHint > db.flushBackendMaxEntries {
		reserveHint = db.flushBackendMaxEntries
	}
	reserveBackendBatchOps(backendBatch, reserveHint)
	if ptrCap > maxDeferredInlineGroupKeys {
		ptrCap = maxDeferredInlineGroupKeys
	}
	ptrKeys := getValueLogKeys(ptrCap)
	ptrVals := getValueLogKeys(ptrCap)
	defer func() {
		putValueLogKeys(ptrKeys)
		putValueLogKeys(ptrVals)
	}()
	var (
		backendPendingOps int
		lastFencePtr      page.ValuePtr
		haveFencePtr      bool
		vlogLane          *lane
		dictID            uint64
		dictIDReady       bool
	)
	var promoter *fenceAnchorPromoter
	if collapseFence {
		lookup := fenceMutationLookupFn(nil)
		// Route-mode set-heavy workloads do not require eager global mutation map
		// construction; defer until a delete is actually observed.
		if !routeAnchorMode {
			lookup = fenceMutationLookupForUnits(units)
		}
		promoter = newFenceAnchorPromoter(db, lookup)
		defer promoter.close()
	}
	if collapseFence && routeAnchorMode && promoter == nil {
		return 0, errors.New("cachingdb: v1_leaflog_route requires anchor promoter for strict directory publish")
	}

	emitPromotionPointer := func(key []byte, ptr page.ValuePtr) error {
		if psv != nil {
			if err := psv.SetPointerView(key, ptr); err != nil {
				return err
			}
		} else if ps != nil {
			if err := ps.SetPointer(key, ptr); err != nil {
				return err
			}
		} else {
			return errors.New("cachingdb: backend batch missing SetPointer")
		}
		backendPendingOps++
		return nil
	}
	emitPromotionDelete := func(key []byte) error {
		var err error
		if dv != nil {
			err = dv.DeleteView(key)
		} else {
			err = backendBatch.Delete(key)
		}
		if err != nil {
			return err
		}
		backendPendingOps++
		return nil
	}

	ensureVlogLane := func() error {
		if vlogLane != nil {
			return nil
		}
		l, err := db.pickLane(false, laneID)
		if err != nil {
			return err
		}
		vlogLane = l
		if !dictIDReady {
			if db.dictStore != nil {
				if id, err := db.currentDictID(context.Background()); err == nil {
					dictID = id
				}
			}
			dictIDReady = true
		}
		return nil
	}

	emitPointer := func(key []byte, ptr page.ValuePtr) error {
		emitPtr := true
		if collapseFence {
			if haveFencePtr && valuePtrSameRecord(ptr, lastFencePtr) {
				emitPtr = false
			} else {
				lastFencePtr = ptr
				haveFencePtr = true
			}
		}
		if !emitPtr {
			return nil
		}
		if psv != nil {
			if err := psv.SetPointerView(key, ptr); err != nil {
				return err
			}
		} else if ps != nil {
			if err := ps.SetPointer(key, ptr); err != nil {
				return err
			}
		} else {
			return errors.New("cachingdb: backend batch missing SetPointer")
		}
		backendPendingOps++
		return nil
	}
	emitPointerForce := func(key []byte, ptr page.ValuePtr) error {
		if psv != nil {
			if err := psv.SetPointerView(key, ptr); err != nil {
				return err
			}
		} else if ps != nil {
			if err := ps.SetPointer(key, ptr); err != nil {
				return err
			}
		} else {
			return errors.New("cachingdb: backend batch missing SetPointer")
		}
		if collapseFence {
			lastFencePtr = ptr
			haveFencePtr = true
		}
		backendPendingOps++
		return nil
	}

	flushInlinePointerGroup := func() error {
		if len(ptrKeys) == 0 {
			return nil
		}
		if !allowPointers {
			ptrKeys = ptrKeys[:0]
			ptrVals = ptrVals[:0]
			return nil
		}
		if len(ptrKeys) != len(ptrVals) {
			return errors.New("cachingdb: deferred inline pointer grouping mismatch")
		}
		totalKeys := len(ptrKeys)
		candidateBytes := sumKeyValueBytes(ptrKeys, ptrVals)
		if err := ensureVlogLane(); err != nil {
			return err
		}
		emittedGroups := uint64(0)
		// Cap keys per inline group to keep regroup allocations bounded while
		// still emitting large enough batches to amortize value-log append costs.
		for chunkStart := 0; chunkStart < totalKeys; chunkStart += maxDeferredInlineGroupKeys {
			chunkEnd := chunkStart + maxDeferredInlineGroupKeys
			if chunkEnd > totalKeys {
				chunkEnd = totalKeys
			}
			chunkKeys := ptrKeys[chunkStart:chunkEnd]
			chunkVals := ptrVals[chunkStart:chunkEnd]

			var (
				ptrs   []page.ValuePtr
				groups []outerLeafRecordGroup
				err    error
			)
			if routeAnchorMode {
				ptrs, groups, err = db.writeRouteOuterLeafValueRecords(vlogLane, dictID, chunkKeys, chunkVals, durability)
				if err != nil {
					return err
				}
				if len(ptrs) == 0 {
					if len(groups) > 0 {
						return errors.New("cachingdb: v1_leaflog_route emitted empty chunk pointer set")
					}
					continue
				}
			} else {
				var records []valuelog.Record
				var outerArena []byte
				records, groups, outerArena, err = db.buildOuterLeafValueRecords(chunkKeys, chunkVals)
				if err != nil {
					return err
				}
				if len(records) == 0 {
					putValueLogRecordsNoClear(records)
					putOuterLeafArena(outerArena)
					continue
				}

				startRID := db.nextRID.Add(uint64(len(records))) - uint64(len(records)) + 1
				for i := range records {
					records[i].RID = startRID + uint64(i)
				}
				var vlogPtrs []page.ValuePtr
				vlogPtrs, err = db.appendValueLog(vlogLane, dictID, nil, records, durability)
				putValueLogRecordsNoClear(records)
				putOuterLeafArena(outerArena)
				if err != nil {
					return err
				}
				if len(vlogPtrs) != len(records) {
					putValueLogPtrs(vlogPtrs)
					return fmt.Errorf("cachingdb: deferred value-log returned %d ptrs for %d records", len(vlogPtrs), len(records))
				}
				ptrs = vlogPtrs
			}

			if len(ptrs) != len(groups) {
				return errors.New("cachingdb: deferred inline pointer group/pointer count mismatch")
			}
			for i := range groups {
				basePtr := ptrs[i]
				ptr := basePtr
				exactPtr := page.ValuePtrClearFenceOuter(basePtr)
				if collapseFence {
					if !routeAnchorMode {
						ptr = page.ValuePtrMarkFenceOuterCollapsed(basePtr)
					}
				}
				group := groups[i]
				emitWholeGroup := false
				if group.start < 0 || group.end < group.start || group.end > len(chunkKeys) {
					putValueLogPtrs(ptrs)
					return errors.New("cachingdb: deferred inline pointer source group out of range")
				}
				group.start += chunkStart
				group.end += chunkStart
				if collapseFence {
					if group.start >= group.end {
						continue
					}
					emittedGroups++
					overlap := false
					if promoter != nil {
						for srcPos := group.start; srcPos < group.end; srcPos++ {
							if promoter.keyCouldExistInBackend(ptrKeys[srcPos]) {
								overlap = true
								break
							}
						}
					}
					if overlap && promoter != nil && promoter.v1LeafLogMode() {
						unresolved := getValueLogEligible(group.end - group.start)
						queuedCount := 0
						for srcPos := group.start; srcPos < group.end; srcPos++ {
							queued, _, _, err := promoter.queueV1LeafLogOverlapRewrite(ptrKeys[srcPos], ptrVals[srcPos])
							if err != nil {
								putValueLogEligible(unresolved)
								putValueLogPtrs(ptrs)
								return err
							}
							if queued {
								queuedCount++
								continue
							}
							unresolved = append(unresolved, srcPos)
						}
						if queuedCount > 0 {
							if routeAnchorMode {
								if len(unresolved) > 0 {
									// Route mode keeps overlap handling in rewrite
									// anchors only; emit one canonical anchor row
									// at payload min. unresolved is built in-group
									// key order, so unresolved[0] is payload-min.
									anchorPos := unresolved[0]
									if err := emitPointer(ptrKeys[anchorPos], ptr); err != nil {
										putValueLogEligible(unresolved)
										putValueLogPtrs(ptrs)
										return err
									}
									for _, srcPos := range unresolved[1:] {
										if err := emitPromotionDelete(ptrKeys[srcPos]); err != nil {
											putValueLogEligible(unresolved)
											putValueLogPtrs(ptrs)
											return err
										}
									}
								}
							} else {
								for _, srcPos := range unresolved {
									key := ptrKeys[srcPos]
									if err := emitPointerForce(key, exactPtr); err != nil {
										putValueLogEligible(unresolved)
										putValueLogPtrs(ptrs)
										return err
									}
								}
							}
							putValueLogEligible(unresolved)
							putValueLogPtrs(ptrs)
							continue
						}
						if len(unresolved) > 0 {
							emitWholeGroup = true
						} else {
							putValueLogEligible(unresolved)
							putValueLogPtrs(ptrs)
							continue
						}
						putValueLogEligible(unresolved)
						if !emitWholeGroup {
							if routeAnchorMode && group.end-group.start > 1 {
								for srcPos := group.start + 1; srcPos < group.end; srcPos++ {
									if err := emitPromotionDelete(ptrKeys[srcPos]); err != nil {
										putValueLogPtrs(ptrs)
										return err
									}
								}
							}
							group.end = group.start + 1
						}
					} else if overlap {
						if routeAnchorMode {
							putValueLogPtrs(ptrs)
							return errors.New("cachingdb: v1_leaflog_route overlap requires promoter rewrite path")
						}
						emitWholeGroup = true
					}
					if !emitWholeGroup {
						if routeAnchorMode && group.end-group.start > 1 {
							for srcPos := group.start + 1; srcPos < group.end; srcPos++ {
								if err := emitPromotionDelete(ptrKeys[srcPos]); err != nil {
									putValueLogPtrs(ptrs)
									return err
								}
							}
						}
						group.end = group.start + 1
					}
				}
				for srcPos := group.start; srcPos < group.end; srcPos++ {
					key := ptrKeys[srcPos]
					if collapseFence && emitWholeGroup && routeAnchorMode {
						// In route mode (strict directory contract), emit only one
						// anchor per new outer-leaf block; do not emit exact per-key siblings.
						if srcPos != group.start {
							if err := emitPromotionDelete(key); err != nil {
								putValueLogPtrs(ptrs)
								return err
							}
							continue
						}
					}
					outPtr := ptr
					if collapseFence && emitWholeGroup && !routeAnchorMode {
						// Legacy v1_leaflog keeps collapsed exact siblings during overlap rewrites.
						outPtr = exactPtr
					}
					var err error
					if collapseFence && emitWholeGroup && !routeAnchorMode {
						// Non-route overlap rewrites intentionally keep exact siblings so
						// lookup still resolves through exact rows when needed.
						err = emitPointerForce(key, outPtr)
					} else {
						err = emitPointer(key, outPtr)
					}
					if err != nil {
						putValueLogPtrs(ptrs)
						return err
					}
				}
			}
			putValueLogPtrs(ptrs)
		}
		if collapseFence && promoter != nil && promoter.v1LeafLogMode() {
			if err := promoter.flushQueuedV1LeafLogOverlapRewrites(emitPromotionPointer, emitPromotionDelete); err != nil {
				return err
			}
		}
		if fenceMode {
			db.recordDeferredFenceMaterialized(totalKeys, candidateBytes)
		}
		if collapseFence && emittedGroups > 0 {
			db.deferredFenceEmittedGroups.Add(emittedGroups)
		}
		ptrKeys = ptrKeys[:0]
		ptrVals = ptrVals[:0]
		return nil
	}

	for len(heap) > 0 {
		top := heap.pop()
		currentKey := top.key

		for len(heap) > 0 {
			next := heap.peek()
			if next != nil && bytes.Equal(next.key, currentKey) {
				shadowed := heap.pop()
				shadowed.iter.Next()
				if shadowed.iter.Valid() {
					shadowed.key = shadowed.iter.Key()
					heap.push(shadowed)
				}
				continue
			}
			break
		}

		entry := top.iter.Entry()
		switch {
		case entry.Type == batch.OpDelete:
			if err := flushInlinePointerGroup(); err != nil {
				return 0, err
			}
			if promoter != nil && promoter.lookup == nil {
				promoter.lookup = fenceMutationLookupForUnits(units)
			}
			if promoter != nil {
				if err := promoter.maybePromoteAnchor(entry.Key, fencePendingMutation{kind: fencePendingMutationDelete}, emitPromotionPointer, emitPromotionDelete); err != nil {
					return 0, err
				}
			}
			var err error
			if dv != nil {
				err = dv.DeleteView(entry.Key)
			} else {
				err = backendBatch.Delete(entry.Key)
			}
			if err != nil {
				return 0, err
			}
			haveFencePtr = false
			backendPendingOps++
		case entry.IsPtr:
			if err := flushInlinePointerGroup(); err != nil {
				return 0, err
			}
			if promoter != nil {
				if err := promoter.maybePromoteAnchor(entry.Key, fencePendingMutation{kind: fencePendingMutationSetPointer, ptr: entry.ValuePtr}, emitPromotionPointer, emitPromotionDelete); err != nil {
					return 0, err
				}
			}
			if err := emitPointer(entry.Key, entry.ValuePtr); err != nil {
				return 0, err
			}
		default:
			if promoter != nil {
				if err := promoter.maybePromoteAnchor(entry.Key, fencePendingMutation{kind: fencePendingMutationSetInline}, emitPromotionPointer, emitPromotionDelete); err != nil {
					return 0, err
				}
			}
			if allowPointers && db.shouldWriteViaValueLogForKeyValue(entry.Key, entry.Value) {
				if len(ptrKeys) >= maxDeferredInlineGroupKeys {
					if err := flushInlinePointerGroup(); err != nil {
						return 0, err
					}
				}
				// top.iter reuses backing buffers across Next(); take stable copies
				// before deferring grouped value-log emission.
				keyCopy := append([]byte(nil), entry.Key...)
				valCopy := append([]byte(nil), entry.Value...)
				ptrKeys = append(ptrKeys, keyCopy)
				ptrVals = append(ptrVals, valCopy)
			} else {
				if err := flushInlinePointerGroup(); err != nil {
					return 0, err
				}
				var err error
				if sv != nil {
					err = sv.SetView(entry.Key, entry.Value)
				} else {
					err = backendBatch.Set(entry.Key, entry.Value)
				}
				if err != nil {
					return 0, err
				}
				haveFencePtr = false
				backendPendingOps++
			}
		}

		top.iter.Next()
		if top.iter.Valid() {
			top.key = top.iter.Key()
			heap.push(top)
		}
	}

	if err := flushInlinePointerGroup(); err != nil {
		return 0, err
	}
	if vlogLane != nil {
		// Do not flush/sync the value-log lane here. flushLaneOnce performs one
		// flush boundary after value-log appends so we avoid redundant syscalls.
		retainPath := db.currentValueLogPath(vlogLane)
		if retainPath != "" {
			db.markValueLogRetain(retainPath)
		}
	}
	return backendPendingOps, nil
}

// SetDictStore installs the dictionary store for current-ID freezing.
func (db *DB) SetDictStore(store DictStore) {
	if db == nil {
		return
	}
	db.dictStore = store
	db.dictCurrentCached.Store(0)
	db.dictCurrentOps.Store(0)
	if store != nil {
		if dictID, err := store.GetCurrent(context.Background()); err == nil {
			db.dictCurrentCached.Store(dictID)
		}
	}
	db.valueLogDictBytesMu.Lock()
	db.valueLogDictBytesID = 0
	db.valueLogDictBytes = nil
	db.valueLogDictBytesMu.Unlock()
	for i := range db.lanes {
		l := &db.lanes[i]
		l.vlogDictBytesMu.Lock()
		l.vlogDictBytes = nil
		l.vlogDictBytesMu.Unlock()
	}
	if db.valueLogReader != nil && store != nil {
		db.valueLogReader.SetDictLookup(func(dictID uint64) ([]byte, error) {
			return store.GetDictBytes(context.Background(), dictID)
		})
	}
	db.ensureValueLogDictTrainer()
}

// SetTemplateStore installs the template store used for template compression.
func (db *DB) SetTemplateStore(store template.Store) {
	if db == nil {
		return
	}
	db.templateStore = store
	if db.valueLogReader != nil && store != nil {
		db.valueLogReader.SetTemplateLookup(func(templateID uint64) ([]byte, error) {
			return db.templateLookup(context.Background(), templateID)
		}, db.valueLogTemplateDecodeOpts)
	}
}

func (db *DB) templateLookup(ctx context.Context, templateID uint64) ([]byte, error) {
	if templateID == 0 {
		return nil, valuelog.ErrMissingTemplate
	}
	if db == nil || db.templateStore == nil {
		return nil, valuelog.ErrMissingTemplate
	}
	defBytes, err := db.templateStore.GetTemplateDef(ctx, templateID)
	if err != nil {
		return nil, err
	}
	return defBytes, nil
}

func (db *DB) currentDictID(ctx context.Context) (uint64, error) {
	if db == nil || db.dictStore == nil {
		return 0, nil
	}
	// Avoid per-write dictdb reads on the hot path; refresh every N uses.
	const refreshEvery = uint64(1 << 16)
	seq := db.dictCurrentOps.Add(1)
	if seq&(refreshEvery-1) != 0 {
		return db.dictCurrentCached.Load(), nil
	}
	dictID, err := db.dictStore.GetCurrent(ctx)
	if err != nil {
		// Fall back to cached value on transient errors (best-effort).
		return db.dictCurrentCached.Load(), nil
	}
	db.dictCurrentCached.Store(dictID)
	return dictID, nil
}

func (db *DB) dictBytes(ctx context.Context, dictID uint64) ([]byte, error) {
	if dictID == 0 {
		return nil, nil
	}
	if db == nil || db.dictStore == nil {
		return nil, valuelog.ErrMissingDict
	}
	db.valueLogDictBytesMu.Lock()
	if db.valueLogDictBytesID == dictID && len(db.valueLogDictBytes) > 0 {
		out := db.valueLogDictBytes
		db.valueLogDictBytesMu.Unlock()
		return out, nil
	}
	db.valueLogDictBytesMu.Unlock()

	out, err := db.dictStore.GetDictBytes(ctx, dictID)
	if err != nil {
		return nil, err
	}
	db.valueLogDictBytesMu.Lock()
	db.valueLogDictBytesID = dictID
	db.valueLogDictBytes = out
	db.valueLogDictBytesMu.Unlock()
	return out, nil
}

func (db *DB) dictBytesForLane(ctx context.Context, l *lane, dictID uint64) ([]byte, error) {
	if dictID == 0 {
		return nil, nil
	}
	if l == nil {
		return db.dictBytes(ctx, dictID)
	}
	l.vlogDictBytesMu.RLock()
	if l.vlogDictBytes != nil {
		if b, ok := l.vlogDictBytes[dictID]; ok && len(b) > 0 {
			l.vlogDictBytesMu.RUnlock()
			return b, nil
		}
	}
	l.vlogDictBytesMu.RUnlock()

	out, err := db.dictBytes(ctx, dictID)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return out, nil
	}
	l.vlogDictBytesMu.Lock()
	if l.vlogDictBytes == nil {
		l.vlogDictBytes = make(map[uint64][]byte)
	}
	if len(l.vlogDictBytes) >= 32 {
		clear(l.vlogDictBytes)
	}
	l.vlogDictBytes[dictID] = out
	l.vlogDictBytesMu.Unlock()
	return out, nil
}

func (db *DB) templateCompressionEnabled() bool {
	return db != nil && db.valueLogTemplateEnabled && db.valueLogTemplateEngine != nil && db.templateStore != nil
}

func (db *DB) valueLogTemplateEncodeRecords(records []valuelog.Record) ([]valuelog.Record, bool) {
	if !db.templateCompressionEnabled() || len(records) == 0 {
		return records, false
	}
	engine := db.valueLogTemplateEngine
	store := db.templateStore
	encoded := records
	used := false
	for i := range records {
		payload, ok := engine.Encode(nil, records[i].Value, store)
		if ok {
			if !used {
				encoded = make([]valuelog.Record, len(records))
				copy(encoded, records)
				used = true
			}
			encoded[i].Value = payload
		}
	}
	return encoded, used
}

func (db *DB) readValueLog(key []byte, ptr page.ValuePtr) ([]byte, error) {
	if db.valueLogReader == nil {
		return nil, errors.New("cachingdb: value-log reader unavailable")
	}
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil, fmt.Errorf("cachingdb: non value-log pointer %#x", ptr.FileID)
	}
	if db.valueLogEnabled() {
		if err := db.flushValueLogForPtr(ptr); err != nil {
			return nil, err
		}
	}
	raw, err := db.valueLogReader.Read(ptr)
	if err != nil {
		return nil, err
	}
	return db.decodeOuterLeafValue(key, raw)
}

func (db *DB) readValueLogAppend(key []byte, ptr page.ValuePtr, dst []byte) ([]byte, error) {
	if db.valueLogReader == nil {
		return nil, errors.New("cachingdb: value-log reader unavailable")
	}
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil, fmt.Errorf("cachingdb: non value-log pointer %#x", ptr.FileID)
	}
	if db.valueLogEnabled() {
		if err := db.flushValueLogForPtr(ptr); err != nil {
			return nil, err
		}
	}
	raw, err := db.valueLogReader.ReadAppend(ptr, dst[:0])
	if err != nil {
		return nil, err
	}
	decoded, err := db.decodeOuterLeafValue(key, raw)
	if err != nil {
		return nil, err
	}
	if len(decoded) == 0 {
		return dst, nil
	}
	oldLen := len(dst)
	dst = append(dst, decoded...)
	if oldLen == 0 {
		return dst, nil
	}
	return dst, nil
}

func (db *DB) flushValueLogForPtr(ptr page.ValuePtr) error {
	if !db.valueLogEnabled() {
		return nil
	}
	laneID, seq := valuelog.DecodeFileID(ptr.FileID)
	if laneID >= uint32(len(db.lanes)) {
		return nil
	}
	l := &db.lanes[laneID]
	currentSeq := db.currentValueLogSeq(l)
	if currentSeq == int(seq) {
		return db.flushValueLogLane(l)
	}
	return nil
}

func (db *DB) flushValueLog(laneIDs ...int) error {
	if !db.valueLogEnabled() {
		return nil
	}
	if len(laneIDs) == 0 {
		for i := range db.lanes {
			if err := db.flushValueLogLane(&db.lanes[i]); err != nil {
				return err
			}
		}
		return nil
	}

	seen := make(map[int]struct{}, len(laneIDs))
	for _, id := range laneIDs {
		if id < 0 || id >= len(db.lanes) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if err := db.flushValueLogLane(&db.lanes[id]); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) flushDeferredValueLogForBackendRead() error {
	if db == nil || !db.deferredValueLogEnabled() || !db.valueLogEnabled() {
		return nil
	}
	dirtySeq := db.backendReadVlogDirtySeq.Load()
	if dirtySeq == 0 || dirtySeq == db.backendReadVlogFlushedSeq.Load() {
		return nil
	}
	if err := db.flushValueLog(); err != nil {
		return err
	}
	db.backendReadVlogFlushedSeq.Store(dirtySeq)
	return nil
}

func (db *DB) syncValueLog(laneIDs ...int) error {
	if !db.valueLogEnabled() {
		return nil
	}
	if len(laneIDs) == 0 {
		for i := range db.lanes {
			if err := db.syncValueLogLane(&db.lanes[i]); err != nil {
				return err
			}
		}
		return nil
	}

	seen := make(map[int]struct{}, len(laneIDs))
	for _, id := range laneIDs {
		if id < 0 || id >= len(db.lanes) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if err := db.syncValueLogLane(&db.lanes[id]); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) flushValueLogLane(l *lane) error {
	if l == nil {
		return errWALUnavailable
	}
	if db.splitValueLogEnabled() {
		waitStart := time.Now()
		l.vlogMu.Lock()
		waited := time.Since(waitStart)
		w := l.vlog
		if w == nil {
			l.vlogMu.Unlock()
			return errWALUnavailable
		}
		// Always take vlogMu first so flush acts as a write barrier for in-flight appends.
		if !l.vlogDirty.Load() {
			l.vlogMu.Unlock()
			return nil
		}
		start := time.Now()
		err := w.Flush()
		if db.testOnVlogFlush != nil {
			db.testOnVlogFlush(int(l.id))
		}
		db.debugVlogTiming("vlog_flush", int(l.id), "vlogMu", waited, time.Since(start))
		if err == nil {
			l.vlogDirty.Store(false)
		}
		l.vlogMu.Unlock()
		return err
	}
	waitStart := time.Now()
	l.walMu.Lock()
	waited := time.Since(waitStart)
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	start := time.Now()
	err := w.Flush()
	if db.testOnVlogFlush != nil {
		db.testOnVlogFlush(int(l.id))
	}
	db.debugVlogTiming("wal_flush", int(l.id), "walMu", waited, time.Since(start))
	l.walMu.Unlock()
	return err
}

func (db *DB) syncValueLogLane(l *lane) error {
	if l == nil {
		return errWALUnavailable
	}
	if db.splitValueLogEnabled() {
		waitStart := time.Now()
		l.vlogMu.Lock()
		waited := time.Since(waitStart)
		w := l.vlog
		if w == nil {
			l.vlogMu.Unlock()
			return errWALUnavailable
		}
		start := time.Now()
		err := w.Sync()
		if db.testOnVlogSync != nil {
			db.testOnVlogSync(int(l.id))
		}
		db.debugVlogTiming("vlog_sync", int(l.id), "vlogMu", waited, time.Since(start))
		if err == nil {
			l.vlogDirty.Store(false)
		}
		l.vlogMu.Unlock()
		return err
	}
	waitStart := time.Now()
	l.walMu.Lock()
	waited := time.Since(waitStart)
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	start := time.Now()
	err := w.Sync()
	if db.testOnVlogSync != nil {
		db.testOnVlogSync(int(l.id))
	}
	db.debugVlogTiming("wal_sync", int(l.id), "walMu", waited, time.Since(start))
	l.walMu.Unlock()
	return err
}

func (db *DB) logSegmentPrefix(laneID int) string {
	return fmt.Sprintf("commit-l%d-", laneID)
}

func (db *DB) markValueLogRetain(path string) {
	if path == "" {
		return
	}
	db.valueLogMu.Lock()
	if db.valueLogRetain == nil {
		db.valueLogRetain = make(map[string]struct{})
	}
	db.valueLogRetain[path] = struct{}{}
	db.valueLogMu.Unlock()
}

func (db *DB) forgetValueLogRetain(path string) {
	if path == "" {
		return
	}
	db.valueLogMu.Lock()
	if db.valueLogRetain != nil {
		delete(db.valueLogRetain, path)
	}
	db.valueLogMu.Unlock()
}

func (db *DB) dropValueLogSegment(path string) {
	if db.valueLogReader == nil || path == "" {
		return
	}
	laneID, seq, valueLog, ok := parseLogSeq(filepath.Base(path))
	if !ok || !valueLog {
		return
	}
	if laneID < 0 {
		return
	}
	id, err := valuelog.EncodeFileID(uint32(laneID), uint32(seq))
	if err != nil {
		return
	}
	_ = db.valueLogReader.RemoveSegment(id)
}

func (db *DB) valueLogRetained(path string) bool {
	if path == "" {
		return false
	}
	db.valueLogMu.Lock()
	defer db.valueLogMu.Unlock()
	_, retained := db.valueLogRetain[path]
	return retained
}

func (db *DB) valueLogRetainedStats() (segments int, bytes int64) {
	stats := db.valueLogRetainedStatsDetailed()
	return stats.SegmentsTotal, stats.BytesTotal
}

type valueLogRetainedGenerationStats struct {
	SegmentsTotal int
	SegmentsHot   int
	SegmentsWarm  int
	SegmentsCold  int
	BytesTotal    int64
	BytesHot      int64
	BytesWarm     int64
	BytesCold     int64
}

func (db *DB) valueLogRetainedStatsDetailed() valueLogRetainedGenerationStats {
	var out valueLogRetainedGenerationStats
	db.valueLogMu.Lock()
	if len(db.valueLogRetain) == 0 {
		db.valueLogMu.Unlock()
		return out
	}
	paths := make([]string, 0, len(db.valueLogRetain))
	for path := range db.valueLogRetain {
		paths = append(paths, path)
	}
	db.valueLogMu.Unlock()

	pathSizes := make(map[string]int64)
	currentSizes := make(map[string]int64)
	pathClasses := make(map[string]uint8)
	if db.splitValueLogEnabled() {
		for i := range db.lanes {
			l := &db.lanes[i]
			l.vlogMu.Lock()
			for path, size := range l.vlogClosedSizes {
				pathSizes[path] = size
				pathClasses[path] = l.vlogGenerationClass
			}
			if l.vlogPath != "" {
				currentSizes[l.vlogPath] = l.vlogLiveBytes.Load()
				pathClasses[l.vlogPath] = l.vlogGenerationClass
			}
			l.vlogMu.Unlock()
		}
	} else {
		for i := range db.lanes {
			l := &db.lanes[i]
			l.walMu.Lock()
			for path, size := range l.walClosedSizes {
				pathSizes[path] = size
			}
			if l.walPath != "" {
				currentSizes[l.walPath] = l.walLiveBytes.Load()
			}
			l.walMu.Unlock()
		}
	}

	for _, path := range paths {
		out.SegmentsTotal++
		class := pathClasses[path]
		switch class {
		case vlogGenerationClassWarm:
			out.SegmentsWarm++
		case vlogGenerationClassCold:
			out.SegmentsCold++
		default:
			out.SegmentsHot++
		}
		size := int64(0)
		if v, ok := currentSizes[path]; ok {
			size = v
		} else if v, ok := pathSizes[path]; ok {
			size = v
		}
		out.BytesTotal += size
		switch class {
		case vlogGenerationClassWarm:
			out.BytesWarm += size
		case vlogGenerationClassCold:
			out.BytesCold += size
		default:
			out.BytesHot += size
		}
	}
	return out
}

func (db *DB) valueLogRetainedPaths() []string {
	db.valueLogMu.Lock()
	if len(db.valueLogRetain) == 0 {
		db.valueLogMu.Unlock()
		return nil
	}
	paths := make([]string, 0, len(db.valueLogRetain))
	for path := range db.valueLogRetain {
		paths = append(paths, path)
	}
	db.valueLogMu.Unlock()
	return paths
}

// ValueLogRetainedPaths returns a best-effort snapshot of retained value-log
// segment paths currently pinned by cached-mode pointer lifecycle tracking.
func (db *DB) ValueLogRetainedPaths() []string {
	return db.valueLogRetainedPaths()
}

// ReadValueLogRecord reads a raw value-log record for diagnostics and tooling.
func (db *DB) ReadValueLogRecord(ptr page.ValuePtr) ([]byte, error) {
	if db == nil || db.valueLogReader == nil {
		return nil, fmt.Errorf("cachingdb: value-log reader unavailable")
	}
	return db.valueLogReader.Read(ptr)
}

type backendPointerProjectionIterator interface {
	IteratorWithOptions(start, end []byte, opts tree.IteratorOptions) (iterator.UnsafeIterator, error)
}

func (db *DB) collectValueLogLiveIDs() (map[uint32]struct{}, error) {
	if db == nil || !db.valueLogEnabled() {
		return make(map[uint32]struct{}), nil
	}
	var (
		it  iterator.UnsafeIterator
		err error
	)
	if proj, ok := db.backend.(backendPointerProjectionIterator); ok {
		it, err = proj.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	} else {
		it, err = db.backend.Iterator(nil, nil)
	}
	if err != nil {
		return nil, err
	}
	defer it.Close()

	live := make(map[uint32]struct{})
	for it.Valid() {
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
			live[ptr.FileID] = struct{}{}
			if err := db.collectNestedValueLogLiveIDsFromOuterLeaf(ptr, live); err != nil {
				if err := it.Close(); err != nil {
					return nil, err
				}
				return nil, err
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}

	if snapper, ok := db.backend.(interface{ AcquireSnapshot() *backenddb.Snapshot }); ok {
		snap := snapper.AcquireSnapshot()
		if snap != nil {
			state := snap.State()
			p := snap.Pager()
			if state != nil && p != nil {
				if err := collectLeafRefValueLogLiveIDs(p, state.RootPageID, live); err != nil {
					_ = snap.Close()
					return nil, err
				}
				if err := collectLeafRefValueLogLiveIDs(p, state.SystemRootPageID, live); err != nil {
					_ = snap.Close()
					return nil, err
				}
			}
			if err := snap.Close(); err != nil {
				return nil, err
			}
		}
	}

	return live, nil
}

func (db *DB) collectNestedValueLogLiveIDsFromOuterLeaf(ptr page.ValuePtr, live map[uint32]struct{}) error {
	if db == nil || len(live) == 0 || db.valueLogReader == nil {
		return nil
	}
	raw, err := db.valueLogReader.Read(ptr)
	if err != nil {
		return err
	}
	if !outerleaf.HasMagic(raw) {
		return nil
	}
	block, err := outerleaf.DecodeBlockLeaseWithVerify(raw, false)
	if err != nil {
		return nil
	}
	defer block.Release()

	typed, err := block.TypedEntries(nil)
	if err != nil {
		return nil
	}
	for i := range typed {
		if typed[i].Kind != outerleaf.EntryKindBlobRef {
			continue
		}
		if typed[i].BlobPtr.FileID == 0 {
			continue
		}
		if !page.IsValueLogFileID(typed[i].BlobPtr.FileID) {
			return fmt.Errorf("cachingdb: invalid nested blob pointer file %d", typed[i].BlobPtr.FileID)
		}
		live[typed[i].BlobPtr.FileID] = struct{}{}
	}
	return nil
}

func (db *DB) checkValueLogRetention() {
	limit := db.maxValueLogRetainedBytes
	if limit <= 0 || !db.valueLogEnabled() {
		return
	}
	_, bytes := db.valueLogRetainedStats()
	if bytes <= limit {
		db.valueLogWarned.Store(false)
		return
	}
	if db.valueLogWarned.CompareAndSwap(false, true) {
		db.reportError(fmt.Errorf("cachingdb: retained value-log bytes %d exceed limit %d", bytes, limit))
	}
}

func (db *DB) allowValueLogPointers() bool {
	if !db.valueLogEnabled() {
		return false
	}
	limit := db.maxValueLogRetainedBytesHard
	if limit <= 0 {
		return true
	}
	bytes := db.valueLogRetainedClosedBytes.Load()
	if db.splitValueLogEnabled() {
		for i := range db.lanes {
			l := &db.lanes[i]
			if l.vlogPath != "" && l.vlogPath == l.vlogRetainedPath {
				bytes += l.vlogLiveBytes.Load()
			}
		}
	}
	if bytes >= limit {
		if db.valueLogHardCapWarned.CompareAndSwap(false, true) {
			db.reportError(fmt.Errorf("cachingdb: retained value-log bytes %d exceed hard cap %d; disabling new value-log pointers", bytes, limit))
		}
		return false
	}
	db.valueLogHardCapWarned.Store(false)
	return true
}

type valueLogZombieMarker interface {
	MarkValueLogZombie(id uint32) error
}

type valueLogSetRefresher interface {
	RefreshValueLogSet() error
}

func (db *DB) pruneRetainedValueLogs() {
	if !db.valueLogEnabled() {
		return
	}
	paths := db.valueLogRetainedPaths()
	if len(paths) == 0 {
		return
	}

	live, err := db.collectValueLogLiveIDs()
	if err != nil {
		db.reportError(fmt.Errorf("cachingdb: failed to scan value-log pointers: %w", err))
		return
	}

	inUse := make(map[string]struct{})
	if db.splitValueLogEnabled() {
		for _, path := range db.currentValueLogPaths() {
			inUse[path] = struct{}{}
		}
		for _, paths := range db.queueValueLogPaths {
			for _, path := range paths {
				inUse[path] = struct{}{}
			}
		}
	} else {
		for _, path := range db.currentWALPaths() {
			inUse[path] = struct{}{}
		}
		for _, paths := range db.queueWALPaths {
			for _, path := range paths {
				inUse[path] = struct{}{}
			}
		}
	}

	removed := false
	marked := false
	for _, path := range paths {
		if _, ok := inUse[path]; ok {
			continue
		}
		laneID, seq, valueLog, ok := parseLogSeq(filepath.Base(path))
		if !ok || !valueLog {
			continue
		}
		if laneID < 0 {
			continue
		}
		id, err := valuelog.EncodeFileID(uint32(laneID), uint32(seq))
		if err != nil {
			continue
		}
		if _, ok := live[id]; ok {
			continue
		}

		if marker, ok := db.backend.(valueLogZombieMarker); ok {
			if db.valueLogReader != nil {
				_ = db.valueLogReader.EvictSegment(id)
			}
			if err := marker.MarkValueLogZombie(id); err != nil {
				db.reportError(fmt.Errorf("cachingdb: failed to mark value-log %d zombie: %w", id, err))
				continue
			}
			marked = true
		} else {
			db.dropValueLogSegment(path)
			_ = db.removeFileRetry(path)
			db.mu.Lock()
			db.untrackValueLogSegmentLocked(path)
			db.mu.Unlock()
			removed = true
		}
		db.forgetValueLogRetain(path)
	}

	if marked {
		if refresher, ok := db.backend.(valueLogSetRefresher); ok {
			if err := refresher.RefreshValueLogSet(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: failed to refresh value-log set: %w", err))
			}
		}
	}
	if removed {
		db.syncDirBestEffort(db.dir)
	}
}

func hashKey(key []byte) uint64 {
	return xxhash.Sum64(key)
}

func (db *DB) shardIndex(key []byte) int {
	if len(db.mutableShards) <= 1 {
		return 0
	}
	return int(hashKey(key) & db.mutableShardMask)
}

func (db *DB) shardForKey(key []byte) *memShard {
	return &db.mutableShards[db.shardIndex(key)]
}

func (db *DB) laneForShardIndex(shardID int) int {
	if len(db.lanes) == 0 {
		return 0
	}
	if shardID < 0 {
		shardID = 0
	}
	if db.valueLogGenerationPolicy == 1 && len(db.valueLogHotLanes) > 0 {
		return db.valueLogHotLanes[shardID%len(db.valueLogHotLanes)]
	}
	return shardID % len(db.lanes)
}

func (db *DB) rebuildGenerationLaneSets() {
	if db == nil {
		return
	}
	db.valueLogHotLanes = db.valueLogHotLanes[:0]
	db.valueLogWarmLanes = db.valueLogWarmLanes[:0]
	db.valueLogColdLanes = db.valueLogColdLanes[:0]
	for i := range db.lanes {
		switch db.lanes[i].vlogGenerationClass {
		case vlogGenerationClassWarm:
			db.valueLogWarmLanes = append(db.valueLogWarmLanes, i)
		case vlogGenerationClassCold:
			db.valueLogColdLanes = append(db.valueLogColdLanes, i)
		default:
			db.valueLogHotLanes = append(db.valueLogHotLanes, i)
		}
	}
	if len(db.valueLogHotLanes) == 0 && len(db.lanes) > 0 {
		db.valueLogHotLanes = append(db.valueLogHotLanes, 0)
	}
}

func (db *DB) valueLogMaxSegmentBytesForLane(l *lane) int64 {
	if db == nil {
		return 0
	}
	// Legacy cap still applies when generation class targets are unset.
	base := db.valueLogMaxSegmentBytes
	if db.valueLogGenerationPolicy != 1 || l == nil {
		return base
	}
	target := int64(0)
	switch l.vlogGenerationClass {
	case vlogGenerationClassWarm:
		target = db.valueLogGenerationWarmTarget
	case vlogGenerationClassCold:
		target = db.valueLogGenerationColdTarget
	default:
		target = db.valueLogGenerationHotTarget
	}
	if target <= 0 {
		return base
	}
	if base > 0 && base < target {
		return base
	}
	return target
}

func (db *DB) shardExceedsLimit(shard *memShard, addBytes int64) bool {
	if maxMemtableBytesPerShard <= 0 {
		return false
	}
	return shard.bytes+addBytes > maxMemtableBytesPerShard
}

func (db *DB) newBackendBatchWithSize(size int) batch.Interface {
	if db == nil || db.backend == nil {
		return nil
	}
	type batchSizer interface {
		NewBatchWithSize(size int) batch.Interface
	}
	if size < 0 {
		size = 0
	}
	maxSize := flushBackendBatchInitEntries
	if db != nil && db.flushBackendInitEntries > 0 {
		maxSize = db.flushBackendInitEntries
	}
	if maxSize > 0 && size > maxSize {
		size = maxSize
	}
	if sizer, ok := db.backend.(batchSizer); ok {
		return sizer.NewBatchWithSize(size)
	}
	return db.backend.NewBatch()
}

// BackendDB defines the subset of treedb.DB needed by CachingDB.
type BackendDB interface {
	Get(key []byte) ([]byte, error)
	GetUnsafe(key []byte) ([]byte, error)
	GetAppend(key, dst []byte) ([]byte, error)
	Has(key []byte) (bool, error)
	Iterator(start, end []byte) (iterator.UnsafeIterator, error)
	ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error)
	NewBatch() batch.Interface
	Close() error
	Print() error
	Stats() map[string]string
}

type backendValueLogRewriter interface {
	ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error)
}

type backendValueLogGCer interface {
	ValueLogGC(ctx context.Context, opts backenddb.ValueLogGCOptions) (backenddb.ValueLogGCStats, error)
}

type backendIndexVacuumer interface {
	VacuumIndexOnline(ctx context.Context) error
}

type Options struct {
	FlushThreshold int64

	// MemtableMode selects the in-memory write buffer implementation.
	// Supported: "skiplist", "hash_sorted", "btree", "append_only", "adaptive".
	// Use "adaptive" or "adaptive:<mode>" to switch per-rotation based on workload.
	MemtableMode string

	// MemtableShards controls the number of mutable memtable shards. Values <= 0
	// use a default derived from GOMAXPROCS. The count is rounded down to a power
	// of two.
	MemtableShards int
	// DomainIngressWorkers enables experimental domain-local write ingress queues.
	// Values <= 0 keep the legacy direct caller write path.
	DomainIngressWorkers int
	// DomainIngressQueueSize configures the per-worker ingress queue length.
	// Values <= 0 use a default.
	DomainIngressQueueSize int

	// Legacy backpressure knob: queue length limit.
	// 0 uses the default (4). <0 disables writer backpressure entirely.
	MaxQueuedMemtables int

	// Adaptive backpressure knobs (seconds/bytes). If any of these are non-zero,
	// the caching layer uses backlog-bytes thresholds instead of queue length.
	SlowdownBacklogSeconds float64
	StopBacklogSeconds     float64
	MaxBacklogBytes        int64

	// Writer flush assist limits when backpressure triggers.
	WriterFlushMaxMemtables int
	WriterFlushMaxDuration  time.Duration

	// FlushBuildConcurrency controls how many goroutines may be used to build a
	// combined flush batch from multiple immutable memtables. Values <= 1 disable
	// parallelism.
	FlushBuildConcurrency int
	// FlushBuildMinEntries gates the parallel build path by total entries.
	// Values <= 0 use a default of 16k.
	FlushBuildMinEntries int
	// FlushBuildMinUnits gates the parallel build path by number of queued units.
	// Values <= 0 use a default of 2.
	FlushBuildMinUnits int
	// FlushBuildChunkCap controls the maximum entries per build chunk.
	// Values < 0 use the fixed default of 8192, 0 enables adaptive chunk sizing,
	// and values > 0 set a fixed cap.
	FlushBuildChunkCap int
	// FlushBuildChunkTargetBytes controls adaptive chunk sizing (bytes per chunk).
	// Values <= 0 use a default of 2MiB.
	FlushBuildChunkTargetBytes int
	// FlushBuildChunkMinBytes clamps adaptive chunk sizes (minimum bytes).
	// Values <= 0 use a default of 1MiB.
	FlushBuildChunkMinBytes int
	// FlushBuildChunkMaxBytes clamps adaptive chunk sizes (maximum bytes).
	// Values <= 0 use a default of 4MiB.
	FlushBuildChunkMaxBytes int
	// FlushBuildPrefetchUnits controls how many memtables to start building ahead
	// of the consumer. Values <= 0 use FlushBuildConcurrency.
	FlushBuildPrefetchUnits int

	// FlushBackendMaxEntries caps how many operations are buffered into a single
	// backend batch before committing it and continuing with a fresh batch.
	//
	// This increases backend commit cadence during very large flushes, which can
	// reduce index.db high-watermark growth under small KeepRecent windows by
	// making retired pages eligible for reuse sooner.
	//
	// 0 uses the internal default. Negative disables chunking (single backend
	// commit per flush).
	FlushBackendMaxEntries int
	// FlushBackendMaxBatches caps how many intermediate backend commits a single
	// flush may emit. This bounds zipper/apply overhead when FlushBackendMaxEntries
	// is very small relative to the flush size.
	//
	// 0 uses the internal default. Negative disables the cap.
	FlushBackendMaxBatches int

	// DisableWAL disables the redo/journal log while keeping the value log enabled.
	DisableWAL bool
	// JournalLanes controls the number of active commit/value log lanes
	// (0=GOMAXPROCS-aware default).
	// Max supported lanes is 255; value-log segment sequence per lane is capped at 8,388,607.
	JournalLanes int
	// WALMaxSegmentBytes caps the size of a single WAL segment payload.
	// 0 uses the default limit.
	WALMaxSegmentBytes int64
	// JournalCompression enables best-effort zstd compression for journal/commitlog
	// segments (metadata only). The writer only keeps compressed bytes when they
	// are smaller than the raw payload, so compression never causes size
	// amplification.
	JournalCompression bool
	// RelaxedSync disables fsync on Sync operations.
	RelaxedSync bool
	// ValueLogPointerThreshold controls when WAL/vlog pointers are used.
	// Values <= 0 use a default threshold. In relaxed durability modes, the
	// default is smaller to avoid catastrophic update-heavy cliffs at large key
	// counts by pushing moderate values into the value log.
	ValueLogPointerThreshold int
	// IndexOuterLeavesInValueLog stores B+Tree leaf pages (the pages containing
	// key/value entries) in the persistent value log instead of index.db.
	IndexOuterLeavesInValueLog bool
	// IndexOuterLeafMode selects the outer-leaf payload format.
	// Supported values: "v1", "v1_leaflog", "v1_leaflog_route",
	// "v1_leaflog_legacy", "v2_blockptr", "v2_fenceptr".
	// Empty defaults to "v2_fenceptr".
	IndexOuterLeafMode string
	// ValueLogWALFenceMode selects WAL encoding behavior for pointer-eligible
	// writes under v2 fence-pointer mode.
	// Supported values: "rid_join" (default), "simple_inline".
	ValueLogWALFenceMode string
	// ValueLogDomainInlineThresholds configures optional per-domain overrides
	// for inline-vs-pointer placement. Longest-prefix match wins.
	ValueLogDomainInlineThresholds []backenddb.ValueLogDomainThreshold
	// ValueLogRawWritevMinAvgBytes controls raw grouped-frame writev usage for
	// the value log.
	//
	// 0 uses adaptive mode (no average-bytes floor); values >0 require average
	// payload bytes/record to meet this floor before raw writev is considered.
	ValueLogRawWritevMinAvgBytes int
	// ValueLogRawWritevMinBatchRecords controls the minimum grouped records before
	// raw writev is considered for value-log appends.
	//
	// Values <=0 use a default of 8.
	ValueLogRawWritevMinBatchRecords int
	// ValueLogCompression selects value-log compression behavior:
	// 0=default(unset; normalized to auto by TreeDB Open), 1=off, 2=block,
	// 3=dict, 4=auto.
	ValueLogCompression uint8
	// ValueLogBlockCodec selects block codec when block compression is enabled:
	// 0=snappy, 1=lz4.
	ValueLogBlockCodec uint8
	// ValueLogBlockTargetCompressedBytes controls block-mode grouped frame K
	// adaptation target (0=default).
	ValueLogBlockTargetCompressedBytes int
	// ValueLogOuterLeafBlockTargetBytes controls v2 outer-leaf payload target
	// size (0=mode-aware default).
	ValueLogOuterLeafBlockTargetBytes int
	// ValueLogOuterLeafBlockCodec selects v2 outer-leaf payload codec:
	// 0=snappy, 1=lz4.
	ValueLogOuterLeafBlockCodec uint8
	// ValueLogOuterLeafBlockRestartInterval controls restart cadence encoded in
	// v2 outer-leaf payload metadata (0=default).
	ValueLogOuterLeafBlockRestartInterval int
	// ValueLogOuterLeafBlobThresholdBytes controls when v2 fence-pointer outer
	// entries use blob references instead of inline bytes (0=default threshold).
	ValueLogOuterLeafBlobThresholdBytes int
	// ValueLogIncompressibleHoldBytes configures auto-mode incompressible hold
	// window bytes (0=default).
	ValueLogIncompressibleHoldBytes int
	// ValueLogIncompressibleProbeBytes configures auto-mode hold probe interval
	// bytes (0=default).
	ValueLogIncompressibleProbeBytes int
	// ValueLogAutoPolicy controls auto-mode dict-vs-block bias:
	// 0=balanced, 1=throughput, 2=size.
	ValueLogAutoPolicy uint8
	// ValueLogMaxSegmentBytes caps the size of a single value-log segment file.
	// 0 disables the cap.
	//
	// This is an internal safety knob used by experimental index encodings
	// (e.g. packed on-disk ValuePtr) that require value-log offsets stay within a
	// smaller representable range.
	ValueLogMaxSegmentBytes int64
	// ValueLogGenerationPolicy selects generational mode:
	// 0=off, 1=hot_warm_cold.
	ValueLogGenerationPolicy uint8
	// ValueLogGenerationHotSegmentTargetBytes configures hot segment target size.
	ValueLogGenerationHotSegmentTargetBytes int64
	// ValueLogGenerationWarmSegmentTargetBytes configures warm segment target size.
	ValueLogGenerationWarmSegmentTargetBytes int64
	// ValueLogGenerationColdSegmentTargetBytes configures cold segment target size.
	ValueLogGenerationColdSegmentTargetBytes int64
	// ValueLogRewriteBudgetBytesPerSec configures incremental rewrite byte budget.
	ValueLogRewriteBudgetBytesPerSec int64
	// ValueLogRewriteBudgetRecordsPerSec configures incremental rewrite record budget.
	ValueLogRewriteBudgetRecordsPerSec int
	// ValueLogRewriteTriggerStaleRatioPPM triggers rewrite by stale/live ratio.
	ValueLogRewriteTriggerStaleRatioPPM uint32
	// ValueLogRewriteTriggerTotalBytes triggers rewrite by retained bytes.
	ValueLogRewriteTriggerTotalBytes int64
	// ValueLogRewriteTriggerChurnPerSec triggers rewrite by churn rate.
	ValueLogRewriteTriggerChurnPerSec int64
	// ForceValueLogPointers stores all values out-of-line in the value log.
	ForceValueLogPointers bool
	// DisableReadChecksum skips CRC verification on value-log reads.
	DisableReadChecksum bool
	// AllowUnsafe acknowledges unsafe durability options.
	// When false, Open will reject DisableWAL or RelaxedSync.
	AllowUnsafe bool
	// MaxValueLogRetainedBytes emits a warning when retained value-log bytes exceed
	// this threshold (0 disables warnings).
	MaxValueLogRetainedBytes int64
	// MaxValueLogRetainedBytesHard disables value-log pointers for new large
	// values once retained bytes exceed this threshold (0 disables the cap).
	MaxValueLogRetainedBytesHard int64

	// ValueLogDictTrain configures background dictionary training for value-log frame compression.
	// TrainBytes <= 0 disables training.
	ValueLogDictTrain compression.TrainConfig
	// ValueLogDictMaxK clamps the maximum group size (K) used for dict-compressed
	// value-log frames. Values <= 0 use the default (32).
	ValueLogDictMaxK int
	// ValueLogDictFrameEncodeLevel controls the zstd encoder level used for
	// dict-compressed value-log frames. Values <= 0 use SpeedFastest.
	ValueLogDictFrameEncodeLevel zstd.EncoderLevel
	// ValueLogDictFrameEnableEntropy enables entropy coding for dict-compressed
	// frames (higher ratio, lower throughput).
	ValueLogDictFrameEnableEntropy bool
	// ValueLogDictAdaptiveRatio enables adaptive pause of dict compression when payload ratios degrade.
	// 0 disables.
	ValueLogDictAdaptiveRatio float64
	// ValueLogDictMetricsWindowBytes controls the metrics window size (0=default).
	ValueLogDictMetricsWindowBytes int
	// ValueLogDictMetricsMinRecords is a minimum record count before pausing (0=default).
	ValueLogDictMetricsMinRecords int
	// ValueLogDictMetricsPauseBytes controls pause duration in bytes (0=default).
	ValueLogDictMetricsPauseBytes int
	// ValueLogDictIncompressibleHoldBytes enables classifier-driven hold mode for
	// high-entropy streams. While hold mode is active, dict compression attempts
	// and trainer collection are bypassed until hold bytes are consumed.
	//
	// 0 uses profile/default hold configuration; <0 explicitly disables hold
	// mode and opts out of profile defaults.
	ValueLogDictIncompressibleHoldBytes int
	// ValueLogDictProbeIntervalBytes controls periodic probe attempts while
	// incompressible hold mode is active.
	//
	// Values <=0 use a default derived from hold bytes.
	ValueLogDictProbeIntervalBytes int
	// ValueLogDictMinPayloadSavingsRatio rejects newly trained dictionaries whose
	// payload ratio does not improve by at least this fraction (0 uses a
	// throughput-oriented default: 0.02 normally, 0.05 with force pointers or
	// WAL disabled).
	ValueLogDictMinPayloadSavingsRatio float64

	// ValueLogCompressionAutotune configures the wall-time value-log compression autotuner.
	// Cached mode only (value log enabled by default).
	ValueLogCompressionAutotune valuelog.AutotuneOptions

	// ValueLogTemplateMode controls template-based compression for value-log values.
	ValueLogTemplateMode template.Mode
	// ValueLogTemplateConfig controls template creation and encoding behavior.
	ValueLogTemplateConfig template.Config
	// ValueLogTemplateReadStrict controls strict template decode behavior.
	ValueLogTemplateReadStrict bool

	// NotifyError is an optional hook for background maintenance failures.
	NotifyError func(error)
}

// DictStore provides access to the current dictionary ID for write freezing.
type DictStore interface {
	GetCurrent(ctx context.Context) (uint64, error)
	GetDictBytes(ctx context.Context, dictID uint64) ([]byte, error)
}

type batchArenaLease struct {
	refs   int
	chunks [][]byte
}

type memtableViewDeferredInfo struct {
	memtables     int64
	sinceUnixNano int64
}

type memtableViewLifecycleTelemetry struct {
	retainTotal  atomic.Uint64
	releaseTotal atomic.Uint64

	leasesInFlight    atomic.Int64
	leasesInFlightMax atomic.Int64

	deferredViewsCurrent     atomic.Int64
	deferredViewsMax         atomic.Int64
	deferredViewsTotal       atomic.Uint64
	deferredMemtablesCurrent atomic.Int64
	deferredMemtablesMax     atomic.Int64
	deferredMemtablesTotal   atomic.Uint64

	deferredMu sync.Mutex
	deferred   map[*memtableView]memtableViewDeferredInfo

	oldestDeferredUnixNano atomic.Int64
}

type DB struct {
	mu      sync.RWMutex
	flushMu sync.Mutex
	writeMu sync.RWMutex
	statsMu sync.Mutex // Re-introduce global statsMu for isolation
	bpMu    sync.Mutex
	bpCond  *sync.Cond

	// Commit workers removed; backend commits are synchronous.

	checkpointMu   sync.Mutex
	checkpointCond *sync.Cond
	checkpointing  atomic.Bool

	// Level 0 (Memory)
	mutableShards         []memShard
	mutableShardMask      uint64
	mutableBytes          atomic.Int64
	mutableThreshold      atomic.Int64
	rotatePending         atomic.Bool
	queue                 []memtable.Table
	queueShardIDs         []uint16
	queueLaneIDs          []uint16
	queueIDs              []uint64
	queueEnqueueNS        []int64
	nextQueueID           atomic.Uint64
	batchEntryHint        atomic.Int32
	batchCopyBytesHint    atomic.Int32
	batchArenaLeaseMu     sync.Mutex
	batchArenaLeasesByMem map[memtable.Table][]*batchArenaLease
	batchEntriesPool      sync.Pool
	batchShardEntriesPool sync.Pool
	batchIntPool          sync.Pool

	// memtables is an RCU-style snapshot of (mutable, queue, queueRanges).
	// Readers load it atomically to avoid holding db.mu around memtable access.
	memtables             atomic.Pointer[memtableView]
	hashSortedIndexer     *memtable.HashSortedIndexer
	appendOnlyMemPool     sync.Pool
	appendOnlyMemLeaseMu  sync.Mutex
	appendOnlyMemLeases   []*memtable.AppendOnly
	pendingRetiredMems    []memtable.Table
	memtableViewTelemetry memtableViewLifecycleTelemetry
	queueRanges           []keyRange
	queueWALPaths         [][]string
	queueValueLogPaths    [][]string
	backendRange          keyRange
	backendRangeKnown     bool
	backendRangeInit      sync.Once
	backendRangeErr       error

	// Durability
	lanes         []lane
	laneMu        sync.Mutex
	laneCond      *sync.Cond
	nextLane      int
	flushLaneMu   []sync.Mutex
	nextCommitSeq atomic.Uint64
	walAckMu      sync.Mutex
	walErr        error
	nextRID       atomic.Uint64

	// Legacy flags removed from public options; retained internally for code paths.
	disableValueLog          bool
	splitValueLog            bool
	memtableValueLogPointers bool

	inlineThreshold                int
	valueLogThreshold              int
	valueLogDomainThresholds       []backenddb.ValueLogDomainThreshold
	indexOuterLeafMode             string
	valueLogWALFenceMode           string
	outerLeafBlockTargetBytes      int
	outerLeafBlockCodec            uint8
	outerLeafBlockRestart          int
	outerLeafBlobThresholdBytes    int
	forceValueLogPointers          bool
	valueLogRawWritevMinAvgBytes   int
	valueLogRawWritevMinRecords    int
	valueLogCompressionMode        uint8
	valueLogBlockCodec             valuelog.BlockCodec
	valueLogBlockTargetBytes       int
	valueLogIncompressibleHold     uint64
	valueLogIncompressibleProbe    uint64
	valueLogAutoPolicy             uint8
	valueLogGenerationPolicy       uint8
	valueLogGenerationHotTarget    int64
	valueLogGenerationWarmTarget   int64
	valueLogGenerationColdTarget   int64
	valueLogRewriteBudgetBytes     int64
	valueLogRewriteBudgetRecords   int
	valueLogRewriteTriggerRatioPPM uint32
	valueLogRewriteTriggerBytes    int64
	valueLogRewriteTriggerChurn    int64
	valueLogReader                 *valuelog.Manager
	valueLogHotLanes               []int
	valueLogWarmLanes              []int
	valueLogColdLanes              []int
	valueLogMu                     sync.Mutex
	valueLogRetain                 map[string]struct{}
	backendReadVlogDirtySeq        atomic.Uint64
	backendReadVlogFlushedSeq      atomic.Uint64
	deferredFenceCandidateKeys     atomic.Uint64
	deferredFenceEmittedGroups     atomic.Uint64
	deferredFenceEnqueuedKeys      atomic.Uint64
	deferredFenceEnqueuedBytes     atomic.Uint64
	deferredFenceMaterializedKeys  atomic.Uint64
	deferredFenceMaterializedBytes atomic.Uint64
	valueLogWarned                 atomic.Bool
	valueLogHardCapWarned          atomic.Bool
	valueLogRetainedClosedBytes    atomic.Int64
	maxValueLogRetainedBytes       int64
	maxValueLogRetainedBytesHard   int64

	// Level 1 (Disk)
	backend       BackendDB
	dictStore     DictStore
	templateStore template.Store

	// Value-log dictionary compression (cached mode).
	valueLogDictTrain              compression.TrainConfig
	valueLogDictMaxK               int
	valueLogDictFrameEncodeLevel   zstd.EncoderLevel
	valueLogDictFrameEnableEntropy bool
	valueLogDictSampleStride       uint64
	valueLogDictSampleStrideCount  atomic.Uint64
	valueLogDictClassifySampled    atomic.Uint64
	valueLogDictClassifySkipped    atomic.Uint64
	valueLogDictAdaptiveRatio      float64
	valueLogDictMinPayloadSavings  float64
	valueLogDictMetricsWindow      int
	valueLogDictMetricsMinRecords  int
	valueLogDictMetricsPauseBytes  int

	valueLogDictTrainerMu sync.Mutex
	valueLogDictTrainer   *compression.Trainer
	valueLogDictKickCh    chan struct{}
	valueLogDictMetrics   compression.Metrics
	valueLogDictFrames    struct {
		total     atomic.Uint64
		attempted atomic.Uint64
		kept      atomic.Uint64
	}
	valueLogAutotuneMetrics          vlogAutotuneMetrics
	valueLogAutotuneOptions          valuelog.AutotuneOptions
	valueLogAutotuneCandidateKSet    bool
	valueLogAutotuneLastProfile      atomic.Value // *vlogAutotuneProfile
	valueLogAutotuneLastSwitchFrames atomic.Uint64

	valueLogDictPauseRemaining               atomic.Uint64
	valueLogDictProbeBytes                   uint64
	valueLogDictProbeRemaining               atomic.Uint64
	valueLogDictIncompressibleHoldBytes      uint64
	valueLogDictIncompressibleHoldRemaining  atomic.Uint64
	valueLogDictIncompressibleProbeBytes     uint64
	valueLogDictIncompressibleProbeRemaining atomic.Uint64
	valueLogDictIncompressibleHitStreak      atomic.Uint32
	valueLogDictIncompressibleHits           atomic.Uint64
	valueLogDictIncompressibleHolds          atomic.Uint64
	valueLogDictIncompressibleBypassBytes    atomic.Uint64
	valueLogDictPausedSampleStride           uint64
	valueLogDictPausedSampleCounter          atomic.Uint64
	valueLogDictLastAppliedDictHash          atomic.Uint64
	valueLogDictLastAppliedDictID            atomic.Uint64
	valueLogDictLastPublishUnixNano          atomic.Int64
	valueLogDictLastKUpdateUnixNano          atomic.Int64
	valueLogDictCurrentK                     atomic.Uint32
	valueLogDictKMu                          sync.RWMutex
	valueLogDictKCache                       map[uint64]int
	valueLogDictBytesMu                      sync.Mutex
	valueLogDictBytesID                      uint64
	valueLogDictBytes                        []byte

	// Value-log template compression (cached mode).
	valueLogTemplateEnabled    bool
	valueLogTemplateMode       template.Mode
	valueLogTemplateEngine     *template.Engine
	valueLogTemplateReadStrict bool
	valueLogTemplateDecodeOpts template.DecodeOptions

	// Cached dictdb current pointer to avoid per-write lookups on the hot path.
	// A stale dictID is safe (it always points to a durable dict); at worst we
	// lag adoption of a newly trained dictionary.
	dictCurrentCached atomic.Uint64
	dictCurrentOps    atomic.Uint64

	// Config
	dir                       string
	flushThreshold            int64
	memtableCap               int
	memtableMode              memtable.Mode
	memtableStats             memtableStats
	memtableAdaptive          bool
	memtableAdaptiveObserve   atomic.Bool
	adaptiveShardedStats      bool
	memtableWarmupActive      bool
	memtableWarmupThreshold   int64
	domainIngressWorkers      int
	domainIngressQueueSize    int
	maxQueuedMemtables        int
	slowdownBacklogSeconds    float64
	stopBacklogSeconds        float64
	maxBacklogBytes           int64
	writerFlushMaxMemtables   int
	writerFlushMaxDuration    time.Duration
	flushBuildConcurrency     int
	flushBuildAutoConcurrency bool
	flushBuildMinEntries      int
	flushBuildMinUnits        int
	flushBuildChunkCap        int
	flushBuildChunkTarget     int
	flushBuildChunkMinBytes   int
	flushBuildChunkMaxBytes   int
	flushBuildPrefetchUnits   int
	flushBackendMaxEntries    int
	flushBackendInitEntries   int
	flushBackendMaxBatches    int
	walMaxSegmentBytes        int64
	valueLogMaxSegmentBytes   int64
	journalCompression        bool

	disableJournal                     bool
	relaxedSync                        bool
	notifyError                        func(error)
	debugFlushPointers                 bool
	debugFlushTiming                   bool
	debugV1LeafLogInvariants           bool
	debugV1LeafLogInvariantsPanic      bool
	debugPtrEligible                   atomic.Int64
	debugPtrUsed                       atomic.Int64
	debugPtrNoPtr                      atomic.Int64
	debugPtrDenied                     atomic.Int64
	debugPtrDisabled                   atomic.Int64
	routeLeaflogFallbackDirectAttempts atomic.Uint64
	routeLeaflogFallbackDirectRewrites atomic.Uint64
	vlogGenerationRemapSuccesses       atomic.Uint64
	vlogGenerationRemapFailures        atomic.Uint64
	vlogGenerationRewriteBytesIn       atomic.Uint64
	vlogGenerationRewriteBytesOut      atomic.Uint64
	vlogGenerationRewriteRuns          atomic.Uint64
	vlogGenerationGCSegmentsDeleted    atomic.Uint64
	vlogGenerationGCBytesDeleted       atomic.Uint64
	vlogGenerationGCRuns               atomic.Uint64
	vlogGenerationVacuumRuns           atomic.Uint64
	vlogGenerationVacuumFailures       atomic.Uint64
	vlogGenerationLastVacuumUnixNano   atomic.Int64
	vlogGenerationChurnBytes           atomic.Uint64
	vlogGenerationSchedulerState       atomic.Uint32
	vlogGenerationLastReason           atomic.Uint32
	vlogGenerationLastChurnBps         atomic.Int64
	vlogGenerationLastChurnSampleBytes atomic.Uint64
	vlogGenerationLastChurnSampleNS    atomic.Int64
	bgErrMu                            sync.Mutex
	bgErr                              error

	// Backpressure state
	queueBacklogBytes                   atomic.Int64
	flushBpsEWMA                        float64
	queueLaneIDMisses                   atomic.Int64
	backendWriteBatchesTotal            atomic.Int64
	deferredFenceAssistCalls            atomic.Uint64
	deferredFenceAssistFlushedMemtables atomic.Uint64
	deferredFenceAssistEarlyTriggers    atomic.Uint64
	deferredFenceAssistTicker           atomic.Uint64
	domainIngressMu                     sync.Mutex
	domainIngressCh                     []chan domainIngressRequest
	domainIngressEnqueued               atomic.Uint64
	domainIngressProcessed              atomic.Uint64
	domainIngressFallback               atomic.Uint64
	domainIngressDepthMax               atomic.Uint64

	// Lifecycle
	closeCh chan struct{}
	closing atomic.Bool
	flushCh chan struct{}
	wg      sync.WaitGroup

	autoCheckpointOnceCh  chan struct{}
	autoCheckpointWriteCh chan struct{}
	autoCheckpointOn      atomic.Bool
	// autoCheckpointSizeArmed gates the maxWALBytes size-triggered checkpoint.
	// It is disarmed after the first size-triggered checkpoint and re-armed only
	// after reclaimable WAL bytes fall below maxWALBytes/2.
	autoCheckpointSizeArmed atomic.Bool

	autoCheckpointCount                    atomic.Uint64
	autoCheckpointLastReason               atomic.Uint32
	autoCheckpointLastUnixNano             atomic.Int64
	autoCheckpointLastDurNanos             atomic.Int64
	autoCheckpointLastWALBefore            atomic.Int64
	autoCheckpointLastWALAfter             atomic.Int64
	autoCheckpointLastWALReclaimableBefore atomic.Int64
	autoCheckpointLastWALReclaimableAfter  atomic.Int64
	autoCheckpointLastWALTrimmed           atomic.Int64
	autoCheckpointLastWALBytes             atomic.Int64
	autoCheckpointMaxWALBytes              atomic.Int64

	checkpointCutoverLastNanos    atomic.Int64
	checkpointCutoverMaxNanos     atomic.Int64
	checkpointCutoverTotalNanos   atomic.Int64
	checkpointCutoverSamples      atomic.Uint64
	checkpointCutoverLastUnixNano atomic.Int64

	materializationLastDrainUnixNano atomic.Int64

	publishWatermarkLagMu            sync.Mutex
	publishWatermarkLastBacklogBytes int64
	publishWatermarkLastUnixNano     int64

	// testing hooks
	testOnVlogFlush      func(laneID int)
	testOnVlogSync       func(laneID int)
	testBeforeVlogUnlock func(laneID int)
}

const (
	vlogGenerationSchedulerDisabled uint32 = iota
	vlogGenerationSchedulerIdle
	vlogGenerationSchedulerRunning
	vlogGenerationSchedulerError
)

const (
	vlogGenerationReasonNone uint32 = iota
	vlogGenerationReasonTotalBytes
	vlogGenerationReasonStaleRatio
	vlogGenerationReasonChurn
	vlogGenerationReasonPeriodicGC
	vlogGenerationReasonPostRewriteVacuum
)

const (
	vlogGenerationLoopInterval = 1 * time.Second
	vlogGenerationGCEvery      = 5
	vlogGenerationGCMinBytes   = int64(1 << 20)
	// Coordinate index vacuum with major rewrite windows; do not run on every GC.
	vlogGenerationVacuumTriggerRewriteBytes = int64(64 << 20)
	vlogGenerationVacuumMinInterval         = 5 * time.Minute
)

func (db *DB) flushBackendEntriesCap(totalOps int, sync bool) int {
	capEntries := db.flushBackendMaxEntries
	if capEntries < 1 {
		capEntries = 1
	}
	maxBatches := db.flushBackendMaxBatches
	if maxBatches == 0 {
		maxBatches = 16
	}
	// Sync-triggered flushes (checkpoint/close) should remain fast; cap the number
	// of intermediate commits in that path even if the steady-state micro-batch
	// policy is aggressive.
	if sync && maxBatches > 8 {
		maxBatches = 8
	}
	if maxBatches > 0 {
		// Avoid overflow when capEntries is very large (e.g., chunking disabled).
		maxInt := int(^uint(0) >> 1)
		if capEntries <= maxInt/maxBatches && totalOps > capEntries*maxBatches {
			// Increase the chunk size so we emit at most maxBatches intermediate
			// commits. This preserves the high-watermark benefits of micro-batching
			// while bounding zipper/apply overhead.
			capEntries = (totalOps + maxBatches - 1) / maxBatches
			if capEntries < 1 {
				capEntries = 1
			}
		}
	}
	return capEntries
}

func (db *DB) flushBackendEntriesCapForOps(totalOps int, deleteOps int, sync bool) int {
	capEntries := db.flushBackendMaxEntries
	if capEntries < 1 {
		capEntries = 1
	}
	maxBatches := db.flushBackendMaxBatches
	if maxBatches == 0 {
		maxBatches = 16
	}
	// Sync-triggered flushes (checkpoint/close) should remain fast; cap the number
	// of intermediate commits in that path even if the steady-state micro-batch
	// policy is aggressive.
	if sync && maxBatches > 8 {
		maxBatches = 8
	}
	// Delete-heavy flushes are expensive to apply in many intermediate commits.
	// Each commit re-writes leaf pages (copying surviving values), so repeated
	// commits amplify work dramatically when deletes touch a large fraction of the
	// keyspace. Favor fewer commits in that case.
	if maxBatches > 0 && deleteOps > 0 && totalOps > 0 {
		// Deterministic "delete-heavy" trigger: deletes are at least 25% of ops.
		if deleteOps*4 >= totalOps && maxBatches > 4 {
			maxBatches = 4
		}
	}
	if maxBatches > 0 {
		// Avoid overflow when capEntries is very large (e.g., chunking disabled).
		maxInt := int(^uint(0) >> 1)
		if capEntries <= maxInt/maxBatches && totalOps > capEntries*maxBatches {
			// Increase the chunk size so we emit at most maxBatches intermediate
			// commits. This preserves the high-watermark benefits of micro-batching
			// while bounding zipper/apply overhead.
			capEntries = (totalOps + maxBatches - 1) / maxBatches
			if capEntries < 1 {
				capEntries = 1
			}
		}
	}
	return capEntries
}

type keyRange struct {
	valid bool
	min   []byte
	max   []byte
}

type memShard struct {
	mu    sync.Mutex
	mem   memtable.Table
	rng   keyRange
	bytes int64
	stats memtableStats
}

// memtableView is an immutable snapshot of the in-memory layers.
// It is published via atomic.Pointer and treated as read-only by readers.
type memtableView struct {
	mutables                 []memtable.Table
	queue                    []memtable.Table
	queueShardIDs            []uint16
	queueRanges              []keyRange
	refs                     atomic.Int64
	retiredMems              []memtable.Table
	deferredRetiredMemtables atomic.Int64
}

const maxAppendOnlyMemLeases = 256
const appendOnlyEstimatedBytesPerEntryDeferredFence = 96

func updateInt64Max(dst *atomic.Int64, value int64) {
	for {
		cur := dst.Load()
		if value <= cur {
			return
		}
		if dst.CompareAndSwap(cur, value) {
			return
		}
	}
}

func (db *DB) noteMemtableViewRetain() {
	if db == nil {
		return
	}
	tel := &db.memtableViewTelemetry
	tel.retainTotal.Add(1)
	inFlight := tel.leasesInFlight.Add(1)
	updateInt64Max(&tel.leasesInFlightMax, inFlight)
}

func (db *DB) noteMemtableViewRelease() {
	if db == nil {
		return
	}
	tel := &db.memtableViewTelemetry
	tel.releaseTotal.Add(1)
	tel.leasesInFlight.Add(-1)
}

func (db *DB) noteMemtableViewDeferredEnter(view *memtableView, memtables int64) {
	if db == nil || view == nil || memtables <= 0 {
		return
	}
	tel := &db.memtableViewTelemetry
	since := time.Now().UnixNano()

	tel.deferredMu.Lock()
	defer tel.deferredMu.Unlock()
	if tel.deferred == nil {
		tel.deferred = make(map[*memtableView]memtableViewDeferredInfo)
	}
	if _, exists := tel.deferred[view]; exists {
		return
	}
	// Avoid recording deferred telemetry after final release. This prevents
	// stale entries if a concurrent releaser already dropped refs to zero.
	if view.refs.Load() == 0 {
		return
	}
	tel.deferred[view] = memtableViewDeferredInfo{
		memtables:     memtables,
		sinceUnixNano: since,
	}
	oldest := int64(0)
	for _, deferred := range tel.deferred {
		if oldest == 0 || deferred.sinceUnixNano < oldest {
			oldest = deferred.sinceUnixNano
		}
	}
	tel.oldestDeferredUnixNano.Store(oldest)

	tel.deferredViewsTotal.Add(1)
	deferredViewsCurrent := tel.deferredViewsCurrent.Add(1)
	updateInt64Max(&tel.deferredViewsMax, deferredViewsCurrent)
	tel.deferredMemtablesTotal.Add(uint64(memtables))
	deferredMemtablesCurrent := tel.deferredMemtablesCurrent.Add(memtables)
	updateInt64Max(&tel.deferredMemtablesMax, deferredMemtablesCurrent)
}

func (db *DB) noteMemtableViewDeferredExit(view *memtableView) {
	if db == nil || view == nil {
		return
	}
	tel := &db.memtableViewTelemetry
	var (
		info  memtableViewDeferredInfo
		found bool
	)
	oldest := int64(0)
	tel.deferredMu.Lock()
	defer tel.deferredMu.Unlock()
	if tel.deferred != nil {
		info, found = tel.deferred[view]
		if found {
			delete(tel.deferred, view)
		}
		for _, deferred := range tel.deferred {
			if oldest == 0 || deferred.sinceUnixNano < oldest {
				oldest = deferred.sinceUnixNano
			}
		}
	}
	tel.oldestDeferredUnixNano.Store(oldest)
	if !found {
		return
	}
	tel.deferredViewsCurrent.Add(-1)
	tel.deferredMemtablesCurrent.Add(-info.memtables)
}

func (db *DB) retainMemtableView() *memtableView {
	if db == nil {
		return nil
	}
	for {
		view := db.memtables.Load()
		if view == nil {
			return nil
		}
		// Fast path: published views keep a baseline ref, so this is usually one
		// atomic add and return.
		if n := view.refs.Add(1); n > 1 {
			db.noteMemtableViewRetain()
			return view
		}
		// We observed a zero-ref view. Undo and retry unless this is still the
		// published view, in which case self-heal by restoring baseline+caller refs.
		view.refs.Add(-1)
		if db.memtables.Load() != view {
			continue
		}
		if view.refs.CompareAndSwap(0, 2) {
			db.noteMemtableViewRetain()
			return view
		}
	}
}

func (db *DB) releaseMemtableView(view *memtableView) {
	db.releaseMemtableViewRef(view, true)
}

func (db *DB) releasePublishedMemtableView(view *memtableView) {
	db.releaseMemtableViewRef(view, false)
}

func (db *DB) releaseMemtableViewRef(view *memtableView, leaseRelease bool) {
	if db == nil || view == nil {
		return
	}
	if leaseRelease {
		db.noteMemtableViewRelease()
	}
	if refs := view.refs.Add(-1); refs != 0 {
		if refs > 0 && view.deferredRetiredMemtables.Load() > 0 {
			db.noteMemtableViewDeferredEnter(view, view.deferredRetiredMemtables.Load())
		}
		return
	}
	db.noteMemtableViewDeferredExit(view)
	db.recycleMemtables(view.retiredMems)
	view.retiredMems = nil
}

func memtableNeedsBatchArenaRetention(mt memtable.Table) bool {
	// Skiplist-backed memtables copy key/value bytes into their own arena, so
	// batch-owned buffers can be recycled immediately after write.
	_, skiplist := mt.(*memtable.Memtable)
	return !skiplist
}

func getBatchArenaLease(refs int, chunks [][]byte) *batchArenaLease {
	lease, _ := batchArenaLeasePool.Get().(*batchArenaLease)
	if lease == nil {
		lease = &batchArenaLease{}
	}
	lease.refs = refs
	lease.chunks = chunks
	return lease
}

func putBatchArenaLease(lease *batchArenaLease) {
	if lease == nil {
		return
	}
	lease.refs = 0
	lease.chunks = nil
	batchArenaLeasePool.Put(lease)
}

func (db *DB) retainBatchArenaChunksForMemtables(chunks [][]byte, mems []memtable.Table) {
	if len(chunks) == 0 {
		return
	}
	if db == nil || len(mems) == 0 {
		putBatchArenas(chunks)
		return
	}
	filtered := mems[:0]
	for _, mt := range mems {
		if mt == nil || !memtableNeedsBatchArenaRetention(mt) {
			continue
		}
		filtered = append(filtered, mt)
	}
	if len(filtered) == 0 {
		putBatchArenas(chunks)
		return
	}
	lease := getBatchArenaLease(len(filtered), chunks)
	db.batchArenaLeaseMu.Lock()
	if db.batchArenaLeasesByMem == nil {
		db.batchArenaLeasesByMem = make(map[memtable.Table][]*batchArenaLease, len(filtered))
	}
	for _, mt := range filtered {
		db.batchArenaLeasesByMem[mt] = append(db.batchArenaLeasesByMem[mt], lease)
	}
	db.batchArenaLeaseMu.Unlock()
}

func (db *DB) releaseBatchArenaLeasesForMemtable(mt memtable.Table) {
	if db == nil || mt == nil {
		return
	}
	var release [][]byte
	db.batchArenaLeaseMu.Lock()
	leases := db.batchArenaLeasesByMem[mt]
	if len(leases) > 0 {
		delete(db.batchArenaLeasesByMem, mt)
		for _, lease := range leases {
			if lease == nil || lease.refs <= 0 {
				continue
			}
			lease.refs--
			if lease.refs == 0 && len(lease.chunks) > 0 {
				release = append(release, lease.chunks...)
				lease.chunks = nil
				putBatchArenaLease(lease)
			}
		}
	}
	db.batchArenaLeaseMu.Unlock()
	if len(release) > 0 {
		putBatchArenas(release)
	}
}

func (db *DB) queueRetiredMemtableLocked(mem memtable.Table) {
	if db == nil || mem == nil {
		return
	}
	db.releaseBatchArenaLeasesForMemtable(mem)
	db.pendingRetiredMems = append(db.pendingRetiredMems, mem)
}

func (db *DB) queueRetiredMemtablesLocked(mems []memtable.Table) {
	if db == nil || len(mems) == 0 {
		return
	}
	for _, mem := range mems {
		db.queueRetiredMemtableLocked(mem)
	}
}

func (db *DB) popAppendOnlyMemLease() *memtable.AppendOnly {
	if db == nil {
		return nil
	}
	db.appendOnlyMemLeaseMu.Lock()
	defer db.appendOnlyMemLeaseMu.Unlock()
	n := len(db.appendOnlyMemLeases)
	if n == 0 {
		return nil
	}
	mt := db.appendOnlyMemLeases[n-1]
	db.appendOnlyMemLeases[n-1] = nil
	db.appendOnlyMemLeases = db.appendOnlyMemLeases[:n-1]
	return mt
}

func (db *DB) putAppendOnlyMemLease(mt *memtable.AppendOnly) bool {
	if db == nil || mt == nil {
		return false
	}
	db.appendOnlyMemLeaseMu.Lock()
	defer db.appendOnlyMemLeaseMu.Unlock()
	if len(db.appendOnlyMemLeases) >= maxAppendOnlyMemLeases {
		return false
	}
	db.appendOnlyMemLeases = append(db.appendOnlyMemLeases, mt)
	return true
}

func (db *DB) recycleMemtables(mems []memtable.Table) {
	if db == nil || len(mems) == 0 {
		return
	}
	for _, mt := range mems {
		switch typed := mt.(type) {
		case *memtable.AppendOnly:
			typed.Reset()
			if !db.putAppendOnlyMemLease(typed) {
				db.appendOnlyMemPool.Put(typed)
			}
		}
	}
}

func (db *DB) newMutableMemtableWithCapacityMode(capacity int, mode memtable.Mode) (memtable.Table, error) {
	if db != nil && mode == memtable.ModeAppendOnly {
		if mt := db.popAppendOnlyMemLease(); mt != nil {
			mt.Reset()
			return mt, nil
		}
		if v := db.appendOnlyMemPool.Get(); v != nil {
			if mt, ok := v.(*memtable.AppendOnly); ok && mt != nil {
				mt.Reset()
				return mt, nil
			}
		}
		estimate := 0
		if db.deferredValueLogEnabled() {
			estimate = appendOnlyEstimatedBytesPerEntryDeferredFence
		}
		return memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(capacity, estimate), nil
	}
	return memtable.NewWithCapacityModeAndIndexer(capacity, mode, db.hashSortedIndexer)
}

// publishMemtablesLocked publishes a new memtable snapshot.
// Caller must hold db.mu with a writer lock.
func (db *DB) publishMemtablesLocked() {
	view := &memtableView{}
	if len(db.mutableShards) > 0 {
		mutables := make([]memtable.Table, len(db.mutableShards))
		for i := range db.mutableShards {
			mutables[i] = db.mutableShards[i].mem
		}
		view.mutables = mutables
	}
	if len(db.queue) > 0 {
		q := make([]memtable.Table, len(db.queue))
		copy(q, db.queue)
		view.queue = q
	}
	if len(db.queueShardIDs) > 0 {
		qs := make([]uint16, len(db.queueShardIDs))
		copy(qs, db.queueShardIDs)
		view.queueShardIDs = qs
	}
	if len(db.queueRanges) > 0 {
		qr := make([]keyRange, len(db.queueRanges))
		copy(qr, db.queueRanges)
		view.queueRanges = qr
	}
	view.refs.Store(1)
	retired := db.pendingRetiredMems
	db.pendingRetiredMems = nil
	old := db.memtables.Swap(view)
	if old != nil {
		if len(retired) > 0 {
			old.retiredMems = append(old.retiredMems, retired...)
			old.deferredRetiredMemtables.Store(int64(len(old.retiredMems)))
		}
		db.releasePublishedMemtableView(old)
		return
	}
	db.recycleMemtables(retired)
}

// ensureQueueLaneIDsLocked keeps queueLaneIDs aligned with queue length.
// Caller must hold db.mu.
func (db *DB) ensureQueueLaneIDsLocked() {
	if len(db.queueLaneIDs) >= len(db.queue) {
		return
	}
	missing := len(db.queue) - len(db.queueLaneIDs)
	if missing <= 0 {
		return
	}
	db.queueLaneIDMisses.Add(int64(missing))
	db.queueLaneIDs = append(db.queueLaneIDs, make([]uint16, missing)...)
}

type memtableStats struct {
	writes          atomic.Uint64
	seqWrites       atomic.Uint64
	overwriteWrites atomic.Uint64
	iterators       atomic.Uint64
	rangeIters      atomic.Uint64
	lastKeyMu       sync.Mutex
	lastKey         []byte
	hasLastKey      bool
}

func (r *keyRange) add(key []byte) {
	if key == nil {
		return
	}
	if !r.valid {
		r.valid = true
		r.min = append([]byte(nil), key...)
		r.max = append([]byte(nil), key...)
		return
	}
	if bytes.Compare(key, r.min) < 0 {
		r.min = append(r.min[:0], key...)
	}
	if bytes.Compare(key, r.max) > 0 {
		r.max = append(r.max[:0], key...)
	}
}

func rangesOverlap(a, b keyRange) bool {
	if !a.valid || !b.valid {
		return false
	}
	// [a.min, a.max] overlaps [b.min, b.max] iff neither is strictly before the other.
	if bytes.Compare(a.max, b.min) < 0 {
		return false
	}
	if bytes.Compare(a.min, b.max) > 0 {
		return false
	}
	return true
}

// overlapsQuery checks if the query range [start, end) overlaps with the keyRange [r.min, r.max].
// nil start means -inf, nil end means +inf.
func overlapsQuery(start, end []byte, r keyRange) bool {
	if !r.valid {
		return false
	}
	// Range is [r.min, r.max]
	// Query is [start, end)

	// Check if Range is strictly before Query: r.max < start
	if start != nil && bytes.Compare(r.max, start) < 0 {
		return false
	}

	// Check if Range is strictly after Query: r.min >= end
	// Note: end is exclusive, so if r.min == end, it's outside.
	if end != nil && bytes.Compare(r.min, end) >= 0 {
		return false
	}

	return true
}

// queryCoversRange reports whether the query domain [start,end) fully covers the
// inclusive key range [r.min,r.max]. A nil bound is treated as -/+ infinity.
//
// Note: since end is exclusive, it must be strictly greater than r.max to cover
// the max key.
func queryCoversRange(start, end []byte, r keyRange) bool {
	if !r.valid {
		return true
	}
	if start != nil && bytes.Compare(start, r.min) > 0 {
		return false
	}
	if end != nil && bytes.Compare(end, r.max) <= 0 {
		return false
	}
	return true
}

func cloneRange(r keyRange) keyRange {
	if !r.valid {
		return r
	}
	r.min = append([]byte(nil), r.min...)
	r.max = append([]byte(nil), r.max...)
	return r
}

func (db *DB) snapshotMutableRange() keyRange {
	var out keyRange
	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		r := cloneRange(shard.rng)
		shard.mu.Unlock()
		if !r.valid {
			continue
		}
		if !out.valid {
			out = r
			continue
		}
		if bytes.Compare(r.min, out.min) < 0 {
			out.min = append(out.min[:0], r.min...)
		}
		if bytes.Compare(r.max, out.max) > 0 {
			out.max = append(out.max[:0], r.max...)
		}
	}
	return out
}

func (db *DB) resetMutableShardsLocked(nextMode memtable.Mode, reuse bool) error {
	db.mutableBytes.Store(0)
	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		reused := false
		if reuse {
			if r, ok := any(shard.mem).(interface{ Reset() }); ok {
				r.Reset()
				reused = true
			}
		}
		if !reused {
			db.queueRetiredMemtableLocked(shard.mem)
			mt, err := db.newMutableMemtableWithCapacityMode(0, nextMode)
			if err != nil {
				shard.mu.Unlock()
				return err
			}
			shard.mem = mt
		}
		shard.rng = keyRange{}
		shard.bytes = 0
		shard.mu.Unlock()
	}
	db.memtableStats.writes.Store(0)
	db.memtableStats.seqWrites.Store(0)
	db.memtableStats.overwriteWrites.Store(0)
	db.memtableStats.iterators.Store(0)
	db.memtableStats.rangeIters.Store(0)
	db.memtableStats.lastKeyMu.Lock()
	db.memtableStats.hasLastKey = false
	db.memtableStats.lastKeyMu.Unlock()
	db.updateAdaptiveObservationLocked()
	db.updateMutableThresholdLocked()
	db.publishMemtablesLocked()
	return nil
}

func (db *DB) noteWriteKey(key []byte) {
	if !db.memtableAdaptive || !db.memtableAdaptiveObserve.Load() {
		return
	}
	stats := &db.memtableStats
	stats.writes.Add(1)
	if len(key) == 0 {
		stats.lastKeyMu.Lock()
		stats.hasLastKey = false
		stats.lastKeyMu.Unlock()
		return
	}
	stats.lastKeyMu.Lock()
	defer stats.lastKeyMu.Unlock()
	if stats.hasLastKey {
		if bytes.Equal(stats.lastKey, key) {
			stats.overwriteWrites.Add(1)
		} else if bytes.Compare(stats.lastKey, key) < 0 {
			stats.seqWrites.Add(1)
		}
	}
	stats.lastKey = append(stats.lastKey[:0], key...)
	stats.hasLastKey = true
}

// noteWriteSortedRun records a strictly increasing key run in one shot.
func (db *DB) noteWriteSortedRun(first, last []byte, count int) {
	if !db.memtableAdaptive || !db.memtableAdaptiveObserve.Load() || count <= 0 {
		return
	}
	stats := &db.memtableStats
	stats.writes.Add(uint64(count))
	if len(last) == 0 {
		stats.lastKeyMu.Lock()
		stats.hasLastKey = false
		stats.lastKeyMu.Unlock()
		return
	}
	seqAdds := uint64(0)
	if count > 1 {
		seqAdds += uint64(count - 1)
	}
	stats.lastKeyMu.Lock()
	if len(first) > 0 && stats.hasLastKey && bytes.Compare(stats.lastKey, first) < 0 {
		seqAdds++
	}
	stats.lastKey = append(stats.lastKey[:0], last...)
	stats.hasLastKey = true
	stats.lastKeyMu.Unlock()
	if seqAdds > 0 {
		stats.seqWrites.Add(seqAdds)
	}
}

func (db *DB) noteIterator(start, end []byte) {
	if !db.memtableAdaptive || !db.memtableAdaptiveObserve.Load() {
		return
	}
	stats := &db.memtableStats
	stats.iterators.Add(1)
	if start != nil || end != nil {
		stats.rangeIters.Add(1)
	}
}

func (db *DB) updateMutableThresholdLocked() {
	threshold := db.flushThreshold
	if db.memtableWarmupActive && db.memtableWarmupThreshold > 0 && db.memtableWarmupThreshold < db.flushThreshold {
		threshold = db.memtableWarmupThreshold
	}
	db.mutableThreshold.Store(threshold)
}

func (db *DB) mutableFlushThreshold() int64 {
	return db.mutableThreshold.Load()
}

func (db *DB) chooseAdaptiveMemtableModeLocked() memtable.Mode {
	// Read stats atomially (no global lock needed for counts)
	writes := db.memtableStats.writes.Load()
	seqWrites := db.memtableStats.seqWrites.Load()
	overwriteWrites := db.memtableStats.overwriteWrites.Load()
	iters := db.memtableStats.iterators.Load()
	rangeIters := db.memtableStats.rangeIters.Load()

	// Default to configured mode if not enough data
	if writes < adaptiveMinWrites {
		return db.memtableMode
	}

	seqWritePct := float64(seqWrites) / float64(writes)
	overwriteWritePct := float64(overwriteWrites) / float64(writes)
	rangeIterPct := 0.0
	if iters > 0 {
		rangeIterPct = float64(rangeIters) / float64(iters)
	}

	// 1) Range-heavy read paths benefit most from BTree order stability.
	if rangeIterPct >= adaptiveRangeIteratorPct {
		return memtable.ModeBTree
	}

	// 2) Mostly increasing writes with low overwrite pressure favor append-only.
	if seqWritePct >= adaptiveSequentialWritePct && overwriteWritePct < adaptiveOverwriteWritePct {
		return memtable.ModeAppendOnly
	}

	// 3) Overwrite-heavy or mixed-write traffic defaults to hash-sorted.
	return memtable.ModeHashSorted
}

func (db *DB) updateAdaptiveObservationLocked() {
	observe := db.memtableAdaptive
	if observe && !db.memtableWarmupActive && db.memtableMode == memtable.ModeAppendOnly {
		observe = false
	}
	db.memtableAdaptiveObserve.Store(observe)
}

func (db *DB) applyAdaptiveMemtableModeLocked() memtable.Mode {
	desired := db.chooseAdaptiveMemtableModeLocked()
	db.memtableMode = desired
	db.updateAdaptiveObservationLocked()
	return desired
}

func validateValueLogDomainThresholds(domains []backenddb.ValueLogDomainThreshold) error {
	seen := make(map[string]struct{}, len(domains))
	for i := range domains {
		d := domains[i]
		if len(d.Prefix) == 0 {
			return fmt.Errorf("cachingdb: value-log domain threshold[%d] has empty prefix", i)
		}
		if d.InlineThreshold < 0 {
			return fmt.Errorf("cachingdb: value-log domain threshold[%d] has negative inline threshold %d", i, d.InlineThreshold)
		}
		key := string(d.Prefix)
		if _, dup := seen[key]; dup {
			return fmt.Errorf("cachingdb: duplicate value-log domain threshold prefix %q", d.Prefix)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func Open(dir string, backend BackendDB, opts Options) (*DB, error) {
	if !opts.AllowUnsafe && (opts.DisableWAL || opts.RelaxedSync || opts.DisableReadChecksum) {
		return nil, ErrUnsafeOptions
	}
	if opts.FlushThreshold <= 0 {
		opts.FlushThreshold = 256 * 1024 * 1024 // 256MB default
	}
	memCap := memtableCapacity(opts.FlushThreshold)
	modeStr := opts.MemtableMode
	if modeStr == "" {
		modeStr = "adaptive"
	}
	adaptive := false
	if modeStr == "adaptive" || modeStr == "auto" {
		adaptive = true
		modeStr = "append_only"
	} else if strings.HasPrefix(modeStr, "adaptive:") {
		adaptive = true
		modeStr = strings.TrimPrefix(modeStr, "adaptive:")
	}
	mode, err := memtable.ModeFromString(modeStr)
	if err != nil {
		return nil, err
	}
	shardCount := opts.MemtableShards
	if shardCount <= 0 {
		shardCount = defaultMemtableShards()
	}
	shardCount = normalizeShardCount(shardCount)
	if shardCount < 1 {
		shardCount = 1
	}
	domainIngressWorkers := opts.DomainIngressWorkers
	if domainIngressWorkers < 0 {
		domainIngressWorkers = 0
	}
	if domainIngressWorkers > shardCount {
		domainIngressWorkers = shardCount
	}
	domainIngressQueueSize := opts.DomainIngressQueueSize
	if domainIngressWorkers > 0 && domainIngressQueueSize <= 0 {
		domainIngressQueueSize = defaultDomainIngressQueueSize
	}
	if opts.MaxQueuedMemtables == 0 {
		// Keep the default queued backlog roughly stable in bytes when callers
		// tune FlushThreshold. Historically: 64MB flush threshold with a queue
		// length of 4 => ~256MB backlog.
		opts.MaxQueuedMemtables = defaultMaxQueuedMemtables(opts.FlushThreshold) * shardCount
	}
	writerFlushMaxMemtablesUnset := opts.WriterFlushMaxMemtables == 0
	if opts.WriterFlushMaxMemtables == 0 {
		opts.WriterFlushMaxMemtables = 1
	}
	flushBuildAutoConcurrency := opts.FlushBuildConcurrency <= 0
	if opts.FlushBuildConcurrency <= 0 {
		opts.FlushBuildConcurrency = runtime.GOMAXPROCS(0)
		if opts.FlushBuildConcurrency < 1 {
			opts.FlushBuildConcurrency = 1
		}
	}
	if opts.FlushBuildMinEntries <= 0 {
		opts.FlushBuildMinEntries = 16 * 1024
	}
	if opts.FlushBuildMinUnits <= 0 {
		opts.FlushBuildMinUnits = 2
	}
	if opts.FlushBuildChunkCap < 0 {
		opts.FlushBuildChunkCap = 8192
	}
	if opts.FlushBuildChunkTargetBytes <= 0 {
		opts.FlushBuildChunkTargetBytes = 2 << 20
	}
	if opts.FlushBuildChunkMinBytes <= 0 {
		opts.FlushBuildChunkMinBytes = 1 << 20
	}
	if opts.FlushBuildChunkMaxBytes <= 0 {
		opts.FlushBuildChunkMaxBytes = 4 << 20
	}
	if opts.FlushBuildPrefetchUnits <= 0 {
		opts.FlushBuildPrefetchUnits = opts.FlushBuildConcurrency
	}
	if opts.FlushBackendMaxEntries == 0 {
		// In relaxed durability modes, additional commit boundaries are cheap and
		// can reduce index.db high-watermark growth under small KeepRecent windows
		// by making retired pages eligible for reuse sooner. Default to a smaller
		// chunk size in that case.
		if opts.DisableWAL || opts.RelaxedSync {
			opts.FlushBackendMaxEntries = 2 * flushBackendBatchInitEntries
		} else {
			opts.FlushBackendMaxEntries = flushBackendBatchMaxEntries
		}
	} else if opts.FlushBackendMaxEntries < 0 {
		// Negative disables chunking; use a very large cap so the hot path
		// never triggers intermediate commits.
		opts.FlushBackendMaxEntries = int(^uint(0) >> 1)
		// Disable the max-batch cap when chunking is explicitly disabled.
		// This preserves the documented "<0 disables chunking" behavior without
		// accidentally re-enabling it via the cap adjustment logic.
		if opts.FlushBackendMaxBatches == 0 {
			opts.FlushBackendMaxBatches = -1
		}
	}
	if opts.FlushBackendMaxBatches == 0 {
		// In relaxed durability modes, additional commit boundaries are much
		// cheaper and can substantially reduce index.db high-watermark growth
		// under small KeepRecent windows by making retired pages eligible for
		// reuse sooner. Use a slightly higher default budget in that case.
		if opts.DisableWAL || opts.RelaxedSync {
			opts.FlushBackendMaxBatches = 32
		} else {
			opts.FlushBackendMaxBatches = 16
		}
	}
	flushBackendInitEntries := flushBackendBatchInitEntries
	if flushBackendInitEntries > opts.FlushBackendMaxEntries {
		flushBackendInitEntries = opts.FlushBackendMaxEntries
	}
	if flushBackendInitEntries < 1 {
		flushBackendInitEntries = 1
	}
	if err := validateValueLogDomainThresholds(opts.ValueLogDomainInlineThresholds); err != nil {
		return nil, err
	}

	// Ensure wal dir exists
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0700); err != nil {
		return nil, err
	}
	warnInsecureDir(walDir, opts.NotifyError)
	segments, _ := listNonEmptyLogSegments(walDir)
	maxLaneID := -1
	maxWALSeq := make(map[int]int)
	maxVlogSeq := make(map[int]int)
	for _, seg := range segments {
		if seg.lane > maxLaneID {
			maxLaneID = seg.lane
		}
		if seg.valueLog {
			if seg.seq > maxVlogSeq[seg.lane] {
				maxVlogSeq[seg.lane] = seg.seq
			}
		} else if seg.seq > maxWALSeq[seg.lane] {
			maxWALSeq[seg.lane] = seg.seq
		}
	}
	laneCount := opts.JournalLanes
	if laneCount <= 0 {
		laneCount = defaultJournalLaneCount(runtime.GOMAXPROCS(0))
	}
	if opts.ValueLogGenerationPolicy == 1 && laneCount < 3 {
		laneCount = 3
	}
	// Temporarily remove the logic that increases laneCount based on maxLaneID
	if maxLaneID+1 > laneCount {
		laneCount = maxLaneID + 1
	}

	inlineThreshold := page.DefaultInlineThreshold
	if provider, ok := backend.(interface{ InlineThreshold() int }); ok {
		if v := provider.InlineThreshold(); v >= 0 {
			inlineThreshold = v
		}
	}
	// In relaxed durability modes, storing moderate values out-of-line avoids a
	// catastrophic random_write cliff at large key counts (perf gate II / #229).
	//
	// We pick a default threshold that still keeps small values inline, but
	// pushes the unified-bench default 128B values into the value log.
	const defaultRelaxedValueLogThreshold = 127
	valueLogThreshold := opts.ValueLogPointerThreshold
	if valueLogThreshold <= 0 {
		valueLogThreshold = page.DefaultInlineThreshold
		if opts.DisableWAL || opts.RelaxedSync {
			valueLogThreshold = defaultRelaxedValueLogThreshold
		}
	}
	valueLogDomainThresholds := backenddb.NormalizeValueLogDomainThresholds(opts.ValueLogDomainInlineThresholds)
	valueLogMaxSegmentBytes := opts.ValueLogMaxSegmentBytes
	if valueLogMaxSegmentBytes < 0 {
		valueLogMaxSegmentBytes = 0
	}
	valueLogGenerationPolicy := opts.ValueLogGenerationPolicy
	if valueLogGenerationPolicy > 1 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generation policy %d", valueLogGenerationPolicy)
	}
	valueLogGenerationHotTarget := opts.ValueLogGenerationHotSegmentTargetBytes
	valueLogGenerationWarmTarget := opts.ValueLogGenerationWarmSegmentTargetBytes
	valueLogGenerationColdTarget := opts.ValueLogGenerationColdSegmentTargetBytes
	valueLogRewriteBudgetBytes := opts.ValueLogRewriteBudgetBytesPerSec
	valueLogRewriteBudgetRecords := opts.ValueLogRewriteBudgetRecordsPerSec
	valueLogRewriteTriggerRatioPPM := opts.ValueLogRewriteTriggerStaleRatioPPM
	valueLogRewriteTriggerBytes := opts.ValueLogRewriteTriggerTotalBytes
	valueLogRewriteTriggerChurn := opts.ValueLogRewriteTriggerChurnPerSec
	if valueLogGenerationHotTarget < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generational hot segment target bytes %d", valueLogGenerationHotTarget)
	}
	if valueLogGenerationWarmTarget < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generational warm segment target bytes %d", valueLogGenerationWarmTarget)
	}
	if valueLogGenerationColdTarget < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generational cold segment target bytes %d", valueLogGenerationColdTarget)
	}
	if valueLogRewriteBudgetBytes < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generational rewrite budget bytes/sec %d", valueLogRewriteBudgetBytes)
	}
	if valueLogRewriteBudgetRecords < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generational rewrite budget records/sec %d", valueLogRewriteBudgetRecords)
	}
	if valueLogRewriteTriggerBytes < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generational rewrite trigger total bytes %d", valueLogRewriteTriggerBytes)
	}
	if valueLogRewriteTriggerChurn < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generational rewrite trigger churn/sec %d", valueLogRewriteTriggerChurn)
	}
	valueLogRawWritevMinAvgBytes := opts.ValueLogRawWritevMinAvgBytes
	if valueLogRawWritevMinAvgBytes < 0 {
		valueLogRawWritevMinAvgBytes = 0
	}
	valueLogRawWritevMinRecords := opts.ValueLogRawWritevMinBatchRecords
	if valueLogRawWritevMinRecords <= 0 {
		valueLogRawWritevMinRecords = 8
	}
	indexOuterLeafMode := strings.ToLower(strings.TrimSpace(opts.IndexOuterLeafMode))
	if indexOuterLeafMode == "" {
		indexOuterLeafMode = backenddb.IndexOuterLeafModeV2FencePtr
	}
	if indexOuterLeafMode != backenddb.IndexOuterLeafModeV2BlockPtr &&
		indexOuterLeafMode != backenddb.IndexOuterLeafModeV2FencePtr &&
		indexOuterLeafMode != backenddb.IndexOuterLeafModeV1LeafLog &&
		indexOuterLeafMode != backenddb.IndexOuterLeafModeV1LeafLogRoute &&
		indexOuterLeafMode != backenddb.IndexOuterLeafModeV1LeafLogLegacy {
		indexOuterLeafMode = backenddb.IndexOuterLeafModeV1
	}
	rawValueLogWALFenceMode := strings.TrimSpace(opts.ValueLogWALFenceMode)
	valueLogWALFenceMode := normalizeValueLogWALFenceMode(rawValueLogWALFenceMode)
	if valueLogWALFenceMode != string(backenddb.ValueLogWALFenceModeRIDJoin) &&
		valueLogWALFenceMode != string(backenddb.ValueLogWALFenceModeSimpleInline) {
		return nil, fmt.Errorf("cachingdb: invalid value-log WAL fence mode %q", opts.ValueLogWALFenceMode)
	}
	if indexOuterLeafMode != backenddb.IndexOuterLeafModeV2FencePtr &&
		valueLogWALFenceMode == string(backenddb.ValueLogWALFenceModeSimpleInline) {
		return nil, fmt.Errorf("cachingdb: value-log WAL fence mode %q requires index outer leaf mode %q", opts.ValueLogWALFenceMode, backenddb.IndexOuterLeafModeV2FencePtr)
	}
	outerLeafBlockTargetOpt := opts.ValueLogOuterLeafBlockTargetBytes
	if outerLeafBlockTargetOpt <= 0 && indexOuterLeafMode == backenddb.IndexOuterLeafModeV2FencePtr {
		outerLeafBlockTargetOpt = defaultOuterLeafFenceBlockTargetBytes
	}
	if outerLeafBlockTargetOpt <= 0 && indexOuterLeafMode == backenddb.IndexOuterLeafModeV1LeafLogRoute {
		// Route mode payload blocks benefit from larger grouping for compression
		// density while keeping decode cost bounded in the hot read path.
		outerLeafBlockTargetOpt = 8 << 10
	}
	outerLeafBlockTarget := outerleaf.NormalizeBlockTargetBytes(outerLeafBlockTargetOpt)
	outerLeafBlockCodec := opts.ValueLogOuterLeafBlockCodec
	outerLeafBlockRestart := outerleaf.NormalizeRestartInterval(opts.ValueLogOuterLeafBlockRestartInterval)
	outerLeafBlobThreshold := opts.ValueLogOuterLeafBlobThresholdBytes
	if outerLeafBlobThreshold < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log outer leaf blob threshold bytes %d", outerLeafBlobThreshold)
	}
	disableJournal := opts.DisableWAL
	if indexOuterLeafMode == backenddb.IndexOuterLeafModeV2FencePtr && !disableJournal {
		// Keep existing default semantics: WAL-on v2_fenceptr defaults to
		// simple_inline unless caller explicitly chooses a compatible mode.
		if rawValueLogWALFenceMode == "" {
			valueLogWALFenceMode = string(backenddb.ValueLogWALFenceModeSimpleInline)
		}
	}
	if writerFlushMaxMemtablesUnset &&
		disableJournal &&
		indexOuterLeafMode == backenddb.IndexOuterLeafModeV2FencePtr &&
		opts.WriterFlushMaxMemtables < v2DeferredAssistSlowdownMinMemtables {
		opts.WriterFlushMaxMemtables = v2DeferredAssistSlowdownMinMemtables
	}
	var retained map[string]struct{}
	for _, seg := range segments {
		if !seg.valueLog {
			continue
		}
		if retained == nil {
			retained = make(map[string]struct{})
		}
		retained[seg.path] = struct{}{}
	}

	warmupThreshold := opts.FlushThreshold
	if adaptive && adaptiveWarmupBytes > 0 && int64(adaptiveWarmupBytes) < opts.FlushThreshold {
		warmupThreshold = int64(adaptiveWarmupBytes)
	}
	memCap = shardCapacity(memCap, shardCount)
	warmupCap := shardCapacity(memtableCapacity(warmupThreshold), shardCount)
	indexer := memtable.NewHashSortedIndexer()
	mutableShards := make([]memShard, shardCount)
	appendOnlyEstimate := 0
	if mode == memtable.ModeAppendOnly && indexOuterLeafMode == backenddb.IndexOuterLeafModeV2FencePtr {
		appendOnlyEstimate = appendOnlyEstimatedBytesPerEntryDeferredFence
	}
	for i := range mutableShards {
		if mode == memtable.ModeAppendOnly {
			mutableShards[i] = memShard{mem: memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(warmupCap, appendOnlyEstimate)}
			continue
		}
		mt, err := memtable.NewWithCapacityModeAndIndexer(warmupCap, mode, indexer)
		if err != nil {
			return nil, err
		}
		mutableShards[i] = memShard{mem: mt}
	}
	reader, err := valuelog.NewManager(walDir)
	if err != nil {
		return nil, err
	}
	reader.SetDisableReadChecksum(opts.DisableReadChecksum)
	valueLogReader := reader
	debugFlushPointers := envBool(envDebugFlushPointers)
	debugFlushTiming := envBool(envDebugFlushTiming)
	debugV1LeafLogInvariants := envBool(envDebugV1LeafLogInvariants)
	debugV1LeafLogInvariantsPanic := envBool(envDebugV1LeafLogInvariantsPanic)

	valueLogCompressionMode := normalizeVlogCompressionMode(opts.ValueLogCompression)
	if valueLogCompressionMode == vlogCompressionDefault {
		// ValueLogCompression=0 is "unset/default" and resolves to auto mode.
		valueLogCompressionMode = vlogCompressionAuto
	}
	valueLogBlockCodec := normalizeVlogBlockCodec(opts.ValueLogBlockCodec)
	valueLogBlockTargetBytes := valuelog.NormalizeBlockTargetCompressedBytes(opts.ValueLogBlockTargetCompressedBytes)
	valueLogIncompressibleHold := opts.ValueLogIncompressibleHoldBytes
	if valueLogIncompressibleHold <= 0 {
		valueLogIncompressibleHold = defaultVlogHoldBytes
	}
	if valueLogIncompressibleHold < 64<<10 {
		valueLogIncompressibleHold = 64 << 10
	}
	valueLogIncompressibleProbe := opts.ValueLogIncompressibleProbeBytes
	if valueLogIncompressibleProbe <= 0 {
		valueLogIncompressibleProbe = defaultVlogProbeBytes
	}
	if valueLogIncompressibleProbe < 64<<10 {
		valueLogIncompressibleProbe = 64 << 10
	}
	if valueLogIncompressibleProbe > valueLogIncompressibleHold {
		valueLogIncompressibleProbe = valueLogIncompressibleHold
	}
	valueLogAutoPolicy := normalizeVlogAutoPolicy(opts.ValueLogAutoPolicy)

	valueLogDictTrain := opts.ValueLogDictTrain
	if valueLogCompressionMode == vlogCompressionOff || valueLogCompressionMode == vlogCompressionBlock {
		// Explicit off/block mode bypasses dictionary writes and training.
		valueLogDictTrain.TrainBytes = -1
	}
	valueLogDictMaxK := opts.ValueLogDictMaxK
	if valueLogDictMaxK <= 0 {
		valueLogDictMaxK = 32
	}
	if valueLogDictMaxK < 1 {
		valueLogDictMaxK = 1
	}
	if valueLogDictMaxK > valuelog.MaxFrameK {
		valueLogDictMaxK = valuelog.MaxFrameK
	}

	valueLogDictFrameEncodeLevel := opts.ValueLogDictFrameEncodeLevel
	if valueLogDictFrameEncodeLevel <= 0 {
		valueLogDictFrameEncodeLevel = zstd.SpeedFastest
	}
	if valueLogDictFrameEncodeLevel < zstd.SpeedFastest || valueLogDictFrameEncodeLevel > zstd.SpeedBestCompression {
		valueLogDictFrameEncodeLevel = zstd.SpeedFastest
	}
	valueLogDictFrameEnableEntropy := opts.ValueLogDictFrameEnableEntropy

	valueLogDictAdaptiveRatio := opts.ValueLogDictAdaptiveRatio
	valueLogDictMetricsWindow := opts.ValueLogDictMetricsWindowBytes
	valueLogDictMetricsMinRecords := opts.ValueLogDictMetricsMinRecords
	valueLogDictMetricsPauseBytes := opts.ValueLogDictMetricsPauseBytes

	minPayloadSavings := opts.ValueLogDictMinPayloadSavingsRatio
	if minPayloadSavings <= 0 {
		// Throughput-oriented default: avoid publishing dictionaries unless they
		// deliver clear payload reduction. This keeps dict-enabled mode close to
		// raw mode on incompressible streams.
		minPayloadSavings = 0.02
		if opts.ForceValueLogPointers || opts.DisableWAL {
			minPayloadSavings = 0.05
		}
	}

	valueLogAutotuneCandidateKSet := len(opts.ValueLogCompressionAutotune.CandidateK) > 0
	valueLogAutotune := valuelog.NormalizeAutotuneOptions(opts.ValueLogCompressionAutotune, true)
	valueLogTemplateEnabled := opts.ValueLogTemplateMode != template.TemplateOff
	valueLogTemplateCfg := template.NormalizeConfig(opts.ValueLogTemplateConfig)
	if opts.ValueLogTemplateMode == template.TemplatePrepass {
		// TemplatePrepass can be CPU-heavy (template match + dict/zstd encode). If
		// templates have not been kept recently, enter cold mode sooner so we don't
		// pay candidate lookup/matching cost on every value.
		if opts.ValueLogTemplateConfig.ColdSearchAfter <= 0 {
			valueLogTemplateCfg.ColdSearchAfter = 64
		}
		if opts.ValueLogTemplateConfig.ColdSearchProbeEvery <= 0 {
			valueLogTemplateCfg.ColdSearchProbeEvery = 64
		}
	}
	valueLogTemplateDecodeOpts := template.DecodeOptions{MaxGaps: valueLogTemplateCfg.MaxGaps, MaxDecodedBytes: valueLogTemplateCfg.MaxDecodedBytes, DefCacheSize: valueLogTemplateCfg.DefCacheSize}
	if valueLogTemplateDecodeOpts.MaxDecodedBytes <= 0 && limits.MaxRecordSize > 0 {
		valueLogTemplateDecodeOpts.MaxDecodedBytes = int(limits.MaxRecordSize)
	}

	// Favor aggressive sampling so the first dict arrives quickly. The trainer
	// still caps total work via TrainBytes and queue backpressure.
	if valueLogDictTrain.TrainBytes > 0 && valueLogDictTrain.SampleStride == 0 {
		valueLogDictTrain.SampleStride = 1
	}

	// If dict training is enabled but no adaptive ratio is specified, default to
	// a conservative pause threshold to avoid wasting CPU on incompressible
	// payload streams.
	if valueLogDictTrain.TrainBytes > 0 && valueLogDictAdaptiveRatio == 0 {
		// Require meaningful savings before staying in "dict mode". Payload ratios
		// close to 1.0 can be slower than raw frames due to additional framing and
		// encode/decode overhead, especially for small values and write-heavy batch
		// workloads.
		valueLogDictAdaptiveRatio = 0.98
	}
	if valueLogDictAdaptiveRatio > 0 {
		if valueLogDictMetricsWindow <= 0 {
			// Smaller windows let us detect "no-op" dict streams quickly and avoid
			// spending long stretches in the slower dict framing path on
			// incompressible payloads.
			valueLogDictMetricsWindow = 256 << 10
		}
		if valueLogDictMetricsPauseBytes <= 0 && valueLogAutotune.Mode != valuelog.AutotuneOff && valueLogAutotune.PauseBytes > 0 {
			valueLogDictMetricsPauseBytes = int(valueLogAutotune.PauseBytes)
		}
		if valueLogDictMetricsPauseBytes <= 0 {
			// Degraded streams should stay paused long enough that "dict enabled"
			// is effectively free on incompressible data, while still allowing
			// occasional probes to detect when compressibility returns.
			valueLogDictMetricsPauseBytes = 64 << 20
		}
	}

	// When dict compression is paused, periodically probe compression to recover
	// quickly if the payload stream becomes compressible again.
	probeBytes := valueLogDictMetricsPauseBytes
	if probeBytes <= 0 && valueLogAutotune.Mode != valuelog.AutotuneOff && valueLogAutotune.ProbeBytes > 0 {
		probeBytes = int(valueLogAutotune.ProbeBytes)
	}
	if probeBytes <= 0 {
		probeBytes = 64 << 20
	}
	probeBytes /= 4
	if probeBytes < 64<<10 {
		probeBytes = 64 << 10
	}
	incompressibleHoldBytes := opts.ValueLogDictIncompressibleHoldBytes
	if incompressibleHoldBytes < 0 {
		incompressibleHoldBytes = 0
	}
	if incompressibleHoldBytes > 0 && incompressibleHoldBytes < 8<<20 {
		incompressibleHoldBytes = 8 << 20
	}
	incompressibleProbeBytes := opts.ValueLogDictProbeIntervalBytes
	if incompressibleHoldBytes > 0 {
		if incompressibleProbeBytes <= 0 {
			incompressibleProbeBytes = incompressibleHoldBytes / 8
		}
		if incompressibleProbeBytes < 64<<10 {
			incompressibleProbeBytes = 64 << 10
		}
		if incompressibleProbeBytes > incompressibleHoldBytes {
			incompressibleProbeBytes = incompressibleHoldBytes
		}
	} else {
		incompressibleProbeBytes = 0
	}
	// While paused on degraded/incompressible streams, keep sampling sparse to
	// minimize hot-path CPU overhead.
	pausedSampleStride := uint64(256)
	selectorSeedCodec := valueLogBlockCodec
	if valueLogCompressionMode == vlogCompressionAuto && valueLogAutoPolicy != vlogAutoThroughput {
		selectorSeedCodec = valuelog.BlockCodecLZ4
	}

	lanes := make([]lane, laneCount)
	for i := range lanes {
		lanes[i].id = i
		lanes[i].walSeq = maxWALSeq[i]
		lanes[i].vlogSeq = maxVlogSeq[i]
		lanes[i].vlogGenerationClass = vlogGenerationClassHot
		lanes[i].vlogCompressionSelector = newVlogCompressionSelectorWithSeed(
			valueLogAutoPolicy,
			uint64(valueLogIncompressibleHold),
			uint64(valueLogIncompressibleProbe),
			selectorSeedCodec,
		)
	}
	if valueLogGenerationPolicy == 1 && len(lanes) >= 3 {
		// Reserve one lane for warm and one for cold; remaining lanes serve hot.
		lanes[1].vlogGenerationClass = vlogGenerationClassWarm
		lanes[2].vlogGenerationClass = vlogGenerationClassCold
	}
	db := &DB{
		dir:                                  walDir,
		backend:                              backend,
		flushThreshold:                       opts.FlushThreshold,
		memtableCap:                          memCap,
		memtableMode:                         mode,
		memtableAdaptive:                     adaptive,
		memtableWarmupActive:                 adaptive && warmupThreshold < opts.FlushThreshold,
		memtableWarmupThreshold:              warmupThreshold,
		domainIngressWorkers:                 domainIngressWorkers,
		domainIngressQueueSize:               domainIngressQueueSize,
		maxQueuedMemtables:                   opts.MaxQueuedMemtables,
		slowdownBacklogSeconds:               opts.SlowdownBacklogSeconds,
		stopBacklogSeconds:                   opts.StopBacklogSeconds,
		maxBacklogBytes:                      opts.MaxBacklogBytes,
		writerFlushMaxMemtables:              opts.WriterFlushMaxMemtables,
		writerFlushMaxDuration:               opts.WriterFlushMaxDuration,
		flushBuildConcurrency:                opts.FlushBuildConcurrency,
		flushBuildAutoConcurrency:            flushBuildAutoConcurrency,
		flushBuildMinEntries:                 opts.FlushBuildMinEntries,
		flushBuildMinUnits:                   opts.FlushBuildMinUnits,
		flushBuildChunkCap:                   opts.FlushBuildChunkCap,
		flushBuildChunkTarget:                opts.FlushBuildChunkTargetBytes,
		flushBuildChunkMinBytes:              opts.FlushBuildChunkMinBytes,
		flushBuildChunkMaxBytes:              opts.FlushBuildChunkMaxBytes,
		flushBuildPrefetchUnits:              opts.FlushBuildPrefetchUnits,
		flushBackendMaxEntries:               opts.FlushBackendMaxEntries,
		flushBackendInitEntries:              flushBackendInitEntries,
		flushBackendMaxBatches:               opts.FlushBackendMaxBatches,
		walMaxSegmentBytes:                   opts.WALMaxSegmentBytes,
		valueLogMaxSegmentBytes:              valueLogMaxSegmentBytes,
		journalCompression:                   opts.JournalCompression,
		disableJournal:                       disableJournal,
		disableValueLog:                      false,
		splitValueLog:                        true,
		relaxedSync:                          opts.RelaxedSync,
		notifyError:                          opts.NotifyError,
		inlineThreshold:                      inlineThreshold,
		valueLogThreshold:                    valueLogThreshold,
		valueLogDomainThresholds:             valueLogDomainThresholds,
		indexOuterLeafMode:                   indexOuterLeafMode,
		valueLogWALFenceMode:                 valueLogWALFenceMode,
		outerLeafBlockTargetBytes:            outerLeafBlockTarget,
		outerLeafBlockCodec:                  outerLeafBlockCodec,
		outerLeafBlockRestart:                outerLeafBlockRestart,
		outerLeafBlobThresholdBytes:          outerLeafBlobThreshold,
		forceValueLogPointers:                opts.ForceValueLogPointers,
		valueLogRawWritevMinAvgBytes:         valueLogRawWritevMinAvgBytes,
		valueLogRawWritevMinRecords:          valueLogRawWritevMinRecords,
		valueLogCompressionMode:              uint8(valueLogCompressionMode),
		valueLogBlockCodec:                   valueLogBlockCodec,
		valueLogBlockTargetBytes:             valueLogBlockTargetBytes,
		valueLogIncompressibleHold:           uint64(valueLogIncompressibleHold),
		valueLogIncompressibleProbe:          uint64(valueLogIncompressibleProbe),
		valueLogAutoPolicy:                   uint8(valueLogAutoPolicy),
		valueLogGenerationPolicy:             valueLogGenerationPolicy,
		valueLogGenerationHotTarget:          valueLogGenerationHotTarget,
		valueLogGenerationWarmTarget:         valueLogGenerationWarmTarget,
		valueLogGenerationColdTarget:         valueLogGenerationColdTarget,
		valueLogRewriteBudgetBytes:           valueLogRewriteBudgetBytes,
		valueLogRewriteBudgetRecords:         valueLogRewriteBudgetRecords,
		valueLogRewriteTriggerRatioPPM:       valueLogRewriteTriggerRatioPPM,
		valueLogRewriteTriggerBytes:          valueLogRewriteTriggerBytes,
		valueLogRewriteTriggerChurn:          valueLogRewriteTriggerChurn,
		memtableValueLogPointers:             true,
		valueLogReader:                       valueLogReader,
		valueLogRetain:                       retained,
		debugFlushPointers:                   debugFlushPointers,
		debugFlushTiming:                     debugFlushTiming,
		debugV1LeafLogInvariants:             debugV1LeafLogInvariants,
		debugV1LeafLogInvariantsPanic:        debugV1LeafLogInvariantsPanic,
		maxValueLogRetainedBytes:             opts.MaxValueLogRetainedBytes,
		maxValueLogRetainedBytesHard:         opts.MaxValueLogRetainedBytesHard,
		valueLogDictTrain:                    valueLogDictTrain,
		valueLogDictMaxK:                     valueLogDictMaxK,
		valueLogDictFrameEncodeLevel:         valueLogDictFrameEncodeLevel,
		valueLogDictFrameEnableEntropy:       valueLogDictFrameEnableEntropy,
		valueLogDictAdaptiveRatio:            valueLogDictAdaptiveRatio,
		valueLogDictMinPayloadSavings:        minPayloadSavings,
		valueLogDictMetricsWindow:            valueLogDictMetricsWindow,
		valueLogDictMetricsMinRecords:        valueLogDictMetricsMinRecords,
		valueLogDictMetricsPauseBytes:        valueLogDictMetricsPauseBytes,
		valueLogDictProbeBytes:               uint64(probeBytes),
		valueLogDictIncompressibleHoldBytes:  uint64(incompressibleHoldBytes),
		valueLogDictIncompressibleProbeBytes: uint64(incompressibleProbeBytes),
		valueLogDictPausedSampleStride:       pausedSampleStride,
		valueLogAutotuneOptions:              valueLogAutotune,
		valueLogAutotuneCandidateKSet:        valueLogAutotuneCandidateKSet,
		valueLogTemplateEnabled:              valueLogTemplateEnabled,
		valueLogTemplateMode:                 opts.ValueLogTemplateMode,
		valueLogTemplateReadStrict:           opts.ValueLogTemplateReadStrict,
		valueLogTemplateDecodeOpts:           valueLogTemplateDecodeOpts,
		valueLogTemplateEngine: func() *template.Engine {
			if !valueLogTemplateEnabled {
				return nil
			}
			return template.NewEngine(valueLogTemplateCfg)
		}(),
		mutableShards:         mutableShards,
		mutableShardMask:      uint64(shardCount - 1),
		hashSortedIndexer:     indexer,
		closeCh:               make(chan struct{}),
		flushCh:               make(chan struct{}, 1),
		autoCheckpointOnceCh:  make(chan struct{}, 1),
		autoCheckpointWriteCh: make(chan struct{}, 1),
		lanes:                 lanes,
		flushLaneMu:           make([]sync.Mutex, len(lanes)),
	}
	db.rebuildGenerationLaneSets()
	if db.indexOuterLeafMode == backenddb.IndexOuterLeafModeV1LeafLogRoute {
		// Route mode must regroup key/value mutations into directory-backed
		// outer-leaf blocks at flush time. Per-key pointer memtable rows can
		// collapse by shared record and leave stale exact rows reachable.
		db.memtableValueLogPointers = false
	}
	db.valueLogAutotuneMetrics.init(valuelog.RealClock{})
	db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerDisabled)
	db.bpCond = sync.NewCond(&db.bpMu)
	db.laneCond = sync.NewCond(&db.laneMu)
	db.checkpointCond = sync.NewCond(&db.checkpointMu)
	db.updateAdaptiveObservationLocked()
	nowNS := time.Now().UnixNano()
	db.materializationLastDrainUnixNano.Store(nowNS)
	db.publishWatermarkLastUnixNano = nowNS

	if db.indexOuterLeafMode == backenddb.IndexOuterLeafModeV1LeafLogRoute && !db.supportsFenceAnchorPromotion() {
		return nil, errors.New("cachingdb: index outer leaf mode v1_leaflog_route requires snapshot-capable backend and value-log reader")
	}

	// Open initial value-log segments (if enabled) and journal/commit log
	// segments (if enabled). Journal and value log are decoupled.
	if db.valueLogEnabled() {
		for i := range db.lanes {
			if err := db.rotateValueLogLocked(&db.lanes[i]); err != nil {
				if db.valueLogReader != nil {
					_ = db.valueLogReader.Close()
					db.valueLogReader = nil
				}
				for j := 0; j <= i && j < len(db.lanes); j++ {
					db.cleanupLaneWALWriters(&db.lanes[j])
				}
				return nil, err
			}
		}
	}
	if !db.disableJournal {
		for i := range db.lanes {
			if err := db.rotateWALLocked(&db.lanes[i]); err != nil {
				if db.valueLogReader != nil {
					_ = db.valueLogReader.Close()
					db.valueLogReader = nil
				}
				for j := 0; j <= i && j < len(db.lanes); j++ {
					db.cleanupLaneWALWriters(&db.lanes[j])
				}
				return nil, err
			}
		}
	}
	if len(segments) > 0 {
		for _, seg := range segments {
			if seg.lane < 0 || seg.lane >= len(db.lanes) {
				continue
			}
			l := &db.lanes[seg.lane]
			if db.splitValueLog {
				if seg.valueLog {
					if seg.path == l.vlogPath {
						continue
					}
					if l.vlogClosedSizes == nil {
						l.vlogClosedSizes = make(map[string]int64)
					}
					l.vlogClosedSizes[seg.path] = seg.size
					l.vlogClosedBytes.Add(seg.size)
				} else {
					if seg.path == l.walPath {
						continue
					}
					if l.walClosedSizes == nil {
						l.walClosedSizes = make(map[string]int64)
					}
					l.walClosedSizes[seg.path] = seg.size
					l.walClosedBytes.Add(seg.size)
				}
				continue
			}
			if seg.valueLog != db.walUsesValueLog() {
				continue
			}
			if seg.path == l.walPath {
				continue
			}
			if l.walClosedSizes == nil {
				l.walClosedSizes = make(map[string]int64)
			}
			l.walClosedSizes[seg.path] = seg.size
			l.walClosedBytes.Add(seg.size)
		}
	}
	if !db.disableJournal {
		for i := range db.lanes {
			db.startWALWriter(&db.lanes[i])
		}
	}
	if db.valueLogEnabled() {
		for i := range db.lanes {
			db.startVlogWriter(&db.lanes[i])
		}
	}

	if opts.IndexOuterLeavesInValueLog {
		setter, ok := backend.(interface{ SetLeafPageLog(backenddb.LeafPageLog) })
		if !ok {
			return nil, errors.New("cachingdb: backend does not support value-log leaf pages")
		}
		setter.SetLeafPageLog(newCachingLeafPageLog(db, 0))
	}

	// Publish initial memtable snapshot for lock-free reads.
	db.mu.Lock()
	db.updateMutableThresholdLocked()
	db.publishMemtablesLocked()
	db.mu.Unlock()

	db.startDomainIngressWorkers()

	// Start background flusher
	db.wg.Add(1)
	go db.flushLoop()
	db.startVlogGenerationLoop()

	return db, nil
}

type flushBuildJob struct {
	mem      memtable.Table
	out      chan<- []batch.Entry
	cancel   <-chan struct{}
	chunkCap int
	errCh    chan<- error
}

func (job flushBuildJob) report(err error) {
	if err == nil || job.errCh == nil {
		return
	}
	select {
	case job.errCh <- err:
	default:
	}
}

func (job flushBuildJob) run(closeCh <-chan struct{}) {
	if job.mem == nil || job.out == nil {
		if job.out != nil {
			close(job.out)
		}
		job.report(errors.New("cachingdb: flush build job missing memtable/out"))
		return
	}
	chunkCap := job.chunkCap
	if chunkCap <= 0 {
		chunkCap = 8192
	}

	iter := job.mem.NewIterator(nil, nil)

	send := func(ops []batch.Entry) bool {
		for {
			select {
			case job.out <- ops:
				return true
			case <-job.cancel:
				return false
			case <-closeCh:
				return false
			}
		}
	}

	ops := getEntrySlice(chunkCap)
	ops = ops[:0]
	for iter.Valid() {
		val, ptr, flags := iter.UnsafeEntry()
		if flags&node.FlagTombstone != 0 {
			ops = append(ops, batch.Entry{
				Type: batch.OpDelete,
				Key:  iter.UnsafeKey(),
			})
		} else if flags&node.FlagPointer != 0 {
			ops = append(ops, batch.Entry{
				Type:     batch.OpPut,
				Key:      iter.UnsafeKey(),
				ValuePtr: ptr,
				IsPtr:    true,
			})
		} else {
			ops = append(ops, batch.Entry{
				Type:  batch.OpPut,
				Key:   iter.UnsafeKey(),
				Value: val,
			})
		}
		iter.Next()

		if len(ops) >= cap(ops) {
			if !send(ops) {
				putEntrySlice(ops)
				close(job.out)
				return
			}
			ops = getEntrySlice(chunkCap)
			ops = ops[:0]
		}
	}

	err := iter.Error()
	cerr := iter.Close()
	if err == nil {
		err = cerr
	}
	if err != nil {
		putEntrySlice(ops)
		close(job.out)
		job.report(err)
		return
	}
	if len(ops) > 0 {
		if !send(ops) {
			putEntrySlice(ops)
			close(job.out)
			return
		}
	} else {
		putEntrySlice(ops)
	}
	close(job.out)
}

func (db *DB) reportError(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, errDBClosing) {
		return
	}
	if db != nil && db.closing.Load() {
		return
	}
	if db.notifyError != nil {
		db.notifyError(err)
	}
	db.bgErrMu.Lock()
	if db.bgErr == nil {
		db.bgErr = err
	}
	db.bgErrMu.Unlock()
}

func (db *DB) backgroundError() error {
	db.bgErrMu.Lock()
	defer db.bgErrMu.Unlock()
	return db.bgErr
}

// StartAutoCheckpoint enables a background loop that periodically forces a
// durable boundary and trims cached-mode WAL segments. When idleInterval > 0,
// it also triggers an opportunistic checkpoint after a period of write-idleness.
//
// interval > 0 enables periodic checkpoints. maxWALBytes is a safety cap: if > 0,
// the loop will attempt to checkpoint when the effective WAL bytes exceed this
// cap. maxWALBytes <= 0 disables the size trigger.
//
// This does not make each individual write durable; it bounds the window of
// unsynced writes for long-running workloads.
func (db *DB) StartAutoCheckpoint(interval time.Duration, maxWALBytes int64, idleInterval time.Duration) {
	if db == nil {
		return
	}
	db.autoCheckpointMaxWALBytes.Store(maxWALBytes)
	if maxWALBytes > 0 {
		db.autoCheckpointSizeArmed.Store(true)
	}
	if interval <= 0 && idleInterval <= 0 && maxWALBytes <= 0 {
		return
	}
	if !db.autoCheckpointOn.CompareAndSwap(false, true) {
		return
	}
	db.wg.Add(1)
	go db.autoCheckpointLoop(interval, maxWALBytes, idleInterval)
}

// TriggerAutoCheckpoint schedules a best-effort immediate auto-checkpoint pass.
func (db *DB) TriggerAutoCheckpoint() {
	if db == nil || !db.autoCheckpointOn.Load() {
		return
	}
	select {
	case db.autoCheckpointOnceCh <- struct{}{}:
	default:
	}
}

func (db *DB) noteWrite() {
	if db == nil || !db.autoCheckpointOn.Load() {
		return
	}
	if db.disableJournal {
		return
	}
	const autoCheckpointWriteEveryBytes int64 = 1 << 20
	current := db.effectiveWALBytes()
	if current <= 0 {
		return
	}
	threshold := autoCheckpointWriteEveryBytes
	if max := db.autoCheckpointMaxWALBytes.Load(); max > 0 {
		// Rearm size-triggered checkpoints once WAL bytes drop substantially.
		if current < max/2 {
			db.autoCheckpointSizeArmed.CompareAndSwap(false, true)
		} else if !db.autoCheckpointSizeArmed.Load() && db.walUsesValueLog() {
			reclaimable := db.reclaimableWALBytes()
			if reclaimable < max/2 {
				db.autoCheckpointSizeArmed.CompareAndSwap(false, true)
			}
		}
		scaled := max / 4
		if scaled < 4*1024 {
			scaled = 4 * 1024
		}
		if scaled < threshold {
			threshold = scaled
		}
	}
	for {
		last := db.autoCheckpointLastWALBytes.Load()
		if current < last {
			if db.autoCheckpointLastWALBytes.CompareAndSwap(last, current) {
				return
			}
			continue
		}
		if current-last < threshold {
			return
		}
		if db.autoCheckpointLastWALBytes.CompareAndSwap(last, current) {
			break
		}
	}
	select {
	case db.autoCheckpointWriteCh <- struct{}{}:
	default:
	}
}

type autoCheckpointMode uint8

const (
	autoCheckpointModeInterval autoCheckpointMode = iota
	autoCheckpointModeIdle
	autoCheckpointModeSize
	autoCheckpointModeForce
)

const (
	autoCheckpointMinIdleWALBytesMin int64 = 1 << 20  // 1MiB
	autoCheckpointMinIdleWALBytesMax int64 = 32 << 20 // 32MiB
	autoCheckpointMinIdleInterval          = 10 * time.Second
)

func autoCheckpointReasonString(v uint32) string {
	switch autoCheckpointMode(v) {
	case autoCheckpointModeInterval:
		return "interval"
	case autoCheckpointModeIdle:
		return "idle"
	case autoCheckpointModeSize:
		return "size"
	case autoCheckpointModeForce:
		return "force"
	default:
		return "unknown"
	}
}

func vlogGenerationSchedulerStateString(v uint32) string {
	switch v {
	case vlogGenerationSchedulerDisabled:
		return "disabled"
	case vlogGenerationSchedulerIdle:
		return "idle"
	case vlogGenerationSchedulerRunning:
		return "running"
	case vlogGenerationSchedulerError:
		return "error"
	default:
		return "unknown"
	}
}

func vlogGenerationReasonString(v uint32) string {
	switch v {
	case vlogGenerationReasonNone:
		return "none"
	case vlogGenerationReasonTotalBytes:
		return "total_bytes"
	case vlogGenerationReasonStaleRatio:
		return "stale_ratio"
	case vlogGenerationReasonChurn:
		return "churn"
	case vlogGenerationReasonPeriodicGC:
		return "periodic_gc"
	case vlogGenerationReasonPostRewriteVacuum:
		return "post_rewrite_vacuum"
	default:
		return "unknown"
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	if t == nil {
		return
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

func (db *DB) effectiveWALBytes() int64 {
	if db == nil {
		return 0
	}
	var total int64
	for i := range db.lanes {
		l := &db.lanes[i]
		total += l.walClosedBytes.Load() + l.walLiveBytes.Load()
	}
	return total
}

func (db *DB) reclaimableWALBytes() int64 {
	if db == nil {
		return 0
	}
	total := db.effectiveWALBytes()
	if total <= 0 {
		return 0
	}
	if !db.walUsesValueLog() {
		return total
	}
	_, retained := db.valueLogRetainedStats()
	if retained >= total {
		return 0
	}
	return total - retained
}

func (db *DB) minIdleCheckpointWALBytes() int64 {
	if db == nil {
		return autoCheckpointMinIdleWALBytesMin
	}
	db.mu.RLock()
	ft := db.flushThreshold
	db.mu.RUnlock()
	min := ft / 16
	if min < autoCheckpointMinIdleWALBytesMin {
		min = autoCheckpointMinIdleWALBytesMin
	}
	if min > autoCheckpointMinIdleWALBytesMax {
		min = autoCheckpointMinIdleWALBytesMax
	}
	return min
}

const (
	commitLogSegmentHeaderBytes = 8
	commitLogBatchHeaderBytes   = 1 + 4
	commitLogRecordHeaderBytes  = 1 + 2 + 4 + 8 + 8
)

func (db *DB) logRecordSize(key, value []byte) int64 {
	return int64(commitLogSegmentHeaderBytes + commitLogBatchHeaderBytes + commitLogRecordHeaderBytes + len(key) + len(value))
}

func (db *DB) logBatchSize(records []logRecord) int64 {
	if len(records) == 0 {
		return 0
	}
	total := commitLogSegmentHeaderBytes + commitLogBatchHeaderBytes
	for _, r := range records {
		total += commitLogRecordHeaderBytes + len(r.Key) + len(r.Value)
	}
	return int64(total)
}

func (db *DB) assignCommitSeq(records []logRecord) {
	if len(records) == 0 {
		return
	}
	seq := db.nextCommitSeq.Add(1)
	for i := range records {
		records[i].Seq = seq
	}
}

type domainIngressOp uint8

const (
	domainIngressOpSet domainIngressOp = iota + 1
	domainIngressOpDelete
)

type domainIngressRequest struct {
	op    domainIngressOp
	key   []byte
	value []byte
	sync  bool
	done  chan error
}

type walWriteRequest struct {
	records []logRecord
	sync    bool
	ack     *walAck
}

type walAck struct {
	wg  sync.WaitGroup
	err error
}

var walAckPool = sync.Pool{
	New: func() any { return &walAck{} },
}

type vlogWriteRequest struct {
	rid              uint64
	value            []byte
	dictID           uint64
	writeMode        vlogCompressionWriteMode
	blockCodec       valuelog.BlockCodec
	probeCompression bool
	durability       journalDurability
	enqueuedAt       time.Time
	ack              *vlogAck
}

type vlogAck struct {
	wg         sync.WaitGroup
	ptr        page.ValuePtr
	retainPath string
	err        error
}

var vlogAckPool = sync.Pool{
	New: func() any { return &vlogAck{} },
}

type vlogDictPrepareTask struct {
	fi             int
	dictID         uint64
	dict           []byte
	records        []valuelog.Record
	level          zstd.EncoderLevel
	enableEntropy  bool
	ioNsPerStored  float64
	encodeNsPerRaw float64
	safetyMargin   float64
	measureEncode  bool
	out            chan<- vlogDictPrepareResult
}

type vlogDictPrepareResult struct {
	fi      int
	body    []byte
	bodyBuf *vlogPreparedFrameBody
	stats   valuelog.FrameStats
	err     error
}

func (db *DB) publishVlogDictPrepareResult(task vlogDictPrepareTask, res vlogDictPrepareResult) {
	if task.out == nil {
		if res.bodyBuf != nil {
			putVlogPreparedFrameBody(res.bodyBuf)
		}
		return
	}
	select {
	case task.out <- res:
		return
	case <-db.closeCh:
		// During shutdown callers may stop receiving. Avoid blocking workers and
		// leaking pooled frame buffers in that case.
		select {
		case task.out <- res:
		default:
			if res.bodyBuf != nil {
				putVlogPreparedFrameBody(res.bodyBuf)
			}
		}
	}
}

const (
	maxEntryPoolCap              = 1 << 20
	maxEntryRunsPoolCap          = 1 << 14
	maxUnitRunsPoolCap           = 1 << 8
	maxOpMergeHeapCap            = 1 << 8
	entrySliceLeaseMinShift      = 4
	entrySliceLeaseMaxShift      = 20
	entrySliceLeaseClassCount    = entrySliceLeaseMaxShift - entrySliceLeaseMinShift + 1
	maxEntrySliceLeasesPerBucket = 128
)

var entrySlicePools [entrySliceLeaseClassCount]sync.Pool
var entryRunsPool sync.Pool
var unitRunsPool sync.Pool
var opMergeHeapPool sync.Pool
var entrySliceLeaseMu sync.Mutex
var entrySliceLeases [entrySliceLeaseClassCount][][]batch.Entry

func entrySliceLeaseClassForLen(capacity int) (idx int, classCap int, ok bool) {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > maxEntryPoolCap {
		return 0, 0, false
	}
	minCap := 1 << entrySliceLeaseMinShift
	if capacity <= minCap {
		return 0, minCap, true
	}
	classCap = 1 << uint(bits.Len(uint(capacity-1)))
	if classCap < minCap {
		classCap = minCap
	}
	if classCap > maxEntryPoolCap {
		return 0, 0, false
	}
	shift := bits.Len(uint(classCap)) - 1
	idx = shift - entrySliceLeaseMinShift
	if idx < 0 || idx >= entrySliceLeaseClassCount {
		return 0, 0, false
	}
	return idx, classCap, true
}

func entrySliceLeaseClassForCap(capacity int) (idx int, ok bool) {
	minCap := 1 << entrySliceLeaseMinShift
	if capacity < minCap || capacity > maxEntryPoolCap {
		return 0, false
	}
	if capacity&(capacity-1) != 0 {
		return 0, false
	}
	shift := bits.TrailingZeros(uint(capacity))
	idx = shift - entrySliceLeaseMinShift
	if idx < 0 || idx >= entrySliceLeaseClassCount {
		return 0, false
	}
	return idx, true
}

func entrySliceMaxReuseCap(capacity int) int {
	if capacity <= 0 {
		return 1 << entrySliceLeaseMinShift
	}
	// Clamp before multiplication to avoid potential integer overflow.
	if capacity > maxEntryPoolCap/8 {
		return maxEntryPoolCap
	}
	maxCap := capacity * 8
	if maxCap < 1<<12 {
		maxCap = 1 << 12
	}
	if maxCap > maxEntryPoolCap {
		maxCap = maxEntryPoolCap
	}
	return maxCap
}

func getEntrySlice(capacity int) []batch.Entry {
	if capacity < 0 {
		capacity = 0
	}
	idx, classCap, ok := entrySliceLeaseClassForLen(capacity)
	if !ok {
		return make([]batch.Entry, 0, capacity)
	}
	maxReuseCap := entrySliceMaxReuseCap(capacity)
	maxIdx, _, maxOK := entrySliceLeaseClassForLen(maxReuseCap)
	if !maxOK {
		maxIdx = idx
	}
	entrySliceLeaseMu.Lock()
	for bucket := idx; bucket <= maxIdx; bucket++ {
		leases := entrySliceLeases[bucket]
		if n := len(leases); n > 0 {
			s := leases[n-1]
			entrySliceLeases[bucket][n-1] = nil
			entrySliceLeases[bucket] = leases[:n-1]
			entrySliceLeaseMu.Unlock()
			if cap(s) >= capacity {
				return s[:0]
			}
			if capIdx, ok := entrySliceLeaseClassForCap(cap(s)); ok {
				entrySlicePools[capIdx].Put(s[:0])
			}
			return make([]batch.Entry, 0, classCap)
		}
	}
	entrySliceLeaseMu.Unlock()
	for bucket := idx; bucket <= maxIdx; bucket++ {
		if v := entrySlicePools[bucket].Get(); v != nil {
			s, ok := v.([]batch.Entry)
			if !ok {
				continue
			}
			if cap(s) >= capacity && cap(s) <= maxReuseCap {
				return s[:0]
			}
		}
	}
	return make([]batch.Entry, 0, classCap)
}

func putEntrySlice(entries []batch.Entry) {
	if entries == nil {
		return
	}
	if cap(entries) > maxEntryPoolCap {
		return
	}
	full := entries[:cap(entries)]
	clear(full)
	idx, ok := entrySliceLeaseClassForCap(cap(entries))
	if !ok {
		return
	}
	entries = entries[:0]
	entrySliceLeaseMu.Lock()
	if len(entrySliceLeases[idx]) < maxEntrySliceLeasesPerBucket {
		entrySliceLeases[idx] = append(entrySliceLeases[idx], entries)
		entrySliceLeaseMu.Unlock()
		return
	}
	entrySliceLeaseMu.Unlock()
	entrySlicePools[idx].Put(entries)
}

func getEntryRuns(capacity int) [][]batch.Entry {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > maxEntryRunsPoolCap {
		return make([][]batch.Entry, 0, capacity)
	}
	if v := entryRunsPool.Get(); v != nil {
		if runs, ok := v.([][]batch.Entry); ok && cap(runs) >= capacity {
			return runs[:0]
		}
	}
	return make([][]batch.Entry, 0, capacity)
}

func putEntryRuns(runs [][]batch.Entry) {
	if runs == nil || cap(runs) > maxEntryRunsPoolCap {
		return
	}
	clear(runs)
	entryRunsPool.Put(runs[:0])
}

func getUnitRuns(length int) [][][]batch.Entry {
	if length < 0 {
		length = 0
	}
	if length > maxUnitRunsPoolCap {
		return make([][][]batch.Entry, length)
	}
	if v := unitRunsPool.Get(); v != nil {
		if runs, ok := v.([][][]batch.Entry); ok && cap(runs) >= length {
			return runs[:length]
		}
	}
	return make([][][]batch.Entry, length)
}

func putUnitRuns(runs [][][]batch.Entry) {
	if runs == nil || cap(runs) > maxUnitRunsPoolCap {
		return
	}
	clear(runs)
	unitRunsPool.Put(runs[:0])
}

func getOpMergeHeap(capacity int) opMergeHeap {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > maxOpMergeHeapCap {
		return make(opMergeHeap, 0, capacity)
	}
	if v := opMergeHeapPool.Get(); v != nil {
		if h, ok := v.(opMergeHeap); ok && cap(h) >= capacity {
			return h[:0]
		}
	}
	return make(opMergeHeap, 0, capacity)
}

func putOpMergeHeap(h opMergeHeap) {
	if h == nil || cap(h) > maxOpMergeHeapCap {
		return
	}
	full := h[:cap(h)]
	clear(full)
	opMergeHeapPool.Put(full[:0])
}

func estimateUnitRunEntries(unitRuns [][][]batch.Entry, floor int) int {
	if floor < 0 {
		floor = 0
	}
	total := floor
	maxInt := int(^uint(0) >> 1)
	for i := range unitRuns {
		for _, run := range unitRuns[i] {
			if len(run) <= 0 {
				continue
			}
			if total > maxInt-len(run) {
				return maxInt
			}
			total += len(run)
		}
	}
	return total
}

func collectOpsInto(mem memtable.Table, dst []batch.Entry) (int, error) {
	if mem == nil {
		return 0, errors.New("cachingdb: nil memtable")
	}
	iter := mem.NewIterator(nil, nil)
	defer func() { _ = iter.Close() }()

	i := 0
	for iter.Valid() {
		if i >= len(dst) {
			return 0, fmt.Errorf("cachingdb: collectOpsInto overflow (have=%d need>=%d)", len(dst), i+1)
		}
		val, ptr, flags := iter.UnsafeEntry()
		if flags&node.FlagTombstone != 0 {
			dst[i] = batch.Entry{
				Type: batch.OpDelete,
				Key:  iter.UnsafeKey(),
			}
		} else if flags&node.FlagPointer != 0 {
			dst[i] = batch.Entry{
				Type:     batch.OpPut,
				Key:      iter.UnsafeKey(),
				ValuePtr: ptr,
				IsPtr:    true,
			}
		} else {
			dst[i] = batch.Entry{
				Type:  batch.OpPut,
				Key:   iter.UnsafeKey(),
				Value: val,
			}
		}
		i++
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}
	return i, nil
}

type opRunIter struct {
	runs   [][]batch.Entry
	runIdx int
	idx    int
	valid  bool
}

func newOpRunIter(runs [][]batch.Entry) *opRunIter {
	it := &opRunIter{runs: runs}
	it.advanceToValid()
	return it
}

func (it *opRunIter) advanceToValid() {
	for it.runIdx < len(it.runs) {
		if it.idx < len(it.runs[it.runIdx]) {
			it.valid = true
			return
		}
		it.runIdx++
		it.idx = 0
	}
	it.valid = false
}

func (it *opRunIter) Valid() bool {
	return it.valid
}

func (it *opRunIter) Next() {
	if !it.valid {
		return
	}
	it.idx++
	it.advanceToValid()
}

func (it *opRunIter) Entry() batch.Entry {
	if !it.valid {
		return batch.Entry{}
	}
	return it.runs[it.runIdx][it.idx]
}

func (it *opRunIter) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.runs[it.runIdx][it.idx].Key
}

type opMergeItem struct {
	iter     *opRunIter
	priority int
	key      []byte
}

type opMergeHeap []opMergeItem

func (h opMergeHeap) Len() int { return len(h) }

func (h opMergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].priority < h[j].priority
}

func (h opMergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *opMergeHeap) push(x opMergeItem) {
	*h = append(*h, x)
	h.up(len(*h) - 1)
}

func (h *opMergeHeap) pop() opMergeItem {
	old := *h
	n := len(old)
	if n == 0 {
		return opMergeItem{}
	}
	old.Swap(0, n-1)
	h.down(0, n-1)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (h opMergeHeap) peek() *opMergeItem {
	if len(h) == 0 {
		return nil
	}
	return &h[0]
}

func (h *opMergeHeap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *opMergeHeap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}

func buildOpRuns(mem memtable.Table, chunkCap int) ([][]batch.Entry, int, error) {
	if mem == nil {
		return nil, 0, errors.New("cachingdb: nil memtable")
	}
	if chunkCap <= 0 {
		chunkCap = 8192
	}
	iter := mem.NewIterator(nil, nil)
	runsCap := mem.Len()/chunkCap + 1
	runs := getEntryRuns(runsCap)
	deleteOps := 0
	ops := getEntrySlice(chunkCap)
	ops = ops[:0]
	stableUnsafe := false
	if stable, ok := mem.(memtable.StableUnsafeIteratorTable); ok {
		stableUnsafe = stable.StableUnsafeIteratorSlices()
	}
	for iter.Valid() {
		val, ptr, flags := iter.UnsafeEntry()
		key := iter.UnsafeKey()
		if !stableUnsafe {
			key = append([]byte(nil), key...)
		}
		if flags&node.FlagTombstone != 0 {
			ops = append(ops, batch.Entry{Type: batch.OpDelete, Key: key})
			deleteOps++
		} else if flags&node.FlagPointer != 0 {
			ops = append(ops, batch.Entry{Type: batch.OpPut, Key: key, ValuePtr: ptr, IsPtr: true})
		} else {
			if !stableUnsafe && val != nil {
				val = append([]byte(nil), val...)
			}
			ops = append(ops, batch.Entry{Type: batch.OpPut, Key: key, Value: val})
		}
		iter.Next()
		if len(ops) >= cap(ops) {
			runs = append(runs, ops)
			ops = getEntrySlice(chunkCap)
			ops = ops[:0]
		}
	}
	err := iter.Error()
	cerr := iter.Close()
	if err == nil {
		err = cerr
	}
	if err != nil {
		putEntrySlice(ops)
		for _, run := range runs {
			putEntrySlice(run)
		}
		putEntryRuns(runs)
		return nil, 0, err
	}
	if len(ops) > 0 {
		runs = append(runs, ops)
	} else {
		putEntrySlice(ops)
	}
	return runs, deleteOps, nil
}

type walFastItem struct {
	record logRecord
	ack    *walAck
}

const (
	walWriteBuffer                = 4096
	walWriteBatchMax              = 512
	walFastBatchMax               = 2048
	walFastQueueMax               = 16384
	defaultDomainIngressQueueSize = 1024
	domainIngressBatchMax         = 128
)

const (
	vlogWriteBuffer    = 4096
	vlogWriteBatchMax  = 512
	vlogDictPrepBuffer = 1024
)

// Always queue values at or above this size to avoid blocking callers on large
// appends and to improve value-log batching efficiency.
const vlogQueueMinValueSize = 1 << 10

// Linger briefly to coalesce micro-batches for small/medium queued writes.
const vlogWriteLinger = 75 * time.Microsecond

func defaultJournalLaneCount(procs int) int {
	if procs <= 2 {
		return 1
	}
	// Keep defaults conservative on low/mid core hosts.
	lanes := procs / 4
	if lanes < 1 {
		lanes = 1
	}
	// On high-core hosts, increase lane fanout to unlock journal/value-log
	// parallelism, but avoid the most aggressive split to limit queue overhead.
	if procs >= 16 {
		highCoreLanes := (procs * 3) / 8
		if highCoreLanes > lanes {
			lanes = highCoreLanes
		}
	}
	if lanes > 8 {
		lanes = 8
	}
	return lanes
}

func (db *DB) startDomainIngressWorkers() {
	if db == nil || db.domainIngressWorkers <= 0 {
		return
	}
	queueSize := db.domainIngressQueueSize
	if queueSize <= 0 {
		queueSize = defaultDomainIngressQueueSize
	}
	db.domainIngressMu.Lock()
	defer db.domainIngressMu.Unlock()
	if len(db.domainIngressCh) > 0 {
		return
	}
	workers := db.domainIngressWorkers
	if workers < 1 {
		return
	}
	db.domainIngressQueueSize = queueSize
	db.domainIngressCh = make([]chan domainIngressRequest, workers)
	for workerID := 0; workerID < workers; workerID++ {
		ch := make(chan domainIngressRequest, queueSize)
		db.domainIngressCh[workerID] = ch
		db.wg.Add(1)
		go db.domainIngressLoop(ch)
	}
}

func (db *DB) stopDomainIngressWorkers() {
	if db == nil {
		return
	}
	db.domainIngressMu.Lock()
	queues := db.domainIngressCh
	db.domainIngressCh = nil
	db.domainIngressMu.Unlock()
	for _, ch := range queues {
		close(ch)
	}
}

func (db *DB) domainIngressLoop(ch <-chan domainIngressRequest) {
	defer db.wg.Done()
	batchReqs := make([]domainIngressRequest, 0, domainIngressBatchMax)
	for {
		req, ok := <-ch
		if !ok {
			return
		}
		batchReqs = append(batchReqs[:0], req)
	drain:
		for len(batchReqs) < domainIngressBatchMax {
			select {
			case req, ok = <-ch:
				if !ok {
					db.processDomainIngressBatch(batchReqs)
					return
				}
				batchReqs = append(batchReqs, req)
			default:
				break drain
			}
		}
		db.processDomainIngressBatch(batchReqs)
	}
}

func (db *DB) processDomainIngressBatch(reqs []domainIngressRequest) {
	if len(reqs) == 0 {
		return
	}
	if len(reqs) == 1 {
		req := reqs[0]
		var err error
		switch req.op {
		case domainIngressOpSet:
			err = db.setDirect(req.key, req.value, false)
		case domainIngressOpDelete:
			err = db.deleteDirect(req.key, false)
		default:
			err = fmt.Errorf("cachingdb: unknown ingress op %d", req.op)
		}
		db.domainIngressProcessed.Add(1)
		if req.done != nil {
			req.done <- err
			close(req.done)
		}
		return
	}

	b := db.NewBatchWithSize(len(reqs))
	var err error
	for i := range reqs {
		req := reqs[i]
		switch req.op {
		case domainIngressOpSet:
			err = b.Set(req.key, req.value)
		case domainIngressOpDelete:
			err = b.Delete(req.key)
		default:
			err = fmt.Errorf("cachingdb: unknown ingress op %d", req.op)
		}
		if err != nil {
			break
		}
	}
	if err == nil {
		err = b.Write()
	}
	if closeErr := b.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	db.domainIngressProcessed.Add(uint64(len(reqs)))
	for i := range reqs {
		req := reqs[i]
		if req.done != nil {
			req.done <- err
			close(req.done)
		}
	}
}

func (db *DB) observeDomainIngressDepth(depth int) {
	if depth < 0 {
		return
	}
	depthU := uint64(depth)
	for {
		prev := db.domainIngressDepthMax.Load()
		if depthU <= prev {
			return
		}
		if db.domainIngressDepthMax.CompareAndSwap(prev, depthU) {
			return
		}
	}
}

func (db *DB) enqueueDomainIngress(op domainIngressOp, key, value []byte, sync bool) (bool, error) {
	if db == nil {
		return false, nil
	}
	if db.domainIngressWorkers <= 0 {
		return false, nil
	}
	// Preserve legacy sync behavior until ingress batching has explicit sync-fence
	// handling and per-request durable completion accounting.
	if sync {
		return false, nil
	}
	if db.closing.Load() {
		return true, errDBClosing
	}

	db.domainIngressMu.Lock()
	if len(db.domainIngressCh) == 0 {
		db.domainIngressMu.Unlock()
		return false, nil
	}
	req := domainIngressRequest{
		op:    op,
		key:   key,
		value: value,
		sync:  sync,
		done:  make(chan error, 1),
	}
	shardID := db.shardIndex(key)
	workerID := shardID % len(db.domainIngressCh)
	ch := db.domainIngressCh[workerID]
	select {
	case ch <- req:
		db.domainIngressEnqueued.Add(1)
		db.observeDomainIngressDepth(len(ch))
		db.domainIngressMu.Unlock()
	default:
		db.domainIngressFallback.Add(1)
		db.domainIngressMu.Unlock()
		return false, nil
	}

	err, ok := <-req.done
	if !ok {
		return true, errDBClosing
	}
	return true, err
}

func (db *DB) startWALWriter(l *lane) {
	if l == nil {
		return
	}
	l.walCh = make(chan walWriteRequest, walWriteBuffer)
	l.walFastCond = sync.NewCond(&l.walFastMu)
	db.wg.Add(1)
	go db.walWriteLoop(l)
	db.wg.Add(1)
	go db.walFastLoop(l)
}

func (db *DB) startVlogWriter(l *lane) {
	if l == nil {
		return
	}
	l.vlogCh = make(chan vlogWriteRequest, vlogWriteBuffer)
	l.vlogDictBytes = make(map[uint64][]byte)
	db.startVlogDictPreparer(l)
	workers := db.vlogWriteWorkerCount()
	l.vlogWorkers = workers
	for i := 0; i < workers; i++ {
		db.wg.Add(1)
		go db.vlogWriteLoop(l)
	}
}

func (db *DB) vlogWriteWorkerCount() int {
	procs := runtime.GOMAXPROCS(0)
	if procs <= 1 {
		return 1
	}
	lanes := len(db.lanes)
	if lanes < 1 {
		lanes = 1
	}
	workers := procs / lanes
	if workers < 1 {
		workers = 1
	}
	if workers > 4 {
		workers = 4
	}
	return workers
}

func (db *DB) vlogDictPrepWorkerCount() int {
	procs := runtime.GOMAXPROCS(0)
	if procs <= 1 {
		return 0
	}
	lanes := len(db.lanes)
	if lanes < 1 {
		lanes = 1
	}
	workers := procs / lanes
	if workers < 2 {
		workers = 2
	}
	if workers > procs {
		workers = procs
	}
	if workers < 2 {
		return 0
	}
	return workers
}

func (db *DB) startVlogDictPreparer(l *lane) {
	if l == nil {
		return
	}
	maxWorkers := db.vlogDictPrepWorkerCount()
	if maxWorkers <= 1 {
		return
	}
	l.vlogPrepWorkers = 0
	l.vlogPrepMaxWorkers = maxWorkers
	l.vlogPrepCh = make(chan vlogDictPrepareTask, vlogDictPrepBuffer)
}

func (db *DB) ensureVlogDictPrepWorkers(l *lane, wanted int) {
	if l == nil || l.vlogPrepCh == nil || l.vlogPrepMaxWorkers <= 1 {
		return
	}
	if wanted <= 0 {
		wanted = 1
	}
	if wanted > l.vlogPrepMaxWorkers {
		wanted = l.vlogPrepMaxWorkers
	}
	l.vlogPrepMu.Lock()
	start := l.vlogPrepWorkers
	if start >= wanted {
		l.vlogPrepMu.Unlock()
		return
	}
	l.vlogPrepWorkers = wanted
	l.vlogPrepMu.Unlock()
	for i := start; i < wanted; i++ {
		db.wg.Add(1)
		go db.vlogDictPrepareLoop(l)
	}
}

func (db *DB) vlogDictPrepareLoop(l *lane) {
	defer db.wg.Done()
	if l == nil || l.vlogPrepCh == nil {
		return
	}
	preparer := valuelog.NewFramePreparer()
	processTask := func(task vlogDictPrepareTask) {
		preparer.SetDictFrameEncoderOptions(task.level, task.enableEntropy)
		preparer.SetKeepPolicy(task.ioNsPerStored, task.encodeNsPerRaw, task.safetyMargin)
		if task.measureEncode {
			preparer.SetEncodeSampleStride(1)
		} else {
			preparer.SetEncodeSampleStride(0)
		}
		bodyBuf := getVlogPreparedFrameBody()
		body, stats, err := preparer.PrepareFrameInto(bodyBuf.buf[:0], task.dictID, task.dict, task.records)
		if err != nil {
			putVlogPreparedFrameBody(bodyBuf)
			db.publishVlogDictPrepareResult(task, vlogDictPrepareResult{
				fi:  task.fi,
				err: err,
			})
			return
		}
		bodyBuf.buf = body
		db.publishVlogDictPrepareResult(task, vlogDictPrepareResult{
			fi:      task.fi,
			body:    body,
			bodyBuf: bodyBuf,
			stats:   stats,
		})
	}
	for {
		// Prefer queued work, even during close, so enqueued tasks are not stranded.
		select {
		case task := <-l.vlogPrepCh:
			processTask(task)
			continue
		default:
		}
		select {
		case task := <-l.vlogPrepCh:
			processTask(task)
		case <-db.closeCh:
			for {
				select {
				case task := <-l.vlogPrepCh:
					db.publishVlogDictPrepareResult(task, vlogDictPrepareResult{
						fi:  task.fi,
						err: errWALClosed,
					})
				default:
					return
				}
			}
		}
	}
}

func (db *DB) walWriteLoop(l *lane) {
	defer db.wg.Done()

	batch := make([]walWriteRequest, 0, walWriteBatchMax)
	for {
		batch = batch[:0]

		var req walWriteRequest
		select {
		case <-db.closeCh:
			db.drainWALWriter(l, batch)
			return
		case req = <-l.walCh:
		}
		batch = append(batch, req)

	drain:
		for len(batch) < walWriteBatchMax {
			select {
			case req = <-l.walCh:
				batch = append(batch, req)
			default:
				break drain
			}
		}

		db.walAckMu.Lock()
		walErr := db.walErr
		db.walAckMu.Unlock()
		if walErr != nil {
			db.finishWALRequests(batch, walErr)
			continue
		}

		err := db.flushWALRequests(l, batch)
		if err != nil {
			db.walAckMu.Lock()
			if db.walErr == nil {
				db.walErr = err
			}
			walErr = db.walErr
			db.walAckMu.Unlock()
			db.finishWALRequests(batch, walErr)
			continue
		}

		db.finishWALRequests(batch, nil)
	}
}

func (db *DB) vlogWriteLoop(l *lane) {
	defer db.wg.Done()

	batch := make([]vlogWriteRequest, 0, vlogWriteBatchMax)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	defer timer.Stop()
	for {
		batch = batch[:0]

		var req vlogWriteRequest
		select {
		case <-db.closeCh:
			db.drainVlogWriter(l, batch)
			return
		case req = <-l.vlogCh:
		}
		batch = append(batch, req)
		backlog := len(l.vlogCh)
		observeLaneVlogQueueDepthSample(l, backlog)
		lingerAllowed := backlog < (vlogWriteBatchMax/4) && !l.vlogQueueing.Load()
		if len(batch) < vlogWriteBatchMax && len(req.value) < vlogQueueMinValueSize && lingerAllowed {
			timer.Reset(vlogWriteLinger)
			lingerDone := false
			for len(batch) < vlogWriteBatchMax && !lingerDone {
				select {
				case req = <-l.vlogCh:
					batch = append(batch, req)
				case <-timer.C:
					lingerDone = true
				case <-db.closeCh:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					db.drainVlogWriter(l, batch)
					return
				}
			}
			if !timer.Stop() && !lingerDone {
				select {
				case <-timer.C:
				default:
				}
			}
		}

	drain:
		for len(batch) < vlogWriteBatchMax {
			select {
			case req = <-l.vlogCh:
				batch = append(batch, req)
			default:
				break drain
			}
		}
		observeLaneVlogQueueDepthSample(l, len(l.vlogCh))

		db.flushVlogRequests(l, batch)
		if len(l.vlogCh) == 0 {
			l.vlogQueueing.Store(false)
		}
	}
}

func (db *DB) walFastLoop(l *lane) {
	defer db.wg.Done()

	batch := make([]walFastItem, 0, walFastBatchMax)
	records := make([]logRecord, 0, walFastBatchMax)

	for {
		l.walFastMu.Lock()
		for !l.walFastClosed && len(l.walFastQueue)-l.walFastHead == 0 {
			l.walFastCond.Wait()
		}

		if l.walFastClosed {
			batch = append(batch[:0], l.walFastQueue[l.walFastHead:]...)
			l.walFastQueue = nil
			l.walFastHead = 0
			l.walFastMu.Unlock()

			for i := range batch {
				ack := batch[i].ack
				ack.err = errWALClosed
				ack.wg.Done()
			}
			return
		}

		available := len(l.walFastQueue) - l.walFastHead
		n := available
		if n > walFastBatchMax {
			n = walFastBatchMax
		}
		batch = append(batch[:0], l.walFastQueue[l.walFastHead:l.walFastHead+n]...)
		l.walFastHead += n

		if l.walFastHead == len(l.walFastQueue) {
			l.walFastQueue = l.walFastQueue[:0]
			l.walFastHead = 0
		} else if l.walFastHead > 1024 && l.walFastHead*2 >= len(l.walFastQueue) {
			copy(l.walFastQueue, l.walFastQueue[l.walFastHead:])
			l.walFastQueue = l.walFastQueue[:len(l.walFastQueue)-l.walFastHead]
			l.walFastHead = 0
		}
		l.walFastCond.Broadcast()
		l.walFastMu.Unlock()

		records = records[:0]
		for i := range batch {
			records = append(records, batch[i].record)
		}
		err := db.appendWALDirect(l, records, false)
		for i := range batch {
			ack := batch[i].ack
			ack.err = err
			ack.wg.Done()
		}
	}
}

func (db *DB) drainWALWriter(l *lane, batch []walWriteRequest) {
	for {
		select {
		case req := <-l.walCh:
			batch = append(batch[:0], req)
		drain:
			for len(batch) < walWriteBatchMax {
				select {
				case req = <-l.walCh:
					batch = append(batch, req)
				default:
					break drain
				}
			}
			db.walAckMu.Lock()
			walErr := db.walErr
			db.walAckMu.Unlock()
			if walErr != nil {
				db.finishWALRequests(batch, walErr)
				continue
			}

			err := db.flushWALRequests(l, batch)
			if err != nil {
				db.walAckMu.Lock()
				if db.walErr == nil {
					db.walErr = err
				}
				walErr = db.walErr
				db.walAckMu.Unlock()
				db.finishWALRequests(batch, walErr)
				continue
			}
			db.finishWALRequests(batch, nil)
		default:
			return
		}
	}
}

func (db *DB) drainVlogWriter(l *lane, batch []vlogWriteRequest) {
	for {
		select {
		case req := <-l.vlogCh:
			batch = append(batch[:0], req)
		drain:
			for len(batch) < vlogWriteBatchMax {
				select {
				case req = <-l.vlogCh:
					batch = append(batch, req)
				default:
					break drain
				}
			}
			observeLaneVlogQueueDepthSample(l, len(l.vlogCh))
			db.flushVlogRequests(l, batch)
		default:
			return
		}
	}
}

func (db *DB) finishWALRequests(requests []walWriteRequest, err error) {
	for i := range requests {
		ack := requests[i].ack
		if ack == nil {
			continue
		}
		ack.err = err
		ack.wg.Done()
	}
}

func (db *DB) flushWALRequests(l *lane, requests []walWriteRequest) error {
	if len(requests) == 0 {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}

	var (
		totalBytes int64
		needSync   bool
	)

	l.walMu.Lock()
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	for i := range requests {
		req := &requests[i]
		if len(req.records) == 1 {
			rec := req.records[0]
			err := w.Append(rec)
			if err != nil {
				l.walMu.Unlock()
				return err
			}
			totalBytes += db.logRecordSize(rec.Key, rec.Value)
		} else {
			err := w.AppendBatch(req.records)
			if err != nil {
				l.walMu.Unlock()
				return err
			}
			totalBytes += db.logBatchSize(req.records)
		}
		if req.sync {
			needSync = true
		}
	}
	if needSync {
		if err := w.Sync(); err != nil {
			l.walMu.Unlock()
			return err
		}
	}
	l.walMu.Unlock()

	if totalBytes > 0 {
		l.walLiveBytes.Add(totalBytes)
	}
	return nil
}

func (db *DB) flushVlogRequests(l *lane, requests []vlogWriteRequest) {
	if len(requests) == 0 {
		return
	}
	if l == nil {
		for i := range requests {
			ack := requests[i].ack
			if ack == nil {
				continue
			}
			ack.ptr = page.ValuePtr{}
			ack.retainPath = ""
			ack.err = errWALUnavailable
			ack.wg.Done()
		}
		return
	}

	var (
		needFlush       bool
		needSync        bool
		rawPayloadBytes int
		singleDictID    uint64
		sawDictID       bool
		multipleDictIDs bool
	)
	records := getValueLogRecordsCap(len(requests))
	records = records[:len(requests)]
	defer putValueLogRecordsNoClear(records)
	for i := range requests {
		req := &requests[i]
		if !req.enqueuedAt.IsZero() {
			observeLaneVlogQueueLag(l, time.Since(req.enqueuedAt))
		}
		records[i] = valuelog.Record{RID: req.rid, Value: req.value}
		if req.durability == journalDurabilitySync {
			needSync = true
		} else if req.durability == journalDurabilityFlush {
			needFlush = true
		}
		if req.dictID != 0 {
			if !sawDictID {
				sawDictID = true
				singleDictID = req.dictID
			} else if req.dictID != singleDictID {
				multipleDictIDs = true
			}
		}
		rawPayloadBytes += len(req.value)
	}

	var (
		singleDict []byte
		dictByID   map[uint64][]byte
	)
	if sawDictID {
		if !multipleDictIDs {
			dictBytes, err := db.dictBytesForLane(context.Background(), l, singleDictID)
			if err == nil && len(dictBytes) > 0 {
				singleDict = dictBytes
			}
		} else {
			dictNeeded := make(map[uint64]struct{})
			for i := range requests {
				if dictID := requests[i].dictID; dictID != 0 {
					dictNeeded[dictID] = struct{}{}
				}
			}
			dictByID = make(map[uint64][]byte, len(dictNeeded))
			for dictID := range dictNeeded {
				dictBytes, err := db.dictBytesForLane(context.Background(), l, dictID)
				if err == nil && len(dictBytes) > 0 {
					dictByID[dictID] = dictBytes
				}
			}
		}
	}

	type vlogBatchPlan struct {
		start       int
		end         int
		writeMode   vlogCompressionWriteMode
		blockCodec  valuelog.BlockCodec
		dictID      uint64
		dict        []byte
		k           int
		probe       bool
		rawBytes    int
		frames      []preparedDictFrame
		storedBytes int
		wallNs      int64
	}
	rawPaused := db.valueLogDictPauseRemaining.Load() > 0
	var planScratch [16]vlogBatchPlan
	plans := planScratch[:0]
	if len(requests) > len(planScratch) {
		plans = make([]vlogBatchPlan, 0, len(requests))
	}
	for i := 0; i < len(requests); {
		writeMode := requests[i].writeMode
		blockCodec := normalizeSelectorBlockCodec(requests[i].blockCodec)
		dictID := requests[i].dictID
		if writeMode != vlogWriteDict {
			dictID = 0
		}
		dict := lookupVlogDictBytes(dictID, singleDictID, singleDict, dictByID)
		if dictID == 0 || len(dict) == 0 || writeMode != vlogWriteDict {
			dictID = 0
			dict = nil
			if writeMode == vlogWriteDict {
				writeMode = vlogWriteOff
			}
		}
		probe := requests[i].probeCompression
		maxValLen := len(requests[i].value)
		rawBytes := len(requests[i].value)
		end := i + 1
		for end < len(requests) {
			nextMode := requests[end].writeMode
			nextCodec := normalizeSelectorBlockCodec(requests[end].blockCodec)
			nextDictID := requests[end].dictID
			nextDict := lookupVlogDictBytes(nextDictID, singleDictID, singleDict, dictByID)
			if nextMode != vlogWriteDict {
				nextDictID = 0
			}
			if nextDictID == 0 || len(nextDict) == 0 || nextMode != vlogWriteDict {
				nextDictID = 0
			}
			if nextMode == vlogWriteDict && nextDictID == 0 {
				nextMode = vlogWriteOff
			}
			if nextMode != writeMode {
				break
			}
			if nextMode == vlogWriteBlock && nextCodec != blockCodec {
				break
			}
			if nextDictID != dictID {
				break
			}
			probe = probe || requests[end].probeCompression
			if n := len(requests[end].value); n > maxValLen {
				maxValLen = n
			}
			rawBytes += len(requests[end].value)
			end++
		}

		k := 1
		if end-i > 1 {
			if writeMode == vlogWriteDict && dictID != 0 {
				k = db.valueLogDictK(dictID)
				k = db.chooseValueLogDictWriteK(k, end-i, rawBytes)
				if db.disableJournal {
					k = valuelog.MaxFrameK
				}
			} else if writeMode == vlogWriteBlock {
				k = db.chooseValueLogBlockWriteK(l, end-i, rawBytes, blockCodec)
			} else if rawPaused && db.disableJournal {
				k = valuelog.MaxFrameK
			} else if cur := int(db.valueLogDictCurrentK.Load()); cur > 1 {
				k = cur
			} else {
				k = 8
				if db.disableJournal && db.forceValueLogPointers {
					k = 16
				}
			}
		}
		if limits.MaxRecordSize > 0 && maxValLen > 0 {
			maxKBySize := int(limits.MaxRecordSize) / maxValLen
			if maxKBySize < 1 {
				maxKBySize = 1
			}
			if k > maxKBySize {
				k = maxKBySize
			}
		}
		k = db.clampValueLogDictK(k)

		plans = append(plans, vlogBatchPlan{
			start:      i,
			end:        end,
			writeMode:  writeMode,
			blockCodec: blockCodec,
			dictID:     dictID,
			dict:       dict,
			k:          k,
			probe:      probe,
			rawBytes:   rawBytes,
		})
		i = end
	}

	ioNsPerStored, encodeNsPerRaw, safetyMargin := db.valueLogKeepPolicy()
	preparedErr := error(nil)
	for pi := range plans {
		plan := &plans[pi]
		if plan.writeMode != vlogWriteDict || plan.dictID == 0 || len(plan.dict) == 0 {
			continue
		}
		keepIoNs := ioNsPerStored
		keepEncodeNs := encodeNsPerRaw
		if plan.probe {
			// Probe writes should always attempt dict compression.
			keepIoNs = 0
			keepEncodeNs = 0
		}
		prepared, _, prepErr := db.prepareAppendDictFrames(
			l,
			plan.dictID,
			plan.dict,
			records[plan.start:plan.end],
			plan.k,
			plan.rawBytes,
			keepIoNs,
			keepEncodeNs,
			safetyMargin,
			time.Time{},
		)
		if prepErr != nil {
			preparedErr = prepErr
			break
		}
		plan.frames = prepared
		for fi := range plan.frames {
			plan.frames[fi].start += plan.start
			plan.frames[fi].end += plan.start
		}
	}
	if preparedErr != nil {
		for i := range plans {
			releasePreparedDictFrames(plans[i].frames)
			putVlogPreparedFrames(plans[i].frames)
			plans[i].frames = nil
		}
		for i := range requests {
			ack := requests[i].ack
			if ack == nil {
				continue
			}
			ack.ptr = page.ValuePtr{}
			ack.retainPath = ""
			ack.err = preparedErr
			ack.wg.Done()
		}
		return
	}
	for i := range plans {
		if len(plans[i].frames) == 0 {
			continue
		}
		defer func(pi int) {
			releasePreparedDictFrames(plans[pi].frames)
			putVlogPreparedFrames(plans[pi].frames)
			plans[pi].frames = nil
		}(i)
	}

	var (
		ptrs        []page.ValuePtr
		startSize   int64
		totalBytes  int64
		framesTotal int
		framesTried int
		framesKept  int
		retainPath  string
		probeKept   bool
		dictRaw     int
		dictStored  int
		dictRecords int
		err         error
	)

	l.vlogMu.Lock()
	w := l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		for i := range requests {
			ack := requests[i].ack
			if ack == nil {
				continue
			}
			ack.ptr = page.ValuePtr{}
			ack.retainPath = ""
			ack.err = errWALUnavailable
			ack.wg.Done()
		}
		return
	}
	firstPath := l.vlogPath
	var retainPaths []string
	noteRotatePath := func(path string) {
		if path == "" {
			return
		}
		if retainPaths == nil {
			if firstPath != "" {
				retainPaths = append(retainPaths, firstPath)
			} else {
				retainPaths = make([]string, 0, 2)
			}
		}
		if len(retainPaths) == 0 || retainPaths[len(retainPaths)-1] != path {
			retainPaths = append(retainPaths, path)
		}
	}
	if rotateErr := db.rotateValueLogForMaxSegmentMuHeld(l, w); rotateErr != nil {
		l.vlogMu.Unlock()
		for i := range requests {
			ack := requests[i].ack
			if ack == nil {
				continue
			}
			ack.ptr = page.ValuePtr{}
			ack.retainPath = ""
			ack.err = rotateErr
			ack.wg.Done()
		}
		return
	}
	// Rotation may replace the writer; reload it so subsequent appends use the
	// correct segment.
	w = l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		for i := range requests {
			ack := requests[i].ack
			if ack == nil {
				continue
			}
			ack.ptr = page.ValuePtr{}
			ack.retainPath = ""
			ack.err = errWALUnavailable
			ack.wg.Done()
		}
		return
	}
	if maxBytes := db.valueLogMaxSegmentBytesForLane(l); maxBytes > 0 {
		// Pre-rotate to ensure this batch never produces pointers with offsets
		// outside the packed-offset cap.
		est := int64(rawPayloadBytes) + int64(len(records))*64
		if est < 0 {
			est = 0
		}
		if est > 0 && w.Size() > maxBytes-est {
			if rotateErr := db.rotateValueLogMuHeld(l); rotateErr != nil {
				l.vlogMu.Unlock()
				for i := range requests {
					ack := requests[i].ack
					if ack == nil {
						continue
					}
					ack.ptr = page.ValuePtr{}
					ack.retainPath = ""
					ack.err = rotateErr
					ack.wg.Done()
				}
				return
			}
			// Reload writer after rotation to ensure subsequent operations use the
			// new segment.
			w = l.vlog
			if w == nil {
				l.vlogMu.Unlock()
				for i := range requests {
					ack := requests[i].ack
					if ack == nil {
						continue
					}
					ack.ptr = page.ValuePtr{}
					ack.retainPath = ""
					ack.err = errWALUnavailable
					ack.wg.Done()
				}
				return
			}
		}
	}
	if l.vlogPath != firstPath {
		noteRotatePath(l.vlogPath)
	}
	if l.vlogCaps.writer != w {
		l.vlogCaps = computeVlogWriterCaps(w)
	}
	caps := l.vlogCaps
	rawWriterInto := caps.rawInto
	rawBufferedInto := caps.rawBuf
	policySetter := caps.keep
	preparedAppender := caps.prepared
	startSize = w.Size()

	baseIoNsPerStored := 0.0
	baseEncodeNsPerRaw := 0.0
	keepSafetyMargin := db.valueLogAutotuneSafetyMargin()
	if policySetter != nil {
		snap := db.valueLogAutotuneMetrics.snapshot()
		baseIoNsPerStored = snap.IoNsPerStoredByte
		baseEncodeNsPerRaw = snap.EncodeNsPerRawByte
		if db.valueLogAutotuneOptions.Mode == valuelog.AutotuneOff {
			baseIoNsPerStored = 0
			baseEncodeNsPerRaw = 0
		}
	}
	autoSelectorMode := normalizeVlogCompressionMode(db.valueLogCompressionMode) == vlogCompressionAuto

	ptrs = getValueLogPtrsCap(len(records))
	ptrs = ptrs[:len(records)]
	defer putValueLogPtrsNoClear(ptrs)
	statsWriter := caps.stats
	statsWriterInto := caps.statsInto
	for pi := range plans {
		plan := &plans[pi]
		if err != nil {
			break
		}
		if policySetter != nil {
			ioNsPerStored := baseIoNsPerStored
			encodeNsPerRaw := baseEncodeNsPerRaw
			// In auto mode, block candidate evaluation must observe real compressed
			// output (not keep-policy short-circuits), same as explicit probes.
			if plan.probe || (autoSelectorMode && plan.writeMode == vlogWriteBlock) {
				ioNsPerStored = 0
				encodeNsPerRaw = 0
			}
			policySetter.SetKeepPolicy(ioNsPerStored, encodeNsPerRaw, keepSafetyMargin)
		}
		db.setVlogWriterMode(l, w, plan.writeMode, plan.blockCodec)
		planStart := time.Now()
		beforePlanSize := w.Size()
		planStoredBytes := 0
		segment := records[plan.start:plan.end]
		if plan.writeMode != vlogWriteDict || plan.dictID == 0 {
			if len(segment) == 1 {
				ptrs[plan.start], err = w.Append(0, nil, segment[0].RID, segment[0].Value)
				if err == nil {
					framesTotal++
				}
			} else if plan.writeMode == vlogWriteOff && (rawWriterInto != nil || rawBufferedInto != nil) {
				useBufferedRaw := rawBufferedInto != nil && db.outerLeafFenceV2Enabled() && allOuterLeafRecordValues(segment)
				var (
					stats    valuelog.FrameStats
					batchErr error
				)
				if useBufferedRaw {
					_, stats, batchErr = rawBufferedInto.AppendRawFramesBufferedInto(segment, plan.k, ptrs[plan.start:plan.end])
				} else if rawWriterInto != nil {
					_, stats, batchErr = rawWriterInto.AppendRawFramesWritevInto(segment, plan.k, ptrs[plan.start:plan.end])
				} else {
					batchErr = errors.New("cachingdb: raw grouped append writer unavailable")
				}
				err = batchErr
				if err == nil && plan.k > 0 {
					framesTotal += (len(segment) + plan.k - 1) / plan.k
					planStoredBytes += stats.StoredPayloadBytes
				}
			} else {
				for i := plan.start; i < plan.end; i += plan.k {
					end := i + plan.k
					if end > plan.end {
						end = plan.end
					}
					frame := records[i:end]
					if statsWriterInto != nil {
						dst := ptrs[i:end]
						_, stats, frameErr := statsWriterInto.AppendFrameWithStatsInto(0, nil, frame, dst)
						if frameErr != nil {
							err = frameErr
							break
						}
						framesTotal++
						planStoredBytes += stats.StoredPayloadBytes
						continue
					}
					if statsWriter != nil {
						framePtrs, stats, frameErr := statsWriter.AppendFrameWithStats(0, nil, frame)
						if frameErr != nil {
							err = frameErr
							break
						}
						copy(ptrs[i:end], framePtrs)
						framesTotal++
						planStoredBytes += stats.StoredPayloadBytes
						continue
					}
					framePtrs, frameErr := w.AppendFrame(0, nil, frame)
					if frameErr != nil {
						err = frameErr
						break
					}
					copy(ptrs[i:end], framePtrs)
					framesTotal++
				}
			}
			if err == nil {
				if planStoredBytes <= 0 {
					if delta := w.Size() - beforePlanSize; delta > 0 {
						planStoredBytes = int(delta)
					}
				}
				plan.storedBytes = planStoredBytes
				plan.wallNs = time.Since(planStart).Nanoseconds()
			}
			continue
		}

		dictRaw += plan.rawBytes
		dictRecords += len(segment)
		if preparedAppender != nil && len(plan.frames) > 0 {
			for fi := range plan.frames {
				pf := &plan.frames[fi]
				dst := ptrs[pf.start:pf.end]
				if _, frameErr := preparedAppender.AppendEncodedFrameInto(pf.body, pf.stats.Records, dst); frameErr != nil {
					err = frameErr
					break
				}
				releasePreparedDictFrame(pf)
				framesTotal++
				if pf.stats.Attempted {
					framesTried++
				}
				if pf.stats.Kept {
					framesKept++
					if plan.probe {
						probeKept = true
					}
				}
				if pf.stats.StoredPayloadBytes > 0 {
					dictStored += pf.stats.StoredPayloadBytes
					planStoredBytes += pf.stats.StoredPayloadBytes
				}
			}
		} else {
			for i := plan.start; i < plan.end; i += plan.k {
				end := i + plan.k
				if end > plan.end {
					end = plan.end
				}
				frame := records[i:end]
				if statsWriterInto != nil {
					dst := ptrs[i:end]
					_, stats, frameErr := statsWriterInto.AppendFrameWithStatsInto(plan.dictID, plan.dict, frame, dst)
					if frameErr != nil {
						err = frameErr
						break
					}
					framesTotal++
					if stats.Attempted {
						framesTried++
					}
					if stats.Kept {
						framesKept++
						if plan.probe {
							probeKept = true
						}
					}
					if stats.StoredPayloadBytes > 0 {
						dictStored += stats.StoredPayloadBytes
						planStoredBytes += stats.StoredPayloadBytes
					}
					continue
				}
				if statsWriter != nil {
					framePtrs, stats, frameErr := statsWriter.AppendFrameWithStats(plan.dictID, plan.dict, frame)
					if frameErr != nil {
						err = frameErr
						break
					}
					copy(ptrs[i:end], framePtrs)
					framesTotal++
					if stats.Attempted {
						framesTried++
					}
					if stats.Kept {
						framesKept++
						if plan.probe {
							probeKept = true
						}
					}
					if stats.StoredPayloadBytes > 0 {
						dictStored += stats.StoredPayloadBytes
						planStoredBytes += stats.StoredPayloadBytes
					}
					continue
				}
				framePtrs, frameErr := w.AppendFrame(plan.dictID, plan.dict, frame)
				if frameErr != nil {
					err = frameErr
					break
				}
				copy(ptrs[i:end], framePtrs)
				framesTotal++
			}
		}
		if err == nil {
			if planStoredBytes <= 0 {
				if delta := w.Size() - beforePlanSize; delta > 0 {
					planStoredBytes = int(delta)
				}
			}
			plan.storedBytes = planStoredBytes
			plan.wallNs = time.Since(planStart).Nanoseconds()
		}
	}

	deferredFlushed := false
	if err == nil {
		switch {
		case needSync:
			err = w.Sync()
		case needFlush:
			err = w.Flush()
		default:
			if db.shouldFlushDeferredValueLog(vlogWriteOff, records) {
				err = w.Flush()
				deferredFlushed = err == nil
			}
		}
	}
	if err == nil {
		totalBytes = w.Size() - startSize
	}
	if err == nil {
		if needSync || needFlush || deferredFlushed {
			l.vlogDirty.Store(false)
		} else if totalBytes > 0 {
			l.vlogDirty.Store(true)
		}
	}
	if err == nil && totalBytes > 0 {
		l.vlogLiveBytes.Add(totalBytes)
	}
	if err == nil && l.vlogPath != "" && l.vlogPath != l.vlogRetainedPath {
		l.vlogRetainedPath = l.vlogPath
		retainPath = l.vlogPath
	}
	l.vlogMu.Unlock()
	if len(retainPaths) > 0 {
		for _, path := range retainPaths {
			db.markValueLogRetain(path)
		}
	}

	if err == nil && framesTotal > 0 {
		db.valueLogDictFrames.total.Add(uint64(framesTotal))
		if framesTried > 0 {
			db.valueLogDictFrames.attempted.Add(uint64(framesTried))
		}
		if framesKept > 0 {
			db.valueLogDictFrames.kept.Add(uint64(framesKept))
		}
	}

	if dictRaw > 0 {
		if dictStored <= 0 {
			dictStored = dictRaw
		}
		db.valueLogDictObservePayload(uint64(dictRaw), uint64(dictStored), dictRecords)
	}
	if probeKept {
		db.valueLogDictPauseRemaining.Store(0)
		if db.valueLogDictProbeBytes > 0 {
			db.valueLogDictProbeRemaining.Store(db.valueLogDictProbeBytes)
		}
		db.valueLogDictIncompressibleHoldRemaining.Store(0)
		db.valueLogDictIncompressibleHitStreak.Store(0)
		if db.valueLogDictIncompressibleProbeBytes > 0 {
			db.valueLogDictIncompressibleProbeRemaining.Store(db.valueLogDictIncompressibleProbeBytes)
		}
	}
	if err == nil {
		for i := range plans {
			plan := &plans[i]
			rawForSelector := plan.rawBytes
			if rawForSelector <= 0 {
				continue
			}
			storedForSelector := plan.storedBytes
			if storedForSelector <= 0 {
				if plan.writeMode == vlogWriteOff {
					storedForSelector = rawForSelector
				} else {
					storedForSelector = rawForSelector
				}
			}
			codec := plan.blockCodec
			if plan.writeMode != vlogWriteBlock {
				codec = db.valueLogBlockCodec
			}
			unitForSelector := rawForSelector
			if recordsInPlan := plan.end - plan.start; recordsInPlan > 0 {
				unitForSelector = rawForSelector / recordsInPlan
			}
			db.observeVlogWriteMode(l, plan.writeMode, codec, rawForSelector, unitForSelector, storedForSelector, plan.probe, plan.wallNs)
		}
	}

	if err != nil {
		for i := range requests {
			ack := requests[i].ack
			if ack == nil {
				continue
			}
			ack.ptr = page.ValuePtr{}
			ack.retainPath = ""
			ack.err = err
			ack.wg.Done()
		}
		return
	}

	for i := range requests {
		ack := requests[i].ack
		if ack == nil {
			continue
		}
		ack.ptr = ptrs[i]
		if i == 0 {
			ack.retainPath = retainPath
		} else {
			ack.retainPath = ""
		}
		ack.err = nil
		ack.wg.Done()
	}
}

// journalDurability represents the durability boundary for journal writes.
// When WAL is enabled, payload durability must complete before the
// commit-intent durability for sync writes.
type journalDurability uint8

const (
	journalDurabilityNone journalDurability = iota
	journalDurabilityFlush
	journalDurabilitySync
)

func (db *DB) appendWAL(l *lane, records []logRecord, durability journalDurability) error {
	if db.disableJournal {
		return nil
	}
	if len(records) == 0 {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}
	select {
	case <-db.closeCh:
		return errWALClosed
	default:
	}
	db.walAckMu.Lock()
	if db.walErr != nil {
		err := db.walErr
		db.walAckMu.Unlock()
		return err
	}
	db.walAckMu.Unlock()

	if len(records) == 1 {
		return db.appendWALOneChecked(l, records[0], durability)
	}

	db.assignCommitSeq(records)

	switch durability {
	case journalDurabilitySync:
		return db.appendWALDirect(l, records, true)
	case journalDurabilityFlush:
		return db.appendWALInline(l, records, true)
	default:
		return db.appendWALInline(l, records, false)
	}
}

func (db *DB) appendWALOne(l *lane, record logRecord, durability journalDurability) error {
	if db.disableJournal {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}
	select {
	case <-db.closeCh:
		return errWALClosed
	default:
	}
	db.walAckMu.Lock()
	if db.walErr != nil {
		err := db.walErr
		db.walAckMu.Unlock()
		return err
	}
	db.walAckMu.Unlock()
	return db.appendWALOneChecked(l, record, durability)
}

func (db *DB) appendWALOneChecked(l *lane, record logRecord, durability journalDurability) error {
	record.Seq = db.nextCommitSeq.Add(1)
	switch durability {
	case journalDurabilitySync:
		return db.appendWALDirect(l, []logRecord{record}, true)
	case journalDurabilityFlush:
		return db.appendWALInlineOne(l, record, true)
	default:
		return db.appendWALInlineOne(l, record, false)
	}
}

type preparedDictFrame struct {
	start   int
	end     int
	body    []byte
	bodyBuf *vlogPreparedFrameBody
	stats   valuelog.FrameStats
}

func releasePreparedDictFrame(frame *preparedDictFrame) {
	if frame == nil || frame.bodyBuf == nil {
		return
	}
	putVlogPreparedFrameBody(frame.bodyBuf)
	frame.bodyBuf = nil
	frame.body = nil
}

func releasePreparedDictFrames(frames []preparedDictFrame) {
	for i := range frames {
		releasePreparedDictFrame(&frames[i])
	}
}

func (db *DB) valueLogKeepPolicy() (ioNsPerStoredByte, encodeNsPerRawByte, safetyMargin float64) {
	safetyMargin = db.valueLogAutotuneSafetyMargin()
	if db.valueLogAutotuneOptions.Mode == valuelog.AutotuneOff {
		return 0, 0, safetyMargin
	}
	snap := db.valueLogAutotuneMetrics.snapshot()
	return snap.IoNsPerStoredByte, snap.EncodeNsPerRawByte, safetyMargin
}

func (db *DB) shouldUseVlogDictPrepWorkers(l *lane, frameCount, rawPayloadBytes int) bool {
	if l == nil || l.vlogPrepCh == nil || l.vlogPrepMaxWorkers < 2 {
		return false
	}
	if frameCount <= 0 {
		return false
	}
	if frameCount < 2 {
		return false
	}
	if rawPayloadBytes <= 0 {
		return false
	}
	if rawPayloadBytes < 128<<10 {
		return false
	}
	return true
}

func (db *DB) shouldQueueValueLogOne(l *lane, dictID uint64, valueLen int, durability journalDurability, writeMode vlogCompressionWriteMode, wallStart time.Time) bool {
	if l == nil || l.vlogCh == nil {
		return false
	}
	if durability == journalDurabilitySync {
		return false
	}
	// Preserve direct timing mode for autotune/profile callers.
	if !wallStart.IsZero() {
		return false
	}
	// Block mode requires direct append so per-frame codec metadata and grouping
	// stay on the caller path.
	if writeMode == vlogWriteBlock {
		return false
	}
	// Force-pointer profiles prefer queue coalescing; appendValueLogOne still
	// takes an uncontended direct fast path before enqueueing.
	if db.forceValueLogPointers {
		return true
	}
	// Dict path benefits from queue coalescing even for small values.
	if writeMode == vlogWriteDict && dictID != 0 {
		return true
	}
	// Always queue large values.
	if valueLen >= vlogQueueMinValueSize {
		return true
	}
	// Adaptive path: queue when contention/backlog is visible.
	if l.vlogQueueing.Load() {
		return true
	}
	if len(l.vlogCh) > 0 {
		return true
	}
	return false
}

func (db *DB) prepareAppendDictFrames(
	l *lane,
	dictID uint64,
	dict []byte,
	records []valuelog.Record,
	k int,
	rawPayloadBytes int,
	ioNsPerStoredByte float64,
	encodeNsPerRawByte float64,
	safetyMargin float64,
	wallStart time.Time,
) ([]preparedDictFrame, int64, error) {
	if dictID == 0 || len(dict) == 0 || len(records) == 0 {
		return nil, 0, nil
	}
	if k <= 0 {
		k = 1
	}
	frameCount := (len(records) + k - 1) / k
	if frameCount <= 0 {
		return nil, 0, nil
	}
	useWorkers := db.shouldUseVlogDictPrepWorkers(l, frameCount, rawPayloadBytes)
	prepStart := time.Now()
	prepared := getVlogPreparedFrames(frameCount)
	clear(prepared)
	if !useWorkers {
		// Keep dict frame encode work out of vlogMu even when worker threads are
		// unavailable. This reduces lock hold time on small and medium batches.
		preparer := valuelog.NewFramePreparer()
		preparer.SetDictFrameEncoderOptions(db.valueLogDictFrameEncodeLevel, db.valueLogDictFrameEnableEntropy)
		preparer.SetKeepPolicy(ioNsPerStoredByte, encodeNsPerRawByte, safetyMargin)
		preparer.SetEncodeSampleStride(0)
		for fi := 0; fi < frameCount; fi++ {
			start := fi * k
			end := start + k
			if end > len(records) {
				end = len(records)
			}
			bodyBuf := getVlogPreparedFrameBody()
			body, stats, err := preparer.PrepareFrameInto(bodyBuf.buf[:0], dictID, dict, records[start:end])
			if err != nil {
				putVlogPreparedFrameBody(bodyBuf)
				releasePreparedDictFrames(prepared)
				putVlogPreparedFrames(prepared)
				return nil, time.Since(prepStart).Nanoseconds(), err
			}
			bodyBuf.buf = body
			prepared[fi] = preparedDictFrame{
				start:   start,
				end:     end,
				body:    body,
				bodyBuf: bodyBuf,
				stats:   stats,
			}
		}
		return prepared, time.Since(prepStart).Nanoseconds(), nil
	}
	wantedWorkers := frameCount/8 + 1
	if rawPayloadBytes >= 1<<20 {
		wantedWorkers++
	}
	db.ensureVlogDictPrepWorkers(l, wantedWorkers)
	// Parallel prep frames can execute across multiple goroutines, so summing
	// per-frame encode times would overcount relative to write-path wall time.
	// Leave encodeNs unset for worker-prepared frames to avoid poisoning
	// autotune keep-policy estimates.
	measureEncode := false
	results := getVlogDictPrepareResults(frameCount)
	for fi := 0; fi < frameCount; fi++ {
		start := fi * k
		end := start + k
		if end > len(records) {
			end = len(records)
		}
		task := vlogDictPrepareTask{
			fi:             fi,
			dictID:         dictID,
			dict:           dict,
			records:        records[start:end],
			level:          db.valueLogDictFrameEncodeLevel,
			enableEntropy:  db.valueLogDictFrameEnableEntropy,
			ioNsPerStored:  ioNsPerStoredByte,
			encodeNsPerRaw: encodeNsPerRawByte,
			safetyMargin:   safetyMargin,
			measureEncode:  measureEncode,
			out:            results,
		}
		select {
		case l.vlogPrepCh <- task:
		case <-db.closeCh:
			releasePreparedDictFrames(prepared)
			putVlogPreparedFrames(prepared)
			// Workers may still publish into results after close is observed; do
			// not pool this channel on early return.
			return nil, 0, errWALClosed
		}
	}

	var firstErr error
	for collected := 0; collected < frameCount; collected++ {
		select {
		case res := <-results:
			if res.err != nil {
				if firstErr == nil {
					firstErr = res.err
				}
				continue
			}
			start := res.fi * k
			end := start + k
			if end > len(records) {
				end = len(records)
			}
			releasePreparedDictFrame(&prepared[res.fi])
			prepared[res.fi] = preparedDictFrame{
				start:   start,
				end:     end,
				body:    res.body,
				bodyBuf: res.bodyBuf,
				stats:   res.stats,
			}
		case <-db.closeCh:
			releasePreparedDictFrames(prepared)
			putVlogPreparedFrames(prepared)
			// Workers may still publish into results after close is observed; do
			// not pool this channel on early return.
			return nil, time.Since(prepStart).Nanoseconds(), errWALClosed
		}
	}
	putVlogDictPrepareResults(results)
	if firstErr != nil {
		releasePreparedDictFrames(prepared)
		putVlogPreparedFrames(prepared)
		return nil, time.Since(prepStart).Nanoseconds(), firstErr
	}
	return prepared, time.Since(prepStart).Nanoseconds(), nil
}

func (db *DB) appendValueLog(l *lane, dictID uint64, dict []byte, records []valuelog.Record, durability journalDurability) ([]page.ValuePtr, error) {
	if !db.splitValueLogEnabled() {
		return nil, errWALUnavailable
	}
	if len(records) == 0 {
		return nil, nil
	}
	if l == nil {
		return nil, errWALUnavailable
	}
	select {
	case <-db.closeCh:
		return nil, errWALClosed
	default:
	}
	wallStart := time.Time{}
	if db.needsVlogAutotuneTiming() {
		wallStart = db.valueLogAutotuneMetrics.now()
	}
	selectorStart := time.Now()

	var (
		bytesWrittenTotal int64
		bytesWrittenLive  int64
		ptrs              []page.ValuePtr
		err               error
	)

	rawPayloadBytes := 0
	routeOuterLeafMode := strings.TrimSpace(db.indexOuterLeafMode) == backenddb.IndexOuterLeafModeV1LeafLogRoute
	for i := range records {
		rawPayloadBytes += len(records[i].Value)
	}
	templatePrepass := false
	if db.valueLogTemplateEnabled && db.valueLogTemplateMode != template.TemplateOff {
		if db.valueLogTemplateMode == template.TemplateOnly {
			dictID = 0
			dict = nil
		} else if db.valueLogTemplateMode == template.TemplatePrepass {
			templatePrepass = true
		}
	}

	if dictID == 0 || templatePrepass {
		records, _ = db.valueLogTemplateEncodeRecords(records)
		rawPayloadBytes = 0
		for i := range records {
			rawPayloadBytes += len(records[i].Value)
		}
	}

	// Track v2 outer-leaf payload batches so raw-mode append paths can use the
	// buffered frame writer when selected.
	autoOuterLeafPayloads := db.outerLeafV2Enabled() &&
		!templatePrepass &&
		dictID == 0 &&
		allOuterLeafRecordValues(records)

	mode := normalizeVlogCompressionMode(db.valueLogCompressionMode)
	selectorPayloadBytes := rawPayloadBytes
	selectorUnitPayloadBytes := rawPayloadBytes
	if n := len(records); n > 0 {
		selectorUnitPayloadBytes = rawPayloadBytes / n
	}
	writeMode, blockCodec, selectorProbe := db.resolveVlogWriteMode(l, dictID, selectorPayloadBytes, selectorUnitPayloadBytes)
	if autoOuterLeafPayloads && mode == vlogCompressionAuto {
		writeMode = vlogWriteOff
	}
	blockMode := writeMode == vlogWriteBlock
	probeCompression := selectorProbe
	paused := false
	if writeMode != vlogWriteDict {
		dictID = 0
		dict = nil
	}

	if dictID != 0 {
		attemptCompression, dictProbe, dictPaused := db.valueLogDictShouldAttemptCompression(rawPayloadBytes)
		probeCompression = probeCompression || dictProbe
		paused = dictPaused
		if !attemptCompression {
			dictID = 0
			dict = nil
			writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
			blockMode = writeMode == vlogWriteBlock
		}
	}
	if dictID != 0 && db.shouldBypassValueLogDictForRecords(records, probeCompression) {
		dictID = 0
		dict = nil
		writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
		blockMode = writeMode == vlogWriteBlock
	}
	if dictID != 0 && db.valueLogAutotuneOptions.DisableBelowValueBytes > 0 {
		avg := rawPayloadBytes / len(records)
		if avg < db.valueLogAutotuneOptions.DisableBelowValueBytes {
			dictID = 0
			dict = nil
			writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
			blockMode = writeMode == vlogWriteBlock
		}
	}
	if dictID != 0 && len(dict) == 0 {
		if b, dictErr := db.dictBytes(context.Background(), dictID); dictErr == nil {
			dict = b
		} else {
			dictID = 0
			dict = nil
			writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
			blockMode = writeMode == vlogWriteBlock
		}
	}
	switch mode {
	case vlogCompressionDefault, vlogCompressionDict:
		db.valueLogDictCollectSamples(records)
	case vlogCompressionAuto:
		// In auto mode, skip background dict sampling while fully bypassing
		// compression so incompressible workloads stay close to off-mode cost.
		allowDictSampling := true
		if l != nil && l.vlogCompressionSelector != nil {
			allowDictSampling = l.vlogCompressionSelector.allowDictSampling(writeMode)
		}
		if writeMode != vlogWriteOff && allowDictSampling {
			if db.valueLogDictLastAppliedDictID.Load() != 0 && writeMode != vlogWriteDict {
				if selectorUnitPayloadBytes <= 256 {
					allowDictSampling = false
				} else {
					allowDictSampling = db.valueLogDictShouldCollectPaused()
				}
			}
		}
		if writeMode != vlogWriteOff && allowDictSampling {
			db.valueLogDictCollectSamples(records)
		}
	}

	k := 1
	if dictID != 0 && len(dict) > 0 {
		k = db.valueLogDictK(dictID)
		k = db.chooseValueLogDictWriteK(k, len(records), rawPayloadBytes)
		if mode == vlogCompressionAuto && normalizeVlogAutoPolicy(db.valueLogAutoPolicy) != vlogAutoSize && k > 16 {
			k = 16
		}
	} else if blockMode && len(records) > 1 {
		k = db.chooseValueLogBlockWriteK(l, len(records), rawPayloadBytes, blockCodec)
	} else if len(records) > 1 {
		// Even when dictionary compression is disabled/paused, grouping records into
		// frames reduces per-record overhead (CRC/header writes) on append-heavy
		// workloads.
		//
		// When no dict is available, we write raw frames (uncompressed) and still
		// benefit from fewer syscalls and less framing work.
		if paused && db.disableJournal {
			k = valuelog.MaxFrameK
		} else if cur := int(db.valueLogDictCurrentK.Load()); cur > 1 {
			k = cur
		} else {
			k = 8
			if db.disableJournal && db.forceValueLogPointers {
				k = 16
			}
		}
	}
	if dictID != 0 && len(dict) > 0 && db.disableJournal {
		// When the redo/journal log is disabled (ingest-mode), favor maximum frame
		// grouping for throughput. This reduces per-record framing overhead and
		// syscall pressure, and is typically safe for write-heavy workloads where
		// random point reads are not the dominant cost.
		k = valuelog.MaxFrameK
	}
	if dictID != 0 && len(dict) > 0 {
		k = db.clampValueLogDictK(k)
	} else {
		if k < 1 {
			k = 1
		}
		if k > valuelog.MaxFrameK {
			k = valuelog.MaxFrameK
		}
	}

	ioNsPerStored, encodeNsPerRaw, safetyMargin := db.valueLogKeepPolicy()
	if probeCompression {
		// Probe writes must actually attempt compression to detect recovery from a
		// paused/degraded stream. Keep-policy gating can short-circuit probes when
		// historical encode-cost estimates are stale or pessimistic.
		ioNsPerStored = 0
		encodeNsPerRaw = 0
	}
	preparedDictFrames, prepEncodeWallNs, prepareErr := db.prepareAppendDictFrames(
		l,
		dictID,
		dict,
		records,
		k,
		rawPayloadBytes,
		ioNsPerStored,
		encodeNsPerRaw,
		safetyMargin,
		wallStart,
	)
	if prepareErr != nil {
		return nil, prepareErr
	}
	if len(preparedDictFrames) > 0 {
		defer func() {
			releasePreparedDictFrames(preparedDictFrames)
			putVlogPreparedFrames(preparedDictFrames)
		}()
	}

	finalWriteMode := vlogWriteOff
	switch {
	case dictID != 0 && len(dict) > 0:
		finalWriteMode = vlogWriteDict
	case blockMode:
		finalWriteMode = vlogWriteBlock
	default:
		finalWriteMode = vlogWriteOff
	}
	finalBlockCodec := blockCodec
	if finalWriteMode != vlogWriteBlock {
		finalBlockCodec = db.valueLogBlockCodec
	}
	ioNsPerStoredForWriter := ioNsPerStored
	encodeNsPerRawForWriter := encodeNsPerRaw
	if normalizeVlogCompressionMode(db.valueLogCompressionMode) == vlogCompressionAuto && finalWriteMode == vlogWriteBlock {
		// Keep-policy bypass is required for fair auto-mode block evaluation; the
		// selector should decide whether block stays active based on real outcomes.
		ioNsPerStoredForWriter = 0
		encodeNsPerRawForWriter = 0
	}

	l.vlogMu.Lock()
	w := l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		return nil, errWALUnavailable
	}
	firstPath := l.vlogPath
	var retainPaths []string
	noteRotatePath := func(path string) {
		if path == "" {
			return
		}
		if retainPaths == nil {
			if firstPath != "" {
				retainPaths = append(retainPaths, firstPath)
			} else {
				retainPaths = make([]string, 0, 2)
			}
		}
		if len(retainPaths) == 0 || retainPaths[len(retainPaths)-1] != path {
			retainPaths = append(retainPaths, path)
		}
	}
	if rotateErr := db.rotateValueLogForMaxSegmentMuHeld(l, w); rotateErr != nil {
		l.vlogMu.Unlock()
		return nil, rotateErr
	}
	if l.vlogPath != firstPath {
		noteRotatePath(l.vlogPath)
	}
	// Rotation may replace the writer; reload it before appending.
	w = l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		return nil, errWALUnavailable
	}
	if l.vlogCaps.writer != w {
		l.vlogCaps = computeVlogWriterCaps(w)
	}
	caps := l.vlogCaps
	db.setVlogWriterMode(l, w, finalWriteMode, finalBlockCodec)
	policySetter := caps.keep
	statsWriter := caps.stats
	statsWriterInto := caps.statsInto
	rawWriterInto := caps.rawInto
	rawBufferedInto := caps.rawBuf
	preparedAppender := caps.prepared
	hasStats := statsWriter != nil
	hasInto := statsWriterInto != nil
	hasRawInto := rawWriterInto != nil
	hasRawBufferedInto := rawBufferedInto != nil
	usePreparedDictFrames := dictID != 0 && len(dict) > 0 && preparedAppender != nil && len(preparedDictFrames) > 0
	segmentStartSize := w.Size()

	if policySetter != nil && !usePreparedDictFrames {
		policySetter.SetKeepPolicy(ioNsPerStoredForWriter, encodeNsPerRawForWriter, safetyMargin)
	}

	storedPayloadBytes := 0
	rawFrameBytes := 0
	frameRecords := 0
	framesTotal := 0
	framesAttempted := 0
	framesKept := 0
	encodeNsTotal := int64(0)
	encodeRawBytes := 0
	rawBatchUsed := false
	durableBoundary := false
	if dictID == 0 && (hasRawInto || hasRawBufferedInto) && finalWriteMode != vlogWriteBlock && len(records) > 1 {
		if maxBytes := db.valueLogMaxSegmentBytesForLane(l); maxBytes > 0 {
			// Ensure the entire raw batch fits within the packed-offset cap so
			// AppendRawFramesWritevInto never returns pointers with out-of-range
			// offsets.
			est := int64(rawPayloadBytes) + int64(len(records))*64
			if est < 0 {
				est = 0
			}
			if est > 0 && w.Size() > maxBytes-est {
				if rotateErr := db.rotateValueLogMuHeld(l); rotateErr != nil {
					l.vlogMu.Unlock()
					return nil, rotateErr
				}
				noteRotatePath(l.vlogPath)
				// Reload writer after rotation so subsequent appends go to the new
				// segment and capabilities match the writer instance.
				w = l.vlog
				if w == nil {
					l.vlogMu.Unlock()
					return nil, errWALUnavailable
				}
				if l.vlogCaps.writer != w {
					l.vlogCaps = computeVlogWriterCaps(w)
				}
				caps = l.vlogCaps
				db.setVlogWriterMode(l, w, finalWriteMode, finalBlockCodec)
				statsWriter = caps.stats
				statsWriterInto = caps.statsInto
				rawWriterInto = caps.rawInto
				rawBufferedInto = caps.rawBuf
				hasStats = statsWriter != nil
				hasInto = statsWriterInto != nil
				hasRawInto = rawWriterInto != nil
				hasRawBufferedInto = rawBufferedInto != nil
				segmentStartSize = w.Size()
			}
		}
	}
	if usePreparedDictFrames {
		rawBatchUsed = true
		ptrs = getValueLogPtrs(len(records))
		for fi := range preparedDictFrames {
			pf := &preparedDictFrames[fi]
			dst := ptrs[pf.start:pf.end]
			if _, frameErr := preparedAppender.AppendEncodedFrameInto(pf.body, pf.stats.Records, dst); frameErr != nil {
				err = frameErr
				break
			}
			releasePreparedDictFrame(pf)
			rawFrameBytes += pf.stats.RawPayloadBytes
			storedPayloadBytes += pf.stats.StoredPayloadBytes
			frameRecords += pf.stats.Records
			framesTotal++
			if pf.stats.Attempted {
				framesAttempted++
			}
			if pf.stats.Kept {
				framesKept++
			}
			if pf.stats.EncodeNs > 0 && pf.stats.RawPayloadBytes > 0 {
				encodeNsTotal += pf.stats.EncodeNs
				encodeRawBytes += pf.stats.RawPayloadBytes
			}
		}
	}

	if err == nil && !rawBatchUsed {
		// Route mode is correctness-critical and must never publish invalid
		// pointers. Keep it on the mature per-frame append path.
		useRawBatch := !routeOuterLeafMode && dictID == 0 && finalWriteMode != vlogWriteBlock && len(records) > 1
		preferBufferedRaw := useRawBatch && autoOuterLeafPayloads && hasRawBufferedInto
		if useRawBatch && (preferBufferedRaw || hasRawInto) {
			ptrs = getValueLogPtrs(len(records))
			var (
				stats    valuelog.FrameStats
				batchErr error
			)
			if preferBufferedRaw {
				_, stats, batchErr = rawBufferedInto.AppendRawFramesBufferedInto(records, k, ptrs)
			} else {
				_, stats, batchErr = rawWriterInto.AppendRawFramesWritevInto(records, k, ptrs)
			}
			if batchErr != nil {
				err = batchErr
				putValueLogPtrs(ptrs)
				ptrs = nil
			} else {
				rawFrameBytes = stats.RawPayloadBytes
				storedPayloadBytes = stats.StoredPayloadBytes
				frameRecords = stats.Records
				rawBatchUsed = true
				if k > 0 {
					framesTotal = (len(records) + k - 1) / k
				}
			}
		} else {
			ptrs = getValueLogPtrs(len(records))
		}
	}
	if err == nil && !rawBatchUsed {
		for i := 0; i < len(records); i += k {
			if i > 0 && i%4096 == 0 {
				l.vlogMu.Unlock()
				runtime.Gosched()
				l.vlogMu.Lock()
				w = l.vlog
				if w == nil {
					l.vlogMu.Unlock()
					putValueLogPtrs(ptrs)
					return nil, errWALUnavailable
				}
				if l.vlogCaps.writer != w {
					l.vlogCaps = computeVlogWriterCaps(w)
				}
				caps = l.vlogCaps
				statsWriter = caps.stats
				statsWriterInto = caps.statsInto
				hasStats = statsWriter != nil
				hasInto = statsWriterInto != nil
			}

			if err == nil {
				if maxBytes := db.valueLogMaxSegmentBytesForLane(l); maxBytes > 0 && w.Size() > maxBytes {
					if delta := w.Size() - segmentStartSize; delta > 0 {
						bytesWrittenTotal += delta
					}
					if rotateErr := db.rotateValueLogMuHeld(l); rotateErr != nil {
						err = rotateErr
						break
					}
					noteRotatePath(l.vlogPath)
					// Rotation may replace the writer; reload it and refresh capabilities
					// for subsequent appends.
					w = l.vlog
					if w == nil {
						err = errWALUnavailable
						break
					}
					if l.vlogCaps.writer != w {
						l.vlogCaps = computeVlogWriterCaps(w)
					}
					caps = l.vlogCaps
					db.setVlogWriterMode(l, w, finalWriteMode, finalBlockCodec)
					statsWriter = caps.stats
					statsWriterInto = caps.statsInto
					hasStats = statsWriter != nil
					hasInto = statsWriterInto != nil
					segmentStartSize = w.Size()
				}
			}

			end := i + k
			if end > len(records) {
				end = len(records)
			}
			if hasInto {
				dst := ptrs[i:end]
				_, stats, frameErr := statsWriterInto.AppendFrameWithStatsInto(dictID, dict, records[i:end], dst)
				if frameErr != nil {
					err = frameErr
					break
				}
				rawFrameBytes += stats.RawPayloadBytes
				storedPayloadBytes += stats.StoredPayloadBytes
				frameRecords += stats.Records
				framesTotal++
				if stats.Attempted {
					framesAttempted++
				}
				if stats.Kept {
					framesKept++
				}
				if stats.EncodeNs > 0 && stats.RawPayloadBytes > 0 {
					encodeNsTotal += stats.EncodeNs
					encodeRawBytes += stats.RawPayloadBytes
				}
				continue
			}
			if hasStats {
				framePtrs, stats, frameErr := statsWriter.AppendFrameWithStats(dictID, dict, records[i:end])
				if frameErr != nil {
					err = frameErr
					break
				}
				copy(ptrs[i:end], framePtrs)
				rawFrameBytes += stats.RawPayloadBytes
				storedPayloadBytes += stats.StoredPayloadBytes
				frameRecords += stats.Records
				framesTotal++
				if stats.Attempted {
					framesAttempted++
				}
				if stats.Kept {
					framesKept++
				}
				if stats.EncodeNs > 0 && stats.RawPayloadBytes > 0 {
					encodeNsTotal += stats.EncodeNs
					encodeRawBytes += stats.RawPayloadBytes
				}
				continue
			}

			framePtrs, frameErr := w.AppendFrame(dictID, dict, records[i:end])
			if frameErr != nil {
				err = frameErr
				break
			}
			copy(ptrs[i:end], framePtrs)
			framesTotal++
		}
	}
	if err == nil {
		switch durability {
		case journalDurabilityFlush:
			err = w.Flush()
			durableBoundary = err == nil
		case journalDurabilitySync:
			err = w.Sync()
			durableBoundary = err == nil
		default:
			if db.shouldFlushDeferredValueLog(finalWriteMode, records) {
				// In deferred value-log mode, the index will publish pointers to
				// value-log records during the flush/commit path. Ensure the value-log
				// bytes are visible to readers even when durability is "none".
				err = w.Flush()
				durableBoundary = err == nil
			}
		}
	}
	if err == nil {
		bytesWrittenLive = w.Size() - segmentStartSize
		if bytesWrittenLive > 0 {
			bytesWrittenTotal += bytesWrittenLive
		}
	}
	if err == nil {
		if durableBoundary {
			l.vlogDirty.Store(false)
		} else if bytesWrittenLive > 0 {
			l.vlogDirty.Store(true)
		}
	}
	if db.testBeforeVlogUnlock != nil {
		db.testBeforeVlogUnlock(int(l.id))
	}
	l.vlogMu.Unlock()
	if len(retainPaths) > 0 {
		for _, path := range retainPaths {
			db.markValueLogRetain(path)
		}
	}
	if err != nil {
		putValueLogPtrs(ptrs)
		return nil, err
	}
	for i := range ptrs {
		if !page.IsValueLogFileID(ptrs[i].FileID) {
			putValueLogPtrs(ptrs)
			return nil, fmt.Errorf("cachingdb: appendValueLog produced invalid pointer idx=%d ptr=%+v", i, ptrs[i])
		}
	}
	if framesTotal > 0 {
		db.valueLogDictFrames.total.Add(uint64(framesTotal))
		if framesAttempted > 0 {
			db.valueLogDictFrames.attempted.Add(uint64(framesAttempted))
		}
		if framesKept > 0 {
			db.valueLogDictFrames.kept.Add(uint64(framesKept))
		}
	}
	if dictID != 0 && len(dict) > 0 {
		if rawFrameBytes == 0 {
			rawFrameBytes = rawPayloadBytes
		}
		if storedPayloadBytes == 0 {
			// Best-effort fallback when writer stats are unavailable.
			storedPayloadBytes = int(bytesWrittenTotal)
		}
		if frameRecords == 0 {
			frameRecords = len(records)
		}
		db.valueLogDictObservePayload(uint64(rawFrameBytes), uint64(storedPayloadBytes), frameRecords)
	}
	if probeCompression && framesKept > 0 {
		// A successful probe indicates compressibility returned; immediately
		// clear the pause so subsequent frames can use dictionaries again.
		db.valueLogDictPauseRemaining.Store(0)
		if db.valueLogDictProbeBytes > 0 {
			db.valueLogDictProbeRemaining.Store(db.valueLogDictProbeBytes)
		}
		db.valueLogDictIncompressibleHoldRemaining.Store(0)
		db.valueLogDictIncompressibleHitStreak.Store(0)
		if db.valueLogDictIncompressibleProbeBytes > 0 {
			db.valueLogDictIncompressibleProbeRemaining.Store(db.valueLogDictIncompressibleProbeBytes)
		}
	}
	rawForSelector := rawFrameBytes
	if rawForSelector == 0 {
		rawForSelector = rawPayloadBytes
	}
	unitForSelector := rawForSelector
	if n := len(records); n > 0 {
		unitForSelector = rawForSelector / n
	}
	storedForSelector := storedPayloadBytes
	if storedForSelector <= 0 {
		switch finalWriteMode {
		case vlogWriteOff:
			storedForSelector = rawForSelector
		default:
			if bytesWrittenTotal > 0 {
				storedForSelector = int(bytesWrittenTotal)
			} else {
				storedForSelector = rawForSelector
			}
		}
	}
	selectorWallNs := time.Since(selectorStart).Nanoseconds()
	db.observeVlogWriteMode(l, finalWriteMode, finalBlockCodec, rawForSelector, unitForSelector, storedForSelector, probeCompression, selectorWallNs)

	if bytesWrittenLive > 0 {
		l.vlogLiveBytes.Add(bytesWrittenLive)
		db.vlogGenerationChurnBytes.Add(uint64(bytesWrittenLive))
	}
	if usePreparedDictFrames && prepEncodeWallNs > 0 && rawFrameBytes > 0 && encodeRawBytes == 0 {
		// Prepared frames are encoded before taking vlogMu; account prep wall-time
		// once per batch so autotune keep-policy sees non-zero encode cost.
		encodeEstimateNs := prepEncodeWallNs
		if encodeEstimateNs < 0 {
			encodeEstimateNs = 0
		}
		if ioNsPerStored > 0 && rawFrameBytes > storedPayloadBytes {
			// Bound accounting to a fraction of observed IO savings so encode cost
			// estimates stay stable instead of oscillating into "always skip".
			maxBySavings := int64(float64(rawFrameBytes-storedPayloadBytes) * ioNsPerStored * 0.5)
			if maxBySavings > 0 && encodeEstimateNs > maxBySavings {
				encodeEstimateNs = maxBySavings
			}
		}
		if encodeNsPerRaw > 0 {
			// Bound wall-time accounting to a multiple of the current encode model
			// so unrelated scheduler stalls do not dominate encode estimates.
			maxNs := int64(float64(rawFrameBytes) * encodeNsPerRaw * 4)
			if maxNs > 0 && encodeEstimateNs > maxNs {
				encodeEstimateNs = maxNs
			}
		} else {
			const maxPrepEncodeNsPerRawByte = 8.0
			maxNs := int64(float64(rawFrameBytes) * maxPrepEncodeNsPerRawByte)
			if maxNs > 0 && encodeEstimateNs > maxNs {
				encodeEstimateNs = maxNs
			}
		}
		if encodeEstimateNs > 0 {
			encodeNsTotal += encodeEstimateNs
			encodeRawBytes += rawFrameBytes
		}
	}
	if !wallStart.IsZero() {
		storedForMetrics := storedPayloadBytes
		if storedForMetrics == 0 && bytesWrittenTotal > 0 {
			storedForMetrics = int(bytesWrittenTotal)
		}
		db.valueLogAutotuneMetrics.observe(wallStart, rawPayloadBytes, storedForMetrics, encodeNsTotal, encodeRawBytes)
	}
	return ptrs, nil
}

func (db *DB) appendValueLogOne(l *lane, dictID uint64, dict []byte, rid uint64, value []byte, durability journalDurability) (page.ValuePtr, string, error) {
	return db.appendValueLogOneWithKey(l, dictID, dict, rid, nil, value, durability)
}

func (db *DB) appendValueLogOneRaw(l *lane, dictID uint64, dict []byte, rid uint64, value []byte, durability journalDurability) (page.ValuePtr, string, error) {
	return db.appendValueLogOneInternal(l, dictID, dict, rid, nil, value, durability, false, true)
}

// appendFenceRIDJoinOversizedOne materializes an oversized v2_fenceptr value as
// a direct value-log record and returns the published pointer RID.
func (db *DB) appendFenceRIDJoinOversizedOne(l *lane, dictID uint64, key, value []byte, durability journalDurability) (ptr page.ValuePtr, rid uint64, retainPath string, err error) {
	if db == nil {
		return page.ValuePtr{}, 0, "", errWALUnavailable
	}
	if l == nil {
		return page.ValuePtr{}, 0, "", errWALUnavailable
	}
	if !db.splitValueLogEnabled() {
		return page.ValuePtr{}, 0, "", errWALUnavailable
	}
	rid = db.nextRID.Add(1)
	ptr, retainPath, err = db.appendValueLogOneRaw(l, dictID, nil, rid, value, durability)
	if err != nil {
		return page.ValuePtr{}, 0, "", err
	}
	return ptr, rid, retainPath, nil
}

func (db *DB) appendValueLogOneWithKey(l *lane, dictID uint64, dict []byte, rid uint64, key []byte, value []byte, durability journalDurability) (page.ValuePtr, string, error) {
	return db.appendValueLogOneInternal(l, dictID, dict, rid, key, value, durability, true, true)
}

func (db *DB) appendValueLogOneInternal(l *lane, dictID uint64, dict []byte, rid uint64, key []byte, value []byte, durability journalDurability, encodeOuterLeaf bool, allowQueue bool) (page.ValuePtr, string, error) {
	if !db.splitValueLogEnabled() {
		return page.ValuePtr{}, "", errWALUnavailable
	}
	if l == nil {
		return page.ValuePtr{}, "", errWALUnavailable
	}
	select {
	case <-db.closeCh:
		return page.ValuePtr{}, "", errWALClosed
	default:
	}
	if encodeOuterLeaf {
		encodedValue, encodeErr := db.encodeOuterLeafValue(key, value)
		if encodeErr != nil {
			return page.ValuePtr{}, "", encodeErr
		}
		value = encodedValue
	}
	wallStart := time.Time{}
	if db.needsVlogAutotuneTiming() {
		wallStart = db.valueLogAutotuneMetrics.now()
	}
	selectorStart := time.Now()

	var (
		totalBytes int64
		ptr        page.ValuePtr
		err        error
	)

	templatePrepass := false
	if db.valueLogTemplateEnabled && db.valueLogTemplateMode != template.TemplateOff {
		if db.valueLogTemplateMode == template.TemplateOnly {
			dictID = 0
			dict = nil
		} else if db.valueLogTemplateMode == template.TemplatePrepass {
			templatePrepass = true
		}
	}

	if (dictID == 0 || templatePrepass) && db.templateCompressionEnabled() {
		if payload, ok := db.valueLogTemplateEngine.Encode(nil, value, db.templateStore); ok {
			value = payload
		}
	}

	mode := normalizeVlogCompressionMode(db.valueLogCompressionMode)
	writeMode, blockCodec, selectorProbe := db.resolveVlogWriteMode(l, dictID, len(value), len(value))
	probeCompression := selectorProbe
	if writeMode != vlogWriteDict {
		dictID = 0
		dict = nil
	}
	if dictID != 0 {
		attemptCompression, dictProbe, _ := db.valueLogDictShouldAttemptCompression(len(value))
		probeCompression = probeCompression || dictProbe
		if !attemptCompression {
			dictID = 0
			dict = nil
			writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
		}
	}
	if dictID != 0 && db.shouldBypassValueLogDictForValue(value, probeCompression) {
		dictID = 0
		dict = nil
		writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
	}
	if dictID != 0 && db.valueLogAutotuneOptions.DisableBelowValueBytes > 0 && len(value) < db.valueLogAutotuneOptions.DisableBelowValueBytes {
		dictID = 0
		dict = nil
		writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
	}
	if dictID != 0 && len(dict) == 0 {
		if b, dictErr := db.dictBytes(context.Background(), dictID); dictErr == nil {
			dict = b
		} else {
			dictID = 0
			dict = nil
			writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
		}
	}
	finalWriteMode := vlogWriteOff
	switch {
	case dictID != 0:
		finalWriteMode = vlogWriteDict
	case writeMode == vlogWriteBlock:
		finalWriteMode = vlogWriteBlock
	default:
		finalWriteMode = vlogWriteOff
	}
	finalBlockCodec := blockCodec
	if finalWriteMode != vlogWriteBlock {
		finalBlockCodec = db.valueLogBlockCodec
	}
	switch mode {
	case vlogCompressionDefault, vlogCompressionDict:
		db.valueLogDictCollectSample(value)
	case vlogCompressionAuto:
		allowDictSampling := true
		if l != nil && l.vlogCompressionSelector != nil {
			allowDictSampling = l.vlogCompressionSelector.allowDictSampling(writeMode)
		}
		if writeMode != vlogWriteOff && allowDictSampling {
			if db.valueLogDictLastAppliedDictID.Load() != 0 && writeMode != vlogWriteDict {
				if len(value) <= 256 {
					allowDictSampling = false
				} else {
					allowDictSampling = db.valueLogDictShouldCollectPaused()
				}
			}
		}
		if writeMode != vlogWriteOff && allowDictSampling {
			db.valueLogDictCollectSample(value)
		}
	}

	if allowQueue && db.shouldQueueValueLogOne(l, dictID, len(value), durability, finalWriteMode, wallStart) {
		if dictID == 0 && !l.vlogQueueing.Load() && l.vlogMu.TryLock() {
			w := l.vlog
			if w == nil {
				l.vlogMu.Unlock()
				return page.ValuePtr{}, "", errWALUnavailable
			}
			if rotateErr := db.rotateValueLogForMaxSegmentMuHeld(l, w); rotateErr != nil {
				l.vlogMu.Unlock()
				return page.ValuePtr{}, "", rotateErr
			}
			// Reload writer in case rotation replaced l.vlog.
			w = l.vlog
			if w == nil {
				l.vlogMu.Unlock()
				return page.ValuePtr{}, "", errWALUnavailable
			}
			if l.vlogCaps.writer != w {
				l.vlogCaps = computeVlogWriterCaps(w)
			}
			caps := l.vlogCaps
			db.setVlogWriterMode(l, w, finalWriteMode, finalBlockCodec)
			policySetter := caps.keep
			startSize := w.Size()

			if policySetter != nil {
				snap := db.valueLogAutotuneMetrics.snapshot()
				ioNsPerStored := snap.IoNsPerStoredByte
				encodeNsPerRaw := snap.EncodeNsPerRawByte
				if db.valueLogAutotuneOptions.Mode == valuelog.AutotuneOff {
					ioNsPerStored = 0
					encodeNsPerRaw = 0
				}
				policySetter.SetKeepPolicy(ioNsPerStored, encodeNsPerRaw, db.valueLogAutotuneSafetyMargin())
			}

			stats := valuelog.FrameStats{Records: 1, RawPayloadBytes: len(value), StoredPayloadBytes: len(value)}
			durableBoundary := false
			if finalWriteMode == vlogWriteBlock {
				if concrete, ok := w.(*valuelog.Writer); ok {
					ptr, stats, err = concrete.AppendOneFrameWithStats(0, nil, rid, value)
				} else {
					var (
						rec        [1]valuelog.Record
						ptrScratch [1]page.ValuePtr
						ptrs       []page.ValuePtr
					)
					rec[0] = valuelog.Record{RID: rid, Value: value}
					switch {
					case caps.statsInto != nil:
						ptrs, stats, err = caps.statsInto.AppendFrameWithStatsInto(0, nil, rec[:], ptrScratch[:])
					case caps.stats != nil:
						ptrs, stats, err = caps.stats.AppendFrameWithStats(0, nil, rec[:])
					default:
						ptr, err = w.Append(0, nil, rid, value)
					}
					if err == nil && ptr == (page.ValuePtr{}) {
						if len(ptrs) != 1 {
							err = fmt.Errorf("cachingdb: value-log wrote %d ptrs for 1 record", len(ptrs))
						} else {
							ptr = ptrs[0]
						}
					}
				}
			} else {
				ptr, err = w.Append(0, nil, rid, value)
			}
			if err == nil {
				switch durability {
				case journalDurabilityFlush:
					err = w.Flush()
					durableBoundary = err == nil
				default:
					if db.shouldFlushDeferredValueLogValue(finalWriteMode, value) {
						err = w.Flush()
						durableBoundary = err == nil
					}
				}
			}
			if err == nil {
				totalBytes = w.Size() - startSize
			}
			retainPath := ""
			if l.vlogPath != "" && l.vlogPath != l.vlogRetainedPath {
				l.vlogRetainedPath = l.vlogPath
				retainPath = l.vlogPath
			}
			if err == nil {
				if durableBoundary {
					l.vlogDirty.Store(false)
				} else if totalBytes > 0 {
					l.vlogDirty.Store(true)
				}
			}
			if db.testBeforeVlogUnlock != nil {
				db.testBeforeVlogUnlock(int(l.id))
			}
			l.vlogMu.Unlock()
			if err != nil {
				return page.ValuePtr{}, "", err
			}
			db.valueLogDictFrames.total.Add(1)
			if stats.Attempted {
				db.valueLogDictFrames.attempted.Add(1)
			}
			if stats.Kept {
				db.valueLogDictFrames.kept.Add(1)
			}
			if totalBytes > 0 {
				l.vlogLiveBytes.Add(totalBytes)
			}
			storedForSelector := stats.StoredPayloadBytes
			if storedForSelector <= 0 || (finalWriteMode == vlogWriteBlock && !stats.Attempted && storedForSelector == len(value) && totalBytes > 0) {
				if totalBytes > 0 {
					storedForSelector = int(totalBytes)
				} else {
					storedForSelector = len(value)
				}
			}
			selectorWallNs := time.Since(selectorStart).Nanoseconds()
			db.observeVlogWriteMode(l, finalWriteMode, finalBlockCodec, len(value), len(value), storedForSelector, probeCompression, selectorWallNs)
			return ptr, retainPath, nil
		}

		l.vlogQueueing.Store(true)
		ack := vlogAckPool.Get().(*vlogAck)
		ack.ptr = page.ValuePtr{}
		ack.retainPath = ""
		ack.err = nil
		ack.wg.Add(1)

		req := vlogWriteRequest{
			rid:              rid,
			value:            value,
			dictID:           dictID,
			writeMode:        finalWriteMode,
			blockCodec:       finalBlockCodec,
			probeCompression: probeCompression,
			durability:       durability,
			enqueuedAt:       time.Now(),
			ack:              ack,
		}
		select {
		case l.vlogCh <- req:
			observeLaneVlogQueueEnqueue(l, len(l.vlogCh))
		case <-db.closeCh:
			ack.err = errWALClosed
			ack.wg.Done()
		}

		ack.wg.Wait()
		ptr := ack.ptr
		retainPath := ack.retainPath
		err := ack.err
		vlogAckPool.Put(ack)
		return ptr, retainPath, err
	}

	l.vlogMu.Lock()
	w := l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		return page.ValuePtr{}, "", errWALUnavailable
	}
	if rotateErr := db.rotateValueLogForMaxSegmentMuHeld(l, w); rotateErr != nil {
		l.vlogMu.Unlock()
		return page.ValuePtr{}, "", rotateErr
	}
	// Reload writer in case rotation replaced l.vlog.
	w = l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		return page.ValuePtr{}, "", errWALUnavailable
	}
	if l.vlogCaps.writer != w {
		l.vlogCaps = computeVlogWriterCaps(w)
	}
	caps := l.vlogCaps
	db.setVlogWriterMode(l, w, finalWriteMode, finalBlockCodec)
	policySetter := caps.keep
	statsWriter := caps.stats
	statsWriterInto := caps.statsInto
	startSize := w.Size()
	durableBoundary := false

	if policySetter != nil {
		snap := db.valueLogAutotuneMetrics.snapshot()
		ioNsPerStored := snap.IoNsPerStoredByte
		encodeNsPerRaw := snap.EncodeNsPerRawByte
		if db.valueLogAutotuneOptions.Mode == valuelog.AutotuneOff {
			ioNsPerStored = 0
			encodeNsPerRaw = 0
		}
		policySetter.SetKeepPolicy(ioNsPerStored, encodeNsPerRaw, db.valueLogAutotuneSafetyMargin())
	}

	stats := valuelog.FrameStats{Records: 1, RawPayloadBytes: len(value), StoredPayloadBytes: len(value)}
	if finalWriteMode == vlogWriteBlock {
		if concrete, ok := w.(*valuelog.Writer); ok {
			ptr, stats, err = concrete.AppendOneFrameWithStats(0, nil, rid, value)
		} else {
			var ptrScratch [1]page.ValuePtr
			var rec [1]valuelog.Record
			rec[0] = valuelog.Record{RID: rid, Value: value}
			var ptrs []page.ValuePtr
			var frameErr error
			if statsWriterInto != nil {
				ptrs, stats, frameErr = statsWriterInto.AppendFrameWithStatsInto(0, nil, rec[:], ptrScratch[:])
			} else if statsWriter != nil {
				ptrs, stats, frameErr = statsWriter.AppendFrameWithStats(0, nil, rec[:])
			} else {
				ptr, err = w.Append(0, nil, rid, value)
			}
			if frameErr != nil {
				err = frameErr
			} else if err == nil && ptr == (page.ValuePtr{}) {
				if len(ptrs) != 1 {
					err = fmt.Errorf("cachingdb: value-log wrote %d ptrs for 1 record", len(ptrs))
				} else {
					ptr = ptrs[0]
				}
			}
		}
	} else if dictID == 0 {
		ptr, err = w.Append(0, nil, rid, value)
	} else if len(dict) == 0 {
		ptr, err = w.Append(0, nil, rid, value)
	} else {
		if concrete, ok := w.(*valuelog.Writer); ok {
			ptr, stats, err = concrete.AppendOneFrameWithStats(dictID, dict, rid, value)
		} else {
			var ptrScratch [1]page.ValuePtr
			var rec [1]valuelog.Record
			rec[0] = valuelog.Record{RID: rid, Value: value}
			var ptrs []page.ValuePtr
			var frameErr error
			if statsWriterInto != nil {
				ptrs, stats, frameErr = statsWriterInto.AppendFrameWithStatsInto(dictID, dict, rec[:], ptrScratch[:])
			} else if statsWriter != nil {
				ptrs, stats, frameErr = statsWriter.AppendFrameWithStats(dictID, dict, rec[:])
			} else {
				ptrs, frameErr = w.AppendFrame(dictID, dict, rec[:])
			}
			if frameErr != nil {
				err = frameErr
			} else if len(ptrs) != 1 {
				err = fmt.Errorf("cachingdb: value-log wrote %d ptrs for 1 record", len(ptrs))
			} else {
				ptr = ptrs[0]
			}
		}
	}
	if err == nil {
		switch durability {
		case journalDurabilityFlush:
			err = w.Flush()
			durableBoundary = err == nil
		case journalDurabilitySync:
			err = w.Sync()
			durableBoundary = err == nil
		default:
			if db.shouldFlushDeferredValueLogValue(finalWriteMode, value) {
				err = w.Flush()
				durableBoundary = err == nil
			}
		}
	}
	if err == nil {
		totalBytes = w.Size() - startSize
	}
	retainPath := ""
	if l.vlogPath != "" && l.vlogPath != l.vlogRetainedPath {
		l.vlogRetainedPath = l.vlogPath
		retainPath = l.vlogPath
	}
	if err == nil {
		if durableBoundary {
			l.vlogDirty.Store(false)
		} else if totalBytes > 0 {
			l.vlogDirty.Store(true)
		}
	}
	if db.testBeforeVlogUnlock != nil {
		db.testBeforeVlogUnlock(int(l.id))
	}
	l.vlogMu.Unlock()
	if err != nil {
		return page.ValuePtr{}, "", err
	}
	db.valueLogDictFrames.total.Add(1)
	if stats.Attempted {
		db.valueLogDictFrames.attempted.Add(1)
	}
	if stats.Kept {
		db.valueLogDictFrames.kept.Add(1)
	}
	if dictID != 0 && len(dict) > 0 {
		db.valueLogDictObservePayload(uint64(stats.RawPayloadBytes), uint64(stats.StoredPayloadBytes), stats.Records)
	}
	if probeCompression && stats.Kept {
		db.valueLogDictPauseRemaining.Store(0)
		if db.valueLogDictProbeBytes > 0 {
			db.valueLogDictProbeRemaining.Store(db.valueLogDictProbeBytes)
		}
		db.valueLogDictIncompressibleHoldRemaining.Store(0)
		db.valueLogDictIncompressibleHitStreak.Store(0)
		if db.valueLogDictIncompressibleProbeBytes > 0 {
			db.valueLogDictIncompressibleProbeRemaining.Store(db.valueLogDictIncompressibleProbeBytes)
		}
	}
	storedForSelector := stats.StoredPayloadBytes
	if storedForSelector <= 0 || (finalWriteMode == vlogWriteBlock && !stats.Attempted && storedForSelector == len(value) && totalBytes > 0) {
		if totalBytes > 0 {
			storedForSelector = int(totalBytes)
		} else {
			storedForSelector = len(value)
		}
	}
	selectorWallNs := time.Since(selectorStart).Nanoseconds()
	db.observeVlogWriteMode(l, finalWriteMode, finalBlockCodec, len(value), len(value), storedForSelector, probeCompression, selectorWallNs)
	if totalBytes > 0 {
		l.vlogLiveBytes.Add(totalBytes)
	}
	encodeNsTotal := int64(0)
	encodeRawBytes := 0
	if stats.EncodeNs > 0 && stats.RawPayloadBytes > 0 {
		encodeNsTotal = stats.EncodeNs
		encodeRawBytes = stats.RawPayloadBytes
	}
	if !wallStart.IsZero() {
		storedForMetrics := stats.StoredPayloadBytes
		if storedForMetrics == 0 && totalBytes > 0 {
			storedForMetrics = int(totalBytes)
		}
		db.valueLogAutotuneMetrics.observe(wallStart, len(value), storedForMetrics, encodeNsTotal, encodeRawBytes)
	}
	return ptr, retainPath, nil
}

func (db *DB) appendWALInline(l *lane, records []logRecord, flush bool) error {
	if l == nil {
		return errWALUnavailable
	}
	if len(records) == 1 {
		return db.appendWALInlineOne(l, records[0], flush)
	}

	var (
		totalBytes int64
		err        error
	)

	l.walMu.Lock()
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	if len(records) == 1 {
		rec := records[0]
		err = w.Append(rec)
		totalBytes = db.logRecordSize(rec.Key, rec.Value)
	} else {
		err = w.AppendBatch(records)
		totalBytes = db.logBatchSize(records)
	}
	if err == nil && flush {
		err = w.Flush()
	}
	l.walMu.Unlock()

	if err != nil {
		db.walAckMu.Lock()
		if db.walErr == nil {
			db.walErr = err
		}
		db.walAckMu.Unlock()
		return err
	}

	if totalBytes > 0 {
		l.walLiveBytes.Add(totalBytes)
	}
	return nil
}

func (db *DB) appendWALInlineOne(l *lane, record logRecord, flush bool) error {
	if l == nil {
		return errWALUnavailable
	}

	l.walMu.Lock()
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	err := w.Append(record)
	totalBytes := db.logRecordSize(record.Key, record.Value)
	if err == nil && flush {
		err = w.Flush()
	}
	l.walMu.Unlock()

	if err != nil {
		db.walAckMu.Lock()
		if db.walErr == nil {
			db.walErr = err
		}
		db.walAckMu.Unlock()
		return err
	}

	if totalBytes > 0 {
		l.walLiveBytes.Add(totalBytes)
	}
	return nil
}

func (db *DB) flushWALLane(l *lane) error {
	if db.disableJournal {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}

	l.walMu.Lock()
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	err := w.Flush()
	l.walMu.Unlock()

	if err != nil {
		db.walAckMu.Lock()
		if db.walErr == nil {
			db.walErr = err
		}
		db.walAckMu.Unlock()
		return err
	}
	return nil
}

func (db *DB) appendWALDirect(l *lane, records []logRecord, sync bool) error {
	if l == nil {
		return errWALUnavailable
	}
	ack := walAckPool.Get().(*walAck)
	ack.err = nil
	ack.wg.Add(1)

	req := walWriteRequest{records: records, sync: sync, ack: ack}
	select {
	case l.walCh <- req:
		// wait for ack
	case <-db.closeCh:
		ack.err = errWALClosed
		ack.wg.Done()
		walAckPool.Put(ack)
		return errWALClosed
	}

	ack.wg.Wait()
	err := ack.err
	walAckPool.Put(ack)
	return err
}

func (db *DB) appendWALFast(l *lane, record logRecord) error {
	ack := walAckPool.Get().(*walAck)
	ack.err = nil
	ack.wg.Add(1)

	if l == nil {
		ack.err = errWALUnavailable
		ack.wg.Done()
		walAckPool.Put(ack)
		return errWALUnavailable
	}

	record.Seq = db.nextCommitSeq.Add(1)

	l.walFastMu.Lock()
	for !l.walFastClosed && len(l.walFastQueue)-l.walFastHead >= walFastQueueMax {
		l.walFastCond.Wait()
	}
	if l.walFastClosed {
		l.walFastMu.Unlock()
		ack.err = errWALClosed
		ack.wg.Done()
		walAckPool.Put(ack)
		return errWALClosed
	}
	l.walFastQueue = append(l.walFastQueue, walFastItem{record: record, ack: ack})
	l.walFastCond.Signal()
	l.walFastMu.Unlock()

	ack.wg.Wait()
	err := ack.err
	walAckPool.Put(ack)
	return err
}

func (db *DB) autoCheckpointLoop(interval time.Duration, maxWALBytes int64, idleInterval time.Duration) {
	defer db.wg.Done()

	var intervalTicker *time.Ticker
	intervalCh := (<-chan time.Time)(nil)
	if interval > 0 {
		intervalTicker = time.NewTicker(interval)
		intervalCh = intervalTicker.C
		defer intervalTicker.Stop()
	}

	var idleTimer *time.Timer
	idleCh := (<-chan time.Time)(nil)
	if idleInterval > 0 {
		idleTimer = time.NewTimer(idleInterval)
		if !idleTimer.Stop() {
			<-idleTimer.C
		}
		idleCh = idleTimer.C
	}

	for {
		select {
		case <-db.closeCh:
			return
		case <-intervalCh:
			db.maybeAutoCheckpoint(maxWALBytes, autoCheckpointModeInterval)
		case <-db.autoCheckpointOnceCh:
			db.maybeAutoCheckpoint(maxWALBytes, autoCheckpointModeForce)
		case <-db.autoCheckpointWriteCh:
			if maxWALBytes > 0 {
				db.maybeAutoCheckpoint(maxWALBytes, autoCheckpointModeSize)
			}
			if idleTimer != nil {
				resetTimer(idleTimer, idleInterval)
			}
		case <-idleCh:
			db.maybeAutoCheckpoint(maxWALBytes, autoCheckpointModeIdle)
		}
	}
}

func (db *DB) maybeAutoCheckpoint(maxWALBytes int64, mode autoCheckpointMode) {
	effectiveBytes := db.effectiveWALBytes()
	if effectiveBytes <= 0 {
		return
	}
	reclaimableBytes := db.reclaimableWALBytes()

	// Avoid thrashing the checkpoint path when workloads are mostly idle but
	// produce tiny write bursts.
	if mode == autoCheckpointModeIdle {
		if effectiveBytes < db.minIdleCheckpointWALBytes() {
			return
		}
		last := db.autoCheckpointLastUnixNano.Load()
		if last > 0 && time.Since(time.Unix(0, last)) < autoCheckpointMinIdleInterval {
			return
		}
	}

	switch mode {
	case autoCheckpointModeInterval, autoCheckpointModeIdle, autoCheckpointModeForce:
		// proceed
	case autoCheckpointModeSize:
		if maxWALBytes <= 0 || reclaimableBytes < maxWALBytes {
			return
		}
		// Avoid repeatedly checkpointing when WAL bytes cannot be reduced (e.g.
		// value-log segments retained for pointers). Rearm once reclaimable bytes
		// drop below maxWALBytes/2.
		if !db.autoCheckpointSizeArmed.CompareAndSwap(true, false) {
			return
		}
	default:
		// Unknown mode: be conservative and do nothing.
		return
	}

	before := effectiveBytes
	beforeReclaimable := reclaimableBytes
	start := time.Now()
	err := db.Checkpoint()
	dur := time.Since(start)
	after := db.effectiveWALBytes()
	afterReclaimable := db.reclaimableWALBytes()
	trimmed := before - after
	if trimmed < 0 {
		trimmed = 0
	}
	if maxWALBytes > 0 && afterReclaimable < maxWALBytes/2 {
		db.autoCheckpointSizeArmed.CompareAndSwap(false, true)
	}

	// Best-effort: failures here should be surfaced via normal write paths or
	// explicit maintenance calls. Avoid printing from background maintenance.
	if err != nil {
		if mode == autoCheckpointModeSize && maxWALBytes > 0 {
			db.autoCheckpointSizeArmed.CompareAndSwap(false, true)
		}
		return
	}

	db.autoCheckpointCount.Add(1)
	db.autoCheckpointLastReason.Store(uint32(mode))
	db.autoCheckpointLastUnixNano.Store(time.Now().UnixNano())
	db.autoCheckpointLastDurNanos.Store(dur.Nanoseconds())
	db.autoCheckpointLastWALBefore.Store(before)
	db.autoCheckpointLastWALAfter.Store(after)
	db.autoCheckpointLastWALReclaimableBefore.Store(beforeReclaimable)
	db.autoCheckpointLastWALReclaimableAfter.Store(afterReclaimable)
	db.autoCheckpointLastWALTrimmed.Store(trimmed)
}

func (db *DB) startVlogGenerationLoop() {
	if db == nil {
		return
	}
	if db.valueLogGenerationPolicy == 0 {
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerDisabled)
		return
	}
	if _, ok := db.backend.(backendValueLogRewriter); !ok {
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerDisabled)
		return
	}
	// GC integration is optional in this phase; rewrite is required.
	db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
	db.wg.Add(1)
	go db.vlogGenerationLoop()
}

func (db *DB) vlogGenerationLoop() {
	defer db.wg.Done()
	ticker := time.NewTicker(vlogGenerationLoopInterval)
	defer ticker.Stop()
	ticks := 0
	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			ticks++
			db.maybeRunVlogGenerationMaintenance(ticks%vlogGenerationGCEvery == 0)
		}
	}
}

func (db *DB) maybeRunVlogGenerationMaintenance(runGC bool) {
	if db == nil || db.valueLogGenerationPolicy == 0 {
		return
	}
	if db.checkpointing.Load() {
		return
	}
	db.mu.RLock()
	queueLen := len(db.queue)
	db.mu.RUnlock()
	if queueLen != 0 {
		return
	}

	retained := db.valueLogRetainedStatsDetailed()
	totalBytes := retained.BytesTotal
	if totalBytes < 0 {
		totalBytes = 0
	}
	reclaimable := db.reclaimableWALBytes()
	if reclaimable < 0 {
		reclaimable = 0
	}
	staleRatioPPM := uint32(0)
	if totalBytes > 0 {
		staleRatioPPM = uint32((reclaimable * 1_000_000) / totalBytes)
	}
	churnBps := db.vlogGenerationEstimateChurnBps()

	shouldRewrite, reason := db.shouldRunVlogGenerationRewrite(totalBytes, staleRatioPPM, churnBps)
	rewriter, hasRewriter := db.backend.(backendValueLogRewriter)
	if shouldRewrite && hasRewriter {
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerRunning)
		db.vlogGenerationLastReason.Store(reason)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		rewriteOpts := backenddb.ValueLogRewriteOnlineOptions{
			BatchSize:         db.valueLogRewriteBatchSize(),
			SyncEachBatch:     false,
			MaxSegmentBytes:   db.valueLogGenerationWarmTarget,
			MaxSourceSegments: 1,
			MaxSourceBytes:    db.valueLogRewriteBudgetBytes,
			MinSegmentStaleRatio: func() float64 {
				if db.valueLogRewriteTriggerRatioPPM <= 0 {
					return 0
				}
				return float64(db.valueLogRewriteTriggerRatioPPM) / 1_000_000.0
			}(),
		}
		stats, err := rewriter.ValueLogRewriteOnline(ctx, rewriteOpts)
		cancel()
		if err != nil {
			db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
			db.vlogGenerationRemapFailures.Add(1)
			if db.notifyError != nil {
				db.notifyError(fmt.Errorf("cachingdb: generational rewrite: %w", err))
			}
		} else {
			db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
			db.vlogGenerationRewriteRuns.Add(1)
			if stats.BytesBefore > 0 {
				db.vlogGenerationRewriteBytesIn.Add(uint64(stats.BytesBefore))
			}
			if stats.BytesAfter > 0 {
				db.vlogGenerationRewriteBytesOut.Add(uint64(stats.BytesAfter))
			}
			if stats.RecordsCopied > 0 {
				db.vlogGenerationRemapSuccesses.Add(uint64(stats.RecordsCopied))
			}
			db.maybeRunVlogGenerationIndexVacuum(int64(stats.BytesBefore))
		}
	}

	if !runGC && !db.shouldRunVlogGenerationGC(retained, reclaimable, churnBps) {
		return
	}
	gcer, ok := db.backend.(backendValueLogGCer)
	if !ok {
		return
	}
	db.vlogGenerationLastReason.Store(vlogGenerationReasonPeriodicGC)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	gcStats, err := gcer.ValueLogGC(ctx, backenddb.ValueLogGCOptions{})
	cancel()
	if err != nil {
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
		if db.notifyError != nil {
			db.notifyError(fmt.Errorf("cachingdb: generational gc: %w", err))
		}
		return
	}
	db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
	db.vlogGenerationGCRuns.Add(1)
	if gcStats.SegmentsDeleted > 0 {
		db.vlogGenerationGCSegmentsDeleted.Add(uint64(gcStats.SegmentsDeleted))
	}
	if gcStats.BytesDeleted > 0 {
		db.vlogGenerationGCBytesDeleted.Add(uint64(gcStats.BytesDeleted))
	}
}

func (db *DB) maybeRunVlogGenerationIndexVacuum(rewriteBytesIn int64) {
	if db == nil || db.valueLogGenerationPolicy == 0 {
		return
	}
	vacuumer, ok := db.backend.(backendIndexVacuumer)
	if !ok {
		return
	}
	now := time.Now()
	if !db.shouldRunVlogGenerationIndexVacuum(rewriteBytesIn, now) {
		return
	}
	db.vlogGenerationLastReason.Store(vlogGenerationReasonPostRewriteVacuum)
	db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerRunning)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err := vacuumer.VacuumIndexOnline(ctx)
	cancel()
	if err != nil {
		db.vlogGenerationVacuumFailures.Add(1)
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
		if db.notifyError != nil {
			db.notifyError(fmt.Errorf("cachingdb: generational index vacuum: %w", err))
		}
		return
	}
	db.vlogGenerationVacuumRuns.Add(1)
	db.vlogGenerationLastVacuumUnixNano.Store(now.UnixNano())
	db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
}

func (db *DB) shouldRunVlogGenerationIndexVacuum(rewriteBytesIn int64, now time.Time) bool {
	if db == nil || db.valueLogGenerationPolicy == 0 {
		return false
	}
	if rewriteBytesIn < vlogGenerationVacuumTriggerRewriteBytes {
		return false
	}
	last := db.vlogGenerationLastVacuumUnixNano.Load()
	if last > 0 {
		lastAt := time.Unix(0, last)
		if now.Sub(lastAt) < vlogGenerationVacuumMinInterval {
			return false
		}
	}
	return true
}

func (db *DB) shouldRunVlogGenerationGC(retained valueLogRetainedGenerationStats, reclaimable int64, churnBps int64) bool {
	if db == nil || db.valueLogGenerationPolicy == 0 {
		return false
	}
	if reclaimable >= vlogGenerationGCMinBytes {
		return true
	}
	if retained.SegmentsHot >= 2 && retained.SegmentsTotal >= 3 {
		return true
	}
	if db.valueLogRewriteTriggerChurn > 0 && churnBps >= db.valueLogRewriteTriggerChurn/2 {
		return true
	}
	return false
}

func (db *DB) vlogGenerationEstimateChurnBps() int64 {
	if db == nil {
		return 0
	}
	now := time.Now().UnixNano()
	cur := db.vlogGenerationChurnBytes.Load()
	prevBytes := db.vlogGenerationLastChurnSampleBytes.Swap(cur)
	prevNs := db.vlogGenerationLastChurnSampleNS.Swap(now)
	if prevNs <= 0 || now <= prevNs || cur < prevBytes {
		db.vlogGenerationLastChurnBps.Store(0)
		return 0
	}
	deltaBytes := cur - prevBytes
	deltaNs := now - prevNs
	if deltaNs <= 0 {
		db.vlogGenerationLastChurnBps.Store(0)
		return 0
	}
	churn := int64((float64(deltaBytes) * float64(time.Second)) / float64(deltaNs))
	if churn < 0 {
		churn = 0
	}
	db.vlogGenerationLastChurnBps.Store(churn)
	return churn
}

func (db *DB) shouldRunVlogGenerationRewrite(totalBytes int64, staleRatioPPM uint32, churnBps int64) (bool, uint32) {
	if db == nil {
		return false, vlogGenerationReasonNone
	}
	if db.valueLogRewriteTriggerBytes > 0 && totalBytes >= db.valueLogRewriteTriggerBytes {
		return true, vlogGenerationReasonTotalBytes
	}
	if db.valueLogRewriteTriggerRatioPPM > 0 && staleRatioPPM >= db.valueLogRewriteTriggerRatioPPM {
		return true, vlogGenerationReasonStaleRatio
	}
	if db.valueLogRewriteTriggerChurn > 0 && churnBps >= db.valueLogRewriteTriggerChurn {
		return true, vlogGenerationReasonChurn
	}
	return false, vlogGenerationReasonNone
}

func (db *DB) valueLogRewriteBatchSize() int {
	if db == nil {
		return 256
	}
	if db.valueLogRewriteBudgetRecords > 0 {
		if db.valueLogRewriteBudgetRecords < 1 {
			return 1
		}
		return db.valueLogRewriteBudgetRecords
	}
	return 256
}

func (db *DB) ensureBackendRange() error {
	if db == nil {
		return nil
	}
	db.backendRangeInit.Do(func() {
		r, known, err := db.computeBackendRange()
		db.mu.Lock()
		defer db.mu.Unlock()
		if err != nil {
			db.backendRangeErr = err
			db.backendRangeKnown = false
			return
		}
		if r.valid {
			db.backendRange.add(r.min)
			db.backendRange.add(r.max)
		}
		db.backendRangeKnown = known
	})

	db.mu.RLock()
	err := db.backendRangeErr
	db.mu.RUnlock()
	return err
}

func (db *DB) computeBackendRange() (keyRange, bool, error) {
	minIter, err := db.backend.Iterator(nil, nil)
	if err != nil {
		return keyRange{}, false, err
	}
	defer minIter.Close()
	minIter.Seek(nil)

	r := keyRange{}
	if minIter.Valid() && !minIter.IsDeleted() {
		r.add(minIter.UnsafeKey())
	}

	maxIter, err := db.backend.ReverseIterator(nil, nil)
	if err != nil {
		// Backend doesn't support reverse iteration; disable backend-range-dependent optimizations.
		return r, false, nil
	}
	defer maxIter.Close()

	if maxIter.Valid() && !maxIter.IsDeleted() {
		r.add(maxIter.UnsafeKey())
	}

	return r, true, nil
}

const stopResumeFraction = 0.70
const stopBackpressureStallLimit = 16

const (
	// Start pushing down v2 deferred fence debt earlier than the generic
	// slowdown threshold so checkpoint work is less bursty.
	// Dividing by 2 starts assists at 50% of the configured slowdown/stop
	// thresholds when possible.
	v2DeferredAssistEarlyTriggerDiv = 2
	// Run the assist path once every 32 write assists to limit overhead.
	v2DeferredAssistTickMask             = 31 // once every 32 write assists
	v2DeferredAssistSlowdownMinMemtables = 2
	v2DeferredAssistStopMinMemtables     = 4
)

func (db *DB) adaptiveBackpressureEnabled() bool {
	return db.slowdownBacklogSeconds > 0 || db.stopBacklogSeconds > 0 || db.maxBacklogBytes > 0
}

func (db *DB) thresholdsLocked() (slowdownBytes, stopBytes, resumeBytes int64) {
	flushBps := db.flushBpsEWMA
	if flushBps <= 0 && db.flushThreshold > 0 {
		// Fallback until we have real measurements: assume ~1 memtable/sec.
		flushBps = float64(db.flushThreshold)
	}
	return computeBackpressureThresholds(backpressureParams{
		flushBps:               flushBps,
		flushThreshold:         db.flushThreshold,
		slowdownBacklogSeconds: db.slowdownBacklogSeconds,
		stopBacklogSeconds:     db.stopBacklogSeconds,
		maxBacklogBytes:        db.maxBacklogBytes,
		stopResumeFraction:     stopResumeFraction,
	})
}

func (db *DB) waitForCheckpoint() {
	if !db.checkpointing.Load() {
		return
	}
	db.checkpointMu.Lock()
	for db.checkpointing.Load() {
		db.checkpointCond.Wait()
	}
	db.checkpointMu.Unlock()
}

func (db *DB) recordCheckpointCutover(d time.Duration) {
	if db == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	ns := d.Nanoseconds()
	db.checkpointCutoverLastNanos.Store(ns)
	db.checkpointCutoverLastUnixNano.Store(time.Now().UnixNano())
	db.checkpointCutoverTotalNanos.Add(ns)
	db.checkpointCutoverSamples.Add(1)
	for {
		cur := db.checkpointCutoverMaxNanos.Load()
		if ns <= cur || db.checkpointCutoverMaxNanos.CompareAndSwap(cur, ns) {
			break
		}
	}
}

// checkpointRotateCapacity returns the memtable capacity used when checkpoint
// rotates mutable shards. We intentionally cap checkpoint-time preallocation to
// keep write-locked cutover latency bounded; normal growth resumes as writers
// repopulate the fresh mutable shard.
func (db *DB) checkpointRotateCapacity() int {
	if db == nil {
		return -1
	}
	capacity := db.memtableCap
	if capacity <= 0 {
		return -1
	}
	const checkpointRotateCapMax = 256 * 1024
	if capacity > checkpointRotateCapMax {
		return checkpointRotateCapMax
	}
	return capacity
}

func (db *DB) observePublishWatermarkLagDrift(backlogBytes int64, now time.Time) float64 {
	if db == nil {
		return 0
	}
	nowNS := now.UnixNano()
	db.publishWatermarkLagMu.Lock()
	defer db.publishWatermarkLagMu.Unlock()
	prevNS := db.publishWatermarkLastUnixNano
	prevBacklog := db.publishWatermarkLastBacklogBytes
	db.publishWatermarkLastUnixNano = nowNS
	db.publishWatermarkLastBacklogBytes = backlogBytes
	if prevNS <= 0 || nowNS <= prevNS {
		return 0
	}
	dt := float64(nowNS-prevNS) / float64(time.Second)
	if dt <= 0 {
		return 0
	}
	return float64(backlogBytes-prevBacklog) / dt
}

// Checkpoint forces a durable backend boundary and trims the WAL so long-running
// cached-mode runs do not accumulate unbounded `wal/` growth.
//
// It blocks writers while it:
//   - rotates the current mutable memtable (if non-empty),
//   - rotates to a fresh WAL segment,
//   - flushes all queued memtables with backend sync,
//   - forces a backend sync boundary (even if the queue is empty),
//   - removes all older WAL segments (keeping only the currently-open one).
func (db *DB) Checkpoint() error {
	// Note: Any code path that takes both flushMu and checkpointMu must acquire
	// flushMu first to avoid deadlocks.
	db.flushMu.Lock()
	defer db.flushMu.Unlock() // Ensure it's released

	db.checkpointMu.Lock()
	for db.checkpointing.Load() {
		db.checkpointCond.Wait()
	}
	db.checkpointing.Store(true) // Set flag only after acquiring flushMu
	db.checkpointMu.Unlock()

	defer func() { // This defer runs when db.Checkpoint() returns
		db.checkpointMu.Lock()
		db.checkpointing.Store(false)
		db.checkpointCond.Broadcast()
		db.checkpointMu.Unlock()
	}()

	db.writeMu.Lock()
	cutoverStart := time.Now()
	releaseWriteMu := func() {
		db.writeMu.Unlock()
		db.recordCheckpointCutover(time.Since(cutoverStart))
	}

	// Rotate mutable into the flush queue and ensure future writes land in a fresh
	// WAL segment (so all older segments can be trimmed after the sync boundary).
	db.mu.Lock()
	if db.mutableBytes.Load() > 0 {
		if err := db.rotateMutableShardsLocked(db.checkpointRotateCapacity(), false); err != nil {
			db.mu.Unlock()
			releaseWriteMu()
			return err
		}
	}
	walDir := db.dir
	preRotateWALPaths := db.currentWALPaths()
	ridBeforeWALRotate := db.nextRID.Load()
	db.mu.Unlock()
	rotateLaneIDs := make([]int, 0, len(db.lanes))
	for i := range db.lanes {
		if db.lanes[i].walLiveBytes.Load() > 0 {
			rotateLaneIDs = append(rotateLaneIDs, i)
		}
	}
	if len(rotateLaneIDs) == 0 {
		for i := range db.lanes {
			rotateLaneIDs = append(rotateLaneIDs, i)
		}
	}
	releaseWriteMu()

	errCh := make(chan error, len(rotateLaneIDs))
	var rotateWG sync.WaitGroup
	for _, laneID := range rotateLaneIDs {
		rotateWG.Add(1)
		go func(id int) {
			defer rotateWG.Done()
			if err := db.rotateWALCheckpointLocked(&db.lanes[id]); err != nil {
				errCh <- err
			}
		}(laneID)
	}
	rotateWG.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	wroteDuringWALRotate := db.nextRID.Load() != ridBeforeWALRotate
	if db.splitValueLogEnabled() {
		for i := range db.lanes {
			if err := db.rotateValueLogLocked(&db.lanes[i]); err != nil {
				return err
			}
		}
	}

	// Flush all queued memtables with backend sync.
	db.flushAllLocked(true)

	segments, nonEmptyBytes := listNonEmptyLogSegments(walDir)
	if len(segments) > 0 {
		filtered := segments[:0]
		nonEmptyBytes = 0
		for _, seg := range segments {
			if seg.valueLog != db.walUsesValueLog() {
				continue
			}
			filtered = append(filtered, seg)
			if seg.size > 0 {
				nonEmptyBytes += seg.size
			}
		}
		segments = filtered
	}
	// New logic: perform sync write only if not relaxedSync
	var commitErr error
	if nonEmptyBytes > 0 {
		backendBatch := db.backend.NewBatch()
		if db.relaxedSync {
			// If relaxed sync, just write the batch without forcing sync
			commitErr = backendBatch.Write()
		} else {
			// Otherwise, force sync
			commitErr = backendBatch.WriteSync()
		}
		cerr := backendBatch.Close()
		if commitErr == nil {
			commitErr = cerr
		}
		if commitErr != nil {
			return commitErr
		}
	}

	currentWALs := make(map[string]struct{})
	for _, path := range db.currentWALPaths() {
		currentWALs[path] = struct{}{}
	}
	unsafeWALDeletes := make(map[string]struct{})
	if wroteDuringWALRotate {
		for _, path := range preRotateWALPaths {
			if path == "" {
				continue
			}
			unsafeWALDeletes[path] = struct{}{}
		}
	}

	removed := false
	for _, seg := range segments {
		path := seg.path
		if _, ok := currentWALs[path]; ok {
			continue
		}
		if _, ok := unsafeWALDeletes[path]; ok {
			continue
		}
		if db.valueLogRetained(path) {
			continue
		}
		db.dropValueLogSegment(path)
		if err := db.removeFileRetry(path); err != nil {
			// Best effort cleanup; ignore errors to prevent flakiness on Windows
			continue
		}
		removed = true
		db.mu.Lock()
		db.untrackWALSegmentLocked(path)
		db.mu.Unlock()
		db.forgetValueLogRetain(path)
	}
	if removed {
		db.syncDirBestEffort(db.dir)
	}
	db.checkValueLogRetention()
	db.pruneRetainedValueLogs()

	if err := db.maybeValidateV1LeafLogInvariants(); err != nil {
		return err
	}

	return nil
}

func (db *DB) waitForStop() {
	if !db.adaptiveBackpressureEnabled() {
		return
	}

	for {
		db.bpMu.Lock()
		_, stopBytes, resumeBytes := db.thresholdsLocked()
		if stopBytes <= 0 {
			db.bpMu.Unlock()
			return
		}
		// Self-heal: backlog bytes should never remain positive when the queue is empty.
		// If this happens, stop backpressure would block forever.
		// Use the lock-free memtable view to avoid deadlock with db.mu.
		queueLen := 0
		if view := db.memtables.Load(); view != nil {
			queueLen = len(view.queue)
		}
		if queueLen == 0 {
			db.queueBacklogBytes.Store(0)
			db.bpMu.Unlock()
			return
		}

		backlog := db.queueBacklogBytes.Load()
		if backlog < stopBytes {
			db.bpMu.Unlock()
			return
		}
		db.bpMu.Unlock()

		// Ensure a background flush pass is scheduled in case backlog was created
		// without a flush trigger (e.g. iterator-driven rotations).
		db.TriggerFlush()

		// Stop backpressure means we are already blocking the caller. Actively
		// flush a bounded amount of work, then return. We avoid looping/sleeping
		// here to prevent per-write stalls when flush progress is slow.
		target := stopBytes
		if resumeBytes > 0 {
			target = resumeBytes
		}
		stalls := 0
		for db.queueBacklogBytes.Load() >= target {
			maxMemtables := db.writerFlushMaxMemtables
			if maxMemtables <= 0 {
				maxMemtables = 1
			}
			// Under stop-backpressure, flush more aggressively to avoid repeated stalls.
			if maxMemtables < 8 {
				maxMemtables = 8
			}
			if maxMemtables > flushCombineMaxMemtables {
				maxMemtables = flushCombineMaxMemtables
			}

			before := db.queueBacklogBytes.Load()
			db.flushSomeBlocking(false, maxMemtables, db.writerFlushMaxDuration)
			after := db.queueBacklogBytes.Load()
			if after < target {
				break
			}
			if after >= before {
				stalls++
				if stalls >= stopBackpressureStallLimit {
					break
				}
			} else {
				stalls = 0
			}
		}
		return
	}
}

func (db *DB) shouldWaitForStop() bool {
	if !db.adaptiveBackpressureEnabled() {
		return false
	}
	backlog := db.queueBacklogBytes.Load()
	if backlog <= 0 {
		return false
	}
	// Self-heal stale backlog accounting when the queue is already empty.
	if view := db.memtables.Load(); view == nil || len(view.queue) == 0 {
		db.queueBacklogBytes.Store(0)
		return false
	}
	db.bpMu.Lock()
	_, stopBytes, _ := db.thresholdsLocked()
	db.bpMu.Unlock()
	if stopBytes <= 0 {
		return false
	}
	return backlog >= stopBytes
}

func (db *DB) maybeWaitForStop() {
	if db.shouldWaitForStop() {
		db.waitForStop()
	}
}

func (db *DB) deferredAssistTargetMemtables(backlogBytes, slowdownBytes, stopBytes int64) (int, bool) {
	if db == nil || !db.deferredValueLogEnabled() {
		return 0, false
	}
	maxMemtables := db.writerFlushMaxMemtables
	if maxMemtables <= 0 {
		maxMemtables = 1
	}
	if stopBytes > 0 && backlogBytes >= stopBytes {
		if maxMemtables < v2DeferredAssistStopMinMemtables {
			maxMemtables = v2DeferredAssistStopMinMemtables
		}
		return maxMemtables, true
	}
	if slowdownBytes > 0 && backlogBytes >= slowdownBytes {
		if maxMemtables < v2DeferredAssistSlowdownMinMemtables {
			maxMemtables = v2DeferredAssistSlowdownMinMemtables
		}
		return maxMemtables, true
	}
	return 0, false
}

func (db *DB) deferredAssistEarlyTriggerBytes(slowdownBytes, stopBytes int64) int64 {
	if db == nil || !db.deferredValueLogEnabled() {
		return 0
	}
	early := int64(0)
	if slowdownBytes > 0 {
		early = slowdownBytes / v2DeferredAssistEarlyTriggerDiv
	} else if stopBytes > 0 {
		early = stopBytes / v2DeferredAssistEarlyTriggerDiv
	}
	if db.flushThreshold > 0 {
		minEarly := db.flushThreshold / v2DeferredAssistEarlyTriggerDiv
		if minEarly <= 0 {
			minEarly = db.flushThreshold
		}
		if early == 0 || early < minEarly {
			early = minEarly
		}
	}
	return early
}

func (db *DB) recordDeferredAssist(flushed int) {
	if db == nil {
		return
	}
	db.deferredFenceAssistCalls.Add(1)
	if flushed > 0 {
		db.deferredFenceAssistFlushedMemtables.Add(uint64(flushed))
	}
}

func (db *DB) maybeAssistFlush() {
	// WAL-off strict route mode is highly sensitive to writer-assist churn:
	// repeated bounded assist attempts can trigger extreme deferred rewrite
	// amplification without making forward queue progress. Let normal
	// background/checkpoint flush boundaries drain the queue instead.
	if db != nil &&
		db.disableJournal &&
		strings.TrimSpace(db.indexOuterLeafMode) == backenddb.IndexOuterLeafModeV1LeafLogRoute {
		return
	}

	if db.writerFlushMaxMemtables <= 0 && db.writerFlushMaxDuration <= 0 {
		return
	}

	// Adaptive policy: thresholds based on queued backlog bytes.
	if db.adaptiveBackpressureEnabled() {
		backlog := db.queueBacklogBytes.Load()
		// Self-heal stale backlog accounting when the queue is empty.
		if backlog > 0 {
			if view := db.memtables.Load(); view == nil || len(view.queue) == 0 {
				db.queueBacklogBytes.Store(0)
				return
			}
		}

		db.bpMu.Lock()
		slowdownBytes, stopBytes, _ := db.thresholdsLocked()
		db.bpMu.Unlock()

		backlog = db.queueBacklogBytes.Load()
		if maxMemtables, ok := db.deferredAssistTargetMemtables(backlog, slowdownBytes, stopBytes); ok {
			flushed := db.flushSome(false, maxMemtables, db.writerFlushMaxDuration)
			db.recordDeferredAssist(flushed)
			return
		}
		if stopBytes > 0 && backlog >= stopBytes {
			_ = db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)
			return
		}
		if slowdownBytes > 0 && backlog > slowdownBytes {
			db.TriggerFlush()
			return
		}
		if early := db.deferredAssistEarlyTriggerBytes(slowdownBytes, stopBytes); early > 0 && backlog >= early {
			db.deferredFenceAssistEarlyTriggers.Add(1)
			db.TriggerFlush()
			tick := db.deferredFenceAssistTicker.Add(1)
			if tick&v2DeferredAssistTickMask == 0 {
				flushed := db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)
				db.recordDeferredAssist(flushed)
			}
		}
		return
	}

	// Legacy policy: thresholds based on queue length.
	if db.maxQueuedMemtables >= 0 {
		queueLen := 0
		if view := db.memtables.Load(); view != nil {
			queueLen = len(view.queue)
		}
		if queueLen > db.maxQueuedMemtables {
			db.TriggerFlush()
		}
	}
}

func (db *DB) flushSome(sync bool, maxMemtables int, maxDuration time.Duration) int {
	if maxMemtables <= 0 && maxDuration <= 0 {
		return 0
	}
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	sync = db.flushSyncRequested(sync)
	start := time.Now()

	flushed := 0
	for {
		if maxMemtables > 0 && flushed >= maxMemtables {
			return flushed
		}
		if maxDuration > 0 && time.Since(start) >= maxDuration {
			return flushed
		}
		laneID, ok := db.pickFlushLane()
		if !ok {
			return flushed
		}
		if laneID < len(db.flushLaneMu) {
			if !db.flushLaneMu[laneID].TryLock() {
				return flushed
			}
		}
		okFlush := db.flushLaneOnce(sync, laneID)
		if laneID < len(db.flushLaneMu) {
			db.flushLaneMu[laneID].Unlock()
		}
		if !okFlush {
			return flushed
		}
		flushed++
	}
}

// flushSomeBlocking is like flushSome, but it blocks on lane locks. This is used
// by stop-backpressure to guarantee forward progress instead of spinning.
func (db *DB) flushSomeBlocking(sync bool, maxMemtables int, maxDuration time.Duration) {
	if maxMemtables <= 0 && maxDuration <= 0 {
		return
	}
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	sync = db.flushSyncRequested(sync)
	start := time.Now()

	flushed := 0
	for {
		if maxMemtables > 0 && flushed >= maxMemtables {
			return
		}
		if maxDuration > 0 && time.Since(start) >= maxDuration {
			return
		}
		laneID, ok := db.pickFlushLane()
		if !ok {
			return
		}
		okFlush := db.flushLaneOnce(sync, laneID)
		if !okFlush {
			return
		}
		flushed++
	}
}

func (db *DB) Close() error {
	var errs []error
	hadMemtables := false
	db.closing.Store(true)
	db.stopDomainIngressWorkers()

	// Lock order must match Checkpoint (flushMu -> writeMu) to avoid a deadlock
	// with the auto-checkpoint goroutine:
	// - Checkpoint takes flushMu, then writeMu.
	// - Close historically took writeMu, then flushMu via flushAll().
	// If auto-checkpoint is in progress (holding flushMu, waiting for writeMu)
	// and Close starts (holding writeMu, waiting for flushMu), the process can
	// deadlock and tests will time out.
	db.flushMu.Lock()
	db.writeMu.Lock()
	db.mu.Lock()
	if db.mutableBytes.Load() > 0 {
		hadMemtables = true
		_ = db.rotateMemtableLocked(true)
	} else if len(db.queue) > 0 {
		hadMemtables = true
	}
	db.mu.Unlock()

	for i := range db.lanes {
		l := &db.lanes[i]
		l.walFastMu.Lock()
		l.walFastClosed = true
		if l.walFastCond != nil {
			l.walFastCond.Broadcast()
		}
		l.walFastMu.Unlock()
	}

	// Flush while closeCh is still open so commit/append paths remain available.
	// This avoids dropping pending memtables on close.
	if hadMemtables {
		// flushMu is already held by Close.
		db.flushAllLocked(true)
	}

	close(db.closeCh)
	db.writeMu.Unlock()
	db.flushMu.Unlock()
	db.wg.Wait()
	db.valueLogDictTrainerMu.Lock()
	trainer := db.valueLogDictTrainer
	db.valueLogDictTrainer = nil
	db.valueLogDictTrainerMu.Unlock()
	if trainer != nil {
		trainer.Close()
	}
	if db.valueLogTemplateEngine != nil {
		db.valueLogTemplateEngine.Close()
		db.valueLogTemplateEngine = nil
	}
	if db.hashSortedIndexer != nil {
		db.hashSortedIndexer.Close()
		db.hashSortedIndexer = nil
	}
	if db.valueLogReader != nil {
		if err := db.valueLogReader.Close(); err != nil {
			errs = append(errs, err)
		}
		db.valueLogReader = nil
	}

	var walBytes int64
	var walPaths []string
	walPaths = make([]string, 0, len(db.lanes))
	for i := range db.lanes {
		l := &db.lanes[i]
		l.walMu.Lock()
		walBytes += l.walClosedBytes.Load()
		for path := range l.walClosedSizes {
			walPaths = append(walPaths, path)
		}
		if l.wal != nil {
			walBytes += l.wal.Size()
			l.walLiveBytes.Store(0)
			if l.walPath != "" {
				walPaths = append(walPaths, l.walPath)
			}
			_ = l.wal.Close()
			l.wal = nil
		} else if l.walPath != "" {
			walPaths = append(walPaths, l.walPath)
		}
		l.walMu.Unlock()

		l.vlogMu.Lock()
		if l.vlog != nil {
			_ = l.vlog.Close()
			l.vlog = nil
			l.vlogCaps = vlogWriterCaps{}
			l.vlogLiveBytes.Store(0)
		}
		l.vlogModeSet = false
		l.vlogModeWriter = nil
		l.vlogMu.Unlock()
	}

	if walBytes > 0 && !hadMemtables {
		backendBatch := db.backend.NewBatch()
		db.flushMu.Lock()
		err := backendBatch.WriteSync()
		db.flushMu.Unlock()
		if cerr := backendBatch.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			errs = append(errs, err)
		}
	}

	seen := make(map[string]struct{}, len(walPaths))
	removed := false
	for _, path := range walPaths {
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		retain := db.valueLogRetained(path)
		if retain {
			continue
		}
		db.dropValueLogSegment(path)
		if err := db.removeFileRetry(path); err != nil {
			// Best effort cleanup; ignore errors to prevent flakiness on Windows
			continue
		}
		removed = true
		db.mu.Lock()
		db.untrackWALSegmentLocked(path)
		db.mu.Unlock()
		db.forgetValueLogRetain(path)
	}
	if removed {
		db.syncDirBestEffort(db.dir)
	}

	if err := db.backend.Close(); err != nil {
		errs = append(errs, err)
	}
	if db.dictStore != nil {
		if closer, ok := db.dictStore.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if bgErr := db.backgroundError(); bgErr != nil {
		errs = append(errs, bgErr)
	}
	return errors.Join(errs...)
}
func (db *DB) Set(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}
	db.waitForCheckpoint()
	return db.set(key, value, false)
}

func (db *DB) SetSync(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}
	db.waitForCheckpoint()
	return db.set(key, value, true)
}

func (db *DB) flushAllMemtablesForSync(sync bool) error {
	db.writeMu.Lock()

	db.mu.Lock()
	if db.mutableBytes.Load() > 0 {
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			db.writeMu.Unlock()
			return err
		}
	}
	db.mu.Unlock()

	db.writeMu.Unlock()

	db.flushMu.Lock()
	db.flushAllLocked(sync)
	db.flushMu.Unlock()
	return db.backgroundError()
}

func (db *DB) syncBarrierAfterWrite(sync bool) error {
	if !sync {
		return nil
	}
	if !db.disableJournal {
		// Journal durability is handled by appendValueLog + appendWAL:
		// - strict: fsync
		// - relaxed: flush-to-kernel (no fsync)
		return nil
	}
	if db.relaxedSync {
		// Journal disabled: enforce a backend flush boundary without fsync.
		return db.flushAllMemtablesForSync(false)
	}
	// Journal disabled: enforce a durable backend boundary.
	return db.Checkpoint()
}

func (db *DB) set(key, value []byte, sync bool) error {
	if handled, err := db.enqueueDomainIngress(domainIngressOpSet, key, value, sync); handled {
		return err
	}
	return db.setDirect(key, value, sync)
}

func (db *DB) setDirect(key, value []byte, sync bool) error {
	db.writeMu.RLock()
	needRotate := false
	needSyncBarrier := false
	var ptr page.ValuePtr
	var retainPath string
	usePointer := false
	debugPtr := db.debugFlushPointers

	shard := db.shardForKey(key)

	durability := journalDurabilityNone
	if sync {
		if db.relaxedSync {
			durability = journalDurabilityFlush
		} else {
			durability = journalDurabilitySync
		}
	}
	eligible := db.shouldWriteViaValueLogForKeyValue(key, value)
	valueLogEnabled := db.valueLogEnabled()
	allowPointers := eligible && valueLogEnabled && db.allowValueLogPointers()
	routeAnchorMode := strings.TrimSpace(db.indexOuterLeafMode) == backenddb.IndexOuterLeafModeV1LeafLogRoute
	hybridRIDJoin := allowPointers && db.fenceRIDJoinHybridEnabled()
	oversized := hybridRIDJoin && db.oversizedOuterLeafValue(value)
	deferPointers := false
	if allowPointers && routeAnchorMode {
		// Route mode publishes one directory anchor per outer-leaf payload block at
		// flush time. Avoid per-key pointer appends on Set path.
		deferPointers = true
		allowPointers = false
	}
	if allowPointers && db.deferredValueLogEnabled() && !(hybridRIDJoin && oversized) {
		// In deferred mode we keep the mutable entry inline and regroup+append to
		// value-log at flush time.
		deferPointers = true
		allowPointers = false
		if eligible {
			db.recordDeferredFenceEnqueued(1, uint64(len(key)+len(value)))
		}
	}
	if allowPointers && db.disableJournal && !db.memtableValueLogPointers {
		// WAL-off: when the journal is disabled, defer value-log appends to the flush boundary
		// so repeated overwrites can coalesce in the memtable before hitting disk.
		allowPointers = false
	}
	addBytesForLimit := int64(len(key) + len(value))
	if allowPointers && db.memtableValueLogPointers {
		// Pointer-in-memtable mode stores only the key plus packed pointer payload.
		addBytesForLimit = int64(len(key) + page.ValuePtrSize)
	}
	if maxMemtableBytesPerShard > 0 {
		if addBytesForLimit > maxMemtableBytesPerShard {
			db.writeMu.RUnlock()
			return ErrMemtableFull
		}
		shard.mu.Lock()
		exceedsLimit := db.shardExceedsLimit(shard, addBytesForLimit)
		shard.mu.Unlock()
		if exceedsLimit {
			db.writeMu.RUnlock()
			return ErrMemtableFull
		}
	}
	if debugPtr && eligible {
		db.debugPtrEligible.Add(1)
	}

	var lane *lane
	if allowPointers || !db.disableJournal {
		l, err := db.pickLane(durability == journalDurabilitySync, db.laneForShardIndex(db.shardIndex(key)))
		if err != nil {
			db.writeMu.RUnlock()
			return err
		}
		lane = l
		if durability == journalDurabilitySync {
			defer db.releaseLaneSync(lane)
		}
	}

	if allowPointers {
		dictID := uint64(0)
		if db.valueLogDictTrain.TrainBytes > 0 {
			id, err := db.currentDictID(context.Background())
			if err != nil {
				db.writeMu.RUnlock()
				return err
			}
			dictID = id
		} else {
			dictID = db.dictCurrentCached.Load()
		}

		walRID := uint64(0)
		if hybridRIDJoin && oversized {
			outerPtr, outerRID, retain, err := db.appendFenceRIDJoinOversizedOne(lane, dictID, key, value, durability)
			if err != nil {
				db.writeMu.RUnlock()
				return err
			}
			ptr = outerPtr
			walRID = outerRID
			retainPath = retain
		} else {
			rid := db.nextRID.Add(1)
			var retain string
			appendPtr, retain, appendErr := db.appendValueLogOneWithKey(lane, dictID, nil, rid, key, value, durability)
			if appendErr != nil {
				db.writeMu.RUnlock()
				return appendErr
			}
			ptr = appendPtr
			walRID = rid
			retainPath = retain
		}
		usePointer = true
		if debugPtr {
			db.debugPtrUsed.Add(1)
		}

		if !db.disableJournal {
			rec := logRecord{Op: logOpSetRID, Key: key, RID: walRID}
			if err := db.appendWALOne(lane, rec, durability); err != nil {
				db.writeMu.RUnlock()
				return err
			}
		}
	} else if !db.disableJournal {
		if debugPtr && eligible && !deferPointers {
			if !valueLogEnabled {
				db.debugPtrDisabled.Add(1)
			} else {
				db.debugPtrDenied.Add(1)
			}
		}
		rec := logRecord{Op: logOpSetInline, Key: key, Value: value}
		if err := db.appendWALOne(lane, rec, durability); err != nil {
			db.writeMu.RUnlock()
			return err
		}
	} else if debugPtr && eligible && !deferPointers {
		if !valueLogEnabled {
			db.debugPtrDisabled.Add(1)
		} else {
			db.debugPtrDenied.Add(1)
		}
	}

	shard.mu.Lock()
	if usePointer {
		memVal := []byte(nil)
		if !db.memtableValueLogPointers {
			memVal = value
		}
		shard.mem.SetEntry(key, memVal, ptr, node.FlagPointer)
	} else {
		shard.mem.SetEntry(key, value, page.ValuePtr{}, node.FlagInline)
	}
	shard.rng.add(key)
	newBytes := shard.mem.Size()
	delta := newBytes - shard.bytes
	shard.bytes = newBytes
	db.mutableBytes.Add(delta)
	shard.mu.Unlock()
	db.noteWriteKey(key)

	// 3. Check Threshold
	if db.mutableBytes.Load() > db.mutableFlushThreshold() {
		needRotate = true
	}
	if sync && db.disableJournal {
		needSyncBarrier = true
	}
	db.writeMu.RUnlock()

	if retainPath != "" {
		db.markValueLogRetain(retainPath)
	}

	if needRotate {
		if err := db.maybeRotateMemtable(true); err != nil {
			return err
		}
	}
	if needSyncBarrier {
		if err := db.syncBarrierAfterWrite(true); err != nil {
			return err
		}
	}

	db.noteWrite()
	db.maybeAssistFlush()
	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	db.waitForCheckpoint()
	return db.delete(key, false)
}

// DeleteRange deletes all keys in the range [start, end).
//
// When WAL is disabled and the backend is empty, a full-range delete can be
// satisfied by clearing the in-memory layers without enumerating keys.
func (db *DB) DeleteRange(start, end []byte) error {
	if db == nil {
		return nil
	}
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return nil
	}
	db.waitForCheckpoint()

	// Journal-enabled mode: do a snapshot scan and apply per-key deletes directly.
	// Append journal records one-by-one to preserve batch atomicity and to avoid
	// post-journal apply divergence on partial batch failure.
	if !db.disableJournal {
		db.writeMu.Lock()
		defer db.writeMu.Unlock()

		it, err := db.Iterator(start, end)
		if err != nil {
			return err
		}
		defer func() { _ = it.Close() }()

		applyDelete := func(key []byte) error {
			if maxMemtableBytesPerShard > 0 && int64(len(key)) > maxMemtableBytesPerShard {
				return ErrMemtableFull
			}
			for {
				shard := db.shardForKey(key)
				shard.mu.Lock()
				if db.shardExceedsLimit(shard, int64(len(key))) {
					shard.mu.Unlock()
					db.mu.Lock()
					err := db.rotateMemtableLocked(true)
					db.mu.Unlock()
					if err != nil {
						return err
					}
					continue
				}
				if err := shard.mem.DeleteWithCallback(key, nil); err != nil {
					shard.mu.Unlock()
					return err
				}
				shard.rng.add(key)
				newBytes := shard.mem.Size()
				delta := newBytes - shard.bytes
				shard.bytes = newBytes
				db.mutableBytes.Add(delta)
				shard.mu.Unlock()
				db.noteWriteKey(key)
				if db.mutableBytes.Load() > db.mutableFlushThreshold() {
					db.mu.Lock()
					if db.mutableBytes.Load() > db.mutableFlushThreshold() {
						if err := db.rotateMemtableLocked(true); err != nil {
							db.mu.Unlock()
							return err
						}
					}
					db.mu.Unlock()
				}
				return nil
			}
		}

		poisonApply := func(err error) error {
			if err == nil {
				return nil
			}
			db.reportError(fmt.Errorf("cachingdb: WAL apply failed: %w", err))
			db.walAckMu.Lock()
			if db.walErr == nil {
				db.walErr = err
			}
			db.walAckMu.Unlock()
			return err
		}

		preRotate := func(key []byte) error {
			if maxMemtableBytesPerShard > 0 && int64(len(key)) > maxMemtableBytesPerShard {
				return ErrMemtableFull
			}
			if db.mutableBytes.Load() > db.mutableFlushThreshold() {
				db.mu.Lock()
				if db.mutableBytes.Load() > db.mutableFlushThreshold() {
					err := db.rotateMemtableLocked(true)
					db.mu.Unlock()
					if err != nil {
						return err
					}
				} else {
					db.mu.Unlock()
				}
			}
			shard := db.shardForKey(key)
			shard.mu.Lock()
			exceeds := db.shardExceedsLimit(shard, int64(len(key)))
			shard.mu.Unlock()
			if exceeds {
				db.mu.Lock()
				err := db.rotateMemtableLocked(true)
				db.mu.Unlock()
				if err != nil {
					return err
				}
			}
			return nil
		}

		lane, err := db.pickLane(false, -1)
		if err != nil {
			return err
		}
		deletedAny := false
		for it.Valid() {
			key := it.Key()
			if err := preRotate(key); err != nil {
				return err
			}
			if err := db.appendWALOne(lane, logRecord{Op: logOpDelete, Key: key}, journalDurabilityNone); err != nil {
				return err
			}
			if err := applyDelete(key); err != nil {
				return poisonApply(err)
			}
			deletedAny = true
			it.Next()
		}
		if err := it.Error(); err != nil {
			return err
		}
		if deletedAny {
			// DeleteRange can emit many non-sync WAL records; flush once so process
			// crashes do not lose buffered tombstones before a later sync op.
			if err := db.flushWALLane(lane); err != nil {
				return err
			}
		}

		db.noteWrite()
		db.maybeAssistFlush()
		return nil
	}

	// Ensure we know whether the backend currently contains any keys so that we
	// can safely take the "clear memtables" fast path on empty backends.
	if err := db.ensureBackendRange(); err != nil {
		return err
	}

	// Fast path: when the journal is disabled and there is no in-memory state to merge,
	// avoid snapshot isolation/merge iterators and delete directly from the
	// backend in a single commit.
	//
	// This is safe only when we have no queued memtables and the mutable memtable
	// is empty; otherwise we'd violate "newest wins" semantics.
	if db.disableJournal {
		db.mu.Lock()
		backendOnly := len(db.queue) == 0 && db.mutableBytes.Load() == 0
		db.mu.Unlock()
		if backendOnly {
			it, err := db.backend.Iterator(start, end)
			if err != nil {
				return err
			}
			defer func() { _ = it.Close() }()

			b := db.backend.NewBatch()
			defer func() { _ = b.Close() }()
			for it.Valid() {
				if err := b.Delete(it.Key()); err != nil {
					return err
				}
				it.Next()
			}
			if err := it.Error(); err != nil {
				return err
			}
			if err := b.Write(); err != nil {
				return err
			}
			// Best-effort: backend range can shrink; force recompute later.
			db.mu.Lock()
			db.backendRangeKnown = false
			db.backendRange = keyRange{}
			db.mu.Unlock()
			return nil
		}
	}

	// Serialize against flushers while we inspect/clear the in-memory layers.
	db.flushMu.Lock()
	db.mu.Lock()

	// Compute overall min/max across in-memory layers.
	var (
		haveAny bool
		minKey  []byte
		maxKey  []byte
	)
	addRange := func(r keyRange) {
		if !r.valid {
			return
		}
		if !haveAny {
			haveAny = true
			minKey = r.min
			maxKey = r.max
			return
		}
		if bytes.Compare(r.min, minKey) < 0 {
			minKey = r.min
		}
		if bytes.Compare(r.max, maxKey) > 0 {
			maxKey = r.max
		}
	}

	mutableRange := db.snapshotMutableRange()
	addRange(mutableRange)
	for _, r := range db.queueRanges {
		addRange(r)
	}

	backendEmpty := db.backendRangeKnown && !db.backendRange.valid
	if !backendEmpty {
		addRange(db.backendRange)
	}

	if haveAny {
		coversAll := true
		if start != nil && bytes.Compare(start, minKey) > 0 {
			coversAll = false
		}
		if end != nil && bytes.Compare(end, maxKey) <= 0 {
			// end is exclusive; to cover maxKey it must be strictly greater.
			coversAll = false
		}

		// Fast path: if the backend is empty and the delete range covers all keys we
		// currently have buffered in memory, just drop the in-memory state. This
		// avoids iterator creation, merges, and per-key tombstones.
		if coversAll && db.disableJournal && backendEmpty {
			curMode := db.memtableMode
			nextMode := curMode
			if db.memtableAdaptive {
				nextMode = db.applyAdaptiveMemtableModeLocked()
				db.memtableWarmupActive = false
				db.updateAdaptiveObservationLocked()
			}

			db.queueRetiredMemtablesLocked(db.queue)
			db.queue = nil
			db.queueShardIDs = nil
			db.queueLaneIDs = nil
			db.queueIDs = nil
			db.queueEnqueueNS = nil
			db.queueRanges = nil
			db.queueWALPaths = nil
			db.queueValueLogPaths = nil
			db.queueBacklogBytes.Store(0)
			db.materializationLastDrainUnixNano.Store(time.Now().UnixNano())
			if err := db.resetMutableShardsLocked(nextMode, nextMode == curMode); err != nil {
				db.mu.Unlock()
				db.flushMu.Unlock()
				return err
			}

			db.mu.Unlock()
			db.flushMu.Unlock()
			return nil
		}
	}

	db.mu.Unlock()
	db.flushMu.Unlock()

	// When the journal is disabled (op-geth style "unsafe" mode), avoid snapshot
	// isolation and MergingIterator overhead. DeleteRange doesn't require sorted
	// enumeration across sources; we can scan each source independently and write
	// tombstones into the current mutable memtable.
	if db.disableJournal {
		// Serialize writers and flushers while we enumerate keys to delete and
		// apply tombstones. This keeps snapshot semantics simple and avoids
		// rotating the memtable (which can allocate large arenas).
		db.flushMu.Lock()
		defer db.flushMu.Unlock()
		db.writeMu.Lock()
		defer db.writeMu.Unlock()

		db.mu.Lock()
		mutableRange := db.snapshotMutableRange()
		coversInMemory := queryCoversRange(start, end, mutableRange)
		for _, r := range db.queueRanges {
			if !queryCoversRange(start, end, r) {
				coversInMemory = false
				break
			}
		}

		// Fast path (DisableWAL only): if the delete range covers all buffered
		// in-memory keys, clear the in-memory layers and delete directly from the
		// backend. This avoids building large tombstone sets and avoids per-key
		// copies into an intermediate slice.
		if coversInMemory {
			curMode := db.memtableMode
			nextMode := curMode
			if db.memtableAdaptive {
				nextMode = db.applyAdaptiveMemtableModeLocked()
				db.memtableWarmupActive = false
				db.updateAdaptiveObservationLocked()
			}

			db.queueRetiredMemtablesLocked(db.queue)
			db.queue = nil
			db.queueShardIDs = nil
			db.queueLaneIDs = nil
			db.queueIDs = nil
			db.queueEnqueueNS = nil
			db.queueRanges = nil
			db.queueWALPaths = nil
			db.queueValueLogPaths = nil
			db.queueBacklogBytes.Store(0)
			db.materializationLastDrainUnixNano.Store(time.Now().UnixNano())
			if err := db.resetMutableShardsLocked(nextMode, nextMode == curMode); err != nil {
				db.mu.Unlock()
				return err
			}
			db.mu.Unlock()

			it, err := db.backend.Iterator(start, end)
			if err != nil {
				return err
			}
			defer func() { _ = it.Close() }()

			b := db.backend.NewBatch()
			defer func() { _ = b.Close() }()

			for it.Valid() {
				if err := b.Delete(it.Key()); err != nil {
					return err
				}
				it.Next()
			}
			if err := it.Error(); err != nil {
				return err
			}
			if err := b.Write(); err != nil {
				return err
			}

			// Best-effort: backend range can shrink; force recompute later.
			db.mu.Lock()
			db.backendRangeKnown = false
			db.backendRange = keyRange{}
			db.mu.Unlock()

			db.noteWrite()
			return nil
		}

		backendRange := db.backendRange

		// If we need to enumerate keys from the current mutable memtable, rotate it
		// first so we never mutate a memtable while iterating it.
		if overlapsQuery(start, end, mutableRange) && db.mutableBytes.Load() > 0 {
			if queryCoversRange(start, end, mutableRange) {
				if err := db.resetMutableShardsLocked(db.memtableMode, true); err != nil {
					db.mu.Unlock()
					return err
				}
			} else {
				if err := db.rotateMemtableLocked(false); err != nil {
					db.mu.Unlock()
					return err
				}
			}
			mutableRange = db.snapshotMutableRange()
		}

		// Drop fully-covered queued memtables without enumerating their keys.
		if len(db.queue) > 0 {
			db.ensureQueueLaneIDsLocked()
			dstQueue := db.queue[:0]
			dstShardIDs := db.queueShardIDs[:0]
			dstLaneIDs := db.queueLaneIDs[:0]
			dstIDs := db.queueIDs[:0]
			dstEnqueueNS := db.queueEnqueueNS[:0]
			dstRanges := db.queueRanges[:0]
			dstWALPaths := db.queueWALPaths[:0]
			dstValueLogPaths := db.queueValueLogPaths[:0]
			for i, mem := range db.queue {
				r := keyRange{}
				if i < len(db.queueRanges) {
					r = db.queueRanges[i]
				}
				if queryCoversRange(start, end, r) {
					db.queueRetiredMemtableLocked(mem)
					db.queueBacklogBytes.Add(-mem.Size())
					continue
				}
				dstQueue = append(dstQueue, mem)
				if i < len(db.queueShardIDs) {
					dstShardIDs = append(dstShardIDs, db.queueShardIDs[i])
				} else {
					dstShardIDs = append(dstShardIDs, 0)
				}
				if i < len(db.queueLaneIDs) {
					dstLaneIDs = append(dstLaneIDs, db.queueLaneIDs[i])
				} else {
					dstLaneIDs = append(dstLaneIDs, 0)
				}
				if i < len(db.queueIDs) {
					dstIDs = append(dstIDs, db.queueIDs[i])
				} else {
					dstIDs = append(dstIDs, 0)
				}
				if i < len(db.queueEnqueueNS) {
					dstEnqueueNS = append(dstEnqueueNS, db.queueEnqueueNS[i])
				} else {
					dstEnqueueNS = append(dstEnqueueNS, 0)
				}
				dstRanges = append(dstRanges, r)
				if i < len(db.queueWALPaths) {
					dstWALPaths = append(dstWALPaths, db.queueWALPaths[i])
				} else {
					dstWALPaths = append(dstWALPaths, nil)
				}
				if i < len(db.queueValueLogPaths) {
					dstValueLogPaths = append(dstValueLogPaths, db.queueValueLogPaths[i])
				} else {
					dstValueLogPaths = append(dstValueLogPaths, nil)
				}
			}
			db.queue = dstQueue
			db.queueShardIDs = dstShardIDs
			db.queueLaneIDs = dstLaneIDs
			db.queueIDs = dstIDs
			db.queueEnqueueNS = dstEnqueueNS
			db.queueRanges = dstRanges
			db.queueWALPaths = dstWALPaths
			db.queueValueLogPaths = dstValueLogPaths
			if len(db.queue) == 0 {
				db.materializationLastDrainUnixNano.Store(time.Now().UnixNano())
			}
		}
		db.publishMemtablesLocked()

		// Snapshot sources after any rotations/drops.
		mutableHasData := db.mutableBytes.Load() > 0
		mutableRanges := make([]keyRange, len(db.mutableShards))
		mutables := make([]memtable.Table, len(db.mutableShards))
		for i := range db.mutableShards {
			shard := &db.mutableShards[i]
			shard.mu.Lock()
			mutables[i] = shard.mem
			mutableRanges[i] = cloneRange(shard.rng)
			shard.mu.Unlock()
		}
		queue := append([]memtable.Table(nil), db.queue...)
		queueRanges := append([]keyRange(nil), db.queueRanges...)
		db.mu.Unlock()

		var (
			backendIter  iterator.UnsafeIterator
			queueIters   []iterator.UnsafeIterator
			mutableIters []iterator.UnsafeIterator
		)

		if overlapsQuery(start, end, backendRange) {
			it, err := db.backend.Iterator(start, end)
			if err != nil {
				return err
			}
			backendIter = it
			defer func() { _ = backendIter.Close() }()
		}

		if mutableHasData {
			for i, mem := range mutables {
				if mem == nil {
					continue
				}
				if i < len(mutableRanges) && !overlapsQuery(start, end, mutableRanges[i]) {
					continue
				}
				it := mem.NewIterator(start, end)
				it.Seek(start)
				mutableIters = append(mutableIters, it)
				defer func(it iterator.UnsafeIterator) { _ = it.Close() }(it)
			}
		}

		for i, mem := range queue {
			if i < len(queueRanges) && !overlapsQuery(start, end, queueRanges[i]) {
				continue
			}
			it := mem.NewIterator(start, end)
			it.Seek(start)
			queueIters = append(queueIters, it)
			defer func(it iterator.UnsafeIterator) { _ = it.Close() }(it)
		}

		db.mu.Lock()
		applyDelete := func(key []byte) error {
			shard := db.shardForKey(key)
			shard.mu.Lock()
			if db.shardExceedsLimit(shard, int64(len(key))) {
				shard.mu.Unlock()
				return ErrMemtableFull
			}
			shard.mem.Delete(key)
			shard.rng.add(key)
			newBytes := shard.mem.Size()
			delta := newBytes - shard.bytes
			shard.bytes = newBytes
			db.mutableBytes.Add(delta)
			shard.mu.Unlock()
			if db.mutableBytes.Load() > db.mutableFlushThreshold() {
				if err := db.rotateMemtableLocked(true); err != nil {
					return err
				}
			}
			return nil
		}

		for _, it := range mutableIters {
			for it.Valid() {
				if !it.IsDeleted() {
					if err := applyDelete(it.UnsafeKey()); err != nil {
						db.mu.Unlock()
						return err
					}
				}
				it.Next()
			}
			if err := it.Error(); err != nil {
				db.mu.Unlock()
				return err
			}
		}

		if backendIter != nil {
			for backendIter.Valid() {
				if err := applyDelete(backendIter.Key()); err != nil {
					db.mu.Unlock()
					return err
				}
				backendIter.Next()
			}
			if err := backendIter.Error(); err != nil {
				db.mu.Unlock()
				return err
			}
		}

		for _, it := range queueIters {
			for it.Valid() {
				if !it.IsDeleted() {
					if err := applyDelete(it.UnsafeKey()); err != nil {
						db.mu.Unlock()
						return err
					}
				}
				it.Next()
			}
			if err := it.Error(); err != nil {
				db.mu.Unlock()
				return err
			}
		}
		db.mu.Unlock()

		db.noteWrite()
		db.maybeAssistFlush()
		return nil
	}

	// Fallback: enumerate keys via a snapshot iterator.
	it, err := db.Iterator(start, end)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()

	b := db.NewBatch()
	defer b.Close()
	for it.Valid() {
		if err := b.Delete(it.Key()); err != nil {
			return err
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return b.Write()
}

func (db *DB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	db.waitForCheckpoint()
	return db.delete(key, true)
}

func (db *DB) delete(key []byte, sync bool) error {
	if handled, err := db.enqueueDomainIngress(domainIngressOpDelete, key, nil, sync); handled {
		return err
	}
	return db.deleteDirect(key, sync)
}

func (db *DB) deleteDirect(key []byte, sync bool) error {
	db.writeMu.RLock()
	needRotate := false
	needSyncBarrier := false

	shard := db.shardForKey(key)
	shard.mu.Lock()
	if db.shardExceedsLimit(shard, int64(len(key))) {
		shard.mu.Unlock()
		db.writeMu.RUnlock()
		return ErrMemtableFull
	}
	shard.mu.Unlock()

	if !db.disableJournal {
		durability := journalDurabilityNone
		if sync {
			if db.relaxedSync {
				durability = journalDurabilityFlush
			} else {
				durability = journalDurabilitySync
			}
		}
		lane, err := db.pickLane(durability == journalDurabilitySync, db.laneForShardIndex(db.shardIndex(key)))
		if err != nil {
			db.writeMu.RUnlock()
			return err
		}
		if durability == journalDurabilitySync {
			defer db.releaseLaneSync(lane)
		}
		rec := logRecord{Op: logOpDelete, Key: key}
		if err := db.appendWALOne(lane, rec, durability); err != nil {
			db.writeMu.RUnlock()
			return err
		}
	}

	shard.mu.Lock()
	shard.mem.Delete(key)
	shard.rng.add(key)
	newBytes := shard.mem.Size()
	delta := newBytes - shard.bytes
	shard.bytes = newBytes
	db.mutableBytes.Add(delta)
	shard.mu.Unlock()
	db.noteWriteKey(key)

	// 3. Threshold
	if db.mutableBytes.Load() > db.mutableFlushThreshold() {
		needRotate = true
	}
	if sync && db.disableJournal {
		needSyncBarrier = true
	}
	db.writeMu.RUnlock()

	if needRotate {
		if err := db.maybeRotateMemtable(true); err != nil {
			return err
		}
	}
	if needSyncBarrier {
		if err := db.syncBarrierAfterWrite(true); err != nil {
			return err
		}
	}

	db.noteWrite()
	db.maybeAssistFlush()
	return nil
}

func (db *DB) canReuseWALSegments() bool {
	for i := range db.lanes {
		l := &db.lanes[i]
		l.walMu.Lock()
		w := l.wal
		live := l.walLiveBytes.Load()
		l.walMu.Unlock()
		if w == nil {
			return false
		}
		if live >= 10*1024*1024 {
			return false
		}
	}
	return true
}

func (db *DB) rotateMemtableLockedWithCapacity(triggerFlush bool, newCapacity int) error {
	var walPaths []string
	var valueLogPaths []string
	if !db.disableJournal {
		walPaths = db.currentWALPaths()
	}
	if db.valueLogEnabled() {
		valueLogPaths = db.currentValueLogPaths()
	}
	if newCapacity < 0 {
		newCapacity = db.memtableCap
	}
	if db.memtableAdaptive {
		db.applyAdaptiveMemtableModeLocked()
	}
	if db.memtableWarmupActive {
		db.memtableWarmupActive = false
		db.updateAdaptiveObservationLocked()
	}
	db.mutableBytes.Store(0)
	enqueueNS := time.Now().UnixNano()
	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		shard.mem.Freeze()
		memBytes := shard.mem.Size()
		queueLaneID := db.laneForShardIndex(i)
		if db.deferredValueLogNeedsSingleLaneRegroup() {
			// Deferred fence mode requires global key-order regrouping; queue all shards
			// on lane 0 so flush can merge and collapse fence pointers consistently.
			queueLaneID = 0
		}
		db.queue = append(db.queue, shard.mem)
		db.queueShardIDs = append(db.queueShardIDs, uint16(i))
		db.queueLaneIDs = append(db.queueLaneIDs, uint16(queueLaneID))
		db.queueIDs = append(db.queueIDs, db.nextQueueID.Add(1))
		db.queueEnqueueNS = append(db.queueEnqueueNS, enqueueNS)
		db.queueBacklogBytes.Add(memBytes)
		db.queueRanges = append(db.queueRanges, shard.rng)
		db.queueWALPaths = append(db.queueWALPaths, walPaths)
		db.queueValueLogPaths = append(db.queueValueLogPaths, valueLogPaths)

		mt, err := db.newMutableMemtableWithCapacityMode(newCapacity, db.memtableMode)
		if err != nil {
			shard.mu.Unlock()
			return err
		}
		shard.mem = mt
		shard.rng = keyRange{}
		shard.bytes = 0
		shard.mu.Unlock()
	}
	db.updateMutableThresholdLocked()
	db.publishMemtablesLocked()

	// Optimization: Reuse WAL if small (e.g. < 10MB) to avoid syscall overhead
	// on frequent rotations (e.g. caused by frequent Iterator creation).
	if !db.disableJournal {
		if db.canReuseWALSegments() {
			if triggerFlush {
				select {
				case db.flushCh <- struct{}{}:
				default:
				}
			}
			return nil
		}
		for i := range db.lanes {
			if err := db.rotateWALLocked(&db.lanes[i]); err != nil {
				return err
			}
		}
	}

	if triggerFlush {
		select {
		case db.flushCh <- struct{}{}:
		default:
		}
	}
	db.bpMu.Lock()
	db.bpCond.Broadcast()
	db.bpMu.Unlock()
	return nil
}

// rotateMemtableLockedForIterator rotates the current mutable memtable into the
// immutable queue for snapshot iteration without rotating the WAL segment.
//
// This is important for concurrency: iterator creation should not need to
// serialize behind writeMu just to protect WAL rotation.
//
// Caller must hold db.mu.
func (db *DB) rotateMemtableLockedForIterator(newCapacity int) error {
	return db.rotateMutableShardsLocked(newCapacity, false)
}

func (db *DB) rotateMemtableLocked(triggerFlush bool) error {
	return db.rotateMemtableLockedWithCapacity(triggerFlush, -1)
}

func (db *DB) rotateMemtableIfNeeded(triggerFlush bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.mutableBytes.Load() <= db.mutableFlushThreshold() {
		return nil
	}
	return db.rotateMutableShardsLocked(-1, triggerFlush)
}

func (db *DB) maybeRotateMemtable(triggerFlush bool) error {
	if db.mutableBytes.Load() <= db.mutableFlushThreshold() {
		return nil
	}
	if !db.rotatePending.CompareAndSwap(false, true) {
		return nil
	}
	defer db.rotatePending.Store(false)
	return db.rotateMemtableIfNeeded(triggerFlush)
}

// rotateMutableShardsLocked rotates the current mutable shards into the queue
// while holding db.mu (write) and the affected shard locks.
//
// It intentionally does not rotate the WAL segment; checkpoint is responsible
// for establishing durable boundaries and trimming old segments. This avoids
// requiring a global writer barrier around WAL rotation.
func (db *DB) rotateMutableShardsLocked(newCapacity int, triggerFlush bool) error {
	if newCapacity < 0 {
		newCapacity = db.memtableCap
	}
	if db.memtableAdaptive {
		db.applyAdaptiveMemtableModeLocked()
	}
	if db.memtableWarmupActive {
		db.memtableWarmupActive = false
		db.updateAdaptiveObservationLocked()
	}
	var walPaths []string
	if !db.disableJournal {
		walPaths = db.currentWALPaths()
	}
	var valueLogPaths []string
	if db.valueLogEnabled() {
		valueLogPaths = db.currentValueLogPaths()
	}

	locked := make([]*memShard, 0, len(db.mutableShards))
	defer func() {
		for i := len(locked) - 1; i >= 0; i-- {
			locked[i].mu.Unlock()
		}
	}()
	enqueueNS := time.Now().UnixNano()

	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		locked = append(locked, shard)

		// Remove this shard's contribution from the global byte counter before
		// resetting it, since writers may still be updating other shards.
		if shard.bytes != 0 {
			db.mutableBytes.Add(-shard.bytes)
		}

		// Freeze and enqueue the old mutable shard.
		shard.mem.Freeze()
		memBytes := shard.mem.Size()
		queueLaneID := db.laneForShardIndex(i)
		if db.deferredValueLogNeedsSingleLaneRegroup() {
			// Deferred fence mode requires global key-order regrouping; queue all shards
			// on lane 0 so flush can merge and collapse fence pointers consistently.
			queueLaneID = 0
		}
		db.queue = append(db.queue, shard.mem)
		db.queueShardIDs = append(db.queueShardIDs, uint16(i))
		db.queueLaneIDs = append(db.queueLaneIDs, uint16(queueLaneID))
		db.queueIDs = append(db.queueIDs, db.nextQueueID.Add(1))
		db.queueEnqueueNS = append(db.queueEnqueueNS, enqueueNS)
		db.queueBacklogBytes.Add(memBytes)
		db.queueRanges = append(db.queueRanges, shard.rng)
		db.queueWALPaths = append(db.queueWALPaths, walPaths)
		db.queueValueLogPaths = append(db.queueValueLogPaths, valueLogPaths)

		mt, err := db.newMutableMemtableWithCapacityMode(newCapacity, db.memtableMode)
		if err != nil {
			return err
		}
		shard.mem = mt
		shard.rng = keyRange{}
		shard.bytes = 0
	}

	db.updateMutableThresholdLocked()
	db.publishMemtablesLocked()

	if triggerFlush {
		select {
		case db.flushCh <- struct{}{}:
		default:
		}
	}
	db.bpMu.Lock()
	db.bpCond.Broadcast()
	db.bpMu.Unlock()
	return nil
}

func (db *DB) cleanupLaneWALWriters(l *lane) {
	if l == nil {
		return
	}
	l.walMu.Lock()
	if l.wal != nil {
		_ = l.wal.Close()
		l.wal = nil
	}
	l.walMu.Unlock()
	l.vlogMu.Lock()
	if l.vlog != nil {
		_ = l.vlog.Close()
		l.vlog = nil
		l.vlogCaps = vlogWriterCaps{}
	}
	l.vlogModeSet = false
	l.vlogModeWriter = nil
	l.vlogMu.Unlock()
}

func (db *DB) defaultVlogWriteMode() vlogCompressionWriteMode {
	if db == nil {
		return vlogWriteOff
	}
	switch normalizeVlogCompressionMode(db.valueLogCompressionMode) {
	case vlogCompressionBlock:
		return vlogWriteBlock
	case vlogCompressionDict:
		return vlogWriteDict
	default:
		return vlogWriteOff
	}
}

func (db *DB) setVlogWriterMode(l *lane, w valueWriter, mode vlogCompressionWriteMode, codec valuelog.BlockCodec) {
	if db == nil || w == nil {
		return
	}
	if l != nil && l.vlogModeSet && l.vlogModeWriter == w && l.vlogMode == mode && l.vlogBlockCodec == codec {
		return
	}
	setter, ok := any(w).(blockCompressionSetter)
	if !ok {
		return
	}
	setter.SetBlockCompression(codec, mode == vlogWriteBlock)
	if l != nil {
		l.vlogModeWriter = w
		l.vlogModeSet = true
		l.vlogMode = mode
		l.vlogBlockCodec = codec
	}
}

func (db *DB) rotateWALLocked(l *lane) error {
	return db.rotateWALLockedWithOptions(l, true)
}

func (db *DB) rotateWALCheckpointLocked(l *lane) error {
	return db.rotateWALLockedWithOptions(l, false)
}

func (db *DB) rotateWALLockedWithOptions(l *lane, rotateValueLog bool) error {
	if db.disableJournal {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}
	l.walMu.Lock()
	defer l.walMu.Unlock()
	l.walSeq++
	name := commitLogName(l.id, l.walSeq)
	path := filepath.Join(db.dir, name)

	if l.wal != nil {
		oldPath := l.walPath
		oldSize := l.wal.Size()
		if err := l.wal.RotateTo(path); err != nil {
			return err
		}
		l.walLiveBytes.Store(0)
		if oldPath != "" {
			if l.walClosedSizes == nil {
				l.walClosedSizes = make(map[string]int64)
			}
			prev := l.walClosedSizes[oldPath]
			l.walClosedSizes[oldPath] = oldSize
			l.walClosedBytes.Add(oldSize - prev)
		}
	} else {
		w, err := commitlog.NewWriterWithOptions(path, commitlog.Options{MaxSegmentSize: db.walMaxSegmentBytes, Compress: db.journalCompression})
		if err != nil {
			return err
		}
		l.wal = w
		l.walLiveBytes.Store(0)
	}
	l.walPath = path
	l.walLiveBytes.Store(0)
	if rotateValueLog && db.splitValueLogEnabled() {
		if err := db.rotateValueLogLocked(l); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) rotateValueLogLocked(l *lane) error {
	if !db.splitValueLogEnabled() {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}
	l.vlogMu.Lock()
	defer l.vlogMu.Unlock()
	return db.rotateValueLogMuHeld(l)
}

func (db *DB) rotateValueLogMuHeld(l *lane) error {
	l.vlogSeq++
	name := valueLogName(l.id, l.vlogSeq)
	path := filepath.Join(db.dir, name)
	fileID, err := valuelog.EncodeFileID(uint32(l.id), uint32(l.vlogSeq))
	if err != nil {
		return err
	}

	if l.vlog != nil {
		oldPath := l.vlogPath
		oldSize := l.vlog.Size()
		if err := l.vlog.RotateTo(path, fileID); err != nil {
			return err
		}
		l.vlog.SetDictFrameEncoderOptions(db.valueLogDictFrameEncodeLevel, db.valueLogDictFrameEnableEntropy)
		// Rotation can reset writer internals; force mode reapply.
		l.vlogModeSet = false
		l.vlogModeWriter = nil
		db.setVlogWriterMode(l, l.vlog, db.defaultVlogWriteMode(), db.valueLogBlockCodec)
		if setter, ok := any(l.vlog).(rawWritevStrategySetter); ok {
			setter.SetRawWritevStrategy(db.valueLogRawWritevMinAvgBytes, db.valueLogRawWritevMinRecords)
		}
		l.vlogLiveBytes.Store(0)
		if oldPath != "" {
			if l.vlogClosedSizes == nil {
				l.vlogClosedSizes = make(map[string]int64)
			}
			prev := l.vlogClosedSizes[oldPath]
			l.vlogClosedSizes[oldPath] = oldSize
			l.vlogClosedBytes.Add(oldSize - prev)
			if oldPath == l.vlogRetainedPath {
				db.valueLogRetainedClosedBytes.Add(oldSize - prev)
			}
		}
	} else {
		w, err := valuelog.NewWriter(path, fileID)
		if err != nil {
			return err
		}
		w.SetDictFrameEncoderOptions(db.valueLogDictFrameEncodeLevel, db.valueLogDictFrameEnableEntropy)
		l.vlogModeSet = false
		l.vlogModeWriter = nil
		db.setVlogWriterMode(l, w, db.defaultVlogWriteMode(), db.valueLogBlockCodec)
		w.SetRawWritevStrategy(db.valueLogRawWritevMinAvgBytes, db.valueLogRawWritevMinRecords)
		l.vlog = w
		l.vlogLiveBytes.Store(0)
	}
	l.vlogPath = path
	l.vlogLiveBytes.Store(0)
	return nil
}

func (db *DB) rotateValueLogForMaxSegmentMuHeld(l *lane, w valueWriter) error {
	if db == nil || l == nil || w == nil {
		return nil
	}
	maxBytes := db.valueLogMaxSegmentBytesForLane(l)
	if maxBytes <= 0 {
		return nil
	}
	if w.Size() <= maxBytes {
		return nil
	}
	return db.rotateValueLogMuHeld(l)
}

func (db *DB) untrackWALSegmentLocked(path string) {
	laneID, _, _, ok := parseLogSeq(filepath.Base(path))
	if !ok || laneID < 0 || laneID >= len(db.lanes) {
		return
	}
	l := &db.lanes[laneID]
	l.walMu.Lock()
	defer l.walMu.Unlock()
	if l.walClosedSizes == nil || path == "" {
		return
	}
	size, ok := l.walClosedSizes[path]
	if !ok {
		return
	}
	delete(l.walClosedSizes, path)
	for {
		cur := l.walClosedBytes.Load()
		next := cur - size
		if next < 0 {
			next = 0
		}
		if l.walClosedBytes.CompareAndSwap(cur, next) {
			break
		}
	}
}

func (db *DB) untrackValueLogSegmentLocked(path string) {
	laneID, _, _, ok := parseLogSeq(filepath.Base(path))
	if !ok || laneID < 0 || laneID >= len(db.lanes) {
		return
	}
	l := &db.lanes[laneID]
	l.vlogMu.Lock()
	defer l.vlogMu.Unlock()
	if l.vlogClosedSizes == nil || path == "" {
		return
	}
	size, ok := l.vlogClosedSizes[path]
	if !ok {
		return
	}
	delete(l.vlogClosedSizes, path)
	db.valueLogRetainedClosedBytes.Add(-size)
	for {
		cur := l.vlogClosedBytes.Load()
		next := cur - size
		if next < 0 {
			next = 0
		}
		if l.vlogClosedBytes.CompareAndSwap(cur, next) {
			break
		}
	}
}

func (db *DB) flushLoop() {
	defer db.wg.Done()

	for {
		select {
		case <-db.closeCh:
			// Flush all remaining with sync on close.
			db.flushAll(true)
			return
		case <-db.flushCh:
			// Background flush is async when WAL is enabled. Without a WAL, we
			// upgrade to a synced flush unless RelaxedSync is set.
			db.flushAll(false)
		}
	}
}

func (db *DB) flushSyncRequested(sync bool) bool {
	return sync && !db.relaxedSync
}

func (db *DB) pickFlushLane() (int, bool) {
	db.mu.RLock()
	if len(db.queue) == 0 {
		db.mu.RUnlock()
		return 0, false
	}
	laneCount := len(db.lanes)
	if laneCount == 0 {
		laneCount = 1
	}
	counts := make([]int, laneCount)
	for i := range db.queue {
		laneID := 0
		if i < len(db.queueLaneIDs) {
			laneID = int(db.queueLaneIDs[i])
		}
		if laneID < 0 || laneID >= laneCount {
			laneID = 0
		}
		counts[laneID]++
	}
	bestLane := 0
	bestCount := 0
	for laneID, count := range counts {
		if count > bestCount {
			bestCount = count
			bestLane = laneID
		}
	}
	db.mu.RUnlock()
	return bestLane, true
}

func (db *DB) flushAll(reqSync bool) {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	db.flushAllLocked(reqSync)
}

func (db *DB) flushAllLocked(reqSync bool) {
	origSync := reqSync
	syncFlag := db.flushSyncRequested(reqSync)
	if !origSync && syncFlag && db.disableJournal && !db.relaxedSync {
		db.debugVlogEvent("flushAll_upgraded_sync", -1, "flushMu")
	}
	lanes := len(db.lanes)
	if lanes == 0 {
		lanes = 1
	}

	// Only spawn flush workers for lanes that actually have queued memtables.
	// Otherwise each lane does an O(queueLen) scan in collectFlushUnitsLocked to
	// discover there's nothing to do, which can be extremely expensive when the
	// queue is large and lanes > 1.
	active := make([]bool, lanes)
	db.mu.RLock()
	queueLen := len(db.queue)
	for i := 0; i < queueLen; i++ {
		laneID := 0
		if i < len(db.queueLaneIDs) {
			laneID = int(db.queueLaneIDs[i])
		}
		if laneID < 0 || laneID >= lanes {
			laneID = 0
		}
		active[laneID] = true
	}
	db.mu.RUnlock()

	activeCount := 0
	for i := range active {
		if active[i] {
			activeCount++
		}
	}
	if activeCount == 0 {
		return
	}
	var wg sync.WaitGroup
	wg.Add(activeCount)
	for i := 0; i < lanes; i++ {
		laneID := i
		if !active[laneID] {
			continue
		}
		go func() {
			if laneID < len(db.flushLaneMu) {
				db.flushLaneMu[laneID].Lock()
				defer db.flushLaneMu[laneID].Unlock()
			}
			for db.flushLaneOnce(syncFlag, laneID) {
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func (db *DB) flushOne() bool {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()

	laneID, ok := db.pickFlushLane()
	if !ok {
		return false
	}
	if laneID < len(db.flushLaneMu) {
		db.flushLaneMu[laneID].Lock()
		defer db.flushLaneMu[laneID].Unlock()
	}
	return db.flushLaneOnce(true, laneID)
}

const (
	flushCombineTargetBytes    int64 = 64 * 1024 * 1024  // 64MiB
	flushCombineTargetBytesMax       = 256 * 1024 * 1024 // 256MiB
	flushCombineMaxMemtables         = 32
	// flushBackendBatchMaxEntries caps how many operations we buffer into a single
	// backend batch before committing it and continuing with a fresh batch.
	//
	// This avoids large one-shot allocations (and their GC / page-fault overhead)
	// when flushing very large immutable memtables (e.g. when value-log pointers
	// are forced).
	flushBackendBatchMaxEntries = 32 * 1024

	// flushBackendBatchInitEntries is a small "reserve hint" used for backend batch
	// creation. It intentionally stays below flushBackendBatchMaxEntries to avoid
	// spending large CPU time zeroing an oversized []batch.Entry on batch creation.
	flushBackendBatchInitEntries = 8 * 1024
)

type flushUnit struct {
	mem      memtable.Table
	memBytes int64
	memLen   int
	memRange keyRange
	walPaths []string
	id       uint64
	laneID   int
}

func (db *DB) collectFlushUnitsLocked(laneID int, maxMemtables int, targetBytes int64) ([]flushUnit, []uint64, int64, int) {
	queueLen := len(db.queue)
	if queueLen == 0 {
		return nil, nil, 0, 0
	}
	if maxMemtables <= 0 || maxMemtables > flushCombineMaxMemtables {
		maxMemtables = flushCombineMaxMemtables
	}
	units := make([]flushUnit, 0, maxMemtables)
	ids := make([]uint64, 0, maxMemtables)
	var totalBytes int64
	var totalLen int
	for i := 0; i < queueLen && len(units) < maxMemtables; i++ {
		if laneID >= 0 {
			unitLaneID := 0
			if i < len(db.queueLaneIDs) {
				unitLaneID = int(db.queueLaneIDs[i])
			}
			if unitLaneID != laneID {
				continue
			}
		} else if i >= maxMemtables {
			break
		}
		mem := db.queue[i]
		memBytes := mem.Size()
		memLen := mem.Len()
		if len(units) > 0 && targetBytes > 0 && totalBytes >= targetBytes {
			break
		}
		var walPaths []string
		if i < len(db.queueWALPaths) {
			walPaths = db.queueWALPaths[i]
		}
		var rng keyRange
		if i < len(db.queueRanges) {
			rng = db.queueRanges[i]
		}
		var id uint64
		if i < len(db.queueIDs) {
			id = db.queueIDs[i]
		}
		unitLaneID := 0
		if i < len(db.queueLaneIDs) {
			unitLaneID = int(db.queueLaneIDs[i])
		}
		units = append(units, flushUnit{
			mem:      mem,
			memBytes: memBytes,
			memLen:   memLen,
			memRange: rng,
			walPaths: walPaths,
			id:       id,
			laneID:   unitLaneID,
		})
		ids = append(ids, id)
		totalBytes += memBytes
		totalLen += memLen
	}
	return units, ids, totalBytes, totalLen
}

func (db *DB) removeQueuedUnitsLocked(removeIDs map[uint64]struct{}, units []flushUnit, totalBytes int64) {
	for _, unit := range units {
		if unit.memRange.valid {
			db.backendRange.add(unit.memRange.min)
			db.backendRange.add(unit.memRange.max)
		}
		db.queueRetiredMemtableLocked(unit.mem)
	}

	dstQueue := db.queue[:0]
	dstShardIDs := db.queueShardIDs[:0]
	dstLaneIDs := db.queueLaneIDs[:0]
	dstIDs := db.queueIDs[:0]
	dstEnqueueNS := db.queueEnqueueNS[:0]
	dstRanges := db.queueRanges[:0]
	dstWALPaths := db.queueWALPaths[:0]
	dstValueLogPaths := db.queueValueLogPaths[:0]

	db.ensureQueueLaneIDsLocked()
	for i, mem := range db.queue {
		var id uint64
		if i < len(db.queueIDs) {
			id = db.queueIDs[i]
		}
		if _, ok := removeIDs[id]; ok {
			continue
		}
		dstQueue = append(dstQueue, mem)
		if i < len(db.queueShardIDs) {
			dstShardIDs = append(dstShardIDs, db.queueShardIDs[i])
		}
		if i < len(db.queueLaneIDs) {
			dstLaneIDs = append(dstLaneIDs, db.queueLaneIDs[i])
		} else {
			dstLaneIDs = append(dstLaneIDs, 0)
		}
		if i < len(db.queueIDs) {
			dstIDs = append(dstIDs, db.queueIDs[i])
		}
		if i < len(db.queueEnqueueNS) {
			dstEnqueueNS = append(dstEnqueueNS, db.queueEnqueueNS[i])
		} else {
			dstEnqueueNS = append(dstEnqueueNS, 0)
		}
		if i < len(db.queueRanges) {
			dstRanges = append(dstRanges, db.queueRanges[i])
		}
		if i < len(db.queueWALPaths) {
			dstWALPaths = append(dstWALPaths, db.queueWALPaths[i])
		}
		if i < len(db.queueValueLogPaths) {
			dstValueLogPaths = append(dstValueLogPaths, db.queueValueLogPaths[i])
		}
	}

	db.queue = dstQueue
	db.queueShardIDs = dstShardIDs
	db.queueLaneIDs = dstLaneIDs
	db.queueIDs = dstIDs
	db.queueEnqueueNS = dstEnqueueNS
	db.queueRanges = dstRanges
	db.queueWALPaths = dstWALPaths
	db.queueValueLogPaths = dstValueLogPaths
	db.queueBacklogBytes.Add(-totalBytes)
	if len(db.queue) == 0 {
		db.materializationLastDrainUnixNano.Store(time.Now().UnixNano())
	}
	db.publishMemtablesLocked()
}

func (db *DB) flushLaneOnce(sync bool, laneID int) bool {
	db.mu.Lock()
	queueLen := len(db.queue)
	if queueLen == 0 {
		db.mu.Unlock()
		return false
	}
	maxMemtables := 1
	targetBytes := int64(0)
	if db.flushBuildConcurrency > 1 || db.deferredValueLogEnabled() {
		maxMemtables = flushCombineMaxMemtables
		targetBytes = flushCombineTargetBytes
		// When FlushThreshold ~= flushCombineTargetBytes (the common default),
		// combining is effectively disabled and large churny workloads are forced
		// through multiple full apply passes. Allow combining several memtables per
		// flush (bounded) to reduce repeated rewrite work.
		desired := db.flushThreshold * 4
		if desired > flushCombineTargetBytesMax {
			desired = flushCombineTargetBytesMax
		}
		if desired > targetBytes {
			targetBytes = desired
		}
	}
	units, ids, totalBytes, totalLen := db.collectFlushUnitsLocked(laneID, maxMemtables, targetBytes)
	db.mu.Unlock()
	if len(units) == 0 {
		return false
	}

	if totalLen == 0 {
		db.mu.Lock()
		removeIDs := make(map[uint64]struct{}, len(ids))
		for _, id := range ids {
			removeIDs[id] = struct{}{}
		}
		db.removeQueuedUnitsLocked(removeIDs, units, totalBytes)
		db.mu.Unlock()
		return true
	}

	backendEntriesCap := db.flushBackendEntriesCap(totalLen, sync)

	useParallel := db.flushBuildConcurrency > 1 &&
		totalLen >= db.flushBuildMinEntries &&
		len(units) >= db.flushBuildMinUnits &&
		runtime.GOMAXPROCS(0) > 1

	if useParallel && !db.deferredValueLogEnabled() {
		chunkCap := db.flushBuildChunkCap
		if chunkCap < 0 {
			chunkCap = 8192
		}

		type buildResult struct {
			idx       int
			runs      [][]batch.Entry
			deleteOps int
			err       error
		}

		jobs := make(chan int, len(units))
		results := make(chan buildResult, len(units))
		closeCh := db.closeCh

		for i := range units {
			jobs <- i
		}
		close(jobs)

		workers := db.flushBuildConcurrency
		if workers <= 0 {
			workers = 1
		}
		if db.flushBuildAutoConcurrency && totalLen > 0 {
			// Small inline-heavy entries are typically memory-copy bound; high
			// worker counts can over-parallelize and add scheduler overhead.
			// Keep wider concurrency for larger entries where per-entry work is
			// heavier (pointer/value encoding and compression).
			bytesPerEntry := totalBytes / int64(totalLen)
			switch {
			case bytesPerEntry <= 64:
				if workers > 4 {
					workers = 4
				}
			case bytesPerEntry <= 256:
				if workers > 6 {
					workers = 6
				}
			}
		}
		if workers > len(units) {
			workers = len(units)
		}

		done := make(chan struct{}, workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer func() { done <- struct{}{} }()
				for idx := range jobs {
					select {
					case <-closeCh:
						results <- buildResult{idx: idx, err: errDBClosing}
						continue
					default:
					}
					runs, deleteOps, err := buildOpRuns(units[idx].mem, chunkCap)
					results <- buildResult{idx: idx, runs: runs, deleteOps: deleteOps, err: err}
				}
			}()
		}

		go func() {
			for i := 0; i < workers; i++ {
				<-done
			}
			close(results)
		}()

		unitRuns := getUnitRuns(len(units))
		defer func() {
			for i := range unitRuns {
				for _, run := range unitRuns[i] {
					putEntrySlice(run)
				}
				putEntryRuns(unitRuns[i])
			}
			putUnitRuns(unitRuns)
		}()
		unitDeleteOps := make([]int, len(units))
		failed := false
		for res := range results {
			if res.err != nil {
				if !failed {
					db.reportError(fmt.Errorf("cachingdb: flush build failed: %w", res.err))
				}
				failed = true
				for _, run := range res.runs {
					putEntrySlice(run)
				}
				putEntryRuns(res.runs)
				continue
			}
			if failed {
				for _, run := range res.runs {
					putEntrySlice(run)
				}
				putEntryRuns(res.runs)
				continue
			}
			unitRuns[res.idx] = res.runs
			unitDeleteOps[res.idx] = res.deleteOps
		}
		if failed {
			return false
		}

		// Adaptive micro-batching: delete-heavy flushes are expensive to apply in
		// many intermediate commits (each commit re-writes leaf pages, copying
		// surviving values). Count deletes and tighten the commit cap in that case.
		deleteOps := 0
		for _, n := range unitDeleteOps {
			deleteOps += n
		}
		backendEntriesCap = db.flushBackendEntriesCapForOps(totalLen, deleteOps, sync)

		reserveChunkOps := totalLen
		if reserveChunkOps > backendEntriesCap {
			reserveChunkOps = backendEntriesCap
		}
		sizeHint := reserveChunkOps
		backendBatch := db.newBackendBatchWithSize(sizeHint)
		reserveBackendBatchOps(backendBatch, reserveChunkOps)
		flushStart := time.Now()
		vlogFlushed := false
		backendPendingOps := 0
		chunkBackend := totalLen > backendEntriesCap
		emittedChunk := false
		fenceMode := db.outerLeafAnchorModeEnabled()
		collapseFence := fenceMode && db.allowFencePointerCollapse()

		type ptrSetterView interface {
			SetPointerView(key []byte, ptr page.ValuePtr) error
		}
		type ptrSetter interface {
			SetPointer(key []byte, ptr page.ValuePtr) error
		}
		psv, _ := backendBatch.(ptrSetterView)
		ps, _ := backendBatch.(ptrSetter)
		var single [1]batch.Entry

		// Best-effort: ensure value-log bytes are flushed before we start committing
		// pointers into the index when we expect to emit multiple backend commits.
		if chunkBackend && db.valueLogEnabled() && !vlogFlushed {
			if err := db.flushValueLog(laneID); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (vlog): %w", err))
				_ = backendBatch.Close()
				return false
			}
			vlogFlushed = true
		}

		flushBackendChunk := func() error {
			if !chunkBackend || backendPendingOps < backendEntriesCap {
				return nil
			}
			emittedChunk = true
			db.backendWriteBatchesTotal.Add(1)
			// If sync==true, we only need a single durability boundary at the end of
			// the flush. Write the intermediate chunks without fsync to avoid
			// repeated pager sync work.
			err := backendBatch.Write()
			cerr := backendBatch.Close()
			if err == nil {
				err = cerr
			}
			if err != nil {
				return err
			}
			backendBatch = db.newBackendBatchWithSize(sizeHint)
			reserveBackendBatchOps(backendBatch, reserveChunkOps)
			psv, _ = backendBatch.(ptrSetterView)
			ps, _ = backendBatch.(ptrSetter)
			backendPendingOps = 0
			return nil
		}

		heap := getOpMergeHeap(len(unitRuns))
		defer func() { putOpMergeHeap(heap) }()
		for i := range unitRuns {
			if len(unitRuns[i]) == 0 {
				continue
			}
			it := newOpRunIter(unitRuns[i])
			if it.Valid() {
				priority := len(unitRuns) - 1 - i
				heap = append(heap, opMergeItem{iter: it, priority: priority, key: it.Key()})
			}
		}
		for i := len(heap)/2 - 1; i >= 0; i-- {
			(&heap).down(i, len(heap))
		}
		var (
			lastFencePtr page.ValuePtr
			haveFencePtr bool
		)
		var promoter *fenceAnchorPromoter
		if collapseFence {
			promoter = newFenceAnchorPromoter(db, fenceMutationLookupForUnits(units))
			defer promoter.close()
		}
		emitPromotionPointer := func(key []byte, ptr page.ValuePtr) error {
			var err error
			if psv != nil {
				err = psv.SetPointerView(key, ptr)
			} else if ps != nil {
				err = ps.SetPointer(key, ptr)
			} else {
				single[0] = batch.Entry{Type: batch.OpPut, Key: key, ValuePtr: ptr, IsPtr: true}
				err = backendBatch.SetOps(single[:])
			}
			if err != nil {
				return err
			}
			backendPendingOps++
			return flushBackendChunk()
		}
		emitPromotionDelete := func(key []byte) error {
			if err := backendBatch.Delete(key); err != nil {
				return err
			}
			backendPendingOps++
			return flushBackendChunk()
		}

		for len(heap) > 0 {
			top := heap.pop()
			currentKey := top.key

			for len(heap) > 0 {
				next := heap.peek()
				if next != nil && bytes.Equal(next.key, currentKey) {
					shadowed := heap.pop()
					shadowed.iter.Next()
					if shadowed.iter.Valid() {
						shadowed.key = shadowed.iter.Key()
						heap.push(shadowed)
					}
					continue
				}
				break
			}

			entry := top.iter.Entry()
			var (
				err     error
				applied bool
			)
			switch {
			case entry.Type == batch.OpDelete:
				if promoter != nil {
					if err = promoter.maybePromoteAnchor(entry.Key, fencePendingMutation{kind: fencePendingMutationDelete}, emitPromotionPointer, emitPromotionDelete); err != nil {
						break
					}
				}
				err = backendBatch.Delete(entry.Key)
				applied = true
				haveFencePtr = false
			case entry.IsPtr:
				if promoter != nil {
					if err = promoter.maybePromoteAnchor(entry.Key, fencePendingMutation{kind: fencePendingMutationSetPointer, ptr: entry.ValuePtr}, emitPromotionPointer, emitPromotionDelete); err != nil {
						break
					}
				}
				emitPtr := true
				if collapseFence {
					if haveFencePtr && valuePtrSameRecord(entry.ValuePtr, lastFencePtr) {
						emitPtr = false
					} else {
						lastFencePtr = entry.ValuePtr
						haveFencePtr = true
					}
				}
				if emitPtr {
					if psv != nil {
						err = psv.SetPointerView(entry.Key, entry.ValuePtr)
					} else if ps != nil {
						err = ps.SetPointer(entry.Key, entry.ValuePtr)
					} else {
						single[0] = batch.Entry{Type: batch.OpPut, Key: entry.Key, ValuePtr: entry.ValuePtr, IsPtr: true}
						err = backendBatch.SetOps(single[:])
					}
					applied = true
				}
			default:
				if promoter != nil {
					if err = promoter.maybePromoteAnchor(entry.Key, fencePendingMutation{kind: fencePendingMutationSetInline}, emitPromotionPointer, emitPromotionDelete); err != nil {
						break
					}
				}
				err = backendBatch.Set(entry.Key, entry.Value)
				applied = true
				haveFencePtr = false
			}
			if err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
				_ = backendBatch.Close()
				return false
			}
			if applied {
				backendPendingOps++
				if err := flushBackendChunk(); err != nil {
					db.reportError(err)
					_ = backendBatch.Close()
					return false
				}
			}

			top.iter.Next()
			if top.iter.Valid() {
				top.key = top.iter.Key()
				heap.push(top)
			}
		}

		if db.valueLogEnabled() {
			if !vlogFlushed {
				if err := db.flushValueLog(laneID); err != nil {
					db.reportError(fmt.Errorf("cachingdb: flush failed (vlog): %w", err))
					_ = backendBatch.Close()
					return false
				}
				vlogFlushed = true
			}
			if sync && !db.relaxedSync {
				if err := db.syncValueLog(laneID); err != nil {
					db.reportError(fmt.Errorf("cachingdb: flush failed (vlog sync): %w", err))
					_ = backendBatch.Close()
					return false
				}
			}
		}

		var err error
		if backendPendingOps > 0 {
			db.backendWriteBatchesTotal.Add(1)
			if sync {
				err = backendBatch.WriteSync()
			} else {
				err = backendBatch.Write()
			}
		} else if sync && emittedChunk {
			// If we emitted intermediate chunks and happened to land exactly on a
			// chunk boundary, force a single durability boundary at the end.
			db.backendWriteBatchesTotal.Add(1)
			err = backendBatch.WriteSync()
		}
		cerr := backendBatch.Close()
		if err == nil {
			err = cerr
		}
		if err != nil {
			db.reportError(err)
			return false
		}

		db.mu.Lock()
		removeIDs := make(map[uint64]struct{}, len(ids))
		for _, id := range ids {
			removeIDs[id] = struct{}{}
		}
		db.removeQueuedUnitsLocked(removeIDs, units, totalBytes)

		deletable := make([]string, 0, len(units))
		if sync {
			inUse := make(map[string]struct{})
			for _, path := range db.currentWALPaths() {
				inUse[path] = struct{}{}
			}
			for _, paths := range db.queueWALPaths {
				for _, path := range paths {
					inUse[path] = struct{}{}
				}
			}
			seen := make(map[string]struct{})
			for _, unit := range units {
				for _, walPath := range unit.walPaths {
					if walPath == "" {
						continue
					}
					if _, ok := inUse[walPath]; ok {
						continue
					}
					if _, ok := seen[walPath]; ok {
						continue
					}
					if db.valueLogRetained(walPath) {
						continue
					}
					seen[walPath] = struct{}{}
					deletable = append(deletable, walPath)
				}
			}
		}
		db.mu.Unlock()

		removed := false
		for _, walPath := range deletable {
			db.dropValueLogSegment(walPath)
			if err := db.removeFileRetry(walPath); err != nil {
				continue
			}
			removed = true
			db.mu.Lock()
			db.untrackWALSegmentLocked(walPath)
			db.mu.Unlock()
			db.forgetValueLogRetain(walPath)
		}
		if removed {
			db.syncDirBestEffort(db.dir)
		}
		db.checkValueLogRetention()

		flushDur := time.Since(flushStart)
		if flushDur > 0 && totalBytes > 0 {
			sample := float64(totalBytes) / flushDur.Seconds()
			db.bpMu.Lock()
			if db.flushBpsEWMA <= 0 {
				db.flushBpsEWMA = sample
			} else {
				db.flushBpsEWMA = 0.9*db.flushBpsEWMA + 0.1*sample
			}
			db.bpCond.Broadcast()
			db.bpMu.Unlock()
		}
		return true
	}

	reserveChunkOps := totalLen
	if reserveChunkOps > backendEntriesCap {
		reserveChunkOps = backendEntriesCap
	}
	sizeHint := reserveChunkOps
	backendBatch := db.newBackendBatchWithSize(sizeHint)
	reserveBackendBatchOps(backendBatch, reserveChunkOps)
	flushStart := time.Now()
	vlogFlushed := false
	backendPendingOps := 0
	// When flushing a large combined batch, commit intermediate backend batches
	// to reduce peak allocator demand (and thus index.db high-watermark growth)
	// under small KeepRecent windows.
	chunkBackend := totalLen > backendEntriesCap

	// backendBatch := db.backend.NewBatch() // Original line, now replaced
	if db.deferredValueLogEnabled() {
		pendingOps, err := db.flushDeferredValueLogUnits(units, backendBatch, sync, laneID)
		if err != nil {
			db.reportError(err)
			_ = backendBatch.Close()
			return false
		}
		backendPendingOps = pendingOps
	} else {
		type (
			ptrSetter interface {
				SetPointer(key []byte, ptr page.ValuePtr) error
			}
			ptrSetterView interface {
				SetPointerView(key []byte, ptr page.ValuePtr) error
			}
		)
		psv, _ := backendBatch.(ptrSetterView)
		ps, _ := backendBatch.(ptrSetter)
		var single [1]batch.Entry
		fenceMode := db.outerLeafAnchorModeEnabled()
		collapseFence := fenceMode && db.allowFencePointerCollapse()
		var (
			lastFencePtr page.ValuePtr
			haveFencePtr bool
			prevFenceKey []byte
		)
		var promoter *fenceAnchorPromoter
		if collapseFence {
			promoter = newFenceAnchorPromoter(db, fenceMutationLookupForUnits(units))
			defer promoter.close()
		}

		// Best-effort: ensure value-log bytes are flushed before we start committing
		// pointers into the index when we expect to emit multiple backend commits.
		// This preserves the relative ordering while still allowing us to amortize
		// the durability boundary to the final commit when sync==true.
		if chunkBackend && db.valueLogEnabled() && !vlogFlushed {
			if err := db.flushValueLog(laneID); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (vlog): %w", err))
				_ = backendBatch.Close()
				return false
			}
			vlogFlushed = true
		}

		flushBackendChunk := func() error {
			if !chunkBackend || backendPendingOps < backendEntriesCap {
				return nil
			}

			db.backendWriteBatchesTotal.Add(1)
			// If sync==true, we only need a single durability boundary at the end of
			// the flush. Write the intermediate chunks without fsync to avoid
			// repeated pager sync work.
			err := backendBatch.Write()
			cerr := backendBatch.Close()
			if err == nil {
				err = cerr
			}
			if err != nil {
				return err
			}
			backendBatch = db.newBackendBatchWithSize(sizeHint)
			reserveBackendBatchOps(backendBatch, reserveChunkOps)
			psv, _ = backendBatch.(ptrSetterView)
			ps, _ = backendBatch.(ptrSetter)
			backendPendingOps = 0
			return nil
		}
		emitPromotionPointer := func(key []byte, ptr page.ValuePtr) error {
			var err error
			if psv != nil {
				err = psv.SetPointerView(key, ptr)
			} else if ps != nil {
				err = ps.SetPointer(key, ptr)
			} else {
				type ptrSetterLegacy interface {
					SetPointer(key []byte, ptr page.ValuePtr) error
				}
				if psl, ok := backendBatch.(ptrSetterLegacy); ok {
					err = psl.SetPointer(key, ptr)
				} else {
					single[0] = batch.Entry{Type: batch.OpPut, Key: key, ValuePtr: ptr, IsPtr: true}
					err = backendBatch.SetOps(single[:])
				}
			}
			if err != nil {
				return err
			}
			backendPendingOps++
			return flushBackendChunk()
		}
		emitPromotionDelete := func(key []byte) error {
			if err := backendBatch.Delete(key); err != nil {
				return err
			}
			backendPendingOps++
			return flushBackendChunk()
		}

		for _, unit := range units {
			iter := unit.mem.NewIterator(nil, nil)
			for iter.Valid() {
				key := iter.UnsafeKey()
				val, ptr, flags := iter.UnsafeEntry()
				if collapseFence && len(prevFenceKey) > 0 && bytes.Compare(key, prevFenceKey) <= 0 {
					// Unit-local iterators are sorted, but unit boundaries can rewind key order.
					// Fence collapse is only safe on ascending key runs.
					haveFencePtr = false
				}
				if flags&node.FlagTombstone != 0 {
					if promoter != nil {
						if err := promoter.maybePromoteAnchor(key, fencePendingMutation{kind: fencePendingMutationDelete}, emitPromotionPointer, emitPromotionDelete); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed (promote): %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					}
					if err := backendBatch.Delete(key); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed (delete): %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
					haveFencePtr = false
					backendPendingOps++
					if err := flushBackendChunk(); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				} else if flags&node.FlagPointer != 0 {
					if promoter != nil {
						if err := promoter.maybePromoteAnchor(key, fencePendingMutation{kind: fencePendingMutationSetPointer, ptr: ptr}, emitPromotionPointer, emitPromotionDelete); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed (promote): %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					}
					emitPtr := true
					if collapseFence {
						if haveFencePtr && valuePtrSameRecord(ptr, lastFencePtr) {
							emitPtr = false
						} else {
							lastFencePtr = ptr
							haveFencePtr = true
						}
					}
					if emitPtr {
						if psv != nil {
							if err := psv.SetPointerView(key, ptr); err != nil {
								db.reportError(fmt.Errorf("cachingdb: flush failed (set ptr): %w", err))
								_ = iter.Close()
								_ = backendBatch.Close()
								return false
							}
						} else if ps != nil {
							if err := ps.SetPointer(key, ptr); err != nil {
								db.reportError(fmt.Errorf("cachingdb: flush failed (set ptr): %w", err))
								_ = iter.Close()
								_ = backendBatch.Close()
								return false
							}
						} else {
							type ptrSetterLegacy interface {
								SetPointer(key []byte, ptr page.ValuePtr) error
							}
							if psl, ok := backendBatch.(ptrSetterLegacy); ok {
								if err := psl.SetPointer(key, ptr); err != nil {
									db.reportError(fmt.Errorf("cachingdb: flush failed (set ptr): %w", err))
									_ = iter.Close()
									_ = backendBatch.Close()
									return false
								}
							} else {
								single[0] = batch.Entry{Type: batch.OpPut, Key: key, ValuePtr: ptr, IsPtr: true}
								if err := backendBatch.SetOps(single[:]); err != nil {
									db.reportError(fmt.Errorf("cachingdb: flush failed (setops ptr): %w", err))
									_ = iter.Close()
									_ = backendBatch.Close()
									return false
								}
							}
						}
						backendPendingOps++
						if err := flushBackendChunk(); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					}
				} else {
					if promoter != nil {
						if err := promoter.maybePromoteAnchor(key, fencePendingMutation{kind: fencePendingMutationSetInline}, emitPromotionPointer, emitPromotionDelete); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed (promote): %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					}
					if err := backendBatch.Set(key, val); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed (set): %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
					haveFencePtr = false
					backendPendingOps++
					if err := flushBackendChunk(); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				}
				if collapseFence {
					prevFenceKey = append(prevFenceKey[:0], key...)
				}
				iter.Next()
			}
			if err := iter.Error(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (iter): %w", err))
				_ = iter.Close()
				_ = backendBatch.Close()
				return false
			}
			if err := iter.Close(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (iter close): %w", err))
				_ = backendBatch.Close()
				return false
			}
		}
	}

	if db.valueLogEnabled() {
		if !vlogFlushed {
			if err := db.flushValueLog(laneID); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (vlog): %w", err))
				_ = backendBatch.Close()
				return false
			}
			vlogFlushed = true
		}
		if sync && !db.relaxedSync {
			if err := db.syncValueLog(laneID); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (vlog sync): %w", err))
				_ = backendBatch.Close()
				return false
			}
		}
	}

	var err error
	if backendPendingOps > 0 {
		db.backendWriteBatchesTotal.Add(1)
		if sync {
			err = backendBatch.WriteSync()
		} else {
			err = backendBatch.Write()
		}
		cerr := backendBatch.Close()
		if err == nil {
			err = cerr
		}
		if err != nil {
			db.reportError(err)
			return false
		}
	} else {
		if err := backendBatch.Close(); err != nil {
			db.reportError(err)
			return false
		}
	}

	// Remove from queue and delete old WAL segments.
	db.mu.Lock()
	removeIDs := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		removeIDs[id] = struct{}{}
	}
	db.removeQueuedUnitsLocked(removeIDs, units, totalBytes)

	deletable := make([]string, 0, len(units))
	if sync {
		inUse := make(map[string]struct{})
		for _, path := range db.currentWALPaths() {
			inUse[path] = struct{}{}
		}
		for _, paths := range db.queueWALPaths {
			for _, path := range paths {
				inUse[path] = struct{}{}
			}
		}
		seen := make(map[string]struct{})
		for _, unit := range units {
			for _, walPath := range unit.walPaths {
				if walPath == "" {
					continue
				}
				if _, ok := inUse[walPath]; ok {
					continue
				}
				if _, ok := seen[walPath]; ok {
					continue
				}
				if db.valueLogRetained(walPath) {
					continue
				}
				seen[walPath] = struct{}{}
				deletable = append(deletable, walPath)
			}
		}
	}
	db.mu.Unlock()

	removed := false
	for _, walPath := range deletable {
		db.dropValueLogSegment(walPath)
		if err := db.removeFileRetry(walPath); err != nil {
			// Best effort cleanup
			continue
		}
		removed = true
		db.mu.Lock()
		db.untrackWALSegmentLocked(walPath)
		db.mu.Unlock()
		db.forgetValueLogRetain(walPath)
	}
	if removed {
		db.syncDirBestEffort(db.dir)
	}
	db.checkValueLogRetention()

	flushDur := time.Since(flushStart)
	if flushDur > 0 && totalBytes > 0 {
		sample := float64(totalBytes) / flushDur.Seconds()
		db.bpMu.Lock()
		if db.flushBpsEWMA <= 0 {
			db.flushBpsEWMA = sample
		} else {
			db.flushBpsEWMA = 0.9*db.flushBpsEWMA + 0.1*sample
		}
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
	}
	return true
}

func (db *DB) finalizeFlushStats(totalLen int, totalBytes int64, flushDur, durPreVlog, durBuild, durSet, durPostVlog, durPostVlogSync, durBackendWrite time.Duration) error {
	return nil
}

func (db *DB) flushOneLocked(sync bool) bool {
	db.mu.Lock()
	if len(db.queue) == 0 {
		db.mu.Unlock()
		return false
	}
	mem := db.queue[0]
	memBytes := mem.Size()
	memLen := mem.Len()
	memRange := keyRange{}
	if len(db.queueRanges) > 0 {
		memRange = db.queueRanges[0]
	}
	var walPaths []string
	if len(db.queueWALPaths) > 0 {
		walPaths = db.queueWALPaths[0]
	}
	laneID := 0
	if len(db.queueLaneIDs) > 0 {
		laneID = int(db.queueLaneIDs[0])
	}
	db.mu.Unlock()

	debugTiming := db.debugFlushTiming
	var (
		durPreVlogFlush time.Duration
		durBuildOps     time.Duration
		durSetOps       time.Duration
		durPostVlog     time.Duration
		durPostVlogSync time.Duration
		durBackendWrite time.Duration
	)

	// Optimization: Skip flush for empty memtables (e.g. from frequent Iterator creation)
	flushStart := time.Time{}
	flushed := false
	if memLen > 0 {
		flushStart = time.Now()

		// Flush 'mem' to backend
		backendEntriesCap := db.flushBackendEntriesCap(memLen, sync)
		reserveChunkOps := memLen
		if reserveChunkOps > backendEntriesCap {
			reserveChunkOps = backendEntriesCap
		}
		sizeHint := reserveChunkOps
		backendBatch := db.newBackendBatchWithSize(sizeHint)
		reserveBackendBatchOps(backendBatch, reserveChunkOps)
		vlogFlushed := false
		backendPendingOps := 0
		iter := mem.NewIterator(nil, nil) // Returns iterator.UnsafeIterator

		if db.deferredValueLogEnabled() {
			t0 := time.Now()
			var mutationLookup fenceMutationLookupFn
			if db.outerLeafAnchorModeEnabled() && db.allowFencePointerCollapse() {
				mutationLookup = fenceMutationLookupForMem(mem)
			}
			var err error
			backendPendingOps, err = db.flushDeferredValueLogMemtable(iter, backendBatch, memLen, sync, laneID, mutationLookup, nil)
			if err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (defer vlog): %w", err))
				_ = iter.Close()
				_ = backendBatch.Close()
				return false
			}
			if backendPendingOps <= 0 {
				db.reportError(fmt.Errorf("cachingdb: flush deferred produced no backend ops for memtable batch=%d", memLen))
				_ = iter.Close()
				_ = backendBatch.Close()
				return false
			}
			durBuildOps = time.Since(t0)
		} else {
			// When flushing very large memtables, avoid building an unbounded backend batch
			// (which can allocate / zero very large buffers and appear "hung").
			chunkBackend := memLen > backendEntriesCap

			// Best-effort: ensure value-log data is durable before committing pointers
			// to the backend. This keeps the relative durability ordering intact when
			// we later commit the backend in chunks.
			if chunkBackend && sync && db.valueLogEnabled() {
				t0 := time.Now()
				if err := db.flushValueLog(laneID); err != nil {
					db.reportError(fmt.Errorf("cachingdb: flush failed (vlog flush): %w", err))
					_ = iter.Close()
					_ = backendBatch.Close()
					return false
				}
				durPostVlog = time.Since(t0)
				vlogFlushed = true
				if sync && !db.relaxedSync {
					t1 := time.Now()
					if err := db.syncValueLog(laneID); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed (vlog sync): %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
					durPostVlogSync = time.Since(t1)
				}
			}

			t0 := time.Now()
			type (
				setViewer interface {
					SetView(key, value []byte) error
				}
				deleteViewer interface {
					DeleteView(key []byte) error
				}
				ptrSetter interface {
					SetPointer(key []byte, ptr page.ValuePtr) error
				}
				ptrSetterView interface {
					SetPointerView(key []byte, ptr page.ValuePtr) error
				}
			)
			sv, _ := backendBatch.(setViewer)
			dv, _ := backendBatch.(deleteViewer)
			psv, _ := backendBatch.(ptrSetterView)
			ps, _ := backendBatch.(ptrSetter)
			var single [1]batch.Entry
			fenceMode := db.outerLeafAnchorModeEnabled()
			collapseFence := fenceMode && db.allowFencePointerCollapse()
			var (
				lastFencePtr page.ValuePtr
				haveFencePtr bool
				prevFenceKey []byte
			)
			var promoter *fenceAnchorPromoter
			if collapseFence {
				promoter = newFenceAnchorPromoter(db, fenceMutationLookupForMem(mem))
				defer promoter.close()
			}

			flushBackendChunk := func() error {
				if !chunkBackend || backendPendingOps < backendEntriesCap {
					return nil
				}
				tw := time.Now()
				// If sync==true, we only need a single durability boundary at the end
				// of the flush. Write intermediate chunks without fsync to avoid
				// repeated pager sync work.
				db.backendWriteBatchesTotal.Add(1)
				err := backendBatch.Write()
				cerr := backendBatch.Close()
				if err == nil {
					err = cerr
				}
				durBackendWrite += time.Since(tw)
				if err != nil {
					return err
				}
				backendBatch = db.newBackendBatchWithSize(sizeHint)
				reserveBackendBatchOps(backendBatch, reserveChunkOps)
				sv, _ = backendBatch.(setViewer)
				dv, _ = backendBatch.(deleteViewer)
				psv, _ = backendBatch.(ptrSetterView)
				ps, _ = backendBatch.(ptrSetter)
				backendPendingOps = 0
				return nil
			}
			emitPromotionPointer := func(key []byte, ptr page.ValuePtr) error {
				var err error
				if psv != nil {
					err = psv.SetPointerView(key, ptr)
				} else if ps != nil {
					err = ps.SetPointer(key, ptr)
				} else {
					single[0] = batch.Entry{Type: batch.OpPut, Key: key, ValuePtr: ptr, IsPtr: true}
					err = backendBatch.SetOps(single[:])
				}
				if err != nil {
					return err
				}
				backendPendingOps++
				return flushBackendChunk()
			}
			emitPromotionDelete := func(key []byte) error {
				var err error
				if dv != nil {
					err = dv.DeleteView(key)
				} else {
					err = backendBatch.Delete(key)
				}
				if err != nil {
					return err
				}
				backendPendingOps++
				return flushBackendChunk()
			}

			for iter.Valid() {
				key := iter.UnsafeKey()
				val, ptr, flags := iter.UnsafeEntry()
				if collapseFence && len(prevFenceKey) > 0 && bytes.Compare(key, prevFenceKey) <= 0 {
					haveFencePtr = false
				}
				if flags&node.FlagTombstone != 0 {
					if promoter != nil {
						if err := promoter.maybePromoteAnchor(key, fencePendingMutation{kind: fencePendingMutationDelete}, emitPromotionPointer, emitPromotionDelete); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed (promote): %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					}
					var err error
					if dv != nil {
						err = dv.DeleteView(key)
					} else {
						err = backendBatch.Delete(key)
					}
					if err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed (delete): %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
					haveFencePtr = false
					backendPendingOps++
					if err := flushBackendChunk(); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				} else if flags&node.FlagPointer != 0 {
					if promoter != nil {
						if err := promoter.maybePromoteAnchor(key, fencePendingMutation{kind: fencePendingMutationSetPointer, ptr: ptr}, emitPromotionPointer, emitPromotionDelete); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed (promote): %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					}
					emitPtr := true
					if collapseFence {
						if haveFencePtr && valuePtrSameRecord(ptr, lastFencePtr) {
							emitPtr = false
						} else {
							lastFencePtr = ptr
							haveFencePtr = true
						}
					}
					if emitPtr {
						if psv != nil {
							if err := psv.SetPointerView(key, ptr); err != nil {
								db.reportError(fmt.Errorf("cachingdb: flush failed (set ptr): %w", err))
								_ = iter.Close()
								_ = backendBatch.Close()
								return false
							}
						} else if ps != nil {
							if err := ps.SetPointer(key, ptr); err != nil {
								db.reportError(fmt.Errorf("cachingdb: flush failed (set ptr): %w", err))
								_ = iter.Close()
								_ = backendBatch.Close()
								return false
							}
						} else {
							single[0] = batch.Entry{Type: batch.OpPut, Key: key, ValuePtr: ptr, IsPtr: true}
							if err := backendBatch.SetOps(single[:]); err != nil {
								db.reportError(fmt.Errorf("cachingdb: flush failed (setops ptr): %w", err))
								_ = iter.Close()
								_ = backendBatch.Close()
								return false
							}
						}
						backendPendingOps++
						if err := flushBackendChunk(); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					}
				} else {
					if promoter != nil {
						if err := promoter.maybePromoteAnchor(key, fencePendingMutation{kind: fencePendingMutationSetInline}, emitPromotionPointer, emitPromotionDelete); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed (promote): %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					}
					var err error
					if sv != nil {
						err = sv.SetView(key, val)
					} else {
						err = backendBatch.Set(key, val)
					}
					if err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed (set): %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
					haveFencePtr = false
					backendPendingOps++
					if err := flushBackendChunk(); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				}
				if collapseFence {
					prevFenceKey = append(prevFenceKey[:0], key...)
				}
				iter.Next()
			}
			if err := iter.Error(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (iter): %w", err))
				_ = iter.Close()
				_ = backendBatch.Close()
				return false
			}
			durBuildOps = time.Since(t0)
		}
		if err := iter.Close(); err != nil {
			db.reportError(fmt.Errorf("cachingdb: flush failed (iter close): %w", err))
			_ = backendBatch.Close()
			return false
		}
		// Commit to backend
		if db.valueLogEnabled() && !vlogFlushed {
			t0 := time.Now()
			if err := db.flushValueLog(laneID); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (vlog flush): %w", err))
				_ = backendBatch.Close()
				return false
			}
			durPostVlog = time.Since(t0)
			if sync && !db.relaxedSync {
				t1 := time.Now()
				if err := db.syncValueLog(laneID); err != nil {
					db.reportError(fmt.Errorf("cachingdb: flush failed (vlog sync): %w", err))
					_ = backendBatch.Close()
					return false
				}
				durPostVlogSync = time.Since(t1)
			}
		}
		if backendPendingOps > 0 {
			tw := time.Now()
			var err error
			if sync {
				err = backendBatch.WriteSync()
			} else {
				err = backendBatch.Write()
			}
			cerr := backendBatch.Close()
			if err == nil {
				err = cerr
			}
			durBackendWrite += time.Since(tw)
			if err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
				return false
			}
			backendBatch = nil
		} else {
			if err := backendBatch.Close(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (close): %w", err))
				return false
			}
		}
		flushed = true
	}
	flushDur := time.Duration(0)
	if flushed {
		flushDur = time.Since(flushStart)
	}
	if debugTiming && flushed {
		fmt.Fprintf(os.Stderr, "treedb: flush_timing combined=0 units=1 entries=%d bytes=%d pre_vlog=%s build=%s setops=%s post_vlog=%s post_vlog_sync=%s backend_write=%s total=%s\n",
			memLen,
			memBytes,
			durPreVlogFlush,
			durBuildOps,
			durSetOps,
			durPostVlog,
			durPostVlogSync,
			durBackendWrite,
			flushDur,
		)
	}

	// Remove from queue and delete old WAL
	db.mu.Lock()
	if memRange.valid {
		db.backendRange.add(memRange.min)
		db.backendRange.add(memRange.max)
	}
	if len(db.queue) > 0 {
		db.queueRetiredMemtableLocked(db.queue[0])
		db.queue = db.queue[1:]
	}
	if len(db.queueShardIDs) > 0 {
		db.queueShardIDs = db.queueShardIDs[1:]
	}
	if len(db.queueLaneIDs) > 0 {
		db.queueLaneIDs = db.queueLaneIDs[1:]
	}
	if len(db.queueIDs) > 0 {
		db.queueIDs = db.queueIDs[1:]
	}
	if len(db.queueEnqueueNS) > 0 {
		db.queueEnqueueNS = db.queueEnqueueNS[1:]
	}
	if len(db.queueRanges) > 0 {
		db.queueRanges = db.queueRanges[1:]
	}
	if len(db.queueWALPaths) > 0 {
		db.queueWALPaths = db.queueWALPaths[1:]
	}
	if len(db.queueValueLogPaths) > 0 {
		db.queueValueLogPaths = db.queueValueLogPaths[1:]
	}
	db.queueBacklogBytes.Add(-memBytes)
	if len(db.queue) == 0 {
		db.materializationLastDrainUnixNano.Store(time.Now().UnixNano())
	}
	db.publishMemtablesLocked()

	deletable := make([]string, 0, len(walPaths))
	if sync {
		inUse := make(map[string]struct{})
		for _, path := range db.currentWALPaths() {
			inUse[path] = struct{}{}
		}
		for _, paths := range db.queueWALPaths {
			for _, path := range paths {
				inUse[path] = struct{}{}
			}
		}
		seen := make(map[string]struct{})
		for _, walPath := range walPaths {
			if walPath == "" {
				continue
			}
			if _, ok := inUse[walPath]; ok {
				continue
			}
			if _, ok := seen[walPath]; ok {
				continue
			}
			if db.valueLogRetained(walPath) {
				continue
			}
			seen[walPath] = struct{}{}
			deletable = append(deletable, walPath)
		}
	}
	db.mu.Unlock()

	for _, walPath := range deletable {
		db.dropValueLogSegment(walPath)
		if err := db.removeFileRetry(walPath); err != nil {
			// Best effort cleanup
			continue
		}
		db.mu.Lock()
		db.untrackWALSegmentLocked(walPath)
		db.mu.Unlock()
		db.forgetValueLogRetain(walPath)
		db.syncDirBestEffort(db.dir)
	}
	db.checkValueLogRetention()

	if flushed && flushDur > 0 && memBytes > 0 {
		sample := float64(memBytes) / flushDur.Seconds()
		db.bpMu.Lock()
		if db.flushBpsEWMA <= 0 {
			db.flushBpsEWMA = sample
		} else {
			db.flushBpsEWMA = 0.9*db.flushBpsEWMA + 0.1*sample
		}
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
	} else {
		db.bpMu.Lock()
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
	}
	return true
}

// canBypassMemtableRead reports whether point-lookups can skip memtable probes
// and go directly to backend lookups for this key.
//
// Safety notes:
//   - We require an empty immutable queue.
//   - We require global mutable bytes to be zero.
//   - We additionally check the target mutable shard length to avoid races where
//     mutableBytes is transiently zero while an old view still has entries.
func (db *DB) canBypassMemtableRead(view *memtableView, key []byte) bool {
	if view == nil || len(view.queue) != 0 || db.mutableBytes.Load() != 0 {
		return false
	}
	if len(view.mutables) == 0 {
		return true
	}
	idx := db.shardIndex(key)
	if idx >= len(view.mutables) {
		return true
	}
	mt := view.mutables[idx]
	if mt == nil {
		return true
	}
	return mt.Len() == 0
}

// canBypassMemtableReadMany is the multi-key equivalent of
// canBypassMemtableRead. It only bypasses memtables when every touched mutable
// shard is observably empty.
func (db *DB) canBypassMemtableReadMany(view *memtableView, keys [][]byte) bool {
	if len(keys) == 0 {
		return true
	}
	if view == nil || len(view.queue) != 0 || db.mutableBytes.Load() != 0 {
		return false
	}
	n := len(view.mutables)
	if n == 0 {
		return true
	}
	// Fast path: common shard counts are small; use a stack bitset to avoid
	// per-call allocations in read-heavy GetMany paths.
	if n <= 64 {
		var checkedBits uint64
		for _, key := range keys {
			idx := db.shardIndex(key)
			if idx >= n {
				continue
			}
			bit := uint64(1) << uint(idx)
			if checkedBits&bit != 0 {
				continue
			}
			checkedBits |= bit
			mt := view.mutables[idx]
			if mt != nil && mt.Len() != 0 {
				return false
			}
		}
		return true
	}

	checked := make([]bool, n)
	for _, key := range keys {
		idx := db.shardIndex(key)
		if idx >= n || checked[idx] {
			continue
		}
		checked[idx] = true
		mt := view.mutables[idx]
		if mt != nil && mt.Len() != 0 {
			return false
		}
	}
	return true
}

func (db *DB) getMemtable(key []byte) ([]byte, bool, error) {
	view := db.retainMemtableView()
	if view != nil {
		defer db.releaseMemtableView(view)
	}
	var (
		mutables      []memtable.Table
		queue         []memtable.Table
		queueShardIDs []uint16
	)
	if view != nil {
		mutables = view.mutables
		queue = view.queue
		queueShardIDs = view.queueShardIDs
	} else {
		// Defensive fallback: should not happen after Open(), but keep safe
		// behavior for zero-value DBs and tests.
		db.mu.RLock()
		if len(db.mutableShards) > 0 {
			mutables = make([]memtable.Table, len(db.mutableShards))
			for i := range db.mutableShards {
				mutables[i] = db.mutableShards[i].mem
			}
		}
		queue = append([]memtable.Table(nil), db.queue...)
		queueShardIDs = append([]uint16(nil), db.queueShardIDs...)
		db.mu.RUnlock()
	}

	if db.canBypassMemtableRead(view, key) {
		return nil, false, nil
	}

	// check mutable
	if len(mutables) > 0 {
		idx := db.shardIndex(key)
		if idx < len(mutables) && mutables[idx] != nil {
			val, ptr, flags, found := mutables[idx].GetEntry(key)
			if found {
				if flags&node.FlagTombstone != 0 {
					return nil, true, nil
				}
				if flags&node.FlagPointer != 0 {
					if val == nil {
						readVal, err := db.readValueLog(key, ptr)
						if err != nil {
							return nil, true, err
						}
						return readVal, true, nil
					}
					return val, true, nil
				}
				if val == nil {
					return []byte{}, true, nil
				}
				return val, true, nil
			}
		}
	}

	// check queue backwards (newest first)
	shardIdx := 0
	if len(mutables) > 0 {
		shardIdx = db.shardIndex(key)
	}
	for i := len(queue) - 1; i >= 0; i-- {
		if len(queueShardIDs) > i && int(queueShardIDs[i]) != shardIdx {
			continue
		}
		val, ptr, flags, found := queue[i].GetEntry(key)
		if found {
			if flags&node.FlagTombstone != 0 {
				return nil, true, nil
			}
			if flags&node.FlagPointer != 0 {
				if val == nil {
					readVal, err := db.readValueLog(key, ptr)
					if err != nil {
						return nil, true, err
					}
					return readVal, true, nil
				}
				return val, true, nil
			}
			if val == nil {
				return []byte{}, true, nil
			}
			return val, true, nil
		}
	}
	return nil, false, nil
}

func (db *DB) getMemtableAppend(key, dst []byte) ([]byte, bool, error) {
	view := db.retainMemtableView()
	if view != nil {
		defer db.releaseMemtableView(view)
	}
	var (
		mutables      []memtable.Table
		queue         []memtable.Table
		queueShardIDs []uint16
	)
	if view != nil {
		mutables = view.mutables
		queue = view.queue
		queueShardIDs = view.queueShardIDs
	} else {
		db.mu.RLock()
		if len(db.mutableShards) > 0 {
			mutables = make([]memtable.Table, len(db.mutableShards))
			for i := range db.mutableShards {
				mutables[i] = db.mutableShards[i].mem
			}
		}
		queue = append([]memtable.Table(nil), db.queue...)
		queueShardIDs = append([]uint16(nil), db.queueShardIDs...)
		db.mu.RUnlock()
	}

	if db.canBypassMemtableRead(view, key) {
		return dst, false, nil
	}

	// check mutable
	if len(mutables) > 0 {
		idx := db.shardIndex(key)
		if idx < len(mutables) && mutables[idx] != nil {
			val, ptr, flags, found := mutables[idx].GetEntry(key)
			if found {
				if flags&node.FlagTombstone != 0 {
					return dst, true, tree.ErrKeyNotFound
				}
				if flags&node.FlagPointer != 0 {
					if val == nil {
						out, err := db.readValueLogAppend(key, ptr, dst)
						if err != nil {
							return dst, true, err
						}
						return out, true, nil
					}
					return append(dst, val...), true, nil
				}
				if val == nil {
					return dst, true, nil
				}
				return append(dst, val...), true, nil
			}
		}
	}

	// check queue backwards (newest first)
	shardIdx := 0
	if len(mutables) > 0 {
		shardIdx = db.shardIndex(key)
	}
	for i := len(queue) - 1; i >= 0; i-- {
		if len(queueShardIDs) > i && int(queueShardIDs[i]) != shardIdx {
			continue
		}
		val, ptr, flags, found := queue[i].GetEntry(key)
		if found {
			if flags&node.FlagTombstone != 0 {
				return dst, true, tree.ErrKeyNotFound
			}
			if flags&node.FlagPointer != 0 {
				if val == nil {
					out, err := db.readValueLogAppend(key, ptr, dst)
					if err != nil {
						return dst, true, err
					}
					return out, true, nil
				}
				return append(dst, val...), true, nil
			}
			if val == nil {
				return dst, true, nil
			}
			return append(dst, val...), true, nil
		}
	}
	return dst, false, nil
}

type backendManyGetter interface {
	GetMany(keys [][]byte) ([][]byte, error)
}

type backendManyPlanner interface {
	GetManyParallelPlan(keyCount int) (workers int, parallel bool)
}

// GetManyParallelPlan reports a safe upper-bound scheduling plan for GetMany.
//
// When cache hit-rates are high, backend calls may involve fewer keys than the
// provided key count. This method intentionally reports based on the input
// upper-bound to stay conservative for concurrency budgeting at callers.
func (db *DB) GetManyParallelPlan(keyCount int) (workers int, parallel bool) {
	if keyCount <= 0 {
		return 1, false
	}
	if planner, ok := db.backend.(backendManyPlanner); ok {
		return planner.GetManyParallelPlan(keyCount)
	}
	workers = runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > keyCount {
		workers = keyCount
	}
	return workers, workers > 1
}

func (db *DB) backendGetMany(keys [][]byte) ([][]byte, error) {
	if err := db.flushDeferredValueLogForBackendRead(); err != nil {
		return nil, err
	}
	if mg, ok := db.backend.(backendManyGetter); ok {
		return mg.GetMany(keys)
	}
	out := make([][]byte, len(keys))
	for i, key := range keys {
		val, err := db.backend.Get(key)
		if err != nil {
			return nil, err
		}
		out[i] = val
	}
	return out, nil
}

// GetUnsafe returns a safe copy of the value.
func (db *DB) GetUnsafe(key []byte) ([]byte, error) {
	return db.Get(key)
}

// Get returns a safe copy of the value.
func (db *DB) Get(key []byte) ([]byte, error) {
	view := db.retainMemtableView()
	bypass := db.canBypassMemtableRead(view, key)
	if view != nil {
		db.releaseMemtableView(view)
	}
	if bypass {
		if err := db.flushDeferredValueLogForBackendRead(); err != nil {
			return nil, err
		}
		return db.backend.Get(key)
	}
	val, found, err := db.getMemtable(key)
	if err != nil {
		return nil, err
	}
	if found {
		if val == nil {
			return nil, nil
		}
		cpy := make([]byte, len(val))
		copy(cpy, val)
		return cpy, nil
	}
	if err := db.flushDeferredValueLogForBackendRead(); err != nil {
		return nil, err
	}
	return db.backend.Get(key)
}

// GetMany returns safe copies of values for keys.
//
// Missing keys are returned as nil entries with no error.
func (db *DB) GetMany(keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return make([][]byte, 0), nil
	}

	// Fast path: no mutable/queued state and all touched mutable shards are
	// observably empty, so we can delegate to backend single-snapshot GetMany.
	view := db.retainMemtableView()
	bypass := db.canBypassMemtableReadMany(view, keys)
	if view != nil {
		db.releaseMemtableView(view)
	}
	if bypass {
		return db.backendGetMany(keys)
	}

	out := make([][]byte, len(keys))
	backendIdx := make([]int, 0, len(keys))
	backendKeys := make([][]byte, 0, len(keys))
	for i, key := range keys {
		val, found, err := db.getMemtable(key)
		if err != nil {
			return nil, err
		}
		if found {
			if val == nil {
				continue
			}
			cpy := make([]byte, len(val))
			copy(cpy, val)
			out[i] = cpy
			continue
		}
		backendIdx = append(backendIdx, i)
		backendKeys = append(backendKeys, key)
	}
	if len(backendKeys) == 0 {
		return out, nil
	}

	backendVals, err := db.backendGetMany(backendKeys)
	if err != nil {
		return nil, err
	}
	if len(backendVals) != len(backendKeys) {
		return nil, fmt.Errorf("cachingdb: backend GetMany returned %d values for %d keys", len(backendVals), len(backendKeys))
	}
	for i, outIdx := range backendIdx {
		out[outIdx] = backendVals[i]
	}
	return out, nil
}

// GetAppend appends the value for the key to dst and returns the new slice.
// If the key is not found, it returns dst and ErrKeyNotFound.
func (db *DB) GetAppend(key, dst []byte) ([]byte, error) {
	// 1. Memtable
	out, found, err := db.getMemtableAppend(key, dst)
	if err != nil {
		return dst, err
	}
	if found {
		return out, nil
	}

	// 2. Backend
	if err := db.flushDeferredValueLogForBackendRead(); err != nil {
		return dst, err
	}
	return db.backend.GetAppend(key, dst)
}

func (db *DB) Has(key []byte) (bool, error) {
	view := db.retainMemtableView()
	if view != nil {
		defer db.releaseMemtableView(view)
	}
	var (
		mutables      []memtable.Table
		queue         []memtable.Table
		queueShardIDs []uint16
	)
	if view != nil {
		mutables = view.mutables
		queue = view.queue
		queueShardIDs = view.queueShardIDs
	} else {
		db.mu.RLock()
		if len(db.mutableShards) > 0 {
			mutables = make([]memtable.Table, len(db.mutableShards))
			for i := range db.mutableShards {
				mutables[i] = db.mutableShards[i].mem
			}
		}
		queue = append([]memtable.Table(nil), db.queue...)
		queueShardIDs = append([]uint16(nil), db.queueShardIDs...)
		db.mu.RUnlock()
	}

	if len(mutables) > 0 {
		idx := db.shardIndex(key)
		if idx < len(mutables) && mutables[idx] != nil {
			_, deleted, found := mutables[idx].Get(key)
			if found {
				return !deleted, nil
			}
		}
	}

	idx := 0
	if len(mutables) > 0 {
		idx = db.shardIndex(key)
	}
	for i := len(queue) - 1; i >= 0; i-- {
		if len(queueShardIDs) > i && int(queueShardIDs[i]) != idx {
			continue
		}
		_, deleted, found := queue[i].Get(key)
		if found {
			return !deleted, nil
		}
	}

	return db.backend.Has(key)
}

func (db *DB) Stats() map[string]string {
	stats := db.backend.Stats()
	if stats == nil {
		stats = make(map[string]string)
	}
	db.mu.RLock()
	queueLen := len(db.queue)
	flushThreshold := db.flushThreshold
	memtableMode := db.memtableMode
	memtableAdaptive := db.memtableAdaptive
	memtableWarmupActive := db.memtableWarmupActive
	maxQueued := db.maxQueuedMemtables
	vlogAutotuneMode := db.valueLogAutotuneOptions.Mode
	oldestQueueEnqueueNS := int64(0)
	for i := range db.queueEnqueueNS {
		ts := db.queueEnqueueNS[i]
		if ts <= 0 {
			continue
		}
		if oldestQueueEnqueueNS == 0 || ts < oldestQueueEnqueueNS {
			oldestQueueEnqueueNS = ts
		}
	}
	db.mu.RUnlock()
	var walCurrentBytes int64
	var walClosedBytes int64
	var queueLagBuckets [vlogQueueLagBucketCount]uint64
	var queueLagCount uint64
	var queueLagTotalNs uint64
	var queueLagMaxNs uint64
	var queueDepthEnqueued uint64
	var queueDepthSamples uint64
	var queueDepthSum uint64
	var queueDepthMax uint64
	var queueDepthLast uint64
	var queueDepthPositiveRunMaxNs uint64
	var rawWritevSyscalls uint64
	var rawWritevBytes uint64
	var rawWritevIovecs uint64
	var rawWritevFlushes uint64
	var rawWriteSyscalls uint64
	var rawWriteBytes uint64
	var rawWriteCalls uint64
	for i := range db.lanes {
		l := &db.lanes[i]
		walCurrentBytes += l.walLiveBytes.Load()
		walClosedBytes += l.walClosedBytes.Load()

		lagSnap := snapshotLaneVlogQueueLag(l)
		depthSnap := snapshotLaneVlogQueueDepth(l)
		queueLagCount += lagSnap.Count
		queueLagTotalNs += lagSnap.TotalNs
		if lagSnap.MaxNs > queueLagMaxNs {
			queueLagMaxNs = lagSnap.MaxNs
		}
		for bucket := 0; bucket < vlogQueueLagBucketCount; bucket++ {
			queueLagBuckets[bucket] += lagSnap.Buckets[bucket]
		}
		queueDepthEnqueued += depthSnap.Enqueued
		queueDepthSamples += depthSnap.Samples
		queueDepthSum += depthSnap.Sum
		if depthSnap.Max > queueDepthMax {
			queueDepthMax = depthSnap.Max
		}
		queueDepthLast += depthSnap.Last
		if depthSnap.PositiveRunMaxNs > queueDepthPositiveRunMaxNs {
			queueDepthPositiveRunMaxNs = depthSnap.PositiveRunMaxNs
		}
		l.vlogMu.Lock()
		vlogWriter := l.vlog
		l.vlogMu.Unlock()
		if snapper, ok := any(vlogWriter).(interface {
			RawWritevStats() valuelog.RawWritevStats
		}); ok {
			snap := snapper.RawWritevStats()
			rawWritevSyscalls += snap.Syscalls
			rawWritevBytes += snap.Bytes
			rawWritevIovecs += snap.Iovecs
			rawWritevFlushes += snap.Flushes
		}
		if snapper, ok := any(vlogWriter).(interface {
			RawWriteStats() valuelog.RawWriteStats
		}); ok {
			snap := snapper.RawWriteStats()
			rawWriteSyscalls += snap.Syscalls
			rawWriteBytes += snap.Bytes
			rawWriteCalls += snap.Calls
		}

		laneLagP99 := estimateVlogQueueLagPercentile(lagSnap.Buckets, lagSnap.Count, 0.99)
		laneLagP999 := estimateVlogQueueLagPercentile(lagSnap.Buckets, lagSnap.Count, 0.999)
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.enqueued", i)] = fmt.Sprintf("%d", depthSnap.Enqueued)
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.depth_samples", i)] = fmt.Sprintf("%d", depthSnap.Samples)
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.depth_last", i)] = fmt.Sprintf("%d", depthSnap.Last)
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.depth_max", i)] = fmt.Sprintf("%d", depthSnap.Max)
		if depthSnap.Samples > 0 {
			stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.depth_avg", i)] = fmt.Sprintf("%.3f", float64(depthSnap.Sum)/float64(depthSnap.Samples))
		}
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.positive_drift_run_max_ms", i)] = fmt.Sprintf("%.3f", float64(depthSnap.PositiveRunMaxNs)/float64(time.Millisecond))
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.lag_samples", i)] = fmt.Sprintf("%d", lagSnap.Count)
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.lag_max_ms", i)] = fmt.Sprintf("%.3f", float64(lagSnap.MaxNs)/float64(time.Millisecond))
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.lag_p99_ms", i)] = fmt.Sprintf("%.3f", float64(laneLagP99)/float64(time.Millisecond))
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.lag_p999_ms", i)] = fmt.Sprintf("%.3f", float64(laneLagP999)/float64(time.Millisecond))
	}

	stats["treedb.cache.queue_len"] = fmt.Sprintf("%d", queueLen)
	stats["treedb.cache.mutable_bytes"] = fmt.Sprintf("%d", db.mutableBytes.Load())
	stats["treedb.cache.flush_threshold_bytes"] = fmt.Sprintf("%d", flushThreshold)
	stats["treedb.cache.memtable_mode"] = memtableMode.String()
	if memtableAdaptive {
		stats["treedb.cache.memtable_mode_config"] = "adaptive"
	} else {
		stats["treedb.cache.memtable_mode_config"] = "fixed"
	}
	memWrites := db.memtableStats.writes.Load()
	memSeqWrites := db.memtableStats.seqWrites.Load()
	memOverwriteWrites := db.memtableStats.overwriteWrites.Load()
	memIters := db.memtableStats.iterators.Load()
	memRangeIters := db.memtableStats.rangeIters.Load()
	memViewRetainTotal := db.memtableViewTelemetry.retainTotal.Load()
	memViewReleaseTotal := db.memtableViewTelemetry.releaseTotal.Load()
	memViewLeasesInFlight := db.memtableViewTelemetry.leasesInFlight.Load()
	memViewLeasesInFlightMax := db.memtableViewTelemetry.leasesInFlightMax.Load()
	memViewDeferredViewsCurrent := db.memtableViewTelemetry.deferredViewsCurrent.Load()
	memViewDeferredViewsMax := db.memtableViewTelemetry.deferredViewsMax.Load()
	memViewDeferredViewsTotal := db.memtableViewTelemetry.deferredViewsTotal.Load()
	memViewDeferredMemtablesCurrent := db.memtableViewTelemetry.deferredMemtablesCurrent.Load()
	memViewDeferredMemtablesMax := db.memtableViewTelemetry.deferredMemtablesMax.Load()
	memViewDeferredMemtablesTotal := db.memtableViewTelemetry.deferredMemtablesTotal.Load()
	memViewOldestDeferredUnixNano := db.memtableViewTelemetry.oldestDeferredUnixNano.Load()
	memViewOldestDeferredAgeMS := 0.0
	if memViewOldestDeferredUnixNano > 0 {
		if ageNS := time.Now().UnixNano() - memViewOldestDeferredUnixNano; ageNS > 0 {
			memViewOldestDeferredAgeMS = float64(ageNS) / float64(time.Millisecond)
		}
	}
	stats["treedb.cache.memtable_stats.writes"] = fmt.Sprintf("%d", memWrites)
	stats["treedb.cache.memtable_stats.seq_writes"] = fmt.Sprintf("%d", memSeqWrites)
	stats["treedb.cache.memtable_stats.overwrite_writes"] = fmt.Sprintf("%d", memOverwriteWrites)
	if memWrites > 0 {
		stats["treedb.cache.memtable_stats.seq_write_pct"] = fmt.Sprintf("%.4f", float64(memSeqWrites)/float64(memWrites))
		stats["treedb.cache.memtable_stats.overwrite_write_pct"] = fmt.Sprintf("%.4f", float64(memOverwriteWrites)/float64(memWrites))
	}
	stats["treedb.cache.memtable_stats.iterators"] = fmt.Sprintf("%d", memIters)
	stats["treedb.cache.memtable_stats.range_iterators"] = fmt.Sprintf("%d", memRangeIters)
	if memIters > 0 {
		stats["treedb.cache.memtable_stats.range_iter_pct"] = fmt.Sprintf("%.4f", float64(memRangeIters)/float64(memIters))
	}
	stats["treedb.cache.memtable_view.retain_total"] = fmt.Sprintf("%d", memViewRetainTotal)
	stats["treedb.cache.memtable_view.release_total"] = fmt.Sprintf("%d", memViewReleaseTotal)
	stats["treedb.cache.memtable_view.leases_inflight"] = fmt.Sprintf("%d", memViewLeasesInFlight)
	stats["treedb.cache.memtable_view.leases_inflight_max"] = fmt.Sprintf("%d", memViewLeasesInFlightMax)
	stats["treedb.cache.memtable_view.deferred_views_current"] = fmt.Sprintf("%d", memViewDeferredViewsCurrent)
	stats["treedb.cache.memtable_view.deferred_views_max"] = fmt.Sprintf("%d", memViewDeferredViewsMax)
	stats["treedb.cache.memtable_view.deferred_views_total"] = fmt.Sprintf("%d", memViewDeferredViewsTotal)
	stats["treedb.cache.memtable_view.deferred_memtables_current"] = fmt.Sprintf("%d", memViewDeferredMemtablesCurrent)
	stats["treedb.cache.memtable_view.deferred_memtables_max"] = fmt.Sprintf("%d", memViewDeferredMemtablesMax)
	stats["treedb.cache.memtable_view.deferred_memtables_total"] = fmt.Sprintf("%d", memViewDeferredMemtablesTotal)
	stats["treedb.cache.memtable_view.deferred_oldest_age_ms"] = fmt.Sprintf("%.3f", memViewOldestDeferredAgeMS)
	stats["treedb.cache.memtable_warmup_active"] = fmt.Sprintf("%t", memtableWarmupActive)
	stats["treedb.cache.max_queued_memtables"] = fmt.Sprintf("%d", maxQueued)
	db.domainIngressMu.Lock()
	ingressWorkers := len(db.domainIngressCh)
	ingressQueueSize := db.domainIngressQueueSize
	ingressDepth := 0
	for _, ch := range db.domainIngressCh {
		ingressDepth += len(ch)
	}
	db.domainIngressMu.Unlock()
	stats["treedb.cache.domain_ingress.enabled"] = fmt.Sprintf("%t", ingressWorkers > 0)
	stats["treedb.cache.domain_ingress.workers"] = fmt.Sprintf("%d", ingressWorkers)
	stats["treedb.cache.domain_ingress.queue_size"] = fmt.Sprintf("%d", ingressQueueSize)
	stats["treedb.cache.domain_ingress.queue_depth"] = fmt.Sprintf("%d", ingressDepth)
	stats["treedb.cache.domain_ingress.queue_depth_max"] = fmt.Sprintf("%d", db.domainIngressDepthMax.Load())
	stats["treedb.cache.domain_ingress.enqueued"] = fmt.Sprintf("%d", db.domainIngressEnqueued.Load())
	stats["treedb.cache.domain_ingress.processed"] = fmt.Sprintf("%d", db.domainIngressProcessed.Load())
	stats["treedb.cache.domain_ingress.fallback_direct"] = fmt.Sprintf("%d", db.domainIngressFallback.Load())
	stats["treedb.cache.wal_bytes_estimate"] = fmt.Sprintf("%d", walClosedBytes+walCurrentBytes)
	stats["treedb.cache.wal_closed_bytes_estimate"] = fmt.Sprintf("%d", walClosedBytes)
	stats["treedb.cache.wal_current_bytes_estimate"] = fmt.Sprintf("%d", walCurrentBytes)
	stats["treedb.cache.vlog_queue.enqueued_total"] = fmt.Sprintf("%d", queueDepthEnqueued)
	stats["treedb.cache.vlog_queue.depth_samples"] = fmt.Sprintf("%d", queueDepthSamples)
	stats["treedb.cache.vlog_queue.depth_last_sum"] = fmt.Sprintf("%d", queueDepthLast)
	stats["treedb.cache.vlog_queue.depth_max"] = fmt.Sprintf("%d", queueDepthMax)
	if queueDepthSamples > 0 {
		stats["treedb.cache.vlog_queue.depth_avg"] = fmt.Sprintf("%.3f", float64(queueDepthSum)/float64(queueDepthSamples))
	}
	stats["treedb.cache.vlog_queue.positive_drift_run_max_ms"] = fmt.Sprintf("%.3f", float64(queueDepthPositiveRunMaxNs)/float64(time.Millisecond))
	stats["treedb.cache.vlog_queue.lag_samples"] = fmt.Sprintf("%d", queueLagCount)
	stats["treedb.cache.vlog_queue.lag_max_ms"] = fmt.Sprintf("%.3f", float64(queueLagMaxNs)/float64(time.Millisecond))
	if queueLagCount > 0 {
		stats["treedb.cache.vlog_queue.lag_avg_ms"] = fmt.Sprintf("%.3f", (float64(queueLagTotalNs)/float64(queueLagCount))/float64(time.Millisecond))
		stats["treedb.cache.vlog_queue.lag_p50_ms"] = fmt.Sprintf("%.3f", float64(estimateVlogQueueLagPercentile(queueLagBuckets, queueLagCount, 0.50))/float64(time.Millisecond))
		stats["treedb.cache.vlog_queue.lag_p95_ms"] = fmt.Sprintf("%.3f", float64(estimateVlogQueueLagPercentile(queueLagBuckets, queueLagCount, 0.95))/float64(time.Millisecond))
		stats["treedb.cache.vlog_queue.lag_p99_ms"] = fmt.Sprintf("%.3f", float64(estimateVlogQueueLagPercentile(queueLagBuckets, queueLagCount, 0.99))/float64(time.Millisecond))
		stats["treedb.cache.vlog_queue.lag_p999_ms"] = fmt.Sprintf("%.3f", float64(estimateVlogQueueLagPercentile(queueLagBuckets, queueLagCount, 0.999))/float64(time.Millisecond))
	}
	for bucket := 0; bucket < vlogQueueLagBucketCount; bucket++ {
		upperUS := vlogQueueLagBucketUpperBounds[bucket].Microseconds()
		key := fmt.Sprintf("treedb.cache.vlog_queue.lag_bucket.le_us.%d", upperUS)
		stats[key] = fmt.Sprintf("%d", queueLagBuckets[bucket])
	}
	retained := db.valueLogRetainedStatsDetailed()
	vlogSegments, vlogBytes := retained.SegmentsTotal, retained.BytesTotal
	stats["treedb.cache.vlog_retained_segments"] = fmt.Sprintf("%d", vlogSegments)
	stats["treedb.cache.vlog_retained_bytes_estimate"] = fmt.Sprintf("%d", vlogBytes)
	stats["treedb.cache.vlog_generation.policy"] = fmt.Sprintf("%d", db.valueLogGenerationPolicy)
	stats["treedb.cache.vlog_generation.enabled"] = fmt.Sprintf("%t", db.valueLogGenerationPolicy != 0)
	stats["treedb.cache.vlog_generation.scheduler_state"] = vlogGenerationSchedulerStateString(db.vlogGenerationSchedulerState.Load())
	stats["treedb.cache.vlog_generation.scheduler_last_reason"] = vlogGenerationReasonString(db.vlogGenerationLastReason.Load())
	stats["treedb.cache.vlog_generation.churn_bytes_total"] = fmt.Sprintf("%d", db.vlogGenerationChurnBytes.Load())
	stats["treedb.cache.vlog_generation.churn_bytes_per_sec"] = fmt.Sprintf("%d", db.vlogGenerationLastChurnBps.Load())
	stats["treedb.cache.vlog_generation.hot.segment_target_bytes"] = fmt.Sprintf("%d", db.valueLogGenerationHotTarget)
	stats["treedb.cache.vlog_generation.warm.segment_target_bytes"] = fmt.Sprintf("%d", db.valueLogGenerationWarmTarget)
	stats["treedb.cache.vlog_generation.cold.segment_target_bytes"] = fmt.Sprintf("%d", db.valueLogGenerationColdTarget)
	stats["treedb.cache.vlog_generation.rewrite_budget.bytes_per_sec"] = fmt.Sprintf("%d", db.valueLogRewriteBudgetBytes)
	stats["treedb.cache.vlog_generation.rewrite_budget.records_per_sec"] = fmt.Sprintf("%d", db.valueLogRewriteBudgetRecords)
	stats["treedb.cache.vlog_generation.rewrite_trigger.stale_ratio_ppm"] = fmt.Sprintf("%d", db.valueLogRewriteTriggerRatioPPM)
	stats["treedb.cache.vlog_generation.rewrite_trigger.total_bytes"] = fmt.Sprintf("%d", db.valueLogRewriteTriggerBytes)
	stats["treedb.cache.vlog_generation.rewrite_trigger.churn_per_sec"] = fmt.Sprintf("%d", db.valueLogRewriteTriggerChurn)
	// PR1 scaffolding: legacy allocator still owns placement; report retained
	// totals under hot generation until generation-aware allocator lands.
	stats["treedb.cache.vlog_generation.bytes.live.total"] = fmt.Sprintf("%d", retained.BytesTotal)
	stats["treedb.cache.vlog_generation.bytes.live.hot"] = fmt.Sprintf("%d", retained.BytesHot)
	stats["treedb.cache.vlog_generation.bytes.live.warm"] = fmt.Sprintf("%d", retained.BytesWarm)
	stats["treedb.cache.vlog_generation.bytes.live.cold"] = fmt.Sprintf("%d", retained.BytesCold)
	stats["treedb.cache.vlog_generation.bytes.stale.total"] = "0"
	stats["treedb.cache.vlog_generation.bytes.stale.hot"] = "0"
	stats["treedb.cache.vlog_generation.bytes.stale.warm"] = "0"
	stats["treedb.cache.vlog_generation.bytes.stale.cold"] = "0"
	stats["treedb.cache.vlog_generation.bytes.total.total"] = fmt.Sprintf("%d", retained.BytesTotal)
	stats["treedb.cache.vlog_generation.bytes.total.hot"] = fmt.Sprintf("%d", retained.BytesHot)
	stats["treedb.cache.vlog_generation.bytes.total.warm"] = fmt.Sprintf("%d", retained.BytesWarm)
	stats["treedb.cache.vlog_generation.bytes.total.cold"] = fmt.Sprintf("%d", retained.BytesCold)
	stats["treedb.cache.vlog_generation.segments.total"] = fmt.Sprintf("%d", retained.SegmentsTotal)
	stats["treedb.cache.vlog_generation.segments.hot"] = fmt.Sprintf("%d", retained.SegmentsHot)
	stats["treedb.cache.vlog_generation.segments.warm"] = fmt.Sprintf("%d", retained.SegmentsWarm)
	stats["treedb.cache.vlog_generation.segments.cold"] = fmt.Sprintf("%d", retained.SegmentsCold)
	stats["treedb.cache.vlog_generation.rewrite.bytes_in"] = fmt.Sprintf("%d", db.vlogGenerationRewriteBytesIn.Load())
	stats["treedb.cache.vlog_generation.rewrite.bytes_out"] = fmt.Sprintf("%d", db.vlogGenerationRewriteBytesOut.Load())
	stats["treedb.cache.vlog_generation.rewrite.runs"] = fmt.Sprintf("%d", db.vlogGenerationRewriteRuns.Load())
	stats["treedb.cache.vlog_generation.gc.deleted_segments"] = fmt.Sprintf("%d", db.vlogGenerationGCSegmentsDeleted.Load())
	stats["treedb.cache.vlog_generation.gc.deleted_bytes"] = fmt.Sprintf("%d", db.vlogGenerationGCBytesDeleted.Load())
	stats["treedb.cache.vlog_generation.gc.runs"] = fmt.Sprintf("%d", db.vlogGenerationGCRuns.Load())
	stats["treedb.cache.vlog_generation.vacuum.runs"] = fmt.Sprintf("%d", db.vlogGenerationVacuumRuns.Load())
	stats["treedb.cache.vlog_generation.vacuum.failures"] = fmt.Sprintf("%d", db.vlogGenerationVacuumFailures.Load())
	stats["treedb.cache.vlog_generation.vacuum.last_unix_nano"] = fmt.Sprintf("%d", db.vlogGenerationLastVacuumUnixNano.Load())
	stats["treedb.cache.vlog_generation.remap.successes"] = fmt.Sprintf("%d", db.vlogGenerationRemapSuccesses.Load())
	stats["treedb.cache.vlog_generation.remap.failures"] = fmt.Sprintf("%d", db.vlogGenerationRemapFailures.Load())
	stats["treedb.cache.vlog_writev.syscalls"] = fmt.Sprintf("%d", rawWritevSyscalls)
	stats["treedb.cache.vlog_writev.bytes"] = fmt.Sprintf("%d", rawWritevBytes)
	stats["treedb.cache.vlog_writev.iovecs"] = fmt.Sprintf("%d", rawWritevIovecs)
	stats["treedb.cache.vlog_writev.flushes"] = fmt.Sprintf("%d", rawWritevFlushes)
	if rawWritevSyscalls > 0 {
		stats["treedb.cache.vlog_writev.bytes_per_syscall"] = fmt.Sprintf("%.1f", float64(rawWritevBytes)/float64(rawWritevSyscalls))
		stats["treedb.cache.vlog_writev.iovecs_per_syscall"] = fmt.Sprintf("%.2f", float64(rawWritevIovecs)/float64(rawWritevSyscalls))
	}
	if rawWritevFlushes > 0 {
		stats["treedb.cache.vlog_writev.bytes_per_flush"] = fmt.Sprintf("%.1f", float64(rawWritevBytes)/float64(rawWritevFlushes))
		stats["treedb.cache.vlog_writev.syscalls_per_flush"] = fmt.Sprintf("%.2f", float64(rawWritevSyscalls)/float64(rawWritevFlushes))
	}
	stats["treedb.cache.vlog_write.syscalls"] = fmt.Sprintf("%d", rawWriteSyscalls)
	stats["treedb.cache.vlog_write.bytes"] = fmt.Sprintf("%d", rawWriteBytes)
	stats["treedb.cache.vlog_write.calls"] = fmt.Sprintf("%d", rawWriteCalls)
	if rawWriteSyscalls > 0 {
		stats["treedb.cache.vlog_write.bytes_per_syscall"] = fmt.Sprintf("%.1f", float64(rawWriteBytes)/float64(rawWriteSyscalls))
	}
	if rawWriteCalls > 0 {
		stats["treedb.cache.vlog_write.bytes_per_call"] = fmt.Sprintf("%.1f", float64(rawWriteBytes)/float64(rawWriteCalls))
		stats["treedb.cache.vlog_write.syscalls_per_call"] = fmt.Sprintf("%.2f", float64(rawWriteSyscalls)/float64(rawWriteCalls))
	}
	totalVlogSyscalls := rawWritevSyscalls + rawWriteSyscalls
	totalVlogBytes := rawWritevBytes + rawWriteBytes
	stats["treedb.cache.vlog_io.syscalls"] = fmt.Sprintf("%d", totalVlogSyscalls)
	stats["treedb.cache.vlog_io.bytes"] = fmt.Sprintf("%d", totalVlogBytes)
	if totalVlogSyscalls > 0 {
		stats["treedb.cache.vlog_io.bytes_per_syscall"] = fmt.Sprintf("%.1f", float64(totalVlogBytes)/float64(totalVlogSyscalls))
	}
	fenceCandidates := db.deferredFenceCandidateKeys.Load()
	fenceGroups := db.deferredFenceEmittedGroups.Load()
	fenceEnqueuedKeys := db.deferredFenceEnqueuedKeys.Load()
	fenceEnqueuedBytes := db.deferredFenceEnqueuedBytes.Load()
	fenceMaterializedKeys := db.deferredFenceMaterializedKeys.Load()
	fenceMaterializedBytes := db.deferredFenceMaterializedBytes.Load()
	fencePendingKeys := uint64(0)
	if fenceEnqueuedKeys > fenceMaterializedKeys {
		fencePendingKeys = fenceEnqueuedKeys - fenceMaterializedKeys
	}
	fencePendingBytes := uint64(0)
	if fenceEnqueuedBytes > fenceMaterializedBytes {
		fencePendingBytes = fenceEnqueuedBytes - fenceMaterializedBytes
	}
	stats["treedb.cache.v2_fenceptr.wal_fence_mode"] = db.valueLogWALFenceMode
	stats["treedb.cache.v2_fenceptr.deferred_candidates"] = fmt.Sprintf("%d", fenceCandidates)
	stats["treedb.cache.v2_fenceptr.deferred_groups"] = fmt.Sprintf("%d", fenceGroups)
	stats["treedb.cache.v2_fenceptr.deferred_enqueued_keys"] = fmt.Sprintf("%d", fenceEnqueuedKeys)
	stats["treedb.cache.v2_fenceptr.deferred_enqueued_bytes"] = fmt.Sprintf("%d", fenceEnqueuedBytes)
	stats["treedb.cache.v2_fenceptr.deferred_materialized_keys"] = fmt.Sprintf("%d", fenceMaterializedKeys)
	stats["treedb.cache.v2_fenceptr.deferred_materialized_bytes"] = fmt.Sprintf("%d", fenceMaterializedBytes)
	stats["treedb.cache.v2_fenceptr.deferred_pending_keys_estimate"] = fmt.Sprintf("%d", fencePendingKeys)
	stats["treedb.cache.v2_fenceptr.deferred_pending_bytes_estimate"] = fmt.Sprintf("%d", fencePendingBytes)
	stats["treedb.cache.v2_fenceptr.assist_calls"] = fmt.Sprintf("%d", db.deferredFenceAssistCalls.Load())
	stats["treedb.cache.v2_fenceptr.assist_flushed_memtables"] = fmt.Sprintf("%d", db.deferredFenceAssistFlushedMemtables.Load())
	stats["treedb.cache.v2_fenceptr.assist_early_triggers"] = fmt.Sprintf("%d", db.deferredFenceAssistEarlyTriggers.Load())
	stats["treedb.cache.v1_leaflog_route.fallback_direct_attempts"] = fmt.Sprintf("%d", db.routeLeaflogFallbackDirectAttempts.Load())
	stats["treedb.cache.v1_leaflog_route.fallback_direct_rewrites"] = fmt.Sprintf("%d", db.routeLeaflogFallbackDirectRewrites.Load())
	if fenceGroups > 0 {
		stats["treedb.cache.v2_fenceptr.avg_keys_per_group"] = fmt.Sprintf("%.3f", float64(fenceCandidates)/float64(fenceGroups))
	}
	if db.adaptiveBackpressureEnabled() {
		stats["treedb.cache.backpressure_mode"] = "adaptive"
	} else {
		stats["treedb.cache.backpressure_mode"] = "queue_len"
	}
	now := time.Now()
	backlogBytes := db.queueBacklogBytes.Load()
	if backlogBytes <= 0 {
		db.materializationLastDrainUnixNano.Store(now.UnixNano())
	}
	stats["treedb.cache.queue_backlog_bytes"] = fmt.Sprintf("%d", backlogBytes)
	stats["treedb.cache.queue_laneid_misses"] = fmt.Sprintf("%d", db.queueLaneIDMisses.Load())
	stats["treedb.cache.stats.backend_write_batches_total"] = fmt.Sprintf("%d", db.backendWriteBatchesTotal.Load())
	watermarkLagDriftBps := db.observePublishWatermarkLagDrift(backlogBytes, now)
	stats["treedb.publish.watermark.lag_drift_bytes_per_sec"] = fmt.Sprintf("%.3f", watermarkLagDriftBps)
	if _, ok := stats["treedb.publish.watermark.lock_delay_share_pct"]; !ok {
		stats["treedb.publish.watermark.lock_delay_share_pct"] = "0.000"
	}
	if _, ok := stats["treedb.publish.watermark.latency_p99_ms"]; !ok {
		stats["treedb.publish.watermark.latency_p99_ms"] = "0.000"
	}
	materializationMarkNS := db.materializationLastDrainUnixNano.Load()
	if backlogBytes > 0 && oldestQueueEnqueueNS > 0 {
		materializationMarkNS = oldestQueueEnqueueNS
	}
	materializationLagAge := time.Duration(0)
	if materializationMarkNS > 0 {
		if ageNS := now.UnixNano() - materializationMarkNS; ageNS > 0 {
			materializationLagAge = time.Duration(ageNS)
		}
	}
	stats["treedb.cache.materialization.last_unix_nano"] = fmt.Sprintf("%d", materializationMarkNS)
	stats["treedb.cache.materialization.oldest_enqueue_unix_nano"] = fmt.Sprintf("%d", oldestQueueEnqueueNS)
	stats["treedb.cache.materialization.lag_age_ms"] = fmt.Sprintf("%.3f", float64(materializationLagAge)/float64(time.Millisecond))
	db.bpMu.Lock()
	stats["treedb.cache.flush_bps_ewma"] = fmt.Sprintf("%.0f", db.flushBpsEWMA)
	db.bpMu.Unlock()

	stats["treedb.cache.auto_checkpoint.count"] = fmt.Sprintf("%d", db.autoCheckpointCount.Load())
	stats["treedb.cache.auto_checkpoint.last_reason"] = autoCheckpointReasonString(db.autoCheckpointLastReason.Load())
	stats["treedb.cache.auto_checkpoint.last_duration_ms"] = fmt.Sprintf("%.3f", float64(db.autoCheckpointLastDurNanos.Load())/float64(time.Millisecond))
	stats["treedb.cache.auto_checkpoint.last_wal_bytes_before"] = fmt.Sprintf("%d", db.autoCheckpointLastWALBefore.Load())
	stats["treedb.cache.auto_checkpoint.last_wal_bytes_after"] = fmt.Sprintf("%d", db.autoCheckpointLastWALAfter.Load())
	stats["treedb.cache.auto_checkpoint.last_wal_reclaimable_before"] = fmt.Sprintf("%d", db.autoCheckpointLastWALReclaimableBefore.Load())
	stats["treedb.cache.auto_checkpoint.last_wal_reclaimable_after"] = fmt.Sprintf("%d", db.autoCheckpointLastWALReclaimableAfter.Load())
	stats["treedb.cache.auto_checkpoint.last_wal_bytes_trimmed"] = fmt.Sprintf("%d", db.autoCheckpointLastWALTrimmed.Load())
	stats["treedb.cache.auto_checkpoint.last_unix_nano"] = fmt.Sprintf("%d", db.autoCheckpointLastUnixNano.Load())
	cutoverSamples := db.checkpointCutoverSamples.Load()
	cutoverTotalNS := db.checkpointCutoverTotalNanos.Load()
	cutoverAvgMS := 0.0
	if cutoverSamples > 0 {
		cutoverAvgMS = (float64(cutoverTotalNS) / float64(cutoverSamples)) / float64(time.Millisecond)
	}
	stats["treedb.cache.checkpoint.cutover_samples"] = fmt.Sprintf("%d", cutoverSamples)
	stats["treedb.cache.checkpoint.cutover_last_ms"] = fmt.Sprintf("%.3f", float64(db.checkpointCutoverLastNanos.Load())/float64(time.Millisecond))
	stats["treedb.cache.checkpoint.cutover_max_ms"] = fmt.Sprintf("%.3f", float64(db.checkpointCutoverMaxNanos.Load())/float64(time.Millisecond))
	stats["treedb.cache.checkpoint.cutover_avg_ms"] = fmt.Sprintf("%.3f", cutoverAvgMS)
	stats["treedb.cache.checkpoint.cutover_last_unix_nano"] = fmt.Sprintf("%d", db.checkpointCutoverLastUnixNano.Load())

	vlogFramesTotal := db.valueLogDictFrames.total.Load()
	vlogFramesAttempted := db.valueLogDictFrames.attempted.Load()
	vlogFramesKept := db.valueLogDictFrames.kept.Load()
	stats["treedb.cache.vlog_dict.frames_total"] = fmt.Sprintf("%d", vlogFramesTotal)
	stats["treedb.cache.vlog_dict.frames_attempted"] = fmt.Sprintf("%d", vlogFramesAttempted)
	stats["treedb.cache.vlog_dict.frames_kept"] = fmt.Sprintf("%d", vlogFramesKept)
	if vlogFramesTotal > 0 {
		stats["treedb.cache.vlog_dict.attempted_frac"] = fmt.Sprintf("%.6f", float64(vlogFramesAttempted)/float64(vlogFramesTotal))
		stats["treedb.cache.vlog_dict.kept_frac"] = fmt.Sprintf("%.6f", float64(vlogFramesKept)/float64(vlogFramesTotal))
	}
	classifySampled := db.valueLogDictClassifySampled.Load()
	classifySkipped := db.valueLogDictClassifySkipped.Load()
	stats["treedb.cache.vlog_dict.classifier.sampled"] = fmt.Sprintf("%d", classifySampled)
	stats["treedb.cache.vlog_dict.classifier.skipped"] = fmt.Sprintf("%d", classifySkipped)
	if classifySampled > 0 {
		stats["treedb.cache.vlog_dict.classifier.skip_frac"] = fmt.Sprintf("%.6f", float64(classifySkipped)/float64(classifySampled))
	}
	if db.valueLogTemplateEngine != nil {
		for k, v := range db.valueLogTemplateEngine.StatsSnapshot() {
			stats["treedb.cache.vlog_template."+k] = v
		}
	}
	if db.valueLogReader != nil {
		hits, misses, entries, capacity := db.valueLogReader.TemplateDefCacheStats()
		stats["treedb.cache.vlog_template_def_cache.hits"] = fmt.Sprintf("%d", hits)
		stats["treedb.cache.vlog_template_def_cache.misses"] = fmt.Sprintf("%d", misses)
		stats["treedb.cache.vlog_template_def_cache.entries"] = fmt.Sprintf("%d", entries)
		stats["treedb.cache.vlog_template_def_cache.capacity"] = fmt.Sprintf("%d", capacity)
		if total := hits + misses; total > 0 {
			stats["treedb.cache.vlog_template_def_cache.hit_ratio"] = fmt.Sprintf("%.6f", float64(hits)/float64(total))
		}
	}
	stats["treedb.cache.vlog_dict.pause_remaining_bytes"] = fmt.Sprintf("%d", db.valueLogDictPauseRemaining.Load())
	stats["treedb.cache.vlog_dict.incompressible_hold_remaining_bytes"] = fmt.Sprintf("%d", db.valueLogDictIncompressibleHoldRemaining.Load())
	stats["treedb.cache.vlog_dict.incompressible_hits"] = fmt.Sprintf("%d", db.valueLogDictIncompressibleHits.Load())
	stats["treedb.cache.vlog_dict.incompressible_holds"] = fmt.Sprintf("%d", db.valueLogDictIncompressibleHolds.Load())
	stats["treedb.cache.vlog_dict.incompressible_bypass_bytes"] = fmt.Sprintf("%d", db.valueLogDictIncompressibleBypassBytes.Load())
	stats["treedb.cache.vlog_dict.last_applied_dict_id"] = fmt.Sprintf("%d", db.valueLogDictLastAppliedDictID.Load())
	stats["treedb.cache.vlog_dict.last_applied_dict_hash"] = fmt.Sprintf("%x", db.valueLogDictLastAppliedDictHash.Load())
	stats["treedb.cache.vlog_dict.last_publish_unix_nano"] = fmt.Sprintf("%d", db.valueLogDictLastPublishUnixNano.Load())
	stats["treedb.cache.vlog_dict.last_k_update_unix_nano"] = fmt.Sprintf("%d", db.valueLogDictLastKUpdateUnixNano.Load())
	stats["treedb.cache.vlog_dict.current_k"] = fmt.Sprintf("%d", db.valueLogDictCurrentK.Load())
	db.valueLogDictBytesMu.Lock()
	stats["treedb.cache.vlog_dict.cached_dict_id"] = fmt.Sprintf("%d", db.valueLogDictBytesID)
	stats["treedb.cache.vlog_dict.cached_dict_bytes"] = fmt.Sprintf("%d", len(db.valueLogDictBytes))
	db.valueLogDictBytesMu.Unlock()
	var blockKSnap vlogBlockKSnapshot
	var blockRatioWeighted [vlogBlockCodecCount]float64
	var blockRatioSamples [vlogBlockCodecCount]uint64
	for i := range db.lanes {
		kSnap := snapshotLaneVlogBlockK(&db.lanes[i])
		rSnap := snapshotLaneVlogBlockRatio(&db.lanes[i])
		for codecIdx := 0; codecIdx < vlogBlockCodecCount; codecIdx++ {
			blockKSnap.Count[codecIdx] += kSnap.Count[codecIdx]
			blockKSnap.Sum[codecIdx] += kSnap.Sum[codecIdx]
			if kSnap.Max[codecIdx] > blockKSnap.Max[codecIdx] {
				blockKSnap.Max[codecIdx] = kSnap.Max[codecIdx]
			}
			for bucket := 0; bucket < vlogBlockKBucketCount; bucket++ {
				blockKSnap.Buckets[codecIdx][bucket] += kSnap.Buckets[codecIdx][bucket]
			}
			samples := rSnap.Samples[codecIdx]
			if samples == 0 {
				continue
			}
			blockRatioSamples[codecIdx] += samples
			blockRatioWeighted[codecIdx] += rSnap.Ratio[codecIdx] * float64(samples)
		}
	}
	for codecIdx := 0; codecIdx < vlogBlockCodecCount; codecIdx++ {
		suffix := vlogBlockCodecSuffix(codecIdx)
		count := blockKSnap.Count[codecIdx]
		sum := blockKSnap.Sum[codecIdx]
		stats["treedb.cache.vlog_block.k.count."+suffix] = fmt.Sprintf("%d", count)
		stats["treedb.cache.vlog_block.k.max."+suffix] = fmt.Sprintf("%d", blockKSnap.Max[codecIdx])
		if count > 0 {
			stats["treedb.cache.vlog_block.k.avg."+suffix] = fmt.Sprintf("%.3f", float64(sum)/float64(count))
		}
		for bucket := 0; bucket < vlogBlockKBucketCount; bucket++ {
			key := fmt.Sprintf("treedb.cache.vlog_block.k.bucket.%s.le_%d", suffix, vlogBlockKBucketUpperBounds[bucket])
			stats[key] = fmt.Sprintf("%d", blockKSnap.Buckets[codecIdx][bucket])
		}
		if blockRatioSamples[codecIdx] > 0 {
			ratio := blockRatioWeighted[codecIdx] / float64(blockRatioSamples[codecIdx])
			stats["treedb.cache.vlog_block.ratio."+suffix] = fmt.Sprintf("%.6f", ratio)
		}
		stats["treedb.cache.vlog_block.ratio.samples."+suffix] = fmt.Sprintf("%d", blockRatioSamples[codecIdx])
	}
	if normalizeVlogCompressionMode(db.valueLogCompressionMode) == vlogCompressionAuto {
		var autoSnap vlogCompressionSelectorStats
		for i := range db.lanes {
			selector := db.lanes[i].vlogCompressionSelector
			if selector == nil {
				continue
			}
			snap := selector.snapshot()
			for c := 0; c < vlogAutoCandidateCount; c++ {
				autoSnap.bytesByCandidate[c] += snap.bytesByCandidate[c]
				autoSnap.framesByCandidate[c] += snap.framesByCandidate[c]
			}
			for from := 0; from < vlogAutoCandidateCount; from++ {
				for to := 0; to < vlogAutoCandidateCount; to++ {
					autoSnap.switches[from][to] += snap.switches[from][to]
				}
			}
			autoSnap.probeAttempts += snap.probeAttempts
			autoSnap.probeSuccesses += snap.probeSuccesses
			autoSnap.holdEnters += snap.holdEnters
			autoSnap.holdExits += snap.holdExits
			autoSnap.bypassBytes += snap.bypassBytes
		}
		var totalAutoFrames uint64
		for c := 0; c < vlogAutoCandidateCount; c++ {
			name := vlogAutoCandidate(c).suffix()
			bytes := autoSnap.bytesByCandidate[c]
			frames := autoSnap.framesByCandidate[c]
			totalAutoFrames += frames
			stats["treedb.cache.vlog_auto.bytes."+name] = fmt.Sprintf("%d", bytes)
			stats["treedb.cache.vlog_auto.frames."+name] = fmt.Sprintf("%d", frames)
		}
		if totalAutoFrames > 0 {
			for c := 0; c < vlogAutoCandidateCount; c++ {
				name := vlogAutoCandidate(c).suffix()
				stats["treedb.cache.vlog_auto.frames_frac."+name] = fmt.Sprintf("%.6f", float64(autoSnap.framesByCandidate[c])/float64(totalAutoFrames))
			}
		}
		stats["treedb.cache.vlog_auto.probe_attempts"] = fmt.Sprintf("%d", autoSnap.probeAttempts)
		stats["treedb.cache.vlog_auto.probe_successes"] = fmt.Sprintf("%d", autoSnap.probeSuccesses)
		if autoSnap.probeAttempts > 0 {
			stats["treedb.cache.vlog_auto.probe_success_frac"] = fmt.Sprintf("%.6f", float64(autoSnap.probeSuccesses)/float64(autoSnap.probeAttempts))
		}
		stats["treedb.cache.vlog_auto.hold_enters"] = fmt.Sprintf("%d", autoSnap.holdEnters)
		stats["treedb.cache.vlog_auto.hold_exits"] = fmt.Sprintf("%d", autoSnap.holdExits)
		stats["treedb.cache.vlog_auto.bypass_bytes"] = fmt.Sprintf("%d", autoSnap.bypassBytes)
		for from := 0; from < vlogAutoCandidateCount; from++ {
			for to := 0; to < vlogAutoCandidateCount; to++ {
				if from == to {
					continue
				}
				n := autoSnap.switches[from][to]
				if n == 0 {
					continue
				}
				key := fmt.Sprintf("treedb.cache.vlog_auto.switches.%s_to_%s", vlogAutoCandidate(from).suffix(), vlogAutoCandidate(to).suffix())
				stats[key] = fmt.Sprintf("%d", n)
			}
		}
	}
	db.valueLogDictTrainerMu.Lock()
	tr := db.valueLogDictTrainer
	db.valueLogDictTrainerMu.Unlock()
	if tr != nil {
		snap := tr.Stats()
		stats["treedb.cache.vlog_dict.trainer.profile_attempts"] = fmt.Sprintf("%d", snap.ProfileAttempts)
		stats["treedb.cache.vlog_dict.trainer.profile_accepts"] = fmt.Sprintf("%d", snap.ProfileAccepts)
		stats["treedb.cache.vlog_dict.trainer.profile_rejects"] = fmt.Sprintf("%d", snap.ProfileRejects)
		stats["treedb.cache.vlog_dict.trainer.profile_reject_reason"] = snap.ProfileRejectReason
		if !snap.LastAcceptTimestamp.IsZero() {
			stats["treedb.cache.vlog_dict.trainer.last_accept_unix_nano"] = fmt.Sprintf("%d", snap.LastAcceptTimestamp.UnixNano())
		} else {
			stats["treedb.cache.vlog_dict.trainer.last_accept_unix_nano"] = "0"
		}
	}
	switch vlogAutotuneMode {
	case valuelog.AutotuneOff:
		stats["treedb.cache.vlog_compression_autotune.mode"] = "off"
	case valuelog.AutotuneMedium:
		stats["treedb.cache.vlog_compression_autotune.mode"] = "medium"
	case valuelog.AutotuneAggressive:
		stats["treedb.cache.vlog_compression_autotune.mode"] = "aggressive"
	default:
		stats["treedb.cache.vlog_compression_autotune.mode"] = fmt.Sprintf("%d", vlogAutotuneMode)
	}
	if snap := db.valueLogAutotuneMetrics.snapshot(); snap.hasData() {
		stats["treedb.cache.vlog_autotune.encode_ns_per_raw_byte"] = fmt.Sprintf("%.3f", snap.EncodeNsPerRawByte)
		stats["treedb.cache.vlog_autotune.io_ns_per_stored_byte"] = fmt.Sprintf("%.3f", snap.IoNsPerStoredByte)
		stats["treedb.cache.vlog_autotune.throughput_raw_MBps"] = fmt.Sprintf("%.3f", snap.ThroughputRawMBps)
		stats["treedb.cache.vlog_autotune.observed_ratio"] = fmt.Sprintf("%.6f", snap.ObservedRatio)
	}
	return stats
}

// TriggerFlush schedules a background flush pass (best-effort).
func (db *DB) TriggerFlush() {
	select {
	case db.flushCh <- struct{}{}:
	default:
	}
}

// QueueBacklogBytes returns the current queued memtable backlog in bytes.
func (db *DB) QueueBacklogBytes() int64 {
	return db.queueBacklogBytes.Load()
}

// CompactionAssist performs bounded flush work when backpressure triggers. It is
// intended to be called by background maintenance (e.g. index compaction) so that
// flush debt does not grow unbounded in the absence of foreground writes.
func (db *DB) CompactionAssist() {
	// Ensure the background flusher is scheduled even if this call ends up doing
	// no synchronous work (e.g. due to low backlog).
	db.TriggerFlush()

	// Adaptive policy: thresholds based on queued backlog bytes.
	if db.adaptiveBackpressureEnabled() {
		db.bpMu.Lock()
		slowdownBytes, stopBytes, _ := db.thresholdsLocked()
		db.bpMu.Unlock()

		backlog := db.queueBacklogBytes.Load()
		if maxMemtables, ok := db.deferredAssistTargetMemtables(backlog, slowdownBytes, stopBytes); ok {
			flushed := db.flushSome(false, maxMemtables, db.writerFlushMaxDuration)
			db.recordDeferredAssist(flushed)
			return
		}
		if stopBytes > 0 && backlog >= stopBytes {
			_ = db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)
			return
		}
		if slowdownBytes > 0 && backlog > slowdownBytes {
			_ = db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)
			return
		}
		if early := db.deferredAssistEarlyTriggerBytes(slowdownBytes, stopBytes); early > 0 && backlog >= early {
			db.deferredFenceAssistEarlyTriggers.Add(1)
			flushed := db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)
			db.recordDeferredAssist(flushed)
			return
		}
		return
	}

	// Legacy policy: thresholds based on queue length.
	if db.maxQueuedMemtables >= 0 {
		db.mu.RLock()
		needs := len(db.queue) > db.maxQueuedMemtables
		db.mu.RUnlock()
		if needs {
			_ = db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)
		}
	}
}

func (db *DB) Print() error {
	return db.backend.Print()
}

// Drain flushes all currently buffered writes (mutable + queued memtables) to the
// backend. It is intended for maintenance operations that require a fully
// materialized backend state (e.g. index vacuum).
//
// Drain does not provide mutual exclusion against concurrent writers; callers
// should ensure no writes occur concurrently if they require a fully drained
// state.
func (db *DB) Drain() error {
	db.writeMu.Lock()
	db.mu.Lock()
	if db.mutableBytes.Load() > 0 {
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			db.writeMu.Unlock()
			return err
		}
	}
	db.mu.Unlock()
	db.writeMu.Unlock()

	db.flushAll(false)
	return nil
}

// Iterator implements DB.Iterator
func (db *DB) Iterator(start, end []byte) (merging.Iterator, error) {
	if err := db.ensureBackendRange(); err != nil {
		return nil, err
	}
	if err := db.flushDeferredValueLogForBackendRead(); err != nil {
		return nil, err
	}

	var view *memtableView
	db.mu.Lock()
	db.noteIterator(start, end)

	// Snapshot Isolation:
	// To ensure the iterator sees a consistent point-in-time view, we rotate the
	// mutable memtable into the immutable queue. The iterator then consumes
	// only the queue and the backend. Any subsequent writes will go to a new
	// mutable memtable which this iterator ignores.
	rotate := db.mutableBytes.Load() > 0
	if !rotate {
		for i := range db.mutableShards {
			mt := db.mutableShards[i].mem
			if mt != nil && mt.Len() != 0 {
				rotate = true
				break
			}
		}
	}
	if rotate {
		// Rotating is required for snapshot semantics, but allocating a large arena
		// for the *new* mutable memtable is often wasted (iterator-heavy paths may
		// not write concurrently). Use a small initial capacity and allow it to grow
		// if/when writes resume.
		if err := db.rotateMemtableLockedForIterator(minMemtablePrealloc); err != nil {
			db.mu.Unlock()
			return nil, err
		}
	}

	backendRangeKnown := db.backendRangeKnown
	backendRange := db.backendRange
	queueLenLocked := len(db.queue)
	if queueLenLocked > 0 {
		view = db.retainMemtableView()
	}

	db.mu.Unlock()

	// Backend-only fast path: with an empty immutable queue after rotation there
	// are no in-memory iterator sources to merge. Avoid memtable view retain/release
	// and construct a backend iterator directly.
	if queueLenLocked == 0 {
		decorate := func(it merging.Iterator, sourcesUsed int) merging.Iterator {
			if iteratorDebugEnabled.Load() {
				return &debugIterator{Iterator: it, queueLen: 0, sourcesUsed: sourcesUsed}
			}
			return it
		}
		if backendRangeKnown && !overlapsQuery(start, end, backendRange) {
			out := merging.Iterator(&emptyIterator{start: start, end: end})
			return decorate(out, 0), nil
		}
		if start == nil && end == nil && backendRangeKnown && backendRange.valid {
			diskIter, err := db.backend.Iterator(nil, nil)
			if err != nil {
				return nil, err
			}
			return decorate(diskIter, 1), nil
		}
		diskIter, err := db.backend.Iterator(start, end)
		if err != nil {
			return nil, err
		}
		return decorate(diskIter, 1), nil
	}

	releaseView := true
	defer func() {
		if releaseView && view != nil {
			db.releaseMemtableView(view)
		}
	}()
	var queue []memtable.Table
	var queueRanges []keyRange
	if view != nil {
		queue = view.queue
		queueRanges = view.queueRanges
	} else {
		// Defensive fallback: should not happen after Open(), but keeps Iterator safe
		// for zero-value DBs and tests.
		db.mu.RLock()
		queue = append([]memtable.Table(nil), db.queue...)
		queueRanges = append([]keyRange(nil), db.queueRanges...)
		db.mu.RUnlock()
	}
	queueLen := len(queue)
	hasMemSource := false
	decorateIterator := func(it merging.Iterator, sourcesUsed int) merging.Iterator {
		if iteratorDebugEnabled.Load() {
			it = &debugIterator{Iterator: it, queueLen: queueLen, sourcesUsed: sourcesUsed}
		}
		if hasMemSource && view != nil {
			leasedView := view
			view = nil
			releaseView = false
			return &leasedMergingIterator{
				Iterator: it,
				release: func() {
					db.releaseMemtableView(leasedView)
				},
			}
		}
		if view != nil {
			db.releaseMemtableView(view)
			view = nil
			releaseView = false
		}
		return it
	}

	// Fast path for full scans: if the in-memory key ranges are disjoint from the
	// backend key range, we can concatenate iterators instead of merging.
	if start == nil && end == nil {
		// Only do this when the queue is empty; queued memtables imply the backend
		// might not yet include older keys, making disjoint-range checks unreliable.
		if backendRangeKnown && len(queue) == 0 && backendRange.valid {
			diskIter, err := db.backend.Iterator(nil, nil)
			if err != nil {
				return nil, err
			}
			return decorateIterator(diskIter, 1), nil
		}
	}

	var sources []merging.IteratorSource

	// Priority 0..N: Queue (Newest first)
	// Note: We skip mutable shards because we just rotated them (so they're empty) or they were already empty.
	prio := 0
	for i := len(queue) - 1; i >= 0; i-- {
		if i < len(queueRanges) && !overlapsQuery(start, end, queueRanges[i]) {
			prio++
			continue
		}
		qIter := queue[i].NewIterator(start, end)
		if db.memtableValueLogPointers {
			qIter = newValueLogIterator(qIter, func(key []byte, ptr page.ValuePtr) ([]byte, error) {
				return db.readValueLog(key, ptr)
			})
		}
		sources = append(sources, merging.IteratorSource{
			Iter:     qIter,
			Priority: prio,
		})
		hasMemSource = true
		prio++
	}

	// Disk Iterator
	// Only skip if we definitively know the range and it doesn't overlap.
	if !backendRangeKnown || overlapsQuery(start, end, backendRange) {
		diskIter, err := db.backend.Iterator(start, end)
		if err != nil {
			for i := range sources {
				if sources[i].Iter != nil {
					_ = sources[i].Iter.Close()
				}
			}
			return nil, err
		}

		sources = append(sources, merging.IteratorSource{
			Iter:     diskIter,
			Priority: prio,
		})
	}

	if len(sources) == 0 {
		out := merging.Iterator(&emptyIterator{start: start, end: end})
		return decorateIterator(out, 0), nil
	}

	if len(sources) == 1 {
		out := newSingleSourceIterator(sources[0].Iter, start, end)
		return decorateIterator(out, 1), nil
	}

	out := merging.NewMergingIterator(sources, start, end)
	return decorateIterator(out, len(sources)), nil
}

type debugIterator struct {
	merging.Iterator
	queueLen    int
	sourcesUsed int
}

func (it *debugIterator) DebugStats() (queueLen int, sourcesUsed int) {
	return it.queueLen, it.sourcesUsed
}

type leasedMergingIterator struct {
	merging.Iterator
	closeOnce sync.Once
	closeErr  error
	release   func()
}

func (it *leasedMergingIterator) Close() error {
	it.closeOnce.Do(func() {
		it.closeErr = it.Iterator.Close()
		if it.release != nil {
			it.release()
		}
	})
	return it.closeErr
}

type concatUnsafeIterator struct {
	first  iterator.UnsafeIterator
	second iterator.UnsafeIterator

	cur        iterator.UnsafeIterator
	usingFirst bool
	valid      bool
	err        error
}

func newConcatUnsafeIterator(first, second iterator.UnsafeIterator) merging.Iterator {
	it := &concatUnsafeIterator{
		first:      first,
		second:     second,
		cur:        first,
		usingFirst: true,
	}
	it.advance()
	return it
}

func (it *concatUnsafeIterator) advance() {
	it.valid = false

	for {
		if it.cur == nil {
			return
		}

		// Switch to second iterator when first is exhausted.
		if !it.cur.Valid() {
			if it.usingFirst {
				it.cur = it.second
				it.usingFirst = false
				continue
			}
			return
		}

		if it.cur.IsDeleted() {
			it.cur.Next()
			continue
		}

		it.valid = true
		return
	}
}

func (it *concatUnsafeIterator) Next() {
	if !it.valid {
		panic("iterator invalid")
	}
	it.cur.Next()
	it.advance()
}

func (it *concatUnsafeIterator) Valid() bool { return it.valid }

func (it *concatUnsafeIterator) Key() []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.Key()
}

func (it *concatUnsafeIterator) Value() []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.Value()
}

func (it *concatUnsafeIterator) KeyCopy(dst []byte) []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.KeyCopy(dst)
}

func (it *concatUnsafeIterator) ValueCopy(dst []byte) []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.ValueCopy(dst)
}

func (it *concatUnsafeIterator) Close() error {
	var firstErr error
	if it.first != nil {
		if err := it.first.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if it.second != nil {
		if err := it.second.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (it *concatUnsafeIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	if it.first != nil && it.first.Error() != nil {
		return it.first.Error()
	}
	if it.second != nil && it.second.Error() != nil {
		return it.second.Error()
	}
	return nil
}

func (it *concatUnsafeIterator) Domain() (start, end []byte) { return nil, nil }

func (db *DB) ReverseIterator(start, end []byte) (merging.Iterator, error) {
	if err := db.flushDeferredValueLogForBackendRead(); err != nil {
		return nil, err
	}

	var view *memtableView
	db.mu.Lock()
	db.noteIterator(start, end)

	// Snapshot Isolation:
	// Mirror Iterator() semantics: rotate mutable memtables into the immutable
	// queue so the reverse iterator sees a stable point-in-time view (queue +
	// backend). Subsequent writes land in a new mutable memtable and are ignored.
	rotate := db.mutableBytes.Load() > 0
	if !rotate {
		for i := range db.mutableShards {
			mt := db.mutableShards[i].mem
			if mt != nil && mt.Len() != 0 {
				rotate = true
				break
			}
		}
	}
	if rotate {
		if err := db.rotateMemtableLockedForIterator(minMemtablePrealloc); err != nil {
			db.mu.Unlock()
			return nil, err
		}
	}

	queueLenLocked := len(db.queue)
	if queueLenLocked > 0 {
		view = db.retainMemtableView()
	}

	db.mu.Unlock()

	if err := db.ensureBackendRange(); err != nil {
		return nil, err
	}
	db.mu.RLock()
	backendRangeKnown := db.backendRangeKnown
	backendRange := db.backendRange
	db.mu.RUnlock()

	// Backend-only fast path.
	if queueLenLocked == 0 {
		decorate := func(it merging.Iterator, sourcesUsed int) merging.Iterator {
			if iteratorDebugEnabled.Load() {
				return &debugIterator{Iterator: it, queueLen: 0, sourcesUsed: sourcesUsed}
			}
			return it
		}
		if backendRangeKnown && !overlapsQuery(start, end, backendRange) {
			out := merging.Iterator(&emptyIterator{start: start, end: end})
			return decorate(out, 0), nil
		}
		if start == nil && end == nil && backendRangeKnown && backendRange.valid {
			diskIter, err := db.backend.ReverseIterator(nil, nil)
			if err != nil {
				return nil, err
			}
			return decorate(diskIter, 1), nil
		}
		diskIter, err := db.backend.ReverseIterator(start, end)
		if err != nil {
			return nil, err
		}
		return decorate(diskIter, 1), nil
	}

	releaseView := true
	defer func() {
		if releaseView && view != nil {
			db.releaseMemtableView(view)
		}
	}()
	var queue []memtable.Table
	var queueRanges []keyRange
	if view != nil {
		queue = view.queue
		queueRanges = view.queueRanges
	} else {
		// Defensive fallback: should not happen after Open().
		db.mu.RLock()
		queue = append([]memtable.Table(nil), db.queue...)
		queueRanges = append([]keyRange(nil), db.queueRanges...)
		db.mu.RUnlock()
	}
	queueLen := len(queue)
	hasMemSource := false
	decorateIterator := func(it merging.Iterator, sourcesUsed int) merging.Iterator {
		if iteratorDebugEnabled.Load() {
			it = &debugIterator{Iterator: it, queueLen: queueLen, sourcesUsed: sourcesUsed}
		}
		if hasMemSource && view != nil {
			leasedView := view
			view = nil
			releaseView = false
			return &leasedMergingIterator{
				Iterator: it,
				release: func() {
					db.releaseMemtableView(leasedView)
				},
			}
		}
		if view != nil {
			db.releaseMemtableView(view)
			view = nil
			releaseView = false
		}
		return it
	}

	var sources []merging.IteratorSource

	// Priority 0..N: Queue (Newest first)
	prio := 0
	for i := len(queue) - 1; i >= 0; i-- {
		if i < len(queueRanges) && !overlapsQuery(start, end, queueRanges[i]) {
			prio++
			continue
		}
		qIter := queue[i].NewReverseIterator(start, end)
		if db.memtableValueLogPointers {
			qIter = newValueLogIterator(qIter, func(key []byte, ptr page.ValuePtr) ([]byte, error) {
				return db.readValueLog(key, ptr)
			})
		}
		sources = append(sources, merging.IteratorSource{
			Iter:     qIter,
			Priority: prio,
		})
		hasMemSource = true
		prio++
	}

	// Disk Iterator
	if !backendRangeKnown || overlapsQuery(start, end, backendRange) {
		diskIter, err := db.backend.ReverseIterator(start, end)
		if err != nil {
			for i := range sources {
				if sources[i].Iter != nil {
					_ = sources[i].Iter.Close()
				}
			}
			return nil, err
		}

		sources = append(sources, merging.IteratorSource{
			Iter:     diskIter,
			Priority: prio,
		})
	}

	if len(sources) == 0 {
		out := merging.Iterator(&emptyIterator{start: start, end: end})
		return decorateIterator(out, 0), nil
	}

	if len(sources) == 1 {
		out := newSingleSourceIterator(sources[0].Iter, start, end)
		return decorateIterator(out, 1), nil
	}

	out := merging.NewReverseMergingIterator(sources, start, end)
	return decorateIterator(out, len(sources)), nil
}

// NewBatch implementation for CachingDB
// batchOp removed, using batch.Entry directly

type Batch struct {
	db              *DB
	entries         []batch.Entry
	backend         batch.Interface
	size            int
	copyArena       []byte
	copyArenaChunks [][]byte
	copyArenaCap    int
	copyBytes       int
	walBuf          []logRecord
	shardIdxs       []int
	eligibleIdxs    []int
	shardAdds       []int64
	shardCnts       []int
	shardEntries    [][]batch.Entry
	shardIdxSets    [][]int
	maxEntries      int

	closed         bool
	streamEligible bool
	streamTried    bool
	firstKey       []byte
	lastKey        []byte
	batchRange     keyRange
	dictID         uint64
	dictIDValid    bool
	dictBytes      []byte
	dictBytesValid bool
}

func (db *DB) NewBatch() *Batch {
	capHint := batchDefaultEntriesCap
	if db != nil {
		capHint = db.batchEntriesCapHint(capHint)
	}
	return &Batch{
		db:             db,
		entries:        db.getBatchEntries(capHint),
		copyArenaCap:   db.batchCopyArenaInitCap(0),
		streamEligible: true,
	}
}

func (db *DB) NewBatchWithSize(size int) *Batch {
	if size < 0 {
		size = 0
	}
	return &Batch{
		db:             db,
		entries:        db.getBatchEntries(size),
		copyArenaCap:   db.batchCopyArenaInitCap(size),
		streamEligible: true,
	}
}

func (db *DB) batchEntriesCapHint(minCap int) int {
	if minCap < batchDefaultEntriesCap {
		minCap = batchDefaultEntriesCap
	}
	if db == nil {
		return minCap
	}
	hint := int(db.batchEntryHint.Load())
	if hint > minCap {
		minCap = hint
	}
	if minCap > batchHintEntriesMax {
		minCap = batchHintEntriesMax
	}
	return minCap
}

func (db *DB) observeBatchEntries(n int) {
	if db == nil || n <= 0 {
		return
	}
	if n < batchDefaultEntriesCap {
		n = batchDefaultEntriesCap
	}
	if n > batchHintEntriesMax {
		n = batchHintEntriesMax
	}
	for {
		old := int(db.batchEntryHint.Load())
		next := n
		if old > 0 {
			// EWMA keeps the hint adaptive: rises quickly for larger batches and
			// decays after sustained smaller batches.
			next = (old*7 + n + 3) / 8
		}
		if next < batchDefaultEntriesCap {
			next = batchDefaultEntriesCap
		}
		if next > batchHintEntriesMax {
			next = batchHintEntriesMax
		}
		if next == old {
			return
		}
		if db.batchEntryHint.CompareAndSwap(int32(old), int32(next)) {
			return
		}
	}
}

func (db *DB) batchCopyArenaInitCap(sizeHint int) int {
	base := batchCopyArenaInitCapForEntries(sizeHint)
	if db == nil {
		return base
	}
	if hint := int(db.batchCopyBytesHint.Load()); hint > base {
		base = hint
	}
	if base < batchCopyArenaMinChunk {
		base = batchCopyArenaMinChunk
	}
	if base > batchCopyArenaInitMax {
		base = batchCopyArenaInitMax
	}
	return base
}

func (db *DB) observeBatchCopyBytes(n int) {
	if db == nil || n <= 0 {
		return
	}
	if n < batchCopyArenaMinChunk {
		n = batchCopyArenaMinChunk
	}
	if n > batchCopyArenaInitMax {
		n = batchCopyArenaInitMax
	}
	for {
		old := int(db.batchCopyBytesHint.Load())
		next := n
		if old > 0 {
			if n > old {
				// Increase quickly so large batches avoid repeated growth.
				next = (old*3 + n + 1) / 4
			} else {
				// Decay quickly so high-water marks do not poison small batches.
				next = (old + n + 1) / 2
			}
		}
		if next < batchCopyArenaMinChunk {
			next = batchCopyArenaMinChunk
		}
		if next > batchCopyArenaInitMax {
			next = batchCopyArenaInitMax
		}
		if next == old {
			return
		}
		if db.batchCopyBytesHint.CompareAndSwap(int32(old), int32(next)) {
			return
		}
	}
}

func (db *DB) getBatchEntries(minCap int) []batch.Entry {
	if minCap < 0 {
		minCap = 0
	}
	if db != nil {
		if pooled := db.batchEntriesPool.Get(); pooled != nil {
			switch v := pooled.(type) {
			case *batchEntrySliceRef:
				entries := v.entries
				putBatchEntrySliceRef(v)
				if cap(entries) >= minCap {
					return entries[:0]
				}
				if c := cap(entries); c > 0 && c <= batchEntriesPoolMaxRetain {
					db.batchEntriesPool.Put(getBatchEntrySliceRef(entries[:0]))
				}
			case []batch.Entry:
				// Backward-compatible fallback for any legacy pooled shape.
				if cap(v) >= minCap {
					return v[:0]
				}
				if c := cap(v); c > 0 && c <= batchEntriesPoolMaxRetain {
					db.batchEntriesPool.Put(getBatchEntrySliceRef(v[:0]))
				}
			}
		}
	}
	return make([]batch.Entry, 0, minCap)
}

func (db *DB) putBatchEntries(entries []batch.Entry) {
	if db == nil || cap(entries) == 0 {
		return
	}
	if cap(entries) > batchEntriesPoolMaxRetain {
		return
	}
	full := entries[:cap(entries)]
	clear(full)
	db.batchEntriesPool.Put(getBatchEntrySliceRef(full[:0]))
}

func (db *DB) getBatchShardEntries(minCap int) []batch.Entry {
	if minCap < 0 {
		minCap = 0
	}
	if db != nil {
		if pooled := db.batchShardEntriesPool.Get(); pooled != nil {
			switch v := pooled.(type) {
			case *batchEntrySliceRef:
				entries := v.entries
				putBatchEntrySliceRef(v)
				if cap(entries) >= minCap {
					return entries[:0]
				}
			case []batch.Entry:
				// Backward-compatible fallback for any legacy pooled shape.
				if cap(v) >= minCap {
					return v[:0]
				}
			}
		}
	}
	return make([]batch.Entry, 0, minCap)
}

func (db *DB) putBatchShardEntries(entries []batch.Entry) {
	if db == nil || cap(entries) == 0 {
		return
	}
	if cap(entries) > batchEntriesPoolMaxRetain {
		return
	}
	full := entries[:cap(entries)]
	clear(full)
	db.batchShardEntriesPool.Put(getBatchEntrySliceRef(full[:0]))
}

func (db *DB) getBatchIntSlice(minCap int) []int {
	if minCap < 0 {
		minCap = 0
	}
	if db != nil {
		if pooled := db.batchIntPool.Get(); pooled != nil {
			if idxs, ok := pooled.([]int); ok {
				if cap(idxs) >= minCap {
					return idxs[:0]
				}
				if c := cap(idxs); c > 0 && c <= batchIntSlicePoolMaxRetain {
					db.batchIntPool.Put(idxs[:0])
				}
			}
		}
	}
	return make([]int, 0, minCap)
}

func (db *DB) putBatchIntSlice(idxs []int) {
	if db == nil || cap(idxs) == 0 {
		return
	}
	if cap(idxs) > batchIntSlicePoolMaxRetain {
		return
	}
	db.batchIntPool.Put(idxs[:0])
}

// Reset clears the batch for reuse without closing it.
//
// This intentionally keeps internal buffers to avoid per-batch allocations in
// callers that frequently reset (e.g. geth benchmarks).
func (b *Batch) Reset() {
	if b == nil {
		return
	}
	if b.backend != nil {
		_ = b.backend.Close()
		b.backend = nil
	}
	if b.entries != nil {
		b.entries = b.entries[:0]
	}
	if b.shardIdxs != nil {
		b.shardIdxs = b.shardIdxs[:0]
	}
	if b.shardAdds != nil {
		b.shardAdds = b.shardAdds[:0]
	}
	if b.eligibleIdxs != nil {
		b.eligibleIdxs = b.eligibleIdxs[:0]
	}
	if b.shardCnts != nil {
		b.shardCnts = b.shardCnts[:0]
	}
	if b.shardEntries != nil {
		b.shardEntries = b.shardEntries[:0]
	}
	if b.shardIdxSets != nil {
		b.shardIdxSets = b.shardIdxSets[:0]
	}
	b.recycleCopyArenaChunks()
	if b.db != nil {
		b.copyArenaCap = b.db.batchCopyArenaInitCap(0)
	}
	b.size = 0
	b.walBuf = b.walBuf[:0]
	b.streamEligible = true
	b.streamTried = false
	b.firstKey = nil
	b.lastKey = nil
	b.batchRange = keyRange{}
	b.dictID = 0
	b.dictIDValid = false
	b.dictBytes = nil
	b.dictBytesValid = false
	b.maxEntries = 0
}

func (b *Batch) noteEntryAppend() {
	if b == nil {
		return
	}
	if n := len(b.entries); n > b.maxEntries {
		b.maxEntries = n
	}
}

func (b *Batch) updateBatchEntryHint() {
	if b == nil || b.db == nil {
		return
	}
	n := b.maxEntries
	if n <= 0 {
		n = len(b.entries)
	}
	if n > 0 {
		b.db.observeBatchEntries(n)
	}
}

func (b *Batch) drainCopyArenaChunks() [][]byte {
	if b == nil {
		return nil
	}
	chunks := b.copyArenaChunks
	b.copyArenaChunks = nil
	b.copyArena = nil
	b.copyBytes = 0
	return chunks
}

func (b *Batch) recycleCopyArenaChunks() {
	if b == nil {
		return
	}
	chunks := b.drainCopyArenaChunks()
	if len(chunks) == 0 {
		return
	}
	putBatchArenas(chunks)
}

func (b *Batch) updateBatchCopyHint() {
	if b == nil || b.db == nil {
		return
	}
	if n := b.copyBytes; n > 0 {
		b.db.observeBatchCopyBytes(n)
	}
}

func (b *Batch) arenaCopy(n int) []byte {
	if n == 0 {
		return nil
	}
	if cap(b.copyArena)-len(b.copyArena) < n {
		chunkCap := cap(b.copyArena) * 2
		if chunkCap < batchCopyArenaMinChunk {
			chunkCap = batchCopyArenaMinChunk
		}
		if cap(b.copyArena) == 0 {
			// Keep unsized batches conservative. Entry-slice capacity can come from
			// pooled high-water marks and is not a reliable signal for copy payload.
			if b.copyArenaCap > chunkCap {
				chunkCap = b.copyArenaCap
			}
		}
		if chunkCap < n {
			chunkCap = n
		}
		// Switch to a fresh chunk when exhausted so existing entry slices keep
		// their backing arrays without per-op allocations.
		chunk := getBatchArena(chunkCap)
		b.copyArena = chunk[:0]
		b.copyArenaChunks = append(b.copyArenaChunks, b.copyArena)
	}
	start := len(b.copyArena)
	b.copyArena = b.copyArena[:start+n]
	b.copyBytes += n
	return b.copyArena[start : start+n : start+n]
}

func (b *Batch) cloneKey(key []byte) []byte {
	dst := b.arenaCopy(len(key))
	copy(dst, key)
	return dst
}

func (b *Batch) cloneValue(value []byte) []byte {
	dst := b.arenaCopy(len(value))
	copy(dst, value)
	return dst
}

func (b *Batch) cloneKeyValue(key, value []byte) ([]byte, []byte) {
	buf := b.arenaCopy(len(key) + len(value))
	keyCopy := buf[:len(key):len(key)]
	copy(keyCopy, key)
	valCopy := buf[len(key):]
	copy(valCopy, value)
	return keyCopy, valCopy
}

func (b *Batch) Set(key, value []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}

	keyCopy, valCopy := b.cloneKeyValue(key, value)
	if b.backend != nil {
		b.batchRange.add(keyCopy)
		b.size += len(keyCopy) + len(valCopy)
		// Use the backend's view method with owned copies to avoid aliasing.
		if sv, ok := b.backend.(interface{ SetView(key, value []byte) error }); ok {
			return sv.SetView(keyCopy, valCopy)
		}
		return b.backend.Set(keyCopy, valCopy)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = keyCopy
			b.lastKey = keyCopy
		} else {
			if bytes.Compare(keyCopy, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = keyCopy
		}
	}
	// We don't know about value-log thresholds here, so we just store inline.
	// The backend will handle promotion to the value log if needed during
	// writeBypass, or standard write will handle it via the journal/memtable.
	b.entries = append(b.entries, batch.Entry{
		Type:  batch.OpPut,
		Key:   keyCopy,
		Value: valCopy,
	})
	b.noteEntryAppend()
	b.size += len(keyCopy) + len(valCopy)

	b.maybeSwitchToStreaming()
	return nil
}

// SetView records a Put without copying key/value bytes. Callers must treat
// key/value as immutable until the batch is written or closed.
func (b *Batch) SetView(key, value []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}

	if b.backend != nil {
		b.batchRange.add(key)
		b.size += len(key) + len(value)
		if sv, ok := b.backend.(interface{ SetView(key, value []byte) error }); ok {
			return sv.SetView(key, value)
		}
		return b.backend.Set(key, value)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = key
			b.lastKey = key
		} else {
			if bytes.Compare(key, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = key
		}
	}
	b.entries = append(b.entries, batch.Entry{
		Type:  batch.OpPut,
		Key:   key,
		Value: value,
	})
	b.noteEntryAppend()
	b.size += len(key) + len(value)

	b.maybeSwitchToStreaming()
	return nil
}

func (b *Batch) Delete(key []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}

	keyCopy := b.cloneKey(key)
	if b.backend != nil {
		b.batchRange.add(keyCopy)
		b.size += len(keyCopy)
		if dv, ok := b.backend.(interface{ DeleteView(key []byte) error }); ok {
			return dv.DeleteView(keyCopy)
		}
		return b.backend.Delete(keyCopy)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = keyCopy
			b.lastKey = keyCopy
		} else {
			if bytes.Compare(keyCopy, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = keyCopy
		}
	}
	b.entries = append(b.entries, batch.Entry{
		Type: batch.OpDelete,
		Key:  keyCopy,
	})
	b.noteEntryAppend()
	b.size += len(keyCopy)

	b.maybeSwitchToStreaming()
	return nil
}

// DeleteView records a Delete without copying key bytes. Callers must treat
// key as immutable until the batch is written or closed.
func (b *Batch) DeleteView(key []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}

	if b.backend != nil {
		b.batchRange.add(key)
		b.size += len(key)
		if dv, ok := b.backend.(interface{ DeleteView(key []byte) error }); ok {
			return dv.DeleteView(key)
		}
		return b.backend.Delete(key)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = key
			b.lastKey = key
		} else {
			if bytes.Compare(key, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = key
		}
	}
	b.entries = append(b.entries, batch.Entry{
		Type: batch.OpDelete,
		Key:  key,
	})
	b.noteEntryAppend()
	b.size += len(key)

	b.maybeSwitchToStreaming()
	return nil
}

func (b *Batch) SetOps(ops []batch.Entry) error {
	if b.closed {
		return ErrBatchClosed
	}
	if b.backend != nil {
		copied := make([]batch.Entry, len(ops))
		for i, op := range ops {
			copiedOp := op
			copiedOp.Key = b.cloneKey(op.Key)
			if op.Value != nil {
				copiedOp.Value = b.cloneValue(op.Value)
			}
			copied[i] = copiedOp
			b.size += len(copiedOp.Key) + len(copiedOp.Value)
			b.batchRange.add(copiedOp.Key)
		}
		return b.backend.SetOps(copied)
	}
	for _, op := range ops {
		copied := op
		if op.Value != nil {
			copied.Key, copied.Value = b.cloneKeyValue(op.Key, op.Value)
		} else {
			copied.Key = b.cloneKey(op.Key)
			copied.Value = nil
		}
		if b.streamEligible {
			if b.firstKey == nil {
				b.firstKey = copied.Key
				b.lastKey = copied.Key
			} else {
				if bytes.Compare(copied.Key, b.lastKey) <= 0 {
					b.streamEligible = false
				}
				b.lastKey = copied.Key
			}
		}
		b.entries = append(b.entries, copied)
		b.noteEntryAppend()
		b.size += len(copied.Key) + len(copied.Value)
	}
	return nil
}

const (
	batchDefaultEntriesCap = 16
	// Keep the adaptive NewBatch reserve bounded so one huge batch cannot
	// permanently over-provision typical batch allocations.
	batchHintEntriesMax    = 8192
	streamSwitchMinEntries = 4096
	streamSwitchMinBytes   = 1 << 20 // 1MiB
	// Only fan out value-log appends across multiple lanes when a batch is large
	// enough to amortize per-lane setup and goroutine overhead.
	multiLaneValueLogMinRecords = 1024
	batchCopyArenaMinChunk      = 4 << 10
	batchCopyArenaUnsizedInit   = 8 << 10
	batchCopyArenaBytesPerEntry = 192
	batchCopyArenaInitMax       = 2 << 20
	batchCopyArenaMaxRetain     = 4 << 20
	batchEntriesPoolMaxRetain   = 16 << 10
	batchIntSlicePoolMaxRetain  = 16 << 10
)

func batchCopyArenaInitCapForEntries(entries int) int {
	if entries <= 0 {
		return batchCopyArenaUnsizedInit
	}
	if entries > batchCopyArenaInitMax/batchCopyArenaBytesPerEntry {
		return batchCopyArenaInitMax
	}
	capHint := entries * batchCopyArenaBytesPerEntry
	if capHint < batchCopyArenaMinChunk {
		return batchCopyArenaMinChunk
	}
	if capHint > batchCopyArenaInitMax {
		return batchCopyArenaInitMax
	}
	return capHint
}

func (b *Batch) maybeSwitchToStreaming() {
	if b.streamTried || !b.streamEligible || b.backend != nil {
		return
	}
	// Streaming writes directly to the backend batch and therefore bypasses the
	// journal/value-log orchestration. Only enable it when the journal is
	// disabled and value-log pointers are disabled.
	if !b.db.disableJournal || b.db.valueLogEnabled() {
		return
	}
	// Switch to streaming (direct-to-backend batch) once a batch is "big enough"
	// that keeping all entries in memory provides little benefit.
	//
	// Why this exists:
	// - The cached/memtable path is great for small/random batches because it
	//   aggregates updates and reduces backend write amplification.
	// - For large strictly-increasing batches that start beyond the max key in
	//   the in-memory layers, we don't benefit from memtable aggregation; we just
	//   pay extra overhead storing the batch entries slice until Write().
	//
	// We intentionally use a small threshold (rather than flushThreshold) so
	// "BatchWrite1M" style workloads can switch early and avoid materializing the
	// entire batch in memory before Write().
	// Require both entry-count and byte-size thresholds to avoid switching tiny-value
	// batches to backend streaming solely due to key count.
	if len(b.entries) < streamSwitchMinEntries || b.size < streamSwitchMinBytes {
		return
	}

	// Only attempt streaming if the batch is strictly increasing and starts beyond
	// the maximum key present in the in-memory layers.
	b.db.mu.RLock()
	queueRanges := append([]keyRange(nil), b.db.queueRanges...)
	b.db.mu.RUnlock()

	var maxKey []byte
	mutableRange := b.db.snapshotMutableRange()
	if mutableRange.valid {
		maxKey = mutableRange.max
	}
	for _, r := range queueRanges {
		if !r.valid {
			continue
		}
		if maxKey == nil || bytes.Compare(r.max, maxKey) > 0 {
			maxKey = r.max
		}
	}

	b.streamTried = true
	if maxKey != nil && bytes.Compare(b.firstKey, maxKey) <= 0 {
		return
	}

	backendBatch := b.db.backend.NewBatch()
	if b.firstKey != nil && b.lastKey != nil {
		// Streaming is strictly increasing and starts beyond the max key in memory,
		// so the batch range is simply [first,last]. Keep copies for backend range
		// tracking after commit.
		b.batchRange.valid = true
		b.batchRange.min = append([]byte(nil), b.firstKey...)
		b.batchRange.max = append([]byte(nil), b.lastKey...)
	}
	if err := backendBatch.SetOps(b.entries); err != nil {
		_ = backendBatch.Close()
		return
	}
	b.backend = backendBatch
	b.entries = b.entries[:0]
}

func (b *Batch) Write() error {
	return b.write(false)
}

func (b *Batch) WriteSync() error {
	return b.write(true)
}

func (b *Batch) freezeDictID(ctx context.Context) error {
	if b.dictIDValid {
		return nil
	}
	if b.db == nil {
		return nil
	}
	dictID, err := b.db.currentDictID(ctx)
	if err != nil {
		return err
	}
	b.dictID = dictID
	b.dictIDValid = true
	return nil
}

func (b *Batch) ensureDictBytes(ctx context.Context) ([]byte, error) {
	if b.dictBytesValid {
		return b.dictBytes, nil
	}
	if b.db == nil {
		return nil, nil
	}
	dictBytes, err := b.db.dictBytes(ctx, b.dictID)
	if err != nil {
		return nil, err
	}
	b.dictBytes = dictBytes
	b.dictBytesValid = true
	return dictBytes, nil
}

func (b *Batch) write(sync bool) error {
	if b.closed {
		return ErrBatchClosed
	}
	b.db.waitForCheckpoint()

	if b.backend != nil {
		var err error
		if sync && !b.db.relaxedSync {
			b.db.flushMu.Lock()
			err = b.backend.WriteSync()
			b.db.flushMu.Unlock()
		} else {
			b.db.flushMu.Lock()
			err = b.backend.Write()
			b.db.flushMu.Unlock()
		}
		if cerr := b.backend.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err == nil && b.batchRange.valid {
			b.db.mu.Lock()
			b.db.backendRange.add(b.batchRange.min)
			b.db.backendRange.add(b.batchRange.max)
			b.db.mu.Unlock()
		}
		b.backend = nil
		if err == nil && b.size > 0 {
			b.db.noteWrite()
		}
		b.updateBatchEntryHint()
		b.updateBatchCopyHint()
		b.Reset()
		return err
	}

	if ok, err := b.tryWriteWALOffStreamBypass(sync); err != nil {
		return err
	} else if ok {
		return nil
	}

	// Optimization: Bypass for Large Batches
	// Generalization: Only bypass if the batch is large enough to be comparable
	// to a memtable flush. Small/Medium random batches cause high write amplification
	// if written directly to the COW backend.
	if b.size >= int(b.db.flushThreshold) {
		return b.writeBypass(sync)
	}
	return b.writeRegular(sync)
}

func (b *Batch) tryWriteWALOffStreamBypass(sync bool) (bool, error) {
	if b == nil || b.db == nil {
		return false, nil
	}
	if !b.db.deferredValueLogEnabled() {
		return false, nil
	}
	if b.db.outerLeafAnchorModeEnabled() {
		// Anchor modes need flush-time fence collapse semantics; direct
		// stream bypass emits backend pointer ops at commit boundaries and can
		// regress immediate read/prefix-scan behavior.
		return false, nil
	}
	if b.backend != nil || !b.streamEligible {
		return false, nil
	}
	if b.firstKey == nil || b.lastKey == nil {
		return false, nil
	}
	// Mirror maybeSwitchToStreaming: bypass only when both dimensions are large.
	if len(b.entries) < streamSwitchMinEntries || b.size < streamSwitchMinBytes {
		return false, nil
	}

	// Only attempt streaming if the batch is strictly increasing and starts beyond
	// the maximum key present in the in-memory layers.
	b.db.mu.RLock()
	queueRanges := append([]keyRange(nil), b.db.queueRanges...)
	queueLen := len(b.db.queue)
	b.db.mu.RUnlock()
	if queueLen > 0 && len(queueRanges) == 0 {
		// Cannot reason about overlap without queue range tracking.
		return false, nil
	}

	var maxKey []byte
	mutableRange := b.db.snapshotMutableRange()
	if mutableRange.valid {
		maxKey = mutableRange.max
	}
	for _, r := range queueRanges {
		if !r.valid {
			continue
		}
		if maxKey == nil || bytes.Compare(r.max, maxKey) > 0 {
			maxKey = r.max
		}
	}
	if maxKey != nil && bytes.Compare(b.firstKey, maxKey) <= 0 {
		return false, nil
	}

	// WAL-off streaming bypass: append large values to the value log and commit
	// backend pointers directly, avoiding memtable ingestion costs for append-only
	// workloads.
	ops := getEntrySlice(len(b.entries))
	ops = append(ops, b.entries...)
	defer putEntrySlice(ops)

	ops, err := b.db.deferValueLogOps(ops, sync && !b.db.relaxedSync)
	if err != nil {
		return false, err
	}

	backendBatch := b.db.backend.NewBatch()
	if err := backendBatch.SetOps(ops); err != nil {
		_ = backendBatch.Close()
		return false, err
	}

	if sync && !b.db.relaxedSync {
		b.db.flushMu.Lock()
		err = backendBatch.WriteSync()
		b.db.flushMu.Unlock()
	} else {
		b.db.flushMu.Lock()
		err = backendBatch.Write()
		b.db.flushMu.Unlock()
	}
	if cerr := backendBatch.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		return false, err
	}

	b.db.mu.Lock()
	b.db.backendRange.add(b.firstKey)
	b.db.backendRange.add(b.lastKey)
	b.db.mu.Unlock()

	if b.size > 0 {
		b.db.noteWrite()
	}
	b.updateBatchEntryHint()
	b.updateBatchCopyHint()
	b.Reset()
	return true, nil
}

func (b *Batch) writeRegular(syncWrite bool) error {
	b.db.writeMu.RLock()
	needRotate := false
	needSyncBarrier := false

	// 1. Memtable capacity pre-check
	shardCount := len(b.db.mutableShards)
	shardAdds := b.shardAdds
	if cap(shardAdds) < shardCount {
		shardAdds = make([]int64, shardCount)
	} else {
		shardAdds = shardAdds[:shardCount]
		clear(shardAdds)
	}
	b.shardAdds = shardAdds

	shardCounts := b.shardCnts
	if cap(shardCounts) < shardCount {
		shardCounts = make([]int, shardCount)
	} else {
		shardCounts = shardCounts[:shardCount]
		clear(shardCounts)
	}
	b.shardCnts = shardCounts

	shardIdxs := b.shardIdxs
	if cap(shardIdxs) < len(b.entries) {
		shardIdxs = b.db.getBatchIntSlice(len(b.entries))
		shardIdxs = shardIdxs[:len(b.entries)]
	} else {
		shardIdxs = shardIdxs[:len(b.entries)]
	}
	b.shardIdxs = shardIdxs
	allDeletes := true
	for i := range b.entries {
		op := &b.entries[i]
		if op.Type != batch.OpDelete {
			allDeletes = false
		}
		idx := b.db.shardIndex(op.Key)
		shardIdxs[i] = idx
		shardCounts[idx]++
		add := int64(len(op.Key))
		if op.Type == batch.OpPut {
			add += int64(len(op.Value))
		}
		shardAdds[idx] += add
	}
	touchedMems := make([]memtable.Table, 0, shardCount)
	for i, count := range shardCounts {
		if count > 0 {
			touchedMems = append(touchedMems, b.db.mutableShards[i].mem)
		}
	}
	for i, add := range shardAdds {
		if add == 0 {
			continue
		}
		shard := &b.db.mutableShards[i]
		shard.mu.Lock()
		over := b.db.shardExceedsLimit(shard, add)
		shard.mu.Unlock()
		if over {
			b.db.writeMu.RUnlock()
			return ErrMemtableFull
		}
	}

	// 2. Optional value-log + journal append loop.
	//
	// The value log (value store) and journal (redo log) are decoupled:
	// - When DisableWAL=true, we still append large values to the value log
	//   and store pointers in memory; there is simply no redo log for crash replay.
	// - When DisableWAL=false, we also append commit-intent records that can
	//   be replayed to recover unflushed writes.
	durability := journalDurabilityNone
	if syncWrite {
		if b.db.relaxedSync {
			durability = journalDurabilityFlush
		} else {
			durability = journalDurabilitySync
		}
	}
	debugPtr := b.db.debugFlushPointers
	valueLogEnabled := b.db.valueLogEnabled()

	var (
		eligibleIdxs       []int
		eligibleCountTotal int
		allowPointers      bool
		hybridRIDJoin      bool
		eligibleCount      int
		multiLanePointers  bool
	)
	if !allDeletes {
		eligibleIdxs = b.eligibleIdxs
		if cap(eligibleIdxs) < len(b.entries) {
			eligibleIdxs = make([]int, 0, len(b.entries))
		} else {
			eligibleIdxs = eligibleIdxs[:0]
		}
		b.eligibleIdxs = eligibleIdxs
		if valueLogEnabled || debugPtr {
			for i := range b.entries {
				op := &b.entries[i]
				if op.Type != batch.OpPut || !b.db.shouldWriteViaValueLogForKeyValue(op.Key, op.Value) {
					continue
				}
				eligibleIdxs = append(eligibleIdxs, i)
			}
		}
		eligibleCountTotal = len(eligibleIdxs)
		deferredFencePath := b.db.deferredValueLogEnabled()
		allowPointers = eligibleCountTotal > 0 && valueLogEnabled && b.db.allowValueLogPointers()
		hybridRIDJoin = allowPointers && b.db.fenceRIDJoinHybridEnabled()
		if allowPointers && deferredFencePath {
			if hybridRIDJoin {
				// WAL-on v2_fenceptr rid_join hybrid:
				// - C1 (non-oversized) values stay deferred-inline and materialize at flush.
				// - C2 (oversized) values take immediate pointer publication with OpSetRID.
				immediate := eligibleIdxs[:0]
				deferredCount := 0
				var deferredBytes uint64
				for _, idx := range eligibleIdxs {
					op := &b.entries[idx]
					if b.db.oversizedOuterLeafValue(op.Value) {
						immediate = append(immediate, idx)
						continue
					}
					deferredCount++
					deferredBytes += uint64(len(op.Key) + len(op.Value))
				}
				if deferredCount > 0 {
					b.db.recordDeferredFenceEnqueued(deferredCount, deferredBytes)
				}
				eligibleIdxs = immediate
				b.eligibleIdxs = eligibleIdxs
				if len(eligibleIdxs) == 0 {
					allowPointers = false
				}
			} else {
				// Deferred fence path: keep values inline in mutable memtables and
				// materialize grouped value-log pointers once at flush time.
				var deferredBytes uint64
				for _, idx := range eligibleIdxs {
					op := &b.entries[idx]
					deferredBytes += uint64(len(op.Key) + len(op.Value))
				}
				b.db.recordDeferredFenceEnqueued(eligibleCountTotal, deferredBytes)
				allowPointers = false
			}
		} else if allowPointers && b.db.disableJournal && !b.db.memtableValueLogPointers {
			// WAL-off: when the journal is disabled, defer value-log appends to the flush boundary
			// so repeated overwrites can coalesce in the memtable before hitting disk.
			allowPointers = false
		}
		eligibleCount = len(eligibleIdxs)
		if debugPtr && eligibleCountTotal > 0 {
			b.db.debugPtrEligible.Add(int64(eligibleCountTotal))
			if !valueLogEnabled {
				b.db.debugPtrDisabled.Add(int64(eligibleCountTotal))
			} else if !allowPointers {
				b.db.debugPtrDenied.Add(int64(eligibleCountTotal))
			} else if hybridRIDJoin && eligibleCountTotal > eligibleCount {
				b.db.debugPtrDenied.Add(int64(eligibleCountTotal - eligibleCount))
			}
		}
		multiLanePointers = allowPointers &&
			!b.db.outerLeafV2Enabled() &&
			b.db.disableJournal &&
			durability == journalDurabilityNone &&
			len(b.db.lanes) > 1 &&
			eligibleCount >= multiLaneValueLogMinRecords
	}

	var (
		lane *lane
		rids []uint64
	)
	needLaneForPointers := !allDeletes && !multiLanePointers && allowPointers
	needLaneForJournal := !b.db.disableJournal
	if needLaneForPointers || needLaneForJournal {
		preferredLane := -1
		if b.db.deferredValueLogNeedsSingleLaneRegroup() {
			// Keep deferred fence-mode pointer appends on a single lane so flush-time
			// fence collapse can operate over one globally merged stream.
			preferredLane = 0
		}
		l, err := b.db.pickLane(durability == journalDurabilitySync, preferredLane)
		if err != nil {
			b.db.writeMu.RUnlock()
			return err
		}
		lane = l
		if durability == journalDurabilitySync {
			defer b.db.releaseLaneSync(lane)
		}
		if !b.db.disableJournal {
			rids = make([]uint64, len(b.entries))
		}
	}

	if !allDeletes && allowPointers && eligibleCount > 0 {
		if err := b.freezeDictID(context.Background()); err != nil {
			b.db.writeMu.RUnlock()
			return err
		}
		if hybridRIDJoin {
			used := 0
			for _, idx := range eligibleIdxs {
				op := &b.entries[idx]
				outerPtr, outerRID, retainPath, err := b.db.appendFenceRIDJoinOversizedOne(lane, b.dictID, op.Key, op.Value, durability)
				if err != nil {
					b.db.writeMu.RUnlock()
					return err
				}
				op.ValuePtr = outerPtr
				op.IsPtr = true
				if b.db.memtableValueLogPointers {
					op.Value = nil
				}
				if rids != nil {
					rids[idx] = outerRID
				}
				if retainPath != "" {
					b.db.markValueLogRetain(retainPath)
				}
				used++
			}
			if debugPtr && used > 0 {
				b.db.debugPtrUsed.Add(int64(used))
			}
		} else if multiLanePointers {
			type laneValueLogBatch struct {
				laneID      int
				idxs        []int
				records     []valuelog.Record
				safeNoClear bool
				ptrs        []page.ValuePtr
				err         error
			}

			laneCounts := make([]int, len(b.db.lanes))
			for _, idx := range eligibleIdxs {
				laneID := b.db.laneForShardIndex(shardIdxs[idx])
				if laneID < 0 || laneID >= len(b.db.lanes) {
					laneID = 0
				}
				laneCounts[laneID]++
			}

			laneBatches := make([]laneValueLogBatch, len(b.db.lanes))
			activeLaneIDs := make([]int, 0, len(b.db.lanes))
			for laneID, count := range laneCounts {
				if count == 0 {
					continue
				}
				lb := &laneBatches[laneID]
				lb.laneID = laneID
				lb.idxs = make([]int, 0, count)
				lb.records = getValueLogRecordsCap(count)
				lb.safeNoClear = true
				activeLaneIDs = append(activeLaneIDs, laneID)
			}

			defer func() {
				for _, laneID := range activeLaneIDs {
					lb := &laneBatches[laneID]
					if lb.records != nil {
						if lb.safeNoClear {
							putValueLogRecordsNoClear(lb.records)
						} else {
							putValueLogRecords(lb.records)
						}
						lb.records = nil
					}
					if lb.ptrs != nil {
						putValueLogPtrs(lb.ptrs)
						lb.ptrs = nil
					}
				}
			}()

			for _, idx := range eligibleIdxs {
				op := &b.entries[idx]
				payload, encErr := b.db.encodeOuterLeafValue(op.Key, op.Value)
				if encErr != nil {
					b.db.writeMu.RUnlock()
					return encErr
				}
				rid := b.db.nextRID.Add(1)
				if rids != nil {
					rids[idx] = rid
				}
				laneID := b.db.laneForShardIndex(shardIdxs[idx])
				if laneID < 0 || laneID >= len(b.db.lanes) {
					laneID = 0
				}
				lb := &laneBatches[laneID]
				if lb.safeNoClear && (len(payload) > 64 || cap(payload) > 64) {
					lb.safeNoClear = false
				}
				lb.idxs = append(lb.idxs, idx)
				lb.records = append(lb.records, valuelog.Record{RID: rid, Value: payload})
			}

			var wg sync.WaitGroup
			for _, laneID := range activeLaneIDs {
				lb := &laneBatches[laneID]
				wg.Add(1)
				go func(batch *laneValueLogBatch) {
					defer wg.Done()
					batch.ptrs, batch.err = b.db.appendValueLog(&b.db.lanes[batch.laneID], b.dictID, nil, batch.records, durability)
				}(lb)
			}
			wg.Wait()

			for _, laneID := range activeLaneIDs {
				lb := &laneBatches[laneID]
				if lb.err != nil {
					b.db.writeMu.RUnlock()
					return lb.err
				}
			}

			for _, laneID := range activeLaneIDs {
				lb := &laneBatches[laneID]
				if len(lb.ptrs) != len(lb.idxs) {
					if debugPtr {
						b.db.debugPtrNoPtr.Add(int64(len(lb.idxs)))
					}
					continue
				}
				for i, idx := range lb.idxs {
					op := &b.entries[idx]
					op.ValuePtr = lb.ptrs[i]
					op.IsPtr = true
					if b.db.memtableValueLogPointers {
						op.Value = nil
					}
				}
				if debugPtr {
					b.db.debugPtrUsed.Add(int64(len(lb.idxs)))
				}
				retainPath := b.db.currentValueLogPath(&b.db.lanes[lb.laneID])
				if retainPath != "" {
					b.db.markValueLogRetain(retainPath)
				}
				putValueLogPtrs(lb.ptrs)
				lb.ptrs = nil
			}
		} else {
			keys := getValueLogKeys(eligibleCount)
			defer putValueLogKeys(keys)
			values := getValueLogKeys(eligibleCount)
			defer putValueLogKeys(values)
			for _, idx := range eligibleIdxs {
				op := &b.entries[idx]
				keys = append(keys, op.Key)
				values = append(values, op.Value)
			}
			routeMode := strings.TrimSpace(b.db.indexOuterLeafMode) == backenddb.IndexOuterLeafModeV1LeafLogRoute
			var (
				ptrs         []page.ValuePtr
				groups       []outerLeafRecordGroup
				valueRecords []valuelog.Record
				outerArena   []byte
			)
			var buildErr error
			if routeMode {
				ptrs, groups, buildErr = b.db.writeRouteOuterLeafValueRecords(lane, b.dictID, keys, values, durability)
				if buildErr != nil {
					b.db.writeMu.RUnlock()
					return buildErr
				}
				if len(ptrs) == 0 {
					if len(groups) > 0 {
						b.db.writeMu.RUnlock()
						return fmt.Errorf("cachingdb: empty route value-log pointer set for %d eligible ops", eligibleCount)
					}
					b.db.writeMu.RUnlock()
					return nil
				}
			} else {
				valueRecords, groups, outerArena, buildErr = b.db.buildOuterLeafValueRecords(keys, values)
				if buildErr != nil {
					b.db.writeMu.RUnlock()
					return buildErr
				}
				if len(valueRecords) == 0 {
					putValueLogRecordsNoClear(valueRecords)
					putOuterLeafArena(outerArena)
					b.db.writeMu.RUnlock()
					return fmt.Errorf("cachingdb: empty value-log record set for %d eligible ops", eligibleCount)
				}
				defer putValueLogRecordsNoClear(valueRecords)
				defer putOuterLeafArena(outerArena)
				startRID := b.db.nextRID.Add(uint64(len(valueRecords))) - uint64(len(valueRecords)) + 1
				for i := range valueRecords {
					rid := startRID + uint64(i)
					valueRecords[i].RID = rid
					if rids != nil {
						group := groups[i]
						if group.start < 0 || group.end < group.start || group.end > len(eligibleIdxs) {
							b.db.writeMu.RUnlock()
							return fmt.Errorf("cachingdb: value-log group out of range [%d,%d) len=%d", group.start, group.end, len(eligibleIdxs))
						}
						for srcPos := group.start; srcPos < group.end; srcPos++ {
							idx := eligibleIdxs[srcPos]
							rids[idx] = rid
						}
					}
				}
				ptrs, buildErr = b.db.appendValueLog(lane, b.dictID, nil, valueRecords, durability)
				if buildErr != nil {
					b.db.writeMu.RUnlock()
					return buildErr
				}
			}
			if len(ptrs) != len(groups) {
				putValueLogPtrs(ptrs)
				b.db.writeMu.RUnlock()
				return fmt.Errorf("cachingdb: value-log pointer group count mismatch expected=%d got=%d", len(groups), len(ptrs))
			}
			defer putValueLogPtrs(ptrs)

			used := 0
			for i := range groups {
				ptr := ptrs[i]
				if b.db.outerLeafFenceV2Enabled() {
					ptr = page.ValuePtrMarkFenceOuterCollapsed(ptr)
				}
				group := groups[i]
				if group.start < 0 || group.end < group.start || group.end > len(eligibleIdxs) {
					b.db.writeMu.RUnlock()
					return fmt.Errorf("cachingdb: value-log pointer group out of range [%d,%d) len=%d", group.start, group.end, len(eligibleIdxs))
				}
				for srcPos := group.start; srcPos < group.end; srcPos++ {
					idx := eligibleIdxs[srcPos]
					op := &b.entries[idx]
					op.ValuePtr = ptr
					op.IsPtr = true
					if b.db.memtableValueLogPointers {
						op.Value = nil
					}
					used++
				}
			}
			if debugPtr && used > 0 {
				b.db.debugPtrUsed.Add(int64(used))
			}
			retainPath := b.db.currentValueLogPath(lane)
			if retainPath != "" {
				b.db.markValueLogRetain(retainPath)
			}
		}
	}

	if !b.db.disableJournal {
		records := b.walBuf[:0]
		if cap(records) < len(b.entries) {
			records = make([]logRecord, 0, len(b.entries))
		}
		for i := range b.entries {
			op := &b.entries[i]
			switch op.Type {
			case batch.OpDelete:
				records = append(records, logRecord{Op: logOpDelete, Key: op.Key})
			case batch.OpPut:
				if rids != nil && rids[i] != 0 {
					records = append(records, logRecord{Op: logOpSetRID, Key: op.Key, RID: rids[i]})
				} else {
					records = append(records, logRecord{Op: logOpSetInline, Key: op.Key, Value: op.Value})
				}
			}
		}
		b.walBuf = records
		if err := b.db.appendWAL(lane, records, durability); err != nil {
			b.db.writeMu.RUnlock()
			return err
		}
	}

	if allDeletes {
		shardIdxSets := b.shardIdxSets
		if cap(shardIdxSets) < shardCount {
			shardIdxSets = make([][]int, shardCount)
		} else {
			shardIdxSets = shardIdxSets[:shardCount]
			for i := range shardIdxSets {
				shardIdxSets[i] = shardIdxSets[i][:0]
			}
		}
		b.shardIdxSets = shardIdxSets
		for i, count := range shardCounts {
			if count <= 0 {
				continue
			}
			idxs := shardIdxSets[i]
			if cap(idxs) < count {
				idxs = b.db.getBatchIntSlice(count)
			} else {
				idxs = idxs[:0]
			}
			shardIdxSets[i] = idxs
		}
		for i := range b.entries {
			idx := shardIdxs[i]
			shardIdxSets[idx] = append(shardIdxSets[idx], i)
		}

		// 3. Memtable Update (delete-only fast path)
		for i := range shardIdxSets {
			idxs := shardIdxSets[i]
			if len(idxs) == 0 {
				continue
			}
			shard := &b.db.mutableShards[i]
			shard.mu.Lock()
			if b.streamEligible {
				first := b.entries[idxs[0]].Key
				last := first
				for _, entryIdx := range idxs {
					key := b.entries[entryIdx].Key
					last = key
					shard.mem.DeleteSteal(key)
				}
				shard.rng.add(first)
				if len(idxs) > 1 {
					shard.rng.add(last)
				}
				b.db.noteWriteSortedRun(first, last, len(idxs))
			} else {
				for _, entryIdx := range idxs {
					key := b.entries[entryIdx].Key
					shard.mem.DeleteSteal(key)
					shard.rng.add(key)
					b.db.noteWriteKey(key)
				}
			}
			newBytes := shard.mem.Size()
			delta := newBytes - shard.bytes
			shard.bytes = newBytes
			b.db.mutableBytes.Add(delta)
			shard.mu.Unlock()
		}
	} else {
		shardEntries := b.shardEntries
		if cap(shardEntries) < shardCount {
			shardEntries = make([][]batch.Entry, shardCount)
		} else {
			shardEntries = shardEntries[:shardCount]
			for i := range shardEntries {
				shardEntries[i] = shardEntries[i][:0]
			}
		}
		b.shardEntries = shardEntries
		for i, count := range shardCounts {
			if count > 0 {
				entries := shardEntries[i]
				if cap(entries) < count {
					entries = b.db.getBatchShardEntries(count)
				} else {
					entries = entries[:0]
				}
				shardEntries[i] = entries
			}
		}
		for i, op := range b.entries {
			idx := shardIdxs[i]
			shardEntries[idx] = append(shardEntries[idx], op)
		}

		// 3. Memtable Update
		for i := range shardEntries {
			entries := shardEntries[i]
			if len(entries) == 0 {
				continue
			}
			shard := &b.db.mutableShards[i]
			shard.mu.Lock()
			useStream := b.streamEligible
			if useStream {
				if applier, ok := shard.mem.(memtable.TrustedSortedBatchApplier); ok {
					applier.ApplyStealSortedBatchTrusted(entries, nil)
				} else if applier, ok := shard.mem.(memtable.SortedBatchApplier); ok {
					applier.ApplyStealSortedBatch(entries, nil)
				} else {
					for _, op := range entries {
						if op.Type == batch.OpDelete {
							shard.mem.DeleteSteal(op.Key)
						} else {
							if op.IsPtr {
								memVal := []byte(nil)
								if !b.db.memtableValueLogPointers {
									memVal = op.Value
								}
								shard.mem.SetEntrySteal(op.Key, memVal, op.ValuePtr, node.FlagPointer)
							} else {
								shard.mem.SetSteal(op.Key, op.Value)
							}
						}
					}
				}
				first := entries[0].Key
				last := entries[len(entries)-1].Key
				shard.rng.add(first)
				if len(entries) > 1 {
					shard.rng.add(last)
				}
				b.db.noteWriteSortedRun(first, last, len(entries))
			} else {
				for _, op := range entries {
					if op.Type == batch.OpDelete {
						shard.mem.DeleteSteal(op.Key)
					} else {
						if op.IsPtr {
							memVal := []byte(nil)
							if !b.db.memtableValueLogPointers {
								memVal = op.Value
							}
							shard.mem.SetEntrySteal(op.Key, memVal, op.ValuePtr, node.FlagPointer)
						} else {
							shard.mem.SetSteal(op.Key, op.Value)
						}
					}
					shard.rng.add(op.Key)
					b.db.noteWriteKey(op.Key)
				}
			}
			newBytes := shard.mem.Size()
			delta := newBytes - shard.bytes
			shard.bytes = newBytes
			b.db.mutableBytes.Add(delta)
			shard.mu.Unlock()
		}
	}

	// 3. Threshold Check
	if b.db.mutableBytes.Load() > b.db.mutableFlushThreshold() {
		needRotate = true
	}
	if syncWrite && b.db.disableJournal {
		needSyncBarrier = true
	}
	b.updateBatchEntryHint()
	b.updateBatchCopyHint()
	b.db.retainBatchArenaChunksForMemtables(b.drainCopyArenaChunks(), touchedMems)
	b.db.writeMu.RUnlock()

	if needRotate {
		if err := b.db.maybeRotateMemtable(true); err != nil {
			return err
		}
	}
	if needSyncBarrier {
		if err := b.db.syncBarrierAfterWrite(true); err != nil {
			return err
		}
	}

	if b.size > 0 {
		b.db.noteWrite()
	}
	b.db.maybeAssistFlush()
	b.Reset()
	return nil
}

type logSegmentInfo struct {
	path     string
	size     int64
	seq      int
	lane     int
	valueLog bool
}

func listNonEmptyLogSegments(walDir string) (segments []logSegmentInfo, nonEmptyBytes int64) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return nil, 0
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		lane, seq, valueLog, ok := parseLogSeq(name)
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(walDir, name)
		segments = append(segments, logSegmentInfo{path: path, size: info.Size(), seq: seq, lane: lane, valueLog: valueLog})
		if info.Size() > 0 {
			nonEmptyBytes += info.Size()
		}
	}
	return segments, nonEmptyBytes
}

func parseLogSeq(name string) (int, int, bool, bool) {
	const (
		commitPrefix = "commit-"
		valuePrefix  = "value-"
		walPrefix    = "wal-"
		vlogPrefix   = "vlog-"
	)
	if filepath.Ext(name) != ".log" {
		return 0, 0, false, false
	}
	base := strings.TrimSuffix(name, ".log")

	parseLaneSeq := func(rest string) (int, int, bool) {
		parts := strings.SplitN(rest, "-", 2)
		if len(parts) != 2 {
			return 0, 0, false
		}
		lane, err := strconv.Atoi(parts[0])
		if err != nil || lane < 0 {
			return 0, 0, false
		}
		seq, err := strconv.Atoi(parts[1])
		if err != nil || seq < 0 {
			return 0, 0, false
		}
		return lane, seq, true
	}

	if strings.HasPrefix(base, "commit-l") {
		lane, seq, ok := parseLaneSeq(strings.TrimPrefix(base, "commit-l"))
		return lane, seq, false, ok
	}
	if strings.HasPrefix(base, "value-l") {
		lane, seq, ok := parseLaneSeq(strings.TrimPrefix(base, "value-l"))
		return lane, seq, true, ok
	}
	if strings.HasPrefix(base, commitPrefix) {
		core := strings.TrimPrefix(base, commitPrefix)
		if core == "" {
			return 0, 0, false, false
		}
		seq, err := strconv.Atoi(core)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, false, true
	}
	if strings.HasPrefix(base, valuePrefix) {
		core := strings.TrimPrefix(base, valuePrefix)
		if core == "" {
			return 0, 0, false, false
		}
		seq, err := strconv.Atoi(core)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, true, true
	}
	if strings.HasPrefix(base, walPrefix) {
		core := strings.TrimPrefix(base, walPrefix)
		if core == "" {
			return 0, 0, false, false
		}
		seq, err := strconv.Atoi(core)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, false, true
	}
	if strings.HasPrefix(base, vlogPrefix) {
		core := strings.TrimPrefix(base, vlogPrefix)
		if core == "" {
			return 0, 0, false, false
		}
		seq, err := strconv.Atoi(core)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, true, true
	}
	return 0, 0, false, false
}

func (b *Batch) writeBypass(sync bool) error {
	// Fast path: if none of these keys exist in mutable/queue, we can write directly
	// to the backend without flushing (no in-memory shadowing possible).
	// Cheap append-only check: if the batch key range does not overlap with any
	// in-memory key ranges, it cannot be shadowed.
	batchRange := keyRange{}
	for _, op := range b.entries {
		key := op.Key
		batchRange.add(key)
	}

	mutableRange := b.db.snapshotMutableRange()

	var (
		mutables      []memtable.Table
		queue         []memtable.Table
		queueShardIDs []uint16
		queueRanges   []keyRange
		overlaps      bool
	)
	view := b.db.retainMemtableView()
	if view != nil {
		defer b.db.releaseMemtableView(view)
	}

	b.db.mu.RLock()
	overlaps = rangesOverlap(batchRange, mutableRange)
	if view != nil {
		mutables = view.mutables
		queue = view.queue
		queueShardIDs = view.queueShardIDs
		queueRanges = view.queueRanges
	} else {
		// Defensive fallback: should not happen after Open(), but keep safe
		// behavior for zero-value DBs and tests.
		if len(b.db.mutableShards) > 0 {
			mutables = make([]memtable.Table, len(b.db.mutableShards))
			for i := range b.db.mutableShards {
				mutables[i] = b.db.mutableShards[i].mem
			}
		}
		if len(b.db.queue) > 0 {
			queue = append([]memtable.Table(nil), b.db.queue...)
		}
		if len(b.db.queueShardIDs) > 0 {
			queueShardIDs = append([]uint16(nil), b.db.queueShardIDs...)
		}
		if len(b.db.queueRanges) > 0 {
			queueRanges = append([]keyRange(nil), b.db.queueRanges...)
		}
	}
	b.db.mu.RUnlock()

	if !overlaps {
		if len(queueRanges) == 0 && len(queue) > 0 {
			overlaps = true
		} else {
			for _, r := range queueRanges {
				if rangesOverlap(batchRange, r) {
					overlaps = true
					break
				}
			}
		}
	}

	if overlaps {
		// Slow path: verify no individual key exists in memory (handles sparse overlap).
		for _, op := range b.entries {
			key := op.Key
			if len(mutables) > 0 {
				idx := b.db.shardIndex(key)
				if idx < len(mutables) && mutables[idx] != nil {
					if _, _, found := mutables[idx].Get(key); found {
						return b.writeRegular(sync)
					}
				}
			}
			for i := len(queue) - 1; i >= 0; i-- {
				if len(queueShardIDs) > i && len(mutables) > 0 {
					idx := b.db.shardIndex(key)
					if int(queueShardIDs[i]) != idx {
						continue
					}
				}
				if _, _, found := queue[i].Get(key); found {
					return b.writeRegular(sync)
				}
			}
		}
	}

	// Write directly to backend
	backendBatch := b.db.backend.NewBatch()

	// Use SetOps for bulk transfer (backend will resolve value-log pointers).
	if err := backendBatch.SetOps(b.entries); err != nil {
		return err
	}

	var err error
	if sync && !b.db.relaxedSync {
		b.db.flushMu.Lock()
		err = backendBatch.WriteSync()
		b.db.flushMu.Unlock()
	} else {
		b.db.flushMu.Lock()
		err = backendBatch.Write()
		b.db.flushMu.Unlock()
	}

	if err != nil {
		return err
	}

	b.db.mu.Lock()
	if batchRange.valid {
		b.db.backendRange.add(batchRange.min)
		b.db.backendRange.add(batchRange.max)
	}
	b.db.mu.Unlock()

	if b.size > 0 {
		b.db.noteWrite()
	}
	b.updateBatchEntryHint()
	b.updateBatchCopyHint()
	b.Reset()
	return nil
}

func (b *Batch) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	if b.backend != nil {
		_ = b.backend.Close()
		b.backend = nil
	}
	if b.db != nil && b.entries != nil {
		b.db.putBatchEntries(b.entries)
	}
	if b.db != nil && b.shardIdxs != nil {
		b.db.putBatchIntSlice(b.shardIdxs)
	}
	if b.db != nil && b.shardIdxSets != nil && cap(b.shardIdxSets) > 0 {
		full := b.shardIdxSets[:cap(b.shardIdxSets)]
		for i := range full {
			if full[i] != nil {
				b.db.putBatchIntSlice(full[i])
				full[i] = nil
			}
		}
	}
	if b.db != nil && b.shardEntries != nil && cap(b.shardEntries) > 0 {
		full := b.shardEntries[:cap(b.shardEntries)]
		for i := range full {
			if full[i] != nil {
				b.db.putBatchShardEntries(full[i])
				full[i] = nil
			}
		}
	}
	b.recycleCopyArenaChunks()
	b.entries = nil
	b.walBuf = nil
	b.shardIdxs = nil
	b.eligibleIdxs = nil
	b.shardAdds = nil
	b.shardCnts = nil
	b.shardEntries = nil
	b.shardIdxSets = nil
	b.firstKey = nil
	b.lastKey = nil
	return nil
}

func (b *Batch) Replay(fn func(batch.Entry) error) error {
	if b.closed {
		return ErrBatchClosed
	}
	if b.backend != nil {
		return b.backend.Replay(fn)
	}

	for _, entry := range b.entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) GetByteSize() (int, error) {
	if b.closed {
		return 0, ErrBatchClosed
	}
	return b.size, nil
}

// singleSourceIterator wraps a single UnsafeIterator to satisfy merging.Iterator,
// skipping tombstones.
type singleSourceIterator struct {
	iter  iterator.UnsafeIterator
	valid bool
	start []byte
	end   []byte
}

func newSingleSourceIterator(iter iterator.UnsafeIterator, start, end []byte) merging.Iterator {
	it := &singleSourceIterator{
		iter:  iter,
		start: start,
		end:   end,
	}
	// Iterator is already sought to start by the caller
	it.advance()
	return it
}

func (it *singleSourceIterator) advance() {
	it.valid = false
	for it.iter.Valid() {
		if it.end != nil && bytes.Compare(it.iter.UnsafeKey(), it.end) >= 0 {
			return
		}
		if it.iter.IsDeleted() {
			it.iter.Next()
			continue
		}
		it.valid = true
		return
	}
}

func (it *singleSourceIterator) Next() {
	if !it.valid {
		panic("iterator invalid")
	}
	it.iter.Next()
	it.advance()
}

func (it *singleSourceIterator) UnsafeKey() []byte {
	if !it.valid {
		return nil
	}
	return it.iter.UnsafeKey()
}

func (it *singleSourceIterator) UnsafeValue() []byte {
	if !it.valid {
		return nil
	}
	return it.iter.UnsafeValue()
}

func (it *singleSourceIterator) Valid() bool               { return it.valid }
func (it *singleSourceIterator) Key() []byte               { return it.iter.Key() }
func (it *singleSourceIterator) Value() []byte             { return it.iter.Value() }
func (it *singleSourceIterator) KeyCopy(dst []byte) []byte { return it.iter.KeyCopy(dst) }
func (it *singleSourceIterator) ValueCopy(dst []byte) []byte {
	return it.iter.ValueCopy(dst)
}
func (it *singleSourceIterator) Close() error             { return it.iter.Close() }
func (it *singleSourceIterator) Error() error             { return it.iter.Error() }
func (it *singleSourceIterator) Domain() ([]byte, []byte) { return it.start, it.end }

// emptyIterator represents an iterator with no elements.
type emptyIterator struct {
	start, end []byte
}

func (it *emptyIterator) Next()                     { panic("iterator invalid") }
func (it *emptyIterator) Valid() bool               { return false }
func (it *emptyIterator) Key() []byte               { panic("iterator invalid") }
func (it *emptyIterator) Value() []byte             { panic("iterator invalid") }
func (it *emptyIterator) KeyCopy(_ []byte) []byte   { panic("iterator invalid") }
func (it *emptyIterator) ValueCopy(_ []byte) []byte { panic("iterator invalid") }
func (it *emptyIterator) Close() error              { return nil }
func (it *emptyIterator) Error() error              { return nil }
func (it *emptyIterator) Domain() ([]byte, []byte)  { return it.start, it.end }
