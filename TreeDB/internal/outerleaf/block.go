package outerleaf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"sync"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	defaultBlockTargetBytes = 4 << 10
	defaultRestartInterval  = 16
	linearScanMaxEntries    = 32

	blockCodecNone   = uint8(0)
	blockCodecSnappy = uint8(1)
	blockCodecLZ4    = uint8(2)

	// Skip full-block compression when a small sample shows incompressible or
	// near-incompressible behavior.
	outerLeafIncompressibleProbeMinBytes = 1024
	outerLeafIncompressibleProbeBytes    = 512
	outerLeafMinSavingsDiv               = 50 // 2%
	outerLeafMinSavingsBytes             = 8
	outerLeafHighEntropyUniqueThreshold  = 224
)

var blockMagic = [4]byte{'T', 'O', 'L', '2'}

const (
	// Keep pooled scratch buffers bounded to avoid retaining outsized slices.
	// These classed pools trade additional retained memory for fewer decode
	// allocations on hot read paths.
	maxPooledOuterLeafBytesCap      = 512 << 10
	maxPooledOuterLeafRestartsCap   = 4096
	maxPooledOuterLeafLeaseKeysCap  = 4096
	maxPooledOuterLeafLeaseArenaCap = 1 << 20
	outerLeafBytesMinClassShift     = 10 // 1 KiB
	outerLeafBytesMaxClassShift     = 19 // 512 KiB
	outerLeafBytesClassCount        = outerLeafBytesMaxClassShift - outerLeafBytesMinClassShift + 1
)

var (
	outerLeafBytesPools     [outerLeafBytesClassCount]sync.Pool
	outerLeafBytesEntryPool = sync.Pool{
		New: func() any {
			return &bytesPoolEntry{}
		},
	}
	outerLeafRestartsPool      sync.Pool
	outerLeafRestartsEntryPool = sync.Pool{
		New: func() any {
			return &restartsPoolEntry{}
		},
	}
	outerLeafLeaseKeysPool      sync.Pool
	outerLeafLeaseArenaPool     sync.Pool
	outerLeafLeaseKeysEntryPool = sync.Pool{
		New: func() any {
			return &leaseKeysPoolEntry{}
		},
	}
	outerLeafLeaseArenaEntryPool = sync.Pool{
		New: func() any {
			return &leaseArenaPoolEntry{}
		},
	}
	outerLeafKeyLeasePool sync.Pool
	outerLeafKeyChunkPool = sync.Pool{
		New: func() any {
			return &keyLeaseChunk{}
		},
	}
)

const (
	blockVersionV1 = uint8(1)
	blockVersionV2 = uint8(2)
	blockVersionV3 = uint8(3)

	blockHeaderSize   = 22
	blockChecksumOff  = 18
	blockChecksumSize = 4

	// v1 header layout
	blockV1KeyLenOff   = 8
	blockV1ValueLenOff = 10
	blockV1RawLenOff   = 14

	// v2 header layout
	blockV2EntryCountOff = 8
	blockV2EntriesLenOff = 10
	blockV2RawLenOff     = 14

	restartDecodeStackCap = 64
)

type restartDecodeMode uint8

const (
	restartDecodeModeNone restartDecodeMode = iota
	restartDecodeModeStack
	restartDecodeModePooled
)

const (
	entryKindInline = EntryKind(0)
	entryKindBlob   = EntryKind(1)
)

// ErrBlobRefEntry is returned by legacy decode helpers when a v3 entry
// contains a blob reference and nested resolution is required.
var ErrBlobRefEntry = errors.New("outerleaf: blob-ref entry requires nested resolution")

// EntryKind identifies the value payload encoding for a v3 entry.
type EntryKind uint8

const (
	EntryKindInline  EntryKind = 0
	EntryKindBlobRef EntryKind = 1
)

// TypedEntry describes one key entry with either inline bytes or a blob ref.
type TypedEntry struct {
	Key     []byte
	Kind    EntryKind
	Value   []byte
	BlobPtr page.ValuePtr
}

// LookupResult is the decoded v3 lookup result kind/payload pair.
type LookupResult struct {
	Kind    EntryKind
	Value   []byte
	BlobPtr page.ValuePtr
}

type keyLeaseChunk struct {
	buf  []byte
	next *keyLeaseChunk
}

type bytesPoolEntry struct {
	buf []byte
}

type restartsPoolEntry struct {
	buf []uint32
}

type leaseKeysPoolEntry struct {
	buf [][]byte
}

type leaseArenaPoolEntry struct {
	buf []byte
}

// KeyLease provides explicit ownership for decoded key vectors.
// Callers must call Release when finished.
type KeyLease struct {
	keys      [][]byte
	chunkHead *keyLeaseChunk
	chunkTail *keyLeaseChunk
	inUse     bool
}

// Keys returns decoded keys owned by the lease.
func (l *KeyLease) Keys() [][]byte {
	if l == nil {
		return nil
	}
	return l.keys
}

// Release returns lease-owned buffers to pools. It is idempotent.
func (l *KeyLease) Release() {
	releaseKeyLease(l)
}

// Entry is one key/value record in an outer-leaf block payload.
type Entry struct {
	Key   []byte
	Value []byte
}

// DecodedBlock represents a parsed outer-leaf payload.
type DecodedBlock struct {
	version      uint8
	entryCount   int
	raw          []byte
	entries      []byte
	restartRaw   []byte
	restartCount int
	restarts     []uint32
	restartKeys  [][]byte
	firstKey     []byte
	firstKind    EntryKind
	firstValue   []byte
	firstBlob    page.ValuePtr
	leaseOwned   bool
	pooledRaw    bool
	pooledRest   bool
	pooledRKeys  bool

	keysOnce        sync.Once
	keys            [][]byte
	keysErr         error
	visitLease      *KeyLease
	restartsOnce    sync.Once
	restartsErr     error
	restartKeysOnce sync.Once
	restartKeysErr  error
}

func (d *DecodedBlock) EntryCount() int {
	if d == nil {
		return 0
	}
	return d.entryCount
}

// RawBytes returns the decoded raw payload backing this block.
func (d *DecodedBlock) RawBytes() []byte {
	if d == nil {
		return nil
	}
	return d.raw
}

// Encoder reuses encode scratch buffers across outer-leaf block encodes.
//
// It is not safe for concurrent use.
type Encoder struct {
	rawScratch      []byte
	encScratch      []byte
	restartsScratch []uint32
}

// HasMagic reports whether payload begins with the outer-leaf block magic
// header. It is a cheap classifier for outer-leaf encoded values.
func HasMagic(payload []byte) bool {
	if len(payload) < len(blockMagic) {
		return false
	}
	return payload[0] == blockMagic[0] &&
		payload[1] == blockMagic[1] &&
		payload[2] == blockMagic[2] &&
		payload[3] == blockMagic[3]
}

func NormalizeBlockTargetBytes(target int) int {
	if target <= 0 {
		return defaultBlockTargetBytes
	}
	return target
}

func NormalizeRestartInterval(interval int) int {
	if interval <= 0 {
		return defaultRestartInterval
	}
	if interval > int(^uint16(0)) {
		return int(^uint16(0))
	}
	return interval
}

func normalizeCodec(codec uint8) uint8 {
	// Input codec uses TreeDB ValueLogBlockCodec values:
	//   0 = snappy
	//   1 = lz4
	switch codec {
	case 1:
		return blockCodecLZ4
	default:
		return blockCodecSnappy
	}
}

func minCompressionSavings(rawLen int) int {
	if rawLen <= 1 {
		return 1
	}
	minSavings := rawLen / outerLeafMinSavingsDiv
	if minSavings < outerLeafMinSavingsBytes {
		minSavings = outerLeafMinSavingsBytes
	}
	if minSavings >= rawLen {
		minSavings = rawLen - 1
	}
	if minSavings < 1 {
		minSavings = 1
	}
	return minSavings
}

func keepCompressedPayload(rawLen, encodedLen int) bool {
	if rawLen <= 0 || encodedLen <= 0 || encodedLen >= rawLen {
		return false
	}
	return rawLen-encodedLen >= minCompressionSavings(rawLen)
}

func sampleForIncompressibleProbe(raw []byte, sampleBuf *[outerLeafIncompressibleProbeBytes]byte) []byte {
	if len(raw) <= outerLeafIncompressibleProbeBytes {
		return raw
	}
	// Spread the sample across the full payload so a compressible middle section
	// is still represented (prefix+suffix-only sampling can miss it).
	last := len(raw) - 1
	for i := 0; i < outerLeafIncompressibleProbeBytes; i++ {
		idx := (i * last) / (outerLeafIncompressibleProbeBytes - 1)
		sampleBuf[i] = raw[idx]
	}
	return sampleBuf[:]
}

func shouldBypassCompressionProbe(codec uint8, raw []byte, dst []byte) bool {
	if len(raw) < outerLeafIncompressibleProbeMinBytes {
		return false
	}
	if normalizeCodec(codec) != blockCodecLZ4 {
		return false
	}
	var sampleBuf [outerLeafIncompressibleProbeBytes]byte
	sample := sampleForIncompressibleProbe(raw, &sampleBuf)
	// Fast-path clearly high-entropy blocks to raw mode and skip probe/full
	// codec work.
	if isLikelyHighEntropy(sample) {
		return true
	}
	bound := lz4.CompressBlockBound(len(sample))
	if cap(dst) < bound {
		dst = make([]byte, bound)
	}
	dst = dst[:bound]
	n, err := lz4.CompressBlock(sample, dst, nil)
	if err != nil {
		return false
	}
	if n <= 0 {
		return true
	}
	return !keepCompressedPayload(len(sample), n)
}

func isLikelyHighEntropy(sample []byte) bool {
	if len(sample) == 0 {
		return false
	}
	var seen [4]uint64
	for i := range sample {
		b := sample[i]
		seen[b>>6] |= 1 << (b & 63)
	}
	unique := bits.OnesCount64(seen[0]) + bits.OnesCount64(seen[1]) + bits.OnesCount64(seen[2]) + bits.OnesCount64(seen[3])
	return unique >= outerLeafHighEntropyUniqueThreshold
}

func encodePayload(codec uint8, raw []byte, dst []byte) ([]byte, uint8, error) {
	if len(raw) == 0 {
		return nil, blockCodecNone, nil
	}
	codec = normalizeCodec(codec)
	// Snappy is the default outer-leaf codec. Running a probe (sample + optional
	// sample-compress) on every block adds measurable CPU overhead on
	// compressible write-heavy paths while the full encode still applies
	// keepCompressedPayload gating. Keep probe logic for non-default codecs.
	if codec != blockCodecSnappy && shouldBypassCompressionProbe(codec, raw, dst) {
		return raw, blockCodecNone, nil
	}
	switch codec {
	case blockCodecLZ4:
		bound := lz4.CompressBlockBound(len(raw))
		if cap(dst) < bound {
			dst = make([]byte, bound)
		}
		dst = dst[:bound]
		n, err := lz4.CompressBlock(raw, dst, nil)
		if err == nil && n > 0 {
			if keepCompressedPayload(len(raw), n) {
				return dst[:n], blockCodecLZ4, nil
			}
			return raw, blockCodecNone, nil
		}
		// Fall through to snappy on lz4 miss for deterministic behavior.
		fallthrough
	default:
		need := snappy.MaxEncodedLen(len(raw))
		if cap(dst) < need {
			dst = make([]byte, need)
		} else {
			dst = dst[:need]
		}
		enc := snappy.Encode(dst, raw)
		if keepCompressedPayload(len(raw), len(enc)) {
			return enc, blockCodecSnappy, nil
		}
		return raw, blockCodecNone, nil
	}
}

func decodePayloadInto(codec uint8, payload []byte, rawLen int, dst []byte, preferPool bool) ([]byte, bool, error) {
	if rawLen < 0 {
		return nil, false, fmt.Errorf("outerleaf: invalid raw length %d", rawLen)
	}
	if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
		return nil, false, fmt.Errorf("outerleaf: payload too large %d", rawLen)
	}

	switch codec {
	case blockCodecNone:
		if len(payload) != rawLen {
			return nil, false, fmt.Errorf("outerleaf: raw payload length mismatch got=%d want=%d", len(payload), rawLen)
		}
		pooled := false
		if cap(dst) < rawLen {
			if preferPool && dst == nil {
				dst = getPooledBytes(rawLen)
				pooled = true
			} else {
				dst = make([]byte, rawLen)
			}
			dst = dst[:rawLen]
		} else {
			dst = dst[:rawLen]
		}
		copy(dst, payload)
		return dst, pooled, nil
	case blockCodecSnappy:
		decodedLen, err := snappy.DecodedLen(payload)
		if err != nil {
			return nil, false, err
		}
		if decodedLen != rawLen {
			return nil, false, fmt.Errorf("outerleaf: snappy decoded length mismatch got=%d want=%d", decodedLen, rawLen)
		}
		pooled := false
		if cap(dst) < rawLen {
			if preferPool && dst == nil {
				dst = getPooledBytes(rawLen)
				pooled = true
			} else {
				dst = make([]byte, rawLen)
			}
			dst = dst[:rawLen]
		} else {
			dst = dst[:rawLen]
		}
		out, err := snappy.Decode(dst, payload)
		if err != nil {
			if pooled {
				putPooledBytes(dst)
			}
			return nil, false, err
		}
		if len(out) != rawLen {
			if pooled {
				putPooledBytes(dst)
			}
			return nil, false, fmt.Errorf("outerleaf: snappy decode length mismatch got=%d want=%d", len(out), rawLen)
		}
		return out, pooled, nil
	case blockCodecLZ4:
		pooled := false
		if cap(dst) < rawLen {
			if preferPool && dst == nil {
				dst = getPooledBytes(rawLen)
				pooled = true
			} else {
				dst = make([]byte, rawLen)
			}
			dst = dst[:rawLen]
		} else {
			dst = dst[:rawLen]
		}
		n, err := lz4.UncompressBlock(payload, dst)
		if err != nil {
			if pooled {
				putPooledBytes(dst)
			}
			return nil, false, err
		}
		if n != rawLen {
			if pooled {
				putPooledBytes(dst)
			}
			return nil, false, fmt.Errorf("outerleaf: lz4 decode length mismatch got=%d want=%d", n, rawLen)
		}
		return dst[:n], pooled, nil
	default:
		return nil, false, fmt.Errorf("outerleaf: unknown codec id %d", codec)
	}
}

func decodePayload(codec uint8, payload []byte, rawLen int, dst []byte) ([]byte, error) {
	out, _, err := decodePayloadInto(codec, payload, rawLen, dst, false)
	return out, err
}

func commonPrefixLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func appendLE16(dst []byte, v uint16) []byte {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	return append(dst, buf[:]...)
}

func appendLE32(dst []byte, v uint32) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	return append(dst, buf[:]...)
}

func appendLE64(dst []byte, v uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	return append(dst, buf[:]...)
}

func appendValuePtr16(dst []byte, ptr page.ValuePtr) []byte {
	dst = appendLE32(dst, ptr.FileID)
	dst = appendLE64(dst, ptr.Offset)
	return appendLE32(dst, ptr.Length)
}

func decodeValuePtr16(payload []byte) (page.ValuePtr, error) {
	if len(payload) != page.ValuePtrSize {
		return page.ValuePtr{}, fmt.Errorf("outerleaf: invalid blob ref length %d", len(payload))
	}
	return page.ValuePtr{
		FileID: binary.LittleEndian.Uint32(payload[0:4]),
		Offset: binary.LittleEndian.Uint64(payload[4:12]),
		Length: binary.LittleEndian.Uint32(payload[12:16]),
	}, nil
}

func outerLeafBytesClassShiftCeil(minCap int) int {
	if minCap <= (1 << outerLeafBytesMinClassShift) {
		return outerLeafBytesMinClassShift
	}
	shift := bits.Len(uint(minCap - 1))
	if shift < outerLeafBytesMinClassShift {
		return outerLeafBytesMinClassShift
	}
	if shift > outerLeafBytesMaxClassShift {
		return outerLeafBytesMaxClassShift
	}
	return shift
}

func outerLeafBytesClassShiftFloor(capacity int) int {
	if capacity <= (1 << outerLeafBytesMinClassShift) {
		return outerLeafBytesMinClassShift
	}
	shift := bits.Len(uint(capacity)) - 1
	if shift < outerLeafBytesMinClassShift {
		return outerLeafBytesMinClassShift
	}
	if shift > outerLeafBytesMaxClassShift {
		return outerLeafBytesMaxClassShift
	}
	return shift
}

func getPooledBytes(minCap int) []byte {
	if minCap <= 0 {
		minCap = 1
	}
	if minCap > maxPooledOuterLeafBytesCap {
		return make([]byte, 0, minCap)
	}
	shift := outerLeafBytesClassShiftCeil(minCap)
	pool := &outerLeafBytesPools[shift-outerLeafBytesMinClassShift]
	if v := pool.Get(); v != nil {
		if entry, ok := v.(*bytesPoolEntry); ok && entry != nil {
			buf := entry.buf
			entry.buf = nil
			outerLeafBytesEntryPool.Put(entry)
			if cap(buf) >= minCap {
				return buf[:0]
			}
		}
	}
	classCap := 1 << shift
	if classCap < minCap {
		classCap = minCap
	}
	return make([]byte, 0, classCap)
}

func putPooledBytes(buf []byte) {
	if cap(buf) == 0 || cap(buf) > maxPooledOuterLeafBytesCap {
		return
	}
	shift := outerLeafBytesClassShiftFloor(cap(buf))
	pool := &outerLeafBytesPools[shift-outerLeafBytesMinClassShift]
	entry := outerLeafBytesEntryPool.Get().(*bytesPoolEntry)
	entry.buf = buf[:0]
	pool.Put(entry)
}

func getPooledRestarts(minCap int) []uint32 {
	if minCap <= 0 {
		minCap = 1
	}
	if v := outerLeafRestartsPool.Get(); v != nil {
		if entry, ok := v.(*restartsPoolEntry); ok && entry != nil {
			buf := entry.buf
			entry.buf = nil
			outerLeafRestartsEntryPool.Put(entry)
			if cap(buf) >= minCap {
				return buf[:0]
			}
		}
	}
	return make([]uint32, 0, minCap)
}

func putPooledRestarts(buf []uint32) {
	if cap(buf) == 0 || cap(buf) > maxPooledOuterLeafRestartsCap {
		return
	}
	entry := outerLeafRestartsEntryPool.Get().(*restartsPoolEntry)
	entry.buf = buf[:0]
	outerLeafRestartsPool.Put(entry)
}

func getPooledLeaseKeys(minCap int) [][]byte {
	if minCap <= 0 {
		minCap = 1
	}
	if v := outerLeafLeaseKeysPool.Get(); v != nil {
		if entry, ok := v.(*leaseKeysPoolEntry); ok && entry != nil {
			keys := entry.buf
			entry.buf = nil
			outerLeafLeaseKeysEntryPool.Put(entry)
			if cap(keys) >= minCap {
				return keys[:0]
			}
			putPooledLeaseKeys(keys)
		}
	}
	return make([][]byte, 0, minCap)
}

func putPooledLeaseKeys(keys [][]byte) {
	if cap(keys) == 0 || cap(keys) > maxPooledOuterLeafLeaseKeysCap {
		return
	}
	full := keys[:cap(keys)]
	clear(full)
	entry := outerLeafLeaseKeysEntryPool.Get().(*leaseKeysPoolEntry)
	entry.buf = full[:0]
	outerLeafLeaseKeysPool.Put(entry)
}

func getPooledLeaseArena(minCap int) []byte {
	if minCap <= 0 {
		minCap = 1
	}
	if v := outerLeafLeaseArenaPool.Get(); v != nil {
		if entry, ok := v.(*leaseArenaPoolEntry); ok && entry != nil {
			arena := entry.buf
			entry.buf = nil
			outerLeafLeaseArenaEntryPool.Put(entry)
			if cap(arena) >= minCap {
				return arena[:0]
			}
			putPooledLeaseArena(arena)
		}
	}
	return make([]byte, 0, minCap)
}

func putPooledLeaseArena(arena []byte) {
	if cap(arena) == 0 || cap(arena) > maxPooledOuterLeafLeaseArenaCap {
		return
	}
	entry := outerLeafLeaseArenaEntryPool.Get().(*leaseArenaPoolEntry)
	entry.buf = arena[:0]
	outerLeafLeaseArenaPool.Put(entry)
}

func acquireKeyLease(minKeys int) *KeyLease {
	if minKeys <= 0 {
		minKeys = 1
	}
	var lease *KeyLease
	if v := outerLeafKeyLeasePool.Get(); v != nil {
		if l, ok := v.(*KeyLease); ok {
			lease = l
		}
	}
	if lease == nil {
		lease = &KeyLease{}
	}
	lease.inUse = true
	lease.keys = getPooledLeaseKeys(minKeys)
	lease.chunkHead = nil
	lease.chunkTail = nil
	return lease
}

func (l *KeyLease) addChunk(arena []byte) {
	if l == nil {
		return
	}
	var node *keyLeaseChunk
	if v := outerLeafKeyChunkPool.Get(); v != nil {
		if n, ok := v.(*keyLeaseChunk); ok {
			node = n
		}
	}
	if node == nil {
		node = &keyLeaseChunk{}
	}
	node.buf = arena[:0]
	node.next = nil
	if l.chunkTail == nil {
		l.chunkHead = node
		l.chunkTail = node
		return
	}
	l.chunkTail.next = node
	l.chunkTail = node
}

func releaseKeyLease(lease *KeyLease) {
	if lease == nil || !lease.inUse {
		return
	}
	lease.inUse = false
	putPooledLeaseKeys(lease.keys)
	lease.keys = nil

	node := lease.chunkHead
	for node != nil {
		next := node.next
		putPooledLeaseArena(node.buf)
		node.buf = nil
		node.next = nil
		outerLeafKeyChunkPool.Put(node)
		node = next
	}
	lease.chunkHead = nil
	lease.chunkTail = nil
	outerLeafKeyLeasePool.Put(lease)
}

func cloneKeySlices(keys [][]byte) [][]byte {
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

func borrowBytes(minCap int, scratch *[]byte) ([]byte, bool) {
	if minCap <= 0 {
		minCap = 1
	}
	if scratch != nil {
		buf := *scratch
		if cap(buf) < minCap {
			buf = make([]byte, 0, minCap)
		}
		return buf[:0], false
	}
	return getPooledBytes(minCap), true
}

func releaseBytes(buf []byte, scratch *[]byte, pooled bool) {
	if scratch != nil {
		if cap(buf) == 0 {
			*scratch = nil
			return
		}
		*scratch = buf[:0]
		return
	}
	if pooled {
		putPooledBytes(buf)
	}
}

func growPooledBytes(buf []byte, minCap int) []byte {
	if minCap <= cap(buf) {
		return buf[:minCap]
	}
	newCap := cap(buf) * 2
	if newCap < minCap {
		newCap = minCap
	}
	if newCap < 64 {
		newCap = 64
	}
	next := getPooledBytes(newCap)
	next = next[:minCap]
	copy(next, buf)
	putPooledBytes(buf)
	return next
}

func borrowRestarts(minCap int, scratch *[]uint32) ([]uint32, bool) {
	if minCap <= 0 {
		minCap = 1
	}
	if scratch != nil {
		buf := *scratch
		if cap(buf) < minCap {
			buf = make([]uint32, 0, minCap)
		}
		return buf[:0], false
	}
	return getPooledRestarts(minCap), true
}

func releaseRestarts(buf []uint32, scratch *[]uint32, pooled bool) {
	if scratch != nil {
		if cap(buf) == 0 {
			*scratch = nil
			return
		}
		*scratch = buf[:0]
		return
	}
	if pooled {
		putPooledRestarts(buf)
	}
}

func encodedPayloadBound(codec uint8, rawLen int) int {
	if rawLen <= 0 {
		return 1
	}
	if normalizeCodec(codec) == blockCodecLZ4 {
		return lz4.CompressBlockBound(rawLen)
	}
	return snappy.MaxEncodedLen(rawLen)
}

func encodeV1SingleCore(dst, key, value []byte, codec uint8, restartInterval int, rawScratch, encScratch *[]byte) ([]byte, error) {
	rawLen := len(key) + len(value)
	if rawLen > int(^uint32(0)) {
		return nil, fmt.Errorf("outerleaf: payload too large %d", rawLen)
	}
	if len(key) > int(^uint16(0)) {
		return nil, fmt.Errorf("outerleaf: key too large %d", len(key))
	}
	if len(value) > int(^uint32(0)) {
		return nil, fmt.Errorf("outerleaf: value too large %d", len(value))
	}

	raw, rawPooled := borrowBytes(rawLen, rawScratch)
	defer func() {
		releaseBytes(raw, rawScratch, rawPooled)
	}()
	raw = append(raw, key...)
	raw = append(raw, value...)

	enc, encPooled := borrowBytes(encodedPayloadBound(codec, len(raw)), encScratch)
	defer func() {
		releaseBytes(enc, encScratch, encPooled)
	}()
	encodedPayload, encodedCodec, err := encodePayload(codec, raw, enc[:0])
	if err != nil {
		return nil, err
	}
	if len(encodedPayload) == 0 {
		encodedCodec = blockCodecNone
	} else if encodedCodec != blockCodecNone {
		// Keep enc pointed at the buffer currently holding encoded payload bytes so
		// deferred release/trim operates on the active allocation.
		enc = encodedPayload[:0]
	}

	total := blockHeaderSize + len(encodedPayload)
	if cap(dst) < total {
		dst = make([]byte, total)
	} else {
		dst = dst[:total]
	}
	copy(dst[:4], blockMagic[:])
	dst[4] = blockVersionV1
	dst[5] = encodedCodec
	binary.LittleEndian.PutUint16(dst[6:8], uint16(NormalizeRestartInterval(restartInterval)))
	binary.LittleEndian.PutUint16(dst[blockV1KeyLenOff:blockV1KeyLenOff+2], uint16(len(key)))
	binary.LittleEndian.PutUint32(dst[blockV1ValueLenOff:blockV1ValueLenOff+4], uint32(len(value)))
	binary.LittleEndian.PutUint32(dst[blockV1RawLenOff:blockV1RawLenOff+4], uint32(rawLen))
	binary.LittleEndian.PutUint32(dst[blockChecksumOff:blockChecksumOff+blockChecksumSize], 0)
	copy(dst[blockHeaderSize:], encodedPayload)

	sum := crc.ChecksumParts(dst[:blockChecksumOff], dst[blockHeaderSize:])
	binary.LittleEndian.PutUint32(dst[blockChecksumOff:blockChecksumOff+blockChecksumSize], sum)
	return dst, nil
}

func encodeV1Single(dst, key, value []byte, codec uint8, restartInterval int) ([]byte, error) {
	return encodeV1SingleCore(dst, key, value, codec, restartInterval, nil, nil)
}

func encodeV2EntriesCore(dst []byte, entries []Entry, codec uint8, restartInterval int, validateOrder bool, rawScratch, encScratch *[]byte, restartsScratch *[]uint32) ([]byte, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("outerleaf: empty entries")
	}
	if len(entries) > int(^uint16(0)) {
		return nil, fmt.Errorf("outerleaf: too many entries %d", len(entries))
	}

	restartInterval = NormalizeRestartInterval(restartInterval)
	if restartInterval <= 0 {
		restartInterval = 1
	}

	estRaw := 4 // restart-count trailer
	for i := range entries {
		estRaw += 8 + len(entries[i].Key) + len(entries[i].Value)
	}
	estRaw += 4 * ((len(entries) + restartInterval - 1) / restartInterval)
	restartCap := (len(entries) + restartInterval - 1) / restartInterval
	raw, rawPooled := borrowBytes(estRaw, rawScratch)
	defer func() {
		releaseBytes(raw, rawScratch, rawPooled)
	}()
	restarts, restartsPooled := borrowRestarts(restartCap, restartsScratch)
	defer func() {
		releaseRestarts(restarts, restartsScratch, restartsPooled)
	}()
	var prev []byte

	for i := range entries {
		e := entries[i]
		if len(e.Value) > int(^uint32(0)) {
			return nil, fmt.Errorf("outerleaf: value too large %d", len(e.Value))
		}
		if validateOrder && i > 0 && bytes.Compare(entries[i-1].Key, e.Key) >= 0 {
			return nil, fmt.Errorf("outerleaf: entries must be strictly increasing")
		}

		shared := 0
		if i%restartInterval == 0 {
			restarts = append(restarts, uint32(len(raw)))
		} else {
			shared = commonPrefixLen(prev, e.Key)
		}
		suffixLen := len(e.Key) - shared
		if shared > int(^uint16(0)) || suffixLen > int(^uint16(0)) {
			return nil, fmt.Errorf("outerleaf: key too large %d", len(e.Key))
		}

		raw = appendLE16(raw, uint16(shared))
		raw = appendLE16(raw, uint16(suffixLen))
		raw = appendLE32(raw, uint32(len(e.Value)))
		raw = append(raw, e.Key[shared:]...)
		raw = append(raw, e.Value...)

		// Keep a direct reference to the previous key while encoding this block.
		// entries keys remain immutable for the duration of this function, so we
		// can avoid per-entry key copying/allocation here.
		prev = e.Key
	}

	entriesLen := len(raw)
	for i := range restarts {
		raw = appendLE32(raw, restarts[i])
	}
	raw = appendLE32(raw, uint32(len(restarts)))

	if len(raw) > int(^uint32(0)) {
		return nil, fmt.Errorf("outerleaf: payload too large %d", len(raw))
	}
	if entriesLen > int(^uint32(0)) {
		return nil, fmt.Errorf("outerleaf: entries payload too large %d", entriesLen)
	}

	enc, encPooled := borrowBytes(encodedPayloadBound(codec, len(raw)), encScratch)
	defer func() {
		releaseBytes(enc, encScratch, encPooled)
	}()
	encodedPayload, encodedCodec, err := encodePayload(codec, raw, enc[:0])
	if err != nil {
		return nil, err
	}
	if len(encodedPayload) == 0 {
		encodedCodec = blockCodecNone
	} else if encodedCodec != blockCodecNone {
		// Keep enc pointed at the buffer currently holding encoded payload bytes so
		// deferred release/trim operates on the active allocation.
		enc = encodedPayload[:0]
	}

	total := blockHeaderSize + len(encodedPayload)
	if cap(dst) < total {
		dst = make([]byte, total)
	} else {
		dst = dst[:total]
	}
	copy(dst[:4], blockMagic[:])
	dst[4] = blockVersionV2
	dst[5] = encodedCodec
	binary.LittleEndian.PutUint16(dst[6:8], uint16(restartInterval))
	binary.LittleEndian.PutUint16(dst[blockV2EntryCountOff:blockV2EntryCountOff+2], uint16(len(entries)))
	binary.LittleEndian.PutUint32(dst[blockV2EntriesLenOff:blockV2EntriesLenOff+4], uint32(entriesLen))
	binary.LittleEndian.PutUint32(dst[blockV2RawLenOff:blockV2RawLenOff+4], uint32(len(raw)))
	binary.LittleEndian.PutUint32(dst[blockChecksumOff:blockChecksumOff+blockChecksumSize], 0)
	copy(dst[blockHeaderSize:], encodedPayload)

	sum := crc.ChecksumParts(dst[:blockChecksumOff], dst[blockHeaderSize:])
	binary.LittleEndian.PutUint32(dst[blockChecksumOff:blockChecksumOff+blockChecksumSize], sum)
	return dst, nil
}

func encodeV2Entries(dst []byte, entries []Entry, codec uint8, restartInterval int, validateOrder bool) ([]byte, error) {
	return encodeV2EntriesCore(dst, entries, codec, restartInterval, validateOrder, nil, nil, nil)
}

func encodeV3EntriesCore(dst []byte, entries []TypedEntry, codec uint8, restartInterval int, validateOrder bool, rawScratch, encScratch *[]byte, restartsScratch *[]uint32) ([]byte, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("outerleaf: empty entries")
	}
	if len(entries) > int(^uint16(0)) {
		return nil, fmt.Errorf("outerleaf: too many entries %d", len(entries))
	}

	restartInterval = NormalizeRestartInterval(restartInterval)
	if restartInterval <= 0 {
		restartInterval = 1
	}

	estRaw := 4 // restart-count trailer
	for i := range entries {
		payloadLen := len(entries[i].Value)
		if entries[i].Kind == EntryKindBlobRef {
			payloadLen = page.ValuePtrSize
		}
		estRaw += 9 + len(entries[i].Key) + payloadLen
	}
	estRaw += 4 * ((len(entries) + restartInterval - 1) / restartInterval)
	restartCap := (len(entries) + restartInterval - 1) / restartInterval
	raw, rawPooled := borrowBytes(estRaw, rawScratch)
	defer func() {
		releaseBytes(raw, rawScratch, rawPooled)
	}()
	restarts, restartsPooled := borrowRestarts(restartCap, restartsScratch)
	defer func() {
		releaseRestarts(restarts, restartsScratch, restartsPooled)
	}()
	var prev []byte

	for i := range entries {
		e := entries[i]
		if validateOrder && i > 0 && bytes.Compare(entries[i-1].Key, e.Key) >= 0 {
			return nil, fmt.Errorf("outerleaf: entries must be strictly increasing")
		}
		payloadLen := len(e.Value)
		if e.Kind == EntryKindBlobRef {
			payloadLen = page.ValuePtrSize
		}
		if payloadLen > int(^uint32(0)) {
			return nil, fmt.Errorf("outerleaf: value too large %d", payloadLen)
		}

		shared := 0
		if i%restartInterval == 0 {
			restarts = append(restarts, uint32(len(raw)))
		} else {
			shared = commonPrefixLen(prev, e.Key)
		}
		suffixLen := len(e.Key) - shared
		if shared > int(^uint16(0)) || suffixLen > int(^uint16(0)) {
			return nil, fmt.Errorf("outerleaf: key too large %d", len(e.Key))
		}

		raw = appendLE16(raw, uint16(shared))
		raw = appendLE16(raw, uint16(suffixLen))
		raw = append(raw, byte(e.Kind))
		raw = appendLE32(raw, uint32(payloadLen))
		raw = append(raw, e.Key[shared:]...)
		switch e.Kind {
		case EntryKindInline:
			raw = append(raw, e.Value...)
		case EntryKindBlobRef:
			raw = appendValuePtr16(raw, e.BlobPtr)
		default:
			return nil, fmt.Errorf("outerleaf: unknown entry kind %d", e.Kind)
		}
		prev = e.Key
	}

	entriesLen := len(raw)
	for i := range restarts {
		raw = appendLE32(raw, restarts[i])
	}
	raw = appendLE32(raw, uint32(len(restarts)))

	if len(raw) > int(^uint32(0)) {
		return nil, fmt.Errorf("outerleaf: payload too large %d", len(raw))
	}
	if entriesLen > int(^uint32(0)) {
		return nil, fmt.Errorf("outerleaf: entries payload too large %d", entriesLen)
	}

	enc, encPooled := borrowBytes(encodedPayloadBound(codec, len(raw)), encScratch)
	defer func() {
		releaseBytes(enc, encScratch, encPooled)
	}()
	encodedPayload, encodedCodec, err := encodePayload(codec, raw, enc[:0])
	if err != nil {
		return nil, err
	}
	if len(encodedPayload) == 0 {
		encodedCodec = blockCodecNone
	} else if encodedCodec != blockCodecNone {
		enc = encodedPayload[:0]
	}

	total := blockHeaderSize + len(encodedPayload)
	if cap(dst) < total {
		dst = make([]byte, total)
	} else {
		dst = dst[:total]
	}
	copy(dst[:4], blockMagic[:])
	dst[4] = blockVersionV3
	dst[5] = encodedCodec
	binary.LittleEndian.PutUint16(dst[6:8], uint16(restartInterval))
	binary.LittleEndian.PutUint16(dst[blockV2EntryCountOff:blockV2EntryCountOff+2], uint16(len(entries)))
	binary.LittleEndian.PutUint32(dst[blockV2EntriesLenOff:blockV2EntriesLenOff+4], uint32(entriesLen))
	binary.LittleEndian.PutUint32(dst[blockV2RawLenOff:blockV2RawLenOff+4], uint32(len(raw)))
	binary.LittleEndian.PutUint32(dst[blockChecksumOff:blockChecksumOff+blockChecksumSize], 0)
	copy(dst[blockHeaderSize:], encodedPayload)

	sum := crc.ChecksumParts(dst[:blockChecksumOff], dst[blockHeaderSize:])
	binary.LittleEndian.PutUint32(dst[blockChecksumOff:blockChecksumOff+blockChecksumSize], sum)
	return dst, nil
}

func encodeV3Entries(dst []byte, entries []TypedEntry, codec uint8, restartInterval int, validateOrder bool) ([]byte, error) {
	return encodeV3EntriesCore(dst, entries, codec, restartInterval, validateOrder, nil, nil, nil)
}

// Reset clears reusable encoder slices while keeping allocated capacity.
func (e *Encoder) Reset() {
	if e == nil {
		return
	}
	e.rawScratch = e.rawScratch[:0]
	e.encScratch = e.encScratch[:0]
	e.restartsScratch = e.restartsScratch[:0]
}

// Trim drops oversized scratch buffers and resets retained buffers to zero len.
func (e *Encoder) Trim(maxRawCap, maxEncCap, maxRestartsCap int) {
	if e == nil {
		return
	}
	if maxRawCap > 0 && cap(e.rawScratch) > maxRawCap {
		e.rawScratch = nil
	} else {
		e.rawScratch = e.rawScratch[:0]
	}
	if maxEncCap > 0 && cap(e.encScratch) > maxEncCap {
		e.encScratch = nil
	} else {
		e.encScratch = e.encScratch[:0]
	}
	if maxRestartsCap > 0 && cap(e.restartsScratch) > maxRestartsCap {
		e.restartsScratch = nil
	} else {
		e.restartsScratch = e.restartsScratch[:0]
	}
}

// EncodeSingle encodes one key/value record using reusable scratch buffers.
func (e *Encoder) EncodeSingle(dst, key, value []byte, codec uint8, restartInterval int) ([]byte, error) {
	if e == nil {
		return encodeV1Single(dst, key, value, codec, restartInterval)
	}
	return encodeV1SingleCore(dst, key, value, codec, restartInterval, &e.rawScratch, &e.encScratch)
}

// EncodeEntries encodes ordered key/value records using reusable scratch buffers.
func (e *Encoder) EncodeEntries(dst []byte, entries []Entry, codec uint8, restartInterval int) ([]byte, error) {
	if len(entries) == 1 {
		return e.EncodeSingle(dst, entries[0].Key, entries[0].Value, codec, restartInterval)
	}
	if e == nil {
		return encodeV2Entries(dst, entries, codec, restartInterval, true)
	}
	return encodeV2EntriesCore(dst, entries, codec, restartInterval, true, &e.rawScratch, &e.encScratch, &e.restartsScratch)
}

// EncodeEntriesAssumeSorted encodes ordered key/value records without
// re-validating strict ordering and reuses scratch buffers across calls.
func (e *Encoder) EncodeEntriesAssumeSorted(dst []byte, entries []Entry, codec uint8, restartInterval int) ([]byte, error) {
	if len(entries) == 1 {
		return e.EncodeSingle(dst, entries[0].Key, entries[0].Value, codec, restartInterval)
	}
	if e == nil {
		return encodeV2Entries(dst, entries, codec, restartInterval, false)
	}
	return encodeV2EntriesCore(dst, entries, codec, restartInterval, false, &e.rawScratch, &e.encScratch, &e.restartsScratch)
}

// EncodeTypedEntries encodes ordered typed entries using v3 layout.
func (e *Encoder) EncodeTypedEntries(dst []byte, entries []TypedEntry, codec uint8, restartInterval int) ([]byte, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("outerleaf: empty entries")
	}
	if e == nil {
		return encodeV3Entries(dst, entries, codec, restartInterval, true)
	}
	return encodeV3EntriesCore(dst, entries, codec, restartInterval, true, &e.rawScratch, &e.encScratch, &e.restartsScratch)
}

// EncodeTypedEntriesAssumeSorted encodes ordered typed entries using v3 layout
// without re-validating strict key ordering.
func (e *Encoder) EncodeTypedEntriesAssumeSorted(dst []byte, entries []TypedEntry, codec uint8, restartInterval int) ([]byte, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("outerleaf: empty entries")
	}
	if e == nil {
		return encodeV3Entries(dst, entries, codec, restartInterval, false)
	}
	return encodeV3EntriesCore(dst, entries, codec, restartInterval, false, &e.rawScratch, &e.encScratch, &e.restartsScratch)
}

// EncodeSingle encodes one key/value record in a v1-compatible payload.
func EncodeSingle(dst, key, value []byte, codec uint8, restartInterval int) ([]byte, error) {
	return encodeV1Single(dst, key, value, codec, restartInterval)
}

// EncodeEntries encodes an ordered block of key/value records using v2 layout.
func EncodeEntries(dst []byte, entries []Entry, codec uint8, restartInterval int) ([]byte, error) {
	if len(entries) == 1 {
		return encodeV1Single(dst, entries[0].Key, entries[0].Value, codec, restartInterval)
	}
	return encodeV2Entries(dst, entries, codec, restartInterval, true)
}

// EncodeEntriesAssumeSorted encodes an ordered block of key/value records using
// v2 layout without re-validating strict key ordering.
func EncodeEntriesAssumeSorted(dst []byte, entries []Entry, codec uint8, restartInterval int) ([]byte, error) {
	if len(entries) == 1 {
		return encodeV1Single(dst, entries[0].Key, entries[0].Value, codec, restartInterval)
	}
	return encodeV2Entries(dst, entries, codec, restartInterval, false)
}

// EncodeTypedEntries encodes ordered typed entries using v3 layout.
func EncodeTypedEntries(dst []byte, entries []TypedEntry, codec uint8, restartInterval int) ([]byte, error) {
	return encodeV3Entries(dst, entries, codec, restartInterval, true)
}

// EncodeTypedEntriesAssumeSorted encodes ordered typed entries using v3 layout
// without re-validating strict key ordering.
func EncodeTypedEntriesAssumeSorted(dst []byte, entries []TypedEntry, codec uint8, restartInterval int) ([]byte, error) {
	return encodeV3Entries(dst, entries, codec, restartInterval, false)
}

// EncodeSingleBlobRef encodes one key -> blob pointer entry using v3 layout.
func EncodeSingleBlobRef(dst, key []byte, ptr page.ValuePtr, codec uint8, restartInterval int) ([]byte, error) {
	entry := TypedEntry{Key: key, Kind: EntryKindBlobRef, BlobPtr: ptr}
	return encodeV3Entries(dst, []TypedEntry{entry}, codec, restartInterval, true)
}

func decodeAndVerifyPayload(payload []byte, rawLen int, codec uint8, scratch []byte) ([]byte, error) {
	return decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, scratch, false, true)
}

func decodeAndVerifyPayloadMode(payload []byte, rawLen int, codec uint8, scratch []byte, allowRawView bool) ([]byte, error) {
	return decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, scratch, allowRawView, true)
}

func decodeAndVerifyPayloadModeWithChecksum(payload []byte, rawLen int, codec uint8, scratch []byte, allowRawView bool, verifyChecksum bool) ([]byte, error) {
	out, _, err := decodeAndVerifyPayloadModeWithChecksumOwned(payload, rawLen, codec, scratch, allowRawView, verifyChecksum, false)
	return out, err
}

func decodeAndVerifyPayloadModeWithChecksumOwned(payload []byte, rawLen int, codec uint8, scratch []byte, allowRawView bool, verifyChecksum bool, preferPool bool) ([]byte, bool, error) {
	if blockHeaderSize > len(payload) {
		return nil, false, fmt.Errorf("outerleaf: truncated header")
	}
	if verifyChecksum {
		gotChecksum := binary.LittleEndian.Uint32(payload[blockChecksumOff : blockChecksumOff+blockChecksumSize])
		wantChecksum := crc.ChecksumParts(payload[:blockChecksumOff], payload[blockHeaderSize:])
		if gotChecksum != wantChecksum {
			return nil, false, fmt.Errorf("outerleaf: checksum mismatch")
		}
	}
	if allowRawView && codec == blockCodecNone {
		raw := payload[blockHeaderSize:]
		if len(raw) != rawLen {
			return nil, false, fmt.Errorf("outerleaf: raw payload length mismatch got=%d want=%d", len(raw), rawLen)
		}
		return raw, false, nil
	}
	out, pooled, err := decodePayloadInto(codec, payload[blockHeaderSize:], rawLen, scratch, preferPool && scratch == nil)
	if err != nil {
		return nil, false, err
	}
	if len(out) != rawLen {
		if pooled {
			putPooledBytes(out)
		}
		return nil, false, fmt.Errorf("outerleaf: decoded payload length mismatch")
	}
	return out, pooled, nil
}

func splitV2RawMeta(raw []byte, entriesLen int, entryCount int, restartInterval int) (entries []byte, restartRaw []byte, restartCount int, err error) {
	if entriesLen < 0 || entriesLen > len(raw) {
		return nil, nil, 0, fmt.Errorf("outerleaf: invalid entries length %d", entriesLen)
	}
	if len(raw) < entriesLen+4 {
		return nil, nil, 0, fmt.Errorf("outerleaf: missing restart trailer")
	}
	restartCount = int(binary.LittleEndian.Uint32(raw[len(raw)-4:]))
	if restartCount < 0 {
		return nil, nil, 0, fmt.Errorf("outerleaf: invalid restart count")
	}
	restartBytes := restartCount * 4
	if entriesLen+restartBytes+4 != len(raw) {
		return nil, nil, 0, fmt.Errorf("outerleaf: invalid restart section")
	}
	if entryCount > 0 && restartCount == 0 {
		return nil, nil, 0, fmt.Errorf("outerleaf: empty restart table")
	}

	restartOff := entriesLen
	prev := uint32(0)
	for i := 0; i < restartCount; i++ {
		off := binary.LittleEndian.Uint32(raw[restartOff+i*4 : restartOff+(i+1)*4])
		if int(off) < 0 || int(off) > entriesLen {
			return nil, nil, 0, fmt.Errorf("outerleaf: restart offset out of range")
		}
		if i > 0 && off < prev {
			return nil, nil, 0, fmt.Errorf("outerleaf: restart offsets not monotonic")
		}
		prev = off
	}
	if restartCount > 0 {
		first := binary.LittleEndian.Uint32(raw[restartOff : restartOff+4])
		if first != 0 {
			return nil, nil, 0, fmt.Errorf("outerleaf: first restart offset must be 0")
		}
	}
	if restartInterval <= 0 {
		restartInterval = 1
	}
	minRestarts := (entryCount + restartInterval - 1) / restartInterval
	if entryCount > 0 && restartCount < minRestarts {
		return nil, nil, 0, fmt.Errorf("outerleaf: restart table too small")
	}
	return raw[:entriesLen], raw[restartOff : restartOff+restartBytes], restartCount, nil
}

func decodeRestartsFromRawInto(entries []byte, restartRaw []byte, restartCount int, dst []uint32) ([]uint32, error) {
	if restartCount == 0 {
		return nil, nil
	}
	if len(restartRaw) != restartCount*4 {
		return nil, fmt.Errorf("outerleaf: invalid restart section")
	}
	if cap(dst) < restartCount {
		dst = make([]uint32, restartCount)
	} else {
		dst = dst[:restartCount]
	}
	restarts := dst
	prev := uint32(0)
	for i := 0; i < restartCount; i++ {
		off := binary.LittleEndian.Uint32(restartRaw[i*4 : (i+1)*4])
		if int(off) < 0 || int(off) > len(entries) {
			return nil, fmt.Errorf("outerleaf: restart offset out of range")
		}
		if i > 0 && off < prev {
			return nil, fmt.Errorf("outerleaf: restart offsets not monotonic")
		}
		restarts[i] = off
		prev = off
	}
	if restarts[0] != 0 {
		return nil, fmt.Errorf("outerleaf: first restart offset must be 0")
	}
	return restarts, nil
}

func decodeRestartsFromRaw(entries []byte, restartRaw []byte, restartCount int) ([]uint32, error) {
	return decodeRestartsFromRawInto(entries, restartRaw, restartCount, nil)
}

func decodeRestartsFromRawPooled(entries []byte, restartRaw []byte, restartCount int) ([]uint32, bool, error) {
	if restartCount == 0 {
		return nil, false, nil
	}
	buf := getPooledRestarts(restartCount)
	restarts, err := decodeRestartsFromRawInto(entries, restartRaw, restartCount, buf)
	if err != nil {
		putPooledRestarts(buf)
		return nil, false, err
	}
	return restarts, true, nil
}

func restartDecodeModeForCount(restartCount int) (restartDecodeMode, error) {
	switch {
	case restartCount < 0:
		return restartDecodeModeNone, fmt.Errorf("outerleaf: invalid restart count %d", restartCount)
	case restartCount == 0:
		return restartDecodeModeNone, nil
	case restartCount <= restartDecodeStackCap:
		return restartDecodeModeStack, nil
	default:
		return restartDecodeModePooled, nil
	}
}

func decodeFirstV2(entries []byte) (key, value []byte, err error) {
	if len(entries) < 8 {
		return nil, nil, fmt.Errorf("outerleaf: truncated first entry")
	}
	shared := int(binary.LittleEndian.Uint16(entries[0:2]))
	suffixLen := int(binary.LittleEndian.Uint16(entries[2:4]))
	valueLen := int(binary.LittleEndian.Uint32(entries[4:8]))
	if shared != 0 {
		return nil, nil, fmt.Errorf("outerleaf: first entry shared prefix must be zero")
	}
	start := 8
	if start+suffixLen+valueLen > len(entries) {
		return nil, nil, fmt.Errorf("outerleaf: truncated first entry payload")
	}
	key = entries[start : start+suffixLen]
	value = entries[start+suffixLen : start+suffixLen+valueLen]
	return key, value, nil
}

func restartV2Key(entries []byte, off int) ([]byte, error) {
	if off < 0 || off+8 > len(entries) {
		return nil, fmt.Errorf("outerleaf: truncated entry")
	}
	shared := int(binary.LittleEndian.Uint16(entries[off : off+2]))
	suffixLen := int(binary.LittleEndian.Uint16(entries[off+2 : off+4]))
	valueLen := int(binary.LittleEndian.Uint32(entries[off+4 : off+8]))
	if shared != 0 {
		return nil, fmt.Errorf("outerleaf: invalid shared prefix")
	}
	start := off + 8
	if start+suffixLen+valueLen > len(entries) {
		return nil, fmt.Errorf("outerleaf: truncated entry payload")
	}
	return entries[start : start+suffixLen], nil
}

func decodeV2RestartKeysInto(entries []byte, restarts []uint32, dst [][]byte) ([][]byte, error) {
	if len(restarts) == 0 {
		return nil, nil
	}
	if cap(dst) < len(restarts) {
		dst = make([][]byte, len(restarts))
	} else {
		dst = dst[:len(restarts)]
	}
	keys := dst
	for i := range restarts {
		k, err := restartV2Key(entries, int(restarts[i]))
		if err != nil {
			return nil, err
		}
		if i > 0 && bytes.Compare(keys[i-1], k) >= 0 {
			return nil, fmt.Errorf("outerleaf: restart keys not strictly increasing")
		}
		keys[i] = k
	}
	return keys, nil
}

func decodeV2RestartKeys(entries []byte, restarts []uint32) ([][]byte, error) {
	return decodeV2RestartKeysInto(entries, restarts, nil)
}

func decodeV2RestartKeysPooled(entries []byte, restarts []uint32) ([][]byte, bool, error) {
	if len(restarts) == 0 {
		return nil, false, nil
	}
	buf := getPooledLeaseKeys(len(restarts))
	keys, err := decodeV2RestartKeysInto(entries, restarts, buf)
	if err != nil {
		putPooledLeaseKeys(buf)
		return nil, false, err
	}
	return keys, true, nil
}

func locateV2Restart(entries []byte, key []byte, restarts []uint32, restartKeys [][]byte) (int, error) {
	if len(restarts) == 0 {
		return -1, nil
	}
	if len(restartKeys) == len(restarts) {
		idx := sort.Search(len(restartKeys), func(i int) bool {
			return bytes.Compare(restartKeys[i], key) > 0
		}) - 1
		return idx, nil
	}
	lo, hi := 0, len(restarts)
	for lo < hi {
		mid := lo + (hi-lo)/2
		rk, err := restartV2Key(entries, int(restarts[mid]))
		if err != nil {
			return -1, err
		}
		if bytes.Compare(rk, key) > 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo - 1, nil
}

func lookupV2ValueRange(entries []byte, key []byte, start int, limit int) ([]byte, bool, error) {
	if start < 0 || start > len(entries) || limit < start || limit > len(entries) {
		return nil, false, fmt.Errorf("outerleaf: invalid entry range")
	}
	off := start
	prevCap := len(key)
	if prevCap < 64 {
		prevCap = 64
	}
	var prevStack [256]byte
	var currStack [256]byte
	prev := prevStack[:0]
	curr := currStack[:0]
	if prevCap > len(prevStack) {
		prev = make([]byte, 0, prevCap)
		curr = make([]byte, 0, prevCap)
	}
	for off < limit {
		if off+8 > limit {
			return nil, false, fmt.Errorf("outerleaf: truncated entry")
		}
		shared := int(binary.LittleEndian.Uint16(entries[off : off+2]))
		suffixLen := int(binary.LittleEndian.Uint16(entries[off+2 : off+4]))
		valueLen := int(binary.LittleEndian.Uint32(entries[off+4 : off+8]))
		off += 8
		if shared > len(prev) {
			return nil, false, fmt.Errorf("outerleaf: invalid shared prefix")
		}
		if off+suffixLen+valueLen > limit {
			return nil, false, fmt.Errorf("outerleaf: truncated entry payload")
		}
		curr = append(curr[:0], prev[:shared]...)
		curr = append(curr, entries[off:off+suffixLen]...)
		off += suffixLen
		val := entries[off : off+valueLen]
		off += valueLen

		cmp := bytes.Compare(curr, key)
		if cmp == 0 {
			return val, true, nil
		}
		if cmp > 0 {
			return nil, false, nil
		}
		prev, curr = curr, prev
	}
	return nil, false, nil
}

func lookupV2Value(entries []byte, entryCount int, key []byte, restarts []uint32, restartKeys [][]byte) ([]byte, bool, error) {
	if entryCount <= 0 {
		return nil, false, nil
	}
	if key == nil {
		_, first, err := decodeFirstV2(entries)
		if err != nil {
			return nil, false, err
		}
		return first, true, nil
	}
	if len(restarts) == 0 {
		return lookupV2ValueRange(entries, key, 0, len(entries))
	}
	restartIdx, err := locateV2Restart(entries, key, restarts, restartKeys)
	if err != nil {
		return nil, false, err
	}
	if restartIdx < 0 {
		return nil, false, nil
	}
	start := int(restarts[restartIdx])
	limit := len(entries)
	if restartIdx+1 < len(restarts) {
		limit = int(restarts[restartIdx+1])
	}
	return lookupV2ValueRange(entries, key, start, limit)
}

func lookupV2ValueFromRestartRaw(entries []byte, entryCount int, key []byte, restartRaw []byte, restartCount int) ([]byte, bool, error) {
	decodeMode, err := restartDecodeModeForCount(restartCount)
	if err != nil {
		return nil, false, err
	}
	if decodeMode == restartDecodeModeNone {
		return lookupV2Value(entries, entryCount, key, nil, nil)
	}
	if decodeMode == restartDecodeModeStack {
		var restartStack [restartDecodeStackCap]uint32
		restarts, err := decodeRestartsFromRawInto(entries, restartRaw, restartCount, restartStack[:0])
		if err != nil {
			return nil, false, err
		}
		return lookupV2Value(entries, entryCount, key, restarts, nil)
	}
	restarts, pooled, err := decodeRestartsFromRawPooled(entries, restartRaw, restartCount)
	if err != nil {
		return nil, false, err
	}
	val, found, lookupErr := lookupV2Value(entries, entryCount, key, restarts, nil)
	if pooled {
		putPooledRestarts(restarts)
	}
	if lookupErr != nil {
		return nil, false, lookupErr
	}
	return val, found, nil
}

func decodeFirstV3(entries []byte) (key []byte, out LookupResult, err error) {
	if len(entries) < 9 {
		return nil, LookupResult{}, fmt.Errorf("outerleaf: truncated first entry")
	}
	shared := int(binary.LittleEndian.Uint16(entries[0:2]))
	suffixLen := int(binary.LittleEndian.Uint16(entries[2:4]))
	kind := EntryKind(entries[4])
	payloadLen := int(binary.LittleEndian.Uint32(entries[5:9]))
	if shared != 0 {
		return nil, LookupResult{}, fmt.Errorf("outerleaf: first entry shared prefix must be zero")
	}
	start := 9
	if start+suffixLen+payloadLen > len(entries) {
		return nil, LookupResult{}, fmt.Errorf("outerleaf: truncated first entry payload")
	}
	key = entries[start : start+suffixLen]
	payload := entries[start+suffixLen : start+suffixLen+payloadLen]
	out.Kind = kind
	switch kind {
	case EntryKindInline:
		out.Value = payload
	case EntryKindBlobRef:
		ptr, err := decodeValuePtr16(payload)
		if err != nil {
			return nil, LookupResult{}, err
		}
		out.BlobPtr = ptr
	default:
		return nil, LookupResult{}, fmt.Errorf("outerleaf: unknown entry kind %d", kind)
	}
	return key, out, nil
}

func restartV3Key(entries []byte, off int) ([]byte, error) {
	if off < 0 || off+9 > len(entries) {
		return nil, fmt.Errorf("outerleaf: truncated entry")
	}
	shared := int(binary.LittleEndian.Uint16(entries[off : off+2]))
	suffixLen := int(binary.LittleEndian.Uint16(entries[off+2 : off+4]))
	payloadLen := int(binary.LittleEndian.Uint32(entries[off+5 : off+9]))
	if shared != 0 {
		return nil, fmt.Errorf("outerleaf: invalid shared prefix")
	}
	start := off + 9
	if start+suffixLen+payloadLen > len(entries) {
		return nil, fmt.Errorf("outerleaf: truncated entry payload")
	}
	return entries[start : start+suffixLen], nil
}

func decodeV3RestartKeysInto(entries []byte, restarts []uint32, dst [][]byte) ([][]byte, error) {
	if len(restarts) == 0 {
		return nil, nil
	}
	if cap(dst) < len(restarts) {
		dst = make([][]byte, len(restarts))
	} else {
		dst = dst[:len(restarts)]
	}
	keys := dst
	for i := range restarts {
		k, err := restartV3Key(entries, int(restarts[i]))
		if err != nil {
			return nil, err
		}
		if i > 0 && bytes.Compare(keys[i-1], k) >= 0 {
			return nil, fmt.Errorf("outerleaf: restart keys not strictly increasing")
		}
		keys[i] = k
	}
	return keys, nil
}

func decodeV3RestartKeys(entries []byte, restarts []uint32) ([][]byte, error) {
	return decodeV3RestartKeysInto(entries, restarts, nil)
}

func decodeV3RestartKeysPooled(entries []byte, restarts []uint32) ([][]byte, bool, error) {
	if len(restarts) == 0 {
		return nil, false, nil
	}
	buf := getPooledLeaseKeys(len(restarts))
	keys, err := decodeV3RestartKeysInto(entries, restarts, buf)
	if err != nil {
		putPooledLeaseKeys(buf)
		return nil, false, err
	}
	return keys, true, nil
}

func locateV3Restart(entries []byte, key []byte, restarts []uint32, restartKeys [][]byte) (int, error) {
	if len(restarts) == 0 {
		return -1, nil
	}
	if len(restartKeys) == len(restarts) {
		idx := sort.Search(len(restartKeys), func(i int) bool {
			return bytes.Compare(restartKeys[i], key) > 0
		}) - 1
		return idx, nil
	}
	lo, hi := 0, len(restarts)
	for lo < hi {
		mid := lo + (hi-lo)/2
		rk, err := restartV3Key(entries, int(restarts[mid]))
		if err != nil {
			return -1, err
		}
		if bytes.Compare(rk, key) > 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo - 1, nil
}

func lookupV3EntryRange(entries []byte, key []byte, start int, limit int) (LookupResult, bool, error) {
	if start < 0 || start > len(entries) || limit < start || limit > len(entries) {
		return LookupResult{}, false, fmt.Errorf("outerleaf: invalid entry range")
	}
	off := start
	prevCap := len(key)
	if prevCap < 64 {
		prevCap = 64
	}
	prev := make([]byte, 0, prevCap)
	curr := make([]byte, 0, prevCap)
	for off < limit {
		if off+9 > limit {
			return LookupResult{}, false, fmt.Errorf("outerleaf: truncated entry")
		}
		shared := int(binary.LittleEndian.Uint16(entries[off : off+2]))
		suffixLen := int(binary.LittleEndian.Uint16(entries[off+2 : off+4]))
		kind := EntryKind(entries[off+4])
		payloadLen := int(binary.LittleEndian.Uint32(entries[off+5 : off+9]))
		off += 9
		if shared > len(prev) {
			return LookupResult{}, false, fmt.Errorf("outerleaf: invalid shared prefix")
		}
		if off+suffixLen+payloadLen > limit {
			return LookupResult{}, false, fmt.Errorf("outerleaf: truncated entry payload")
		}
		curr = append(curr[:0], prev[:shared]...)
		curr = append(curr, entries[off:off+suffixLen]...)
		off += suffixLen
		payload := entries[off : off+payloadLen]
		off += payloadLen

		cmp := bytes.Compare(curr, key)
		if cmp == 0 {
			out := LookupResult{Kind: kind}
			switch kind {
			case EntryKindInline:
				out.Value = payload
			case EntryKindBlobRef:
				ptr, err := decodeValuePtr16(payload)
				if err != nil {
					return LookupResult{}, false, err
				}
				out.BlobPtr = ptr
			default:
				return LookupResult{}, false, fmt.Errorf("outerleaf: unknown entry kind %d", kind)
			}
			return out, true, nil
		}
		if cmp > 0 {
			return LookupResult{}, false, nil
		}
		prev, curr = curr, prev
	}
	return LookupResult{}, false, nil
}

func lookupV3Entry(entries []byte, entryCount int, key []byte, restarts []uint32, restartKeys [][]byte) (LookupResult, bool, error) {
	if entryCount <= 0 {
		return LookupResult{}, false, nil
	}
	if key == nil {
		_, first, err := decodeFirstV3(entries)
		if err != nil {
			return LookupResult{}, false, err
		}
		return first, true, nil
	}
	if len(restarts) == 0 {
		return lookupV3EntryRange(entries, key, 0, len(entries))
	}
	restartIdx, err := locateV3Restart(entries, key, restarts, restartKeys)
	if err != nil {
		return LookupResult{}, false, err
	}
	if restartIdx < 0 {
		return LookupResult{}, false, nil
	}
	start := int(restarts[restartIdx])
	limit := len(entries)
	if restartIdx+1 < len(restarts) {
		limit = int(restarts[restartIdx+1])
	}
	return lookupV3EntryRange(entries, key, start, limit)
}

func lookupV3EntryFromRestartRaw(entries []byte, entryCount int, key []byte, restartRaw []byte, restartCount int) (LookupResult, bool, error) {
	decodeMode, err := restartDecodeModeForCount(restartCount)
	if err != nil {
		return LookupResult{}, false, err
	}
	if decodeMode == restartDecodeModeNone {
		return lookupV3Entry(entries, entryCount, key, nil, nil)
	}
	if decodeMode == restartDecodeModeStack {
		var restartStack [restartDecodeStackCap]uint32
		restarts, err := decodeRestartsFromRawInto(entries, restartRaw, restartCount, restartStack[:0])
		if err != nil {
			return LookupResult{}, false, err
		}
		return lookupV3Entry(entries, entryCount, key, restarts, nil)
	}
	restarts, pooled, err := decodeRestartsFromRawPooled(entries, restartRaw, restartCount)
	if err != nil {
		return LookupResult{}, false, err
	}
	out, found, lookupErr := lookupV3Entry(entries, entryCount, key, restarts, nil)
	if pooled {
		putPooledRestarts(restarts)
	}
	if lookupErr != nil {
		return LookupResult{}, false, lookupErr
	}
	return out, found, nil
}

// DecodeBlock parses an outer-leaf payload and keeps the decoded data alive.
func DecodeBlock(payload []byte, scratch []byte) (*DecodedBlock, error) {
	return decodeBlock(payload, scratch, true)
}

// DecodeBlockWithVerify parses an outer-leaf payload and optionally verifies
// the block checksum.
func DecodeBlockWithVerify(payload []byte, scratch []byte, verifyChecksum bool) (*DecodedBlock, error) {
	return decodeBlock(payload, scratch, verifyChecksum)
}

// DecodeBlockLease parses an outer-leaf payload using lease-owned decode
// buffers. Callers must call Release on the returned block.
//
// Any slices returned by the decoded block may alias lease-owned storage and
// become invalid after Release.
func DecodeBlockLease(payload []byte) (*DecodedBlock, error) {
	return decodeBlockLease(payload, true)
}

// DecodeBlockLeaseWithVerify parses an outer-leaf payload using lease-owned
// decode buffers and optional checksum verification. Callers must call Release
// on the returned block.
//
// Any slices returned by the decoded block may alias lease-owned storage and
// become invalid after Release.
func DecodeBlockLeaseWithVerify(payload []byte, verifyChecksum bool) (*DecodedBlock, error) {
	return decodeBlockLease(payload, verifyChecksum)
}

// DecodeBlockLeaseWithScratchAndVerify parses an outer-leaf payload using
// caller-provided scratch and lease-owned decode buffers.
//
// It returns the decoded block and a caller-owned scratch slice to reuse on the
// next decode attempt.
//
// Any slices returned by the decoded block may alias lease-owned storage and
// become invalid after Release.
func DecodeBlockLeaseWithScratchAndVerify(payload []byte, scratch []byte, dst *DecodedBlock, verifyChecksum bool) (*DecodedBlock, []byte, error) {
	var nextScratch []byte
	if scratch != nil {
		nextScratch = scratch[:0]
	}
	block, err := decodeBlockMode(payload, scratch, verifyChecksum, true, dst)
	if err != nil {
		return nil, nextScratch, err
	}
	if block == nil {
		return nil, nextScratch, nil
	}
	// Compressed blocks decode into scratch/new backing; reuse that backing on
	// subsequent decodes. Raw codec (none) may alias payload bytes and should not
	// become reusable decode scratch.
	if len(payload) >= 6 && payload[5] != blockCodecNone && block.raw != nil {
		nextScratch = block.raw[:0]
		// Transfer ownership of pooled raw bytes to caller-managed scratch reuse.
		// block.Release must not recycle the same backing twice.
		if block.pooledRaw {
			block.pooledRaw = false
		}
	}
	return block, nextScratch, nil
}

// ReclaimTransferredScratchForRelease hands caller-owned scratch (returned by
// DecodeBlockLeaseWithScratchAndVerify) back to the block so Release can return
// it to the outerleaf bytes pool.
//
// It returns nil when ownership was reclaimed; otherwise it returns scratch
// unchanged.
func (d *DecodedBlock) ReclaimTransferredScratchForRelease(scratch []byte) []byte {
	if d == nil || len(d.raw) == 0 || cap(scratch) == 0 {
		return scratch
	}
	scratchView := scratch
	if len(scratchView) == 0 {
		scratchView = scratch[:cap(scratch)]
	}
	if &d.raw[0] != &scratchView[0] {
		return scratch
	}
	d.pooledRaw = true
	return nil
}

func decodeBlock(payload []byte, scratch []byte, verifyChecksum bool) (*DecodedBlock, error) {
	return decodeBlockMode(payload, scratch, verifyChecksum, false, nil)
}

func decodeBlockLease(payload []byte, verifyChecksum bool) (*DecodedBlock, error) {
	return decodeBlockMode(payload, nil, verifyChecksum, true, nil)
}

func prepareDecodedBlock(dst *DecodedBlock) *DecodedBlock {
	if dst == nil {
		return &DecodedBlock{}
	}
	*dst = DecodedBlock{}
	return dst
}

func decodeBlockMode(payload []byte, scratch []byte, verifyChecksum bool, leaseOwned bool, dst *DecodedBlock) (*DecodedBlock, error) {
	if len(payload) < blockHeaderSize {
		return nil, fmt.Errorf("outerleaf: truncated header")
	}
	if payload[0] != blockMagic[0] || payload[1] != blockMagic[1] || payload[2] != blockMagic[2] || payload[3] != blockMagic[3] {
		return nil, fmt.Errorf("outerleaf: invalid outer-leaf payload")
	}

	version := payload[4]
	codec := payload[5]
	switch version {
	case blockVersionV1:
		keyLen := int(binary.LittleEndian.Uint16(payload[blockV1KeyLenOff : blockV1KeyLenOff+2]))
		valueLen := int(binary.LittleEndian.Uint32(payload[blockV1ValueLenOff : blockV1ValueLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV1RawLenOff : blockV1RawLenOff+4]))
		expected := keyLen + valueLen
		if rawLen != expected {
			return nil, fmt.Errorf("outerleaf: invalid raw length %d want %d", rawLen, expected)
		}
		if rawLen < 0 || keyLen < 0 || valueLen < 0 {
			return nil, fmt.Errorf("outerleaf: invalid lengths")
		}
		outScratch, pooledRaw, err := decodeAndVerifyPayloadModeWithChecksumOwned(payload, rawLen, codec, scratch, true, verifyChecksum, leaseOwned)
		if err != nil {
			return nil, err
		}
		block := prepareDecodedBlock(dst)
		block.version = version
		block.entryCount = 1
		block.raw = outScratch
		block.entries = outScratch
		block.firstKey = outScratch[:keyLen]
		block.firstKind = EntryKindInline
		block.firstValue = outScratch[keyLen : keyLen+valueLen]
		block.leaseOwned = leaseOwned
		block.pooledRaw = pooledRaw
		return block, nil
	case blockVersionV2:
		entryCount := int(binary.LittleEndian.Uint16(payload[blockV2EntryCountOff : blockV2EntryCountOff+2]))
		entriesLen := int(binary.LittleEndian.Uint32(payload[blockV2EntriesLenOff : blockV2EntriesLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV2RawLenOff : blockV2RawLenOff+4]))
		if entryCount <= 0 {
			return nil, fmt.Errorf("outerleaf: empty v2 block")
		}
		if rawLen <= 0 {
			return nil, fmt.Errorf("outerleaf: invalid raw length %d", rawLen)
		}
		outScratch, pooledRaw, err := decodeAndVerifyPayloadModeWithChecksumOwned(payload, rawLen, codec, scratch, true, verifyChecksum, leaseOwned)
		if err != nil {
			return nil, err
		}
		restartInterval := int(binary.LittleEndian.Uint16(payload[6:8]))
		entries, restartRaw, restartCount, splitErr := splitV2RawMeta(outScratch, entriesLen, entryCount, restartInterval)
		if splitErr != nil {
			if pooledRaw {
				putPooledBytes(outScratch)
			}
			return nil, splitErr
		}
		firstKey, firstValue, err := decodeFirstV2(entries)
		if err != nil {
			if pooledRaw {
				putPooledBytes(outScratch)
			}
			return nil, err
		}
		block := prepareDecodedBlock(dst)
		block.version = version
		block.entryCount = entryCount
		block.raw = outScratch
		block.entries = entries
		block.restartRaw = restartRaw
		block.restartCount = restartCount
		block.firstKey = firstKey
		block.firstKind = EntryKindInline
		block.firstValue = firstValue
		block.leaseOwned = leaseOwned
		block.pooledRaw = pooledRaw
		return block, nil
	case blockVersionV3:
		entryCount := int(binary.LittleEndian.Uint16(payload[blockV2EntryCountOff : blockV2EntryCountOff+2]))
		entriesLen := int(binary.LittleEndian.Uint32(payload[blockV2EntriesLenOff : blockV2EntriesLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV2RawLenOff : blockV2RawLenOff+4]))
		if entryCount <= 0 {
			return nil, fmt.Errorf("outerleaf: empty v3 block")
		}
		if rawLen <= 0 {
			return nil, fmt.Errorf("outerleaf: invalid raw length %d", rawLen)
		}
		outScratch, pooledRaw, err := decodeAndVerifyPayloadModeWithChecksumOwned(payload, rawLen, codec, scratch, true, verifyChecksum, leaseOwned)
		if err != nil {
			return nil, err
		}
		restartInterval := int(binary.LittleEndian.Uint16(payload[6:8]))
		entries, restartRaw, restartCount, splitErr := splitV2RawMeta(outScratch, entriesLen, entryCount, restartInterval)
		if splitErr != nil {
			if pooledRaw {
				putPooledBytes(outScratch)
			}
			return nil, splitErr
		}
		firstKey, first, err := decodeFirstV3(entries)
		if err != nil {
			if pooledRaw {
				putPooledBytes(outScratch)
			}
			return nil, err
		}
		block := prepareDecodedBlock(dst)
		block.version = version
		block.entryCount = entryCount
		block.raw = outScratch
		block.entries = entries
		block.restartRaw = restartRaw
		block.restartCount = restartCount
		block.firstKey = firstKey
		block.firstKind = first.Kind
		block.firstValue = first.Value
		block.firstBlob = first.BlobPtr
		block.leaseOwned = leaseOwned
		block.pooledRaw = pooledRaw
		return block, nil
	default:
		return nil, fmt.Errorf("outerleaf: unsupported version %d", version)
	}
}

func (d *DecodedBlock) lookupRestartKeys() ([][]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("outerleaf: nil block")
	}
	if d.version != blockVersionV2 && d.version != blockVersionV3 {
		return nil, nil
	}
	restarts, err := d.lookupRestarts()
	if err != nil {
		return nil, err
	}
	d.restartKeysOnce.Do(func() {
		if len(restarts) == 0 {
			return
		}
		var (
			keys [][]byte
			err  error
		)
		if d.version == blockVersionV2 {
			if d.leaseOwned {
				var pooled bool
				keys, pooled, err = decodeV2RestartKeysPooled(d.entries, restarts)
				d.pooledRKeys = pooled
			} else {
				keys, err = decodeV2RestartKeys(d.entries, restarts)
			}
		} else {
			if d.leaseOwned {
				var pooled bool
				keys, pooled, err = decodeV3RestartKeysPooled(d.entries, restarts)
				d.pooledRKeys = pooled
			} else {
				keys, err = decodeV3RestartKeys(d.entries, restarts)
			}
		}
		if err != nil {
			d.restartKeysErr = err
			return
		}
		d.restartKeys = keys
	})
	if d.restartKeysErr != nil {
		return nil, d.restartKeysErr
	}
	return d.restartKeys, nil
}

func (d *DecodedBlock) lookupRestarts() ([]uint32, error) {
	if d == nil {
		return nil, fmt.Errorf("outerleaf: nil block")
	}
	if d.version != blockVersionV2 && d.version != blockVersionV3 {
		return nil, nil
	}
	d.restartsOnce.Do(func() {
		if d.restartCount == 0 {
			return
		}
		var (
			restarts []uint32
			pooled   bool
			err      error
		)
		if d.leaseOwned {
			restarts, pooled, err = decodeRestartsFromRawPooled(d.entries, d.restartRaw, d.restartCount)
			d.pooledRest = pooled
		} else {
			restarts, err = decodeRestartsFromRaw(d.entries, d.restartRaw, d.restartCount)
		}
		if err != nil {
			d.restartsErr = err
			return
		}
		d.restarts = restarts
	})
	if d.restartsErr != nil {
		return nil, d.restartsErr
	}
	return d.restarts, nil
}

// Release returns lease-owned decode buffers back to outerleaf pools.
// Any slices previously returned by this block become invalid after Release.
// It is safe to call Release multiple times.
func (d *DecodedBlock) Release() {
	if d == nil {
		return
	}
	if !d.leaseOwned && !d.pooledRaw && !d.pooledRest && !d.pooledRKeys {
		return
	}
	if d.pooledRKeys {
		putPooledLeaseKeys(d.restartKeys)
		d.pooledRKeys = false
	}
	if d.pooledRest {
		putPooledRestarts(d.restarts)
		d.pooledRest = false
	}
	if d.pooledRaw {
		putPooledBytes(d.raw)
		d.pooledRaw = false
	}
	if d.visitLease != nil {
		releaseKeyLease(d.visitLease)
		d.visitLease = nil
	}
	d.leaseOwned = false
	d.raw = nil
	d.entries = nil
	d.restartRaw = nil
	d.restarts = nil
	d.restartKeys = nil
	d.firstKey = nil
	d.firstValue = nil
	d.firstBlob = page.ValuePtr{}
	d.keys = nil
}

// FirstKind reports the payload kind of the first logical entry.
func (d *DecodedBlock) FirstKind() EntryKind {
	if d == nil {
		return EntryKindInline
	}
	if d.version == blockVersionV3 {
		return d.firstKind
	}
	return EntryKindInline
}

// FirstValue returns the first value stored in the decoded block.
func (d *DecodedBlock) FirstValue() ([]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("outerleaf: nil block")
	}
	switch d.version {
	case blockVersionV1:
		if d.firstValue == nil {
			return nil, fmt.Errorf("outerleaf: missing first value")
		}
		return d.firstValue, nil
	case blockVersionV2:
		if d.firstValue != nil {
			return d.firstValue, nil
		}
		restartKeys, err := d.lookupRestartKeys()
		if err != nil {
			return nil, err
		}
		restarts, err := d.lookupRestarts()
		if err != nil {
			return nil, err
		}
		val, _, err := lookupV2Value(d.entries, d.entryCount, nil, restarts, restartKeys)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, fmt.Errorf("outerleaf: empty block")
		}
		return val, nil
	case blockVersionV3:
		if d.firstKind == EntryKindBlobRef {
			return nil, ErrBlobRefEntry
		}
		if d.firstValue != nil {
			return d.firstValue, nil
		}
		restartKeys, err := d.lookupRestartKeys()
		if err != nil {
			return nil, err
		}
		restarts, err := d.lookupRestarts()
		if err != nil {
			return nil, err
		}
		entry, found, err := lookupV3Entry(d.entries, d.entryCount, nil, restarts, restartKeys)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("outerleaf: empty block")
		}
		if entry.Kind == EntryKindBlobRef {
			return nil, ErrBlobRefEntry
		}
		return entry.Value, nil
	default:
		return nil, fmt.Errorf("outerleaf: unsupported version %d", d.version)
	}
}

// ValueForKey resolves a value for the provided key inside the decoded block.
func (d *DecodedBlock) ValueForKey(key []byte) ([]byte, bool, error) {
	if d == nil {
		return nil, false, fmt.Errorf("outerleaf: nil block")
	}
	switch d.version {
	case blockVersionV1:
		if key == nil || bytes.Equal(d.firstKey, key) {
			return d.firstValue, true, nil
		}
		return nil, false, nil
	case blockVersionV2:
		if key == nil {
			if d.firstValue != nil {
				return d.firstValue, true, nil
			}
			restarts, err := d.lookupRestarts()
			if err != nil {
				return nil, false, err
			}
			restartKeys := d.restartKeys
			if len(restartKeys) != len(restarts) {
				restartKeys = nil
			}
			return lookupV2Value(d.entries, d.entryCount, nil, restarts, restartKeys)
		}
		if d.entryCount <= linearScanMaxEntries {
			return lookupV2ValueRange(d.entries, key, 0, len(d.entries))
		}
		restarts, err := d.lookupRestarts()
		if err != nil {
			return nil, false, err
		}
		restartKeys := d.restartKeys
		if len(restartKeys) != len(restarts) {
			restartKeys = nil
		}
		val, found, err := lookupV2Value(d.entries, d.entryCount, key, restarts, restartKeys)
		return val, found, err
	case blockVersionV3:
		entry, found, err := d.EntryForKey(key)
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, nil
		}
		if entry.Kind == EntryKindBlobRef {
			return nil, true, ErrBlobRefEntry
		}
		return entry.Value, true, nil
	default:
		return nil, false, fmt.Errorf("outerleaf: unsupported version %d", d.version)
	}
}

// EntryForKeyNoRestartKeys resolves key inside a decoded block without
// materializing restart key slices.
func (d *DecodedBlock) EntryForKeyNoRestartKeys(key []byte) (LookupResult, bool, error) {
	if d == nil {
		return LookupResult{}, false, fmt.Errorf("outerleaf: nil block")
	}
	switch d.version {
	case blockVersionV1:
		if key == nil || bytes.Equal(d.firstKey, key) {
			return LookupResult{Kind: EntryKindInline, Value: d.firstValue}, true, nil
		}
		return LookupResult{}, false, nil
	case blockVersionV2:
		if key == nil {
			if d.firstValue == nil {
				return LookupResult{}, false, fmt.Errorf("outerleaf: empty block")
			}
			return LookupResult{Kind: EntryKindInline, Value: d.firstValue}, true, nil
		}
		if d.entryCount <= linearScanMaxEntries {
			val, found, err := lookupV2ValueRange(d.entries, key, 0, len(d.entries))
			if err != nil {
				return LookupResult{}, false, err
			}
			if !found {
				return LookupResult{}, false, nil
			}
			return LookupResult{Kind: EntryKindInline, Value: val}, true, nil
		}
		restarts, err := d.lookupRestarts()
		if err != nil {
			return LookupResult{}, false, err
		}
		val, found, err := lookupV2Value(d.entries, d.entryCount, key, restarts, nil)
		if err != nil {
			return LookupResult{}, false, err
		}
		if !found {
			return LookupResult{}, false, nil
		}
		return LookupResult{Kind: EntryKindInline, Value: val}, true, nil
	case blockVersionV3:
		if key == nil {
			if d.firstKind == EntryKindBlobRef {
				return LookupResult{Kind: EntryKindBlobRef, BlobPtr: d.firstBlob}, true, nil
			}
			if d.firstValue != nil {
				return LookupResult{Kind: EntryKindInline, Value: d.firstValue}, true, nil
			}
		}
		restarts, err := d.lookupRestarts()
		if err != nil {
			return LookupResult{}, false, err
		}
		return lookupV3Entry(d.entries, d.entryCount, key, restarts, nil)
	default:
		return LookupResult{}, false, fmt.Errorf("outerleaf: unsupported version %d", d.version)
	}
}

// EntryForKey resolves key inside a decoded block and returns typed payload data.
func (d *DecodedBlock) EntryForKey(key []byte) (LookupResult, bool, error) {
	if d == nil {
		return LookupResult{}, false, fmt.Errorf("outerleaf: nil block")
	}
	switch d.version {
	case blockVersionV1:
		if key == nil || bytes.Equal(d.firstKey, key) {
			return LookupResult{Kind: EntryKindInline, Value: d.firstValue}, true, nil
		}
		return LookupResult{}, false, nil
	case blockVersionV2:
		val, found, err := d.ValueForKey(key)
		if err != nil {
			return LookupResult{}, false, err
		}
		if !found {
			return LookupResult{}, false, nil
		}
		return LookupResult{Kind: EntryKindInline, Value: val}, true, nil
	case blockVersionV3:
		if key == nil {
			if d.firstKind == EntryKindBlobRef {
				return LookupResult{Kind: EntryKindBlobRef, BlobPtr: d.firstBlob}, true, nil
			}
			if d.firstValue != nil {
				return LookupResult{Kind: EntryKindInline, Value: d.firstValue}, true, nil
			}
		}
		if d.restartKeysErr != nil {
			return LookupResult{}, false, d.restartKeysErr
		}
		restarts, err := d.lookupRestarts()
		if err != nil {
			return LookupResult{}, false, err
		}
		restartKeys := d.restartKeys
		if len(restartKeys) != len(restarts) {
			restartKeys = nil
		}
		return lookupV3Entry(d.entries, d.entryCount, key, restarts, restartKeys)
	default:
		return LookupResult{}, false, fmt.Errorf("outerleaf: unsupported version %d", d.version)
	}
}

// VisitTypedEntries iterates over decoded logical entries and calls fn for each.
// Keys and inline values alias decoded block storage; blob refs are surfaced via ptr.
//
// This avoids allocating a full []TypedEntry slice for callers that only need
// to stream through entries.
func (d *DecodedBlock) VisitTypedEntries(fn func(key []byte, kind EntryKind, value []byte, ptr page.ValuePtr) error) error {
	if d == nil {
		return fmt.Errorf("outerleaf: nil block")
	}
	if fn == nil {
		return nil
	}
	switch d.version {
	case blockVersionV1:
		if d.firstKey == nil || d.firstValue == nil {
			return fmt.Errorf("outerleaf: missing v1 entry")
		}
		return fn(d.firstKey, EntryKindInline, d.firstValue, page.ValuePtr{})
	case blockVersionV2:
		encoded := d.entries
		off := 0
		var prevKey []byte
		var keyArena []byte
		usePooledArena := d.leaseOwned && d.visitLease == nil
		if usePooledArena {
			d.visitLease = acquireKeyLease(1)
		}
		for i := 0; i < d.entryCount; i++ {
			if off+8 > len(encoded) {
				return fmt.Errorf("outerleaf: truncated v2 entry header")
			}
			shared := int(binary.LittleEndian.Uint16(encoded[off : off+2]))
			suffixLen := int(binary.LittleEndian.Uint16(encoded[off+2 : off+4]))
			valueLen := int(binary.LittleEndian.Uint32(encoded[off+4 : off+8]))
			off += 8
			if shared < 0 || shared > len(prevKey) {
				return fmt.Errorf("outerleaf: invalid shared prefix")
			}
			if suffixLen < 0 || off+suffixLen > len(encoded) {
				return fmt.Errorf("outerleaf: invalid key suffix length")
			}
			suffix := encoded[off : off+suffixLen]
			off += suffixLen

			if valueLen < 0 || off+valueLen > len(encoded) {
				return fmt.Errorf("outerleaf: invalid value length")
			}
			value := encoded[off : off+valueLen]
			off += valueLen

			var key []byte
			if shared == 0 {
				key = suffix
			} else {
				keyLen := shared + suffixLen
				need := len(keyArena) + keyLen
				if need > cap(keyArena) {
					newCap := cap(keyArena) * 2
					if newCap < keyLen {
						newCap = keyLen
					}
					if newCap < 64 {
						newCap = 64
					}
					if usePooledArena {
						keyArena = getPooledLeaseArena(newCap)
						d.visitLease.addChunk(keyArena)
					} else {
						keyArena = make([]byte, 0, newCap)
					}
					need = keyLen
				}
				arenaOff := len(keyArena)
				keyArena = keyArena[:need]
				key = keyArena[arenaOff:need]
				copy(key[:shared], prevKey[:shared])
				copy(key[shared:], suffix)
			}
			if err := fn(key, EntryKindInline, value, page.ValuePtr{}); err != nil {
				return err
			}
			prevKey = key
		}
		if off != len(encoded) {
			return fmt.Errorf("outerleaf: trailing v2 entry bytes")
		}
		return nil
	case blockVersionV3:
		encoded := d.entries
		off := 0
		var prevKey []byte
		var keyArena []byte
		usePooledArena := d.leaseOwned && d.visitLease == nil
		if usePooledArena {
			d.visitLease = acquireKeyLease(1)
		}
		for i := 0; i < d.entryCount; i++ {
			if off+9 > len(encoded) {
				return fmt.Errorf("outerleaf: truncated v3 entry header")
			}
			shared := int(binary.LittleEndian.Uint16(encoded[off : off+2]))
			suffixLen := int(binary.LittleEndian.Uint16(encoded[off+2 : off+4]))
			kind := EntryKind(encoded[off+4])
			payloadLen := int(binary.LittleEndian.Uint32(encoded[off+5 : off+9]))
			off += 9
			if shared < 0 || shared > len(prevKey) {
				return fmt.Errorf("outerleaf: invalid shared prefix")
			}
			if suffixLen < 0 || off+suffixLen > len(encoded) {
				return fmt.Errorf("outerleaf: invalid key suffix length")
			}
			suffix := encoded[off : off+suffixLen]
			off += suffixLen
			if payloadLen < 0 || off+payloadLen > len(encoded) {
				return fmt.Errorf("outerleaf: invalid value length")
			}
			payload := encoded[off : off+payloadLen]
			off += payloadLen

			var key []byte
			if shared == 0 {
				key = suffix
			} else {
				keyLen := shared + suffixLen
				need := len(keyArena) + keyLen
				if need > cap(keyArena) {
					newCap := cap(keyArena) * 2
					if newCap < keyLen {
						newCap = keyLen
					}
					if newCap < 64 {
						newCap = 64
					}
					if usePooledArena {
						keyArena = getPooledLeaseArena(newCap)
						d.visitLease.addChunk(keyArena)
					} else {
						keyArena = make([]byte, 0, newCap)
					}
					need = keyLen
				}
				arenaOff := len(keyArena)
				keyArena = keyArena[:need]
				key = keyArena[arenaOff:need]
				copy(key[:shared], prevKey[:shared])
				copy(key[shared:], suffix)
			}

			switch kind {
			case EntryKindInline:
				if err := fn(key, kind, payload, page.ValuePtr{}); err != nil {
					return err
				}
			case EntryKindBlobRef:
				ptr, err := decodeValuePtr16(payload)
				if err != nil {
					return err
				}
				if err := fn(key, kind, nil, ptr); err != nil {
					return err
				}
			default:
				return fmt.Errorf("outerleaf: unknown entry kind %d", kind)
			}
			prevKey = key
		}
		if off != len(encoded) {
			return fmt.Errorf("outerleaf: trailing v3 entry bytes")
		}
		return nil
	default:
		return fmt.Errorf("outerleaf: unsupported version %d", d.version)
	}
}

// Entries decodes all logical key/value records from a parsed block.
// Returned keys/values reference decoded block storage where possible.
func (d *DecodedBlock) Entries(dst []Entry) ([]Entry, error) {
	if d == nil {
		return nil, fmt.Errorf("outerleaf: nil block")
	}
	if cap(dst) < d.entryCount {
		dst = make([]Entry, 0, d.entryCount)
	} else {
		dst = dst[:0]
	}
	err := d.VisitTypedEntries(func(key []byte, kind EntryKind, value []byte, ptr page.ValuePtr) error {
		if kind == EntryKindBlobRef {
			return ErrBlobRefEntry
		}
		dst = append(dst, Entry{Key: key, Value: value})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dst, nil
}

// TypedEntries decodes all logical records from a parsed block with kind info.
func (d *DecodedBlock) TypedEntries(dst []TypedEntry) ([]TypedEntry, error) {
	if d == nil {
		return nil, fmt.Errorf("outerleaf: nil block")
	}
	if cap(dst) < d.entryCount {
		dst = make([]TypedEntry, 0, d.entryCount)
	} else {
		dst = dst[:0]
	}
	err := d.VisitTypedEntries(func(key []byte, kind EntryKind, value []byte, ptr page.ValuePtr) error {
		dst = append(dst, TypedEntry{
			Key:     key,
			Kind:    kind,
			Value:   value,
			BlobPtr: ptr,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dst, nil
}

// Keys decodes all logical keys from a parsed block.
// Returned key slices reference decoded block storage where possible.
func (d *DecodedBlock) Keys(dst [][]byte) ([][]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("outerleaf: nil block")
	}
	switch d.version {
	case blockVersionV1:
		if cap(dst) < d.entryCount {
			dst = make([][]byte, 0, d.entryCount)
		} else {
			dst = dst[:0]
		}
		if d.firstKey == nil {
			return nil, fmt.Errorf("outerleaf: missing v1 key")
		}
		dst = append(dst, d.firstKey)
		return dst, nil
	case blockVersionV2, blockVersionV3:
		keys, err := d.cachedStructuredKeys()
		if err != nil {
			return nil, err
		}
		if dst == nil {
			return keys, nil
		}
		if cap(dst) < len(keys) {
			dst = make([][]byte, len(keys))
		} else {
			dst = dst[:len(keys)]
		}
		copy(dst, keys)
		return dst, nil
	default:
		return nil, fmt.Errorf("outerleaf: unsupported version %d", d.version)
	}
}

// KeysRange decodes only keys in [lower, upper) from a parsed block.
// Returned key slices reference decoded block storage where possible.
func (d *DecodedBlock) KeysRange(dst [][]byte, lower []byte, upper []byte) ([][]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("outerleaf: nil block")
	}
	if lower == nil && upper == nil {
		return d.Keys(dst)
	}
	if lower != nil && upper != nil && bytes.Compare(lower, upper) >= 0 {
		if dst == nil {
			return nil, nil
		}
		return dst[:0], nil
	}
	switch d.version {
	case blockVersionV1:
		if d.firstKey == nil {
			return nil, fmt.Errorf("outerleaf: missing v1 key")
		}
		if !keyWithinRange(d.firstKey, lower, upper) {
			if dst == nil {
				return nil, nil
			}
			return dst[:0], nil
		}
		if cap(dst) < 1 {
			dst = make([][]byte, 1)
		} else {
			dst = dst[:1]
		}
		dst[0] = d.firstKey
		return dst, nil
	case blockVersionV2, blockVersionV3:
		keys, err := decodeStructuredKeysBounded(d.version, d.entryCount, d.entries, lower, upper)
		if err != nil {
			return nil, err
		}
		if dst == nil {
			return keys, nil
		}
		if cap(dst) < len(keys) {
			dst = make([][]byte, len(keys))
		} else {
			dst = dst[:len(keys)]
		}
		copy(dst, keys)
		return dst, nil
	default:
		return nil, fmt.Errorf("outerleaf: unsupported version %d", d.version)
	}
}

// KeysRangeLease decodes keys in [lower, upper) and returns them via a lease.
// Callers must release the lease when finished.
func (d *DecodedBlock) KeysRangeLease(lower []byte, upper []byte) (*KeyLease, error) {
	if d == nil {
		return nil, fmt.Errorf("outerleaf: nil block")
	}
	if lower != nil && upper != nil && bytes.Compare(lower, upper) >= 0 {
		return nil, nil
	}
	switch d.version {
	case blockVersionV1:
		if d.firstKey == nil {
			return nil, fmt.Errorf("outerleaf: missing v1 key")
		}
		if !keyWithinRange(d.firstKey, lower, upper) {
			return nil, nil
		}
		lease := acquireKeyLease(1)
		lease.keys = append(lease.keys, d.firstKey)
		return lease, nil
	case blockVersionV2, blockVersionV3:
		return decodeStructuredKeysBoundedLease(d.version, d.entryCount, d.entries, lower, upper)
	default:
		return nil, fmt.Errorf("outerleaf: unsupported version %d", d.version)
	}
}

// LowerBound returns the first entry index whose key is >= target and classifies
// whether target falls below the block range, within it, or above it.
func (d *DecodedBlock) LowerBound(target []byte) (pos int, below bool, above bool, err error) {
	if d == nil {
		return 0, false, false, fmt.Errorf("outerleaf: nil block")
	}
	switch d.version {
	case blockVersionV1:
		if d.firstKey == nil {
			return 0, false, false, fmt.Errorf("outerleaf: missing v1 key")
		}
		if target == nil {
			return 0, false, false, nil
		}
		cmp := bytes.Compare(d.firstKey, target)
		if cmp > 0 {
			return 0, true, false, nil
		}
		if cmp < 0 {
			return 1, false, true, nil
		}
		return 0, false, false, nil
	case blockVersionV2, blockVersionV3:
		return lowerBoundStructured(d.version, d.entryCount, d.entries, target)
	default:
		return 0, false, false, fmt.Errorf("outerleaf: unsupported version %d", d.version)
	}
}

func (d *DecodedBlock) cachedStructuredKeys() ([][]byte, error) {
	d.keysOnce.Do(func() {
		keys, err := decodeStructuredKeys(d.version, d.entryCount, d.entries)
		if err != nil {
			d.keysErr = err
			return
		}
		d.keys = keys
	})
	if d.keysErr != nil {
		return nil, d.keysErr
	}
	return d.keys, nil
}

func lowerBoundStructured(version uint8, entryCount int, encoded []byte, target []byte) (pos int, below bool, above bool, err error) {
	if entryCount <= 0 {
		return 0, false, false, fmt.Errorf("outerleaf: empty v%d block", version)
	}
	if target == nil {
		return 0, false, false, nil
	}
	headerLen := 8
	valueLenOffDelta := 4
	if version == blockVersionV3 {
		headerLen = 9
		valueLenOffDelta = 5
	}

	off := 0
	const lowerBoundStackKeyCap = 64
	var prevStack [lowerBoundStackKeyCap]byte
	var currStack [lowerBoundStackKeyCap]byte
	prev := prevStack[:0]
	curr := currStack[:0]
	for i := 0; i < entryCount; i++ {
		if off+headerLen > len(encoded) {
			return 0, false, false, fmt.Errorf("outerleaf: truncated v%d entry header", version)
		}
		shared := int(binary.LittleEndian.Uint16(encoded[off : off+2]))
		suffixLen := int(binary.LittleEndian.Uint16(encoded[off+2 : off+4]))
		valueLen := int(binary.LittleEndian.Uint32(encoded[off+valueLenOffDelta : off+valueLenOffDelta+4]))
		off += headerLen
		if shared < 0 || shared > len(prev) {
			return 0, false, false, fmt.Errorf("outerleaf: invalid shared prefix")
		}
		if suffixLen < 0 || off+suffixLen > len(encoded) {
			return 0, false, false, fmt.Errorf("outerleaf: invalid key suffix length")
		}
		suffix := encoded[off : off+suffixLen]
		off += suffixLen
		if valueLen < 0 || off+valueLen > len(encoded) {
			return 0, false, false, fmt.Errorf("outerleaf: invalid value length")
		}

		keyLen := shared + suffixLen
		if keyLen < 0 {
			return 0, false, false, fmt.Errorf("outerleaf: invalid key length")
		}
		if cap(curr) < keyLen {
			newCap := cap(curr) * 2
			if newCap < keyLen {
				newCap = keyLen
			}
			if newCap < lowerBoundStackKeyCap {
				newCap = lowerBoundStackKeyCap
			}
			curr = make([]byte, keyLen, newCap)
		} else {
			curr = curr[:keyLen]
		}
		if shared > 0 {
			copy(curr[:shared], prev[:shared])
		}
		copy(curr[shared:], suffix)

		cmp := bytes.Compare(curr, target)
		if cmp >= 0 {
			if i == 0 && cmp > 0 {
				return i, true, false, nil
			}
			return i, false, false, nil
		}

		off += valueLen
		prev, curr = curr, prev[:0]
	}
	if off != len(encoded) {
		return 0, false, false, fmt.Errorf("outerleaf: trailing v%d entry bytes", version)
	}
	return entryCount, false, true, nil
}

func decodeStructuredKeys(version uint8, entryCount int, encoded []byte) ([][]byte, error) {
	headerLen := 8
	if version == blockVersionV3 {
		headerLen = 9
	}
	keys := make([][]byte, entryCount)
	off := 0
	estKeyLen := 16
	if len(encoded) >= headerLen {
		if v := int(binary.LittleEndian.Uint16(encoded[2:4])); v > 0 {
			if v > 128 {
				v = 128
			}
			estKeyLen = v
		}
	}
	estCap := entryCount * estKeyLen
	if estCap < 64 {
		estCap = 64
	}
	keyArena := make([]byte, 0, estCap)
	var prevKey []byte
	for i := 0; i < entryCount; i++ {
		if off+headerLen > len(encoded) {
			return nil, fmt.Errorf("outerleaf: truncated v%d entry header", version)
		}
		shared := int(binary.LittleEndian.Uint16(encoded[off : off+2]))
		suffixLen := int(binary.LittleEndian.Uint16(encoded[off+2 : off+4]))
		valueLenOff := off + 4
		if version == blockVersionV3 {
			valueLenOff = off + 5
		}
		valueLen := int(binary.LittleEndian.Uint32(encoded[valueLenOff : valueLenOff+4]))
		off += headerLen
		if shared < 0 || shared > len(prevKey) {
			return nil, fmt.Errorf("outerleaf: invalid shared prefix")
		}
		if suffixLen < 0 || off+suffixLen > len(encoded) {
			return nil, fmt.Errorf("outerleaf: invalid key suffix length")
		}
		suffix := encoded[off : off+suffixLen]
		off += suffixLen
		if valueLen < 0 || off+valueLen > len(encoded) {
			return nil, fmt.Errorf("outerleaf: invalid value length")
		}
		off += valueLen

		keyLen := shared + suffixLen
		if keyLen < 0 {
			return nil, fmt.Errorf("outerleaf: invalid key length")
		}
		needCap := len(keyArena) + keyLen
		if needCap > cap(keyArena) {
			newCap := cap(keyArena) * 2
			if newCap < keyLen {
				newCap = keyLen
			}
			// Existing key slices already reference older arena segments; avoid
			// copying historical bytes when rolling to a fresh segment.
			keyArena = make([]byte, 0, newCap)
			needCap = keyLen
		}
		arenaOff := len(keyArena)
		keyArena = keyArena[:needCap]
		key := keyArena[arenaOff:needCap]
		if shared > 0 {
			copy(key[:shared], prevKey[:shared])
		}
		copy(key[shared:], suffix)
		keys[i] = key
		prevKey = key
	}
	if off != len(encoded) {
		return nil, fmt.Errorf("outerleaf: trailing v%d entry bytes", version)
	}
	return keys, nil
}

func keyWithinRange(key []byte, lower []byte, upper []byte) bool {
	if lower != nil && bytes.Compare(key, lower) < 0 {
		return false
	}
	if upper != nil && bytes.Compare(key, upper) >= 0 {
		return false
	}
	return true
}

// decodeStructuredKeysBounded reconstructs keys in [lower, upper) with reduced allocation pressure.
func decodeStructuredKeysBounded(version uint8, entryCount int, encoded []byte, lower []byte, upper []byte) ([][]byte, error) {
	lease, err := decodeStructuredKeysBoundedLease(version, entryCount, encoded, lower, upper)
	if err != nil {
		return nil, err
	}
	if lease == nil {
		return nil, nil
	}
	keys := cloneKeySlices(lease.keys)
	lease.Release()
	return keys, nil
}

func decodeStructuredKeysBoundedLease(version uint8, entryCount int, encoded []byte, lower []byte, upper []byte) (*KeyLease, error) {
	if entryCount <= 0 {
		return nil, fmt.Errorf("outerleaf: empty v%d block", version)
	}
	if lower != nil && upper != nil && bytes.Compare(lower, upper) >= 0 {
		return nil, nil
	}

	headerLen := 8
	valueLenOffset := 4
	if version == blockVersionV3 {
		headerLen = 9
		valueLenOffset = 5
	}

	off := 0
	estKeyLen := 16
	if len(encoded) >= 4 {
		if v := int(uint16(encoded[2]) | uint16(encoded[3])<<8); v > 0 {
			if v > 128 {
				v = 128
			}
			estKeyLen = v
		}
	}

	selectedCap := entryCount
	hasBounds := lower != nil || upper != nil
	if hasBounds {
		selectedCap = entryCount / 8
		if selectedCap < 4 {
			selectedCap = 4
		}
		if selectedCap > entryCount {
			selectedCap = entryCount
		}
	}
	lease := acquireKeyLease(selectedCap)
	released := false
	defer func() {
		if !released {
			releaseKeyLease(lease)
		}
	}()

	estCap := selectedCap * estKeyLen
	if estCap < estKeyLen {
		estCap = estKeyLen
	}

	prev, prevPooled := borrowBytes(estKeyLen, nil)
	defer func() {
		releaseBytes(prev, nil, prevPooled)
	}()

	var keyArena []byte

	for i := 0; i < entryCount; i++ {
		if off+headerLen > len(encoded) {
			return nil, fmt.Errorf("outerleaf: truncated v%d entry header", version)
		}

		// Hot decode loop: inline little-endian loads avoid binary.LittleEndian
		// call overhead in prefix-scan profiles.
		shared := int(uint16(encoded[off]) | uint16(encoded[off+1])<<8)
		suffixLen := int(uint16(encoded[off+2]) | uint16(encoded[off+3])<<8)
		vlOff := off + valueLenOffset
		valueLen := int(uint32(encoded[vlOff]) | uint32(encoded[vlOff+1])<<8 | uint32(encoded[vlOff+2])<<16 | uint32(encoded[vlOff+3])<<24)

		off += headerLen

		if shared < 0 || shared > len(prev) {
			return nil, fmt.Errorf("outerleaf: invalid shared prefix")
		}
		if suffixLen < 0 || off+suffixLen > len(encoded) {
			return nil, fmt.Errorf("outerleaf: invalid key suffix length")
		}

		keyLen := shared + suffixLen
		if keyLen < 0 {
			return nil, fmt.Errorf("outerleaf: invalid key length")
		}
		if cap(prev) < keyLen {
			prev = growPooledBytes(prev, keyLen)
		} else {
			prev = prev[:keyLen]
		}
		copy(prev[shared:], encoded[off:off+suffixLen])

		off += suffixLen
		if valueLen < 0 || off+valueLen > len(encoded) {
			return nil, fmt.Errorf("outerleaf: invalid value length")
		}
		off += valueLen

		if hasBounds {
			if upper != nil && bytes.Compare(prev, upper) >= 0 {
				if len(lease.keys) == 0 {
					released = true
					releaseKeyLease(lease)
					return nil, nil
				}
				released = true
				return lease, nil
			}
			if lower != nil && bytes.Compare(prev, lower) < 0 {
				continue
			}
		}

		if keyLen == 0 {
			lease.keys = append(lease.keys, []byte{})
			continue
		}

		needCap := len(keyArena) + keyLen
		if needCap > cap(keyArena) {
			newCap := cap(keyArena) * 2
			if newCap < needCap {
				newCap = needCap
			}
			if keyArena == nil && newCap < estCap {
				newCap = estCap
			}
			keyArena = getPooledLeaseArena(newCap)
			lease.addChunk(keyArena)
			needCap = keyLen
		}

		arenaOff := len(keyArena)
		keyArena = keyArena[:needCap]
		key := keyArena[arenaOff:needCap]
		copy(key, prev)
		lease.keys = append(lease.keys, key)
	}

	if off != len(encoded) {
		return nil, fmt.Errorf("outerleaf: trailing v%d entry bytes", version)
	}
	if len(lease.keys) == 0 {
		released = true
		releaseKeyLease(lease)
		return nil, nil
	}
	released = true
	return lease, nil
}

// DecodeKeysWithVerify decodes logical keys from an encoded outer-leaf payload.
func DecodeKeysWithVerify(payload []byte, verifyChecksum bool) ([][]byte, error) {
	if len(payload) < blockHeaderSize {
		return nil, fmt.Errorf("outerleaf: truncated header")
	}
	if payload[0] != blockMagic[0] || payload[1] != blockMagic[1] || payload[2] != blockMagic[2] || payload[3] != blockMagic[3] {
		return nil, fmt.Errorf("outerleaf: invalid outer-leaf payload")
	}

	version := payload[4]
	codec := payload[5]
	switch version {
	case blockVersionV1:
		keyLen := int(binary.LittleEndian.Uint16(payload[blockV1KeyLenOff : blockV1KeyLenOff+2]))
		valueLen := int(binary.LittleEndian.Uint32(payload[blockV1ValueLenOff : blockV1ValueLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV1RawLenOff : blockV1RawLenOff+4]))
		expected := keyLen + valueLen
		if rawLen != expected {
			return nil, fmt.Errorf("outerleaf: invalid raw length %d want %d", rawLen, expected)
		}
		raw, err := decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, nil, true, verifyChecksum)
		if err != nil {
			return nil, err
		}
		if keyLen < 0 || keyLen > len(raw) {
			return nil, fmt.Errorf("outerleaf: invalid key length %d", keyLen)
		}
		key := make([]byte, keyLen)
		copy(key, raw[:keyLen])
		return [][]byte{key}, nil
	case blockVersionV2, blockVersionV3:
		entryCount := int(binary.LittleEndian.Uint16(payload[blockV2EntryCountOff : blockV2EntryCountOff+2]))
		entriesLen := int(binary.LittleEndian.Uint32(payload[blockV2EntriesLenOff : blockV2EntriesLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV2RawLenOff : blockV2RawLenOff+4]))
		if entryCount <= 0 {
			return nil, fmt.Errorf("outerleaf: empty v%d block", version)
		}
		if rawLen <= 0 {
			return nil, fmt.Errorf("outerleaf: invalid raw length %d", rawLen)
		}
		restartInterval := int(binary.LittleEndian.Uint16(payload[6:8]))
		if codec == blockCodecNone {
			raw, err := decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, nil, true, verifyChecksum)
			if err != nil {
				return nil, err
			}
			entries, _, _, err := splitV2RawMeta(raw, entriesLen, entryCount, restartInterval)
			if err != nil {
				return nil, err
			}
			return decodeStructuredKeys(version, entryCount, entries)
		}
		scratch := getPooledBytes(rawLen)
		defer putPooledBytes(scratch)
		raw, err := decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, scratch[:0], true, verifyChecksum)
		if err != nil {
			return nil, err
		}
		entries, _, _, err := splitV2RawMeta(raw, entriesLen, entryCount, restartInterval)
		if err != nil {
			return nil, err
		}
		return decodeStructuredKeys(version, entryCount, entries)
	default:
		return nil, fmt.Errorf("outerleaf: unsupported version %d", version)
	}
}

// DecodeKeysRangeWithVerify decodes only keys in [lower, upper) from an
// encoded outer-leaf payload.
func DecodeKeysRangeWithVerify(payload []byte, lower []byte, upper []byte, verifyChecksum bool) ([][]byte, error) {
	lease, err := DecodeKeysRangeLeaseWithVerify(payload, lower, upper, verifyChecksum)
	if err != nil {
		return nil, err
	}
	if lease == nil {
		return nil, nil
	}
	keys := cloneKeySlices(lease.keys)
	lease.Release()
	return keys, nil
}

// DecodeKeysRangeLeaseWithVerify decodes keys in [lower, upper) and returns a
// lease that must be released by the caller.
func DecodeKeysRangeLeaseWithVerify(payload []byte, lower []byte, upper []byte, verifyChecksum bool) (*KeyLease, error) {
	if lower != nil && upper != nil && bytes.Compare(lower, upper) >= 0 {
		return nil, nil
	}
	if len(payload) < blockHeaderSize {
		return nil, fmt.Errorf("outerleaf: truncated header")
	}
	if payload[0] != blockMagic[0] || payload[1] != blockMagic[1] || payload[2] != blockMagic[2] || payload[3] != blockMagic[3] {
		return nil, fmt.Errorf("outerleaf: invalid outer-leaf payload")
	}

	version := payload[4]
	codec := payload[5]
	switch version {
	case blockVersionV1:
		keyLen := int(binary.LittleEndian.Uint16(payload[blockV1KeyLenOff : blockV1KeyLenOff+2]))
		valueLen := int(binary.LittleEndian.Uint32(payload[blockV1ValueLenOff : blockV1ValueLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV1RawLenOff : blockV1RawLenOff+4]))
		expected := keyLen + valueLen
		if rawLen != expected {
			return nil, fmt.Errorf("outerleaf: invalid raw length %d want %d", rawLen, expected)
		}
		raw, err := decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, nil, true, verifyChecksum)
		if err != nil {
			return nil, err
		}
		if keyLen < 0 || keyLen > len(raw) {
			return nil, fmt.Errorf("outerleaf: invalid key length %d", keyLen)
		}
		key := raw[:keyLen]
		if !keyWithinRange(key, lower, upper) {
			return nil, nil
		}
		lease := acquireKeyLease(1)
		if len(key) == 0 {
			lease.keys = append(lease.keys, []byte{})
			return lease, nil
		}
		arena := getPooledLeaseArena(len(key))
		arena = append(arena, key...)
		lease.addChunk(arena)
		lease.keys = append(lease.keys, arena[:len(key)])
		return lease, nil
	case blockVersionV2, blockVersionV3:
		entryCount := int(binary.LittleEndian.Uint16(payload[blockV2EntryCountOff : blockV2EntryCountOff+2]))
		entriesLen := int(binary.LittleEndian.Uint32(payload[blockV2EntriesLenOff : blockV2EntriesLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV2RawLenOff : blockV2RawLenOff+4]))
		if entryCount <= 0 {
			return nil, fmt.Errorf("outerleaf: empty v%d block", version)
		}
		if rawLen <= 0 {
			return nil, fmt.Errorf("outerleaf: invalid raw length %d", rawLen)
		}
		restartInterval := int(binary.LittleEndian.Uint16(payload[6:8]))
		if codec == blockCodecNone {
			raw, err := decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, nil, true, verifyChecksum)
			if err != nil {
				return nil, err
			}
			entries, _, _, err := splitV2RawMeta(raw, entriesLen, entryCount, restartInterval)
			if err != nil {
				return nil, err
			}
			return decodeStructuredKeysBoundedLease(version, entryCount, entries, lower, upper)
		}
		scratch := getPooledBytes(rawLen)
		defer putPooledBytes(scratch)
		raw, err := decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, scratch[:0], true, verifyChecksum)
		if err != nil {
			return nil, err
		}
		entries, _, _, err := splitV2RawMeta(raw, entriesLen, entryCount, restartInterval)
		if err != nil {
			return nil, err
		}
		return decodeStructuredKeysBoundedLease(version, entryCount, entries, lower, upper)
	default:
		return nil, fmt.Errorf("outerleaf: unsupported version %d", version)
	}
}

// DecodeLowerBoundAndKeysOnMatchWithVerify decodes lower-bound classification
// for target and materializes full keys only when target falls within block
// bounds. For below/above classifications, keys are not allocated.
func DecodeLowerBoundAndKeysOnMatchWithVerify(payload []byte, target []byte, verifyChecksum bool) (pos int, below bool, above bool, keys [][]byte, err error) {
	pos, below, above, lease, err := DecodeLowerBoundAndKeysOnMatchLeaseWithVerify(payload, target, verifyChecksum)
	if err != nil {
		return 0, false, false, nil, err
	}
	if lease == nil {
		return pos, below, above, nil, nil
	}
	keys = cloneKeySlices(lease.keys)
	lease.Release()
	return pos, below, above, keys, nil
}

// DecodeLowerBoundAndKeysOnMatchLeaseWithVerify decodes lower-bound
// classification for target and returns keys via a lease only when target falls
// within block bounds.
func DecodeLowerBoundAndKeysOnMatchLeaseWithVerify(payload []byte, target []byte, verifyChecksum bool) (pos int, below bool, above bool, lease *KeyLease, err error) {
	if len(payload) < blockHeaderSize {
		return 0, false, false, nil, fmt.Errorf("outerleaf: truncated header")
	}
	if payload[0] != blockMagic[0] || payload[1] != blockMagic[1] || payload[2] != blockMagic[2] || payload[3] != blockMagic[3] {
		return 0, false, false, nil, fmt.Errorf("outerleaf: invalid outer-leaf payload")
	}

	version := payload[4]
	codec := payload[5]
	switch version {
	case blockVersionV1:
		keyLen := int(binary.LittleEndian.Uint16(payload[blockV1KeyLenOff : blockV1KeyLenOff+2]))
		valueLen := int(binary.LittleEndian.Uint32(payload[blockV1ValueLenOff : blockV1ValueLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV1RawLenOff : blockV1RawLenOff+4]))
		expected := keyLen + valueLen
		if rawLen != expected {
			return 0, false, false, nil, fmt.Errorf("outerleaf: invalid raw length %d want %d", rawLen, expected)
		}
		raw, decErr := decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, nil, true, verifyChecksum)
		if decErr != nil {
			return 0, false, false, nil, decErr
		}
		if keyLen < 0 || keyLen > len(raw) {
			return 0, false, false, nil, fmt.Errorf("outerleaf: invalid key length %d", keyLen)
		}
		key := raw[:keyLen]
		if target != nil {
			cmp := bytes.Compare(key, target)
			if cmp > 0 {
				return 0, true, false, nil, nil
			}
			if cmp < 0 {
				return 1, false, true, nil, nil
			}
		}
		lease := acquireKeyLease(1)
		if len(key) == 0 {
			lease.keys = append(lease.keys, []byte{})
			return 0, false, false, lease, nil
		}
		arena := getPooledLeaseArena(len(key))
		arena = append(arena, key...)
		lease.addChunk(arena)
		lease.keys = append(lease.keys, arena[:len(key)])
		return 0, false, false, lease, nil
	case blockVersionV2, blockVersionV3:
		entryCount := int(binary.LittleEndian.Uint16(payload[blockV2EntryCountOff : blockV2EntryCountOff+2]))
		entriesLen := int(binary.LittleEndian.Uint32(payload[blockV2EntriesLenOff : blockV2EntriesLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV2RawLenOff : blockV2RawLenOff+4]))
		if entryCount <= 0 {
			return 0, false, false, nil, fmt.Errorf("outerleaf: empty v%d block", version)
		}
		if rawLen <= 0 {
			return 0, false, false, nil, fmt.Errorf("outerleaf: invalid raw length %d", rawLen)
		}
		restartInterval := int(binary.LittleEndian.Uint16(payload[6:8]))

		var raw []byte
		if codec == blockCodecNone {
			raw, err = decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, nil, true, verifyChecksum)
		} else {
			scratch := getPooledBytes(rawLen)
			defer putPooledBytes(scratch)
			raw, err = decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, scratch[:0], true, verifyChecksum)
		}
		if err != nil {
			return 0, false, false, nil, err
		}
		entries, _, _, splitErr := splitV2RawMeta(raw, entriesLen, entryCount, restartInterval)
		if splitErr != nil {
			return 0, false, false, nil, splitErr
		}
		pos, below, above, err = lowerBoundStructured(version, entryCount, entries, target)
		if err != nil {
			return 0, false, false, nil, err
		}
		if below || above {
			return pos, below, above, nil, nil
		}
		lease, err = decodeStructuredKeysBoundedLease(version, entryCount, entries, nil, nil)
		if err != nil {
			return 0, false, false, nil, err
		}
		return pos, false, false, lease, nil
	default:
		return 0, false, false, nil, fmt.Errorf("outerleaf: unsupported version %d", version)
	}
}

// Decode returns the first key/value in an encoded outer-leaf payload.
func Decode(payload []byte, scratch []byte) (key []byte, value []byte, ok bool, outScratch []byte, err error) {
	if len(payload) < blockHeaderSize {
		return nil, nil, false, scratch, nil
	}
	if payload[0] != blockMagic[0] || payload[1] != blockMagic[1] || payload[2] != blockMagic[2] || payload[3] != blockMagic[3] {
		return nil, nil, false, scratch, nil
	}

	version := payload[4]
	codec := payload[5]
	switch version {
	case blockVersionV1:
		keyLen := int(binary.LittleEndian.Uint16(payload[blockV1KeyLenOff : blockV1KeyLenOff+2]))
		valueLen := int(binary.LittleEndian.Uint32(payload[blockV1ValueLenOff : blockV1ValueLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV1RawLenOff : blockV1RawLenOff+4]))
		expected := keyLen + valueLen
		if rawLen != expected {
			return nil, nil, true, scratch, fmt.Errorf("outerleaf: invalid raw length %d want %d", rawLen, expected)
		}
		if rawLen < 0 || keyLen < 0 || valueLen < 0 {
			return nil, nil, true, scratch, fmt.Errorf("outerleaf: invalid lengths")
		}
		outScratch, err = decodeAndVerifyPayload(payload, rawLen, codec, scratch)
		if err != nil {
			return nil, nil, true, scratch, err
		}
		key = outScratch[:keyLen]
		value = outScratch[keyLen : keyLen+valueLen]
		return key, value, true, outScratch, nil
	case blockVersionV2:
		entryCount := int(binary.LittleEndian.Uint16(payload[blockV2EntryCountOff : blockV2EntryCountOff+2]))
		entriesLen := int(binary.LittleEndian.Uint32(payload[blockV2EntriesLenOff : blockV2EntriesLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV2RawLenOff : blockV2RawLenOff+4]))
		if entryCount <= 0 {
			return nil, nil, true, scratch, fmt.Errorf("outerleaf: empty v2 block")
		}
		if rawLen <= 0 {
			return nil, nil, true, scratch, fmt.Errorf("outerleaf: invalid raw length %d", rawLen)
		}
		outScratch, err = decodeAndVerifyPayload(payload, rawLen, codec, scratch)
		if err != nil {
			return nil, nil, true, scratch, err
		}
		entries, _, _, splitErr := splitV2RawMeta(outScratch, entriesLen, entryCount, int(binary.LittleEndian.Uint16(payload[6:8])))
		if splitErr != nil {
			return nil, nil, true, outScratch, splitErr
		}
		key, value, err = decodeFirstV2(entries)
		if err != nil {
			return nil, nil, true, outScratch, err
		}
		return key, value, true, outScratch, nil
	case blockVersionV3:
		entryCount := int(binary.LittleEndian.Uint16(payload[blockV2EntryCountOff : blockV2EntryCountOff+2]))
		entriesLen := int(binary.LittleEndian.Uint32(payload[blockV2EntriesLenOff : blockV2EntriesLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV2RawLenOff : blockV2RawLenOff+4]))
		if entryCount <= 0 {
			return nil, nil, true, scratch, fmt.Errorf("outerleaf: empty v3 block")
		}
		if rawLen <= 0 {
			return nil, nil, true, scratch, fmt.Errorf("outerleaf: invalid raw length %d", rawLen)
		}
		outScratch, err = decodeAndVerifyPayload(payload, rawLen, codec, scratch)
		if err != nil {
			return nil, nil, true, scratch, err
		}
		entries, _, _, splitErr := splitV2RawMeta(outScratch, entriesLen, entryCount, int(binary.LittleEndian.Uint16(payload[6:8])))
		if splitErr != nil {
			return nil, nil, true, outScratch, splitErr
		}
		key, first, err := decodeFirstV3(entries)
		if err != nil {
			return nil, nil, true, outScratch, err
		}
		if first.Kind == EntryKindBlobRef {
			return key, nil, true, outScratch, ErrBlobRefEntry
		}
		return key, first.Value, true, outScratch, nil
	default:
		return nil, nil, true, scratch, fmt.Errorf("outerleaf: unsupported version %d", version)
	}
}

// DecodeValue returns the first value in an encoded outer-leaf payload.
func DecodeValue(payload []byte, scratch []byte) (value []byte, ok bool, outScratch []byte, err error) {
	_, value, ok, outScratch, err = Decode(payload, scratch)
	return value, ok, outScratch, err
}

// DecodeValueForKey resolves key inside an encoded outer-leaf payload.
//
// ok reports whether payload is an outer-leaf payload.
// found reports whether key exists in that payload.
func DecodeValueForKey(payload []byte, key []byte, scratch []byte) (value []byte, ok bool, found bool, outScratch []byte, err error) {
	if len(payload) < blockHeaderSize {
		return nil, false, false, scratch, nil
	}
	if payload[0] != blockMagic[0] || payload[1] != blockMagic[1] || payload[2] != blockMagic[2] || payload[3] != blockMagic[3] {
		return nil, false, false, scratch, nil
	}

	version := payload[4]
	codec := payload[5]
	switch version {
	case blockVersionV1:
		keyLen := int(binary.LittleEndian.Uint16(payload[blockV1KeyLenOff : blockV1KeyLenOff+2]))
		valueLen := int(binary.LittleEndian.Uint32(payload[blockV1ValueLenOff : blockV1ValueLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV1RawLenOff : blockV1RawLenOff+4]))
		expected := keyLen + valueLen
		if rawLen != expected {
			return nil, true, false, scratch, fmt.Errorf("outerleaf: invalid raw length %d want %d", rawLen, expected)
		}
		outScratch, err = decodeAndVerifyPayload(payload, rawLen, codec, scratch)
		if err != nil {
			return nil, true, false, scratch, err
		}
		blockKey := outScratch[:keyLen]
		blockValue := outScratch[keyLen : keyLen+valueLen]
		if key == nil || bytes.Equal(blockKey, key) {
			return blockValue, true, true, outScratch, nil
		}
		return nil, true, false, outScratch, nil
	case blockVersionV2:
		entryCount := int(binary.LittleEndian.Uint16(payload[blockV2EntryCountOff : blockV2EntryCountOff+2]))
		entriesLen := int(binary.LittleEndian.Uint32(payload[blockV2EntriesLenOff : blockV2EntriesLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV2RawLenOff : blockV2RawLenOff+4]))
		if entryCount <= 0 {
			return nil, true, false, scratch, fmt.Errorf("outerleaf: empty v2 block")
		}
		outScratch, err = decodeAndVerifyPayload(payload, rawLen, codec, scratch)
		if err != nil {
			return nil, true, false, scratch, err
		}
		restartsInterval := int(binary.LittleEndian.Uint16(payload[6:8]))
		entries, restartRaw, restartCount, splitErr := splitV2RawMeta(outScratch, entriesLen, entryCount, restartsInterval)
		if splitErr != nil {
			return nil, true, false, outScratch, splitErr
		}
		val, found, err := lookupV2ValueFromRestartRaw(entries, entryCount, key, restartRaw, restartCount)
		if err != nil {
			return nil, true, false, outScratch, err
		}
		if !found {
			return nil, true, false, outScratch, nil
		}
		return val, true, true, outScratch, nil
	case blockVersionV3:
		entryCount := int(binary.LittleEndian.Uint16(payload[blockV2EntryCountOff : blockV2EntryCountOff+2]))
		entriesLen := int(binary.LittleEndian.Uint32(payload[blockV2EntriesLenOff : blockV2EntriesLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV2RawLenOff : blockV2RawLenOff+4]))
		if entryCount <= 0 {
			return nil, true, false, scratch, fmt.Errorf("outerleaf: empty v3 block")
		}
		outScratch, err = decodeAndVerifyPayload(payload, rawLen, codec, scratch)
		if err != nil {
			return nil, true, false, scratch, err
		}
		restartsInterval := int(binary.LittleEndian.Uint16(payload[6:8]))
		entries, restartRaw, restartCount, splitErr := splitV2RawMeta(outScratch, entriesLen, entryCount, restartsInterval)
		if splitErr != nil {
			return nil, true, false, outScratch, splitErr
		}
		entry, found, err := lookupV3EntryFromRestartRaw(entries, entryCount, key, restartRaw, restartCount)
		if err != nil {
			return nil, true, false, outScratch, err
		}
		if !found {
			return nil, true, false, outScratch, nil
		}
		if entry.Kind == EntryKindBlobRef {
			return nil, true, true, outScratch, ErrBlobRefEntry
		}
		return entry.Value, true, true, outScratch, nil
	default:
		return nil, true, false, scratch, fmt.Errorf("outerleaf: unsupported version %d", version)
	}
}

// DecodeEntryForKey resolves key inside an encoded outer-leaf payload and
// returns typed payload info (inline value or blob reference).
//
// ok reports whether payload is an outer-leaf payload.
// found reports whether key exists in that payload.
func DecodeEntryForKey(payload []byte, key []byte, scratch []byte) (entry LookupResult, ok bool, found bool, outScratch []byte, err error) {
	return DecodeEntryForKeyWithVerify(payload, key, scratch, true)
}

// DecodeEntryForKeyWithVerify resolves key inside an encoded outer-leaf payload
// and optionally verifies the block checksum.
//
// ok reports whether payload is an outer-leaf payload.
// found reports whether key exists in that payload.
func DecodeEntryForKeyWithVerify(payload []byte, key []byte, scratch []byte, verifyChecksum bool) (entry LookupResult, ok bool, found bool, outScratch []byte, err error) {
	if len(payload) < blockHeaderSize {
		return LookupResult{}, false, false, scratch, nil
	}
	if payload[0] != blockMagic[0] || payload[1] != blockMagic[1] || payload[2] != blockMagic[2] || payload[3] != blockMagic[3] {
		return LookupResult{}, false, false, scratch, nil
	}

	version := payload[4]
	codec := payload[5]
	switch version {
	case blockVersionV1:
		keyLen := int(binary.LittleEndian.Uint16(payload[blockV1KeyLenOff : blockV1KeyLenOff+2]))
		valueLen := int(binary.LittleEndian.Uint32(payload[blockV1ValueLenOff : blockV1ValueLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV1RawLenOff : blockV1RawLenOff+4]))
		expected := keyLen + valueLen
		if rawLen != expected {
			return LookupResult{}, true, false, scratch, fmt.Errorf("outerleaf: invalid raw length %d want %d", rawLen, expected)
		}
		outScratch, err = decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, scratch, false, verifyChecksum)
		if err != nil {
			return LookupResult{}, true, false, scratch, err
		}
		blockKey := outScratch[:keyLen]
		blockValue := outScratch[keyLen : keyLen+valueLen]
		if key == nil || bytes.Equal(blockKey, key) {
			return LookupResult{Kind: EntryKindInline, Value: blockValue}, true, true, outScratch, nil
		}
		return LookupResult{}, true, false, outScratch, nil
	case blockVersionV2:
		val, ok, found, outScratch, err := decodeValueForKeyWithVerify(payload, key, scratch, verifyChecksum)
		if err != nil {
			return LookupResult{}, ok, found, outScratch, err
		}
		if !ok || !found {
			return LookupResult{}, ok, found, outScratch, nil
		}
		return LookupResult{Kind: EntryKindInline, Value: val}, true, true, outScratch, nil
	case blockVersionV3:
		entryCount := int(binary.LittleEndian.Uint16(payload[blockV2EntryCountOff : blockV2EntryCountOff+2]))
		entriesLen := int(binary.LittleEndian.Uint32(payload[blockV2EntriesLenOff : blockV2EntriesLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV2RawLenOff : blockV2RawLenOff+4]))
		if entryCount <= 0 {
			return LookupResult{}, true, false, scratch, fmt.Errorf("outerleaf: empty v3 block")
		}
		outScratch, err = decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, scratch, false, verifyChecksum)
		if err != nil {
			return LookupResult{}, true, false, scratch, err
		}
		restartsInterval := int(binary.LittleEndian.Uint16(payload[6:8]))
		entries, restartRaw, restartCount, splitErr := splitV2RawMeta(outScratch, entriesLen, entryCount, restartsInterval)
		if splitErr != nil {
			return LookupResult{}, true, false, outScratch, splitErr
		}
		result, found, err := lookupV3EntryFromRestartRaw(entries, entryCount, key, restartRaw, restartCount)
		if err != nil {
			return LookupResult{}, true, false, outScratch, err
		}
		if !found {
			return LookupResult{}, true, false, outScratch, nil
		}
		return result, true, true, outScratch, nil
	default:
		return LookupResult{}, true, false, scratch, fmt.Errorf("outerleaf: unsupported version %d", version)
	}
}

func decodeValueForKeyWithVerify(payload []byte, key []byte, scratch []byte, verifyChecksum bool) (value []byte, ok bool, found bool, outScratch []byte, err error) {
	if len(payload) < blockHeaderSize {
		return nil, false, false, scratch, nil
	}
	if payload[0] != blockMagic[0] || payload[1] != blockMagic[1] || payload[2] != blockMagic[2] || payload[3] != blockMagic[3] {
		return nil, false, false, scratch, nil
	}

	version := payload[4]
	codec := payload[5]
	switch version {
	case blockVersionV1:
		keyLen := int(binary.LittleEndian.Uint16(payload[blockV1KeyLenOff : blockV1KeyLenOff+2]))
		valueLen := int(binary.LittleEndian.Uint32(payload[blockV1ValueLenOff : blockV1ValueLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV1RawLenOff : blockV1RawLenOff+4]))
		expected := keyLen + valueLen
		if rawLen != expected {
			return nil, true, false, scratch, fmt.Errorf("outerleaf: invalid raw length %d want %d", rawLen, expected)
		}
		outScratch, err = decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, scratch, false, verifyChecksum)
		if err != nil {
			return nil, true, false, scratch, err
		}
		blockKey := outScratch[:keyLen]
		blockValue := outScratch[keyLen : keyLen+valueLen]
		if key == nil || bytes.Equal(blockKey, key) {
			return blockValue, true, true, outScratch, nil
		}
		return nil, true, false, outScratch, nil
	case blockVersionV2:
		entryCount := int(binary.LittleEndian.Uint16(payload[blockV2EntryCountOff : blockV2EntryCountOff+2]))
		entriesLen := int(binary.LittleEndian.Uint32(payload[blockV2EntriesLenOff : blockV2EntriesLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV2RawLenOff : blockV2RawLenOff+4]))
		if entryCount <= 0 {
			return nil, true, false, scratch, fmt.Errorf("outerleaf: empty v2 block")
		}
		outScratch, err = decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, scratch, false, verifyChecksum)
		if err != nil {
			return nil, true, false, scratch, err
		}
		restartsInterval := int(binary.LittleEndian.Uint16(payload[6:8]))
		entries, restartRaw, restartCount, splitErr := splitV2RawMeta(outScratch, entriesLen, entryCount, restartsInterval)
		if splitErr != nil {
			return nil, true, false, outScratch, splitErr
		}
		val, found, err := lookupV2ValueFromRestartRaw(entries, entryCount, key, restartRaw, restartCount)
		if err != nil {
			return nil, true, false, outScratch, err
		}
		if !found {
			return nil, true, false, outScratch, nil
		}
		return val, true, true, outScratch, nil
	case blockVersionV3:
		entryCount := int(binary.LittleEndian.Uint16(payload[blockV2EntryCountOff : blockV2EntryCountOff+2]))
		entriesLen := int(binary.LittleEndian.Uint32(payload[blockV2EntriesLenOff : blockV2EntriesLenOff+4]))
		rawLen := int(binary.LittleEndian.Uint32(payload[blockV2RawLenOff : blockV2RawLenOff+4]))
		if entryCount <= 0 {
			return nil, true, false, scratch, fmt.Errorf("outerleaf: empty v3 block")
		}
		outScratch, err = decodeAndVerifyPayloadModeWithChecksum(payload, rawLen, codec, scratch, false, verifyChecksum)
		if err != nil {
			return nil, true, false, scratch, err
		}
		restartsInterval := int(binary.LittleEndian.Uint16(payload[6:8]))
		entries, restartRaw, restartCount, splitErr := splitV2RawMeta(outScratch, entriesLen, entryCount, restartsInterval)
		if splitErr != nil {
			return nil, true, false, outScratch, splitErr
		}
		entry, found, err := lookupV3EntryFromRestartRaw(entries, entryCount, key, restartRaw, restartCount)
		if err != nil {
			return nil, true, false, outScratch, err
		}
		if !found {
			return nil, true, false, outScratch, nil
		}
		if entry.Kind == EntryKindBlobRef {
			return nil, true, true, outScratch, ErrBlobRefEntry
		}
		return entry.Value, true, true, outScratch, nil
	default:
		return nil, true, false, scratch, fmt.Errorf("outerleaf: unsupported version %d", version)
	}
}
