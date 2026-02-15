package outerleaf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"sync"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/limits"
)

const (
	ModeV2BlockPtr = "v2_blockptr"
	ModeV2FencePtr = "v2_fenceptr"

	defaultBlockTargetBytes = 4 << 10
	defaultRestartInterval  = 16

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
	maxPooledOuterLeafBytesCap    = 64 << 10
	maxPooledOuterLeafRestartsCap = 4096
)

var (
	outerLeafBytesPool    sync.Pool
	outerLeafRestartsPool sync.Pool
)

const (
	blockVersionV1 = uint8(1)
	blockVersionV2 = uint8(2)

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
)

// Entry is one key/value record in an outer-leaf block payload.
type Entry struct {
	Key   []byte
	Value []byte
}

// DecodedBlock represents a parsed outer-leaf payload.
type DecodedBlock struct {
	version     uint8
	entryCount  int
	raw         []byte
	entries     []byte
	restarts    []uint32
	restartKeys [][]byte
	firstKey    []byte
	firstValue  []byte
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

func ModeEnabled(mode string) bool {
	switch strings.TrimSpace(mode) {
	case ModeV2BlockPtr, ModeV2FencePtr:
		return true
	default:
		return false
	}
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
	var sampleBuf [outerLeafIncompressibleProbeBytes]byte
	sample := sampleForIncompressibleProbe(raw, &sampleBuf)
	// Fast-path clearly high-entropy blocks to raw mode and skip probe/full
	// codec work.
	if isLikelyHighEntropy(sample) {
		return true
	}
	switch codec {
	case blockCodecLZ4:
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
	default:
		need := snappy.MaxEncodedLen(len(sample))
		if cap(dst) < need {
			dst = make([]byte, need)
		} else {
			dst = dst[:need]
		}
		enc := snappy.Encode(dst, sample)
		return !keepCompressedPayload(len(sample), len(enc))
	}
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
	if shouldBypassCompressionProbe(codec, raw, dst) {
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

func decodePayload(codec uint8, payload []byte, rawLen int, dst []byte) ([]byte, error) {
	if rawLen < 0 {
		return nil, fmt.Errorf("outerleaf: invalid raw length %d", rawLen)
	}
	if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
		return nil, fmt.Errorf("outerleaf: payload too large %d", rawLen)
	}

	switch codec {
	case blockCodecNone:
		if len(payload) != rawLen {
			return nil, fmt.Errorf("outerleaf: raw payload length mismatch got=%d want=%d", len(payload), rawLen)
		}
		if cap(dst) < rawLen {
			dst = make([]byte, rawLen)
		} else {
			dst = dst[:rawLen]
		}
		copy(dst, payload)
		return dst, nil
	case blockCodecSnappy:
		decodedLen, err := snappy.DecodedLen(payload)
		if err != nil {
			return nil, err
		}
		if decodedLen != rawLen {
			return nil, fmt.Errorf("outerleaf: snappy decoded length mismatch got=%d want=%d", decodedLen, rawLen)
		}
		out, err := snappy.Decode(dst[:0], payload)
		if err != nil {
			return nil, err
		}
		if len(out) != rawLen {
			return nil, fmt.Errorf("outerleaf: snappy decode length mismatch got=%d want=%d", len(out), rawLen)
		}
		return out, nil
	case blockCodecLZ4:
		if cap(dst) < rawLen {
			dst = make([]byte, rawLen)
		} else {
			dst = dst[:rawLen]
		}
		n, err := lz4.UncompressBlock(payload, dst)
		if err != nil {
			return nil, err
		}
		if n != rawLen {
			return nil, fmt.Errorf("outerleaf: lz4 decode length mismatch got=%d want=%d", n, rawLen)
		}
		return dst[:n], nil
	default:
		return nil, fmt.Errorf("outerleaf: unknown codec id %d", codec)
	}
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

func getPooledBytes(minCap int) []byte {
	if minCap <= 0 {
		minCap = 1
	}
	if v := outerLeafBytesPool.Get(); v != nil {
		if buf, ok := v.([]byte); ok && cap(buf) >= minCap {
			return buf[:0]
		}
	}
	return make([]byte, 0, minCap)
}

func putPooledBytes(buf []byte) {
	if cap(buf) == 0 || cap(buf) > maxPooledOuterLeafBytesCap {
		return
	}
	outerLeafBytesPool.Put(buf[:0])
}

func getPooledRestarts(minCap int) []uint32 {
	if minCap <= 0 {
		minCap = 1
	}
	if v := outerLeafRestartsPool.Get(); v != nil {
		if buf, ok := v.([]uint32); ok && cap(buf) >= minCap {
			return buf[:0]
		}
	}
	return make([]uint32, 0, minCap)
}

func putPooledRestarts(buf []uint32) {
	if cap(buf) == 0 || cap(buf) > maxPooledOuterLeafRestartsCap {
		return
	}
	outerLeafRestartsPool.Put(buf[:0])
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

func decodeAndVerifyPayload(payload []byte, rawLen int, codec uint8, scratch []byte) ([]byte, error) {
	if blockHeaderSize > len(payload) {
		return nil, fmt.Errorf("outerleaf: truncated header")
	}
	gotChecksum := binary.LittleEndian.Uint32(payload[blockChecksumOff : blockChecksumOff+blockChecksumSize])
	wantChecksum := crc.ChecksumParts(payload[:blockChecksumOff], payload[blockHeaderSize:])
	if gotChecksum != wantChecksum {
		return nil, fmt.Errorf("outerleaf: checksum mismatch")
	}
	out, err := decodePayload(codec, payload[blockHeaderSize:], rawLen, scratch)
	if err != nil {
		return nil, err
	}
	if len(out) != rawLen {
		return nil, fmt.Errorf("outerleaf: decoded payload length mismatch")
	}
	return out, nil
}

func splitV2Raw(raw []byte, entriesLen int, entryCount int, restartInterval int) (entries []byte, restarts []uint32, err error) {
	if entriesLen < 0 || entriesLen > len(raw) {
		return nil, nil, fmt.Errorf("outerleaf: invalid entries length %d", entriesLen)
	}
	if len(raw) < entriesLen+4 {
		return nil, nil, fmt.Errorf("outerleaf: missing restart trailer")
	}
	restartCount := int(binary.LittleEndian.Uint32(raw[len(raw)-4:]))
	if restartCount < 0 {
		return nil, nil, fmt.Errorf("outerleaf: invalid restart count")
	}
	restartBytes := restartCount * 4
	if entriesLen+restartBytes+4 != len(raw) {
		return nil, nil, fmt.Errorf("outerleaf: invalid restart section")
	}
	if entryCount > 0 && restartCount == 0 {
		return nil, nil, fmt.Errorf("outerleaf: empty restart table")
	}

	restarts = make([]uint32, restartCount)
	restartOff := entriesLen
	for i := 0; i < restartCount; i++ {
		off := binary.LittleEndian.Uint32(raw[restartOff+i*4 : restartOff+(i+1)*4])
		if int(off) < 0 || int(off) > entriesLen {
			return nil, nil, fmt.Errorf("outerleaf: restart offset out of range")
		}
		if i > 0 && off < restarts[i-1] {
			return nil, nil, fmt.Errorf("outerleaf: restart offsets not monotonic")
		}
		restarts[i] = off
	}
	if restartCount > 0 && restarts[0] != 0 {
		return nil, nil, fmt.Errorf("outerleaf: first restart offset must be 0")
	}
	if restartInterval <= 0 {
		restartInterval = 1
	}
	minRestarts := (entryCount + restartInterval - 1) / restartInterval
	if entryCount > 0 && restartCount < minRestarts {
		return nil, nil, fmt.Errorf("outerleaf: restart table too small")
	}

	return raw[:entriesLen], restarts, nil
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

func decodeV2RestartKeys(entries []byte, restarts []uint32) ([][]byte, error) {
	if len(restarts) == 0 {
		return nil, nil
	}
	keys := make([][]byte, len(restarts))
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
	if len(key) == 0 {
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

// DecodeBlock parses an outer-leaf payload and keeps the decoded data alive.
func DecodeBlock(payload []byte, scratch []byte) (*DecodedBlock, error) {
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
		outScratch, err := decodeAndVerifyPayload(payload, rawLen, codec, scratch)
		if err != nil {
			return nil, err
		}
		return &DecodedBlock{
			version:    version,
			entryCount: 1,
			raw:        outScratch,
			entries:    outScratch,
			firstKey:   outScratch[:keyLen],
			firstValue: outScratch[keyLen : keyLen+valueLen],
		}, nil
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
		outScratch, err := decodeAndVerifyPayload(payload, rawLen, codec, scratch)
		if err != nil {
			return nil, err
		}
		restartInterval := int(binary.LittleEndian.Uint16(payload[6:8]))
		entries, restarts, splitErr := splitV2Raw(outScratch, entriesLen, entryCount, restartInterval)
		if splitErr != nil {
			return nil, splitErr
		}
		restartKeys, err := decodeV2RestartKeys(entries, restarts)
		if err != nil {
			return nil, err
		}
		firstKey, firstValue, err := decodeFirstV2(entries)
		if err != nil {
			return nil, err
		}
		return &DecodedBlock{
			version:     version,
			entryCount:  entryCount,
			raw:         outScratch,
			entries:     entries,
			restarts:    restarts,
			restartKeys: restartKeys,
			firstKey:    firstKey,
			firstValue:  firstValue,
		}, nil
	default:
		return nil, fmt.Errorf("outerleaf: unsupported version %d", version)
	}
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
		val, _, err := lookupV2Value(d.entries, d.entryCount, nil, d.restarts, d.restartKeys)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, fmt.Errorf("outerleaf: empty block")
		}
		return val, nil
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
		if len(key) == 0 || bytes.Equal(d.firstKey, key) {
			return d.firstValue, true, nil
		}
		return nil, false, nil
	case blockVersionV2:
		if len(key) == 0 {
			if d.firstValue != nil {
				return d.firstValue, true, nil
			}
			return lookupV2Value(d.entries, d.entryCount, nil, d.restarts, d.restartKeys)
		}
		val, found, err := lookupV2Value(d.entries, d.entryCount, key, d.restarts, d.restartKeys)
		return val, found, err
	default:
		return nil, false, fmt.Errorf("outerleaf: unsupported version %d", d.version)
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
	switch d.version {
	case blockVersionV1:
		if d.firstKey == nil || d.firstValue == nil {
			return nil, fmt.Errorf("outerleaf: missing v1 entry")
		}
		dst = append(dst, Entry{Key: d.firstKey, Value: d.firstValue})
		return dst, nil
	case blockVersionV2:
		encoded := d.entries
		off := 0
		var prevKey []byte
		for i := 0; i < d.entryCount; i++ {
			if off+8 > len(encoded) {
				return nil, fmt.Errorf("outerleaf: truncated v2 entry header")
			}
			shared := int(binary.LittleEndian.Uint16(encoded[off : off+2]))
			suffixLen := int(binary.LittleEndian.Uint16(encoded[off+2 : off+4]))
			valueLen := int(binary.LittleEndian.Uint32(encoded[off+4 : off+8]))
			off += 8
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
			value := encoded[off : off+valueLen]
			off += valueLen

			var key []byte
			if shared == 0 {
				key = suffix
			} else {
				key = make([]byte, shared+suffixLen)
				copy(key, prevKey[:shared])
				copy(key[shared:], suffix)
			}
			dst = append(dst, Entry{Key: key, Value: value})
			prevKey = key
		}
		if off != len(encoded) {
			return nil, fmt.Errorf("outerleaf: trailing v2 entry bytes")
		}
		return dst, nil
	default:
		return nil, fmt.Errorf("outerleaf: unsupported version %d", d.version)
	}
}

// Keys decodes all logical keys from a parsed block.
// Returned key slices reference decoded block storage where possible.
func (d *DecodedBlock) Keys(dst [][]byte) ([][]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("outerleaf: nil block")
	}
	if cap(dst) < d.entryCount {
		dst = make([][]byte, 0, d.entryCount)
	} else {
		dst = dst[:0]
	}
	switch d.version {
	case blockVersionV1:
		if d.firstKey == nil {
			return nil, fmt.Errorf("outerleaf: missing v1 key")
		}
		dst = append(dst, d.firstKey)
		return dst, nil
	case blockVersionV2:
		encoded := d.entries
		off := 0
		var prevKey []byte
		for i := 0; i < d.entryCount; i++ {
			if off+8 > len(encoded) {
				return nil, fmt.Errorf("outerleaf: truncated v2 entry header")
			}
			shared := int(binary.LittleEndian.Uint16(encoded[off : off+2]))
			suffixLen := int(binary.LittleEndian.Uint16(encoded[off+2 : off+4]))
			valueLen := int(binary.LittleEndian.Uint32(encoded[off+4 : off+8]))
			off += 8
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

			var key []byte
			if shared == 0 {
				key = suffix
			} else {
				key = make([]byte, shared+suffixLen)
				copy(key, prevKey[:shared])
				copy(key[shared:], suffix)
			}
			dst = append(dst, key)
			prevKey = key
		}
		if off != len(encoded) {
			return nil, fmt.Errorf("outerleaf: trailing v2 entry bytes")
		}
		return dst, nil
	default:
		return nil, fmt.Errorf("outerleaf: unsupported version %d", d.version)
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
		entries, _, splitErr := splitV2Raw(outScratch, entriesLen, entryCount, int(binary.LittleEndian.Uint16(payload[6:8])))
		if splitErr != nil {
			return nil, nil, true, outScratch, splitErr
		}
		key, value, err = decodeFirstV2(entries)
		if err != nil {
			return nil, nil, true, outScratch, err
		}
		return key, value, true, outScratch, nil
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
	// This function remains unchanged to protect legacy behavior.

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
		if len(key) == 0 || bytes.Equal(blockKey, key) {
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
		entries, restarts, splitErr := splitV2Raw(outScratch, entriesLen, entryCount, restartsInterval)
		if splitErr != nil {
			return nil, true, false, outScratch, splitErr
		}
		val, found, err := lookupV2Value(entries, entryCount, key, restarts, nil)
		if err != nil {
			return nil, true, false, outScratch, err
		}
		if !found {
			return nil, true, false, outScratch, nil
		}
		return val, true, true, outScratch, nil
	default:
		return nil, true, false, scratch, fmt.Errorf("outerleaf: unsupported version %d", version)
	}
}
