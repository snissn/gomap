package outerleaf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/gomap/TreeDB/internal/crc"
)

const (
	ModeV2BlockPtr = "v2_blockptr"

	defaultBlockTargetBytes = 4 << 10
	defaultRestartInterval  = 16

	blockCodecNone   = uint8(0)
	blockCodecSnappy = uint8(1)
	blockCodecLZ4    = uint8(2)
)

var blockMagic = [4]byte{'T', 'O', 'L', '2'}

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

func ModeEnabled(mode string) bool {
	return strings.TrimSpace(mode) == ModeV2BlockPtr
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
	switch codec {
	case 1:
		return blockCodecLZ4
	default:
		return blockCodecSnappy
	}
}

func encodePayload(codec uint8, raw []byte, dst []byte) ([]byte, uint8, error) {
	if len(raw) == 0 {
		return nil, blockCodecNone, nil
	}
	switch normalizeCodec(codec) {
	case blockCodecLZ4:
		bound := lz4.CompressBlockBound(len(raw))
		if cap(dst) < bound {
			dst = make([]byte, bound)
		}
		dst = dst[:bound]
		n, err := lz4.CompressBlock(raw, dst, nil)
		if err == nil && n > 0 {
			return dst[:n], blockCodecLZ4, nil
		}
		// Fall through to snappy on lz4 miss for deterministic behavior.
		fallthrough
	default:
		enc := snappy.Encode(dst[:0], raw)
		return enc, blockCodecSnappy, nil
	}
}

func decodePayload(codec uint8, payload []byte, rawLen int, dst []byte) ([]byte, error) {
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

func encodeV1Single(dst, key, value []byte, codec uint8, restartInterval int) ([]byte, error) {
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

	raw := make([]byte, 0, rawLen)
	raw = append(raw, key...)
	raw = append(raw, value...)

	encodedPayload, encodedCodec, err := encodePayload(codec, raw, nil)
	if err != nil {
		return nil, err
	}
	if len(encodedPayload) == 0 {
		encodedCodec = blockCodecNone
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

func encodeV2Entries(dst []byte, entries []Entry, codec uint8, restartInterval int) ([]byte, error) {
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
	raw := make([]byte, 0, estRaw)
	restarts := make([]uint32, 0, (len(entries)+restartInterval-1)/restartInterval)
	var prev []byte

	for i := range entries {
		e := entries[i]
		if len(e.Value) > int(^uint32(0)) {
			return nil, fmt.Errorf("outerleaf: value too large %d", len(e.Value))
		}
		if i > 0 && bytes.Compare(entries[i-1].Key, e.Key) >= 0 {
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

		prev = append(prev[:0], e.Key...)
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

	encodedPayload, encodedCodec, err := encodePayload(codec, raw, nil)
	if err != nil {
		return nil, err
	}
	if len(encodedPayload) == 0 {
		encodedCodec = blockCodecNone
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

// EncodeSingle encodes one key/value record in a v1-compatible payload.
func EncodeSingle(dst, key, value []byte, codec uint8, restartInterval int) ([]byte, error) {
	return encodeV1Single(dst, key, value, codec, restartInterval)
}

// EncodeEntries encodes an ordered block of key/value records using v2 layout.
func EncodeEntries(dst []byte, entries []Entry, codec uint8, restartInterval int) ([]byte, error) {
	if len(entries) == 1 {
		return encodeV1Single(dst, entries[0].Key, entries[0].Value, codec, restartInterval)
	}
	return encodeV2Entries(dst, entries, codec, restartInterval)
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
	prev := make([]byte, 0, prevCap)
	curr := make([]byte, 0, prevCap)
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
