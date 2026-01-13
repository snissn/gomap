package slab

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"

	"github.com/klauspost/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/compression"
)

const (
	frameGroupVersion0 byte = 0
	frameGroupHeader        = 4 // version(1) + k(1) + offsetsCount(uint16)
)

var (
	errFrameGroupCorrupt = errors.New("slab: invalid frame group record")
)

// buildFrameGroupRecord builds a grouped slab record (header + offsets + compressed payload).
// values must be non-empty. Returns the full record bytes (including checksum header) and the
// number of values packed.
func buildFrameGroupRecord(values [][]byte, cfg *compression.Config) ([]byte, int, error) {
	if len(values) == 0 {
		return nil, 0, errFrameGroupCorrupt
	}
	if cfg == nil || cfg.ZstdEncs == nil {
		return nil, 0, errors.New("slab: compression config missing for frame group")
	}

	k := len(values)
	offsets := make([]uint32, k+1)
	rawTotal := 0
	for i, v := range values {
		rawTotal += len(v)
		if rawTotal > math.MaxUint32 {
			return nil, 0, ErrRecordTooLarge
		}
		offsets[i+1] = uint32(rawTotal)
	}
	if rawTotal == 0 {
		return nil, 0, errFrameGroupCorrupt
	}

	// Concatenate payload.
	payload := make([]byte, rawTotal)
	pos := 0
	for _, v := range values {
		copy(payload[pos:], v)
		pos += len(v)
	}

	enc := cfg.ZstdEncs.Get().(*zstd.Encoder)
	compressed := enc.EncodeAll(payload, nil)
	cfg.ZstdEncs.Put(enc)

	offsetBytes := 4 * (k + 1)
	bodyLen := frameGroupHeader + offsetBytes + len(compressed)
	if bodyLen > int(math.MaxUint32) {
		return nil, 0, ErrRecordTooLarge
	}

	recordLen := HeaderSize + bodyLen
	record := make([]byte, recordLen)

	// Header: checksum (filled later) + keyLen=0 + valueLen=bodyLen.
	binary.LittleEndian.PutUint16(record[4:6], 0) // keyLen = 0
	binary.LittleEndian.PutUint32(record[6:10], uint32(bodyLen))

	body := record[HeaderSize:]
	body[0] = frameGroupVersion0
	body[1] = byte(k)
	binary.LittleEndian.PutUint16(body[2:4], uint16(k+1)) // offsets count
	offPos := 4
	for _, off := range offsets {
		binary.LittleEndian.PutUint32(body[offPos:offPos+4], off)
		offPos += 4
	}
	copy(body[frameGroupHeader+offsetBytes:], compressed)

	// CRC over header fields + body.
	var lenArr [6]byte
	copy(lenArr[:2], record[4:6])
	copy(lenArr[2:6], record[6:10])
	sum := crc32.Update(0, crc32cTable, lenArr[:])
	sum = crc32.Update(sum, crc32cTable, body)
	binary.LittleEndian.PutUint32(record[0:4], sum)

	return record, k, nil
}

// decompressFrameGroup extracts a single value (subIndex) from a grouped record body (value bytes).
func decompressFrameGroup(cfg *compression.Config, body []byte, subIndex int) ([]byte, error) {
	if cfg == nil || cfg.ZstdDecs == nil {
		return nil, compression.ErrCorrupt
	}
	dec := cfg.ZstdDecs.Get().(*zstd.Decoder)
	defer cfg.ZstdDecs.Put(dec)
	return decompressFrameGroupWithDecoder(dec, body, subIndex)
}

func decompressFrameGroupWithDecoder(dec *zstd.Decoder, body []byte, subIndex int) ([]byte, error) {
	if len(body) < frameGroupHeader {
		return nil, errFrameGroupCorrupt
	}
	if body[0] != frameGroupVersion0 {
		return nil, errFrameGroupCorrupt
	}
	k := int(body[1])
	if k <= 0 {
		return nil, errFrameGroupCorrupt
	}
	offsetCount := int(binary.LittleEndian.Uint16(body[2:4]))
	if offsetCount != k+1 {
		return nil, errFrameGroupCorrupt
	}
	if subIndex < 0 || subIndex >= k {
		return nil, errFrameGroupCorrupt
	}

	offsetBytes := offsetCount * 4
	if len(body) < frameGroupHeader+offsetBytes {
		return nil, errFrameGroupCorrupt
	}
	offsets := body[4 : 4+offsetBytes]

	start := int(binary.LittleEndian.Uint32(offsets[subIndex*4 : subIndex*4+4]))
	end := int(binary.LittleEndian.Uint32(offsets[(subIndex+1)*4 : (subIndex+1)*4+4]))
	last := int(binary.LittleEndian.Uint32(offsets[offsetBytes-4:]))
	if start < 0 || end < start || last < end {
		return nil, errFrameGroupCorrupt
	}
	payloadLen := last
	if len(body) < frameGroupHeader+offsetBytes {
		return nil, errFrameGroupCorrupt
	}
	compressed := body[frameGroupHeader+offsetBytes:]
	if len(compressed) == 0 {
		return nil, errFrameGroupCorrupt
	}

	out, err := dec.DecodeAll(compressed, make([]byte, 0, payloadLen))
	if err != nil {
		return nil, err
	}
	if len(out) != payloadLen || end > len(out) {
		return nil, errFrameGroupCorrupt
	}
	return out[start:end], nil
}
