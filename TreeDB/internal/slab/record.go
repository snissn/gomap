package slab

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/page"
)

var (
	ErrKeyTooLarge   = errors.New("slab record key too large")
	ErrValueTooLarge = errors.New("slab record value too large")
	ErrRecordCorrupt = errors.New("slab record corrupt")
	ErrRecordTruncated = errors.New("slab record truncated")
)

// EncodeRecordAt encodes a slab record for key/value beginning at baseOffset
// within slab file fileID. It returns the record bytes and a ValuePtr that
// points to the KeyLen field (immediately after the CRC).
func EncodeRecordAt(key, value []byte, fileID uint32, baseOffset uint64) ([]byte, page.ValuePtr, error) {
	if len(key) > math.MaxUint16 {
		return nil, page.ValuePtr{}, ErrKeyTooLarge
	}
	if len(value) > math.MaxUint32 {
		return nil, page.ValuePtr{}, ErrValueTooLarge
	}

	protectedLen := 2 + 4 + len(key) + len(value)
	recordLen := 4 + protectedLen
	rec := make([]byte, recordLen)

	// Layout after CRC.
	binary.LittleEndian.PutUint16(rec[4:6], uint16(len(key)))
	binary.LittleEndian.PutUint32(rec[6:10], uint32(len(value)))
	copy(rec[10:10+len(key)], key)
	copy(rec[10+len(key):], value)

	sum := crc.Checksum(rec[4:])
	binary.LittleEndian.PutUint32(rec[0:4], sum)

	ptr := page.ValuePtr{
		Offset: baseOffset + 4,
		Length: uint32(protectedLen),
		FileID: fileID,
	}

	return rec, ptr, nil
}

// DecodeRecord reads and verifies a slab record from buf using ptr.
// buf should contain the slab bytes starting at offset 0 for ptr.FileID.
// Returned key and value are copies.
func DecodeRecord(buf []byte, ptr page.ValuePtr) ([]byte, []byte, error) {
	if ptr.Offset < 4 {
		return nil, nil, ErrRecordCorrupt
	}
	recordStart := ptr.Offset - 4
	recordEnd := ptr.Offset + uint64(ptr.Length)
	if recordEnd > uint64(len(buf)) {
		return nil, nil, ErrRecordTruncated
	}
	if recordStart+4 > uint64(len(buf)) {
		return nil, nil, ErrRecordTruncated
	}

	start := int(recordStart)
	end := int(recordEnd)
	wantCRC := binary.LittleEndian.Uint32(buf[start : start+4])
	protected := buf[int(ptr.Offset):end]
	if err := crc.Verify(protected, wantCRC); err != nil {
		return nil, nil, err
	}

	if len(protected) < 6 {
		return nil, nil, ErrRecordCorrupt
	}
	keyLen := binary.LittleEndian.Uint16(protected[0:2])
	valueLen := binary.LittleEndian.Uint32(protected[2:6])

	expLen := uint32(2 + 4 + int(keyLen) + int(valueLen))
	if expLen != ptr.Length {
		return nil, nil, fmt.Errorf("%w: length mismatch", ErrRecordCorrupt)
	}
	if int(expLen) > len(protected) {
		return nil, nil, ErrRecordTruncated
	}

	keyStart := 6
	keyEnd := keyStart + int(keyLen)
	valStart := keyEnd
	valEnd := valStart + int(valueLen)
	if valEnd > len(protected) {
		return nil, nil, ErrRecordTruncated
	}

	keyOut := append([]byte(nil), protected[keyStart:keyEnd]...)
	valOut := append([]byte(nil), protected[valStart:valEnd]...)
	return keyOut, valOut, nil
}

// NextRecordOffset returns the offset of the KeyLen field for the next record
// following ptr.
func NextRecordOffset(ptr page.ValuePtr) uint64 {
	return ptr.Offset + uint64(ptr.Length) + 4
}
