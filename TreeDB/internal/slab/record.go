package slab

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"treedb/internal/crc"
	"treedb/internal/page"
)

var (
	ErrKeyTooLarge     = errors.New("slab record key too large")
	ErrValueTooLarge   = errors.New("slab record value too large")
	ErrRecordCorrupt   = errors.New("slab record corrupt")
	ErrRecordTruncated = errors.New("slab record truncated")
)

const (
	recordCRCLen             = 4
	recordKeyLenLen          = 2
	recordValueLenLen        = 4
	recordProtectedHeaderLen = recordKeyLenLen + recordValueLenLen
	recordHeaderLen          = recordCRCLen + recordProtectedHeaderLen
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func recordLengths(key, value []byte) (keyLen uint16, valueLen uint32, protectedLen int, recordLen int, _ error) {
	if len(key) > math.MaxUint16 {
		return 0, 0, 0, 0, ErrKeyTooLarge
	}
	if len(value) > math.MaxUint32 {
		return 0, 0, 0, 0, ErrValueTooLarge
	}
	keyLen = uint16(len(key))
	valueLen = uint32(len(value))
	protectedLen = recordProtectedHeaderLen + len(key) + len(value)
	recordLen = recordCRCLen + protectedLen
	return keyLen, valueLen, protectedLen, recordLen, nil
}

func recordChecksum(keyLen uint16, valueLen uint32, key, value []byte) uint32 {
	var hdr [recordProtectedHeaderLen]byte
	binary.LittleEndian.PutUint16(hdr[0:2], keyLen)
	binary.LittleEndian.PutUint32(hdr[2:6], valueLen)

	sum := crc32.Update(0, castagnoliTable, hdr[:])
	sum = crc32.Update(sum, castagnoliTable, key)
	sum = crc32.Update(sum, castagnoliTable, value)
	return sum
}

func encodeRecordHeader(dst []byte, sum uint32, keyLen uint16, valueLen uint32) {
	binary.LittleEndian.PutUint32(dst[0:4], sum)
	binary.LittleEndian.PutUint16(dst[4:6], keyLen)
	binary.LittleEndian.PutUint32(dst[6:10], valueLen)
}

// EncodeRecordAt encodes a slab record for key/value beginning at baseOffset
// within slab file fileID. It returns the record bytes and a ValuePtr that
// points to the KeyLen field (immediately after the CRC).
func EncodeRecordAt(key, value []byte, fileID uint32, baseOffset uint64) ([]byte, page.ValuePtr, error) {
	keyLen, valueLen, protectedLen, recordLen, err := recordLengths(key, value)
	if err != nil {
		return nil, page.ValuePtr{}, err
	}
	rec := make([]byte, recordLen)

	sum := recordChecksum(keyLen, valueLen, key, value)
	encodeRecordHeader(rec[0:recordHeaderLen], sum, keyLen, valueLen)
	copy(rec[recordHeaderLen:recordHeaderLen+len(key)], key)
	copy(rec[recordHeaderLen+len(key):], value)

	ptr := page.ValuePtr{
		Offset: baseOffset + recordCRCLen,
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
