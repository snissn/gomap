package valuelog

import (
	"encoding/binary"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/page"
)

// PreparedGroupedRecord is a fully-prepared grouped value-log record (header +
// frame prefix + payload) ready to append.
//
// This is intended for higher-level pipelines that compute compression
// concurrently and then append frames sequentially.
type PreparedGroupedRecord struct {
	Record []byte
	K      int

	RawPayloadBytes    int
	StoredPayloadBytes int
	Attempted          bool
	Kept               bool
	EncodeNs           int64
}

// BuildPreparedGroupedRecordFromPayloadInto builds a grouped value-log record
// into dst and returns a PreparedGroupedRecord that can be appended via
// Writer.AppendPreparedGroupedRecordInto.
//
// payload must be either the raw concatenated values (kept=false) or the
// compressed payload bytes (kept=true).
func BuildPreparedGroupedRecordFromPayloadInto(dst []byte, dictID uint64, rids []uint64, offsets []uint32, rawPayloadBytes int, payload []byte, attempted bool, kept bool, encodeNs int64) (PreparedGroupedRecord, error) {
	k := len(rids)
	if k <= 0 || k > MaxFrameK {
		return PreparedGroupedRecord{}, ErrRecordTooLarge
	}
	if len(offsets) != k+1 {
		return PreparedGroupedRecord{}, ErrCorrupt
	}
	if dictID == 0 {
		kept = false
	}
	if rawPayloadBytes < 0 {
		return PreparedGroupedRecord{}, ErrRecordTooLarge
	}
	if rawPayloadBytes > int(^uint32(0)) {
		return PreparedGroupedRecord{}, ErrRecordTooLarge
	}
	if offsets[0] != 0 {
		return PreparedGroupedRecord{}, ErrCorrupt
	}
	if offsets[k] != uint32(rawPayloadBytes) {
		return PreparedGroupedRecord{}, ErrCorrupt
	}
	last := uint32(0)
	for i := 1; i < len(offsets); i++ {
		if offsets[i] < last {
			return PreparedGroupedRecord{}, ErrCorrupt
		}
		last = offsets[i]
	}
	if limits.MaxRecordSize > 0 && int64(rawPayloadBytes) > limits.MaxRecordSize {
		return PreparedGroupedRecord{}, ErrRecordTooLarge
	}

	prefixLen := FrameHeaderSize + (k * 8) + ((k + 1) * 4)
	bodyLen := prefixLen + len(payload)
	if limits.MaxRecordSize > 0 && int64(HeaderSize+bodyLen) > limits.MaxRecordSize {
		return PreparedGroupedRecord{}, ErrRecordTooLarge
	}
	if bodyLen > int(^uint32(0)) {
		return PreparedGroupedRecord{}, ErrRecordTooLarge
	}
	if recordSizeExceedsMax(uint32(bodyLen)) {
		return PreparedGroupedRecord{}, ErrRecordTooLarge
	}

	recordLen := HeaderSize + bodyLen
	if cap(dst) < recordLen {
		dst = make([]byte, recordLen)
	} else {
		dst = dst[:recordLen]
	}

	header := dst[:HeaderSize]
	prefix := dst[HeaderSize : HeaderSize+prefixLen]
	payloadDst := dst[HeaderSize+prefixLen:]
	copy(payloadDst, payload)

	header[4] = Version
	header[5] = recordFlagGrouped
	header[6] = 0
	header[7] = 0
	binary.LittleEndian.PutUint64(header[8:16], 0)
	binary.LittleEndian.PutUint32(header[16:20], uint32(bodyLen))

	prefixOff := 0
	prefix[prefixOff] = FrameVersion
	if kept {
		prefix[prefixOff+1] = FrameFlagCompressed
	} else {
		prefix[prefixOff+1] = 0
	}
	prefix[prefixOff+2] = byte(k)
	prefix[prefixOff+3] = 0
	binary.LittleEndian.PutUint64(prefix[prefixOff+4:prefixOff+12], dictID)
	prefixOff += FrameHeaderSize

	for i := 0; i < k; i++ {
		rid := rids[i]
		if rid == 0 {
			return PreparedGroupedRecord{}, errors.New("valuelog: missing rid")
		}
		binary.LittleEndian.PutUint64(prefix[prefixOff:prefixOff+8], rid)
		prefixOff += 8
	}
	for i := 0; i < k+1; i++ {
		binary.LittleEndian.PutUint32(prefix[prefixOff:prefixOff+4], offsets[i])
		prefixOff += 4
	}

	sum := crc.ChecksumParts(header[4:HeaderSize], prefix, payloadDst)
	binary.LittleEndian.PutUint32(header[0:4], sum)

	return PreparedGroupedRecord{
		Record:             dst,
		K:                  k,
		RawPayloadBytes:    rawPayloadBytes,
		StoredPayloadBytes: len(payloadDst),
		Attempted:          attempted,
		Kept:               kept,
		EncodeNs:           encodeNs,
	}, nil
}

// AppendPreparedGroupedRecordInto appends a fully-prepared grouped record and
// fills dst with the returned pointers.
func (w *Writer) AppendPreparedGroupedRecordInto(prep PreparedGroupedRecord, dst []page.ValuePtr) ([]page.ValuePtr, FrameStats, error) {
	if w == nil {
		return nil, FrameStats{}, errors.New("valuelog: nil writer")
	}
	if prep.K <= 0 || prep.K > MaxFrameK {
		return nil, FrameStats{}, ErrRecordTooLarge
	}
	if len(dst) < prep.K {
		return nil, FrameStats{}, errors.New("valuelog: dst too small")
	}
	dst = dst[:prep.K]
	if len(prep.Record) < HeaderSize {
		return nil, FrameStats{}, ErrCorrupt
	}
	if prep.Record[4] != Version {
		return nil, FrameStats{}, ErrCorrupt
	}
	if prep.Record[5]&recordFlagGrouped == 0 {
		return nil, FrameStats{}, ErrCorrupt
	}
	if len(prep.Record) < HeaderSize+FrameHeaderSize {
		return nil, FrameStats{}, ErrCorrupt
	}
	bodyLen := binary.LittleEndian.Uint32(prep.Record[16:20])
	if int(bodyLen) != len(prep.Record)-HeaderSize {
		return nil, FrameStats{}, ErrCorrupt
	}
	if prep.Record[HeaderSize] != FrameVersion {
		return nil, FrameStats{}, ErrCorrupt
	}
	if int(prep.Record[HeaderSize+2]) != prep.K {
		return nil, FrameStats{}, ErrCorrupt
	}

	start := w.size
	if err := w.writeBytes(prep.Record); err != nil {
		return nil, FrameStats{}, err
	}
	w.size += int64(len(prep.Record))

	recordLenHint := uint32(headerWithoutCRC) + bodyLen
	if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
		recordLenHint = 0
	}
	for i := range dst {
		dst[i] = page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLenHint, uint8(i)),
			FileID: w.fileID,
		}
	}

	return dst, FrameStats{
		Records:            prep.K,
		RawPayloadBytes:    prep.RawPayloadBytes,
		StoredPayloadBytes: prep.StoredPayloadBytes,
		Attempted:          prep.Attempted,
		Kept:               prep.Kept,
		EncodeNs:           prep.EncodeNs,
	}, nil
}
