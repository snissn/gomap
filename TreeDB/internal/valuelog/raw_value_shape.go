package valuelog

import (
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/template"
)

// ErrUnboundedValueShape reports a value-log record whose decoded length
// cannot be established by inspecting its raw record/frame prefix alone.
var ErrUnboundedValueShape = errors.New("valuelog: raw value shape exceeds bound or requires decoding")

// InspectRawValueLength inspects a pointer without decoding or allocating a
// record payload. Prepared catalog publication uses this before accepting a
// pointer-backed root descriptor. Compressed, template-coded and compact-leaf
// payloads are ineligible because their decoded backing may exceed their
// encoded record length.
func (f *File) InspectRawValueLength(ptr page.ValuePtr, maxDecoded int64) (int64, error) {
	if f == nil || f.File == nil {
		return 0, ErrCorrupt
	}
	if err := f.ensureCurrentWritableReadableFor(ptr); err != nil {
		return 0, err
	}
	return inspectRawValueLengthAt(f.File, ptr, maxDecoded)
}

func (m *Manager) InspectRawValueLength(ptr page.ValuePtr, maxDecoded int64) (int64, error) {
	f, err := m.fileFor(ptr.FileID)
	if err != nil {
		return 0, err
	}
	return f.InspectRawValueLength(ptr, maxDecoded)
}

func (s *Set) InspectRawValueLength(ptr page.ValuePtr, maxDecoded int64) (int64, error) {
	f, ok := s.Files[ptr.FileID]
	if !ok {
		return 0, &fileNotFoundError{id: ptr.FileID, inSnapshot: true}
	}
	return f.InspectRawValueLength(ptr, maxDecoded)
}

func inspectRawValueLengthAt(r io.ReaderAt, ptr page.ValuePtr, maxDecoded int64) (int64, error) {
	if r == nil || ptr.Offset < 4 || ptr.Offset > math.MaxInt64-HeaderSize || maxDecoded < 0 {
		return 0, ErrCorrupt
	}
	start := int64(ptr.Offset - 4)
	var header [HeaderSize]byte
	if _, err := r.ReadAt(header[:], start); err != nil {
		return 0, err
	}
	if header[4] != Version {
		return 0, ErrCorrupt
	}
	storedLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(storedLen) || uint64(storedLen)+headerWithoutCRC > uint64(^uint32(0)) {
		return 0, ErrRecordTooLarge
	}
	if !page.ValuePtrRecordLengthHintMatches(ptr, uint32(headerWithoutCRC)+storedLen) {
		return 0, ErrCorrupt
	}
	grouped := header[5]&recordFlagGrouped != 0
	if grouped != page.ValuePtrIsGrouped(ptr) || (!grouped && page.ValuePtrIsCompressed(ptr)) {
		return 0, ErrUnboundedValueShape
	}
	valueOffset := start + HeaderSize
	valueLen := int64(storedLen)
	if grouped {
		var frameHeader [FrameHeaderSize]byte
		if _, err := r.ReadAt(frameHeader[:], valueOffset); err != nil {
			return 0, err
		}
		if frameHeader[0] != FrameVersion || frameHeader[2] == 0 {
			return 0, ErrCorrupt
		}
		if frameHeader[1]&FrameFlagCompressed != 0 {
			return 0, ErrUnboundedValueShape
		}
		k := int(frameHeader[2])
		subIndex := int(page.ValuePtrSubIndex(ptr))
		if subIndex >= k {
			return 0, ErrCorrupt
		}
		prefixLen := FrameHeaderSize + k*8 + (k+1)*4
		if int64(prefixLen) > valueLen {
			return 0, ErrCorrupt
		}
		var offsets [MaxFrameK + 1]uint32
		var offsetBytes [(MaxFrameK + 1) * 4]byte
		if _, err := r.ReadAt(offsetBytes[:(k+1)*4], valueOffset+int64(FrameHeaderSize+k*8)); err != nil {
			return 0, err
		}
		for i := 0; i <= k; i++ {
			offsets[i] = binary.LittleEndian.Uint32(offsetBytes[i*4 : i*4+4])
			if i > 0 && offsets[i] < offsets[i-1] {
				return 0, ErrCorrupt
			}
		}
		if int64(offsets[k])+int64(prefixLen) != valueLen {
			return 0, ErrCorrupt
		}
		valueLen = int64(offsets[subIndex+1] - offsets[subIndex])
		valueOffset += int64(prefixLen) + int64(offsets[subIndex])
	}
	if valueLen > maxDecoded {
		return 0, ErrUnboundedValueShape
	}
	var marker [compactLeafPagePayloadHeaderSize]byte
	if valueLen > 0 {
		n := min(int(valueLen), len(marker))
		if _, err := r.ReadAt(marker[:n], valueOffset); err != nil {
			return 0, err
		}
		if template.IsEncodedPayload(marker[:n]) || HasCompactLeafLogPayload(marker[:n]) {
			return 0, ErrUnboundedValueShape
		}
	}
	return valueLen, nil
}
