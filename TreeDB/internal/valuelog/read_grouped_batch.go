package valuelog

import (
	"encoding/binary"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/page"
)

// groupedRecordBatchRun returns the maximal contiguous run starting at i that
// points at the same grouped value-log record. The caller still validates the
// record header/shape before trusting any value bytes.
func groupedRecordBatchRun(ptrs []page.ValuePtr, i int) int {
	if i < 0 || i >= len(ptrs) {
		return i
	}
	first := ptrs[i]
	if first.Offset < valueLogRecordCRCPrefixBytes || !page.ValuePtrIsGrouped(first) {
		return i + 1
	}
	recordLen := page.ValuePtrRecordLength(first)
	j := i + 1
	for j < len(ptrs) {
		ptr := ptrs[j]
		if ptr.FileID != first.FileID || ptr.Offset != first.Offset || !page.ValuePtrIsGrouped(ptr) || page.ValuePtrRecordLength(ptr) != recordLen {
			break
		}
		j++
	}
	return j
}

func (f *File) readUnsafeAppendGroupedRecordBatch(ptrs []page.ValuePtr, verifyCRC bool, dst [][]byte) (bool, error) {
	if f == nil || f.File == nil {
		return true, errors.New("valuelog: nil file")
	}
	// A batch scan should decode a grouped record once regardless of checksum
	// mode. Verified callers additionally amortize the record CRC; unchecked
	// maintenance callers avoid retaining every decoded frame in the shared
	// point-read cache.
	if len(ptrs) < 2 {
		return false, nil
	}
	first := ptrs[0]
	if first.Offset < valueLogRecordCRCPrefixBytes || !page.ValuePtrIsGrouped(first) {
		return false, nil
	}
	for _, ptr := range ptrs[1:] {
		if ptr.FileID != first.FileID || ptr.Offset != first.Offset || !page.ValuePtrIsGrouped(ptr) || page.ValuePtrRecordLength(ptr) != page.ValuePtrRecordLength(first) {
			return false, nil
		}
	}
	if err := f.ensureCurrentWritableReadableFor(first); err != nil {
		return true, err
	}
	if ok, err := f.readViaMmapAppendGroupedRecordBatch(ptrs, verifyCRC, dst); ok {
		f.mmapReadHits.Add(1)
		return true, err
	}
	data, _ := f.mmapData.Load().([]byte)
	if data != nil && deadMappingsCapExhausted(f.deadMappingsCount.Load(), len(data)) {
		f.mmapReadMissDeadMappingCap.Add(1)
	}
	f.mmapReadFallbackReadAt.Add(1)
	return true, f.readAtAppendGroupedRecordBatch(ptrs, verifyCRC, dst)
}

func (f *File) readViaMmapAppendGroupedRecordBatch(ptrs []page.ValuePtr, verifyCRC bool, dst [][]byte) (bool, error) {
	data, _ := f.mmapData.Load().([]byte)
	if data == nil {
		f.mmapReadMissNoMapping.Add(1)
		f.maybeScheduleRemap()
		return false, nil
	}
	ptr := ptrs[0]
	if ptr.Offset < valueLogRecordCRCPrefixBytes {
		return true, ErrCorrupt
	}
	start := int64(ptr.Offset - valueLogRecordCRCPrefixBytes)
	fullRangeReadable := false
	if refreshed, ok, full := f.ensureMmapRecordInitialRange(data, ptr, start); ok {
		data = refreshed
		fullRangeReadable = full
	} else {
		f.mmapReadMissOutOfRange.Add(1)
		return false, nil
	}

	header := data[start : start+HeaderSize]
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return true, ErrRecordTooLarge
	}
	end := start + HeaderSize + int64(valueLen)
	if !fullRangeReadable {
		if refreshed, ok := f.ensureMmapRangeReadable(data, start, end); ok {
			data = refreshed
			header = data[start : start+HeaderSize]
		} else {
			f.mmapReadMissOutOfRange.Add(1)
			return false, nil
		}
	}
	payload := data[start+HeaderSize : end]
	return true, f.appendGroupedRecordBatchFromRecord(header, payload, ptrs, verifyCRC, dst)
}

func (f *File) readAtAppendGroupedRecordBatch(ptrs []page.ValuePtr, verifyCRC bool, dst [][]byte) error {
	ptr := ptrs[0]
	if ptr.Offset < valueLogRecordCRCPrefixBytes {
		return ErrCorrupt
	}
	start := int64(ptr.Offset - valueLogRecordCRCPrefixBytes)
	header := getHeaderScratch()
	defer putHeaderScratch(header)
	if _, err := f.File.ReadAt(header[:], start); err != nil {
		return err
	}
	if header[4] != Version || header[5]&recordFlagGrouped == 0 {
		return ErrCorrupt
	}
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return ErrRecordTooLarge
	}
	expectedLen := uint32(headerWithoutCRC) + valueLen
	first := ptrs[0]
	for _, ptr := range ptrs {
		if ptr.FileID != first.FileID || ptr.Offset != first.Offset || !page.ValuePtrIsGrouped(ptr) || !page.ValuePtrRecordLengthHintMatches(ptr, expectedLen) {
			return ErrCorrupt
		}
	}
	payloadScratch := getDecodeScratch(int(valueLen))
	payload := payloadScratch[:int(valueLen)]
	if _, err := f.File.ReadAt(payload, start+HeaderSize); err != nil {
		putDecodeScratch(payloadScratch)
		return err
	}
	err := f.appendGroupedRecordBatchFromRecord(header[:], payload, ptrs, verifyCRC, dst)
	putDecodeScratch(payloadScratch)
	return err
}

func parseGroupedFramePayloadAll(payload []byte) (FrameHeader, int, [MaxFrameK + 1]uint32, uint32, []byte, error) {
	var offsets [MaxFrameK + 1]uint32
	if len(payload) < FrameHeaderSize {
		return FrameHeader{}, 0, offsets, 0, nil, ErrCorrupt
	}
	if payload[0] != FrameVersion {
		return FrameHeader{}, 0, offsets, 0, nil, ErrCorrupt
	}
	k := int(payload[2])
	if k <= 0 || k > MaxFrameK {
		return FrameHeader{}, 0, offsets, 0, nil, ErrCorrupt
	}
	ridBytes := k * 8
	offsetBytes := (k + 1) * 4
	prefixLen := FrameHeaderSize + ridBytes + offsetBytes
	if len(payload) < prefixLen {
		return FrameHeader{}, 0, offsets, 0, nil, ErrCorrupt
	}
	frame := FrameHeader{
		Version:  payload[0],
		Flags:    payload[1],
		K:        uint8(k),
		Reserved: payload[3],
		DictID:   binary.LittleEndian.Uint64(payload[4:12]),
	}
	ridOff := FrameHeaderSize
	for i := 0; i < k; i++ {
		if binary.LittleEndian.Uint64(payload[ridOff:ridOff+8]) == 0 {
			return FrameHeader{}, 0, offsets, 0, nil, ErrCorrupt
		}
		ridOff += 8
	}
	off := FrameHeaderSize + ridBytes
	prev := uint32(0)
	for i := 0; i < k+1; i++ {
		cur := binary.LittleEndian.Uint32(payload[off : off+4])
		if cur < prev {
			return FrameHeader{}, 0, offsets, 0, nil, ErrCorrupt
		}
		offsets[i] = cur
		prev = cur
		off += 4
	}
	rawLen := offsets[k]
	framePayload := payload[prefixLen:]
	if len(framePayload) == 0 && rawLen != 0 {
		return FrameHeader{}, 0, offsets, 0, nil, ErrCorrupt
	}
	if frame.Flags&FrameFlagCompressed == 0 && uint32(len(framePayload)) != rawLen {
		return FrameHeader{}, 0, offsets, 0, nil, ErrCorrupt
	}
	return frame, k, offsets, rawLen, framePayload, nil
}

func (f *File) appendGroupedRecordBatchFromRecord(header []byte, payload []byte, ptrs []page.ValuePtr, verifyCRC bool, dst [][]byte) error {
	if len(header) < HeaderSize || len(ptrs) == 0 || len(dst) < len(ptrs) {
		return ErrCorrupt
	}
	first := ptrs[0]
	if first.Offset < valueLogRecordCRCPrefixBytes || !page.ValuePtrIsGrouped(first) {
		return ErrCorrupt
	}
	crcVal := binary.LittleEndian.Uint32(header[0:4])
	if header[4] != Version {
		return ErrCorrupt
	}
	if header[5]&recordFlagGrouped == 0 {
		return ErrCorrupt
	}
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) || int(valueLen) != len(payload) {
		return ErrCorrupt
	}
	expectedLen := uint32(headerWithoutCRC) + valueLen
	for _, ptr := range ptrs {
		if ptr.FileID != first.FileID || ptr.Offset != first.Offset || !page.ValuePtrIsGrouped(ptr) || !page.ValuePtrRecordLengthHintMatches(ptr, expectedLen) {
			return ErrCorrupt
		}
	}
	if verifyCRC {
		f.noteRecordCRCCheck()
		if sum := crc.ChecksumParts(header[4:HeaderSize], payload); sum != crcVal {
			return ErrCorrupt
		}
	}

	frame, k, offsets, rawLen, framePayload, err := parseGroupedFramePayloadAll(payload)
	if err != nil {
		return err
	}
	if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
		return ErrRecordTooLarge
	}
	for _, ptr := range ptrs {
		subIndex := int(page.ValuePtrSubIndex(ptr))
		if subIndex < 0 || subIndex >= k {
			return ErrCorrupt
		}
		valStart := offsets[subIndex]
		valEnd := offsets[subIndex+1]
		if valEnd < valStart || valEnd > rawLen {
			return ErrCorrupt
		}
	}

	raw := framePayload
	pooledRaw := false
	if frame.Flags&FrameFlagCompressed != 0 {
		decodeDst := f.takeDecodeScratch(int(rawLen))
		pooledRaw = true
		raw, err = decodeFramePayloadTo(frame, framePayload, f.dictLookup, rawLen, decodeDst)
		if err != nil {
			f.releaseDecodeScratch(decodeDst)
			return err
		}
		if uint32(len(raw)) != rawLen {
			f.releaseDecodeScratch(raw)
			return ErrCorrupt
		}
	}
	if pooledRaw {
		defer f.releaseDecodeScratch(raw)
	}

	for i, ptr := range ptrs {
		subIndex := int(page.ValuePtrSubIndex(ptr))
		valStart := offsets[subIndex]
		valEnd := offsets[subIndex+1]
		out, err := f.appendGroupedBatchValue(dst[i][:0], raw[valStart:valEnd])
		if err != nil {
			return err
		}
		dst[i] = out
	}
	return nil
}

func (f *File) appendGroupedBatchValue(dst []byte, val []byte) ([]byte, error) {
	oldLen := len(dst)
	out, err := appendDecodedTemplatePayload(dst, val, f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
	if err != nil {
		return nil, err
	}
	return f.appendMaybeDecodeLeafLogPayload(out[:oldLen], out[oldLen:])
}
