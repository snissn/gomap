package valuelog

import (
	"encoding/binary"
	"runtime"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

// MaxDeadMappings caps the number of old mmaps retained to avoid exhausting
// vm.max_map_count. Set <= 0 to disable the cap.
//
// Each remap retains the previous mapping until the file is closed to avoid
// use-after-unmap with concurrent readers.
var MaxDeadMappings = 64

func (f *File) maybeScheduleRemap() {
	if f == nil || f.closed.Load() {
		return
	}
	if !f.remapRequested.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer f.remapRequested.Store(false)
		f.remapToFileSize()
	}()
}

func (f *File) remapToFileSize() {
	if f == nil || f.closed.Load() || f.File == nil {
		return
	}

	f.remapMu.Lock()
	defer f.remapMu.Unlock()
	if f.closed.Load() || f.File == nil {
		return
	}

	info, err := f.File.Stat()
	if err != nil {
		return
	}
	currentSize := info.Size()
	if currentSize <= 0 || currentSize > int64(int(currentSize)) {
		return
	}
	if runtime.GOARCH == "386" && currentSize > int64(^uint32(0)) {
		return
	}

	data, _ := f.mmapData.Load().([]byte)
	if data != nil && int64(len(data)) >= currentSize {
		return
	}
	if data != nil && MaxDeadMappings > 0 && len(f.deadMappings) >= MaxDeadMappings {
		return
	}
	if data != nil {
		f.deadMappings = append(f.deadMappings, data)
		f.deadMappingsCount.Add(1)
	}

	b, err := mmapReadOnly(f.File, int(currentSize))
	if err != nil {
		return
	}
	f.mmapData.Store(b)
	f.remapCount.Add(1)
}

func (f *File) readViaMmap(ptr page.ValuePtr, verifyCRC bool) ([]byte, error, bool) {
	val, err, ok := f.readViaMmapAppend(ptr, verifyCRC, nil)
	if !ok || err != nil {
		return nil, err, ok
	}
	// readViaMmapAppend already returns a copy (it appends into dst).
	return val, nil, true
}

// readViaMmapView returns a best-effort zero-copy view into the current mmap.
//
// It mirrors readViaMmapAppend's decoding logic but avoids copying for
// uncompressed records. The returned slice is only valid as long as the mapping
// remains alive; callers must treat it as ephemeral/unsafe.
func (f *File) readViaMmapView(ptr page.ValuePtr, verifyCRC bool) ([]byte, error, bool) {
	data, _ := f.mmapData.Load().([]byte)
	if data == nil {
		f.maybeScheduleRemap()
		return nil, nil, false
	}
	if ptr.Offset < 4 {
		return nil, ErrCorrupt, true
	}
	start := int64(ptr.Offset - 4)
	if start < 0 || start+HeaderSize > int64(len(data)) {
		f.maybeScheduleRemap()
		return nil, nil, false
	}

	header := data[start : start+HeaderSize]
	crcVal := binary.LittleEndian.Uint32(header[0:4])
	version := header[4]
	if version != Version {
		return nil, ErrCorrupt, true
	}
	flags := header[5]
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return nil, ErrRecordTooLarge, true
	}
	if recordLen := page.ValuePtrRecordLength(ptr); recordLen != 0 {
		expectedLen := uint32(headerWithoutCRC) + valueLen
		if recordLen != expectedLen {
			return nil, ErrCorrupt, true
		}
	}

	end := start + HeaderSize + int64(valueLen)
	if end > int64(len(data)) {
		f.maybeScheduleRemap()
		return nil, nil, false
	}

	payload := data[start+HeaderSize : end]
	if verifyCRC {
		sum := crc.ChecksumParts(header[4:], payload)
		if sum != crcVal {
			return nil, ErrCorrupt, true
		}
	}

	// Non-grouped record: the payload is the value.
	if flags&recordFlagGrouped == 0 {
		if f.templateLookup != nil && templ.IsEncodedPayload(payload) {
			decoded, err := templ.DecodePayloadAppend(nil, payload, func(id uint64) (templ.TemplateDef, error) {
				return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
			}, f.templateDecodeOpts)
			if err != nil {
				return nil, err, true
			}
			return decoded, nil, true
		}
		return payload, nil, true
	}

	if !page.ValuePtrIsGrouped(ptr) {
		return nil, ErrCorrupt, true
	}
	if int64(FrameHeaderSize) > int64(len(payload)) {
		return nil, ErrCorrupt, true
	}
	frameHeader := payload[:FrameHeaderSize]
	if frameHeader[0] != FrameVersion {
		return nil, ErrCorrupt, true
	}
	k := int(frameHeader[2])
	if k <= 0 || k > MaxFrameK {
		return nil, ErrCorrupt, true
	}
	fFlags := frameHeader[1]

	decodeTemplatePayload := func(val []byte) ([]byte, error) {
		if f.templateLookup == nil || !templ.IsEncodedPayload(val) {
			return val, nil
		}
		return templ.DecodePayloadAppend(nil, val, func(id uint64) (templ.TemplateDef, error) {
			return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
		}, f.templateDecodeOpts)
	}

	subIndex := int(page.ValuePtrSubIndex(ptr))
	if subIndex < 0 || subIndex >= k {
		return nil, ErrCorrupt, true
	}
	ridBytes := k * 8
	offsetBytes := (k + 1) * 4
	prefixLen := FrameHeaderSize + ridBytes + offsetBytes
	if int(valueLen) < prefixLen {
		return nil, ErrCorrupt, true
	}

	// Cache hit?
	if f.cacheStart.Load() == start {
		f.cacheMu.Lock()
		hit := f.cacheStart.Load() == start &&
			f.cacheK > 0 &&
			f.cacheLen > 0 &&
			subIndex < f.cacheK &&
			(f.cacheFlags&FrameFlagCompressed) == (fFlags&FrameFlagCompressed)
		if fFlags&FrameFlagCompressed != 0 {
			hit = hit && len(f.cacheRaw) > 0 && !f.cacheRawPooled
		}
		if hit {
			cPrefixLen := f.cacheLen
			valStart := f.cacheOffs[subIndex]
			valEnd := f.cacheOffs[subIndex+1]
			rawLen := f.cacheOffs[f.cacheK]
			cachedRaw := f.cacheRaw
			f.cacheMu.Unlock()

			if valEnd < valStart || valEnd > rawLen {
				return nil, ErrCorrupt, true
			}
			if fFlags&FrameFlagCompressed == 0 && cPrefixLen+int(rawLen) != int(valueLen) {
				return nil, ErrCorrupt, true
			}

			if fFlags&FrameFlagCompressed != 0 {
				if uint32(len(cachedRaw)) != rawLen {
					return nil, ErrCorrupt, true
				}
				val, err := decodeTemplatePayload(cachedRaw[valStart:valEnd])
				if err != nil {
					return nil, err, true
				}
				return val, nil, true
			}

			srcStart := cPrefixLen + int(valStart)
			srcEnd := cPrefixLen + int(valEnd)
			if srcStart < 0 || srcEnd < srcStart || srcEnd > len(payload) {
				return nil, ErrCorrupt, true
			}
			val, err := decodeTemplatePayload(payload[srcStart:srcEnd])
			if err != nil {
				return nil, err, true
			}
			return val, nil, true
		}
		f.cacheMu.Unlock()
	}

	off := FrameHeaderSize + ridBytes
	var offsets [MaxFrameK + 1]uint32
	prev := uint32(0)
	for i := 0; i < k+1; i++ {
		cur := binary.LittleEndian.Uint32(payload[off : off+4])
		if cur < prev {
			return nil, ErrCorrupt, true
		}
		offsets[i] = cur
		prev = cur
		off += 4
	}

	rawLen := offsets[k]
	if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
		return nil, ErrRecordTooLarge, true
	}
	if fFlags&FrameFlagCompressed == 0 && prefixLen+int(rawLen) != int(valueLen) {
		return nil, ErrCorrupt, true
	}
	valStart := offsets[subIndex]
	valEnd := offsets[subIndex+1]
	if valEnd < valStart || valEnd > rawLen {
		return nil, ErrCorrupt, true
	}

	if fFlags&FrameFlagCompressed != 0 {
		if len(payload) < prefixLen {
			return nil, ErrCorrupt, true
		}
		frame := FrameHeader{
			Version:  frameHeader[0],
			Flags:    fFlags,
			K:        uint8(k),
			Reserved: frameHeader[3],
			DictID:   binary.LittleEndian.Uint64(frameHeader[4:12]),
		}
		raw, err := decodeFramePayloadTo(frame, payload[prefixLen:], f.dictLookup, rawLen, nil)
		if err != nil {
			return nil, err, true
		}
		if uint32(len(raw)) != rawLen {
			return nil, ErrCorrupt, true
		}

		// Publish cache for subsequent reads from this grouped record.
		f.cacheMu.Lock()
		f.cacheK = k
		f.cacheFlags = fFlags
		f.cacheLen = prefixLen
		f.cacheOffs = offsets
		// readViaMmapView can return a view into cached raw bytes, so this cache
		// must never own pooled decode scratch memory.
		f.setCacheRawLocked(raw, false)
		f.cacheStart.Store(start)
		f.cacheMu.Unlock()

		val, err := decodeTemplatePayload(raw[valStart:valEnd])
		if err != nil {
			return nil, err, true
		}
		return val, nil, true
	}

	// Publish prefix cache for subsequent uncompressed grouped reads.
	f.cacheMu.Lock()
	f.cacheK = k
	f.cacheFlags = fFlags
	f.cacheLen = prefixLen
	f.cacheOffs = offsets
	f.setCacheRawLocked(nil, false)
	f.cacheStart.Store(start)
	f.cacheMu.Unlock()

	srcStart := prefixLen + int(valStart)
	srcEnd := prefixLen + int(valEnd)
	if srcStart < 0 || srcEnd < srcStart || srcEnd > len(payload) {
		return nil, ErrCorrupt, true
	}
	val, err := decodeTemplatePayload(payload[srcStart:srcEnd])
	if err != nil {
		return nil, err, true
	}
	return val, nil, true
}

func (f *File) readViaMmapAppend(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, error, bool) {
	data, _ := f.mmapData.Load().([]byte)
	if data == nil {
		f.maybeScheduleRemap()
		return nil, nil, false
	}
	if ptr.Offset < 4 {
		return nil, ErrCorrupt, true
	}
	start := int64(ptr.Offset - 4)
	if start < 0 || start+HeaderSize > int64(len(data)) {
		f.maybeScheduleRemap()
		return nil, nil, false
	}

	header := data[start : start+HeaderSize]
	crcVal := binary.LittleEndian.Uint32(header[0:4])
	version := header[4]
	if version != Version {
		return nil, ErrCorrupt, true
	}
	flags := header[5]
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return nil, ErrRecordTooLarge, true
	}
	if recordLen := page.ValuePtrRecordLength(ptr); recordLen != 0 {
		expectedLen := uint32(headerWithoutCRC) + valueLen
		if recordLen != expectedLen {
			return nil, ErrCorrupt, true
		}
	}

	end := start + HeaderSize + int64(valueLen)
	if end > int64(len(data)) {
		f.maybeScheduleRemap()
		return nil, nil, false
	}

	payload := data[start+HeaderSize : end]
	if verifyCRC {
		sum := crc.ChecksumParts(header[4:], payload)
		if sum != crcVal {
			return nil, ErrCorrupt, true
		}
	}

	if flags&recordFlagGrouped == 0 {
		oldLen := len(dst)
		dst = grow(dst, len(payload))
		copy(dst[oldLen:], payload)
		if f.templateLookup != nil && templ.IsEncodedPayload(dst[oldLen:]) {
			payload := dst[oldLen:]
			decodedStart := len(dst)
			dst, err := templ.DecodePayloadAppend(dst, payload, func(id uint64) (templ.TemplateDef, error) {
				return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
			}, f.templateDecodeOpts)
			if err != nil {
				return nil, err, true
			}
			decodedLen := len(dst) - decodedStart
			copy(dst[oldLen:], dst[decodedStart:])
			dst = dst[:oldLen+decodedLen]
		}
		return dst, nil, true
	}
	if !page.ValuePtrIsGrouped(ptr) {
		return nil, ErrCorrupt, true
	}
	if int64(FrameHeaderSize) > int64(len(payload)) {
		return nil, ErrCorrupt, true
	}
	frameHeader := payload[:FrameHeaderSize]
	if frameHeader[0] != FrameVersion {
		return nil, ErrCorrupt, true
	}
	k := int(frameHeader[2])
	if k <= 0 || k > MaxFrameK {
		return nil, ErrCorrupt, true
	}
	fFlags := frameHeader[1]

	if fFlags&FrameFlagCompressed == 0 {
		subIndex := int(page.ValuePtrSubIndex(ptr))
		if subIndex < 0 || subIndex >= k {
			return nil, ErrCorrupt, true
		}
		ridBytes := k * 8
		offsetBytes := (k + 1) * 4
		prefixLen := FrameHeaderSize + ridBytes + offsetBytes
		if int(valueLen) < prefixLen {
			return nil, ErrCorrupt, true
		}

		if f.cacheStart.Load() == start {
			f.cacheMu.Lock()
			hit := f.cacheStart.Load() == start && f.cacheK > 0 && f.cacheLen > 0 && subIndex < f.cacheK && f.cacheFlags&FrameFlagCompressed == 0
			if hit {
				cPrefixLen := f.cacheLen
				valStart := f.cacheOffs[subIndex]
				valEnd := f.cacheOffs[subIndex+1]
				rawLen := f.cacheOffs[f.cacheK]
				f.cacheMu.Unlock()

				if valEnd < valStart || valEnd > rawLen {
					return nil, ErrCorrupt, true
				}
				if cPrefixLen+int(rawLen) != int(valueLen) {
					return nil, ErrCorrupt, true
				}
				n := int(valEnd - valStart)
				oldLen := len(dst)
				dst = grow(dst, n)
				srcStart := cPrefixLen + int(valStart)
				srcEnd := srcStart + n
				copy(dst[oldLen:], payload[srcStart:srcEnd])
				if f.templateLookup != nil && templ.IsEncodedPayload(dst[oldLen:]) {
					payload := dst[oldLen:]
					decodedStart := len(dst)
					dst, err := templ.DecodePayloadAppend(dst, payload, func(id uint64) (templ.TemplateDef, error) {
						return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
					}, f.templateDecodeOpts)
					if err != nil {
						return nil, err, true
					}
					decodedLen := len(dst) - decodedStart
					copy(dst[oldLen:], dst[decodedStart:])
					dst = dst[:oldLen+decodedLen]
				}
				return dst, nil, true
			}
			f.cacheMu.Unlock()
		}

		off := FrameHeaderSize + ridBytes
		var offsets [MaxFrameK + 1]uint32
		prev := uint32(0)
		for i := 0; i < k+1; i++ {
			cur := binary.LittleEndian.Uint32(payload[off : off+4])
			if cur < prev {
				return nil, ErrCorrupt, true
			}
			offsets[i] = cur
			prev = cur
			off += 4
		}

		rawLen := offsets[k]
		if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
			return nil, ErrRecordTooLarge, true
		}
		if prefixLen+int(rawLen) != int(valueLen) {
			return nil, ErrCorrupt, true
		}
		valStart := offsets[subIndex]
		valEnd := offsets[subIndex+1]
		if valEnd < valStart || valEnd > rawLen {
			return nil, ErrCorrupt, true
		}

		f.cacheMu.Lock()
		f.cacheK = k
		f.cacheFlags = fFlags
		f.cacheLen = prefixLen
		f.cacheOffs = offsets
		f.setCacheRawLocked(nil, false)
		f.cacheStart.Store(start)
		f.cacheMu.Unlock()

		n := int(valEnd - valStart)
		oldLen := len(dst)
		dst = grow(dst, n)
		srcStart := prefixLen + int(valStart)
		srcEnd := srcStart + n
		copy(dst[oldLen:], payload[srcStart:srcEnd])
		if f.templateLookup != nil && templ.IsEncodedPayload(dst[oldLen:]) {
			payload := dst[oldLen:]
			decodedStart := len(dst)
			dst, err := templ.DecodePayloadAppend(dst, payload, func(id uint64) (templ.TemplateDef, error) {
				return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
			}, f.templateDecodeOpts)
			if err != nil {
				return nil, err, true
			}
			decodedLen := len(dst) - decodedStart
			copy(dst[oldLen:], dst[decodedStart:])
			dst = dst[:oldLen+decodedLen]
		}
		return dst, nil, true
	}

	subIndex := int(page.ValuePtrSubIndex(ptr))
	if subIndex < 0 || subIndex >= k {
		return nil, ErrCorrupt, true
	}
	ridBytes := k * 8
	offsetBytes := (k + 1) * 4
	prefixLen := FrameHeaderSize + ridBytes + offsetBytes
	if int(valueLen) < prefixLen {
		return nil, ErrCorrupt, true
	}

	if f.cacheStart.Load() == start {
		f.cacheMu.Lock()
		hit := f.cacheStart.Load() == start &&
			f.cacheK > 0 &&
			f.cacheLen > 0 &&
			subIndex < f.cacheK &&
			(f.cacheFlags&FrameFlagCompressed) == (fFlags&FrameFlagCompressed) &&
			len(f.cacheRaw) > 0
		if hit {
			valStart := f.cacheOffs[subIndex]
			valEnd := f.cacheOffs[subIndex+1]
			rawLen := f.cacheOffs[f.cacheK]
			if valEnd < valStart || valEnd > rawLen {
				f.cacheMu.Unlock()
				return nil, ErrCorrupt, true
			}
			if uint32(len(f.cacheRaw)) != rawLen {
				f.cacheMu.Unlock()
				return nil, ErrCorrupt, true
			}
			n := int(valEnd - valStart)
			oldLen := len(dst)
			dst = grow(dst, n)
			copy(dst[oldLen:], f.cacheRaw[valStart:valEnd])
			f.cacheMu.Unlock()
			if f.templateLookup != nil && templ.IsEncodedPayload(dst[oldLen:]) {
				payload := dst[oldLen:]
				decodedStart := len(dst)
				var err error
				dst, err = templ.DecodePayloadAppend(dst, payload, func(id uint64) (templ.TemplateDef, error) {
					return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
				}, f.templateDecodeOpts)
				if err != nil {
					return nil, err, true
				}
				decodedLen := len(dst) - decodedStart
				copy(dst[oldLen:], dst[decodedStart:])
				dst = dst[:oldLen+decodedLen]
			}
			return dst, nil, true
		}
		f.cacheMu.Unlock()
	}
	off := FrameHeaderSize + ridBytes
	var offsets [MaxFrameK + 1]uint32
	prev := uint32(0)
	for i := 0; i < k+1; i++ {
		cur := binary.LittleEndian.Uint32(payload[off : off+4])
		if cur < prev {
			return nil, ErrCorrupt, true
		}
		offsets[i] = cur
		prev = cur
		off += 4
	}
	rawLen := offsets[k]
	if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
		return nil, ErrRecordTooLarge, true
	}
	valStart := offsets[subIndex]
	valEnd := offsets[subIndex+1]
	if valEnd < valStart || valEnd > rawLen {
		return nil, ErrCorrupt, true
	}

	frame := FrameHeader{
		Version:  frameHeader[0],
		Flags:    fFlags,
		K:        uint8(k),
		Reserved: frameHeader[3],
		DictID:   binary.LittleEndian.Uint64(frameHeader[4:12]),
	}
	scratch := getDecodeScratch(int(rawLen))
	raw, err := decodeFramePayloadTo(frame, payload[prefixLen:], f.dictLookup, rawLen, scratch)
	if err != nil {
		putDecodeScratch(scratch)
		return nil, err, true
	}
	if uint32(len(raw)) != rawLen {
		putDecodeScratch(raw)
		return nil, ErrCorrupt, true
	}

	n := int(valEnd - valStart)
	oldLen := len(dst)
	dst = grow(dst, n)
	copy(dst[oldLen:], raw[valStart:valEnd])

	f.cacheMu.Lock()
	f.cacheK = k
	f.cacheFlags = fFlags
	f.cacheLen = prefixLen
	f.cacheOffs = offsets
	f.setCacheRawLocked(raw, true)
	f.cacheStart.Store(start)
	f.cacheMu.Unlock()

	if f.templateLookup != nil && templ.IsEncodedPayload(dst[oldLen:]) {
		payload := dst[oldLen:]
		decodedStart := len(dst)
		dst, err = templ.DecodePayloadAppend(dst, payload, func(id uint64) (templ.TemplateDef, error) {
			return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
		}, f.templateDecodeOpts)
		if err != nil {
			return nil, err, true
		}
		decodedLen := len(dst) - decodedStart
		copy(dst[oldLen:], dst[decodedStart:])
		dst = dst[:oldLen+decodedLen]
	}
	return dst, nil, true
}
