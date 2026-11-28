package valuelog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/snissn/gomap/TreeDB/internal/limits"
	templ "github.com/snissn/gomap/TreeDB/template"
)

// SegmentCompressionIDs summarizes the distinct dictionary IDs and template IDs
// referenced by records in a single value-log or leaf-log segment.
type SegmentCompressionIDs struct {
	DictIDs        map[uint64]struct{}
	TemplateIDs    map[uint64]struct{}
	RecordsScanned int64
	FramesScanned  int64
}

// ScanSegmentCompressionIDs scans a value-log or leaf-log segment and returns
// the distinct dictionary IDs (and, when scanTemplates is true, template IDs)
// referenced by its records.
//
// When scanTemplates is false, record and frame payload bodies are skipped with
// io.SeekCurrent without reading or decompressing payload bytes.
func ScanSegmentCompressionIDs(path string, fileID uint32, scanTemplates bool, dictLookup DictLookup) (SegmentCompressionIDs, error) {
	var res SegmentCompressionIDs
	if path == "" {
		return res, errors.New("valuelog: empty segment path")
	}
	f, err := os.Open(path)
	if err != nil {
		return res, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return res, err
	}
	if info.Size() == 0 {
		return res, nil
	}
	if !scanTemplates {
		return scanSegmentDictIDsSeek(f, info.Size())
	}
	return scanSegmentDictAndTemplateIDs(f, fileID, dictLookup)
}

func scanSegmentDictIDsSeek(f *os.File, fileSize int64) (SegmentCompressionIDs, error) {
	var res SegmentCompressionIDs
	var hdr [HeaderSize]byte
	var fh [FrameHeaderSize]byte
	var pos int64

	for pos < fileSize {
		_, err := io.ReadFull(f, hdr[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return res, err
		}
		if hdr[4] != Version {
			return res, ErrCorrupt
		}
		flags := hdr[5]
		valueLenU32 := binary.LittleEndian.Uint32(hdr[16:20])
		if recordSizeExceedsMax(valueLenU32) {
			return res, ErrRecordTooLarge
		}
		res.RecordsScanned++
		pos += HeaderSize
		valueLen := int64(valueLenU32)
		if pos+valueLen > fileSize {
			return res, io.ErrUnexpectedEOF
		}

		if flags&recordFlagGrouped == 0 {
			if valueLen > 0 {
				if _, err := f.Seek(valueLen, io.SeekCurrent); err != nil {
					return res, err
				}
				pos += valueLen
			}
			continue
		}

		if valueLen < FrameHeaderSize {
			return res, ErrCorrupt
		}
		if _, err := io.ReadFull(f, fh[:]); err != nil {
			return res, err
		}
		pos += FrameHeaderSize
		if fh[0] != FrameVersion {
			return res, ErrCorrupt
		}
		k := int(fh[2])
		if k <= 0 || k > MaxFrameK {
			return res, ErrCorrupt
		}
		res.FramesScanned++
		frameFlags := fh[1]
		dictID := binary.LittleEndian.Uint64(fh[4:12])
		if frameFlags&FrameFlagCompressed != 0 && dictID != 0 {
			if res.DictIDs == nil {
				res.DictIDs = make(map[uint64]struct{}, 4)
			}
			res.DictIDs[dictID] = struct{}{}
		}
		rem := valueLen - FrameHeaderSize
		if rem > 0 {
			if _, err := f.Seek(rem, io.SeekCurrent); err != nil {
				return res, err
			}
			pos += rem
		}
	}
	return res, nil
}

func scanSegmentDictAndTemplateIDs(f *os.File, _ uint32, dictLookup DictLookup) (SegmentCompressionIDs, error) {
	var res SegmentCompressionIDs
	br := bufio.NewReaderSize(f, 64<<10)
	var hdr [HeaderSize]byte
	var payloadBuf []byte

	for {
		_, err := io.ReadFull(br, hdr[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return res, err
		}
		if hdr[4] != Version {
			return res, ErrCorrupt
		}
		flags := hdr[5]
		valueLenU32 := binary.LittleEndian.Uint32(hdr[16:20])
		if recordSizeExceedsMax(valueLenU32) {
			return res, ErrRecordTooLarge
		}
		res.RecordsScanned++
		valueLen := int(valueLenU32)
		if cap(payloadBuf) < valueLen {
			payloadBuf = make([]byte, valueLen)
		}
		payload := payloadBuf[:valueLen]
		if _, err := io.ReadFull(br, payload); err != nil {
			return res, err
		}

		if flags&recordFlagGrouped == 0 {
			if templ.IsEncodedPayload(payload) {
				if tid, err := templ.EncodedPayloadTemplateID(payload); err == nil && tid != 0 {
					if res.TemplateIDs == nil {
						res.TemplateIDs = make(map[uint64]struct{}, 8)
					}
					res.TemplateIDs[tid] = struct{}{}
				}
			}
			continue
		}

		frameHeader, _, offsets, framePayload, err := DecodeFrame(payload)
		if err != nil {
			return res, err
		}
		res.FramesScanned++
		if frameHeader.Flags&FrameFlagCompressed != 0 && frameHeader.DictID != 0 {
			if res.DictIDs == nil {
				res.DictIDs = make(map[uint64]struct{}, 4)
			}
			res.DictIDs[frameHeader.DictID] = struct{}{}
		}
		rawLen := offsets[len(offsets)-1]
		if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
			return res, ErrRecordTooLarge
		}
		raw, err := decodeFramePayload(frameHeader, framePayload, dictLookup, rawLen)
		if err != nil {
			return res, err
		}
		for i := 0; i+1 < len(offsets); i++ {
			start := offsets[i]
			end := offsets[i+1]
			if end < start || end > uint32(len(raw)) {
				return res, ErrCorrupt
			}
			val := raw[start:end]
			if templ.IsEncodedPayload(val) {
				if tid, err := templ.EncodedPayloadTemplateID(val); err == nil && tid != 0 {
					if res.TemplateIDs == nil {
						res.TemplateIDs = make(map[uint64]struct{}, 8)
					}
					res.TemplateIDs[tid] = struct{}{}
				}
			}
		}
	}
	return res, nil
}
