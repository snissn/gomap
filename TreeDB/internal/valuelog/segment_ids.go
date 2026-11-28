package valuelog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/snissn/gomap/TreeDB/internal/crc"
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
// Both paths verify record CRCs so bit flips in record/frame flags, dictionary
// IDs, or payload prefixes fail closed with ErrCorrupt. When scanTemplates is
// false, frame bodies are streamed through CRC verification into a small fixed
// buffer without decompressing or allocating per-record payloads.
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
		return scanSegmentDictIDs(f)
	}
	return scanSegmentDictAndTemplateIDs(f, fileID, dictLookup)
}

func discardAndUpdateCRC(r io.Reader, n int64, sum uint32, scratch []byte) (uint32, error) {
	rem := n
	for rem > 0 {
		chunk := len(scratch)
		if int64(chunk) > rem {
			chunk = int(rem)
		}
		got, err := io.ReadFull(r, scratch[:chunk])
		if got > 0 {
			sum = crc.Update(sum, scratch[:got])
			rem -= int64(got)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return sum, io.ErrUnexpectedEOF
			}
			return sum, err
		}
	}
	return sum, nil
}

func scanSegmentDictIDs(f *os.File) (SegmentCompressionIDs, error) {
	var res SegmentCompressionIDs
	br := bufio.NewReaderSize(f, 64<<10)
	var hdr [HeaderSize]byte
	var fh [FrameHeaderSize]byte
	scratch := make([]byte, discardScratchSize)

	for {
		_, err := io.ReadFull(br, hdr[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return res, err
		}
		crcVal := binary.LittleEndian.Uint32(hdr[0:4])
		if hdr[4] != Version {
			return res, ErrCorrupt
		}
		flags := hdr[5]
		if flags&^recordFlagGrouped != 0 {
			return res, ErrCorrupt
		}
		rid := binary.LittleEndian.Uint64(hdr[8:16])
		valueLenU32 := binary.LittleEndian.Uint32(hdr[16:20])
		if recordSizeExceedsMax(valueLenU32) {
			return res, ErrRecordTooLarge
		}
		res.RecordsScanned++
		valueLen := int64(valueLenU32)
		sum := crc.Update(0, hdr[4:])

		if flags&recordFlagGrouped == 0 {
			if rid == 0 {
				return res, ErrCorrupt
			}
			sum, err = discardAndUpdateCRC(br, valueLen, sum, scratch)
			if err != nil {
				return res, err
			}
			if sum != crcVal {
				return res, ErrCorrupt
			}
			continue
		}

		if valueLen < FrameHeaderSize {
			return res, ErrCorrupt
		}
		if _, err := io.ReadFull(br, fh[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return res, io.ErrUnexpectedEOF
			}
			return res, err
		}
		sum = crc.Update(sum, fh[:])
		if fh[0] != FrameVersion {
			return res, ErrCorrupt
		}
		k := int(fh[2])
		if k <= 0 || k > MaxFrameK {
			return res, ErrCorrupt
		}
		rem := valueLen - FrameHeaderSize
		sum, err = discardAndUpdateCRC(br, rem, sum, scratch)
		if err != nil {
			return res, err
		}
		if sum != crcVal {
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
		if flags&^recordFlagGrouped != 0 {
			return res, ErrCorrupt
		}
		rid := binary.LittleEndian.Uint64(hdr[8:16])
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
		crcVal := binary.LittleEndian.Uint32(hdr[0:4])
		if crc.ChecksumParts(hdr[4:], payload) != crcVal {
			return res, ErrCorrupt
		}

		if flags&recordFlagGrouped == 0 {
			if rid == 0 {
				return res, ErrCorrupt
			}
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
