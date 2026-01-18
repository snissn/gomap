package valuelog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/slab"
)

type Reader struct {
	f            *os.File
	r            *bufio.Reader
	pos          int64
	fileID       uint32
	verifies     bool
	decodeValues bool
	dictLookup   DictLookup
	pending      []frameEntry
	pendingIndex int
}

type frameEntry struct {
	rid   uint64
	value []byte
	ptr   page.ValuePtr
}

func NewReader(path string, fileID uint32) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{
		f:            f,
		r:            bufio.NewReaderSize(f, defaultBufferSize),
		fileID:       fileID,
		verifies:     true,
		decodeValues: true,
	}, nil
}

func (r *Reader) DisableChecksum() {
	r.verifies = false
}

func (r *Reader) DisableValueDecode() {
	r.decodeValues = false
}

func (r *Reader) SetDictLookup(lookup DictLookup) {
	r.dictLookup = lookup
}

func (r *Reader) ReadNext() (uint64, []byte, page.ValuePtr, error) {
	if r.pendingIndex < len(r.pending) {
		entry := r.pending[r.pendingIndex]
		r.pendingIndex++
		return entry.rid, entry.value, entry.ptr, nil
	}

	var header [HeaderSize]byte
	if _, err := io.ReadFull(r.r, header[:]); err != nil {
		return 0, nil, page.ValuePtr{}, err
	}

	crcVal := binary.LittleEndian.Uint32(header[0:4])
	version := header[4]
	if version != Version {
		return 0, nil, page.ValuePtr{}, ErrCorrupt
	}
	flags := header[5]
	rid := binary.LittleEndian.Uint64(header[8:16])
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return 0, nil, page.ValuePtr{}, ErrRecordTooLarge
	}

	payload := make([]byte, int(valueLen))
	if _, err := io.ReadFull(r.r, payload); err != nil {
		return 0, nil, page.ValuePtr{}, err
	}

	if r.verifies {
		sum := crc.ChecksumParts(header[4:], payload)
		if sum != crcVal {
			return 0, nil, page.ValuePtr{}, ErrCorrupt
		}
	}

	start := r.pos
	r.pos += int64(HeaderSize + valueLen)
	recordLen := uint32(headerWithoutCRC) + valueLen

	if flags&recordFlagGrouped == 0 {
		if rid == 0 {
			return 0, nil, page.ValuePtr{}, ErrCorrupt
		}
		ptr := page.ValuePtr{
			Offset: uint64(start + 4),
			Length: recordLen,
			FileID: r.fileID,
		}
		return rid, payload, ptr, nil
	}

	frameHeader, rids, offsets, framePayload, err := DecodeFrame(payload)
	if err != nil {
		return 0, nil, page.ValuePtr{}, err
	}
	rawLen := offsets[len(offsets)-1]
	if slab.MaxRecordSize > 0 && int64(rawLen) > slab.MaxRecordSize {
		return 0, nil, page.ValuePtr{}, ErrRecordTooLarge
	}

	var raw []byte
	if r.decodeValues {
		raw, err = decodeFramePayload(frameHeader, framePayload, r.dictLookup, rawLen)
		if err != nil {
			return 0, nil, page.ValuePtr{}, err
		}
	}

	r.pending = r.pending[:0]
	for i, frameRID := range rids {
		if frameRID == 0 {
			return 0, nil, page.ValuePtr{}, ErrCorrupt
		}
		ptr := page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLen, uint8(i)),
			FileID: r.fileID,
		}
		var val []byte
		if r.decodeValues {
			start := offsets[i]
			end := offsets[i+1]
			if end < start || end > uint32(len(raw)) {
				return 0, nil, page.ValuePtr{}, ErrCorrupt
			}
			val = raw[start:end]
		}
		r.pending = append(r.pending, frameEntry{rid: frameRID, value: val, ptr: ptr})
	}
	if len(r.pending) == 0 {
		return 0, nil, page.ValuePtr{}, ErrCorrupt
	}
	entry := r.pending[0]
	r.pendingIndex = 1
	return entry.rid, entry.value, entry.ptr, nil
}

func (r *Reader) Close() error {
	if r == nil || r.f == nil {
		return nil
	}
	return r.f.Close()
}

func decodeFramePayload(header FrameHeader, payload []byte, dictLookup DictLookup, rawLen uint32) ([]byte, error) {
	if header.Flags&FrameFlagCompressed == 0 {
		if uint32(len(payload)) != rawLen {
			return nil, ErrCorrupt
		}
		return payload, nil
	}
	if slab.MaxRecordSize > 0 && int64(rawLen) > slab.MaxRecordSize {
		return nil, ErrRecordTooLarge
	}
	var dict []byte
	if header.DictID != 0 {
		if dictLookup == nil {
			return nil, ErrMissingDict
		}
		var err error
		dict, err = dictLookup(header.DictID)
		if err != nil {
			return nil, err
		}
		if len(dict) == 0 {
			return nil, ErrMissingDict
		}
	}

	var (
		dec *zstd.Decoder
		err error
	)
	if len(dict) > 0 {
		dec, err = zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	} else {
		dec, err = zstd.NewReader(nil)
	}
	if err != nil {
		return nil, err
	}
	defer dec.Close()

	out, err := dec.DecodeAll(payload, make([]byte, 0, rawLen))
	if err != nil {
		return nil, err
	}
	if uint32(len(out)) != rawLen {
		return nil, ErrCorrupt
	}
	return out, nil
}

func ReadAt(f *os.File, ptr page.ValuePtr, verifyCRC bool) ([]byte, error) {
	return ReadAtWithDict(f, ptr, verifyCRC, nil)
}

func ReadAtWithDict(f *os.File, ptr page.ValuePtr, verifyCRC bool, dictLookup DictLookup) ([]byte, error) {
	if f == nil {
		return nil, errors.New("valuelog: nil file")
	}
	if ptr.Offset < 4 {
		return nil, ErrCorrupt
	}
	start := int64(ptr.Offset - 4)
	recordLen := page.ValuePtrRecordLength(ptr)
	if recordLen != 0 {
		totalLen := int(recordLen) + 4
		if totalLen < HeaderSize {
			return nil, ErrCorrupt
		}
		buf := make([]byte, totalLen)
		if _, err := f.ReadAt(buf, start); err != nil {
			return nil, err
		}
		header := buf[:HeaderSize]
		payload := buf[HeaderSize:]
		return decodeRecord(header, payload, ptr, verifyCRC, dictLookup)
	}

	var header [HeaderSize]byte
	if _, err := f.ReadAt(header[:], start); err != nil {
		return nil, err
	}
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return nil, ErrRecordTooLarge
	}
	payload := make([]byte, int(valueLen))
	if _, err := f.ReadAt(payload, start+HeaderSize); err != nil {
		return nil, err
	}
	return decodeRecord(header[:], payload, ptr, verifyCRC, dictLookup)
}

func decodeRecord(header []byte, payload []byte, ptr page.ValuePtr, verifyCRC bool, dictLookup DictLookup) ([]byte, error) {
	if len(header) < HeaderSize {
		return nil, ErrCorrupt
	}
	crcVal := binary.LittleEndian.Uint32(header[0:4])
	version := header[4]
	if version != Version {
		return nil, ErrCorrupt
	}
	flags := header[5]
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return nil, ErrRecordTooLarge
	}
	if int(valueLen) != len(payload) {
		return nil, ErrCorrupt
	}
	if recordLen := page.ValuePtrRecordLength(ptr); recordLen != 0 {
		expectedLen := uint32(headerWithoutCRC) + valueLen
		if recordLen != expectedLen {
			return nil, ErrCorrupt
		}
	}
	if verifyCRC {
		sum := crc.ChecksumParts(header[4:], payload)
		if sum != crcVal {
			return nil, ErrCorrupt
		}
	}

	if flags&recordFlagGrouped == 0 {
		if page.ValuePtrIsGrouped(ptr) {
			return nil, ErrCorrupt
		}
		return payload, nil
	}
	if !page.ValuePtrIsGrouped(ptr) {
		return nil, ErrCorrupt
	}

	frameHeader, rids, offsets, framePayload, err := DecodeFrame(payload)
	if err != nil {
		return nil, err
	}
	if len(rids) == 0 || len(offsets) < 2 {
		return nil, ErrCorrupt
	}
	subIndex := int(page.ValuePtrSubIndex(ptr))
	if subIndex < 0 || subIndex >= len(rids) {
		return nil, ErrCorrupt
	}
	rawLen := offsets[len(offsets)-1]
	if slab.MaxRecordSize > 0 && int64(rawLen) > slab.MaxRecordSize {
		return nil, ErrRecordTooLarge
	}
	raw, err := decodeFramePayload(frameHeader, framePayload, dictLookup, rawLen)
	if err != nil {
		return nil, err
	}
	start := offsets[subIndex]
	end := offsets[subIndex+1]
	if end < start || end > uint32(len(raw)) {
		return nil, ErrCorrupt
	}
	return raw[start:end], nil
}
