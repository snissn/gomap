package valuelog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"unsafe"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

const (
	maxDecodeScratchKeep      = 256 << 10 // 256KiB (small scratch, bounded pool + per-file stash)
	maxLargeDecodeScratchKeep = 4 << 20   // 4MiB (bounded pool to cap RSS overhead)
	decodeScratchDefaultCap   = 8 << 10   // 8KiB default small scratch cap
)

const (
	smallDecodeScratchPoolEntries = 128
	largeDecodeScratchPoolEntries = 8
)

const discardScratchSize = 32 << 10 // 32KiB

// smallDecodeScratchPool is bounded because Celestia opens many value-log
// segments; an unbounded sync.Pool can retain large final-heap plateaus.
var smallDecodeScratchPool = make(chan []byte, smallDecodeScratchPoolEntries)

// largeDecodeScratchPool is a bounded pool for larger decode scratch buffers.
// We avoid sync.Pool here to keep a hard cap on retained multi-MiB slices.
var largeDecodeScratchPool = make(chan []byte, largeDecodeScratchPoolEntries)

var noDictDecoderPool sync.Pool
var headerScratchPool = sync.Pool{
	New: func() any { return &[HeaderSize]byte{} },
}

func getNoDictDecoder() (*zstd.Decoder, error) {
	if v := noDictDecoderPool.Get(); v != nil {
		if dec, ok := v.(*zstd.Decoder); ok && dec != nil {
			return dec, nil
		}
	}
	return zstd.NewReader(nil)
}

func putNoDictDecoder(dec *zstd.Decoder) {
	if dec == nil {
		return
	}
	// DecodeAll doesn't require a reader, but Reset is the supported way to
	// prepare the decoder for reuse and drop any lingering state.
	_ = dec.Reset(nil)
	noDictDecoderPool.Put(dec)
}

func getHeaderScratch() *[HeaderSize]byte {
	if v := headerScratchPool.Get(); v != nil {
		if h, ok := v.(*[HeaderSize]byte); ok && h != nil {
			return h
		}
	}
	return &[HeaderSize]byte{}
}

func putHeaderScratch(header *[HeaderSize]byte) {
	if header == nil {
		return
	}
	headerScratchPool.Put(header)
}

func getDecodeScratch(minCap int) []byte {
	if minCap <= 0 {
		return nil
	}
	if minCap > maxDecodeScratchKeep {
		// Bounded large-slice pool to reduce alloc churn on large grouped frames.
		// If empty, fall back to allocating; we intentionally don't block.
		if minCap <= maxLargeDecodeScratchKeep {
			decodeScratchLargePoolGetsTotal.Add(1)
			select {
			case buf := <-largeDecodeScratchPool:
				noteDecodeScratchLargePoolTake(cap(buf))
				if cap(buf) < minCap {
					decodeScratchLargePoolTooSmallTotal.Add(1)
					decodeScratchLargePoolMissesTotal.Add(1)
					decodeScratchLargeAllocCallsTotal.Add(1)
					decodeScratchLargeAllocatedBytesTotal.Add(uint64(minCap))
					buf = make([]byte, 0, minCap)
				} else {
					decodeScratchLargePoolHitsTotal.Add(1)
				}
				return buf[:0]
			default:
				decodeScratchLargePoolMissesTotal.Add(1)
				decodeScratchLargeAllocCallsTotal.Add(1)
				decodeScratchLargeAllocatedBytesTotal.Add(uint64(minCap))
				return make([]byte, 0, minCap)
			}
		}
		decodeScratchOversizeAllocCallsTotal.Add(1)
		decodeScratchOversizeAllocatedBytesTotal.Add(uint64(minCap))
		return make([]byte, 0, minCap)
	}
	var buf []byte
	decodeScratchSmallPoolGetsTotal.Add(1)
	select {
	case buf = <-smallDecodeScratchPool:
		noteDecodeScratchSmallPoolTake(cap(buf))
		if cap(buf) < minCap {
			decodeScratchSmallPoolTooSmallTotal.Add(1)
			decodeScratchSmallPoolMissesTotal.Add(1)
			buf = nil
		} else {
			decodeScratchSmallPoolHitsTotal.Add(1)
		}
	default:
		decodeScratchSmallPoolMissesTotal.Add(1)
	}
	if cap(buf) < minCap {
		capHint := minCap
		if capHint < decodeScratchDefaultCap {
			capHint = decodeScratchDefaultCap
		}
		decodeScratchSmallAllocCallsTotal.Add(1)
		decodeScratchSmallAllocatedBytesTotal.Add(uint64(capHint))
		buf = make([]byte, 0, capHint)
	}
	return buf[:0]
}

func putDecodeScratch(buf []byte) {
	if buf == nil {
		return
	}
	c := cap(buf)
	if c == 0 {
		return
	}
	buf = buf[:0]
	if c <= maxDecodeScratchKeep {
		decodeScratchSmallPoolPutsTotal.Add(1)
		noteDecodeScratchSmallPoolPut(c)
		select {
		case smallDecodeScratchPool <- buf:
		default:
			noteDecodeScratchSmallPoolTake(c)
			decodeScratchSmallPoolDropsTotal.Add(1)
			decodeScratchSmallPoolDroppedBytesTotal.Add(uint64(c))
		}
		return
	}
	if c <= maxLargeDecodeScratchKeep {
		decodeScratchLargePoolPutsTotal.Add(1)
		noteDecodeScratchLargePoolPut(c)
		select {
		case largeDecodeScratchPool <- buf:
		default:
			noteDecodeScratchLargePoolTake(c)
			decodeScratchLargePoolDropsTotal.Add(1)
			decodeScratchLargePoolDroppedBytesTotal.Add(uint64(c))
		}
		return
	}
	decodeScratchOversizeDropsTotal.Add(1)
	decodeScratchOversizeDroppedBytesTotal.Add(uint64(c))
}

func sliceDataPtr(b []byte) (uintptr, bool) {
	if cap(b) == 0 {
		return 0, false
	}
	if len(b) > 0 {
		return uintptr(unsafe.Pointer(&b[0])), true
	}
	return uintptr(unsafe.Pointer(&b[:1][0])), true
}

func sliceAliasesBytes(buf, candidate []byte) bool {
	bufStart, ok := sliceDataPtr(buf)
	if !ok {
		return false
	}
	candidateStart, ok := sliceDataPtr(candidate)
	if !ok {
		return false
	}
	bufEnd := bufStart + uintptr(cap(buf))
	return candidateStart >= bufStart && candidateStart < bufEnd
}

type Reader struct {
	f                  *os.File
	closeFn            func() error
	r                  *bufio.Reader
	pos                int64
	fileID             uint32
	verifies           bool
	decodeValues       bool
	validateDicts      bool
	dictLookup         DictLookup
	templateLookup     TemplateLookup
	templateDecodeOpts templ.DecodeOptions
	templateDefCache   *templateDefCache
	pending            []frameEntry
	pendingIndex       int
	headerScratch      [HeaderSize]byte
	frameHeaderScratch [FrameHeaderSize]byte
	ridScratch         [8]byte
	offScratch         [4]byte
	discardScratch     []byte
}

type frameEntry struct {
	rid   uint64
	value []byte
	ptr   page.ValuePtr
}

func NewReader(path string, fileID uint32) (*Reader, error) {
	return NewReaderWithBufferSize(path, fileID, defaultBufferSize)
}

// NewReaderWithBufferSize creates a value-log reader with an explicit bufio
// read buffer size. Callers doing metadata-only scans can use a smaller buffer
// to reduce transient allocation footprint.
func NewReaderWithBufferSize(path string, fileID uint32, bufferSize int) (*Reader, error) {
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{
		f:            f,
		closeFn:      f.Close,
		r:            bufio.NewReaderSize(f, bufferSize),
		fileID:       fileID,
		verifies:     true,
		decodeValues: true,
	}, nil
}

// NewReaderFromFileWithBufferSize creates a metadata/value reader from an
// already-open segment handle without taking ownership of that file. The file
// is read via a section reader so the caller's current offset is not disturbed.
func NewReaderFromFileWithBufferSize(f *os.File, fileID uint32, bufferSize int) *Reader {
	if f == nil {
		return nil
	}
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	sr := io.NewSectionReader(f, 0, 1<<63-1)
	return &Reader{
		f:            f,
		r:            bufio.NewReaderSize(sr, bufferSize),
		fileID:       fileID,
		verifies:     true,
		decodeValues: true,
	}
}

func (r *Reader) DisableChecksum() {
	r.verifies = false
}

func (r *Reader) DisableValueDecode() {
	r.decodeValues = false
}

// ValidateDicts enables dictionary existence checks even when value decoding is
// disabled. This provides a low-cost "fail fast" validation pass during WAL
// replay and open.
func (r *Reader) ValidateDicts() {
	r.validateDicts = true
}

func (r *Reader) SetDictLookup(lookup DictLookup) {
	r.dictLookup = lookup
}

func (r *Reader) SetTemplateLookup(lookup TemplateLookup, opts templ.DecodeOptions) {
	r.templateLookup = lookup
	r.templateDecodeOpts = opts
	r.templateDefCache = newTemplateDefCache(opts.DefCacheSize)
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
		if r.fileID == 0 {
			return rid, payload, ptr, nil
		}
		payload, _, _, err := maybeDecodeLeafLogPayloadTo(r.fileID, r.f.Name(), payload, nil)
		if err != nil {
			return 0, nil, page.ValuePtr{}, err
		}
		return rid, payload, ptr, nil
	}

	frameHeader, rids, offsets, framePayload, err := DecodeFrame(payload)
	if err != nil {
		return 0, nil, page.ValuePtr{}, err
	}
	rawLen := offsets[len(offsets)-1]
	if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
		return 0, nil, page.ValuePtr{}, ErrRecordTooLarge
	}

	var raw []byte
	if r.decodeValues {
		raw, err = decodeFramePayload(frameHeader, framePayload, r.dictLookup, rawLen)
		if err != nil {
			return 0, nil, page.ValuePtr{}, err
		}
	} else if r.validateDicts && frameHeader.Flags&FrameFlagCompressed != 0 && frameHeader.DictID != 0 {
		if r.dictLookup == nil {
			return 0, nil, page.ValuePtr{}, ErrMissingDict
		}
		dict, err := r.dictLookup(frameHeader.DictID)
		if err != nil {
			return 0, nil, page.ValuePtr{}, err
		}
		if len(dict) == 0 {
			return 0, nil, page.ValuePtr{}, ErrMissingDict
		}
	}

	recordLenHint := recordLen
	if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
		recordLenHint = 0
	}
	r.pending = r.pending[:0]
	for i, frameRID := range rids {
		if frameRID == 0 {
			return 0, nil, page.ValuePtr{}, ErrCorrupt
		}
		ptr := page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLenHint, uint8(i)),
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
			if r.templateLookup != nil && templ.IsEncodedPayload(val) {
				decoded, decErr := templ.DecodePayloadAppend(nil, val, func(id uint64) (templ.TemplateDef, error) {
					return resolveTemplateDef(id, r.templateLookup, r.templateDefCache)
				}, r.templateDecodeOpts)
				if decErr != nil {
					return 0, nil, page.ValuePtr{}, decErr
				}
				val = decoded
			}
			val, _, _, err = maybeDecodeLeafLogPayloadTo(r.fileID, r.f.Name(), val, nil)
			if err != nil {
				return 0, nil, page.ValuePtr{}, err
			}
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

// ReadNextMeta streams the next RID + pointer without allocating or returning
// the record payload bytes.
//
// It mirrors ReadNext's RID/pointer semantics, including grouped-record pending
// expansion, but always returns a nil value payload.
func (r *Reader) ReadNextMeta() (uint64, page.ValuePtr, error) {
	if r.pendingIndex < len(r.pending) {
		entry := r.pending[r.pendingIndex]
		r.pendingIndex++
		return entry.rid, entry.ptr, nil
	}

	header := r.headerScratch[:]
	if _, err := io.ReadFull(r.r, header); err != nil {
		return 0, page.ValuePtr{}, err
	}

	crcVal := binary.LittleEndian.Uint32(header[0:4])
	version := header[4]
	if version != Version {
		return 0, page.ValuePtr{}, ErrCorrupt
	}
	flags := header[5]
	rid := binary.LittleEndian.Uint64(header[8:16])
	valueLenU32 := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLenU32) {
		return 0, page.ValuePtr{}, ErrRecordTooLarge
	}
	valueLen := int(valueLenU32)

	// If checksums are enabled we have to read the bytes anyway; compute the CRC
	// incrementally while discarding to avoid allocating the full payload.
	var sum uint32
	if r.verifies {
		sum = crc.Update(0, header[4:])
	}

	start := r.pos
	recordLen := uint32(headerWithoutCRC) + valueLenU32

	if flags&recordFlagGrouped == 0 {
		if r.verifies {
			updated, err := r.discardNUpdateSum(valueLen, sum)
			if err != nil {
				return 0, page.ValuePtr{}, err
			}
			sum = updated
			if sum != crcVal {
				return 0, page.ValuePtr{}, ErrCorrupt
			}
		} else {
			if err := r.discardN(valueLen); err != nil {
				return 0, page.ValuePtr{}, err
			}
		}
		r.pos += int64(HeaderSize + valueLenU32)
		if rid == 0 {
			return 0, page.ValuePtr{}, ErrCorrupt
		}
		ptr := page.ValuePtr{
			Offset: uint64(start + 4),
			Length: recordLen,
			FileID: r.fileID,
		}
		return rid, ptr, nil
	}

	// Grouped record: read/validate prefix (frame header + rids + offsets) and
	// then discard the remaining payload bytes.
	fh := r.frameHeaderScratch[:]
	if _, err := io.ReadFull(r.r, fh); err != nil {
		return 0, page.ValuePtr{}, err
	}
	if r.verifies {
		sum = crc.Update(sum, fh)
	}
	if fh[0] != FrameVersion {
		return 0, page.ValuePtr{}, ErrCorrupt
	}
	k := int(fh[2])
	if k <= 0 || k > MaxFrameK {
		return 0, page.ValuePtr{}, ErrCorrupt
	}
	frameHeader := FrameHeader{
		Version:  fh[0],
		Flags:    fh[1],
		K:        fh[2],
		Reserved: fh[3],
		DictID:   binary.LittleEndian.Uint64(fh[4:12]),
	}
	if r.validateDicts && frameHeader.Flags&FrameFlagCompressed != 0 && frameHeader.DictID != 0 {
		if r.dictLookup == nil {
			return 0, page.ValuePtr{}, ErrMissingDict
		}
		dict, err := r.dictLookup(frameHeader.DictID)
		if err != nil {
			return 0, page.ValuePtr{}, err
		}
		if len(dict) == 0 {
			return 0, page.ValuePtr{}, ErrMissingDict
		}
	}

	var rids [MaxFrameK]uint64
	for i := 0; i < k; i++ {
		ridBytes := r.ridScratch[:]
		if _, err := io.ReadFull(r.r, ridBytes); err != nil {
			return 0, page.ValuePtr{}, err
		}
		if r.verifies {
			sum = crc.Update(sum, ridBytes)
		}
		frameRID := binary.LittleEndian.Uint64(ridBytes)
		if frameRID == 0 {
			return 0, page.ValuePtr{}, ErrCorrupt
		}
		rids[i] = frameRID
	}

	prev := uint32(0)
	rawLen := uint32(0)
	for i := 0; i < k+1; i++ {
		offBytes := r.offScratch[:]
		if _, err := io.ReadFull(r.r, offBytes); err != nil {
			return 0, page.ValuePtr{}, err
		}
		if r.verifies {
			sum = crc.Update(sum, offBytes)
		}
		cur := binary.LittleEndian.Uint32(offBytes)
		if cur < prev {
			return 0, page.ValuePtr{}, ErrCorrupt
		}
		prev = cur
		if i == k {
			rawLen = cur
		}
	}
	if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
		return 0, page.ValuePtr{}, ErrRecordTooLarge
	}

	ridBytesLen := k * 8
	offsetBytesLen := (k + 1) * 4
	prefixLen := FrameHeaderSize + ridBytesLen + offsetBytesLen
	if valueLen < prefixLen {
		return 0, page.ValuePtr{}, ErrCorrupt
	}
	if r.verifies {
		updated, err := r.discardNUpdateSum(valueLen-prefixLen, sum)
		if err != nil {
			return 0, page.ValuePtr{}, err
		}
		sum = updated
		if sum != crcVal {
			return 0, page.ValuePtr{}, ErrCorrupt
		}
	} else {
		if err := r.discardN(valueLen - prefixLen); err != nil {
			return 0, page.ValuePtr{}, err
		}
	}
	r.pos += int64(HeaderSize + valueLenU32)

	recordLenHint := recordLen
	if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
		recordLenHint = 0
	}
	r.pending = r.pending[:0]
	for i := 0; i < k; i++ {
		ptr := page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLenHint, uint8(i)),
			FileID: r.fileID,
		}
		r.pending = append(r.pending, frameEntry{rid: rids[i], value: nil, ptr: ptr})
	}
	if len(r.pending) == 0 {
		return 0, page.ValuePtr{}, ErrCorrupt
	}
	entry := r.pending[0]
	r.pendingIndex = 1
	return entry.rid, entry.ptr, nil
}

// ReadRIDAtUnverified reads only the RID metadata for ptr and does not verify
// the record CRC. It avoids full segment scans when the caller's ValuePtr was
// just produced by this process's value-log write path and only the append RID
// is needed for a command-WAL external-reference fence.
//
// It MUST NOT be used on pointers loaded from durable index pages or any other
// untrusted source; use Reader for integrity-checked reads from durable pointers.
func ReadRIDAtUnverified(f *os.File, fileID uint32, ptr page.ValuePtr) (uint64, error) {
	if f == nil || fileID == 0 || ptr.FileID != fileID || ptr.Offset < 4 {
		return 0, ErrCorrupt
	}
	// ValuePtr.Offset is stored immediately after the CRC prefix, so the first
	// valid value-log record has Offset == 4 and starts at file position 0.
	start := int64(ptr.Offset) - 4
	var header [HeaderSize]byte
	if _, err := f.ReadAt(header[:], start); err != nil {
		return 0, err
	}
	if header[4] != Version {
		return 0, ErrCorrupt
	}
	flags := header[5]
	rid := binary.LittleEndian.Uint64(header[8:16])
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return 0, ErrRecordTooLarge
	}
	recordLen := uint32(headerWithoutCRC) + valueLen
	grouped := flags&recordFlagGrouped != 0
	if !grouped {
		if page.ValuePtrIsGrouped(ptr) || !page.ValuePtrRecordLengthHintMatches(ptr, recordLen) || rid == 0 {
			return 0, ErrCorrupt
		}
		return rid, nil
	}
	if !page.ValuePtrIsGrouped(ptr) || !page.ValuePtrRecordLengthHintMatches(ptr, recordLen) || valueLen < FrameHeaderSize {
		return 0, ErrCorrupt
	}
	var frameHeader [FrameHeaderSize]byte
	if _, err := f.ReadAt(frameHeader[:], start+HeaderSize); err != nil {
		return 0, err
	}
	if frameHeader[0] != FrameVersion {
		return 0, ErrCorrupt
	}
	k := int(frameHeader[2])
	if k <= 0 || k > MaxFrameK {
		return 0, ErrCorrupt
	}
	// k is bounded by MaxFrameK above, keeping this prefix length calculation
	// small and safe on all supported platforms.
	prefixLen := FrameHeaderSize + k*8 + (k+1)*4
	if int(valueLen) < prefixLen {
		return 0, ErrCorrupt
	}
	subIndex := int(page.ValuePtrSubIndex(ptr))
	if subIndex >= k {
		return 0, ErrCorrupt
	}
	var ridBytes [8]byte
	ridRel := FrameHeaderSize + subIndex*8
	if ridRel+8 > int(valueLen) {
		return 0, ErrCorrupt
	}
	ridOffset := start + HeaderSize + int64(ridRel)
	if _, err := f.ReadAt(ridBytes[:], ridOffset); err != nil {
		return 0, err
	}
	rid = binary.LittleEndian.Uint64(ridBytes[:])
	if rid == 0 {
		return 0, ErrCorrupt
	}
	return rid, nil
}

func (r *Reader) discardN(n int) error {
	if n <= 0 {
		return nil
	}
	remain := n
	for remain > 0 {
		discarded, err := r.r.Discard(remain)
		remain -= discarded
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Reader) discardNUpdateSum(n int, sum uint32) (uint32, error) {
	if n <= 0 {
		return sum, nil
	}
	if cap(r.discardScratch) < discardScratchSize {
		r.discardScratch = make([]byte, discardScratchSize)
	}
	buf := r.discardScratch[:discardScratchSize]
	remain := n
	for remain > 0 {
		chunk := buf
		if remain < len(chunk) {
			chunk = buf[:remain]
		}
		if _, err := io.ReadFull(r.r, chunk); err != nil {
			return 0, err
		}
		sum = crc.Update(sum, chunk)
		remain -= len(chunk)
	}
	return sum, nil
}

func (r *Reader) Close() error {
	if r == nil || r.closeFn == nil {
		return nil
	}
	return r.closeFn()
}

func decodeFramePayload(header FrameHeader, payload []byte, dictLookup DictLookup, rawLen uint32) ([]byte, error) {
	if header.Flags&FrameFlagCompressed == 0 {
		if uint32(len(payload)) != rawLen {
			return nil, ErrCorrupt
		}
		return payload, nil
	}
	return decodeFramePayloadTo(header, payload, dictLookup, rawLen, nil)
}

func decodeFramePayloadTo(header FrameHeader, payload []byte, dictLookup DictLookup, rawLen uint32, dst []byte) ([]byte, error) {
	if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
		return nil, ErrRecordTooLarge
	}

	if header.DictID == 0 && header.Flags&FrameFlagCompressed != 0 && header.Reserved != 0 {
		if dst == nil {
			dst = make([]byte, 0, rawLen)
		} else if cap(dst) < int(rawLen) {
			dst = make([]byte, 0, rawLen)
		} else {
			dst = dst[:0]
		}
		return decodeBlockPayload(header.Reserved, payload, rawLen, dst)
	}

	var dec *zstd.Decoder
	var release func()
	if header.DictID != 0 {
		if dictLookup == nil {
			return nil, ErrMissingDict
		}
		// Fast path: reuse pooled decoders if this dictID is already cached.
		codecs := getDictCodecs(header.DictID, nil)
		if codecs == nil || codecs.decPool == nil {
			dict, err := dictLookup(header.DictID)
			if err != nil {
				return nil, err
			}
			if len(dict) == 0 {
				return nil, ErrMissingDict
			}
			codecs = getDictCodecs(header.DictID, dict)
			if codecs == nil || codecs.decPool == nil {
				return nil, ErrMissingDict
			}
		}
		dec = codecs.decPool.Get().(*zstd.Decoder)
		release = func() { codecs.decPool.Put(dec) }
	} else {
		pooled, err := getNoDictDecoder()
		if err != nil {
			return nil, err
		}
		dec = pooled
		release = func() { putNoDictDecoder(dec) }
	}
	defer release()

	if dst == nil {
		dst = make([]byte, 0, rawLen)
	} else {
		if cap(dst) < int(rawLen) {
			dst = make([]byte, 0, rawLen)
		} else {
			dst = dst[:0]
		}
	}
	out, err := dec.DecodeAll(payload, dst)
	if err != nil {
		return nil, err
	}
	if uint32(len(out)) != rawLen {
		return nil, ErrCorrupt
	}
	return out, nil
}

func ReadAt(f *os.File, ptr page.ValuePtr, verifyCRC bool) ([]byte, error) {
	return ReadAtWithDict(f, ptr, verifyCRC, nil, nil, nil, templ.DecodeOptions{})
}

// ReadAtTo decodes ptr from f and, when possible, writes decoded bytes into
// dst. It returns the decoded value, whether dst backed the returned slice, and
// an error.
//
// Callers must treat the returned bytes as immutable. When usedDst is true,
// the returned slice aliases dst.
func ReadAtTo(f *os.File, ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, bool, error) {
	return ReadAtWithDictTo(f, ptr, verifyCRC, nil, nil, nil, templ.DecodeOptions{}, dst)
}

func ReadAtWithDict(f *os.File, ptr page.ValuePtr, verifyCRC bool, dictLookup DictLookup, templateLookup TemplateLookup, templateCache *templateDefCache, templateOpts templ.DecodeOptions) ([]byte, error) {
	if f == nil {
		return nil, errors.New("valuelog: nil file")
	}
	if ptr.Offset < 4 {
		return nil, ErrCorrupt
	}
	start := int64(ptr.Offset - 4)
	var header [HeaderSize]byte
	if _, err := f.ReadAt(header[:], start); err != nil {
		return nil, err
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

	expectedLen := uint32(headerWithoutCRC) + valueLen
	if !page.ValuePtrRecordLengthHintMatches(ptr, expectedLen) {
		return nil, ErrCorrupt
	}

	// Fast path: for grouped, uncompressed frames with checksums disabled, read
	// only the requested sub-record instead of allocating and reading the full
	// frame payload.
	if flags&recordFlagGrouped != 0 && page.ValuePtrIsGrouped(ptr) && !verifyCRC {
		if valueLen < FrameHeaderSize {
			return nil, ErrCorrupt
		}
		frameOff := start + HeaderSize

		var frameHeader [FrameHeaderSize]byte
		if _, err := f.ReadAt(frameHeader[:], frameOff); err != nil {
			return nil, err
		}
		if frameHeader[0] != FrameVersion {
			return nil, ErrCorrupt
		}
		k := int(frameHeader[2])
		if k <= 0 || k > MaxFrameK {
			return nil, ErrCorrupt
		}
		fFlags := frameHeader[1]
		if fFlags&FrameFlagCompressed == 0 {
			ridBytes := k * 8
			offsetBytes := (k + 1) * 4
			prefixLen := FrameHeaderSize + ridBytes + offsetBytes
			if int(valueLen) < prefixLen {
				return nil, ErrCorrupt
			}

			const maxRIDOffsetsLen = (MaxFrameK * 8) + ((MaxFrameK + 1) * 4)
			var ridOffsets [maxRIDOffsetsLen]byte
			ridOffsetsLen := ridBytes + offsetBytes
			if _, err := f.ReadAt(ridOffsets[:ridOffsetsLen], frameOff+FrameHeaderSize); err != nil {
				return nil, err
			}

			subIndex := int(page.ValuePtrSubIndex(ptr))
			if subIndex < 0 || subIndex >= k {
				return nil, ErrCorrupt
			}

			// Validate RIDs and parse offsets.
			ridOff := 0
			for i := 0; i < k; i++ {
				rid := binary.LittleEndian.Uint64(ridOffsets[ridOff : ridOff+8])
				if rid == 0 {
					return nil, ErrCorrupt
				}
				ridOff += 8
			}

			off := ridBytes
			var offsets [MaxFrameK + 1]uint32
			prev := uint32(0)
			for i := 0; i < k+1; i++ {
				cur := binary.LittleEndian.Uint32(ridOffsets[off : off+4])
				if cur < prev {
					return nil, ErrCorrupt
				}
				offsets[i] = cur
				prev = cur
				off += 4
			}

			rawLen := offsets[k]
			if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
				return nil, ErrRecordTooLarge
			}
			if uint64(prefixLen)+uint64(rawLen) != uint64(valueLen) {
				return nil, ErrCorrupt
			}

			valStart := offsets[subIndex]
			valEnd := offsets[subIndex+1]
			if valEnd < valStart || valEnd > rawLen {
				return nil, ErrCorrupt
			}

			val := make([]byte, int(valEnd-valStart))
			readOff := frameOff + int64(prefixLen) + int64(valStart)
			if _, err := f.ReadAt(val, readOff); err != nil {
				return nil, err
			}
			if templateLookup != nil && templ.IsEncodedPayload(val) {
				decoded, err := templ.DecodePayloadAppend(nil, val, func(id uint64) (templ.TemplateDef, error) {
					return resolveTemplateDef(id, templateLookup, templateCache)
				}, templateOpts)
				if err != nil {
					return nil, err
				}
				decoded, _, _, err = maybeDecodeLeafLogPayloadTo(ptr.FileID, f.Name(), decoded, nil)
				if err != nil {
					return nil, err
				}
				return decoded, nil
			}
			val, _, _, err := maybeDecodeLeafLogPayloadTo(ptr.FileID, f.Name(), val, nil)
			if err != nil {
				return nil, err
			}
			return val, nil
		}
	}

	payload := make([]byte, int(valueLen))
	if _, err := f.ReadAt(payload, start+HeaderSize); err != nil {
		return nil, err
	}
	if verifyCRC {
		sum := crc.ChecksumParts(header[4:], payload)
		if sum != crcVal {
			return nil, ErrCorrupt
		}
	}
	val, err := decodeRecord(header[:], payload, ptr, false, dictLookup, templateLookup, templateCache, templateOpts)
	if err != nil {
		return nil, err
	}
	val, _, _, err = maybeDecodeLeafLogPayloadTo(ptr.FileID, f.Name(), val, nil)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// ReadAtWithDictTo is ReadAtWithDict with caller-provided decode storage.
//
// The returned slice may alias dst when usedDst is true. The returned bytes are
// immutable from the caller perspective.
func ReadAtWithDictTo(f *os.File, ptr page.ValuePtr, verifyCRC bool, dictLookup DictLookup, templateLookup TemplateLookup, templateCache *templateDefCache, templateOpts templ.DecodeOptions, dst []byte) ([]byte, bool, error) {
	return readAtWithDictToScratch(f, ptr, verifyCRC, dictLookup, templateLookup, templateCache, templateOpts, dst, getDecodeScratch, putDecodeScratch)
}

func readAtWithDictToScratch(f *os.File, ptr page.ValuePtr, verifyCRC bool, dictLookup DictLookup, templateLookup TemplateLookup, templateCache *templateDefCache, templateOpts templ.DecodeOptions, dst []byte, getScratch func(int) []byte, putScratch func([]byte)) ([]byte, bool, error) {
	if f == nil {
		return nil, false, errors.New("valuelog: nil file")
	}
	if getScratch == nil {
		getScratch = getDecodeScratch
	}
	if putScratch == nil {
		putScratch = putDecodeScratch
	}
	if ptr.Offset < 4 {
		return nil, false, ErrCorrupt
	}
	start := int64(ptr.Offset - 4)
	header := getHeaderScratch()
	defer putHeaderScratch(header)
	if _, err := f.ReadAt(header[:], start); err != nil {
		return nil, false, err
	}

	crcVal := binary.LittleEndian.Uint32(header[0:4])
	version := header[4]
	if version != Version {
		return nil, false, ErrCorrupt
	}
	flags := header[5]
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return nil, false, ErrRecordTooLarge
	}

	expectedLen := uint32(headerWithoutCRC) + valueLen
	if !page.ValuePtrRecordLengthHintMatches(ptr, expectedLen) {
		return nil, false, ErrCorrupt
	}

	// Fast path: for grouped, uncompressed frames with checksums disabled, read
	// only the requested sub-record instead of allocating and reading the full
	// frame payload.
	if flags&recordFlagGrouped != 0 && page.ValuePtrIsGrouped(ptr) && !verifyCRC {
		if valueLen < FrameHeaderSize {
			return nil, false, ErrCorrupt
		}
		frameOff := start + HeaderSize

		var frameHeader [FrameHeaderSize]byte
		if _, err := f.ReadAt(frameHeader[:], frameOff); err != nil {
			return nil, false, err
		}
		if frameHeader[0] != FrameVersion {
			return nil, false, ErrCorrupt
		}
		k := int(frameHeader[2])
		if k <= 0 || k > MaxFrameK {
			return nil, false, ErrCorrupt
		}
		fFlags := frameHeader[1]
		if fFlags&FrameFlagCompressed == 0 {
			ridBytes := k * 8
			offsetBytes := (k + 1) * 4
			prefixLen := FrameHeaderSize + ridBytes + offsetBytes
			if int(valueLen) < prefixLen {
				return nil, false, ErrCorrupt
			}

			const maxRIDOffsetsLen = (MaxFrameK * 8) + ((MaxFrameK + 1) * 4)
			var ridOffsets [maxRIDOffsetsLen]byte
			ridOffsetsLen := ridBytes + offsetBytes
			if _, err := f.ReadAt(ridOffsets[:ridOffsetsLen], frameOff+FrameHeaderSize); err != nil {
				return nil, false, err
			}

			subIndex := int(page.ValuePtrSubIndex(ptr))
			if subIndex < 0 || subIndex >= k {
				return nil, false, ErrCorrupt
			}

			// Validate RIDs and parse offsets.
			ridOff := 0
			for i := 0; i < k; i++ {
				rid := binary.LittleEndian.Uint64(ridOffsets[ridOff : ridOff+8])
				if rid == 0 {
					return nil, false, ErrCorrupt
				}
				ridOff += 8
			}

			off := ridBytes
			var offsets [MaxFrameK + 1]uint32
			prev := uint32(0)
			for i := 0; i < k+1; i++ {
				cur := binary.LittleEndian.Uint32(ridOffsets[off : off+4])
				if cur < prev {
					return nil, false, ErrCorrupt
				}
				offsets[i] = cur
				prev = cur
				off += 4
			}

			rawLen := offsets[k]
			if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
				return nil, false, ErrRecordTooLarge
			}
			if uint64(prefixLen)+uint64(rawLen) != uint64(valueLen) {
				return nil, false, ErrCorrupt
			}

			valStart := offsets[subIndex]
			valEnd := offsets[subIndex+1]
			if valEnd < valStart || valEnd > rawLen {
				return nil, false, ErrCorrupt
			}

			outLen := int(valEnd - valStart)
			var val []byte
			usedDst := false
			if dst != nil && cap(dst) >= outLen {
				val = dst[:outLen]
				usedDst = true
			} else {
				val = make([]byte, outLen)
			}
			readOff := frameOff + int64(prefixLen) + int64(valStart)
			if _, err := f.ReadAt(val, readOff); err != nil {
				return nil, false, err
			}
			if templateLookup != nil && templ.IsEncodedPayload(val) {
				decoded, err := templ.DecodePayloadAppend(nil, val, func(id uint64) (templ.TemplateDef, error) {
					return resolveTemplateDef(id, templateLookup, templateCache)
				}, templateOpts)
				if err != nil {
					return nil, false, err
				}
				decoded, _, _, err = maybeDecodeLeafLogPayloadTo(ptr.FileID, f.Name(), decoded, nil)
				if err != nil {
					return nil, false, err
				}
				return decoded, false, nil
			}
			val, compactUsedDst, compactDecoded, err := maybeDecodeLeafLogPayloadTo(ptr.FileID, f.Name(), val, dst)
			if err != nil {
				return nil, false, err
			}
			if compactDecoded {
				return val, compactUsedDst, nil
			}
			return val, usedDst, nil
		}
	}

	// Non-grouped record: read payload directly into dst when possible to avoid
	// allocating an intermediate buffer on the fallback path.
	if flags&recordFlagGrouped == 0 && !page.ValuePtrIsGrouped(ptr) {
		var payload []byte
		usedDst := false
		if dst != nil && cap(dst) >= int(valueLen) {
			payload = dst[:int(valueLen)]
			usedDst = true
		} else {
			payload = make([]byte, int(valueLen))
		}
		if _, err := f.ReadAt(payload, start+HeaderSize); err != nil {
			return nil, false, err
		}
		if verifyCRC {
			sum := crc.ChecksumParts(header[4:], payload)
			if sum != crcVal {
				return nil, false, ErrCorrupt
			}
		}
		if templateLookup != nil && templ.IsEncodedPayload(payload) {
			decoded, err := templ.DecodePayloadAppend(nil, payload, func(id uint64) (templ.TemplateDef, error) {
				return resolveTemplateDef(id, templateLookup, templateCache)
			}, templateOpts)
			if err != nil {
				return nil, false, err
			}
			decoded, _, _, err = maybeDecodeLeafLogPayloadTo(ptr.FileID, f.Name(), decoded, nil)
			if err != nil {
				return nil, false, err
			}
			return decoded, false, nil
		}
		payload, compactUsedDst, compactDecoded, err := maybeDecodeLeafLogPayloadTo(ptr.FileID, f.Name(), payload, dst)
		if err != nil {
			return nil, false, err
		}
		if compactDecoded {
			return payload, compactUsedDst, nil
		}
		return payload, usedDst, nil
	}

	payloadScratch := getScratch(int(valueLen))
	payload := payloadScratch[:int(valueLen)]
	if _, err := f.ReadAt(payload, start+HeaderSize); err != nil {
		putScratch(payloadScratch)
		return nil, false, err
	}
	if verifyCRC {
		sum := crc.ChecksumParts(header[4:], payload)
		if sum != crcVal {
			putScratch(payloadScratch)
			return nil, false, ErrCorrupt
		}
	}
	val, usedDst, err := decodeRecordToScratch(header, payload, ptr, false, dictLookup, templateLookup, templateCache, templateOpts, dst, getScratch, putScratch)
	if err != nil {
		putScratch(payloadScratch)
		return nil, false, err
	}
	val, compactUsedDst, compactDecoded, err := maybeDecodeLeafLogPayloadTo(ptr.FileID, f.Name(), val, dst)
	if err != nil {
		putScratch(payloadScratch)
		return nil, false, err
	}
	// Safe to return payload scratch to the pool whenever the returned slice
	// does not alias the payload buffer backed by payloadScratch.
	finalUsedDst := usedDst
	if compactDecoded {
		finalUsedDst = compactUsedDst
	}
	if finalUsedDst || !sliceAliasesBytes(payload, val) {
		putScratch(payloadScratch)
	}
	return val, finalUsedDst, nil
}

func decodeRecord(header []byte, payload []byte, ptr page.ValuePtr, verifyCRC bool, dictLookup DictLookup, templateLookup TemplateLookup, templateCache *templateDefCache, templateOpts templ.DecodeOptions) ([]byte, error) {
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
	expectedLen := uint32(headerWithoutCRC) + valueLen
	if !page.ValuePtrRecordLengthHintMatches(ptr, expectedLen) {
		return nil, ErrCorrupt
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
		if templateLookup != nil && templ.IsEncodedPayload(payload) {
			decoded, err := templ.DecodePayloadAppend(nil, payload, func(id uint64) (templ.TemplateDef, error) {
				return resolveTemplateDef(id, templateLookup, templateCache)
			}, templateOpts)
			if err != nil {
				return nil, err
			}
			return decoded, nil
		}
		return payload, nil
	}
	if !page.ValuePtrIsGrouped(ptr) {
		return nil, ErrCorrupt
	}

	subIndex := int(page.ValuePtrSubIndex(ptr))
	frameHeader, start, end, rawLen, framePayload, err := decodeFrameValueBounds(payload, subIndex)
	if err != nil {
		return nil, err
	}
	if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
		return nil, ErrRecordTooLarge
	}
	if frameHeader.Flags&FrameFlagCompressed != 0 {
		// Random-access decode wants only a single value. Decode into a pooled
		// buffer and then copy just the requested range so we don't retain the
		// full frame allocation.
		if end < start || end > rawLen {
			return nil, ErrCorrupt
		}
		scratch := getDecodeScratch(int(rawLen))
		raw, err := decodeFramePayloadTo(frameHeader, framePayload, dictLookup, rawLen, scratch)
		if err != nil {
			putDecodeScratch(scratch)
			return nil, err
		}
		if uint32(len(raw)) != rawLen {
			putDecodeScratch(raw)
			return nil, ErrCorrupt
		}
		val := make([]byte, int(end-start))
		copy(val, raw[start:end])
		putDecodeScratch(raw)
		if templateLookup != nil && templ.IsEncodedPayload(val) {
			decoded, err := templ.DecodePayloadAppend(nil, val, func(id uint64) (templ.TemplateDef, error) {
				return resolveTemplateDef(id, templateLookup, templateCache)
			}, templateOpts)
			if err != nil {
				return nil, err
			}
			return decoded, nil
		}
		return val, nil
	}
	raw, err := decodeFramePayload(frameHeader, framePayload, dictLookup, rawLen)
	if err != nil {
		return nil, err
	}
	if end < start || end > uint32(len(raw)) {
		return nil, ErrCorrupt
	}
	val := raw[start:end]
	if templateLookup != nil && templ.IsEncodedPayload(val) {
		decoded, err := templ.DecodePayloadAppend(nil, val, func(id uint64) (templ.TemplateDef, error) {
			return resolveTemplateDef(id, templateLookup, templateCache)
		}, templateOpts)
		if err != nil {
			return nil, err
		}
		val = decoded
	}
	return val, nil
}

func decodeRecordTo(header *[HeaderSize]byte, payload []byte, ptr page.ValuePtr, verifyCRC bool, dictLookup DictLookup, templateLookup TemplateLookup, templateCache *templateDefCache, templateOpts templ.DecodeOptions, dst []byte) ([]byte, bool, error) {
	return decodeRecordToScratch(header, payload, ptr, verifyCRC, dictLookup, templateLookup, templateCache, templateOpts, dst, getDecodeScratch, putDecodeScratch)
}

func decodeRecordToScratch(header *[HeaderSize]byte, payload []byte, ptr page.ValuePtr, verifyCRC bool, dictLookup DictLookup, templateLookup TemplateLookup, templateCache *templateDefCache, templateOpts templ.DecodeOptions, dst []byte, getScratch func(int) []byte, putScratch func([]byte)) ([]byte, bool, error) {
	if header == nil {
		return nil, false, ErrCorrupt
	}
	if getScratch == nil {
		getScratch = getDecodeScratch
	}
	if putScratch == nil {
		putScratch = putDecodeScratch
	}
	crcVal := binary.LittleEndian.Uint32(header[0:4])
	version := header[4]
	if version != Version {
		return nil, false, ErrCorrupt
	}
	flags := header[5]
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return nil, false, ErrRecordTooLarge
	}
	if int(valueLen) != len(payload) {
		return nil, false, ErrCorrupt
	}
	expectedLen := uint32(headerWithoutCRC) + valueLen
	if !page.ValuePtrRecordLengthHintMatches(ptr, expectedLen) {
		return nil, false, ErrCorrupt
	}
	if verifyCRC {
		sum := crc.ChecksumParts(header[4:], payload)
		if sum != crcVal {
			return nil, false, ErrCorrupt
		}
	}

	if flags&recordFlagGrouped == 0 {
		if page.ValuePtrIsGrouped(ptr) {
			return nil, false, ErrCorrupt
		}
		if templateLookup != nil && templ.IsEncodedPayload(payload) {
			decoded, err := templ.DecodePayloadAppend(nil, payload, func(id uint64) (templ.TemplateDef, error) {
				return resolveTemplateDef(id, templateLookup, templateCache)
			}, templateOpts)
			if err != nil {
				return nil, false, err
			}
			return decoded, false, nil
		}
		return payload, false, nil
	}
	if !page.ValuePtrIsGrouped(ptr) {
		return nil, false, ErrCorrupt
	}

	subIndex := int(page.ValuePtrSubIndex(ptr))
	frameHeader, start, end, rawLen, framePayload, err := decodeFrameValueBounds(payload, subIndex)
	if err != nil {
		return nil, false, err
	}
	if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
		return nil, false, ErrRecordTooLarge
	}
	if end < start || end > rawLen {
		return nil, false, ErrCorrupt
	}

	outLen := int(end - start)
	if outLen < 0 {
		return nil, false, ErrCorrupt
	}

	if frameHeader.Flags&FrameFlagCompressed != 0 {
		// Random-access decode wants only a single value. When dst is large
		// enough for the sub-range, decode into a pooled scratch buffer and copy
		// into dst to avoid allocating a fresh slice on every read.
		if dst != nil && cap(dst) >= outLen {
			// If dst can hold the whole decoded frame and we want the entire
			// payload, decode directly into it and avoid the extra copy.
			if cap(dst) >= int(rawLen) && start == 0 && end == rawLen {
				raw, err := decodeFramePayloadTo(frameHeader, framePayload, dictLookup, rawLen, dst)
				if err != nil {
					return nil, false, err
				}
				if uint32(len(raw)) != rawLen {
					return nil, false, ErrCorrupt
				}
				val := raw
				if templateLookup != nil && templ.IsEncodedPayload(val) {
					decoded, err := templ.DecodePayloadAppend(nil, val, func(id uint64) (templ.TemplateDef, error) {
						return resolveTemplateDef(id, templateLookup, templateCache)
					}, templateOpts)
					if err != nil {
						return nil, false, err
					}
					return decoded, false, nil
				}
				return val, true, nil
			}

			scratch := getScratch(int(rawLen))
			raw, err := decodeFramePayloadTo(frameHeader, framePayload, dictLookup, rawLen, scratch)
			if err != nil {
				putScratch(scratch)
				return nil, false, err
			}
			if uint32(len(raw)) != rawLen {
				putScratch(raw)
				return nil, false, ErrCorrupt
			}
			val := dst[:outLen]
			copy(val, raw[start:end])
			putScratch(raw)
			if templateLookup != nil && templ.IsEncodedPayload(val) {
				decoded, err := templ.DecodePayloadAppend(nil, val, func(id uint64) (templ.TemplateDef, error) {
					return resolveTemplateDef(id, templateLookup, templateCache)
				}, templateOpts)
				if err != nil {
					return nil, false, err
				}
				return decoded, false, nil
			}
			return val, true, nil
		}

		// Fallback: keep existing behavior (decode into pooled scratch, then copy
		// into a fresh allocation so we don't retain the full frame).
		scratch := getScratch(int(rawLen))
		raw, err := decodeFramePayloadTo(frameHeader, framePayload, dictLookup, rawLen, scratch)
		if err != nil {
			putScratch(scratch)
			return nil, false, err
		}
		if uint32(len(raw)) != rawLen {
			putScratch(raw)
			return nil, false, ErrCorrupt
		}
		val := make([]byte, outLen)
		copy(val, raw[start:end])
		putScratch(raw)
		if templateLookup != nil && templ.IsEncodedPayload(val) {
			decoded, err := templ.DecodePayloadAppend(nil, val, func(id uint64) (templ.TemplateDef, error) {
				return resolveTemplateDef(id, templateLookup, templateCache)
			}, templateOpts)
			if err != nil {
				return nil, false, err
			}
			return decoded, false, nil
		}
		return val, false, nil
	}

	raw, err := decodeFramePayload(frameHeader, framePayload, dictLookup, rawLen)
	if err != nil {
		return nil, false, err
	}
	if end > uint32(len(raw)) {
		return nil, false, ErrCorrupt
	}
	val := raw[start:end]
	usedDst := false
	if templateLookup != nil && templ.IsEncodedPayload(val) {
		decoded, err := templ.DecodePayloadAppend(nil, val, func(id uint64) (templ.TemplateDef, error) {
			return resolveTemplateDef(id, templateLookup, templateCache)
		}, templateOpts)
		if err != nil {
			return nil, false, err
		}
		val = decoded
	} else if dst != nil && cap(dst) >= outLen {
		// Uncompressed path: copy into dst to allow callers to avoid retaining a
		// large frame payload allocation.
		out := dst[:outLen]
		copy(out, val)
		val = out
		usedDst = true
	}
	return val, usedDst, nil
}
