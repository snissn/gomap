package valuelog

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"sync"
	"unsafe"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

var (
	ErrCOWReadCapacity    = errors.New("valuelog: COW read capacity unavailable")
	ErrCOWReadUnsupported = errors.New("valuelog: unsupported COW record shape")
	ErrCOWDecoderClosed   = errors.New("valuelog: COW decoder closed")
)

// COWReadLimits must contain positive, finite caller-owned limits. These reads
// never consult the mutable process-wide record-size limit.
type COWReadLimits struct {
	MaxRecordBytes, MaxRawBytes, MaxValueBytes uint64
}

// COWRecordShape binds admitted storage to one pointer and its exact record
// metadata. RecordBytes includes the CRC and record header. DictID is the
// encoded header ID; it is a decoder dependency only when Compressed is true.
// Shapes are obtained through InspectCOWRecord and must not be modified.
type COWRecordShape struct {
	RecordBytes, RawBytes, ValueBytes uint64
	DictID                            uint64
	Compressed                        bool
	Codec                             BlockCodec
	ptr                               page.ValuePtr
	limits                            COWReadLimits
	frame                             FrameHeader
	metadata                          [32]byte
	prefix, valueStart                uint32
	valid                             bool
	leaf                              bool
}

// PayloadBytes is the exact caller-owned input scratch capacity needed by Read,
// including the CRC and record header as well as the payload body.
func (s COWRecordShape) PayloadBytes() uint64 {
	if !s.valid || s.RecordBytes < HeaderSize {
		return 0
	}
	return s.RecordBytes
}

// COWDecodeFunc must use dst as its only output backing, return exactly len(dst)
// bytes, and resolve dependencies from its retained owner, without global codec
// caches. Read passes dst with length and capacity equal to checked RawBytes.
type COWDecodeFunc func(frame FrameHeader, payload, dst []byte) ([]byte, error)

const cowMaxPrefix = FrameHeaderSize + MaxFrameK*8 + (MaxFrameK+1)*4

func cowReadHeader(f *os.File, ptr page.ValuePtr, header []byte, limits COWReadLimits, leaf bool) (uint32, error) {
	if f == nil || ptr.Offset < 4 || ptr.Offset-4 > math.MaxInt64 || isLeafLogFileID(ptr.FileID) != leaf || (leaf && !allowsCompactLeafLogPayload(ptr.FileID, f.Name())) {
		return 0, ErrCOWReadUnsupported
	}
	maxInt := uint64(^uint(0) >> 1)
	if limits.MaxRecordBytes == 0 || limits.MaxRawBytes == 0 || limits.MaxValueBytes == 0 ||
		limits.MaxRecordBytes > maxInt || limits.MaxRawBytes > maxInt || limits.MaxValueBytes > maxInt {
		return 0, ErrCOWReadCapacity
	}
	if _, err := f.ReadAt(header, int64(ptr.Offset-4)); err != nil {
		return 0, err
	}
	if header[4] != Version || header[5]&^recordFlagGrouped != 0 || header[6] != 0 || header[7] != 0 {
		return 0, ErrCorrupt
	}
	n := binary.LittleEndian.Uint32(header[16:20])
	if uint64(n)+HeaderSize > limits.MaxRecordBytes || uint64(n)+headerWithoutCRC > math.MaxUint32 {
		return 0, ErrCOWReadCapacity
	}
	if ptr.Offset-4 > math.MaxInt64-uint64(HeaderSize)-uint64(n) ||
		!page.ValuePtrRecordLengthHintMatches(ptr, uint32(headerWithoutCRC)+n) {
		return 0, ErrCorrupt
	}
	return n, nil
}

func cowShape(header, prefix []byte, n uint32, ptr page.ValuePtr, limits COWReadLimits) (COWRecordShape, error) {
	s := COWRecordShape{RecordBytes: uint64(n) + HeaderSize, ptr: ptr, limits: limits, valid: true}
	grouped := header[5]&recordFlagGrouped != 0
	if grouped != page.ValuePtrIsGrouped(ptr) {
		return COWRecordShape{}, ErrCorrupt
	}
	if !grouped {
		if binary.LittleEndian.Uint64(header[8:16]) == 0 {
			return COWRecordShape{}, ErrCorrupt
		}
		s.RawBytes, s.ValueBytes = uint64(n), uint64(n)
	} else {
		if binary.LittleEndian.Uint64(header[8:16]) != 0 || len(prefix) < FrameHeaderSize || prefix[0] != FrameVersion ||
			prefix[1]&^FrameFlagCompressed != 0 || prefix[2] == 0 {
			return COWRecordShape{}, ErrCorrupt
		}
		k := int(prefix[2])
		p := FrameHeaderSize + k*8 + (k+1)*4
		if uint64(p) > uint64(n) || len(prefix) != p || int(page.ValuePtrSubIndex(ptr)) >= k {
			return COWRecordShape{}, ErrCorrupt
		}
		s.frame = FrameHeader{Version: prefix[0], Flags: prefix[1], K: prefix[2], Reserved: prefix[3], DictID: binary.LittleEndian.Uint64(prefix[4:12])}
		s.DictID, s.Compressed = s.frame.DictID, s.frame.Flags&FrameFlagCompressed != 0
		if s.frame.Reserved != 0 {
			if !s.Compressed || s.DictID != 0 || BlockCodec(s.frame.Reserved) > BlockCodecZSTD {
				return COWRecordShape{}, ErrCOWReadUnsupported
			}
			s.Codec = BlockCodec(s.frame.Reserved)
		}
		for i := 0; i < k; i++ {
			if binary.LittleEndian.Uint64(prefix[FrameHeaderSize+i*8:]) == 0 {
				return COWRecordShape{}, ErrCorrupt
			}
		}
		off := FrameHeaderSize + k*8
		var prev, start, end uint32
		for i := 0; i <= k; i++ {
			cur := binary.LittleEndian.Uint32(prefix[off+i*4:])
			if cur < prev || (i == 0 && cur != 0) {
				return COWRecordShape{}, ErrCorrupt
			}
			if i == int(page.ValuePtrSubIndex(ptr)) {
				start = cur
			}
			if i == int(page.ValuePtrSubIndex(ptr))+1 {
				end = cur
			}
			prev = cur
		}
		s.RawBytes, s.ValueBytes = uint64(prev), uint64(end-start)
		s.prefix, s.valueStart = uint32(p), start
		if (!s.Compressed && uint64(p)+s.RawBytes != uint64(n)) || (s.Compressed && uint32(p) == n) {
			return COWRecordShape{}, ErrCorrupt
		}
	}
	if s.RawBytes > limits.MaxRawBytes || s.ValueBytes > limits.MaxValueBytes {
		return COWRecordShape{}, ErrCOWReadCapacity
	}
	var metadata [HeaderSize + cowMaxPrefix]byte
	copy(metadata[:HeaderSize], header)
	copy(metadata[HeaderSize:], prefix)
	s.metadata = sha256.Sum256(metadata[:HeaderSize+len(prefix)])
	return s, nil
}

// InspectCOWRecord uses fixed-size metadata. Before inspection the caller must
// admit the separate buffers reported by COWInspectionMetadataAllocationSizes;
// Windows and race instrumentation make those buffers escape through ReadAt.
// The caller must already own the open registered file and producer visibility;
// no refresh, sync or cache is performed. Raw template/compact-leaf payloads are
// refused here. Compressed payloads can only be checked for those encodings
// after admitted decoding.
func InspectCOWRecord(f *os.File, ptr page.ValuePtr, limits COWReadLimits) (COWRecordShape, error) {
	return inspectCOWRecord(f, ptr, limits, false)
}

// InspectCOWLeafRecord is the explicit bounded external-leaf sibling. The
// caller must own the registered leaf file with its canonical native path and
// the inspection metadata admission. Its existing namespace authority is
// checked here. Leaves are at most one page, grouped raw bytes at most 255
// pages, and stored records at most 2MiB, in addition to the caller's limits.
func InspectCOWLeafRecord(f *os.File, ptr page.ValuePtr, limits COWReadLimits) (COWRecordShape, error) {
	limits.MaxRecordBytes = min(limits.MaxRecordBytes, 2<<20)
	limits.MaxRawBytes = min(limits.MaxRawBytes, MaxFrameK*page.PageSize)
	limits.MaxValueBytes = min(limits.MaxValueBytes, page.PageSize)
	return inspectCOWRecord(f, ptr, limits, true)
}

func inspectCOWRecord(f *os.File, ptr page.ValuePtr, limits COWReadLimits, leaf bool) (COWRecordShape, error) {
	var header [HeaderSize]byte
	n, err := cowReadHeader(f, ptr, header[:], limits, leaf)
	if err != nil {
		return COWRecordShape{}, err
	}
	var prefix [cowMaxPrefix]byte
	p := 0
	if header[5]&recordFlagGrouped != 0 {
		if n < FrameHeaderSize {
			return COWRecordShape{}, ErrCorrupt
		}
		if _, err := f.ReadAt(prefix[:FrameHeaderSize], int64(ptr.Offset-4)+HeaderSize); err != nil {
			return COWRecordShape{}, err
		}
		k := int(prefix[2])
		if k == 0 {
			return COWRecordShape{}, ErrCorrupt
		}
		p = FrameHeaderSize + k*8 + (k+1)*4
		if uint64(p) > uint64(n) {
			return COWRecordShape{}, ErrCorrupt
		}
		if _, err := f.ReadAt(prefix[FrameHeaderSize:p], int64(ptr.Offset-4)+HeaderSize+FrameHeaderSize); err != nil {
			return COWRecordShape{}, err
		}
	}
	s, err := cowShape(header[:], prefix[:p], n, ptr, limits)
	if err != nil {
		return COWRecordShape{}, err
	}
	if !s.Compressed {
		var valuePrefix [compactLeafPagePayloadHeaderSize]byte
		v := min(uint64(len(valuePrefix)), s.ValueBytes)
		if _, err := f.ReadAt(valuePrefix[:int(v)], int64(ptr.Offset-4)+HeaderSize+int64(s.prefix)+int64(s.valueStart)); err != nil {
			return COWRecordShape{}, err
		}
		if templ.IsEncodedPayload(valuePrefix[:int(v)]) || (!leaf && HasCompactLeafLogPayload(valuePrefix[:int(v)])) {
			return COWRecordShape{}, ErrCOWReadUnsupported
		}
		if leaf {
			if HasCompactLeafLogPayload(valuePrefix[:int(v)]) {
				prefixLen := uint64(binary.LittleEndian.Uint16(valuePrefix[len(compactLeafPagePayloadMagic):]))
				suffixLen := uint64(binary.LittleEndian.Uint16(valuePrefix[len(compactLeafPagePayloadMagic)+2:]))
				compactLen := uint64(compactLeafPagePayloadHeaderSize) + prefixLen + suffixLen
				if prefixLen < page.PageHeaderSize || prefixLen+suffixLen > page.PageSize || compactLen > page.PageSize || compactLen != s.ValueBytes {
					return COWRecordShape{}, ErrCorrupt
				}
			} else if s.ValueBytes != page.PageSize {
				return COWRecordShape{}, ErrCorrupt
			}
		}
	}
	s.leaf = leaf
	return s, nil
}

// ReadCOWRecord allocates no backing storage. Scratch buffers must be disjoint,
// exclusively owned during the call, and already admitted. Returned bytes
// alias outputScratch; both payloadScratch and rawScratch may remain referenced
// by the decoder owner and must stay admitted through its final reference.
// The file must stay immutable, physically pinned and producer-visible. A
// shape is revalidated against the full input before any decoder is called.
func ReadCOWRecord(f *os.File, ptr page.ValuePtr, shape COWRecordShape, verifyCRC bool, payloadScratch, rawScratch, outputScratch []byte, decode COWDecodeFunc) ([]byte, error) {
	return readCOWRecord(f, ptr, shape, verifyCRC, payloadScratch, rawScratch, outputScratch, decode, false)
}

// ReadCOWLeafPage expands a compact leaf into the pre-admitted pageDst. Its
// capacity must be at least page.PageSize before any read/decode. Returned
// bytes are exactly one page and alias pageDst. No materialization fallback or
// global codec cache is used. The retained file has the canonical native path
// required by InspectCOWLeafRecord.
func ReadCOWLeafPage(f *os.File, ptr page.ValuePtr, shape COWRecordShape, verifyCRC bool, payloadScratch, rawScratch, pageDst []byte, decode COWDecodeFunc) ([]byte, error) {
	if cap(pageDst) < page.PageSize || shape.ValueBytes > page.PageSize || shape.RawBytes > MaxFrameK*page.PageSize || shape.RecordBytes > 2<<20 {
		return nil, ErrCOWReadCapacity
	}
	value, err := readCOWRecord(f, ptr, shape, verifyCRC, payloadScratch, rawScratch, pageDst, decode, true)
	if err != nil {
		return nil, err
	}
	out, _, _, err := decodeCompactLeafLogPayloadTo(value, pageDst[:page.PageSize:page.PageSize])
	if err != nil {
		return nil, err
	}
	if len(out) != page.PageSize || unsafe.SliceData(out) != unsafe.SliceData(pageDst) {
		return nil, ErrCorrupt
	}
	return out, nil
}

func readCOWRecord(f *os.File, ptr page.ValuePtr, shape COWRecordShape, verifyCRC bool, payloadScratch, rawScratch, outputScratch []byte, decode COWDecodeFunc, leaf bool) ([]byte, error) {
	if !shape.valid || shape.leaf != leaf || shape.ptr != ptr || shape.RecordBytes < HeaderSize ||
		uint64(cap(payloadScratch)) < shape.PayloadBytes() || uint64(cap(outputScratch)) < shape.ValueBytes ||
		(shape.Compressed && uint64(cap(rawScratch)) < shape.RawBytes) {
		return nil, ErrCOWReadCapacity
	}
	input := payloadScratch[:int(shape.RecordBytes):int(shape.RecordBytes)]
	header := input[:HeaderSize:HeaderSize]
	n, err := cowReadHeader(f, ptr, header, shape.limits, leaf)
	if err != nil {
		return nil, err
	}
	if uint64(n)+HeaderSize != shape.RecordBytes {
		return nil, ErrCorrupt
	}
	payload := input[HeaderSize:]
	if _, err := f.ReadAt(payload, int64(ptr.Offset-4)+HeaderSize); err != nil {
		return nil, err
	}
	p := 0
	if header[5]&recordFlagGrouped != 0 {
		if len(payload) < FrameHeaderSize {
			return nil, ErrCorrupt
		}
		p = FrameHeaderSize + int(payload[2])*8 + (int(payload[2])+1)*4
		if p > len(payload) {
			return nil, ErrCorrupt
		}
	}
	actual, err := cowShape(header, payload[:p], n, ptr, shape.limits)
	if err != nil {
		return nil, err
	}
	actual.leaf = leaf
	if actual != shape {
		return nil, ErrCorrupt
	}
	if verifyCRC && crc.Checksum(input[4:]) != binary.LittleEndian.Uint32(header[:4]) {
		return nil, ErrCorrupt
	}
	raw := payload[p:]
	if shape.Compressed {
		if decode == nil {
			return nil, ErrMissingDict
		}
		dst := rawScratch[:int(shape.RawBytes):int(shape.RawBytes)]
		raw, err = decode(shape.frame, raw, dst)
		if err != nil {
			return nil, err
		}
		if uint64(len(raw)) != shape.RawBytes || (len(raw) > 0 && unsafe.SliceData(raw) != unsafe.SliceData(dst)) {
			return nil, ErrCorrupt
		}
	}
	value := raw[int(shape.valueStart):int(uint64(shape.valueStart)+shape.ValueBytes)]
	if templ.IsEncodedPayload(value) || (!leaf && HasCompactLeafLogPayload(value)) {
		return nil, ErrCOWReadUnsupported
	}
	out := outputScratch[:int(shape.ValueBytes):int(shape.ValueBytes)]
	copy(out, value)
	return out, nil
}

// COWDecoderAllocationSizes reports separately rounded raw retained capacities
// BEFORE constructor allocation. Original definition bytes belong to the
// caller's existing read owner; DefinitionCopy accounts for zstd's parse copy.
// This intentionally loose envelope is pinned to snissn/compress 87fb149e4721,
// 64-bit Go, nil reader, concurrency=1 and stateless DecodeAll only. It covers
// retained owner storage, not process RSS or transient GC allocation peaks.
type COWDecoderAllocationSizes struct {
	OwnerWrapper, DefinitionCopy, RawBacking, InputBacking, SequenceBacking uint64
	FixedAllocationBytes                                                    uint64
	FixedAllocationCount                                                    int
}

func COWDecoderRetentionSizes(definitionBytes, rawCap, inputCap, windowCap uint64) (COWDecoderAllocationSizes, error) {
	if unsafe.Sizeof(uintptr(0)) != 8 || definitionBytes > math.MaxInt32 || rawCap == 0 || rawCap > math.MaxUint32 || inputCap == 0 || inputCap > math.MaxUint32 || windowCap < 1024 || windowCap > math.MaxUint32 {
		return COWDecoderAllocationSizes{}, ErrCOWReadCapacity
	}
	// Block/literal buffers <=128KiB+17; Huff tables <=2048 uint16;
	// dictionary/frame FSE tables have fixed arrays. Include the 24-byte
	// max-sequence array even though synchronous DecodeAll avoids that branch.
	// Generic synchronous sequence decoding may grow output by one <=128KiB
	// block before a later size/error check. Include Go 1.26 slice growth and
	// rounding in the retained replacement backing's conservative envelope.
	return COWDecoderAllocationSizes{OwnerWrapper: uint64(unsafe.Sizeof(COWDecoder{})), DefinitionCopy: definitionBytes, RawBacking: 2*max(rawCap, windowCap) + 512<<10, InputBacking: inputCap,
		SequenceBacking: 24 * (0x7f00 + 0xffff), FixedAllocationBytes: 256 << 10, FixedAllocationCount: 64}, nil
}

// COWDecoder owns parsed dictionary and decode workspace; it never consults
// gomap's global dict/no-dict codec caches. Existing library Huff/FSE pools own
// ephemeral scratch after return; the envelope covers scratch while this
// decoder prolongs references. The reservation lasts through final Close.
type COWDecoder struct {
	mu                           sync.Mutex
	decoder                      *zstd.Decoder
	blockCodec                   BlockCodec
	blockOnly, closed            bool
	producerID, rawCap, inputCap uint64
}

// COWBlockDecoderRetentionSizes describes allocation-free Snappy/LZ4 decode
// into caller-owned scratch. These codecs retain no dictionary or zstd state.
func COWBlockDecoderRetentionSizes(codec BlockCodec, rawCap, inputCap uint64) (COWDecoderAllocationSizes, error) {
	if (codec != BlockCodecSnappy && codec != BlockCodecLZ4) || rawCap == 0 || rawCap > math.MaxUint32 || inputCap == 0 || inputCap > math.MaxUint32 {
		return COWDecoderAllocationSizes{}, ErrCOWReadCapacity
	}
	return COWDecoderAllocationSizes{OwnerWrapper: uint64(unsafe.Sizeof(COWDecoder{})), RawBacking: rawCap, InputBacking: inputCap}, nil
}

// NewCOWBlockDecoder owns only finite native block scratch. The actual encoded
// codec must match on every Decode; dictionary/zstd records cannot use it.
func NewCOWBlockDecoder(codec BlockCodec, rawCap, inputCap uint64, admit func(COWDecoderAllocationSizes) error) (*COWDecoder, error) {
	sizes, err := COWBlockDecoderRetentionSizes(codec, rawCap, inputCap)
	if err != nil {
		return nil, err
	}
	if admit == nil {
		return nil, ErrCOWReadCapacity
	}
	if err := admit(sizes); err != nil {
		return nil, err
	}
	return &COWDecoder{blockOnly: true, blockCodec: codec, rawCap: rawCap, inputCap: inputCap}, nil
}

// NewCOWDecoder validates exact owned-definition provenance before admission.
// No-dictionary owners use producerID=0 and an empty definition. Admission is
// mandatory and must reserve every reported constituent before returning nil.
// inputCap bounds the full backing allocation containing payload, including any
// enclosing record header. InputBacking and RawBacking reserve caller storage
// this decoder can prolong after a read's other owners drop their references.
func NewCOWDecoder(producerID uint64, definition []byte, rawCap, inputCap, windowCap uint64, admit func(COWDecoderAllocationSizes) error) (*COWDecoder, error) {
	if producerID != 0 {
		if len(definition) == 0 {
			return nil, ErrMissingDict
		}
		digest := sha256.Sum256(definition)
		if binary.BigEndian.Uint64(digest[:8]) != producerID {
			return nil, ErrCorrupt
		}
	} else if len(definition) != 0 {
		return nil, ErrCorrupt
	}
	sizes, err := COWDecoderRetentionSizes(uint64(len(definition)), rawCap, inputCap, windowCap)
	if err != nil {
		return nil, err
	}
	if admit == nil {
		return nil, ErrCOWReadCapacity
	}
	if err := admit(sizes); err != nil {
		return nil, err
	}
	var dec *zstd.Decoder
	if producerID == 0 {
		dec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true), zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderMaxMemory(max(rawCap, windowCap, 1024)), zstd.WithDecoderMaxWindow(windowCap))
	} else {
		dec, err = zstd.NewReader(nil, zstd.WithDecoderDicts(definition), zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true), zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderMaxMemory(max(rawCap, windowCap, 1024)), zstd.WithDecoderMaxWindow(windowCap))
	}
	if err != nil {
		return nil, err
	}
	return &COWDecoder{decoder: dec, producerID: producerID, rawCap: rawCap, inputCap: inputCap}, nil
}

// Decode checks the producer ID before selecting any codec. dst's length is
// the exact admitted raw length, and its capacity is clipped to that length.
// The caller must ensure the full backing allocations containing payload and
// dst fit inputCap and rawCap, respectively, and stay admitted through this
// decoder's last reference. Close must follow the caller's reader/lifecycle drain.
func (d *COWDecoder) Decode(frame FrameHeader, payload, dst []byte) ([]byte, error) {
	if d == nil {
		return nil, ErrCOWDecoderClosed
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.decoder == nil && !d.blockOnly {
		return nil, ErrCOWDecoderClosed
	}
	if frame.DictID != d.producerID {
		return nil, ErrMissingDict
	}
	if uint64(len(payload)) > d.inputCap {
		return nil, ErrCOWReadCapacity
	}
	if frame.Version != FrameVersion || frame.Flags != FrameFlagCompressed || frame.K == 0 || uint64(len(dst)) > d.rawCap {
		return nil, ErrCorrupt
	}
	if frame.DictID != 0 && frame.Reserved != 0 {
		return nil, ErrCorrupt
	}
	if d.blockOnly && (frame.DictID != 0 || BlockCodec(frame.Reserved) != d.blockCodec) {
		return nil, ErrCorrupt
	}
	need := len(dst)
	dst = dst[:need:need]
	var out []byte
	var err error
	switch BlockCodec(frame.Reserved) {
	case BlockCodecNone, BlockCodecZSTD:
		out, err = d.decoder.DecodeAll(payload, dst[:0])
	case BlockCodecSnappy:
		var n int
		n, err = snappy.DecodedLen(payload)
		if err == nil && n != need {
			return nil, ErrCorrupt
		}
		if err == nil {
			out, err = snappy.Decode(dst, payload)
		}
	case BlockCodecLZ4:
		var n int
		n, err = lz4.UncompressBlock(payload, dst)
		if err == nil {
			out = dst[:n]
		}
	default:
		return nil, ErrCOWReadUnsupported
	}
	if err != nil {
		return nil, err
	}
	if len(out) != need || (need > 0 && unsafe.SliceData(out) != unsafe.SliceData(dst)) {
		return nil, ErrCorrupt
	}
	return out, nil
}

// Close waits for an active Decode and clears this owner's references. The
// caller must prevent new readers and drain queued reads before calling Close,
// then release its reservation after the final owner reference disappears.
func (d *COWDecoder) Close() {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.decoder != nil {
		d.decoder.Close()
		d.decoder = nil
	}
	d.closed = true
	d.producerID, d.rawCap, d.inputCap = 0, 0, 0
}
