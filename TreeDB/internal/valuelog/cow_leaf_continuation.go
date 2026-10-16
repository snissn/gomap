package valuelog

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

var ErrCOWCodecWorkUnbound = errors.New("valuelog: configured codec requires scoped SAME work binding")

var ErrCOWReadPending = errors.New("valuelog: COW leaf continuation pending")

// COWLeafContinuation is the resumable sibling of the ORIGINAL COW leaf reader.
// Caller-owned input/raw/page backings must already be admitted, disjoint,
// exclusive and retained under the SAME immutable registered file/root owner.
// This core neither discovers files nor adds owners, refreshes, or closes files.
// Limits cover actual containing capacities, not only returned subslices.
// ZSTD/dictionary borrow the caller-admitted SAME original codec continuation.
// Exact immutable definition custody remains with its existing read owner.
type COWLeafContinuation struct {
	TemplateScratch                          []byte
	templateDecoded                          bool
	ZSTD                                     *COWZSTDContinuation
	Template                                 *COWTemplateContinuation
	f                                        *os.File
	record                                   iterator.OrdinalRecordCursor
	limits, requestedLimits                  COWReadLimits
	input, raw, dst, value                   []byte
	shape                                    COWRecordShape
	decoder                                  COWBlockContinuation
	phase                                    uint8
	leaf                                     bool
	decodeAt                                 int
	active, verifyCRC, completed, cancelling bool
	err                                      error
	prefix, readAt, crcAt, copyAt            int
	sum                                      uint32
}

func (c *COWLeafContinuation) Begin(f *os.File, ptr page.ValuePtr, limits COWReadLimits, input, raw, dst []byte, verifyCRC bool, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*c))+128) {
		return false, nil
	}
	return c.beginAdmitted(f, ptr, limits, input, raw, dst, verifyCRC)
}

// BeginValue borrows one actual persistent value record under the same file
// owner. Unlike leaf mode it neither clamps the supported value domain nor
// expands a page. Caller-admitted full capacities remain charged until Cancel.
func (c *COWLeafContinuation) BeginValue(f *os.File, ptr page.ValuePtr, limits COWReadLimits, input, raw []byte, verifyCRC bool, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*c))+128) {
		return false, nil
	}
	return c.beginModeAdmitted(f, ptr, limits, input, raw, nil, verifyCRC, false)
}

// beginAdmitted is shared only with the concrete projection adapter, whose
// SAME OrdinalScanWork reservation covers this fixed constructor and probes.
func (c *COWLeafContinuation) beginAdmitted(f *os.File, ptr page.ValuePtr, limits COWReadLimits, input, raw, dst []byte, verifyCRC bool) (bool, error) {
	return c.beginModeAdmitted(f, ptr, limits, input, raw, dst, verifyCRC, true)
}
func (c *COWLeafContinuation) beginModeAdmitted(f *os.File, ptr page.ValuePtr, limits COWReadLimits, input, raw, dst []byte, verifyCRC, leaf bool) (bool, error) {
	if c.active || c.cancelling {
		return false, ErrCOWReadPending
	}
	requestedLimits := limits
	if leaf {
		limits.MaxRecordBytes = min(limits.MaxRecordBytes, 2<<20)
		limits.MaxRawBytes = min(limits.MaxRawBytes, MaxFrameK*page.PageSize)
		limits.MaxValueBytes = min(limits.MaxValueBytes, page.PageSize)
	}
	if f == nil || ptr.Offset < 4 || (leaf && len(dst) < page.PageSize) || (!leaf && isLeafLogFileID(ptr.FileID)) || limits.MaxRecordBytes < HeaderSize || limits.MaxRawBytes == 0 || limits.MaxValueBytes == 0 ||
		limits.MaxRecordBytes > uint64(^uint(0)>>1) || limits.MaxRawBytes > uint64(^uint(0)>>1) || limits.MaxValueBytes > uint64(^uint(0)>>1) ||
		uint64(cap(input)) > limits.MaxRecordBytes || uint64(cap(raw)) > limits.MaxRawBytes ||
		uint64(len(input)) < limits.MaxRecordBytes || uint64(len(raw)) < limits.MaxRawBytes ||
		cowBuffersOverlap(input, raw) || cowBuffersOverlap(input, dst) || cowBuffersOverlap(raw, dst) {
		return false, ErrCOWReadCapacity
	}
	if c.leaf == leaf && c.completed && c.record.CRCProved && c.err == nil && c.f == f && page.ValuePtrIsGrouped(ptr) && page.ValuePtrIsGrouped(c.record.Pointer) &&
		ptr.FileID == c.record.Pointer.FileID && ptr.Offset == c.record.Pointer.Offset &&
		page.ValuePtrRecordLength(ptr) == page.ValuePtrRecordLength(c.record.Pointer) &&
		c.limits == limits && c.requestedLimits == requestedLimits && c.verifyCRC == verifyCRC && cowSameBacking(c.input, input) && cowSameBacking(c.raw, raw) {
		c.record.Pointer = ptr
		c.shape.ptr = ptr
		c.dst = dst
		if leaf {
			c.dst = dst[:page.PageSize]
		}
		c.completed = false
		c.active = true
		c.record.Retarget = true
		c.record.SelectedOffsets = 0
		c.value = nil
		c.templateDecoded = false
		c.copyAt = 0
		c.phase = 15
		return true, nil
	}
	detach := c.completed && (c.ZSTD != nil && c.ZSTD.begun || c.Template != nil && c.Template.active)
	*c = COWLeafContinuation{ZSTD: c.ZSTD, Template: c.Template, TemplateScratch: c.TemplateScratch, f: f, record: iterator.OrdinalRecordCursor{Pointer: ptr, Active: true}, limits: limits, requestedLimits: requestedLimits, input: input, raw: raw, dst: dst, leaf: leaf, verifyCRC: verifyCRC, active: true}
	if leaf {
		c.dst = dst[:page.PageSize]
	}
	if detach {
		c.phase = 19
	}
	return true, nil
}

// Step reserves BEFORE every actual metadata/file read, RID/offset probe,
// record CRC/hash, literal/match decode, selected copy and compact expansion.
// A caller may pause/cancel anywhere; no scalar loop replays accepted work.
// StepWithCodec binds the SAME preadmitted work object for library callbacks.
// Callers of configured ZSTD leaves use this entry; ordinary block/raw readers
// retain Step's allocation-free stack work path.
func (c *COWLeafContinuation) StepWithCodec(w *iterator.OrdinalScanWork) ([]byte, bool, error) {
	if c.ZSTD != nil {
		c.ZSTD.BindWork(w)
		defer c.ZSTD.DropWork()
	}
	return c.Step(w)
}
func (c *COWLeafContinuation) Step(w *iterator.OrdinalScanWork) ([]byte, bool, error) {
	if c.ZSTD != nil && (w == nil || c.ZSTD.work != w) {
		return nil, false, ErrCOWCodecWorkUnbound
	}
	if c.err != nil {
		return nil, false, c.err
	}
	if c.cancelling {
		return nil, false, ErrCOWReadPending
	}
	if !c.active {
		if c.completed {
			return c.result(), true, nil
		}
		return nil, false, ErrCOWDecoderClosed
	}
	value, done, err := c.step(w)
	if err != nil {
		c.err = err
	}
	return value, done, err
}
func (c *COWLeafContinuation) step(w *iterator.OrdinalScanWork) ([]byte, bool, error) {
	if w == nil || w.Records >= w.RecordLimit || w.Bytes >= w.ByteLimit {
		return nil, false, nil
	}
	for {
		switch c.phase {
		case 19:
			if c.ZSTD != nil && c.ZSTD.begun {
				ok, err := c.ZSTD.detachBound(w)
				if err != nil || !ok {
					return nil, false, err
				}
			}
			if c.Template != nil && c.Template.active {
				if !c.Template.detach(w) {
					return nil, false, nil
				}
			}
			c.phase = 0
		case 0:
			if !w.Reserve(1, uint64(2*HeaderSize+len(c.f.Name())+128)) {
				return nil, false, nil
			}
			n, err := cowReadHeader(c.f, c.record.Pointer, c.input[:HeaderSize], c.limits, c.leaf)
			if err != nil {
				return nil, false, err
			}
			copy(c.record.Header[:], c.input[:HeaderSize])
			c.shape = COWRecordShape{RecordBytes: uint64(n) + HeaderSize, ptr: c.record.Pointer, limits: c.limits, leaf: c.leaf, valid: true}
			grouped := c.input[5]&recordFlagGrouped != 0
			if grouped != page.ValuePtrIsGrouped(c.record.Pointer) {
				return nil, false, ErrCorrupt
			}
			if !grouped {
				if binary.LittleEndian.Uint64(c.input[8:16]) == 0 {
					return nil, false, ErrCorrupt
				}
				c.shape.RawBytes, c.shape.ValueBytes = uint64(n), uint64(n)
				c.phase = 4
				continue
			}
			if binary.LittleEndian.Uint64(c.input[8:16]) != 0 || n < FrameHeaderSize {
				return nil, false, ErrCorrupt
			}
			c.phase = 1
		case 1:
			if !w.Reserve(1, 4*FrameHeaderSize) {
				return nil, false, nil
			}
			h := c.input[HeaderSize : HeaderSize+FrameHeaderSize]
			if _, err := c.f.ReadAt(h, int64(c.record.Pointer.Offset-4)+HeaderSize); err != nil {
				return nil, false, err
			}
			if h[0] != FrameVersion || h[1]&^FrameFlagCompressed != 0 || h[2] == 0 {
				return nil, false, ErrCorrupt
			}
			copy(c.record.Frame[:], h)
			c.shape.frame = FrameHeader{Version: h[0], Flags: h[1], K: h[2], Reserved: h[3], DictID: binary.LittleEndian.Uint64(h[4:12])}
			c.shape.DictID = c.shape.frame.DictID
			c.shape.Compressed = h[1]&FrameFlagCompressed != 0
			c.prefix = FrameHeaderSize + int(h[2])*8 + (int(h[2])+1)*4
			if uint64(c.prefix)+HeaderSize > c.shape.RecordBytes || int(page.ValuePtrSubIndex(c.record.Pointer)) >= int(h[2]) {
				return nil, false, ErrCorrupt
			}
			if c.shape.frame.Reserved != 0 {
				if !c.shape.Compressed || c.shape.DictID != 0 || BlockCodec(c.shape.frame.Reserved) > BlockCodecZSTD {
					return nil, false, ErrCOWReadUnsupported
				}
				c.shape.Codec = BlockCodec(c.shape.frame.Reserved)
			}
			// Existing compression IDs/definitions stay original. No fallback decoder.
			if c.shape.Compressed && (c.shape.DictID != 0 || c.shape.Codec == BlockCodecNone || c.shape.Codec == BlockCodecZSTD) {
				if c.ZSTD == nil {
					return nil, false, ErrCOWReadUnsupported
				}
				if c.ZSTD.ProducerID != c.shape.DictID {
					return nil, false, ErrMissingDict
				}
			}
			c.phase = 2
		case 2:
			if c.record.Directory.NextRID == uint16(c.shape.frame.K) {
				c.phase = 3
				continue
			}
			if !w.Reserve(1, 16) {
				return nil, false, nil
			}
			at := HeaderSize + FrameHeaderSize + int(c.record.Directory.NextRID)*8
			if _, err := c.f.ReadAt(c.input[at:at+8], int64(c.record.Pointer.Offset-4)+int64(at)); err != nil {
				return nil, false, err
			}
			if binary.LittleEndian.Uint64(c.input[at:at+8]) == 0 {
				return nil, false, ErrCorrupt
			}
			c.record.Directory.NextRID++
		case 3:
			if c.record.Directory.NextOffset > uint16(c.shape.frame.K) {
				c.shape.RawBytes = uint64(c.record.Directory.Previous)
				c.shape.ValueBytes = uint64(c.record.Directory.End - c.record.Directory.Start)
				c.shape.prefix = uint32(c.prefix)
				c.shape.valueStart = c.record.Directory.Start
				n := c.shape.RecordBytes - HeaderSize
				if (!c.shape.Compressed && uint64(c.prefix)+c.shape.RawBytes != n) || (c.shape.Compressed && uint64(c.prefix) == n) {
					return nil, false, ErrCorrupt
				}
				c.phase = 4
				continue
			}
			if !w.Reserve(1, 8) {
				return nil, false, nil
			}
			at := HeaderSize + FrameHeaderSize + int(c.shape.frame.K)*8 + int(c.record.Directory.NextOffset)*4
			if _, err := c.f.ReadAt(c.input[at:at+4], int64(c.record.Pointer.Offset-4)+int64(at)); err != nil {
				return nil, false, err
			}
			next := binary.LittleEndian.Uint32(c.input[at : at+4])
			if next < c.record.Directory.Previous || (c.record.Directory.NextOffset == 0 && next != 0) {
				return nil, false, ErrCorrupt
			}
			if c.record.Directory.NextOffset == uint16(page.ValuePtrSubIndex(c.record.Pointer)) {
				c.record.Directory.Start = next
			}
			if c.record.Directory.NextOffset == uint16(page.ValuePtrSubIndex(c.record.Pointer))+1 {
				c.record.Directory.End = next
			}
			c.record.Directory.Previous = next
			c.record.Directory.NextOffset++
		case 4:
			if !w.Reserve(1, uint64(HeaderSize+c.prefix)+uint64(unsafe.Sizeof(COWRecordShape{}))) {
				return nil, false, nil
			}
			if c.shape.RawBytes > c.limits.MaxRawBytes || c.shape.ValueBytes > c.limits.MaxValueBytes {
				return nil, false, ErrCOWReadCapacity
			}
			// SAME metadata digest format as cowShape, computed over actual owned bytes.
			c.shape.metadata = sha256.Sum256(c.input[:HeaderSize+c.prefix])
			c.readAt = HeaderSize + c.prefix
			c.crcAt = 4
			c.phase = 5
		case 5:
			if c.readAt == int(c.shape.RecordBytes) {
				c.phase = 6
				continue
			}
			n := cowContinuationChunk(w, int(c.shape.RecordBytes)-c.readAt, 2)
			if n == 0 || !w.Reserve(1, uint64(2*n)) {
				return nil, false, nil
			}
			if _, err := c.f.ReadAt(c.input[c.readAt:c.readAt+n], int64(c.record.Pointer.Offset-4)+int64(c.readAt)); err != nil {
				return nil, false, err
			}
			c.readAt += n
		case 6:
			if !c.verifyCRC {
				c.phase = 7
				continue
			}
			if c.crcAt == int(c.shape.RecordBytes) {
				if !w.Reserve(1, 4) {
					return nil, false, nil
				}
				if c.sum != binary.LittleEndian.Uint32(c.input[:4]) {
					return nil, false, ErrCorrupt
				}
				c.record.CRCProved = true
				c.phase = 7
				continue
			}
			n := cowContinuationChunk(w, int(c.shape.RecordBytes)-c.crcAt, 1)
			if n == 0 || !w.Reserve(1, uint64(n)) {
				return nil, false, nil
			}
			c.sum = crc.Update(c.sum, c.input[c.crcAt:c.crcAt+n])
			c.crcAt += n
		case 7:
			if c.shape.Compressed {
				var ok bool
				var err error
				if c.shape.Codec == BlockCodecNone || c.shape.Codec == BlockCodecZSTD {
					if c.leaf {
						ok, err = c.ZSTD.Begin(c.input[HeaderSize+c.prefix:int(c.shape.RecordBytes)], c.raw[:int(c.shape.RawBytes):int(c.shape.RawBytes)], w)
					} else {
						ok, err = c.ZSTD.BeginSpans(c.input[HeaderSize+c.prefix:int(c.shape.RecordBytes)], w)
					}
				} else {
					ok, err = c.decoder.Begin(c.shape.Codec, c.input[HeaderSize+c.prefix:int(c.shape.RecordBytes)], c.raw[:int(c.shape.RawBytes)], w)
				}
				if err != nil || !ok {
					return nil, false, err
				}
				c.phase = 8
				continue
			}
			if !w.Reserve(1, 24) {
				return nil, false, nil
			}
			c.value = c.input[HeaderSize+c.prefix+int(c.shape.valueStart) : HeaderSize+c.prefix+int(c.shape.valueStart)+int(c.shape.ValueBytes)]
			c.phase = 9
		case 8:
			var done bool
			var err error
			if c.shape.Codec == BlockCodecNone || c.shape.Codec == BlockCodecZSTD {
				var output []byte
				if !c.leaf {
					output = c.raw[c.decodeAt:min(c.decodeAt+4096, int(c.shape.RawBytes))]
				}
				n, yes, stepErr := c.ZSTD.stepBound(output, w)
				done, err = yes, stepErr
				c.decodeAt += n
				if done && err == nil && !c.leaf && uint64(c.decodeAt) != c.shape.RawBytes {
					err = ErrCorrupt
				}
				if done && err == nil && c.leaf {
					var raw []byte
					raw, err = c.ZSTD.RawResult()
					if err == nil && uint64(len(raw)) != c.shape.RawBytes {
						err = ErrCorrupt
					}
				}
			} else {
				done, err = c.decoder.Step(w)
			}
			if err != nil || !done {
				return nil, false, err
			}
			if !w.Reserve(1, 24) {
				return nil, false, nil
			}
			c.value = c.raw[int(c.shape.valueStart):int(uint64(c.shape.valueStart)+c.shape.ValueBytes)]
			c.phase = 9
		case 9:
			if !w.Reserve(1, uint64(compactLeafPagePayloadHeaderSize+32)) {
				return nil, false, nil
			}
			if !c.templateDecoded && templ.IsEncodedPayload(c.value) {
				if c.Template == nil {
					return nil, false, ErrCOWReadUnsupported
				}
				c.phase = 17
				continue
			}
			if !c.leaf {
				if HasCompactLeafLogPayload(c.value) {
					return nil, false, ErrCOWReadUnsupported
				}
				if uint64(len(c.value)) > c.limits.MaxValueBytes {
					return nil, false, ErrCOWReadCapacity
				}
				c.phase = 14
				continue
			}
			pre, suf, compact, err := compactLeafLogPayloadBounds(c.value)
			if err != nil {
				return nil, false, err
			}
			if compact {
				c.record.Directory.Start, c.record.Directory.End = uint32(pre), uint32(suf)
				c.phase = 11
			} else {
				if len(c.value) != page.PageSize {
					return nil, false, ErrCorrupt
				}
				c.phase = 10
			}
			c.copyAt = 0
		case 17:
			if !w.Reserve(1, 96) {
				return nil, false, nil
			}
			required := c.limits.MaxValueBytes
			if c.leaf {
				required = page.PageSize
			}
			if uint64(cap(c.TemplateScratch)) > c.limits.MaxValueBytes || uint64(len(c.TemplateScratch)) < required || cowBuffersOverlap(c.TemplateScratch, c.dst) || cowBuffersOverlap(c.TemplateScratch, c.input) || cowBuffersOverlap(c.TemplateScratch, c.raw) {
				return nil, false, ErrCOWReadCapacity
			}
			ok, err := c.Template.Begin(c.value, c.TemplateScratch[:int(required)], w)
			if err != nil || !ok {
				return nil, false, err
			}
			c.phase = 18
		case 18:
			value, done, err := c.Template.Step(w)
			if err != nil || !done {
				return nil, false, err
			}
			c.value = value
			c.templateDecoded = true
			c.phase = 9
		case 10:
			if c.copyAt == page.PageSize {
				c.phase = 14
				continue
			}
			n := cowContinuationChunk(w, page.PageSize-c.copyAt, 2)
			if n == 0 || !w.Reserve(1, uint64(2*n)) {
				return nil, false, nil
			}
			copy(c.dst[c.copyAt:c.copyAt+n], c.value[c.copyAt:c.copyAt+n])
			c.copyAt += n
		case 11:
			if c.copyAt == int(c.record.Directory.Start) {
				c.phase = 12
				continue
			}
			n := cowContinuationChunk(w, int(c.record.Directory.Start)-c.copyAt, 2)
			if n == 0 || !w.Reserve(1, uint64(2*n)) {
				return nil, false, nil
			}
			copy(c.dst[c.copyAt:c.copyAt+n], c.value[compactLeafPagePayloadHeaderSize+c.copyAt:compactLeafPagePayloadHeaderSize+c.copyAt+n])
			c.copyAt += n
		case 12:
			edge := page.PageSize - int(c.record.Directory.End)
			if c.copyAt == edge {
				c.phase = 13
				continue
			}
			n := cowContinuationChunk(w, edge-c.copyAt, 1)
			if n == 0 || !w.Reserve(1, uint64(n)) {
				return nil, false, nil
			}
			clear(c.dst[c.copyAt : c.copyAt+n])
			c.copyAt += n
		case 13:
			if c.copyAt == page.PageSize {
				c.phase = 14
				continue
			}
			n := cowContinuationChunk(w, page.PageSize-c.copyAt, 2)
			if n == 0 || !w.Reserve(1, uint64(2*n)) {
				return nil, false, nil
			}
			at := compactLeafPagePayloadHeaderSize + int(c.record.Directory.Start) + c.copyAt - (page.PageSize - int(c.record.Directory.End))
			copy(c.dst[c.copyAt:c.copyAt+n], c.value[at:at+n])
			c.copyAt += n
		case 15:
			if !w.Reserve(1, 8) {
				return nil, false, nil
			}
			index := uint16(page.ValuePtrSubIndex(c.record.Pointer))
			if index >= uint16(c.shape.frame.K) {
				return nil, false, ErrCorrupt
			}
			at := HeaderSize + FrameHeaderSize + int(c.shape.frame.K)*8 + int(index+uint16(c.record.SelectedOffsets))*4
			value := binary.LittleEndian.Uint32(c.input[at : at+4])
			if c.record.SelectedOffsets == 0 {
				c.shape.valueStart = value
			} else {
				if value < c.shape.valueStart || uint64(value) > c.shape.RawBytes {
					return nil, false, ErrCorrupt
				}
				c.shape.ValueBytes = uint64(value - c.shape.valueStart)
				if c.shape.ValueBytes > c.limits.MaxValueBytes {
					return nil, false, ErrCOWReadCapacity
				}
			}
			c.record.SelectedOffsets++
			if c.record.SelectedOffsets == 2 {
				c.phase = 16
			}
		case 16:
			if !w.Reserve(1, 24) {
				return nil, false, nil
			}
			from := int(c.shape.valueStart)
			to := from + int(c.shape.ValueBytes)
			if c.shape.Compressed {
				c.value = c.raw[from:to]
			} else {
				c.value = c.input[HeaderSize+c.prefix+from : HeaderSize+c.prefix+to]
			}
			c.record.Retarget = false
			c.phase = 9
		case 14:
			if !w.Reserve(1, 48) {
				return nil, false, nil
			}
			c.active = false
			c.completed = true
			return c.result(), true, nil
		}
	}
}

func (c *COWLeafContinuation) result() []byte {
	if c.leaf {
		return c.dst
	}
	return c.value
}

// Cancel removes this consumer's aliases in charged phases. It never closes the
// borrowed file, frees caller storage, drains original owners or claims refund.
func (c *COWLeafContinuation) CancelWithCodec(w *iterator.OrdinalScanWork) (bool, error) {
	if c.ZSTD != nil {
		z := c.ZSTD
		z.BindWork(w)
		defer z.DropWork()
	}
	return c.Cancel(w)
}
func (c *COWLeafContinuation) Cancel(w *iterator.OrdinalScanWork) (bool, error) {
	if c.ZSTD != nil && (w == nil || c.ZSTD.work != w) {
		return false, ErrCOWCodecWorkUnbound
	}
	if !c.cancelling {
		if c.Template != nil && !c.Template.Cancel(w) {
			return false, nil
		}
		if c.ZSTD != nil {
			done, err := c.ZSTD.cancelBound(w)
			if err != nil || !done {
				return false, err
			}
		}
		if !c.decoder.Cancel(w) {
			return false, nil
		}
		c.cancelling = true
	}
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*c))) {
		return false, nil
	}
	*c = COWLeafContinuation{}
	return true, nil
}
