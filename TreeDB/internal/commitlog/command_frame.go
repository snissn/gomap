package commitlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
)

const (
	CommandFrameVersion uint16 = 1
	// CommandFrameVersionV2 is the active command-WAL frame version. The V1
	// constant remains for stable fixture decoding and rebuild diagnostics.
	CommandFrameVersionV2 uint16 = 2

	CommandWALNonCriticalFlagStart uint64 = 1 << 32

	commandFrameHeaderSize                = 4 + 2 + 2 + 2 + 2 + 8 + 8 + 8 + 8 + 8 + 2 + 2 + 4 + 4 + 4 + 4
	rawKVBatchPayloadVersion              = uint16(1)
	rawKVZeroBatchPayloadV2               = uint16(2)
	rawKVZeroBatchPayloadV3               = uint16(3)
	rawKVBatchPayloadVersionWithRevisions = uint16(4)
	rawKVBatchHeaderSize                  = 2 + 4
	rawKVZeroBatchHeaderSize              = 2 + 4 + 4
	rawKVOpHeaderSize                     = 1 + 4 + 4
	rawKVOpRevisionHeaderSize             = rawKVOpHeaderSize + 8
	rawKVZeroOpHeaderSize                 = 4
	rawKVZeroOpHeaderSizeV3               = 2
	rawKVRevisionOffsetInlineCap          = 32
	rawKVNilRangeBoundLenUint32           = uint32(^uint32(0))

	collectionRebuildVectorIndexPayloadVersion      = uint16(1)
	collectionRebuildVectorIndexVersionEnd          = 2
	collectionRebuildVectorIndexCollectionLenStart  = collectionRebuildVectorIndexVersionEnd
	collectionRebuildVectorIndexCollectionLenEnd    = collectionRebuildVectorIndexCollectionLenStart + 4
	collectionRebuildVectorIndexIndexLenStart       = collectionRebuildVectorIndexCollectionLenEnd
	collectionRebuildVectorIndexIndexLenEnd         = collectionRebuildVectorIndexIndexLenStart + 4
	collectionRebuildVectorIndexPayloadHeaderSize   = collectionRebuildVectorIndexIndexLenEnd
	collectionRebuildVectorIndexCollectionNameStart = collectionRebuildVectorIndexPayloadHeaderSize

	externalRefEncodedFixedSize      = 2 + 2 + 4 + 8 + 8 + 8 + sha256.Size
	commandExtensionEncodedFixedSize = 2 + 2 + 4
)

var commandFrameMagic = [4]byte{'T', 'C', 'W', '1'}
var rawKVEmptyByteView = make([]byte, 0)
var rawKVSharedZeroValueView = make([]byte, 64<<10)
var rawKVCommandFrameHeaderTemplate = func() [commandFrameHeaderSize]byte {
	var h [commandFrameHeaderSize]byte
	copy(h[0:4], commandFrameMagic[:])
	binary.LittleEndian.PutUint16(h[4:6], CommandFrameVersion)
	binary.LittleEndian.PutUint16(h[6:8], CommandFrameVersion)
	binary.LittleEndian.PutUint16(h[8:10], uint16(CommandKindRawKVBatch))
	binary.LittleEndian.PutUint16(h[10:12], uint16(CommandScopeRawKV))
	binary.LittleEndian.PutUint16(h[52:54], uint16(PayloadFormatRawKVBatchV1))
	return h
}()

const commandWALCriticalFlagsMask uint64 = CommandWALNonCriticalFlagStart - 1
const maxCommandFrameUint32 = uint64(^uint32(0))

// CommandKind identifies the deterministic command payload schema carried by a
// command WAL frame.
type CommandKind uint16

const (
	CommandKindRawKVBatch CommandKind = 1

	// Collection command frames carry deterministic user-level collection
	// mutations. They do not encode physical root deltas.
	CommandKindCollectionInsertBatchByID    CommandKind = 100
	CommandKindCollectionDeleteBatchByID    CommandKind = 101
	CommandKindCollectionUpdateBatchByID    CommandKind = 102
	CommandKindCollectionRebuildVectorIndex CommandKind = 103
	CommandKindCollectionReplaceSourceByID  CommandKind = 104
	CommandKindCatalogCreateCollection      CommandKind = 200
	CommandKindCatalogMutationPlaceholder   CommandKind = CommandKindCatalogCreateCollection
	CommandKindDurablePrefixBarrier         CommandKind = 300
)

// CommandScope identifies which logical TreeDB surface a command mutates.
type CommandScope uint16

const (
	CommandScopeRawKV CommandScope = iota + 1
	CommandScopeCollection
	CommandScopeCatalog
	CommandScopeSystem
)

// PayloadFormat identifies the canonical payload encoding inside the envelope.
type PayloadFormat uint16

const (
	PayloadFormatRawKVBatchV1                   PayloadFormat = 1
	PayloadFormatNativeWireDeterministic        PayloadFormat = 2
	PayloadFormatCollectionInsertBatchByIDV1    PayloadFormat = 3
	PayloadFormatCollectionDeleteBatchByIDV1    PayloadFormat = 4
	PayloadFormatCollectionUpdateBatchByIDV1    PayloadFormat = 5
	PayloadFormatCatalogCreateCollectionV1      PayloadFormat = 6
	PayloadFormatCollectionRebuildVectorIndexV1 PayloadFormat = 7
	PayloadFormatDurablePrefixBarrierV1         PayloadFormat = 8
	PayloadFormatRawKVBatchV2                   PayloadFormat = 9
	PayloadFormatCollectionReplaceSourceByIDV1  PayloadFormat = 10
	PayloadFormatCollectionTypedBatchByIDV1     PayloadFormat = 11
)

// RawKVOp is a deterministic raw key/value mutation inside a RawKVBatch
// command payload. The command frame LSN is the batch identity; individual ops
// intentionally do not carry their own sequence numbers.
type RawKVOp byte

const (
	RawKVOpSet RawKVOp = iota + 1
	RawKVOpDelete
	RawKVOpSetRID
	RawKVOpDeleteRange
	RawKVOpSetMaterializedRID
)

// RawKVOperation represents a single operation in a RawKVBatch command frame.
//
// Field constraints by op type:
//   - RawKVOpSet: Key and Value are the raw bytes; RID must be zero.
//   - RawKVOpDelete: Key is set; Value must be empty; RID must be zero.
//   - RawKVOpSetRID: Key is set; Value MUST be empty (the RID is encoded
//     separately as an 8-byte payload); RID must be non-zero.
//   - RawKVOpDeleteRange: Key is the start bound and Value is the exclusive
//     end bound. Nil bounds are unbounded and are encoded with a sentinel
//     length; bounded empty/reversed ranges are invalid in command frames.
//   - RawKVOpSetMaterializedRID: Key and Value are the raw bytes and RID is
//     non-zero. Recovery must preserve that logical RID, reusing a matching
//     value-log record or materializing Value under that exact RID.
//
// Revision is optional per-entry metadata. Zero is the legacy/no-revision
// value. Revision is valid on point Set/Delete/SetRID/SetMaterializedRID
// operations; range deletes do not carry per-item revision metadata.
type RawKVOperation struct {
	Op       RawKVOp
	Key      []byte
	Value    []byte
	RID      uint64
	Revision uint64
}

// RawKVBatchOperationScanner replays RawKV operations in deterministic command
// order. Implementations must be replayable: planning and writing scan the
// same operations separately so large command frames do not need to materialize
// a canonical payload slice.
type RawKVBatchOperationScanner func(func(RawKVOperation) error) error

// RawKVBatchPayloadPlan is the exact shared RawKVBatchV1/V2 payload shape for a
// replayable operation source. The enclosing payload-format ID gates V2-only
// operations.
type RawKVBatchPayloadPlan struct {
	Count          int
	PayloadLen     int
	EntryRevisions bool
}

// RawKVBatchPayloadBuilder incrementally constructs a canonical RawKVBatch
// payload while returning stable key/value views into the owned payload bytes.
type RawKVBatchPayloadBuilder struct {
	payload                []byte
	zeroPayload            []byte
	zeroSetValueView       []byte
	payloadCapHint         int
	opHint                 int
	count                  int
	zeroSetCandidate       bool
	zeroSetCompactOnly     bool
	zeroSetValuesOmitted   bool
	zeroSetValueLen        int
	zeroSetKeyBytes        int
	zeroSetMaxKeyLen       int
	zeroSetCompactVersion  uint16
	zeroSetValueRef        []byte
	payloadValueViews      bool
	zeroSetValueViewGlobal bool
	entryRevisions         bool
	revisionOffsets        []uint32
	revisionOffsetInline   [rawKVRevisionOffsetInlineCap]uint32
}

// RawKVBatchPayloadBufferMask identifies builder-owned buffers whose byte
// slices were handed to another owner.
type RawKVBatchPayloadBufferMask uint8

const (
	RawKVBatchPayloadBufferPayload RawKVBatchPayloadBufferMask = 1 << iota
	RawKVBatchPayloadBufferZeroValue
)

// NewRawKVBatchPayloadBuilder returns a builder initialized with best-effort
// capacity hints. Invalid or overflowing hints intentionally fall back to the
// minimum header capacity; callers that need to observe hint errors should use
// RawKVBatchPayloadBuilder.ResetWithHint directly.
func NewRawKVBatchPayloadBuilder(opHint, byteHint int) RawKVBatchPayloadBuilder {
	var b RawKVBatchPayloadBuilder
	_ = b.ResetWithHint(opHint, byteHint)
	return b
}

// ResetWithHint resets the builder and reserves capacity for the supplied
// approximate operation and key/value byte counts. Hint errors are advisory:
// invalid or overflowing hints return ErrRecordTooLarge and fall back to the
// minimum header capacity so callers may intentionally ignore the error when
// they can tolerate a small reusable buffer.
func (b *RawKVBatchPayloadBuilder) ResetWithHint(opHint, byteHint int) error {
	capHint := rawKVBatchHeaderSize
	var err error
	if hint, hintErr := rawKVBatchPayloadSizeHint(opHint, byteHint); hintErr == nil {
		capHint = hint
	} else {
		err = hintErr
	}
	b.payloadCapHint = capHint
	if opHint > 0 {
		b.opHint = opHint
	} else {
		b.opHint = 0
	}
	if cap(b.payload) < rawKVBatchHeaderSize {
		b.payload = make([]byte, rawKVBatchHeaderSize, rawKVBatchHeaderSize)
	} else {
		b.payload = b.payload[:rawKVBatchHeaderSize]
		clear(b.payload)
	}
	binary.LittleEndian.PutUint16(b.payload[0:2], rawKVBatchPayloadVersion)
	b.count = 0
	b.zeroSetCandidate = true
	b.zeroSetCompactOnly = true
	b.zeroSetValuesOmitted = false
	b.zeroSetValueLen = -1
	b.zeroSetKeyBytes = 0
	b.zeroSetMaxKeyLen = 0
	b.zeroSetCompactVersion = 0
	b.zeroSetValueRef = nil
	b.payloadValueViews = false
	b.zeroSetValueViewGlobal = false
	b.entryRevisions = false
	if b.revisionOffsets != nil {
		b.revisionOffsets = b.revisionOffsets[:0]
	}
	if b.zeroPayload != nil {
		b.zeroPayload = b.zeroPayload[:0]
	}
	return err
}

// EnableEntryRevisions switches the builder to RawKVBatch v4 layout before
// appending operations, reserving an 8-byte revision slot per operation.
func (b *RawKVBatchPayloadBuilder) EnableEntryRevisions() error {
	if b == nil {
		return ErrCorrupt
	}
	if b.payload == nil {
		_ = b.ResetWithHint(0, 0)
	}
	if b.entryRevisions {
		return nil
	}
	if b.count != 0 {
		return ErrCorrupt
	}
	b.entryRevisions = true
	b.zeroSetCandidate = false
	b.zeroSetCompactOnly = false
	b.zeroSetValuesOmitted = false
	b.zeroSetValueLen = -1
	b.zeroSetKeyBytes = 0
	b.zeroSetMaxKeyLen = 0
	b.zeroSetCompactVersion = 0
	b.zeroSetValueRef = nil
	b.resetRevisionOffsetsForHint()
	if len(b.payload) < rawKVBatchHeaderSize {
		if cap(b.payload) < rawKVBatchHeaderSize {
			b.payload = make([]byte, rawKVBatchHeaderSize, rawKVBatchHeaderSize)
		} else {
			b.payload = b.payload[:rawKVBatchHeaderSize]
		}
	}
	binary.LittleEndian.PutUint16(b.payload[0:2], rawKVBatchPayloadVersionWithRevisions)
	return nil
}

func (b *RawKVBatchPayloadBuilder) resetRevisionOffsetsForHint() {
	if b == nil {
		return
	}
	if b.opHint > rawKVRevisionOffsetInlineCap {
		if cap(b.revisionOffsets) < b.opHint {
			b.revisionOffsets = make([]uint32, 0, b.opHint)
			return
		}
		b.revisionOffsets = b.revisionOffsets[:0]
		return
	}
	b.revisionOffsets = b.revisionOffsetInline[:0]
}

// EntryRevisionsEnabled reports whether the builder is emitting revision-aware
// RawKVBatch payloads.
func (b *RawKVBatchPayloadBuilder) EntryRevisionsEnabled() bool {
	return b != nil && b.entryRevisions
}

// RevisionSlotOffsets returns byte offsets of revision slots in the canonical
// payload. The returned slice aliases builder-owned storage and is read-only.
func (b *RawKVBatchPayloadBuilder) RevisionSlotOffsets() []uint32 {
	if b == nil || !b.entryRevisions || len(b.revisionOffsets) == 0 {
		return nil
	}
	return b.revisionOffsets
}

func (b *RawKVBatchPayloadBuilder) Payload() []byte {
	if b == nil {
		return nil
	}
	if len(b.payload) >= rawKVBatchHeaderSize {
		version := rawKVBatchPayloadVersion
		if b.entryRevisions {
			version = rawKVBatchPayloadVersionWithRevisions
		}
		binary.LittleEndian.PutUint16(b.payload[0:2], version)
		binary.LittleEndian.PutUint32(b.payload[2:6], uint32(b.count))
	}
	if b.entryRevisions {
		return b.payload
	}
	if b.zeroSetCompactOnly && b.zeroSetCandidate && b.count > 0 && b.zeroSetValueLen > 0 && len(b.zeroPayload) >= rawKVZeroBatchHeaderSize {
		version := b.zeroSetCompactVersion
		if version == 0 {
			version = rawKVZeroBatchPayloadV2
		}
		binary.LittleEndian.PutUint16(b.zeroPayload[0:2], version)
		binary.LittleEndian.PutUint32(b.zeroPayload[2:6], uint32(b.count))
		binary.LittleEndian.PutUint32(b.zeroPayload[6:10], uint32(b.zeroSetValueLen))
		return b.zeroPayload
	}
	if b.zeroSetCompactOnly && b.zeroSetCandidate && b.count > 0 && b.zeroSetValueLen > 0 {
		if err := b.materializeCompactZeroSetPayload(); err == nil {
			b.zeroSetCompactOnly = false
			return b.payload
		}
	}
	if b.zeroSetCandidate && b.count > 0 && b.zeroSetValueLen > 0 {
		if payload, ok := b.compactZeroSetPayload(); ok {
			return payload
		}
	}
	b.materializeOmittedZeroValues()
	return b.payload
}

// StampEntryRevisions writes revision slots in payload order for builders that
// were initialized with EnableEntryRevisions.
func (b *RawKVBatchPayloadBuilder) StampEntryRevisions(scan func(func(uint64) error) error) error {
	if b == nil || scan == nil {
		return ErrCorrupt
	}
	payload := b.Payload()
	if !b.entryRevisions || len(payload) < rawKVBatchHeaderSize || binary.LittleEndian.Uint16(payload[0:2]) != rawKVBatchPayloadVersionWithRevisions {
		return ErrCommandWALUnsupportedVersion
	}
	count := int(binary.LittleEndian.Uint32(payload[2:6]))
	if count < 0 || count > (len(payload)-rawKVBatchHeaderSize)/rawKVOpRevisionHeaderSize {
		return ErrCorrupt
	}
	if len(b.revisionOffsets) == count {
		seen := 0
		err := scan(func(revision uint64) error {
			if seen >= count {
				return ErrCorrupt
			}
			off := int(b.revisionOffsets[seen])
			if off < rawKVBatchHeaderSize || off+8 > len(payload) {
				return ErrCorrupt
			}
			binary.LittleEndian.PutUint64(payload[off:off+8], revision)
			seen++
			return nil
		})
		if err != nil {
			return err
		}
		if seen != count {
			return ErrCorrupt
		}
		return nil
	}
	off := rawKVBatchHeaderSize
	seen := 0
	err := scan(func(revision uint64) error {
		if seen >= count || off+rawKVOpRevisionHeaderSize > len(payload) {
			return ErrCorrupt
		}
		op := RawKVOp(payload[off])
		keyLen := binary.LittleEndian.Uint32(payload[off+1 : off+5])
		valueLen := binary.LittleEndian.Uint32(payload[off+5 : off+9])
		keyBytes := keyLen
		valueBytes := valueLen
		if op == RawKVOpDeleteRange {
			if keyLen == rawKVNilRangeBoundLenUint32 {
				keyBytes = 0
			}
			if valueLen == rawKVNilRangeBoundLenUint32 {
				valueBytes = 0
			}
		}
		need := uint64(rawKVOpRevisionHeaderSize) + uint64(keyBytes) + uint64(valueBytes)
		if need > uint64(len(payload)-off) || need > uint64(^uint(0)>>1) {
			return ErrCorrupt
		}
		binary.LittleEndian.PutUint64(payload[off+9:off+17], revision)
		off += int(need)
		seen++
		return nil
	})
	if err != nil {
		return err
	}
	if seen != count || off != len(payload) {
		return ErrCorrupt
	}
	return nil
}

func (b *RawKVBatchPayloadBuilder) appendRevisionOffset(headerStart int) error {
	if b == nil || !b.entryRevisions {
		return nil
	}
	slot := headerStart + rawKVOpHeaderSize
	if commandFrameIntExceedsUint32(slot + 8) {
		return ErrRecordTooLarge
	}
	b.revisionOffsets = append(b.revisionOffsets, uint32(slot))
	return nil
}

func (b *RawKVBatchPayloadBuilder) Count() int {
	if b == nil {
		return 0
	}
	return b.count
}

func (b *RawKVBatchPayloadBuilder) Len() int {
	if b == nil {
		return 0
	}
	return len(b.payload)
}

// RetainedCap returns the backing capacity retained by the canonical payload.
func (b *RawKVBatchPayloadBuilder) RetainedCap() int {
	if b == nil {
		return 0
	}
	return cap(b.payload)
}

// RetainedBytes returns the total builder-owned backing capacity retained across
// canonical, compact-zero, zero-value-view, and heap revision-offset buffers.
func (b *RawKVBatchPayloadBuilder) RetainedBytes() int {
	if b == nil {
		return 0
	}
	total := 0
	maxInt := int(^uint(0) >> 1)
	add := func(n int) {
		if n < 0 || n > maxInt-total {
			total = maxInt
			return
		}
		total += n
	}
	add(cap(b.payload))
	add(cap(b.zeroPayload))
	add(cap(b.zeroSetValueView))
	if cap(b.revisionOffsets) > rawKVRevisionOffsetInlineCap {
		if cap(b.revisionOffsets) > maxInt/4 {
			return maxInt
		}
		add(cap(b.revisionOffsets) * 4)
	}
	return total
}

// PrepareForReuse clears logical state while retaining bounded backing buffers.
// It also drops inline-sized revision-offset slices because a copied builder may
// have that slice pointing at another builder's embedded inline array.
func (b *RawKVBatchPayloadBuilder) PrepareForReuse(maxRetainedBytes int) bool {
	if b == nil {
		return false
	}
	if maxRetainedBytes >= 0 && b.RetainedBytes() > maxRetainedBytes {
		*b = RawKVBatchPayloadBuilder{}
		return false
	}
	payload := b.payload
	zeroPayload := b.zeroPayload
	zeroSetValueView := b.zeroSetValueView
	revisionOffsets := b.revisionOffsets
	keepRevisionOffsets := cap(revisionOffsets) > rawKVRevisionOffsetInlineCap
	*b = RawKVBatchPayloadBuilder{}
	if cap(payload) > 0 {
		b.payload = payload[:0]
	}
	if cap(zeroPayload) > 0 {
		b.zeroPayload = zeroPayload[:0]
	}
	if cap(zeroSetValueView) > 0 {
		b.zeroSetValueView = zeroSetValueView[:0]
	}
	if keepRevisionOffsets {
		b.revisionOffsets = revisionOffsets[:0]
	}
	return b.RetainedBytes() > 0
}

// RetainedByteBuffers returns the builder-owned byte buffers that may back
// key/value views returned by AppendSet/AppendDelete. Callers must not mutate,
// pool, or reuse these buffers while any returned view can still be observed.
func (b *RawKVBatchPayloadBuilder) RetainedByteBuffers() [][]byte {
	if b == nil {
		return nil
	}
	var out [][]byte
	if cap(b.payload) > 0 {
		out = append(out, b.payload[:0])
	}
	if cap(b.zeroPayload) > 0 {
		out = append(out, b.zeroPayload[:0])
	}
	if cap(b.zeroSetValueView) > 0 {
		out = append(out, b.zeroSetValueView[:0])
	}
	return out
}

// RetainedValueByteBuffers returns builder-owned buffers that may back value
// views returned by AppendSet. It intentionally excludes compact-zero key
// payload buffers, since append-only memtables copy keys and only retain values.
// When value views point at the process-global zero view, it returns one
// zero-cap sentinel chunk as a stable-borrow permission signal and no mask.
func (b *RawKVBatchPayloadBuilder) RetainedValueByteBuffers() ([][]byte, RawKVBatchPayloadBufferMask) {
	if b == nil {
		return nil, 0
	}
	var out [][]byte
	var mask RawKVBatchPayloadBufferMask
	if b.zeroSetValueViewGlobal {
		out = append(out, rawKVEmptyByteView[:0:0])
	}
	if cap(b.zeroSetValueView) > 0 {
		out = append(out, b.zeroSetValueView[:0])
		mask |= RawKVBatchPayloadBufferZeroValue
	}
	if b.payloadValueViews && cap(b.payload) > 0 {
		out = append(out, b.payload[:0])
		mask |= RawKVBatchPayloadBufferPayload
	}
	return out, mask
}

// DetachRetainedByteBuffers drops builder references to payload buffers without
// returning them to the builder's reuse path. Use this when those buffers were
// handed to another owner with a longer lifetime, such as a mutable memtable.
func (b *RawKVBatchPayloadBuilder) DetachRetainedByteBuffers() {
	if b == nil {
		return
	}
	revisionOffsets := b.revisionOffsets
	keepRevisionOffsets := cap(revisionOffsets) > rawKVRevisionOffsetInlineCap
	*b = RawKVBatchPayloadBuilder{}
	if keepRevisionOffsets {
		b.revisionOffsets = revisionOffsets[:0]
	}
}

// DetachRetainedValueByteBuffers drops only the value buffers selected by mask.
// This lets compact-zero command payload/key buffers remain reusable when the
// only memtable-retained data is the separate zero-value view buffer.
func (b *RawKVBatchPayloadBuilder) DetachRetainedValueByteBuffers(mask RawKVBatchPayloadBufferMask) {
	if b == nil || mask == 0 {
		return
	}
	if mask&RawKVBatchPayloadBufferPayload != 0 {
		b.payload = nil
	}
	if mask&RawKVBatchPayloadBufferZeroValue != 0 {
		b.zeroSetValueView = nil
	}
}

// RetainedCapAfterAppend returns the backing capacity that would be retained
// after appending needed bytes with the same growth rule used by Append.
func (b *RawKVBatchPayloadBuilder) RetainedCapAfterAppend(needed int) (int, error) {
	if b == nil {
		return 0, nil
	}
	payloadLen, payloadCap := b.payloadLenCapForAppend()
	_, newCap, err := b.payloadGrowthFrom(payloadLen, payloadCap, needed)
	return newCap, err
}

// RetainedCapAfterAppendSet returns the payload-builder backing capacity that
// AppendSet would retain. If the append stays compact-zero, the returned
// capacity includes the compact-zero payload and zero-value view buffers. If the
// append must expand compact-zero sets first, it includes that materialization.
func (b *RawKVBatchPayloadBuilder) RetainedCapAfterAppendSet(key, value []byte) (int, error) {
	if b == nil {
		return 0, nil
	}
	if key == nil || value == nil {
		return 0, ErrCorrupt
	}
	if commandFrameIntExceedsUint32(b.count+1) || commandFrameIntExceedsUint32(len(key)) || commandFrameIntExceedsUint32(len(value)) {
		return 0, ErrRecordTooLarge
	}
	if b.zeroSetCompactOnly && b.canAppendCompactZeroSet(value) {
		return b.retainedCapAfterCompactZeroSetAppend(key, value)
	}
	needed := rawKVOperationHeaderSize(b.entryRevisions) + len(key) + len(value)
	if b.zeroSetCompactOnly {
		return b.retainedCapAfterCompactZeroMaterializeAppend(needed)
	}
	payloadCap, err := b.RetainedCapAfterAppend(needed)
	if err != nil {
		return 0, err
	}
	revisionOffsetBytes, err := b.retainedRevisionOffsetBytesAfterAppend()
	if err != nil {
		return 0, err
	}
	return b.retainedPayloadBufferCap(payloadCap, cap(b.zeroPayload), cap(b.zeroSetValueView), revisionOffsetBytes)
}

// RetainedCapAfterAppendDelete returns the payload-builder backing capacity
// that AppendDelete would retain.
func (b *RawKVBatchPayloadBuilder) RetainedCapAfterAppendDelete(key []byte) (int, error) {
	if b == nil {
		return 0, nil
	}
	if key == nil {
		return 0, ErrCorrupt
	}
	if commandFrameIntExceedsUint32(b.count+1) || commandFrameIntExceedsUint32(len(key)) {
		return 0, ErrRecordTooLarge
	}
	needed := rawKVOperationHeaderSize(b.entryRevisions) + len(key)
	if b.zeroSetCompactOnly {
		return b.retainedCapAfterCompactZeroMaterializeAppend(needed)
	}
	payloadCap, err := b.RetainedCapAfterAppend(needed)
	if err != nil {
		return 0, err
	}
	revisionOffsetBytes, err := b.retainedRevisionOffsetBytesAfterAppend()
	if err != nil {
		return 0, err
	}
	return b.retainedPayloadBufferCap(payloadCap, cap(b.zeroPayload), cap(b.zeroSetValueView), revisionOffsetBytes)
}

// RetainedCapAfterAppendDeleteRange returns the payload-builder backing capacity
// that AppendDeleteRange would retain.
func (b *RawKVBatchPayloadBuilder) RetainedCapAfterAppendDeleteRange(start, end []byte) (int, error) {
	if b == nil {
		return 0, nil
	}
	if err := validateRawKVDeleteRangeBounds(start, end); err != nil {
		return 0, err
	}
	if commandFrameIntExceedsUint32(b.count + 1) {
		return 0, ErrRecordTooLarge
	}
	_, startBytes, err := rawKVRangeBoundEncodedLen(start)
	if err != nil {
		return 0, err
	}
	_, endBytes, err := rawKVRangeBoundEncodedLen(end)
	if err != nil {
		return 0, err
	}
	needed := rawKVOperationHeaderSize(b.entryRevisions) + startBytes + endBytes
	if b.zeroSetCompactOnly {
		return b.retainedCapAfterCompactZeroMaterializeAppend(needed)
	}
	payloadCap, err := b.RetainedCapAfterAppend(needed)
	if err != nil {
		return 0, err
	}
	revisionOffsetBytes, err := b.retainedRevisionOffsetBytesAfterAppend()
	if err != nil {
		return 0, err
	}
	return b.retainedPayloadBufferCap(payloadCap, cap(b.zeroPayload), cap(b.zeroSetValueView), revisionOffsetBytes)
}

func (b *RawKVBatchPayloadBuilder) Truncate(payloadLen, count int) {
	if b == nil || payloadLen < rawKVBatchHeaderSize || payloadLen > len(b.payload) || count < 0 {
		return
	}
	if b.zeroSetCompactOnly {
		b.truncateCompactZeroSetPayload(count)
		return
	}
	b.materializeOmittedZeroValues()
	b.payload = b.payload[:payloadLen]
	b.count = count
	if b.entryRevisions && count <= len(b.revisionOffsets) {
		b.revisionOffsets = b.revisionOffsets[:count]
	}
	binary.LittleEndian.PutUint32(b.payload[2:6], uint32(count))
	b.recomputeZeroSetCandidate()
}

func (b *RawKVBatchPayloadBuilder) Append(op RawKVOperation) (keyView, valueView []byte, err error) {
	if b == nil {
		return nil, nil, ErrCorrupt
	}
	if b.payload == nil {
		b.ResetWithHint(0, 0)
	}
	if commandFrameIntExceedsUint32(b.count + 1) {
		return nil, nil, ErrRecordTooLarge
	}
	if err := validateRawKVOperation(&op); err != nil {
		return nil, nil, err
	}
	if op.Revision != 0 && !b.entryRevisions {
		return nil, nil, ErrCommandWALUnsupportedVersion
	}
	if op.Op == RawKVOpDeleteRange {
		keyView, err := b.AppendDeleteRange(op.Key, op.Value)
		return keyView, nil, err
	}
	valueLen := len(op.Value)
	if op.Op == RawKVOpSetRID {
		valueLen = 8
	} else if op.Op == RawKVOpSetMaterializedRID {
		valueLen += 8
	}
	if commandFrameIntExceedsUint32(len(op.Key)) || commandFrameIntExceedsUint32(valueLen) {
		return nil, nil, ErrRecordTooLarge
	}
	value := op.Value
	var ridBuf [8]byte
	if op.Op == RawKVOpSetRID {
		binary.LittleEndian.PutUint64(ridBuf[:], op.RID)
		value = ridBuf[:]
	} else if op.Op == RawKVOpSetMaterializedRID {
		materialized := make([]byte, 8+len(op.Value))
		binary.LittleEndian.PutUint64(materialized[:8], op.RID)
		copy(materialized[8:], op.Value)
		value = materialized
	}
	return b.appendValidatedWithRevision(op.Op, op.Key, value, op.Revision)
}

// AppendSet appends a validated RawKV Set operation without materializing a
// RawKVOperation wrapper. The key and value must be non-nil.
func (b *RawKVBatchPayloadBuilder) AppendSet(key, value []byte) (keyView, valueView []byte, err error) {
	return b.appendSet(key, value, false)
}

// AppendSetKnownZeroValue is like AppendSet but the caller has already verified
// that value is non-empty and all zero bytes. The canonical command-WAL payload
// still receives the supplied value bytes; the returned value view can use the
// shared zero buffer without rescanning.
func (b *RawKVBatchPayloadBuilder) AppendSetKnownZeroValue(key, value []byte) (keyView, valueView []byte, err error) {
	return b.appendSet(key, value, true)
}

func (b *RawKVBatchPayloadBuilder) appendSet(key, value []byte, knownZeroValue bool) (keyView, valueView []byte, err error) {
	if b == nil {
		return nil, nil, ErrCorrupt
	}
	if key == nil || value == nil {
		return nil, nil, ErrCorrupt
	}
	if b.payload == nil {
		_ = b.ResetWithHint(0, 0)
	}
	if commandFrameIntExceedsUint32(b.count + 1) {
		return nil, nil, ErrRecordTooLarge
	}
	if commandFrameIntExceedsUint32(len(key)) || commandFrameIntExceedsUint32(len(value)) {
		return nil, nil, ErrRecordTooLarge
	}
	if b.zeroSetCompactOnly && b.canAppendCompactZeroSet(value) {
		keyView, err := b.appendCompactZeroSet(key, value)
		if err != nil {
			return nil, nil, err
		}
		return keyView, b.zeroSetValueViewForLen(len(value)), nil
	}
	if b.zeroSetCompactOnly {
		if err := b.materializeCompactZeroSetPayload(); err != nil {
			return nil, nil, err
		}
		b.zeroSetCompactOnly = false
	}
	headerSize := rawKVOperationHeaderSize(b.entryRevisions)
	off, err := b.appendRawKVPayloadSpace(headerSize + len(key) + len(value))
	if err != nil {
		return nil, nil, err
	}
	b.payload[off] = byte(RawKVOpSet)
	binary.LittleEndian.PutUint32(b.payload[off+1:off+5], uint32(len(key)))
	binary.LittleEndian.PutUint32(b.payload[off+5:off+9], uint32(len(value)))
	if b.entryRevisions {
		binary.LittleEndian.PutUint64(b.payload[off+9:off+17], 0)
		if err := b.appendRevisionOffset(off); err != nil {
			return nil, nil, err
		}
	}
	off += headerSize
	keyStart := off
	copy(b.payload[off:], key)
	off += len(key)
	valueStart := off
	zeroCompact := b.recordZeroSetCandidateForAppended(key, value)
	if zeroCompact {
		b.zeroSetValuesOmitted = true
		valueView = b.zeroSetValueViewForLen(len(value))
	} else {
		copy(b.payload[off:], value)
		if knownZeroValue || allZeroBytes(value) {
			valueView = b.zeroSetValueViewForLen(len(value))
		} else {
			valueView = b.payload[valueStart : valueStart+len(value) : valueStart+len(value)]
			b.payloadValueViews = true
		}
	}
	off += len(value)
	b.count++
	return b.payload[keyStart : keyStart+len(key) : keyStart+len(key)], valueView, nil
}

// AppendDelete appends a validated RawKV Delete operation without
// materializing a RawKVOperation wrapper. The key must be non-nil.
func (b *RawKVBatchPayloadBuilder) AppendDelete(key []byte) (keyView []byte, err error) {
	if b == nil {
		return nil, ErrCorrupt
	}
	if key == nil {
		return nil, ErrCorrupt
	}
	if b.payload == nil {
		_ = b.ResetWithHint(0, 0)
	}
	if commandFrameIntExceedsUint32(b.count + 1) {
		return nil, ErrRecordTooLarge
	}
	if commandFrameIntExceedsUint32(len(key)) {
		return nil, ErrRecordTooLarge
	}
	if b.zeroSetCompactOnly {
		if err := b.materializeCompactZeroSetPayload(); err != nil {
			return nil, err
		}
		b.zeroSetCompactOnly = false
	}
	headerSize := rawKVOperationHeaderSize(b.entryRevisions)
	off, err := b.appendRawKVPayloadSpace(headerSize + len(key))
	if err != nil {
		return nil, err
	}
	b.payload[off] = byte(RawKVOpDelete)
	binary.LittleEndian.PutUint32(b.payload[off+1:off+5], uint32(len(key)))
	binary.LittleEndian.PutUint32(b.payload[off+5:off+9], 0)
	if b.entryRevisions {
		binary.LittleEndian.PutUint64(b.payload[off+9:off+17], 0)
		if err := b.appendRevisionOffset(off); err != nil {
			return nil, err
		}
	}
	off += headerSize
	keyStart := off
	copy(b.payload[off:], key)
	off += len(key)
	b.count++
	b.disableZeroSetCandidate()
	return b.payload[keyStart:off:off], nil
}

// AppendDeleteRange appends a validated RawKV DeleteRange operation. Nil start
// or end bounds are encoded as unbounded sentinels and returned as nil views.
func (b *RawKVBatchPayloadBuilder) AppendDeleteRange(start, end []byte) (startView []byte, err error) {
	if b == nil {
		return nil, ErrCorrupt
	}
	if err := validateRawKVDeleteRangeBounds(start, end); err != nil {
		return nil, err
	}
	if b.payload == nil {
		_ = b.ResetWithHint(0, 0)
	}
	if commandFrameIntExceedsUint32(b.count + 1) {
		return nil, ErrRecordTooLarge
	}
	startLen, startBytes, err := rawKVRangeBoundEncodedLen(start)
	if err != nil {
		return nil, err
	}
	endLen, endBytes, err := rawKVRangeBoundEncodedLen(end)
	if err != nil {
		return nil, err
	}
	if b.zeroSetCompactOnly {
		if err := b.materializeCompactZeroSetPayload(); err != nil {
			return nil, err
		}
		b.zeroSetCompactOnly = false
	}
	headerSize := rawKVOperationHeaderSize(b.entryRevisions)
	off, err := b.appendRawKVPayloadSpace(headerSize + startBytes + endBytes)
	if err != nil {
		return nil, err
	}
	b.payload[off] = byte(RawKVOpDeleteRange)
	binary.LittleEndian.PutUint32(b.payload[off+1:off+5], startLen)
	binary.LittleEndian.PutUint32(b.payload[off+5:off+9], endLen)
	if b.entryRevisions {
		binary.LittleEndian.PutUint64(b.payload[off+9:off+17], 0)
		if err := b.appendRevisionOffset(off); err != nil {
			return nil, err
		}
	}
	off += headerSize
	if startBytes > 0 {
		startStart := off
		copy(b.payload[off:], start)
		off += startBytes
		startView = b.payload[startStart:off:off]
	}
	if endBytes > 0 {
		copy(b.payload[off:], end)
		off += endBytes
	}
	b.count++
	b.disableZeroSetCandidate()
	return startView, nil
}

func (b *RawKVBatchPayloadBuilder) appendRawKVPayloadSpace(needed int) (int, error) {
	off := len(b.payload)
	newLen, newCap, err := b.payloadGrowth(needed)
	if err != nil {
		return 0, err
	}
	if newLen > cap(b.payload) {
		next := make([]byte, newLen, newCap)
		copy(next, b.payload)
		b.payload = next
	} else {
		b.payload = b.payload[:newLen]
	}
	return off, nil
}

func (b *RawKVBatchPayloadBuilder) payloadGrowth(needed int) (newLen, newCap int, err error) {
	if b == nil {
		return 0, 0, ErrCorrupt
	}
	if needed > int(^uint32(0))-len(b.payload) || needed > int(^uint(0)>>1)-len(b.payload) {
		return 0, 0, ErrRecordTooLarge
	}
	newLen = len(b.payload) + needed
	newCap = cap(b.payload)
	if newLen > newCap {
		newCap *= 2
		if newCap < newLen {
			newCap = newLen
		}
		if b.payloadCapHint > newCap {
			newCap = b.payloadCapHint
		}
		if newCap < 0 {
			return 0, 0, ErrRecordTooLarge
		}
	}
	return newLen, newCap, nil
}

func (b *RawKVBatchPayloadBuilder) payloadGrowthFrom(payloadLen, payloadCap, needed int) (newLen, newCap int, err error) {
	if needed < 0 || payloadLen < 0 || payloadCap < 0 {
		return 0, 0, ErrRecordTooLarge
	}
	if needed > int(^uint32(0))-payloadLen || needed > int(^uint(0)>>1)-payloadLen {
		return 0, 0, ErrRecordTooLarge
	}
	newLen = payloadLen + needed
	newCap = payloadCap
	if newLen > newCap {
		newCap *= 2
		if newCap < newLen {
			newCap = newLen
		}
		if b.payloadCapHint > newCap {
			newCap = b.payloadCapHint
		}
		if newCap < 0 {
			return 0, 0, ErrRecordTooLarge
		}
	}
	return newLen, newCap, nil
}

func (b *RawKVBatchPayloadBuilder) payloadLenCapForAppend() (payloadLen, payloadCap int) {
	if b == nil {
		return 0, 0
	}
	payloadLen = len(b.payload)
	payloadCap = cap(b.payload)
	if payloadLen < rawKVBatchHeaderSize {
		payloadLen = rawKVBatchHeaderSize
	}
	if payloadCap < payloadLen {
		payloadCap = payloadLen
	}
	return payloadLen, payloadCap
}

func (b *RawKVBatchPayloadBuilder) retainedCapAfterCompactZeroMaterializeAppend(needed int) (int, error) {
	if b == nil {
		return 0, nil
	}
	if !b.zeroSetCompactOnly || b.count == 0 {
		return b.RetainedCapAfterAppend(needed)
	}
	if b.zeroSetValueLen <= 0 || len(b.zeroPayload) < rawKVZeroBatchHeaderSize {
		return 0, ErrCorrupt
	}
	expandedLen, ok := rawKVExpandedZeroBatchPayloadSize(b.count, b.zeroSetKeyBytes, b.zeroSetValueLen)
	if !ok || commandFrameIntExceedsUint32(expandedLen) {
		return 0, ErrRecordTooLarge
	}
	_, payloadCap := b.payloadLenCapForAppend()
	if payloadCap < expandedLen {
		payloadCap = expandedLen
	}
	_, newCap, err := b.payloadGrowthFrom(expandedLen, payloadCap, needed)
	if err != nil {
		return 0, err
	}
	return b.retainedPayloadBufferCap(newCap, cap(b.zeroPayload), cap(b.zeroSetValueView))
}

func (b *RawKVBatchPayloadBuilder) retainedCapAfterCompactZeroSetAppend(key, value []byte) (int, error) {
	if b == nil {
		return 0, nil
	}
	zeroLen := len(b.zeroPayload)
	zeroCap := cap(b.zeroPayload)
	if zeroLen == 0 {
		zeroLen = rawKVZeroBatchHeaderSize
	}
	version := b.zeroSetCompactVersion
	if version == 0 {
		version = rawKVZeroBatchPayloadV3
		if len(key) > int(^uint16(0)) {
			version = rawKVZeroBatchPayloadV2
		}
		capHint := rawKVZeroBatchHeaderSize
		if b.opHint > 0 {
			if hint, ok := rawKVZeroBatchPayloadSizeHint(b.opHint, len(key)); ok {
				capHint = hint
			}
		}
		if zeroCap < rawKVZeroBatchHeaderSize {
			zeroCap = capHint
		}
	}
	if version == rawKVZeroBatchPayloadV3 && len(key) > int(^uint16(0)) {
		promotedLen := rawKVZeroBatchHeaderSize
		if b.count > (int(^uint(0)>>1)-promotedLen)/rawKVZeroOpHeaderSize {
			return 0, ErrRecordTooLarge
		}
		promotedLen += b.count * rawKVZeroOpHeaderSize
		if b.zeroSetKeyBytes > int(^uint(0)>>1)-promotedLen {
			return 0, ErrRecordTooLarge
		}
		promotedLen += b.zeroSetKeyBytes
		if commandFrameIntExceedsUint32(promotedLen) {
			return 0, ErrRecordTooLarge
		}
		zeroLen = promotedLen
		zeroCap = promotedLen
		version = rawKVZeroBatchPayloadV2
	}
	opHeaderSize := rawKVZeroOpHeaderSizeForVersion(version)
	needed := opHeaderSize + len(key)
	if needed > int(^uint32(0))-zeroLen || needed > int(^uint(0)>>1)-zeroLen {
		return 0, ErrRecordTooLarge
	}
	zeroLen += needed
	if zeroLen > zeroCap {
		zeroCap *= 2
		if zeroCap < zeroLen {
			zeroCap = zeroLen
		}
		if zeroCap < 0 {
			return 0, ErrRecordTooLarge
		}
	}
	_, payloadCap := b.payloadLenCapForAppend()
	zeroSetValueViewCap := cap(b.zeroSetValueView)
	if zeroSetValueViewCap < len(value) {
		zeroSetValueViewCap = len(value)
	}
	return b.retainedPayloadBufferCap(payloadCap, zeroCap, zeroSetValueViewCap)
}

func (b *RawKVBatchPayloadBuilder) retainedPayloadBufferCap(caps ...int) (int, error) {
	total := 0
	maxInt := int(^uint(0) >> 1)
	for _, retained := range caps {
		if retained < 0 || retained > maxInt-total {
			return 0, ErrRecordTooLarge
		}
		total += retained
	}
	return total, nil
}

func (b *RawKVBatchPayloadBuilder) retainedRevisionOffsetBytesAfterAppend() (int, error) {
	if b == nil || !b.entryRevisions {
		return 0, nil
	}
	newLen := len(b.revisionOffsets) + 1
	newCap := cap(b.revisionOffsets)
	if newLen > newCap {
		newCap *= 2
		if newCap < newLen {
			newCap = newLen
		}
		if b.opHint > newCap {
			newCap = b.opHint
		}
		if newCap < rawKVRevisionOffsetInlineCap {
			newCap = rawKVRevisionOffsetInlineCap
		}
	}
	if newCap <= rawKVRevisionOffsetInlineCap {
		return 0, nil
	}
	maxInt := int(^uint(0) >> 1)
	if newCap > maxInt/4 {
		return 0, ErrRecordTooLarge
	}
	return newCap * 4, nil
}

func (b *RawKVBatchPayloadBuilder) appendValidatedWithRevision(op RawKVOp, key, value []byte, revision uint64) (keyView, valueView []byte, err error) {
	if b == nil {
		return nil, nil, ErrCorrupt
	}
	if b.payload == nil {
		_ = b.ResetWithHint(0, 0)
	}
	if commandFrameIntExceedsUint32(b.count + 1) {
		return nil, nil, ErrRecordTooLarge
	}
	if b.zeroSetCompactOnly && op == RawKVOpSet && b.canAppendCompactZeroSet(value) {
		keyView, err := b.appendCompactZeroSet(key, value)
		if err != nil {
			return nil, nil, err
		}
		return keyView, b.zeroSetValueViewForLen(len(value)), nil
	}
	if b.zeroSetCompactOnly {
		if err := b.materializeCompactZeroSetPayload(); err != nil {
			return nil, nil, err
		}
		b.zeroSetCompactOnly = false
	}
	headerSize := rawKVOperationHeaderSize(b.entryRevisions)
	off, err := b.appendRawKVPayloadSpace(headerSize + len(key) + len(value))
	if err != nil {
		return nil, nil, err
	}
	b.payload[off] = byte(op)
	binary.LittleEndian.PutUint32(b.payload[off+1:off+5], uint32(len(key)))
	binary.LittleEndian.PutUint32(b.payload[off+5:off+9], uint32(len(value)))
	if b.entryRevisions {
		binary.LittleEndian.PutUint64(b.payload[off+9:off+17], revision)
		if err := b.appendRevisionOffset(off); err != nil {
			return nil, nil, err
		}
	}
	off += headerSize
	keyStart := off
	copy(b.payload[off:], key)
	off += len(key)
	valueStart := off
	zeroCompact := false
	if op == RawKVOpSet {
		zeroCompact = b.recordZeroSetCandidateForAppended(key, value)
	} else {
		b.disableZeroSetCandidate()
	}
	if zeroCompact {
		b.zeroSetValuesOmitted = true
		valueView = b.zeroSetValueViewForLen(len(value))
	} else {
		copy(b.payload[off:], value)
		if op == RawKVOpSet {
			if allZeroBytes(value) {
				valueView = b.zeroSetValueViewForLen(len(value))
			} else {
				valueView = b.payload[valueStart : valueStart+len(value) : valueStart+len(value)]
				b.payloadValueViews = true
			}
		}
	}
	off += len(value)
	b.count++
	keyView = b.payload[keyStart : keyStart+len(key) : keyStart+len(key)]
	return keyView, valueView, nil
}

func (b *RawKVBatchPayloadBuilder) canAppendCompactZeroSet(value []byte) bool {
	if b == nil || !b.zeroSetCandidate || !b.zeroSetCompactOnly || len(value) == 0 {
		return false
	}
	if b.zeroSetValueLen >= 0 && b.zeroSetValueLen != len(value) {
		return false
	}
	if sameNonEmptyBytesData(value, b.zeroSetValueRef) {
		return true
	}
	return allZeroBytes(value)
}

func (b *RawKVBatchPayloadBuilder) appendCompactZeroSet(key, value []byte) ([]byte, error) {
	if b == nil || len(value) == 0 {
		return nil, ErrCorrupt
	}
	if commandFrameIntExceedsUint32(len(key)) || commandFrameIntExceedsUint32(b.count+1) {
		return nil, ErrRecordTooLarge
	}
	if len(b.zeroPayload) == 0 {
		b.zeroSetCompactVersion = rawKVZeroBatchPayloadV3
		if len(key) > int(^uint16(0)) {
			b.zeroSetCompactVersion = rawKVZeroBatchPayloadV2
		}
		capHint := rawKVZeroBatchHeaderSize
		if b.opHint > 0 {
			if hint, ok := rawKVZeroBatchPayloadSizeHint(b.opHint, len(key)); ok {
				capHint = hint
			}
		}
		if cap(b.zeroPayload) < rawKVZeroBatchHeaderSize {
			b.zeroPayload = make([]byte, rawKVZeroBatchHeaderSize, capHint)
		} else {
			b.zeroPayload = b.zeroPayload[:rawKVZeroBatchHeaderSize]
			clear(b.zeroPayload)
		}
	}
	if b.zeroSetCompactVersion == rawKVZeroBatchPayloadV3 && len(key) > int(^uint16(0)) {
		if err := b.promoteCompactZeroPayloadToV2(); err != nil {
			return nil, err
		}
	}
	opHeaderSize := rawKVZeroOpHeaderSizeForVersion(b.zeroSetCompactVersion)
	needed := opHeaderSize + len(key)
	if needed > int(^uint32(0))-len(b.zeroPayload) || needed > int(^uint(0)>>1)-len(b.zeroPayload) {
		return nil, ErrRecordTooLarge
	}
	off := len(b.zeroPayload)
	newLen := off + needed
	if newLen > cap(b.zeroPayload) {
		newCap := cap(b.zeroPayload) * 2
		if newCap < newLen {
			newCap = newLen
		}
		next := make([]byte, newLen, newCap)
		copy(next, b.zeroPayload)
		b.zeroPayload = next
	} else {
		b.zeroPayload = b.zeroPayload[:newLen]
	}
	if b.zeroSetValueLen < 0 {
		b.zeroSetValueLen = len(value)
	}
	if !sameNonEmptyBytesData(value, b.zeroSetValueRef) {
		b.zeroSetValueRef = value
	}
	if b.zeroSetCompactVersion == rawKVZeroBatchPayloadV3 {
		binary.LittleEndian.PutUint16(b.zeroPayload[off:off+rawKVZeroOpHeaderSizeV3], uint16(len(key)))
	} else {
		binary.LittleEndian.PutUint32(b.zeroPayload[off:off+rawKVZeroOpHeaderSize], uint32(len(key)))
	}
	keyStart := off + opHeaderSize
	copy(b.zeroPayload[keyStart:keyStart+len(key)], key)
	b.zeroSetKeyBytes += len(key)
	if len(key) > b.zeroSetMaxKeyLen {
		b.zeroSetMaxKeyLen = len(key)
	}
	b.count++
	return b.zeroPayload[keyStart : keyStart+len(key) : keyStart+len(key)], nil
}

func (b *RawKVBatchPayloadBuilder) materializeCompactZeroSetPayload() error {
	if b == nil || !b.zeroSetCompactOnly || b.count == 0 {
		return nil
	}
	if b.zeroSetValueLen <= 0 || len(b.zeroPayload) < rawKVZeroBatchHeaderSize {
		return ErrCorrupt
	}
	total, ok := rawKVExpandedZeroBatchPayloadSize(b.count, b.zeroSetKeyBytes, b.zeroSetValueLen)
	if !ok || commandFrameIntExceedsUint32(total) {
		return ErrRecordTooLarge
	}
	if cap(b.payload) < total {
		b.payload = make([]byte, total)
	} else {
		b.payload = b.payload[:total]
		clear(b.payload)
	}
	binary.LittleEndian.PutUint16(b.payload[0:2], rawKVBatchPayloadVersion)
	binary.LittleEndian.PutUint32(b.payload[2:6], uint32(b.count))
	version := b.zeroSetCompactVersion
	if version == 0 {
		version = binary.LittleEndian.Uint16(b.zeroPayload[0:2])
	}
	srcOff := rawKVZeroBatchHeaderSize
	dstOff := rawKVBatchHeaderSize
	for i := 0; i < b.count; i++ {
		keyLen, nextOff, err := readCompactZeroKeyLen(b.zeroPayload, srcOff, version)
		if err != nil || dstOff+rawKVOpHeaderSize > len(b.payload) {
			return ErrCorrupt
		}
		srcOff = nextOff
		if keyLen < 0 || keyLen > len(b.zeroPayload)-srcOff || dstOff+rawKVOpHeaderSize+keyLen+b.zeroSetValueLen > len(b.payload) {
			return ErrCorrupt
		}
		b.payload[dstOff] = byte(RawKVOpSet)
		binary.LittleEndian.PutUint32(b.payload[dstOff+1:dstOff+5], uint32(keyLen))
		binary.LittleEndian.PutUint32(b.payload[dstOff+5:dstOff+9], uint32(b.zeroSetValueLen))
		dstOff += rawKVOpHeaderSize
		copy(b.payload[dstOff:dstOff+keyLen], b.zeroPayload[srcOff:srcOff+keyLen])
		srcOff += keyLen
		dstOff += keyLen + b.zeroSetValueLen
	}
	if srcOff != len(b.zeroPayload) || dstOff != len(b.payload) {
		return ErrCorrupt
	}
	b.zeroSetValuesOmitted = false
	return nil
}

func rawKVZeroOpHeaderSizeForKeyLen(keyLen int) int {
	if keyLen >= 0 && keyLen <= int(^uint16(0)) {
		return rawKVZeroOpHeaderSizeV3
	}
	return rawKVZeroOpHeaderSize
}

func rawKVZeroOpHeaderSizeForVersion(version uint16) int {
	if version == rawKVZeroBatchPayloadV3 {
		return rawKVZeroOpHeaderSizeV3
	}
	return rawKVZeroOpHeaderSize
}

func readCompactZeroKeyLen(payload []byte, off int, version uint16) (int, int, error) {
	switch version {
	case rawKVZeroBatchPayloadV3:
		if off+rawKVZeroOpHeaderSizeV3 > len(payload) {
			return 0, off, ErrCorrupt
		}
		return int(binary.LittleEndian.Uint16(payload[off : off+rawKVZeroOpHeaderSizeV3])), off + rawKVZeroOpHeaderSizeV3, nil
	case rawKVZeroBatchPayloadV2:
		if off+rawKVZeroOpHeaderSize > len(payload) {
			return 0, off, ErrCorrupt
		}
		keyLen := binary.LittleEndian.Uint32(payload[off : off+rawKVZeroOpHeaderSize])
		if uint64(keyLen) > uint64(^uint(0)>>1) {
			return 0, off, ErrRecordTooLarge
		}
		return int(keyLen), off + rawKVZeroOpHeaderSize, nil
	default:
		return 0, off, ErrCommandWALUnsupportedVersion
	}
}

func (b *RawKVBatchPayloadBuilder) promoteCompactZeroPayloadToV2() error {
	if b == nil || b.zeroSetCompactVersion != rawKVZeroBatchPayloadV3 {
		return nil
	}
	total := rawKVZeroBatchHeaderSize + b.count*rawKVZeroOpHeaderSize + b.zeroSetKeyBytes
	if total < rawKVZeroBatchHeaderSize || commandFrameIntExceedsUint32(total) {
		return ErrRecordTooLarge
	}
	next := make([]byte, rawKVZeroBatchHeaderSize, total)
	copy(next, b.zeroPayload[:rawKVZeroBatchHeaderSize])
	binary.LittleEndian.PutUint16(next[0:2], rawKVZeroBatchPayloadV2)
	off := rawKVZeroBatchHeaderSize
	for i := 0; i < b.count; i++ {
		keyLen, nextOff, err := readCompactZeroKeyLen(b.zeroPayload, off, rawKVZeroBatchPayloadV3)
		if err != nil {
			return err
		}
		off = nextOff
		if keyLen > len(b.zeroPayload)-off {
			return ErrCorrupt
		}
		dstOff := len(next)
		next = next[:dstOff+rawKVZeroOpHeaderSize+keyLen]
		binary.LittleEndian.PutUint32(next[dstOff:dstOff+rawKVZeroOpHeaderSize], uint32(keyLen))
		copy(next[dstOff+rawKVZeroOpHeaderSize:], b.zeroPayload[off:off+keyLen])
		off += keyLen
	}
	if off != len(b.zeroPayload) {
		return ErrCorrupt
	}
	b.zeroPayload = next
	b.zeroSetCompactVersion = rawKVZeroBatchPayloadV2
	return nil
}

func rawKVZeroBatchPayloadSizeHint(opHint, firstKeyLen int) (int, bool) {
	if opHint <= 0 || firstKeyLen < 0 {
		return rawKVZeroBatchHeaderSize, true
	}
	maxInt := int(^uint(0) >> 1)
	perOp := rawKVZeroOpHeaderSizeForKeyLen(firstKeyLen) + firstKeyLen
	if perOp < rawKVZeroOpHeaderSizeV3 || perOp > (maxInt-rawKVZeroBatchHeaderSize)/opHint {
		return 0, false
	}
	total := rawKVZeroBatchHeaderSize + opHint*perOp
	if commandFrameIntExceedsUint32(total) {
		return 0, false
	}
	return total, true
}

func rawKVExpandedZeroBatchPayloadSize(count, keyBytes, valueLen int) (int, bool) {
	if count < 0 || keyBytes < 0 || valueLen <= 0 {
		return 0, false
	}
	maxInt := int(^uint(0) >> 1)
	total := rawKVBatchHeaderSize
	if count > (maxInt-total)/rawKVOpHeaderSize {
		return 0, false
	}
	total += count * rawKVOpHeaderSize
	if keyBytes > maxInt-total {
		return 0, false
	}
	total += keyBytes
	if count > (maxInt-total)/valueLen {
		return 0, false
	}
	total += count * valueLen
	return total, true
}

func (b *RawKVBatchPayloadBuilder) truncateCompactZeroSetPayload(count int) {
	if b == nil || count < 0 {
		return
	}
	if count == 0 {
		b.count = 0
		b.zeroSetCandidate = true
		b.zeroSetCompactOnly = true
		b.zeroSetValuesOmitted = false
		b.zeroSetValueLen = -1
		b.zeroSetKeyBytes = 0
		b.zeroSetMaxKeyLen = 0
		b.zeroSetCompactVersion = 0
		b.zeroSetValueRef = nil
		b.payloadValueViews = false
		if b.zeroPayload != nil {
			b.zeroPayload = b.zeroPayload[:0]
		}
		if len(b.payload) >= rawKVBatchHeaderSize {
			b.payload = b.payload[:rawKVBatchHeaderSize]
			clear(b.payload)
			binary.LittleEndian.PutUint16(b.payload[0:2], rawKVBatchPayloadVersion)
		}
		return
	}
	if count >= b.count {
		return
	}
	off := rawKVZeroBatchHeaderSize
	keyBytes := 0
	maxKeyLen := 0
	version := b.zeroSetCompactVersion
	if version == 0 && len(b.zeroPayload) >= 2 {
		version = binary.LittleEndian.Uint16(b.zeroPayload[0:2])
	}
	for i := 0; i < count; i++ {
		keyLen, nextOff, err := readCompactZeroKeyLen(b.zeroPayload, off, version)
		if err != nil {
			return
		}
		off = nextOff
		if keyLen < 0 || keyLen > len(b.zeroPayload)-off {
			return
		}
		off += keyLen
		keyBytes += keyLen
		if keyLen > maxKeyLen {
			maxKeyLen = keyLen
		}
	}
	b.zeroPayload = b.zeroPayload[:off]
	b.count = count
	b.zeroSetKeyBytes = keyBytes
	b.zeroSetMaxKeyLen = maxKeyLen
}

func (b *RawKVBatchPayloadBuilder) recordZeroSetCandidateForAppended(key, value []byte) bool {
	if b == nil || !b.zeroSetCandidate {
		return false
	}
	if len(value) == 0 {
		b.disableZeroSetCandidate()
		return false
	}
	if b.zeroSetValueLen < 0 {
		b.zeroSetValueLen = len(value)
	} else if b.zeroSetValueLen != len(value) {
		b.disableZeroSetCandidate()
		return false
	}
	if sameNonEmptyBytesData(value, b.zeroSetValueRef) {
		// Same immutable zero buffer as an earlier op in this batch.
	} else if allZeroBytes(value) {
		b.zeroSetValueRef = value
	} else {
		b.disableZeroSetCandidate()
		return false
	}
	b.zeroSetKeyBytes += len(key)
	if len(key) > b.zeroSetMaxKeyLen {
		b.zeroSetMaxKeyLen = len(key)
	}
	return true
}

func (b *RawKVBatchPayloadBuilder) disableZeroSetCandidate() {
	if b == nil || !b.zeroSetCandidate {
		return
	}
	b.zeroSetCandidate = false
	b.materializeOmittedZeroValues()
}

func (b *RawKVBatchPayloadBuilder) zeroSetValueViewForLen(n int) []byte {
	if n == 0 {
		return rawKVEmptyByteView
	}
	if b == nil || n < 0 {
		return nil
	}
	if n <= len(rawKVSharedZeroValueView) {
		b.zeroSetValueViewGlobal = true
		return rawKVSharedZeroValueView[:n:n]
	}
	if cap(b.zeroSetValueView) < n {
		b.zeroSetValueView = make([]byte, n)
	}
	return b.zeroSetValueView[:n:n]
}

func (b *RawKVBatchPayloadBuilder) materializeOmittedZeroValues() {
	if b == nil || !b.zeroSetValuesOmitted || b.count <= 0 || len(b.payload) < rawKVBatchHeaderSize {
		return
	}
	off := rawKVBatchHeaderSize
	for i := 0; i < b.count; i++ {
		if off+rawKVOpHeaderSize > len(b.payload) {
			break
		}
		op := RawKVOp(b.payload[off])
		keyLen := int(binary.LittleEndian.Uint32(b.payload[off+1 : off+5]))
		valueLen := int(binary.LittleEndian.Uint32(b.payload[off+5 : off+9]))
		off += rawKVOpHeaderSize
		if keyLen < 0 || keyLen > len(b.payload)-off {
			break
		}
		off += keyLen
		if valueLen < 0 || valueLen > len(b.payload)-off {
			break
		}
		if op == RawKVOpSet && valueLen > 0 {
			clear(b.payload[off : off+valueLen])
		}
		off += valueLen
	}
	b.zeroSetValuesOmitted = false
}

func (b *RawKVBatchPayloadBuilder) recomputeZeroSetCandidate() {
	if b == nil {
		return
	}
	b.materializeOmittedZeroValues()
	b.zeroSetCandidate = true
	b.zeroSetCompactOnly = false
	b.zeroSetValuesOmitted = false
	b.zeroSetValueLen = -1
	b.zeroSetKeyBytes = 0
	b.zeroSetMaxKeyLen = 0
	b.zeroSetCompactVersion = 0
	b.zeroSetValueRef = nil
	if b.count == 0 || len(b.payload) < rawKVBatchHeaderSize {
		return
	}
	_ = ScanRawKVBatchPayload(b.payload, func(op RawKVOp, key, value []byte) error {
		if op != RawKVOpSet {
			b.disableZeroSetCandidate()
			return nil
		}
		b.recordZeroSetCandidateForAppended(key, value)
		return nil
	})
}

func (b *RawKVBatchPayloadBuilder) compactZeroSetPayload() ([]byte, bool) {
	if b == nil || !b.zeroSetCandidate || b.count <= 0 || b.zeroSetValueLen <= 0 {
		return nil, false
	}
	version := rawKVZeroBatchPayloadV2
	if b.zeroSetMaxKeyLen <= int(^uint16(0)) {
		version = rawKVZeroBatchPayloadV3
	}
	opHeaderSize := rawKVZeroOpHeaderSizeForVersion(version)
	total := rawKVZeroBatchHeaderSize + b.count*opHeaderSize + b.zeroSetKeyBytes
	if total < rawKVZeroBatchHeaderSize || commandFrameIntExceedsUint32(total) {
		return nil, false
	}
	if cap(b.zeroPayload) < total {
		b.zeroPayload = make([]byte, total)
	} else {
		b.zeroPayload = b.zeroPayload[:total]
	}
	dst := b.zeroPayload
	b.zeroSetCompactVersion = version
	binary.LittleEndian.PutUint16(dst[0:2], version)
	binary.LittleEndian.PutUint32(dst[2:6], uint32(b.count))
	binary.LittleEndian.PutUint32(dst[6:10], uint32(b.zeroSetValueLen))
	srcOff := rawKVBatchHeaderSize
	dstOff := rawKVZeroBatchHeaderSize
	for i := 0; i < b.count; i++ {
		if srcOff+rawKVOpHeaderSize > len(b.payload) || dstOff+opHeaderSize > len(dst) {
			return nil, false
		}
		op := RawKVOp(b.payload[srcOff])
		keyLen := int(binary.LittleEndian.Uint32(b.payload[srcOff+1 : srcOff+5]))
		valueLen := int(binary.LittleEndian.Uint32(b.payload[srcOff+5 : srcOff+9]))
		srcOff += rawKVOpHeaderSize
		if op != RawKVOpSet || valueLen != b.zeroSetValueLen || keyLen < 0 || keyLen > len(b.payload)-srcOff || valueLen > len(b.payload)-srcOff-keyLen {
			return nil, false
		}
		if version == rawKVZeroBatchPayloadV3 {
			if keyLen > int(^uint16(0)) {
				return nil, false
			}
			binary.LittleEndian.PutUint16(dst[dstOff:dstOff+rawKVZeroOpHeaderSizeV3], uint16(keyLen))
		} else {
			binary.LittleEndian.PutUint32(dst[dstOff:dstOff+rawKVZeroOpHeaderSize], uint32(keyLen))
		}
		dstOff += opHeaderSize
		copy(dst[dstOff:], b.payload[srcOff:srcOff+keyLen])
		srcOff += keyLen + valueLen
		dstOff += keyLen
	}
	if srcOff != len(b.payload) || dstOff != len(dst) {
		return nil, false
	}
	return dst, true
}

type CollectionDocument struct {
	ID       []byte
	Document []byte
}

type CollectionInsertBatchByIDPayload struct {
	Collection string
	Documents  []CollectionDocument
}

type CollectionDeleteBatchByIDPayload struct {
	Collection string
	IDs        [][]byte
}

type CollectionUpdateBatchByIDPayload struct {
	Collection string
	Documents  []CollectionDocument
}

type CollectionReplaceSourceByIDPayload struct {
	Collection string
	DeleteIDs  [][]byte
	Documents  []CollectionDocument
}

type CollectionRebuildVectorIndexPayload struct {
	Collection string
	IndexName  string
}

type CatalogCreateCollectionPayload struct {
	Collection string
	Metadata   []byte
}

type ExternalRefClass uint16

const (
	ExternalRefValueLog ExternalRefClass = iota + 1
	ExternalRefLeafLog
	ExternalRefPayloadFile
)

type ExternalRef struct {
	Class  ExternalRefClass
	Flags  uint16
	FileID uint64
	Offset uint64
	Length uint64
	Digest [32]byte
	Path   []byte
}

type CommandExtension struct {
	Type    uint16
	Payload []byte
}

type CommandEnvelope struct {
	Version          uint16
	DurabilityClass  CommandDurabilityClass
	LSN              uint64
	Kind             CommandKind
	Scope            CommandScope
	FeatureFlags     uint64
	CatalogEpoch     uint64
	SchemaEpoch      uint64
	BaseAppliedLSN   uint64
	PayloadFormat    PayloadFormat
	Payload          []byte
	ExternalRefs     []ExternalRef
	Preconditions    []CommandExtension
	ResultAssertions []CommandExtension
}

func EncodeCommandFrame(env CommandEnvelope) ([]byte, error) {
	return encodeCommandFrameTo(nil, env)
}

func encodeRawKVSingleCommandFrameTo(dst []byte, lsn, baseAppliedLSN uint64, op RawKVOperation) ([]byte, error) {
	if lsn == 0 {
		return nil, fmt.Errorf("%w: zero lsn", ErrCorrupt)
	}
	if err := validateRawKVOperation(&op); err != nil {
		return nil, err
	}
	if op.Op == RawKVOpSetMaterializedRID {
		return nil, ErrCommandWALUnsupportedVersion
	}
	if op.Op == RawKVOpDeleteRange {
		payload, err := EncodeRawKVSingleOperationPayload(op)
		if err != nil {
			return nil, err
		}
		return encodeCommandFrameTo(dst, CommandEnvelope{
			LSN:            lsn,
			Kind:           CommandKindRawKVBatch,
			Scope:          CommandScopeRawKV,
			BaseAppliedLSN: baseAppliedLSN,
			PayloadFormat:  PayloadFormatRawKVBatchV1,
			Payload:        payload,
		})
	}
	valueLen := len(op.Value)
	if op.Op == RawKVOpSetRID {
		valueLen = 8
	}
	if commandFrameIntExceedsUint32(len(op.Key)) || commandFrameIntExceedsUint32(valueLen) {
		return nil, ErrRecordTooLarge
	}
	payloadLen := rawKVBatchHeaderSize + rawKVOpHeaderSize + len(op.Key) + valueLen
	total, err := commandFrameEncodedSizeFromLengths(payloadLen, 0, 0, 0)
	if err != nil {
		return nil, err
	}
	if cap(dst) < total {
		dst = make([]byte, total)
	} else {
		dst = dst[:total]
	}
	frame := dst
	copy(frame[:commandFrameHeaderSize], rawKVCommandFrameHeaderTemplate[:])
	binary.LittleEndian.PutUint64(frame[20:28], lsn)
	binary.LittleEndian.PutUint64(frame[44:52], baseAppliedLSN)
	binary.LittleEndian.PutUint32(frame[56:60], uint32(payloadLen))

	off := commandFrameHeaderSize
	binary.LittleEndian.PutUint16(frame[off:off+2], 1)
	binary.LittleEndian.PutUint32(frame[off+2:off+6], 1)
	off += rawKVBatchHeaderSize
	value := op.Value
	var ridBuf [8]byte
	if op.Op == RawKVOpSetRID {
		binary.LittleEndian.PutUint64(ridBuf[:], op.RID)
		value = ridBuf[:]
	}
	frame[off] = byte(op.Op)
	binary.LittleEndian.PutUint32(frame[off+1:off+5], uint32(len(op.Key)))
	binary.LittleEndian.PutUint32(frame[off+5:off+9], uint32(len(value)))
	off += rawKVOpHeaderSize
	copy(frame[off:], op.Key)
	off += len(op.Key)
	copy(frame[off:], value)
	return frame, nil
}

func encodeTrustedRawKVPointCommandFrameTo(dst []byte, lsn, baseAppliedLSN uint64, op RawKVOp, key, value []byte) ([]byte, error) {
	return encodeTrustedRawKVPointCommandFrameWithRevisionTo(dst, lsn, baseAppliedLSN, op, key, value, 0)
}

func encodeTrustedRawKVPointCommandFrameWithRevisionTo(dst []byte, lsn, baseAppliedLSN uint64, op RawKVOp, key, value []byte, revision uint64) ([]byte, error) {
	if lsn == 0 {
		return nil, fmt.Errorf("%w: zero lsn", ErrCorrupt)
	}
	valueLen, payloadLen, total, err := trustedRawKVPointCommandFrameLens(op, key, value, revision)
	if err != nil {
		return nil, err
	}
	return encodeTrustedRawKVPointCommandFrameWithRevisionSizedTo(dst, lsn, baseAppliedLSN, op, key, value, revision, valueLen, payloadLen, total)
}

func trustedRawKVPointCommandFrameLens(op RawKVOp, key, value []byte, revision uint64) (valueLen, payloadLen, total int, err error) {
	if op != RawKVOpSet && op != RawKVOpDelete {
		return 0, 0, 0, fmt.Errorf("commitlog: unsupported trusted raw kv point op %d", op)
	}
	valueLen = len(value)
	if op == RawKVOpDelete {
		valueLen = 0
	}
	if commandFrameIntExceedsUint32(len(key)) || commandFrameIntExceedsUint32(valueLen) {
		return 0, 0, 0, ErrRecordTooLarge
	}
	opHeaderSize := rawKVOpHeaderSize
	if revision != 0 {
		opHeaderSize = rawKVOpRevisionHeaderSize
	}
	payloadLen = rawKVBatchHeaderSize + opHeaderSize + len(key) + valueLen
	total, err = commandFrameEncodedSizeFromLengths(payloadLen, 0, 0, 0)
	if err != nil {
		return 0, 0, 0, err
	}
	return valueLen, payloadLen, total, nil
}

func encodeTrustedRawKVPointCommandFrameSizedTo(dst []byte, lsn, baseAppliedLSN uint64, op RawKVOp, key, value []byte, valueLen, payloadLen, total int) ([]byte, error) {
	return encodeTrustedRawKVPointCommandFrameWithRevisionSizedTo(dst, lsn, baseAppliedLSN, op, key, value, 0, valueLen, payloadLen, total)
}

func encodeTrustedRawKVPointCommandFrameWithRevisionSizedTo(dst []byte, lsn, baseAppliedLSN uint64, op RawKVOp, key, value []byte, revision uint64, valueLen, payloadLen, total int) ([]byte, error) {
	return encodeTrustedRawKVPointCommandFramePayloadSizedWithRevisionTo(dst, lsn, baseAppliedLSN, op, key, value, revision, valueLen, payloadLen, total), nil
}

func encodeTrustedRawKVPointCommandFramePayloadSizedTo(dst []byte, lsn, baseAppliedLSN uint64, op RawKVOp, key, value []byte, valueLen, payloadLen, total int) []byte {
	return encodeTrustedRawKVPointCommandFramePayloadSizedWithRevisionTo(dst, lsn, baseAppliedLSN, op, key, value, 0, valueLen, payloadLen, total)
}

func encodeTrustedRawKVPointCommandFramePayloadSizedWithRevisionTo(dst []byte, lsn, baseAppliedLSN uint64, op RawKVOp, key, value []byte, revision uint64, valueLen, payloadLen, total int) []byte {
	if cap(dst) < total {
		dst = make([]byte, total)
	} else {
		dst = dst[:total]
	}
	frame := dst
	copy(frame[:commandFrameHeaderSize], rawKVCommandFrameHeaderTemplate[:])
	binary.LittleEndian.PutUint64(frame[20:28], lsn)
	binary.LittleEndian.PutUint64(frame[44:52], baseAppliedLSN)
	binary.LittleEndian.PutUint32(frame[56:60], uint32(payloadLen))

	off := commandFrameHeaderSize
	payloadVersion := rawKVBatchPayloadVersion
	opHeaderSize := rawKVOpHeaderSize
	if revision != 0 {
		payloadVersion = rawKVBatchPayloadVersionWithRevisions
		opHeaderSize = rawKVOpRevisionHeaderSize
	}
	binary.LittleEndian.PutUint16(frame[off:off+2], payloadVersion)
	binary.LittleEndian.PutUint32(frame[off+2:off+6], 1)
	off += rawKVBatchHeaderSize
	frame[off] = byte(op)
	binary.LittleEndian.PutUint32(frame[off+1:off+5], uint32(len(key)))
	binary.LittleEndian.PutUint32(frame[off+5:off+9], uint32(valueLen))
	if revision != 0 {
		binary.LittleEndian.PutUint64(frame[off+9:off+17], revision)
	}
	off += opHeaderSize
	copy(frame[off:], key)
	off += len(key)
	if op == RawKVOpSet {
		copy(frame[off:], value)
		off += valueLen
	}
	return frame
}

func commandFrameEncodedSize(env CommandEnvelope) (int, error) {
	payloadLen := len(env.Payload)
	if env.Kind == CommandKindRawKVBatch && env.Payload == nil {
		payloadLen = rawKVBatchHeaderSize
	}
	extRefsLen, err := externalRefsEncodedLen(env.ExternalRefs)
	if err != nil {
		return 0, err
	}
	preconditionsLen, err := commandExtensionsEncodedLen(env.Preconditions)
	if err != nil {
		return 0, err
	}
	assertionsLen, err := commandExtensionsEncodedLen(env.ResultAssertions)
	if err != nil {
		return 0, err
	}
	return commandFrameEncodedSizeFromLengths(payloadLen, extRefsLen, preconditionsLen, assertionsLen)
}

func commandFrameEncodedSizeFromLengths(payloadLen, extRefsLen, preconditionsLen, assertionsLen int) (int, error) {
	total := commandFrameHeaderSize
	var err error
	if total, err = addCommandFrameEncodedSectionLen(total, payloadLen); err != nil {
		return 0, err
	}
	if total, err = addCommandFrameEncodedSectionLen(total, extRefsLen); err != nil {
		return 0, err
	}
	if total, err = addCommandFrameEncodedSectionLen(total, preconditionsLen); err != nil {
		return 0, err
	}
	if total, err = addCommandFrameEncodedSectionLen(total, assertionsLen); err != nil {
		return 0, err
	}
	return total, nil
}

func addCommandFrameEncodedSectionLen(total, n int) (int, error) {
	if commandFrameIntExceedsUint32(n) {
		return 0, ErrRecordTooLarge
	}
	if total > int(^uint(0)>>1)-n {
		return 0, ErrRecordTooLarge
	}
	return total + n, nil
}

func encodeCommandFrameTo(dst []byte, env CommandEnvelope) ([]byte, error) {
	if env.Version == 0 {
		env.Version = CommandFrameVersion
	}
	if env.Version != CommandFrameVersion {
		return nil, ErrCommandWALUnsupportedVersion
	}
	if env.Kind == CommandKindRawKVBatch && env.Payload == nil {
		payload, err := EncodeRawKVBatchPayload(nil)
		if err != nil {
			return nil, err
		}
		env.Payload = payload
	}
	if err := validateCommandEnvelopeForEncode(env); err != nil {
		return nil, err
	}
	extRefs, err := encodeExternalRefs(env.ExternalRefs)
	if err != nil {
		return nil, err
	}
	preconditions, err := encodeCommandExtensions(env.Preconditions)
	if err != nil {
		return nil, err
	}
	assertions, err := encodeCommandExtensions(env.ResultAssertions)
	if err != nil {
		return nil, err
	}
	total, err := commandFrameEncodedSizeFromLengths(len(env.Payload), len(extRefs), len(preconditions), len(assertions))
	if err != nil {
		return nil, err
	}
	if cap(dst) < total {
		dst = make([]byte, total)
	} else {
		dst = dst[:total]
	}
	frame := dst
	copy(frame[0:4], commandFrameMagic[:])
	binary.LittleEndian.PutUint16(frame[4:6], env.Version)
	binary.LittleEndian.PutUint16(frame[6:8], CommandFrameVersion)
	binary.LittleEndian.PutUint16(frame[8:10], uint16(env.Kind))
	binary.LittleEndian.PutUint16(frame[10:12], uint16(env.Scope))
	binary.LittleEndian.PutUint64(frame[12:20], env.FeatureFlags)
	binary.LittleEndian.PutUint64(frame[20:28], env.LSN)
	binary.LittleEndian.PutUint64(frame[28:36], env.CatalogEpoch)
	binary.LittleEndian.PutUint64(frame[36:44], env.SchemaEpoch)
	binary.LittleEndian.PutUint64(frame[44:52], env.BaseAppliedLSN)
	binary.LittleEndian.PutUint16(frame[52:54], uint16(env.PayloadFormat))
	binary.LittleEndian.PutUint16(frame[54:56], 0)
	binary.LittleEndian.PutUint32(frame[56:60], uint32(len(env.Payload)))
	binary.LittleEndian.PutUint32(frame[60:64], uint32(len(extRefs)))
	binary.LittleEndian.PutUint32(frame[64:68], uint32(len(preconditions)))
	binary.LittleEndian.PutUint32(frame[68:72], uint32(len(assertions)))
	off := commandFrameHeaderSize
	copy(frame[off:], env.Payload)
	off += len(env.Payload)
	copy(frame[off:], extRefs)
	off += len(extRefs)
	copy(frame[off:], preconditions)
	off += len(preconditions)
	copy(frame[off:], assertions)
	return frame, nil
}

func DecodeCommandFrame(frame []byte) (CommandEnvelope, error) {
	return decodeCommandFrame(frame, false)
}

func decodeCommandFrame(frame []byte, borrowPayload bool) (CommandEnvelope, error) {
	var env CommandEnvelope
	if len(frame) >= batchHeaderSize && len(frame) < commandFrameHeaderSize && isBatchPayloadVersion(frame[0]) {
		return env, ErrCommandWALLegacyPayload
	}
	if len(frame) < commandFrameHeaderSize {
		return env, ErrCorrupt
	}
	if !bytes.Equal(frame[0:4], commandFrameMagic[:]) {
		if isBatchPayloadVersion(frame[0]) {
			return env, ErrCommandWALLegacyPayload
		}
		return env, ErrCorrupt
	}
	version := binary.LittleEndian.Uint16(frame[4:6])
	if version == CommandFrameVersionV2 {
		return decodeCommandFrameV2(frame, borrowPayload)
	}
	minReader := binary.LittleEndian.Uint16(frame[6:8])
	if version != CommandFrameVersion || minReader > CommandFrameVersion {
		return env, ErrCommandWALUnsupportedVersion
	}
	env.Version = version
	env.Kind = CommandKind(binary.LittleEndian.Uint16(frame[8:10]))
	env.Scope = CommandScope(binary.LittleEndian.Uint16(frame[10:12]))
	env.FeatureFlags = binary.LittleEndian.Uint64(frame[12:20])
	if env.FeatureFlags&commandWALCriticalFlagsMask != 0 {
		return env, ErrCommandWALUnsupportedCriticalFlag
	}
	env.LSN = binary.LittleEndian.Uint64(frame[20:28])
	env.CatalogEpoch = binary.LittleEndian.Uint64(frame[28:36])
	env.SchemaEpoch = binary.LittleEndian.Uint64(frame[36:44])
	env.BaseAppliedLSN = binary.LittleEndian.Uint64(frame[44:52])
	env.PayloadFormat = PayloadFormat(binary.LittleEndian.Uint16(frame[52:54]))
	if binary.LittleEndian.Uint16(frame[54:56]) != 0 {
		return env, ErrCorrupt
	}
	payloadLen := binary.LittleEndian.Uint32(frame[56:60])
	extRefsLen := binary.LittleEndian.Uint32(frame[60:64])
	preconditionsLen := binary.LittleEndian.Uint32(frame[64:68])
	assertionsLen := binary.LittleEndian.Uint32(frame[68:72])
	if err := validateCommandEnvelopeIdentity(env); err != nil {
		return env, err
	}
	total := uint64(commandFrameHeaderSize) + uint64(payloadLen) + uint64(extRefsLen) + uint64(preconditionsLen) + uint64(assertionsLen)
	if total > uint64(len(frame)) || total > uint64(^uint(0)>>1) || int(total) != len(frame) {
		return env, ErrCorrupt
	}
	off := commandFrameHeaderSize
	env.Payload = frame[off : off+int(payloadLen)]
	if !borrowPayload {
		env.Payload = append([]byte(nil), env.Payload...)
	}
	off += int(payloadLen)
	extRefs, err := decodeExternalRefs(frame[off : off+int(extRefsLen)])
	if err != nil {
		return env, err
	}
	env.ExternalRefs = extRefs
	if err := validateExternalRefs(env.ExternalRefs); err != nil {
		return env, err
	}
	off += int(extRefsLen)
	preconditions, err := decodeCommandExtensions(frame[off : off+int(preconditionsLen)])
	if err != nil {
		return env, err
	}
	env.Preconditions = preconditions
	off += int(preconditionsLen)
	assertions, err := decodeCommandExtensions(frame[off : off+int(assertionsLen)])
	if err != nil {
		return env, err
	}
	env.ResultAssertions = assertions
	if err := validateCommandEnvelopePayload(env); err != nil {
		return env, err
	}
	return env, nil
}

func validateCommandEnvelopeForEncode(env CommandEnvelope) error {
	if env.Version != 0 && env.Version != CommandFrameVersion {
		return ErrCommandWALUnsupportedVersion
	}
	if env.FeatureFlags&commandWALCriticalFlagsMask != 0 {
		return ErrCommandWALUnsupportedCriticalFlag
	}
	if err := validateCommandEnvelopeIdentity(env); err != nil {
		return err
	}
	if err := validateExternalRefs(env.ExternalRefs); err != nil {
		return err
	}
	return validateCommandEnvelopePayload(env)
}

// validateExternalRefs keeps the typed external-reference wire section
// fail-closed because no complete producer, pin, sync, recovery, and
// deletion-owner closure exists for it. RawKV SetRID references use the active
// V2 ExternalRefFenceV1 contract and are intentionally separate.
func validateExternalRefs(refs []ExternalRef) error {
	if len(refs) != 0 {
		return ErrCommandWALUnsupportedExternalRef
	}
	return nil
}

func validateCommandEnvelopeIdentity(env CommandEnvelope) error {
	if env.LSN == 0 {
		return fmt.Errorf("%w: zero lsn", ErrCorrupt)
	}
	switch env.Kind {
	case CommandKindRawKVBatch:
		if env.Scope != CommandScopeRawKV ||
			(env.PayloadFormat != PayloadFormatRawKVBatchV1 && env.PayloadFormat != PayloadFormatRawKVBatchV2) {
			return ErrCorrupt
		}
		// RawKVBatchV2 is coupled to the V2 frame's persisted durability
		// class and external-reference fence. Never allow a legacy V1 frame
		// (including a zero-version caller that defaults to V1) to carry the
		// V2 payload identity and bypass those checks.
		if env.PayloadFormat == PayloadFormatRawKVBatchV2 && env.Version != CommandFrameVersionV2 {
			return ErrCommandWALUnsupportedVersion
		}
	case CommandKindCollectionInsertBatchByID:
		if env.Scope != CommandScopeCollection || (env.PayloadFormat != PayloadFormatCollectionInsertBatchByIDV1 && env.PayloadFormat != PayloadFormatCollectionTypedBatchByIDV1) {
			return ErrCorrupt
		}
	case CommandKindCollectionDeleteBatchByID:
		if env.Scope != CommandScopeCollection || env.PayloadFormat != PayloadFormatCollectionDeleteBatchByIDV1 {
			return ErrCorrupt
		}
	case CommandKindCollectionUpdateBatchByID:
		if env.Scope != CommandScopeCollection || (env.PayloadFormat != PayloadFormatCollectionUpdateBatchByIDV1 && env.PayloadFormat != PayloadFormatCollectionTypedBatchByIDV1) {
			return ErrCorrupt
		}
	case CommandKindCollectionReplaceSourceByID:
		if env.Scope != CommandScopeCollection || env.PayloadFormat != PayloadFormatCollectionReplaceSourceByIDV1 {
			return ErrCorrupt
		}
	case CommandKindCollectionRebuildVectorIndex:
		if env.Scope != CommandScopeCollection || env.PayloadFormat != PayloadFormatCollectionRebuildVectorIndexV1 {
			return ErrCorrupt
		}
	case CommandKindCatalogCreateCollection:
		if env.Scope != CommandScopeCatalog || env.PayloadFormat != PayloadFormatCatalogCreateCollectionV1 {
			return ErrCorrupt
		}
	default:
		return ErrCommandWALUnsupportedKind
	}
	return nil
}

func validateCommandEnvelopePayload(env CommandEnvelope) error {
	switch env.Kind {
	case CommandKindRawKVBatch:
		return validateRawKVBatchPayloadForFormat(env.PayloadFormat, env.Payload)
	case CommandKindCollectionInsertBatchByID:
		if env.PayloadFormat == PayloadFormatCollectionTypedBatchByIDV1 {
			return validateCollectionTypedBatchPayload(env.Payload)
		}
		return validateCollectionInsertBatchByIDPayload(env.Payload)
	case CommandKindCollectionDeleteBatchByID:
		return validateCollectionDeleteBatchByIDPayload(env.Payload)
	case CommandKindCollectionUpdateBatchByID:
		if env.PayloadFormat == PayloadFormatCollectionTypedBatchByIDV1 {
			if len(env.Payload) < collectionTypedBatchPrefixSize || env.Payload[collectionTypedBatchFlagsOffset] != 0 {
				return ErrCorrupt
			}
			return validateCollectionTypedBatchPayload(env.Payload)
		}
		return validateCollectionInsertBatchByIDPayload(env.Payload)
	case CommandKindCollectionReplaceSourceByID:
		_, err := DecodeCollectionReplaceSourceByIDPayload(env.Payload)
		return err
	case CommandKindCollectionRebuildVectorIndex:
		return validateCollectionRebuildVectorIndexPayload(env.Payload)
	case CommandKindCatalogCreateCollection:
		return validateCatalogCreateCollectionPayload(env.Payload)
	default:
		return nil
	}
}

func EncodeRawKVBatchPayload(ops []RawKVOperation) ([]byte, error) {
	scan := func(emit func(RawKVOperation) error) error {
		for i := range ops {
			if err := emit(ops[i]); err != nil {
				return err
			}
		}
		return nil
	}
	plan, err := PlanRawKVBatchPayloadScan(scan)
	if err != nil {
		return nil, err
	}
	return encodeRawKVBatchPayloadPlannedTo(nil, plan, scan)
}

// PlanRawKVBatchPayloadScan validates a replayable operation source and returns
// the exact RawKVBatch payload length needed to encode it.
func PlanRawKVBatchPayloadScan(scan RawKVBatchOperationScanner) (RawKVBatchPayloadPlan, error) {
	plan := RawKVBatchPayloadPlan{PayloadLen: rawKVBatchHeaderSize}
	if scan == nil {
		return plan, nil
	}
	if err := scan(func(op RawKVOperation) error {
		keyBytes, valueBytes, err := rawKVOperationPayloadLens(&op)
		if err != nil {
			return err
		}
		if commandFrameIntExceedsUint32(plan.Count + 1) {
			return ErrRecordTooLarge
		}
		if op.Revision != 0 && !plan.EntryRevisions {
			if plan.Count > (int(^uint(0)>>1)-plan.PayloadLen)/8 {
				return ErrRecordTooLarge
			}
			plan.PayloadLen += plan.Count * 8
			plan.EntryRevisions = true
			if commandFrameIntExceedsUint32(plan.PayloadLen) {
				return ErrRecordTooLarge
			}
		}
		opHeaderSize := rawKVOperationHeaderSize(plan.EntryRevisions)
		needed := opHeaderSize + keyBytes + valueBytes
		if needed < opHeaderSize || plan.PayloadLen > int(^uint(0)>>1)-needed {
			return ErrRecordTooLarge
		}
		plan.Count++
		plan.PayloadLen += needed
		if commandFrameIntExceedsUint32(plan.PayloadLen) {
			return ErrRecordTooLarge
		}
		return nil
	}); err != nil {
		return RawKVBatchPayloadPlan{}, err
	}
	return plan, nil
}

func rawKVOperationHeaderSize(entryRevisions bool) int {
	if entryRevisions {
		return rawKVOpRevisionHeaderSize
	}
	return rawKVOpHeaderSize
}

func rawKVOperationPayloadLens(op *RawKVOperation) (keyBytes, valueBytes int, err error) {
	if err := validateRawKVOperation(op); err != nil {
		return 0, 0, err
	}
	keyBytes = len(op.Key)
	valueBytes = len(op.Value)
	if op.Op == RawKVOpSetRID {
		valueBytes = 8
	} else if op.Op == RawKVOpSetMaterializedRID {
		if len(op.Value) > int(^uint(0)>>1)-8 {
			return 0, 0, ErrRecordTooLarge
		}
		valueBytes = 8 + len(op.Value)
	} else if op.Op == RawKVOpDeleteRange {
		_, startBytes, err := rawKVRangeBoundEncodedLen(op.Key)
		if err != nil {
			return 0, 0, err
		}
		_, endBytes, err := rawKVRangeBoundEncodedLen(op.Value)
		if err != nil {
			return 0, 0, err
		}
		keyBytes = startBytes
		valueBytes = endBytes
	}
	if commandFrameIntExceedsUint32(keyBytes) || commandFrameIntExceedsUint32(valueBytes) {
		return 0, 0, ErrRecordTooLarge
	}
	return keyBytes, valueBytes, nil
}

func (plan RawKVBatchPayloadPlan) validate() error {
	if plan.Count < 0 || plan.PayloadLen < rawKVBatchHeaderSize {
		return ErrCorrupt
	}
	if commandFrameIntExceedsUint32(plan.Count) || commandFrameIntExceedsUint32(plan.PayloadLen) {
		return ErrRecordTooLarge
	}
	if plan.Count == 0 && plan.PayloadLen != rawKVBatchHeaderSize {
		return ErrCorrupt
	}
	if plan.Count > 0 && plan.PayloadLen == rawKVBatchHeaderSize {
		return ErrCorrupt
	}
	return nil
}

func encodeRawKVBatchPayloadPlannedTo(dst []byte, plan RawKVBatchPayloadPlan, scan RawKVBatchOperationScanner) ([]byte, error) {
	if err := plan.validate(); err != nil {
		return nil, err
	}
	if cap(dst) < plan.PayloadLen {
		dst = make([]byte, plan.PayloadLen)
	} else {
		dst = dst[:plan.PayloadLen]
	}
	if n, err := writeRawKVBatchPayloadTo(dst, plan, scan); err != nil {
		return nil, err
	} else if n != plan.PayloadLen {
		return nil, ErrCorrupt
	}
	return dst, nil
}

// EncodeRawKVBatchPayloadPlanned encodes a caller-planned RawKVBatch scan. It is
// for callers that already validated the scan with PlanRawKVBatchPayloadScan and
// need the exact planned format, including revision-bearing payloads.
func EncodeRawKVBatchPayloadPlanned(plan RawKVBatchPayloadPlan, scan RawKVBatchOperationScanner) ([]byte, error) {
	return encodeRawKVBatchPayloadPlannedTo(nil, plan, scan)
}

func writeRawKVBatchPayloadPieces(plan RawKVBatchPayloadPlan, scan RawKVBatchOperationScanner, write func([]byte) error) error {
	if err := plan.validate(); err != nil {
		return err
	}
	if write == nil {
		return ErrCorrupt
	}
	var header [rawKVBatchHeaderSize]byte
	payloadVersion := rawKVBatchPayloadVersion
	if plan.EntryRevisions {
		payloadVersion = rawKVBatchPayloadVersionWithRevisions
	}
	binary.LittleEndian.PutUint16(header[0:2], payloadVersion)
	binary.LittleEndian.PutUint32(header[2:6], uint32(plan.Count))
	if err := write(header[:]); err != nil {
		return err
	}
	if scan == nil {
		if plan.Count != 0 {
			return ErrCorrupt
		}
		return nil
	}
	count := 0
	written := rawKVBatchHeaderSize
	if err := scan(func(op RawKVOperation) error {
		n, err := writeRawKVOperationPayloadPiecesWithRevisionFlag(op, plan.EntryRevisions, write)
		if err != nil {
			return err
		}
		count++
		written += n
		return nil
	}); err != nil {
		return err
	}
	if count != plan.Count || written != plan.PayloadLen {
		return ErrCorrupt
	}
	return nil
}

func writeRawKVBatchPayloadTo(dst []byte, plan RawKVBatchPayloadPlan, scan RawKVBatchOperationScanner) (int, error) {
	if err := plan.validate(); err != nil {
		return 0, err
	}
	if len(dst) < plan.PayloadLen {
		return 0, ErrCorrupt
	}
	payloadVersion := rawKVBatchPayloadVersion
	if plan.EntryRevisions {
		payloadVersion = rawKVBatchPayloadVersionWithRevisions
	}
	binary.LittleEndian.PutUint16(dst[0:2], payloadVersion)
	binary.LittleEndian.PutUint32(dst[2:6], uint32(plan.Count))
	if scan == nil {
		if plan.Count != 0 {
			return 0, ErrCorrupt
		}
		return rawKVBatchHeaderSize, nil
	}
	count := 0
	written := rawKVBatchHeaderSize
	if err := scan(func(op RawKVOperation) error {
		n, err := writeRawKVOperationPayloadTo(dst[written:], op, plan.EntryRevisions)
		if err != nil {
			return err
		}
		count++
		written += n
		return nil
	}); err != nil {
		return 0, err
	}
	if count != plan.Count || written != plan.PayloadLen {
		return 0, ErrCorrupt
	}
	return written, nil
}

func writeRawKVOperationPayloadPieces(op RawKVOperation, write func([]byte) error) (int, error) {
	return writeRawKVOperationPayloadPiecesWithRevisionFlag(op, false, write)
}

func writeRawKVOperationPayloadTo(dst []byte, op RawKVOperation, entryRevisions bool) (int, error) {
	keyBytes, valueBytes, err := rawKVOperationPayloadLens(&op)
	if err != nil {
		return 0, err
	}
	if op.Revision != 0 && !entryRevisions {
		return 0, ErrCorrupt
	}
	keyLen := uint32(len(op.Key))
	valueLen := uint32(len(op.Value))
	if op.Op == RawKVOpSetRID {
		valueLen = 8
	} else if op.Op == RawKVOpSetMaterializedRID {
		valueLen = uint32(valueBytes)
	} else if op.Op == RawKVOpDeleteRange {
		keyLen, _, err = rawKVRangeBoundEncodedLen(op.Key)
		if err != nil {
			return 0, err
		}
		valueLen, _, err = rawKVRangeBoundEncodedLen(op.Value)
		if err != nil {
			return 0, err
		}
	}
	headerSize := rawKVOperationHeaderSize(entryRevisions)
	needed := headerSize + keyBytes + valueBytes
	if needed < headerSize || needed > len(dst) {
		return 0, ErrCorrupt
	}
	dst[0] = byte(op.Op)
	binary.LittleEndian.PutUint32(dst[1:5], keyLen)
	binary.LittleEndian.PutUint32(dst[5:9], valueLen)
	if entryRevisions {
		binary.LittleEndian.PutUint64(dst[9:17], op.Revision)
	}
	off := headerSize
	if len(op.Key) > 0 {
		copy(dst[off:], op.Key)
		off += len(op.Key)
	}
	switch op.Op {
	case RawKVOpSetRID:
		if off > len(dst) || len(dst)-off < 8 {
			return 0, ErrCorrupt
		}
		binary.LittleEndian.PutUint64(dst[off:off+8], op.RID)
		off += 8
	case RawKVOpSetMaterializedRID:
		if off > len(dst) || len(dst)-off < 8 {
			return 0, ErrCorrupt
		}
		binary.LittleEndian.PutUint64(dst[off:off+8], op.RID)
		off += 8
		copy(dst[off:], op.Value)
		off += len(op.Value)
	default:
		if len(op.Value) > 0 {
			copy(dst[off:], op.Value)
			off += len(op.Value)
		}
	}
	if off != needed {
		return 0, ErrCorrupt
	}
	return needed, nil
}

func writeRawKVOperationPayloadPiecesWithRevisionFlag(op RawKVOperation, entryRevisions bool, write func([]byte) error) (int, error) {
	keyBytes, valueBytes, err := rawKVOperationPayloadLens(&op)
	if err != nil {
		return 0, err
	}
	if op.Revision != 0 && !entryRevisions {
		return 0, ErrCorrupt
	}
	key := op.Key
	value := op.Value
	keyLen := uint32(len(key))
	valueLen := uint32(len(value))
	var ridBuf [8]byte
	if op.Op == RawKVOpSetRID {
		binary.LittleEndian.PutUint64(ridBuf[:], op.RID)
		value = ridBuf[:]
		valueLen = uint32(len(value))
	} else if op.Op == RawKVOpSetMaterializedRID {
		valueLen = uint32(valueBytes)
	} else if op.Op == RawKVOpDeleteRange {
		keyLen, _, err = rawKVRangeBoundEncodedLen(key)
		if err != nil {
			return 0, err
		}
		valueLen, _, err = rawKVRangeBoundEncodedLen(value)
		if err != nil {
			return 0, err
		}
	}
	var header [rawKVOpRevisionHeaderSize]byte
	header[0] = byte(op.Op)
	binary.LittleEndian.PutUint32(header[1:5], keyLen)
	binary.LittleEndian.PutUint32(header[5:9], valueLen)
	headerSize := rawKVOperationHeaderSize(entryRevisions)
	if entryRevisions {
		binary.LittleEndian.PutUint64(header[9:17], op.Revision)
	}
	if err := write(header[:headerSize]); err != nil {
		return 0, err
	}
	if len(key) > 0 {
		if err := write(key); err != nil {
			return 0, err
		}
	}
	if op.Op == RawKVOpSetMaterializedRID {
		binary.LittleEndian.PutUint64(ridBuf[:], op.RID)
		if err := write(ridBuf[:]); err != nil {
			return 0, err
		}
	}
	if len(value) > 0 {
		if err := write(value); err != nil {
			return 0, err
		}
	}
	return headerSize + keyBytes + valueBytes, nil
}

// EncodeRawKVSingleOperationPayload encodes the common one-op RawKVBatch
// command without forcing callers to materialize a single-entry operation slice.
func EncodeRawKVSingleOperationPayload(op RawKVOperation) ([]byte, error) {
	return EncodeRawKVBatchPayload([]RawKVOperation{op})
}

func rawKVBatchPayloadSizeHint(opHint, byteHint int) (int, error) {
	capHint := rawKVBatchHeaderSize
	if opHint > 0 {
		if commandFrameIntExceedsUint32(opHint) || opHint > (int(^uint(0)>>1)-capHint)/rawKVOpHeaderSize {
			return 0, ErrRecordTooLarge
		}
		capHint += opHint * rawKVOpHeaderSize
	}
	if byteHint > 0 {
		if byteHint > int(^uint(0)>>1)-capHint {
			return 0, ErrRecordTooLarge
		}
		capHint += byteHint
	}
	return capHint, nil
}

// EncodeRawKVBatchPayloadScan encodes a RawKVBatch payload by scanning the
// caller's operation source once. It avoids materializing a []RawKVOperation
// when the caller already owns a replayable batch representation.
func EncodeRawKVBatchPayloadScan(scan func(func(RawKVOperation) error) error) ([]byte, error) {
	return EncodeRawKVBatchPayloadScanWithHint(scan, 0, 0)
}

// EncodeRawKVBatchPayloadScanWithHint is EncodeRawKVBatchPayloadScan with
// best-effort capacity hints. opHint is an approximate operation count and
// byteHint is an approximate total key+value byte count.
func EncodeRawKVBatchPayloadScanWithHint(scan func(func(RawKVOperation) error) error, opHint, byteHint int) ([]byte, error) {
	if scan == nil {
		return EncodeRawKVBatchPayload(nil)
	}
	var builder RawKVBatchPayloadBuilder
	if err := builder.ResetWithHint(opHint, byteHint); err != nil {
		return nil, err
	}
	if err := scan(func(op RawKVOperation) error {
		_, _, err := builder.Append(op)
		return err
	}); err != nil {
		return nil, err
	}
	return builder.Payload(), nil
}

func DecodeRawKVBatchPayload(payload []byte) ([]RawKVOperation, error) {
	if err := validateRawKVBatchPayload(payload); err != nil {
		return nil, err
	}
	version := binary.LittleEndian.Uint16(payload[0:2])
	if version == rawKVZeroBatchPayloadV2 || version == rawKVZeroBatchPayloadV3 {
		count := binary.LittleEndian.Uint32(payload[2:6])
		valueLen := int(binary.LittleEndian.Uint32(payload[6:10]))
		ops := make([]RawKVOperation, 0, count)
		zeroValue := make([]byte, valueLen)
		off := rawKVZeroBatchHeaderSize
		for i := uint32(0); i < count; i++ {
			keyLen, nextOff, err := readCompactZeroKeyLen(payload, off, version)
			if err != nil {
				return nil, err
			}
			off = nextOff
			entry := RawKVOperation{Op: RawKVOpSet}
			entry.Key = cloneBytesPreserveEmpty(payload[off : off+keyLen])
			off += keyLen
			entry.Value = cloneBytesPreserveEmpty(zeroValue)
			ops = append(ops, entry)
		}
		return ops, nil
	}
	count := binary.LittleEndian.Uint32(payload[2:6])
	ops := make([]RawKVOperation, 0, count)
	if err := ScanRawKVBatchPayloadWithRevision(payload, func(op RawKVOp, key, value []byte, revision uint64) error {
		entry := RawKVOperation{Op: op, Key: cloneBytesPreserveEmpty(key), Revision: revision}
		if op == RawKVOpSetRID {
			entry.RID = binary.LittleEndian.Uint64(value)
		} else if op == RawKVOpSetMaterializedRID {
			entry.RID = binary.LittleEndian.Uint64(value[:8])
			entry.Value = cloneBytesPreserveEmpty(value[8:])
		} else {
			entry.Value = cloneBytesPreserveEmpty(value)
		}
		ops = append(ops, entry)
		return nil
	}); err != nil {
		return nil, err
	}
	return ops, nil
}

func validateRawKVBatchPayload(payload []byte) error {
	return ScanRawKVBatchPayload(payload, nil)
}

func validateRawKVBatchPayloadForFormat(format PayloadFormat, payload []byte) error {
	switch format {
	case PayloadFormatRawKVBatchV1:
		return ScanRawKVBatchPayload(payload, func(op RawKVOp, _, _ []byte) error {
			if op == RawKVOpSetMaterializedRID {
				return ErrCorrupt
			}
			return nil
		})
	case PayloadFormatRawKVBatchV2:
		return ScanRawKVBatchPayload(payload, nil)
	default:
		return ErrCorrupt
	}
}

// ScanRawKVBatchPayload validates payload and visits each op with slices that
// reference payload. For RawKVOpSetRID, value is exactly the 8-byte
// little-endian RID payload. For RawKVOpSetMaterializedRID, value is the
// 8-byte little-endian RID followed by the materialization bytes. Use
// DecodeRawKVBatchPayload when callers need owned copies or typed RID fields.
func ScanRawKVBatchPayload(payload []byte, visit func(op RawKVOp, key, value []byte) error) error {
	if visit == nil {
		return ScanRawKVBatchPayloadWithRevision(payload, nil)
	}
	return ScanRawKVBatchPayloadWithRevision(payload, func(op RawKVOp, key, value []byte, _ uint64) error {
		return visit(op, key, value)
	})
}

// ScanRawKVBatchRIDs validates payload and visits only SetRID record IDs.
// Compact-zero payloads cannot contain SetRID operations, so they are
// validated without materializing their declared zero-filled values.
func ScanRawKVBatchRIDs(payload []byte, visit func(rid uint64) error) error {
	if len(payload) < rawKVBatchHeaderSize {
		return ErrCorrupt
	}
	version := binary.LittleEndian.Uint16(payload[0:2])
	if version == rawKVZeroBatchPayloadV2 || version == rawKVZeroBatchPayloadV3 {
		return scanRawKVZeroBatchPayload(payload, nil)
	}
	return ScanRawKVBatchPayload(payload, func(op RawKVOp, _ []byte, value []byte) error {
		if op != RawKVOpSetRID || visit == nil {
			return nil
		}
		return visit(binary.LittleEndian.Uint64(value))
	})
}

// ScanRawKVBatchPayloadWithRevision is ScanRawKVBatchPayload plus the optional
// per-entry revision carried by revision-aware RawKV payloads. Legacy and
// compact-zero payloads report revision zero for every operation.
func ScanRawKVBatchPayloadWithRevision(payload []byte, visit func(op RawKVOp, key, value []byte, revision uint64) error) error {
	if len(payload) < rawKVBatchHeaderSize {
		return ErrCorrupt
	}
	version := binary.LittleEndian.Uint16(payload[0:2])
	if version == rawKVZeroBatchPayloadV2 || version == rawKVZeroBatchPayloadV3 {
		if visit == nil {
			return scanRawKVZeroBatchPayload(payload, nil)
		}
		return scanRawKVZeroBatchPayload(payload, func(op RawKVOp, key, value []byte) error {
			return visit(op, key, value, 0)
		})
	}
	entryRevisions := false
	switch version {
	case rawKVBatchPayloadVersion:
	case rawKVBatchPayloadVersionWithRevisions:
		entryRevisions = true
	default:
		return ErrCommandWALUnsupportedVersion
	}
	count := binary.LittleEndian.Uint32(payload[2:6])
	opHeaderSize := rawKVOperationHeaderSize(entryRevisions)
	if count > uint32((len(payload)-rawKVBatchHeaderSize)/opHeaderSize) {
		return ErrCorrupt
	}
	off := rawKVBatchHeaderSize
	for i := uint32(0); i < count; i++ {
		if off+opHeaderSize > len(payload) {
			return ErrCorrupt
		}
		op := RawKVOp(payload[off])
		keyLen := binary.LittleEndian.Uint32(payload[off+1 : off+5])
		valueLen := binary.LittleEndian.Uint32(payload[off+5 : off+9])
		revision := uint64(0)
		if entryRevisions {
			revision = binary.LittleEndian.Uint64(payload[off+9 : off+17])
		}
		off += opHeaderSize
		keyBytes := keyLen
		valueBytes := valueLen
		keyNil := false
		valueNil := false
		if op == RawKVOpDeleteRange {
			if keyLen == rawKVNilRangeBoundLenUint32 {
				keyBytes = 0
				keyNil = true
			}
			if valueLen == rawKVNilRangeBoundLenUint32 {
				valueBytes = 0
				valueNil = true
			}
		}
		need := uint64(keyBytes) + uint64(valueBytes)
		if need > uint64(len(payload)-off) || need > uint64(^uint(0)>>1) {
			return ErrCorrupt
		}
		var key []byte
		if !keyNil {
			key = payload[off : off+int(keyBytes)]
		}
		off += int(keyBytes)
		valueLenForShape := int(valueBytes)
		if valueNil {
			valueLenForShape = -1
		}
		if err := validateRawKVOperationShape(op, key, valueLenForShape); err != nil {
			return err
		}
		var value []byte
		if !valueNil {
			value = payload[off : off+int(valueBytes)]
		}
		if op == RawKVOpSetRID || op == RawKVOpSetMaterializedRID {
			// validateRawKVOperationShape enforces valueLen == 8 for SetRID and
			// valueLen >= 8 for SetMaterializedRID, so the RID prefix is present.
			// Only check for the zero-RID sentinel, which is always invalid.
			if binary.LittleEndian.Uint64(value) == 0 {
				return ErrCorrupt
			}
		}
		if op == RawKVOpDeleteRange {
			if err := validateRawKVDeleteRangeBounds(key, value); err != nil {
				return err
			}
			if revision != 0 {
				return ErrCorrupt
			}
		}
		if visit != nil {
			if err := visit(op, key, value, revision); err != nil {
				return err
			}
		}
		off += int(valueBytes)
	}
	if off != len(payload) {
		return ErrCorrupt
	}
	return nil
}

func scanRawKVZeroBatchPayload(payload []byte, visit func(op RawKVOp, key, value []byte) error) error {
	if len(payload) < rawKVZeroBatchHeaderSize {
		return ErrCorrupt
	}
	count := binary.LittleEndian.Uint32(payload[2:6])
	valueLen := binary.LittleEndian.Uint32(payload[6:10])
	version := binary.LittleEndian.Uint16(payload[0:2])
	opHeaderSize := rawKVZeroOpHeaderSizeForVersion(version)
	if valueLen == 0 {
		return ErrCorrupt
	}
	if count > uint32((len(payload)-rawKVZeroBatchHeaderSize)/opHeaderSize) {
		return ErrCorrupt
	}
	if uint64(valueLen) > uint64(^uint(0)>>1) {
		return ErrRecordTooLarge
	}
	var zeroValue []byte
	if visit != nil {
		zeroValue = make([]byte, int(valueLen))
	}
	off := rawKVZeroBatchHeaderSize
	for i := uint32(0); i < count; i++ {
		keyLen, nextOff, err := readCompactZeroKeyLen(payload, off, version)
		if err != nil {
			return err
		}
		off = nextOff
		if keyLen > len(payload)-off {
			return ErrCorrupt
		}
		key := payload[off : off+keyLen]
		off += keyLen
		if key == nil {
			return ErrCorrupt
		}
		if visit != nil {
			if err := visit(RawKVOpSet, key, zeroValue); err != nil {
				return err
			}
		}
	}
	if off != len(payload) {
		return ErrCorrupt
	}
	return nil
}

func validateRawKVOperation(op *RawKVOperation) error {
	if op == nil {
		return ErrCorrupt
	}
	if op.Op == RawKVOpDeleteRange {
		if op.Revision != 0 {
			return ErrCorrupt
		}
		return validateRawKVDeleteRangeBounds(op.Key, op.Value)
	}
	if op.Key == nil {
		return ErrCorrupt
	}
	valueLen := len(op.Value)
	if op.Op == RawKVOpSetRID {
		if op.RID == 0 || len(op.Value) != 0 {
			return ErrCorrupt
		}
		valueLen = 8
	} else if op.Op == RawKVOpSetMaterializedRID {
		if op.RID == 0 || op.Value == nil {
			return ErrCorrupt
		}
		if len(op.Value) > int(^uint(0)>>1)-8 {
			return ErrRecordTooLarge
		}
		valueLen = 8 + len(op.Value)
	}
	if commandFrameIntExceedsUint32(valueLen) {
		return ErrRecordTooLarge
	}
	return validateRawKVOperationShape(op.Op, op.Key, valueLen)
}

func validateRawKVOperationShape(op RawKVOp, key []byte, valueLen int) error {
	switch op {
	case RawKVOpSet:
		if key == nil {
			return ErrCorrupt
		}
		return nil
	case RawKVOpDelete:
		if key == nil || valueLen != 0 {
			return ErrCorrupt
		}
		return nil
	case RawKVOpSetRID:
		if key == nil || valueLen != 8 {
			return ErrCorrupt
		}
		return nil
	case RawKVOpSetMaterializedRID:
		if key == nil || valueLen < 8 {
			return ErrCorrupt
		}
		return nil
	case RawKVOpDeleteRange:
		// valueLen == -1 is the scanner's sentinel for a nil/unbounded end.
		if valueLen < -1 {
			return ErrCorrupt
		}
		return nil
	default:
		return ErrCorrupt
	}
}

func validateRawKVDeleteRangeBounds(start, end []byte) error {
	if start != nil && commandFrameIntExceedsUint32(len(start)) {
		return ErrRecordTooLarge
	}
	if end != nil && commandFrameIntExceedsUint32(len(end)) {
		return ErrRecordTooLarge
	}
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return ErrCorrupt
	}
	if start == nil && end != nil && len(end) == 0 {
		return ErrCorrupt
	}
	return nil
}

func rawKVRangeBoundEncodedLen(bound []byte) (encoded uint32, bytesLen int, err error) {
	if bound == nil {
		return rawKVNilRangeBoundLenUint32, 0, nil
	}
	if commandFrameIntExceedsUint32(len(bound)) || uint32(len(bound)) == rawKVNilRangeBoundLenUint32 {
		return 0, 0, ErrRecordTooLarge
	}
	return uint32(len(bound)), len(bound), nil
}

func EncodeCollectionInsertBatchByIDPayload(collection string, docs []CollectionDocument) ([]byte, error) {
	if collection == "" {
		return nil, fmt.Errorf("%w: empty collection", ErrCorrupt)
	}
	ordered, err := canonicalCollectionDocuments(docs)
	if err != nil {
		return nil, err
	}
	total, err := collectionBatchPayloadHeaderLen(collection, len(ordered))
	if err != nil {
		return nil, err
	}
	for i := range ordered {
		doc := ordered[i]
		if commandFrameIntExceedsUint32(len(doc.Document)) {
			return nil, ErrRecordTooLarge
		}
		total, err = addCommandFrameEncodedSectionLen(total, 8+len(doc.ID)+len(doc.Document))
		if err != nil {
			return nil, err
		}
	}
	payload := make([]byte, total)
	off := encodeCollectionBatchHeader(payload, collection, len(ordered))
	for i := range ordered {
		doc := ordered[i]
		binary.LittleEndian.PutUint32(payload[off:off+4], uint32(len(doc.ID)))
		binary.LittleEndian.PutUint32(payload[off+4:off+8], uint32(len(doc.Document)))
		off += 8
		copy(payload[off:], doc.ID)
		off += len(doc.ID)
		copy(payload[off:], doc.Document)
		off += len(doc.Document)
	}
	return payload, nil
}

func DecodeCollectionInsertBatchByIDPayload(payload []byte) (CollectionInsertBatchByIDPayload, error) {
	collection, count, off, err := decodeCollectionBatchHeader(payload)
	if err != nil {
		return CollectionInsertBatchByIDPayload{}, err
	}
	if uint64(count)*8 > uint64(len(payload)-off) {
		return CollectionInsertBatchByIDPayload{}, ErrCorrupt
	}
	docCount, err := commandPayloadCountToInt(count)
	if err != nil {
		return CollectionInsertBatchByIDPayload{}, err
	}
	docs := make([]CollectionDocument, 0, docCount)
	for i := uint32(0); i < count; i++ {
		if off+8 > len(payload) {
			return CollectionInsertBatchByIDPayload{}, ErrCorrupt
		}
		idLen := binary.LittleEndian.Uint32(payload[off : off+4])
		docLen := binary.LittleEndian.Uint32(payload[off+4 : off+8])
		off += 8
		if uint64(idLen)+uint64(docLen) > uint64(len(payload)-off) {
			return CollectionInsertBatchByIDPayload{}, ErrCorrupt
		}
		id := payload[off : off+int(idLen)]
		off += int(idLen)
		doc := payload[off : off+int(docLen)]
		off += int(docLen)
		if len(id) == 0 {
			return CollectionInsertBatchByIDPayload{}, ErrCorrupt
		}
		docs = append(docs, CollectionDocument{
			ID:       cloneBytesPreserveEmpty(id),
			Document: cloneBytesPreserveEmpty(doc),
		})
	}
	if off != len(payload) {
		return CollectionInsertBatchByIDPayload{}, ErrCorrupt
	}
	if err := validateStrictlyIncreasingCollectionIDsFromDocs(docs); err != nil {
		return CollectionInsertBatchByIDPayload{}, err
	}
	return CollectionInsertBatchByIDPayload{Collection: collection, Documents: docs}, nil
}

func validateCollectionInsertBatchByIDPayload(payload []byte) error {
	_, count, off, err := parseCollectionBatchHeader(payload)
	if err != nil {
		return err
	}
	if uint64(count)*8 > uint64(len(payload)-off) {
		return ErrCorrupt
	}
	if _, err := commandPayloadCountToInt(count); err != nil {
		return err
	}
	var previousID []byte
	for i := uint32(0); i < count; i++ {
		if off+8 > len(payload) {
			return ErrCorrupt
		}
		idLen := binary.LittleEndian.Uint32(payload[off : off+4])
		docLen := binary.LittleEndian.Uint32(payload[off+4 : off+8])
		off += 8
		if uint64(idLen)+uint64(docLen) > uint64(len(payload)-off) {
			return ErrCorrupt
		}
		id := payload[off : off+int(idLen)]
		if len(id) == 0 || previousID != nil && bytes.Compare(previousID, id) >= 0 {
			return ErrCorrupt
		}
		previousID = id
		off += int(idLen) + int(docLen)
	}
	if off != len(payload) {
		return ErrCorrupt
	}
	return nil
}

func EncodeCollectionUpdateBatchByIDPayload(collection string, docs []CollectionDocument) ([]byte, error) {
	return EncodeCollectionInsertBatchByIDPayload(collection, docs)
}

func DecodeCollectionUpdateBatchByIDPayload(payload []byte) (CollectionUpdateBatchByIDPayload, error) {
	decoded, err := DecodeCollectionInsertBatchByIDPayload(payload)
	if err != nil {
		return CollectionUpdateBatchByIDPayload{}, err
	}
	return CollectionUpdateBatchByIDPayload{
		Collection: decoded.Collection,
		Documents:  decoded.Documents,
	}, nil
}

func EncodeCollectionDeleteBatchByIDPayload(collection string, ids [][]byte) ([]byte, error) {
	if collection == "" {
		return nil, fmt.Errorf("%w: empty collection", ErrCorrupt)
	}
	ordered, err := canonicalCollectionIDs(ids)
	if err != nil {
		return nil, err
	}
	total, err := collectionBatchPayloadHeaderLen(collection, len(ordered))
	if err != nil {
		return nil, err
	}
	for _, id := range ordered {
		total, err = addCommandFrameEncodedSectionLen(total, 4+len(id))
		if err != nil {
			return nil, err
		}
	}
	payload := make([]byte, total)
	off := encodeCollectionBatchHeader(payload, collection, len(ordered))
	for _, id := range ordered {
		binary.LittleEndian.PutUint32(payload[off:off+4], uint32(len(id)))
		off += 4
		copy(payload[off:], id)
		off += len(id)
	}
	return payload, nil
}

func DecodeCollectionDeleteBatchByIDPayload(payload []byte) (CollectionDeleteBatchByIDPayload, error) {
	collection, count, off, err := decodeCollectionBatchHeader(payload)
	if err != nil {
		return CollectionDeleteBatchByIDPayload{}, err
	}
	if uint64(count)*4 > uint64(len(payload)-off) {
		return CollectionDeleteBatchByIDPayload{}, ErrCorrupt
	}
	idCount, err := commandPayloadCountToInt(count)
	if err != nil {
		return CollectionDeleteBatchByIDPayload{}, err
	}
	ids := make([][]byte, 0, idCount)
	for i := uint32(0); i < count; i++ {
		if off+4 > len(payload) {
			return CollectionDeleteBatchByIDPayload{}, ErrCorrupt
		}
		idLen := binary.LittleEndian.Uint32(payload[off : off+4])
		off += 4
		if uint64(idLen) > uint64(len(payload)-off) {
			return CollectionDeleteBatchByIDPayload{}, ErrCorrupt
		}
		id := payload[off : off+int(idLen)]
		off += int(idLen)
		if len(id) == 0 {
			return CollectionDeleteBatchByIDPayload{}, ErrCorrupt
		}
		ids = append(ids, cloneBytesPreserveEmpty(id))
	}
	if off != len(payload) {
		return CollectionDeleteBatchByIDPayload{}, ErrCorrupt
	}
	if err := validateStrictlyIncreasingCollectionIDs(ids); err != nil {
		return CollectionDeleteBatchByIDPayload{}, err
	}
	return CollectionDeleteBatchByIDPayload{Collection: collection, IDs: ids}, nil
}

func EncodeCollectionReplaceSourceByIDPayload(collection string, deleteIDs [][]byte, docs []CollectionDocument) ([]byte, error) {
	deletePayload, err := EncodeCollectionDeleteBatchByIDPayload(collection, deleteIDs)
	if err != nil {
		return nil, err
	}
	insertPayload, err := EncodeCollectionInsertBatchByIDPayload(collection, docs)
	if err != nil {
		return nil, err
	}
	if commandFrameIntExceedsUint32(len(deletePayload)) {
		return nil, ErrRecordTooLarge
	}
	total, err := addCommandFrameEncodedSectionLen(4, len(deletePayload))
	if err != nil {
		return nil, err
	}
	total, err = addCommandFrameEncodedSectionLen(total, len(insertPayload))
	if err != nil {
		return nil, err
	}
	payload := make([]byte, total)
	binary.LittleEndian.PutUint32(payload[:4], uint32(len(deletePayload)))
	copy(payload[4:], deletePayload)
	copy(payload[4+len(deletePayload):], insertPayload)
	return payload, nil
}

func DecodeCollectionReplaceSourceByIDPayload(payload []byte) (CollectionReplaceSourceByIDPayload, error) {
	if len(payload) < 4 {
		return CollectionReplaceSourceByIDPayload{}, ErrCorrupt
	}
	deleteLenRaw := binary.LittleEndian.Uint32(payload[:4])
	if uint64(deleteLenRaw) > uint64(len(payload)-4) {
		return CollectionReplaceSourceByIDPayload{}, ErrCorrupt
	}
	deleteLen := int(deleteLenRaw)
	deleted, err := DecodeCollectionDeleteBatchByIDPayload(payload[4 : 4+deleteLen])
	if err != nil {
		return CollectionReplaceSourceByIDPayload{}, err
	}
	inserted, err := DecodeCollectionInsertBatchByIDPayload(payload[4+deleteLen:])
	if err != nil {
		return CollectionReplaceSourceByIDPayload{}, err
	}
	if deleted.Collection != inserted.Collection {
		return CollectionReplaceSourceByIDPayload{}, ErrCorrupt
	}
	return CollectionReplaceSourceByIDPayload{
		Collection: deleted.Collection,
		DeleteIDs:  deleted.IDs,
		Documents:  inserted.Documents,
	}, nil
}

func validateCollectionDeleteBatchByIDPayload(payload []byte) error {
	_, count, off, err := parseCollectionBatchHeader(payload)
	if err != nil {
		return err
	}
	if uint64(count)*4 > uint64(len(payload)-off) {
		return ErrCorrupt
	}
	if _, err := commandPayloadCountToInt(count); err != nil {
		return err
	}
	var previousID []byte
	for i := uint32(0); i < count; i++ {
		if off+4 > len(payload) {
			return ErrCorrupt
		}
		idLen := binary.LittleEndian.Uint32(payload[off : off+4])
		off += 4
		if uint64(idLen) > uint64(len(payload)-off) {
			return ErrCorrupt
		}
		id := payload[off : off+int(idLen)]
		if len(id) == 0 || previousID != nil && bytes.Compare(previousID, id) >= 0 {
			return ErrCorrupt
		}
		previousID = id
		off += int(idLen)
	}
	if off != len(payload) {
		return ErrCorrupt
	}
	return nil
}

func EncodeCollectionRebuildVectorIndexPayload(collection, indexName string) ([]byte, error) {
	if collection == "" {
		return nil, fmt.Errorf("%w: invalid collection vector index rebuild payload: collection name is empty", ErrCorrupt)
	}
	if indexName == "" {
		return nil, fmt.Errorf("%w: invalid collection vector index rebuild payload: index name is empty", ErrCorrupt)
	}
	if commandFrameIntExceedsUint32(len(collection)) || commandFrameIntExceedsUint32(len(indexName)) {
		return nil, ErrRecordTooLarge
	}
	total, err := addCommandFrameEncodedSectionLen(collectionRebuildVectorIndexPayloadHeaderSize, len(collection))
	if err != nil {
		return nil, err
	}
	total, err = addCommandFrameEncodedSectionLen(total, len(indexName))
	if err != nil {
		return nil, err
	}
	payload := make([]byte, total)
	binary.LittleEndian.PutUint16(payload[:collectionRebuildVectorIndexVersionEnd], collectionRebuildVectorIndexPayloadVersion)
	binary.LittleEndian.PutUint32(payload[collectionRebuildVectorIndexCollectionLenStart:collectionRebuildVectorIndexCollectionLenEnd], uint32(len(collection)))
	binary.LittleEndian.PutUint32(payload[collectionRebuildVectorIndexIndexLenStart:collectionRebuildVectorIndexIndexLenEnd], uint32(len(indexName)))
	copy(payload[collectionRebuildVectorIndexCollectionNameStart:], collection)
	copy(payload[collectionRebuildVectorIndexCollectionNameStart+len(collection):], indexName)
	return payload, nil
}

func DecodeCollectionRebuildVectorIndexPayload(payload []byte) (CollectionRebuildVectorIndexPayload, error) {
	if len(payload) < collectionRebuildVectorIndexPayloadHeaderSize {
		return CollectionRebuildVectorIndexPayload{}, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(payload[:collectionRebuildVectorIndexVersionEnd]) != collectionRebuildVectorIndexPayloadVersion {
		return CollectionRebuildVectorIndexPayload{}, ErrCommandWALUnsupportedVersion
	}
	nameLen := binary.LittleEndian.Uint32(payload[collectionRebuildVectorIndexCollectionLenStart:collectionRebuildVectorIndexCollectionLenEnd])
	indexLen := binary.LittleEndian.Uint32(payload[collectionRebuildVectorIndexIndexLenStart:collectionRebuildVectorIndexIndexLenEnd])
	off := collectionRebuildVectorIndexCollectionNameStart
	if uint64(nameLen)+uint64(indexLen) > uint64(len(payload)-off) {
		return CollectionRebuildVectorIndexPayload{}, ErrCorrupt
	}
	collection := payload[off : off+int(nameLen)]
	off += int(nameLen)
	indexName := payload[off : off+int(indexLen)]
	off += int(indexLen)
	if off != len(payload) || len(collection) == 0 || len(indexName) == 0 {
		return CollectionRebuildVectorIndexPayload{}, ErrCorrupt
	}
	return CollectionRebuildVectorIndexPayload{
		Collection: string(collection),
		IndexName:  string(indexName),
	}, nil
}

func validateCollectionRebuildVectorIndexPayload(payload []byte) error {
	if len(payload) < collectionRebuildVectorIndexPayloadHeaderSize {
		return ErrCorrupt
	}
	if binary.LittleEndian.Uint16(payload[:collectionRebuildVectorIndexVersionEnd]) != collectionRebuildVectorIndexPayloadVersion {
		return ErrCommandWALUnsupportedVersion
	}
	nameLen := binary.LittleEndian.Uint32(payload[collectionRebuildVectorIndexCollectionLenStart:collectionRebuildVectorIndexCollectionLenEnd])
	indexLen := binary.LittleEndian.Uint32(payload[collectionRebuildVectorIndexIndexLenStart:collectionRebuildVectorIndexIndexLenEnd])
	off := collectionRebuildVectorIndexCollectionNameStart
	if uint64(nameLen)+uint64(indexLen) != uint64(len(payload)-off) || nameLen == 0 || indexLen == 0 {
		return ErrCorrupt
	}
	return nil
}

func EncodeCatalogCreateCollectionPayload(collection string, metadata []byte) ([]byte, error) {
	if collection == "" || metadata == nil {
		return nil, fmt.Errorf("%w: invalid catalog create collection payload", ErrCorrupt)
	}
	if commandFrameIntExceedsUint32(len(collection)) || commandFrameIntExceedsUint32(len(metadata)) {
		return nil, ErrRecordTooLarge
	}
	total, err := addCommandFrameEncodedSectionLen(2+4+4, len(collection))
	if err != nil {
		return nil, err
	}
	total, err = addCommandFrameEncodedSectionLen(total, len(metadata))
	if err != nil {
		return nil, err
	}
	payload := make([]byte, total)
	binary.LittleEndian.PutUint16(payload[0:2], 1)
	binary.LittleEndian.PutUint32(payload[2:6], uint32(len(collection)))
	binary.LittleEndian.PutUint32(payload[6:10], uint32(len(metadata)))
	copy(payload[10:], collection)
	copy(payload[10+len(collection):], metadata)
	return payload, nil
}

func DecodeCatalogCreateCollectionPayload(payload []byte) (CatalogCreateCollectionPayload, error) {
	if len(payload) < 10 {
		return CatalogCreateCollectionPayload{}, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(payload[0:2]) != 1 {
		return CatalogCreateCollectionPayload{}, ErrCommandWALUnsupportedVersion
	}
	nameLen := binary.LittleEndian.Uint32(payload[2:6])
	metaLen := binary.LittleEndian.Uint32(payload[6:10])
	off := 10
	if uint64(nameLen)+uint64(metaLen) > uint64(len(payload)-off) {
		return CatalogCreateCollectionPayload{}, ErrCorrupt
	}
	collection := payload[off : off+int(nameLen)]
	off += int(nameLen)
	metadata := payload[off : off+int(metaLen)]
	off += int(metaLen)
	if off != len(payload) || len(collection) == 0 || metadata == nil {
		return CatalogCreateCollectionPayload{}, ErrCorrupt
	}
	return CatalogCreateCollectionPayload{
		Collection: string(collection),
		Metadata:   cloneBytesPreserveEmpty(metadata),
	}, nil
}

func validateCatalogCreateCollectionPayload(payload []byte) error {
	if len(payload) < 10 {
		return ErrCorrupt
	}
	if binary.LittleEndian.Uint16(payload[0:2]) != 1 {
		return ErrCommandWALUnsupportedVersion
	}
	nameLen := binary.LittleEndian.Uint32(payload[2:6])
	metaLen := binary.LittleEndian.Uint32(payload[6:10])
	if uint64(nameLen)+uint64(metaLen) != uint64(len(payload)-10) || nameLen == 0 {
		return ErrCorrupt
	}
	return nil
}

func commandPayloadCountToInt(count uint32) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if uint64(count) > uint64(maxInt) {
		return 0, ErrRecordTooLarge
	}
	return int(count), nil
}

func collectionBatchPayloadHeaderLen(collection string, count int) (int, error) {
	if commandFrameIntExceedsUint32(len(collection)) || commandFrameIntExceedsUint32(count) {
		return 0, ErrRecordTooLarge
	}
	return addCommandFrameEncodedSectionLen(2+4+4, len(collection))
}

func encodeCollectionBatchHeader(payload []byte, collection string, count int) int {
	binary.LittleEndian.PutUint16(payload[0:2], 1)
	binary.LittleEndian.PutUint32(payload[2:6], uint32(len(collection)))
	binary.LittleEndian.PutUint32(payload[6:10], uint32(count))
	copy(payload[10:], collection)
	return 10 + len(collection)
}

func decodeCollectionBatchHeader(payload []byte) (collection string, count uint32, off int, err error) {
	nameBytes, count, off, err := parseCollectionBatchHeader(payload)
	if err != nil {
		return "", 0, 0, err
	}
	return string(nameBytes), count, off, nil
}

func parseCollectionBatchHeader(payload []byte) (collection []byte, count uint32, off int, err error) {
	if len(payload) < 10 {
		return nil, 0, 0, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(payload[0:2]) != 1 {
		return nil, 0, 0, ErrCommandWALUnsupportedVersion
	}
	nameLen := binary.LittleEndian.Uint32(payload[2:6])
	count = binary.LittleEndian.Uint32(payload[6:10])
	off = 10
	if uint64(nameLen) > uint64(len(payload)-off) {
		return nil, 0, 0, ErrCorrupt
	}
	nameBytes := payload[off : off+int(nameLen)]
	off += int(nameLen)
	if len(nameBytes) == 0 {
		return nil, 0, 0, ErrCorrupt
	}
	return nameBytes, count, off, nil
}

func canonicalCollectionDocuments(docs []CollectionDocument) ([]CollectionDocument, error) {
	if commandFrameIntExceedsUint32(len(docs)) {
		return nil, ErrRecordTooLarge
	}
	needsSort := false
	for i := range docs {
		doc := docs[i]
		if len(doc.ID) == 0 || doc.ID == nil || doc.Document == nil {
			return nil, ErrCorrupt
		}
		if commandFrameIntExceedsUint32(len(doc.ID)) {
			return nil, ErrRecordTooLarge
		}
		if i > 0 && bytes.Compare(docs[i-1].ID, doc.ID) >= 0 {
			needsSort = true
		}
	}
	if !needsSort {
		return docs, nil
	}
	ordered := make([]CollectionDocument, len(docs))
	copy(ordered, docs)
	sort.Slice(ordered, func(i, j int) bool {
		return bytes.Compare(ordered[i].ID, ordered[j].ID) < 0
	})
	if err := validateStrictlyIncreasingCollectionIDsFromDocs(ordered); err != nil {
		return nil, err
	}
	return ordered, nil
}

func canonicalCollectionIDs(ids [][]byte) ([][]byte, error) {
	if commandFrameIntExceedsUint32(len(ids)) {
		return nil, ErrRecordTooLarge
	}
	needsSort := false
	for i := range ids {
		if len(ids[i]) == 0 || ids[i] == nil {
			return nil, ErrCorrupt
		}
		if commandFrameIntExceedsUint32(len(ids[i])) {
			return nil, ErrRecordTooLarge
		}
		if i > 0 && bytes.Compare(ids[i-1], ids[i]) >= 0 {
			needsSort = true
		}
	}
	if !needsSort {
		return ids, nil
	}
	ordered := make([][]byte, len(ids))
	copy(ordered, ids)
	sort.Slice(ordered, func(i, j int) bool {
		return bytes.Compare(ordered[i], ordered[j]) < 0
	})
	if err := validateStrictlyIncreasingCollectionIDs(ordered); err != nil {
		return nil, err
	}
	return ordered, nil
}

func validateStrictlyIncreasingCollectionIDsFromDocs(docs []CollectionDocument) error {
	for i := 1; i < len(docs); i++ {
		if bytes.Compare(docs[i-1].ID, docs[i].ID) >= 0 {
			return ErrCorrupt
		}
	}
	return nil
}

func validateStrictlyIncreasingCollectionIDs(ids [][]byte) error {
	for i := 1; i < len(ids); i++ {
		if bytes.Compare(ids[i-1], ids[i]) >= 0 {
			return ErrCorrupt
		}
	}
	return nil
}

func cloneBytesPreserveEmpty(src []byte) []byte {
	if src == nil {
		return nil
	}
	if len(src) == 0 {
		return []byte{}
	}
	return append([]byte(nil), src...)
}

func encodeExternalRefs(refs []ExternalRef) ([]byte, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	total, err := externalRefsEncodedLen(refs)
	if err != nil {
		return nil, err
	}
	out := make([]byte, total)
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(refs)))
	off := 4
	for i := range refs {
		ref := &refs[i]
		binary.LittleEndian.PutUint16(out[off:off+2], uint16(ref.Class))
		binary.LittleEndian.PutUint16(out[off+2:off+4], ref.Flags)
		binary.LittleEndian.PutUint32(out[off+4:off+8], uint32(len(ref.Path)))
		binary.LittleEndian.PutUint64(out[off+8:off+16], ref.FileID)
		binary.LittleEndian.PutUint64(out[off+16:off+24], ref.Offset)
		binary.LittleEndian.PutUint64(out[off+24:off+32], ref.Length)
		copy(out[off+32:off+32+sha256.Size], ref.Digest[:])
		off += 32 + sha256.Size
		copy(out[off:], ref.Path)
		off += len(ref.Path)
	}
	return out, nil
}

func externalRefsEncodedLen(refs []ExternalRef) (int, error) {
	if len(refs) == 0 {
		return 0, nil
	}
	if commandFrameIntExceedsUint32(len(refs)) {
		return 0, ErrRecordTooLarge
	}
	total := 4
	maxInt := int(^uint(0) >> 1)
	for i := range refs {
		pathLen := len(refs[i].Path)
		if commandFrameIntExceedsUint32(pathLen) {
			return 0, ErrRecordTooLarge
		}
		if pathLen > maxInt-externalRefEncodedFixedSize {
			return 0, ErrRecordTooLarge
		}
		n := externalRefEncodedFixedSize + pathLen
		if total > maxInt-n {
			return 0, ErrRecordTooLarge
		}
		total += n
	}
	if commandFrameIntExceedsUint32(total) {
		return 0, ErrRecordTooLarge
	}
	return total, nil
}

func decodeExternalRefs(data []byte) ([]ExternalRef, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, ErrCorrupt
	}
	count := binary.LittleEndian.Uint32(data[0:4])
	if count > uint32((len(data)-4)/externalRefEncodedFixedSize) {
		return nil, ErrCorrupt
	}
	refs := make([]ExternalRef, 0, count)
	off := 4
	for i := uint32(0); i < count; i++ {
		if off+externalRefEncodedFixedSize > len(data) {
			return nil, ErrCorrupt
		}
		ref := ExternalRef{}
		ref.Class = ExternalRefClass(binary.LittleEndian.Uint16(data[off : off+2]))
		ref.Flags = binary.LittleEndian.Uint16(data[off+2 : off+4])
		pathLen := binary.LittleEndian.Uint32(data[off+4 : off+8])
		ref.FileID = binary.LittleEndian.Uint64(data[off+8 : off+16])
		ref.Offset = binary.LittleEndian.Uint64(data[off+16 : off+24])
		ref.Length = binary.LittleEndian.Uint64(data[off+24 : off+32])
		copy(ref.Digest[:], data[off+32:off+32+sha256.Size])
		off += externalRefEncodedFixedSize
		if uint64(pathLen) > uint64(len(data)-off) || uint64(pathLen) > uint64(^uint(0)>>1) {
			return nil, ErrCorrupt
		}
		ref.Path = append([]byte(nil), data[off:off+int(pathLen)]...)
		off += int(pathLen)
		refs = append(refs, ref)
	}
	if off != len(data) {
		return nil, ErrCorrupt
	}
	return refs, nil
}

func encodeCommandExtensions(exts []CommandExtension) ([]byte, error) {
	if len(exts) == 0 {
		return nil, nil
	}
	total, err := commandExtensionsEncodedLen(exts)
	if err != nil {
		return nil, err
	}
	out := make([]byte, total)
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(exts)))
	off := 4
	for i := range exts {
		ext := &exts[i]
		binary.LittleEndian.PutUint16(out[off:off+2], ext.Type)
		binary.LittleEndian.PutUint16(out[off+2:off+4], 0)
		binary.LittleEndian.PutUint32(out[off+4:off+8], uint32(len(ext.Payload)))
		off += 8
		copy(out[off:], ext.Payload)
		off += len(ext.Payload)
	}
	return out, nil
}

func commandExtensionsEncodedLen(exts []CommandExtension) (int, error) {
	if len(exts) == 0 {
		return 0, nil
	}
	if commandFrameIntExceedsUint32(len(exts)) {
		return 0, ErrRecordTooLarge
	}
	total := 4
	maxInt := int(^uint(0) >> 1)
	for i := range exts {
		payloadLen := len(exts[i].Payload)
		if commandFrameIntExceedsUint32(payloadLen) {
			return 0, ErrRecordTooLarge
		}
		if payloadLen > maxInt-commandExtensionEncodedFixedSize {
			return 0, ErrRecordTooLarge
		}
		n := commandExtensionEncodedFixedSize + payloadLen
		if total > maxInt-n {
			return 0, ErrRecordTooLarge
		}
		total += n
	}
	if commandFrameIntExceedsUint32(total) {
		return 0, ErrRecordTooLarge
	}
	return total, nil
}

func commandFrameIntExceedsUint32(n int) bool {
	return n < 0 || uint64(n) > maxCommandFrameUint32
}

func decodeCommandExtensions(data []byte) ([]CommandExtension, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, ErrCorrupt
	}
	count := binary.LittleEndian.Uint32(data[0:4])
	if count > uint32((len(data)-4)/commandExtensionEncodedFixedSize) {
		return nil, ErrCorrupt
	}
	exts := make([]CommandExtension, 0, count)
	off := 4
	for i := uint32(0); i < count; i++ {
		if off+commandExtensionEncodedFixedSize > len(data) {
			return nil, ErrCorrupt
		}
		ext := CommandExtension{Type: binary.LittleEndian.Uint16(data[off : off+2])}
		if binary.LittleEndian.Uint16(data[off+2:off+4]) != 0 {
			return nil, ErrCorrupt
		}
		payloadLen := binary.LittleEndian.Uint32(data[off+4 : off+8])
		off += commandExtensionEncodedFixedSize
		if uint64(payloadLen) > uint64(len(data)-off) || uint64(payloadLen) > uint64(^uint(0)>>1) {
			return nil, ErrCorrupt
		}
		ext.Payload = append([]byte(nil), data[off:off+int(payloadLen)]...)
		off += int(payloadLen)
		exts = append(exts, ext)
	}
	if off != len(data) {
		return nil, ErrCorrupt
	}
	return exts, nil
}

func putCommandFramePayloadLenForTest(frame []byte, n uint32) {
	if len(frame) >= 60 {
		binary.LittleEndian.PutUint32(frame[56:60], n)
	}
}

func (r *Reader) ReadCommandFrame() (CommandEnvelope, error) {
	payload, err := r.readSegmentPayload(true)
	if err != nil {
		return CommandEnvelope{}, err
	}
	return DecodeCommandFrame(payload)
}

// ReadValidatedCommandFrameLSN fully validates the next command frame and
// returns only its LSN. The reader reuses payload storage between calls, so
// this is reserved for scans that do not retain decoded frame data.
func (r *Reader) ReadValidatedCommandFrameLSN() (uint64, error) {
	payload, err := r.readSegmentPayloadForCommandScan()
	if err != nil {
		return 0, err
	}
	env, err := decodeCommandFrame(payload, true)
	if err != nil {
		return 0, err
	}
	return env.LSN, nil
}

func ScanCommandFrames(path string, opts Options) ([]CommandEnvelope, error) {
	return scanCommandFrames(path, opts, nil, true)
}

func ScanCommandFrameSegments(paths []string, opts Options) ([]CommandEnvelope, error) {
	seen := make(map[uint64]struct{})
	var frames []CommandEnvelope
	allowTail := commandFrameSegmentTailAllowance(paths)
	for i, path := range paths {
		segmentFrames, err := scanCommandFrames(path, opts, seen, allowTail[i])
		if err != nil {
			return frames, err
		}
		frames = append(frames, segmentFrames...)
	}
	sort.Slice(frames, func(i, j int) bool {
		return frames[i].LSN < frames[j].LSN
	})
	return frames, nil
}

func commandFrameSegmentTailAllowance(paths []string) []bool {
	allow := make([]bool, len(paths))
	type parsedSegment struct {
		lane int
		seq  uint64
		ok   bool
	}
	parsed := make([]parsedSegment, len(paths))
	latestByLane := make(map[int]uint64)
	for i, path := range paths {
		lane, seq, ok := parseCommandSegmentName(filepath.Base(path))
		parsed[i] = parsedSegment{lane: lane, seq: seq, ok: ok}
		if ok && seq > latestByLane[lane] {
			latestByLane[lane] = seq
		}
	}
	for i := range paths {
		if parsed[i].ok {
			allow[i] = parsed[i].seq == latestByLane[parsed[i].lane]
			continue
		}
		allow[i] = i == len(paths)-1
	}
	return allow
}

func scanCommandFrames(path string, opts Options, seen map[uint64]struct{}, allowTerminalTail bool) ([]CommandEnvelope, error) {
	r, err := NewReaderWithOptions(path, opts)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	if seen == nil {
		seen = make(map[uint64]struct{})
	}
	var frames []CommandEnvelope
	for {
		env, err := r.ReadCommandFrame()
		if err != nil {
			if err == io.EOF || (allowTerminalTail && errors.Is(err, ErrCommandWALTerminalTail)) {
				return frames, nil
			}
			return frames, err
		}
		if _, dup := seen[env.LSN]; dup {
			return frames, ErrCommandWALDuplicateLSN
		}
		seen[env.LSN] = struct{}{}
		frames = append(frames, env)
	}
}

func scanCommandSegmentSummary(path string, opts Options, onLSN func(uint64) error) (maxLSN uint64, typed bool, completeEnd int64, err error) {
	r, err := NewReaderWithOptions(path, opts)
	if err != nil {
		return 0, false, 0, err
	}
	defer r.Close()

	var lastLSN uint64
	for {
		start, seekErr := r.f.Seek(0, io.SeekCurrent)
		if seekErr != nil {
			return 0, typed, completeEnd, seekErr
		}
		env, err := r.ReadCommandFrame()
		if err != nil {
			if errorsIsEOFOrTail(err) {
				return maxLSN, typed, start, nil
			}
			if errors.Is(err, ErrCommandWALLegacyPayload) && !typed {
				return 0, false, completeEnd, err
			}
			return 0, typed, completeEnd, err
		}
		if lastLSN != 0 && env.LSN <= lastLSN {
			return 0, true, completeEnd, ErrCommandWALDuplicateLSN
		}
		lastLSN = env.LSN
		typed = true
		if onLSN != nil {
			if err := onLSN(env.LSN); err != nil {
				return 0, typed, completeEnd, err
			}
		}
		if env.LSN > maxLSN {
			maxLSN = env.LSN
		}
		completeEnd, err = r.f.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, typed, completeEnd, err
		}
	}
}

func errorsIsEOFOrTail(err error) bool {
	return err == io.EOF || errors.Is(err, ErrCommandWALTerminalTail)
}
