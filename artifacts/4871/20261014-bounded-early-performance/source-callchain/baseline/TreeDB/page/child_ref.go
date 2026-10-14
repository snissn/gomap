package page

import (
	"encoding/binary"
	"fmt"
)

// ChildRefKind identifies the storage class of an internal-page child.
//
// TODO(treedb-format): This is currently modeled explicitly in memory. For
// on-disk internal pages, the kind is page-level rather than per child, and a
// future compact format can encode that more densely.
type ChildRefKind uint8

const (
	ChildRefPage ChildRefKind = iota
	ChildRefLeafLog
)

// LogRecordRef points to a record inside a persistent TreeDB value/leaf log.
//
// This is intentionally wider and more explicit than the historical packed
// LeafRef page-id encoding. TreeDB is pre-alpha, so prefer first-class fields
// over bit stealing here; compact page-local encodings can be added after the
// format is easier to reason about.
type LogRecordRef struct {
	FileID uint32
	Offset uint64
	// RecordLengthHint stores the value-log pointer length hint, including any
	// ValuePtr grouping/compression flags needed to reconstruct a ValuePtr.
	RecordLengthHint uint32
	SubIndex         uint16
}

// LeafLogPtr is retained as the public/internal name for leaf-log records while
// the underlying representation becomes explicit.
type LeafLogPtr = LogRecordRef

// ChildRef is a typed child reference used by tree/internal-node code.
type ChildRef struct {
	Kind ChildRefKind
	Page uint64
	Log  LogRecordRef
}

const LogRecordRefSize = 18 // u32 file | u64 offset | u32 length hint | u16 sub-index

func PageChildRef(pageID uint64) ChildRef {
	return ChildRef{Kind: ChildRefPage, Page: pageID}
}

func LeafLogChildRef(ptr LogRecordRef) ChildRef {
	return ChildRef{Kind: ChildRefLeafLog, Log: ptr}
}

func (r ChildRef) IsPage() bool {
	return r.Kind == ChildRefPage
}

func (r ChildRef) IsLeafLog() bool {
	return r.Kind == ChildRefLeafLog
}

func EncodeLogRecordRef(dst []byte, ref LogRecordRef) {
	_ = dst[LogRecordRefSize-1]
	binary.LittleEndian.PutUint32(dst[0:4], ref.FileID)
	binary.LittleEndian.PutUint64(dst[4:12], ref.Offset)
	binary.LittleEndian.PutUint32(dst[12:16], ref.RecordLengthHint)
	binary.LittleEndian.PutUint16(dst[16:18], ref.SubIndex)
}

func DecodeLogRecordRef(src []byte) LogRecordRef {
	_ = src[LogRecordRefSize-1]
	return LogRecordRef{
		FileID:           binary.LittleEndian.Uint32(src[0:4]),
		Offset:           binary.LittleEndian.Uint64(src[4:12]),
		RecordLengthHint: binary.LittleEndian.Uint32(src[12:16]),
		SubIndex:         binary.LittleEndian.Uint16(src[16:18]),
	}
}

func (ptr LogRecordRef) ValueLogFileID() uint32 {
	return ValueLogFileID(ptr.FileID)
}

func (ptr LogRecordRef) RecordLength() uint32 {
	return ValuePtrRecordLength(ValuePtr{Length: ptr.RecordLengthHint})
}

func (ptr LogRecordRef) IsGrouped() bool {
	return ValuePtrIsGrouped(ValuePtr{Length: ptr.RecordLengthHint})
}

func (ptr LogRecordRef) ValuePtr() ValuePtr {
	length := ptr.RecordLengthHint
	if ValuePtrIsGrouped(ValuePtr{Length: length}) {
		length = ValuePtrMarkGrouped(ValuePtrRecordLength(ValuePtr{Length: length}), uint8(ptr.SubIndex))
	}
	return ValuePtr{
		Offset: ptr.Offset,
		Length: length,
		FileID: ptr.ValueLogFileID(),
	}
}

func LeafLogPtrFromValuePtr(ptr ValuePtr) (LeafLogPtr, error) {
	if !IsValueLogFileID(ptr.FileID) {
		return LeafLogPtr{}, fmt.Errorf("page: leaf log pointer requires value-log file id, got %d", ptr.FileID)
	}
	return LeafLogPtr{
		Offset:           ptr.Offset,
		FileID:           ValueLogSegmentID(ptr.FileID),
		RecordLengthHint: ptr.Length,
		SubIndex:         uint16(ValuePtrSubIndex(ptr)),
	}, nil
}
