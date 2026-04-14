package page

import "fmt"

// LeafLogPtr points to a leaf page stored in the external leaf log.
//
// The FileID is the raw segment identifier in the leaf-log namespace. It is
// intentionally distinct from ValuePtr.FileID, which carries the value-log
// marker bit for payload lookups.
type LeafLogPtr struct {
	Offset uint64
	FileID uint32
}

func (ptr LeafLogPtr) ValueLogFileID() uint32 {
	return ValueLogFileID(ptr.FileID)
}

func (ptr LeafLogPtr) ValuePtr() ValuePtr {
	return ValuePtr{
		Offset: ptr.Offset,
		Length: leafRefPtrLength,
		FileID: ptr.ValueLogFileID(),
	}
}

func LeafLogPtrFromValuePtr(ptr ValuePtr) (LeafLogPtr, error) {
	if !IsValueLogFileID(ptr.FileID) {
		return LeafLogPtr{}, fmt.Errorf("page: leaf log pointer requires value-log file id, got %d", ptr.FileID)
	}
	return LeafLogPtr{
		Offset: ptr.Offset,
		FileID: ValueLogSegmentID(ptr.FileID),
	}, nil
}
