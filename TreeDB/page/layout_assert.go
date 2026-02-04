package page

import "unsafe"

// Compile-time assertions for the struct layouts assumed by the fast-path
// little-endian Encode/Decode implementations (see PageHeader.Encode,
// DecodeHeader, ValuePtr.Encode, DecodeValuePtr).
//
// These prevent accidental changes (field reordering/types/padding) from
// silently corrupting on-disk formats.
var (
	// Size checks.
	_ [PageHeaderSize - int(unsafe.Sizeof(PageHeader{}))]byte
	_ [int(unsafe.Sizeof(PageHeader{})) - PageHeaderSize]byte
	_ [ValuePtrSize - int(unsafe.Sizeof(ValuePtr{}))]byte
	_ [int(unsafe.Sizeof(ValuePtr{})) - ValuePtrSize]byte

	// PageHeader field offsets: 0, 8, 12, 14.
	_ [int(unsafe.Offsetof(PageHeader{}.PageID)) - 0]byte
	_ [0 - int(unsafe.Offsetof(PageHeader{}.PageID))]byte
	_ [int(unsafe.Offsetof(PageHeader{}.Checksum)) - 8]byte
	_ [8 - int(unsafe.Offsetof(PageHeader{}.Checksum))]byte
	_ [int(unsafe.Offsetof(PageHeader{}.Flags)) - 12]byte
	_ [12 - int(unsafe.Offsetof(PageHeader{}.Flags))]byte
	_ [int(unsafe.Offsetof(PageHeader{}.Count)) - 14]byte
	_ [14 - int(unsafe.Offsetof(PageHeader{}.Count))]byte

	// ValuePtr field offsets: 0, 8, 12.
	_ [int(unsafe.Offsetof(ValuePtr{}.Offset)) - 0]byte
	_ [0 - int(unsafe.Offsetof(ValuePtr{}.Offset))]byte
	_ [int(unsafe.Offsetof(ValuePtr{}.Length)) - 8]byte
	_ [8 - int(unsafe.Offsetof(ValuePtr{}.Length))]byte
	_ [int(unsafe.Offsetof(ValuePtr{}.FileID)) - 12]byte
	_ [12 - int(unsafe.Offsetof(ValuePtr{}.FileID))]byte
)
