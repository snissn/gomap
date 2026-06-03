package iterator

import "github.com/snissn/gomap/TreeDB/page"

type UnsafeIterator interface {
	Valid() bool
	Next()
	Seek(key []byte)
	// UnsafeKey returns a view into underlying storage (no copy).
	// The returned slice is valid until the next Next()/Seek()/Close() on this iterator.
	UnsafeKey() []byte
	// UnsafeValue returns a view into underlying storage (no copy) and may trigger I/O.
	// The returned slice is valid until the next Next()/Seek()/Close() on this iterator.
	UnsafeValue() []byte
	UnsafeEntry() (val []byte, ptr page.ValuePtr, flags byte) // Returns raw entry details
	// Key and Value are the fast-path accessors: they return views and follow the
	// same lifetime rules as UnsafeKey/UnsafeValue.
	Key() []byte
	Value() []byte
	// KeyCopy/ValueCopy copy the current key/value into dst[:0] and return
	// caller-owned bytes that remain stable after iterator movement/close.
	KeyCopy(dst []byte) []byte
	ValueCopy(dst []byte) []byte
	IsDeleted() bool // True if the current item is a tombstone
	Error() error
	Close() error
	Domain() (start, end []byte)
}
