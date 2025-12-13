package iterator

type UnsafeIterator interface {
	Valid() bool
	Next()
	Seek(key []byte)
	UnsafeKey() []byte   // Returns a view (no copy)
	UnsafeValue() []byte // Returns a view (no copy), potentially triggering I/O
	IsDeleted() bool     // True if the current item is a tombstone
	Close() error
}