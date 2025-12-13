package iterator

type UnsafeIterator interface {
	Valid() bool
	Next()
	Seek(key []byte)
	UnsafeKey() []byte   // Returns a view (no copy)
	UnsafeValue() []byte // Returns a view (no copy), potentially triggering I/O
	Key() []byte         // Returns a copy (safe)
	Value() []byte       // Returns a copy (safe)
	IsDeleted() bool     // True if the current item is a tombstone
	Error() error
	Close() error
	Domain() (start, end []byte)
}
