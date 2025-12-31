package kvstore

import "errors"

// ErrUnsupported is returned by optional capabilities that are not implemented
// by a particular engine (e.g. ordered iteration for HashDB).
var ErrUnsupported = errors.New("kvstore: unsupported")

// DB is the minimal common interface shared across all engines.
type DB interface {
	Name() string
	Close() error

	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
}

// Haser is an optional capability for checking existence without retrieving values.
type Haser interface {
	Has(key []byte) (bool, error)
}

// Syncer is an optional capability for durability-oriented writes.
type Syncer interface {
	SetSync(key, value []byte) error
	DeleteSync(key []byte) error
}

// StatsProvider is an optional capability for exposing engine stats.
type StatsProvider interface {
	Stats() map[string]string
}

// Printer is an optional capability for debug printing.
type Printer interface {
	Print() error
}

// Iterator is a forward-only iterator over key/value pairs.
type Iterator interface {
	Valid() bool
	Next()

	Key() []byte
	Value() []byte
	KeyCopy(dst []byte) []byte
	ValueCopy(dst []byte) []byte

	Error() error
	Close() error
}

// RangeScanner is an optional capability for ordered iteration over a key domain.
// Start is inclusive, End is exclusive; nil means unbounded.
type RangeScanner interface {
	Iterator(start, end []byte) (Iterator, error)
	ReverseIterator(start, end []byte) (Iterator, error)
}

// ForEacher is an optional capability for visiting all live key/value pairs.
// Iteration order is engine-defined and may be arbitrary.
type ForEacher interface {
	ForEach(func(key, value []byte) error) error
}

// Batch is a buffered write unit.
type Batch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	Commit() error
	CommitSync() error
	Close() error
}

// Batcher is an optional capability for batched writes.
type Batcher interface {
	NewBatch() (Batch, error)
}

// BatcherWithSize is an optional capability for batched writes with a size
// hint (in bytes) for internal buffering.
type BatcherWithSize interface {
	NewBatchWithSize(size int) (Batch, error)
}
