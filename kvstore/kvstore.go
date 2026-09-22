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

// MultiGetter is an optional capability for batched point reads.
type MultiGetter interface {
	GetMany(keys [][]byte) ([][]byte, error)
}

// MultiGetViewFunc receives one MultiGetterView result. The callback order is
// unspecified and implementations may invoke callbacks concurrently; callers
// that mutate shared state must synchronize it. The index argument identifies
// the input key. The value slice is a read-only view that is valid only until
// the callback returns; callers must copy it before retaining. Missing keys are
// reported with found=false and value=nil.
type MultiGetViewFunc func(index int, key []byte, value []byte, found bool) error

// MultiGetterView is an optional lower-allocation batched point-read capability
// for callers that can consume each value before the callback returns.
type MultiGetterView interface {
	GetManyView(keys [][]byte, fn MultiGetViewFunc) error
}

// BatchReader is an optional capability for batched read execution where
// callers only need completion/error, not materialized values.
//
// Semantics:
//   - Missing keys are not errors (same model as GetMany nil entries).
//   - Duplicate keys are not errors (implementations may deduplicate).
//   - Empty key slices are a no-op and must return nil.
//   - If a batch-read mechanism is unavailable for the current DB state
//     (for example, snapshot acquisition fails), return ErrUnsupported.
//   - Errors should represent batch-level failures, not per-key absence.
type BatchReader interface {
	ReadBatch(keys [][]byte) error
}

// ReadSnapshot is an optional point-read snapshot used by benchmarks to
// measure snapshot-amortized read throughput.
type ReadSnapshot interface {
	Get(key []byte) ([]byte, error)
	GetAppend(key, dst []byte) ([]byte, error)
	Close() error
}

// ReadSnapshotter is an optional capability for acquiring a point-read
// snapshot that can be reused across many reads.
type ReadSnapshotter interface {
	AcquireReadSnapshot() (ReadSnapshot, error)
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
//
// Key()/Value() may return read-only views that are valid only until the next
// Next()/Close() on the same iterator. KeyCopy()/ValueCopy() return
// caller-owned stable bytes, reusing dst capacity when possible.
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

// BatchRangeDeleter is an optional batch capability for half-open range
// deletion. Start is inclusive, end is exclusive; nil bounds mean unbounded
// when the underlying engine supports that. Implementations that emulate range
// deletion with iteration/point deletes should report that via
// RangeDeleteModeReporter rather than claiming native parity.
type BatchRangeDeleter interface {
	DeleteRange(start, end []byte) error
}

const (
	// RangeDeleteModeNative indicates the adapter uses an engine-native range
	// deletion primitive for DeleteRange batches.
	RangeDeleteModeNative = "native"
	// RangeDeleteModeFallbackIteratorDelete indicates the adapter expands
	// DeleteRange into iterator-driven point deletes.
	RangeDeleteModeFallbackIteratorDelete = "fallback_iterator_delete"
)

// RangeDeleteModeReporter reports whether batch DeleteRange is native or a
// fallback path for benchmark/reporting consumers.
type RangeDeleteModeReporter interface {
	RangeDeleteMode() string
}

// Batcher is an optional capability for batched writes.
type Batcher interface {
	NewBatch() (Batch, error)
}
