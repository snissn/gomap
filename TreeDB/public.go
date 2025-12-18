package treedb

import (
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/compaction"
	"github.com/snissn/gomap/TreeDB/db"
)

// Options configures TreeDB. It is re-exported from TreeDB/db for convenience.
type Options = db.Options

// Mode selects which TreeDB implementation to open.
type Mode = db.Mode

const (
	ModeCached  = db.ModeCached
	ModeBackend = db.ModeBackend
)

// Iterator is the public iterator contract returned by TreeDB.
//
// Semantics (performance-first; callers must treat slices as read-only):
//   - Key() and Value() return views valid until the next Next()/Close().
//   - Use KeyCopy/ValueCopy if you need stable bytes.
type Iterator interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	KeyCopy(dst []byte) []byte
	ValueCopy(dst []byte) []byte
	Close() error
	Error() error
}

// Batch is the public batch contract returned by TreeDB.
// Both cached and backend implementations satisfy it.
type Batch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	Write() error
	WriteSync() error
	Close() error
	Replay(func(batch.Entry) error) error
	GetByteSize() (int, error)
}

// DB is the public TreeDB handle. It can represent either cached mode (default)
// or backend-only mode depending on Options.
type DB struct {
	mode    Mode
	cached  *caching.DB
	backend *db.DB
}

func (db *DB) ensureOpen() error {
	if db == nil || (db.cached == nil && db.backend == nil) {
		return ErrClosed
	}
	return nil
}

// Open opens TreeDB. By default it enables caching (write-back layer).
// To open the backend-only engine, set opts.Mode = ModeBackend.
func Open(opts Options) (*DB, error) {
	// Cached mode writes to the backend in large flush batches, so commit sequence
	// advances much more slowly than "number of writes". A large KeepRecent value
	// can therefore delay page reuse for a very long time (and cause index.db to
	// balloon under update-heavy workloads). Default to aggressive reuse in cached
	// mode unless the caller specifies otherwise.
	if opts.KeepRecent == 0 && opts.Mode != ModeBackend {
		opts.KeepRecent = 1
	}

	backend, err := db.Open(opts)
	if err != nil {
		return nil, err
	}

	if opts.Mode == ModeBackend {
		return &DB{mode: ModeBackend, backend: backend}, nil
	}

	cached, err := caching.Open(opts.Dir, backend, caching.Options{
		FlushThreshold:          opts.FlushThreshold,
		MaxQueuedMemtables:      opts.MaxQueuedMemtables,
		SlowdownBacklogSeconds:  opts.SlowdownBacklogSeconds,
		StopBacklogSeconds:      opts.StopBacklogSeconds,
		MaxBacklogBytes:         opts.MaxBacklogBytes,
		WriterFlushMaxMemtables: opts.WriterFlushMaxMemtables,
		WriterFlushMaxDuration:  opts.WriterFlushMaxDuration,
	})
	if err != nil {
		_ = backend.Close()
		return nil, err
	}

	return &DB{mode: ModeCached, cached: cached, backend: backend}, nil
}

// OpenCached is an explicit cached-mode opener (alias of Open with ModeCached).
func OpenCached(opts Options) (*DB, error) {
	opts.Mode = ModeCached
	return Open(opts)
}

// OpenBackend opens TreeDB in backend-only mode (no caching).
func OpenBackend(opts Options) (*DB, error) {
	opts.Mode = ModeBackend
	// Backend-only mode is primarily used for correctness tests and profiling the
	// core engine. Keep background pruning off by default to avoid introducing
	// concurrent allocator work into single-op write benchmarks (callers can opt
	// in by using Open with ModeBackend).
	opts.DisableBackgroundPrune = true
	return Open(opts)
}

// Close closes the DB.
func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	if db.cached != nil {
		err := db.cached.Close()
		db.cached = nil
		db.backend = nil
		return err
	}
	if db.backend != nil {
		err := db.backend.Close()
		db.backend = nil
		return err
	}
	return nil
}

// Get returns the value for a key.
//
// Semantics (performance-first): the returned slice may be a read-only view into
// internal storage (e.g. mmapped slabs) and must not be modified by the caller.
// If you need stable bytes independent of TreeDB internals, copy the slice.
func (db *DB) Get(key []byte) ([]byte, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.cached != nil {
		return db.cached.Get(key)
	}
	return db.backend.Get(key)
}

func (db *DB) Has(key []byte) (bool, error) {
	if err := db.ensureOpen(); err != nil {
		return false, err
	}
	if db.cached != nil {
		return db.cached.Has(key)
	}
	return db.backend.Has(key)
}

func (db *DB) Set(key, value []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.Set(key, value)
	}
	return db.backend.Set(key, value)
}

func (db *DB) SetSync(key, value []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.SetSync(key, value)
	}
	return db.backend.SetSync(key, value)
}

func (db *DB) Delete(key []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.Delete(key)
	}
	return db.backend.Delete(key)
}

func (db *DB) DeleteSync(key []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.DeleteSync(key)
	}
	return db.backend.DeleteSync(key)
}

func (db *DB) Iterator(start, end []byte) (Iterator, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.cached != nil {
		return db.cached.Iterator(start, end)
	}
	return db.backend.Iterator(start, end)
}

func (db *DB) ReverseIterator(start, end []byte) (Iterator, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.cached != nil {
		return db.cached.ReverseIterator(start, end)
	}
	return db.backend.ReverseIterator(start, end)
}

func (db *DB) NewBatch() Batch {
	if db == nil || (db.cached == nil && db.backend == nil) {
		return nil
	}
	if db.cached != nil {
		return db.cached.NewBatch()
	}
	return db.backend.NewBatch()
}

func (db *DB) NewBatchWithSize(size int) Batch {
	if db == nil || (db.cached == nil && db.backend == nil) {
		return nil
	}
	if db.cached != nil {
		return db.cached.NewBatchWithSize(size)
	}
	return db.backend.NewBatchWithSize(size)
}

func (db *DB) Stats() map[string]string {
	if db == nil || (db.cached == nil && db.backend == nil) {
		return nil
	}
	if db.cached != nil {
		return db.cached.Stats()
	}
	return db.backend.Stats()
}

func (db *DB) Print() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.Print()
	}
	return db.backend.Print()
}

// CompactCandidates runs slab compaction based on the provided selection options.
// In cached mode it will also perform bounded flush assist when the caching layer
// is under backpressure, so compaction does not starve the foreground flush path.
func (db *DB) CompactCandidates(opts compaction.Options) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.backend == nil {
		return ErrClosed
	}

	if db.cached != nil {
		// Kick the flusher once up front, and wire compaction to periodically
		// perform bounded flush assist when backlog is high.
		db.cached.CompactionAssist()
		userAssist := opts.Assist
		opts.Assist = func() {
			db.cached.CompactionAssist()
			if userAssist != nil {
				userAssist()
			}
		}
	}

	c := compaction.New(db.backend)
	return c.CompactCandidates(opts)
}

// CompactIndex performs an in-place index vacuum (bulk rebuild) on the backend.
// In cached mode it first drains the caching layer so the backend reflects all
// buffered writes before rebuilding.
func (db *DB) CompactIndex() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.backend == nil {
		return ErrClosed
	}

	if db.cached != nil {
		if err := db.cached.Drain(); err != nil {
			return err
		}
	}
	return db.backend.CompactIndex()
}

// FragmentationReport returns best-effort structural stats about the on-disk user
// index that help diagnose scan regressions after churn.
//
// Note: In cached mode this reflects the backend state only; queued memtables are
// not included unless the caller has explicitly drained the cache (e.g. via
// close+reopen or a maintenance operation that drains).
func (db *DB) FragmentationReport() (map[string]string, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.backend == nil {
		return nil, ErrClosed
	}
	return db.backend.FragmentationReport()
}
