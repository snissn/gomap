// Package gethethdb adapts TreeDB to go-ethereum's ethdb.KeyValueStore.
//
// The adapter is intentionally kept in an integration submodule so gomap's
// root module and core TreeDB packages do not depend on go-ethereum. It uses
// TreeDB's native raw-KV byte-string semantics: nil point keys are canonicalized
// by TreeDB to empty keys, nil values to zero-length values, and nil range
// bounds remain unbounded.
package gethethdb

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/ethdb"
	treedb "github.com/snissn/gomap/TreeDB"
)

// Adapter errors are aliases to TreeDB errors so callers can use errors.Is.
var (
	ErrClosed   = treedb.ErrClosed
	ErrNotFound = treedb.ErrKeyNotFound
)

// OpenOptions configures Open.
type OpenOptions struct {
	// Profile selects a public TreeDB profile. Empty defaults to
	// treedb.ProfileCommandWALDurable. Non-command-WAL write profiles are
	// rejected because this adapter is intended for geth/Nitro persistent KV
	// durability through TreeDB command WAL.
	Profile treedb.Profile

	// Options, when non-nil, supplies the exact TreeDB options to use. Open copies
	// the struct, forces Dir to the Open path when path is non-empty, applies
	// ReadOnly/KeepRecent/MemtableMode overrides below, and still requires
	// CommandWAL for writable opens.
	Options *treedb.Options

	// ReadOnly opens the TreeDB directory read-only.
	ReadOnly bool

	// KeepRecent and MemtableMode are optional TreeDB knobs useful for downstream
	// integrations. Zero/empty leave the selected profile defaults intact.
	KeepRecent   uint64
	MemtableMode string
}

// DefaultOpenOptions returns the production-oriented adapter defaults.
func DefaultOpenOptions() OpenOptions {
	return OpenOptions{Profile: treedb.ProfileCommandWALDurable}
}

// Open opens a TreeDB-backed ethdb.KeyValueStore at path.
func Open(path string, options *OpenOptions) (*Database, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("gethethdb: path must be non-empty")
	}
	opts, err := resolveTreeDBOptions(path, options)
	if err != nil {
		return nil, err
	}
	if opts.ReadOnly {
		info, statErr := os.Stat(opts.Dir)
		if statErr != nil {
			return nil, fmt.Errorf("gethethdb: read-only TreeDB directory check: %w", statErr)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("gethethdb: read-only TreeDB path is not a directory: %s", opts.Dir)
		}
	} else if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("gethethdb: create TreeDB directory: %w", err)
	}
	return OpenWithOptions(opts)
}

// OpenWithOptions opens TreeDB with caller-supplied options and wraps it as an
// ethdb.KeyValueStore. Writable options must enable TreeDB command WAL.
func OpenWithOptions(opts treedb.Options) (*Database, error) {
	if strings.TrimSpace(opts.Dir) == "" {
		return nil, errors.New("gethethdb: TreeDB options Dir must be non-empty")
	}
	if !opts.ReadOnly && !opts.CommandWAL {
		return nil, errors.New("gethethdb: writable TreeDB ethdb adapter requires command WAL")
	}
	tdb, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return Wrap(tdb), nil
}

func resolveTreeDBOptions(path string, options *OpenOptions) (treedb.Options, error) {
	var opts treedb.Options
	if options != nil && options.Options != nil {
		opts = *options.Options
		if path != "" {
			opts.Dir = path
		}
	} else {
		profile := treedb.ProfileCommandWALDurable
		if options != nil && options.Profile != "" {
			normalized, ok := treedb.ParsePublicProfile(string(options.Profile), treedb.ProfileCommandWALDurable)
			if !ok {
				return treedb.Options{}, fmt.Errorf("gethethdb: unsupported TreeDB profile %q; use %s", options.Profile, treedb.ProfileFlagHelp)
			}
			profile = normalized
		}
		opts = treedb.OptionsFor(profile, path)
	}
	if options != nil {
		if options.ReadOnly {
			opts.ReadOnly = true
		}
		if options.KeepRecent != 0 {
			opts.KeepRecent = options.KeepRecent
		}
		if mode := strings.TrimSpace(options.MemtableMode); mode != "" {
			opts.MemtableMode = strings.ToLower(mode)
		}
	}
	if strings.TrimSpace(opts.Dir) == "" {
		return treedb.Options{}, errors.New("gethethdb: TreeDB options Dir must be non-empty")
	}
	if !opts.ReadOnly && !opts.CommandWAL {
		return treedb.Options{}, errors.New("gethethdb: writable TreeDB ethdb adapter requires command WAL")
	}
	return opts, nil
}

// Wrap adapts an already-open TreeDB handle. The caller owns the handle through
// the returned Database; Close closes the wrapped TreeDB handle.
func Wrap(db *treedb.DB) *Database {
	return &Database{db: db}
}

// Database implements ethdb.KeyValueStore on top of TreeDB.
type Database struct {
	db     *treedb.DB
	closed atomic.Bool
}

var _ ethdb.KeyValueStore = (*Database)(nil)

func (d *Database) tree() (*treedb.DB, error) {
	if d == nil || d.db == nil || d.closed.Load() {
		return nil, ErrClosed
	}
	return d.db, nil
}

// TreeDB exposes the wrapped TreeDB handle for integration tests and advanced
// maintenance tooling. Callers must not close it separately from Database.Close.
func (d *Database) TreeDB() *treedb.DB {
	if d == nil {
		return nil
	}
	return d.db
}

// Close closes the wrapped TreeDB handle. It is safe to call more than once.
func (d *Database) Close() error {
	if d == nil {
		return nil
	}
	if !d.closed.CompareAndSwap(false, true) {
		return nil
	}
	if d.db == nil {
		return nil
	}
	return d.db.Close()
}

// Has retrieves if a key is present in the key-value data store.
func (d *Database) Has(key []byte) (bool, error) {
	tdb, err := d.tree()
	if err != nil {
		return false, err
	}
	return tdb.Has(key)
}

// Get retrieves the given key if it is present.
func (d *Database) Get(key []byte) ([]byte, error) {
	tdb, err := d.tree()
	if err != nil {
		return nil, err
	}
	value, err := tdb.GetAppend(key, nil)
	if errors.Is(err, treedb.ErrKeyNotFound) {
		return nil, ErrNotFound
	}
	return value, err
}

// Put inserts the given value into the key-value data store.
func (d *Database) Put(key []byte, value []byte) error {
	tdb, err := d.tree()
	if err != nil {
		return err
	}
	return tdb.Set(key, value)
}

// Delete removes the key from the key-value data store.
func (d *Database) Delete(key []byte) error {
	tdb, err := d.tree()
	if err != nil {
		return err
	}
	return tdb.Delete(key)
}

// DeleteRange deletes all keys in [start, end). Nil bounds remain unbounded;
// empty bounds are passed through as concrete empty byte-string bounds.
func (d *Database) DeleteRange(start, end []byte) error {
	tdb, err := d.tree()
	if err != nil {
		return err
	}
	return tdb.DeleteRange(start, end)
}

// Stat returns TreeDB diagnostic stats as a stable key=value text block.
func (d *Database) Stat() (string, error) {
	tdb, err := d.tree()
	if err != nil {
		return "", err
	}
	stats := tdb.Stats()
	if stats == nil {
		return "", nil
	}
	keys := make([]string, 0, len(stats))
	for key := range stats {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(stats[key])
		b.WriteByte('\n')
	}
	return b.String(), nil
}

// SyncKeyValue forces a TreeDB checkpoint/durability boundary for pending raw-KV
// command-WAL writes.
func (d *Database) SyncKeyValue() error {
	tdb, err := d.tree()
	if err != nil {
		return err
	}
	return tdb.Checkpoint()
}

// NewBatch creates a write-only batch. After Database.Close it still returns a
// batch object so geth's operations-after-close expectations are preserved;
// that batch accepts queued mutations but Write returns ErrClosed.
func (d *Database) NewBatch() ethdb.Batch {
	return d.newBatch(0)
}

// NewBatchWithSize creates a write-only batch with a best-effort size hint.
func (d *Database) NewBatchWithSize(size int) ethdb.Batch {
	return d.newBatch(size)
}

func (d *Database) newBatch(size int) ethdb.Batch {
	if d == nil || d.db == nil || d.closed.Load() {
		return newClosedBatch(ErrClosed)
	}
	var inner treedb.Batch
	if size > 0 {
		inner = d.db.NewBatchWithSize(size)
	} else {
		inner = d.db.NewBatch()
	}
	if inner == nil {
		return newClosedBatch(ErrClosed)
	}
	return &Batch{db: d, inner: inner}
}

// NewIterator creates a binary-alphabetical iterator over keys with prefix,
// starting at prefix||start.
func (d *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	tdb, err := d.tree()
	if err != nil {
		return &Iterator{err: err, released: true}
	}
	inner, err := tdb.Iterator(iteratorLowerBound(prefix, start), prefixUpperBound(prefix))
	if err != nil {
		return &Iterator{err: err, released: true}
	}
	return &Iterator{inner: inner}
}

// Compact compacts TreeDB's index. TreeDB's public compaction primitive is
// currently whole-index, so start/limit are accepted for ethdb compatibility but
// not used to narrow the compaction.
func (d *Database) Compact(start []byte, limit []byte) error {
	tdb, err := d.tree()
	if err != nil {
		return err
	}
	return tdb.CompactIndex()
}

func iteratorLowerBound(prefix, start []byte) []byte {
	if len(prefix) == 0 {
		if start == nil {
			return nil
		}
		return append([]byte(nil), start...)
	}
	lower := make([]byte, len(prefix)+len(start))
	copy(lower, prefix)
	copy(lower[len(prefix):], start)
	return lower
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	limit := append([]byte(nil), prefix...)
	for i := len(limit) - 1; i >= 0; i-- {
		if limit[i] == 0xff {
			continue
		}
		limit[i]++
		return limit[:i+1]
	}
	return nil
}

// Batch implements ethdb.Batch.
type Batch struct {
	db     *Database
	inner  treedb.Batch
	size   int
	closed error
	ops    []batchOp
}

var _ ethdb.Batch = (*Batch)(nil)

type batchOpKind uint8

const (
	batchOpPut batchOpKind = iota
	batchOpDelete
	batchOpDeleteRange
)

type batchOp struct {
	kind  batchOpKind
	key   []byte
	value []byte
}

func (b *Batch) recordOp(op batchOp) {
	b.ops = append(b.ops, op)
}

func (b *Batch) applyOpToInner(op batchOp) error {
	if b == nil || b.inner == nil {
		return ErrClosed
	}
	switch op.kind {
	case batchOpPut:
		return b.inner.Set(op.key, op.value)
	case batchOpDelete:
		return b.inner.Delete(op.key)
	case batchOpDeleteRange:
		return b.inner.DeleteRange(op.key, op.value)
	default:
		return fmt.Errorf("gethethdb: unknown batch op %d", op.kind)
	}
}

func newClosedBatch(err error) *Batch {
	if err == nil {
		err = ErrClosed
	}
	return &Batch{closed: err}
}

// Put inserts the given value into the batch.
func (b *Batch) Put(key []byte, value []byte) error {
	if b == nil {
		return ErrClosed
	}
	if b.inner == nil {
		b.recordOp(batchOp{kind: batchOpPut, key: cloneBytes(key), value: cloneBytes(value)})
		b.size += len(key) + len(value)
		return nil
	}
	if err := b.inner.Set(key, value); err != nil {
		return err
	}
	b.recordOp(batchOp{kind: batchOpPut, key: cloneBytes(key), value: cloneBytes(value)})
	b.size += len(key) + len(value)
	return nil
}

// Delete inserts the key removal into the batch.
func (b *Batch) Delete(key []byte) error {
	if b == nil {
		return ErrClosed
	}
	if b.inner == nil {
		b.recordOp(batchOp{kind: batchOpDelete, key: cloneBytes(key)})
		b.size += len(key)
		return nil
	}
	if err := b.inner.Delete(key); err != nil {
		return err
	}
	b.recordOp(batchOp{kind: batchOpDelete, key: cloneBytes(key)})
	b.size += len(key)
	return nil
}

// DeleteRange records a range deletion in [start, end).
func (b *Batch) DeleteRange(start, end []byte) error {
	if b == nil {
		return ErrClosed
	}
	if b.inner == nil {
		b.recordOp(batchOp{kind: batchOpDeleteRange, key: cloneBytes(start), value: cloneBytes(end)})
		b.size += len(start) + len(end)
		return nil
	}
	if err := b.inner.DeleteRange(start, end); err != nil {
		return err
	}
	b.recordOp(batchOp{kind: batchOpDeleteRange, key: cloneBytes(start), value: cloneBytes(end)})
	b.size += len(start) + len(end)
	return nil
}

// ValueSize returns the total key/value bytes queued in this batch, matching
// geth adapter convention rather than TreeDB's internal encoded byte size.
func (b *Batch) ValueSize() int {
	if b == nil {
		return 0
	}
	return b.size
}

// Write flushes the batch to TreeDB.
func (b *Batch) Write() error {
	if b == nil || b.inner == nil {
		if b != nil && b.closed != nil {
			return b.closed
		}
		return ErrClosed
	}
	if b.db == nil || b.db.closed.Load() {
		return ErrClosed
	}
	if err := b.inner.Write(); err != nil {
		return err
	}
	return b.rebuildInnerFromOps()
}

func (b *Batch) rebuildInnerFromOps() error {
	if b == nil {
		return ErrClosed
	}
	if b.db == nil || b.db.closed.Load() || b.db.db == nil {
		b.closed = ErrClosed
		return ErrClosed
	}
	if b.inner != nil {
		if resetter, ok := b.inner.(interface{ Reset() }); ok {
			resetter.Reset()
		} else {
			_ = b.inner.Close()
			b.inner = b.db.db.NewBatchWithSize(len(b.ops))
		}
	} else {
		b.inner = b.db.db.NewBatchWithSize(len(b.ops))
	}
	if b.inner == nil {
		b.closed = ErrClosed
		return ErrClosed
	}
	for _, op := range b.ops {
		if err := b.applyOpToInner(op); err != nil {
			return err
		}
	}
	return nil
}

// Reset resets the batch for reuse.
func (b *Batch) Reset() {
	if b == nil {
		return
	}
	b.size = 0
	b.ops = b.ops[:0]
	if b.inner == nil {
		return
	}
	if resetter, ok := b.inner.(interface{ Reset() }); ok {
		resetter.Reset()
		return
	}
	_ = b.inner.Close()
	b.inner = nil
	if b.db == nil || b.db.closed.Load() || b.db.db == nil {
		b.closed = ErrClosed
		return
	}
	b.inner = b.db.db.NewBatch()
	if b.inner == nil {
		b.closed = ErrClosed
	}
}

// Replay replays the batch contents in submitted order. TreeDB's internal
// batch replay may compact or sort point-only batches, so the adapter keeps a
// small copied operation log specifically for ethdb Replay semantics.
func (b *Batch) Replay(w ethdb.KeyValueWriter) error {
	if b == nil {
		return nil
	}
	if w == nil {
		return errors.New("gethethdb: nil replay writer")
	}
	return replayOps(b.ops, w)
}

func replayOps(ops []batchOp, w ethdb.KeyValueWriter) error {
	for _, op := range ops {
		switch op.kind {
		case batchOpPut:
			if err := w.Put(op.key, op.value); err != nil {
				return err
			}
		case batchOpDelete:
			if err := w.Delete(op.key); err != nil {
				return err
			}
		case batchOpDeleteRange:
			rangeDeleter, ok := w.(ethdb.KeyValueRangeDeleter)
			if !ok {
				return errors.New("ethdb.KeyValueWriter does not implement DeleteRange")
			}
			if err := rangeDeleter.DeleteRange(op.key, op.value); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close releases the underlying TreeDB batch when present. It matches newer
// geth ethdb.Batch interfaces and is harmless as an extra method for older
// geth versions where Batch has no Close method.
func (b *Batch) Close() {
	if b == nil || b.inner == nil {
		return
	}
	_ = b.inner.Close()
	b.inner = nil
	b.closed = ErrClosed
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}

// Iterator implements ethdb.Iterator.
type Iterator struct {
	inner    treedb.Iterator
	err      error
	started  bool
	released bool
}

var _ ethdb.Iterator = (*Iterator)(nil)

// Next moves the iterator to the next key/value pair.
func (it *Iterator) Next() bool {
	if it == nil || it.released || it.inner == nil || it.err != nil {
		return false
	}
	if !it.started {
		it.started = true
		return it.inner.Valid()
	}
	it.inner.Next()
	return it.inner.Valid()
}

// Error returns any accumulated iterator error.
func (it *Iterator) Error() error {
	if it == nil {
		return nil
	}
	if it.err != nil {
		return it.err
	}
	if it.inner == nil {
		return nil
	}
	return it.inner.Error()
}

// Key returns the current key view.
func (it *Iterator) Key() []byte {
	if it == nil || it.released || it.inner == nil || !it.started || !it.inner.Valid() {
		return nil
	}
	return it.inner.Key()
}

// Value returns the current value view.
func (it *Iterator) Value() []byte {
	if it == nil || it.released || it.inner == nil || !it.started || !it.inner.Valid() {
		return nil
	}
	return it.inner.Value()
}

// Release releases iterator resources. It is idempotent.
func (it *Iterator) Release() {
	if it == nil || it.released {
		return
	}
	it.released = true
	if it.inner != nil {
		if err := it.inner.Close(); err != nil && it.err == nil {
			it.err = err
		}
		it.inner = nil
	}
}
