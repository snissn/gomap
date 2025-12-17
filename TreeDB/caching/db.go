package caching

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/internal/wal"
)

var ErrKeyEmpty = fmt.Errorf("key cannot be empty")
var ErrValueNil = fmt.Errorf("value cannot be nil")
var ErrBatchClosed = fmt.Errorf("batch has been written or closed")

const maxQueuedMemtables = 4

var iteratorDebugEnabled atomic.Bool

// SetIteratorDebug toggles attaching debug metadata to iterators returned by
// CachingDB.Iterator. It is intended for benchmarking/diagnostics.
func SetIteratorDebug(enabled bool) {
	iteratorDebugEnabled.Store(enabled)
}

func bytesToStringNoCopy(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// BackendDB defines the subset of treedb.DB needed by CachingDB.
type BackendDB interface {
	Get(key []byte) ([]byte, error)
	Iterator(start, end []byte) (iterator.UnsafeIterator, error)
	ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error)
	NewBatch() batch.Interface
	Close() error
	Print() error
	Stats() map[string]string
}

type DB struct {
	mu      sync.RWMutex
	flushMu sync.Mutex

	// Level 0 (Memory)
	mutable           *memtable.Memtable
	queue             []*memtable.Memtable
	mutableRange      keyRange
	queueRanges       []keyRange
	queueWALPaths     []string
	backendRange      keyRange
	backendRangeKnown bool

	// Durability
	wal     *wal.Writer
	walPath string
	walSeq  int // Sequence number for WAL files

	// Level 1 (Disk)
	backend BackendDB

	// Config
	dir            string
	flushThreshold int64

	// Lifecycle
	closeCh chan struct{}
	flushCh chan struct{}
	wg      sync.WaitGroup
}

type keyRange struct {
	valid bool
	min   []byte
	max   []byte
}

func (r *keyRange) add(key []byte) {
	if key == nil {
		return
	}
	if !r.valid {
		r.valid = true
		r.min = append([]byte(nil), key...)
		r.max = append([]byte(nil), key...)
		return
	}
	if bytes.Compare(key, r.min) < 0 {
		r.min = append(r.min[:0], key...)
	}
	if bytes.Compare(key, r.max) > 0 {
		r.max = append(r.max[:0], key...)
	}
}

func rangesOverlap(a, b keyRange) bool {
	if !a.valid || !b.valid {
		return false
	}
	// [a.min, a.max] overlaps [b.min, b.max] iff neither is strictly before the other.
	if bytes.Compare(a.max, b.min) < 0 {
		return false
	}
	if bytes.Compare(a.min, b.max) > 0 {
		return false
	}
	return true
}

// overlapsQuery checks if the query range [start, end) overlaps with the keyRange [r.min, r.max].
// nil start means -inf, nil end means +inf.
func overlapsQuery(start, end []byte, r keyRange) bool {
	if !r.valid {
		return false
	}
	// Range is [r.min, r.max]
	// Query is [start, end)

	// Check if Range is strictly before Query: r.max < start
	if start != nil && bytes.Compare(r.max, start) < 0 {
		return false
	}

	// Check if Range is strictly after Query: r.min >= end
	// Note: end is exclusive, so if r.min == end, it's outside.
	if end != nil && bytes.Compare(r.min, end) >= 0 {
		return false
	}

	return true
}

func Open(dir string, backend BackendDB, flushThreshold int64) (*DB, error) {
	if flushThreshold <= 0 {
		flushThreshold = 64 * 1024 * 1024 // 64MB default
	}

	// Ensure wal dir exists
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, err
	}

	db := &DB{
		dir:            walDir,
		backend:        backend,
		flushThreshold: flushThreshold,
		mutable:        memtable.New(),
		closeCh:        make(chan struct{}),
		flushCh:        make(chan struct{}, 1),
	}

	// Open initial WAL
	if err := db.rotateWALLocked(); err != nil {
		return nil, err
	}

	// Start background flusher
	db.wg.Add(1)
	go db.flushLoop()

	// Initialize backend key range for safe scan fast-path decisions.
	if err := db.initBackendRange(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func (db *DB) initBackendRange() error {
	minIter, err := db.backend.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer minIter.Close()
	minIter.Seek(nil)

	r := keyRange{}
	if minIter.Valid() && !minIter.IsDeleted() {
		r.add(minIter.UnsafeKey())
	}

	maxIter, err := db.backend.ReverseIterator(nil, nil)
	if err != nil {
		// Backend doesn't support reverse iteration; disable backend-range-dependent optimizations.
		db.mu.Lock()
		db.backendRange = keyRange{}
		db.backendRangeKnown = false
		db.mu.Unlock()
		return nil
	}
	defer maxIter.Close()

	if maxIter.Valid() && !maxIter.IsDeleted() {
		r.add(maxIter.UnsafeKey())
	}

	db.mu.Lock()
	db.backendRange = r
	db.backendRangeKnown = true
	db.mu.Unlock()
	return nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	if db.mutable.Len() > 0 {
		_ = db.rotateMemtableLocked(true)
	}
	db.mu.Unlock()

	close(db.closeCh)
	db.wg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.wal != nil {
		db.wal.Close()
	}
	return db.backend.Close()
}
func (db *DB) Set(key, value []byte) error {
	if key == nil {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}
	return db.set(key, value, false)
}

func (db *DB) SetSync(key, value []byte) error {
	if key == nil {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}
	return db.set(key, value, true)
}

func (db *DB) set(key, value []byte, sync bool) error {
	db.mu.Lock()

	// 1. Insert to Memtable (Allocates in Arena) -> WAL Write (Reads from Arena)
	// This avoids reading 'key/value' (User Memory) twice.
	err := db.mutable.PutWithCallback(key, value, func(kView, vView []byte) error {
		if err := db.wal.Append(wal.OpSet, kView, vView); err != nil {
			return err
		}
		if sync {
			if err := db.wal.Sync(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.mu.Unlock()
		return err
	}

	db.mutableRange.add(key)

	// 3. Check Threshold
	needsBackpressureFlush := false
	if db.mutable.Size() > db.flushThreshold {
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			return err
		}
		needsBackpressureFlush = len(db.queue) > maxQueuedMemtables
	}
	db.mu.Unlock()

	// Backpressure: if writers outrun the background flusher, the queue grows and
	// each range iterator becomes a k-way merge. Force a durable flush to cap
	// merge fanout and preserve scan performance.
	if needsBackpressureFlush {
		db.flushAll(true)
	}
	return nil
}

func (db *DB) Delete(key []byte) error {
	if key == nil {
		return ErrKeyEmpty
	}
	return db.delete(key, false)
}

func (db *DB) DeleteSync(key []byte) error {
	if key == nil {
		return ErrKeyEmpty
	}
	return db.delete(key, true)
}

func (db *DB) delete(key []byte, sync bool) error {
	db.mu.Lock()

	// 1. Memtable -> WAL
	err := db.mutable.DeleteWithCallback(key, func(kView, vView []byte) error {
		// Value is empty for delete
		if err := db.wal.Append(wal.OpDelete, kView, nil); err != nil {
			return err
		}
		if sync {
			if err := db.wal.Sync(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.mu.Unlock()
		return err
	}

	db.mutableRange.add(key)

	// 3. Threshold
	needsBackpressureFlush := false
	if db.mutable.Size() > db.flushThreshold {
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			return err
		}
		needsBackpressureFlush = len(db.queue) > maxQueuedMemtables
	}
	db.mu.Unlock()

	if needsBackpressureFlush {
		db.flushAll(true)
	}
	return nil
}

func (db *DB) rotateMemtableLocked(triggerFlush bool) error {
	walPath := db.walPath
	db.queue = append(db.queue, db.mutable)
	db.queueRanges = append(db.queueRanges, db.mutableRange)
	db.queueWALPaths = append(db.queueWALPaths, walPath)
	db.mutable = memtable.New()
	db.mutableRange = keyRange{}

	// Optimization: Reuse WAL if small (e.g. < 10MB) to avoid syscall overhead
	// on frequent rotations (e.g. caused by frequent Iterator creation).
	if db.wal != nil && db.wal.Size() < 10*1024*1024 {
		if triggerFlush {
			select {
			case db.flushCh <- struct{}{}:
			default:
			}
		}
		return nil
	}

	if err := db.rotateWALLocked(); err != nil {
		return err
	}
	if triggerFlush {
		select {
		case db.flushCh <- struct{}{}:
		default:
		}
	}
	return nil
}

func (db *DB) rotateWALLocked() error {
	if db.wal != nil {
		db.wal.Close()
	}
	db.walSeq++
	name := fmt.Sprintf("wal-%06d.log", db.walSeq)
	path := filepath.Join(db.dir, name)
	w, err := wal.NewWriter(path)
	if err != nil {
		return err
	}
	db.wal = w
	db.walPath = path
	return nil
}

func (db *DB) flushLoop() {
	defer db.wg.Done()

	for {
		select {
		case <-db.closeCh:
			// Flush all remaining with sync on close.
			db.flushAll(true)
			return
		case <-db.flushCh:
			// Background flush uses synced backend commits so we can safely
			// retire WAL segments as they are applied.
			db.flushAll(true)
		}
	}
}

func (db *DB) flushAll(sync bool) {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()

	for db.flushOneLocked(sync) {
	}
}

func (db *DB) flushOne() bool {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	return db.flushOneLocked(true)
}

func (db *DB) flushOneLocked(sync bool) bool {
	db.mu.Lock()
	if len(db.queue) == 0 {
		db.mu.Unlock()
		return false
	}
	mem := db.queue[0]
	memRange := keyRange{}
	if len(db.queueRanges) > 0 {
		memRange = db.queueRanges[0]
	}
	walPath := ""
	if len(db.queueWALPaths) > 0 {
		walPath = db.queueWALPaths[0]
	}
	db.mu.Unlock()

	// Optimization: Skip flush for empty memtables (e.g. from frequent Iterator creation)
	if mem.Len() > 0 {
		// Flush 'mem' to backend
		backendBatch := db.backend.NewBatch()
		iter := mem.NewIterator(nil, nil) // Returns iterator.UnsafeIterator
		iter.Seek(nil)                    // Start

		// For larger memtables, bulk-load ops into the backend batch to reduce per-op overhead.
		if mem.Len() > 2000 {
			ops := make([]batch.Entry, 0, mem.Len())
			for iter.Valid() {
				// Zero-copy: UnsafeKey/Value point to memtable nodes (heap).
				// They are valid as long as memtable is reachable.
				// flushOneLocked holds 'mem' reference.
				// backendBatch.SetOps appends Entry structs (shallow copy of slices).
				// backendBatch.Write consumes them.
				// All within flushOneLocked (or until backendBatch.Write returns).
				// So this is safe.

				if iter.IsDeleted() {
					ops = append(ops, batch.Entry{
						Type: batch.OpDelete,
						Key:  iter.UnsafeKey(),
					})
				} else {
					ops = append(ops, batch.Entry{
						Type:  batch.OpPut,
						Key:   iter.UnsafeKey(),
						Value: iter.UnsafeValue(),
					})
				}
				iter.Next()
			}
			if err := backendBatch.SetOps(ops); err != nil {
				fmt.Fprintf(os.Stderr, "cachingdb: flush failed (setops): %v\n", err)
				_ = iter.Close()
				_ = backendBatch.Close()
				return false
			}
		} else {
			for iter.Valid() {
				key := iter.UnsafeKey()
				if iter.IsDeleted() {
					if err := backendBatch.Delete(key); err != nil {
						fmt.Fprintf(os.Stderr, "cachingdb: flush failed (delete): %v\n", err)
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				} else {
					if err := backendBatch.Set(key, iter.UnsafeValue()); err != nil {
						fmt.Fprintf(os.Stderr, "cachingdb: flush failed (set): %v\n", err)
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				}
				iter.Next()
			}
		}
		_ = iter.Close()
		// Commit to backend
		var err error
		if sync {
			err = backendBatch.WriteSync()
		} else {
			err = backendBatch.Write()
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "cachingdb: flush failed: %v\n", err)
			_ = backendBatch.Close()
			return false
		}
		_ = backendBatch.Close()
	}

	// Remove from queue and delete old WAL
	db.mu.Lock()
	if memRange.valid {
		db.backendRange.add(memRange.min)
		db.backendRange.add(memRange.max)
	}
	if len(db.queue) > 0 {
		db.queue = db.queue[1:]
	}
	if len(db.queueRanges) > 0 {
		db.queueRanges = db.queueRanges[1:]
	}
	if len(db.queueWALPaths) > 0 {
		db.queueWALPaths = db.queueWALPaths[1:]
	}

	// Check if WAL is still in use
	inUse := false
	if db.walPath == walPath {
		inUse = true
	} else {
		for _, p := range db.queueWALPaths {
			if p == walPath {
				inUse = true
				break
			}
		}
	}
	db.mu.Unlock()

	if walPath != "" && !inUse {
		if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "cachingdb: failed to remove WAL segment %q: %v\n", walPath, err)
		}
	}
	return true
}

// Get implements DB.Get using Merging logic logic conceptually,
// but optimized: check mutable, then queue (newest to oldest), then disk.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	// check mutable
	val, deleted, found := db.mutable.Get(key)
	if found {
		db.mu.RUnlock()
		if deleted {
			return nil, nil
		}
		if val == nil {
			return []byte{}, nil
		}
		return val, nil
	}

	// check queue backwards (newest first)
	for i := len(db.queue) - 1; i >= 0; i-- {
		val, deleted, found = db.queue[i].Get(key)
		if found {
			db.mu.RUnlock()
			if deleted {
				return nil, nil
			}
			if val == nil {
				return []byte{}, nil
			}
			return val, nil
		}
	}
	db.mu.RUnlock()

	return db.backend.Get(key)
}

func (db *DB) Has(key []byte) (bool, error) {
	v, err := db.Get(key)
	return v != nil, err
}

func (db *DB) Stats() map[string]string {
	return db.backend.Stats()
}

func (db *DB) Print() error {
	return db.backend.Print()
}

// Iterator implements DB.Iterator
func (db *DB) Iterator(start, end []byte) (merging.Iterator, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Snapshot Isolation:
	// To ensure the iterator sees a consistent point-in-time view, we rotate the
	// mutable memtable into the immutable queue. The iterator then consumes
	// only the queue and the backend. Any subsequent writes will go to a new
	// mutable memtable which this iterator ignores.
	if db.mutable.Len() > 0 {
		if err := db.rotateMemtableLocked(false); err != nil {
			return nil, err
		}
	}

	queueLen := len(db.queue)

	// Fast path for full scans: if the in-memory key ranges are disjoint from the
	// backend key range, we can concatenate iterators instead of merging.
	if start == nil && end == nil {
		// Only do this when the queue is empty; queued memtables imply the backend
		// might not yet include older keys, making disjoint-range checks unreliable.
		if db.backendRangeKnown && len(db.queue) == 0 && db.mutableRange.valid && db.backendRange.valid && !rangesOverlap(db.mutableRange, db.backendRange) {
			diskIter, err := db.backend.Iterator(nil, nil)
			if err != nil {
				return nil, err
			}

			// Since we rotated or mutable is empty, and queue is empty (checked above),
			// mutable is empty. So we just return diskIter?
			// Wait, if len(queue) == 0 and mutable.Size() == 0 (from rotate check logic),
			// then there is no memory data.
			if iteratorDebugEnabled.Load() {
				return &debugIterator{Iterator: diskIter, queueLen: queueLen, sourcesUsed: 1}, nil
			}
			return diskIter, nil
		}
	}

	var sources []merging.IteratorSource

	// Priority 0..N: Queue (Newest first)
	// Note: We skip db.mutable because we just rotated it (so it's empty) or it was already empty.
	prio := 0
	for i := len(db.queue) - 1; i >= 0; i-- {
		if overlapsQuery(start, end, db.queueRanges[i]) {
			qIter := db.queue[i].NewIterator(start, end)
			qIter.Seek(start)
			sources = append(sources, merging.IteratorSource{
				Iter:     qIter,
				Priority: prio,
			})
		}
		prio++
	}

	// Disk Iterator
	// Only skip if we definitively know the range and it doesn't overlap.
	if !db.backendRangeKnown || overlapsQuery(start, end, db.backendRange) {
		diskIter, err := db.backend.Iterator(start, end)
		if err != nil {
			return nil, err
		}

		sources = append(sources, merging.IteratorSource{
			Iter:     diskIter,
			Priority: prio,
		})
	}

	if len(sources) == 0 {
		out := merging.Iterator(&emptyIterator{start: start, end: end})
		if iteratorDebugEnabled.Load() {
			out = &debugIterator{Iterator: out, queueLen: queueLen, sourcesUsed: 0}
		}
		return out, nil
	}

	if len(sources) == 1 {
		out := newSingleSourceIterator(sources[0].Iter, start, end)
		if iteratorDebugEnabled.Load() {
			out = &debugIterator{Iterator: out, queueLen: queueLen, sourcesUsed: 1}
		}
		return out, nil
	}

	out := merging.NewMergingIterator(sources, start, end)
	if iteratorDebugEnabled.Load() {
		out = &debugIterator{Iterator: out, queueLen: queueLen, sourcesUsed: len(sources)}
	}
	return out, nil
}

type debugIterator struct {
	merging.Iterator
	queueLen    int
	sourcesUsed int
}

func (it *debugIterator) DebugStats() (queueLen int, sourcesUsed int) {
	return it.queueLen, it.sourcesUsed
}

type concatUnsafeIterator struct {
	first  iterator.UnsafeIterator
	second iterator.UnsafeIterator

	cur        iterator.UnsafeIterator
	usingFirst bool
	valid      bool
	err        error
}

func newConcatUnsafeIterator(first, second iterator.UnsafeIterator) merging.Iterator {
	it := &concatUnsafeIterator{
		first:      first,
		second:     second,
		cur:        first,
		usingFirst: true,
	}
	it.advance()
	return it
}

func (it *concatUnsafeIterator) advance() {
	it.valid = false

	for {
		if it.cur == nil {
			return
		}

		// Switch to second iterator when first is exhausted.
		if !it.cur.Valid() {
			if it.usingFirst {
				it.cur = it.second
				it.usingFirst = false
				continue
			}
			return
		}

		if it.cur.IsDeleted() {
			it.cur.Next()
			continue
		}

		it.valid = true
		return
	}
}

func (it *concatUnsafeIterator) Next() {
	if !it.valid {
		panic("iterator invalid")
	}
	it.cur.Next()
	it.advance()
}

func (it *concatUnsafeIterator) Valid() bool { return it.valid }

func (it *concatUnsafeIterator) Key() []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.Key()
}

func (it *concatUnsafeIterator) Value() []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.Value()
}

func (it *concatUnsafeIterator) KeyCopy(dst []byte) []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.KeyCopy(dst)
}

func (it *concatUnsafeIterator) ValueCopy(dst []byte) []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.ValueCopy(dst)
}

func (it *concatUnsafeIterator) Close() error {
	var firstErr error
	if it.first != nil {
		if err := it.first.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if it.second != nil {
		if err := it.second.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (it *concatUnsafeIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	if it.first != nil && it.first.Error() != nil {
		return it.first.Error()
	}
	if it.second != nil && it.second.Error() != nil {
		return it.second.Error()
	}
	return nil
}

func (it *concatUnsafeIterator) Domain() (start, end []byte) { return nil, nil }

func (db *DB) ReverseIterator(start, end []byte) (merging.Iterator, error) {
	// Flush everything to backend to simplify reverse iteration
	db.mu.Lock()
	if db.mutable.Len() > 0 {
		_ = db.rotateMemtableLocked(true) // Flush to backend
	}
	db.mu.Unlock()
	db.flushAll(false)

	return db.backend.ReverseIterator(start, end)
}

// NewBatch implementation for CachingDB
// batchOp removed, using batch.Entry directly

type Batch struct {
	db      *DB
	entries []batch.Entry
	backend batch.Interface
	size    int

	closed         bool
	streamEligible bool
	streamTried    bool
	firstKey       []byte
	lastKey        []byte
	batchRange     keyRange
}

func (db *DB) NewBatch() *Batch {
	return &Batch{db: db, entries: make([]batch.Entry, 0, 16), streamEligible: true}
}

func (db *DB) NewBatchWithSize(size int) *Batch {
	return &Batch{db: db, entries: make([]batch.Entry, 0, size), streamEligible: true}
}

func (b *Batch) Set(key, value []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if key == nil {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}

	b.batchRange.add(key)

	if b.backend != nil {
		b.size += len(key) + len(value)
		return b.backend.Set(key, value)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = append([]byte(nil), key...)
			b.lastKey = append([]byte(nil), key...)
		} else {
			if bytes.Compare(key, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = append(b.lastKey[:0], key...)
		}
	}
	// We don't know about slabs/thresholds here, so we just store inline.
	// Backend will handle promotion to slab if needed during writeBypass,
	// or standard write will handle it via WAL/Memtable (which don't use slabs yet).
	b.entries = append(b.entries, batch.Entry{
		Type:  batch.OpPut,
		Key:   key,
		Value: value,
	})
	b.size += len(key) + len(value)

	b.maybeSwitchToStreaming()
	return nil
}

func (b *Batch) Delete(key []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if key == nil {
		return ErrKeyEmpty
	}

	b.batchRange.add(key)

	if b.backend != nil {
		b.size += len(key)
		return b.backend.Delete(key)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = append([]byte(nil), key...)
			b.lastKey = append([]byte(nil), key...)
		} else {
			if bytes.Compare(key, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = append(b.lastKey[:0], key...)
		}
	}
	b.entries = append(b.entries, batch.Entry{
		Type: batch.OpDelete,
		Key:  key,
	})
	b.size += len(key)

	b.maybeSwitchToStreaming()
	return nil
}

func (b *Batch) SetOps(ops []batch.Entry) error {
	if b.closed {
		return ErrBatchClosed
	}
	if b.backend != nil {
		for _, op := range ops {
			b.size += len(op.Key) + len(op.Value)
			b.batchRange.add(op.Key)
		}
		return b.backend.SetOps(ops)
	}
	for _, op := range ops {
		b.entries = append(b.entries, op)
		b.size += len(op.Key) + len(op.Value)
		b.batchRange.add(op.Key)
	}
	return nil
}

func (b *Batch) maybeSwitchToStreaming() {
	if b.streamTried || !b.streamEligible || b.backend != nil {
		return
	}
	// The streamSwitchThreshold check is now redundant because the b.size
	// check against db.flushThreshold effectively handles this.
	// If a batch is small enough to be under the flush threshold, we want it
	// to go through the regular memtable path for aggregation, regardless
	// of sequentiality.

	// Also require the batch to be large enough to justify a direct write.
	// Otherwise, small sequential batches bypass the memtable and cause
	// write amplification in the backend.
	if b.size < int(b.db.flushThreshold) {
		return
	}

	// Only attempt streaming if the batch is strictly increasing and starts beyond
	// the maximum key present in the in-memory layers.
	b.db.mu.RLock()
	var maxKey []byte
	if b.db.mutableRange.valid {
		maxKey = b.db.mutableRange.max
	}
	for _, r := range b.db.queueRanges {
		if !r.valid {
			continue
		}
		if maxKey == nil || bytes.Compare(r.max, maxKey) > 0 {
			maxKey = r.max
		}
	}
	b.db.mu.RUnlock()

	b.streamTried = true
	if maxKey != nil && bytes.Compare(b.firstKey, maxKey) <= 0 {
		return
	}

	backendBatch := b.db.backend.NewBatch()
	if err := backendBatch.SetOps(b.entries); err != nil {
		_ = backendBatch.Close()
		return
	}
	b.backend = backendBatch
	b.entries = nil
}

func (b *Batch) Write() error {
	return b.write(false)
}

func (b *Batch) WriteSync() error {
	return b.write(true)
}

func (b *Batch) write(sync bool) error {
	if b.closed {
		return ErrBatchClosed
	}

	if b.backend != nil {
		var err error
		if sync {
			b.db.flushMu.Lock()
			err = b.backend.WriteSync()
			b.db.flushMu.Unlock()
		} else {
			b.db.flushMu.Lock()
			err = b.backend.Write()
			b.db.flushMu.Unlock()
		}
		if cerr := b.backend.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err == nil && b.batchRange.valid {
			b.db.mu.Lock()
			b.db.backendRange.add(b.batchRange.min)
			b.db.backendRange.add(b.batchRange.max)
			b.db.mu.Unlock()
		}
		b.backend = nil
		b.closed = true
		return err
	}

	// Optimization: Bypass for Large Batches
	// Generalization: Only bypass if the batch is large enough to be comparable
	// to a memtable flush. Small/Medium random batches cause high write amplification
	// if written directly to the COW backend.
	if b.size >= int(b.db.flushThreshold) {
		return b.writeBypass(sync)
	}
	return b.writeRegular(sync)
}

func (b *Batch) writeRegular(sync bool) error {
	b.db.mu.Lock()

	// entries is already a slice. Sort it.
	sort.SliceStable(b.entries, func(i, j int) bool {
		return bytes.Compare(b.entries[i].Key, b.entries[j].Key) < 0
	})

	// 1. WAL Append loop
	records := make([]wal.Record, 0, len(b.entries))
	for _, op := range b.entries {
		if op.Type == batch.OpDelete {
			records = append(records, wal.Record{Op: wal.OpDelete, Key: op.Key})
		} else {
			records = append(records, wal.Record{Op: wal.OpSet, Key: op.Key, Value: op.Value})
		}
	}
	if err := b.db.wal.AppendBatch(records); err != nil {
		b.db.mu.Unlock()
		return err
	}

	if sync {
		if err := b.db.wal.Sync(); err != nil {
			b.db.mu.Unlock()
			return err
		}
	}

	// 2. Memtable Update
	for _, op := range b.entries {
		// We use Steal methods because b.entries owns the key/value copies
		// created in Batch.Set/Delete. We transfer ownership to Memtable.
		if op.Type == batch.OpDelete {
			b.db.mutable.DeleteSteal(op.Key)
		} else {
			b.db.mutable.SetSteal(op.Key, op.Value)
		}
		b.db.mutableRange.add(op.Key)
	}

	// 3. Threshold Check
	needsBackpressureFlush := false
	if b.db.mutable.Size() > b.db.flushThreshold {
		if err := b.db.rotateMemtableLocked(true); err != nil {
			b.db.mu.Unlock()
			return err
		}
		needsBackpressureFlush = len(b.db.queue) > maxQueuedMemtables
	}

	b.db.mu.Unlock()

	if needsBackpressureFlush {
		b.db.flushAll(true)
	}

	return b.Close()
}

func (b *Batch) writeBypass(sync bool) error {
	// Fast path: if none of these keys exist in mutable/queue, we can write directly
	// to the backend without flushing (no in-memory shadowing possible).
	b.db.mu.RLock()
	mutable := b.db.mutable
	queue := append([]*memtable.Memtable(nil), b.db.queue...)
	mutableRange := b.db.mutableRange
	queueRanges := append([]keyRange(nil), b.db.queueRanges...)
	b.db.mu.RUnlock()

	// Cheap append-only check: if the batch key range does not overlap with any
	// in-memory key ranges, it cannot be shadowed.
	batchRange := keyRange{}
	for _, op := range b.entries {
		key := op.Key
		batchRange.add(key)
	}

	overlaps := rangesOverlap(batchRange, mutableRange)
	if !overlaps {
		for _, r := range queueRanges {
			if rangesOverlap(batchRange, r) {
				overlaps = true
				break
			}
		}
	}

	if overlaps {
		// Slow path: verify no individual key exists in memory (handles sparse overlap).
		for _, op := range b.entries {
			key := op.Key
			if _, _, found := mutable.Get(key); found {
				return b.writeRegular(sync)
			}
			for i := len(queue) - 1; i >= 0; i-- {
				if _, _, found := queue[i].Get(key); found {
					return b.writeRegular(sync)
				}
			}
		}
	}

	// Write directly to backend
	backendBatch := b.db.backend.NewBatch()

	// Use SetOps for bulk transfer (checking slabs internally in backend)
	if err := backendBatch.SetOps(b.entries); err != nil {
		return err
	}

	var err error
	if sync {
		b.db.flushMu.Lock()
		err = backendBatch.WriteSync()
		b.db.flushMu.Unlock()
	} else {
		b.db.flushMu.Lock()
		err = backendBatch.Write()
		b.db.flushMu.Unlock()
	}

	if err != nil {
		return err
	}

	b.db.mu.Lock()
	if batchRange.valid {
		b.db.backendRange.add(batchRange.min)
		b.db.backendRange.add(batchRange.max)
	}
	b.db.mu.Unlock()

	return b.Close()
}

func (b *Batch) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	b.entries = nil
	if b.backend != nil {
		_ = b.backend.Close()
		b.backend = nil
	}
	return nil
}

func (b *Batch) Replay(fn func(batch.Entry) error) error {
	if b.closed {
		return ErrBatchClosed
	}
	if b.backend != nil {
		return b.backend.Replay(fn)
	}

	for _, entry := range b.entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) GetByteSize() (int, error) {
	if b.closed {
		return 0, ErrBatchClosed
	}
	return b.size, nil
}

// singleSourceIterator wraps a single UnsafeIterator to satisfy merging.Iterator,
// skipping tombstones.
type singleSourceIterator struct {
	iter  iterator.UnsafeIterator
	valid bool
	start []byte
	end   []byte
}

func newSingleSourceIterator(iter iterator.UnsafeIterator, start, end []byte) merging.Iterator {
	it := &singleSourceIterator{
		iter:  iter,
		start: start,
		end:   end,
	}
	// Iterator is already sought to start by the caller
	it.advance()
	return it
}

func (it *singleSourceIterator) advance() {
	it.valid = false
	for it.iter.Valid() {
		if it.end != nil && bytes.Compare(it.iter.UnsafeKey(), it.end) >= 0 {
			return
		}
		if it.iter.IsDeleted() {
			it.iter.Next()
			continue
		}
		it.valid = true
		return
	}
}

func (it *singleSourceIterator) Next() {
	if !it.valid {
		panic("iterator invalid")
	}
	it.iter.Next()
	it.advance()
}

func (it *singleSourceIterator) Valid() bool               { return it.valid }
func (it *singleSourceIterator) Key() []byte               { return it.iter.Key() }
func (it *singleSourceIterator) Value() []byte             { return it.iter.Value() }
func (it *singleSourceIterator) KeyCopy(dst []byte) []byte { return it.iter.KeyCopy(dst) }
func (it *singleSourceIterator) ValueCopy(dst []byte) []byte {
	return it.iter.ValueCopy(dst)
}
func (it *singleSourceIterator) Close() error             { return it.iter.Close() }
func (it *singleSourceIterator) Error() error             { return it.iter.Error() }
func (it *singleSourceIterator) Domain() ([]byte, []byte) { return it.start, it.end }

// emptyIterator represents an iterator with no elements.
type emptyIterator struct {
	start, end []byte
}

func (it *emptyIterator) Next()                     { panic("iterator invalid") }
func (it *emptyIterator) Valid() bool               { return false }
func (it *emptyIterator) Key() []byte               { panic("iterator invalid") }
func (it *emptyIterator) Value() []byte             { panic("iterator invalid") }
func (it *emptyIterator) KeyCopy(_ []byte) []byte   { panic("iterator invalid") }
func (it *emptyIterator) ValueCopy(_ []byte) []byte { panic("iterator invalid") }
func (it *emptyIterator) Close() error              { return nil }
func (it *emptyIterator) Error() error              { return nil }
func (it *emptyIterator) Domain() ([]byte, []byte)  { return it.start, it.end }
