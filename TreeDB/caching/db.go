package caching

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/snissn/gomap-gemini/TreeDB/internal/memtable"
	"github.com/snissn/gomap-gemini/TreeDB/internal/merging"
	"github.com/snissn/gomap-gemini/TreeDB/internal/wal"
)

var ErrKeyEmpty = fmt.Errorf("key cannot be empty")
var ErrValueNil = fmt.Errorf("value cannot be nil")
var ErrBatchClosed = fmt.Errorf("batch has been written or closed")

// BatchInterface matches the Batch methods we need from backend.
type BatchInterface interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	Write() error
	WriteSync() error
	Close() error
	GetByteSize() (int, error)
}

// BackendDB defines the subset of treedb.DB needed by CachingDB.
type BackendDB interface {
	Get(key []byte) ([]byte, error)
	Iterator(start, end []byte) (merging.Iterator, error)
	ReverseIterator(start, end []byte) (merging.Iterator, error)
	NewBatch() BatchInterface
	Close() error
	Print() error
	Stats() map[string]string
}

// ...

func (db *DB) Print() error {
	return db.backend.Print()
}

func (db *DB) Stats() map[string]string {
	return db.backend.Stats()
}

type DB struct {
	mu sync.RWMutex

	// Level 0 (Memory)
	mutable   *memtable.Memtable
	queue     []*memtable.Memtable
	
	// Durability
	wal       *wal.Writer
	walPath   string
	walSeq    int // Sequence number for WAL files

	// Level 1 (Disk)
	backend   BackendDB
	
	// Config
	dir            string
	flushThreshold int64
	
	// Lifecycle
	closeCh chan struct{}
	wg      sync.WaitGroup
}

func Open(dir string, backend BackendDB, flushThreshold int64) (*DB, error) {
	if flushThreshold <= 0 {
		flushThreshold = 4 * 1024 * 1024 // 4MB default
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
	}

	// Open initial WAL
	if err := db.rotateWALLocked(); err != nil {
		return nil, err
	}

	// Start background flusher
	db.wg.Add(1)
	go db.flushLoop()

	return db, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	if db.mutable.Size() > 0 {
		_ = db.rotateMemtableLocked() // Ignore error (what can we do? WAL error?)
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
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}
	return db.set(key, value, false)
}

func (db *DB) SetSync(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}
	return db.set(key, value, true)
}

func (db *DB) set(key, value []byte, sync bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 1. Append to WAL
	if err := db.wal.Append(wal.OpSet, key, value); err != nil {
		return err
	}
	if sync {
		if err := db.wal.Sync(); err != nil {
			return err
		}
	}

	// 2. Insert to Memtable
	db.mutable.Set(key, value)

	// 3. Check Threshold
	if db.mutable.Size() > db.flushThreshold {
		if err := db.rotateMemtableLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	return db.delete(key, false)
}

func (db *DB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	return db.delete(key, true)
}

func (db *DB) delete(key []byte, sync bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 1. WAL
	if err := db.wal.Append(wal.OpDelete, key, nil); err != nil {
		return err
	}
	if sync {
		if err := db.wal.Sync(); err != nil {
			return err
		}
	}

	// 2. Memtable
	db.mutable.Delete(key)

	// 3. Threshold
	if db.mutable.Size() > db.flushThreshold {
		if err := db.rotateMemtableLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) rotateMemtableLocked() error {
	db.queue = append(db.queue, db.mutable)
	db.mutable = memtable.New()
	return db.rotateWALLocked()
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
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			// Flush all remaining
			db.flushAll()
			return
		case <-ticker.C:
			db.flushOne()
		}
	}
}

func (db *DB) flushAll() {
	for {
		if !db.flushOne() {
			break
		}
	}
}

func (db *DB) flushOne() bool {
	db.mu.Lock()
	if len(db.queue) == 0 {
		db.mu.Unlock()
		return false
	}
	mem := db.queue[0]
	// Remove from queue? No, wait until flushed.
	// Ideally we copy the pointer and release lock to flush.
	// But we need to ensure we don't flush same memtable twice or concurrently.
	// We'll peek, flush, then remove under lock.
	db.mu.Unlock()

	// Flush 'mem' to backend
	batch := db.backend.NewBatch()
	iter := mem.NewIterator()
	iter.Seek(nil) // Start
	for iter.Valid() {
		item := iter.Item()
		if item.IsDeleted {
			batch.Delete(item.Key)
		} else {
			batch.Set(item.Key, item.Value)
		}
		iter.Next()
	}
	// Commit to disk
	if err := batch.WriteSync(); err != nil {
		// Retry? Log? For now panic/log.
		fmt.Fprintf(os.Stderr, "cachingdb: flush failed: %v\n", err)
		return false
	}

	// Remove from queue and delete old WAL
	// Note: We need to know WHICH wal corresponds to this memtable.
	// Simplified: We rotate WAL every time we rotate Memtable.
	// So queue[0] corresponds to walSeq - len(queue).
	// We should probably track (Memtable, WalPath) pairs in the queue.
	
db.mu.Lock()
	db.queue = db.queue[1:]
	// TODO: Delete old WAL file here.
	// Need to track WAL paths.
	db.mu.Unlock()
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

// Iterator implements DB.Iterator
func (db *DB) Iterator(start, end []byte) (merging.Iterator, error) {
	db.mu.RLock()
	defer db.mu.RUnlock() // Need to hold lock while creating iterators?
	// If we create iterators from memtables, they are snapshots (COW).
	
	var sources []merging.IteratorSource
	
	// Priority 0: Mutable
	sources = append(sources, merging.IteratorSource{
		Iter:     wrapMemIterator(db.mutable.NewIterator(), start, end),
		Priority: 0,
	})
	
	// Priority 1..N: Queue (Newest first? No, newest has lower priority number/better precedence)
	// MergingIterator expects: Priority 0 is best.
	// So Mutable = 0.
	// Queue[Last] (Newest) = 1
	// Queue[0] (Oldest) = N
	// Disk = N+1
	
	prio := 1
	for i := len(db.queue) - 1; i >= 0; i-- {
		sources = append(sources, merging.IteratorSource{
			Iter:     wrapMemIterator(db.queue[i].NewIterator(), start, end),
			Priority: prio,
		})
		prio++
	}
	
	diskIter, err := db.backend.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	sources = append(sources, merging.IteratorSource{
		Iter:     diskIter, 
		Priority: prio,
	})
	
	return merging.NewMergingIterator(sources, start, end), nil
}

// wrapMemIterator adapts memtable.Iterator to merging.Iterator (and cosmosdb.Iterator subset)
type memIterAdapter struct {
	it    *memtable.Iterator
	start []byte
	end   []byte
}

func wrapMemIterator(it *memtable.Iterator, start, end []byte) *memIterAdapter {
	if start != nil {
		it.Seek(start)
	} else {
		it.Seek(nil)
	}
	return &memIterAdapter{it: it, start: start, end: end}
}

func (m *memIterAdapter) Next() {
	m.it.Next()
	// Bounds check
	if m.it.Valid() && m.end != nil && bytes.Compare(m.it.Key(), m.end) >= 0 {
		// Invalidate manually or just rely on MergingIterator to handle?
		// MergingIterator relies on Valid(). We should make Valid() return false if out of bounds.
		// But memtable iterator doesn't know bounds.
		// We handle it in Valid().
	}
}

func (m *memIterAdapter) Valid() bool {
	if !m.it.Valid() {
		return false
	}
	if m.end != nil && bytes.Compare(m.it.Key(), m.end) >= 0 {
		return false
	}
	return true
}

func (m *memIterAdapter) Key() []byte { return m.it.Key() }
func (m *memIterAdapter) Value() []byte { return m.it.Value() }
func (m *memIterAdapter) Close() error { return m.it.Close() }
func (m *memIterAdapter) IsDeleted() bool { return m.it.IsDeleted() }
func (m *memIterAdapter) Error() error      { return nil } // Memtable iterator doesn't produce errors
func (m *memIterAdapter) Domain() (start, end []byte) { return m.start, m.end }

func (db *DB) ReverseIterator(start, end []byte) (merging.Iterator, error) {
	// Flush everything to backend to simplify reverse iteration
	db.mu.Lock()
	if db.mutable.Size() > 0 {
		_ = db.rotateMemtableLocked()
	}
	db.mu.Unlock()
	db.flushAll()
	
	return db.backend.ReverseIterator(start, end)
}

// NewBatch implementation for CachingDB
type batchOp struct {
	value []byte
	del   bool
}

type Batch struct {
	db   *DB
	ops  map[string]batchOp
	size int
}

func (db *DB) NewBatch() *Batch {
	return &Batch{db: db, ops: make(map[string]batchOp)}
}

func (db *DB) NewBatchWithSize(size int) *Batch {
	return &Batch{db: db, ops: make(map[string]batchOp, size)}
}
func (b *Batch) Set(key, value []byte) error {
	if b.ops == nil {
		return ErrBatchClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}
	b.ops[string(key)] = batchOp{value: value, del: false}
	b.size += len(key) + len(value)
	return nil
}

func (b *Batch) Delete(key []byte) error {
	if b.ops == nil {
		return ErrBatchClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	b.ops[string(key)] = batchOp{del: true}
	b.size += len(key)
	return nil
}

func (b *Batch) Write() error {
	return b.write(false)
}

func (b *Batch) WriteSync() error {
	return b.write(true)
}

func (b *Batch) write(sync bool) error {
	if b.ops == nil {
		return ErrBatchClosed
	}
	b.db.mu.Lock()
	defer b.db.mu.Unlock()

	// 1. WAL Append loop
	for k, op := range b.ops {
		key := []byte(k)
		var err error
		if op.del {
			err = b.db.wal.Append(wal.OpDelete, key, nil)
		} else {
			err = b.db.wal.Append(wal.OpSet, key, op.value)
		}
		if err != nil {
			return err
		}
	}

	if sync {
		if err := b.db.wal.Sync(); err != nil {
			return err
		}
	}

	// 2. Memtable Update
	for k, op := range b.ops {
		key := []byte(k)
		if op.del {
			b.db.mutable.Delete(key)
		} else {
			b.db.mutable.Set(key, op.value)
		}
	}

	// 3. Threshold Check
	if b.db.mutable.Size() > b.db.flushThreshold {
		if err := b.db.rotateMemtableLocked(); err != nil {
			return err
		}
	}
	
	return b.Close()
}

func (b *Batch) Close() error {
	b.ops = nil
	return nil
}

func (b *Batch) GetByteSize() (int, error) {
	if b.ops == nil {
		return 0, ErrBatchClosed
	}
	return b.size, nil
}
