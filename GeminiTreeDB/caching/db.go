package caching

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/snissn/gomap-gemini/TreeDB/internal/iterator"
	"github.com/snissn/gomap-gemini/TreeDB/internal/memtable"
	"github.com/snissn/gomap-gemini/TreeDB/internal/merging"
	"github.com/snissn/gomap-gemini/TreeDB/internal/wal"
	"github.com/snissn/gomap-gemini/TreeDB/batch"
)

var ErrKeyEmpty = fmt.Errorf("key cannot be empty")
var ErrValueNil = fmt.Errorf("value cannot be nil")
var ErrBatchClosed = fmt.Errorf("batch has been written or closed")

// BatchInterface matches the Batch methods we need from backend.
type BatchInterface interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	SetOps(ops map[string]batch.Entry) error // New method
	Write() error
	WriteSync() error
	Close() error
	GetByteSize() (int, error)
}

// BackendDB defines the subset of treedb.DB needed by CachingDB.
type BackendDB interface {
	Get(key []byte) ([]byte, error)
	Iterator(start, end []byte) (iterator.UnsafeIterator, error) 
	ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error)
	NewBatch() BatchInterface
	Close() error
	Print() error
	Stats() map[string]string
}

type DB struct {
	mu sync.RWMutex

	// Level 0 (Memory)
	mutable *memtable.Memtable
	queue   []*memtable.Memtable

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
		_ = db.rotateMemtableLocked() 
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
	db.mu.Unlock()

	// Flush 'mem' to backend

batch := db.backend.NewBatch()
	iter := mem.NewIterator() // Returns iterator.UnsafeIterator
	iter.Seek(nil)             // Start
	for iter.Valid() {
		if iter.IsDeleted() {
			batch.Delete(iter.UnsafeKey())
		} else {
			batch.Set(iter.UnsafeKey(), iter.UnsafeValue())
		}
		iter.Next()
	}
	// Commit to disk
	if err := batch.WriteSync(); err != nil {
		fmt.Fprintf(os.Stderr, "cachingdb: flush failed: %v\n", err)
		return false
	}

	// Remove from queue and delete old WAL
	db.mu.Lock()
	if len(db.queue) > 0 {
		db.queue = db.queue[1:]
	}
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

func (db *DB) Stats() map[string]string {
	return db.backend.Stats()
}

func (db *DB) Print() error {
	return db.backend.Print()
}

// Iterator implements DB.Iterator
func (db *DB) Iterator(start, end []byte) (merging.Iterator, error) {
	db.mu.RLock()
	defer db.mu.RUnlock() // Need to hold lock while creating iterators?

	var sources []merging.IteratorSource

	// Priority 0: Mutable (always exists)
	mutableIter := db.mutable.NewIterator()
	mutableIter.Seek(start)
	sources = append(sources, merging.IteratorSource{
		Iter:     mutableIter,
		Priority: 0,
	})

	// Priority 1..N: Queue (Newest first)
	prio := 1
	for i := len(db.queue) - 1; i >= 0; i-- {
		qIter := db.queue[i].NewIterator()
		qIter.Seek(start)
		sources = append(sources, merging.IteratorSource{
			Iter:     qIter,
			Priority: prio,
		})
		prio++
	}

	// Disk Iterator
	diskIter, err := db.backend.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	// Disk Iterator from backend.Iterator might already be initialized/seeked?
	// The interface is: Iterator(start, end) returns an iterator.
	// Usually database iterators are returned in a Valid state pointing to start, or Invalid if empty.
	// Let's assume backend.Iterator does the right thing (Seek is implied by creation with start/end).
	// But let's check backend contract.
	// If backend.Iterator returns a tree.Iterator, it probably does a Seek internally.
	// But to be safe and consistent with UnsafeIterator interface which usually requires Seek/Next...
	// Actually, treedb.Iterator(start, end) usually does the initial seek.
	// Let's verify tree.Iterator in a moment.
	
	// If the diskIter comes pre-seeked (which is common for DB.Iterator(start, end)),
	// we don't need to Seek it again, or Seek(start) is idempotent/cheap.
	// However, if we Seek it here, we ensure it's valid.
	diskIter.Seek(start)
	
	sources = append(sources, merging.IteratorSource{
		Iter:     diskIter,
		Priority: prio,
	})

	return merging.NewMergingIterator(sources, start, end), nil
}

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
// batchOp removed, using batch.Entry directly

type Batch struct {
	db   *DB
	ops  map[string]batch.Entry
	size int
}

func (db *DB) NewBatch() *Batch {
	return &Batch{db: db, ops: make(map[string]batch.Entry)}
}

func (db *DB) NewBatchWithSize(size int) *Batch {
	return &Batch{db: db, ops: make(map[string]batch.Entry, size)}
}
func (b *Batch) Set(key, value []byte) error {
	if b.ops == nil {
		return ErrBatchClosed
	}
	if key == nil {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}
	// We don't know about slabs/thresholds here, so we just store inline.
	// Backend will handle promotion to slab if needed during writeBypass,
	// or standard write will handle it via WAL/Memtable (which don't use slabs yet).
	b.ops[string(key)] = batch.Entry{
		Type:  batch.OpPut,
		Key:   key,
		Value: value,
	}
	b.size += len(key) + len(value)
	return nil
}

func (b *Batch) Delete(key []byte) error {
	if b.ops == nil {
		return ErrBatchClosed
	}
	if key == nil {
		return ErrKeyEmpty
	}
	b.ops[string(key)] = batch.Entry{
		Type: batch.OpDelete,
		Key:  key,
	}
	b.size += len(key)
	return nil
}

func (b *Batch) SetOps(ops map[string]batch.Entry) error {
	if b.ops == nil {
		return ErrBatchClosed
	}
	for k, op := range ops {
		b.ops[k] = op
		b.size += len(op.Key) + len(op.Value)
	}
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
	
	// Optimization: Bypass for Large Batches
	if len(b.ops) > 2000 {
		return b.writeBypass(sync)
	}
	
	b.db.mu.Lock()
	defer b.db.mu.Unlock()

	// 1. WAL Append loop
	for k, op := range b.ops {
		key := []byte(k)
		var err error
		if op.Type == batch.OpDelete {
			err = b.db.wal.Append(wal.OpDelete, key, nil)
		} else {
			err = b.db.wal.Append(wal.OpSet, key, op.Value)
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
		if op.Type == batch.OpDelete {
			b.db.mutable.Delete(key)
		} else {
			b.db.mutable.Set(key, op.Value)
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

func (b *Batch) writeBypass(sync bool) error {
	// 1. Flush all pending memtables to disk to maintain consistency
	b.db.mu.Lock()
	if b.db.mutable.Size() > 0 {
		_ = b.db.rotateMemtableLocked()
	}
	b.db.mu.Unlock()
	
	// This flushes everything (mutable -> queue -> backend)
	b.db.flushAll()

	// 2. Write directly to backend
	backendBatch := b.db.backend.NewBatch()
	
	// Use SetOps for bulk transfer (checking slabs internally in backend)
	if err := backendBatch.SetOps(b.ops); err != nil {
		return err
	}
	
	var err error
	if sync {
		err = backendBatch.WriteSync()
	} else {
		err = backendBatch.Write()
	}
	
	if err != nil {
		return err
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