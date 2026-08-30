package db

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	publiciterator "github.com/snissn/gomap/TreeDB/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	getManyValueGuessBytes          = 128
	getManyMaxArenaBytes            = 1 << 20
	getManyParallelMinKeys          = 128
	getManyParallelMinKeysPerWorker = 32
	getManyParallelMaxWorkers       = 8
)

var getManyEmptyValue = []byte{}

// GetManyViewFunc receives one GetManyView result. The value slice is a
// read-only view that is valid only until the callback returns; callers must
// copy it before retaining it. Missing/tombstoned keys are reported with
// found=false and value=nil.
type GetManyViewFunc = tree.GetManyViewFunc

type foregroundReadObserver struct {
	id     uint64
	notify func()
	begin  func() func()
}

// RegisterForegroundReadObserver installs the cached layer's observer for
// logical collection reads routed through this raw backend. begin returns an
// idempotent function that ends a snapshot-backed read. The returned
// registration function removes this exact observer.
func (db *DB) RegisterForegroundReadObserver(notify func(), begin func() func()) func() {
	if db == nil || notify == nil || begin == nil {
		return func() {}
	}
	db.foregroundReadObserverMu.Lock()
	db.foregroundReadObserverID++
	registration := &foregroundReadObserver{
		id:     db.foregroundReadObserverID,
		notify: notify,
		begin:  begin,
	}
	db.foregroundReadObserver.Store(registration)
	db.foregroundReadObserverMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			db.foregroundReadObserverMu.Lock()
			if current := db.foregroundReadObserver.Load(); current != nil && current.id == registration.id {
				db.foregroundReadObserver.Store(nil)
			}
			db.foregroundReadObserverMu.Unlock()
		})
	}
}

// NotifyForegroundRead forwards one instantaneous logical collection read to
// the cached scheduler without classifying backend-internal maintenance scans
// as foreground work.
func (db *DB) NotifyForegroundRead() {
	if db == nil {
		return
	}
	if observer := db.foregroundReadObserver.Load(); observer != nil {
		observer.notify()
	}
}

func (db *DB) beginForegroundRead() func() {
	if db == nil {
		return nil
	}
	if observer := db.foregroundReadObserver.Load(); observer != nil {
		return observer.begin()
	}
	return nil
}

func getManyArenaCap(keyCount int) int {
	if keyCount <= 0 {
		return 0
	}
	arenaCap := keyCount * getManyValueGuessBytes
	if arenaCap < 0 {
		arenaCap = 0
	}
	if arenaCap > getManyMaxArenaBytes {
		arenaCap = getManyMaxArenaBytes
	}
	return arenaCap
}

func getManyWorkerCount(keyCount int) int {
	if keyCount <= 0 {
		return 1
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > getManyParallelMaxWorkers {
		workers = getManyParallelMaxWorkers
	}
	if workers > keyCount {
		workers = keyCount
	}
	return workers
}

func getManyCanParallelize(keyCount, workers int) bool {
	if keyCount < getManyParallelMinKeys {
		return false
	}
	if workers <= 1 {
		return false
	}
	return keyCount/workers >= getManyParallelMinKeysPerWorker
}

func getManyChunkBounds(worker, workers, keyCount int) (int, int) {
	start := (worker * keyCount) / workers
	end := ((worker + 1) * keyCount) / workers
	return start, end
}

// GetManyParallelPlan reports how this backend would schedule GetMany for the
// provided key count.
func (db *DB) GetManyParallelPlan(keyCount int) (workers int, parallel bool) {
	workers = getManyWorkerCount(keyCount)
	return workers, getManyCanParallelize(keyCount, workers)
}

// --- Public API ---

func (db *DB) acquireSnapshotOrErr() (*Snapshot, error) {
	if db == nil {
		return nil, ErrClosed
	}
	if err := db.publicationPoisonedError(); err != nil {
		return nil, err
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		return nil, ErrClosed
	}
	return snap, nil
}

func (db *DB) refreshOnValueLogFileNotFound(err error) bool {
	if db == nil || db.valueLogManager == nil {
		return false
	}
	return errors.Is(err, valuelog.ErrFileNotFound)
}

func (db *DB) refreshValueLogSetForReadRetry(observedEpoch uint64) error {
	if db == nil {
		return ErrClosed
	}
	for {
		db.readRetryRefreshMu.Lock()
		if !db.readRetryRefreshInFlight {
			if db.readRetryRefreshEpoch.Load() != observedEpoch {
				db.readRetryRefreshSkippedEpoch.Add(1)
				db.readRetryRefreshMu.Unlock()
				return nil
			}
			done := make(chan struct{})
			db.readRetryRefreshInFlight = true
			db.readRetryRefreshDone = done
			db.readRetryRefreshErr = nil
			db.readRetryRefreshLeaderCount.Add(1)
			db.readRetryRefreshMu.Unlock()

			err := db.RefreshValueLogSet()

			db.readRetryRefreshMu.Lock()
			db.readRetryRefreshErr = err
			if err == nil {
				db.readRetryRefreshEpoch.Add(1)
			}
			db.readRetryRefreshInFlight = false
			db.readRetryRefreshDone = nil
			close(done)
			db.readRetryRefreshMu.Unlock()
			return err
		}
		done := db.readRetryRefreshDone
		db.readRetryRefreshFollowerCount.Add(1)
		db.readRetryRefreshMu.Unlock()

		if done == nil {
			runtime.Gosched()
			continue
		}

		<-done
		db.readRetryRefreshMu.Lock()
		err := db.readRetryRefreshErr
		db.readRetryRefreshMu.Unlock()
		return err
	}
}

// Get returns the value for a key.
//
// Semantics: Returns a safe copy of the value.
func (db *DB) Get(key []byte) ([]byte, error) {
	key = normalizeRawKVPointKey(key)
	readOnce := func() ([]byte, error) {
		snap, err := db.acquireSnapshotOrErr()
		if err != nil {
			return nil, err
		}
		defer snap.Close()
		return snap.Get(key)
	}

	retryEpoch := db.readRetryRefreshEpoch.Load()
	val, err := readOnce()
	if db.refreshOnValueLogFileNotFound(err) {
		if refreshErr := db.refreshValueLogSetForReadRetry(retryEpoch); refreshErr != nil {
			return nil, refreshErr
		}
		val, err = readOnce()
	}
	if err == tree.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// GetVersioned returns the value for key plus the native entry revision stored
// beside that value. Missing keys return a nil value, LegacyEntryRevision, and
// nil error; tombstones return a nil value with their tombstone revision.
func (db *DB) GetVersioned(key []byte) ([]byte, page.EntryRevision, error) {
	key = normalizeRawKVPointKey(key)
	readOnce := func() ([]byte, page.EntryRevision, error) {
		snap, err := db.acquireSnapshotOrErr()
		if err != nil {
			return nil, page.LegacyEntryRevision, err
		}
		defer snap.Close()
		return snap.GetVersioned(key)
	}

	retryEpoch := db.readRetryRefreshEpoch.Load()
	val, revision, err := readOnce()
	if db.refreshOnValueLogFileNotFound(err) {
		if refreshErr := db.refreshValueLogSetForReadRetry(retryEpoch); refreshErr != nil {
			return nil, page.LegacyEntryRevision, refreshErr
		}
		val, revision, err = readOnce()
	}
	if err == tree.ErrKeyNotFound {
		return nil, revision, nil
	}
	return val, revision, err
}

// DurabilityMode reports the backend durability mode configured at open.
func (db *DB) DurabilityMode() DurabilityMode {
	if db == nil {
		return DurabilityDurable
	}
	return db.durability
}

// ResolvedProfile reports the immutable canonical profile carried into the
// backend by a public constructor. Low-level internal opens may return empty.
func (db *DB) ResolvedProfile() DurabilityProfile {
	if db == nil {
		return ""
	}
	return db.resolvedProfile
}

// GetMany returns values for keys.
//
// Semantics: Returns safe copies of values. Missing keys are returned as nil
// entries with no error.
func (db *DB) GetMany(keys [][]byte) ([][]byte, error) {
	keys = normalizeRawKVPointKeys(keys)
	retryEpoch := db.readRetryRefreshEpoch.Load()
	out, err := db.getManyOnce(keys)
	if db.refreshOnValueLogFileNotFound(err) {
		if refreshErr := db.refreshValueLogSetForReadRetry(retryEpoch); refreshErr != nil {
			return nil, refreshErr
		}
		return db.getManyOnce(keys)
	}
	return out, err
}

func (db *DB) getManyOnce(keys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	snap, err := db.acquireSnapshotOrErr()
	if err != nil {
		return nil, err
	}
	defer snap.Close()

	workers := getManyWorkerCount(len(keys))
	if getManyCanParallelize(len(keys), workers) {
		if err := db.getManyParallel(snap, keys, out, workers); err != nil {
			return nil, err
		}
		return out, nil
	}
	if err := db.getManySequential(snap, keys, out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetManyView calls fn once for each key with a read-only value view.
// Missing keys are reported with found=false and value=nil. Callback values are
// valid only until fn returns and must be copied before retaining. Large batches
// may invoke callbacks concurrently; callers that mutate shared state must
// synchronize it. If fn returns an error, iteration stops best-effort and that
// error is returned; callbacks already invoked are not retried.
func (db *DB) GetManyView(keys [][]byte, fn GetManyViewFunc) error {
	keys = normalizeRawKVPointKeys(keys)
	if fn == nil {
		return errors.New("GetManyView: nil callback")
	}
	retryEpoch := db.readRetryRefreshEpoch.Load()
	var called atomic.Bool
	err := db.getManyViewOnce(keys, func(index int, key []byte, value []byte, found bool) error {
		called.Store(true)
		return fn(index, key, value, found)
	})
	if db.refreshOnValueLogFileNotFound(err) && !called.Load() {
		if refreshErr := db.refreshValueLogSetForReadRetry(retryEpoch); refreshErr != nil {
			return refreshErr
		}
		return db.getManyViewOnce(keys, fn)
	}
	return err
}

func (db *DB) getManyViewOnce(keys [][]byte, fn GetManyViewFunc) error {
	if len(keys) == 0 {
		return nil
	}
	snap, err := db.acquireSnapshotOrErr()
	if err != nil {
		return err
	}
	defer snap.Close()

	workers := getManyWorkerCount(len(keys))
	if getManyCanParallelize(len(keys), workers) {
		return db.getManyViewParallel(snap, keys, fn, workers)
	}
	return snap.GetManyView(keys, fn)
}

func (db *DB) getManyViewParallel(snap *Snapshot, keys [][]byte, fn GetManyViewFunc, workers int) error {
	var (
		wg       sync.WaitGroup
		stop     atomic.Bool
		errMu    sync.Mutex
		firstErr error
	)
	setErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			stop.Store(true)
		}
		errMu.Unlock()
	}
	getErr := func() error {
		errMu.Lock()
		err := firstErr
		errMu.Unlock()
		return err
	}
	for worker := 0; worker < workers; worker++ {
		start, end := getManyChunkBounds(worker, workers, len(keys))
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			err := snap.tree.GetManyView(keys[start:end], func(index int, key []byte, value []byte, found bool) error {
				if stop.Load() {
					return getErr()
				}
				err := fn(start+index, key, value, found)
				if err != nil {
					setErr(err)
				}
				return err
			})
			setErr(err)
		}(start, end)
	}
	wg.Wait()
	return getErr()
}

func (db *DB) getManySequential(snap *Snapshot, keys [][]byte, out [][]byte) error {
	// Copy all found values into a single arena to avoid one allocation per key.
	// Each returned slice is capacity-capped to preserve safe-copy semantics.
	arena := make([]byte, 0, getManyArenaCap(len(keys)))
	_, err := snap.tree.GetManyAppend(keys, out, arena)
	return err
}

func (db *DB) getManyParallel(snap *Snapshot, keys [][]byte, out [][]byte, workers int) error {
	var (
		wg       sync.WaitGroup
		stop     atomic.Bool
		firstErr error
		errMu    sync.Mutex
	)
	for worker := 0; worker < workers; worker++ {
		start, end := getManyChunkBounds(worker, workers, len(keys))
		if start >= end {
			continue
		}
		workerArenaCap := getManyArenaCap(end - start)
		wg.Add(1)
		go func(start, end, arenaCap int) {
			defer wg.Done()
			arena := make([]byte, 0, arenaCap)
			if _, err := snap.tree.GetManyAppend(keys[start:end], out[start:end], arena); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
					stop.Store(true)
				}
				errMu.Unlock()
			}
		}(start, end, workerArenaCap)
	}
	wg.Wait()
	return firstErr
}

// GetUnsafe returns the value for a key.
//
// Semantics: Returns a safe copy of the value. For zero-copy views tied to a
// snapshot lifetime, use Snapshot.GetUnsafe.
func (db *DB) GetUnsafe(key []byte) ([]byte, error) {
	return db.Get(key)
}

// Dir returns the on-disk directory backing the DB.
func (db *DB) Dir() string {
	if db == nil {
		return ""
	}
	return db.dir
}

// ColumnAssetRootDir returns the isolated physical root for typed column asset
// manager payloads. It is separate from ordinary value_vlog and leaf_vlog.
func (db *DB) ColumnAssetRootDir() string {
	if db == nil {
		return ""
	}
	if db.columnAssetRootDir != "" {
		return db.columnAssetRootDir
	}
	return ColumnAssetRootDirPath(db.dir)
}

// CheckStorageMaintenanceReady verifies this handle may run destructive storage
// maintenance such as GC, rewrite cleanup, or typed column asset reclamation.
// It returns ErrClosed for nil or closing handles, ErrReadOnly for read-only
// handles, and otherwise returns any command-WAL poison error that would make
// storage maintenance unsafe.
func (db *DB) CheckStorageMaintenanceReady() error {
	if db == nil || db.closing.Load() {
		return ErrClosed
	}
	if db.readOnly {
		return ErrReadOnly
	}
	return db.commandWALPoisonedError()
}

// GetAppend appends the value for the key to dst and returns the new slice.
// If the key is not found, it returns dst and ErrKeyNotFound.
func (db *DB) GetAppend(key, dst []byte) ([]byte, error) {
	key = normalizeRawKVPointKey(key)
	readOnce := func(base []byte) ([]byte, error) {
		snap, err := db.acquireSnapshotOrErr()
		if err != nil {
			return base, err
		}
		defer snap.Close()
		return snap.GetAppend(key, base)
	}

	retryEpoch := db.readRetryRefreshEpoch.Load()
	val, err := readOnce(dst)
	if db.refreshOnValueLogFileNotFound(err) {
		if refreshErr := db.refreshValueLogSetForReadRetry(retryEpoch); refreshErr != nil {
			return dst, refreshErr
		}
		val, err = readOnce(dst)
	}
	if err == tree.ErrKeyNotFound {
		return dst, err
	}
	if err != nil {
		return dst, err
	}
	return val, nil
}

// GetVersionedAppend appends the value for key to dst and returns the native
// entry revision stored with the visible entry. Missing/tombstoned keys return
// dst and tree.ErrKeyNotFound; tombstones preserve their stored revision.
func (db *DB) GetVersionedAppend(key, dst []byte) ([]byte, page.EntryRevision, error) {
	key = normalizeRawKVPointKey(key)
	readOnce := func(base []byte) ([]byte, page.EntryRevision, error) {
		snap, err := db.acquireSnapshotOrErr()
		if err != nil {
			return base, page.LegacyEntryRevision, err
		}
		defer snap.Close()
		return snap.GetVersionedAppend(key, base)
	}

	retryEpoch := db.readRetryRefreshEpoch.Load()
	val, revision, err := readOnce(dst)
	if db.refreshOnValueLogFileNotFound(err) {
		if refreshErr := db.refreshValueLogSetForReadRetry(retryEpoch); refreshErr != nil {
			return dst, page.LegacyEntryRevision, refreshErr
		}
		val, revision, err = readOnce(dst)
	}
	if err != nil {
		return dst, revision, err
	}
	return val, revision, nil
}

// Has checks if a key exists.
func (db *DB) Has(key []byte) (bool, error) {
	key = normalizeRawKVPointKey(key)
	snap, err := db.acquireSnapshotOrErr()
	if err != nil {
		return false, err
	}
	defer snap.Close()
	return snap.Has(key)
}

// Set sets the value for a key.
func (db *DB) Set(key, value []byte) error {
	key = normalizeRawKVPointKey(key)
	value = normalizeRawKVValue(value)
	guard := db.lockUpdateKey(key)
	defer guard.Unlock()
	return db.setPoint(key, value, false)
}

func (db *DB) setPoint(key, value []byte, sync bool) error {
	key = normalizeRawKVPointKey(key)
	value = normalizeRawKVValue(value)
	if handled, err := db.writeViaCommitCombiner(key, value, false, sync); handled {
		return err
	}
	return db.writeSingleKV(key, value, false, sync)
}

// SetSync sets the value and syncs to disk.
func (db *DB) SetSync(key, value []byte) error {
	key = normalizeRawKVPointKey(key)
	value = normalizeRawKVValue(value)
	guard := db.lockUpdateKey(key)
	defer guard.Unlock()
	return db.setPoint(key, value, true)
}

// Delete removes a key.
func (db *DB) Delete(key []byte) error {
	key = normalizeRawKVPointKey(key)
	guard := db.lockUpdateKey(key)
	defer guard.Unlock()
	return db.deletePoint(key, false)
}

func (db *DB) deletePoint(key []byte, sync bool) error {
	key = normalizeRawKVPointKey(key)
	if handled, err := db.writeViaCommitCombiner(key, nil, true, sync); handled {
		return err
	}
	return db.writeSingleKV(key, nil, true, sync)
}

// DeleteSync removes a key and syncs.
func (db *DB) DeleteSync(key []byte) error {
	key = normalizeRawKVPointKey(key)
	guard := db.lockUpdateKey(key)
	defer guard.Unlock()
	return db.deletePoint(key, true)
}

// DBIterator wraps tree.Iterator and holds a Snapshot.
type DBIterator struct {
	snap *Snapshot
	iter iterator.UnsafeIterator
	err  error
}

type IteratorMode = tree.IteratorMode

const (
	IteratorModeFull              = tree.IteratorModeFull
	IteratorModeKeysOnly          = tree.IteratorModeKeysOnly
	IteratorModePointerProjection = tree.IteratorModePointerProjection
)

type IteratorOptions = tree.IteratorOptions

func (it *DBIterator) DebugStats() (queueLen int, sourcesUsed int) {
	return 0, 1
}

func (it *DBIterator) Next() {
	if !it.Valid() {
		return
	}
	it.iter.Next()
}

func (it *DBIterator) Valid() bool {
	return it.iter.Valid() && it.err == nil
}

// Key returns a read-only view of the current key. The view is valid only
// until the next Next()/Seek()/Close() on this iterator; use KeyCopy for
// stable bytes.
func (it *DBIterator) Key() []byte {
	return it.UnsafeKey()
}

// Value returns a read-only view of the current value. The view is valid only
// until the next Next()/Seek()/Close() on this iterator; use ValueCopy for
// stable bytes.
func (it *DBIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *DBIterator) KeyCopy(dst []byte) []byte {
	k := it.iter.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *DBIterator) ValueCopy(dst []byte) []byte {
	val := it.UnsafeValue()
	if it.err != nil {
		return dst
	}
	if val == nil {
		return nil
	}
	return append(dst[:0], val...)
}

func (it *DBIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.iter.Error()
}

func (it *DBIterator) Close() error {
	err := it.iter.Close()
	if e := it.snap.Close(); e != nil {
		if err == nil {
			err = e
		}
	}
	return err
}

// UnsafeIterator methods
func (it *DBIterator) Seek(key []byte) {
	it.iter.Seek(key)
}

func (it *DBIterator) UnsafeKey() []byte {
	return it.iter.UnsafeKey()
}

func (it *DBIterator) UnsafeValue() []byte {
	if it.err != nil {
		return nil
	}
	val := it.iter.UnsafeValue()
	if err := it.iter.Error(); err != nil {
		it.err = err
		return nil
	}
	it.err = nil
	return val
}

func (it *DBIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return it.iter.UnsafeEntry()
}

func (it *DBIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	return iterator.UnsafeEntryWithRevision(it.iter)
}

func (it *DBIterator) IsDeleted() bool {
	return it.iter.IsDeleted()
}

func (it *DBIterator) Domain() (start, end []byte) {
	return it.iter.Domain()
}

// Iterator returns an iterator.
func (db *DB) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return db.IteratorWithOptions(start, end, IteratorOptions{})
}

// IteratorWithOptions returns an iterator with explicit value materialization
// controls.
func (db *DB) IteratorWithOptions(start, end []byte, opts IteratorOptions) (iterator.UnsafeIterator, error) {
	snap, err := db.acquireSnapshotOrErr()
	if err != nil {
		return nil, err
	}
	it := snap.tree.IteratorWithOptions(start, end, opts)
	return &DBIterator{snap: snap, iter: it}, nil
}

// Iterator returns an iterator bound to an existing snapshot.
func (s *Snapshot) Iterator(start, end []byte) (publiciterator.Iterator, error) {
	return s.IteratorWithOptions(start, end, IteratorOptions{})
}

// IteratorWithOptions returns an iterator bound to an existing snapshot with
// explicit value materialization controls.
func (s *Snapshot) IteratorWithOptions(start, end []byte, opts IteratorOptions) (iterator.UnsafeIterator, error) {
	return s.bindNewIterator(func() iterator.UnsafeIterator {
		return s.buildIteratorLocked(start, end, opts, false)
	})
}

// Iterate calls fn for each visible key/value pair in [start, end) using this
// snapshot's pinned view. Key and value are read-only views valid only until fn
// returns. Returning an error from fn stops iteration and returns that error.
func (s *Snapshot) Iterate(start, end []byte, fn func(key, value []byte) error) error {
	return s.iterate(start, end, false, fn)
}

// ReverseIterator returns a reverse iterator.
func (db *DB) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return db.ReverseIteratorWithOptions(start, end, IteratorOptions{})
}

// ReverseIteratorWithOptions returns a reverse iterator with explicit value
// materialization controls.
func (db *DB) ReverseIteratorWithOptions(start, end []byte, opts IteratorOptions) (iterator.UnsafeIterator, error) {
	snap, err := db.acquireSnapshotOrErr()
	if err != nil {
		return nil, err
	}
	it := snap.tree.ReverseIteratorWithOptions(start, end, opts)
	return &DBIterator{snap: snap, iter: it}, nil
}

// ReverseIterator returns a reverse iterator bound to an existing snapshot.
func (s *Snapshot) ReverseIterator(start, end []byte) (publiciterator.Iterator, error) {
	return s.ReverseIteratorWithOptions(start, end, IteratorOptions{})
}

// ReverseIteratorWithOptions returns a reverse iterator bound to an existing
// snapshot with explicit value materialization controls.
func (s *Snapshot) ReverseIteratorWithOptions(start, end []byte, opts IteratorOptions) (iterator.UnsafeIterator, error) {
	return s.bindNewIterator(func() iterator.UnsafeIterator {
		return s.buildIteratorLocked(start, end, opts, true)
	})
}

// buildIteratorLocked reads the snapshot's pinned tree and must run while
// iteratorMu is held by bindNewIterator.
func (s *Snapshot) buildIteratorLocked(start, end []byte, opts IteratorOptions, reverse bool) iterator.UnsafeIterator {
	if reverse {
		return s.tree.ReverseIteratorWithOptions(start, end, opts)
	}
	return s.tree.IteratorWithOptions(start, end, opts)
}

// ReverseIterate calls fn for each visible key/value pair in [start, end) in
// reverse order using this snapshot's pinned view. Key and value are read-only
// views valid only until fn returns. Returning an error from fn stops iteration
// and returns that error.
func (s *Snapshot) ReverseIterate(start, end []byte, fn func(key, value []byte) error) error {
	return s.iterate(start, end, true, fn)
}

func (s *Snapshot) iterate(start, end []byte, reverse bool, fn func(key, value []byte) error) (err error) {
	if err := s.beginRead(); err != nil {
		return err
	}
	s.endRead()
	if fn == nil {
		return errors.New("treedb: snapshot iterate nil callback")
	}
	var it publiciterator.Iterator
	if reverse {
		it, err = s.ReverseIterator(start, end)
	} else {
		it, err = s.Iterator(start, end)
	}
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, it.Close())
	}()
	var iterErr error
	for it.Valid() {
		key := it.Key()
		value := it.Value()
		if err := it.Error(); err != nil {
			iterErr = err
			break
		}
		if err := fn(key, value); err != nil {
			iterErr = err
			break
		}
		it.Next()
	}
	if iterErr == nil {
		iterErr = it.Error()
	}
	return iterErr
}

// Stats returns database statistics.
func (db *DB) Stats() map[string]string {
	stats := make(map[string]string)
	stats["cosmos.db.type"] = "treedb"
	stats["treedb.profile.resolved"] = string(db.resolvedProfile)
	stats["treedb.profile.ordinary_ack_class"] = db.resolvedProfile.OrdinaryAckClass()
	stats["treedb.profile.production"] = fmt.Sprintf("%t", db.resolvedProfile.Production())
	stats["treedb.profile.bench_unsafe"] = fmt.Sprintf("%t", db.resolvedProfile == ProfileBenchUnsafe)
	stats["treedb.profile.deprecated_alias"] = string(db.deprecatedProfileAlias)

	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.state == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return stats
	}
	defer func() { _ = snap.Close() }()

	state := snap.state
	idx := snap.idx

	stats["treedb.commit_seq"] = fmt.Sprintf("%d", state.CommitSeq)
	stats["treedb.root_page"] = fmt.Sprintf("%d", state.RootPageID)
	stats["treedb.system_root_page"] = fmt.Sprintf("%d", state.SystemRootPageID)
	stats["treedb.applied_command_lsn"] = fmt.Sprintf("%d", state.AppliedCommandLSN)
	stats["treedb.max_entry_revision"] = fmt.Sprintf("%d", state.MaxEntryRevision)
	db.appendDurableRootStats(stats)
	stats["treedb.command_wal.enabled"] = fmt.Sprintf("%t", db.commandWAL)
	writeCommandWALStats(stats, db)
	db.appendConditionalTxnStats(stats)
	db.appendFlushApplyStats(stats)
	db.appendRawSpanNativeStats(stats)
	stats["treedb.keep_recent"] = fmt.Sprintf("%d", db.keepRecent)
	stats["treedb.prefer_append_alloc"] = fmt.Sprintf("%t", db.preferAppendAlloc)
	stats["treedb.freelist_region_pages"] = fmt.Sprintf("%d", db.freelistRegionPages)
	stats["treedb.freelist_region_radius"] = fmt.Sprintf("%d", db.freelistRegionRadius)

	writeLeafGenerationMetrics(stats, db.collectLeafGenerationMetrics(state.ValueLogSet, snap.leafGenerationPinnedIDs))

	stats["treedb.pages.total"] = fmt.Sprintf("%d", idx.pager.PageCount())
	fs := idx.allocator.Counters()
	stats["treedb.freelist.head"] = fmt.Sprintf("%d", fs.Head)
	stats["treedb.freelist.alloc_pages_total"] = fmt.Sprintf("%d", fs.AllocPages)
	stats["treedb.freelist.append_alloc_pages_total"] = fmt.Sprintf("%d", fs.AppendAllocPages)
	stats["treedb.freelist.reuse_alloc_pages_total"] = fmt.Sprintf("%d", fs.ReuseAllocPages)
	stats["treedb.freelist.free_pages_total"] = fmt.Sprintf("%d", fs.FreePages)
	graveyard := idx.graveyard.Stats()
	stats["treedb.graveyard.batches"] = fmt.Sprintf("%d", graveyard.Batches)
	stats["treedb.graveyard.pages"] = fmt.Sprintf("%d", graveyard.Pages)
	// PR1 generational scaffolding (backend/read-only path). Cached mode exports
	// richer live counters; backend path reports stable defaults.
	stats["treedb.vlog_generation.enabled"] = "false"
	stats["treedb.vlog_generation.policy"] = "0"
	stats["treedb.vlog_generation.scheduler_state"] = "disabled"
	stats["treedb.vlog_generation.bytes.live.total"] = "0"
	stats["treedb.vlog_generation.bytes.stale.total"] = "0"
	stats["treedb.vlog_generation.bytes.total.total"] = "0"
	stats["treedb.vlog_generation.segments.total"] = "0"
	stats["treedb.vlog_generation.rewrite.bytes_in"] = "0"
	stats["treedb.vlog_generation.rewrite.bytes_out"] = "0"
	stats["treedb.vlog_generation.gc.deleted_segments"] = "0"
	stats["treedb.vlog_generation.gc.deleted_bytes"] = "0"
	stats["treedb.vlog_generation.remap.successes"] = "0"
	stats["treedb.vlog_generation.remap.failures"] = "0"
	growStats := valuelog.GrowBufferStatsSnapshot()
	stats["treedb.vlog.decode_buffer_grow.calls_total"] = fmt.Sprintf("%d", growStats.CallsTotal)
	stats["treedb.vlog.decode_buffer_grow.realloc_calls_total"] = fmt.Sprintf("%d", growStats.ReallocCallsTotal)
	stats["treedb.vlog.decode_buffer_grow.requested_bytes_total"] = fmt.Sprintf("%d", growStats.RequestedBytesTotal)
	stats["treedb.vlog.decode_buffer_grow.allocated_bytes_total"] = fmt.Sprintf("%d", growStats.AllocatedBytesTotal)
	stats["treedb.vlog.decode_buffer_grow.copied_bytes_total"] = fmt.Sprintf("%d", growStats.CopiedBytesTotal)
	stats["treedb.vlog.decode_buffer_grow.capacity_waste_bytes_total"] = fmt.Sprintf("%d", growStats.CapacityWasteBytesTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_compressed_fallback.calls_total"] = fmt.Sprintf("%d", growStats.ReadAppendCompressedFallbackCallsTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_compressed_fallback.requested_bytes_total"] = fmt.Sprintf("%d", growStats.ReadAppendCompressedFallbackRequestedBytesTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_compressed_fallback.dst_present_calls_total"] = fmt.Sprintf("%d", growStats.ReadAppendCompressedFallbackDstPresentCallsTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_compressed_fallback.dst_fit_calls_total"] = fmt.Sprintf("%d", growStats.ReadAppendCompressedFallbackDstFitCallsTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_compressed_fallback.dst_fit_requested_bytes_total"] = fmt.Sprintf("%d", growStats.ReadAppendCompressedFallbackDstFitRequestedBytesTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_payload.calls_total"] = fmt.Sprintf("%d", growStats.ReadAppendPayloadCallsTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_payload.requested_bytes_total"] = fmt.Sprintf("%d", growStats.ReadAppendPayloadRequestedBytesTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_current_mmap_direct_decode.calls_total"] = fmt.Sprintf("%d", growStats.ReadAppendCurrentMmapDirectDecodeCallsTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_current_mmap_direct_decode.requested_bytes_total"] = fmt.Sprintf("%d", growStats.ReadAppendCurrentMmapDirectDecodeRequestedBytesTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_decoded_payload.calls_total"] = fmt.Sprintf("%d", growStats.ReadAppendDecodedPayloadCallsTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_decoded_payload.requested_bytes_total"] = fmt.Sprintf("%d", growStats.ReadAppendDecodedPayloadRequestedBytesTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_decoded_payload.dst_present_calls_total"] = fmt.Sprintf("%d", growStats.ReadAppendDecodedPayloadDstPresentCallsTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_decoded_payload.dst_fit_calls_total"] = fmt.Sprintf("%d", growStats.ReadAppendDecodedPayloadDstFitCallsTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_decoded_payload.dst_fit_requested_bytes_total"] = fmt.Sprintf("%d", growStats.ReadAppendDecodedPayloadDstFitRequestedBytesTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_template_encoded_payload.calls_total"] = fmt.Sprintf("%d", growStats.ReadAppendTemplateEncodedPayloadCallsTotal)
	stats["treedb.vlog.decode_buffer_grow.read_append_template_encoded_payload.requested_bytes_total"] = fmt.Sprintf("%d", growStats.ReadAppendTemplateEncodedPayloadRequestedBytesTotal)
	if growStats.CallsTotal > 0 {
		stats["treedb.vlog.decode_buffer_grow.realloc_rate"] = fmt.Sprintf("%.6f", float64(growStats.ReallocCallsTotal)/float64(growStats.CallsTotal))
	}
	if growStats.ReallocCallsTotal > 0 {
		stats["treedb.vlog.decode_buffer_grow.avg_allocated_bytes_per_realloc"] = fmt.Sprintf("%.3f", float64(growStats.AllocatedBytesTotal)/float64(growStats.ReallocCallsTotal))
		stats["treedb.vlog.decode_buffer_grow.avg_copied_bytes_per_realloc"] = fmt.Sprintf("%.3f", float64(growStats.CopiedBytesTotal)/float64(growStats.ReallocCallsTotal))
	}
	if growStats.RequestedBytesTotal > 0 {
		stats["treedb.vlog.decode_buffer_grow.overalloc_ratio"] = fmt.Sprintf("%.6f", float64(growStats.AllocatedBytesTotal)/float64(growStats.RequestedBytesTotal))
	}
	readPathStats := tree.ReadPathStatsSnapshot()
	stats["treedb.process.read_path.backend_tree.get_append_inline_hits_total"] = fmt.Sprintf("%d", readPathStats.GetAppendInlineHitsTotal)
	stats["treedb.process.read_path.backend_tree.get_append_inline_bytes_total"] = fmt.Sprintf("%d", readPathStats.GetAppendInlineBytesTotal)
	stats["treedb.process.read_path.backend_tree.get_append_pointer_hits_total"] = fmt.Sprintf("%d", readPathStats.GetAppendPointerHitsTotal)
	stats["treedb.process.read_path.backend_tree.get_append_pointer_bytes_total"] = fmt.Sprintf("%d", readPathStats.GetAppendPointerBytesTotal)
	getManyReadStats := tree.GetManyReadStatsSnapshot()
	stats["treedb.process.read_path.backend_tree.getmany.calls_total"] = fmt.Sprintf("%d", getManyReadStats.CallsTotal)
	stats["treedb.process.read_path.backend_tree.getmany.grouped_calls_total"] = fmt.Sprintf("%d", getManyReadStats.GroupedCallsTotal)
	stats["treedb.process.read_path.backend_tree.getmany.fallback_calls_total"] = fmt.Sprintf("%d", getManyReadStats.FallbackCallsTotal)
	stats["treedb.process.read_path.backend_tree.getmany.leaf_groups_total"] = fmt.Sprintf("%d", getManyReadStats.LeafGroupsTotal)
	stats["treedb.process.read_path.backend_tree.getmany.leaf_group_items_total"] = fmt.Sprintf("%d", getManyReadStats.LeafGroupItemsTotal)
	stats["treedb.process.read_path.backend_tree.getmany.leaf_loads_saved_total"] = fmt.Sprintf("%d", getManyReadStats.LeafLoadsSavedTotal)
	outerLeafReadStats := tree.OuterLeafReadStatsSnapshot()
	stats["treedb.process.read_path.outer_leaf.loads_total"] = fmt.Sprintf("%d", outerLeafReadStats.LoadsTotal)
	stats["treedb.process.read_path.outer_leaf.point_loads_total"] = fmt.Sprintf("%d", outerLeafReadStats.PointLoadsTotal)
	stats["treedb.process.read_path.outer_leaf.iterator_loads_total"] = fmt.Sprintf("%d", outerLeafReadStats.IteratorLoadsTotal)
	stats["treedb.process.read_path.outer_leaf.bytes_total"] = fmt.Sprintf("%d", outerLeafReadStats.BytesTotal)
	stats["treedb.process.read_path.outer_leaf.checksum.verifications_total"] = fmt.Sprintf("%d", outerLeafReadStats.ChecksumVerifiedTotal)
	stats["treedb.process.read_path.outer_leaf.checksum.skips_total"] = fmt.Sprintf("%d", outerLeafReadStats.ChecksumSkippedTotal)
	stats["treedb.process.read_path.outer_leaf.sample_mod"] = fmt.Sprintf("%d", outerLeafReadStats.SampleMod)
	stats["treedb.process.read_path.outer_leaf.samples_total"] = fmt.Sprintf("%d", outerLeafReadStats.SamplesTotal)
	stats["treedb.process.read_path.outer_leaf.cache_potential.capacity_64_hits_total"] = fmt.Sprintf("%d", outerLeafReadStats.Recent64HitsTotal)
	stats["treedb.process.read_path.outer_leaf.cache_potential.capacity_256_hits_total"] = fmt.Sprintf("%d", outerLeafReadStats.Recent256HitsTotal)
	stats["treedb.process.read_path.outer_leaf.cache_potential.capacity_1024_hits_total"] = fmt.Sprintf("%d", outerLeafReadStats.Recent1KHitsTotal)
	stats["treedb.process.read_path.outer_leaf.cache_potential.capacity_4096_hits_total"] = fmt.Sprintf("%d", outerLeafReadStats.Recent4KHitsTotal)
	if outerLeafReadStats.SamplesTotal > 0 {
		stats["treedb.process.read_path.outer_leaf.cache_potential.capacity_64_hit_ratio"] = fmt.Sprintf("%.6f", float64(outerLeafReadStats.Recent64HitsTotal)/float64(outerLeafReadStats.SamplesTotal))
		stats["treedb.process.read_path.outer_leaf.cache_potential.capacity_256_hit_ratio"] = fmt.Sprintf("%.6f", float64(outerLeafReadStats.Recent256HitsTotal)/float64(outerLeafReadStats.SamplesTotal))
		stats["treedb.process.read_path.outer_leaf.cache_potential.capacity_1024_hit_ratio"] = fmt.Sprintf("%.6f", float64(outerLeafReadStats.Recent1KHitsTotal)/float64(outerLeafReadStats.SamplesTotal))
		stats["treedb.process.read_path.outer_leaf.cache_potential.capacity_4096_hit_ratio"] = fmt.Sprintf("%.6f", float64(outerLeafReadStats.Recent4KHitsTotal)/float64(outerLeafReadStats.SamplesTotal))
	}
	cacheStats := db.leafPageReadCache.stats()
	stats["treedb.process.read_path.outer_leaf.cache.hits"] = fmt.Sprintf("%d", cacheStats.Hits)
	stats["treedb.process.read_path.outer_leaf.cache.misses"] = fmt.Sprintf("%d", cacheStats.Misses)
	stats["treedb.process.read_path.outer_leaf.cache.stores"] = fmt.Sprintf("%d", cacheStats.Stores)
	stats["treedb.process.read_path.outer_leaf.cache.evictions"] = fmt.Sprintf("%d", cacheStats.Evictions)
	stats["treedb.process.read_path.outer_leaf.cache.conflict_evictions"] = fmt.Sprintf("%d", cacheStats.ConflictEvictions)
	stats["treedb.process.read_path.outer_leaf.cache.capacity_evictions"] = fmt.Sprintf("%d", cacheStats.CapacityEvictions)
	stats["treedb.process.read_path.outer_leaf.cache.entries"] = fmt.Sprintf("%d", cacheStats.Entries)
	stats["treedb.process.read_path.outer_leaf.cache.capacity"] = fmt.Sprintf("%d", cacheStats.Capacity)
	stats["treedb.process.read_path.outer_leaf.cache.buckets"] = fmt.Sprintf("%d", cacheStats.Buckets)
	stats["treedb.process.read_path.outer_leaf.cache.ways"] = fmt.Sprintf("%d", cacheStats.Ways)
	stats["treedb.process.read_path.outer_leaf.cache.bytes"] = fmt.Sprintf("%d", cacheStats.Bytes)
	stats["treedb.process.read_path.outer_leaf.cache.write_admission_policy"] = db.leafPageReadCache.writeAdmissionPolicyName()
	stats["treedb.process.read_path.outer_leaf.cache.read_miss_admission_skips"] = fmt.Sprintf("%d", cacheStats.ReadMissAdmissionSkips)
	stats["treedb.process.read_path.outer_leaf.cache.read_miss_admission_candidate_skips"] = fmt.Sprintf("%d", cacheStats.ReadMissAdmissionCandidateSkips)
	stats["treedb.process.read_path.outer_leaf.cache.read_miss_admission_lock_skips"] = fmt.Sprintf("%d", cacheStats.ReadMissAdmissionLockSkips)
	stats["treedb.process.read_path.outer_leaf.cache.read_miss_admission_stores"] = fmt.Sprintf("%d", cacheStats.ReadMissAdmissionStores)
	stats["treedb.process.read_path.outer_leaf.cache.write_admission_attempts"] = fmt.Sprintf("%d", cacheStats.WriteAdmissionAttempts)
	stats["treedb.process.read_path.outer_leaf.cache.write_admission_stores"] = fmt.Sprintf("%d", cacheStats.WriteAdmissionStores)
	stats["treedb.process.read_path.outer_leaf.cache.write_admission_skips"] = fmt.Sprintf("%d", cacheStats.WriteAdmissionSkips)
	stats["treedb.process.read_path.outer_leaf.cache.write_admission_lock_skips"] = fmt.Sprintf("%d", cacheStats.WriteAdmissionLockSkips)
	stats["treedb.process.read_path.outer_leaf.cache.record_checksum_verified_stores"] = fmt.Sprintf("%d", cacheStats.RecordChecksumVerifiedStores)
	stats["treedb.process.read_path.outer_leaf.cache.page_checksum_verified_marks"] = fmt.Sprintf("%d", cacheStats.PageChecksumVerifiedMarks)
	stats["treedb.process.read_path.outer_leaf.cache.page_checksum_verified_hits"] = fmt.Sprintf("%d", cacheStats.PageChecksumVerifiedHits)
	stats["treedb.process.read_path.outer_leaf.cache.page_checksum_unverified_hits"] = fmt.Sprintf("%d", cacheStats.PageChecksumUnverifiedHits)
	stats["treedb.process.read_path.outer_leaf.cache.page_checksum_mark_misses"] = fmt.Sprintf("%d", cacheStats.PageChecksumMarkMisses)
	stats["treedb.process.read_path.outer_leaf.cache.page_checksum_mark_unsafe_skips"] = fmt.Sprintf("%d", cacheStats.PageChecksumMarkUnsafeSkips)

	if db.valueLogManager != nil {
		vlogRemaps, vlogDeadMappings := db.valueLogManager.RemapStats()
		stats["treedb.vlog.mmap_remaps"] = fmt.Sprintf("%d", vlogRemaps)
		stats["treedb.vlog.mmap_dead_mappings"] = fmt.Sprintf("%d", vlogDeadMappings)
		stats["treedb.vlog.mmap_dead_mappings.cap_base"] = fmt.Sprintf("%d", valuelog.MaxDeadMappings)
		stats["treedb.vlog.mmap_current_writable_map_target_bytes"] = fmt.Sprintf("%d", valuelog.CurrentWritableMmapTargetBytes)
		stats["treedb.vlog.mmap_max_mapped_sealed_segments"] = fmt.Sprintf("%d", valuelog.MaxMappedSealedSegments)
		stats["treedb.vlog.mmap_max_mapped_sealed_bytes"] = fmt.Sprintf("%d", valuelog.MaxMappedSealedBytes)
		stats["treedb.vlog.mmap_max_mapped_leaf_sealed_segments"] = fmt.Sprintf("%d", valuelog.MaxMappedLeafSealedSegments)
		stats["treedb.vlog.mmap_max_mapped_leaf_sealed_bytes"] = fmt.Sprintf("%d", valuelog.MaxMappedLeafSealedBytes)
		currentSegments, currentBytes, sealedSegments, sealedBytes, _, deadBytes := db.valueLogManager.MmapResidencyStats()
		stats["treedb.vlog.mmap_active_segments"] = fmt.Sprintf("%d", currentSegments+sealedSegments)
		stats["treedb.vlog.mmap_active_bytes"] = fmt.Sprintf("%d", currentBytes+sealedBytes)
		stats["treedb.vlog.mmap_current_segments"] = fmt.Sprintf("%d", currentSegments)
		stats["treedb.vlog.mmap_current_bytes"] = fmt.Sprintf("%d", currentBytes)
		stats["treedb.vlog.mmap_sealed_segments"] = fmt.Sprintf("%d", sealedSegments)
		stats["treedb.vlog.mmap_sealed_bytes"] = fmt.Sprintf("%d", sealedBytes)
		stats["treedb.vlog.mmap_dead_bytes"] = fmt.Sprintf("%d", deadBytes)
		sealedDeniedCountCap, sealedDeniedBytesCap := db.valueLogManager.SealedMapDeniedByReasonStats()
		stats["treedb.vlog.mmap_sealed_map_denied.count_cap"] = fmt.Sprintf("%d", sealedDeniedCountCap)
		stats["treedb.vlog.mmap_sealed_map_denied.bytes_cap"] = fmt.Sprintf("%d", sealedDeniedBytesCap)
		stats["treedb.vlog.mmap_sealed_map_denied"] = fmt.Sprintf("%d", sealedDeniedCountCap+sealedDeniedBytesCap)

		mmapHits, mmapMissOutOfRange, mmapMissNoMapping, mmapMissDeadCap, mmapFallbackReadAt := db.valueLogManager.MmapReadStats()
		stats["treedb.vlog.mmap_read.hits"] = fmt.Sprintf("%d", mmapHits)
		stats["treedb.vlog.mmap_read.miss_out_of_range"] = fmt.Sprintf("%d", mmapMissOutOfRange)
		stats["treedb.vlog.mmap_read.miss_no_mapping"] = fmt.Sprintf("%d", mmapMissNoMapping)
		stats["treedb.vlog.mmap_read.miss_dead_mapping_cap"] = fmt.Sprintf("%d", mmapMissDeadCap)
		stats["treedb.vlog.mmap_read.fallback_readat"] = fmt.Sprintf("%d", mmapFallbackReadAt)
		if total := mmapHits + mmapFallbackReadAt; total > 0 {
			stats["treedb.vlog.mmap_read.hit_ratio"] = fmt.Sprintf("%.6f", float64(mmapHits)/float64(total))
		}

		readStats := db.valueLogManager.ReadStats()
		stats["treedb.vlog.read.crc32_checks_total"] = fmt.Sprintf("%d", readStats.RecordCRCChecks)

		valuelog.AppendDecodeScratchStats(stats, "treedb.vlog.decode_scratch", db.valueLogManager.DecodeScratchStats())
		valuelog.AppendWriterAppendBufferStats(stats, "treedb.vlog.writer_append_buf", valuelog.WriterAppendBufferStatsSnapshot())

		gStats := db.valueLogManager.GroupedFrameCacheDetailedStats()
		stats["treedb.vlog.grouped_frame_cache.hits"] = fmt.Sprintf("%d", gStats.Hits)
		stats["treedb.vlog.grouped_frame_cache.misses"] = fmt.Sprintf("%d", gStats.Misses)
		stats["treedb.vlog.grouped_frame_cache.stores"] = fmt.Sprintf("%d", gStats.Stores)
		stats["treedb.vlog.grouped_frame_cache.evictions"] = fmt.Sprintf("%d", gStats.Evictions)
		stats["treedb.vlog.grouped_frame_cache.releases"] = fmt.Sprintf("%d", gStats.Releases)
		stats["treedb.vlog.grouped_frame_cache.retained_bytes"] = fmt.Sprintf("%d", gStats.RetainedBytes)
		stats["treedb.vlog.grouped_frame_cache.budget_bytes"] = fmt.Sprintf("%d", gStats.BudgetBytes)
		stats["treedb.vlog.grouped_frame_cache.skipped_disabled"] = fmt.Sprintf("%d", gStats.SkippedDisabled)
		stats["treedb.vlog.grouped_frame_cache.skipped_oversize"] = fmt.Sprintf("%d", gStats.SkippedOversize)
		stats["treedb.vlog.grouped_frame_cache.skipped_budget"] = fmt.Sprintf("%d", gStats.SkippedBudget)
		stats["treedb.vlog.grouped_frame_cache.skipped_contention"] = fmt.Sprintf("%d", gStats.SkippedContention)
		stats["treedb.vlog.grouped_frame_cache.entries"] = fmt.Sprintf("%d", gStats.Entries)
		stats["treedb.vlog.grouped_frame_cache.capacity"] = fmt.Sprintf("%d", gStats.Capacity)
		stats["treedb.vlog.grouped_frame_cache.allocated_shards"] = fmt.Sprintf("%d", gStats.AllocatedShards)
		stats["treedb.vlog.grouped_frame_cache.allocated_slots"] = fmt.Sprintf("%d", gStats.AllocatedSlots)
		if total := gStats.Hits + gStats.Misses; total > 0 {
			stats["treedb.vlog.grouped_frame_cache.hit_ratio"] = fmt.Sprintf("%.6f", float64(gStats.Hits)/float64(total))
		}

		stats["treedb.vlog.read_retry_refresh.leader_calls"] = fmt.Sprintf("%d", db.readRetryRefreshLeaderCount.Load())
		stats["treedb.vlog.read_retry_refresh.follower_calls"] = fmt.Sprintf("%d", db.readRetryRefreshFollowerCount.Load())
		stats["treedb.vlog.read_retry_refresh.skipped_epoch_calls"] = fmt.Sprintf("%d", db.readRetryRefreshSkippedEpoch.Load())

		hits, misses, entries, capacity := db.valueLogManager.TemplateDefCacheStats()
		stats["treedb.vlog.template_def_cache.hits"] = fmt.Sprintf("%d", hits)
		stats["treedb.vlog.template_def_cache.misses"] = fmt.Sprintf("%d", misses)
		stats["treedb.vlog.template_def_cache.entries"] = fmt.Sprintf("%d", entries)
		stats["treedb.vlog.template_def_cache.capacity"] = fmt.Sprintf("%d", capacity)
		if total := hits + misses; total > 0 {
			stats["treedb.vlog.template_def_cache.hit_ratio"] = fmt.Sprintf("%.6f", float64(hits)/float64(total))
		}
	}
	watermarkLockDelaySharePct, watermarkLatencyP99Ms := db.publishWatermarkStats()
	stats["treedb.publish.watermark.lock_delay_share_pct"] = fmt.Sprintf("%.3f", watermarkLockDelaySharePct)
	stats["treedb.publish.watermark.latency_p99_ms"] = fmt.Sprintf("%.3f", watermarkLatencyP99Ms)
	// Backend DB path currently doesn't track queue drift; emit a stable default
	// for suite compatibility and fail-closed checks that require key presence.
	stats["treedb.publish.watermark.lag_drift_bytes_per_sec"] = "0.000"
	orderedDeltaStats := db.orderedRootDeltaGroupPublishStats()
	// Ordered-root delta group stats cover calls that entered the DB write
	// lock, including failed calls. roots_total counts successfully published
	// non-system roots, avg_roots_per_call divides by calls_total, and the
	// latency fields include both write-lock wait time and lock hold time.
	stats["treedb.publish.ordered_root_delta_group.calls_total"] = fmt.Sprintf("%d", orderedDeltaStats.calls)
	stats["treedb.publish.ordered_root_delta_group.errors_total"] = fmt.Sprintf("%d", orderedDeltaStats.errors)
	stats["treedb.publish.ordered_root_delta_group.roots_total"] = fmt.Sprintf("%d", orderedDeltaStats.roots)
	stats["treedb.publish.ordered_root_delta_group.avg_roots_per_call"] = fmt.Sprintf("%.3f", orderedDeltaStats.avgRootsPerCall)
	stats["treedb.publish.ordered_root_delta_group.write_lock_wait_ns_total"] = fmt.Sprintf("%d", orderedDeltaStats.waitTotalNs)
	stats["treedb.publish.ordered_root_delta_group.write_lock_hold_ns_total"] = fmt.Sprintf("%d", orderedDeltaStats.holdTotalNs)
	stats["treedb.publish.ordered_root_delta_group.write_lock_wait_share_pct"] = fmt.Sprintf("%.3f", orderedDeltaStats.writeLockWaitShare)
	stats["treedb.publish.ordered_root_delta_group.preflight_ns_total"] = fmt.Sprintf("%d", orderedDeltaStats.preflightNs)
	stats["treedb.publish.ordered_root_delta_group.root_apply_ns_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyNs)
	stats["treedb.publish.ordered_root_delta_group.root_apply_calls_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyCalls)
	stats["treedb.publish.ordered_root_delta_group.root_apply_parallel_groups_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyParallelGroups)
	stats["treedb.publish.ordered_root_delta_group.root_apply_parallel_roots_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyParallelRoots)
	stats["treedb.publish.ordered_root_delta_group.root_apply_ops_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyOps)
	stats["treedb.publish.ordered_root_delta_group.root_apply_node_loads_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyNodeLoads)
	stats["treedb.publish.ordered_root_delta_group.root_apply_pager_node_loads_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyPagerNodeLoads)
	stats["treedb.publish.ordered_root_delta_group.root_apply_leaf_log_node_loads_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyLeafLogNodeLoads)
	stats["treedb.publish.ordered_root_delta_group.root_apply_leaf_log_cache_hits_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyLeafLogCacheHits)
	stats["treedb.publish.ordered_root_delta_group.root_apply_leaf_log_reader_calls_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyLeafLogReaderCalls)
	stats["treedb.publish.ordered_root_delta_group.root_apply_leaf_log_view_reads_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyLeafLogViewReads)
	stats["treedb.publish.ordered_root_delta_group.root_apply_leaf_log_scratch_reads_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyLeafLogScratchReads)
	stats["treedb.publish.ordered_root_delta_group.root_apply_pager_node_bytes_read_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyPagerNodeBytesRead)
	stats["treedb.publish.ordered_root_delta_group.root_apply_leaf_log_node_bytes_read_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyLeafLogNodeBytesRead)
	stats["treedb.publish.ordered_root_delta_group.root_apply_leaf_log_record_hint_bytes_read_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyLeafLogRecordHintBytesRead)
	stats["treedb.publish.ordered_root_delta_group.root_apply_leaf_merges_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyLeafMerges)
	stats["treedb.publish.ordered_root_delta_group.root_apply_internal_merges_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyInternalMerges)
	stats["treedb.publish.ordered_root_delta_group.root_apply_internal_parallel_merges_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyInternalParallelMerges)
	stats["treedb.publish.ordered_root_delta_group.root_apply_internal_parallel_children_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyInternalParallelChildren)
	stats["treedb.publish.ordered_root_delta_group.root_apply_internal_parallel_workers_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyInternalParallelWorkers)
	stats["treedb.publish.ordered_root_delta_group.root_apply_internal_parallel_ops_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyInternalParallelOps)
	stats["treedb.publish.ordered_root_delta_group.root_apply_leaf_pages_written_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyLeafPagesWritten)
	stats["treedb.publish.ordered_root_delta_group.root_apply_pager_leaf_pages_written_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyPagerLeafPagesWritten)
	stats["treedb.publish.ordered_root_delta_group.root_apply_leaf_log_pages_written_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyLeafLogPagesWritten)
	stats["treedb.publish.ordered_root_delta_group.root_apply_leaf_page_bytes_written_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyLeafPageBytesWritten)
	stats["treedb.publish.ordered_root_delta_group.root_apply_pager_leaf_page_bytes_written_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyPagerLeafPageBytesWritten)
	stats["treedb.publish.ordered_root_delta_group.root_apply_leaf_log_page_bytes_written_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyLeafLogPageBytesWritten)
	stats["treedb.publish.ordered_root_delta_group.root_apply_leaf_log_record_hint_bytes_written_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyLeafLogRecordHintBytesWritten)
	stats["treedb.publish.ordered_root_delta_group.root_apply_internal_pages_written_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyInternalPagesWritten)
	stats["treedb.publish.ordered_root_delta_group.root_apply_internal_page_bytes_written_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyInternalPageBytesWritten)
	stats["treedb.publish.ordered_root_delta_group.root_apply_internal_child_refs_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyInternalChildRefs)
	stats["treedb.publish.ordered_root_delta_group.root_apply_internal_page_child_refs_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyInternalPageChildRefs)
	stats["treedb.publish.ordered_root_delta_group.root_apply_internal_leaf_log_refs_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyInternalLeafLogRefs)
	stats["treedb.publish.ordered_root_delta_group.root_apply_internal_leaf_log_ref_copies_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyInternalLeafLogRefCopies)
	stats["treedb.publish.ordered_root_delta_group.root_apply_root_split_levels_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyRootSplitLevels)
	stats["treedb.publish.ordered_root_delta_group.root_apply_read_only_prepare_ns_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyReadOnlyPrepareNs)
	stats["treedb.publish.ordered_root_delta_group.root_apply_read_only_prepare_calls_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyReadOnlyPrepareCalls)
	stats["treedb.publish.ordered_root_delta_group.root_apply_read_only_prepare_errors_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyReadOnlyPrepareErrors)
	stats["treedb.publish.ordered_root_delta_group.root_apply_read_only_prepare_validation_failures_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyReadOnlyPrepareValidationFail)
	stats["treedb.publish.ordered_root_delta_group.root_apply_read_only_prepare_requested_workers_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyReadOnlyPrepareRequested)
	stats["treedb.publish.ordered_root_delta_group.root_apply_read_only_prepare_requested_workers_max"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyReadOnlyPrepareRequestedMax)
	stats["treedb.publish.ordered_root_delta_group.root_apply_read_only_prepare_spans_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyReadOnlyPrepareSpans)
	stats["treedb.publish.ordered_root_delta_group.root_apply_read_only_prepare_span_ops_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyReadOnlyPrepareSpanOps)
	stats["treedb.publish.ordered_root_delta_group.root_apply_read_only_prepare_span_bytes_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyReadOnlyPrepareSpanBytes)
	stats["treedb.publish.ordered_root_delta_group.root_apply_read_only_prepare_worker_ranges_total"] = fmt.Sprintf("%d", orderedDeltaStats.rootApplyReadOnlyPrepareWorkerRanges)
	stats["treedb.publish.ordered_root_delta_group.system_build_ns_total"] = fmt.Sprintf("%d", orderedDeltaStats.systemBuildNs)
	stats["treedb.publish.ordered_root_delta_group.system_apply_ns_total"] = fmt.Sprintf("%d", orderedDeltaStats.systemApplyNs)
	stats["treedb.publish.ordered_root_delta_group.system_apply_calls_total"] = fmt.Sprintf("%d", orderedDeltaStats.systemApplyCalls)
	stats["treedb.publish.ordered_root_delta_group.system_apply_ops_total"] = fmt.Sprintf("%d", orderedDeltaStats.systemApplyOps)
	stats["treedb.publish.ordered_root_delta_group.system_apply_node_loads_total"] = fmt.Sprintf("%d", orderedDeltaStats.systemApplyNodeLoads)
	stats["treedb.publish.ordered_root_delta_group.publish_prepare_ns_total"] = fmt.Sprintf("%d", orderedDeltaStats.publishPrepareNs)
	stats["treedb.publish.ordered_root_delta_group.publish_prepare_calls_total"] = fmt.Sprintf("%d", orderedDeltaStats.publishPrepareCalls)
	stats["treedb.publish.ordered_root_delta_group.publish_prepare_errors_total"] = fmt.Sprintf("%d", orderedDeltaStats.publishPrepareErrors)
	stats["treedb.publish.ordered_root_delta_group.finalize_ns_total"] = fmt.Sprintf("%d", orderedDeltaStats.finalizeNs)
	stats["treedb.publish.ordered_root_delta_group.finalize_calls_total"] = fmt.Sprintf("%d", orderedDeltaStats.finalizeCalls)
	stats["treedb.publish.ordered_root_delta_group.latency_p99_ms"] = fmt.Sprintf("%.3f", float64(orderedDeltaStats.latencyP99)/float64(time.Millisecond))
	stats["treedb.publish.ordered_root_delta_group.latency_max_ms"] = fmt.Sprintf("%.3f", float64(orderedDeltaStats.latencyMax)/float64(time.Millisecond))
	db.appendOrderedRootSpanNativeStats(stats)
	warmPublishStats := db.systemRootPublishStatsSnapshot()
	stats["treedb.publish.system_root.warm_attempts"] = fmt.Sprintf("%d", warmPublishStats.warmAttempts)
	stats["treedb.publish.system_root.warm_native_apply_attempts"] = fmt.Sprintf("%d", warmPublishStats.warmNativeApplyAttempts)
	stats["treedb.publish.system_root.warm_rebuild_fallbacks"] = fmt.Sprintf("%d", warmPublishStats.warmRebuildFallbacks)
	stats["treedb.publish.system_root.warm_preserved_pages"] = fmt.Sprintf("%d", warmPublishStats.warmPreservedPages)
	stats["treedb.publish.system_root.warm_rewritten_pages"] = fmt.Sprintf("%d", warmPublishStats.warmRewrittenPages)
	rootProbeStats := db.rootProbeStatsSnapshot()
	stats["treedb.root_probe.has_any_sorted.fallback_calls"] = fmt.Sprintf("%d", rootProbeStats.keyFallbackCalls)
	stats["treedb.root_probe.has_any_sorted.fallback_items"] = fmt.Sprintf("%d", rootProbeStats.keyFallbackItems)
	stats["treedb.root_probe.has_prefixes.fallback_calls"] = fmt.Sprintf("%d", rootProbeStats.prefixFallbackCalls)
	stats["treedb.root_probe.has_prefixes.fallback_items"] = fmt.Sprintf("%d", rootProbeStats.prefixFallbackItems)
	stats["treedb.native_fastpath.per_item_key_probe_fallback_count"] = fmt.Sprintf("%d", rootProbeStats.keyFallbackItems)
	stats["treedb.native_fastpath.per_item_prefix_probe_fallback_count"] = fmt.Sprintf("%d", rootProbeStats.prefixFallbackItems)

	pruneStatsInto(stats, &db.pruner)

	return stats
}

// Print debugs the tree (simple dump).
func (db *DB) Print() error {
	// Not implemented fully
	return nil
}
