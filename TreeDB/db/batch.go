package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
)

// Batch implements the cosmos-db Batch interface.
type Batch struct {
	db    *DB
	batch *batch.Batch
}

const optimisticWriteMaxAttempts = 3

const (
	// Public NewBatchWithSize comes from cosmos-db, where callers often pass a
	// byte-oriented flush threshold rather than an entry count. Keep small hints
	// behaving like entry reserves, but normalize larger hints conservatively so
	// a 100kB budget does not preallocate 100k entries.
	publicBatchHintExactEntryReserveMax = 8 * 1024
	publicBatchHintApproxBytesPerEntry  = 256
	publicBatchHintNormalizedEntryCap   = publicBatchHintExactEntryReserveMax
)

func (db *DB) NewBatch() batch.Interface {
	return db.newBatchWithEntryReserve(0)
}

// NewBatchWithSize accepts the public cosmos-db style size hint. Small values
// are treated like exact entry reserves; larger values are normalized as
// approximate byte budgets and capped to avoid preallocating one entry per
// byte. The normalization is intentionally discontinuous at the cutover:
// `publicBatchHintExactEntryReserveMax` still means "reserve that many
// entries", while the next value is treated as a byte budget and normalized
// downward.
func (db *DB) NewBatchWithSize(size int) batch.Interface {
	reserveHint := NormalizePublicBatchReserveHint(size)
	return db.newBatchWithReserveHint(reserveHint)
}

// NormalizePublicBatchReserveHint keeps small public hints behaving like entry
// reserves, but treats larger hints as approximate byte budgets so callers do
// not accidentally preallocate one entry per byte. This is intentionally
// discontinuous at the cutover for compatibility with small entry-count hints.
//
// For internal use; behavior may change without notice and is not part of the
// supported external API surface of the db package.
func NormalizePublicBatchReserveHint(size int) int {
	if size <= 0 {
		return 0
	}
	if size <= publicBatchHintExactEntryReserveMax {
		return size
	}
	entries := size / publicBatchHintApproxBytesPerEntry
	if size%publicBatchHintApproxBytesPerEntry != 0 {
		entries++
	}
	// Defensive guard in case the cutover/bytes-per-entry constants change.
	if entries < 1 {
		entries = 1
	}
	if entries > publicBatchHintNormalizedEntryCap {
		entries = publicBatchHintNormalizedEntryCap
	}
	return entries
}

func (db *DB) newBatchWithEntryReserve(entries int) batch.Interface {
	return db.newBatchWithReserveHint(entries)
}

func (db *DB) newBatchWithReserveHint(reserveHint int) batch.Interface {
	if db != nil && db.testBatchCreateHook != nil {
		db.testBatchCreateHook()
	}
	threshold := db.InlineThreshold()
	domains := db.valueLogDomainThresholds
	if reserveHint < 0 {
		reserveHint = 0
	}
	internal := batch.New(db.valueLogManager, threshold)
	if threshold > 0 {
		internal.SetInlineThresholdResolver(func(key []byte) int {
			return ResolveInlineThresholdForKey(threshold, key, domains)
		})
	}
	internal.Reserve(reserveHint)
	return &Batch{
		db:    db,
		batch: internal,
	}
}

func (b *Batch) Set(key, value []byte) error {
	return b.batch.Set(key, value)
}

// SetView records a Put without copying key/value bytes. Callers must treat
// key/value as immutable until the batch is written or closed.
//
// This is intentionally not part of the public batch.Interface; it is a
// best-effort optimization used by higher-level layers (e.g. cached streaming).
func (b *Batch) SetView(key, value []byte) error {
	return b.batch.SetView(key, value)
}

func (b *Batch) Delete(key []byte) error {
	return b.batch.Delete(key)
}

// DeleteView records a Delete without copying the key bytes. Callers must treat
// key as immutable until the batch is written or closed.
func (b *Batch) DeleteView(key []byte) error {
	return b.batch.DeleteView(key)
}

// SetPointer records a pointer without copying the value bytes.
func (b *Batch) SetPointer(key []byte, ptr page.ValuePtr) error {
	return b.batch.SetPointer(key, ptr)
}

// SetPointerView records a pointer without copying the key bytes.
func (b *Batch) SetPointerView(key []byte, ptr page.ValuePtr) error {
	return b.batch.SetPointerView(key, ptr)
}

func (b *Batch) SetOps(ops []batch.Entry) error {
	return b.batch.SetOps(ops)
}

// Reserve forwards best-effort preallocation hints to the internal batch.
func (b *Batch) Reserve(n int) {
	if b == nil || b.batch == nil || n <= 0 {
		return
	}
	b.batch.Reserve(n)
}

func (b *Batch) Write() error {
	return b.write(false)
}

func (b *Batch) WriteSync() error {
	return b.write(true)
}

func (b *Batch) write(sync bool) error {
	if b == nil || b.db == nil {
		return fmt.Errorf("missing db")
	}
	if b.db.readOnly {
		return ErrReadOnly
	}
	if sync && b.batch != nil && len(b.batch.SortedEntries()) == 0 {
		return b.db.Checkpoint()
	}
	intent, err := b.db.prepareRawKVCommandWALIntent(b)
	if err != nil {
		return err
	}
	return b.writeWithCommandWALIntent(sync, intent)
}

func (b *Batch) writeWithCommandWALIntent(sync bool, intent *commandWALBatchIntent) error {
	// If a command frame is appended but root publication later returns an
	// error, the durable frame is intentionally left for reopen recovery. Reuse
	// the same intent across optimistic/serialized attempts so one user batch
	// keeps one command LSN.
	for attempt := 0; attempt < optimisticWriteMaxAttempts; attempt++ {
		committed, err := b.writeOptimistic(sync, intent)
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
	}
	return b.writeSerialized(sync, intent)
}

func (b *Batch) writeOptimistic(sync bool, intent *commandWALBatchIntent) (bool, error) {
	touchedValueLogSegments := b.batch.TouchedValueLogSegments()

	b.db.writeMu.RLock()
	idx := b.db.idx.Load()
	if idx == nil {
		b.db.writeMu.RUnlock()
		return false, fmt.Errorf("missing index")
	}

	b.db.mu.RLock()
	rootID := b.db.meta.UserRootPageID
	baseSeq := b.db.meta.CommitSeq
	// Register this writer as a "reader" of the base state to prevent the
	// pruner from reclaiming pages we are about to read during z.Apply.
	regID := idx.registry.Register(baseSeq)
	b.db.mu.RUnlock()

	defer idx.registry.Unregister(regID)

	tracker := newAllocTracker(idx.allocator)
	z := idx.zipper.CloneWithAllocator(tracker)
	newRoot, retired, metrics, err := z.Apply(rootID, b.batch)
	if err != nil {
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, err
	}
	entries := b.batch.SortedEntries()
	vlogRefDelta, err := b.db.buildValueLogRefDelta(idx.pager, rootID, baseSeq, entries)
	if err != nil {
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, err
	}
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()
	b.db.commitMu.Lock()
	b.db.mu.RLock()
	currentRoot := b.db.meta.UserRootPageID
	sysRoot := b.db.meta.SystemRootPageID
	b.db.mu.RUnlock()
	if currentRoot != rootID {
		b.db.commitMu.Unlock()
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, nil
	}

	if _, err := b.db.appendRawKVCommandWALIntent(intent, sync); err != nil {
		b.db.commitMu.Unlock()
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, err
	}
	post, err := b.db.finalizeCommitLockedWithOptions(newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments, b.db.indexOuterLeavesInValueLog, vlogRefDelta, nil, nil, commandWALFinalizeOptions(intent))
	b.db.commitMu.Unlock()
	if err != nil {
		b.db.writeMu.RUnlock()
		return false, err
	}
	vlogRefDelta = nil
	b.db.invalidateLeafGenerationSubtreeStats(tracker.Pages())
	b.db.finalizeCommitPostWork(post)
	if b.db.vacuum.Active() {
		b.db.vacuum.RecordEntries(entries)
	}
	b.db.writeMu.RUnlock()
	return true, nil
}

func (b *Batch) writeSerialized(sync bool, intent *commandWALBatchIntent) error {
	touchedValueLogSegments := b.batch.TouchedValueLogSegments()

	b.db.writeMu.Lock()
	defer b.db.writeMu.Unlock()

	idx := b.db.idx.Load()
	if idx == nil {
		return fmt.Errorf("missing index")
	}

	b.db.mu.RLock()
	rootID := b.db.meta.UserRootPageID
	baseSeq := b.db.meta.CommitSeq
	regID := idx.registry.Register(baseSeq)
	b.db.mu.RUnlock()

	defer idx.registry.Unregister(regID)

	newRoot, retired, metrics, err := idx.zipper.Apply(rootID, b.batch)
	if err != nil {
		return err
	}
	entries := b.batch.SortedEntries()
	vlogRefDelta, err := b.db.buildValueLogRefDelta(idx.pager, rootID, baseSeq, entries)
	if err != nil {
		return err
	}
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()

	b.db.mu.Lock()
	if b.db.meta.UserRootPageID != rootID {
		// This should not happen if writeMu is held and we are the only writer.
		b.db.mu.Unlock()
		return fmt.Errorf("concurrent modification detected during batch write")
	}
	sysRoot := b.db.meta.SystemRootPageID
	b.db.mu.Unlock()

	if _, err := b.db.appendRawKVCommandWALIntent(intent, sync); err != nil {
		return err
	}
	post, err := b.db.finalizeCommitLockedWithOptions(newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments, b.db.indexOuterLeavesInValueLog, vlogRefDelta, nil, nil, commandWALFinalizeOptions(intent))
	if err != nil {
		return err
	}
	vlogRefDelta = nil
	b.db.finalizeCommitPostWork(post)
	b.db.clearLeafGenerationReachabilityCaches()
	if b.db.vacuum.Active() {
		b.db.vacuum.RecordEntries(entries)
	}
	return nil
}

func (b *Batch) Close() error {
	if b.batch != nil {
		err := b.batch.Close()
		b.batch = nil
		return err
	}
	b.batch = nil
	return nil
}

// Reset clears the batch for reuse.
func (b *Batch) Reset() {
	if b == nil || b.batch == nil {
		return
	}
	b.batch.Reset()
}

func (b *Batch) Replay(fn func(batch.Entry) error) error {
	if b == nil || b.batch == nil {
		return nil
	}
	entries := b.batch.SortedEntries()
	for _, entry := range entries {
		if entry.IsPtr && entry.Value == nil {
			ptr := entry.ValuePtr
			if !page.IsValueLogFileID(ptr.FileID) {
				return fmt.Errorf("expected value-log pointer, got file=%d", ptr.FileID)
			}
			if b.db == nil || b.db.valueLogManager == nil {
				return fmt.Errorf("missing value log manager")
			}
			val, err := b.db.valueLogManager.Read(ptr)
			if err != nil {
				return err
			}
			entry.Value = val
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) GetByteSize() (int, error) {
	return b.batch.ByteSize(), nil
}
