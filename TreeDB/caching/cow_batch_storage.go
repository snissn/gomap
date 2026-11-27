package caching

import (
	"math"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	cowBatchMaxReuseHint     = 8192
	cowBatchCopyArenaHintMax = 2 << 20
)

func cowBatchCopyArenaHint(db *DB, entryHint int) int {
	observed := db.batchCopyBytesHint.Load()
	if observed <= 0 {
		return 0
	}
	if observed > batchCopyArenaInitMax {
		if observed > cowBatchCopyArenaHintMax {
			return cowBatchCopyArenaHintMax
		}
		// Round up to 64 KiB so batches slightly above 1 MiB (e.g. 8000*136B)
		// fit in a single copyArena without a 64 KiB overflow tail chunk.
		const align = 64 << 10
		rounded := int(((observed + align - 1) / align) * align)
		if rounded > cowBatchCopyArenaHintMax {
			rounded = cowBatchCopyArenaHintMax
		}
		return rounded
	}
	return db.batchCopyArenaInitCap(entryHint)
}

// cowBatchScratch retains ONLY cleared, zero-length batch slice backings for
// budget-admitted idle reuse. It never references Batch, DB, COWExternalLease,
// keys, values, or read cuts.
type cowBatchScratch struct {
	entries           []batch.Entry
	copyArena         []byte
	predictionGroups  [][]memtable.COWMutation
	predictionStorage []memtable.COWMutation
	shardAdds         []int64
	shardCnts         []int
	shardIdxs         []int
	eligibleIdxs      []int
}

func cowBatchScratchCharge(s *cowBatchScratch) uint64 {
	if s == nil {
		return 0
	}
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBatchScratch{})))
	if cap(s.entries) > 0 {
		bytes += memtable.COWAllocationCharge(uint64(cap(s.entries)) * uint64(unsafe.Sizeof(batch.Entry{})))
	}
	if cap(s.copyArena) > 0 {
		bytes += memtable.COWAllocationCharge(uint64(cap(s.copyArena)))
	}
	if cap(s.predictionGroups) > 0 {
		bytes += memtable.COWAllocationCharge(uint64(cap(s.predictionGroups)) * uint64(unsafe.Sizeof([]memtable.COWMutation{})))
	}
	if cap(s.predictionStorage) > 0 {
		bytes += memtable.COWAllocationCharge(uint64(cap(s.predictionStorage)) * uint64(unsafe.Sizeof(memtable.COWMutation{})))
	}
	if cap(s.shardAdds) > 0 {
		bytes += memtable.COWAllocationCharge(uint64(cap(s.shardAdds)) * uint64(unsafe.Sizeof(int64(0))))
	}
	if cap(s.shardCnts) > 0 {
		bytes += memtable.COWAllocationCharge(uint64(cap(s.shardCnts)) * uint64(unsafe.Sizeof(int(0))))
	}
	if cap(s.shardIdxs) > 0 {
		bytes += memtable.COWAllocationCharge(uint64(cap(s.shardIdxs)) * uint64(unsafe.Sizeof(int(0))))
	}
	if cap(s.eligibleIdxs) > 0 {
		bytes += memtable.COWAllocationCharge(uint64(cap(s.eligibleIdxs)) * uint64(unsafe.Sizeof(int(0))))
	}
	return bytes
}

func cowDeleteArenaCap(entryHint, keyLen, copyHint int) int {
	if copyHint <= 0 {
		return 0
	}
	perKey := max(keyLen, 16)
	if entryHint <= 0 {
		return min(copyHint, 4096)
	}
	if entryHint > math.MaxInt/perKey {
		return copyHint
	}
	est := max(4096, entryHint*perKey)
	return min(copyHint, est)
}

func normalizeCOWBatchReuseHint(n int) int {
	if n <= 0 {
		return 0
	}
	if n > cowBatchMaxReuseHint {
		return cowBatchMaxReuseHint
	}
	return n
}

// cowBatchState exists only for explicitly admitted COW batches. Keeping its
// ownership and prediction fields separate preserves ordinary batch allocations.
type cowBatchState struct {
	native                       *cowNativeOwner
	holdBegan                    time.Time
	receipt                      COWPublicationReceipt
	accepted                     rootpublication.AcceptedTicket
	guard                        *backenddb.CommandWALStagingGuardV1
	txn                          *ConditionalTxn
	lease                        *memtable.COWExternalLease
	storageBytes                 uint64
	lastHint                     int
	copyArenaHint                int
	copiedBytes                  int
	err                          error
	prepared                     *cowBatchPreparation
	inlinePrep                   cowBatchPreparation
	producer                     *cowFrameCapture
	predictionGroups             [][]memtable.COWMutation
	predictionStorage            []memtable.COWMutation
	scratch                      *cowBatchScratch
	largestCopyArena             []byte
	inPublication, trustedAppend bool
	guardReserved                bool
	shardAssignmentsReady        bool
	privatePoint                 bool
	prepareAttempted             bool
}

// One admitted allocation owns the public batch and its COW-only state. Both
// interior pointers retain this owner; ordinary batches need no sidecar storage.
type cowBatchOwner struct {
	batch Batch
	state cowBatchState
}

// Shared immutable refusal carrier: constructing a refused batch allocates no
// wrapper. All COW staging/write/close paths test the state error before mutation.
var cowDeniedBatch = Batch{cowState: &cowBatchState{err: memtable.ErrCOWCapacity}, closed: true}

// AcquireCOWAllocation admits concrete caller-owned wrapper/backing capacities
// before allocation. The returned independent lease lasts until that caller
// drops its final references; it does not transfer ownership of batch storage.
func (db *DB) AcquireCOWAllocation(bytes uint64) (*memtable.COWExternalLease, error) {
	if db == nil || db.cow == nil {
		return nil, ErrCOWUnsupported
	}
	return db.cow.budget.AcquireExternal(bytes)
}

// The immutable constructor reservation is derivable without keeping a second
// byte count beside storageBytes. The guard itself can be absent or released.
func cowBatchBaseBytes(guardReserved bool) uint64 {
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBatchOwner{})))
	if guardReserved {
		charge += memtable.COWAllocationCharge(backenddb.CommandWALStagingGuardAllocationBytesV1())
	}
	return charge
}

// NewPrivateCOWPointBatch marks one-shot writes from the owning public DB.
// The caller retains its input through Write and Close. Actual inline
// eligibility is established after placement and final revision assignment.
func (db *DB) NewPrivateCOWPointBatch(owner *transactionowner.Owner) *Batch {
	if db == nil || db.cow == nil || owner == nil || owner != db.cow.txnOwner || !db.disableJournal || db.externalCommandWAL {
		return nil
	}
	b := db.newCOWBatch(1)
	if b.cowState.err == nil {
		b.cowState.privatePoint = true
	}
	return b
}

func (b *Batch) cowAttachIdleScratch(sc *cowBatchScratch, payload uint64) {
	if cap(sc.entries) > 0 {
		b.entries = sc.entries[:0]
	}
	if cap(sc.copyArena) > 0 {
		b.copyArena = sc.copyArena[:0]
	}
	if cap(sc.shardAdds) > 0 {
		b.shardAdds = sc.shardAdds[:0]
	}
	if cap(sc.shardCnts) > 0 {
		b.shardCnts = sc.shardCnts[:0]
	}
	if cap(sc.shardIdxs) > 0 {
		b.shardIdxs = sc.shardIdxs[:0]
	}
	if cap(sc.eligibleIdxs) > 0 {
		b.eligibleIdxs = sc.eligibleIdxs[:0]
	}
	if cap(sc.predictionGroups) > 0 {
		b.cowState.predictionGroups = sc.predictionGroups[:0]
	}
	if cap(sc.predictionStorage) > 0 {
		b.cowState.predictionStorage = sc.predictionStorage[:0]
	}
	b.cowState.storageBytes += payload
	b.cowState.scratch = sc
	*sc = cowBatchScratch{}
}

func (db *DB) newCOWBatch(hint int) *Batch {
	guardReserved := db.externalCommandWAL && db.cow.txnOwner != nil
	charge := cowBatchBaseBytes(guardReserved)
	normHint := normalizeCOWBatchReuseHint(hint)
	copyHint := cowBatchCopyArenaHint(db, normHint)
	if normHint > batchDefaultEntriesCap {
		if val, lease, payload := db.cow.budget.TakeIdleBatchScratch(charge); lease != nil {
			sc := val.(*cowBatchScratch)
			owner := &cowBatchOwner{
				batch: Batch{db: db, streamBypassOff: true},
				state: cowBatchState{
					lease:         lease,
					guardReserved: guardReserved,
					lastHint:      normHint,
					copyArenaHint: copyHint,
				},
			}
			b := &owner.batch
			b.cowState = &owner.state
			b.cowAttachIdleScratch(sc, payload)
			if cap(b.entries) < hint {
				b.cowReserve(hint)
			}
			return b
		}
	}
	lease, err := db.cow.budget.AcquireExternal(charge)
	if err != nil {
		return &cowDeniedBatch
	}
	owner := &cowBatchOwner{
		batch: Batch{db: db, streamBypassOff: true},
		state: cowBatchState{
			lease:         lease,
			guardReserved: guardReserved,
			lastHint:      normHint,
			copyArenaHint: copyHint,
		},
	}
	b := &owner.batch
	b.cowState = &owner.state
	b.cowReserve(hint)
	return b
}

func (db *DB) newCOWBatchAdaptive() *Batch {
	guardReserved := db.externalCommandWAL && db.cow.txnOwner != nil
	charge := cowBatchBaseBytes(guardReserved)
	entryHint := normalizeCOWBatchReuseHint(db.batchEntriesCapHint(batchDefaultEntriesCap))
	copyHint := cowBatchCopyArenaHint(db, entryHint)
	if entryHint > batchDefaultEntriesCap {
		if val, lease, payload := db.cow.budget.TakeIdleBatchScratch(charge); lease != nil {
			sc := val.(*cowBatchScratch)
			owner := &cowBatchOwner{
				batch: Batch{db: db, streamBypassOff: true},
				state: cowBatchState{
					lease:         lease,
					guardReserved: guardReserved,
					lastHint:      entryHint,
					copyArenaHint: copyHint,
				},
			}
			b := &owner.batch
			b.cowState = &owner.state
			b.cowAttachIdleScratch(sc, payload)
			return b
		}
	}
	lease, err := db.cow.budget.AcquireExternal(charge)
	if err != nil {
		return &cowDeniedBatch
	}
	owner := &cowBatchOwner{
		batch: Batch{db: db, streamBypassOff: true},
		state: cowBatchState{
			lease:         lease,
			guardReserved: guardReserved,
			lastHint:      entryHint,
			copyArenaHint: copyHint,
		},
	}
	b := &owner.batch
	b.cowState = &owner.state
	// Keep cold-start constructor behavior eager at batchDefaultEntriesCap so
	// callers and tests expecting partial-admission failure on initial NewBatch()
	// keep their contract; larger learned hints defer to combined lazy admission.
	if entryHint <= batchDefaultEntriesCap {
		b.cowReserve(batchDefaultEntriesCap)
	}
	return b
}

// newCOWBatchDeferred creates a COW batch without eagerly allocating raw entry
// staging slices. Native-only conditional transactions publish zero raw entries
// and avoid paying the 16-entry slice and lease-resize cost unless raw entries
// are actually staged.
func (db *DB) newCOWBatchDeferred() *Batch {
	guardReserved := db.externalCommandWAL && db.cow.txnOwner != nil
	charge := cowBatchBaseBytes(guardReserved)
	lease, err := db.cow.budget.AcquireExternal(charge)
	if err != nil {
		return &cowDeniedBatch
	}
	owner := &cowBatchOwner{
		batch: Batch{db: db, streamBypassOff: true},
		state: cowBatchState{
			lease:         lease,
			guardReserved: guardReserved,
		},
	}
	b := &owner.batch
	b.cowState = &owner.state
	return b
}

func (b *Batch) cowAdmitStorage(bytes uint64) error {
	if b.cowState.err != nil {
		return b.cowState.err
	}
	// The existing batch lease owns all backing growth, including discarded
	// arrays whose immutable aliases can survive until transaction completion.
	// Admit before allocation; replacing a backing never refunds its charge.
	if bytes > ^uint64(0)-b.cowState.storageBytes {
		return memtable.ErrCOWCapacity
	}
	storageBytes := b.cowState.storageBytes + bytes
	baseBytes := cowBatchBaseBytes(b.cowState.guardReserved)
	if storageBytes > ^uint64(0)-baseBytes {
		return memtable.ErrCOWCapacity
	}
	if err := b.cowState.lease.Resize(baseBytes + storageBytes); err != nil {
		return err
	}
	b.cowState.storageBytes = storageBytes
	return nil
}

func (b *Batch) cowReserve(n int) {
	if b.cowState.err != nil || b.closed || n <= cap(b.entries) {
		return
	}
	if n < 0 || uint64(n) > ^uint64(0)/uint64(unsafe.Sizeof(batch.Entry{})) {
		b.cowState.err = memtable.ErrCOWCapacity
		return
	}
	if err := b.cowAdmitStorage(memtable.COWAllocationCharge(uint64(n) * uint64(unsafe.Sizeof(batch.Entry{})))); err != nil {
		b.cowState.err = err
		return
	}
	entries := make([]batch.Entry, len(b.entries), n)
	copy(entries, b.entries)
	b.entries = entries
}

// cowTryReserveWithCopyArena attempts to admit and allocate at least n entries
// (plus an optional copyArena chunk of arenaCap bytes) in one atomic budget
// check without latching b.cowState.err on refusal. Callers use it for
// best-effort reuse hints and geometric growth so tight budgets fall back
// cleanly instead of consuming remaining headroom on oversized entry capacity.
func (b *Batch) cowTryReserveWithCopyArena(n, arenaCap int) bool {
	if b.cowState.err != nil || b.closed {
		return false
	}
	var totalBytes uint64
	needEntries := n > cap(b.entries)
	if needEntries {
		if n < 0 || uint64(n) > ^uint64(0)/uint64(unsafe.Sizeof(batch.Entry{})) {
			return false
		}
		totalBytes = memtable.COWAllocationCharge(uint64(n) * uint64(unsafe.Sizeof(batch.Entry{})))
	}
	needArena := arenaCap > 0
	if needArena {
		arenaBytes := memtable.COWAllocationCharge(uint64(arenaCap))
		if arenaBytes > ^uint64(0)-totalBytes {
			return false
		}
		totalBytes += arenaBytes
	}
	if totalBytes == 0 {
		return true
	}
	if err := b.cowAdmitStorage(totalBytes); err != nil {
		return false
	}
	if needEntries {
		entries := make([]batch.Entry, len(b.entries), n)
		copy(entries, b.entries)
		b.entries = entries
	}
	if needArena {
		b.copyArena = make([]byte, 0, arenaCap)
	}
	return true
}

func cowNextCopyArenaCap(curCap, n int) int {
	capacity := curCap * 2
	if capacity < 256 {
		capacity = 256
	}
	if capacity > 64<<10 {
		capacity = 64 << 10
	}
	if capacity < n {
		capacity = n
	}
	return capacity
}

func (b *Batch) cowAddEntry(e batch.Entry, owned bool) error {
	if b.cowState.err != nil {
		return b.cowState.err
	}
	if b.closed {
		return ErrBatchClosed
	}
	n := len(e.Key) + len(e.Value)
	if n < len(e.Key) {
		return memtable.ErrCOWCapacity
	}
	if len(b.entries) == cap(b.entries) {
		if cap(b.entries) == 0 && b.cowState.txn == nil && b.cowState.lastHint > batchDefaultEntriesCap &&
			b.db != nil && b.db.cow != nil {
			if val, payload := b.db.cow.budget.TakeIdleBatchScratchIntoLease(b.cowState.lease); val != nil {
				b.cowAttachIdleScratch(val.(*cowBatchScratch), payload)
			}
		}
	}
	if len(b.entries) == cap(b.entries) {
		if cap(b.entries) == 0 && (b.cowState.lastHint > batchDefaultEntriesCap || b.cowState.copyArenaHint > 256) {
			targetEntries := max(batchDefaultEntriesCap, b.cowState.lastHint)
			arenaCap := 0
			if owned && cap(b.copyArena)-len(b.copyArena) < n {
				arenaCap = cowNextCopyArenaCap(cap(b.copyArena), n)
				copyHint := b.cowState.copyArenaHint
				if e.Type == batch.OpDelete {
					copyHint = cowDeleteArenaCap(targetEntries, len(e.Key), copyHint)
				}
				if copyHint > arenaCap {
					arenaCap = copyHint
				}
			}
			if !b.cowTryReserveWithCopyArena(targetEntries, arenaCap) {
				b.cowReserve(batchDefaultEntriesCap)
				if b.cowState.err != nil {
					return b.cowState.err
				}
			}
		} else {
			next := cap(b.entries) * 2
			if next < 16 {
				next = 16
			}
			if next <= cap(b.entries) {
				return memtable.ErrCOWCapacity
			}
			b.cowReserve(next)
			if b.cowState.err != nil {
				return b.cowState.err
			}
		}
	}
	if owned {
		buf, err := b.cowCopyStorageWithDelete(n, len(e.Key), e.Type == batch.OpDelete)
		if err != nil {
			return err
		}
		b.cowState.copiedBytes += n
		copy(buf, e.Key)
		e.Key = normalizeRawKVPointKey(buf[:len(e.Key):len(e.Key)])
		if e.Type == batch.OpPut {
			value := buf[len(e.Key):]
			copy(value, e.Value)
			e.Value = normalizeRawKVValue(value)
		}
	} else {
		b.hasViewOps = true
	}
	b.entries = append(b.entries, e)
	b.cowState.shardAssignmentsReady = false
	b.noteEntryAppend()
	b.size += n
	return nil
}

func (b *Batch) cowSetOps(ops []batch.Entry) error {
	if b.cowState.err != nil {
		return b.cowState.err
	}
	if b.closed {
		return ErrBatchClosed
	}
	// Validate every operation before staging any part of a caller's group.
	for _, e := range ops {
		if (e.Type != batch.OpPut && e.Type != batch.OpDelete) || e.IsPtr {
			return ErrCOWUnsupported
		}
	}
	if len(ops) > 0 {
		need := len(b.entries) + len(ops)
		if need < len(b.entries) {
			return memtable.ErrCOWCapacity
		}
		if need > cap(b.entries) {
			target := need
			if cap(b.entries) <= int(^uint(0)>>1)/2 {
				if doubled := cap(b.entries) * 2; doubled > target {
					target = doubled
				}
			}
			if target < 16 {
				target = 16
			}
			if target > need && !b.cowTryReserveWithCopyArena(target, 0) {
				target = need
			}
			if need > cap(b.entries) {
				b.cowReserve(target)
				if b.cowState.err != nil {
					return b.cowState.err
				}
			}
		}
	}
	for _, e := range ops {
		e.Key = normalizeRawKVPointKey(e.Key)
		if e.Type == batch.OpPut {
			e.Value = normalizeRawKVValue(e.Value)
		}
		if err := b.cowAddEntry(e, true); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) cowBuildIdleScratch() (*cowBatchScratch, uint64) {
	clear(b.entries)
	clear(b.cowState.predictionStorage)
	clear(b.cowState.predictionGroups)
	sc := b.cowState.scratch
	if sc == nil {
		sc = &cowBatchScratch{}
	}
	*sc = cowBatchScratch{}
	if cap(b.entries) > 0 {
		sc.entries = b.entries[:0]
	}
	bestArena := b.copyArena
	if cap(b.cowState.largestCopyArena) > cap(bestArena) {
		bestArena = b.cowState.largestCopyArena
	}
	if cap(bestArena) > 0 {
		sc.copyArena = bestArena[:0]
	}
	if cap(b.cowState.predictionGroups) > 0 {
		sc.predictionGroups = b.cowState.predictionGroups[:0]
	}
	if cap(b.cowState.predictionStorage) > 0 {
		sc.predictionStorage = b.cowState.predictionStorage[:0]
	}
	if cap(b.shardAdds) > 0 {
		sc.shardAdds = b.shardAdds[:0]
	}
	if cap(b.shardCnts) > 0 {
		sc.shardCnts = b.shardCnts[:0]
	}
	if cap(b.shardIdxs) > 0 {
		sc.shardIdxs = b.shardIdxs[:0]
	}
	if cap(b.eligibleIdxs) > 0 {
		sc.eligibleIdxs = b.eligibleIdxs[:0]
	}
	return sc, cowBatchScratchCharge(sc)
}

func (b *Batch) cowResetFields() {
	b.releaseNativePublication()
	if n := normalizeCOWBatchReuseHint(len(b.entries)); n > b.cowState.lastHint {
		b.cowState.lastHint = n
	}
	if copied := b.cowState.copiedBytes; copied > b.cowState.copyArenaHint {
		if copied > cowBatchCopyArenaHintMax {
			copied = cowBatchCopyArenaHintMax
		} else if copied > batchCopyArenaInitMax {
			const align = 64 << 10
			copied = ((copied + align - 1) / align) * align
			if copied > cowBatchCopyArenaHintMax {
				copied = cowBatchCopyArenaHintMax
			}
		}
		b.cowState.copyArenaHint = copied
	}
	b.cowState.copiedBytes = 0
	b.entries = nil
	b.mvccSummary = mvccadmission.Summary{}
	b.copyArena = nil
	b.cowState.largestCopyArena = nil
	b.shardAdds = nil
	b.shardCnts = nil
	b.shardIdxs = nil
	b.eligibleIdxs = nil
	b.cowState.predictionGroups = nil
	b.cowState.predictionStorage = nil
	b.cowState.scratch = nil
	b.cowState.shardAssignmentsReady = false
	b.cowState.privatePoint = false
	b.stableViewValueLeaseChunks = nil
	b.size, b.maxEntries = 0, 0
	b.hasViewOps, b.hasDeleteRanges = false, false
	b.commandWALAppend = nil
	b.commandWALPreparedRevision, b.commandWALMaterializedRID = false, false
	b.entryRevision = page.LegacyEntryRevision
	b.cowState.err = nil
}

func (b *Batch) cowShrinkLeaseToBase() {
	if b.cowState.txn != nil {
		// Read dependencies can still alias discarded copyArena backing.
		// ConditionalTxn.finish clears them before closing this batch.
		return
	}
	if b.cowState.storageBytes != 0 {
		// Shrink follows alias cleanup and is allowed after budget Close.
		// A failed resize must not forget charge that is still outstanding.
		if err := b.cowState.lease.Resize(cowBatchBaseBytes(b.cowState.guardReserved)); err != nil {
			b.cowState.err = err
			return
		}
		b.cowState.storageBytes = 0
	}
}

func (b *Batch) cowReset() {
	if b == &cowDeniedBatch {
		return
	}
	b.cowResetFields()
	b.cowShrinkLeaseToBase()
}

func (b *Batch) cowResetAfterWrite() {
	if b == &cowDeniedBatch {
		return
	}
	if b.cowState.txn == nil && b.cowState.err == nil && !b.closed &&
		b.db != nil && b.db.cow != nil && cap(b.entries) > batchDefaultEntriesCap {
		sc, retained := b.cowBuildIdleScratch()
		b.cowResetFields()
		if b.db.cow.budget.ParkIdleBatchScratchFromLease(sc, b.cowState.lease, cowBatchBaseBytes(b.cowState.guardReserved), retained) {
			b.cowState.storageBytes = 0
			return
		}
		sc = nil
		b.cowShrinkLeaseToBase()
		return
	}
	b.cowReset()
}

func (b *Batch) cowClose() error {
	if b == &cowDeniedBatch || b.closed {
		return nil
	}
	if b.cowState.err == nil && b.cowState.txn == nil &&
		!b.cowState.inPublication && b.cowState.native == nil &&
		b.cowState.prepared == nil && b.cowState.producer == nil &&
		b.db != nil && b.db.cow != nil &&
		b.db.batchEntryHint.Load() > batchDefaultEntriesCap &&
		cap(b.entries) > batchDefaultEntriesCap {
		sc, retained := b.cowBuildIdleScratch()
		b.cowResetFields()
		b.closed = true
		if b.db.cow.budget.PutIdleBatchScratch(sc, b.cowState.lease, retained) {
			b.cowState.storageBytes = 0
			return nil
		}
		sc = nil
		b.cowState.storageBytes = 0
		b.cowState.lease.Close()
		return nil
	}
	b.cowReset()
	b.closed = true
	b.cowState.lease.Close()
	// Keep the closed lease identity so repeated methods stay on the COW path.
	return nil
}

// Each write attempt reserves exact auxiliary capacities before placement or
// the writer sequencer. Failed attempts remain charged until the batch resets.
func (b *Batch) cowPrepareWriteStorage() error {
	n, shards := len(b.entries), len(b.db.mutableShards)
	if n == 0 && b.cowState.native != nil {
		// Zero-raw-entry native publications skip shard prediction and raw
		// staging vectors completely; only the canonical dispatch/unlock
		// callback environments remain charged on this attempt.
		bytes := 3*memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0)))) +
			memtable.COWAllocationCharge(3*uint64(unsafe.Sizeof(uintptr(0))))
		if err := b.cowAdmitStorage(bytes); err != nil {
			return err
		}
		b.cowState.shardAssignmentsReady = false
		return nil
	}
	needShardAdds := cap(b.shardAdds) < shards
	needShardCnts := cap(b.shardCnts) < shards
	needShardIdxs := cap(b.shardIdxs) < n
	needEligibleIdxs := cap(b.eligibleIdxs) < n
	needPredictionGroups := cap(b.cowState.predictionGroups) < shards
	needPredictionStorage := cap(b.cowState.predictionStorage) < n

	var bytes uint64
	if needShardAdds {
		bytes += memtable.COWAllocationCharge(uint64(shards) * uint64(unsafe.Sizeof(int64(0))))
	}
	if needShardCnts {
		bytes += memtable.COWAllocationCharge(uint64(shards) * uint64(unsafe.Sizeof(int(0))))
	}
	if needShardIdxs {
		bytes += memtable.COWAllocationCharge(uint64(n) * uint64(unsafe.Sizeof(int(0))))
	}
	if needEligibleIdxs {
		bytes += memtable.COWAllocationCharge(uint64(n) * uint64(unsafe.Sizeof(int(0))))
	}
	if needPredictionGroups {
		bytes += memtable.COWAllocationCharge(uint64(shards) * uint64(unsafe.Sizeof([]memtable.COWMutation{})))
	}
	if needPredictionStorage {
		bytes += memtable.COWAllocationCharge(uint64(n) * uint64(unsafe.Sizeof(memtable.COWMutation{})))
	}
	// Per-attempt transient allocations remain charged on every write attempt.
	bytes += 2 * memtable.COWAllocationCharge(uint64(n)*uint64(unsafe.Sizeof([]byte{})))
	bytes += memtable.COWAllocationCharge(uint64(n) * uint64(unsafe.Sizeof(valuelog.Record{})))
	bytes += memtable.COWAllocationCharge(uint64(n) * uint64(unsafe.Sizeof(outerLeafRecordGroup{})))
	bytes += memtable.COWAllocationCharge(uint64(n) * uint64(unsafe.Sizeof(page.ValuePtr{})))
	// Fresh COW prepare/finalize/replay method values are created later in
	// canonical dispatch. Admit their function+receiver environments before
	// creating any of them; keep the shared scratch owner until Reset/Close.
	bytes += 3 * memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0))))
	// The unlock environment exists for inline publications too. Pointer-only
	// producer capture has its own admission when placement actually needs it.
	bytes += memtable.COWAllocationCharge(3 * uint64(unsafe.Sizeof(uintptr(0))))
	if err := b.cowAdmitStorage(bytes); err != nil {
		return err
	}
	if needPredictionGroups {
		b.cowState.predictionGroups = make([][]memtable.COWMutation, shards)
	} else {
		if len(b.cowState.predictionGroups) > shards {
			clear(b.cowState.predictionGroups[shards:])
		}
		b.cowState.predictionGroups = b.cowState.predictionGroups[:shards]
		clear(b.cowState.predictionGroups)
	}
	if needPredictionStorage {
		b.cowState.predictionStorage = make([]memtable.COWMutation, n)
	} else {
		if len(b.cowState.predictionStorage) > 0 {
			clear(b.cowState.predictionStorage)
		}
		b.cowState.predictionStorage = b.cowState.predictionStorage[:n]
	}
	if needShardAdds {
		b.shardAdds = make([]int64, shards)
	} else {
		b.shardAdds = b.shardAdds[:shards]
		clear(b.shardAdds)
	}
	if needShardCnts {
		b.shardCnts = make([]int, shards)
	} else {
		b.shardCnts = b.shardCnts[:shards]
		clear(b.shardCnts)
	}
	if needShardIdxs {
		b.shardIdxs = make([]int, n)
	} else {
		b.shardIdxs = b.shardIdxs[:n]
	}
	if needEligibleIdxs {
		b.eligibleIdxs = make([]int, 0, n)
	} else {
		b.eligibleIdxs = b.eligibleIdxs[:0]
	}
	// Keys and fixed shard ownership remain unchanged through prediction,
	// placement and canonical preparation. Only values/pointers/revisions change.
	for i, entry := range b.entries {
		shard := b.db.shardIndex(entry.Key)
		b.shardIdxs[i] = shard
		b.shardCnts[shard]++
	}
	b.cowState.shardAssignmentsReady = true
	return nil
}

func cowValueLogPtrs(n int, observer valuelog.ProducedFrameObserver) []page.ValuePtr {
	if observer != nil {
		return make([]page.ValuePtr, n)
	}
	return getValueLogPtrs(n)
}
func putCOWValueLogPtrs(ptrs []page.ValuePtr, observer valuelog.ProducedFrameObserver) {
	if observer == nil {
		putValueLogPtrs(ptrs)
	}
}

// cowCopyStorage is the existing batch's admitted, chunked immutable arena.
// Read dependencies share its transaction lifetime instead of growing a second
// arena whose discarded backing remains aliased by old read-index keys.
func (b *Batch) cowCopyStorage(n int) ([]byte, error) {
	return b.cowCopyStorageWithDelete(n, n, false)
}

func (b *Batch) cowCopyStorageWithDelete(n, keyLen int, isDelete bool) ([]byte, error) {
	if n < 0 {
		return nil, memtable.ErrCOWCapacity
	}
	if cap(b.copyArena)-len(b.copyArena) < n {
		if cap(b.copyArena) > cap(b.cowState.largestCopyArena) {
			b.cowState.largestCopyArena = b.copyArena[:0]
		}
		capacity := cowNextCopyArenaCap(cap(b.copyArena), n)
		hint := b.cowState.copyArenaHint
		if isDelete {
			hint = cowDeleteArenaCap(max(batchDefaultEntriesCap, b.cowState.lastHint), keyLen, hint)
		}
		useHint := (cap(b.copyArena) == 0 || cap(b.copyArena) < hint) && hint > capacity
		if !(useHint && b.cowTryReserveWithCopyArena(0, hint)) {
			if err := b.cowAdmitStorage(memtable.COWAllocationCharge(uint64(capacity))); err != nil {
				return nil, err
			}
			b.copyArena = make([]byte, 0, capacity)
		}
	}
	start := len(b.copyArena)
	b.copyArena = b.copyArena[:start+n]
	return b.copyArena[start : start+n : start+n], nil
}
