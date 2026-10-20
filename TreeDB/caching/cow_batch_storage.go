package caching

import (
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

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
	err                          error
	prepared                     *cowBatchPreparation
	producer                     *cowFrameCapture
	predictionGroups             [][]memtable.COWMutation
	predictionStorage            []memtable.COWMutation
	inPublication, trustedAppend bool
	guardReserved                bool
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

func (db *DB) newCOWBatch(hint int) *Batch {
	guardReserved := db.externalCommandWAL && db.cow.txnOwner != nil
	charge := cowBatchBaseBytes(guardReserved)
	lease, err := db.cow.budget.AcquireExternal(charge)
	if err != nil {
		return &cowDeniedBatch
	}
	owner := &cowBatchOwner{batch: Batch{db: db, streamBypassOff: true}, state: cowBatchState{lease: lease, guardReserved: guardReserved}}
	b := &owner.batch
	b.cowState = &owner.state
	b.cowReserve(hint)
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

func (b *Batch) cowAddEntry(e batch.Entry, owned bool) error {
	if b.cowState.err != nil {
		return b.cowState.err
	}
	if b.closed {
		return ErrBatchClosed
	}
	if len(b.entries) == cap(b.entries) {
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
	if owned {
		n := len(e.Key) + len(e.Value)
		if n < len(e.Key) {
			return memtable.ErrCOWCapacity
		}
		buf, err := b.cowCopyStorage(n)
		if err != nil {
			return err
		}
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
	b.noteEntryAppend()
	b.size += len(e.Key) + len(e.Value)
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

func (b *Batch) cowReset() {
	if b == &cowDeniedBatch {
		return
	}
	b.releaseNativePublication()
	b.entries = nil
	b.copyArena = nil
	b.shardAdds = nil
	b.shardCnts = nil
	b.shardIdxs = nil
	b.eligibleIdxs = nil
	b.cowState.predictionGroups = nil
	b.cowState.predictionStorage = nil
	b.stableViewValueLeaseChunks = nil
	b.size, b.maxEntries = 0, 0
	b.hasViewOps, b.hasDeleteRanges = false, false
	b.commandWALAppend = nil
	b.commandWALPreparedRevision, b.commandWALMaterializedRID = false, false
	b.entryRevision = page.LegacyEntryRevision
	b.cowState.err = nil
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

func (b *Batch) cowClose() error {
	if b == &cowDeniedBatch || b.closed {
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
	bytes := memtable.COWAllocationCharge(uint64(shards)*8) + memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof(int(0))))
	bytes += 2 * memtable.COWAllocationCharge(uint64(n)*uint64(unsafe.Sizeof(int(0))))
	bytes += 2 * memtable.COWAllocationCharge(uint64(n)*uint64(unsafe.Sizeof([]byte{})))
	bytes += memtable.COWAllocationCharge(uint64(n) * uint64(unsafe.Sizeof(valuelog.Record{})))
	bytes += memtable.COWAllocationCharge(uint64(n) * uint64(unsafe.Sizeof(outerLeafRecordGroup{})))
	bytes += memtable.COWAllocationCharge(uint64(n) * uint64(unsafe.Sizeof(page.ValuePtr{})))
	bytes += memtable.COWAllocationCharge(uint64(shards) * uint64(unsafe.Sizeof([]memtable.COWMutation{})))
	bytes += memtable.COWAllocationCharge(uint64(n) * uint64(unsafe.Sizeof(memtable.COWMutation{})))
	// Fresh COW prepare/finalize/replay method values are created later in
	// canonical dispatch. Admit their function+receiver environments before
	// creating any of them; keep the shared scratch owner until Reset/Close.
	bytes += 3 * memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0))))
	if err := b.cowAdmitStorage(bytes); err != nil {
		return err
	}
	b.cowState.predictionGroups = make([][]memtable.COWMutation, shards)
	b.cowState.predictionStorage = make([]memtable.COWMutation, n)
	b.shardAdds = make([]int64, shards)
	b.shardCnts = make([]int, shards)
	b.shardIdxs = make([]int, n)
	b.eligibleIdxs = make([]int, 0, n)
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
	if n < 0 {
		return nil, memtable.ErrCOWCapacity
	}
	if cap(b.copyArena)-len(b.copyArena) < n {
		capacity := cap(b.copyArena) * 2
		if capacity < 256 {
			capacity = 256
		}
		if capacity > 64<<10 {
			capacity = 64 << 10
		}
		if capacity < n {
			capacity = n
		}
		if err := b.cowAdmitStorage(memtable.COWAllocationCharge(uint64(capacity))); err != nil {
			return nil, err
		}
		b.copyArena = make([]byte, 0, capacity)
	}
	start := len(b.copyArena)
	b.copyArena = b.copyArena[:start+n]
	return b.copyArena[start : start+n : start+n], nil
}
