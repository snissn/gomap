package caching

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// cowBatchState exists only for explicitly admitted COW batches. Keeping its
// ownership and prediction fields separate preserves ordinary batch allocations.
type cowBatchState struct {
	lease             *memtable.COWExternalLease
	storage           *cowBatchStorage
	err               error
	prepared          *cowBatchPreparation
	producer          *cowFrameCapture
	predictionGroups  [][]memtable.COWMutation
	predictionStorage []memtable.COWMutation
}

// One admitted allocation owns the public batch and its COW-only state. Both
// interior pointers retain this owner; ordinary batches need no sidecar storage.
type cowBatchOwner struct {
	batch Batch
	state cowBatchState
}

// A batch owns every admitted backing, including discarded growth arrays,
// until Reset drops its references. These leases never enter legacy pools.
type cowBatchStorage struct {
	lease *memtable.COWExternalLease
	next  *cowBatchStorage
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

func (db *DB) newCOWBatch(hint int) *Batch {
	lease, err := db.cow.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBatchOwner{}))))
	if err != nil {
		return &cowDeniedBatch
	}
	owner := &cowBatchOwner{batch: Batch{db: db, streamBypassOff: true}, state: cowBatchState{lease: lease}}
	b := &owner.batch
	b.cowState = &owner.state
	b.cowReserve(hint)
	return b
}

func (b *Batch) cowAdmitStorage(bytes uint64) error {
	if b.cowState.err != nil {
		return b.cowState.err
	}
	ownerCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBatchStorage{})))
	if bytes > ^uint64(0)-ownerCharge {
		return memtable.ErrCOWCapacity
	}
	lease, err := b.db.cow.budget.AcquireExternal(bytes + ownerCharge)
	if err != nil {
		return err
	}
	b.cowState.storage = &cowBatchStorage{lease: lease, next: b.cowState.storage}
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
				return err
			}
			b.copyArena = make([]byte, 0, capacity)
		}
		start := len(b.copyArena)
		b.copyArena = append(b.copyArena, e.Key...)
		e.Key = normalizeRawKVPointKey(b.copyArena[start:len(b.copyArena):len(b.copyArena)])
		if e.Type == batch.OpPut {
			start = len(b.copyArena)
			b.copyArena = append(b.copyArena, e.Value...)
			e.Value = normalizeRawKVValue(b.copyArena[start:len(b.copyArena):len(b.copyArena)])
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
	b.entries = nil
	b.copyArena = nil
	b.shardAdds = nil
	b.shardCnts = nil
	b.shardIdxs = nil
	b.eligibleIdxs = nil
	b.shardEntries = nil
	b.shardIdxSets = nil
	b.cowState.predictionGroups = nil
	b.cowState.predictionStorage = nil
	b.firstKey, b.lastKey = nil, nil
	b.stableViewValueLeaseChunks = nil
	b.size, b.maxEntries = 0, 0
	b.hasViewOps, b.hasDeleteRanges = false, false
	b.commandWALAppend = nil
	b.commandWALPreparedRevision, b.commandWALMaterializedRID = false, false
	b.entryRevision = page.LegacyEntryRevision
	b.cowState.err = nil
	for b.cowState.storage != nil {
		owner := b.cowState.storage
		b.cowState.storage = owner.next
		owner.next = nil
		owner.lease.Close()
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
