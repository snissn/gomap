package caching

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func (tx *nativeConditionalTxn) NativeConditionalBlockValueAtPointer(ptr page.ValuePtr, scratch []byte, admitOutput func(uint64) error) ([]byte, error) {
	if tx == nil || tx.ConditionalTxn == nil {
		return nil, backenddb.ErrConditionalTxnClosed
	}
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	half := len(scratch) / 2
	if !valuelog.COWSnappySingletonProbeEligible(ptr, half) {
		return tx.NativeValueAtPointer(ptr, admitOutput)
	}
	basis, cache := tx.snap.cowCut.basis, tx.db.cow
	// An idle shared reader keeps its own admitted workspace and policy. Only
	// a busy reader justifies probing the record in the transaction's key page.
	if cache.physicalMu.TryLock() {
		cache.physicalMu.Unlock()
		return tx.NativeValueAtPointer(ptr, admitOutput)
	}
	f, _, err := basis.snapshot.PinnedValueLogFile(ptr.FileID)
	if err != nil {
		return nil, err
	}
	limit := cache.budget.Limits().MaxInFlightBytes
	limits := valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit}
	value, handled, err := valuelog.ReadCOWSnappySingleton(f, ptr, limits, scratch[:half:half], scratch[half:])
	if !handled {
		return tx.NativeValueAtPointer(ptr, admitOutput)
	}
	if err != nil {
		return nil, err
	}
	if admitOutput != nil {
		if err := admitOutput(memtable.COWAllocationCharge(uint64(len(value)))); err != nil {
			return nil, err
		}
	}
	owned := make([]byte, len(value))
	copy(owned, value)
	return owned, nil
}
