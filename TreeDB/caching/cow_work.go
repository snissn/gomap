package caching

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// MVCCWriteWorkEstimator is an immutable composable shaping bound, not admission authority.
// It includes caller/codec arena growth, native entry/scratch/placement backing,
// canonical node copies. Shared command controls are reserved in the work limit.
// Unknown resource identities and already retained history remain canonically admitted.
type MVCCWriteWorkEstimator struct {
	canonical memtable.COWOrderedWorkEstimator
	scalar    uint64
	supported bool
}

// NewMVCCWriteWorkEstimator captures immutable cost constants only.
func (db *DB) NewMVCCWriteWorkEstimator() MVCCWriteWorkEstimator {
	if db == nil || db.cow == nil || !db.cow.ordered {
		return MVCCWriteWorkEstimator{}
	}
	scalar := uint64(unsafe.Sizeof(batch.Entry{})) + uint64(unsafe.Sizeof(memtable.COWMutation{})) +
		2*uint64(unsafe.Sizeof(int(0))) + 2*uint64(unsafe.Sizeof([]byte{})) +
		uint64(unsafe.Sizeof(valuelog.Record{})) + uint64(unsafe.Sizeof(outerLeafRecordGroup{})) + uint64(unsafe.Sizeof(page.ValuePtr{}))
	return MVCCWriteWorkEstimator{
		canonical: memtable.NewCOWOrderedWorkEstimator(db.cow.budget.Limits().MaxGenerationBytes),
		scalar:    4 * memtable.COWAllocationCharge(scalar), supported: true,
	}
}

func (db *DB) EstimateMVCCWriteWork(keyBytes, valueBytes int) uint64 {
	return db.NewMVCCWriteWorkEstimator().Estimate(keyBytes, valueBytes)
}

func (e MVCCWriteWorkEstimator) Estimate(keyBytes, valueBytes int) uint64 {
	const saturated = ^uint64(0)
	if !e.supported || keyBytes < 0 || valueBytes < 0 {
		return saturated
	}
	key, value := uint64(keyBytes), uint64(valueBytes)
	// Worst-case escaped MVCC key: every input byte can escape, plus the nine-byte namespace, two-byte terminator
	// and eight-byte timestamp. The MVCC record adds its one-byte discriminator.
	if key > (saturated-19)/2 || value == saturated {
		return saturated
	}
	key, value = 2*key+19, value+1
	add := func(total *uint64, n uint64) bool {
		if n > saturated-*total {
			return false
		}
		*total += n
		return true
	}
	total := e.canonical.Estimate(key, value)
	if total == saturated {
		return saturated
	}
	// Doubling arenas retain old capacities; include producer canonical bytes,
	// command encoding, consumer ownership and one staged successor. Per-row
	// allocator rounding is more conservative than shared contiguous arrays.
	for range 8 {
		if !add(&total, memtable.COWAllocationCharge(key)) || !add(&total, memtable.COWAllocationCharge(value)) {
			return saturated
		}
	}
	// Entry and mutation backing can grow before a submission fixes its size.
	if !add(&total, e.scalar) {
		return saturated
	}
	return total
}

// MVCCWriteWorkLimit partitions the configured producer allowance between two
// simultaneously owned aggregate records. Maintenance keeps its existing half;
// persistent transaction-journal controls are deducted before this division.
// No runtime budget counters are rewritten or converted into a reservation.
func (db *DB) MVCCWriteWorkLimit() uint64 {
	if db == nil || db.cow == nil || !db.cow.ordered {
		return 0
	}
	c := db.cow
	limits := c.budget.Limits()
	available := min(limits.MaxInFlightBytes, limits.MaxTotalBytes, limits.MaxRetiredBytes)
	maintenance := c.maintenance.Limits().MaxInFlightBytes
	if maintenance >= available {
		return 1
	}
	available -= maintenance
	if c.txnOwner != nil {
		journal := txnJournalControlCharge(c.txnJournalCapacity())
		if journal >= available {
			return 1
		}
		available -= journal
	}
	// Whole-command controls are reserved twice, rather than charged for every
	// row. The original helper charges include fixed shard/frozen vectors;
	// single-row variable tails here only make this reserve more conservative.
	fixed := cowBatchBaseBytes(db.externalCommandWAL && c.txnOwner != nil) +
		cowPreparedCutCharge(len(db.mutableShards), limits.MaxSources, true) +
		cowPreparationScratchCharge(len(db.mutableShards), 1, 1, false) + cowFrameCaptureCharge(1)
	// Root/fork headers use the same canonical per-command charge helper below.
	fixed += uint64(len(db.mutableShards)) * memtable.COWOrderedCommandWork()
	if fixed > available/2 {
		return 1
	}
	return max(uint64(1), available/2-fixed)
}
