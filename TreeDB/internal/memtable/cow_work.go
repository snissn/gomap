package memtable

import (
	"unsafe"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
)

// COWOrderedMutationWork bounds Store-owned qualified logical PUT/tombstone
// work using canonical degree, allocator and copy/growth envelopes. Raw writes
// in the Store's reserved namespace are unsupported: there is no raw-incarnation
// deletion to charge. A new singleton inserts its primary key but creates no
// numeric tree. An existing key replaces the primary path and either promotes
// its singleton (two height-one numeric inserts) or inserts one numeric version.
// These alternatives are exclusive; adding all three would not model a legal
// logical mutation. The result grants no preparation lease or publication right.
func COWOrderedMutationWork(keyBytes, valueBytes, generationBytes uint64) uint64 {
	const saturated = ^uint64(0)
	historyHeader := cowAllocation(uint64(unsafe.Sizeof(cowOrderedHistory{})))
	descriptor := cowAllocation(uint64(unsafe.Sizeof(cowOrderedValue{})))
	// Each retained primary/history entry owns at least its concrete header or
	// descriptor. This bounds possible cardinality more tightly than one byte per
	// entry without observing a mutable writer basis.
	maximum := generationBytes/min(historyHeader, descriptor) + 1
	if maximum > uint64(int(^uint(0)>>1)) {
		return saturated
	}
	height := cowMaximumHeight(int(maximum))
	full := 2*btreeDefaultDegree - 1
	primaryPair := uint64(unsafe.Sizeof(cowOrderedPairLayout{}))
	historyPair := uint64(unsafe.Sizeof(cowOrderedVersionPairLayout{}))
	fresh := cowOrderedNodeReserve(height, false, false, full, primaryPair)
	replacement := cowOrderedNodeReserve(height, true, false, full, primaryPair)
	promotion := 2 * cowOrderedNodeReserve(1, false, false, 2, historyPair)
	insertion := cowOrderedNodeReserve(height, false, false, full, historyPair)
	total := max(fresh, replacement+max(promotion, insertion))
	add := func(n uint64) bool { return cowPredictionAdd(&total, n) }
	if valueBytes > saturated-cowValueMetadataBytes {
		return saturated
	}
	if !add(cowAllocation(keyBytes)) || !add(cowAllocation(valueBytes+cowValueMetadataBytes)) ||
		!add(historyHeader) || !add(2*descriptor) ||
		!add(cowAllocation(uint64(unsafe.Sizeof(btree.Map[uint64, *cowOrderedValue]{})))) ||
		!add(cowAllocation(uint64(unsafe.Sizeof((*cowOrderedHistory)(nil))))) {
		return saturated
	}
	return total
}

// COWOrderedCommandWork bounds shared root/fork/prepared headers once per
// aggregate, independent of its row count.
func COWOrderedCommandWork() uint64 {
	return 2*cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, cowValue]{}))) +
		cowAllocation(uint64(unsafe.Sizeof(COWRoot{}))) + 2*cowOrderedForkCharge() +
		cowAllocation(uint64(unsafe.Sizeof(COWPrepared{})))
}

// COWOrderedWorkEstimator captures basis-independent canonical path constants.
// It owns no writer, root, budget or lifecycle pointer and is safe after Close.
type COWOrderedWorkEstimator struct{ base uint64 }

func NewCOWOrderedWorkEstimator(generationBytes uint64) COWOrderedWorkEstimator {
	total := COWOrderedMutationWork(0, 0, generationBytes)
	if total == ^uint64(0) {
		return COWOrderedWorkEstimator{base: total}
	}
	return COWOrderedWorkEstimator{base: total - cowAllocation(0) - cowAllocation(cowValueMetadataBytes)}
}

func (e COWOrderedWorkEstimator) Estimate(keyBytes, valueBytes uint64) uint64 {
	total := e.base
	if valueBytes > ^uint64(0)-cowValueMetadataBytes ||
		!cowPredictionAdd(&total, cowAllocation(keyBytes)) ||
		!cowPredictionAdd(&total, cowAllocation(valueBytes+cowValueMetadataBytes)) {
		return ^uint64(0)
	}
	return total
}
