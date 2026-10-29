package collections

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func columnGraphHNSWRowCharge(def VectorIndexDefinition, id []byte) (uint64, error) {
	if def.M <= 0 || def.Dimensions <= 0 {
		return 0, memtable.ErrCOWCapacity
	}
	probe := columnGraphHNSWBuilder{name: def.Name, m: def.M}
	level := probe.levelForDocumentID(id)
	var total uint64
	add := func(count, size uint64) bool {
		if count != 0 && size > (^uint64(0)-total)/count {
			return false
		}
		total += count * size
		return true
	}
	allocation := func(count, size uint64) bool {
		if count != 0 && size > ^uint64(0)/count {
			return false
		}
		return add(1, memtable.COWAllocationCharge(count*size))
	}
	// Mutable descriptor growth cohorts plus two exact-length view descriptors.
	if !add(6, uint64(unsafe.Sizeof(vectorIndexNode{}))) ||
		!allocation(uint64(def.Dimensions), 4) ||
		!add(6, memtable.COWAllocationCharge(uint64(len(id)))) ||
		!add(3, memtable.COWAllocationCharge(uint64(level+1)*uint64(unsafe.Sizeof([]vectorIndexNeighbor{})))) {
		return 0, memtable.ErrCOWCapacity
	}
	for layer := 0; layer <= level; layer++ {
		limit := uint64(def.M)
		if layer == 0 {
			if limit > (^uint64(0)-1)/2 {
				return 0, memtable.ErrCOWCapacity
			}
			limit *= 2
		}
		if !allocation(limit+1, uint64(unsafe.Sizeof(vectorIndexNeighbor{}))) ||
			!add(2, memtable.COWAllocationCharge(limit*uint64(unsafe.Sizeof(vectorIndexNeighbor{})))) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	// Current ordinal, dirty node/document, unnotified document and dirty-view
	// maps plus two immutable ordinal maps. Four slots per logical entry bound
	// hash-table occupancy/growth, including control bytes and rounded groups.
	mapSlotBytes := uint64(unsafe.Sizeof(string(""))+unsafe.Sizeof(int(0)))*3 +
		uint64(unsafe.Sizeof(int(0)))*2 + uint64(unsafe.Sizeof(string("")))*2 + 7*8
	if !add(4, mapSlotBytes) {
		return 0, memtable.ErrCOWCapacity
	}
	return total, nil
}

func columnGraphHNSWConstructionCharge(def VectorIndexDefinition, nodes uint64) (uint64, error) {
	return columnGraphHNSWConstructionChargeForWorkers(def, nodes, 1)
}

func columnGraphHNSWConstructionChargeForWorkers(def VectorIndexDefinition, nodes, workers uint64) (uint64, error) {
	if def.M <= 0 || def.Dimensions <= 0 || def.EfConstruction <= 0 {
		return 0, memtable.ErrCOWCapacity
	}
	var total uint64
	add := func(count, size uint64) bool {
		if count != 0 && size > (^uint64(0)-total)/count {
			return false
		}
		total += count * size
		return true
	}
	capacity := uint64(64)
	for capacity < nodes {
		if capacity > ^uint64(0)/2 {
			return 0, memtable.ErrCOWCapacity
		}
		capacity *= 2
	}
	degree := uint64(def.M)
	if degree > (^uint64(0)-1)/2 {
		return 0, memtable.ErrCOWCapacity
	}
	degree = degree*2 + 1
	bounded := min(nodes, max(uint64(def.EfConstruction), degree))
	// Queue; visited epochs; best, liveBest, output, scored/rejected/selected
	// candidates; selected neighbor ordinals; quantization/conversion vector.
	if !add(nodes, 8*uint64(unsafe.Sizeof(vectorIndexCandidate{}))) ||
		!add(capacity, 2*uint64(unsafe.Sizeof(uint32(0)))) ||
		!add(bounded, 8*6*uint64(unsafe.Sizeof(vectorIndexCandidate{}))) ||
		!add(degree, 2*uint64(unsafe.Sizeof(int(0)))) || !add(uint64(def.Dimensions), 8) {
		return 0, memtable.ErrCOWCapacity
	}
	// Native float32 rows keep their per-node backing. The maintained int8
	// fold temporarily dequantizes a whole delta before inserting its rows.
	if def.Encoding == VectorIndexEncodingInt8 {
		if nodes != 0 && uint64(def.Dimensions) > ^uint64(0)/nodes {
			return 0, memtable.ErrCOWCapacity
		}
		if !add(nodes*uint64(def.Dimensions), 4) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	// Frozen-prefix planning/reciprocal pruning owns independent worker scratch.
	// The same runtime peak grows at the existing fold boundary before workers
	// start; the serial builder pays for one workspace.
	if degree > ^uint64(0)/degree || !add(degree*degree, 4) {
		return 0, memtable.ErrCOWCapacity
	}
	if workers == 0 || total > ^uint64(0)/workers {
		return 0, memtable.ErrCOWCapacity
	}
	return memtable.COWAllocationCharge(total * workers), nil
}

func admittedLeasePeakClosure(lease *memtable.COWExternalLease) func(uint64) error {
	var peak uint64
	return func(n uint64) error {
		if n > ^uint64(0)/32 {
			return memtable.ErrCOWCapacity
		}
		n = memtable.COWAllocationCharge(n * 32)
		if n <= peak {
			return nil
		}
		if err := lease.Resize(n); err != nil {
			return err
		}
		peak = n
		return nil
	}
}
