package collections

import (
	"bytes"
	"encoding/json"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

// The complete output image is retained through publication. Only one encoder
// buffer and one edge conversion workspace exist; no full graph snapshot or
// temporary table is constructed. Their construction phase ends when encoding
// returns; only the owned rows survive. The caller owns the mutation/use gate.
type nativeHNSWImagePlan struct {
	charge, control, maxRecord uint64
	entries, neighbors         int
}

func (idx *VectorIndex) nativeHNSWImagePlan() (nativeHNSWImagePlan, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var plan nativeHNSWImagePlan
	maximum := ^uint64(0)
	var charge uint64 = 64 << 10
	add := func(n uint64) bool {
		if n > maximum-charge {
			return false
		}
		charge += n
		return true
	}
	// The fixed control envelope survives encoding: publication/reprepare,
	// effects/dependencies, custody plumbing and the command intent are still
	// constructed afterward. Their variable buffers are charged separately:
	// domain name, catalog dependency key, canonical 10-byte-header payload,
	// and the returned qualified root name. The intent borrows that payload.
	collectionBytes, indexBytes := uint64(len(idx.collection.name)), uint64(len(idx.name))
	if collectionBytes > maximum-indexBytes || collectionBytes+indexBytes > maximum-14 {
		return plan, memtable.ErrCOWCapacity
	}
	for _, n := range [...]uint64{13 + indexBytes, collectionBytes, 10 + collectionBytes + indexBytes, 14 + collectionBytes + indexBytes} {
		if !add(memtable.COWAllocationCharge(n)) {
			return plan, memtable.ErrCOWCapacity
		}
	}
	plan.control = charge
	product := func(a, b uint64) (uint64, bool) {
		if a != 0 && b > maximum/a {
			return 0, false
		}
		return a * b, true
	}
	record := func(key, encoded, payload uint64) bool {
		if plan.entries == int(^uint(0)>>1) {
			return false
		}
		plan.entries++
		plan.maxRecord = max(plan.maxRecord, encoded)
		// The returned encoded record, key plus formatting intermediates, and
		// interface/record construction cohorts remain charged until cleanup.
		keyCharge := memtable.COWAllocationCharge(key)
		return keyCharge <= maximum/3 && payload <= maximum-32 &&
			add(memtable.COWAllocationCharge(encoded)) &&
			add(3*keyCharge) && add(memtable.COWAllocationCharge(payload+32))
	}
	// All fixed metadata fields and numeric spellings fit within4096 bytes;
	// each byte of a declaration string can require six JSON escape bytes.
	metaStrings := uint64(len(idx.name)) + uint64(len(idx.field))
	if metaStrings > (maximum-4096)/6 || !record(4, 4096+6*metaStrings, uint64(unsafe.Sizeof(vectorIndexPersistMeta{}))) {
		return plan, memtable.ErrCOWCapacity
	}
	if idx.partitionLive != nil || idx.partitionPreparation != nil { // Typed/partition declarations have a separate persisted owner.
		return plan, backenddb.ErrNativeSemanticUnsupported
	}
	plane := func(p *VectorIndex) bool {
		for _, n := range p.nodes {
			vector, ok := product(uint64(len(n.vector)), 17)
			if !ok {
				return false
			}
			quantized, ok := product(uint64(len(n.quantized)), 5)
			if !ok || uint64(len(n.documentID)) > (maximum-256)/6 {
				return false
			}
			encoded := 256 + 6*uint64(len(n.documentID))
			if vector > maximum-encoded || quantized > maximum-encoded-vector {
				return false
			}
			encoded += vector + quantized
			if !record(25, encoded, uint64(unsafe.Sizeof(vectorIndexPersistNode{}))) || !add(memtable.COWAllocationCharge(uint64(len(n.documentID)))) {
				return false
			}
			for layer, neighbors := range n.neighbors {
				// Folding may add reciprocal edges. Bound the final degree, not
				// only the current base/delta edge lengths before the fold.
				degree := maxInt(len(neighbors), idx.maxNeighborsForLayer(layer))
				plan.neighbors = maxInt(plan.neighbors, degree)
				if uint64(degree) > (maximum-128)/28 || !record(49, 128+28*uint64(degree), uint64(unsafe.Sizeof(vectorIndexPersistEdges{}))) {
					return false
				}
			}
			// A fold can tombstone an older base node with the same ID.
			if !record(25, 21, uint64(unsafe.Sizeof(int(0)))) {
				return false
			}
		}
		for id := range p.currentNode {
			if uint64(len(id)) > maximum-4 || !record(4+uint64(len(id)), 21, uint64(unsafe.Sizeof(int(0)))) {
				return false
			}
		}
		return true
	}
	if !plane(idx) {
		return plan, memtable.ErrCOWCapacity
	}
	if idx.liveDelta != nil {
		idx.liveDelta.mu.RLock()
		ok := plane(idx.liveDelta)
		idx.liveDelta.mu.RUnlock()
		if !ok {
			return plan, memtable.ErrCOWCapacity
		}
	}
	// bytes.Buffer grows geometrically. Four rounded twice-largest-record
	// cohorts cover the original private buffer and all obsolete backings.
	// json.Encoder uses the same defaults/record structs as json.Marshal and
	// supplies the maintained trailing newline without per-record append growth.
	if plan.maxRecord > maximum/2 || memtable.COWAllocationCharge(plan.maxRecord*2) > maximum/4 || !add(4*memtable.COWAllocationCharge(plan.maxRecord*2)) {
		return plan, memtable.ErrCOWCapacity
	}
	rows, ok := product(uint64(plan.entries), uint64(unsafe.Sizeof(memtable.COWMutation{})))
	if !ok || !add(memtable.COWAllocationCharge(rows)) {
		return plan, memtable.ErrCOWCapacity
	}
	ints, ok := product(uint64(plan.neighbors), uint64(unsafe.Sizeof(int(0))))
	if !ok || !add(memtable.COWAllocationCharge(ints)) {
		return plan, memtable.ErrCOWCapacity
	}
	floats, ok := product(uint64(plan.neighbors), 4)
	if !ok || !add(memtable.COWAllocationCharge(floats)) {
		return plan, memtable.ErrCOWCapacity
	}
	plan.charge = charge
	return plan, nil
}

func (idx *VectorIndex) encodeNativeHNSWImage(version uint64, plan nativeHNSWImagePlan) ([]memtable.COWMutation, int64, uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	rows := make([]memtable.COWMutation, 0, plan.entries)
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	var disk int64
	add := func(key []byte, payload any) error {
		if len(rows) == cap(rows) {
			return memtable.ErrCOWCapacity
		}
		buffer.Reset()
		if err := encoder.Encode(payload); err != nil {
			return err
		}
		if uint64(buffer.Len()) > plan.maxRecord {
			return memtable.ErrCOWCapacity
		}
		data := bytes.Clone(buffer.Bytes())
		rows = append(rows, memtable.COWMutation{Key: key, Value: data, Flags: node.FlagInline})
		disk += int64(len(data))
		return nil
	}
	meta := idx.persistMetaLocked()
	meta.NativeLogicalVersion = version
	if err := add([]byte(vectorIndexNativeKeyMeta), meta); err != nil {
		return nil, 0, 0, err
	}
	edge := vectorIndexPersistEdges{Neighbor: make([]int, 0, plan.neighbors), Distances: make([]float32, 0, plan.neighbors)}
	for id := range idx.nodes {
		if err := idx.emitNativeNodeLocked(id, &edge, add); err != nil {
			return nil, 0, 0, err
		}
	}
	for id, ordinal := range idx.currentNode {
		if err := add(vectorIndexNativeDocKey(id), ordinal); err != nil {
			return nil, 0, 0, err
		}
	}
	return rows, disk, idx.mutationSeq, nil
}

// nativeHNSWImageRetainedCharge measures the original output allocations, not
// their logical lengths. Keys and cloned values have no encoder/workspace
// aliases, and the row array retains its complete original capacity.
func nativeHNSWImageRetainedCharge(rows []memtable.COWMutation) (uint64, error) {
	maximum := ^uint64(0)
	header := uint64(unsafe.Sizeof(memtable.COWMutation{}))
	if uint64(cap(rows)) > maximum/header {
		return 0, memtable.ErrCOWCapacity
	}
	charge := memtable.COWAllocationCharge(uint64(cap(rows)) * header)
	if charge == maximum {
		return 0, memtable.ErrCOWCapacity
	}
	for _, row := range rows {
		for _, capacity := range []int{cap(row.Key), cap(row.Value)} {
			n := memtable.COWAllocationCharge(uint64(capacity))
			if n > maximum-charge {
				return 0, memtable.ErrCOWCapacity
			}
			charge += n
		}
	}
	return charge, nil
}
