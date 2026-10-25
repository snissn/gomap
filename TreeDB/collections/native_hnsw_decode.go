package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
	"unsafe"

	"github.com/buger/jsonparser"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

// Only a publication conflict is retryable against a fresh view. Capacity,
// malformed codec data, closure and I/O failures retain their original cause.
func nativeHNSWLoadSearchError(err error) error {
	if errors.Is(err, backenddb.ErrConcurrentModification) {
		return fmt.Errorf("%w: native runtime load: %w", ErrVectorIndexSnapshotMismatch, err)
	}
	return fmt.Errorf("%w: native runtime load: %w", ErrVectorIndexSearchUnavailable, err)
}

// These are capacities for the maintained record structs, not another graph
// format or decoder. Raw record copies remain admitted by namedDomain*Admitted.
type nativeHNSWDecodePlan struct {
	vector, quantized, neighbors, distances int
	charge                                  uint64
}

func (p *nativeHNSWDecodePlan) add(n uint64) error {
	if n > ^uint64(0)-p.charge {
		return memtable.ErrCOWCapacity
	}
	p.charge += n
	return nil
}

func (p *nativeHNSWDecodePlan) allocation(count, width uint64) error {
	if count != 0 && width > ^uint64(0)/count {
		return memtable.ErrCOWCapacity
	}
	return p.add(memtable.COWAllocationCharge(count * width))
}

// encoding/json's scanner appends an int for each open container before it
// decodes any selected field. Count opening bytes even inside strings: this is
// an upper bound, including malformed JSON and ignored nested fields. Up to256
// elements Go's slice growth doubles; rounded Q charges dominate those cohorts.
// Above256, nextslicecap grows by at least1.25, and at most2; the geometric sum
// is at most5 final capacities. Q and the final over-allocation each cost at
// most2, giving20*Q(logical bytes), including discarded backing until cleanup.
func nativeHNSWDecodeGrowth(count, width uint64) (uint64, error) {
	if count == 0 {
		return 0, nil
	}
	var p nativeHNSWDecodePlan
	if count > 256 {
		if err := p.allocation(count, width); err != nil || p.charge > ^uint64(0)/20 {
			return 0, memtable.ErrCOWCapacity
		}
		return p.charge * 20, nil
	}
	for capacity := uint64(1); ; capacity *= 2 {
		if err := p.allocation(capacity, width); err != nil {
			return 0, err
		}
		if capacity >= count {
			return p.charge, nil
		}
	}
}

// Preflight uses the existing borrowed JSON walker, then the existing record
// codec still performs json.Unmarshal. Numeric validation prevents malformed
// elements from constructing an unbounded sequence of type/number errors.
// One failed parse's error and source spelling are admitted before this walk.
// Arrays are seeded at their maximum observed length. An empty/null duplicate
// can discard a seed; charge all subsequent growth cohorts as well.
func planNativeHNSWDecode(key, value []byte, admit func(uint64) error) (nativeHNSWDecodePlan, string, error) {
	return planNativeHNSWRecordDecode(key, value, admit, true)
}

func planNativeHNSWRuntimeDecode(key, value []byte, admit func(uint64) error) (nativeHNSWDecodePlan, string, error) {
	return planNativeHNSWRecordDecode(key, value, admit, false)
}

func planNativeHNSWRecordDecode(key, value []byte, admit func(uint64) error, snapshotContainers bool) (nativeHNSWDecodePlan, string, error) {
	var plan nativeHNSWDecodePlan
	var opens uint64
	for _, b := range value {
		if b == '[' || b == '{' {
			opens++
		}
	}
	scanner, err := nativeHNSWDecodeGrowth(opens, uint64(unsafe.Sizeof(int(0))))
	if err != nil {
		return plan, "", err
	}
	// The operation's initial64KiB covers small-map startup and static decoder
	// machinery. Per-entry hash charge uses the same four-slot occupancy/growth
	// policy as nativeHNSWRowCharge, including controls. Append descriptor growth
	// uses eight backing elements per accepted record; nodes also retain the
	// map-to-final-slice copy. The local typed record and decoder context survive
	// alongside these containers until operation cleanup.
	fixed := uint64(256) // typed record, decoder context and field-stack backing
	if snapshotContainers {
		switch {
		case bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixNode)):
			fixed += 4*(uint64(unsafe.Sizeof(int(0)))+uint64(unsafe.Sizeof(vectorIndexPersistNode{}))+16) + uint64(unsafe.Sizeof(vectorIndexPersistNode{})) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(vectorIndexPersistNode{})))
		case bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixEdge)):
			fixed += 8*uint64(unsafe.Sizeof(vectorIndexPersistEdges{})) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(vectorIndexPersistEdges{})))
		case bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixDoc)):
			fixed += 4 * (uint64(unsafe.Sizeof(string(""))) + uint64(unsafe.Sizeof(int(0))) + 16)
		case bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixTomb)):
			fixed += 8 * uint64(unsafe.Sizeof(int(0)))
		}
	}
	// Failed scalar preflight returns immediately: its one number/type error
	// and spelling are bounded independently of successful record containers.
	// One failed preflight immediately ends the operation. Its error fits the
	// original64KiB operation header; successful direct loads retain no snapshot
	// map or descriptor slice. Their final graph is admitted separately first.
	if snapshotContainers {
		fixed = max(fixed, uint64(512))
	}
	if err = plan.add(fixed); err == nil {
		err = plan.add(scanner)
	}
	if err == nil {
		err = plan.allocation(uint64(len(value)), 1) // failed numeric spelling / parser scratch
	}
	// Both walkers may unescape keys. The JSON decoder also substitutes invalid
	// UTF-8 and copies decoded strings. Two three-byte replacement cohorts bound
	// these exceptional spellings before either walker sees them.
	if err == nil && (bytes.IndexByte(value, '\\') >= 0 || !utf8.Valid(value)) {
		err = plan.allocation(uint64(len(value)), 6)
	}
	if err != nil {
		return plan, "", err
	}
	if err = admit(plan.charge); err != nil {
		return plan, "", err
	}
	plan.charge = 0
	isNode := bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixNode))
	isEdge := bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixEdge))
	if !isNode && !isEdge {
		// Doc-map integers and tombstone keys have no decoded numeric arrays.
		// Doc-map key-to-string ownership is independent of its small value.
		if bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixDoc)) {
			err = plan.allocation(uint64(len(key)), 1)
		}
		if err == nil {
			err = admit(plan.charge)
		}
		return plan, "", err
	}
	var reset [4]bool
	number := func(raw []byte, kind jsonparser.ValueType, width int, floating bool) error {
		if kind == jsonparser.Null {
			return nil
		}
		if kind != jsonparser.Number {
			return jsonparser.MalformedValueError
		}
		if floating {
			n, e := jsonparser.ParseFloat(raw)
			if e != nil || math.IsInf(n, 0) {
				return jsonparser.MalformedValueError
			}
			// The maintained float32 decoder decides exact overflow rounding.
			// A large finite float64 may produce its own number/type error there;
			// retain that possible error's structs and spelling cohorts as well.
			if math.Abs(n) > math.MaxFloat32 {
				if e = plan.add(512); e == nil {
					e = plan.allocation(uint64(len(raw)), 4)
				}
				return e
			}
			return nil
		}
		n, e := jsonparser.ParseInt(raw)
		if e != nil || (width == 1 && (n < -128 || n > 127)) || (width == 8 && int64(int(n)) != n) {
			return jsonparser.MalformedValueError
		}
		return nil
	}
	err = jsonparser.ObjectEach(value, func(field, raw []byte, kind jsonparser.ValueType, _ int) error {
		// Unknown/case-folded long keys can allocate fold-name backing in the
		// maintained decoder. Small keys fit its local buffer; charging all keys
		// also covers descriptor rounding without assuming compiler borrowing.
		if e := plan.allocation(uint64(len(field)), 1); e != nil {
			return e
		}
		var slot, width int
		var floating bool
		switch {
		case isNode && bytes.EqualFold(field, []byte("vector")):
			slot, width, floating = 0, 4, true
		case isNode && bytes.EqualFold(field, []byte("quantized")):
			slot, width = 1, 1
		case isEdge && bytes.EqualFold(field, []byte("neighbors")):
			slot, width = 2, 8
		case isEdge && bytes.EqualFold(field, []byte("distances")):
			slot, width, floating = 3, 4, true
		case isNode && bytes.EqualFold(field, []byte("document_id")):
			if kind != jsonparser.String && kind != jsonparser.Null {
				return jsonparser.MalformedValueError
			}
			// Unquote replacement backing plus the final owned string.
			return plan.allocation(uint64(len(raw)), 6)
		case (isNode && bytes.EqualFold(field, []byte("level"))) || (isEdge && (bytes.EqualFold(field, []byte("node_id")) || bytes.EqualFold(field, []byte("layer")))):
			return number(raw, kind, 8, false)
		case isNode && bytes.EqualFold(field, []byte("quant_scale")):
			return number(raw, kind, 4, true)
		case isNode && bytes.EqualFold(field, []byte("deleted")):
			if kind != jsonparser.Boolean && kind != jsonparser.Null {
				return jsonparser.MalformedValueError
			}
			return nil
		default:
			return nil // encoding/json's maintained unknown-field behavior
		}
		if kind == jsonparser.Null {
			reset[slot] = true
			return nil
		}
		if kind != jsonparser.Array {
			return jsonparser.MalformedValueError
		}
		count := 0
		var invalid error
		_, e := jsonparser.ArrayEach(raw, func(v []byte, k jsonparser.ValueType, _ int, parseErr error) {
			if invalid != nil {
				return
			}
			if parseErr != nil {
				invalid = parseErr
				return
			}
			invalid = number(v, k, width, floating)
			count++
		})
		if e != nil {
			return e
		}
		if invalid != nil {
			return invalid
		}
		capacities := [...]*int{&plan.vector, &plan.quantized, &plan.neighbors, &plan.distances}
		*capacities[slot] = max(*capacities[slot], count)
		if reset[slot] {
			n, e := nativeHNSWDecodeGrowth(uint64(count), uint64(width))
			if e != nil {
				return e
			}
			if e = plan.add(n); e != nil {
				return e
			}
		}
		if count == 0 {
			reset[slot] = true
		}
		return nil
	})
	if err != nil {
		if err == memtable.ErrCOWCapacity {
			return plan, "", err
		}
		return plan, vectorIndexFallbackInvalidGraphRootEntry, nil
	}
	for _, array := range [...]struct {
		count int
		width uint64
	}{{plan.vector, 4}, {plan.quantized, 1}, {plan.neighbors, 8}, {plan.distances, 4}} {
		if err = plan.allocation(uint64(array.count), array.width); err != nil {
			return plan, "", err
		}
	}
	err = admit(plan.charge)
	return plan, "", err
}

func (p nativeHNSWDecodePlan) seeds() (vectorIndexPersistNode, vectorIndexPersistEdges) {
	var node vectorIndexPersistNode
	var edge vectorIndexPersistEdges
	if p.vector != 0 {
		node.Vector = make([]float32, 0, p.vector)
	}
	if p.quantized != 0 {
		node.Quantized = make([]int8, 0, p.quantized)
	}
	if p.neighbors != 0 {
		edge.Neighbor = make([]int, 0, p.neighbors)
	}
	if p.distances != 0 {
		edge.Distances = make([]float32, 0, p.distances)
	}
	return node, edge
}

type nativeHNSWRecordCounts struct{ nodes, edges, docs, tombs int }

// The census borrows only keys under this read's lock/cut. It rejects unknown
// families without copying any values, and sizes final node storage before its
// allocation. The one cursor's backing cohorts stay on the operation receipt.
func inspectNativeHNSWRecordKeys(read *NativeJSONRead, name string, admit func(uint64) error) (counts nativeHNSWRecordCounts, reason string, resultErr error) {
	read.mu.Lock()
	defer read.mu.Unlock()
	domain, err := read.borrowedNamedDomain(name)
	if err != nil {
		return counts, "", err
	}
	scratch, err := read.physicalScratch()
	if err != nil {
		return counts, "", err
	}
	var cursor, start []byte
	for {
		entry, found, err := domain.seek(start, nil, scratch, read.readLeaf, false, nil)
		if err != nil || !found {
			return counts, "", err
		}
		if entry.Flags&node.FlagTombstone == 0 {
			var count *int
			switch {
			case bytes.Equal(entry.Key, []byte(vectorIndexNativeKeyMeta)):
			case bytes.HasPrefix(entry.Key, []byte(vectorIndexNativeKeyPrefixNode)):
				if _, ok := parseVectorIndexNativeOrdinal(string(entry.Key[len(vectorIndexNativeKeyPrefixNode):])); !ok {
					return counts, vectorIndexFallbackInvalidGraphRootKey, nil
				}
				count = &counts.nodes
			case bytes.HasPrefix(entry.Key, []byte(vectorIndexNativeKeyPrefixEdge)):
				count = &counts.edges
			case bytes.HasPrefix(entry.Key, []byte(vectorIndexNativeKeyPrefixDoc)):
				count = &counts.docs
			case bytes.HasPrefix(entry.Key, []byte(vectorIndexNativeKeyPrefixTomb)):
				if _, ok := parseVectorIndexNativeOrdinal(string(entry.Key[len(vectorIndexNativeKeyPrefixTomb):])); !ok {
					return counts, vectorIndexFallbackInvalidGraphRootKey, nil
				}
				count = &counts.tombs
			default:
				return counts, vectorIndexFallbackInvalidGraphRootKey, nil
			}
			if count != nil {
				if *count == int(^uint(0)>>1) {
					return counts, "", memtable.ErrCOWCapacity
				}
				(*count)++
			}
		}
		if len(entry.Key) == int(^uint(0)>>1) {
			return counts, "", memtable.ErrCOWCapacity
		}
		n := len(entry.Key) + 1
		if cap(cursor) < n {
			if err = admit(memtable.COWAllocationCharge(uint64(n))); err != nil {
				return counts, "", err
			}
			cursor = make([]byte, n)
		}
		cursor = cursor[:n]
		copy(cursor, entry.Key)
		cursor[n-1] = 0
		start = cursor
	}
}

func walkNativeHNSWFamily(read *NativeJSONRead, name, prefix string, admit func(uint64) error, visit func([]byte, []byte) (string, error)) (disk int64, reason string, resultErr error) {
	start, end := []byte(prefix), []byte(prefix)
	end[len(end)-1]++ // all maintained family prefixes end in '/', below0xff
	for {
		key, value, found, err := read.namedDomainSeekAdmitted(name, start, end, false, admit)
		if err != nil || !found {
			return disk, "", err
		}
		if int64(len(value)) > math.MaxInt64-disk {
			return disk, "", memtable.ErrCOWCapacity
		}
		disk += int64(len(value))
		if reason, err := visit(key, value); err != nil || reason != "" {
			return disk, reason, err
		}
		start = append(key, 0) // namedDomainSeekAdmitted prepaid key's extra byte
	}
}

// Fill the final graph directly, one record family at a time. The original
// operation lease covers raw copies and serial decoder cohorts. Final graph
// storage is admitted on the original runtime receipt before its allocation;
// no whole snapshot or second node/edge/doc-map image is retained.
func loadNativeHNSWRecords(index *VectorIndex, read *NativeJSONRead, def VectorIndexDefinition, meta vectorIndexPersistMeta, counts nativeHNSWRecordCounts, admit func(uint64) error) (disk int64, reason string, resultErr error) {
	name := "vector-index/" + def.Name
	nodes := make([]vectorIndexNode, counts.nodes) // descriptor envelope prepaid by caller
	if err := admit(memtable.COWAllocationCharge(uint64(counts.nodes))); err != nil {
		return 0, "", err
	}
	tombstoned := make([]bool, counts.nodes)
	nextNode := 0
	rowDescriptor := uint64(6) * uint64(unsafe.Sizeof(vectorIndexNode{}))
	n, reason, err := walkNativeHNSWFamily(read, name, vectorIndexNativeKeyPrefixNode, admit, func(key, value []byte) (string, error) {
		ordinal, ok := parseVectorIndexNativeOrdinal(string(key[len(vectorIndexNativeKeyPrefixNode):]))
		if !ok {
			return vectorIndexFallbackInvalidGraphRootKey, nil
		}
		if ordinal != nextNode || nextNode >= len(nodes) {
			return vectorIndexFallbackMissingGraphRootEntry, nil
		}
		plan, reason, err := planNativeHNSWRuntimeDecode(key, value, admit)
		if err != nil || reason != "" {
			return reason, err
		}
		seed, _ := plan.seeds()
		row, err := decodeVectorIndexPersistNode(value, seed)
		if err != nil {
			return vectorIndexFallbackInvalidGraphRootEntry, nil
		}
		if reason := validateVectorIndexPersistNode(row, meta); reason != "" {
			return reason, nil
		}
		probe := VectorIndex{name: def.Name, m: def.M}
		if row.Level != probe.levelForDocumentID([]byte(row.DocumentID)) {
			return vectorIndexFallbackInvalidGraphRootEntry, nil
		}
		charge, err := nativeHNSWRowCharge(def, []byte(row.DocumentID))
		if err != nil || charge < rowDescriptor || charge-rowDescriptor > ^uint64(0)-index.nativeHNSWCharge {
			return "", memtable.ErrCOWCapacity
		}
		charge -= rowDescriptor // all node descriptor cohorts prepaid once
		if err = index.nativeHNSWLease.Resize(index.nativeHNSWCharge + charge); err != nil {
			return "", err
		}
		index.nativeHNSWCharge += charge
		if uint64(len(row.DocumentID)) > ^uint64(0)-index.nativeHNSWIDBytes {
			return "", memtable.ErrCOWCapacity
		}
		index.nativeHNSWIDBytes += uint64(len(row.DocumentID))
		converted, reason := index.convertVectorIndexPersistNode(row, meta)
		if reason != "" {
			return reason, nil
		}
		nodes[nextNode] = converted
		nextNode++
		return "", nil
	})
	if n > math.MaxInt64-disk {
		return disk, "", memtable.ErrCOWCapacity
	}
	disk += n
	if err != nil || reason != "" {
		return disk, reason, err
	}
	if nextNode != counts.nodes {
		return disk, vectorIndexFallbackMissingGraphRootEntry, nil
	}
	n, reason, err = walkNativeHNSWFamily(read, name, vectorIndexNativeKeyPrefixTomb, admit, func(key, value []byte) (string, error) {
		ordinal, ok := parseVectorIndexNativeOrdinal(string(key[len(vectorIndexNativeKeyPrefixTomb):]))
		if !ok {
			return vectorIndexFallbackInvalidGraphRootKey, nil
		}
		if ordinal >= len(nodes) {
			return vectorIndexFallbackInvalidTombstone, nil
		}
		tombstoned[ordinal] = true
		return "", nil
	})
	if n > math.MaxInt64-disk {
		return disk, "", memtable.ErrCOWCapacity
	}
	disk += n
	if err != nil || reason != "" {
		return disk, reason, err
	}
	for ordinal, row := range nodes {
		if row.deleted != tombstoned[ordinal] {
			return disk, "tombstone_mismatch", nil
		}
	}
	n, reason, err = walkNativeHNSWFamily(read, name, vectorIndexNativeKeyPrefixEdge, admit, func(key, value []byte) (string, error) {
		plan, reason, err := planNativeHNSWRuntimeDecode(key, value, admit)
		if err != nil || reason != "" {
			return reason, err
		}
		_, seed := plan.seeds()
		edge, err := decodeVectorIndexPersistEdge(value, seed)
		if err != nil {
			return vectorIndexFallbackInvalidGraphRootEntry, nil
		}
		return index.applyVectorIndexPersistEdge(nodes, edge, meta), nil
	})
	if n > math.MaxInt64-disk {
		return disk, "", memtable.ErrCOWCapacity
	}
	disk += n
	if err != nil || reason != "" {
		return disk, reason, err
	}
	n, reason, err = walkNativeHNSWFamily(read, name, vectorIndexNativeKeyPrefixDoc, admit, func(key, value []byte) (string, error) {
		if _, reason, err := planNativeHNSWRuntimeDecode(key, value, admit); err != nil || reason != "" {
			return reason, err
		}
		var ordinal int
		if err := json.Unmarshal(value, &ordinal); err != nil {
			return vectorIndexFallbackInvalidGraphRootEntry, nil
		}
		id := string(key[len(vectorIndexNativeKeyPrefixDoc):])
		if reason := validateVectorIndexPersistDocMapNode(nodes, id, ordinal); reason != "" {
			return reason, nil
		}
		index.currentNode[id] = ordinal // row receipt includes map/key growth
		return "", nil
	})
	if n > math.MaxInt64-disk {
		return disk, "", memtable.ErrCOWCapacity
	}
	disk += n
	if err != nil || reason != "" {
		return disk, reason, err
	}
	return disk, index.finishVectorIndexPersistLoad(meta, nodes, index.currentNode), nil
}
