package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Read only the opening cut's index state. Private members are reduced once to
// their final declared values; neither typed vectors nor residuals are copied
// into a second row representation. The original stage owns all decoded state.
func (t *NativeJSONTxn) typedOpeningIndexState(g *nativeJSONTxnBinding, id []byte) ([]byte, error) {
	domain, ok := g.probe.collection.domains.Get("index-state")
	if !ok {
		return nil, nil
	}
	read, err := newNativeDomainRead(domain, t.core.Snapshot())
	if err != nil {
		return nil, err
	}
	entry, resolved, err := read.memoryEntry(id)
	if !resolved {
		scratch, e := g.probe.physicalScratch()
		if e != nil {
			return nil, e
		}
		entry, err = read.baseEntry(id, scratch, g.probe.readLeaf)
	}
	if errors.Is(err, tree.ErrKeyNotFound) || err == nil && entry.Flags&node.FlagTombstone != 0 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	admit := func(n uint64) error { return t.admitStage(g, n) }
	if entry.Flags&node.FlagPointer != 0 {
		return g.probe.readValue(entry.ValuePtr, admit)
	}
	if err = admit(memtable.COWAllocationCharge(uint64(len(entry.Value)))); err != nil {
		return nil, err
	}
	return bytes.Clone(entry.Value), nil
}

// Preflight the original decoder's allocations before it reads persisted
// counts. Eligible typed scalar indexes have exactly one value per runtime.
// Validation/normalization still belongs to decodeDocumentIndexState.
func nativeTypedDecodeIndexState(raw []byte, runtimes []indexRuntime, encoder *indexEncodeArena, admit func(uint64) error) (orderedDocumentIndexState, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	invalid := func() (orderedDocumentIndexState, error) { return nil, backenddb.ErrNativeSemanticUnsupported }
	if len(raw) < 3 || raw[0] != documentIndexStateVersion || int(binary.BigEndian.Uint16(raw[1:])) != len(runtimes) {
		return invalid()
	}
	if err := admit(memtable.COWAllocationCharge(256)); err != nil {
		return nil, err
	}
	pos := 3
	for range runtimes {
		if len(raw)-pos < 2 {
			return invalid()
		}
		n := int(binary.BigEndian.Uint16(raw[pos:]))
		pos += 2
		if n == 0 || len(raw)-pos < n+2 {
			return invalid()
		}
		nameBytes := n
		pos += n
		if binary.BigEndian.Uint16(raw[pos:]) != 1 {
			return invalid()
		}
		pos += 2
		if len(raw)-pos < 2 {
			return invalid()
		}
		n = int(binary.BigEndian.Uint16(raw[pos:]))
		pos += 2
		if n == 0 || len(raw)-pos < n {
			return invalid()
		}
		// The maintained decoder owns a name, map bucket/control allowance,
		// original and normalized value refs and both copied scalar images.
		for _, size := range []uint64{uint64(nameBytes), preparedSemanticStreamMapEntryReserveBytes, uint64(unsafe.Sizeof([]byte{})), uint64(unsafe.Sizeof([]byte{})), uint64(n), uint64(n)} {
			if err := admit(memtable.COWAllocationCharge(size)); err != nil {
				return nil, err
			}
		}
		pos += n
	}
	if pos != len(raw) {
		return invalid()
	}
	decoded, err := decodeDocumentIndexState(raw)
	if err != nil {
		return nil, err
	}
	if len(decoded) != len(runtimes) {
		return invalid()
	}
	state := encoder.appendState(len(runtimes))
	for i, runtime := range runtimes {
		values, ok := decoded[runtime.def.name]
		if !ok || len(values) != 1 {
			return invalid()
		}
		state[i] = values
	}
	return state, nil
}

// Validate each canonical member before final-state reduction can erase it.
// This is the SAME typed scalar encoder/state-size validator used by the final
// builder. Reusable operation scratch never publishes or retains another row.
func nativeTypedValidateIndexRows(meta CollectionMeta, rows []columnDeclaredRow, admit func(uint64) error) error {
	if len(meta.Indexes) == 0 || len(rows) == 0 {
		return nil
	}
	header, err := nativeJSONIndexedPlanCharge(meta, nil, nil)
	if err != nil {
		return err
	}
	if err = admit(header); err != nil {
		return err
	}
	planner, err := nativeJSONInsertPlanner(meta)
	if err != nil {
		return err
	}
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		return err
	}
	var capacity uint64
	for _, row := range rows {
		n, err := nativeTypedIndexEncodedCapacity(meta, row)
		if err != nil {
			return err
		}
		capacity = max(capacity, n)
	}
	if capacity > uint64(int(^uint(0)>>1)) {
		return memtable.ErrCOWCapacity
	}
	for _, allocation := range []struct {
		count int
		size  uint64
	}{
		{int(capacity), 1}, {len(runtimes), uint64(unsafe.Sizeof([][]byte{}))}, {len(runtimes), uint64(unsafe.Sizeof([]byte{}))},
	} {
		if err = admitNativeInsertArray(allocation.count, allocation.size, admit); err != nil {
			return err
		}
	}
	encoder := indexEncodeArena{buf: make([]byte, 0, int(capacity)), states: make([][][]byte, 0, len(runtimes)), valueRefs: make([][]byte, 0, len(runtimes))}
	for _, row := range rows {
		state, err := typedIndexState(row.Values, meta.Options.ColumnStore.Columns, runtimes, &encoder)
		if err != nil {
			return err
		}
		if _, _, err = runtimeOrderedDocumentIndexStateStats(state, runtimes); err != nil {
			return err
		}
		clear(encoder.states)
		clear(encoder.valueRefs)
		encoder.buf, encoder.states, encoder.valueRefs = encoder.buf[:0], encoder.states[:0], encoder.valueRefs[:0]
	}
	return nil
}

func nativeTypedIndexEncodedCapacity(meta CollectionMeta, row columnDeclaredRow) (capacity uint64, err error) {
	for _, index := range meta.Indexes {
		i := typedStringColumnIndex(meta.Options.ColumnStore.Columns, index.Field)
		if i < 0 || i >= len(row.Values) {
			return 0, backenddb.ErrNativeSemanticUnsupported
		}
		// The maintained string component doubles embedded NUL and appends a
		// two-byte terminator. This bound precedes its original encoder.
		n, e := nativeTypedCheckedMul(uint64(len(row.Values[i].String)), 2)
		if e != nil || nativeTypedCheckedAdd(&n, 2) != nil || nativeTypedCheckedAdd(&capacity, n) != nil {
			return 0, memtable.ErrCOWCapacity
		}
	}
	return capacity, nil
}

// Foreground and original startup replay share this final typed-index plan.
// Old values come from the exact opening index-state root; new values use the
// maintained typedIndexState encoder and ordinary final-batch uniqueness rule.
func nativeTypedIndexFinalPlan(meta CollectionMeta, mutations []memtable.COWMutation, head *nativeTypedDelta, readOld func([]byte) ([]byte, error), admit func(uint64) error) (*directBufferedInsertPlan, []preparedBatchUpdate, []indexRuntime, error) {
	header, err := nativeJSONIndexedPlanCharge(meta, nil, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = admit(header); err != nil {
		return nil, nil, nil, err
	}
	planner, err := nativeJSONInsertPlanner(meta)
	if err != nil {
		return nil, nil, nil, err
	}
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		return nil, nil, nil, err
	}
	var encoded, states uint64
	for _, mutation := range mutations {
		row, _, _ := head.find(mutation.Key)
		if err = nativeTypedCheckedAdd(&states, 2*uint64(len(runtimes))); err != nil {
			return nil, nil, nil, err
		}
		if row == nil {
			continue
		}
		n, e := nativeTypedIndexEncodedCapacity(meta, *row)
		if e != nil {
			return nil, nil, nil, e
		}
		if err = nativeTypedCheckedAdd(&encoded, n); err != nil {
			return nil, nil, nil, err
		}
	}
	if encoded > uint64(int(^uint(0)>>1)) || states > uint64(int(^uint(0)>>1)) {
		return nil, nil, nil, memtable.ErrCOWCapacity
	}
	for _, allocation := range []struct {
		count int
		size  uint64
	}{
		{len(mutations), uint64(unsafe.Sizeof(preparedBatchUpdate{}))},
		{int(encoded), 1}, {int(states), uint64(unsafe.Sizeof([][]byte{}))}, {int(states), uint64(unsafe.Sizeof([]byte{}))},
	} {
		if err = admitNativeInsertArray(allocation.count, allocation.size, admit); err != nil {
			return nil, nil, nil, err
		}
	}
	encoder := indexEncodeArena{buf: make([]byte, 0, int(encoded)), states: make([][][]byte, 0, int(states)), valueRefs: make([][]byte, 0, int(states))}
	changed := make([]preparedBatchUpdate, 0, len(mutations))
	var stats CollectionUpdateStats
	for i, mutation := range mutations {
		item := preparedBatchUpdate{itemIndex: i, documentID: mutation.Key}
		raw, e := readOld(mutation.Key)
		if e != nil {
			return nil, nil, nil, e
		}
		item.oldState, err = nativeTypedDecodeIndexState(raw, runtimes, &encoder, admit)
		if err != nil {
			return nil, nil, nil, err
		}
		row, residual, _ := head.find(mutation.Key)
		if row != nil {
			item.document = residual // Borrowed only; physical typed primary remains authoritative.
			item.newState, err = typedIndexState(row.Values, meta.Options.ColumnStore.Columns, runtimes, &encoder)
			if err != nil {
				return nil, nil, nil, err
			}
		}
		markBatchUpdateIndexEffects(&item, runtimes, &stats)
		changed = append(changed, item)
	}
	charge, stateEntries, roots, err := nativeJSONFinalPlanCharge(meta.Name, planner.options, runtimes, changed)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = admit(charge); err != nil {
		return nil, nil, nil, err
	}
	if err = rejectBatchUniqueConflicts(runtimes, changed); err != nil {
		return nil, nil, nil, err
	}
	plan, err := buildNativeCRUDFinalPlan(meta.Name, planner.options, runtimes, changed, stateEntries, roots)
	return plan, changed, runtimes, err
}

func (t *NativeJSONTxn) finalizeNativeTypedIndexes(g *nativeJSONTxnBinding, mutations []memtable.COWMutation) ([]nativeJSONDomainChange, []txnhistory.Effect, error) {
	admit := func(n uint64) error { return t.admitStage(g, n) }
	plan, changed, runtimes, err := nativeTypedIndexFinalPlan(g.probe.meta, mutations, g.typed, func(id []byte) ([]byte, error) { return t.typedOpeningIndexState(g, id) }, admit)
	if err != nil {
		return nil, nil, err
	}
	// Same conservative replacement-owner map backing as ordinary native CRUD.
	charge := memtable.COWAllocationCharge(256)
	for runtimeIdx, runtime := range runtimes {
		if !runtime.def.unique {
			continue
		}
		for _, item := range changed {
			if !preparedBatchUpdateIndexChanged(item, runtimeIdx) {
				continue
			}
			for _, value := range item.oldState.valuesAt(runtimeIdx) {
				if documentIndexStateContainsValue(item.newState.valuesAt(runtimeIdx), value) {
					continue
				}
				for _, n := range []uint64{uint64(len(value)), uint64(len(item.documentID)), uint64(len(runtime.def.name)), 768} {
					if err = nativeTypedCheckedAdd(&charge, memtable.COWAllocationCharge(n)); err != nil {
						return nil, nil, err
					}
				}
			}
		}
	}
	if err = admit(charge); err != nil {
		return nil, nil, err
	}
	if err = t.checkNativeUpdateUnique(g, runtimes, changed, batchUniqueReplacementOwners(runtimes, changed)); err != nil {
		return nil, nil, err
	}
	additional, effects, err := extractNativeInsertEffects(g.name, g.incarnation, plan, mutations, admit, g.probe.meta)
	// extract adapts primary keys, but never install residual JSON as a typed
	// primary value. The existing immutable typed delta owns those values.
	for i := range mutations {
		mutations[i].Value = nil
	}
	return additional, effects, err
}
