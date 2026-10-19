package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// A row owns only its first captured document and latest staged document. Nil
// denotes absence. This same finite reduction is used by foreground and replay;
// it owns no engine/root, publication authority or callback.
type nativeJSONRow struct{ id, opening, current []byte }
type nativeJSONRows struct{ rows []nativeJSONRow }

func (p *nativeJSONRows) find(id []byte) *nativeJSONRow {
	i := sort.Search(len(p.rows), func(i int) bool { return bytes.Compare(p.rows[i].id, id) >= 0 })
	if i < len(p.rows) && bytes.Equal(p.rows[i].id, id) {
		return &p.rows[i]
	}
	return nil
}
func (p *nativeJSONRows) touch(id []byte, read func([]byte) ([]byte, error), admit func(uint64) error) (*nativeJSONRow, error) {
	if row := p.find(id); row != nil {
		return row, nil
	}
	value, err := read(id)
	if errors.Is(err, tree.ErrKeyNotFound) {
		value = nil
	} else if err != nil {
		return nil, err
	}
	if err = admit(memtable.COWAllocationCharge(uint64(len(id))) + memtable.COWAllocationCharge(uint64(len(value)))); err != nil {
		return nil, err
	}
	if len(p.rows) == cap(p.rows) {
		capacity := 2 * cap(p.rows)
		if capacity == 0 {
			capacity = 1
		}
		if capacity < 0 || uint64(capacity) > ^uint64(0)/uint64(unsafe.Sizeof(nativeJSONRow{})) {
			return nil, memtable.ErrCOWCapacity
		}
		if err = admit(memtable.COWAllocationCharge(uint64(capacity) * uint64(unsafe.Sizeof(nativeJSONRow{})))); err != nil {
			return nil, err
		}
		next := make([]nativeJSONRow, len(p.rows), capacity)
		copy(next, p.rows)
		p.rows = next
	}
	i := sort.Search(len(p.rows), func(i int) bool { return bytes.Compare(p.rows[i].id, id) >= 0 })
	p.rows = append(p.rows, nativeJSONRow{})
	copy(p.rows[i+1:], p.rows[i:])
	owned := bytes.Clone(value)
	p.rows[i] = nativeJSONRow{id: bytes.Clone(id), opening: owned, current: owned}
	return &p.rows[i], nil
}

// nativeJSONFinalRows is the maintained pure old/new-effect boundary. It is
// independent of command kind: inserts have no old state, deletes no new state.
func nativeJSONFinalRows(name string, meta CollectionMeta, rows *nativeJSONRows, admit func(uint64) error) (*directBufferedInsertPlan, []preparedBatchUpdate, []indexRuntime, error) {
	// Runtime descriptors are allocated before per-row parsing; admit them first.
	headerCharge, err := nativeJSONIndexedPlanCharge(meta, nil, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = admit(headerCharge); err != nil {
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
	if err = admit(memtable.COWAllocationCharge(uint64(len(rows.rows)) * uint64(unsafe.Sizeof(preparedBatchUpdate{})))); err != nil {
		return nil, nil, nil, err
	}
	changed := make([]preparedBatchUpdate, 0, len(rows.rows))
	stats := CollectionUpdateStats{}
	for i, row := range rows.rows {
		for _, doc := range [][]byte{row.opening, row.current} {
			n, err := nativeJSONIndexedPlanCharge(meta, [][]byte{row.id}, [][]byte{doc})
			if err != nil {
				return nil, nil, nil, err
			}
			if err = admit(n); err != nil {
				return nil, nil, nil, err
			}
		}
		item := preparedBatchUpdate{itemIndex: i, documentID: row.id, document: row.current}
		if row.opening != nil {
			item.oldState, err = orderedIndexStateForDocumentWithArena(row.opening, runtimes, planner.options, nil)
			if err != nil {
				return nil, nil, nil, err
			}
		}
		if row.current == nil {
			markBatchUpdateIndexEffects(&item, runtimes, &stats)
		} else if err = prepareBatchUpdateIndexEffects(&item, runtimes, planner.options, nil, &stats, false); err != nil {
			return nil, nil, nil, err
		}
		changed = append(changed, item)
	}
	if err = rejectBatchUniqueConflicts(runtimes, changed); err != nil {
		return nil, nil, nil, err
	}
	plan, err := buildNativeCRUDFinalPlan(name, planner.options, runtimes, changed)
	return plan, changed, runtimes, err
}

func (t *NativeJSONTxn) admitStage(g *nativeJSONTxnBinding, n uint64) error {
	index := 0
	for &t.bindings[index] != g {
		index++
	}
	if n > ^uint64(0)-g.stageCharge {
		return memtable.ErrCOWCapacity
	}
	next := g.stageCharge + n
	if t.stageLeases[index] == nil {
		lease, err := t.core.NativeAllocator().AdmitNativeBytes(next)
		if err != nil {
			return err
		}
		t.stageLeases[index] = lease
	} else if err := t.stageLeases[index].Resize(next); err != nil {
		return err
	}
	g.stageCharge = next
	return nil
}
func (t *NativeJSONTxn) admitRowDocument(g *nativeJSONTxnBinding, id, doc []byte) error {
	basic, err := nativeJSONInsertCharge(g.name, [][]byte{id}, [][]byte{doc})
	if err != nil {
		return err
	}
	indexed, err := nativeJSONIndexedPlanCharge(g.probe.meta, [][]byte{id}, [][]byte{doc})
	if err != nil || indexed > ^uint64(0)-basic {
		return memtable.ErrCOWCapacity
	}
	extra := memtable.COWAllocationCharge(uint64(len(doc)))
	if extra > ^uint64(0)-basic-indexed {
		return memtable.ErrCOWCapacity
	}
	return t.admitStage(g, basic+indexed+extra)
}

// Admit aggregate staging headers before the maintained planner/callback adapter.
// Old grown backings coexist until the original transaction receipt terminates.
func (t *NativeJSONTxn) admitRowControls(g *nativeJSONTxnBinding, count int) error {
	perItem := uint64(unsafe.Sizeof(insertBatchItem{})) + uint64(unsafe.Sizeof(int(0))) + uint64(unsafe.Sizeof(preparedBatchUpdate{})) + uint64(unsafe.Sizeof(updateBatchItem{})) + uint64(unsafe.Sizeof(UpdateBatchResult{})) + uint64(unsafe.Sizeof(commitlog.CollectionDocument{})) + 8*uint64(unsafe.Sizeof([]byte{})) + 256
	if uint64(count) > ^uint64(0)/perItem {
		return memtable.ErrCOWCapacity
	}
	return t.admitStage(g, memtable.COWAllocationCharge(uint64(count)*perItem))
}
func (t *NativeJSONTxn) touchNativeRow(g *nativeJSONTxnBinding, id []byte) (*nativeJSONRow, error) {
	if len(id) == 0 {
		return nil, errors.New("collections: document id cannot be empty")
	}
	if err := t.trackRead(g, id); err != nil {
		return nil, err
	}
	admit := func(n uint64) error { return t.admitStage(g, n) }
	// The transaction mutex excludes every use of this private probe. Borrow the
	// exact captured domain/scratch until touch copies it; public Get's output is
	// caller-owned and must not become an unadmitted internal temporary.
	read := func(id []byte) ([]byte, error) {
		// Create publishes catalog identity before any primary domain exists.
		// Match NativeJSONRead.Get's captured-empty semantics while retaining
		// the original point dependency registered above.
		if g.probe.domain.domain == nil {
			return nil, tree.ErrKeyNotFound
		}
		entry, resolved, err := g.probe.domain.memoryEntry(id)
		if !resolved {
			scratch, scratchErr := g.probe.physicalScratch()
			if scratchErr != nil {
				return nil, scratchErr
			}
			entry, err = g.probe.domain.baseEntry(id, scratch, g.probe.readLeaf)
		}
		if err != nil {
			return nil, err
		}
		if entry.Flags&node.FlagTombstone != 0 {
			return nil, tree.ErrKeyNotFound
		}
		if entry.Flags&node.FlagPointer != 0 {
			return g.probe.readValue(entry.ValuePtr, admit)
		}
		return entry.Value, nil
	}
	return g.rows.touch(id, read, admit)
}
func (t *NativeJSONTxn) appendNativeMember(g *nativeJSONTxnBinding, kind commitlog.CommandKind, format commitlog.PayloadFormat, payload []byte) error {
	limit := commitlog.MaxCompositeMutationMembersV1
	if t.rawStaged {
		limit--
	}
	if len(t.members) >= limit {
		return backenddb.ErrNativeSemanticUnsupported
	}
	if len(t.members) == cap(t.members) {
		capacity := cap(t.members) * 2
		if capacity == 0 {
			capacity = 1
		}
		if capacity > limit {
			capacity = limit
		}
		n := memtable.COWAllocationCharge(uint64(capacity) * uint64(unsafe.Sizeof(commitlog.CompositeMutationMember{})))
		if n > ^uint64(0)-t.charge {
			return memtable.ErrCOWCapacity
		}
		if err := t.lease.Resize(t.charge + n); err != nil {
			return err
		}
		t.charge += n
		next := make([]commitlog.CompositeMutationMember, len(t.members), capacity)
		copy(next, t.members)
		t.members = next
	}
	t.members = append(t.members, commitlog.CompositeMutationMember{Kind: kind, PayloadFormat: format, Incarnation: g.incarnation, Payload: payload})
	return nil
}
func (t *NativeJSONTxn) stageNativeInsert(g *nativeJSONTxnBinding, ids, documents [][]byte) error {
	if len(ids) == 0 || len(ids) != len(documents) {
		return backenddb.ErrNativeSemanticUnsupported
	}
	if err := t.admitRowControls(g, len(ids)); err != nil {
		return err
	}
	for i := range ids {
		if err := t.admitRowDocument(g, ids[i], documents[i]); err != nil {
			return err
		}
	}
	planner, err := nativeJSONInsertPlanner(g.probe.meta)
	if err != nil {
		return err
	}
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		return err
	}
	// Reuse maintained normalization, scalar validation and ID rules here. Full
	// primary/secondary effects and uniqueness are computed once from opening
	// to final rows at Commit; a transient unique value may disappear later.
	ownedIDs, err := cloneBatchDocumentIDs(ids)
	if err != nil {
		return err
	}
	normalized, _, _, _, err := prepareInsertDocuments(documents, planner.options)
	if err != nil {
		return err
	}
	items := make([]insertBatchItem, len(ids))
	for i := range ids {
		items[i] = insertBatchItem{id: ownedIDs[i], document: normalized[i]}
	}
	order := sortedItemOrderByKey(items, func(item *insertBatchItem) []byte { return item.id })
	if err = rejectDuplicateDocumentIDs(items, order); err != nil {
		return err
	}
	docs := make([]commitlog.CollectionDocument, len(items))
	for i := range items {
		item := items[orderedItemIndex(order, i)]
		if item.document == nil {
			return backenddb.ErrNativeSemanticUnsupported
		}
		if _, err := orderedIndexStateForDocumentWithArena(item.document, runtimes, planner.options, nil); err != nil {
			return err
		}
		row, err := t.touchNativeRow(g, item.id)
		if err != nil {
			return err
		}
		if row.current != nil {
			return ErrDocumentExists
		}
		row.current = bytes.Clone(item.document)
		docs[i] = commitlog.CollectionDocument{ID: row.id, Document: row.current}
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload(g.name, docs)
	if err != nil {
		return err
	}
	return t.appendNativeMember(g, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)
}

// UpdateBatch/DeleteBatch share the same staged row source as Insert/Get. Only
// immutable final documents or IDs enter canonical members, never callbacks.
func (t *NativeJSONTxn) UpdateBatch(items []UpdateBatchItem) ([]UpdateBatchResult, error) {
	return t.UpdateBatchInto("", items)
}
func (t *NativeJSONTxn) UpdateBatchInto(name string, items []UpdateBatchItem) (result []UpdateBatchResult, resultErr error) {
	if t == nil {
		return nil, backenddb.ErrClosed
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.core == nil {
		return nil, backenddb.ErrClosed
	}
	if t.failure != nil {
		return nil, t.failure
	}
	defer func() {
		if resultErr != nil {
			t.failure = resultErr
		}
	}()
	g := t.binding(name)
	if g == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if len(items) == 0 {
		return nil, nil
	}
	if err := t.admitRowControls(g, len(items)); err != nil {
		return nil, err
	}
	for _, item := range items {
		if err := t.admitRowDocument(g, item.DocumentID, nil); err != nil {
			return nil, err
		}
	}
	owned, err := prepareUpdateBatchItems(items)
	if err != nil {
		return nil, err
	}
	result = make([]UpdateBatchResult, len(items))
	docs := make([]commitlog.CollectionDocument, 0, len(items))
	planner, err := nativeJSONInsertPlanner(g.probe.meta)
	if err != nil {
		return nil, err
	}
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		return nil, err
	}
	for i, item := range owned {
		row, err := t.touchNativeRow(g, item.DocumentID)
		if err != nil {
			return nil, err
		}
		if row.current == nil {
			continue
		}
		result[i].Matched = true
		// A callback may mutate its input. It never receives the retained overlay.
		if err = t.admitRowDocument(g, row.id, row.current); err != nil {
			return nil, err
		}
		document, modified, err := callCollectionUpdateCallback(item.Update, bytes.Clone(row.current))
		if err != nil {
			return nil, updateBatchItemError(i, err)
		}
		if !modified {
			continue
		}
		if len(document) == 0 {
			return nil, updateBatchItemError(i, errors.New("changed replacement document cannot be empty"))
		}
		if err = t.admitRowDocument(g, row.id, document); err != nil {
			return nil, err
		}
		normalized, _, _, _, err := prepareInsertDocuments([][]byte{bytes.Clone(document)}, planner.options)
		if err != nil {
			return nil, updateBatchItemError(i, err)
		}
		if _, err := orderedIndexStateForDocumentWithArena(normalized[0], runtimes, planner.options, nil); err != nil {
			return nil, updateBatchItemError(i, err)
		}
		row.current = normalized[0]
		result[i].Modified = true
		docs = append(docs, commitlog.CollectionDocument{ID: row.id, Document: row.current})
	}
	if len(docs) == 0 {
		return result, nil
	}
	payload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload(g.name, docs)
	if err != nil {
		return nil, err
	}
	return result, t.appendNativeMember(g, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionUpdateBatchByIDV1, payload)
}
func (t *NativeJSONTxn) DeleteBatch(ids [][]byte) (int, error) { return t.DeleteBatchFrom("", ids) }
func (t *NativeJSONTxn) DeleteBatchFrom(name string, ids [][]byte) (count int, resultErr error) {
	if t == nil {
		return 0, backenddb.ErrClosed
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.core == nil {
		return 0, backenddb.ErrClosed
	}
	if t.failure != nil {
		return 0, t.failure
	}
	defer func() {
		if resultErr != nil {
			t.failure = resultErr
		}
	}()
	g := t.binding(name)
	if g == nil {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	if err := t.admitRowControls(g, len(ids)); err != nil {
		return 0, err
	}
	removed := make([][]byte, 0, len(ids))
	for i, id := range ids {
		for _, prior := range ids[:i] {
			if bytes.Equal(id, prior) {
				return 0, ErrDuplicateDocumentID
			}
		}
		if err := t.admitRowDocument(g, id, nil); err != nil {
			return 0, err
		}
		row, err := t.touchNativeRow(g, id)
		if err != nil {
			return 0, err
		}
		if row.current == nil {
			continue
		}
		row.current = nil
		removed = append(removed, row.id)
	}
	if len(removed) == 0 {
		return 0, nil
	}
	payload, err := commitlog.EncodeCollectionDeleteBatchByIDPayload(g.name, removed)
	if err != nil {
		return 0, err
	}
	return len(removed), t.appendNativeMember(g, commitlog.CommandKindCollectionDeleteBatchByID, commitlog.PayloadFormatCollectionDeleteBatchByIDV1, payload)
}

func (t *NativeJSONTxn) finalizeNativeRows() error {
	for i := range t.bindings {
		g := &t.bindings[i]
		if len(g.rows.rows) == 0 {
			continue
		}
		admit := func(n uint64) error { return t.admitStage(g, n) }
		direct, changed, runtimes, err := nativeJSONFinalRows(g.name, g.probe.meta, &g.rows, admit)
		if err != nil {
			return err
		}
		// Reserve the maintained nested unique-owner maps and copied string keys
		// before construction. Scalar schemas contribute at most one old value per
		// row/index; charge each actual value independently, including map headers
		// and small-map backing, while earlier indexed allowances own plan arenas.
		uniqueCharge := memtable.COWAllocationCharge(256)
		for runtimeIdx, runtime := range runtimes {
			if !runtime.def.unique {
				continue
			}
			for _, item := range changed {
				for _, value := range item.oldState.valuesAt(runtimeIdx) {
					n := memtable.COWAllocationCharge(uint64(len(value))) + memtable.COWAllocationCharge(uint64(len(item.documentID))) + memtable.COWAllocationCharge(uint64(len(runtime.def.name))) + memtable.COWAllocationCharge(768)
					if n > ^uint64(0)-uniqueCharge {
						return memtable.ErrCOWCapacity
					}
					uniqueCharge += n
				}
			}
		}
		if err = admit(uniqueCharge); err != nil {
			return err
		}
		if err = t.checkNativeUpdateUnique(g, runtimes, changed, batchUniqueReplacementOwners(runtimes, changed)); err != nil {
			return err
		}
		if err = admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeJSONLogicalInsert{}))) + memtable.COWAllocationCharge(uint64(len(direct.primaryEntries))*uint64(unsafe.Sizeof(memtable.COWMutation{})))); err != nil {
			return err
		}
		g.plan = &nativeJSONLogicalInsert{name: g.name, incarnation: g.incarnation, bootstrap: t.imported, custody: t.custody, primaryPolicy: nativeInsertRootPolicy(direct, direct.primaryRootName), mutations: make([]memtable.COWMutation, len(direct.primaryEntries))}
		g.plan.additional, g.effects, err = extractNativeInsertEffects(g.name, g.incarnation, direct, g.plan.mutations, admit)
		if err != nil {
			return err
		}
	}
	if len(t.members) == 0 {
		return fmt.Errorf("%w: no staged row mutations", backenddb.ErrNativeSemanticUnsupported)
	}
	return nil
}
