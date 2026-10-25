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
func nativeJSONFinalRows(name string, meta CollectionMeta, rows *nativeJSONRows, admit func(uint64) error, prepareScratch func([]byte) error) (*directBufferedInsertPlan, []preparedBatchUpdate, []indexRuntime, error) {
	return nativeJSONFinalRowsWithMode(name, meta, rows, admit, prepareScratch, updateBatchModeAny)
}

func nativeJSONFinalRowsWithMode(name string, meta CollectionMeta, rows *nativeJSONRows, admit func(uint64) error, prepareScratch func([]byte) error, mode updateBatchMode) (*directBufferedInsertPlan, []preparedBatchUpdate, []indexRuntime, error) {
	return nativeJSONFinalRowsResolved(name, meta, rows, admit, prepareScratch, mode, nil)
}
func nativeJSONFinalRowsResolved(name string, meta CollectionMeta, rows *nativeJSONRows, admit func(uint64) error, prepareScratch func([]byte) error, mode updateBatchMode, resolver templateV1Resolver) (*directBufferedInsertPlan, []preparedBatchUpdate, []indexRuntime, error) {
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
	planner.options.templateResolver = resolver
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		return nil, nil, nil, err
	}
	if err = admitNativeInsertArray(len(rows.rows), uint64(unsafe.Sizeof(preparedBatchUpdate{})), admit); err != nil {
		return nil, nil, nil, err
	}
	// One maintained arena owns every retained old/new state. Multi-value
	// extraction keeps its own normalized reference slices; reserve those
	// retained slices before the original parser allocates them.
	var bytesCap, stateCount, maxDocument int
	for _, row := range rows.rows {
		for _, doc := range [][]byte{row.opening, row.current} {
			if doc == nil {
				continue
			}
			capacity, err := nativeJSONDocumentEncodedCapacity(doc, meta)
			if err != nil || capacity > int(^uint(0)>>1)-bytesCap || len(runtimes) > int(^uint(0)>>1)-stateCount {
				return nil, nil, nil, memtable.ErrCOWCapacity
			}
			bytesCap += capacity
			stateCount += len(runtimes)
			source, scalar := nativeJSONIndexedDocumentSource(doc, meta)
			for _, index := range meta.Indexes {
				if !scalar && (index.MultiKey || meta.Options.AllowArrayValuesInIndex) {
					// Every array value occupies at least one source byte.
					// The maintained parser allocates a reference for every
					// input element before sorting/deduplicating it.
					if err = admitNativeInsertArray(len(doc), uint64(unsafe.Sizeof([]byte{})), admit); err != nil {
						return nil, nil, nil, err
					}
				}
			}
			if len(runtimes) != 0 {
				maxDocument = max(maxDocument, source)
			}
		}
	}
	// Admit each actual backing once. Source-sized scalar/value bounds cover
	// encoded images before parsing; decoder scratch has its separate high-water
	// receipt. The final arena never grows while old/new states borrow it.
	for _, allocation := range []struct {
		count int
		size  uint64
	}{
		{bytesCap, 1}, {maxDocument, 1},
		{stateCount, uint64(unsafe.Sizeof([][]byte{}))},
		{stateCount, uint64(unsafe.Sizeof([]byte{}))},
	} {
		if err = admitNativeInsertArray(allocation.count, allocation.size, admit); err != nil {
			return nil, nil, nil, err
		}
	}
	encoder := indexEncodeArena{buf: make([]byte, 0, bytesCap), scratch: make([]byte, 0, maxDocument), states: make([][][]byte, 0, stateCount), valueRefs: make([][]byte, 0, stateCount)}
	changed := make([]preparedBatchUpdate, 0, len(rows.rows))
	stats := CollectionUpdateStats{}
	for i, row := range rows.rows {
		item := preparedBatchUpdate{itemIndex: i, documentID: row.id, document: row.current}
		if row.opening != nil {
			if prepareScratch != nil {
				if err = prepareScratch(row.opening); err != nil {
					return nil, nil, nil, err
				}
			}
			item.oldState, err = orderedIndexStateForDocumentWithArena(row.opening, runtimes, planner.options, &encoder)
			if err != nil {
				return nil, nil, nil, err
			}
		}
		if row.current == nil {
			markBatchUpdateIndexEffects(&item, runtimes, &stats)
		} else {
			if prepareScratch != nil {
				if err = prepareScratch(row.current); err != nil {
					return nil, nil, nil, err
				}
			}
			if err = prepareBatchUpdateIndexEffects(&item, runtimes, planner.options, &encoder, &stats, false); err != nil {
				return nil, nil, nil, err
			}
		}
		changed = append(changed, item)
	}
	if mode == updateBatchModeNoSecondaryUniqueIndexChanges {
		if updateBatchChangesSecondaryUniqueIndex(runtimes, changed) {
			return nil, nil, nil, errUpdateBatchChangesSecondaryUniqueIndex
		}
		// Conditional eligibility is decided before final uniqueness or publication.
		return nil, changed, runtimes, nil
	}
	charge, stateEntries, roots, err := nativeJSONFinalPlanCharge(name, planner.options, runtimes, changed)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = admit(charge); err != nil {
		return nil, nil, nil, err
	}
	if err = rejectBatchUniqueConflicts(runtimes, changed); err != nil {
		return nil, nil, nil, err
	}
	plan, err := buildNativeCRUDFinalPlan(name, planner.options, runtimes, changed, stateEntries, roots)
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

// Staging retains actual ID/document copies, not a fresh publication/planner
// envelope per document. touch accounts opening copies independently; canonical
// member images are admitted from their complete lengths immediately below.
func (t *NativeJSONTxn) admitRowDocument(g *nativeJSONTxnBinding, id, doc []byte) error {
	ids := memtable.COWAllocationCharge(uint64(len(id)))
	document := memtable.COWAllocationCharge(uint64(len(doc)))
	if ids > (^uint64(0)-document)/2 {
		return memtable.ErrCOWCapacity
	}
	return t.admitStage(g, 2*ids+document)
}

// Scalar queries and scalar-only schemas use bounded string escaping or the
// maintained V2 numeric digit cap. Array documents add their per-value bound below.
func nativeJSONEncodedCapacity(document int, indexes []IndexDefinition) (int, error) {
	limit := int(^uint(0) >> 1)
	if document < 0 || document > (limit-16)/2 {
		return 0, memtable.ErrCOWCapacity
	}
	var total int
	for _, index := range indexes {
		capacity := 2*document + 16
		if index.ValueType == IndexValueBSONOrderedV2 {
			// V2 numbers have a marker, numeric tag/class, two-byte exponent,
			// packed decimal digits and a terminator. Exact float expansion can
			// exceed its eight-byte source; use the maintained codec's digit cap.
			capacity = max(capacity, 6+(bsonIndexKeyNumericV2MaxDigits+1)/2)
		}
		// Each compound component owns an encoded image in the SAME tuple.
		// Repeated paths still encode independently; numeric caps apply to each.
		components := max(1, len(index.Components))
		if capacity > (limit-total)/components {
			return 0, memtable.ErrCOWCapacity
		}
		total += capacity * components
	}
	return total, nil
}

// nativeJSONDocumentEncodedCapacity extends the original scalar bound only
// for array-capable schemas. At most len(document) values can be present per
// indexed path. Each fixed scalar/tag/terminator fits 16 bytes; escaped source
// strings fit twice their source. BSON V2 additionally has the maintained exact
// numeric digit cap: each numeric element has its BSON type byte somewhere in
// the source. Counting every such byte (including false positives inside values)
// is a conservative allocation-free bound, not an alternate document parser.
// Repeated/dotted indexed paths each receive their independent full allowance.
func nativeJSONDocumentEncodedCapacity(document []byte, meta CollectionMeta) (int, error) {
	limit := int(^uint(0) >> 1)
	indexes, allowArrays := meta.Indexes, meta.Options.AllowArrayValuesInIndex
	source, scalar := nativeJSONIndexedDocumentSource(document, meta)
	var total, numericBytes int
	for _, index := range indexes {
		if scalar || index.ValueType != IndexValueBSONOrderedV2 || (!index.MultiKey && !allowArrays) {
			continue
		}
		for _, b := range document {
			switch b {
			case 0x01, 0x10, 0x12, 0x13:
				numericBytes++
			} // double, int32, int64, decimal128
		}
		break
	}
	for _, index := range indexes {
		capacity, err := nativeJSONEncodedCapacity(source, []IndexDefinition{index})
		if err != nil {
			return 0, err
		}
		if !scalar && (index.MultiKey || allowArrays) {
			if len(document) > (limit-capacity)/16 {
				return 0, memtable.ErrCOWCapacity
			}
			capacity += len(document) * 16
			if index.ValueType == IndexValueBSONOrderedV2 {
				const numeric = 6 + (bsonIndexKeyNumericV2MaxDigits+1)/2
				if numericBytes > (limit-capacity)/numeric {
					return 0, memtable.ErrCOWCapacity
				}
				capacity += numericBytes * numeric
			}
		}
		if capacity > limit-total {
			return 0, memtable.ErrCOWCapacity
		}
		total += capacity
	}
	return total, nil
}

// One original transaction-local scratch receipt owns the maintained arena and
// the decoder fallback envelope. Only its maximum document demand is retained.
// No encoded validation state escapes this scope; final states use a different,
// retained arena. Growth admits old+new coexistence before replacement.
func (t *NativeJSONTxn) prepareValidation(g *nativeJSONTxnBinding, doc []byte) error {
	if len(g.probe.meta.Indexes) == 0 {
		return nil
	}
	n, err := nativeJSONIndexedPlanCharge(g.probe.meta, [][]byte{nil}, [][]byte{doc})
	if err != nil {
		return err
	}
	capacity, err := nativeJSONDocumentEncodedCapacity(doc, g.probe.meta)
	if err != nil {
		return err
	}
	source, _ := nativeJSONIndexedDocumentSource(doc, g.probe.meta)
	if n <= g.validationCharge && capacity <= cap(g.validation.buf) && source <= cap(g.validation.scratch) {
		return nil
	}
	n = max(n, g.validationCharge)
	if n > ^uint64(0)-g.validationCharge {
		return memtable.ErrCOWCapacity
	}
	if g.validationLease == nil {
		g.validationLease, err = t.core.NativeAllocator().AdmitNativeBytes(n)
	} else {
		err = g.validationLease.Resize(g.validationCharge + n)
	}
	if err != nil {
		return err
	}
	g.validation = indexEncodeArena{buf: make([]byte, 0, capacity), scratch: make([]byte, 0, source), states: make([][][]byte, 0, len(g.probe.meta.Indexes)), valueRefs: make([][]byte, 0, len(g.probe.meta.Indexes))}
	// Replacement detached every old arena alias before the original charge shrinks.
	if err = g.validationLease.Resize(n); err != nil {
		return err
	}
	g.validationCharge = n
	return nil
}
func (t *NativeJSONTxn) validateDocument(g *nativeJSONTxnBinding, doc []byte, runtimes []indexRuntime, opts collectionOptions) error {
	if err := t.prepareValidation(g, doc); err != nil {
		return err
	}
	_, err := orderedIndexStateForDocumentWithArena(doc, runtimes, opts, &g.validation)
	clear(g.validation.states)
	clear(g.validation.valueRefs)
	g.validation.buf = g.validation.buf[:0]
	g.validation.scratch = g.validation.scratch[:0]
	g.validation.states = g.validation.states[:0]
	g.validation.valueRefs = g.validation.valueRefs[:0]
	if err != nil || !opts.allowArrayValuesInIndex || len(g.probe.meta.VectorIndexes) == 0 || !nativeOrdinaryHNSWMeta(g.probe.meta) {
		return err
	}
	// Ordinary HNSW stores one value per eligible scalar column. Reuse its
	// strict maintained parser before publication so an allowed collection
	// array cannot become a canonical row that graph maintenance cannot read.
	// Explicit MultiKey indexes are excluded by the same column predicate.
	strict := opts
	strict.allowArrayValuesInIndex = false
	for i, def := range g.probe.meta.Indexes {
		if !supportsNativeScalarColumn(def) {
			continue
		}
		_, err = orderedIndexStateForDocumentWithArena(doc, runtimes[i:i+1], strict, &g.validation)
		clear(g.validation.states)
		clear(g.validation.valueRefs)
		g.validation.buf = g.validation.buf[:0]
		g.validation.scratch = g.validation.scratch[:0]
		g.validation.states = g.validation.states[:0]
		g.validation.valueRefs = g.validation.valueRefs[:0]
		if err != nil {
			return fmt.Errorf("collections: native HNSW scalar index %q: %w", def.Name, err)
		}
	}
	return err
}

func (t *NativeJSONTxn) admitMemberPayload(g *nativeJSONTxnBinding, docs []commitlog.CollectionDocument, ids [][]byte) error {
	// v1 header=10+name; canonical encoders copy one header array before sort.
	n := uint64(10 + len(g.name))
	for _, doc := range docs {
		extra := uint64(8) + uint64(len(doc.ID)) + uint64(len(doc.Document))
		if extra > ^uint64(0)-n {
			return memtable.ErrCOWCapacity
		}
		n += extra
	}
	for _, id := range ids {
		extra := uint64(4) + uint64(len(id))
		if extra > ^uint64(0)-n {
			return memtable.ErrCOWCapacity
		}
		n += extra
	}
	docSize, idSize := uint64(unsafe.Sizeof(commitlog.CollectionDocument{})), uint64(unsafe.Sizeof([]byte{}))
	if uint64(len(docs)) > ^uint64(0)/docSize || uint64(len(ids)) > ^uint64(0)/idSize {
		return memtable.ErrCOWCapacity
	}
	docHeaders := memtable.COWAllocationCharge(uint64(len(docs)) * docSize)
	idHeaders := memtable.COWAllocationCharge(uint64(len(ids)) * idSize)
	charge := memtable.COWAllocationCharge(n)
	if docHeaders > ^uint64(0)-charge || idHeaders > ^uint64(0)-charge-docHeaders {
		return memtable.ErrCOWCapacity
	}
	return t.admitStage(g, charge+docHeaders+idHeaders)
}

// Admit aggregate staging headers before the maintained planner/callback adapter.
// Old grown backings coexist until the original transaction receipt terminates.
func (t *NativeJSONTxn) admitRowControls(g *nativeJSONTxnBinding, count int) error {
	perItem := uint64(unsafe.Sizeof(insertBatchItem{})) + uint64(unsafe.Sizeof(int(0))) + uint64(unsafe.Sizeof(preparedBatchUpdate{})) + uint64(unsafe.Sizeof(updateBatchItem{})) + uint64(unsafe.Sizeof(UpdateBatchResult{})) + uint64(unsafe.Sizeof(commitlog.CollectionDocument{})) + 8*uint64(unsafe.Sizeof([]byte{})) + 256
	if uint64(count) > ^uint64(0)/perItem {
		return memtable.ErrCOWCapacity
	}
	// Fixed publication/planner/read headers occur once per member scope, rather
	// than once per row. Keep the original conservative header formula.
	fixed, err := nativeJSONInsertCharge(g.name, nil, nil)
	if err != nil {
		return err
	}
	indexed, err := nativeJSONIndexedPlanCharge(g.probe.meta, nil, nil)
	if err != nil || indexed > ^uint64(0)-fixed {
		return memtable.ErrCOWCapacity
	}
	rows := memtable.COWAllocationCharge(uint64(count) * perItem)
	if rows > ^uint64(0)-fixed-indexed {
		return memtable.ErrCOWCapacity
	}
	return t.admitStage(g, fixed+indexed+rows)
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
	normalized, options, err := t.prepareTemplateDocuments(g, documents, planner.options)
	planner.options = options
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
		if err := t.validateDocument(g, item.document, runtimes, planner.options); err != nil {
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
		canonical := row.current
		if normalizedDocumentFormat(planner.options.documentFormat) == DocumentFormatTemplateV1 {
			canonical = documents[orderedItemIndex(order, i)]
		}
		docs[i] = commitlog.CollectionDocument{ID: row.id, Document: canonical}
	}
	if err = t.admitMemberPayload(g, docs, nil); err != nil {
		return err
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
	if g.probe.meta.Options.ColumnStore != nil {
		return t.stageNativeTypedUpdate(g, items)
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
	templateBatch := normalizedDocumentFormat(planner.options.documentFormat) == DocumentFormatTemplateV1
	var changedIndices []int
	if templateBatch {
		for _, size := range [2]uint64{uint64(unsafe.Sizeof(int(0))), uint64(unsafe.Sizeof([]byte{}))} {
			if err = admitNativeInsertArray(len(items), size, func(n uint64) error { return t.admitStage(g, n) }); err != nil {
				return nil, err
			}
		}
		changedIndices = make([]int, 0, len(items))
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
		if err := validateBSONReplacementPreservesID(row.current, document, planner.options); err != nil {
			return nil, updateBatchItemError(i, err)
		}
		if err = t.admitRowDocument(g, row.id, document); err != nil {
			return nil, err
		}
		// Copy callback-owned bytes before invoking another callback. Template
		// envelopes must be prepared together: a TD1H replacement may precede
		// its defining TD1I envelope later in this same UpdateBatch member.
		ownedDocument := bytes.Clone(document)
		if templateBatch {
			docs = append(docs, commitlog.CollectionDocument{ID: row.id, Document: ownedDocument})
			changedIndices = append(changedIndices, i)
			continue
		}
		normalized, options, err := t.prepareTemplateDocuments(g, [][]byte{ownedDocument}, planner.options)
		planner.options = options
		if err != nil {
			return nil, updateBatchItemError(i, err)
		}
		if err := t.validateDocument(g, normalized[0], runtimes, planner.options); err != nil {
			return nil, updateBatchItemError(i, err)
		}
		row.current = normalized[0]
		result[i].Modified = true
		docs = append(docs, commitlog.CollectionDocument{ID: row.id, Document: row.current})
	}
	if len(docs) == 0 {
		return result, nil
	}
	if templateBatch {
		// The input header backing was admitted with the item indices above;
		// the original envelope copies stay owned through canonical encoding.
		inputs := make([][]byte, len(docs))
		for i := range docs {
			inputs[i] = docs[i].Document
		}
		normalized, options, err := t.prepareTemplateDocuments(g, inputs, planner.options)
		if err != nil {
			return nil, fmt.Errorf("collections: update batch replacement prepare: %w", err)
		}
		planner.options = options
		for i, document := range normalized {
			if err = t.validateDocument(g, document, runtimes, planner.options); err != nil {
				return nil, updateBatchItemError(changedIndices[i], err)
			}
			// Later touches may grow or shift the sorted row slice. Only the
			// separately owned ID survives that movement, never a saved row pointer.
			row := g.rows.find(docs[i].ID)
			if row == nil {
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
			row.current = document
			result[changedIndices[i]].Modified = true
		}
	}
	if err = t.admitMemberPayload(g, docs, nil); err != nil {
		return nil, err
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
	if g != nil && g.probe.meta.Options.ColumnStore != nil {
		_, count, err := t.stageNativeTypedBatch(g, nil, nil, nil, ids, commitlog.CommandKindCollectionDeleteBatchByID, false)
		return count, err
	}
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
	if err := t.admitMemberPayload(g, nil, removed); err != nil {
		return 0, err
	}
	payload, err := commitlog.EncodeCollectionDeleteBatchByIDPayload(g.name, removed)
	if err != nil {
		return 0, err
	}
	return len(removed), t.appendNativeMember(g, commitlog.CommandKindCollectionDeleteBatchByID, commitlog.PayloadFormatCollectionDeleteBatchByIDV1, payload)
}

func (t *NativeJSONTxn) finalizeNativeRows(graphSingleton bool) error {
	return t.finalizeNativeRowsWithStats(graphSingleton, nil)
}

func (t *NativeJSONTxn) finalizeNativeRowsWithStats(graphSingleton bool, stats *CollectionInsertStats) error {
	for i := range t.bindings {
		g := &t.bindings[i]
		if g.typed != nil {
			if err := t.finalizeNativeTyped(g); err != nil {
				return err
			}
			continue
		}
		if len(g.rows.rows) == 0 {
			continue
		}
		admit := func(n uint64) error { return t.admitStage(g, n) }
		var opts collectionOptions
		if normalizedDocumentFormat(g.probe.meta.Options.DocumentFormat) == DocumentFormatTemplateV1 {
			var err error
			opts, err = collectionPlannerOptions(g.probe.meta)
			if err != nil {
				return err
			}
			opts, err = t.templateOptions(g, opts)
			if err != nil {
				return err
			}
		}
		direct, changed, runtimes, err := nativeJSONFinalRowsResolved(g.name, g.probe.meta, &g.rows, admit, func(doc []byte) error { return t.prepareValidation(g, doc) }, updateBatchModeAny, g.templates)
		if err != nil {
			return err
		}
		if err = nativeTemplatePlanEntries(direct, g.name, g.templateRecords, opts, admit); err != nil {
			return err
		}
		// Reserve the maintained nested unique-owner maps and copied string keys
		// before construction. Charge every actual old value independently, including
		// multikey values, map headers
		// and small-map backing. The final-plan inventory already owns its arenas.
		uniqueCharge := memtable.COWAllocationCharge(256)
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
		if err == nil {
			err = t.finalizeNativeText(g)
		}
		if err != nil {
			return err
		}
		if stats != nil {
			if err = t.collectNativeInsertStats(g, direct, stats); err != nil {
				return err
			}
		}
	}
	// Graph rebuild finalizes a catalog effect and a typed recipe without CRUD
	// members. Ordinary empty transactions still have no publishable mutation.
	if len(t.members) == 0 && !graphSingleton {
		return fmt.Errorf("%w: no staged row mutations", backenddb.ErrNativeSemanticUnsupported)
	}
	return nil
}
