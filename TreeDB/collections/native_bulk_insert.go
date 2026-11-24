package collections

import (
	"bytes"
	"errors"
	"time"
	"unsafe"
	"weak"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// nativeBulkInsertPreimage holds strictly COW-admitted, schema-validated insert
// artifacts computed outside the per-collection bulk commit gate so concurrent
// bulk producers overlap CPU work (validation, sorting, index extraction,
// canonical WAL encoding) across cores while opening against the latest cut
// immediately before CommitNative.
type nativeBulkInsertPreimage struct {
	name          string
	incarnation   uint64
	schema        weak.Pointer[nativeSchemaState]
	lease         *memtable.COWExternalLease
	charge        uint64
	primaryPolicy backenddb.OrderedRootStoragePolicy
	primary       []memtable.COWMutation
	additional    []nativeJSONDomainChange
	effects       []txnhistory.Effect
	payload       []byte
	direct        *directBufferedInsertPlan
	hasUnique     bool
	runtimes      []indexRuntime
	changed       []preparedBatchUpdate
}

func (p *nativeBulkInsertPreimage) Close() {
	if p == nil {
		return
	}
	lease := p.lease
	*p = nativeBulkInsertPreimage{}
	if lease != nil {
		lease.Close()
	}
}

func (p *nativeBulkInsertPreimage) admit(n uint64) error {
	if n == 0 {
		return nil
	}
	if n > ^uint64(0)-p.charge {
		return memtable.ErrCOWCapacity
	}
	next := p.charge + n
	if err := p.lease.Resize(next); err != nil {
		return err
	}
	p.charge = next
	return nil
}

func nativeBulkInsertEligibleMeta(db *backenddb.DB, meta CollectionMeta) bool {
	if meta.Options.ColumnStore != nil || len(meta.VectorIndexes) != 0 || len(meta.TextIndexes) != 0 {
		return false
	}
	format := normalizedDocumentFormat(meta.Options.DocumentFormat)
	if format == DocumentFormatTemplateV1 {
		return false
	}
	// Indexed JSON documents may fall back to json.NewDecoder (for example on
	// dotted paths), whose decoded object graph must be admitted via the
	// standard validateDocument/prepareValidation staging path. Keep those on
	// the standard path (still coordinated by coord.nativeBulkInsertMu).
	if format != DocumentFormatBSON && len(meta.Indexes) != 0 {
		return false
	}
	if err := nativeTypedProfile(db, meta); err != nil {
		return false
	}
	if _, err := nativeJSONMeta(meta); err != nil {
		return false
	}
	return true
}

func isSimpleScalarBSONMeta(meta CollectionMeta) bool {
	if normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatBSON || meta.Options.AllowArrayValuesInIndex {
		return false
	}
	for _, idx := range meta.Indexes {
		if idx.MultiKey || len(idx.Components) > 0 {
			return false
		}
	}
	return true
}

// prepareNativeBulkInsertPreimage prepares an immutable, strictly COW-admitted
// bulk insert plan outside coord.nativeBulkInsertMu. Returns (nil, nil) when
// the collection is ineligible or requires fallback to the standard transaction
// path inside the gate.
func prepareNativeBulkInsertPreimage(m *NativeJSONManager, name string, ids, documents [][]byte, wantStats bool) (pre *nativeBulkInsertPreimage, resultErr error) {
	if m == nil || m.db == nil || len(ids) <= 1 || len(ids) != len(documents) {
		return nil, nil
	}
	read, err := m.captureCatalog(name)
	if err != nil {
		// Fall back to the standard path inside the gate so closed/missing/capacity
		// errors preserve exact transaction semantics.
		return nil, nil
	}
	if read.collection == nil || read.collection.schema == nil || read.collection.schema.meta == nil {
		_ = read.Close()
		return nil, nil
	}
	meta := *read.collection.schema.meta
	incarnation := read.collection.incarnation
	schemaWeak := weak.Make(read.collection.schema)
	allocator := read.capture.NativeAllocator()
	if closeErr := read.Close(); closeErr != nil {
		return nil, closeErr
	}
	if allocator == nil || !nativeBulkInsertEligibleMeta(m.db, meta) {
		return nil, nil
	}

	// Validate inputs before admitting buffers.
	var totalIDBytes, totalDocBytes uint64
	isBSON := normalizedDocumentFormat(meta.Options.DocumentFormat) == DocumentFormatBSON
	for i := range ids {
		if len(ids[i]) == 0 {
			return nil, errors.New("collections: document id cannot be empty")
		}
		if documents[i] == nil {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		if uint64(len(ids[i])) > ^uint64(0)-totalIDBytes {
			return nil, memtable.ErrCOWCapacity
		}
		totalIDBytes += uint64(len(ids[i]))
		if !isBSON {
			if uint64(len(documents[i])) > ^uint64(0)-totalDocBytes {
				return nil, memtable.ErrCOWCapacity
			}
			totalDocBytes += uint64(len(documents[i]))
		}
	}

	lease, err := allocator.AdmitNativeBytes(0)
	if err != nil {
		return nil, err
	}
	p := &nativeBulkInsertPreimage{
		name:        name,
		incarnation: incarnation,
		schema:      schemaWeak,
		lease:       lease,
	}
	defer func() {
		if resultErr != nil {
			p.Close()
		}
	}()

	// Batch-admit initial arenas, planner headers, sort buffers, and WAL member descriptors.
	n := len(ids)
	fixedHeaderCharge, err := nativeJSONIndexedPlanCharge(meta, nil, nil)
	if err != nil {
		return nil, err
	}
	perRow := uint64(unsafe.Sizeof(insertBatchItem{})) +
		uint64(unsafe.Sizeof(int(0))) +
		uint64(unsafe.Sizeof(preparedBatchUpdate{})) +
		uint64(unsafe.Sizeof(commitlog.CollectionDocument{})) +
		uint64(unsafe.Sizeof(memtable.COWMutation{})) +
		uint64(unsafe.Sizeof(txnhistory.Effect{}))
	if uint64(n) > ^uint64(0)/perRow {
		return nil, memtable.ErrCOWCapacity
	}
	initialCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeBulkInsertPreimage{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeJSONLogicalInsert{}))) +
		fixedHeaderCharge +
		memtable.COWAllocationCharge(uint64(n)*perRow) +
		memtable.COWAllocationCharge(totalIDBytes) +
		memtable.COWAllocationCharge(totalDocBytes)
	if err = p.admit(initialCharge); err != nil {
		return nil, err
	}

	planner, err := nativeJSONInsertPlanner(meta)
	if err != nil {
		return nil, err
	}
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		return nil, err
	}
	normalized, _, _, _, err := prepareInsertDocuments(documents, planner.options)
	if err != nil {
		return nil, err
	}

	// Copy all IDs into one contiguous batch arena (1 allocation instead of N).
	// Documents are borrowed read-only during this synchronous one-shot insert
	// and copied once into the WAL payload and once into COWWriter.Prepare.
	idArena := make([]byte, totalIDBytes)
	items := make([]insertBatchItem, n)
	idOff := 0
	for i := range ids {
		idLen := len(ids[i])
		idCopy := idArena[idOff : idOff+idLen : idOff+idLen]
		copy(idCopy, ids[i])
		idOff += idLen

		docSrc := normalized[i]
		if docSrc == nil {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		items[i] = insertBatchItem{id: idCopy, document: docSrc}
	}

	order := sortedItemOrderByKey(items, func(item *insertBatchItem) []byte { return item.id })
	if err = rejectDuplicateDocumentIDs(items, order); err != nil {
		return nil, err
	}

	changed := make([]preparedBatchUpdate, n)
	docs := make([]commitlog.CollectionDocument, n)
	for i := 0; i < n; i++ {
		item := items[orderedItemIndex(order, i)]
		changed[i] = preparedBatchUpdate{
			itemIndex:  i,
			documentID: item.id,
			document:   item.document,
		}
		docs[i] = commitlog.CollectionDocument{ID: item.id, Document: item.document}
	}

	// Extract secondary index states with strict pre-admission and zero per-row
	// COWBudget.mu churn.
	if len(runtimes) > 0 {
		simpleBSON := isSimpleScalarBSONMeta(meta)
		stateCount := n * len(runtimes)
		var scratchCap int
		var initialBufCap int
		if simpleBSON {
			maxDocLen := 0
			for i := 0; i < n; i++ {
				if l := len(changed[i].document); l > maxDocLen {
					maxDocLen = l
				}
			}
			perIdx := max(maxDocLen+16, bsonIndexKeyNumericV2MaxDigits)
			scratchCap = max(64, len(runtimes)*perIdx)
			initialBufCap = min(n*len(runtimes)*24, 256<<10)
			if initialBufCap < 4096 {
				initialBufCap = 4096
			}
		} else {
			for i := 0; i < n; i++ {
				doc := changed[i].document
				capDoc, capErr := nativeJSONDocumentEncodedCapacity(doc, meta)
				if capErr != nil || capDoc > int(^uint(0)>>1)-initialBufCap {
					return nil, memtable.ErrCOWCapacity
				}
				initialBufCap += capDoc
				source, scalar := nativeJSONIndexedDocumentSource(doc, meta)
				if source > scratchCap {
					scratchCap = source
				}
				for _, index := range meta.Indexes {
					if !scalar && (index.MultiKey || meta.Options.AllowArrayValuesInIndex) {
						if err = admitNativeInsertArray(len(doc), uint64(unsafe.Sizeof([]byte{})), p.admit); err != nil {
							return nil, err
						}
					}
				}
			}
		}
		for _, alloc := range []struct {
			count int
			size  uint64
		}{
			{initialBufCap, 1},
			{scratchCap, 1},
			{stateCount, uint64(unsafe.Sizeof([][]byte{}))},
			{stateCount, uint64(unsafe.Sizeof([]byte{}))},
		} {
			if err = admitNativeInsertArray(alloc.count, alloc.size, p.admit); err != nil {
				return nil, err
			}
		}
		encoder := indexEncodeArena{
			buf:       make([]byte, 0, initialBufCap),
			scratch:   make([]byte, 0, scratchCap),
			states:    make([][][]byte, 0, stateCount),
			valueRefs: make([][]byte, 0, stateCount),
		}
		var dummyStats CollectionUpdateStats
		for i := 0; i < n; i++ {
			doc := changed[i].document
			if simpleBSON {
				// Ensure the current arena chunk has space for this document's worst-case
				// encoded output so append never triggers an unadmitted reallocation.
				perIdxNeed := max(2*len(doc)+16, 6+(bsonIndexKeyNumericV2MaxDigits+1)/2)
				need := len(runtimes) * perIdxNeed
				if need > cap(encoder.buf)-len(encoder.buf) {
					chunkCap := max(need, 64<<10)
					if err = p.admit(memtable.COWAllocationCharge(uint64(chunkCap))); err != nil {
						return nil, err
					}
					encoder.buf = make([]byte, 0, chunkCap)
				}
			}
			state, err := orderedIndexStateForDocumentWithArena(doc, runtimes, planner.options, &encoder)
			if err != nil {
				return nil, err
			}
			changed[i].newState = state
			markBatchUpdateIndexEffects(&changed[i], runtimes, &dummyStats)
		}
	}

	if err = rejectBatchUniqueConflicts(runtimes, changed); err != nil {
		return nil, err
	}

	planCharge, stateEntries, roots, err := nativeJSONFinalPlanCharge(name, planner.options, runtimes, changed)
	if err != nil {
		return nil, err
	}
	if err = p.admit(planCharge); err != nil {
		return nil, err
	}
	direct, err := buildNativeCRUDFinalPlan(name, planner.options, runtimes, changed, stateEntries, roots)
	if err != nil {
		return nil, err
	}

	primary := make([]memtable.COWMutation, len(direct.primaryEntries))
	additional, effects, err := extractNativeInsertEffects(name, incarnation, direct, primary, p.admit, meta)
	if err != nil {
		return nil, err
	}

	// Admit and encode the canonical Command WAL payload.
	payloadSize := uint64(10 + len(name))
	for _, doc := range docs {
		extra := uint64(8) + uint64(len(doc.ID)) + uint64(len(doc.Document))
		if extra > ^uint64(0)-payloadSize {
			return nil, memtable.ErrCOWCapacity
		}
		payloadSize += extra
	}
	if err = p.admit(memtable.COWAllocationCharge(payloadSize)); err != nil {
		return nil, err
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload(name, docs)
	if err != nil {
		return nil, err
	}

	hasUnique := false
	for _, rt := range runtimes {
		if rt.def.unique {
			hasUnique = true
			break
		}
	}

	p.primaryPolicy = nativeInsertRootPolicy(direct, direct.primaryRootName)
	p.primary = primary
	p.additional = additional
	p.effects = effects
	p.payload = payload
	p.hasUnique = hasUnique
	if hasUnique {
		p.runtimes = runtimes
		p.changed = changed
	}
	if wantStats {
		p.direct = direct
	}
	return p, nil
}

// attachNativeBulkInsertPreimage binds a precomputed bulk insert plan to a
// freshly opened transaction under the per-collection bulk commit gate, running
// primary/unique existence checks against the transaction's fresh opening cut.
// Returns attached=false if the collection schema/incarnation changed since
// preimage construction so the caller can fall back to standard staging.
func (t *NativeJSONTxn) attachNativeBulkInsertPreimage(pre *nativeBulkInsertPreimage, stats *CollectionInsertStats) (attached bool, resultErr error) {
	if t == nil || pre == nil {
		return false, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.core == nil {
		return false, backenddb.ErrClosed
	}
	if t.failure != nil {
		return false, t.failure
	}
	g := &t.bindings[0]
	if g.incarnation != pre.incarnation || g.probe.collection.schema != pre.schema.Value() {
		return false, nil
	}
	defer func() {
		if resultErr != nil {
			t.failure = resultErr
		}
	}()

	n := len(pre.primary)
	// Check primary key existence against the fresh opening cut.
	if g.probe.domain.domain != nil && n > 0 {
		pending := g.probe.domain.domain.pending
		captured := g.probe.domain.domain.captured
		hasBase := g.probe.domain.hasBase
		var pendingMax string
		var hasPendingMax bool
		if pending != nil && pending.Len() > 0 {
			pendingMax, hasPendingMax = pending.MaxKey()
		}
		allAbovePending := pending == nil || pending.Len() == 0 ||
			(hasPendingMax && bytes.Compare(pre.primary[0].Key, nativeBorrowedStringBytes(pendingMax)) > 0)
		if !hasBase && captured == nil && allAbovePending {
			// Fast path: all sorted batch IDs lie strictly above pending max key
			// and there are no captured or base physical layers.
		} else {
			var physical backenddb.NativePhysicalReader
			if hasBase {
				physical, _ = g.probe.capture.(backenddb.NativePhysicalReader)
			}
			for i := 0; i < n; i++ {
				id := pre.primary[i].Key
				if pending != nil && pending.Len() > 0 && (!hasPendingMax || bytes.Compare(id, nativeBorrowedStringBytes(pendingMax)) <= 0) {
					if row, ok := pending.Get(id); ok {
						if row.Flags&node.FlagTombstone == 0 {
							return true, ErrDocumentExists
						}
						continue
					}
				}
				if captured != nil {
					if row, ok := captured.get(id); ok {
						if row.Flags&node.FlagTombstone == 0 {
							return true, ErrDocumentExists
						}
						continue
					}
				}
				if !hasBase {
					continue
				}
				if physical != nil {
					entry, err := physical.NativeEntryAtRoot(g.probe.domain.domain.baseRoot, id, false)
					if errors.Is(err, tree.ErrKeyNotFound) {
						continue
					}
					if err != nil {
						return true, err
					}
					if entry.Flags&node.FlagTombstone == 0 {
						return true, ErrDocumentExists
					}
					continue
				}
				scratch, scratchErr := g.probe.physicalScratch()
				if scratchErr != nil {
					return true, scratchErr
				}
				entry, err := g.probe.domain.baseEntry(id, scratch, g.probe.readLeaf)
				if errors.Is(err, tree.ErrKeyNotFound) {
					continue
				}
				if err != nil {
					return true, err
				}
				if entry.Flags&node.FlagTombstone == 0 {
					return true, ErrDocumentExists
				}
			}
		}
	}

	// Batch-admit all primary point dependencies in one Resize instead of N calls.
	depSize := uint64(unsafe.Sizeof(txnhistory.Dependency{}))
	needCap := len(t.dependencies) + n
	if needCap > cap(t.dependencies) {
		if uint64(needCap) > ^uint64(0)/depSize {
			return true, memtable.ErrCOWCapacity
		}
		backing := memtable.COWAllocationCharge(uint64(needCap) * depSize)
		if backing > ^uint64(0)-t.charge {
			return true, memtable.ErrCOWCapacity
		}
		if err := t.lease.Resize(t.charge + backing); err != nil {
			return true, err
		}
		t.charge += backing
		next := make([]txnhistory.Dependency, len(t.dependencies), needCap)
		copy(next, t.dependencies)
		t.dependencies = next
	}
	ns := g.namespace()
	for i := 0; i < n; i++ {
		// Key slices belong to pre.idArena which stays live and admitted until after tx.Close().
		t.dependencies = append(t.dependencies, txnhistory.Dependency{
			Namespace: ns,
			Kind:      txnhistory.PointEffect,
			Key:       pre.primary[i].Key,
		})
	}

	if pre.hasUnique {
		if err := t.checkNativeUpdateUnique(g, pre.runtimes, pre.changed, nil); err != nil {
			return true, err
		}
	}

	g.plan = &nativeJSONLogicalInsert{
		name:          g.name,
		incarnation:   g.incarnation,
		bootstrap:     t.imported,
		custody:       t.custody,
		primaryPolicy: pre.primaryPolicy,
		mutations:     pre.primary,
		additional:    pre.additional,
	}
	g.effects = pre.effects
	if err := t.appendNativeMember(g, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, pre.payload); err != nil {
		return true, err
	}
	if stats != nil && pre.direct != nil {
		if err := t.collectNativeInsertStats(g, pre.direct, stats); err != nil {
			return true, err
		}
	}
	return true, nil
}

func (m *NativeJSONManager) insertBulkBatch(name string, ids, documents [][]byte) error {
	for attempt := 0; ; attempt++ {
		if err := m.db.PrepareNativeProducer(); err != nil {
			return err
		}
		pre, preErr := prepareNativeBulkInsertPreimage(m, name, ids, documents, false)
		if preErr != nil {
			return mutationNotAcceptedError(preErr)
		}
		_, retry, err := m.commitBulkInsertAttempt(name, ids, documents, pre, 0, nil, false, nil, nil, nil)
		if !retry || attempt+1 == nativeInsertAttempts {
			return err
		}
	}
}

func (m *NativeJSONManager) commitBulkInsertAttempt(
	name string,
	ids, documents [][]byte,
	pre *nativeBulkInsertPreimage,
	preElapsed time.Duration,
	stats *CollectionInsertStats,
	ordinaryOnly bool,
	projection *trustedFloat32Projection,
	projected **nativeInsertProjectedRefusal,
	journal *nativeOperationJournal,
) (bool, bool, error) {
	if pre != nil {
		defer pre.Close()
	}
	coord := collectionSchemaCoordinatorForDBCollection(m.db, name)
	if coord != nil {
		coord.nativeBulkInsertMu.Lock()
		defer coord.nativeBulkInsertMu.Unlock()
	}
	tx, err := m.beginPrepared(name)
	return finishNativeJSONInsertWithPreimage(tx, err, ids, documents, pre, preElapsed, stats, ordinaryOnly, projection, projected, journal)
}
