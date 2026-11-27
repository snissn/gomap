package collections

import (
	"bytes"
	"errors"
	"fmt"
	"time"
	"unsafe"
	"weak"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type nativeBulkUniqueCheck struct {
	domainName string
	indexName  string
	valueType  IndexValueType
	startDep   int
	depCount   int
	// entries holds the sorted secondary keys (encodedValue || docID) so
	// conflict checks can compare owners without retaining changed/runtimes.
	entries []directBufferedSecondaryRootEntry
}

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
	dependencies  []txnhistory.Dependency
	payload       []byte
	direct        *directBufferedInsertPlan
	uniqueChecks  []nativeBulkUniqueCheck
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
	if nativeTypedMeta(meta) || len(meta.VectorIndexes) != 0 || len(meta.TextIndexes) != 0 {
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
func prepareNativeBulkInsertPreimage(m *NativeJSONManager, name string, ids, documents [][]byte, wantStats, trustedBSON bool) (pre *nativeBulkInsertPreimage, resultErr error) {
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
	schema := read.collection.schema
	meta := *schema.meta
	incarnation := read.collection.incarnation
	schemaWeak := weak.Make(schema)
	allocator := read.capture.NativeAllocator()
	schema.retain()
	defer func() {
		schema.release().Complete()
	}()
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
		if pre == nil {
			p.Close()
		}
	}()

	// Batch-admit initial arenas, planner headers, sort buffers, and WAL member descriptors.
	n := len(ids)
	fixedHeaderCharge, err := nativeJSONIndexedPlanCharge(meta, nil, nil)
	if err != nil {
		return nil, err
	}
	tempPerRow := uint64(unsafe.Sizeof(insertBatchItem{})) +
		uint64(unsafe.Sizeof(int(0))) +
		uint64(unsafe.Sizeof(preparedBatchUpdate{})) +
		uint64(unsafe.Sizeof(commitlog.CollectionDocument{}))
	retainedPerRow := uint64(unsafe.Sizeof(memtable.COWMutation{})) +
		uint64(unsafe.Sizeof(txnhistory.Effect{})) +
		uint64(unsafe.Sizeof(txnhistory.Dependency{}))
	if uint64(n) > ^uint64(0)/tempPerRow || uint64(n) > ^uint64(0)/retainedPerRow {
		return nil, memtable.ErrCOWCapacity
	}
	tempRowCharge := memtable.COWAllocationCharge(uint64(n) * tempPerRow)
	retainedRowCharge := memtable.COWAllocationCharge(uint64(n) * retainedPerRow)
	if tempRowCharge > ^uint64(0)-retainedRowCharge {
		return nil, memtable.ErrCOWCapacity
	}
	initialCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeBulkInsertPreimage{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeJSONLogicalInsert{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(txnhistory.Dependency{}))+uint64(len(name))) +
		fixedHeaderCharge +
		tempRowCharge + retainedRowCharge +
		memtable.COWAllocationCharge(totalIDBytes) +
		memtable.COWAllocationCharge(totalDocBytes)
	if err = p.admit(initialCharge); err != nil {
		return nil, err
	}
	tempCharge := tempRowCharge
	admitTemp := func(bytes uint64) error {
		if err := p.admit(bytes); err != nil {
			return err
		}
		tempCharge += bytes
		return nil
	}

	planner, err := nativeJSONInsertPlannerForSchema(schema, meta)
	if err != nil {
		return nil, err
	}
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		return nil, err
	}
	plannerOpts := planner.options
	if trustedBSON && isBSON {
		plannerOpts.trustedBSONDocuments = true
	}
	normalized, _, _, _, err := prepareInsertDocuments(documents, plannerOpts)
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
		var parserScratchCharge uint64
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
			// Admit one-document parser/extraction work scratch (matching
			// prepareValidation) so dotted-path or malformed/array BSON
			// traversal cannot allocate beyond COW capacity.
			maxDepth := uint64(1)
			for _, rt := range runtimes {
				if uint64(len(rt.path)) > maxDepth {
					maxDepth = uint64(len(rt.path))
				}
			}
			sourceUpper := uint64(max(maxDocLen, scratchCap))
			if sourceUpper > ^uint64(0)/128 {
				return nil, memtable.ErrCOWCapacity
			}
			decoderAllowance := memtable.COWAllocationCharge(sourceUpper * 128)
			refUnit := 4 * uint64(unsafe.Sizeof(bson.RawValue{}))
			if uint64(maxDocLen) > ^uint64(0)/maxDepth || uint64(maxDocLen)*maxDepth > ^uint64(0)/refUnit {
				return nil, memtable.ErrCOWCapacity
			}
			refAllowance := memtable.COWAllocationCharge(uint64(maxDocLen) * maxDepth * refUnit)
			if refAllowance > ^uint64(0)-decoderAllowance {
				return nil, memtable.ErrCOWCapacity
			}
			parserScratchCharge = decoderAllowance + refAllowance
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
				docCharge, chargeErr := nativeJSONIndexedCharge(meta, [][]byte{nil}, [][]byte{doc}, false, true)
				if chargeErr != nil {
					return nil, chargeErr
				}
				if docCharge > parserScratchCharge {
					parserScratchCharge = docCharge
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
		if err = p.admit(parserScratchCharge); err != nil {
			if simpleBSON && errors.Is(err, memtable.ErrCOWCapacity) {
				// Conservative O(1) upper bound exceeded available capacity; fall
				// back to the standard per-document prepareValidation path so
				// large scalar-field documents with small indexed values still
				// succeed while oversized non-scalar arrays are rejected before
				// allocation.
				return nil, nil
			}
			return nil, err
		}
		tempCharge += parserScratchCharge
		for _, alloc := range []struct {
			count int
			size  uint64
		}{
			{initialBufCap, 1},
			{scratchCap, 1},
			{stateCount, uint64(unsafe.Sizeof([][]byte{}))},
			{stateCount, uint64(unsafe.Sizeof([]byte{}))},
		} {
			if err = admitNativeInsertArray(alloc.count, alloc.size, admitTemp); err != nil {
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
					if err = admitTemp(memtable.COWAllocationCharge(uint64(chunkCap))); err != nil {
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

	planCharge, stateEntries, roots, err := nativeJSONFinalPlanCharge(name, planner.options, runtimes, changed)
	if err != nil {
		return nil, err
	}
	if err = p.admit(planCharge); err != nil {
		return nil, err
	}
	direct, err := buildNativeCRUDFinalPlanWithRootNames(name, planner.primaryRoot, planner.indexStateRoot, planner.options, runtimes, changed, stateEntries, roots)
	if err != nil {
		return nil, err
	}

	// Validate intra-batch unique conflicts directly over the already-sorted
	// secondary runs (zero map allocations) and compute the off-gate unique
	// range dependency backing, caching per-row prefix/end lengths so the
	// dependency-building loop avoids a second indexKeyValuePrefixAndDocumentID pass.
	var (
		uniqueDepCount int
		uniqueEndBytes uint64
		uniqueRuns     int
		totalUniqueCap int
	)
	for _, run := range direct.secondaryRootPlans {
		rt := runtimes[run.runtimeIdx]
		if rt.def.unique && len(run.entries) > 0 {
			uniqueRuns++
			if len(run.entries) > int(^uint(0)>>1)-totalUniqueCap {
				return nil, memtable.ErrCOWCapacity
			}
			totalUniqueCap += len(run.entries)
		}
	}
	var uniquePrefixLens, uniqueEndLens []uint16
	if totalUniqueCap > 0 {
		if err = admitNativeInsertArray(totalUniqueCap, 2*uint64(unsafe.Sizeof(uint16(0))), admitTemp); err != nil {
			return nil, err
		}
		uniquePrefixLens = make([]uint16, 0, totalUniqueCap)
		uniqueEndLens = make([]uint16, 0, totalUniqueCap)
		for _, run := range direct.secondaryRootPlans {
			rt := runtimes[run.runtimeIdx]
			if !rt.def.unique || len(run.entries) == 0 {
				continue
			}
			var prevPrefix, prevRowKey []byte
			for i, row := range run.entries {
				var pLen int
				var keyErr error
				if rt.def.valueType == IndexValueBSONOrderedV2 {
					pLen, keyErr = bsonIndexKeyValuePrefixLengthV2(row.key)
				} else {
					pLen, keyErr = indexComponentLength(rt.def.valueType, row.key)
				}
				if keyErr != nil {
					return nil, keyErr
				}
				if len(row.key) == pLen {
					return nil, errors.New("collections: malformed secondary index key: missing document id")
				}
				if pLen > 65535 {
					return nil, errors.New("collections: index key too large")
				}
				prefix := row.key[:pLen:pLen]
				if i > 0 && bytes.Equal(prefix, prevPrefix) && !bytes.Equal(row.key[pLen:], prevRowKey[len(prevPrefix):]) {
					prevDocID, err := indexKeyDocumentID(rt.def.valueType, prevRowKey)
					if err != nil {
						return nil, err
					}
					docID, err := indexKeyDocumentID(rt.def.valueType, row.key)
					if err != nil {
						return nil, err
					}
					if !bytes.Equal(docID, prevDocID) {
						return nil, fmt.Errorf("%w %q: document ids %q and %q", ErrUniqueIndexConflict, rt.def.name, prevDocID, docID)
					}
				}
				prevPrefix, prevRowKey = prefix, row.key
				if uniqueDepCount == int(^uint(0)>>1) {
					return nil, memtable.ErrCOWCapacity
				}
				uniqueDepCount++
				endLen := uniquePrefixEndLen(prefix)
				if uint64(endLen) > ^uint64(0)-uniqueEndBytes {
					return nil, memtable.ErrCOWCapacity
				}
				uniqueEndBytes += uint64(endLen)
				uniquePrefixLens = append(uniquePrefixLens, uint16(pLen))
				uniqueEndLens = append(uniqueEndLens, uint16(endLen))
			}
		}
	}

	primary := make([]memtable.COWMutation, len(direct.primaryEntries))
	additional, effects, err := extractNativeInsertEffects(name, incarnation, direct, primary, p.admit, meta)
	if err != nil {
		return nil, err
	}

	if uniqueDepCount > 0 {
		if err = admitNativeInsertArray(uniqueDepCount, uint64(unsafe.Sizeof(txnhistory.Dependency{})), p.admit); err != nil {
			return nil, err
		}
		if err = admitNativeInsertArray(uniqueRuns, uint64(unsafe.Sizeof(nativeBulkUniqueCheck{})), p.admit); err != nil {
			return nil, err
		}
		if uniqueEndBytes > 0 {
			if err = p.admit(memtable.COWAllocationCharge(uniqueEndBytes)); err != nil {
				return nil, err
			}
		}
	}

	dependencies := make([]txnhistory.Dependency, 1+len(primary)+uniqueDepCount)
	dependencies[0] = txnhistory.Dependency{
		Namespace: txnhistory.Namespace{Kind: txnhistory.CatalogNamespace},
		Kind:      txnhistory.PointEffect,
		Key:       []byte(name),
	}
	primaryNS := txnhistory.Namespace{
		Kind:           txnhistory.CollectionNamespace,
		Collection:     incarnation,
		Domain:         1,
		Interpretation: 1,
	}
	for i := range primary {
		dependencies[1+i] = txnhistory.Dependency{
			Namespace: primaryNS,
			Kind:      txnhistory.PointEffect,
			Key:       primary[i].Key,
		}
	}
	var uniqueChecks []nativeBulkUniqueCheck
	if uniqueDepCount > 0 {
		uniqueChecks = make([]nativeBulkUniqueCheck, 0, uniqueRuns)
		uniqueEndArena := make([]byte, 0, uniqueEndBytes)
		depOff := 1 + len(primary)
		uIdx := 0
		for _, run := range direct.secondaryRootPlans {
			rt := runtimes[run.runtimeIdx]
			if !rt.def.unique || len(run.entries) == 0 {
				continue
			}
			ns := txnhistory.Namespace{
				Kind:           txnhistory.CollectionNamespace,
				Collection:     incarnation,
				Domain:         nativeJSONSecondaryDomainNumber(meta, run.runtimeIdx),
				Interpretation: 1,
			}
			startDep := depOff
			for _, row := range run.entries {
				pLen := int(uniquePrefixLens[uIdx])
				endLen := int(uniqueEndLens[uIdx])
				uIdx++
				prefix := row.key[:pLen:pLen]
				var end []byte
				if endLen > 0 {
					eStart := len(uniqueEndArena)
					uniqueEndArena = append(uniqueEndArena, prefix[:endLen]...)
					uniqueEndArena[len(uniqueEndArena)-1]++
					end = uniqueEndArena[eStart:len(uniqueEndArena):len(uniqueEndArena)]
				}
				dependencies[depOff] = txnhistory.Dependency{
					Namespace: ns,
					Kind:      txnhistory.RangeEffect,
					Key:       prefix,
					End:       end,
				}
				depOff++
			}
			uniqueChecks = append(uniqueChecks, nativeBulkUniqueCheck{
				domainName: "index/" + rt.def.name,
				indexName:  rt.def.name,
				valueType:  rt.def.valueType,
				startDep:   startDep,
				depCount:   len(run.entries),
				entries:    run.entries,
			})
		}
		uniquePrefixLens = nil
		uniqueEndLens = nil
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

	// Release dead temporary scratch (items, order, changed, docs, parser
	// scratch, encoder arena) before returning so concurrent bulk preimages
	// retain only live admitted bytes while waiting on coord.nativeBulkInsertMu.
	items = nil
	order = nil
	changed = nil
	docs = nil
	if tempCharge > 0 && tempCharge <= p.charge {
		nextCharge := p.charge - tempCharge
		if resizeErr := p.lease.Resize(nextCharge); resizeErr == nil {
			p.charge = nextCharge
		}
	}

	p.primaryPolicy = nativeInsertRootPolicy(direct, direct.primaryRootName)
	p.primary = primary
	p.additional = additional
	p.effects = effects
	p.dependencies = dependencies[:len(dependencies):len(dependencies)]
	p.payload = payload
	p.uniqueChecks = uniqueChecks
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
		var pendingMin, pendingMax string
		var hasPendingMin, hasPendingMax bool
		if pending != nil && pending.Len() > 0 {
			pendingMax, hasPendingMax = pending.MaxKey()
			pendingMin, hasPendingMin = pending.MinKey()
		}
		allDisjointPending := pending == nil || pending.Len() == 0 ||
			(hasPendingMax && bytes.Compare(pre.primary[0].Key, nativeBorrowedStringBytes(pendingMax)) > 0) ||
			(hasPendingMin && bytes.Compare(pre.primary[n-1].Key, nativeBorrowedStringBytes(pendingMin)) < 0)
		if !hasBase && captured == nil && allDisjointPending {
			// Fast path: all sorted batch IDs lie strictly outside [pendingMin, pendingMax]
			// and there are no captured or base physical layers.
		} else if !hasBase && captured == nil && pending != nil && pending.Len() > 0 {
			// Gap-jumping sorted SeekGE over pending: disjoint contiguous ranges
			// (common under multi-producer bulk insert) complete in O(log N) seeks
			// instead of N root-to-leaf Get calls.
			lastKey := pre.primary[n-1].Key
			for i := 0; i < n; {
				row, ok := pending.SeekGE(pre.primary[i].Key, nil)
				if !ok {
					break
				}
				rowKey := nativeBorrowedStringBytes(row.Key)
				cmp := bytes.Compare(rowKey, pre.primary[i].Key)
				if cmp == 0 {
					if row.Flags&node.FlagTombstone == 0 {
						return true, ErrDocumentExists
					}
					i++
					continue
				}
				if bytes.Compare(rowKey, lastKey) > 0 {
					break
				}
				// Advance i to the first batch key >= rowKey.
				lo, hi := i+1, n
				for lo < hi {
					mid := lo + (hi-lo)/2
					if bytes.Compare(pre.primary[mid].Key, rowKey) < 0 {
						lo = mid + 1
					} else {
						hi = mid
					}
				}
				i = lo
			}
		} else {
			var physical backenddb.NativePhysicalReader
			if hasBase {
				physical, _ = g.probe.capture.(backenddb.NativePhysicalReader)
			}
			for i := 0; i < n; i++ {
				id := pre.primary[i].Key
				if pending != nil && pending.Len() > 0 &&
					(!hasPendingMax || bytes.Compare(id, nativeBorrowedStringBytes(pendingMax)) <= 0) &&
					(!hasPendingMin || bytes.Compare(id, nativeBorrowedStringBytes(pendingMin)) >= 0) {
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

	// Reuse off-gate prebuilt primary + unique dependencies when the transaction
	// holds only the initial catalog dependency (avoiding COWBudget.mu and slice
	// allocation inside coord.nativeBulkInsertMu). pre.Close runs after tx.Close.
	if len(pre.dependencies) >= 1+n &&
		len(t.dependencies) == 1 &&
		t.dependencies[0].Namespace.Kind == txnhistory.CatalogNamespace &&
		t.dependencies[0].Kind == txnhistory.PointEffect &&
		bytes.Equal(t.dependencies[0].Key, pre.dependencies[0].Key) {
		t.dependencies = pre.dependencies
	} else {
		// Batch-admit all primary + unique dependencies in one Resize instead of N calls.
		extraDeps := pre.dependencies[1:]
		depSize := uint64(unsafe.Sizeof(txnhistory.Dependency{}))
		needCap := len(t.dependencies) + len(extraDeps)
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
		t.dependencies = append(t.dependencies, extraDeps...)
	}

	if len(pre.uniqueChecks) > 0 {
		if err := t.checkNativeBulkUniquePrechecks(g, pre.uniqueChecks, pre.dependencies); err != nil {
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

func (t *NativeJSONTxn) checkNativeBulkUniquePrechecks(g *nativeJSONTxnBinding, checks []nativeBulkUniqueCheck, deps []txnhistory.Dependency) error {
	var cursor [page.PageSize + 1]byte
	for _, u := range checks {
		domain, hasDomain := g.probe.collection.domains.Get(u.domainName)
		if !hasDomain || (domain.baseRoot == 0 && domain.captured == nil && (domain.pending == nil || domain.pending.Len() == 0)) {
			continue
		}
		onlyPending := domain.baseRoot == 0 && domain.captured == nil && domain.pending != nil
		if onlyPending {
			pending := domain.pending
			if maxStr, hasPendingMax := pending.MaxKey(); hasPendingMax {
				if bytes.Compare(deps[u.startDep].Key, nativeBorrowedStringBytes(maxStr)) > 0 {
					continue
				}
			}
			lastDep := deps[u.startDep+u.depCount-1]
			if minStr, hasPendingMin := pending.MinKey(); hasPendingMin && lastDep.End != nil {
				if bytes.Compare(lastDep.End, nativeBorrowedStringBytes(minStr)) <= 0 {
					continue
				}
			}
			// Gap-jumping sorted SeekGE over pending: because unique prefixes
			// are self-delimiting and strictly increasing, [dep.Key, dep.End)
			// intervals are strictly disjoint and ordered. When pending.SeekGE
			// lands at rowKey >= dep.End, every batch dep with End <= rowKey is
			// provably empty in O(log N) seeks instead of N seeks.
			for i := 0; i < u.depCount; {
				dep := deps[u.startDep+i]
				row, ok := pending.SeekGE(dep.Key, nil)
				if !ok {
					break
				}
				rowKey := nativeBorrowedStringBytes(row.Key)
				if dep.End == nil || bytes.Compare(rowKey, dep.End) < 0 {
					var docID []byte
					start := dep.Key
					for {
						if row.Flags&node.FlagTombstone == 0 {
							if docID == nil {
								var err error
								docID, err = indexKeyDocumentID(u.valueType, u.entries[i].key)
								if err != nil {
									return err
								}
							}
							owner, err := indexKeyDocumentID(u.valueType, rowKey)
							if err != nil {
								return err
							}
							if !bytes.Equal(owner, docID) {
								return fmt.Errorf("%w %q", ErrUniqueIndexConflict, u.indexName)
							}
						}
						if len(rowKey) >= len(cursor) {
							return memtable.ErrCOWCapacity
						}
						n := copy(cursor[:], rowKey)
						cursor[n] = 0
						start = cursor[:n+1]
						row, ok = pending.SeekGE(start, dep.End)
						if !ok {
							break
						}
						rowKey = nativeBorrowedStringBytes(row.Key)
					}
					i++
					continue
				}
				if lastDep.End != nil && bytes.Compare(rowKey, lastDep.End) >= 0 {
					break
				}
				lo, hi := i+1, u.depCount
				for lo < hi {
					mid := lo + (hi-lo)/2
					midEnd := deps[u.startDep+mid].End
					if midEnd != nil && bytes.Compare(midEnd, rowKey) <= 0 {
						lo = mid + 1
					} else {
						hi = mid
					}
				}
				i = lo
			}
			continue
		}
		read, err := newNativeDomainRead(domain, t.core.Snapshot())
		if err != nil {
			return err
		}
		for i := 0; i < u.depCount; i++ {
			dep := deps[u.startDep+i]
			start := dep.Key
			end := dep.End
			var docID []byte
			for {
				entry, found, err := nativeJSONSeekLive(&read, &g.probe, start, end, cursor[:])
				if err != nil {
					return err
				}
				if !found {
					break
				}
				if docID == nil {
					docID, err = indexKeyDocumentID(u.valueType, u.entries[i].key)
					if err != nil {
						return err
					}
				}
				owner, err := indexKeyDocumentID(u.valueType, entry.Key)
				if err != nil {
					return err
				}
				if !bytes.Equal(owner, docID) {
					return fmt.Errorf("%w %q", ErrUniqueIndexConflict, u.indexName)
				}
				if len(entry.Key) >= len(cursor) {
					return memtable.ErrCOWCapacity
				}
				n := copy(cursor[:], entry.Key)
				cursor[n] = 0
				start = cursor[:n+1]
			}
		}
	}
	return nil
}

func (m *NativeJSONManager) insertBulkBatch(name string, ids, documents [][]byte) error {
	coord := collectionSchemaCoordinatorForDBCollection(m.db, name)
	capacityRetried := false
	allowBulkPreimage := true
	for attempt := 0; ; {
		retry, capacityRetry, err := m.insertBulkBatchOnce(coord, name, ids, documents, allowBulkPreimage)
		if capacityRetry && !capacityRetried {
			if reliefErr := m.db.PrepareNativeProducer(); reliefErr != nil {
				return errors.Join(err, reliefErr)
			}
			capacityRetried = true
			allowBulkPreimage = false
			continue
		}
		if retry && attempt+1 < nativeInsertAttempts {
			attempt++
			continue
		}
		return err
	}
}

func (m *NativeJSONManager) insertBulkBatchOnce(coord *collectionSchemaCoordinator, name string, ids, documents [][]byte, allowBulkPreimage bool) (retry, capacityRetry bool, err error) {
	releaseSlot := coord.acquireBulkPreimageSlot()
	defer releaseSlot()
	if err := m.db.PrepareNativeProducer(); err != nil {
		return false, false, err
	}
	var pre *nativeBulkInsertPreimage
	if allowBulkPreimage {
		var preErr error
		pre, preErr = prepareNativeBulkInsertPreimage(m, name, ids, documents, false, false)
		if preErr != nil {
			preErr = mutationNotAcceptedError(preErr)
			return false, isRetryableBulkCapacityRefusal(preErr, nil), preErr
		}
	}
	_, retry, err = m.commitBulkInsertAttempt(coord, name, ids, documents, pre, 0, nil, false, nil, nil, nil, false)
	if err != nil && !retry && isRetryableBulkCapacityRefusal(err, nil) {
		capacityRetry = true
	}
	return retry, capacityRetry, err
}

func (m *NativeJSONManager) commitBulkInsertAttempt(
	coord *collectionSchemaCoordinator,
	name string,
	ids, documents [][]byte,
	pre *nativeBulkInsertPreimage,
	preElapsed time.Duration,
	stats *CollectionInsertStats,
	ordinaryOnly bool,
	projection *trustedFloat32Projection,
	projected **nativeInsertProjectedRefusal,
	journal *nativeOperationJournal,
	trustedBSON bool,
) (bool, bool, error) {
	// Register pre.Close BEFORE acquiring coord.nativeBulkInsertMu so LIFO
	// defer order releases nativeBulkInsertMu first, then releases pre's
	// admitted COW lease, and only after commitBulkInsertAttempt returns does
	// the caller's deferred releaseSlot() return the bulk preimage concurrency
	// token.
	if pre != nil {
		defer pre.Close()
	}
	if coord == nil {
		coord = collectionSchemaCoordinatorForDBCollection(m.db, name)
	}
	if coord != nil {
		coord.nativeBulkInsertMu.Lock()
		locked := true
		defer func() {
			if locked {
				coord.nativeBulkInsertMu.Unlock()
			}
		}()
		tx, err := m.beginPreparedPooled(name)
		if err == nil && tx != nil && m.db != nil && m.db.NativeCommandWALGroupCommitActive() {
			tx.beforeDurableWait = func() {
				if locked {
					locked = false
					coord.nativeBulkInsertMu.Unlock()
				}
				// Logical COW publication and WAL frame append have already
				// copied pre's payloads. Release pre's COW budget lease before
				// blocking on durable fdatasync so concurrent group-commit
				// waves do not accumulate idle preimage reservations.
				if pre != nil {
					pre.Close()
				}
			}
		}
		return finishNativeJSONInsertWithPreimage(tx, err, ids, documents, pre, preElapsed, stats, ordinaryOnly, projection, projected, journal, trustedBSON)
	}
	tx, err := m.beginPreparedPooled(name)
	return finishNativeJSONInsertWithPreimage(tx, err, ids, documents, pre, preElapsed, stats, ordinaryOnly, projection, projected, journal, trustedBSON)
}
