package collections

import (
	"errors"
	"sync/atomic"
	"time"
	"unsafe"
	"weak"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

type nativeOpenCacheEntry struct {
	schema weak.Pointer[nativeSchemaState]
	meta   CollectionMeta
	coord  *collectionSchemaCoordinator
}

var nativeBulkCapacityDrainHook atomic.Pointer[func()]

func setNativeBulkCapacityDrainHookForTest(fn func()) func() {
	var next *func()
	if fn != nil {
		f := fn
		next = &f
	}
	prev := nativeBulkCapacityDrainHook.Swap(next)
	return func() {
		nativeBulkCapacityDrainHook.Store(prev)
	}
}

func isRetryableBulkCapacityRefusal(err error, journal *nativeOperationJournal) bool {
	if !errors.Is(err, memtable.ErrCOWCapacity) {
		return false
	}
	if errors.Is(err, ErrCommitAmbiguous) || errors.Is(err, backenddb.ErrClosed) || backenddb.CommitPublicationAccepted(err) {
		return false
	}
	if journal != nil && (journal.completion.AssignedLSN() != 0 || journal.completion.Attempted()) {
		return false
	}
	return true
}

// The existing API's native role owns no legacy coordinator, write domain or
// raw barrier. Both public frontends use the same NativeJSONTxn preparation.
func (m *CollectionManager) openNativeCollection(name string) (collection *Collection, resultErr error) {
	if err := ValidateCollectionName(name); err != nil {
		return nil, err
	}
	if m.nativeJSON == nil || m.nativeJSON.db == nil {
		return nil, backenddb.ErrClosed
	}
	capture, err := m.nativeJSON.db.CaptureNativeSemanticState()
	if err != nil {
		return nil, err
	}
	_, state, privateEngine, err := resolveNativeCatalog(capture, name)
	defer func() { resultErr = errors.Join(resultErr, closeNativeCatalog(capture, privateEngine)) }()
	if errors.Is(err, tree.ErrKeyNotFound) {
		m.nativeJSON.openCache.Delete(name)
		return nil, errors.Join(ErrCollectionNotFound, err)
	}
	if err != nil {
		return nil, err
	}
	if domain, ok := state.domains.Get("primary"); ok {
		if _, err = newNativeDomainRead(domain, capture.Snapshot()); err != nil {
			return nil, err
		}
	}
	if cached, ok := m.nativeJSON.openCache.Load(name); ok {
		entry := cached.(*nativeOpenCacheEntry)
		if entry.schema.Value() == state.schema && entry.coord != nil {
			return &Collection{
				manager:                 m,
				nativeJSON:              m.nativeJSON,
				nativeSchemaCoordinator: entry.coord,
				name:                    name,
				meta:                    entry.meta,
			}, nil
		}
	}
	meta := state.schema.meta.copy()
	coord := collectionSchemaCoordinatorForDBCollection(m.nativeJSON.db, name)
	if coord == nil {
		return nil, backenddb.ErrClosed
	}
	if len(meta.VectorIndexes) != 0 {
		coord.hasNativeVectorIndexes.Store(true)
	}
	m.nativeJSON.openCache.Store(name, &nativeOpenCacheEntry{
		schema: weak.Make(state.schema),
		meta:   *meta,
		coord:  coord,
	})
	return &Collection{manager: m, nativeJSON: m.nativeJSON, nativeSchemaCoordinator: coord, name: name, meta: *meta}, nil
}

// InsertBatchValidatedBSONNoResultIDs inserts validated BSON documents without
// cloning response document IDs when the caller discards the returned slice.
func (c *Collection) InsertBatchValidatedBSONNoResultIDs(ids, documents [][]byte) error {
	if c != nil && c.nativeJSON != nil {
		_, err := c.nativeInsertBatchWithJournalTrusted(ids, documents, nil, nil, false, false, true, nil)
		return err
	}
	_, err := c.InsertBatchValidatedBSON(ids, documents)
	return err
}

func (c *Collection) nativeInsertBatch(ids, documents [][]byte, retryProjectedGeneration bool) ([][]byte, error) {
	return c.nativeInsertBatchWithResultIDs(ids, documents, nil, nil, true, retryProjectedGeneration)
}

func (c *Collection) nativeInsertBatchWithStats(ids, documents [][]byte, requested *CollectionInsertStats) ([][]byte, error) {
	return c.nativeInsertBatchWithResultIDs(ids, documents, requested, nil, true, true)
}

func (c *Collection) nativeInsertBatchWithStatsProjection(ids, documents [][]byte, requested *CollectionInsertStats, projection *trustedFloat32Projection) ([][]byte, error) {
	return c.nativeInsertBatchWithResultIDs(ids, documents, requested, projection, true, false)
}

// The transaction owns mutation inputs independently of optional response IDs.
// Omitted-ID gateways share preparation, validation and publication with the
// ordinary API, without allocating a response-only ID vector and byte arena.
func (c *Collection) nativeInsertBatchWithResultIDs(ids, documents [][]byte, requested *CollectionInsertStats, projection *trustedFloat32Projection, returnResultIDs, retryProjectedGeneration bool) ([][]byte, error) {
	return c.nativeInsertBatchWithJournalTrusted(ids, documents, requested, projection, returnResultIDs, retryProjectedGeneration, false, nil)
}

// nativeOperationJournal belongs to this one synchronous operation. It is never
// stored on a Collection or read from a last-result/cache diagnostic.
type nativeOperationJournal struct {
	requested  bool
	completion backenddb.NativeJournalCompletion
}

// selectCapturedBSON authenticates the schema of the actual transaction. The
// completion owner is authenticated independently by CommitNative's bridge.
func (j *nativeOperationJournal) selectCapturedBSON(meta CollectionMeta) {
	if j != nil {
		j.requested = j.requested && normalizedDocumentFormat(meta.Options.DocumentFormat) == DocumentFormatBSON &&
			meta.Options.ColumnStore == nil && len(meta.VectorIndexes) == 0 && len(meta.TextIndexes) == 0
	}
}

func (c *Collection) nativeInsertBatchWithJournal(ids, documents [][]byte, requested *CollectionInsertStats, projection *trustedFloat32Projection, returnResultIDs, retryProjectedGeneration bool, journal *nativeOperationJournal) ([][]byte, error) {
	return c.nativeInsertBatchWithJournalTrusted(ids, documents, requested, projection, returnResultIDs, retryProjectedGeneration, false, journal)
}

func (c *Collection) nativeInsertBatchWithJournalTrusted(ids, documents [][]byte, requested *CollectionInsertStats, projection *trustedFloat32Projection, returnResultIDs, retryProjectedGeneration, trustedBSON bool, journal *nativeOperationJournal) ([][]byte, error) {
	if c.manager.isClosing() {
		return nil, backenddb.ErrClosed
	}
	if len(ids) != len(documents) {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	var stats CollectionInsertStats
	if len(ids) == 0 {
		read, err := c.nativeJSON.Capture(c.name)
		if err != nil {
			return nil, err
		}
		ordinary := read.meta.Options.ColumnStore == nil || nativeOrdinaryColumnGraphMeta(read.meta)
		if projection != nil {
			if !nativeOrdinaryColumnGraphMeta(read.meta) {
				err = backenddb.ErrNativeSemanticUnsupported
			} else {
				err = validateTrustedFloat32ProjectionMeta(read.meta, projection)
			}
			if err != nil {
				return nil, errors.Join(err, read.Close())
			}
		}
		stats.Indexes = len(read.meta.Indexes)
		if err = read.Close(); err != nil {
			return nil, err
		}
		if !ordinary {
			if requested != nil {
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
			return nil, nil
		}
		c.recordInsertBatchStats(stats, insertBatchExecutionOptions{insertStats: requested})
		return nil, nil
	}
	// Returned IDs and diagnostics are caller/handle-owned outputs, like the
	// legacy planner result; neither borrows the transaction's captured schema.
	var result [][]byte
	if returnResultIDs {
		var err error
		result, err = cloneBatchDocumentIDs(ids)
		if err != nil {
			return nil, err
		}
	}
	var refusal *nativeInsertProjectedRefusal
	var observeProjection **nativeInsertProjectedRefusal
	if retryProjectedGeneration {
		observeProjection = &refusal
	}
	var refusedPrepare, refusedPublish time.Duration
	historyExpiryRetries := 0
	capacityRetried := false
	allowBulkPreimage := true
	for attempt := 0; ; {
		stats = CollectionInsertStats{}
		recordStats, retry, capacityRetry, err := c.nativeInsertPreparedOnceWithJournalTrusted(ids, documents, &stats, requested != nil, projection, observeProjection, journal, allowBulkPreimage, trustedBSON)
		// The original history/projection refusal is retryable only before a
		// physical attempt. Preserve the same original owner and bound.
		if journal != nil && (journal.completion.AssignedLSN() != 0 || journal.completion.Attempted()) {
			retry, refusal, capacityRetry = false, nil, false
		}
		if capacityRetry && !capacityRetried {
			// Both unlockVector and the bulk preimage slot have been released.
			// Drain producer pressure outside unlockVector before retrying once
			// with pre == nil so two preimages cannot double-charge COW budget.
			if c.manager.isClosing() {
				return nil, backenddb.ErrClosed
			}
			if hook := nativeBulkCapacityDrainHook.Load(); hook != nil && *hook != nil {
				(*hook)()
			}
			if reliefErr := c.nativeJSON.db.PrepareNativeProducer(); reliefErr != nil {
				return nil, errors.Join(err, reliefErr)
			}
			capacityRetried = true
			allowBulkPreimage = false
			refusedPrepare += stats.PrepareDocuments
			refusedPublish += stats.Publish
			if journal != nil {
				journal.completion = backenddb.NativeJournalCompletion{}
			}
			continue
		}
		if attempt+1 < nativeInsertAttempts && (retry || refusal != nil) {
			if refusal != nil {
				// The first transaction and its capture, lease and producer
				// gate are gone. The maintenance request owns only its stamp.
				if c.manager.isClosing() {
					return nil, backenddb.ErrClosed
				}
				if reliefErr := c.nativeJSON.db.PrepareNativeProjectedGeneration(refusal.pressure); reliefErr != nil {
					return nil, errors.Join(err, reliefErr)
				}
			}
			if retry {
				historyExpiryRetries++
			}
			attempt++
			refusedPrepare += stats.PrepareDocuments
			refusedPublish += stats.Publish
			if journal != nil {
				journal.completion = backenddb.NativeJournalCompletion{}
			}
			continue
		}
		if err != nil {
			if journal != nil && journal.completion.Installed() {
				return result, err
			}
			return nil, err
		}
		if recordStats {
			stats.HistoryExpiryRetries = historyExpiryRetries
			stats.PrepareDocuments += refusedPrepare
			stats.Publish += refusedPublish
			c.recordInsertBatchStats(stats, insertBatchExecutionOptions{insertStats: requested})
		}
		return result, nil
	}
}

// End the original producer admission before fresh maintenance/capture. The
// deferred unlock also covers panic/error paths; no private attempt survives it.
func (c *Collection) nativeInsertPreparedOnce(ids, documents [][]byte, stats *CollectionInsertStats, ordinaryOnly bool, projection *trustedFloat32Projection, projected **nativeInsertProjectedRefusal) (bool, bool, error) {
	recordStats, retry, _, err := c.nativeInsertPreparedOnceWithJournal(ids, documents, stats, ordinaryOnly, projection, projected, nil, true)
	return recordStats, retry, err
}

func (c *Collection) nativeInsertPreparedOnceWithJournal(ids, documents [][]byte, stats *CollectionInsertStats, ordinaryOnly bool, projection *trustedFloat32Projection, projected **nativeInsertProjectedRefusal, journal *nativeOperationJournal, allowBulkPreimage bool) (bool, bool, bool, error) {
	return c.nativeInsertPreparedOnceWithJournalTrusted(ids, documents, stats, ordinaryOnly, projection, projected, journal, allowBulkPreimage, false)
}

func (c *Collection) nativeInsertPreparedOnceWithJournalTrusted(ids, documents [][]byte, stats *CollectionInsertStats, ordinaryOnly bool, projection *trustedFloat32Projection, projected **nativeInsertProjectedRefusal, journal *nativeOperationJournal, allowBulkPreimage, trustedBSON bool) (bool, bool, bool, error) {
	if len(ids) > 1 && projection == nil {
		// Bound concurrent bulk producers before prepareNativeProducerMutation so
		// queued writers neither hold vectorDDLMutationMu while waiting nor carry
		// a stale pre-wait pressure observation across intervening commits.
		coord := c.collectionSchemaCoordinator()
		releaseSlot := coord.acquireBulkPreimageSlot()
		defer releaseSlot()
		unlockVector, _, err := c.prepareNativeProducerMutation()
		if err != nil {
			return false, false, false, err
		}
		defer unlockVector()
		var pre *nativeBulkInsertPreimage
		var preElapsed time.Duration
		if allowBulkPreimage {
			var preStarted time.Time
			if stats != nil {
				preStarted = time.Now()
			}
			var preErr error
			pre, preErr = prepareNativeBulkInsertPreimage(c.nativeJSON, c.name, ids, documents, stats != nil, trustedBSON)
			if preErr != nil {
				preErr = mutationNotAcceptedError(preErr)
				return false, false, isRetryableBulkCapacityRefusal(preErr, journal), preErr
			}
			if stats != nil {
				preElapsed = time.Since(preStarted)
			}
		}
		recordStats, retry, err := c.nativeJSON.commitBulkInsertAttempt(coord, c.name, ids, documents, pre, preElapsed, stats, ordinaryOnly, projection, projected, journal, trustedBSON)
		capacityRetry := err != nil && !retry && (projected == nil || *projected == nil) && isRetryableBulkCapacityRefusal(err, journal)
		return recordStats, retry, capacityRetry, err
	}
	unlockVector, _, err := c.prepareNativeProducerMutation()
	if err != nil {
		return false, false, false, err
	}
	defer unlockVector()
	recordStats, retry, err := c.nativeJSON.insertPreparedTrusted(c.name, ids, documents, stats, ordinaryOnly, projection, projected, journal, trustedBSON)
	return recordStats, retry, false, err
}

func (c *Collection) nativeGetInto(id, dst []byte) ([]byte, bool, error) {
	value, _, found, err := c.nativeGetIntoWithFormat(id, dst)
	return value, found, err
}

func (c *Collection) nativeGetIntoWithFormat(id, dst []byte) ([]byte, DocumentFormat, bool, error) {
	if c.manager.isClosing() {
		return dst[:0], "", false, backenddb.ErrClosed
	}
	if len(id) == 0 {
		return dst[:0], "", false, errors.New("collections: document id cannot be empty")
	}
	value, format, err := c.nativeJSON.getIntoWithFormat(c.name, id, dst)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return dst[:0], format, false, nil
	}
	if err != nil {
		return dst[:0], format, false, err
	}
	return value, format, true, nil
}

func (c *Collection) openNativeReadView() (*CollectionReadView, error) {
	if c.manager.isClosing() {
		return nil, backenddb.ErrClosed
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return nil, err
	}
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(CollectionReadView{}))))
	if err != nil {
		return nil, errors.Join(err, read.Close())
	}
	return &CollectionReadView{collection: c, nativeRead: read, nativeLease: lease}, nil
}

func (v *CollectionReadView) fetchNativeDocuments(ids [][]byte, opts DocumentFetchOptions) (DocumentFetchResponse, error) {
	if v.closed || v.nativeRead == nil {
		return DocumentFetchResponse{}, backenddb.ErrClosed
	}
	// The stored-format and JSON projection contracts are shared with legacy
	// materialization; Get already returns independent caller-owned documents.
	var admitOutput func(uint64) error
	if v.catalog != nil && v.catalog.nativeText != nil {
		admitOutput = v.catalog.nativeText.admit
		if nativeTypedMeta(v.nativeRead.meta) && v.nativeRead.meta.Options.ColumnStore.RetainedPayload != ColumnRetainedPayloadFull {
			return DocumentFetchResponse{}, backenddb.ErrNativeSemanticUnsupported
		}
		if uint64(len(ids)) > ^uint64(0)/uint64(unsafe.Sizeof(DocumentFetchResult{})) {
			return DocumentFetchResponse{}, memtable.ErrCOWCapacity
		}
		if err := admitOutput(memtable.COWAllocationCharge(uint64(len(ids)) * uint64(unsafe.Sizeof(DocumentFetchResult{})))); err != nil {
			return DocumentFetchResponse{}, err
		}
	}
	projection, err := normalizeDocumentFetchProjection(v.nativeRead.meta, opts)
	if err != nil {
		return DocumentFetchResponse{}, err
	}
	out := DocumentFetchResponse{Results: make([]DocumentFetchResult, len(ids))}
	out.Stats.DocumentsRequested = uint64(len(ids))
	for i, id := range ids {
		if opts.Context != nil {
			if err := opts.Context.Err(); err != nil {
				return DocumentFetchResponse{}, err
			}
		}
		if admitOutput != nil {
			if err := admitOutput(memtable.COWAllocationCharge(uint64(len(id)))); err != nil {
				return DocumentFetchResponse{}, err
			}
		}
		value, err := v.nativeRead.getAdmitted(id, admitOutput)
		out.Results[i].ID = append([]byte(nil), id...)
		if errors.Is(err, tree.ErrKeyNotFound) {
			out.Stats.DocumentsMissing++
			continue
		}
		if err != nil {
			return DocumentFetchResponse{}, err
		}
		if projection.active() {
			value, err = projectJSONDocument(value, projection, &out.Stats)
			if err != nil {
				return DocumentFetchResponse{}, err
			}
		}
		out.Results[i].Found = true
		out.Results[i].Document = value
		out.Stats.DocumentsFetched++
		out.Stats.DocumentBytes += uint64(len(value))
		out.Stats.OutputBytes += uint64(len(value))
	}
	return out, nil
}

// The existing public encoder uses the SAME private native transaction. Learned
// IDs become caller state only after original publication succeeds.
func (c *Collection) nativeInsertTemplateBatch(ids, documents [][]byte, encoder *TemplateV1Encoder) (result [][]byte, resultErr error) {
	if c.manager.isClosing() {
		return nil, backenddb.ErrClosed
	}
	tx, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	g := tx.binding(c.name)
	if normalizedDocumentFormat(g.probe.meta.Options.DocumentFormat) != DocumentFormatTemplateV1 {
		return nil, errors.New("collections: InsertBatchWithTemplateV1Encoder requires a template-v1 collection")
	}
	if encoder.learnedTemplateV1ScopeMismatch(c) {
		return nil, errors.New("collections: template-v1 encoder is bound to a different collection")
	}
	if len(ids) != len(documents) {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if len(ids) == 0 {
		return nil, nil
	}
	g.allowStored = encoder.allowsTemplateV1StoredDocuments(c)
	g.learnTemplates = true
	if err = tx.InsertBatch(ids, documents); err != nil {
		return nil, err
	}
	result, err = cloneBatchDocumentIDs(ids)
	if err != nil {
		return nil, err
	}
	// Commit detaches and refunds the stage. Keep only caller-owned scalar
	// hash/ID pairs for teaching the encoder after publication succeeds.
	learned := append([]templateV1LearnedTemplate(nil), g.templateLearned...)
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	encoder.learnTemplateV1Templates(c, learned)
	return result, nil
}
