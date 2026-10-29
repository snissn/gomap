package collections

import (
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

// The existing API's native role owns no legacy coordinator, write domain or
// raw barrier. Both public frontends use the same NativeJSONTxn preparation.
func (m *CollectionManager) openNativeCollection(name string) (collection *Collection, resultErr error) {
	read, err := m.nativeJSON.Capture(name)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, errors.Join(ErrCollectionNotFound, err)
	}
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	meta := read.meta.copy()
	coord := collectionSchemaCoordinatorForDBCollection(m.nativeJSON.db, name)
	if coord == nil {
		return nil, backenddb.ErrClosed
	}
	if len(meta.VectorIndexes) != 0 {
		coord.hasNativeVectorIndexes.Store(true)
	}
	return &Collection{manager: m, nativeJSON: m.nativeJSON, nativeSchemaCoordinator: coord, name: name, meta: *meta}, nil
}

func (c *Collection) nativeInsertBatch(ids, documents [][]byte) ([][]byte, error) {
	return c.nativeInsertBatchWithStats(ids, documents, nil)
}

func (c *Collection) nativeInsertBatchWithStats(ids, documents [][]byte, requested *CollectionInsertStats) ([][]byte, error) {
	return c.nativeInsertBatchWithStatsProjection(ids, documents, requested, nil)
}

func (c *Collection) nativeInsertBatchWithStatsProjection(ids, documents [][]byte, requested *CollectionInsertStats, projection *trustedFloat32Projection) ([][]byte, error) {
	return c.nativeInsertBatchWithResultIDs(ids, documents, requested, projection, true)
}

// The transaction owns mutation inputs independently of optional response IDs.
// Omitted-ID gateways share preparation, validation and publication with the
// ordinary API, without allocating a response-only ID vector and byte arena.
func (c *Collection) nativeInsertBatchWithResultIDs(ids, documents [][]byte, requested *CollectionInsertStats, projection *trustedFloat32Projection, returnResultIDs bool) ([][]byte, error) {
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
	unlockVector, _, err := c.prepareNativeProducerMutation()
	if err != nil {
		return nil, err
	}
	defer unlockVector()
	recordStats, err := c.nativeJSON.insertPrepared(c.name, ids, documents, &stats, requested != nil, projection)
	if err != nil {
		return nil, err
	}

	if recordStats {
		c.recordInsertBatchStats(stats, insertBatchExecutionOptions{insertStats: requested})
	}
	return result, nil
}

func (c *Collection) nativeGetInto(id, dst []byte) ([]byte, bool, error) {
	if c.manager.isClosing() {
		return dst[:0], false, backenddb.ErrClosed
	}
	if len(id) == 0 {
		return dst[:0], false, errors.New("collections: document id cannot be empty")
	}
	value, err := c.nativeJSON.Get(c.name, id)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return dst[:0], false, nil
	}
	if err != nil {
		return dst[:0], false, err
	}
	if dst == nil {
		return value, true, nil
	}
	return append(dst[:0], value...), true, nil
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
