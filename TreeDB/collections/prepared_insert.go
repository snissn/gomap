package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

var ErrPreparedInsertIneligible = errors.New("collections: prepared insert ineligible")

const preparedInsertMaxRows = 16 << 10
const preparedInsertMaxDocumentBytes = 128 << 10

// PreparedInsertBatch owns its input slices until Commit or Abandon. The caller
// must not mutate or reuse IDs and documents after handing them to Prepare.
// It owns no durable identity or asset; only Commit may publish the batch.
type PreparedInsertBatch struct {
	collection     *Collection
	meta           CollectionMeta
	ids, documents [][]byte
	entries        []noIndexBatchEntry
	retained       columnRetainedPayloadStorageDocuments
	prepareElapsed time.Duration
	ownedBytes     int64
	commitReserve  int64
	state          atomic.Uint32
}

// Capture only stable schema. Manifest progress grows with committed parts and
// is re-read at publication; copying it into every queued batch would make
// pipeline-owned memory grow with the database rather than the batch.
func preparedInsertSchemaMeta(meta CollectionMeta) CollectionMeta {
	if meta.Options.ColumnStore != nil {
		cfg := *meta.Options.ColumnStore
		cfg.ActiveManifest = nil
		cfg.RecoveryAuthoritativeManifest = nil
		cfg.RecoveryAuthoritativeAppliedCommandLSN = 0
		cfg.PhysicalMutationParts = 0
		meta.Options.ColumnStore = &cfg
	}
	return copyCollectionMeta(meta)
}

func preparedInsertInputBytes(ids, documents [][]byte) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	outerCapacity := int64(cap(ids)) + int64(cap(documents))
	if outerCapacity > maxInt64/int64(unsafe.Sizeof([]byte{})) {
		return maxInt64
	}
	bytes := outerCapacity * int64(unsafe.Sizeof([]byte{}))
	for i := range ids {
		bytes = saturatingAddNonNegativeInt64(bytes, int64(cap(ids[i])))
		bytes = saturatingAddNonNegativeInt64(bytes, int64(cap(documents[i])))
	}
	return bytes
}

// The retained semantic encoder can hold token/path scratch and compressed
// output at the same time. Reserve generous headroom before entering it, then
// charge the actual retained backing after preparation. This is an admission
// bound, not a prediction of the encoded size.
func preparedInsertAdmissionBytes(inputBytes int64, rows, columns int) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	if inputBytes > maxInt64/32 || int64(rows) > maxInt64/256 || columns > 0 && int64(rows) > maxInt64/int64(columns)/256 {
		return maxInt64
	}
	return inputBytes*32 + int64(rows)*int64(columns)*256
}

// Commit retains the prepared input while it clones result IDs, constructs the
// command-WAL payload and root runs, and builds typed scalar batches and string
// dictionaries. This estimate admits a token; it is not a proven peak bound.
func preparedInsertCommitReserveBytes(ownedBytes int64, rows, columns int) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	if ownedBytes > maxInt64/3 || int64(rows) > maxInt64/512 || columns > 0 && int64(rows) > maxInt64/int64(columns)/512 {
		return maxInt64
	}
	return saturatingAddNonNegativeInt64(ownedBytes*3, int64(rows)*int64(columns)*512+8<<20)
}

func preparedInsertBoundedScalarColumns(columns []ColumnStoreColumn) bool {
	for _, column := range columns {
		// Other scalar types take the full-batch declared-row extractor before
		// the bounded semantic cursor runs. Keep them on ordinary InsertBatch.
		if column.Path == "" || !columnDeclaredJSONParserValueSupported(column.ValueType) {
			return false
		}
		switch column.ValueType {
		case ColumnStoreValueInt64, ColumnStoreValueString:
		default:
			return false
		}
	}
	return true
}

func preparedSemanticStreamBackingBytes(table memtable.Table) (int64, error) {
	if table == nil {
		return 0, fmt.Errorf("%w: missing semantic-stream blocks", ErrPreparedInsertIneligible)
	}
	// The semantic-stream builder uses an append-only table with stolen, unique
	// encoded block buffers. Size() reports logical payload, not retained backing.
	appendOnly, ok := table.(*memtable.AppendOnly)
	if !ok {
		return 0, fmt.Errorf("%w: unsupported semantic-stream block table", ErrPreparedInsertIneligible)
	}
	return appendOnly.EntryBackingBytes() + appendOnly.EntryBufferCapacityBytes() + int64(appendOnly.Len())*256, nil // index/map overhead
}

func preparedDeclaredBackingBytes(rows []columnDeclaredRow) int64 {
	owned := int64(cap(rows)) * int64(unsafe.Sizeof(columnDeclaredRow{}))
	for i := range rows {
		owned += int64(cap(rows[i].ID))
		owned += int64(cap(rows[i].Values)) * int64(unsafe.Sizeof(columnDeclaredValue{}))
		for j := range rows[i].Values {
			v := &rows[i].Values[j]
			owned += int64(len(v.String) + cap(v.Float32Vector)*4 + cap(v.DenseNumericVector) +
				cap(v.Uint32List)*4 + cap(v.AdjacencyList)*4 + cap(v.Bytes) + cap(v.StringBytes))
		}
	}
	return owned
}

// PrepareInsertBatchOwned prepares the no-index JSON semantic-stream path.
// maxOwnedBytes limits the retained request and prepared payload. Estimated
// headroom rejects large requests before encoding; a post-encode capacity check
// can also reject a batch. These estimates are not a strict transient heap
// bound. Callers may use ordinary InsertBatch on ineligibility.
func (c *Collection) PrepareInsertBatchOwned(ids, documents [][]byte, maxOwnedBytes int64) (*PreparedInsertBatch, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if len(ids) != len(documents) {
		return nil, errors.New("collections: caller-provided batch ids length mismatch")
	}
	if len(ids) == 0 || maxOwnedBytes <= 0 {
		return nil, fmt.Errorf("%w: empty batch or byte limit", ErrPreparedInsertIneligible)
	}
	if len(ids) > preparedInsertMaxRows {
		return nil, fmt.Errorf("%w: batch has %d rows, maximum %d", ErrPreparedInsertIneligible, len(ids), preparedInsertMaxRows)
	}
	for _, id := range ids {
		if len(id) == 0 {
			return nil, errors.New("collections: document id cannot be empty")
		}
	}
	for _, document := range documents {
		if len(document) > preparedInsertMaxDocumentBytes {
			return nil, fmt.Errorf("%w: document has %d bytes, maximum %d", ErrPreparedInsertIneligible, len(document), preparedInsertMaxDocumentBytes)
		}
	}
	inputBytes := preparedInsertInputBytes(ids, documents)
	if inputBytes > maxOwnedBytes {
		return nil, fmt.Errorf("%w: input bytes %d exceed %d", ErrPreparedInsertIneligible, inputBytes, maxOwnedBytes)
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	if err := c.ensureWriteDomainOpen(); err != nil {
		return nil, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	meta := catalog.meta
	cfg := meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatJSON ||
		len(meta.Indexes) != 0 || len(meta.TextIndexes) != 0 || len(meta.VectorIndexes) != 0 ||
		cfg.RetainedPayload != ColumnRetainedPayloadNonColumn ||
		columnRetainedPayloadEffectiveEncoding(cfg) != ColumnRetainedPayloadEncodingSemanticStreamV1 {
		return nil, fmt.Errorf("%w: requires no-index JSON semantic-stream column store", ErrPreparedInsertIneligible)
	}
	if !preparedInsertBoundedScalarColumns(cfg.Columns) {
		return nil, fmt.Errorf("%w: prepared declared-row cursor supports nonempty Int64/String column paths only", ErrPreparedInsertIneligible)
	}
	if err := c.requireColumnStoreCommandWAL(meta, nil); err != nil {
		return nil, err
	}
	if err := requireColumnStoreWriteOperationSupported(meta, ColumnPublishOperationInsert); err != nil {
		return nil, err
	}
	if preparedInsertAdmissionBytes(inputBytes, len(ids), len(cfg.Columns)) > maxOwnedBytes {
		return nil, fmt.Errorf("%w: input needs semantic preparation headroom within %d bytes", ErrPreparedInsertIneligible, maxOwnedBytes)
	}
	entries := make([]noIndexBatchEntry, len(ids))
	for i := range ids {
		entries[i] = noIndexBatchEntry{id: ids[i], document: documents[i]}
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].id, entries[j].id) < 0 })
	for i := 1; i < len(entries); i++ {
		if bytes.Equal(entries[i-1].id, entries[i].id) {
			return nil, ErrDuplicateDocumentID
		}
	}
	if err := rejectNoIndexBatchDocumentConflicts(snap, catalog, collectionPrimaryRootName(meta.Name), entries); err != nil {
		return nil, err
	}
	orderedIDs, orderedDocs := make([][]byte, len(entries)), make([][]byte, len(entries))
	for i := range entries {
		orderedIDs[i], orderedDocs[i] = entries[i].id, entries[i].document
	}
	start := time.Now()
	retained, err := prepareColumnRetainedSemanticStreamV1StorageDocumentsWithIDsBudget(*cfg, orderedIDs, orderedDocs, maxOwnedBytes)
	if err != nil {
		return nil, err
	}
	// Charge backing capacities, including the semantic block arena. The caller
	// controls the size of the one batch admitted to this preparation path.
	ownedBytes := inputBytes + int64(cap(entries))*int64(unsafe.Sizeof(noIndexBatchEntry{})) +
		int64(cap(orderedIDs)+cap(orderedDocs)+cap(retained.documents))*int64(unsafe.Sizeof([]byte{})) +
		preparedDeclaredBackingBytes(retained.declaredRows)
	for _, document := range retained.documents {
		ownedBytes += int64(cap(document))
	}
	semanticBytes, err := preparedSemanticStreamBackingBytes(retained.semanticStreamBlocks)
	if err != nil {
		if retained.semanticStreamBlocks != nil {
			resetCollectionRunTable(retained.semanticStreamBlocks)
		}
		return nil, err
	}
	ownedBytes += semanticBytes
	commitReserve := preparedInsertCommitReserveBytes(ownedBytes, len(ids), len(cfg.Columns))
	if ownedBytes > maxOwnedBytes || commitReserve > maxOwnedBytes-ownedBytes {
		if retained.semanticStreamBlocks != nil {
			resetCollectionRunTable(retained.semanticStreamBlocks)
		}
		return nil, fmt.Errorf("%w: prepared bytes %d plus commit reserve %d exceed %d", ErrPreparedInsertIneligible, ownedBytes, commitReserve, maxOwnedBytes)
	}
	return &PreparedInsertBatch{collection: c, meta: preparedInsertSchemaMeta(meta), ids: ids, documents: documents,
		entries: entries, retained: retained, prepareElapsed: time.Since(start), ownedBytes: ownedBytes, commitReserve: commitReserve}, nil
}

func (p *PreparedInsertBatch) OwnedBytes() int64 {
	if p == nil {
		return 0
	}
	return p.ownedBytes
}

// ReservedBytes includes the preparation's retained backing and estimated
// commit headroom admitted against maxOwnedBytes. It is not heap telemetry or
// a proven strict transient bound.
func (p *PreparedInsertBatch) ReservedBytes() int64 {
	if p == nil {
		return 0
	}
	return p.ownedBytes + p.commitReserve
}

func (p *PreparedInsertBatch) release() {
	if p.retained.semanticStreamBlocks != nil {
		resetCollectionRunTable(p.retained.semanticStreamBlocks)
	}
	p.ids, p.documents, p.entries, p.retained = nil, nil, nil, columnRetainedPayloadStorageDocuments{}
	p.meta = CollectionMeta{}
	p.collection = nil
}

// Abandon discards uncommitted memory. It never changes durable state.
func (p *PreparedInsertBatch) Abandon() {
	if p != nil && p.state.CompareAndSwap(0, 1) {
		p.release()
	}
}

// Commit consumes the prepared batch once and returns only after the ordinary
// ordered durable insert has acknowledged it.
func (p *PreparedInsertBatch) Commit() ([][]byte, error) {
	if p == nil || !p.state.CompareAndSwap(0, 1) {
		return nil, errors.New("collections: prepared insert already consumed")
	}
	defer p.release()
	c := p.collection
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	resultIDs, err := c.insertBatchWithCommandWALIntentSchemaLocked(p.ids, p.documents, false, nil, nil,
		insertBatchExecutionOptions{returnResultIDs: true, prepared: p})
	if err == nil {
		err = commitAmbiguousError("PreparedInsertBatch vector index maintenance", c.notifyVectorIndexesUpsert(resultIDs))
	}
	return resultIDs, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}
