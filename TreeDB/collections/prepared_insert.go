package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

var ErrPreparedInsertIneligible = errors.New("collections: prepared insert ineligible")

// ErrPreparedInsertResourceLimit identifies a size or structural limit. It is
// distinct from ineligibility so callers cannot accidentally retry a rejected
// batch through ordinary InsertBatch.
var ErrPreparedInsertResourceLimit = errors.New("collections: prepared insert resource limit")

const preparedInsertMaxRows = 16 << 10
const preparedInsertMaxDocumentBytes = 128 << 10

// A prepared primary key must fit both a leaf entry and two separators in a
// 4 KiB internal page. The latter gives cold publication a finite page-output
// bound before the command WAL frame is appended.
const preparedInsertMaxIDBytes = 1024
const preparedInsertMaxScalarColumns = 5

// Catalog validation permits longer paths and asset namespaces. Bound their
// retained schema references before copying the prepared token's metadata.
const preparedInsertMaxSchemaPathBytes = 1024
const preparedInsertMaxAssetNamespaceBytes = 512
const preparedInsertMaxRootDescriptors = 4096
const preparedInsertMaxRootDescriptorBytes = 1 << 20
const preparedInsertMaxDescriptorRootIDs = 4096
const preparedInsertMaxManifestRecords = 4096
const preparedInsertMaxManifestBytes = 1 << 20
const preparedInsertMaxAggregateMetadata = 3
const preparedInsertMaxAggregatePredicates = 3
const preparedInsertMaxAggregatePredicateValues = 4
const preparedInsertMaxAggregateConfigBytes = 8 << 10

// The stable schema copy allocates only bounded column/sort/aggregate slices
// and optional config structs; catalog strings remain shared immutable data.
const preparedInsertSchemaCopyReserveBytes = 32 << 10

// PreparedInsertBatch owns its input slices until Commit or Abandon. The caller
// must not mutate or reuse IDs and documents after handing them to Prepare.
// For byte accounting, callers must transfer complete, non-aliased backing
// allocations: cap must describe each whole underlying ID/document allocation,
// and unused outer-slice slots must not retain other buffers. A full-sliced
// view into a larger allocation does not meet that ownership contract.
// It owns no durable identity or asset; only Commit may publish the batch.
type PreparedInsertBatch struct {
	collection         *Collection
	meta               CollectionMeta
	ids, documents     [][]byte
	entries            []noIndexBatchEntry
	retained           columnRetainedPayloadStorageDocuments
	typedBatch         *typedColumnAdapterPreparedBatch
	prepareElapsed     time.Duration
	ownedBytes         int64
	commitReserve      int64
	materializeReserve int64
	state              atomic.Uint32
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

func preparedInsertUnusedOuterSlotsEmpty(ids, documents [][]byte) bool {
	for _, id := range ids[len(ids):cap(ids)] {
		if id != nil {
			return false
		}
	}
	for _, document := range documents[len(documents):cap(documents)] {
		if document != nil {
			return false
		}
	}
	return true
}

// The retained semantic encoder can hold token/path scratch and compressed
// output at the same time. Reserve generous headroom before entering it, then
// charge the actual retained backing after preparation. This is an admission
// bound, not a prediction of the encoded size.
// The eligible typed prebuild borrows declared strings. Its distinct string
// count is at most rows * columns. Four KiB per distinct value covers the
// temporary and final dictionary maps (including old/new map growth), value
// slices, sorted copy, and recode vector. The per-cell term covers fixed batch
// vectors and nullable bitmaps; the fixed term covers schema/maps. This credit
// is held only during preparation, before the token is returned.
func preparedTypedScalarPrepareReserve(rows, columns int) int64 {
	cells := preparedInsertMul(int64(rows), int64(columns))
	return saturatingAddNonNegativeInt64(preparedInsertMul(4224, cells), 8<<20)
}

// These disjoint allowances stay live together during an ordered commit:
// result IDs, command WAL, primary/semantic runs and pointerization; ordered
// root materialization; typed part and compressed image; row asset; fused row
// sidecars; and bounded catalog/manifest cloning. The prepared backing is
// charged separately. N <= 16K, C <= 5, and the row/column image codecs are
// restricted by preparation eligibility. The caller adds the separate fixed
// publisher allowance before it returns the prepared token.
type preparedInsertCommitBudget struct {
	materialization int64
	total           int64
}

func preparedInsertCommitReserveBytes(publishedBytes, keyBytes, typedDictionaryStringBytes, rowAssetStringBytes, aggregateGroupBytes int64, rows, columns, typedColumns, granules int) preparedInsertCommitBudget {
	n := int64(rows)
	c := int64(columns)
	tc := int64(typedColumns)
	g := int64(granules)
	// The two root allowances are separate: passing the whole commit credit to
	// the materializer would let it consume credit needed by the asset builders.
	materialization := preparedInsertMul(3, publishedBytes)
	materialization = saturatingAddNonNegativeInt64(materialization, preparedInsertMul(1024, n))
	materialization = saturatingAddNonNegativeInt64(materialization, 8<<20)
	// The command payload can coexist with old and replacement V2 frame
	// scratch (three source-sized copies). Result IDs, pointerized run keys and
	// inline values can add another three source-sized copies. The per-row
	// allowance covers both pooled AppendOnly entry arrays, pointerization
	// headers/ref slices, command-document headers and sort metadata.
	walAndRuns := preparedInsertMul(6, publishedBytes)
	walAndRuns = saturatingAddNonNegativeInt64(walAndRuns, preparedInsertMul(2048, n))
	walAndRuns = saturatingAddNonNegativeInt64(walAndRuns, 8<<20)
	// BuildColumnPart retains coded blocks, locators, marks and dictionaries;
	// BuildColumnPartImage retains sections, a final image, and ZSTD workspace.
	typed := int64(0)
	if typedColumns > 0 {
		typed = preparedInsertMul(12, typedDictionaryStringBytes)
		typed = saturatingAddNonNegativeInt64(typed, preparedInsertMul(1024, preparedInsertMul(n, tc)))
		typed = saturatingAddNonNegativeInt64(typed, preparedInsertMul(4096, preparedInsertMul(g, tc+1)))
		typed = saturatingAddNonNegativeInt64(typed, 64<<20)
	}
	// bytes.Buffer may hold old and replacement backing while encoding the
	// row asset; scalar strings borrow the prepared declared-row backing.
	// A row-owned string is serialized once per row. Distinct interned string
	// backing can be much smaller when many rows repeat the same value.
	rowAsset := preparedInsertMul(3, saturatingAddNonNegativeInt64(keyBytes, rowAssetStringBytes))
	rowAsset = saturatingAddNonNegativeInt64(rowAsset, preparedInsertMul(384, preparedInsertMul(n, c)))
	rowAsset = saturatingAddNonNegativeInt64(rowAsset, 1<<20)
	// Dictionary-code, int64, and at most three aggregate sidecar builders and
	// their encoded assets coexist with the row and typed outputs.
	sidecars := preparedInsertMul(1024, preparedInsertMul(n, c+3))
	// Aggregate entry sets can serialize a group again in each granule. Charge
	// the per-row source lengths across all specs and row-owned dictionaries.
	sidecarStringBytes := saturatingAddNonNegativeInt64(rowAssetStringBytes, aggregateGroupBytes)
	sidecars = saturatingAddNonNegativeInt64(sidecars, preparedInsertMul(6, sidecarStringBytes))
	sidecars = saturatingAddNonNegativeInt64(sidecars, 16<<20)
	// Pre-WAL gates cap the existing manifest and root-descriptor inputs at
	// 4096 records and 1 MiB inline bytes each.
	catalog := int64(16 << 20)
	total := materialization
	for _, allowance := range []int64{walAndRuns, typed, rowAsset, sidecars, catalog} {
		total = saturatingAddNonNegativeInt64(total, allowance)
	}
	return preparedInsertCommitBudget{materialization: materialization, total: total}
}

// A string interned once in declared rows can appear in several column
// dictionaries. The image serializes each column dictionary independently, so
// charge per-column dictionary keys before Commit rather than the unique
// interned backing size.
func preparedTypedDictionaryStringBytes(prepared *typedColumnAdapterPreparedBatch) int64 {
	if prepared == nil {
		return 0
	}
	var total int64
	for _, column := range prepared.Columns {
		for value := range column.Dictionary {
			total = saturatingAddNonNegativeInt64(total, int64(len(value)))
		}
	}
	return total
}

// These source totals are read from the prepared declared rows without cloning
// their strings. Each aggregate entry represents at least one source row, so
// the sum of all encoded group-string lengths is no greater than the per-row
// total for that aggregate, including typed-granule entry sets.
func preparedInsertRepeatedStringBytes(cfg ColumnStoreConfig, rows []columnDeclaredRow) (rowAsset, aggregateGroups int64) {
	for _, row := range rows {
		if row.Deleted || len(row.Values) != len(cfg.Columns) {
			continue
		}
		for colIdx, col := range cfg.Columns {
			value := row.Values[colIdx]
			if col.ValueType != ColumnStoreValueString || !value.Present || value.Null {
				continue
			}
			if columnStoreColumnIsTypedRowAsset(col) {
				rowAsset = saturatingAddNonNegativeInt64(rowAsset, int64(len(value.String)+len(value.StringBytes)))
			}
			for _, aggregate := range cfg.AggregateMetadata {
				if aggregate.GroupColumn == col.Name {
					aggregateGroups = saturatingAddNonNegativeInt64(aggregateGroups, int64(len(value.String)+len(value.StringBytes)))
				}
			}
		}
	}
	return rowAsset, aggregateGroups
}

func preparedInsertMul(a, b int64) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	if a < 0 || b < 0 || b != 0 && a > maxInt64/b {
		return maxInt64
	}
	return a * b
}

func preparedInsertBoundedScalarColumns(columns []ColumnStoreColumn) bool {
	if len(columns) == 0 || len(columns) > preparedInsertMaxScalarColumns {
		return false
	}
	for _, column := range columns {
		// Other scalar types take the full-batch declared-row extractor before
		// the bounded semantic cursor runs. Keep them on ordinary InsertBatch.
		if column.Path == "" || len(column.Path) > preparedInsertMaxSchemaPathBytes ||
			!columnDeclaredJSONParserValueSupported(column.ValueType) {
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

func preparedInsertBoundedAggregateMetadata(cfg ColumnStoreConfig) bool {
	if len(cfg.AggregateMetadata) > preparedInsertMaxAggregateMetadata {
		return false
	}
	configBytes := 0
	for _, aggregate := range cfg.AggregateMetadata {
		switch aggregate.Kind {
		case ColumnAggregateCount, ColumnAggregateGroupHourCount, ColumnAggregateMin:
		default:
			return false
		}
		if len(aggregate.Predicates) > preparedInsertMaxAggregatePredicates {
			return false
		}
		configBytes += len(aggregate.Name) + len(aggregate.Column) + len(aggregate.GroupColumn) + len(aggregate.Kind)
		for _, predicate := range aggregate.Predicates {
			if len(predicate.Values) > preparedInsertMaxAggregatePredicateValues {
				return false
			}
			configBytes += len(predicate.Column) + len(predicate.Kind) + len(predicate.Value)
			for _, value := range predicate.Values {
				configBytes += len(value)
			}
		}
		if configBytes > preparedInsertMaxAggregateConfigBytes {
			return false
		}
		if _, ok, err := newColumnAggregateMetadataBuildSpec(cfg, aggregate); err != nil || !ok {
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

func preparedDeclaredBackingBytes(rows []columnDeclaredRow, stringBackingBytes int64) int64 {
	owned := int64(cap(rows))*int64(unsafe.Sizeof(columnDeclaredRow{})) + stringBackingBytes
	for i := range rows {
		owned += int64(cap(rows[i].ID))
		owned += int64(cap(rows[i].Values)) * int64(unsafe.Sizeof(columnDeclaredValue{}))
		for j := range rows[i].Values {
			v := &rows[i].Values[j]
			owned += int64(cap(v.Float32Vector)*4 + cap(v.DenseNumericVector) +
				cap(v.Uint32List)*4 + cap(v.AdjacencyList)*4 + cap(v.Bytes) + cap(v.StringBytes))
		}
	}
	return owned
}

// The typed batch borrows strings already owned by declaredRows. Charge its
// separate vectors, schema slices and map buckets without counting those
// strings a second time. Map bucket accounting remains conservative rather
// than an exact runtime capacity measurement.
func preparedTypedBatchBackingBytes(prepared *typedColumnAdapterPreparedBatch) int64 {
	if prepared == nil {
		return 0
	}
	const mapEntryCharge = 256
	owned := int64(cap(prepared.Columns))*int64(unsafe.Sizeof(typedColumnAdapterColumn{})) +
		int64(cap(prepared.Options.Fields))*int64(unsafe.Sizeof(TypedStorageField{})) +
		int64(cap(prepared.Options.SortKey))*int64(unsafe.Sizeof(ColumnSortKey{}))
	for _, values := range prepared.Batch.Columns {
		owned += int64(cap(values))*8 + mapEntryCharge
	}
	for _, values := range prepared.Batch.Nulls {
		owned += int64(cap(values)) + mapEntryCharge
	}
	for _, values := range prepared.Batch.Defaults {
		owned += int64(cap(values)) + mapEntryCharge
	}
	for _, column := range prepared.Columns {
		owned += int64(len(column.Dictionary)+len(column.ReverseDictionary))*mapEntryCharge +
			int64(cap(column.DictionaryValuesByCode))*int64(unsafe.Sizeof(""))
	}
	owned += int64(len(prepared.Options.DictionaryModes)) * mapEntryCharge
	return owned
}

// PrepareInsertBatchOwned prepares the no-index JSON semantic-stream path.
// maxOwnedBytes limits the retained request plus source-derived preparation,
// commit, WAL, and ordered-publication credit. Fixed reserves reject large
// requests before their covered allocations, and retained capacities are
// checked after encoding. The result is an admission bound for request-owned
// memory rather than heap or process-RSS telemetry. Callers may use ordinary
// InsertBatch only on configuration ineligibility; resource-limit errors must
// fail closed in bounded callers.
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
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: empty batch or byte limit", ErrPreparedInsertIneligible)
	}
	for _, id := range ids {
		if len(id) == 0 {
			return nil, errors.New("collections: document id cannot be empty")
		}
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	if err := c.ensureWriteDomainOpen(); err != nil {
		return nil, err
	}
	// The catalog is immutable and its pointer is guarded by catalogMu.
	// Preparation needs only schema; Commit binds it to the current root and
	// checks conflicts before WAL. Avoid snapshot pins and catalog/root reads
	// before the request's publication budget is admitted.
	c.catalogMu.RLock()
	catalog := c.catalog
	c.catalogMu.RUnlock()
	if catalog == nil {
		return nil, fmt.Errorf("%w: collection catalog is not cached", ErrPreparedInsertResourceLimit)
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
		return nil, fmt.Errorf("%w: prepared declared-row cursor supports one to %d Int64/String column paths of at most %d bytes", ErrPreparedInsertIneligible, preparedInsertMaxScalarColumns, preparedInsertMaxSchemaPathBytes)
	}
	if cfg.AssetManager == nil || len(cfg.AssetManager.Namespace) > preparedInsertMaxAssetNamespaceBytes {
		return nil, fmt.Errorf("%w: column asset namespace exceeds %d bytes", ErrPreparedInsertIneligible, preparedInsertMaxAssetNamespaceBytes)
	}
	// Restrict metadata fanout and spec shape before preparation. The scalar
	// JSONBench target declares three supported aggregate specs even when timed
	// queries opt out of using them.
	if !preparedInsertBoundedAggregateMetadata(*cfg) {
		return nil, fmt.Errorf("%w: aggregate metadata is outside prepared insert eligibility", ErrPreparedInsertIneligible)
	}
	if maxOwnedBytes <= 0 {
		return nil, fmt.Errorf("%w: byte limit %d", ErrPreparedInsertResourceLimit, maxOwnedBytes)
	}
	if len(ids) > preparedInsertMaxRows {
		return nil, fmt.Errorf("%w: batch has %d rows, maximum %d", ErrPreparedInsertResourceLimit, len(ids), preparedInsertMaxRows)
	}
	for _, id := range ids {
		if len(id) > preparedInsertMaxIDBytes {
			return nil, fmt.Errorf("%w: document ID has %d bytes, maximum %d", ErrPreparedInsertResourceLimit, len(id), preparedInsertMaxIDBytes)
		}
	}
	for _, document := range documents {
		if len(document) > preparedInsertMaxDocumentBytes {
			return nil, fmt.Errorf("%w: document has %d bytes, maximum %d", ErrPreparedInsertResourceLimit, len(document), preparedInsertMaxDocumentBytes)
		}
	}
	inputBytes := preparedInsertInputBytes(ids, documents)
	if inputBytes > maxOwnedBytes {
		return nil, fmt.Errorf("%w: input bytes %d exceed %d", ErrPreparedInsertResourceLimit, inputBytes, maxOwnedBytes)
	}
	if !preparedInsertUnusedOuterSlotsEmpty(ids, documents) {
		return nil, fmt.Errorf("%w: unused outer-slice slots retain uncharged buffers", ErrPreparedInsertResourceLimit)
	}
	// The semantic builder subtracts this same batch allowance before dividing
	// credit among block workers. Check it before allocating entries or ordered
	// headers: 48 B/row for entries, 72 B/row for three [][]byte headers, and
	// 8 B/row plus 1 MiB for bounded batch tables and worker bookkeeping.
	batchHeaderCredit := saturatingAddNonNegativeInt64(inputBytes, preparedInsertMul(preparedSemanticStreamBatchHeaderBytesPerRow, int64(len(ids))))
	batchHeaderCredit = saturatingAddNonNegativeInt64(batchHeaderCredit, preparedSemanticStreamBatchReserveBytes)
	if batchHeaderCredit >= maxOwnedBytes {
		return nil, fmt.Errorf("%w: batch header credit %d leaves no retained block credit under %d", ErrPreparedInsertResourceLimit, batchHeaderCredit, maxOwnedBytes)
	}
	if err := c.requireColumnStoreCommandWAL(meta, nil); err != nil {
		return nil, err
	}
	if err := requireColumnStoreWriteOperationSupported(meta, ColumnPublishOperationInsert); err != nil {
		return nil, err
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
	orderedIDs, orderedDocs := make([][]byte, len(entries)), make([][]byte, len(entries))
	for i := range entries {
		orderedIDs[i], orderedDocs[i] = entries[i].id, entries[i].document
	}
	start := time.Now()
	retained, err := prepareColumnRetainedSemanticStreamV1StorageDocumentsWithIDsBudget(*cfg, orderedIDs, orderedDocs, maxOwnedBytes)
	if err != nil {
		return nil, err
	}
	// Count every retained owner before the typed builder allocates its batch and
	// transient dictionaries. The semantic builder has finished, so its scratch
	// credit can be reused, while these retained buffers remain live.
	var retainedDocumentBytes int64
	for _, document := range retained.documents {
		retainedDocumentBytes = saturatingAddNonNegativeInt64(retainedDocumentBytes, int64(cap(document)))
	}
	ownedBeforeTyped := inputBytes + int64(cap(entries))*int64(unsafe.Sizeof(noIndexBatchEntry{})) +
		int64(cap(orderedIDs)+cap(orderedDocs)+cap(retained.documents))*int64(unsafe.Sizeof([]byte{})) +
		preparedDeclaredBackingBytes(retained.declaredRows, retained.declaredStringBackingBytes)
	ownedBeforeTyped = saturatingAddNonNegativeInt64(ownedBeforeTyped, retainedDocumentBytes)
	semanticBytes, err := preparedSemanticStreamBackingBytes(retained.semanticStreamBlocks)
	if err != nil {
		if retained.semanticStreamBlocks != nil {
			resetCollectionRunTable(retained.semanticStreamBlocks)
		}
		return nil, err
	}
	ownedBeforeTyped = saturatingAddNonNegativeInt64(ownedBeforeTyped, semanticBytes)
	if columnStoreHasTypedColumnPartOwners(*cfg) &&
		(ownedBeforeTyped > maxOwnedBytes || preparedTypedScalarPrepareReserve(len(ids), len(cfg.Columns)) > maxOwnedBytes-ownedBeforeTyped) {
		if retained.semanticStreamBlocks != nil {
			resetCollectionRunTable(retained.semanticStreamBlocks)
		}
		return nil, fmt.Errorf("%w: typed preparation exceeds request credit", ErrPreparedInsertResourceLimit)
	}
	var typedBatch *typedColumnAdapterPreparedBatch
	if columnStoreHasTypedColumnPartOwners(*cfg) {
		typedBatch, err = prepareTypedColumnPartBatchFromSource(*cfg, 0, newTypedColumnDeclaredRowSource(cfg.Columns, retained.declaredRows))
		if err != nil {
			if retained.semanticStreamBlocks != nil {
				resetCollectionRunTable(retained.semanticStreamBlocks)
			}
			return nil, err
		}
		if preparedTypedBatchHasZSTDGranule(typedBatch) {
			if retained.semanticStreamBlocks != nil {
				resetCollectionRunTable(retained.semanticStreamBlocks)
			}
			return nil, fmt.Errorf("%w: ZSTD typed granules are outside prepared insert eligibility", ErrPreparedInsertIneligible)
		}
	}
	// Charge backing capacities, including the semantic block arena. The caller
	// controls the size of the one batch admitted to this preparation path.
	ownedBytes := saturatingAddNonNegativeInt64(ownedBeforeTyped, preparedTypedBatchBackingBytes(typedBatch))
	ownedBytes = saturatingAddNonNegativeInt64(ownedBytes, preparedInsertSchemaCopyReserveBytes)
	var keyBytes int64
	for _, id := range ids {
		keyBytes += int64(len(id))
	}
	typedColumns, granules := 0, 0
	if typedBatch != nil {
		typedColumns = len(typedBatch.Columns) + 1 // include the primary ID
		rowsPerGranule := typedBatch.Options.RowsPerGranule
		if rowsPerGranule == 0 {
			rowsPerGranule = typedColumnDefaultRowsPerGranule()
		}
		if adaptive := typedBatch.Options.AdaptiveMarkSizing; adaptive.Enabled {
			rowsPerGranule = adaptive.MinRows
		}
		if rowsPerGranule < 1 {
			rowsPerGranule = 1
		}
		granules = (len(ids) + rowsPerGranule - 1) / rowsPerGranule
	}
	publishedBytes := saturatingAddNonNegativeInt64(inputBytes, retainedDocumentBytes)
	publishedBytes = saturatingAddNonNegativeInt64(publishedBytes, semanticBytes)
	rowAssetStringBytes, aggregateGroupBytes := preparedInsertRepeatedStringBytes(*cfg, retained.declaredRows)
	commitBudget := preparedInsertCommitReserveBytes(publishedBytes, keyBytes, preparedTypedDictionaryStringBytes(typedBatch), rowAssetStringBytes, aggregateGroupBytes,
		len(ids), len(cfg.Columns), typedColumns, granules)
	commitBudget.total = saturatingAddNonNegativeInt64(commitBudget.total, preparedInsertPublisherReserveBytes())
	if ownedBytes > maxOwnedBytes || commitBudget.total > maxOwnedBytes-ownedBytes {
		if retained.semanticStreamBlocks != nil {
			resetCollectionRunTable(retained.semanticStreamBlocks)
		}
		return nil, fmt.Errorf("%w: prepared bytes %d plus commit reserve %d exceed %d", ErrPreparedInsertResourceLimit, ownedBytes, commitBudget.total, maxOwnedBytes)
	}
	return &PreparedInsertBatch{collection: c, meta: preparedInsertSchemaMeta(meta), ids: ids, documents: documents,
		entries: entries, retained: retained, typedBatch: typedBatch, prepareElapsed: time.Since(start),
		ownedBytes: ownedBytes, commitReserve: commitBudget.total, materializeReserve: commitBudget.materialization}, nil
}

func (p *PreparedInsertBatch) OwnedBytes() int64 {
	if p == nil {
		return 0
	}
	return p.ownedBytes
}

// ReservedBytes includes retained preparation backing and conservative commit
// and publication credit admitted against maxOwnedBytes. It is an admission
// bound for request-owned memory, not measured heap or process RSS.
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
	p.ids, p.documents, p.entries, p.retained, p.typedBatch = nil, nil, nil, columnRetainedPayloadStorageDocuments{}, nil
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
