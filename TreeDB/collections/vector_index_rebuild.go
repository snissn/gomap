package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

const columnVectorGraphNeighborInsertionLimit = 32

var (
	errColumnVectorGraphInvNormEmpty          = errors.New("collections: column_graph vector is empty")
	errColumnVectorGraphInvNormValueNotFinite = errors.New("collections: column_graph vector value is not finite")
	errColumnVectorGraphInvNormNormInvalid    = errors.New("collections: column_graph vector norm must be finite and non-zero")
	errColumnVectorGraphInvNormOutOfRange     = errors.New("collections: column_graph vector inverse norm must be finite and fit float32")
)

// RebuildVectorIndex rebuilds the named vector index through the collection
// product lifecycle. V2A only builds and publishes physical column graph assets;
// it does not load or search a decoded in-memory graph.
func (c *Collection) RebuildVectorIndex(name string) (VectorIndexStatus, error) {
	return c.rebuildVectorIndexWithCommandWALIntent(name, nil)
}

func (c *Collection) rebuildVectorIndexWithCommandWALIntent(name string, replay *backenddb.CommandWALIntent) (VectorIndexStatus, error) {
	if err := ValidateIndexName(name); err != nil {
		return VectorIndexStatus{}, err
	}
	if c == nil {
		return VectorIndexStatus{}, errCollectionNil
	}
	if c.db == nil {
		return VectorIndexStatus{}, errCollectionDBNil
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWrites(); err != nil {
		return VectorIndexStatus{}, err
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return VectorIndexStatus{}, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		_ = snap.Close()
		return VectorIndexStatus{}, err
	}
	if catalog == nil {
		_ = snap.Close()
		return VectorIndexStatus{}, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		_ = snap.Close()
		return VectorIndexStatus{}, err
	}
	baseMeta := catalog.meta
	c.meta = baseMeta
	def, ok := findVectorIndex(baseMeta.VectorIndexes, name)
	if !ok {
		_ = snap.Close()
		return VectorIndexStatus{}, ErrIndexNotFound
	}
	if def.Strategy != VectorIndexStrategyColumnGraph {
		_ = snap.Close()
		return c.finishRebuildVectorIndexNoopStatus(name, c.nativeVectorIndexRebuildStatus(def), nil, replay)
	}
	cfg := baseMeta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.AssetManager == nil {
		_ = snap.Close()
		status, statusErr := c.columnGraphVectorIndexStatus(def.Name)
		return c.finishRebuildVectorIndexNoopStatus(name, status, statusErr, replay)
	}
	if normalizedDocumentFormat(baseMeta.Options.DocumentFormat) != DocumentFormatJSON {
		_ = snap.Close()
		return VectorIndexStatus{}, fmt.Errorf("collections: column_graph rebuild for %q requires JSON documents, got %q", name, baseMeta.Options.DocumentFormat)
	}

	state := snap.State()
	if state == nil {
		_ = snap.Close()
		return VectorIndexStatus{}, backenddb.ErrClosed
	}
	baseCommitSeq := state.CommitSeq
	baseSystemRoot := state.SystemRootPageID
	rootName := collectionColumnManifestRootName(baseMeta.Name)
	baseManifestRootID := catalog.rootID(rootName)
	if baseManifestRootID == 0 {
		rows, err := c.columnVectorGraphRowsFromCatalogSnapshot(snap, catalog, def)
		_ = snap.Close()
		if err != nil {
			return VectorIndexStatus{}, err
		}
		if len(rows) == 0 {
			return c.rebuildEmptyColumnGraphVectorIndexWithoutBaseManifestRoot(name, catalog, baseMeta, def, *cfg, baseCommitSeq, baseSystemRoot, rootName, replay)
		}
		return VectorIndexStatus{}, fmt.Errorf("collections: column_graph rebuild for %q requires an initial physical column manifest root before rebuilding %d documents", name, len(rows))
	}
	if cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil {
		_ = snap.Close()
		status, statusErr := c.columnGraphVectorIndexStatus(def.Name)
		return c.finishRebuildVectorIndexNoopStatus(name, status, statusErr, replay)
	}
	if err := validateColumnManifestIdentityAtRoot(snap, baseManifestRootID, *cfg.ActiveManifest); err != nil {
		_ = snap.Close()
		status, statusErr := c.columnGraphVectorIndexStatus(def.Name)
		return c.finishRebuildVectorIndexNoopStatus(name, status, statusErr, replay)
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, baseManifestRootID)
	if err != nil {
		_ = snap.Close()
		return VectorIndexStatus{}, err
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		_ = snap.Close()
		return VectorIndexStatus{}, err
	}
	if err := validateColumnManifestSnapshot(manifest, records, *cfg, *cfg.ActiveManifest, baseMeta.Name, "column vector graph rebuild"); err != nil {
		_ = snap.Close()
		return VectorIndexStatus{}, err
	}
	rows, err := c.columnVectorGraphRowsFromCatalogSnapshot(snap, catalog, def)
	if err != nil {
		_ = snap.Close()
		return VectorIndexStatus{}, err
	}
	_ = snap.Close()

	if err := buildColumnVectorGraphAdjacency(rows, def); err != nil {
		return VectorIndexStatus{}, err
	}
	rootNames := []string{rootName}
	baseRootIDs := map[string]uint64{rootName: baseManifestRootID}
	intent, err := c.newCollectionRebuildVectorIndexCommandWALIntent(name, replay)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	var newSystemRoot uint64
	var rootIDs []uint64
	if intent != nil {
		var prepared columnVectorGraphPreparedPhysicalAsset
		var updatedMeta CollectionMeta
		buildContextDeltas := func(ctx backenddb.CommandWALPublishContext) ([]backenddb.OrderedRootDeltaPublishInput, error) {
			var deltaRecords []columnManifestRecord
			var nextIdentity ColumnManifestIdentity
			prepared, deltaRecords, nextIdentity, err = prepareColumnVectorGraphRebuildManifest(baseMeta.Name, *cfg, baseMeta.VectorIndexes, def, manifest, records, ctx.AppliedCommandLSN, rows, c.db.ColumnAssetRootDir())
			if err != nil {
				return nil, err
			}
			if prepared.RowCount != len(rows) {
				return nil, fmt.Errorf("collections: column_graph rebuild row count changed rows=%d prepared=%d", len(rows), prepared.RowCount)
			}
			delta := ColumnManifestRootDelta{
				RootName:       rootName,
				BaseRootID:     baseManifestRootID,
				StoragePolicy:  cfg.ManifestRoot.StoragePolicy,
				Identity:       nextIdentity,
				IdentityRecord: encodeColumnManifestIdentityRecordArray(nextIdentity),
				Records:        deltaRecords,
			}
			ordered, err := delta.OrderedRootDeltaPublishInput()
			if err != nil {
				return nil, err
			}
			updatedMeta, err = columnGraphRebuildUpdatedMeta(baseMeta, nextIdentity, ctx.AppliedCommandLSN)
			if err != nil {
				return nil, err
			}
			return []backenddb.OrderedRootDeltaPublishInput{ordered}, nil
		}
		buildSystemDelta := func(ctx backenddb.CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if prepared.RowCount != len(rows) {
				return nil, errors.New("collections: column_graph rebuild did not prepare physical graph asset")
			}
			if updatedMeta.Name == "" {
				return nil, errors.New("collections: column_graph rebuild did not prepare updated metadata")
			}
			return c.buildColumnGraphRebuildSystemDeltaIterator(baseMeta, updatedMeta, baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
		}
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(nil, intent, buildContextDeltas, buildSystemDelta)
		if err != nil {
			return VectorIndexStatus{}, err
		}
		c.meta = updatedMeta
	} else {
		prepared, deltaRecords, nextIdentity, err := prepareColumnVectorGraphRebuildManifest(baseMeta.Name, *cfg, baseMeta.VectorIndexes, def, manifest, records, manifest.AppliedCommandLSN, rows, c.db.ColumnAssetRootDir())
		if err != nil {
			return VectorIndexStatus{}, err
		}
		delta := ColumnManifestRootDelta{
			RootName:       rootName,
			BaseRootID:     baseManifestRootID,
			StoragePolicy:  cfg.ManifestRoot.StoragePolicy,
			Identity:       nextIdentity,
			IdentityRecord: encodeColumnManifestIdentityRecordArray(nextIdentity),
			Records:        deltaRecords,
		}
		ordered, err := delta.OrderedRootDeltaPublishInput()
		if err != nil {
			return VectorIndexStatus{}, err
		}
		updatedMeta, err := columnGraphRebuildUpdatedMeta(baseMeta, nextIdentity, manifest.AppliedCommandLSN)
		if err != nil {
			return VectorIndexStatus{}, err
		}
		buildSystemDelta := func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildColumnGraphRebuildSystemDeltaIterator(baseMeta, updatedMeta, baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
		}
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]backenddb.OrderedRootDeltaPublishInput{ordered}, buildSystemDelta)
		if err != nil {
			return VectorIndexStatus{}, err
		}
		if prepared.RowCount != len(rows) {
			return VectorIndexStatus{}, fmt.Errorf("collections: column_graph rebuild row count changed rows=%d prepared=%d", len(rows), prepared.RowCount)
		}
		c.meta = updatedMeta
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		return VectorIndexStatus{}, unexpectedOrderedRootCountError(baseMeta.Name, 1, len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, c.meta, rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return c.columnGraphVectorIndexStatus(def.Name)
}

func (c *Collection) rebuildEmptyColumnGraphVectorIndexWithoutBaseManifestRoot(name string, catalog *collectionCatalog, baseMeta CollectionMeta, def VectorIndexDefinition, cfg ColumnStoreConfig, baseCommitSeq, baseSystemRoot uint64, rootName string, replay *backenddb.CommandWALIntent) (VectorIndexStatus, error) {
	intent, err := c.newCollectionRebuildVectorIndexCommandWALIntent(name, replay)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if intent == nil {
		return VectorIndexStatus{}, fmt.Errorf("%w: empty column_graph rebuild for %q requires command WAL to publish the initial physical column manifest root", backenddb.ErrCommandWALRejected, name)
	}

	rootNames := []string{rootName}
	baseRootIDs := map[string]uint64{rootName: 0}
	var updatedMeta CollectionMeta
	buildContextDeltas := func(ctx backenddb.CommandWALPublishContext) ([]backenddb.OrderedRootDeltaPublishInput, error) {
		manifest, records, err := initialColumnVectorGraphBaseManifestForRebuild(baseMeta.Name, cfg, ctx.AppliedCommandLSN)
		if err != nil {
			return nil, err
		}
		prepared, deltaRecords, nextIdentity, err := prepareColumnVectorGraphRebuildManifest(baseMeta.Name, cfg, baseMeta.VectorIndexes, def, manifest, records, ctx.AppliedCommandLSN, nil, c.db.ColumnAssetRootDir())
		if err != nil {
			return nil, err
		}
		if prepared.RowCount != 0 {
			return nil, fmt.Errorf("collections: empty column_graph rebuild prepared rows=%d want 0", prepared.RowCount)
		}
		delta := ColumnManifestRootDelta{
			RootName:       rootName,
			BaseRootID:     0,
			StoragePolicy:  cfg.ManifestRoot.StoragePolicy,
			Identity:       nextIdentity,
			IdentityRecord: encodeColumnManifestIdentityRecordArray(nextIdentity),
			Records:        deltaRecords,
		}
		ordered, err := delta.OrderedRootDeltaPublishInput()
		if err != nil {
			return nil, err
		}
		updatedMeta, err = columnGraphRebuildUpdatedMeta(baseMeta, nextIdentity, ctx.AppliedCommandLSN)
		if err != nil {
			return nil, err
		}
		return []backenddb.OrderedRootDeltaPublishInput{ordered}, nil
	}
	buildSystemDelta := func(ctx backenddb.CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if updatedMeta.Name == "" {
			return nil, errors.New("collections: empty column_graph rebuild did not prepare updated metadata")
		}
		return c.buildColumnGraphRebuildSystemDeltaIterator(baseMeta, updatedMeta, baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
	}
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(nil, intent, buildContextDeltas, buildSystemDelta)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		return VectorIndexStatus{}, unexpectedOrderedRootCountError(baseMeta.Name, 1, len(rootIDs))
	}
	c.meta = updatedMeta
	nextCatalog := cloneCatalogWithRootUpdates(catalog, updatedMeta, rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return c.columnGraphVectorIndexStatus(def.Name)
}

func (c *Collection) nativeVectorIndexRebuildStatus(def VectorIndexDefinition) VectorIndexStatus {
	return VectorIndexStatus{
		Name:     def.Name,
		Strategy: def.Strategy,
		State:    VectorIndexStateNativeRuntime,
		Reason:   VectorIndexReasonNativeRuntime,
	}
}

func (c *Collection) finishRebuildVectorIndexNoopStatus(name string, status VectorIndexStatus, statusErr error, replay *backenddb.CommandWALIntent) (VectorIndexStatus, error) {
	if statusErr != nil {
		return VectorIndexStatus{}, statusErr
	}
	intent, err := c.newCollectionRebuildVectorIndexCommandWALIntent(name, replay)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if intent != nil {
		if err := c.db.PublishCommandWALNoop(intent, false); err != nil {
			return VectorIndexStatus{}, err
		}
	}
	return status, nil
}

func (c *Collection) columnVectorGraphRowsFromCatalogSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, def VectorIndexDefinition) ([]columnVectorGraphAssetRow, error) {
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	vectorCfg, err := normalizeColumnStoreConfig(catalog.meta.Name, &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{
			Name:       columnVectorGraphVectorColumnName,
			Path:       def.Field,
			ValueType:  ColumnStoreValueFloat32Vector,
			VectorDims: def.Dimensions,
		}},
	})
	if err != nil {
		return nil, err
	}
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, collectionPrimaryRootName(catalog.meta.Name), nil, nil, false)
	if err != nil {
		return nil, err
	}
	if it == nil {
		return nil, nil
	}
	defer func() { _ = it.Close() }()

	rows := make([]columnVectorGraphAssetRow, 0)
	visit := func(record DocumentRecord) (bool, error) {
		declared, err := extractColumnDeclaredRowsFromJSONDocuments(*vectorCfg, []columnWriteDocument{{
			ID:       record.ID,
			Document: record.Document,
		}})
		if err != nil {
			return false, err
		}
		if len(declared) != 1 || len(declared[0].Values) != 1 {
			values := 0
			if len(declared) != 0 {
				values = len(declared[0].Values)
			}
			return false, fmt.Errorf("collections: column_graph rebuild vector extraction returned rows=%d values=%d", len(declared), values)
		}
		value := declared[0].Values[0]
		if !value.Present || value.Null {
			return false, fmt.Errorf("collections: column_graph rebuild missing vector for document id %q", string(record.ID))
		}
		vector := append([]float32(nil), value.Float32Vector...)
		invNorm, err := columnVectorGraphInvNorm(vector)
		if err != nil {
			return false, fmt.Errorf("collections: column_graph rebuild document id %q: %w", string(record.ID), err)
		}
		// The scan producers clone iterator keys before calling visit; do
		// not add another per-row ID copy on the rebuild hot path.
		rows = append(rows, columnVectorGraphAssetRow{
			ID:      record.ID,
			Vector:  vector,
			InvNorm: invNorm,
		})
		return true, nil
	}
	if columnStoreCanReconstructDocument(catalog.meta) {
		_, err = c.scanDocumentsFuncWithColumnReconstruction(snap, catalog, it, maxCollectionInt, visit)
		return rows, err
	}
	for it.Valid() {
		if it.IsDeleted() {
			it.Next()
			continue
		}
		record := DocumentRecord{
			ID:       bytes.Clone(it.UnsafeKey()),
			Document: it.ValueCopy(nil),
		}
		next, err := visit(record)
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return rows, nil
}

func columnVectorGraphInvNorm(vector []float32) (float32, error) {
	if len(vector) == 0 {
		return 0, errColumnVectorGraphInvNormEmpty
	}
	var sum float64
	for i, v := range vector {
		f := float64(v)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, fmt.Errorf("vector[%d] is not finite: %w", i, errColumnVectorGraphInvNormValueNotFinite)
		}
		sum += f * f
	}
	if sum == 0 || math.IsNaN(sum) || math.IsInf(sum, 0) {
		return 0, errColumnVectorGraphInvNormNormInvalid
	}
	invNorm := 1 / math.Sqrt(sum)
	if invNorm > math.MaxFloat32 || math.IsNaN(invNorm) || math.IsInf(invNorm, 0) {
		return 0, errColumnVectorGraphInvNormOutOfRange
	}
	return float32(invNorm), nil
}

func buildColumnVectorGraphAdjacency(rows []columnVectorGraphAssetRow, def VectorIndexDefinition) error {
	degree := def.M
	if degree <= 0 {
		degree = defaultVectorIndexM
	}
	if degree > len(rows)-1 {
		degree = len(rows) - 1
	}
	for i := range rows {
		if len(rows[i].Vector) != def.Dimensions {
			return fmt.Errorf("collections: column vector graph row[%d] vector dims=%d want %d", i, len(rows[i].Vector), def.Dimensions)
		}
	}
	for i := range rows {
		if degree <= 0 {
			rows[i].Adjacency = nil
			continue
		}
		candidates, err := topColumnVectorGraphNeighbors(rows, i, degree)
		if err != nil {
			return err
		}
		neighbors := make([]uint32, len(candidates))
		for n := range candidates {
			neighbors[n] = uint32(candidates[n].ordinal)
		}
		rows[i].Adjacency = neighbors
	}
	return nil
}

func topColumnVectorGraphNeighbors(rows []columnVectorGraphAssetRow, row, degree int) ([]columnVectorGraphNeighbor, error) {
	if degree <= columnVectorGraphNeighborInsertionLimit {
		candidates := make([]columnVectorGraphNeighbor, 0, degree)
		for j := range rows {
			if row == j {
				continue
			}
			score := columnVectorGraphCosine(rows[row], rows[j])
			if math.IsNaN(score) {
				return nil, fmt.Errorf("collections: column vector graph cosine row[%d,%d] is NaN", row, j)
			}
			candidates = insertColumnVectorGraphTopNeighbor(candidates, degree, columnVectorGraphNeighbor{ordinal: j, score: score})
		}
		return candidates, nil
	}

	candidates := make(columnVectorGraphNeighborHeap, 0, degree)
	for j := range rows {
		if row == j {
			continue
		}
		score := columnVectorGraphCosine(rows[row], rows[j])
		if math.IsNaN(score) {
			return nil, fmt.Errorf("collections: column vector graph cosine row[%d,%d] is NaN", row, j)
		}
		candidates.pushTop(degree, columnVectorGraphNeighbor{ordinal: j, score: score})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return columnVectorGraphNeighborLess(candidates[i], candidates[j])
	})
	return candidates, nil
}

type columnVectorGraphNeighbor struct {
	ordinal int
	score   float64
}

type columnVectorGraphNeighborHeap []columnVectorGraphNeighbor

func insertColumnVectorGraphTopNeighbor(top []columnVectorGraphNeighbor, limit int, candidate columnVectorGraphNeighbor) []columnVectorGraphNeighbor {
	if limit <= 0 {
		return top
	}
	pos := len(top)
	for pos > 0 && columnVectorGraphNeighborLess(candidate, top[pos-1]) {
		pos--
	}
	if pos >= limit {
		return top
	}
	if len(top) < limit {
		top = append(top, columnVectorGraphNeighbor{})
	}
	copy(top[pos+1:], top[pos:len(top)-1])
	top[pos] = candidate
	return top
}

func (h *columnVectorGraphNeighborHeap) pushTop(limit int, candidate columnVectorGraphNeighbor) {
	if limit <= 0 {
		return
	}
	if len(*h) < limit {
		*h = append(*h, candidate)
		h.siftUp(len(*h) - 1)
		return
	}
	if !columnVectorGraphNeighborLess(candidate, (*h)[0]) {
		return
	}
	(*h)[0] = candidate
	h.siftDown(0)
}

func (h columnVectorGraphNeighborHeap) siftUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if !columnVectorGraphNeighborWorse(h[idx], h[parent]) {
			return
		}
		h[idx], h[parent] = h[parent], h[idx]
		idx = parent
	}
}

func (h columnVectorGraphNeighborHeap) siftDown(idx int) {
	for {
		left := idx*2 + 1
		if left >= len(h) {
			return
		}
		child := left
		if right := left + 1; right < len(h) && columnVectorGraphNeighborWorse(h[right], h[left]) {
			child = right
		}
		if !columnVectorGraphNeighborWorse(h[child], h[idx]) {
			return
		}
		h[idx], h[child] = h[child], h[idx]
		idx = child
	}
}

func columnVectorGraphNeighborLess(left, right columnVectorGraphNeighbor) bool {
	if left.score == right.score {
		return left.ordinal < right.ordinal
	}
	return left.score > right.score
}

func columnVectorGraphNeighborWorse(left, right columnVectorGraphNeighbor) bool {
	if left.score == right.score {
		return left.ordinal > right.ordinal
	}
	return left.score < right.score
}

func columnVectorGraphCosine(left, right columnVectorGraphAssetRow) float64 {
	var dot float64
	for i, v := range left.Vector {
		dot += float64(v) * float64(right.Vector[i])
	}
	return dot * float64(left.InvNorm) * float64(right.InvNorm)
}

func prepareColumnVectorGraphRebuildManifest(collection string, cfg ColumnStoreConfig, activeVectorIndexes []VectorIndexDefinition, def VectorIndexDefinition, manifest columnManifestSnapshot, records []columnManifestRecord, appliedCommandLSN uint64, rows []columnVectorGraphAssetRow, assetRootDir string) (columnVectorGraphPreparedPhysicalAsset, []columnManifestRecord, ColumnManifestIdentity, error) {
	if appliedCommandLSN == 0 {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, errors.New("collections: column_graph rebuild requires non-zero AppliedCommandLSN")
	}
	recordsForLSN, err := columnVectorGraphManifestRecordsWithAppliedCommandLSN(manifest, records, cfg, activeVectorIndexes, appliedCommandLSN)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	manifestForLSN := manifest
	manifestForLSN.AppliedCommandLSN = appliedCommandLSN
	baseChecksum, err := columnVectorGraphBaseManifestChecksum(manifestForLSN, recordsForLSN, cfg)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig(collection, cfg, def)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	partID := nextColumnVectorGraphPartID(recordsForLSN, graphCfg.AssetManager.Namespace)
	prepared, err := prepareColumnVectorGraphPhysicalAsset(assetRootDir, collection, cfg, def, manifest.Generation, partID, appliedCommandLSN, rows)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	graph := columnVectorGraphManifestSnapshot{
		IndexName:              def.Name,
		Field:                  def.Field,
		Metric:                 def.Metric,
		Encoding:               def.Encoding,
		Dimensions:             def.Dimensions,
		M:                      def.M,
		EfConstruction:         def.EfConstruction,
		EfSearch:               def.EfSearch,
		BaseManifestGeneration: manifest.Generation,
		BaseManifestChecksum:   baseChecksum,
		BaseSchemaHash:         cfg.SchemaHash,
		GraphSchemaHash:        graphCfg.SchemaHash,
		RowCount:               prepared.RowCount,
		AssetRef:               prepared.Ref,
		AssetBytes:             prepared.Bytes,
	}
	raw, err := encodeColumnVectorGraphManifestRecord(graph)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	record := columnManifestRecord{key: columnVectorGraphManifestRecordKey(def.Name), value: raw}
	nextRecords, err := replaceColumnVectorGraphManifestRecord(recordsForLSN, manifest.Generation, record)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	identity := ColumnManifestIdentity{
		Generation: manifest.Generation,
		Format:     columnManifestFormatTCS1,
		Version:    columnManifestIdentityVersion,
		Checksum: checksumColumnManifestRecords(ColumnPublishManifestEncodeInput{
			Collection:        manifest.Collection,
			ColumnStore:       cfg,
			Operation:         manifest.Operation,
			AppliedCommandLSN: appliedCommandLSN,
		}, manifest.Generation, nextRecords),
	}
	normalizeColumnManifestIdentityDefaults(&identity)
	return prepared, nextRecords, identity, nil
}

func columnVectorGraphManifestRecordsWithAppliedCommandLSN(manifest columnManifestSnapshot, records []columnManifestRecord, cfg ColumnStoreConfig, activeVectorIndexes []VectorIndexDefinition, appliedCommandLSN uint64) ([]columnManifestRecord, error) {
	active, err := activeColumnManifestRecordsForScan(records, manifest.Generation)
	if err != nil {
		return nil, err
	}
	out := make([]columnManifestRecord, 0, len(active))
	activePartCount := uint64(0)
	for _, record := range active {
		if bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			partGeneration, err := columnManifestPartGenerationFromRecordKeyForScan(record.key)
			if err != nil {
				return nil, err
			}
			if partGeneration == manifest.Generation {
				activePartCount++
			}
		}
	}
	header := encodeColumnVectorGraphRebuildHeaderRecord(manifest, cfg, appliedCommandLSN, activePartCount)
	replacedHeader := false
	for _, record := range active {
		if bytes.Equal(record.key, columnManifestHeaderRecordKeyBytes) {
			out = append(out, columnManifestRecord{key: bytes.Clone(record.key), value: header})
			replacedHeader = true
			continue
		}
		if bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes) &&
			!retainColumnManifestVectorGraphRecordForWrite(record.key, true, activeVectorIndexes) {
			continue
		}
		out = append(out, columnManifestRecord{key: bytes.Clone(record.key), value: bytes.Clone(record.value)})
	}
	if !replacedHeader {
		return nil, errors.New("collections: column_graph rebuild missing column manifest header")
	}
	sortColumnManifestRecords(out)
	return out, nil
}

func encodeColumnVectorGraphRebuildHeaderRecord(manifest columnManifestSnapshot, cfg ColumnStoreConfig, appliedCommandLSN, activePartCount uint64) []byte {
	var b bytes.Buffer
	writeManifestUint32(&b, columnManifestHeaderMagic)
	writeManifestUint16(&b, columnManifestRecordVersion)
	writeManifestString(&b, manifest.Collection)
	writeManifestString(&b, string(manifest.Operation))
	writeManifestUint64(&b, manifest.Generation)
	writeManifestUint64(&b, appliedCommandLSN)
	writeManifestUint64(&b, cfg.SchemaHash)
	writeManifestUint64(&b, uint64(manifest.RowCount))
	writeManifestUint64(&b, uint64(manifest.CommandBytes))
	writeManifestUint64(&b, uint64(manifest.RowRemainderBytes))
	writeManifestUint64(&b, uint64(manifest.ColumnPayloadBytes))
	writeManifestUint64(&b, activePartCount)
	return b.Bytes()
}

func initialColumnVectorGraphBaseManifestForRebuild(collection string, cfg ColumnStoreConfig, appliedCommandLSN uint64) (columnManifestSnapshot, []columnManifestRecord, error) {
	encoded, err := encodeColumnManifestForWrite(ColumnPublishManifestEncodeInput{
		Collection:        collection,
		ColumnStore:       cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: appliedCommandLSN,
		Prepared: ColumnPublishPreparedAssets{
			RowCount: 0,
		},
	})
	if err != nil {
		return columnManifestSnapshot{}, nil, err
	}
	manifest, err := decodeColumnManifestSnapshotForScan(encoded.Records)
	if err != nil {
		return columnManifestSnapshot{}, nil, err
	}
	return manifest, encoded.Records, nil
}

func replaceColumnVectorGraphManifestRecord(records []columnManifestRecord, generation uint64, replacement columnManifestRecord) ([]columnManifestRecord, error) {
	active, err := activeColumnManifestRecordsForScan(records, generation)
	if err != nil {
		return nil, err
	}
	out := make([]columnManifestRecord, 0, len(active)+1)
	replaced := false
	for _, record := range active {
		if bytes.Equal(record.key, replacement.key) {
			out = append(out, columnManifestRecord{key: bytes.Clone(replacement.key), value: bytes.Clone(replacement.value)})
			replaced = true
			continue
		}
		out = append(out, columnManifestRecord{key: bytes.Clone(record.key), value: bytes.Clone(record.value)})
	}
	if !replaced {
		out = append(out, columnManifestRecord{key: bytes.Clone(replacement.key), value: bytes.Clone(replacement.value)})
	}
	sortColumnManifestRecords(out)
	return out, nil
}

func nextColumnVectorGraphPartID(records []columnManifestRecord, namespace string) uint64 {
	next := uint64(1)
	for _, record := range records {
		if bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			_, partID, err := columnManifestPartKeyFromRecordKeyForScan(record.key)
			if err != nil {
				continue
			}
			next = nextColumnVectorGraphPartIDAfter(next, partID)
			continue
		}
		if !bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes) {
			continue
		}
		graph, err := decodeColumnVectorGraphManifestRecord(record.value)
		if err != nil {
			continue
		}
		if graph.AssetRef.Namespace != namespace {
			continue
		}
		next = nextColumnVectorGraphPartIDAfter(next, graph.AssetRef.PartID)
	}
	return next
}

func nextColumnVectorGraphPartIDAfter(next, observed uint64) uint64 {
	if observed < next || observed == ^uint64(0) {
		return next
	}
	return observed + 1
}

func columnGraphRebuildUpdatedMeta(base CollectionMeta, identity ColumnManifestIdentity, appliedCommandLSN uint64) (CollectionMeta, error) {
	updated := copyCollectionMeta(base)
	if updated.Options.ColumnStore == nil || !updated.Options.ColumnStore.Enabled {
		return CollectionMeta{}, errColumnPublishPlanRequiresEnabledColumnStore
	}
	cfg := updated.Options.ColumnStore.copy()
	active := identity
	recovery := identity
	cfg.ActiveManifest = &active
	cfg.RecoveryAuthoritativeManifest = &recovery
	cfg.RecoveryAuthoritativeAppliedCommandLSN = appliedCommandLSN
	updated.Options.ColumnStore = &cfg
	return normalizeCollectionMeta(updated)
}

func (c *Collection) buildColumnGraphRebuildSystemDeltaIterator(baseMeta, updatedMeta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, rootIDs []uint64) (iterator.UnsafeIterator, error) {
	if len(rootIDs) != len(rootNames) {
		return nil, unexpectedOrderedRootCountError(baseMeta.Name, len(rootNames), len(rootIDs))
	}
	if err := c.validateRootDescriptorSystemDeltaForMeta(baseMeta, expectedCommitSeq, expectedSystemRoot, rootNames, baseRootIDs); err != nil {
		return nil, err
	}
	encodedMeta, err := encodeNormalizedCollectionMeta(updatedMeta)
	if err != nil {
		return nil, err
	}
	updates := make(map[string][]byte, len(rootNames)+1)
	updates[systemCollectionMetaKey(baseMeta.Name)] = encodedMeta
	for i, rootName := range rootNames {
		if rootIDs[i] == 0 {
			return nil, fmt.Errorf("collections: ordered root publish returned zero root for %q", rootName)
		}
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
	}
	return buildSystemDeltaIterator(updates)
}
