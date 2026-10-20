package collections

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

// A request carries only the public operation and its caller-owned result. The
// captured state, original build ledger and covered handoff retain all custody.
type nativeColumnCompaction struct {
	collection *Collection
	ctx        context.Context
	options    ColumnStoreCompactOptions
	target     uint64
	stats      ColumnStoreCompactStats
}

func (c *Collection) nativeColumnStoreCompact(ctx context.Context, options ColumnStoreCompactOptions) (ColumnStoreCompactStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ColumnStoreCompactStats{}, err
	}
	if c.manager.isClosing() {
		return ColumnStoreCompactStats{}, backenddb.ErrClosed
	}
	operation := &nativeColumnCompaction{collection: c, ctx: ctx, options: options}
	err := c.nativeJSON.db.CompactNativeColumnStore(ctx, operation)
	if err != nil {
		return ColumnStoreCompactStats{}, err
	}
	return operation.stats, nil
}

func (e *nativeEngineVersion) PrepareColumnStoreCompaction(request backenddb.NativeColumnStoreCompaction, group *backenddb.RootPublicationBuildGroup, basis *backenddb.Snapshot, capture *backenddb.StableResourceCaptureLease) error {
	operation, ok := request.(*nativeColumnCompaction)
	if !ok || operation == nil || e == nil {
		return backenddb.NativeSemanticBuildRetryableRefusal{Cause: backenddb.ErrNativeSemanticUnsupported}
	}
	c, ok := e.names.Get(operation.collection.name)
	if !ok {
		return backenddb.NativeSemanticBuildRetryableRefusal{Cause: errors.Join(ErrCollectionNotFound, tree.ErrKeyNotFound)}
	}
	if !nativeTypedMeta(*c.schema.meta) {
		return backenddb.NativeSemanticBuildRetryableRefusal{Cause: backenddb.ErrNativeSemanticUnsupported}
	}
	if err := nativeTypedProfile(operation.collection.nativeJSON.db, *c.schema.meta); err != nil {
		return backenddb.NativeSemanticBuildRetryableRefusal{Cause: err}
	}
	operation.target = c.incarnation
	build, err := prepareNativeEngineBuildForCompaction(e, basis, capture, group, operation)
	if err != nil {
		return err
	}
	return build.materializeDomains(group)
}

func (operation *nativeColumnCompaction) CompleteColumnStoreCompaction(systemRoot uint64) {
	operation.stats.SystemRoot = systemRoot
}

// The pending inventory is unchanged for ordinary builds. Maintenance adds only
// its explicit original captured target, including physical-only domains.
func scanNativeBuildCollections(e *nativeEngineVersion, target uint64, visit func(uint64, *nativeCollectionState) bool) {
	if target == 0 {
		e.pending.Scan(visit)
		return
	}
	e.collections.Scan(func(id uint64, c *nativeCollectionState) bool {
		if id != target && c.pending.Len() == 0 {
			return true
		}
		return visit(id, c)
	})
}

// This is the same operation-local READ adapter as native_typed_read.go. The
// maintained compaction scanner reads only the supplied snapshot/catalog/roots;
// neither it nor the adapter captures or publishes current state.
func (w *nativeTypedMaterialization) prepareCompactionRows(admit func(uint64) error) error {
	operation := w.input.compaction
	c := w.input.capturedCollection
	if operation == nil || c == nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	cfg := *w.meta.Options.ColumnStore
	if (cfg.ActiveManifest == nil) != (cfg.RecoveryAuthoritativeManifest == nil) || (cfg.ActiveManifest != nil && *cfg.ActiveManifest != *cfg.RecoveryAuthoritativeManifest) {
		return backenddb.ErrNativeSemanticUnsupported
	}
	if err := columnStoreCompactionRejectUnsupportedManifestRecords(w.records); err != nil {
		return err
	}
	rootDir := operation.collection.nativeJSON.db.ColumnAssetRootDir()
	extra, _, err := nativeTypedFamilyReadCapacity(w.meta, w.input.snapshot, w.input.baseManifestRoot, rootDir, admit)
	if err != nil {
		return err
	}
	if err = admit(extra); err != nil {
		return err
	}
	roots := make(map[string]uint64, c.domains.Len())
	c.domains.Scan(func(name string, d *nativeDomainState) bool { roots[w.meta.Name+"/"+name] = d.baseRoot; return true })
	catalog := newCollectionCatalog(w.meta, roots)
	local := &Collection{db: operation.collection.nativeJSON.db, nativeOperationLocalReads: true}
	var rows []columnDeclaredRow
	if cfg.ActiveManifest != nil {
		var diagnostics columnPhysicalScanDiagnostics
		rows, diagnostics, err = local.materializeColumnStoreCompactionRows(operation.ctx, columnStoreCompactionState{snap: w.input.snapshot, catalog: catalog, meta: w.meta, cfg: cfg, baseRoot: w.input.baseManifestRoot}, operation.options.ReadIntegrity)
		if err != nil {
			return err
		}
		operation.stats = ColumnStoreCompactStats{PreviousGeneration: cfg.ActiveManifest.Generation, ManifestRecordsBefore: len(w.records), MutationPartsBefore: diagnostics.MutationParts, RowsScanned: diagnostics.RowsScanned, DeletedRows: diagnostics.DeletedRows, PhysicalBytesRead: diagnostics.PhysicalBytesScanned}
	} else if w.input.baseManifestRoot != 0 {
		return backenddb.ErrNativeSemanticUnsupported
	}
	mutations := []nativeTypedMaterializeMutation(nil)
	if len(w.input.deltas) != 0 {
		mutations, err = selectNativeTypedMaterializeMutations(w.input.deltas)
		if err != nil {
			return err
		}
		slices.SortFunc(mutations, func(a, b nativeTypedMaterializeMutation) int { return bytes.Compare(a.id, b.id) })
	}
	// Merge the exact physical row set with the captured last writer per ID.
	// Pending values borrow the same immutable captured delta until build ends.
	merged := make([]columnDeclaredRow, 0, len(rows)+len(mutations))
	i, j := 0, 0
	for i < len(rows) || j < len(mutations) {
		if j == len(mutations) || (i < len(rows) && bytes.Compare(rows[i].ID, mutations[j].id) < 0) {
			merged = append(merged, rows[i])
			i++
			continue
		}
		m := mutations[j]
		if i < len(rows) && bytes.Equal(rows[i].ID, m.id) {
			i++
		}
		if m.row >= 0 {
			merged = append(merged, w.input.deltas[m.delta].rows[m.row])
		} else {
			operation.stats.DeletedRows++
		}
		j++
	}
	w.compactionRows = merged
	// The maintained source-dependent output proof covers the actual complete
	// replacement, not compressed asset size. Admit it on the SAME original work
	// receipt before any fallible asset constructor is entered.
	output, err := nativeTypedDeltaBuildCapacity(w.meta, merged, make([][]byte, len(merged)))
	if err != nil {
		return err
	}
	return admit(output)
}

func cloneNativeCompactionRefs(refs []ColumnAssetRef) []ColumnAssetRef {
	result := make([]ColumnAssetRef, len(refs))
	copy(result, refs)
	for i := range result {
		result[i].Kind = ColumnAssetKind(strings.Clone(string(result[i].Kind)))
		result[i].Namespace = strings.Clone(result[i].Namespace)
	}
	return result
}

func (w *nativeTypedMaterialization) materializeCompaction(context columnPhysicalAssetBuildContext) error {
	operation := w.input.compaction
	if err := operation.ctx.Err(); err != nil {
		return err
	}
	cfg := *w.meta.Options.ColumnStore
	// An already insert-only physical prefix needs no rewrite. Pending commands
	// have been materialized in this same group before this decision.
	if cfg.PhysicalMutationParts == 0 {
		return nil
	}
	if cfg.ActiveManifest == nil || cfg.ActiveManifest.Generation == ^uint64(0) {
		return memtable.ErrCOWCapacity
	}
	generation := cfg.ActiveManifest.Generation + 1
	rows := w.compactionRows
	input := columnWritePublishInput{meta: w.meta, operation: ColumnPublishOperationInsert, rows: len(rows), declaredRows: rows, declaredRowsReady: true}
	prepared := ColumnPublishPreparedAssets{RowCount: len(rows)}
	var err error
	if len(rows) != 0 {
		prepared, err = context.prepareRows(prepared, input, ColumnPublishAssetPrepareInput{Collection: w.meta.Name, ColumnStore: cfg, Operation: ColumnPublishOperationInsert, AppliedCommandLSN: cfg.RecoveryAuthoritativeAppliedCommandLSN, CurrentManifest: cfg.ActiveManifest}, rows, nil, nil, generation, columnPhysicalRowAssetPartID, typedColumnPartAssetPartID)
		if err != nil {
			return err
		}
	}
	// Adopt original asset custody before hooks, encoding or root construction.
	if prepared.stableResources != nil {
		resources := prepared.stableResources
		prepared.stableResources = nil
		if err = w.resourceBuilder.Merge(resources); err != nil {
			resources.Release()
			return err
		}
	}
	if err = runColumnStoreCompactionAfterPrepareHook(prepared); err != nil {
		return err
	}
	manifest, err := encodeColumnManifestForWrite(ColumnPublishManifestEncodeInput{Collection: w.meta.Name, ColumnStore: cfg, Operation: ColumnPublishOperationInsert, AppliedCommandLSN: cfg.RecoveryAuthoritativeAppliedCommandLSN, CurrentManifest: cfg.ActiveManifest, Prepared: prepared})
	if err != nil {
		return err
	}
	superseded := columnStoreCompactionRefsFromManifestRecords(w.records)
	identity := encodeColumnManifestIdentityRecordArray(manifest.Identity)
	it := columnStoreCompactionManifestDeltaIterator(identity, w.records, manifest.Records)
	policy, err := collectionRootStoragePolicy(w.meta, w.meta.Name+"/column/manifest")
	if err == nil {
		err = w.buildIterator(1, it, policy)
	}
	err = errors.Join(err, it.Close())
	if err != nil {
		return err
	}
	documents := make([]columnWriteDocument, len(rows))
	for i := range rows {
		documents[i].ID = rows[i].ID
	}
	if err = w.buildDocuments(2, documents, ColumnPublishPlan{UpdatedActiveManifest: manifest.Identity, AppliedCommandLSN: cfg.RecoveryAuthoritativeAppliedCommandLSN}, true, columnPhysicalRowAssetPartID); err != nil {
		return err
	}
	updated, err := columnStoreCompactionUpdatedMeta(w.meta, manifest.Identity)
	if err != nil {
		return err
	}
	requirements, err := stableColumnManifestDurableRequirements(manifest.Records, manifest.Identity.Generation, cfg.AssetManager.Namespace)
	if err != nil {
		return err
	}
	w.meta, w.records, w.requirements = updated, manifest.Records, requirements
	// Public output is copied while its original source/work receipt is live.
	stats := &operation.stats
	stats.Compacted = true
	stats.NewGeneration = manifest.Identity.Generation
	stats.ManifestRoot = w.roots[1].root
	stats.ManifestRecordsAfter = len(manifest.Records)
	stats.RowsCompacted = len(rows)
	stats.AssetsPublished = len(prepared.Assets)
	stats.AggregateMetadataPublished = columnStoreCompactionAggregateMetadataAssets(prepared.Assets)
	stats.PublishedRefs = cloneNativeCompactionRefs(columnStoreCompactionPreparedRefs(prepared.Assets))
	stats.SupersededRefs = cloneNativeCompactionRefs(superseded)
	return operation.ctx.Err()
}
