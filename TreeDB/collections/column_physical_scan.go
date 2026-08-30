package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/cespare/xxhash/v2"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Corrupt manifests can declare arbitrarily large expected_parts values. Cap
// speculative preallocation to a small metadata budget while still letting
// legitimately larger manifests append as records are decoded.
const columnManifestScanPreallocMaxParts = 4096

type columnPhysicalScanRequest struct {
	ProjectedColumns    []string
	Visitor             func(columnPhysicalScanRowView) error
	RequireInsertOnly   bool
	RefOrdinalModulo    int
	RefOrdinalRemainder int
	ShouldCancel        func() bool
	ReadIntegrity       ColumnAssetReadIntegrity
	ReadCache           *columnPhysicalAssetReadCache
}

type columnPhysicalScanDiagnostics struct {
	ManifestRoot               uint64
	ManifestRootName           string
	ManifestGeneration         uint64
	ActiveManifestChecksum     uint64
	RecoveryManifestGeneration uint64
	RecoveryManifestChecksum   uint64
	AppliedCommandLSN          uint64
	ManifestRecords            int
	AssetRefs                  int
	MutationParts              int
	DecodedBlocks              int
	ScheduledGranules          int
	// Reserved for M14 predicate pushdown; M13A schedules every manifest ref.
	SkippedGranules  int
	RowsScanned      int
	DeletedRows      int
	ProjectedColumns int
	// Counts full-document row materialization; M13A emits declared-column row views only.
	RowMaterializations      int
	PhysicalBytesScanned     int64
	SegmentFileCacheHits     uint64
	SegmentFileCacheMisses   uint64
	ColumnAssetReadIntegrity string
}

type columnPhysicalScanRowView struct {
	Generation        uint64
	PartID            uint64
	AppliedCommandLSN uint64
	Operation         ColumnPublishOperation
	RowIndex          int
	// ID is passed by value but aliases scanner-owned storage; copy to retain it.
	ID []byte
	// Values aliases scanner scratch; copy before the visitor returns to retain it.
	Values  []columnDeclaredValue
	Deleted bool
}

type columnPhysicalScanProjection struct {
	outputByColumn []int
	values         []columnDeclaredValue
	count          int
}

type columnPhysicalAssetScanSummary struct {
	rows    int
	deleted int
}

type columnPhysicalAssetScanHeader struct {
	Collection        []byte
	Namespace         []byte
	Generation        uint64
	PartID            uint64
	AppliedCommandLSN uint64
	Operation         ColumnPublishOperation
	SchemaHash        uint64
	RowCount          int
	ColumnCount       int
}

type columnManifestAssetRefForScan struct {
	Ref     ColumnAssetRef
	Reason  ColumnPublishOperation
	Role    ColumnManifestPartRole
	Rows    int
	SortKey []ColumnSortKey
}

type columnManifestScanSidecarFilter struct {
	AggregateMetadata     bool
	AggregateMetadataName string
	DictionaryCodes       bool
	DictionaryColumn      string
	DictionaryColumn2     string
	DictionaryColumns     []string
	Int64Values           bool
	Int64Column           string
}

func columnManifestScanAllSidecars() columnManifestScanSidecarFilter {
	return columnManifestScanSidecarFilter{
		AggregateMetadata: true,
		DictionaryCodes:   true,
		Int64Values:       true,
	}
}

func columnManifestScanNoSidecars() columnManifestScanSidecarFilter {
	return columnManifestScanSidecarFilter{}
}

func (f columnManifestScanSidecarFilter) aggregateMetadataNameForScan(name []byte) (string, bool) {
	if !f.AggregateMetadata {
		return "", false
	}
	if f.AggregateMetadataName == "" {
		return "", true
	}
	if columnManifestBytesEqualString(name, f.AggregateMetadataName) {
		return f.AggregateMetadataName, true
	}
	return "", false
}

func (f columnManifestScanSidecarFilter) dictionaryColumnNameForScan(columnName []byte) (string, bool) {
	if !f.DictionaryCodes {
		return "", false
	}
	if len(f.DictionaryColumns) > 0 {
		for _, column := range f.DictionaryColumns {
			if columnManifestBytesEqualString(columnName, column) {
				return column, true
			}
		}
		return "", false
	}
	if f.DictionaryColumn == "" && f.DictionaryColumn2 == "" {
		return "", true
	}
	if columnManifestBytesEqualString(columnName, f.DictionaryColumn) {
		return f.DictionaryColumn, true
	}
	if f.DictionaryColumn2 != "" && columnManifestBytesEqualString(columnName, f.DictionaryColumn2) {
		return f.DictionaryColumn2, true
	}
	return "", false
}

func (f columnManifestScanSidecarFilter) int64ColumnNameForScan(columnName []byte) (string, bool) {
	if !f.Int64Values {
		return "", false
	}
	if f.Int64Column == "" {
		return "", true
	}
	if columnManifestBytesEqualString(columnName, f.Int64Column) {
		return f.Int64Column, true
	}
	return "", false
}

func columnManifestDictionarySidecarColumns(cfg ColumnStoreConfig) int {
	count := 0
	for _, col := range cfg.Columns {
		if col.Dictionary && col.ValueType == ColumnStoreValueString {
			count++
		}
	}
	return count
}

func columnManifestInt64SidecarColumns(cfg ColumnStoreConfig) int {
	count := 0
	for _, col := range cfg.Columns {
		if col.ValueType == ColumnStoreValueInt64 {
			count++
		}
	}
	return count
}

func columnManifestScanSidecarCapacity(parts, perPart int) int {
	if parts <= 0 || perPart <= 0 {
		return 0
	}
	if parts > maxCollectionInt/perPart {
		return 0
	}
	return parts * perPart
}

func columnManifestScanPartPreallocCapacity(expectedParts uint64) int {
	if expectedParts == 0 {
		return 0
	}
	if expectedParts > uint64(columnManifestScanPreallocMaxParts) {
		return columnManifestScanPreallocMaxParts
	}
	return int(expectedParts)
}

type columnPhysicalScanSnapshotView struct {
	CollectionName        string
	Catalog               *collectionCatalog
	Config                ColumnStoreConfig
	FullConfig            ColumnStoreConfig
	ColumnStoreEnabled    bool
	CommitSeq             uint64
	SystemRoot            uint64
	AssetRefs             []columnManifestAssetRefForScan
	TypedColumnPartRefs   []columnManifestAssetRefForScan
	AggregateMetadata     []columnManifestAggregateMetadataSnapshot
	DictionaryCodes       []columnManifestDictionaryCodesSnapshot
	Int64Values           []columnManifestInt64ValuesSnapshot
	GraphAssetRefs        []ColumnAssetRef
	VectorIndexState      columnVectorIndexStateSnapshot
	VectorIndexStateFound bool
	MutationParts         int
	Diagnostics           columnPhysicalScanDiagnostics
	ColumnAssetRootDir    string
	AssetNamespace        string
	ManifestCatalogBytes  int64
	snapshot              *backenddb.Snapshot
}

func columnPhysicalScanSnapshotViewAssetRefs(view columnPhysicalScanSnapshotView) []ColumnAssetRef {
	expectedRefs := len(view.AssetRefs) + len(view.TypedColumnPartRefs) + len(view.AggregateMetadata) + len(view.DictionaryCodes) + len(view.Int64Values) + len(view.GraphAssetRefs)
	if expectedRefs == 0 {
		return nil
	}
	refs := make([]ColumnAssetRef, 0, expectedRefs)
	seen := make(map[ColumnAssetRef]struct{}, expectedRefs)
	add := func(ref ColumnAssetRef) {
		if _, ok := seen[ref]; ok {
			return
		}
		seen[ref] = struct{}{}
		refs = append(refs, ref)
	}
	for _, assetRef := range view.AssetRefs {
		add(assetRef.Ref)
	}
	for _, typedPartRef := range view.TypedColumnPartRefs {
		add(typedPartRef.Ref)
	}
	for _, metadataRef := range view.AggregateMetadata {
		add(metadataRef.AssetRef)
	}
	for _, dictionaryRef := range view.DictionaryCodes {
		add(dictionaryRef.AssetRef)
	}
	for _, valuesRef := range view.Int64Values {
		add(valuesRef.AssetRef)
	}
	for _, ref := range view.GraphAssetRefs {
		add(ref)
	}
	return refs
}

var errColumnPhysicalAssetManifestOperationMismatch = errors.New("collections: column physical asset operation does not match manifest reason")

func (c *Collection) scanColumnPhysicalRows(req columnPhysicalScanRequest) (columnPhysicalScanDiagnostics, error) {
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotView()
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		return view.Diagnostics, err
	}
	return c.scanColumnPhysicalRowsInSnapshotView(view, req)
}

func (c *Collection) prepareColumnPhysicalScanSnapshotView() (columnPhysicalScanSnapshotView, func(), error) {
	return c.prepareColumnPhysicalScanSnapshotViewWithContext(context.Background())
}

func (c *Collection) prepareColumnPhysicalScanSnapshotViewWithContext(ctx context.Context) (columnPhysicalScanSnapshotView, func(), error) {
	return c.prepareColumnPhysicalScanSnapshotViewWithContextAndSidecars(ctx, columnManifestScanAllSidecars())
}

func (c *Collection) prepareColumnPhysicalScanSnapshotViewWithSidecars(filter columnManifestScanSidecarFilter) (columnPhysicalScanSnapshotView, func(), error) {
	return c.prepareColumnPhysicalScanSnapshotViewWithContextAndSidecars(context.Background(), filter)
}

func (c *Collection) prepareColumnPhysicalScanSnapshotViewWithContextAndSidecars(ctx context.Context, filter columnManifestScanSidecarFilter) (columnPhysicalScanSnapshotView, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return columnPhysicalScanSnapshotView{}, nil, err
	}
	if c == nil {
		return columnPhysicalScanSnapshotView{}, nil, errCollectionNil
	}
	if c.db == nil {
		return columnPhysicalScanSnapshotView{}, nil, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return columnPhysicalScanSnapshotView{}, nil, errCollectionDBNil
	}
	closeView := func() { _ = snap.Close() }
	if err := ctx.Err(); err != nil {
		closeView()
		return columnPhysicalScanSnapshotView{}, nil, err
	}

	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		closeView()
		return columnPhysicalScanSnapshotView{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		closeView()
		return columnPhysicalScanSnapshotView{}, nil, err
	}
	collectionName := catalog.meta.Name
	cfgPtr := catalog.meta.Options.ColumnStore
	columnStoreEnabled := cfgPtr != nil
	var cfg ColumnStoreConfig
	if cfgPtr != nil {
		// Collection catalogs are immutable after publication; snapshot views
		// only read the config, so a shallow copy avoids per-scan slice clones.
		cfg = *cfgPtr
	}
	rootName := catalog.columnManifestRootName
	if rootName == "" && cfg.ManifestRoot != nil {
		rootName = cfg.ManifestRoot.Name
	}
	if rootName == "" {
		rootName = collectionColumnManifestRootName(collectionName)
	}
	rootID := catalog.rootID(rootName)

	view, err := c.prepareColumnPhysicalScanSnapshotViewAtSnapshotWithSidecars(snap, catalog, collectionName, rootID, cfg, columnStoreEnabled, filter)
	if err != nil {
		closeView()
		return view, nil, err
	}
	if err := ctx.Err(); err != nil {
		closeView()
		return columnPhysicalScanSnapshotView{}, nil, err
	}
	view.snapshot = snap
	return view, closeView, nil
}

func (c *Collection) scanColumnPhysicalRowsAtSnapshot(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	collectionName string,
	rootID uint64,
	cfg ColumnStoreConfig,
	columnStoreEnabled bool,
	req columnPhysicalScanRequest,
) (columnPhysicalScanDiagnostics, error) {
	view, err := c.prepareColumnPhysicalScanSnapshotViewAtSnapshot(snap, catalog, collectionName, rootID, cfg, columnStoreEnabled)
	if err != nil {
		return view.Diagnostics, err
	}
	return c.scanColumnPhysicalRowsInSnapshotView(view, req)
}

func (c *Collection) prepareColumnPhysicalScanSnapshotViewAtSnapshot(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	collectionName string,
	rootID uint64,
	cfg ColumnStoreConfig,
	columnStoreEnabled bool,
) (columnPhysicalScanSnapshotView, error) {
	return c.prepareColumnPhysicalScanSnapshotViewAtSnapshotWithSidecars(snap, catalog, collectionName, rootID, cfg, columnStoreEnabled, columnManifestScanAllSidecars())
}

func (c *Collection) prepareColumnPhysicalScanSnapshotViewAtSnapshotWithSidecars(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	collectionName string,
	rootID uint64,
	cfg ColumnStoreConfig,
	columnStoreEnabled bool,
	filter columnManifestScanSidecarFilter,
) (columnPhysicalScanSnapshotView, error) {
	if c == nil {
		return columnPhysicalScanSnapshotView{}, errCollectionNil
	}
	if c.db == nil || snap == nil {
		return columnPhysicalScanSnapshotView{}, errCollectionDBNil
	}
	if catalog == nil {
		return columnPhysicalScanSnapshotView{}, errCollectionNotFound
	}
	if !columnStoreEnabled || !cfg.Enabled {
		return columnPhysicalScanSnapshotView{}, errors.New("collections: physical column scan requires enabled column_store")
	}
	if cfg.ActiveManifest == nil {
		return columnPhysicalScanSnapshotView{}, errors.New("collections: physical column scan requires active column manifest")
	}
	if cfg.RecoveryAuthoritativeManifest == nil {
		return columnPhysicalScanSnapshotView{}, errors.New("collections: physical column scan requires recovery-authoritative column manifest")
	}
	if !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		return columnPhysicalScanSnapshotView{}, errors.New("collections: physical column scan requires active recovery-authoritative manifest")
	}
	if cfg.AssetManager == nil {
		return columnPhysicalScanSnapshotView{}, errors.New("collections: physical column scan requires column asset manager metadata")
	}
	rootName := catalog.columnManifestRootName
	if rootName == "" && cfg.ManifestRoot != nil {
		rootName = cfg.ManifestRoot.Name
	}
	if rootName == "" {
		rootName = collectionColumnManifestRootName(collectionName)
	}
	if rootID == 0 {
		return columnPhysicalScanSnapshotView{}, fmt.Errorf("collections: physical column scan missing manifest root %q", rootName)
	}
	snapshotState, ok := snap.StateToken()
	if !ok {
		return columnPhysicalScanSnapshotView{}, backenddb.ErrClosed
	}

	diag := columnPhysicalScanDiagnostics{
		ManifestRoot:               rootID,
		ManifestRootName:           rootName,
		ManifestGeneration:         cfg.ActiveManifest.Generation,
		ActiveManifestChecksum:     cfg.ActiveManifest.Checksum,
		RecoveryManifestGeneration: cfg.RecoveryAuthoritativeManifest.Generation,
		RecoveryManifestChecksum:   cfg.RecoveryAuthoritativeManifest.Checksum,
		AppliedCommandLSN:          cfg.RecoveryAuthoritativeAppliedCommandLSN,
	}
	rowAssetConfig := columnStoreRowAssetConfig(cfg)
	view := columnPhysicalScanSnapshotView{
		CollectionName:     collectionName,
		Catalog:            catalog,
		Config:             rowAssetConfig,
		FullConfig:         cfg,
		ColumnStoreEnabled: columnStoreEnabled,
		CommitSeq:          snapshotState.CommitSeq,
		SystemRoot:         snapshotState.SystemRootPageID,
		Diagnostics:        diag,
		ColumnAssetRootDir: c.db.ColumnAssetRootDir(),
		AssetNamespace:     cfg.AssetManager.Namespace,
	}

	if err := validateColumnManifestIdentityAtRoot(snap, rootID, *cfg.ActiveManifest); err != nil {
		view.Diagnostics = diag
		return view, err
	}
	manifest, refs, typedColumnPartRefs, graphRefs, mutationParts, manifestRecords, err := loadColumnManifestSnapshotViewForScanFromRootWithSidecars(snap, rootID, rowAssetConfig, *cfg.ActiveManifest, collectionName, filter, true, catalog.meta.VectorIndexes)
	if err != nil {
		diag.ManifestRecords = manifestRecords
		view.Diagnostics = diag
		return view, err
	}
	diag.ManifestRecords = manifestRecords
	diag.AssetRefs = len(refs)
	diag.MutationParts = mutationParts
	view.AssetRefs = refs
	view.TypedColumnPartRefs = typedColumnPartRefs
	view.ManifestCatalogBytes = manifest.ManifestBytes
	view.AggregateMetadata = manifest.AggregateMetadata
	view.DictionaryCodes = manifest.DictionaryCodes
	view.Int64Values = manifest.Int64Values
	view.GraphAssetRefs = graphRefs
	view.MutationParts = mutationParts
	view.Diagnostics = diag
	return view, nil
}

func (c *Collection) scanColumnPhysicalRowsInSnapshotView(
	view columnPhysicalScanSnapshotView,
	req columnPhysicalScanRequest,
) (columnPhysicalScanDiagnostics, error) {
	cfg := view.Config
	diag := view.Diagnostics
	if c == nil {
		return diag, errCollectionNil
	}
	if c.db == nil {
		return diag, errCollectionDBNil
	}
	if !view.ColumnStoreEnabled || !cfg.Enabled {
		return diag, errors.New("collections: physical column scan requires enabled column_store")
	}
	if cfg.ActiveManifest == nil {
		return diag, errors.New("collections: physical column scan requires active column manifest")
	}
	projection, err := newColumnPhysicalScanProjection(cfg, req.ProjectedColumns)
	if err != nil {
		return diag, err
	}
	diag.ProjectedColumns = projection.count
	if req.RequireInsertOnly && view.MutationParts != 0 {
		return diag, errColumnPhysicalQueryNeedsVisibility
	}
	if req.RefOrdinalModulo < 0 {
		return diag, errors.New("collections: physical column scan ref ordinal modulo cannot be negative")
	}
	if req.RefOrdinalModulo == 0 && req.RefOrdinalRemainder != 0 {
		return diag, fmt.Errorf("collections: physical column scan ref ordinal remainder=%d requires non-zero modulo", req.RefOrdinalRemainder)
	}
	if req.RefOrdinalModulo > 0 && (req.RefOrdinalRemainder < 0 || req.RefOrdinalRemainder >= req.RefOrdinalModulo) {
		return diag, fmt.Errorf("collections: physical column scan ref ordinal remainder=%d outside modulo=%d", req.RefOrdinalRemainder, req.RefOrdinalModulo)
	}
	readCache := req.ReadCache
	if readCache == nil {
		localReadCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ReadIntegrity)
		if err != nil {
			return diag, err
		}
		readCache = &localReadCache
		defer func() { _ = readCache.close() }()
	} else if columnAssetReadIntegrityLabel(req.ReadIntegrity) != columnAssetReadIntegrityLabel(readCache.readIntegrity) {
		return diag, fmt.Errorf("collections: physical column scan read cache integrity=%q does not match request integrity=%q", columnAssetReadIntegrityLabel(readCache.readIntegrity), columnAssetReadIntegrityLabel(req.ReadIntegrity))
	}
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(readCache.readIntegrity)
	var rawScratch []byte
	start, step := columnPhysicalScanRefOrdinalPartition(req)
	for ordinal := start; ordinal < len(view.AssetRefs); ordinal += step {
		assetRef := view.AssetRefs[ordinal]
		if req.ShouldCancel != nil && req.ShouldCancel() {
			return diag, errColumnPhysicalScanCancelled
		}
		diag.ScheduledGranules++
		ref := assetRef.Ref
		raw, err := readCache.read(ref, rawScratch)
		diag.SegmentFileCacheHits = readCache.hits
		diag.SegmentFileCacheMisses = readCache.misses
		if err != nil {
			return diag, fmt.Errorf("collections: column physical scan read generation=%d part_id=%d: %w", ref.Generation, ref.PartID, err)
		}
		rawScratch = raw
		diag.PhysicalBytesScanned += int64(len(raw))
		summary, err := scanColumnPhysicalAssetRowsWithManifestOperation(raw, ref, view.CollectionName, &cfg, projection, assetRef.Reason, req.Visitor)
		if err != nil {
			if req.RequireInsertOnly && errors.Is(err, errColumnPhysicalAssetManifestOperationMismatch) {
				return diag, errColumnPhysicalQueryNeedsVisibility
			}
			return diag, fmt.Errorf("collections: column physical scan decode generation=%d part_id=%d: %w", ref.Generation, ref.PartID, err)
		}
		diag.DecodedBlocks++
		diag.RowsScanned += summary.rows
		diag.DeletedRows += summary.deleted
	}
	diag.SegmentFileCacheHits = readCache.hits
	diag.SegmentFileCacheMisses = readCache.misses
	return diag, nil
}

func columnPhysicalScanRefOrdinalPartition(req columnPhysicalScanRequest) (start int, step int) {
	if req.RefOrdinalModulo <= 1 {
		return 0, 1
	}
	return req.RefOrdinalRemainder, req.RefOrdinalModulo
}

func newColumnPhysicalScanProjection(cfg ColumnStoreConfig, projected []string) (columnPhysicalScanProjection, error) {
	outputByColumn := make([]int, len(cfg.Columns))
	for i := range outputByColumn {
		outputByColumn[i] = -1
	}
	if projected == nil {
		for i := range cfg.Columns {
			outputByColumn[i] = i
		}
		return columnPhysicalScanProjection{
			outputByColumn: outputByColumn,
			values:         make([]columnDeclaredValue, len(cfg.Columns)),
			count:          len(cfg.Columns),
		}, nil
	}
	if len(projected) == 0 {
		return columnPhysicalScanProjection{
			outputByColumn: outputByColumn,
			values:         nil,
			count:          0,
		}, nil
	}
	for outIdx, name := range projected {
		if name == "" {
			return columnPhysicalScanProjection{}, errors.New("collections: physical column scan projection contains empty column")
		}
		for prev := 0; prev < outIdx; prev++ {
			if projected[prev] == name {
				return columnPhysicalScanProjection{}, fmt.Errorf("collections: physical column scan duplicate projected column %q", name)
			}
		}
		found := false
		for colIdx, col := range cfg.Columns {
			if col.Name == name {
				outputByColumn[colIdx] = outIdx
				found = true
				break
			}
		}
		if !found {
			return columnPhysicalScanProjection{}, fmt.Errorf("collections: physical column scan requested undeclared column %q", name)
		}
	}
	return columnPhysicalScanProjection{
		outputByColumn: outputByColumn,
		values:         make([]columnDeclaredValue, len(projected)),
		count:          len(projected),
	}, nil
}

func validateColumnManifestIdentityAtRoot(snap *backenddb.Snapshot, rootID uint64, identity ColumnManifestIdentity) error {
	entry, err := snap.GetEntryAtRoot(rootID, newColumnManifestIdentityRecordKey())
	if errors.Is(err, tree.ErrKeyNotFound) {
		return fmt.Errorf("%w: column manifest root %d", ErrColumnManifestIdentityMissing, rootID)
	}
	if err != nil {
		return fmt.Errorf("collections: column manifest root %d identity unreadable: %w", rootID, err)
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return fmt.Errorf("%w: column manifest root %d deleted identity", ErrColumnManifestIdentityMissing, rootID)
	}
	record, err := decodeColumnManifestIdentityRecord(entry.Value)
	if err != nil {
		return fmt.Errorf("collections: column manifest root %d invalid identity: %w", rootID, err)
	}
	if err := validateColumnManifestIdentityFor("active", identity); err != nil {
		return err
	}
	if record.Generation != identity.Generation || record.Version != identity.Version || record.Checksum != identity.Checksum {
		return fmt.Errorf("collections: column manifest identity mismatch root=%+v active=%+v", record, identity)
	}
	return nil
}

func loadColumnManifestRecordsFromRoot(snap *backenddb.Snapshot, rootID uint64) ([]columnManifestRecord, error) {
	iter, err := snap.IteratorAtRoot(rootID, columnManifestHeaderRecordKeyBytes, nil)
	if err != nil {
		return nil, fmt.Errorf("collections: column manifest root %d unreadable: %w", rootID, err)
	}
	defer func() { _ = iter.Close() }()
	records := make([]columnManifestRecord, 0, 8)
	for iter.Valid() {
		key := iter.UnsafeKey()
		if !columnManifestRecordKeyKnownForScan(key) {
			break
		}
		if iter.IsDeleted() {
			iter.Next()
			continue
		}
		value, _, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			return nil, fmt.Errorf("collections: column manifest record %q must be inline", key)
		}
		records = append(records, columnManifestRecord{key: bytes.Clone(key), value: bytes.Clone(value)})
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return records, nil
}

func loadColumnManifestSnapshotViewForScanFromRoot(snap *backenddb.Snapshot, rootID uint64, cfg ColumnStoreConfig, identity ColumnManifestIdentity, collection string, activeVectorIndexesKnown bool, activeVectorIndexes []VectorIndexDefinition) (columnManifestSnapshot, []columnManifestAssetRefForScan, []ColumnAssetRef, int, int, error) {
	manifest, refs, _, graphRefs, mutationParts, manifestRecords, err := loadColumnManifestSnapshotViewForScanFromRootWithSidecars(snap, rootID, cfg, identity, collection, columnManifestScanAllSidecars(), activeVectorIndexesKnown, activeVectorIndexes)
	return manifest, refs, graphRefs, mutationParts, manifestRecords, err
}

func loadColumnManifestSnapshotViewForScanFromRootWithSidecars(snap *backenddb.Snapshot, rootID uint64, cfg ColumnStoreConfig, identity ColumnManifestIdentity, collection string, filter columnManifestScanSidecarFilter, activeVectorIndexesKnown bool, activeVectorIndexes []VectorIndexDefinition) (columnManifestSnapshot, []columnManifestAssetRefForScan, []columnManifestAssetRefForScan, []ColumnAssetRef, int, int, error) {
	iter, err := snap.IteratorAtRoot(rootID, columnManifestHeaderRecordKeyBytes, nil)
	if err != nil {
		return columnManifestSnapshot{}, nil, nil, nil, 0, 0, fmt.Errorf("collections: column manifest root %d unreadable: %w", rootID, err)
	}
	defer func() { _ = iter.Close() }()

	var snapshot columnManifestSnapshot
	var d xxhash.Digest
	d.Reset()
	sawHeader := false
	activeParts := uint64(0)
	manifestRecords := 0
	manifestBytes := int64(len(columnManifestIdentityRecordKey) + columnManifestIdentityRecordSize)
	mutationParts := 0
	var refs []columnManifestAssetRefForScan
	var typedColumnPartRefs []columnManifestAssetRefForScan
	var graphRefs []ColumnAssetRef
	var livePartRows columnManifestLivePartRowsForScan
	for iter.Valid() {
		key := iter.UnsafeKey()
		if !columnManifestRecordKeyKnownForScan(key) {
			break
		}
		if iter.IsDeleted() {
			iter.Next()
			continue
		}
		value, _, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, fmt.Errorf("collections: column manifest record %q must be inline", key)
		}
		manifestRecords++
		manifestBytes = addColumnAssetReachabilityBytes(manifestBytes, int64(len(key)+len(value)))

		switch {
		case bytes.Equal(key, columnManifestHeaderRecordKeyBytes):
			if sawHeader {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, errors.New("collections: duplicate column manifest binary header record")
			}
			header, err := decodeColumnManifestHeaderRecordForScan(value)
			if err != nil {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
			}
			if err := validateColumnManifestHeaderRecordForScan(header, cfg, identity, collection); err != nil {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
			}
			operation, _ := columnPhysicalScanOperationFromBytes(header.operation)
			snapshot = columnManifestSnapshot{
				Collection:         collection,
				Operation:          operation,
				Generation:         header.generation,
				AppliedCommandLSN:  header.appliedCommandLSN,
				SchemaHash:         header.schemaHash,
				ExpectedParts:      header.expectedParts,
				RowCount:           int(header.rowCount),
				CommandBytes:       int64(header.commandBytes),
				RowRemainderBytes:  int64(header.rowRemainderBytes),
				ColumnPayloadBytes: int64(header.columnPayloadBytes),
			}
			if expectedParts := columnManifestScanPartPreallocCapacity(header.expectedParts); expectedParts > 0 {
				refs = make([]columnManifestAssetRefForScan, 0, expectedParts)
				livePartRows.initCapacity(expectedParts)
				if filter.AggregateMetadata {
					if capacity := columnManifestScanSidecarCapacity(expectedParts, len(cfg.AggregateMetadata)); capacity > 0 {
						snapshot.AggregateMetadata = make([]columnManifestAggregateMetadataSnapshot, 0, capacity)
					}
				}
				if filter.DictionaryCodes {
					if capacity := columnManifestScanSidecarCapacity(expectedParts, columnManifestDictionarySidecarColumns(cfg)); capacity > 0 {
						snapshot.DictionaryCodes = make([]columnManifestDictionaryCodesSnapshot, 0, capacity)
					}
				}
				if filter.Int64Values {
					if capacity := columnManifestScanSidecarCapacity(expectedParts, columnManifestInt64SidecarColumns(cfg)); capacity > 0 {
						snapshot.Int64Values = make([]columnManifestInt64ValuesSnapshot, 0, capacity)
					}
				}
			}
			writeHashBytes(&d, header.collection)
			writeHashBytes(&d, header.operation)
			writeHashUint64(&d, header.generation)
			writeHashUint64(&d, header.appliedCommandLSN)
			writeHashUint64(&d, header.schemaHash)
			writeHashBytes(&d, key)
			writeHashBytes(&d, value)
			sawHeader = true
		case bytes.HasPrefix(key, columnManifestPartRecordPrefixBytes):
			if !sawHeader {
				iter.Next()
				continue
			}
			keyGeneration, keyPartID, err := columnManifestPartKeyFromRecordKeyForScan(key)
			if err != nil {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
			}
			if keyGeneration > snapshot.Generation {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, fmt.Errorf("collections: column manifest part generation=%d is newer than header generation=%d", keyGeneration, snapshot.Generation)
			}
			ref, rows, _, _, _, reason, err := decodeColumnManifestPartFieldsForScan(value, cfg.AssetManager.Namespace)
			if err != nil {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
			}
			if ref.Kind != ColumnAssetKindTCS1PartImage && ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, fmt.Errorf("collections: unsupported column manifest part asset kind %q", ref.Kind)
			}
			if ref.Generation != keyGeneration {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, fmt.Errorf("collections: column manifest part key generation=%d does not match ref generation=%d", keyGeneration, ref.Generation)
			}
			if ref.PartID != keyPartID {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, fmt.Errorf("collections: column manifest part key part_id=%d does not match ref part_id=%d", keyPartID, ref.PartID)
			}
			operation, ok := columnPhysicalScanOperationFromBytes(reason)
			if !ok {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, fmt.Errorf("collections: unsupported column manifest part reason %q", string(reason))
			}
			role, err := decodeColumnManifestPartRoleForScan(value, ref, reason)
			if err != nil {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
			}
			sortKey, err := decodeColumnManifestPartSortKeyForScan(value)
			if err != nil {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
			}
			livePartRows.add(ref.Generation, ref.PartID, rows)
			if ref.Kind == ColumnAssetKindTCS1PartImage {
				refs = append(refs, columnManifestAssetRefForScan{Ref: ref, Reason: operation, Role: role, Rows: rows, SortKey: sortKey})
				if role != ColumnManifestPartRoleBase {
					mutationParts++
				}
			} else {
				typedColumnPartRefs = append(typedColumnPartRefs, columnManifestAssetRefForScan{Ref: ref, Reason: operation, Role: role, Rows: rows, SortKey: sortKey})
			}
			if ref.Generation == snapshot.Generation {
				activeParts++
			}
			writeHashBytes(&d, key)
			writeHashBytes(&d, value)
		case bytes.HasPrefix(key, columnManifestAggregateMetadataRecordPrefixBytes):
			if !sawHeader {
				iter.Next()
				continue
			}
			_, _, name, err := columnManifestAggregateMetadataKeyPartsFromRecordKey(key)
			if err != nil {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
			}
			preferredName, include := filter.aggregateMetadataNameForScan(name)
			if !include {
				if err := validateColumnManifestAggregateMetadataRecordForScan(key, value, cfg.AssetManager.Namespace, snapshot.Generation, &livePartRows); err != nil {
					return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
				}
				writeHashBytes(&d, key)
				writeHashBytes(&d, value)
				break
			}
			aggregate, err := decodeColumnManifestAggregateMetadataRecordForScan(key, value, cfg.AssetManager.Namespace, snapshot.Generation, &livePartRows, preferredName)
			if err != nil {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
			}
			snapshot.AggregateMetadata = append(snapshot.AggregateMetadata, aggregate)
			writeHashBytes(&d, key)
			writeHashBytes(&d, value)
		case bytes.HasPrefix(key, columnManifestDictionaryCodesRecordPrefixBytes):
			if !sawHeader {
				iter.Next()
				continue
			}
			_, _, columnName, err := columnManifestDictionaryCodesKeyPartsFromRecordKey(key)
			if err != nil {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
			}
			preferredColumnName, include := filter.dictionaryColumnNameForScan(columnName)
			if !include {
				if err := validateColumnManifestDictionaryCodesRecordForScan(key, value, cfg.AssetManager.Namespace, snapshot.Generation, &livePartRows); err != nil {
					return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
				}
				writeHashBytes(&d, key)
				writeHashBytes(&d, value)
				break
			}
			dictionary, err := decodeColumnManifestDictionaryCodesRecordForScan(key, value, cfg.AssetManager.Namespace, snapshot.Generation, &livePartRows, preferredColumnName)
			if err != nil {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
			}
			snapshot.DictionaryCodes = append(snapshot.DictionaryCodes, dictionary)
			writeHashBytes(&d, key)
			writeHashBytes(&d, value)
		case bytes.HasPrefix(key, columnManifestInt64ValuesRecordPrefixBytes):
			if !sawHeader {
				iter.Next()
				continue
			}
			_, _, columnName, err := columnManifestInt64ValuesKeyPartsFromRecordKey(key)
			if err != nil {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
			}
			preferredColumnName, include := filter.int64ColumnNameForScan(columnName)
			if !include {
				if err := validateColumnManifestInt64ValuesRecordForScan(key, value, cfg.AssetManager.Namespace, snapshot.Generation, &livePartRows); err != nil {
					return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
				}
				writeHashBytes(&d, key)
				writeHashBytes(&d, value)
				break
			}
			values, err := decodeColumnManifestInt64ValuesRecordForScan(key, value, cfg.AssetManager.Namespace, snapshot.Generation, &livePartRows, preferredColumnName)
			if err != nil {
				return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
			}
			snapshot.Int64Values = append(snapshot.Int64Values, values)
			writeHashBytes(&d, key)
			writeHashBytes(&d, value)
		case bytes.HasPrefix(key, columnManifestVectorGraphRecordPrefixBytes):
			if !sawHeader {
				iter.Next()
				continue
			}
			if retainColumnManifestVectorGraphRecordForWrite(key, activeVectorIndexesKnown, activeVectorIndexes) {
				graph, err := decodeColumnVectorGraphManifestRecord(value)
				if err != nil {
					return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
				}
				refs, err := columnVectorGraphManifestAssetRefsForScan(graph, snapshot.Generation, cfg.AssetManager.Namespace)
				if err != nil {
					return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
				}
				graphRefs = append(graphRefs, refs...)
			}
			writeHashBytes(&d, key)
			writeHashBytes(&d, value)
		case bytes.HasPrefix(key, columnVectorIndexStateRecordPrefixBytes):
			if !sawHeader {
				iter.Next()
				continue
			}
			if retainColumnVectorIndexStateRecordForWrite(key, activeVectorIndexesKnown, activeVectorIndexes) {
				state, err := decodeColumnVectorIndexStateRecord(value)
				if err != nil {
					return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
				}
				refs, err := columnVectorIndexStateManifestAssetRefsForScan(state, snapshot.Generation, cfg.AssetManager.Namespace)
				if err != nil {
					return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
				}
				graphRefs = append(graphRefs, refs...)
			}
			writeHashBytes(&d, key)
			writeHashBytes(&d, value)
		}
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, err
	}
	if !sawHeader {
		return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, errors.New("collections: column manifest missing binary header record")
	}
	if activeParts != snapshot.ExpectedParts {
		return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, fmt.Errorf("collections: invalid column manifest part count=%d want %d", activeParts, snapshot.ExpectedParts)
	}
	checksum := d.Sum64()
	if checksum == 0 {
		checksum = 1
	}
	if checksum != identity.Checksum {
		return columnManifestSnapshot{}, nil, nil, nil, 0, manifestRecords, fmt.Errorf("collections: physical column scan manifest checksum=%d want active identity checksum=%d", checksum, identity.Checksum)
	}
	snapshot.ManifestBytes = manifestBytes
	return snapshot, refs, typedColumnPartRefs, graphRefs, mutationParts, manifestRecords, nil
}

func validateColumnManifestSnapshot(snapshot columnManifestSnapshot, records []columnManifestRecord, cfg ColumnStoreConfig, identity ColumnManifestIdentity, collection string, context string) error {
	if snapshot.Collection != collection {
		return fmt.Errorf("collections: %s manifest collection=%q want %q", context, snapshot.Collection, collection)
	}
	if snapshot.Generation != identity.Generation {
		return fmt.Errorf("collections: %s manifest generation=%d want %d", context, snapshot.Generation, identity.Generation)
	}
	if snapshot.SchemaHash != cfg.SchemaHash {
		return fmt.Errorf("collections: %s manifest schema_hash=%d want %d", context, snapshot.SchemaHash, cfg.SchemaHash)
	}
	if snapshot.AppliedCommandLSN != cfg.RecoveryAuthoritativeAppliedCommandLSN {
		return fmt.Errorf("collections: %s manifest AppliedCommandLSN=%d want recovery %d", context, snapshot.AppliedCommandLSN, cfg.RecoveryAuthoritativeAppliedCommandLSN)
	}
	activeRecords, err := activeColumnManifestRecordsForScan(records, snapshot.Generation)
	if err != nil {
		return err
	}
	checksum := checksumColumnManifestRecords(ColumnPublishManifestEncodeInput{
		Collection:        snapshot.Collection,
		ColumnStore:       cfg,
		Operation:         snapshot.Operation,
		AppliedCommandLSN: snapshot.AppliedCommandLSN,
	}, snapshot.Generation, activeRecords)
	if checksum != identity.Checksum {
		return fmt.Errorf("collections: %s manifest checksum=%d want active identity checksum=%d", context, checksum, identity.Checksum)
	}
	return nil
}

type columnManifestPlannerCapabilitiesForScan struct {
	PhysicalAssetCount int
	MutationParts      int
}

type columnManifestHeaderRecordForScan struct {
	collection         []byte
	operation          []byte
	generation         uint64
	appliedCommandLSN  uint64
	schemaHash         uint64
	rowCount           uint64
	commandBytes       uint64
	rowRemainderBytes  uint64
	columnPayloadBytes uint64
	expectedParts      uint64
}

func loadColumnManifestPlannerCapabilitiesForScan(snap *backenddb.Snapshot, rootID uint64, cfg ColumnStoreConfig, identity ColumnManifestIdentity, collection string) (columnManifestPlannerCapabilitiesForScan, error) {
	iter, err := snap.IteratorAtRoot(rootID, columnManifestHeaderRecordKeyBytes, nil)
	if err != nil {
		return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: physical column planner manifest root %d unreadable: %w", rootID, err)
	}
	defer func() { _ = iter.Close() }()

	var caps columnManifestPlannerCapabilitiesForScan
	var header columnManifestHeaderRecordForScan
	var d xxhash.Digest
	d.Reset()
	sawHeader := false
	activeParts := uint64(0)
	var livePartRows columnManifestLivePartRowsForScan
	for iter.Valid() {
		key := iter.UnsafeKey()
		if !columnManifestRecordKeyKnownForScan(key) {
			break
		}
		if iter.IsDeleted() {
			iter.Next()
			continue
		}
		value, _, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: physical column planner manifest record %q must be inline", key)
		}

		switch {
		case bytes.Equal(key, columnManifestHeaderRecordKeyBytes):
			if sawHeader {
				return columnManifestPlannerCapabilitiesForScan{}, errors.New("collections: duplicate column manifest binary header record")
			}
			// The decoded byte slices alias the inline manifest record; the
			// planner scan validates and hashes them before advancing the
			// iterator and never retains them.
			header, err = decodeColumnManifestHeaderRecordForScan(value)
			if err != nil {
				return columnManifestPlannerCapabilitiesForScan{}, err
			}
			if err := validateColumnManifestHeaderRecordForScan(header, cfg, identity, collection); err != nil {
				return columnManifestPlannerCapabilitiesForScan{}, err
			}
			if expectedParts := columnManifestScanPartPreallocCapacity(header.expectedParts); expectedParts > 0 {
				livePartRows.initCapacity(expectedParts)
			}
			writeHashBytes(&d, header.collection)
			writeHashBytes(&d, header.operation)
			writeHashUint64(&d, header.generation)
			writeHashUint64(&d, header.appliedCommandLSN)
			writeHashUint64(&d, header.schemaHash)
			writeHashBytes(&d, key)
			writeHashBytes(&d, value)
			sawHeader = true
		case bytes.HasPrefix(key, columnManifestPartRecordPrefixBytes):
			if !sawHeader {
				iter.Next()
				continue
			}
			keyGeneration, keyPartID, err := columnManifestPartKeyFromRecordKeyForScan(key)
			if err != nil {
				return columnManifestPlannerCapabilitiesForScan{}, err
			}
			if keyGeneration > header.generation {
				return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: column manifest part generation=%d is newer than header generation=%d", keyGeneration, header.generation)
			}
			ref, rows, _, _, _, reason, err := decodeColumnManifestPartFieldsForScan(value, cfg.AssetManager.Namespace)
			if err != nil {
				return columnManifestPlannerCapabilitiesForScan{}, err
			}
			if ref.Kind != ColumnAssetKindTCS1PartImage && ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
				return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: unsupported column manifest part asset kind %q", ref.Kind)
			}
			if ref.Generation != keyGeneration {
				return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: column manifest part key generation=%d does not match ref generation=%d", keyGeneration, ref.Generation)
			}
			if ref.PartID != keyPartID {
				return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: column manifest part key part_id=%d does not match ref part_id=%d", keyPartID, ref.PartID)
			}
			_, ok := columnPhysicalScanOperationFromBytes(reason)
			if !ok {
				return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: unsupported column manifest part reason %q", string(reason))
			}
			role, err := decodeColumnManifestPartRoleForScan(value, ref, reason)
			if err != nil {
				return columnManifestPlannerCapabilitiesForScan{}, err
			}
			livePartRows.add(ref.Generation, ref.PartID, rows)
			if ref.Kind == ColumnAssetKindTCS1PartImage {
				// Capability counts describe the refs a physical scan would read:
				// all reachable lineage through the active generation. The header
				// expected-parts check below is generation-local and counts typed-row
				// locator parts plus typed-column part records.
				if caps.PhysicalAssetCount == maxCollectionInt {
					return columnManifestPlannerCapabilitiesForScan{}, errors.New("collections: column manifest physical asset count overflows int")
				}
				caps.PhysicalAssetCount++
				if role != ColumnManifestPartRoleBase {
					if caps.MutationParts == maxCollectionInt {
						return columnManifestPlannerCapabilitiesForScan{}, errors.New("collections: column manifest mutation part count overflows int")
					}
					caps.MutationParts++
				}
			}
			writeHashBytes(&d, key)
			writeHashBytes(&d, value)
			if keyGeneration == header.generation {
				activeParts++
			}
		case bytes.HasPrefix(key, columnManifestAggregateMetadataRecordPrefixBytes):
			if !sawHeader {
				iter.Next()
				continue
			}
			keyGeneration, _, _, err := columnManifestAggregateMetadataKeyPartsFromRecordKey(key)
			if err != nil {
				return columnManifestPlannerCapabilitiesForScan{}, err
			}
			if keyGeneration > header.generation {
				return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: column manifest aggregate metadata generation=%d is newer than header generation=%d", keyGeneration, header.generation)
			}
			if err := validateColumnManifestAggregateMetadataRecordForScan(key, value, cfg.AssetManager.Namespace, header.generation, &livePartRows); err != nil {
				return columnManifestPlannerCapabilitiesForScan{}, err
			}
			writeHashBytes(&d, key)
			writeHashBytes(&d, value)
		case bytes.HasPrefix(key, columnManifestDictionaryCodesRecordPrefixBytes):
			if !sawHeader {
				iter.Next()
				continue
			}
			keyGeneration, _, _, err := columnManifestDictionaryCodesKeyPartsFromRecordKey(key)
			if err != nil {
				return columnManifestPlannerCapabilitiesForScan{}, err
			}
			if keyGeneration > header.generation {
				return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: column manifest dictionary codes generation=%d is newer than header generation=%d", keyGeneration, header.generation)
			}
			if err := validateColumnManifestDictionaryCodesRecordForScan(key, value, cfg.AssetManager.Namespace, header.generation, &livePartRows); err != nil {
				return columnManifestPlannerCapabilitiesForScan{}, err
			}
			writeHashBytes(&d, key)
			writeHashBytes(&d, value)
		case bytes.HasPrefix(key, columnManifestInt64ValuesRecordPrefixBytes):
			if !sawHeader {
				iter.Next()
				continue
			}
			keyGeneration, _, _, err := columnManifestInt64ValuesKeyPartsFromRecordKey(key)
			if err != nil {
				return columnManifestPlannerCapabilitiesForScan{}, err
			}
			if keyGeneration > header.generation {
				return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: column manifest int64 values generation=%d is newer than header generation=%d", keyGeneration, header.generation)
			}
			if err := validateColumnManifestInt64ValuesRecordForScan(key, value, cfg.AssetManager.Namespace, header.generation, &livePartRows); err != nil {
				return columnManifestPlannerCapabilitiesForScan{}, err
			}
			writeHashBytes(&d, key)
			writeHashBytes(&d, value)
		case bytes.HasPrefix(key, columnManifestVectorGraphRecordPrefixBytes):
			if sawHeader {
				writeHashBytes(&d, key)
				writeHashBytes(&d, value)
			}
		case bytes.HasPrefix(key, columnVectorIndexStateRecordPrefixBytes):
			if sawHeader {
				writeHashBytes(&d, key)
				writeHashBytes(&d, value)
			}
		}
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return columnManifestPlannerCapabilitiesForScan{}, err
	}
	if !sawHeader {
		return columnManifestPlannerCapabilitiesForScan{}, errors.New("collections: column manifest missing binary header record")
	}
	if activeParts != header.expectedParts {
		return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: invalid column manifest part count=%d want %d", activeParts, header.expectedParts)
	}
	checksum := d.Sum64()
	if checksum == 0 {
		checksum = 1
	}
	if checksum != identity.Checksum {
		return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: physical column planner manifest checksum=%d want active identity checksum=%d", checksum, identity.Checksum)
	}
	return caps, nil
}

func decodeColumnManifestHeaderRecordForScan(raw []byte) (columnManifestHeaderRecordForScan, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnManifestHeaderMagic {
		return columnManifestHeaderRecordForScan{}, fmt.Errorf("collections: bad column manifest header magic=0x%08x", magic)
	}
	if version := cur.u16(); !isSupportedColumnManifestRecordVersion(version) {
		return columnManifestHeaderRecordForScan{}, fmt.Errorf("collections: unsupported column manifest header version=%d", version)
	}
	header := columnManifestHeaderRecordForScan{
		collection:         cur.stringBytes(),
		operation:          cur.stringBytes(),
		generation:         cur.u64(),
		appliedCommandLSN:  cur.u64(),
		schemaHash:         cur.u64(),
		rowCount:           cur.u64(),
		commandBytes:       cur.u64(),
		rowRemainderBytes:  cur.u64(),
		columnPayloadBytes: cur.u64(),
		expectedParts:      cur.u64(),
	}
	if err := cur.err; err != nil {
		return columnManifestHeaderRecordForScan{}, err
	}
	if header.rowCount > uint64(maxCollectionInt) {
		return columnManifestHeaderRecordForScan{}, errors.New("collections: column manifest row count overflows int")
	}
	if header.commandBytes > uint64(math.MaxInt64) || header.rowRemainderBytes > uint64(math.MaxInt64) || header.columnPayloadBytes > uint64(math.MaxInt64) {
		return columnManifestHeaderRecordForScan{}, errors.New("collections: column manifest byte counts overflow int64")
	}
	if cur.pos != len(raw) {
		return columnManifestHeaderRecordForScan{}, errors.New("collections: trailing bytes in column manifest header record")
	}
	return header, nil
}

func validateColumnManifestHeaderRecordForScan(header columnManifestHeaderRecordForScan, cfg ColumnStoreConfig, identity ColumnManifestIdentity, collection string) error {
	if !columnPhysicalBytesEqualString(header.collection, collection) {
		return fmt.Errorf("collections: physical column planner manifest collection=%q want %q", string(header.collection), collection)
	}
	if _, ok := columnPhysicalScanOperationFromBytes(header.operation); !ok {
		return fmt.Errorf("collections: unsupported column manifest header operation %q", string(header.operation))
	}
	if header.generation != identity.Generation {
		return fmt.Errorf("collections: physical column planner manifest generation=%d want %d", header.generation, identity.Generation)
	}
	if header.schemaHash != cfg.SchemaHash {
		return fmt.Errorf("collections: physical column planner manifest schema_hash=%d want %d", header.schemaHash, cfg.SchemaHash)
	}
	if header.appliedCommandLSN != cfg.RecoveryAuthoritativeAppliedCommandLSN {
		return fmt.Errorf("collections: physical column planner manifest applied_command_lsn=%d want recovery_authoritative_applied_command_lsn=%d", header.appliedCommandLSN, cfg.RecoveryAuthoritativeAppliedCommandLSN)
	}
	return nil
}

func activeColumnManifestRecordsForScan(records []columnManifestRecord, generation uint64) ([]columnManifestRecord, error) {
	active := make([]columnManifestRecord, 0, len(records))
	for _, record := range records {
		switch {
		case bytes.Equal(record.key, columnManifestHeaderRecordKeyBytes):
			active = append(active, record)
		case bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes):
			active = append(active, record)
		case bytes.HasPrefix(record.key, columnVectorIndexStateRecordPrefixBytes):
			active = append(active, record)
		case bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes):
			partGeneration, err := columnManifestPartGenerationFromRecordKeyForScan(record.key)
			if err != nil {
				return nil, err
			}
			if partGeneration <= generation {
				active = append(active, record)
			}
		case bytes.HasPrefix(record.key, columnManifestAggregateMetadataRecordPrefixBytes):
			metadataGeneration, _, _, err := columnManifestAggregateMetadataKeyPartsFromRecordKey(record.key)
			if err != nil {
				return nil, err
			}
			if metadataGeneration <= generation {
				active = append(active, record)
			}
		case bytes.HasPrefix(record.key, columnManifestDictionaryCodesRecordPrefixBytes):
			dictionaryGeneration, _, _, err := columnManifestDictionaryCodesKeyPartsFromRecordKey(record.key)
			if err != nil {
				return nil, err
			}
			if dictionaryGeneration <= generation {
				active = append(active, record)
			}
		case bytes.HasPrefix(record.key, columnManifestInt64ValuesRecordPrefixBytes):
			valuesGeneration, _, _, err := columnManifestInt64ValuesKeyPartsFromRecordKey(record.key)
			if err != nil {
				return nil, err
			}
			if valuesGeneration <= generation {
				active = append(active, record)
			}
		}
	}
	return active, nil
}

func decodeColumnManifestSnapshotForScan(records []columnManifestRecord) (columnManifestSnapshot, error) {
	var snapshot columnManifestSnapshot
	sawHeader := false
	activeParts := 0
	for _, record := range records {
		switch {
		case bytes.Equal(record.key, columnManifestHeaderRecordKeyBytes):
			if sawHeader {
				return columnManifestSnapshot{}, errors.New("collections: duplicate column manifest binary header record")
			}
			header, err := decodeColumnManifestHeaderRecord(record.value)
			if err != nil {
				return columnManifestSnapshot{}, err
			}
			snapshot = header
			sawHeader = true
		case bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes):
			if !sawHeader {
				continue
			}
			partGeneration, err := columnManifestPartGenerationFromRecordKeyForScan(record.key)
			if err != nil {
				return columnManifestSnapshot{}, err
			}
			if partGeneration > snapshot.Generation {
				return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest part generation=%d is newer than header generation=%d", partGeneration, snapshot.Generation)
			}
			if partGeneration == snapshot.Generation {
				activeParts++
			}
		case bytes.HasPrefix(record.key, columnManifestAggregateMetadataRecordPrefixBytes):
			if !sawHeader {
				continue
			}
			metadataGeneration, _, _, err := columnManifestAggregateMetadataKeyPartsFromRecordKey(record.key)
			if err != nil {
				return columnManifestSnapshot{}, err
			}
			if metadataGeneration > snapshot.Generation {
				return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest aggregate metadata generation=%d is newer than header generation=%d", metadataGeneration, snapshot.Generation)
			}
		case bytes.HasPrefix(record.key, columnManifestDictionaryCodesRecordPrefixBytes):
			if !sawHeader {
				continue
			}
			dictionaryGeneration, _, _, err := columnManifestDictionaryCodesKeyPartsFromRecordKey(record.key)
			if err != nil {
				return columnManifestSnapshot{}, err
			}
			if dictionaryGeneration > snapshot.Generation {
				return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest dictionary codes generation=%d is newer than header generation=%d", dictionaryGeneration, snapshot.Generation)
			}
		case bytes.HasPrefix(record.key, columnManifestInt64ValuesRecordPrefixBytes):
			if !sawHeader {
				continue
			}
			valuesGeneration, _, _, err := columnManifestInt64ValuesKeyPartsFromRecordKey(record.key)
			if err != nil {
				return columnManifestSnapshot{}, err
			}
			if valuesGeneration > snapshot.Generation {
				return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest int64 values generation=%d is newer than header generation=%d", valuesGeneration, snapshot.Generation)
			}
		}
	}
	if !sawHeader {
		return columnManifestSnapshot{}, errors.New("collections: column manifest missing binary header record")
	}
	if uint64(activeParts) != snapshot.ExpectedParts {
		return columnManifestSnapshot{}, fmt.Errorf("collections: invalid column manifest part count=%d want %d", activeParts, snapshot.ExpectedParts)
	}
	return snapshot, nil
}

func decodeColumnManifestSnapshotViewForScan(records []columnManifestRecord, expectedNamespace string) (columnManifestSnapshot, []columnManifestAssetRefForScan, int, error) {
	var snapshot columnManifestSnapshot
	sawHeader := false
	partRecords := 0
	metadataRecords := 0
	dictionaryRecords := 0
	int64ValueRecords := 0
	for _, record := range records {
		switch {
		case bytes.Equal(record.key, columnManifestHeaderRecordKeyBytes):
			if sawHeader {
				return columnManifestSnapshot{}, nil, 0, errors.New("collections: duplicate column manifest binary header record")
			}
			header, err := decodeColumnManifestHeaderRecord(record.value)
			if err != nil {
				return columnManifestSnapshot{}, nil, 0, err
			}
			snapshot = header
			sawHeader = true
		case bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes):
			partRecords++
		case bytes.HasPrefix(record.key, columnManifestAggregateMetadataRecordPrefixBytes):
			metadataRecords++
		case bytes.HasPrefix(record.key, columnManifestDictionaryCodesRecordPrefixBytes):
			dictionaryRecords++
		case bytes.HasPrefix(record.key, columnManifestInt64ValuesRecordPrefixBytes):
			int64ValueRecords++
		}
	}
	if !sawHeader {
		return columnManifestSnapshot{}, nil, 0, errors.New("collections: column manifest missing binary header record")
	}
	if metadataRecords > 0 {
		snapshot.AggregateMetadata = make([]columnManifestAggregateMetadataSnapshot, 0, metadataRecords)
	}
	if dictionaryRecords > 0 {
		snapshot.DictionaryCodes = make([]columnManifestDictionaryCodesSnapshot, 0, dictionaryRecords)
	}
	if int64ValueRecords > 0 {
		snapshot.Int64Values = make([]columnManifestInt64ValuesSnapshot, 0, int64ValueRecords)
	}

	refs := make([]columnManifestAssetRefForScan, 0, partRecords)
	var livePartRows columnManifestLivePartRowsForScan
	livePartRows.initCapacity(partRecords)
	mutationParts := 0
	activeParts := 0
	for _, record := range records {
		if !bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			continue
		}
		keyGeneration, keyPartID, err := columnManifestPartKeyFromRecordKeyForScan(record.key)
		if err != nil {
			return columnManifestSnapshot{}, nil, 0, err
		}
		if keyGeneration > snapshot.Generation {
			return columnManifestSnapshot{}, nil, 0, fmt.Errorf("collections: column manifest part generation=%d is newer than header generation=%d", keyGeneration, snapshot.Generation)
		}
		ref, rows, _, _, _, reason, err := decodeColumnManifestPartFieldsForScan(record.value, expectedNamespace)
		if err != nil {
			return columnManifestSnapshot{}, nil, 0, err
		}
		if ref.Kind != ColumnAssetKindTCS1PartImage && ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
			return columnManifestSnapshot{}, nil, 0, fmt.Errorf("collections: unsupported column manifest part asset kind %q", ref.Kind)
		}
		if ref.Generation != keyGeneration {
			return columnManifestSnapshot{}, nil, 0, fmt.Errorf("collections: column manifest part key generation=%d does not match ref generation=%d", keyGeneration, ref.Generation)
		}
		if ref.PartID != keyPartID {
			return columnManifestSnapshot{}, nil, 0, fmt.Errorf("collections: column manifest part key part_id=%d does not match ref part_id=%d", keyPartID, ref.PartID)
		}
		operation, ok := columnPhysicalScanOperationFromBytes(reason)
		if !ok {
			return columnManifestSnapshot{}, nil, 0, fmt.Errorf("collections: unsupported column manifest part reason %q", string(reason))
		}
		role, err := decodeColumnManifestPartRoleForScan(record.value, ref, reason)
		if err != nil {
			return columnManifestSnapshot{}, nil, 0, err
		}
		livePartRows.add(ref.Generation, ref.PartID, rows)
		if ref.Kind == ColumnAssetKindTCS1PartImage {
			refs = append(refs, columnManifestAssetRefForScan{Ref: ref, Reason: operation, Role: role, Rows: rows})
			if role != ColumnManifestPartRoleBase {
				mutationParts++
			}
		}
		if ref.Generation == snapshot.Generation {
			activeParts++
		}
	}
	for _, record := range records {
		switch {
		case bytes.HasPrefix(record.key, columnManifestAggregateMetadataRecordPrefixBytes):
			aggregate, err := decodeColumnManifestAggregateMetadataRecordForScan(record.key, record.value, expectedNamespace, snapshot.Generation, &livePartRows, "")
			if err != nil {
				return columnManifestSnapshot{}, nil, 0, err
			}
			snapshot.AggregateMetadata = append(snapshot.AggregateMetadata, aggregate)
		case bytes.HasPrefix(record.key, columnManifestDictionaryCodesRecordPrefixBytes):
			dictionary, err := decodeColumnManifestDictionaryCodesRecordForScan(record.key, record.value, expectedNamespace, snapshot.Generation, &livePartRows, "")
			if err != nil {
				return columnManifestSnapshot{}, nil, 0, err
			}
			snapshot.DictionaryCodes = append(snapshot.DictionaryCodes, dictionary)
		case bytes.HasPrefix(record.key, columnManifestInt64ValuesRecordPrefixBytes):
			values, err := decodeColumnManifestInt64ValuesRecordForScan(record.key, record.value, expectedNamespace, snapshot.Generation, &livePartRows, "")
			if err != nil {
				return columnManifestSnapshot{}, nil, 0, err
			}
			snapshot.Int64Values = append(snapshot.Int64Values, values)
		}
	}
	if uint64(activeParts) != snapshot.ExpectedParts {
		return columnManifestSnapshot{}, nil, 0, fmt.Errorf("collections: invalid column manifest part count=%d want %d", activeParts, snapshot.ExpectedParts)
	}
	return snapshot, refs, mutationParts, nil
}

type columnManifestLivePartRowsForScan struct {
	inlineKey  [2]uint64
	inlineRows int
	inlineSet  bool
	rows       map[[2]uint64]int
}

func (r *columnManifestLivePartRowsForScan) initCapacity(parts int) {
	if parts > 1 {
		r.rows = make(map[[2]uint64]int, parts)
	}
}

func (r *columnManifestLivePartRowsForScan) add(generation, partID uint64, rows int) {
	key := [2]uint64{generation, partID}
	if r.rows != nil {
		r.rows[key] = rows
		return
	}
	if !r.inlineSet || r.inlineKey == key {
		r.inlineKey = key
		r.inlineRows = rows
		r.inlineSet = true
		return
	}
	r.rows = make(map[[2]uint64]int, 2)
	r.rows[r.inlineKey] = r.inlineRows
	r.rows[key] = rows
}

func (r *columnManifestLivePartRowsForScan) get(generation, partID uint64) (int, bool) {
	key := [2]uint64{generation, partID}
	if r.rows != nil {
		rows, ok := r.rows[key]
		return rows, ok
	}
	if r.inlineSet && r.inlineKey == key {
		return r.inlineRows, true
	}
	return 0, false
}

func decodeColumnManifestAggregateMetadataRecordForScan(key, raw []byte, expectedNamespace string, activeGeneration uint64, livePartRows *columnManifestLivePartRowsForScan, preferredName string) (columnManifestAggregateMetadataSnapshot, error) {
	generation, partID, name, err := columnManifestAggregateMetadataKeyPartsFromRecordKey(key)
	if err != nil {
		return columnManifestAggregateMetadataSnapshot{}, err
	}
	ref, _, bytesValue, publishID, generationID, reason, err := decodeColumnManifestPartFieldsForScan(raw, expectedNamespace)
	if err != nil {
		return columnManifestAggregateMetadataSnapshot{}, err
	}
	if ref.Kind != ColumnAssetKindTCS1AggregateMetadata {
		return columnManifestAggregateMetadataSnapshot{}, fmt.Errorf("collections: column manifest aggregate metadata asset kind=%q want %q", ref.Kind, ColumnAssetKindTCS1AggregateMetadata)
	}
	if ref.Generation != generation || ref.PartID != partID {
		return columnManifestAggregateMetadataSnapshot{}, fmt.Errorf("collections: column manifest aggregate metadata key generation/part does not match ref")
	}
	if ref.Generation > activeGeneration {
		return columnManifestAggregateMetadataSnapshot{}, fmt.Errorf("collections: column manifest aggregate metadata generation=%d is newer than header generation=%d", ref.Generation, activeGeneration)
	}
	if _, ok := livePartRows.get(ref.Generation, ref.PartID); !ok {
		return columnManifestAggregateMetadataSnapshot{}, fmt.Errorf("collections: column manifest aggregate metadata generation=%d part_id=%d has no matching live part record", ref.Generation, ref.PartID)
	}
	if !bytes.Equal(name, reason) {
		return columnManifestAggregateMetadataSnapshot{}, fmt.Errorf("collections: column manifest aggregate metadata key name=%q does not match record name=%q", string(name), string(reason))
	}
	if preferredName == "" {
		preferredName = string(name)
	}
	return columnManifestAggregateMetadataSnapshot{
		AssetRef:     ref,
		Name:         preferredName,
		Bytes:        bytesValue,
		PublishID:    publishID,
		GenerationID: generationID,
	}, nil
}

func validateColumnManifestAggregateMetadataRecordForScan(key, raw []byte, expectedNamespace string, activeGeneration uint64, livePartRows *columnManifestLivePartRowsForScan) error {
	generation, partID, name, err := columnManifestAggregateMetadataKeyPartsFromRecordKey(key)
	if err != nil {
		return err
	}
	ref, _, _, _, _, reason, err := decodeColumnManifestPartFieldsForScan(raw, expectedNamespace)
	if err != nil {
		return err
	}
	if ref.Kind != ColumnAssetKindTCS1AggregateMetadata {
		return fmt.Errorf("collections: column manifest aggregate metadata asset kind=%q want %q", ref.Kind, ColumnAssetKindTCS1AggregateMetadata)
	}
	if ref.Generation != generation || ref.PartID != partID {
		return fmt.Errorf("collections: column manifest aggregate metadata key generation/part does not match ref")
	}
	if ref.Generation > activeGeneration {
		return fmt.Errorf("collections: column manifest aggregate metadata generation=%d is newer than header generation=%d", ref.Generation, activeGeneration)
	}
	if _, ok := livePartRows.get(ref.Generation, ref.PartID); !ok {
		return fmt.Errorf("collections: column manifest aggregate metadata generation=%d part_id=%d has no matching live part record", ref.Generation, ref.PartID)
	}
	if !bytes.Equal(name, reason) {
		return fmt.Errorf("collections: column manifest aggregate metadata key name=%q does not match record name=%q", string(name), string(reason))
	}
	return nil
}

func decodeColumnManifestDictionaryCodesRecordForScan(key, raw []byte, expectedNamespace string, activeGeneration uint64, livePartRows *columnManifestLivePartRowsForScan, preferredColumnName string) (columnManifestDictionaryCodesSnapshot, error) {
	generation, partID, columnName, err := columnManifestDictionaryCodesKeyPartsFromRecordKey(key)
	if err != nil {
		return columnManifestDictionaryCodesSnapshot{}, err
	}
	ref, _, bytesValue, publishID, generationID, reason, err := decodeColumnManifestPartFieldsForScan(raw, expectedNamespace)
	if err != nil {
		return columnManifestDictionaryCodesSnapshot{}, err
	}
	if ref.Kind != ColumnAssetKindTCS1DictionaryCodes {
		return columnManifestDictionaryCodesSnapshot{}, fmt.Errorf("collections: column manifest dictionary codes asset kind=%q want %q", ref.Kind, ColumnAssetKindTCS1DictionaryCodes)
	}
	if ref.Generation != generation || ref.PartID != partID {
		return columnManifestDictionaryCodesSnapshot{}, fmt.Errorf("collections: column manifest dictionary codes key generation/part does not match ref")
	}
	if ref.Generation > activeGeneration {
		return columnManifestDictionaryCodesSnapshot{}, fmt.Errorf("collections: column manifest dictionary codes generation=%d is newer than header generation=%d", ref.Generation, activeGeneration)
	}
	if _, ok := livePartRows.get(ref.Generation, ref.PartID); !ok {
		return columnManifestDictionaryCodesSnapshot{}, fmt.Errorf("collections: column manifest dictionary codes generation=%d part_id=%d has no matching live part record", ref.Generation, ref.PartID)
	}
	if !bytes.Equal(columnName, reason) {
		return columnManifestDictionaryCodesSnapshot{}, fmt.Errorf("collections: column manifest dictionary codes key column=%q does not match record column=%q", string(columnName), string(reason))
	}
	if preferredColumnName == "" {
		preferredColumnName = string(columnName)
	}
	return columnManifestDictionaryCodesSnapshot{
		AssetRef:     ref,
		ColumnName:   preferredColumnName,
		Bytes:        bytesValue,
		PublishID:    publishID,
		GenerationID: generationID,
	}, nil
}

func validateColumnManifestDictionaryCodesRecordForScan(key, raw []byte, expectedNamespace string, activeGeneration uint64, livePartRows *columnManifestLivePartRowsForScan) error {
	generation, partID, columnName, err := columnManifestDictionaryCodesKeyPartsFromRecordKey(key)
	if err != nil {
		return err
	}
	ref, _, _, _, _, reason, err := decodeColumnManifestPartFieldsForScan(raw, expectedNamespace)
	if err != nil {
		return err
	}
	if ref.Kind != ColumnAssetKindTCS1DictionaryCodes {
		return fmt.Errorf("collections: column manifest dictionary codes asset kind=%q want %q", ref.Kind, ColumnAssetKindTCS1DictionaryCodes)
	}
	if ref.Generation != generation || ref.PartID != partID {
		return fmt.Errorf("collections: column manifest dictionary codes key generation/part does not match ref")
	}
	if ref.Generation > activeGeneration {
		return fmt.Errorf("collections: column manifest dictionary codes generation=%d is newer than header generation=%d", ref.Generation, activeGeneration)
	}
	if _, ok := livePartRows.get(ref.Generation, ref.PartID); !ok {
		return fmt.Errorf("collections: column manifest dictionary codes generation=%d part_id=%d has no matching live part record", ref.Generation, ref.PartID)
	}
	if !bytes.Equal(columnName, reason) {
		return fmt.Errorf("collections: column manifest dictionary codes key column=%q does not match record column=%q", string(columnName), string(reason))
	}
	return nil
}

func decodeColumnManifestInt64ValuesRecordForScan(key, raw []byte, expectedNamespace string, activeGeneration uint64, livePartRows *columnManifestLivePartRowsForScan, preferredColumnName string) (columnManifestInt64ValuesSnapshot, error) {
	generation, partID, columnName, err := columnManifestInt64ValuesKeyPartsFromRecordKey(key)
	if err != nil {
		return columnManifestInt64ValuesSnapshot{}, err
	}
	ref, rows, bytesValue, publishID, generationID, reason, err := decodeColumnManifestPartFieldsForScan(raw, expectedNamespace)
	if err != nil {
		return columnManifestInt64ValuesSnapshot{}, err
	}
	if ref.Kind != ColumnAssetKindTCS1Int64Values {
		return columnManifestInt64ValuesSnapshot{}, fmt.Errorf("collections: column manifest int64 values asset kind=%q want %q", ref.Kind, ColumnAssetKindTCS1Int64Values)
	}
	if ref.Generation != generation || ref.PartID != partID {
		return columnManifestInt64ValuesSnapshot{}, fmt.Errorf("collections: column manifest int64 values key generation/part does not match ref")
	}
	if ref.Generation > activeGeneration {
		return columnManifestInt64ValuesSnapshot{}, fmt.Errorf("collections: column manifest int64 values generation=%d is newer than header generation=%d", ref.Generation, activeGeneration)
	}
	partRows, ok := livePartRows.get(ref.Generation, ref.PartID)
	if !ok {
		return columnManifestInt64ValuesSnapshot{}, fmt.Errorf("collections: column manifest int64 values generation=%d part_id=%d has no matching live part record", ref.Generation, ref.PartID)
	}
	if rows != partRows {
		return columnManifestInt64ValuesSnapshot{}, fmt.Errorf("collections: column manifest int64 values rows=%d does not match part rows=%d", rows, partRows)
	}
	if !bytes.Equal(columnName, reason) {
		return columnManifestInt64ValuesSnapshot{}, fmt.Errorf("collections: column manifest int64 values key column=%q does not match record column=%q", string(columnName), string(reason))
	}
	if preferredColumnName == "" {
		preferredColumnName = string(columnName)
	}
	return columnManifestInt64ValuesSnapshot{
		AssetRef:     ref,
		ColumnName:   preferredColumnName,
		Rows:         rows,
		Bytes:        bytesValue,
		PublishID:    publishID,
		GenerationID: generationID,
	}, nil
}

func validateColumnManifestInt64ValuesRecordForScan(key, raw []byte, expectedNamespace string, activeGeneration uint64, livePartRows *columnManifestLivePartRowsForScan) error {
	generation, partID, columnName, err := columnManifestInt64ValuesKeyPartsFromRecordKey(key)
	if err != nil {
		return err
	}
	ref, rows, _, _, _, reason, err := decodeColumnManifestPartFieldsForScan(raw, expectedNamespace)
	if err != nil {
		return err
	}
	if ref.Kind != ColumnAssetKindTCS1Int64Values {
		return fmt.Errorf("collections: column manifest int64 values asset kind=%q want %q", ref.Kind, ColumnAssetKindTCS1Int64Values)
	}
	if ref.Generation != generation || ref.PartID != partID {
		return fmt.Errorf("collections: column manifest int64 values key generation/part does not match ref")
	}
	if ref.Generation > activeGeneration {
		return fmt.Errorf("collections: column manifest int64 values generation=%d is newer than header generation=%d", ref.Generation, activeGeneration)
	}
	partRows, ok := livePartRows.get(ref.Generation, ref.PartID)
	if !ok {
		return fmt.Errorf("collections: column manifest int64 values generation=%d part_id=%d has no matching live part record", ref.Generation, ref.PartID)
	}
	if rows != partRows {
		return fmt.Errorf("collections: column manifest int64 values rows=%d does not match part rows=%d", rows, partRows)
	}
	if !bytes.Equal(columnName, reason) {
		return fmt.Errorf("collections: column manifest int64 values key column=%q does not match record column=%q", string(columnName), string(reason))
	}
	return nil
}

func columnManifestPartGenerationFromRecordKeyForScan(key []byte) (uint64, error) {
	generation, _, err := columnManifestPartKeyFromRecordKeyForScan(key)
	return generation, err
}

func columnManifestPartKeyFromRecordKeyForScan(key []byte) (uint64, uint64, error) {
	if !bytes.HasPrefix(key, columnManifestPartRecordPrefixBytes) {
		return 0, 0, fmt.Errorf("collections: column manifest part key %q missing prefix", string(key))
	}
	if len(key) != len(columnManifestPartRecordPrefix)+16 {
		return 0, 0, fmt.Errorf("collections: column manifest part key length=%d want %d", len(key), len(columnManifestPartRecordPrefix)+16)
	}
	return binary.BigEndian.Uint64(key[len(columnManifestPartRecordPrefix):]),
		binary.BigEndian.Uint64(key[len(columnManifestPartRecordPrefix)+8:]), nil
}

func columnManifestAssetRefsFromRecordsForScan(records []columnManifestRecord, activeGeneration uint64, expectedNamespace string) ([]columnManifestAssetRefForScan, int, error) {
	refs := make([]columnManifestAssetRefForScan, 0, len(records))
	mutationParts := 0
	for _, record := range records {
		if !bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			continue
		}
		keyGeneration, keyPartID, err := columnManifestPartKeyFromRecordKeyForScan(record.key)
		if err != nil {
			return nil, 0, err
		}
		// The active manifest root contains the reachable immutable lineage:
		// the original base parts plus later delta/tombstone parts. Older
		// generations are therefore live until compaction publishes a root
		// that omits them; only future-generation records are impossible here.
		if keyGeneration > activeGeneration {
			return nil, 0, fmt.Errorf("collections: column manifest part generation=%d is newer than active manifest generation=%d", keyGeneration, activeGeneration)
		}
		ref, rows, _, _, _, reason, err := decodeColumnManifestPartFieldsForScan(record.value, expectedNamespace)
		if err != nil {
			return nil, 0, err
		}
		if ref.Kind == ColumnAssetKindTCS1TypedColumnPart {
			continue
		}
		if ref.Kind != ColumnAssetKindTCS1PartImage {
			return nil, 0, fmt.Errorf("collections: unsupported column manifest part asset kind %q", ref.Kind)
		}
		if ref.Generation != keyGeneration {
			return nil, 0, fmt.Errorf("collections: column manifest part key generation=%d does not match ref generation=%d", keyGeneration, ref.Generation)
		}
		if ref.PartID != keyPartID {
			return nil, 0, fmt.Errorf("collections: column manifest part key part_id=%d does not match ref part_id=%d", keyPartID, ref.PartID)
		}
		operation, ok := columnPhysicalScanOperationFromBytes(reason)
		if !ok {
			return nil, 0, fmt.Errorf("collections: unsupported column manifest part reason %q", string(reason))
		}
		role, err := decodeColumnManifestPartRoleForScan(record.value, ref, reason)
		if err != nil {
			return nil, 0, err
		}
		refs = append(refs, columnManifestAssetRefForScan{Ref: ref, Reason: operation, Role: role, Rows: rows})
		if role != ColumnManifestPartRoleBase {
			mutationParts++
		}
	}
	return refs, mutationParts, nil
}

func decodeColumnManifestPartRefForScan(raw []byte, expectedNamespace string) (ColumnAssetRef, []byte, error) {
	ref, _, _, _, _, reason, err := decodeColumnManifestPartFieldsForScan(raw, expectedNamespace)
	if err != nil {
		return ColumnAssetRef{}, nil, err
	}
	if ref.Kind != ColumnAssetKindTCS1PartImage {
		return ColumnAssetRef{}, nil, fmt.Errorf("collections: unsupported column manifest part asset kind %q", ref.Kind)
	}
	return ref, reason, nil
}

func decodeColumnManifestPartFieldsForScan(raw []byte, expectedNamespace string) (ColumnAssetRef, int, int64, uint64, uint64, []byte, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnManifestPartMagic {
		return ColumnAssetRef{}, 0, 0, 0, 0, nil, fmt.Errorf("collections: bad column manifest part magic=0x%08x", magic)
	}
	version := cur.u16()
	if !isSupportedColumnManifestRecordVersion(version) {
		return ColumnAssetRef{}, 0, 0, 0, 0, nil, fmt.Errorf("collections: unsupported column manifest part version=%d", version)
	}
	kindBytes := cur.stringBytes()
	namespaceBytes := cur.stringBytes()
	generation := cur.u64()
	partID := cur.u64()
	fileID64 := cur.u64()
	offset64 := cur.u64()
	length64 := cur.u64()
	checksum64 := cur.u64()
	rows64 := uint64(0)
	if version >= columnManifestRecordVersionV2 {
		rows64 = cur.u64()
	}
	bytes64 := cur.u64()
	publishID := cur.u64()
	generationID := cur.u64()
	reason := cur.stringBytes()
	if version >= columnManifestRecordVersionV3 {
		_ = cur.stringBytes()
	}
	var sortKey []ColumnSortKey
	if version >= columnManifestRecordVersionV4 {
		sortKey = readColumnManifestSortKey(&cur)
	}
	if err := cur.err; err != nil {
		return ColumnAssetRef{}, 0, 0, 0, 0, nil, err
	}
	if cur.pos != len(raw) {
		return ColumnAssetRef{}, 0, 0, 0, 0, nil, errors.New("collections: trailing bytes in column manifest part record")
	}
	if !columnPhysicalBytesEqualString(namespaceBytes, expectedNamespace) {
		return ColumnAssetRef{}, 0, 0, 0, 0, nil, fmt.Errorf("collections: column manifest part namespace=%q want %q", string(namespaceBytes), expectedNamespace)
	}
	if fileID64 > uint64(math.MaxUint32) {
		return ColumnAssetRef{}, 0, 0, 0, 0, nil, errors.New("collections: column manifest part file_id overflows uint32")
	}
	if checksum64 > uint64(math.MaxUint32) {
		return ColumnAssetRef{}, 0, 0, 0, 0, nil, errors.New("collections: column manifest part checksum overflows uint32")
	}
	if rows64 > uint64(maxCollectionInt) {
		return ColumnAssetRef{}, 0, 0, 0, 0, nil, errors.New("collections: column manifest part rows overflows int")
	}
	if offset64 > uint64(math.MaxInt64) || length64 > uint64(math.MaxInt64) || bytes64 > uint64(math.MaxInt64) {
		return ColumnAssetRef{}, 0, 0, 0, 0, nil, errors.New("collections: column manifest part offsets or byte counts overflow int64")
	}
	kind, ok := columnAssetKindFromBytesForScan(kindBytes)
	if !ok {
		return ColumnAssetRef{}, 0, 0, 0, 0, nil, fmt.Errorf("collections: unsupported column manifest part asset kind %q", string(kindBytes))
	}
	if err := validateColumnManifestPartSortKeyForScan(kind, sortKey); err != nil {
		return ColumnAssetRef{}, 0, 0, 0, 0, nil, err
	}
	ref := ColumnAssetRef{
		Kind:       kind,
		Namespace:  expectedNamespace,
		Generation: generation,
		PartID:     partID,
		FileID:     uint32(fileID64),
		Offset:     int64(offset64),
		Length:     int64(length64),
		Checksum:   uint32(checksum64),
	}
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		return ColumnAssetRef{}, 0, 0, 0, 0, nil, err
	}
	if bytes64 == 0 {
		return ColumnAssetRef{}, 0, 0, 0, 0, nil, errors.New("collections: column prepared asset bytes=0 must be positive")
	}
	if int64(bytes64) != ref.Length {
		return ColumnAssetRef{}, 0, 0, 0, 0, nil, fmt.Errorf("collections: column prepared asset bytes=%d does not match ref length=%d", bytes64, ref.Length)
	}
	return ref, int(rows64), int64(bytes64), publishID, generationID, reason, nil
}

func decodeColumnManifestPartSortKeyForScan(raw []byte) ([]ColumnSortKey, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnManifestPartMagic {
		return nil, fmt.Errorf("collections: bad column manifest part magic=0x%08x", magic)
	}
	version := cur.u16()
	if !isSupportedColumnManifestRecordVersion(version) {
		return nil, fmt.Errorf("collections: unsupported column manifest part version=%d", version)
	}
	kindBytes := cur.stringBytes()
	_ = cur.stringBytes()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.u64()
	if version >= columnManifestRecordVersionV2 {
		_ = cur.u64()
	}
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.stringBytes()
	if version >= columnManifestRecordVersionV3 {
		_ = cur.stringBytes()
	}
	var sortKey []ColumnSortKey
	if version >= columnManifestRecordVersionV4 {
		sortKey = readColumnManifestSortKey(&cur)
	}
	if err := cur.err; err != nil {
		return nil, err
	}
	if cur.pos != len(raw) {
		return nil, errors.New("collections: trailing bytes in column manifest part record")
	}
	kind, ok := columnAssetKindFromBytesForScan(kindBytes)
	if !ok {
		return nil, fmt.Errorf("collections: unsupported column manifest part asset kind %q", string(kindBytes))
	}
	if err := validateColumnManifestPartSortKeyForScan(kind, sortKey); err != nil {
		return nil, err
	}
	return sortKey, nil
}

func validateColumnManifestPartSortKeyForScan(kind ColumnAssetKind, sortKey []ColumnSortKey) error {
	if len(sortKey) != 0 && kind != ColumnAssetKindTCS1TypedColumnPart {
		return fmt.Errorf("collections: column manifest sort key requires %q part records, got %q", ColumnAssetKindTCS1TypedColumnPart, kind)
	}
	if kind == ColumnAssetKindTCS1TypedColumnPart && len(sortKey) > typedColumnPartSortKeyMaxColumns {
		return fmt.Errorf("collections: column manifest sort key columns=%d exceeds cap %d", len(sortKey), typedColumnPartSortKeyMaxColumns)
	}
	seen := make(map[string]struct{}, len(sortKey))
	for _, sortKeyColumn := range sortKey {
		if sortKeyColumn.Column == "" {
			return errors.New("collections: column manifest sort key column is required")
		}
		if _, exists := seen[sortKeyColumn.Column]; exists {
			return fmt.Errorf("collections: column manifest duplicate sort key column %q", sortKeyColumn.Column)
		}
		seen[sortKeyColumn.Column] = struct{}{}
		if sortKeyColumn.Direction != ColumnSortAscending {
			return fmt.Errorf("collections: unsupported column manifest sort direction %q", sortKeyColumn.Direction)
		}
	}
	return nil
}

func decodeColumnManifestPartRoleForScan(raw []byte, ref ColumnAssetRef, reason []byte) (ColumnManifestPartRole, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnManifestPartMagic {
		return "", fmt.Errorf("collections: bad column manifest part magic=0x%08x", magic)
	}
	version := cur.u16()
	if !isSupportedColumnManifestRecordVersion(version) {
		return "", fmt.Errorf("collections: unsupported column manifest part version=%d", version)
	}
	_ = cur.stringBytes()
	_ = cur.stringBytes()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.u64()
	if version >= columnManifestRecordVersionV2 {
		_ = cur.u64()
	}
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.stringBytes()
	role := ColumnManifestPartRole("")
	if version >= columnManifestRecordVersionV3 {
		roleBytes := cur.stringBytes()
		var ok bool
		role, ok = columnManifestPartRoleFromBytesForScan(roleBytes)
		if !ok {
			return "", fmt.Errorf("collections: unsupported column manifest part role %q", string(roleBytes))
		}
	}
	if version >= columnManifestRecordVersionV4 {
		skipColumnManifestSortKey(&cur)
	}
	if err := cur.err; err != nil {
		return "", err
	}
	if cur.pos != len(raw) {
		return "", errors.New("collections: trailing bytes in column manifest part record")
	}
	if role == "" {
		role = inferColumnManifestPartRoleForScan(ref.Kind, reason)
	}
	if err := validateColumnManifestPartRoleForScan(role, ref.Kind, reason); err != nil {
		return "", err
	}
	return role, nil
}

func columnManifestPartRoleFromBytesForScan(raw []byte) (ColumnManifestPartRole, bool) {
	switch {
	case columnPhysicalBytesEqualString(raw, string(ColumnManifestPartRoleBase)):
		return ColumnManifestPartRoleBase, true
	case columnPhysicalBytesEqualString(raw, string(ColumnManifestPartRoleDelta)):
		return ColumnManifestPartRoleDelta, true
	case columnPhysicalBytesEqualString(raw, string(ColumnManifestPartRoleTombstone)):
		return ColumnManifestPartRoleTombstone, true
	default:
		return "", false
	}
}

func inferColumnManifestPartRoleForScan(kind ColumnAssetKind, reason []byte) ColumnManifestPartRole {
	_ = kind
	switch {
	case columnPhysicalBytesEqualString(reason, string(ColumnPublishOperationDelete)):
		return ColumnManifestPartRoleTombstone
	case columnPhysicalBytesEqualString(reason, string(ColumnPublishOperationUpdate)):
		return ColumnManifestPartRoleDelta
	case columnPhysicalBytesEqualString(reason, string(ColumnPublishOperationInsert)):
		return ColumnManifestPartRoleBase
	default:
		return ""
	}
}

func validateColumnManifestPartRoleForScan(role ColumnManifestPartRole, kind ColumnAssetKind, reason []byte) error {
	switch role {
	case ColumnManifestPartRoleBase, ColumnManifestPartRoleDelta, ColumnManifestPartRoleTombstone:
	default:
		return fmt.Errorf("collections: unsupported column manifest part role %q", role)
	}
	operation, ok := columnPhysicalScanOperationFromBytes(reason)
	if !ok {
		return fmt.Errorf("collections: unsupported column manifest part reason %q", string(reason))
	}
	want := inferColumnManifestPartRoleForScan(kind, reason)
	if role != want {
		return fmt.Errorf("collections: column manifest part role=%q does not match operation=%q want role=%q", role, operation, want)
	}
	if role == ColumnManifestPartRoleTombstone && kind != ColumnAssetKindTCS1PartImage {
		return fmt.Errorf("collections: column manifest tombstone role requires %s ref, got %s", ColumnAssetKindTCS1PartImage, kind)
	}
	if role == ColumnManifestPartRoleTombstone && !columnPhysicalBytesEqualString(reason, string(ColumnPublishOperationDelete)) {
		return fmt.Errorf("collections: column manifest tombstone role requires delete reason, got %q", string(reason))
	}
	return nil
}

func columnAssetKindFromBytesForScan(raw []byte) (ColumnAssetKind, bool) {
	switch {
	case columnPhysicalBytesEqualString(raw, string(ColumnAssetKindTCS1PartImage)):
		return ColumnAssetKindTCS1PartImage, true
	case columnPhysicalBytesEqualString(raw, string(ColumnAssetKindTCS1TypedColumnPart)):
		return ColumnAssetKindTCS1TypedColumnPart, true
	case columnPhysicalBytesEqualString(raw, string(ColumnAssetKindTCS1AggregateMetadata)):
		return ColumnAssetKindTCS1AggregateMetadata, true
	case columnPhysicalBytesEqualString(raw, string(ColumnAssetKindTCS1DictionaryCodes)):
		return ColumnAssetKindTCS1DictionaryCodes, true
	case columnPhysicalBytesEqualString(raw, string(ColumnAssetKindTCS1Int64Values)):
		return ColumnAssetKindTCS1Int64Values, true
	default:
		return "", false
	}
}

func scanColumnPhysicalAssetRows(raw []byte, ref ColumnAssetRef, expectedCollection string, cfg *ColumnStoreConfig, projection columnPhysicalScanProjection, visitor func(columnPhysicalScanRowView) error) (columnPhysicalAssetScanSummary, error) {
	return scanColumnPhysicalAssetRowsWithManifestOperation(raw, ref, expectedCollection, cfg, projection, "", visitor)
}

func parseColumnPhysicalAssetScanHeader(raw []byte, ref ColumnAssetRef, expectedCollection string, cfg *ColumnStoreConfig, expectedOperation ColumnPublishOperation) (columnPhysicalAssetScanHeader, uint16, int, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnPhysicalAssetMagic {
		return columnPhysicalAssetScanHeader{}, 0, 0, fmt.Errorf("bad column physical asset magic=0x%08x", magic)
	}
	version := cur.u16()
	if !isSupportedColumnPhysicalAssetVersion(version) {
		return columnPhysicalAssetScanHeader{}, 0, 0, fmt.Errorf("unsupported column physical asset version=%d", version)
	}
	collection := cur.stringBytes()
	namespace := cur.stringBytes()
	generation := cur.u64()
	partID := cur.u64()
	appliedCommandLSN := cur.u64()
	operationBytes := cur.stringBytes()
	operation, operationOK := columnPhysicalScanOperationFromBytes(operationBytes)
	schemaHash := cur.u64()
	columnCount := cur.u64()
	rowCount := cur.u64()
	if err := cur.err; err != nil {
		return columnPhysicalAssetScanHeader{}, 0, 0, err
	}
	if columnCount > uint64(maxCollectionInt) {
		return columnPhysicalAssetScanHeader{}, 0, 0, fmt.Errorf("column physical asset column_count=%d overflows int max=%d", columnCount, maxCollectionInt)
	}
	if rowCount > uint64(maxCollectionInt) {
		return columnPhysicalAssetScanHeader{}, 0, 0, fmt.Errorf("column physical asset row_count=%d overflows int max=%d", rowCount, maxCollectionInt)
	}
	header := columnPhysicalAssetScanHeader{
		Collection:        collection,
		Namespace:         namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: appliedCommandLSN,
		Operation:         operation,
		SchemaHash:        schemaHash,
		ColumnCount:       int(columnCount),
		RowCount:          int(rowCount),
	}
	if !operationOK {
		return columnPhysicalAssetScanHeader{}, 0, 0, fmt.Errorf("unsupported column physical asset operation %q", operationBytes)
	}
	if version == columnPhysicalAssetVersionV1 && header.Operation == ColumnPublishOperationDelete {
		return columnPhysicalAssetScanHeader{}, 0, 0, errors.New("legacy v1 column physical asset delete operation unsupported")
	}
	if err := validateColumnPhysicalAssetScanHeader(header, ref, expectedCollection, cfg); err != nil {
		return columnPhysicalAssetScanHeader{}, 0, 0, err
	}
	if expectedOperation != "" && header.Operation != expectedOperation {
		return columnPhysicalAssetScanHeader{}, 0, 0, fmt.Errorf("%w: manifest reason=%q asset operation=%q", errColumnPhysicalAssetManifestOperationMismatch, expectedOperation, header.Operation)
	}
	if header.ColumnCount != len(cfg.Columns) {
		return columnPhysicalAssetScanHeader{}, 0, 0, fmt.Errorf("column physical asset columns=%d want %d", header.ColumnCount, len(cfg.Columns))
	}
	for colIdx := 0; colIdx < header.ColumnCount; colIdx++ {
		name := cur.stringBytes()
		path := cur.stringBytes()
		valueType := cur.stringBytes()
		nullable := cur.bool()
		dictionary := cur.bool()
		vectorDims := 0
		if version >= columnPhysicalAssetVersionV4 {
			rawVectorDims := cur.u64()
			if rawVectorDims > uint64(maxCollectionInt) {
				return columnPhysicalAssetScanHeader{}, 0, 0, errors.New("column physical asset vector_dims overflows int")
			}
			vectorDims = int(rawVectorDims)
		}
		elementsPerRow := 0
		if version >= columnPhysicalAssetVersionV6 {
			rawElementsPerRow := cur.u64()
			if rawElementsPerRow > uint64(maxCollectionInt) {
				return columnPhysicalAssetScanHeader{}, 0, 0, errors.New("column physical asset elements_per_row overflows int")
			}
			elementsPerRow = int(rawElementsPerRow)
		}
		fixedWidthEncoding := ColumnFixedWidthEncodingDefault
		if version >= columnPhysicalAssetVersionV5 {
			fixedWidthEncoding = ColumnFixedWidthEncoding(string(cur.stringBytes()))
			if _, err := normalizeColumnFixedWidthEncoding(fixedWidthEncoding); err != nil {
				return columnPhysicalAssetScanHeader{}, 0, 0, fmt.Errorf("column physical asset column[%d] fixed_width_encoding: %w", colIdx, err)
			}
			if fixedWidthEncoding != ColumnFixedWidthEncodingDefault && !columnStoreValueTypeSupportsFixedWidthEncoding(ColumnStoreValueType(string(valueType))) {
				return columnPhysicalAssetScanHeader{}, 0, 0, fmt.Errorf("column physical asset column[%d] fixed_width_encoding unsupported for value_type %q", colIdx, string(valueType))
			}
			if fixedWidthEncoding != ColumnFixedWidthEncodingDefault && columnStoreValueTypeHasScalarFixedWidthPayload(ColumnStoreValueType(string(valueType))) {
				return columnPhysicalAssetScanHeader{}, 0, 0, fmt.Errorf("column physical asset column[%d] scalar fixed_width_encoding unsupported for value_type %q", colIdx, string(valueType))
			}
		}
		if cur.err != nil {
			return columnPhysicalAssetScanHeader{}, 0, 0, cur.err
		}
		want := cfg.Columns[colIdx]
		gotValueType := ColumnStoreValueType(string(valueType))
		geometryMatches := vectorDims == want.VectorDims && elementsPerRow == want.ElementsPerRow
		if gotValueType == ColumnStoreValueFloat32Vector && want.ValueType == ColumnStoreValueFloat32Vector {
			gotWidth := columnStoreFloat32VectorElementsPerRow(ColumnStoreColumn{ValueType: gotValueType, VectorDims: vectorDims, ElementsPerRow: elementsPerRow})
			geometryMatches = gotWidth == columnStoreFloat32VectorElementsPerRow(want)
		}
		if !columnPhysicalBytesEqualString(name, want.Name) ||
			!columnPhysicalBytesEqualString(path, want.Path) ||
			gotValueType != want.ValueType ||
			nullable != want.Nullable ||
			dictionary != want.Dictionary ||
			!geometryMatches ||
			fixedWidthEncoding != want.FixedWidthEncoding {
			return columnPhysicalAssetScanHeader{}, 0, 0, fmt.Errorf("column physical asset column[%d]={Name:%q Path:%q ValueType:%q Nullable:%t Dictionary:%t VectorDims:%d ElementsPerRow:%d FixedWidthEncoding:%q} want %+v",
				colIdx, string(name), string(path), string(valueType), nullable, dictionary, vectorDims, elementsPerRow, fixedWidthEncoding, want)
		}
	}
	return header, version, cur.pos, nil
}

func scanColumnPhysicalAssetRowsWithManifestOperation(raw []byte, ref ColumnAssetRef, expectedCollection string, cfg *ColumnStoreConfig, projection columnPhysicalScanProjection, expectedOperation ColumnPublishOperation, visitor func(columnPhysicalScanRowView) error) (columnPhysicalAssetScanSummary, error) {
	header, version, rowsOffset, err := parseColumnPhysicalAssetScanHeader(raw, ref, expectedCollection, cfg, expectedOperation)
	if err != nil {
		return columnPhysicalAssetScanSummary{}, err
	}
	cur := manifestCursor{raw: raw, pos: rowsOffset}
	if version >= columnPhysicalAssetVersionV7 {
		return scanColumnPhysicalAssetEncodedIDRows(&cur, raw, version, header, visitor)
	}
	valuesBuf := projection.values
	var summary columnPhysicalAssetScanSummary
	for rowIdx := 0; rowIdx < header.RowCount; rowIdx++ {
		id := cur.bytesView()
		deleted := false
		if version >= columnPhysicalAssetVersionV2 {
			deleted = cur.bool()
		}
		if cur.err != nil {
			return columnPhysicalAssetScanSummary{}, cur.err
		}
		rowValues := valuesBuf[:0]
		if deleted {
			// Delete assets encode no column values; the operation check enforces that contract.
			if header.Operation != ColumnPublishOperationDelete {
				return columnPhysicalAssetScanSummary{}, fmt.Errorf("column physical asset %s row[%d] is marked deleted", header.Operation, rowIdx)
			}
			summary.deleted++
		} else {
			if header.Operation == ColumnPublishOperationDelete {
				return columnPhysicalAssetScanSummary{}, fmt.Errorf("column physical asset delete row[%d] is not marked deleted", rowIdx)
			}
			rowValues = valuesBuf[:projection.count]
			if err := scanColumnPhysicalRowValues(&cur, version, cfg, projection, rowValues); err != nil {
				return columnPhysicalAssetScanSummary{}, fmt.Errorf("row[%d]: %w", rowIdx, err)
			}
		}
		summary.rows++
		if visitor != nil {
			// ID and Values alias the asset buffer and scanner scratch; visitors must copy to retain them.
			if err := visitor(columnPhysicalScanRowView{
				Generation:        header.Generation,
				PartID:            header.PartID,
				AppliedCommandLSN: header.AppliedCommandLSN,
				Operation:         header.Operation,
				RowIndex:          rowIdx,
				ID:                id,
				Deleted:           deleted,
				Values:            rowValues,
			}); err != nil {
				return columnPhysicalAssetScanSummary{}, err
			}
		}
	}
	if cur.err != nil {
		return columnPhysicalAssetScanSummary{}, cur.err
	}
	if cur.pos != len(raw) {
		return columnPhysicalAssetScanSummary{}, errors.New("trailing bytes in column physical asset")
	}
	return summary, nil
}

func scanColumnPhysicalAssetEncodedIDRows(cur *manifestCursor, raw []byte, version uint16, header columnPhysicalAssetScanHeader, visitor func(columnPhysicalScanRowView) error) (columnPhysicalAssetScanSummary, error) {
	rowEncoding := cur.string()
	if header.ColumnCount != 0 {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("column physical asset row encoding %q requires zero columns", rowEncoding)
	}
	deleted := header.Operation == ColumnPublishOperationDelete
	if header.Operation != ColumnPublishOperationInsert && header.Operation != ColumnPublishOperationUpdate && header.Operation != ColumnPublishOperationDelete {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("unsupported column physical asset operation %q", header.Operation)
	}
	switch rowEncoding {
	case columnPhysicalAssetRowEncodingFixedID:
		return scanColumnPhysicalAssetFixedIDRows(cur, raw, header, deleted, visitor)
	case columnPhysicalAssetRowEncodingDenseIDRange:
		if version < columnPhysicalAssetVersionV8 {
			return columnPhysicalAssetScanSummary{}, fmt.Errorf("column physical asset row encoding %q requires version >= %d", rowEncoding, columnPhysicalAssetVersionV8)
		}
		return scanColumnPhysicalAssetDenseIDRangeRows(cur, header, deleted, visitor)
	default:
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("unsupported column physical asset row encoding %q", rowEncoding)
	}
}

func scanColumnPhysicalAssetFixedIDRows(cur *manifestCursor, raw []byte, header columnPhysicalAssetScanHeader, deleted bool, visitor func(columnPhysicalScanRowView) error) (columnPhysicalAssetScanSummary, error) {
	idWidth := cur.u64()
	if cur.err != nil {
		return columnPhysicalAssetScanSummary{}, cur.err
	}
	if idWidth == 0 || idWidth > uint64(maxCollectionInt) {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("column physical asset fixed id width=%d invalid", idWidth)
	}
	var summary columnPhysicalAssetScanSummary
	for rowIdx := 0; rowIdx < header.RowCount; rowIdx++ {
		if uint64(len(raw)-cur.pos) < idWidth {
			return columnPhysicalAssetScanSummary{}, errors.New("short column physical asset fixed row id block")
		}
		id := raw[cur.pos : cur.pos+int(idWidth)]
		cur.pos += int(idWidth)
		summary.rows++
		if deleted {
			summary.deleted++
		}
		if visitor != nil {
			if err := visitor(columnPhysicalScanRowView{
				Generation:        header.Generation,
				PartID:            header.PartID,
				AppliedCommandLSN: header.AppliedCommandLSN,
				Operation:         header.Operation,
				RowIndex:          rowIdx,
				ID:                id,
				Deleted:           deleted,
			}); err != nil {
				return columnPhysicalAssetScanSummary{}, err
			}
		}
	}
	if cur.err != nil {
		return columnPhysicalAssetScanSummary{}, cur.err
	}
	if cur.pos != len(raw) {
		return columnPhysicalAssetScanSummary{}, errors.New("trailing bytes in column physical asset")
	}
	return summary, nil
}

func scanColumnPhysicalAssetDenseIDRangeRows(cur *manifestCursor, header columnPhysicalAssetScanHeader, deleted bool, visitor func(columnPhysicalScanRowView) error) (columnPhysicalAssetScanSummary, error) {
	baseID := cur.u64()
	if cur.err != nil {
		return columnPhysicalAssetScanSummary{}, cur.err
	}
	if header.RowCount > 0 && baseID > ^uint64(0)-uint64(header.RowCount-1) {
		return columnPhysicalAssetScanSummary{}, errors.New("column physical asset dense id range overflows uint64")
	}
	if cur.pos != len(cur.raw) {
		return columnPhysicalAssetScanSummary{}, errors.New("trailing bytes in column physical asset")
	}
	var summary columnPhysicalAssetScanSummary
	idScratch := make([]byte, 8)
	for rowIdx := 0; rowIdx < header.RowCount; rowIdx++ {
		binary.BigEndian.PutUint64(idScratch, baseID+uint64(rowIdx))
		summary.rows++
		if deleted {
			summary.deleted++
		}
		if visitor != nil {
			if err := visitor(columnPhysicalScanRowView{
				Generation:        header.Generation,
				PartID:            header.PartID,
				AppliedCommandLSN: header.AppliedCommandLSN,
				Operation:         header.Operation,
				RowIndex:          rowIdx,
				ID:                idScratch,
				Deleted:           deleted,
			}); err != nil {
				return columnPhysicalAssetScanSummary{}, err
			}
		}
	}
	return summary, nil
}

func validateColumnPhysicalAssetScanHeader(header columnPhysicalAssetScanHeader, ref ColumnAssetRef, expectedCollection string, cfg *ColumnStoreConfig) error {
	if cfg == nil {
		return errors.New("column physical asset scan requires column store config")
	}
	if cfg.AssetManager == nil {
		return errors.New("column physical asset scan requires asset manager")
	}
	if !columnPhysicalBytesEqualString(header.Collection, expectedCollection) {
		return fmt.Errorf("column physical asset collection=%q want %q", string(header.Collection), expectedCollection)
	}
	if !columnPhysicalBytesEqualString(header.Namespace, cfg.AssetManager.Namespace) || ref.Namespace != cfg.AssetManager.Namespace {
		return fmt.Errorf("column physical asset namespace=%q ref_namespace=%q want %q", string(header.Namespace), ref.Namespace, cfg.AssetManager.Namespace)
	}
	if header.Generation != ref.Generation {
		return fmt.Errorf("column physical asset generation=%d does not match ref generation=%d", header.Generation, ref.Generation)
	}
	if header.PartID != ref.PartID {
		return fmt.Errorf("column physical asset part_id=%d does not match ref part_id=%d", header.PartID, ref.PartID)
	}
	if header.SchemaHash != cfg.SchemaHash {
		return fmt.Errorf("column physical asset schema_hash=%d want %d", header.SchemaHash, cfg.SchemaHash)
	}
	if !isSupportedColumnPhysicalAssetOperation(header.Operation) {
		return fmt.Errorf("unsupported column physical asset operation %q", header.Operation)
	}
	return nil
}

func columnPhysicalScanOperationFromBytes(raw []byte) (ColumnPublishOperation, bool) {
	switch {
	case columnPhysicalBytesEqualString(raw, string(ColumnPublishOperationInsert)):
		return ColumnPublishOperationInsert, true
	case columnPhysicalBytesEqualString(raw, string(ColumnPublishOperationUpdate)):
		return ColumnPublishOperationUpdate, true
	case columnPhysicalBytesEqualString(raw, string(ColumnPublishOperationDelete)):
		return ColumnPublishOperationDelete, true
	default:
		return "", false
	}
}

func scanColumnPhysicalRowValues(cur *manifestCursor, version uint16, cfg *ColumnStoreConfig, projection columnPhysicalScanProjection, rowValues []columnDeclaredValue) error {
	for colIdx, col := range cfg.Columns {
		typeBytes := cur.stringBytes()
		if cur.err != nil {
			return cur.err
		}
		if !columnPhysicalBytesEqualString(typeBytes, string(col.ValueType)) {
			return fmt.Errorf("column[%d] type=%q want %q", colIdx, string(typeBytes), col.ValueType)
		}
		null := cur.bool()
		if cur.err != nil {
			return cur.err
		}
		present := true
		if version >= columnPhysicalAssetVersionV3 {
			present = cur.bool()
			if cur.err != nil {
				return cur.err
			}
		}
		outputIdx := projection.outputByColumn[colIdx]
		selected := outputIdx >= 0
		if selected {
			rowValues[outputIdx] = columnDeclaredValue{
				Type:    col.ValueType,
				Present: present,
				Null:    null,
			}
		}
		if !present {
			if !null {
				return fmt.Errorf("column[%d] absent value is not null", colIdx)
			}
			if !col.Nullable {
				return fmt.Errorf("column[%d] is absent but column is not nullable", colIdx)
			}
			continue
		}
		if null {
			if !col.Nullable {
				return fmt.Errorf("column[%d] is null but column is not nullable", colIdx)
			}
			continue
		}
		switch col.ValueType {
		case ColumnStoreValueBool:
			value := cur.bool()
			if selected {
				rowValues[outputIdx].Bool = value
			}
		case ColumnStoreValueInt64:
			value := int64(cur.u64())
			if selected {
				rowValues[outputIdx].Int64 = value
			}
		case ColumnStoreValueFloat32:
			value := math.Float32frombits(cur.u32())
			if selected {
				rowValues[outputIdx].Float32 = value
			}
		case ColumnStoreValueDouble:
			value := math.Float64frombits(cur.u64())
			if selected {
				rowValues[outputIdx].Double = value
			}
		case ColumnStoreValueString:
			if selected {
				rowValues[outputIdx].StringBytes = cur.stringBytes()
			} else {
				_ = cur.stringBytes()
			}
		case ColumnStoreValueInt8:
			value := int8(cur.u8())
			if selected {
				rowValues[outputIdx].Int8 = value
			}
		case ColumnStoreValueUint8:
			value := cur.u8()
			if selected {
				rowValues[outputIdx].Uint8 = value
			}
		case ColumnStoreValueInt16:
			value := int16(cur.u16())
			if selected {
				rowValues[outputIdx].Int16 = value
			}
		case ColumnStoreValueUint16:
			value := cur.u16()
			if selected {
				rowValues[outputIdx].Uint16 = value
			}
		case ColumnStoreValueInt32:
			value := int32(cur.u32())
			if selected {
				rowValues[outputIdx].Int32 = value
			}
		case ColumnStoreValueUint32:
			value := cur.u32()
			if selected {
				rowValues[outputIdx].Uint32 = value
			}
		case ColumnStoreValueUint64:
			value := cur.u64()
			if selected {
				rowValues[outputIdx].Uint64 = value
			}
		case ColumnStoreValueFloat16:
			value := cur.u16()
			if selected {
				rowValues[outputIdx].Float16 = value
			}
		case ColumnStoreValueBFloat16:
			value := cur.u16()
			if selected {
				rowValues[outputIdx].BFloat16 = value
			}
		case ColumnStoreValueFloat32Vector:
			if selected {
				value := cur.float32SliceWithExpectedLengthAndEncoding(columnStoreFloat32VectorElementsPerRow(col), col.FixedWidthEncoding)
				if cur.err != nil {
					return cur.err
				}
				rowValues[outputIdx].Float32Vector = value
			} else {
				cur.skipFloat32SliceWithExpectedLength(columnStoreFloat32VectorElementsPerRow(col))
				if cur.err != nil {
					return cur.err
				}
			}
		case ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector, ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector, ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector, ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector, ColumnStoreValueFloat64Vector:
			if selected {
				rowValues[outputIdx].DenseNumericVector = cur.denseNumericVectorBytesWithExpectedLength(col)
				if cur.err != nil {
					return cur.err
				}
			} else {
				cur.skipDenseNumericVectorBytesWithExpectedLength(col)
				if cur.err != nil {
					return cur.err
				}
			}
		case ColumnStoreValueUint32List:
			if selected {
				rowValues[outputIdx].Uint32List = cur.uint32SliceWithEncoding(col.FixedWidthEncoding)
				if cur.err != nil {
					return cur.err
				}
			} else {
				cur.skipUint32Slice()
				if cur.err != nil {
					return cur.err
				}
			}
		case ColumnStoreValueAdjacencyList:
			if selected {
				rowValues[outputIdx].AdjacencyList = cur.uint32SliceWithEncoding(col.FixedWidthEncoding)
				if cur.err != nil {
					return cur.err
				}
			} else {
				cur.skipUint32Slice()
				if cur.err != nil {
					return cur.err
				}
			}
		default:
			return fmt.Errorf("unsupported column physical value type %q", col.ValueType)
		}
		if cur.err != nil {
			return cur.err
		}
	}
	return nil
}

func columnPhysicalBytesEqualString(b []byte, s string) bool {
	// Keep the scanner's success path independent of []byte->string conversion allocation behavior.
	if len(b) != len(s) {
		return false
	}
	for i := range b {
		if b[i] != s[i] {
			return false
		}
	}
	return true
}
