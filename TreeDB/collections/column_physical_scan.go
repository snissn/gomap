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

type columnPhysicalScanRequest struct {
	ProjectedColumns    []string
	Visitor             func(columnPhysicalScanRowView) error
	RequireInsertOnly   bool
	RefOrdinalModulo    int
	RefOrdinalRemainder int
	ShouldCancel        func() bool
}

type columnPhysicalScanDiagnostics struct {
	ManifestRoot               uint64
	ManifestGeneration         uint64
	RecoveryManifestGeneration uint64
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
	RowMaterializations    int
	PhysicalBytesScanned   int64
	SegmentFileCacheHits   uint64
	SegmentFileCacheMisses uint64
}

type columnPhysicalScanRowView struct {
	Generation        uint64
	PartID            uint64
	AppliedCommandLSN uint64
	Operation         ColumnPublishOperation
	RowIndex          int
	// ID is passed by value but aliases the raw asset buffer; copy to retain it.
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
	Ref    ColumnAssetRef
	Reason ColumnPublishOperation
}

type columnPhysicalScanSnapshotView struct {
	CollectionName     string
	Catalog            *collectionCatalog
	Config             ColumnStoreConfig
	ColumnStoreEnabled bool
	CommitSeq          uint64
	AssetRefs          []columnManifestAssetRefForScan
	GraphAssetRefs     []ColumnAssetRef
	MutationParts      int
	Diagnostics        columnPhysicalScanDiagnostics
	ColumnAssetRootDir string
	AssetNamespace     string
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
	rootName := collectionColumnManifestRootName(collectionName)
	rootID := catalog.rootID(rootName)
	cfgPtr := catalog.meta.Options.ColumnStore
	columnStoreEnabled := cfgPtr != nil
	var cfg ColumnStoreConfig
	if cfgPtr != nil {
		// Collection catalogs are immutable after publication; snapshot views
		// only read the config, so a shallow copy avoids per-scan slice clones.
		cfg = *cfgPtr
	}

	view, err := c.prepareColumnPhysicalScanSnapshotViewAtSnapshot(snap, catalog, collectionName, rootID, cfg, columnStoreEnabled)
	if err != nil {
		closeView()
		return view, nil, err
	}
	if err := ctx.Err(); err != nil {
		closeView()
		return columnPhysicalScanSnapshotView{}, nil, err
	}
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
	if rootID == 0 {
		return columnPhysicalScanSnapshotView{}, fmt.Errorf("collections: physical column scan missing manifest root %q", collectionColumnManifestRootName(collectionName))
	}
	snapshotState := snap.State()
	if snapshotState == nil {
		return columnPhysicalScanSnapshotView{}, backenddb.ErrClosed
	}

	diag := columnPhysicalScanDiagnostics{
		ManifestRoot:               rootID,
		ManifestGeneration:         cfg.ActiveManifest.Generation,
		RecoveryManifestGeneration: cfg.RecoveryAuthoritativeManifest.Generation,
		AppliedCommandLSN:          cfg.RecoveryAuthoritativeAppliedCommandLSN,
	}
	view := columnPhysicalScanSnapshotView{
		CollectionName:     collectionName,
		Catalog:            catalog,
		Config:             cfg,
		ColumnStoreEnabled: columnStoreEnabled,
		CommitSeq:          snapshotState.CommitSeq,
		Diagnostics:        diag,
		ColumnAssetRootDir: c.db.ColumnAssetRootDir(),
		AssetNamespace:     cfg.AssetManager.Namespace,
	}

	if err := validateColumnManifestIdentityAtRoot(snap, rootID, *cfg.ActiveManifest); err != nil {
		view.Diagnostics = diag
		return view, err
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, rootID)
	if err != nil {
		view.Diagnostics = diag
		return view, err
	}
	diag.ManifestRecords = len(records)
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		view.Diagnostics = diag
		return view, err
	}
	if err := validateColumnManifestSnapshot(manifest, records, cfg, *cfg.ActiveManifest, collectionName, "physical column scan"); err != nil {
		view.Diagnostics = diag
		return view, err
	}
	refs, mutationParts, err := columnManifestAssetRefsFromRecordsForScan(records, manifest.Generation, cfg.AssetManager.Namespace)
	if err != nil {
		view.Diagnostics = diag
		return view, err
	}
	graphRefs, err := columnVectorGraphAssetRefsFromManifestRecordsForReachability(records, manifest.Generation, cfg.AssetManager.Namespace)
	if err != nil {
		view.Diagnostics = diag
		return view, err
	}
	diag.AssetRefs = len(refs)
	diag.MutationParts = mutationParts
	view.AssetRefs = refs
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
	readCache, err := newColumnPhysicalAssetReadCache(view.ColumnAssetRootDir, view.AssetNamespace)
	if err != nil {
		return diag, err
	}
	defer func() { _ = readCache.close() }()
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
	if len(projected) == 0 {
		for i := range cfg.Columns {
			outputByColumn[i] = i
		}
		return columnPhysicalScanProjection{
			outputByColumn: outputByColumn,
			values:         make([]columnDeclaredValue, len(cfg.Columns)),
			count:          len(cfg.Columns),
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
			ref, reason, err := decodeColumnManifestPartRefForScan(value, cfg.AssetManager.Namespace)
			if err != nil {
				return columnManifestPlannerCapabilitiesForScan{}, err
			}
			if ref.Generation != keyGeneration {
				return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: column manifest part key generation=%d does not match ref generation=%d", keyGeneration, ref.Generation)
			}
			if ref.PartID != keyPartID {
				return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: column manifest part key part_id=%d does not match ref part_id=%d", keyPartID, ref.PartID)
			}
			operation, ok := columnPhysicalScanOperationFromBytes(reason)
			if !ok {
				return columnManifestPlannerCapabilitiesForScan{}, fmt.Errorf("collections: unsupported column manifest part reason %q", string(reason))
			}
			// Capability counts describe the refs a physical scan would read:
			// all reachable lineage through the active generation. The header
			// expected-parts check below is generation-local and intentionally
			// only counts current-generation records.
			if caps.PhysicalAssetCount == maxCollectionInt {
				return columnManifestPlannerCapabilitiesForScan{}, errors.New("collections: column manifest physical asset count overflows int")
			}
			caps.PhysicalAssetCount++
			if operation != ColumnPublishOperationInsert {
				if caps.MutationParts == maxCollectionInt {
					return columnManifestPlannerCapabilitiesForScan{}, errors.New("collections: column manifest mutation part count overflows int")
				}
				caps.MutationParts++
			}
			writeHashBytes(&d, key)
			writeHashBytes(&d, value)
			if keyGeneration == header.generation {
				activeParts++
			}
		case bytes.HasPrefix(key, columnManifestVectorGraphRecordPrefixBytes):
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
	if version := cur.u16(); version != columnManifestRecordVersion {
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
		case bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes):
			partGeneration, err := columnManifestPartGenerationFromRecordKeyForScan(record.key)
			if err != nil {
				return nil, err
			}
			if partGeneration <= generation {
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
	mutationParts, err := walkColumnManifestAssetRefsFromRecordsForScan(records, activeGeneration, expectedNamespace, func(ref ColumnAssetRef, operation ColumnPublishOperation) error {
		refs = append(refs, columnManifestAssetRefForScan{Ref: ref, Reason: operation})
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return refs, mutationParts, nil
}

func columnManifestMutationPartsFromRecordsForScan(records []columnManifestRecord, activeGeneration uint64, expectedNamespace string) (int, error) {
	return walkColumnManifestAssetRefsFromRecordsForScan(records, activeGeneration, expectedNamespace, nil)
}

func walkColumnManifestAssetRefsFromRecordsForScan(records []columnManifestRecord, activeGeneration uint64, expectedNamespace string, visit func(ColumnAssetRef, ColumnPublishOperation) error) (int, error) {
	mutationParts := 0
	for _, record := range records {
		if !bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			continue
		}
		keyGeneration, keyPartID, err := columnManifestPartKeyFromRecordKeyForScan(record.key)
		if err != nil {
			return 0, err
		}
		// The active manifest root contains the reachable immutable lineage:
		// the original base parts plus later delta/tombstone parts. Older
		// generations are therefore live until compaction publishes a root
		// that omits them; only future-generation records are impossible here.
		if keyGeneration > activeGeneration {
			return 0, fmt.Errorf("collections: column manifest part generation=%d is newer than active manifest generation=%d", keyGeneration, activeGeneration)
		}
		ref, reason, err := decodeColumnManifestPartRefForScan(record.value, expectedNamespace)
		if err != nil {
			return 0, err
		}
		if ref.Generation != keyGeneration {
			return 0, fmt.Errorf("collections: column manifest part key generation=%d does not match ref generation=%d", keyGeneration, ref.Generation)
		}
		if ref.PartID != keyPartID {
			return 0, fmt.Errorf("collections: column manifest part key part_id=%d does not match ref part_id=%d", keyPartID, ref.PartID)
		}
		operation, ok := columnPhysicalScanOperationFromBytes(reason)
		if !ok {
			return 0, fmt.Errorf("collections: unsupported column manifest part reason %q", string(reason))
		}
		if visit != nil {
			if err := visit(ref, operation); err != nil {
				return 0, err
			}
		}
		if operation != ColumnPublishOperationInsert {
			mutationParts++
		}
	}
	return mutationParts, nil
}

func decodeColumnManifestPartRefForScan(raw []byte, expectedNamespace string) (ColumnAssetRef, []byte, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnManifestPartMagic {
		return ColumnAssetRef{}, nil, fmt.Errorf("collections: bad column manifest part magic=0x%08x", magic)
	}
	if version := cur.u16(); version != columnManifestRecordVersion {
		return ColumnAssetRef{}, nil, fmt.Errorf("collections: unsupported column manifest part version=%d", version)
	}
	kindBytes := cur.stringBytes()
	namespaceBytes := cur.stringBytes()
	generation := cur.u64()
	partID := cur.u64()
	fileID64 := cur.u64()
	offset64 := cur.u64()
	length64 := cur.u64()
	checksum64 := cur.u64()
	_ = cur.u64() // bytes; scan only needs the durable asset ref.
	_ = cur.u64() // publish_id
	_ = cur.u64() // generation_id
	reason := cur.stringBytes()
	if err := cur.err; err != nil {
		return ColumnAssetRef{}, nil, err
	}
	if cur.pos != len(raw) {
		return ColumnAssetRef{}, nil, errors.New("collections: trailing bytes in column manifest part record")
	}
	if !columnPhysicalBytesEqualString(kindBytes, string(ColumnAssetKindTCS1PartImage)) {
		return ColumnAssetRef{}, nil, fmt.Errorf("collections: unsupported column manifest part asset kind %q", string(kindBytes))
	}
	if !columnPhysicalBytesEqualString(namespaceBytes, expectedNamespace) {
		return ColumnAssetRef{}, nil, fmt.Errorf("collections: column manifest part namespace=%q want %q", string(namespaceBytes), expectedNamespace)
	}
	if fileID64 > uint64(math.MaxUint32) {
		return ColumnAssetRef{}, nil, errors.New("collections: column manifest part file_id overflows uint32")
	}
	if checksum64 > uint64(math.MaxUint32) {
		return ColumnAssetRef{}, nil, errors.New("collections: column manifest part checksum overflows uint32")
	}
	if offset64 > uint64(math.MaxInt64) || length64 > uint64(math.MaxInt64) {
		return ColumnAssetRef{}, nil, errors.New("collections: column manifest part offsets or byte counts overflow int64")
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  expectedNamespace,
		Generation: generation,
		PartID:     partID,
		FileID:     uint32(fileID64),
		Offset:     int64(offset64),
		Length:     int64(length64),
		Checksum:   uint32(checksum64),
	}
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		return ColumnAssetRef{}, nil, err
	}
	return ref, reason, nil
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
		if cur.err != nil {
			return columnPhysicalAssetScanHeader{}, 0, 0, cur.err
		}
		want := cfg.Columns[colIdx]
		if !columnPhysicalBytesEqualString(name, want.Name) ||
			!columnPhysicalBytesEqualString(path, want.Path) ||
			!columnPhysicalBytesEqualString(valueType, string(want.ValueType)) ||
			nullable != want.Nullable ||
			dictionary != want.Dictionary ||
			vectorDims != want.VectorDims {
			return columnPhysicalAssetScanHeader{}, 0, 0, fmt.Errorf("column physical asset column[%d]={Name:%q Path:%q ValueType:%q Nullable:%t Dictionary:%t VectorDims:%d} want %+v",
				colIdx, string(name), string(path), string(valueType), nullable, dictionary, vectorDims, want)
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
		case ColumnStoreValueFloat32Vector:
			if selected {
				value := cur.float32SliceWithExpectedLength(col.VectorDims)
				if cur.err != nil {
					return cur.err
				}
				rowValues[outputIdx].Float32Vector = value
			} else {
				n := cur.skipUint32Slice()
				if cur.err != nil {
					return cur.err
				}
				if n != uint64(col.VectorDims) {
					return fmt.Errorf("column[%d] float32_vector length=%d want vector_dims=%d", colIdx, n, col.VectorDims)
				}
			}
		case ColumnStoreValueAdjacencyList:
			if selected {
				rowValues[outputIdx].AdjacencyList = cur.uint32Slice()
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
