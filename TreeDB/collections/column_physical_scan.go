package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

type columnPhysicalScanRequest struct {
	ProjectedColumns  []string
	Visitor           func(columnPhysicalScanRowView) error
	RequireInsertOnly bool
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
	RowMaterializations  int
	PhysicalBytesScanned int64
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

var errColumnPhysicalAssetManifestOperationMismatch = errors.New("collections: column physical asset operation does not match manifest reason")

func (c *Collection) scanColumnPhysicalRows(req columnPhysicalScanRequest) (columnPhysicalScanDiagnostics, error) {
	if c == nil {
		return columnPhysicalScanDiagnostics{}, errCollectionNil
	}
	if c.db == nil {
		return columnPhysicalScanDiagnostics{}, errCollectionDBNil
	}
	c.catalogMu.RLock()
	catalog := c.catalog
	if catalog == nil {
		c.catalogMu.RUnlock()
		return columnPhysicalScanDiagnostics{}, errCollectionNotFound
	}
	collectionName := catalog.meta.Name
	rootName := collectionColumnManifestRootName(collectionName)
	rootID := catalog.rootID(rootName)
	cfgPtr := catalog.meta.Options.ColumnStore
	columnStoreEnabled := cfgPtr != nil
	var cfg ColumnStoreConfig
	if cfgPtr != nil {
		cfg = cfgPtr.copy()
	}
	c.catalogMu.RUnlock()

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return columnPhysicalScanDiagnostics{}, errCollectionDBNil
	}
	defer func() { _ = snap.Close() }()
	return c.scanColumnPhysicalRowsAtSnapshot(snap, catalog, collectionName, rootID, cfg, columnStoreEnabled, req)
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
	if c == nil {
		return columnPhysicalScanDiagnostics{}, errCollectionNil
	}
	if c.db == nil || snap == nil {
		return columnPhysicalScanDiagnostics{}, errCollectionDBNil
	}
	if catalog == nil {
		return columnPhysicalScanDiagnostics{}, errCollectionNotFound
	}
	if !columnStoreEnabled || !cfg.Enabled {
		return columnPhysicalScanDiagnostics{}, errors.New("collections: physical column scan requires enabled column_store")
	}
	if cfg.ActiveManifest == nil {
		return columnPhysicalScanDiagnostics{}, errors.New("collections: physical column scan requires active column manifest")
	}
	if cfg.RecoveryAuthoritativeManifest == nil {
		return columnPhysicalScanDiagnostics{}, errors.New("collections: physical column scan requires recovery-authoritative column manifest")
	}
	if !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		return columnPhysicalScanDiagnostics{}, errors.New("collections: physical column scan requires active recovery-authoritative manifest")
	}
	if cfg.AssetManager == nil {
		return columnPhysicalScanDiagnostics{}, errors.New("collections: physical column scan requires column asset manager metadata")
	}
	if rootID == 0 {
		return columnPhysicalScanDiagnostics{}, fmt.Errorf("collections: physical column scan missing manifest root %q", collectionColumnManifestRootName(collectionName))
	}
	projection, err := newColumnPhysicalScanProjection(cfg, req.ProjectedColumns)
	if err != nil {
		return columnPhysicalScanDiagnostics{}, err
	}

	diag := columnPhysicalScanDiagnostics{
		ManifestRoot:               rootID,
		ManifestGeneration:         cfg.ActiveManifest.Generation,
		RecoveryManifestGeneration: cfg.RecoveryAuthoritativeManifest.Generation,
		AppliedCommandLSN:          cfg.RecoveryAuthoritativeAppliedCommandLSN,
		ProjectedColumns:           projection.count,
	}

	if err := validateColumnManifestIdentityAtRootForScan(snap, rootID, *cfg.ActiveManifest); err != nil {
		return diag, err
	}
	records, err := loadColumnManifestRecordsFromRootForScan(snap, rootID)
	if err != nil {
		return diag, err
	}
	diag.ManifestRecords = len(records)
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		return diag, err
	}
	if err := validateColumnManifestSnapshotForScan(manifest, records, cfg, *cfg.ActiveManifest, collectionName); err != nil {
		return diag, err
	}
	refs, mutationParts, err := columnManifestAssetRefsFromRecordsForScan(records, manifest.Generation, cfg.AssetManager.Namespace)
	if err != nil {
		return diag, err
	}
	diag.AssetRefs = len(refs)
	diag.MutationParts = mutationParts
	diag.ScheduledGranules = len(refs)
	if req.RequireInsertOnly && mutationParts != 0 {
		return diag, errColumnPhysicalQueryNeedsVisibility
	}
	columnAssetRootDir := c.db.ColumnAssetRootDir()
	readCache, err := newColumnPhysicalAssetReadCache(columnAssetRootDir, cfg.AssetManager.Namespace)
	if err != nil {
		return diag, err
	}
	defer func() { _ = readCache.close() }()
	var rawScratch []byte
	for _, assetRef := range refs {
		ref := assetRef.Ref
		if ref.Generation > cfg.ActiveManifest.Generation {
			return diag, fmt.Errorf("collections: column physical scan ref generation=%d is newer than active manifest generation=%d", ref.Generation, cfg.ActiveManifest.Generation)
		}
		raw, err := readCache.read(ref, rawScratch)
		if err != nil {
			return diag, fmt.Errorf("collections: column physical scan read generation=%d part_id=%d: %w", ref.Generation, ref.PartID, err)
		}
		rawScratch = raw
		diag.PhysicalBytesScanned += int64(len(raw))
		summary, err := scanColumnPhysicalAssetRowsWithManifestOperation(raw, ref, collectionName, cfg, projection, assetRef.Reason, req.Visitor)
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
	return diag, nil
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
	seen := make(map[string]struct{}, len(projected))
	for outIdx, name := range projected {
		if name == "" {
			return columnPhysicalScanProjection{}, errors.New("collections: physical column scan projection contains empty column")
		}
		if _, ok := seen[name]; ok {
			return columnPhysicalScanProjection{}, fmt.Errorf("collections: physical column scan duplicate projected column %q", name)
		}
		seen[name] = struct{}{}
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

func validateColumnManifestIdentityAtRootForScan(snap *backenddb.Snapshot, rootID uint64, identity ColumnManifestIdentity) error {
	entry, err := snap.GetEntryAtRoot(rootID, newColumnManifestIdentityRecordKey())
	if errors.Is(err, tree.ErrKeyNotFound) {
		return fmt.Errorf("%w: physical column scan manifest root %d", ErrColumnManifestIdentityMissing, rootID)
	}
	if err != nil {
		return fmt.Errorf("collections: physical column scan manifest root %d identity unreadable: %w", rootID, err)
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return fmt.Errorf("%w: physical column scan manifest root %d deleted identity", ErrColumnManifestIdentityMissing, rootID)
	}
	record, err := decodeColumnManifestIdentityRecord(entry.Value)
	if err != nil {
		return fmt.Errorf("collections: physical column scan manifest root %d invalid identity: %w", rootID, err)
	}
	if record.Generation != identity.Generation || record.Version != identity.Version || record.Checksum != identity.Checksum {
		return fmt.Errorf("collections: physical column scan manifest identity mismatch root=%+v active=%+v", record, identity)
	}
	return nil
}

func loadColumnManifestRecordsFromRootForScan(snap *backenddb.Snapshot, rootID uint64) ([]columnManifestRecord, error) {
	iter, err := snap.IteratorAtRoot(rootID, []byte(columnManifestHeaderRecordKey), nil)
	if err != nil {
		return nil, fmt.Errorf("collections: physical column scan manifest root %d unreadable: %w", rootID, err)
	}
	defer func() { _ = iter.Close() }()
	records := make([]columnManifestRecord, 0, 8)
	for iter.Valid() {
		key := iter.UnsafeKey()
		if !bytes.Equal(key, []byte(columnManifestHeaderRecordKey)) && !bytes.HasPrefix(key, []byte(columnManifestPartRecordPrefix)) {
			break
		}
		if iter.IsDeleted() {
			iter.Next()
			continue
		}
		value, _, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			return nil, fmt.Errorf("collections: physical column scan manifest record %q must be inline", key)
		}
		records = append(records, columnManifestRecord{key: bytes.Clone(key), value: bytes.Clone(value)})
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return records, nil
}

func validateColumnManifestSnapshotForScan(snapshot columnManifestSnapshot, records []columnManifestRecord, cfg ColumnStoreConfig, identity ColumnManifestIdentity, collection string) error {
	if snapshot.Collection != collection {
		return fmt.Errorf("collections: physical column scan manifest collection=%q want %q", snapshot.Collection, collection)
	}
	if snapshot.Generation != identity.Generation {
		return fmt.Errorf("collections: physical column scan manifest generation=%d want %d", snapshot.Generation, identity.Generation)
	}
	if snapshot.SchemaHash != cfg.SchemaHash {
		return fmt.Errorf("collections: physical column scan manifest schema_hash=%d want %d", snapshot.SchemaHash, cfg.SchemaHash)
	}
	if snapshot.AppliedCommandLSN != cfg.RecoveryAuthoritativeAppliedCommandLSN {
		return fmt.Errorf("collections: physical column scan manifest AppliedCommandLSN=%d want recovery %d", snapshot.AppliedCommandLSN, cfg.RecoveryAuthoritativeAppliedCommandLSN)
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
		return fmt.Errorf("collections: physical column scan manifest checksum=%d want active identity checksum=%d", checksum, identity.Checksum)
	}
	return nil
}

func activeColumnManifestRecordsForScan(records []columnManifestRecord, generation uint64) ([]columnManifestRecord, error) {
	active := make([]columnManifestRecord, 0, len(records))
	for _, record := range records {
		switch {
		case bytes.Equal(record.key, []byte(columnManifestHeaderRecordKey)):
			active = append(active, record)
		case bytes.HasPrefix(record.key, []byte(columnManifestPartRecordPrefix)):
			partGeneration, err := columnManifestPartGenerationFromRecordKeyForScan(record.key)
			if err != nil {
				return nil, err
			}
			if partGeneration == generation {
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
		case bytes.Equal(record.key, []byte(columnManifestHeaderRecordKey)):
			if sawHeader {
				return columnManifestSnapshot{}, errors.New("collections: duplicate column manifest binary header record")
			}
			header, err := decodeColumnManifestHeaderRecord(record.value)
			if err != nil {
				return columnManifestSnapshot{}, err
			}
			snapshot = header
			sawHeader = true
		case bytes.HasPrefix(record.key, []byte(columnManifestPartRecordPrefix)):
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
	if !bytes.HasPrefix(key, []byte(columnManifestPartRecordPrefix)) {
		return 0, fmt.Errorf("collections: column manifest part key %q missing prefix", string(key))
	}
	if len(key) != len(columnManifestPartRecordPrefix)+16 {
		return 0, fmt.Errorf("collections: column manifest part key length=%d want %d", len(key), len(columnManifestPartRecordPrefix)+16)
	}
	return binary.BigEndian.Uint64(key[len(columnManifestPartRecordPrefix):]), nil
}

func columnManifestAssetRefsFromRecordsForScan(records []columnManifestRecord, activeGeneration uint64, expectedNamespace string) ([]columnManifestAssetRefForScan, int, error) {
	refs := make([]columnManifestAssetRefForScan, 0, len(records))
	mutationParts := 0
	for _, record := range records {
		if !bytes.HasPrefix(record.key, []byte(columnManifestPartRecordPrefix)) {
			continue
		}
		keyGeneration, err := columnManifestPartGenerationFromRecordKeyForScan(record.key)
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
		ref, reason, err := decodeColumnManifestPartRefForScan(record.value, expectedNamespace)
		if err != nil {
			return nil, 0, err
		}
		if ref.Generation != keyGeneration {
			return nil, 0, fmt.Errorf("collections: column manifest part key generation=%d does not match ref generation=%d", keyGeneration, ref.Generation)
		}
		operation, ok := columnPhysicalScanOperationFromBytes(reason)
		if !ok {
			return nil, 0, fmt.Errorf("collections: unsupported column manifest part reason %q", string(reason))
		}
		refs = append(refs, columnManifestAssetRefForScan{Ref: ref, Reason: operation})
		if operation != ColumnPublishOperationInsert {
			mutationParts++
		}
	}
	return refs, mutationParts, nil
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

func scanColumnPhysicalAssetRows(raw []byte, ref ColumnAssetRef, expectedCollection string, cfg ColumnStoreConfig, projection columnPhysicalScanProjection, visitor func(columnPhysicalScanRowView) error) (columnPhysicalAssetScanSummary, error) {
	return scanColumnPhysicalAssetRowsWithManifestOperation(raw, ref, expectedCollection, cfg, projection, "", visitor)
}

func scanColumnPhysicalAssetRowsWithManifestOperation(raw []byte, ref ColumnAssetRef, expectedCollection string, cfg ColumnStoreConfig, projection columnPhysicalScanProjection, expectedOperation ColumnPublishOperation, visitor func(columnPhysicalScanRowView) error) (columnPhysicalAssetScanSummary, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnPhysicalAssetMagic {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("bad column physical asset magic=0x%08x", magic)
	}
	version := cur.u16()
	if !isSupportedColumnPhysicalAssetVersion(version) {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("unsupported column physical asset version=%d", version)
	}
	collection := cur.stringBytes()
	namespace := cur.stringBytes()
	generation := cur.u64()
	partID := cur.u64()
	appliedCommandLSN := cur.u64()
	operation, operationOK := columnPhysicalScanOperationFromBytes(cur.stringBytes())
	schemaHash := cur.u64()
	columnCount := cur.u64()
	rowCount := cur.u64()
	if err := cur.err; err != nil {
		return columnPhysicalAssetScanSummary{}, err
	}
	if columnCount > uint64(maxCollectionInt) || rowCount > uint64(maxCollectionInt) {
		return columnPhysicalAssetScanSummary{}, errors.New("column physical asset dimensions overflow int")
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
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("unsupported column physical asset operation %q", string(operation))
	}
	if err := validateColumnPhysicalAssetScanHeader(header, ref, expectedCollection, cfg); err != nil {
		return columnPhysicalAssetScanSummary{}, err
	}
	if expectedOperation != "" && header.Operation != expectedOperation {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("%w: manifest reason=%q asset operation=%q", errColumnPhysicalAssetManifestOperationMismatch, expectedOperation, header.Operation)
	}
	if header.ColumnCount != len(cfg.Columns) {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("column physical asset columns=%d want %d", header.ColumnCount, len(cfg.Columns))
	}
	for colIdx := 0; colIdx < header.ColumnCount; colIdx++ {
		name := cur.stringBytes()
		path := cur.stringBytes()
		valueType := cur.stringBytes()
		nullable := cur.bool()
		dictionary := cur.bool()
		if cur.err != nil {
			return columnPhysicalAssetScanSummary{}, cur.err
		}
		want := cfg.Columns[colIdx]
		if !columnPhysicalBytesEqualString(name, want.Name) ||
			!columnPhysicalBytesEqualString(path, want.Path) ||
			!columnPhysicalBytesEqualString(valueType, string(want.ValueType)) ||
			nullable != want.Nullable ||
			dictionary != want.Dictionary {
			return columnPhysicalAssetScanSummary{}, fmt.Errorf("column physical asset column[%d]={Name:%q Path:%q ValueType:%q Nullable:%t Dictionary:%t} want %+v",
				colIdx, string(name), string(path), string(valueType), nullable, dictionary, want)
		}
	}
	valuesBuf := projection.values
	if len(valuesBuf) < projection.count {
		valuesBuf = make([]columnDeclaredValue, projection.count)
	}
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

func validateColumnPhysicalAssetScanHeader(header columnPhysicalAssetScanHeader, ref ColumnAssetRef, expectedCollection string, cfg ColumnStoreConfig) error {
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
		return ColumnPublishOperation(string(raw)), false
	}
}

func scanColumnPhysicalRowValues(cur *manifestCursor, version uint16, cfg ColumnStoreConfig, projection columnPhysicalScanProjection, rowValues []columnDeclaredValue) error {
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
		if version >= columnPhysicalAssetVersion {
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
