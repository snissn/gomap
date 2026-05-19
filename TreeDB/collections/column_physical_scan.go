package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

type columnPhysicalScanRequest struct {
	ProjectedColumns []string
	Visitor          func(columnPhysicalScanRowView) error
}

type columnPhysicalScanDiagnostics struct {
	ManifestRoot               uint64
	ManifestGeneration         uint64
	RecoveryManifestGeneration uint64
	AppliedCommandLSN          uint64
	ManifestRecords            int
	AssetRefs                  int
	DecodedBlocks              int
	ScheduledGranules          int
	SkippedGranules            int
	RowsScanned                int
	DeletedRows                int
	ProjectedColumns           int
	RowMaterializations        int
	PhysicalBytesScanned       int64
}

type columnPhysicalScanRowView struct {
	Generation        uint64
	PartID            uint64
	AppliedCommandLSN uint64
	Operation         ColumnPublishOperation
	RowIndex          int
	// ID and Values are valid only until the visitor returns.
	ID      []byte
	Deleted bool
	Values  []columnDeclaredValue
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
	if rootID == 0 {
		return columnPhysicalScanDiagnostics{}, fmt.Errorf("collections: physical column scan missing manifest root %q", rootName)
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
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return diag, errCollectionDBNil
	}
	defer func() { _ = snap.Close() }()

	if err := validateColumnManifestIdentityAtRootForScan(snap, rootID, *cfg.ActiveManifest); err != nil {
		return diag, err
	}
	records, err := loadColumnManifestRecordsFromRootForScan(snap, rootID)
	if err != nil {
		return diag, err
	}
	diag.ManifestRecords = len(records)
	manifest, err := decodeColumnManifestRecords(records)
	if err != nil {
		return diag, err
	}
	if err := validateColumnManifestSnapshotForScan(manifest, records, cfg, *cfg.ActiveManifest, collectionName); err != nil {
		return diag, err
	}
	refs, err := columnManifestAssetRefsFromRecordsForScan(records)
	if err != nil {
		return diag, err
	}
	diag.AssetRefs = len(refs)
	diag.ScheduledGranules = len(refs)
	for _, ref := range refs {
		if ref.Generation > cfg.ActiveManifest.Generation {
			return diag, fmt.Errorf("collections: column physical scan ref generation=%d is newer than active manifest generation=%d", ref.Generation, cfg.ActiveManifest.Generation)
		}
		raw, err := readColumnPhysicalAssetFromManager(c.db.ColumnAssetRootDir(), ref)
		if err != nil {
			return diag, fmt.Errorf("collections: column physical scan read generation=%d part_id=%d: %w", ref.Generation, ref.PartID, err)
		}
		diag.PhysicalBytesScanned += int64(len(raw))
		summary, err := scanColumnPhysicalAssetRows(raw, ref, cfg, projection, req.Visitor)
		if err != nil {
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
			part, err := decodeColumnManifestPartRecord(record.value)
			if err != nil {
				return nil, err
			}
			if part.AssetRef.Generation == generation {
				active = append(active, record)
			}
		}
	}
	return active, nil
}

func columnManifestAssetRefsFromRecordsForScan(records []columnManifestRecord) ([]ColumnAssetRef, error) {
	refs := make([]ColumnAssetRef, 0, len(records))
	for _, record := range records {
		if !bytes.HasPrefix(record.key, []byte(columnManifestPartRecordPrefix)) {
			continue
		}
		part, err := decodeColumnManifestPartRecord(record.value)
		if err != nil {
			return nil, err
		}
		refs = append(refs, part.AssetRef)
	}
	return refs, nil
}

func scanColumnPhysicalAssetRows(raw []byte, ref ColumnAssetRef, cfg ColumnStoreConfig, projection columnPhysicalScanProjection, visitor func(columnPhysicalScanRowView) error) (columnPhysicalAssetScanSummary, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnPhysicalAssetMagic {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("bad column physical asset magic=0x%08x", magic)
	}
	version := cur.u16()
	if version != columnPhysicalAssetVersionV1 && version != columnPhysicalAssetVersion {
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
	if err := validateColumnPhysicalAssetScanHeader(header, ref, cfg); err != nil {
		return columnPhysicalAssetScanSummary{}, err
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
		if version >= columnPhysicalAssetVersion {
			deleted = cur.bool()
		}
		if cur.err != nil {
			return columnPhysicalAssetScanSummary{}, cur.err
		}
		rowValues := valuesBuf[:0]
		if deleted {
			if header.Operation != ColumnPublishOperationDelete {
				return columnPhysicalAssetScanSummary{}, fmt.Errorf("column physical asset %s row[%d] is marked deleted", header.Operation, rowIdx)
			}
			summary.deleted++
		} else {
			if header.Operation == ColumnPublishOperationDelete {
				return columnPhysicalAssetScanSummary{}, fmt.Errorf("column physical asset delete row[%d] is not marked deleted", rowIdx)
			}
			rowValues = valuesBuf[:projection.count]
			if err := scanColumnPhysicalRowValues(&cur, cfg, projection, rowValues); err != nil {
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

func validateColumnPhysicalAssetScanHeader(header columnPhysicalAssetScanHeader, ref ColumnAssetRef, cfg ColumnStoreConfig) error {
	if cfg.AssetManager == nil {
		return errors.New("column physical asset scan requires asset manager")
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

func scanColumnPhysicalRowValues(cur *manifestCursor, cfg ColumnStoreConfig, projection columnPhysicalScanProjection, rowValues []columnDeclaredValue) error {
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
		outputIdx := projection.outputByColumn[colIdx]
		selected := outputIdx >= 0
		if selected {
			rowValues[outputIdx] = columnDeclaredValue{
				Type: col.ValueType,
				Null: null,
			}
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
				rowValues[outputIdx].String = cur.string()
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
