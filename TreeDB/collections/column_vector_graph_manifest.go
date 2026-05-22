package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	columnManifestVectorGraphRecordPrefix = "\x03column-manifest/v1/vector-graph/"
	columnManifestVectorGraphMagic        = uint32(0x54434d47) // TCMG

	columnVectorGraphVectorColumnName    = "vector"
	columnVectorGraphInvNormColumnName   = "inv_norm"
	columnVectorGraphAdjacencyColumnName = "adjacency"
)

var columnManifestVectorGraphRecordPrefixBytes = []byte(columnManifestVectorGraphRecordPrefix)

type columnVectorGraphManifestSnapshot struct {
	IndexName              string
	Field                  string
	Metric                 VectorMetric
	Encoding               VectorIndexEncoding
	Dimensions             int
	M                      int
	EfConstruction         int
	EfSearch               int
	BaseManifestGeneration uint64
	BaseManifestChecksum   uint64
	BaseSchemaHash         uint64
	GraphSchemaHash        uint64
	RowCount               int
	AssetRef               ColumnAssetRef
	AssetBytes             int64
}

type columnVectorGraphAssetRow struct {
	ID        []byte
	Vector    []float32
	InvNorm   float32
	Adjacency []uint32
}

type columnVectorGraphPreparedPhysicalAsset struct {
	AssetRootDir string
	Config       ColumnStoreConfig
	Ref          ColumnAssetRef
	Bytes        int64
	RowCount     int
}

func columnVectorGraphManifestRecordKey(indexName string) []byte {
	key := make([]byte, len(columnManifestVectorGraphRecordPrefix)+len(indexName))
	copy(key, columnManifestVectorGraphRecordPrefix)
	copy(key[len(columnManifestVectorGraphRecordPrefix):], indexName)
	return key
}

func columnManifestRecordKeyKnownForScan(key []byte) bool {
	return bytes.Equal(key, columnManifestHeaderRecordKeyBytes) ||
		bytes.HasPrefix(key, columnManifestPartRecordPrefixBytes) ||
		bytes.HasPrefix(key, columnManifestAggregateMetadataRecordPrefixBytes) ||
		bytes.HasPrefix(key, columnManifestDictionaryCodesRecordPrefixBytes) ||
		bytes.HasPrefix(key, columnManifestInt64ValuesRecordPrefixBytes) ||
		bytes.HasPrefix(key, columnManifestVectorGraphRecordPrefixBytes)
}

func findColumnVectorGraphManifestRecord(records []columnManifestRecord, indexName string) (columnManifestRecord, bool) {
	key := columnVectorGraphManifestRecordKey(indexName)
	for _, record := range records {
		if bytes.Equal(record.key, key) {
			return record, true
		}
	}
	return columnManifestRecord{}, false
}

func encodeColumnVectorGraphManifestRecord(snapshot columnVectorGraphManifestSnapshot) ([]byte, error) {
	if err := validateColumnVectorGraphManifestSnapshot(snapshot); err != nil {
		return nil, err
	}
	var b bytes.Buffer
	writeManifestUint32(&b, columnManifestVectorGraphMagic)
	writeManifestUint16(&b, columnManifestRecordVersion)
	writeManifestString(&b, snapshot.IndexName)
	writeManifestString(&b, snapshot.Field)
	writeManifestString(&b, string(snapshot.Metric))
	writeManifestString(&b, string(snapshot.Encoding))
	writeManifestUint64(&b, uint64(snapshot.Dimensions))
	writeManifestUint64(&b, uint64(snapshot.M))
	writeManifestUint64(&b, uint64(snapshot.EfConstruction))
	writeManifestUint64(&b, uint64(snapshot.EfSearch))
	writeManifestUint64(&b, snapshot.BaseManifestGeneration)
	writeManifestUint64(&b, snapshot.BaseManifestChecksum)
	writeManifestUint64(&b, snapshot.BaseSchemaHash)
	writeManifestUint64(&b, snapshot.GraphSchemaHash)
	writeManifestUint64(&b, uint64(snapshot.RowCount))
	writeManifestString(&b, string(snapshot.AssetRef.Kind))
	writeManifestString(&b, snapshot.AssetRef.Namespace)
	writeManifestUint64(&b, snapshot.AssetRef.Generation)
	writeManifestUint64(&b, snapshot.AssetRef.PartID)
	writeManifestUint64(&b, uint64(snapshot.AssetRef.FileID))
	writeManifestUint64(&b, uint64(snapshot.AssetRef.Offset))
	writeManifestUint64(&b, uint64(snapshot.AssetRef.Length))
	writeManifestUint64(&b, uint64(snapshot.AssetRef.Checksum))
	writeManifestUint64(&b, uint64(snapshot.AssetBytes))
	return b.Bytes(), nil
}

func decodeColumnVectorGraphManifestRecord(raw []byte) (columnVectorGraphManifestSnapshot, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnManifestVectorGraphMagic {
		return columnVectorGraphManifestSnapshot{}, fmt.Errorf("collections: bad column vector graph manifest magic=0x%08x", magic)
	}
	if version := cur.u16(); version != columnManifestRecordVersion {
		return columnVectorGraphManifestSnapshot{}, fmt.Errorf("collections: unsupported column vector graph manifest version=%d", version)
	}
	snapshot := columnVectorGraphManifestSnapshot{
		IndexName:              cur.string(),
		Field:                  cur.string(),
		Metric:                 VectorMetric(cur.string()),
		Encoding:               VectorIndexEncoding(cur.string()),
		Dimensions:             int(cur.u64()),
		M:                      int(cur.u64()),
		EfConstruction:         int(cur.u64()),
		EfSearch:               int(cur.u64()),
		BaseManifestGeneration: cur.u64(),
		BaseManifestChecksum:   cur.u64(),
		BaseSchemaHash:         cur.u64(),
		GraphSchemaHash:        cur.u64(),
		RowCount:               int(cur.u64()),
	}
	snapshot.AssetRef = ColumnAssetRef{
		Kind:       ColumnAssetKind(cur.string()),
		Namespace:  cur.string(),
		Generation: cur.u64(),
		PartID:     cur.u64(),
	}
	fileID64 := cur.u64()
	offset64 := cur.u64()
	length64 := cur.u64()
	checksum64 := cur.u64()
	assetBytes64 := cur.u64()
	if err := cur.err; err != nil {
		return columnVectorGraphManifestSnapshot{}, err
	}
	if snapshot.Dimensions < 0 || snapshot.M < 0 || snapshot.EfConstruction < 0 || snapshot.EfSearch < 0 || snapshot.RowCount < 0 {
		return columnVectorGraphManifestSnapshot{}, errors.New("collections: column vector graph manifest integer overflow")
	}
	if fileID64 > uint64(math.MaxUint32) {
		return columnVectorGraphManifestSnapshot{}, errors.New("collections: column vector graph manifest file_id overflows uint32")
	}
	if checksum64 > uint64(math.MaxUint32) {
		return columnVectorGraphManifestSnapshot{}, errors.New("collections: column vector graph manifest checksum overflows uint32")
	}
	if offset64 > uint64(math.MaxInt64) || length64 > uint64(math.MaxInt64) || assetBytes64 > uint64(math.MaxInt64) {
		return columnVectorGraphManifestSnapshot{}, errors.New("collections: column vector graph manifest offsets or byte counts overflow int64")
	}
	snapshot.AssetRef.FileID = uint32(fileID64)
	snapshot.AssetRef.Offset = int64(offset64)
	snapshot.AssetRef.Length = int64(length64)
	snapshot.AssetRef.Checksum = uint32(checksum64)
	snapshot.AssetBytes = int64(assetBytes64)
	if cur.pos != len(raw) {
		return columnVectorGraphManifestSnapshot{}, errors.New("collections: trailing bytes in column vector graph manifest record")
	}
	if err := validateColumnVectorGraphManifestSnapshot(snapshot); err != nil {
		return columnVectorGraphManifestSnapshot{}, err
	}
	return snapshot, nil
}

func validateColumnVectorGraphManifestSnapshot(snapshot columnVectorGraphManifestSnapshot) error {
	if err := ValidateIndexName(snapshot.IndexName); err != nil {
		return fmt.Errorf("collections: invalid column vector graph manifest index name: %w", err)
	}
	if err := ValidateIndexPath(snapshot.Field); err != nil {
		return fmt.Errorf("collections: invalid column vector graph manifest field: %w", err)
	}
	if metric, err := normalizeVectorMetric(snapshot.Metric); err != nil || metric != snapshot.Metric {
		if err == nil {
			err = fmt.Errorf("unsupported metric %q", snapshot.Metric)
		}
		return fmt.Errorf("collections: invalid column vector graph manifest metric: %w", err)
	}
	if encoding, err := normalizeVectorIndexEncoding(snapshot.Encoding); err != nil || encoding != snapshot.Encoding {
		if err == nil {
			err = fmt.Errorf("unsupported encoding %q", snapshot.Encoding)
		}
		return fmt.Errorf("collections: invalid column vector graph manifest encoding: %w", err)
	}
	if snapshot.Dimensions <= 0 {
		return errors.New("collections: column vector graph manifest dimensions must be positive")
	}
	if snapshot.M <= 0 || snapshot.EfConstruction <= 0 || snapshot.EfSearch <= 0 {
		return errors.New("collections: column vector graph manifest graph parameters must be positive")
	}
	if snapshot.BaseManifestGeneration == 0 || snapshot.BaseManifestChecksum == 0 || snapshot.BaseSchemaHash == 0 || snapshot.GraphSchemaHash == 0 {
		return errors.New("collections: column vector graph manifest missing base or graph identity")
	}
	if snapshot.RowCount < 0 {
		return errors.New("collections: column vector graph manifest row count must be non-negative")
	}
	if err := validateColumnAssetRefForPlan(snapshot.AssetRef); err != nil {
		return err
	}
	if snapshot.AssetBytes <= 0 {
		return errors.New("collections: column vector graph manifest asset bytes must be positive")
	}
	if snapshot.AssetBytes != snapshot.AssetRef.Length {
		return fmt.Errorf("collections: column vector graph manifest asset bytes=%d does not match ref length=%d", snapshot.AssetBytes, snapshot.AssetRef.Length)
	}
	return nil
}

func columnVectorGraphPhysicalColumnStoreConfig(collection string, base ColumnStoreConfig, def VectorIndexDefinition) (ColumnStoreConfig, error) {
	normalizedDef, err := normalizeVectorIndexDefinition(def)
	if err != nil {
		return ColumnStoreConfig{}, err
	}
	if normalizedDef.Strategy != VectorIndexStrategyColumnGraph {
		return ColumnStoreConfig{}, fmt.Errorf("collections: vector index %q strategy=%q is not column_graph", normalizedDef.Name, normalizedDef.Strategy)
	}
	if !base.Enabled {
		return ColumnStoreConfig{}, errors.New("collections: column vector graph physical config requires enabled base column_store")
	}
	if base.AssetManager == nil {
		return ColumnStoreConfig{}, errors.New("collections: column vector graph physical config requires base asset manager")
	}
	cfg, err := normalizeColumnStoreConfig(collection, &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: columnVectorGraphVectorColumnName, Path: normalizedDef.Field, ValueType: ColumnStoreValueFloat32Vector, VectorDims: normalizedDef.Dimensions, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
			{Name: columnVectorGraphInvNormColumnName, Path: normalizedDef.Field + "_inv_norm", ValueType: ColumnStoreValueFloat32},
			{Name: columnVectorGraphAdjacencyColumnName, Path: normalizedDef.Field + "_neighbors", ValueType: ColumnStoreValueAdjacencyList, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
		},
		RetainedPayload: ColumnRetainedPayloadNone,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
		AssetManager: &ColumnAssetManagerConfig{
			Kind:      base.AssetManager.Kind,
			Namespace: base.AssetManager.Namespace,
		},
	})
	if err != nil {
		return ColumnStoreConfig{}, err
	}
	return *cfg, nil
}

func prepareColumnVectorGraphPhysicalAsset(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, generation, partID, appliedCommandLSN uint64, rows []columnVectorGraphAssetRow) (columnVectorGraphPreparedPhysicalAsset, error) {
	if assetRootDir == "" {
		return columnVectorGraphPreparedPhysicalAsset{}, errors.New("collections: column vector graph physical asset requires asset root dir")
	}
	normalizedDef, err := normalizeVectorIndexDefinition(def)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, err
	}
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig(collection, base, def)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, err
	}
	declared := make([]columnDeclaredRow, len(rows))
	for i, row := range rows {
		if len(row.ID) == 0 {
			return columnVectorGraphPreparedPhysicalAsset{}, fmt.Errorf("collections: column vector graph row[%d] missing document id", i)
		}
		if len(row.Vector) != normalizedDef.Dimensions {
			return columnVectorGraphPreparedPhysicalAsset{}, fmt.Errorf("collections: column vector graph row[%d] vector dims=%d want %d", i, len(row.Vector), normalizedDef.Dimensions)
		}
		declared[i] = columnDeclaredRow{
			ID: bytes.Clone(row.ID),
			Values: []columnDeclaredValue{
				{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: append([]float32(nil), row.Vector...)},
				{Type: ColumnStoreValueFloat32, Present: true, Float32: row.InvNorm},
				{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: append([]uint32(nil), row.Adjacency...)},
			},
		}
	}
	encoded, summary, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        collection,
		Namespace:         graphCfg.AssetManager.Namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: appliedCommandLSN,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        graphCfg.SchemaHash,
		Columns:           graphCfg.Columns,
		Rows:              declared,
	})
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, err
	}
	ref, err := writeColumnVectorGraphPhysicalAssetToManager(assetRootDir, graphCfg, encoded, generation, partID)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, err
	}
	return columnVectorGraphPreparedPhysicalAsset{
		AssetRootDir: assetRootDir,
		Config:       graphCfg,
		Ref:          ref,
		Bytes:        summary.PayloadBytes,
		RowCount:     summary.RowCount,
	}, nil
}

func writeColumnVectorGraphPhysicalAssetToManager(rootDir string, cfg ColumnStoreConfig, payload []byte, generation, partID uint64) (ColumnAssetRef, error) {
	if len(payload) == 0 {
		return ColumnAssetRef{}, errors.New("collections: column_graph physical asset payload is empty")
	}
	if generation == 0 || partID == 0 {
		return ColumnAssetRef{}, errors.New("collections: column_graph physical asset append requires generation and part_id")
	}
	appender, err := newNextColumnPhysicalAssetSegmentAppender(rootDir, cfg)
	if err != nil {
		return ColumnAssetRef{}, err
	}
	ref, appendErr := appender.append(payload, generation, partID)
	closeErr := appender.close()
	if appendErr != nil {
		return ColumnAssetRef{}, errors.Join(appendErr, closeErr)
	}
	return ref, closeErr
}

func (c *Collection) columnGraphVectorIndexStatus(name string) (VectorIndexStatus, error) {
	if c == nil {
		return VectorIndexStatus{}, errCollectionNil
	}
	if c.db == nil {
		return VectorIndexStatus{}, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return VectorIndexStatus{}, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	return c.columnGraphVectorIndexStatusAtSnapshot(name, snap)
}

func (c *Collection) columnGraphVectorIndexStatusAtSnapshot(name string, snap *backenddb.Snapshot) (VectorIndexStatus, error) {
	if c == nil {
		return VectorIndexStatus{}, errCollectionNil
	}
	if c.db == nil {
		return VectorIndexStatus{}, errCollectionDBNil
	}
	if snap == nil {
		return VectorIndexStatus{}, backenddb.ErrClosed
	}
	status := VectorIndexStatus{
		Name:     name,
		Strategy: VectorIndexStrategyColumnGraph,
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if catalog == nil {
		return VectorIndexStatus{}, errCollectionNotFound
	}
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, name)
	if ok {
		status.Name = def.Name
		status.Strategy = def.Strategy
	} else {
		return VectorIndexStatus{}, ErrIndexNotFound
	}
	switch def.Strategy {
	case VectorIndexStrategyNativeRuntime:
		status.State = VectorIndexStateNativeRuntime
		status.Reason = VectorIndexReasonNativeRuntime
		return status, nil
	case VectorIndexStrategyColumnGraph:
	default:
		return VectorIndexStatus{}, fmt.Errorf("collections: unsupported vector index strategy %q", def.Strategy)
	}
	cfg := catalog.meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.AssetManager == nil {
		status.State = VectorIndexStateColumnGraphUnavailable
		status.Reason = VectorIndexReasonPhysicalColumnAssetSupportMissing
		status.RebuildNeeded = true
		return status, nil
	}
	if cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil {
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphRebuildNeeded
		status.RebuildNeeded = true
		return status, nil
	}
	if !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphRebuildNeeded
		status.RebuildNeeded = true
		return status, nil
	}
	rootID := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
	if rootID == 0 {
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphRebuildNeeded
		status.RebuildNeeded = true
		return status, nil
	}
	if err := validateColumnManifestIdentityAtRoot(snap, rootID, *cfg.ActiveManifest); err != nil {
		status.State = VectorIndexStateColumnGraphUnavailable
		status.Reason = VectorIndexReasonColumnGraphCorrupt
		status.RebuildNeeded = true
		return columnGraphVectorIndexStatusError(status, err)
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, rootID)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		status.State = VectorIndexStateColumnGraphUnavailable
		status.Reason = VectorIndexReasonColumnGraphCorrupt
		status.RebuildNeeded = true
		return columnGraphVectorIndexStatusError(status, err)
	}
	if err := validateColumnManifestSnapshot(manifest, records, *cfg, *cfg.ActiveManifest, catalog.meta.Name, "column vector graph status"); err != nil {
		status.State = VectorIndexStateColumnGraphUnavailable
		status.Reason = VectorIndexReasonColumnGraphCorrupt
		status.RebuildNeeded = true
		return columnGraphVectorIndexStatusError(status, err)
	}
	graphRecord, ok := findColumnVectorGraphManifestRecord(records, def.Name)
	if !ok {
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphRebuildNeeded
		status.RebuildNeeded = true
		return status, nil
	}
	graph, err := decodeColumnVectorGraphManifestRecord(graphRecord.value)
	if err != nil {
		status.State = VectorIndexStateColumnGraphUnavailable
		status.Reason = VectorIndexReasonColumnGraphCorrupt
		status.RebuildNeeded = true
		return columnGraphVectorIndexStatusError(status, err)
	}
	switch columnVectorGraphManifestMatchStatus(catalog.meta.Name, graph, def, *cfg, manifest, records) {
	case columnVectorGraphManifestMatchLoaded:
	case columnVectorGraphManifestMatchUnsupportedVisibility:
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphUnsupportedVisibility
		status.RebuildNeeded = true
		return status, nil
	default:
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphAssetMismatch
		status.RebuildNeeded = true
		return status, nil
	}
	if err := validateColumnVectorGraphAssetRefAvailable(c.db.ColumnAssetRootDir(), graph.AssetRef); err != nil {
		status.State = VectorIndexStateColumnGraphUnavailable
		status.Reason = VectorIndexReasonColumnGraphCorrupt
		status.RebuildNeeded = true
		return columnGraphVectorIndexStatusError(status, err)
	}
	status.State = VectorIndexStateColumnGraphLoaded
	status.Loaded = true
	return status, nil
}

func columnGraphVectorIndexStatusError(status VectorIndexStatus, err error) (VectorIndexStatus, error) {
	if err == nil {
		return status, nil
	}
	return status, fmt.Errorf("collections: column_graph vector index %q status=%s reason=%s: %w", status.Name, status.State, status.Reason, err)
}

type columnVectorGraphManifestMatch uint8

const (
	columnVectorGraphManifestMatchMismatch columnVectorGraphManifestMatch = iota
	columnVectorGraphManifestMatchLoaded
	columnVectorGraphManifestMatchUnsupportedVisibility
)

func columnVectorGraphManifestMatchesDefinition(collection string, graph columnVectorGraphManifestSnapshot, def VectorIndexDefinition, cfg ColumnStoreConfig, manifest columnManifestSnapshot, records []columnManifestRecord) bool {
	return columnVectorGraphManifestMatchStatus(collection, graph, def, cfg, manifest, records) == columnVectorGraphManifestMatchLoaded
}

func columnVectorGraphManifestMatchStatus(collection string, graph columnVectorGraphManifestSnapshot, def VectorIndexDefinition, cfg ColumnStoreConfig, manifest columnManifestSnapshot, records []columnManifestRecord) columnVectorGraphManifestMatch {
	if cfg.ActiveManifest == nil {
		return columnVectorGraphManifestMatchMismatch
	}
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig(collection, cfg, def)
	if err != nil {
		return columnVectorGraphManifestMatchMismatch
	}
	baseChecksum, err := columnVectorGraphBaseManifestChecksum(manifest, records, cfg)
	if err != nil {
		return columnVectorGraphManifestMatchMismatch
	}
	if !columnVectorGraphCoreParametersMatch(&graph, &def, &cfg, &graphCfg) {
		return columnVectorGraphManifestMatchMismatch
	}
	if graph.BaseManifestGeneration != cfg.ActiveManifest.Generation {
		return columnVectorGraphManifestMatchUnsupportedVisibility
	}
	if graph.BaseManifestChecksum != baseChecksum {
		return columnVectorGraphManifestMatchMismatch
	}
	return columnVectorGraphManifestMatchLoaded
}

func columnVectorGraphCoreParametersMatch(graph *columnVectorGraphManifestSnapshot, def *VectorIndexDefinition, cfg *ColumnStoreConfig, graphCfg *ColumnStoreConfig) bool {
	return columnVectorGraphDefinitionParametersMatch(graph, def) &&
		columnVectorGraphAssetParametersMatch(graph, cfg, graphCfg)
}

func columnVectorGraphDefinitionParametersMatch(graph *columnVectorGraphManifestSnapshot, def *VectorIndexDefinition) bool {
	return graph.IndexName == def.Name &&
		graph.Field == def.Field &&
		graph.Metric == def.Metric &&
		graph.Encoding == def.Encoding &&
		graph.Dimensions == def.Dimensions &&
		graph.M == def.M &&
		graph.EfConstruction == def.EfConstruction &&
		graph.EfSearch == def.EfSearch
}

func columnVectorGraphAssetParametersMatch(graph *columnVectorGraphManifestSnapshot, cfg *ColumnStoreConfig, graphCfg *ColumnStoreConfig) bool {
	return graph.BaseSchemaHash == cfg.SchemaHash &&
		graph.GraphSchemaHash == graphCfg.SchemaHash &&
		graph.AssetRef.Kind == ColumnAssetKindTCS1PartImage &&
		graph.AssetRef.Namespace == graphCfg.AssetManager.Namespace &&
		graph.AssetRef.Generation == graph.BaseManifestGeneration
}

func columnVectorGraphBaseManifestChecksum(manifest columnManifestSnapshot, records []columnManifestRecord, cfg ColumnStoreConfig) (uint64, error) {
	activeRecords, err := activeColumnManifestRecordsForScan(records, manifest.Generation)
	if err != nil {
		return 0, err
	}
	baseRecords := activeRecords[:0]
	for _, record := range activeRecords {
		if bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes) {
			continue
		}
		baseRecords = append(baseRecords, record)
	}
	sortColumnManifestRecords(baseRecords)
	return checksumColumnManifestRecords(ColumnPublishManifestEncodeInput{
		Collection:        manifest.Collection,
		ColumnStore:       cfg,
		Operation:         manifest.Operation,
		AppliedCommandLSN: manifest.AppliedCommandLSN,
	}, manifest.Generation, baseRecords), nil
}

func columnVectorGraphAssetRefsFromManifestRecordsForReachability(records []columnManifestRecord, activeGeneration uint64, expectedNamespace string, activeVectorIndexesKnown bool, activeVectorIndexes []VectorIndexDefinition) ([]ColumnAssetRef, error) {
	var refs []ColumnAssetRef
	for _, record := range records {
		if !bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes) {
			continue
		}
		if !retainColumnManifestVectorGraphRecordForWrite(record.key, activeVectorIndexesKnown, activeVectorIndexes) {
			continue
		}
		graph, err := decodeColumnVectorGraphManifestRecord(record.value)
		if err != nil {
			return nil, err
		}
		if graph.AssetRef.Generation > activeGeneration {
			return nil, fmt.Errorf("collections: column vector graph asset generation=%d is newer than active manifest generation=%d", graph.AssetRef.Generation, activeGeneration)
		}
		if graph.BaseManifestGeneration != graph.AssetRef.Generation {
			return nil, fmt.Errorf("collections: column vector graph base manifest generation=%d does not match asset generation=%d", graph.BaseManifestGeneration, graph.AssetRef.Generation)
		}
		if graph.AssetRef.Kind != ColumnAssetKindTCS1PartImage {
			return nil, fmt.Errorf("collections: column vector graph asset kind=%q want %q", graph.AssetRef.Kind, ColumnAssetKindTCS1PartImage)
		}
		if graph.AssetRef.Namespace != expectedNamespace {
			return nil, fmt.Errorf("collections: column vector graph asset namespace=%q want %q", graph.AssetRef.Namespace, expectedNamespace)
		}
		if err := validateColumnAssetRefForPlan(graph.AssetRef); err != nil {
			return nil, err
		}
		refs = append(refs, graph.AssetRef)
	}
	return refs, nil
}

func validateColumnVectorGraphAssetRefAvailable(rootDir string, ref ColumnAssetRef) error {
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		return err
	}
	path, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("collections: column vector graph asset segment %q is a directory", path)
	}
	if ref.Offset < 0 || ref.Length <= 0 || ref.Offset > math.MaxInt64-ref.Length {
		return fmt.Errorf("collections: invalid column vector graph asset range offset=%d length=%d", ref.Offset, ref.Length)
	}
	if info.Size() < ref.Offset+ref.Length {
		return fmt.Errorf("collections: column vector graph asset segment size=%d shorter than ref end=%d", info.Size(), ref.Offset+ref.Length)
	}
	return nil
}
