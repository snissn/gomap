package collections

import (
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// The census belongs to the ORIGINAL captured source. ManifestBackingBytes is
// the sum of individually rounded record-header/key/value allocations, not raw
// payload lengths. Only typed vector TCIM assets contribute source payloads;
// retained Full JSON images and old graph assets contribute manifest metadata
// but are never opened by the supplied-source graph builder. DecodedBytes and
// DictionarySlots include ALL sections inspected by the maintained TCIM parser.
// TypedSourceReadPeak is the maximum maintained preflight scratch requirement
// for ONE asset (16*stored + metadata + 12*decoded), not a sum of all raw files.
// PendingBuildBytes is the sum of the original pending producers' build promises.
// SchemaBasisBytes is zero when nativeTypedCollectionRetainedCapacity already
// reserves these same schema/handoff/build-group owners. It is never refunded.
type nativeOrdinaryColumnGraphSourceCensus struct {
	ManifestRecords, ManifestBackingBytes                                                              uint64
	TypedSourceParts, TypedSourceStoredBytes, TypedSourceDecodedBytes                                  uint64
	DictionarySlots, TypedSourceReadPeak, TypedSourceDecoderBytes, PendingBuildBytes, SchemaBasisBytes uint64
}

type nativeOrdinaryColumnGraphPhasePlan struct {
	MetadataPreflightBytes, PersistentBytes, AssetsRetainedBytes, SourceReadBytes     uint64
	GraphPrivateBytes, SerialAssetBytes, PackBytes, ValidationBytes, RequirementBytes uint64
	NamespaceScanBytes, FreshStateRawBytes, AssetCount, TotalBytes                    uint64
}

// One structural control uses the existing producer's 1KiB policy, not a
// multiplier on metadata bytes. Variable string backing is priced separately.
// Eight cohorts cover the maintained append/map old+replacement growth policy.
const nativeOrdinaryColumnGraphControlBytes = preparedSemanticStreamMapEntryReserveBytes

func nativeOrdinaryColumnGraphControlFloor() error {
	for _, width := range [...]uintptr{
		unsafe.Sizeof(ColumnStoreConfig{}), unsafe.Sizeof(ColumnStoreColumn{}),
		unsafe.Sizeof(typedColumnAdapterColumn{}), unsafe.Sizeof(TypedStorageField{}),
		unsafe.Sizeof(columnPhysicalAssetSegmentAppender{}),
		unsafe.Sizeof(columnVectorGraphStableResourceAccumulator{}),
		unsafe.Sizeof(rootpublication.StableResourceSetBuilder{}), unsafe.Sizeof(rootpublication.StableResourceSet{}),
		unsafe.Sizeof(rootpublication.StableResourceToken{}), unsafe.Sizeof(rootpublication.StableLogicalObligation{}),
		unsafe.Sizeof(rootpublication.StableResourceDescriptor{}), unsafe.Sizeof(rootpublication.IdentityPinRegistry{}),
		unsafe.Sizeof(mappedresource.Manager{}), unsafe.Sizeof(mappedresource.Handle{}),
		unsafe.Sizeof(columnVectorGraphTypedColumnVectorSource{}), unsafe.Sizeof(columnVectorGraphTypedColumnVectorPart{}),
	} {
		if uint64(width) > nativeOrdinaryColumnGraphControlBytes {
			return memtable.ErrCOWCapacity
		}
	}
	return nil
}

func nativeOrdinaryColumnGraphLayers(c nativeColumnGraphConstructionPlan) (uint64, error) {
	if c.Rows == 0 || c.Rows > maxColumnVectorGraphAdjacencyOrdinal || c.Rows > uint64(int(^uint(0)>>1)) {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	var rows uint64
	layers := uint64(0)
	for level, n := range c.LevelRows {
		if err := nativeTypedCheckedAdd(&rows, n); err != nil {
			return 0, err
		}
		if n != 0 {
			layers = uint64(level + 1)
		}
	}
	if rows != c.Rows || layers == 0 {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	return layers, nil
}

// This allocation-free bootstrap MUST be admitted before calling the planner,
// which invokes the existing Config/options/dictionary factories. It is kept
// on the original stage owner; it is excluded from the later build promise.
// All planner-only factory results end before that build recipe is returned.
// At most 33 layer configs plus the five row-ref, norm and ID configs are made.
// No N-element planning carrier, fake assets, or ID copies are constructed.
func nativeOrdinaryColumnGraphMetadataPreflightCapacity(meta CollectionMeta, def VectorIndexDefinition, rootDir string) (uint64, error) {
	if !nativeOrdinaryColumnGraphMeta(meta) || meta.Options.ColumnStore.AssetManager == nil || rootDir == "" || def.Name == "" || def.Field == "" {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	if err := nativeOrdinaryColumnGraphControlFloor(); err != nil {
		return 0, err
	}
	// Bootstrap reserves only planning calls' transient metadata, not all live
	// assets. The physical result config has THREE columns; three one-column
	// factory units cover that maximum as well as every raw one-column factory.
	// Each factory call returns before the next; 20 named structural
	// groups: config+copy, two column arrays, two asset managers, root+locator,
	// three validator maps, field+adapter carriers, three Options carriers,
	// dictionary outer+metadata maps, logical-type map, reflection/closure.
	var s nativeColumnGraphCapacitySum
	s.copies(8, 3*20, nativeOrdinaryColumnGraphControlBytes)
	var strings uint64
	for _, v := range [...]string{meta.Name, def.Name, def.Field, rootDir, meta.Options.ColumnStore.AssetManager.Namespace} {
		if err := nativeTypedCheckedAdd(&strings, uint64(len(v))); err != nil {
			return 0, err
		}
	}
	// Generated suffixes are bounded by the actual constants and uint32 decimal
	// field widths; this is an allocation bound, never an identifier restriction.
	for _, v := range [...]string{columnVectorGraphRowRefStatePathSuffix, columnVectorGraphRowRefStateAssetIDPrefix, columnVectorGraphDocumentIDStatePathSuffix, columnVectorGraphInvNormStatePathSuffix, columnVectorGraphRowRefStateColumnName(columnVectorGraphRowRefStateFieldOrdinalByPhysicalRow)} {
		if err := nativeTypedCheckedAdd(&strings, uint64(len(v))); err != nil {
			return 0, err
		}
	}
	if err := nativeTypedCheckedAdd(&strings, 2*20); err != nil {
		return 0, err
	} // two uint64 decimal renderings
	s.copies(8, 3, strings)
	s.copies(8, 3*(uint64(len(def.Field))+1), uint64(unsafe.Sizeof(string(""))))
	s.allocation(1, uint64(unsafe.Sizeof(nativeOrdinaryColumnGraphPhasePlan{})))
	return s.bytes, s.err
}

// The maintained ordinary builder is scoped: source extraction -> private
// HNSW -> serial raw constructors -> streamed pack -> direct validation ->
// fresh TVIS requirements. Asset authority/configuration starts only AFTER the
// private HNSW builder returns. Reserve common + max(source, HNSW, assets +
// max(serial, pack, validation, requirements)) BEFORE construction and retain
// that entire workspace through publication.
// There is no Shrink/Resize, new receipt, cache, owner, or changed algorithm.
func nativeOrdinaryColumnGraphPhaseCapacity(meta CollectionMeta, def VectorIndexDefinition, rootDir string, c nativeColumnGraphConstructionPlan, source nativeOrdinaryColumnGraphSourceCensus) (nativeOrdinaryColumnGraphPhasePlan, error) {
	var p nativeOrdinaryColumnGraphPhasePlan
	layers, err := nativeOrdinaryColumnGraphLayers(c)
	if err != nil {
		return p, err
	}
	if !nativeOrdinaryColumnGraphMeta(meta) || len(def.QuantizedIndexes) != 0 || def.Representation != "" || def.Strategy != VectorIndexStrategyColumnGraph || def.M != 16 || def.EfConstruction != 128 || def.Metric != VectorMetricCosine || def.Encoding != VectorIndexEncodingFloat32 || def.Dimensions <= 0 || c.IDBytes > uint64(int(^uint(0)>>1)) {
		return p, backenddb.ErrNativeSemanticUnsupported
	}
	p.MetadataPreflightBytes, err = nativeOrdinaryColumnGraphMetadataPreflightCapacity(meta, def, rootDir)
	if err != nil {
		return p, err
	}
	p.AssetCount = layers + 8 // actual adjacency layers, 5 rowrefs, norm, IDs, pack
	cfg := *meta.Options.ColumnStore
	shape, err := newNativeFreshVectorStateShape(def.Name, def.Field, def.Metric.String(), def.Encoding.String())
	if err != nil {
		return p, err
	}
	var persistent, assets, csr nativeColumnGraphCapacitySum
	persistent.add(c.CallerRowsBytes)
	persistent.add(source.SchemaBasisBytes)
	// Pending producers emit new source images that remain open through HNSW
	// and pack. Keep their complete original promises additive until an exact
	// retained subset is derived; their production phase stays charged too.
	persistent.add(source.PendingBuildBytes)
	// Planner-only metadata belongs to the unchanged original stage lease.
	// None of those factory results escapes into the scalar build recipe.
	// The later physical producer's real Config metadata is in assets below.
	// The returned physical result keeps this three-column compatibility Config
	// even though current graph payloads are the individual raw state assets.
	// Its owned columns, validator maps and path strings cannot end with a raw
	// constructor scope. Three complete one-column units conservatively cover
	// the three-column config without pricing any unused legacy graph payload.
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig(meta.Name, cfg, def)
	if err != nil {
		return p, err
	}
	assets.copies(8, uint64(len(graphCfg.Columns))*20, nativeOrdinaryColumnGraphControlBytes)
	for _, column := range graphCfg.Columns {
		assets.copies(8, uint64(len(column.Name)), 1)
		assets.copies(8, uint64(len(column.Path)), 1)
		assets.copies(8, uint64(len(column.Path))+1, uint64(unsafe.Sizeof(string(""))))
	}
	for _, v := range [...]string{graphCfg.AssetManager.Namespace, graphCfg.ManifestRoot.Name} {
		assets.copies(8, uint64(len(v)), 1)
	}
	// All manifest consumers retain keys/values/maps independently of opened
	// payload selection. Existing 16-cohort manifest admission remains intact.
	manifest, err := nativeTypedCheckedMul(source.ManifestBackingBytes, 16)
	if err != nil {
		return p, err
	}
	persistent.add(manifest)
	// Retain all consumed typed bytes and decoded sections conservatively through
	// pack. This includes heap fallback and parsed-vector backing; no mmap-only
	// assumption, alias-based refund, or credit for unused row-image payloads.
	persistent.allocation(source.TypedSourceStoredBytes, 1)
	persistent.copies(12, source.TypedSourceDecodedBytes, 1)
	persistent.copies(8, source.DictionarySlots, uint64(unsafe.Sizeof(string("")))+8)
	// Per source part: part pointer, generation map, mapped handle, resource
	// descriptor, reader, registry link, outcome and source close control.
	sourceControls, err := nativeTypedCheckedMul(source.TypedSourceParts, 8)
	if err != nil {
		return p, err
	}
	persistent.copies(8, sourceControls, nativeOrdinaryColumnGraphControlBytes)
	// Authority per asset: ref/prepared/snapshot, obligation, token view and
	// validation index. Per five family segments: appender/file/parent/pin,
	// resource entry/backings, freeze/merge/union/closure and namespace controls.
	assetControls, err := nativeTypedCheckedMul(p.AssetCount, 6)
	if err != nil {
		return p, err
	}
	assets.copies(8, assetControls, nativeOrdinaryColumnGraphControlBytes)
	assets.copies(8, 5*16, nativeOrdinaryColumnGraphControlBytes)
	// One work.resourceBuilder is created before source/HNSW. Transfer this
	// original structural unit to common; the separate engine builder stays
	// with its existing parent floor. No unit is removed or counted twice.
	persistent.add(nativeOrdinaryColumnGraphControlBytes)
	if assets.err != nil {
		return p, assets.err
	}
	if assets.bytes < nativeOrdinaryColumnGraphControlBytes {
		return p, memtable.ErrCOWCapacity
	}
	assets.bytes -= nativeOrdinaryColumnGraphControlBytes
	assets.copies(8, p.AssetCount, uint64(unsafe.Sizeof(columnVectorIndexStateAssetSnapshot{})))
	assets.copies(8, p.AssetCount, uint64(unsafe.Sizeof(ColumnPreparedAsset{})))
	// Namespace path construction has eight separately backed paths per family.
	var path uint64
	for _, v := range [...]string{rootDir, cfg.AssetManager.Namespace, "column_assets/segments/prepared/quarantine/index/tmp/4294967295"} {
		if err = nativeTypedCheckedAdd(&path, uint64(len(v))); err != nil {
			return p, err
		}
	}
	assets.copies(8, 5*8, path)
	p.SourceReadBytes = source.TypedSourceReadPeak
	if err = nativeTypedCheckedAdd(&p.SourceReadBytes, source.TypedSourceDecoderBytes); err != nil {
		return p, err
	}
	p.SourceReadBytes = max(p.SourceReadBytes, source.PendingBuildBytes)
	p.NamespaceScanBytes = columnAssetSegmentFileIDScanWorkspaceBytes
	if err = nativeTypedCheckedAdd(&p.SourceReadBytes, p.NamespaceScanBytes); err != nil {
		return p, err
	}
	p.GraphPrivateBytes = c.PrivateBytes
	for layer := uint64(0); layer < layers; layer++ {
		var present uint64
		for level := layer; level < uint64(len(c.LevelRows)); level++ {
			if err = nativeTypedCheckedAdd(&present, c.LevelRows[level]); err != nil {
				return p, err
			}
		}
		degree := uint64(def.M)
		if layer == 0 {
			degree *= 2
		}
		values, e := nativeTypedCheckedMul(present, degree)
		if e != nil {
			return p, e
		}
		csr.allocation(c.Rows+1, 8)
		csr.allocation(values, 4)
	}
	csr.copies(8, layers, uint64(unsafe.Sizeof(int(0))))
	csr.allocation(layers, uint64(unsafe.Sizeof(typedcolumn.Uint32List{})))
	if csr.err != nil {
		return p, csr.err
	}
	// Each real factory is used for exactly the constructor it will feed. No
	// fabricated Rows slices and no copied graph/ID data are needed for planning.
	addAsset := func(acfg ColumnStoreConfig, column typedColumnAdapterColumn, kind typedcolumn.ColumnType, values uint64, role, id, logical, encoding string) error {
		opts := columnVectorGraphRawOrdinalOptions(acfg, column)
		imageOpts := typedcolumn.ColumnPartImageOptions{Dictionaries: typedColumnAdapterDictionaries([]typedColumnAdapterColumn{column}), LayoutLogicalTypes: map[string]string{typedColumnAdapterPrimaryIDColumn: string(typedcolumn.ColumnTypeInt64), column.Definition.Name: logical}}
		raw, e := typedcolumn.PlanRawOrdinalColumnPartImageShape(opts, typedcolumn.RawOrdinalColumnShape{Rows: int(c.Rows), Type: kind, ValueCount: values}, imageOpts)
		if e != nil {
			return e
		}
		p.SerialAssetBytes = max(p.SerialAssetBytes, raw.ConstructorBytes)
		// Keep actual generated names and metadata cohorts. The 20 structural
		// groups are listed in the bootstrap helper; strings have independent charge.
		assets.copies(8, 20, nativeOrdinaryColumnGraphControlBytes)
		for _, v := range [...]string{acfg.Columns[0].Name, acfg.Columns[0].Path, id, role, logical, encoding, acfg.AssetManager.Namespace, acfg.ManifestRoot.Name, typedColumnAdapterMetadataKey(column)} {
			assets.copies(8, uint64(len(v)), 1)
		}
		assets.copies(8, uint64(len(acfg.Columns[0].Path))+1, uint64(unsafe.Sizeof(string(""))))
		shape, e = shape.withAsset(role, id, logical, encoding, string(ColumnAssetKindTCS1TypedColumnPart), acfg.AssetManager.Namespace)
		return e
	}
	for layer := uint64(0); layer < layers; layer++ {
		acfg, col, e := columnVectorIndexStateAdjacencyColumnStoreConfig(meta.Name, cfg, def, int(layer))
		if e != nil {
			return p, e
		}
		var present uint64
		for level := layer; level < uint64(len(c.LevelRows)); level++ {
			if e = nativeTypedCheckedAdd(&present, c.LevelRows[level]); e != nil {
				return p, e
			}
		}
		degree := uint64(def.M)
		if layer == 0 {
			degree *= 2
		}
		values, e := nativeTypedCheckedMul(present, degree)
		if e != nil {
			return p, e
		}
		if e = addAsset(acfg, col, typedcolumn.ColumnTypeUint32List, values, columnVectorIndexStateAssetRoleAdjacency, columnVectorIndexStateAdjacencyAssetID(int(layer)), columnVectorIndexStateLogicalTypeUint32List, columnVectorIndexStateEncodingRawUint32List); e != nil {
			return p, e
		}
	}
	acfg, col, err := columnVectorGraphInvNormStateColumnStoreConfig(meta.Name, cfg, def)
	if err != nil {
		return p, err
	}
	if err = addAsset(acfg, col, typedcolumn.ColumnTypeFloat32, c.Rows, columnVectorIndexStateAssetRoleInverseNorm, columnVectorGraphInvNormStateAssetID, columnVectorIndexStateLogicalTypeFloat32, columnVectorIndexStateEncodingRawFloat32); err != nil {
		return p, err
	}
	for _, field := range [...]columnVectorGraphRowRefStateField{columnVectorGraphRowRefStateFieldGeneration, columnVectorGraphRowRefStateFieldPartID, columnVectorGraphRowRefStateFieldRowIndex, columnVectorGraphRowRefStateFieldAppliedCommandLSN, columnVectorGraphRowRefStateFieldOrdinalByPhysicalRow} {
		acfg, col, err = columnVectorGraphRowRefStateColumnStoreConfig(meta.Name, cfg, def, field)
		if err != nil {
			return p, err
		}
		if err = addAsset(acfg, col, typedcolumn.ColumnTypeInt64, c.Rows, columnVectorIndexStateAssetRoleRowRefs, columnVectorGraphRowRefStateAssetID(field), columnVectorIndexStateLogicalTypeInt64, columnVectorIndexStateEncodingRawInt64); err != nil {
			return p, err
		}
	}
	acfg, col, err = columnVectorGraphDocumentIDStateColumnStoreConfig(meta.Name, cfg, def)
	if err != nil {
		return p, err
	}
	if err = addAsset(acfg, col, typedcolumn.ColumnTypeBytes, c.IDBytes, columnVectorIndexStateAssetRoleDocumentIDs, columnVectorGraphDocumentIDStateAssetID, columnVectorIndexStateLogicalTypeBytes, columnVectorIndexStateEncodingRawBytesOffsets); err != nil {
		return p, err
	}
	shape, err = shape.withAsset(columnVectorIndexStateAssetRoleHNSWSearchPack, columnVectorIndexStateHNSWSearchPackAssetID, columnVectorIndexStateLogicalTypeSearchPack, columnVectorIndexStateEncodingHNSWSearchPackV1, string(ColumnAssetKindTCS1HNSWSearchPack), cfg.AssetManager.Namespace)
	if err != nil {
		return p, err
	}
	p.FreshStateRawBytes, err = shape.rawBytes()
	if err != nil {
		return p, err
	}
	if shape.assetCount != p.AssetCount {
		return p, backenddb.ErrNativeSemanticUnsupported
	}
	slot, err := nativeFreshVectorStateCapacityV2(p.FreshStateRawBytes, p.FreshStateRawBytes, p.AssetCount)
	if err != nil {
		return p, err
	} // no unproved compression-to-inline promise
	var requirements nativeColumnGraphCapacitySum
	requirements.add(slot)
	requirements.add(slot) // independent Walk and Lookup
	// Buffer doubling+final output+state/manifest encoder controls; numeric fields
	// are fixed width. No untrusted Snappy record is passed to this fresh route.
	requirements.copies(8, p.FreshStateRawBytes, 1)
	requirements.copies(8, p.AssetCount, nativeOrdinaryColumnGraphControlBytes)
	p.RequirementBytes = requirements.bytes
	// All CSR lists are built before the serial layer loop. Only one raw payload
	// and one rowref/invnorm/input are live inside each serial append scope.
	var serial nativeColumnGraphCapacitySum
	serial.add(p.SerialAssetBytes)
	serial.add(csr.bytes)
	serial.allocation(c.Rows, 8)
	serial.allocation(c.Rows+1, 8)
	serial.allocation(c.IDBytes, 1)
	serial.add(p.NamespaceScanBytes)
	serial.allocation(uint64(typedColumnPartDirectViewAssetAlignment), 1)
	p.SerialAssetBytes = serial.bytes
	sections := 8 + 2*layers
	var pack nativeColumnGraphCapacitySum
	pack.add(p.NamespaceScanBytes)
	pack.allocation(uint64(typedColumnPartDirectViewAssetAlignment), 1)
	pack.add(csr.bytes)
	pack.allocation(c.Rows, 2)
	pack.copies(4, c.Rows, 8)
	pack.allocation(c.Rows+1, 8)
	pack.allocation(c.IDBytes, 1)
	pack.allocation(layers, uint64(unsafe.Sizeof(columnHNSWSearchPackLayerInput{})))
	pack.copies(2, sections, uint64(unsafe.Sizeof(columnHNSWSearchPackSection{})))
	pack.allocation(1, 1<<20)
	pack.allocation(uint64(columnHNSWSearchPackHeaderSizeV2)+sections*uint64(columnHNSWSearchPackSectionEntrySize)+uint64(columnHNSWSearchPackVectorSectionAlignment), 1)
	pack.copies(8, sections, nativeOrdinaryColumnGraphControlBytes)
	p.PackBytes = pack.bytes
	var validation nativeColumnGraphCapacitySum
	// Every maintained direct-file check owns a fresh buffer. Charge all checks,
	// not one reused scratch: section checksums, vector, levels, L adjacency, four
	// rowrefs and ID offsets. No auxiliary-navigation or sourceV2 branch is used.
	validation.copies(sections+layers+8, 64<<10, 1)
	validation.allocation(uint64(columnHNSWSearchPackHeaderSizeV2)+sections*uint64(columnHNSWSearchPackSectionEntrySize), 1)
	validation.copies(8, sections, nativeOrdinaryColumnGraphControlBytes)
	validation.allocation(sections, uint64(unsafe.Sizeof(columnHNSWSearchPackSectionRange{})))
	p.ValidationBytes = validation.bytes
	if persistent.err != nil {
		return p, persistent.err
	}
	if serial.err != nil {
		return p, serial.err
	}
	if pack.err != nil {
		return p, pack.err
	}
	if validation.err != nil {
		return p, validation.err
	}
	if requirements.err != nil {
		return p, requirements.err
	}
	if assets.err != nil {
		return p, assets.err
	}
	p.PersistentBytes = persistent.bytes
	p.AssetsRetainedBytes = assets.bytes
	// Execution creates the new authority only after source extraction and the
	// offline graph builder have returned (materializeOrdinaryVectorGraph).
	// Original source mappings, pending producers and schema/basis remain
	// common; planner-only bootstrap stays on the original stage lease. Every future asset control/name/Config amount stays exact;
	// only its actual first-live phase changes. No credit or late growth occurs.
	suffix := max(p.SerialAssetBytes, p.PackBytes, p.ValidationBytes, p.RequirementBytes)
	if err = nativeTypedCheckedAdd(&suffix, p.AssetsRetainedBytes); err != nil {
		return p, err
	}
	peak := max(p.SourceReadBytes, p.GraphPrivateBytes, suffix)
	p.TotalBytes = p.PersistentBytes
	if err = nativeTypedCheckedAdd(&p.TotalBytes, peak); err != nil {
		return p, err
	}
	return p, nil
}
