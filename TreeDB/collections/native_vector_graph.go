package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// The recipe owns an original immutable catalog reference, not rows or an
// engine. Source completion and the catalog point dependency bind its physical
// basis before assignment. The original build executes it after assignment.
type nativeVectorGraphRecipe struct {
	schema              *nativeSchemaState
	def                 VectorIndexDefinition
	identity            ColumnManifestIdentity
	digest              [32]byte
	rows                uint64
	ordinary            bool
	sourceRoot          uint64
	constructionWorkers int
	pendingSourceCodec  nativeOrdinaryGraphPendingCodec
}

// Zero means the original conservative promise. A bound codec is the exact
// maintained producer's ordinal and vector data codecs, each plus one. Metadata
// codecs are absent because the graph projection does not decode those sections.
type nativeOrdinaryGraphPendingCodec uint32

func (c nativeOrdinaryGraphPendingCodec) needsZSTD() bool {
	zstd := nativeOrdinaryGraphPendingCodec(typedcolumn.CompressionZSTD) + 1
	return c&255 == zstd || c>>8&255 == zstd
}

func nativeOrdinaryGraphPendingSourceCodec(meta CollectionMeta) (nativeOrdinaryGraphPendingCodec, error) {
	if !nativeOrdinaryColumnGraphMeta(meta) || meta.Options.ColumnStore.AssetManager == nil {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	cfg := *meta.Options.ColumnStore
	fields := columnStoreTypedColumnPartFields(cfg)
	sortKey, err := typedColumnPartPublicationSortKey(cfg, fields)
	if err != nil {
		return 0, err
	}
	opts, err := typedColumnPublicationAdapterOptionsFromConfig(cfg, typedColumnPartAssetPartID, fields, sortKey)
	if err != nil {
		return 0, err
	}
	columns, err := typedColumnAdapterColumnsForFieldsWithOptions(fields, opts)
	if err != nil {
		return 0, err
	}
	return nativeOrdinaryGraphPendingSourceCodecForOptions(opts, columns)
}

func nativeOrdinaryGraphPendingSourceCodecForOptions(opts typedColumnAdapterOptions, columns []typedColumnAdapterColumn) (nativeOrdinaryGraphPendingCodec, error) {
	if len(columns) != 1 || columns[0].Field.ValueType != ColumnStoreValueFloat32Vector {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	// appendColumnSections copies these definitions' granule codecs to each
	// column-data section. imageOptions.SectionCompression applies only to
	// locator/dictionary/pruning metadata (canCompressImageSection), which the
	// supplied graph projection skips. Keep ordinal and vector codecs distinct.
	ordinal := typedColumnAdapterPrimaryIDDefinition(opts).Compression
	vector := columns[0].Definition.Compression
	for _, codec := range [...]typedcolumn.Compression{ordinal, vector} {
		switch codec {
		case typedcolumn.CompressionNone, typedcolumn.CompressionSnappy, typedcolumn.CompressionLZ4, typedcolumn.CompressionZSTD:
		default:
			return 0, backenddb.ErrNativeSemanticUnsupported
		}
	}
	codec := nativeOrdinaryGraphPendingCodec(ordinal) + 1 | (nativeOrdinaryGraphPendingCodec(vector)+1)<<8
	return codec, nil
}

// Producer-option validation has its OWN retained metadata envelope, separate
// from the admission-only bootstrap and every original pending/source charge.
// The existing bootstrap bounds three one-column factory units, each with 20
// structural groups and eight old/replacement growth cohorts. Validation adds
// at most two column arrays + their six one-entry mapper maps, one field array,
// two sort-key maps + clone, and fixed Options/reflect/closure controls: fewer
// than those 60 groups. Ordinary schema has exactly one non-null vector field;
// no row-sized allocation occurs here. Names/path strings borrow the original
// recipe/producer backing, and even full source-string backing is reserved by
// the envelope. One callback borrows two pointers for the whole work; each row
// producer joins before the next starts and its validation factories return
// before its batch is built. Thus ONE envelope covers their maximum live peak,
// independent of pending row count or graph-marker count, and remains charged
// through graph construction/publication rather than claiming unused 1MiB slack.
func nativeOrdinaryGraphBuildCapacityWithPendingValidation(total, metadataEnvelope uint64, codec nativeOrdinaryGraphPendingCodec) (uint64, error) {
	if codec == 0 {
		return total, nil
	}
	if metadataEnvelope == 0 {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	if err := nativeTypedCheckedAdd(&total, metadataEnvelope); err != nil {
		return 0, err
	}
	return total, nil
}

// Scalar-index drop DDL may replace the catalog schema while preserving the
// original primary typed chain. Independent scalar/text declarations do not
// describe the physical producer. Bind its complete column schema and ordinary
// vector definition; publication progress is already schema-free.
func nativeOrdinaryGraphPendingSourceMetaEqual(actual, prior CollectionMeta) bool {
	if actual.Name != prior.Name || !nativeOrdinaryColumnGraphMeta(actual) || !nativeOrdinaryColumnGraphMeta(prior) {
		return false
	}
	cfg := *actual.Options.ColumnStore
	nativeTypedCopyPhysicalMetadata(&cfg, prior.Options.ColumnStore)
	return columnStoreConfigEqual(&cfg, prior.Options.ColumnStore) && vectorIndexDefinitionValuesEqual(actual.VectorIndexes[0], prior.VectorIndexes[0])
}

// Summarize all guarded graph markers once without another retained slice/map.
// The returned callback borrows only this work and one original recipe until
// the synchronous physical producer joins. Non-graph chains get no callback.
func (w *nativeTypedMaterialization) ordinaryGraphPendingSourceValidator() (typedColumnPartOptionsValidator, error) {
	var bound *nativeVectorGraphRecipe
	for _, d := range w.input.deltas {
		if d == nil || d.data == nil || d.data.graph == nil || d.data.graph.pendingSourceCodec == 0 {
			continue
		}
		recipe := d.data.graph
		if !recipe.ordinary || recipe.schema == nil || recipe.schema.meta == nil || !nativeOrdinaryGraphPendingSourceMetaEqual(w.meta, *recipe.schema.meta) {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		if bound != nil && recipe.pendingSourceCodec != bound.pendingSourceCodec {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		bound = recipe
	}
	if bound == nil {
		return nil, nil
	}
	return func(cfg ColumnStoreConfig, opts typedColumnAdapterOptions, columns []typedColumnAdapterColumn) error {
		return validateNativeOrdinaryGraphPendingSourceOptions(w.meta, bound, cfg, opts, columns)
	}, nil
}

// Validate the actual producer's immutable options, not a second environment
// lookup. Both fresh and prepared batches pass the definitions the maintained
// builder will consume. This per-part check never walks the pending chain.
func validateNativeOrdinaryGraphPendingSourceOptions(meta CollectionMeta, recipe *nativeVectorGraphRecipe, cfg ColumnStoreConfig, opts typedColumnAdapterOptions, columns []typedColumnAdapterColumn) error {
	meta.Options.ColumnStore = &cfg
	if recipe == nil || recipe.schema == nil || recipe.schema.meta == nil || !nativeOrdinaryGraphPendingSourceMetaEqual(meta, *recipe.schema.meta) || cfg.AssetManager == nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	fields := columnStoreTypedColumnPartFields(cfg)
	sortKey, err := typedColumnPartPublicationSortKey(cfg, fields)
	if err != nil {
		return err
	}
	if opts.Namespace != cfg.AssetManager.Namespace || opts.SchemaVersion != uint32(cfg.SchemaHash) || !reflect.DeepEqual(opts.Fields, fields) || !reflect.DeepEqual(opts.SortKey, sortKey) {
		return backenddb.ErrNativeSemanticUnsupported
	}
	// Prepared batches carry their mapped definitions separately. Require
	// those same definitions as well as the options before the builder uses
	// them; stale options cannot authenticate a different prepared layout.
	expected, err := typedColumnAdapterColumnsForFieldsWithOptions(fields, opts)
	if err != nil {
		return err
	}
	if len(columns) != len(expected) {
		return backenddb.ErrNativeSemanticUnsupported
	}
	for i := range columns {
		if !reflect.DeepEqual(columns[i].Field, expected[i].Field) || columns[i].Definition != expected[i].Definition {
			return backenddb.ErrNativeSemanticUnsupported
		}
	}
	codec, err := nativeOrdinaryGraphPendingSourceCodecForOptions(opts, columns)
	if err != nil {
		return err
	}
	if codec != recipe.pendingSourceCodec {
		return backenddb.ErrNativeSemanticUnsupported
	}
	return nil
}

func (g *nativeVectorGraphRecipe) release() cowbtree.OwnedMapCompletion {
	schema := g.schema
	*g = nativeVectorGraphRecipe{}
	if schema == nil {
		return cowbtree.OwnedMapCompletion{}
	}
	return schema.release()
}

func nativeVectorGraphHeader(meta CollectionMeta, snap *backenddb.Snapshot, root uint64) (columnSourceDirectoryHeaderV2, error) {
	var zero columnSourceDirectoryHeaderV2
	cfg := meta.Options.ColumnStore
	if !nativeSourceImportMetaV2(meta) || cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil || !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) || root == 0 {
		return zero, backenddb.ErrNativeSemanticUnsupported
	}
	if err := validateColumnManifestIdentityAtRoot(snap, root, *cfg.ActiveManifest); err != nil {
		return zero, err
	}
	entry, err := snap.GetEntryAtRoot(root, []byte(columnSourceDirectoryHeaderKeyV2))
	if err != nil {
		return zero, err
	}
	if entry.Flags&(node.FlagTombstone|node.FlagPointer) != 0 {
		return zero, backenddb.ErrNativeSemanticUnsupported
	}
	h, err := decodeColumnSourceDirectoryHeaderV2(entry.Value)
	if err != nil {
		return zero, err
	}
	if h.CollectionDigest != sha256.Sum256([]byte(meta.Name)) || h.Generation != cfg.ActiveManifest.Generation || binary.BigEndian.Uint64(h.Digest[:8]) != cfg.ActiveManifest.Checksum || h.Rows > uint64(int(^uint(0)>>1)) {
		return zero, backenddb.ErrNativeSemanticUnsupported
	}
	return h, nil
}

// Inspect the exact captured records before admission. Per-record TCIM parsing
// uses the original admitted read preflight and its raw-section bounds. The
// constructor promise additionally covers coexisting HNSW maps, up to 33 layers
// of 2*M adjacency, all row/ref/ID/output views and both input/decode images.
func nativeVectorGraphCapacity(meta CollectionMeta, snap *backenddb.Snapshot, root uint64, rootDir string, def VectorIndexDefinition, h columnSourceDirectoryHeaderV2, admit func(uint64) error) (uint64, error) {
	total, err := nativeVectorGraphRowCapacity(meta, def, h.Rows)
	if err != nil {
		return 0, err
	}
	it, e := snap.IteratorAtRoot(root, columnManifestPartRecordPrefixBytes, []byte(columnSourceDirectoryNodePrefixV2))
	if e != nil {
		return 0, e
	}
	defer it.Close()
	for it.Valid() {
		if !it.IsDeleted() {
			raw, _, flags := it.UnsafeEntry()
			if flags&node.FlagPointer != 0 {
				return 0, backenddb.ErrNativeSemanticUnsupported
			}
			r := columnManifestRecord{key: it.UnsafeKey(), value: raw}
			if !columnManifestRecordKeyKnownForScan(r.key) {
				return 0, backenddb.ErrNativeSemanticUnsupported
			}
			n := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(r))) + memtable.COWAllocationCharge(uint64(len(r.key))) + memtable.COWAllocationCharge(uint64(len(r.value))) + preparedSemanticStreamMapEntryReserveBytes
			n, e = nativeTypedCheckedMul(n, 16)
			if e != nil {
				return 0, e
			}
			if e = nativeTypedCheckedAdd(&total, n); e != nil {
				return 0, e
			}
			// Reuse the original complete record decoder, including prior TVIS
			// assets. Their stored bytes and decoded row views can coexist with
			// replacement output and status validation.
			if e = visitColumnManifestPhysicalRefsForOwnership([]columnManifestRecord{r}, h.Generation, meta.Options.ColumnStore.AssetManager.Namespace, func(ref ColumnAssetRef, _ bool) error {
				n, e := nativeTypedCheckedMul(uint64(ref.Length), 16)
				if e != nil {
					return e
				}
				return nativeTypedCheckedAdd(&total, n)
			}); e != nil {
				return 0, e
			}
			if bytes.HasPrefix(r.key, columnManifestPartRecordPrefixBytes) {
				extra, _, e := nativeTypedFamilyReadCapacity(meta, snap, root, rootDir, nil, admit, r)
				if e != nil {
					return 0, e
				}
				if e = nativeTypedCheckedAdd(&total, extra); e != nil {
					return 0, e
				}
			}
		}
		it.Next()
	}
	if e = it.Error(); e != nil {
		return 0, e
	}
	// The fresh graph suffix can contain a compressed TVIS image up to the
	// original decoded limit even when no prior graph exists. Admit both
	// independent requirement-decoder slots BEFORE assignment, in this same
	// source-derived constructor promise. Captured old-state slots stay charged
	// separately; no typed-row codec is reachable from this graph-only branch.
	// IDs are not bounded by vector dimensions or encoded compressed asset size.
	token, ok := snap.StateToken()
	if !ok {
		return 0, backenddb.ErrClosed
	}
	primary, e := snap.GetAtRoot(token.SystemRootPageID, []byte(systemCollectionRootKey(collectionPrimaryRootName(meta.Name))))
	if e != nil {
		return 0, e
	}
	primaryRoot, e := decodeRootID(primary)
	if e != nil {
		return 0, e
	}
	ids, e := snap.IteratorAtRoot(primaryRoot, nil, nil)
	if e != nil {
		return 0, e
	}
	defer ids.Close()
	var count uint64
	for ids.Valid() {
		if !ids.IsDeleted() {
			count++
			n, e := nativeTypedCheckedMul(uint64(len(ids.UnsafeKey())), 64)
			if e != nil {
				return 0, e
			}
			if e = nativeTypedCheckedAdd(&total, n); e != nil {
				return 0, e
			}
		}
		ids.Next()
	}
	if e = ids.Error(); e != nil {
		return 0, e
	}
	if count != h.Rows {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	return total, nil
}

func (c *Collection) nativeRebuildVectorGraph(name string) (out VectorIndexStatus, resultErr error) {
	if err := ValidateIndexName(name); err != nil {
		return out, err
	}
	tx, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		return out, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	g := &tx.bindings[0]
	ordinary := nativeOrdinaryColumnGraphMeta(g.probe.meta)
	if (!nativeSourceImportMetaV2(g.probe.meta) && !ordinary) || tx.db.StableResourceMetadataOwner() == nil {
		return out, backenddb.ErrNativeSemanticUnsupported
	}
	if err = nativeTypedProfile(tx.db, g.probe.meta); err != nil {
		return out, err
	}
	def, ok := findVectorIndex(g.probe.meta.VectorIndexes, name)
	if !ok {
		return out, ErrIndexNotFound
	}
	// The original normalized representation rebuild rewrites its base corpus;
	// this cdir2-preserving producer does not implement that separate operation.
	if vectorIndexUsesCosineNormalizedF32V1(def) {
		return out, backenddb.ErrNativeSemanticUnsupported
	}
	primary, _ := g.probe.collection.domains.Get("primary")
	if !ordinary && primary != nil && primary.typed != nil {
		return out, backenddb.ErrNativeSemanticUnsupported
	}
	manifest, ok := g.probe.collection.domains.Get("column/manifest")
	if !ok {
		return out, backenddb.ErrNativeSemanticUnsupported
	}
	snap := tx.core.Snapshot()
	var h columnSourceDirectoryHeaderV2
	var capacity uint64
	var constructionWorkers int
	var pendingSourceCodec nativeOrdinaryGraphPendingCodec
	if ordinary {
		capacity, h.Rows, constructionWorkers, pendingSourceCodec, err = nativeOrdinaryVectorGraphCapacity(tx, g, primary, manifest.baseRoot, def)
	} else {
		h, err = nativeVectorGraphHeader(g.probe.meta, snap, manifest.baseRoot)
		if err == nil {
			var stage uint64
			stage, err = nativeSourceRequirementBasisCapacityV2(snap, manifest.baseRoot)
			if err == nil {
				err = tx.admitStage(g, stage)
			}
		}
		if err == nil {
			capacity, err = nativeVectorGraphCapacity(g.probe.meta, snap, manifest.baseRoot, tx.db.ColumnAssetRootDir(), def, h, func(n uint64) error { return tx.admitStage(g, n) })
		}
	}
	if err != nil {
		return out, fmt.Errorf("collections: native graph source preflight: %w", err)
	}
	owned := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeVectorGraphRecipe{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTypedRows{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})))
	lease, err := tx.core.NativeAllocator().AdmitNativeBytes(owned)
	if err != nil {
		return out, fmt.Errorf("collections: native graph retained recipe: %w", err)
	}
	if err = tx.adoptNativeTyped(g, nil, nil, nil, ColumnPublishOperationInsert, capacity, lease); err != nil {
		lease.Close()
		return out, fmt.Errorf("collections: native graph adopt recipe: %w", err)
	}
	schema := g.probe.collection.schema
	schema.retain()
	recipe := &nativeVectorGraphRecipe{schema: schema, def: def, digest: h.Digest, rows: h.Rows, ordinary: ordinary, sourceRoot: manifest.baseRoot, constructionWorkers: constructionWorkers, pendingSourceCodec: pendingSourceCodec}
	if identity := g.probe.meta.Options.ColumnStore.ActiveManifest; identity != nil {
		recipe.identity = *identity
	}
	g.typed.data.graph = recipe
	// The original payload has a fixed ten-byte header and the two names.
	payloadBytes := uint64(10)
	if err = nativeTypedCheckedAdd(&payloadBytes, uint64(len(g.name))); err != nil {
		return out, err
	}
	if err = nativeTypedCheckedAdd(&payloadBytes, uint64(len(name))); err != nil {
		return out, err
	}
	if err = tx.admitStage(g, memtable.COWAllocationCharge(payloadBytes)); err != nil {
		return out, err
	}
	payload, err := commitlog.EncodeCollectionRebuildVectorIndexPayload(g.name, name)
	if err != nil {
		return out, err
	}
	apply := nativeCommandWALApplyCommit{frame: commandwalapply.LoweredFrame{Kind: commitlog.CommandKindCollectionRebuildVectorIndex, Scope: commitlog.CommandScopeCollection, PayloadFormat: commitlog.PayloadFormatCollectionRebuildVectorIndexV1, Payload: payload}, sync: true}
	if err = tx.commitNativeApply(true, &apply); err != nil {
		if apply.result.LogicalInstalled {
			err = commitAmbiguousError("graph rebuild sealed completion", err)
		}
		return out, fmt.Errorf("collections: native graph commit logical_installed=%t consumed=%t constructor_bytes=%d: %w", apply.result.LogicalInstalled, apply.result.Consumed, capacity, err)
	}
	out, err = c.nativeVectorGraphStatus(name)
	if err != nil {
		return out, commitAmbiguousError("graph rebuild completed; status unavailable", err)
	}
	return out, nil
}

func nativeVectorGraphRecords(snap *backenddb.Snapshot, root uint64, ordinary bool) ([]columnManifestRecord, error) {
	start := columnManifestPartRecordPrefixBytes
	if ordinary {
		start = columnManifestHeaderRecordKeyBytes
	}
	it, err := snap.IteratorAtRoot(root, start, []byte(columnSourceDirectoryNodePrefixV2))
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var records []columnManifestRecord
	for it.Valid() {
		if !it.IsDeleted() {
			raw, _, flags := it.UnsafeEntry()
			if flags&node.FlagPointer != 0 {
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
			key := it.UnsafeKey()
			if !columnManifestRecordKeyKnownForScan(key) {
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
			records = append(records, columnManifestRecord{key: bytes.Clone(key), value: bytes.Clone(raw)})
		}
		it.Next()
	}
	return records, it.Error()
}

func (c *Collection) nativeVectorGraphStatus(name string) (out VectorIndexStatus, resultErr error) {
	if err := ValidateIndexName(name); err != nil {
		return out, err
	}
	r, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return out, err
	}
	defer func() { resultErr = errors.Join(resultErr, r.Close()) }()
	def, ok := findVectorIndex(r.meta.VectorIndexes, name)
	if !ok {
		return out, ErrIndexNotFound
	}
	primary, _ := r.collection.domains.Get("primary")
	if primary != nil && primary.typed != nil {
		return out, backenddb.ErrNativeSemanticUnsupported
	}
	manifest, ok := r.collection.domains.Get("column/manifest")
	if !ok {
		return out, backenddb.ErrNativeSemanticUnsupported
	}
	var checksum uint64
	if nativeOrdinaryColumnGraphMeta(r.meta) {
		if r.meta.Options.ColumnStore.ActiveManifest == nil {
			return out, backenddb.ErrNativeSemanticUnsupported
		}
		identity := *r.meta.Options.ColumnStore.ActiveManifest
		if err = validateColumnManifestIdentityAtRoot(r.capture.Snapshot(), manifest.baseRoot, identity); err != nil {
			return out, err
		}
	} else {
		h, e := nativeVectorGraphHeader(r.meta, r.capture.Snapshot(), manifest.baseRoot)
		if e != nil {
			return out, e
		}
		checksum = binary.BigEndian.Uint64(h.Digest[:8])
	}
	// A status inspection uses the same original read lease and exact record
	// census; it does not construct graph rows or retain an engine/cache.
	charge, err := nativeVectorGraphRecordCapacity(r.capture.Snapshot(), manifest.baseRoot, nativeOrdinaryColumnGraphMeta(r.meta))
	if err != nil {
		return out, err
	}
	metadata, err := nativeSchemaBackingCharge(reflect.ValueOf(&r.meta), 0)
	if err != nil {
		return out, err
	}
	metadata, err = nativeTypedCheckedMul(metadata, 16)
	if err != nil {
		return out, err
	}
	if err = nativeTypedCheckedAdd(&charge, metadata); err != nil {
		return out, err
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return out, err
	}
	defer lease.Close()
	records, err := nativeVectorGraphRecords(r.capture.Snapshot(), manifest.baseRoot, nativeOrdinaryColumnGraphMeta(r.meta))
	if err != nil {
		return out, err
	}
	if nativeOrdinaryColumnGraphMeta(r.meta) {
		base, e := decodeColumnManifestSnapshotForScan(records)
		if e != nil {
			return out, e
		}
		checksum, err = columnVectorGraphBaseManifestChecksum(base, records, *r.meta.Options.ColumnStore)
		if err != nil {
			return out, err
		}
	}
	// Status never constructs HNSW or a replacement TVIS. Its original decoder
	// first checks the captured state identity; stale graphs open no assets.
	// Only a matching state reaches reference/config validation and the inverse-
	// norm reader. Status does not open adjacency, rowref, ID or pack payloads.
	// The shared scan-only norm projection requires uncompressed float32 data;
	// sixteen copies of that asset cover input, parsed views and heap fallbacks.
	// Existing record/state/schema charges retain the complete metadata controls.
	// This narrower bound applies only to the ordinary unquantized lane. Other
	// shapes retain their existing allowance, including quantized row metadata.
	return columnGraphVectorIndexStatusForRecordsWithAdmission(r.database.ColumnAssetRootDir(), r.meta.Name, r.meta.Options.ColumnStore, def, records, checksum, true, func(state columnVectorIndexStateSnapshot) error {
		for _, asset := range state.Assets {
			if asset.Ref.Length <= 0 {
				return backenddb.ErrNativeSemanticUnsupported
			}
			if nativeOrdinaryColumnGraphMeta(r.meta) && asset.Role != columnVectorIndexStateAssetRoleInverseNorm {
				continue
			}
			n, err := nativeTypedCheckedMul(uint64(asset.Ref.Length), 16)
			if err != nil {
				return err
			}
			if err = nativeTypedCheckedAdd(&charge, n); err != nil {
				return err
			}
		}
		return lease.Resize(charge)
	})
}

func nativeVectorGraphRecordCapacity(snap *backenddb.Snapshot, root uint64, ordinary bool) (uint64, error) {
	total, err := nativeVectorGraphRecordBackingCapacity(snap, root, ordinary)
	if err != nil {
		return 0, err
	}
	basis, err := nativeSourceRequirementBasisCapacityV2(snap, root)
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&total, basis); err != nil {
		return 0, err
	}
	return total, nil
}

// Price the actual copied manifest records independently of materialization's
// requirement-stream codec. Serving never constructs that stream, but must
// separately admit its actual TVIS decodes and payload validation phases.
func nativeVectorGraphRecordBackingCapacity(snap *backenddb.Snapshot, root uint64, ordinary bool) (uint64, error) {
	start := columnManifestPartRecordPrefixBytes
	if ordinary {
		start = columnManifestHeaderRecordKeyBytes
	}
	it, err := snap.IteratorAtRoot(root, start, []byte(columnSourceDirectoryNodePrefixV2))
	if err != nil {
		return 0, err
	}
	defer it.Close()
	var total uint64
	for it.Valid() {
		if !it.IsDeleted() {
			raw, _, flags := it.UnsafeEntry()
			if flags&node.FlagPointer != 0 {
				return 0, backenddb.ErrNativeSemanticUnsupported
			}
			n := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(columnManifestRecord{}))) + memtable.COWAllocationCharge(uint64(len(it.UnsafeKey()))) + memtable.COWAllocationCharge(uint64(len(raw))) + preparedSemanticStreamMapEntryReserveBytes
			n, err = nativeTypedCheckedMul(n, 16)
			if err != nil {
				return 0, err
			}
			if err = nativeTypedCheckedAdd(&total, n); err != nil {
				return 0, err
			}
		}
		it.Next()
	}
	if err = it.Error(); err != nil {
		return 0, err
	}
	return total, nil
}

func (w *nativeTypedMaterialization) materializeVectorGraph(d *nativeTypedDelta) error {
	recipe := d.data.graph
	cfg := *w.meta.Options.ColumnStore
	h, err := nativeVectorGraphHeader(w.meta, w.input.snapshot, w.input.baseManifestRoot)
	if err != nil {
		return err
	}
	if d.appliedLSN == 0 || !columnManifestIdentityValueEqual(recipe.identity, *cfg.ActiveManifest) || h.Digest != recipe.digest || h.Rows != recipe.rows {
		return backenddb.ErrNativeSemanticUnsupported
	}
	rootDir, registry, err := w.input.group.NativeColumnAssetBuildContext()
	if err != nil {
		return err
	}
	records, err := nativeVectorGraphRecords(w.input.snapshot, w.input.baseManifestRoot, false)
	if err != nil {
		return err
	}
	roots := make(map[string]uint64, len(w.roots))
	for _, root := range w.roots {
		roots[w.meta.Name+"/"+root.name] = root.root
	}
	catalog := newCollectionCatalog(w.meta, roots)
	rows, source, used, err := columnVectorGraphRowsFromTypedColumnSnapshot(nil, rootDir, w.input.snapshot, catalog, cfg, records, columnManifestSnapshot{Collection: w.meta.Name, Generation: h.Generation, RowCount: int(h.Rows), AppliedCommandLSN: h.AppliedCommandLSN}, recipe.def)
	w.graphSource = source // Original work owns even a partial reader until cleanup.
	defer func() { clear(rows) }()
	if err != nil {
		return err
	}
	if !used || uint64(len(rows)) != h.Rows {
		return backenddb.ErrNativeSemanticUnsupported
	}
	if err = buildColumnVectorGraphAdjacency(rows, recipe.def); err != nil {
		return err
	}
	authority, err := newColumnVectorGraphStableResourceAccumulator(registry)
	if err != nil {
		return err
	}
	w.graphAuthority = authority
	capture, err := typedGraphBaseCaptureAdmission(w.meta)
	if err != nil {
		return err
	}
	prepared, graph, state, canonical, err := prepareColumnVectorGraphRebuildAssets(w.meta.Name, cfg, recipe.def, h.Generation, recipe.identity.Checksum, h.AppliedCommandLSN, records, rows, rootDir, authority, source, nil, capture)
	clear(rows)
	rows = nil
	if err != nil {
		return err
	}
	// Freeze the same producer authority before any later record/root failure.
	assets := state.Assets
	if canonical {
		assets = make([]columnVectorIndexStateAssetSnapshot, 0, len(state.Assets))
		for _, a := range state.Assets {
			if a.Role != columnVectorIndexStateAssetRoleNormalizedVectors {
				assets = append(assets, a)
			}
		}
	}
	resources, err := authority.freeze(assets)
	if err != nil {
		return err
	}
	w.graphAuthority = nil
	// Merge transfers the frozen child only on success. The original work owns
	// it until then, so a conflict follows the same retained cleanup as any
	// later materialization failure.
	w.resources = resources
	if err = w.resourceBuilder.Merge(w.resources); err != nil {
		return err
	}
	w.resources = nil
	_ = prepared
	raw, err := encodeColumnVectorGraphManifestRecord(graph)
	if err != nil {
		return err
	}
	stateRaw, err := encodeColumnVectorIndexStateRecord(state)
	if err != nil {
		return err
	}
	suffix := []columnManifestRecord{{key: columnVectorGraphManifestRecordKey(recipe.def.Name), value: raw}, {key: columnVectorIndexStateRecordKey(recipe.def.Name), value: stateRaw}}
	sortColumnManifestRecords(suffix)
	delta := batch.NewBoundedRetainingLargeEntries(nil, int(^uint(0)>>1), nativeBuildBatchEntries)
	defer delta.CloseWithoutPooling()
	for _, r := range suffix {
		if err = delta.Set(r.key, r.value); err != nil {
			return err
		}
	}
	policy, err := collectionRootStoragePolicy(w.meta, collectionColumnManifestRootName(w.meta.Name))
	if err != nil {
		return err
	}
	root, err := w.input.group.BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput{BaseRoot: w.input.baseManifestRoot, Delta: delta, StoragePolicy: policy})
	if err != nil {
		return err
	}
	w.roots[1].root = root
	w.meta, err = columnGraphRebuildUpdatedMeta(w.meta, recipe.identity, d.appliedLSN)
	if err != nil {
		return err
	}
	w.records = suffix
	w.requirements, err = nativeSourceManifestRequirementsV2(w.input.snapshot, w.input.baseManifestRoot, h.Generation, cfg.AssetManager.Namespace, suffix, w.meta.VectorIndexes...)
	if err != nil {
		return err
	}
	w.resources, err = w.resourceBuilder.Freeze()
	if err != nil {
		return err
	}
	w.resourceBuilder = nil
	return nil
}

func nativeVectorGraphRowCapacity(meta CollectionMeta, def VectorIndexDefinition, rows uint64) (uint64, error) {
	// Original graph/state config normalization and asset encoders can retain
	// several derived schema/name views alongside both final handoff schemas.
	metadata, err := nativeSchemaBackingCharge(reflect.ValueOf(&meta), 0)
	if err != nil {
		return 0, err
	}
	total, err := nativeTypedCheckedMul(metadata, 16)
	if err != nil {
		return 0, err
	}
	perRow := uint64(64 * 1024)
	for _, term := range [][2]uint64{{uint64(def.Dimensions), 128}, {uint64(max(def.M, defaultVectorIndexM)), 33 * 2 * 128}} {
		n, e := nativeTypedCheckedMul(term[0], term[1])
		if e != nil {
			return 0, e
		}
		if e = nativeTypedCheckedAdd(&perRow, n); e != nil {
			return 0, e
		}
	}
	// Each declared quantizer retains its original code/alpha payload, decoded
	// views and per-row calibration candidates alongside the graph emission.
	quantized, e := nativeTypedCheckedMul(uint64(def.Dimensions), 128)
	if e != nil {
		return 0, e
	}
	if e = nativeTypedCheckedAdd(&quantized, 64*1024); e != nil {
		return 0, e
	}
	quantized, e = nativeTypedCheckedMul(quantized, uint64(len(def.QuantizedIndexes)))
	if e != nil {
		return 0, e
	}
	if e = nativeTypedCheckedAdd(&perRow, quantized); e != nil {
		return 0, e
	}
	n, e := nativeTypedCheckedMul(rows, perRow)
	if e != nil {
		return 0, e
	}
	if e = nativeTypedCheckedAdd(&total, n); e != nil {
		return 0, e
	}
	if e = nativeTypedCheckedAdd(&total, 2*nativeSourceRequirementStateCapacityV2(columnVectorIndexStateMaxDecodedBytes)); e != nil {
		return 0, e
	}
	return total, nil
}

// The scoped ordinary constructor uses the maintained fixed-width graph and
// raw-asset producers. Other shapes retain their original conservative promise.
// Capture the execution ceiling here; the recipe binds the actual builder to it
// even if GOMAXPROCS changes after this immutable source census.
func nativeOrdinaryVectorGraphCapacity(tx *NativeJSONTxn, g *nativeJSONTxnBinding, primary *nativeDomainState, root uint64, def VectorIndexDefinition) (uint64, uint64, int, nativeOrdinaryGraphPendingCodec, error) {
	if def.M != 16 || def.EfConstruction != 128 || def.Metric != VectorMetricCosine || def.Encoding != VectorIndexEncodingFloat32 || def.Representation != "" || len(def.QuantizedIndexes) != 0 {
		n, rows, err := nativeOrdinaryVectorGraphConservativeCapacity(tx, g, primary, root, def)
		return n, rows, 0, 0, err
	}
	workers := min(max(runtime.GOMAXPROCS(0), 1), 16)
	construction, pending, err := nativeOrdinaryVectorGraphConstructionCensus(tx, primary, def, workers)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if construction.Rows == 0 {
		n, rows, e := nativeOrdinaryVectorGraphConservativeCapacity(tx, g, primary, root, def)
		return n, rows, 0, 0, e
	}
	if root == 0 && pending == 0 {
		return 0, 0, 0, 0, fmt.Errorf("collections: ordinary graph requires an initial manifest or row-producing pending prefix: %w", backenddb.ErrNativeSemanticUnsupported)
	}
	source := nativeOrdinaryColumnGraphSourceCensus{PendingBuildBytes: pending}
	// Physical source sections below keep their exact stored-codec proof.
	// Pending output uses the maintained producer policy, bound into the recipe.
	var pendingCodec nativeOrdinaryGraphPendingCodec
	needsZSTD := false
	// Schema/handoff and the original four-root group are already retained by
	// nativeTypedCollectionRetainedCapacity. No second schema owner is invented.
	rootDir := tx.db.ColumnAssetRootDir()
	bootstrap, err := nativeOrdinaryColumnGraphMetadataPreflightCapacity(g.probe.meta, def, rootDir)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if err = tx.admitStage(g, bootstrap); err != nil {
		return 0, 0, 0, 0, err
	}
	if pending != 0 {
		pendingCodec, err = nativeOrdinaryGraphPendingSourceCodec(g.probe.meta)
		if err != nil {
			return 0, 0, 0, 0, err
		}
		needsZSTD = pendingCodec.needsZSTD()
	}
	snap := tx.core.Snapshot()
	if root != 0 {
		identity := g.probe.meta.Options.ColumnStore.ActiveManifest
		if identity == nil {
			return 0, 0, 0, 0, backenddb.ErrNativeSemanticUnsupported
		}
		if err = validateColumnManifestIdentityAtRoot(snap, root, *identity); err != nil {
			return 0, 0, 0, 0, err
		}
		metadata, e := nativeTypedManifestCapacity(snap, root)
		if e != nil {
			return 0, 0, 0, 0, e
		}
		if e = tx.admitStage(g, metadata); e != nil {
			return 0, 0, 0, 0, e
		}
		// One parser/file scope finishes before the next. Retain the maximum scratch
		// on this original stage lease, admitting only its incremental high-water.
		var scratch uint64
		admit := func(n uint64) error {
			if n <= scratch {
				return nil
			}
			if e := tx.admitStage(g, n-scratch); e != nil {
				return e
			}
			scratch = n
			return nil
		}
		it, e := snap.IteratorAtRoot(root, columnManifestHeaderRecordKeyBytes, nil)
		if e != nil {
			return 0, 0, 0, 0, e
		}
		for it.Valid() {
			key := it.UnsafeKey()
			if !columnManifestRecordKeyKnownForScan(key) {
				break
			}
			if !it.IsDeleted() {
				raw, _, flags := it.UnsafeEntry()
				if flags&node.FlagPointer != 0 {
					e = backenddb.ErrNativeSemanticUnsupported
					break
				}
				n := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(columnManifestRecord{}))) + memtable.COWAllocationCharge(uint64(len(key))) + memtable.COWAllocationCharge(uint64(len(raw))) + preparedSemanticStreamMapEntryReserveBytes
				if e = nativeTypedCheckedAdd(&source.ManifestBackingBytes, n); e != nil {
					break
				}
				if e = nativeTypedCheckedAdd(&source.ManifestRecords, 1); e != nil {
					break
				}
				if bytes.HasPrefix(key, columnManifestPartRecordPrefixBytes) {
					part, pe := decodeColumnManifestPartRecord(raw)
					if pe != nil {
						e = pe
						break
					}
					if part.AssetRef.Kind == ColumnAssetKindTCS1TypedColumnPart {
						if part.AssetRef.Length < 0 {
							e = backenddb.ErrNativeSemanticUnsupported
							break
						}
						if e = nativeTypedCheckedAdd(&source.TypedSourceStoredBytes, uint64(part.AssetRef.Length)); e != nil {
							break
						}
						if e = nativeTypedCheckedAdd(&source.TypedSourceParts, 1); e != nil {
							break
						}
						var shape nativeTypedFamilyReadShape
						var extra uint64
						extra, _, e = nativeTypedFamilyReadCapacity(g.probe.meta, snap, root, rootDir, &shape, admit, columnManifestRecord{key: key, value: raw})
						if e != nil {
							break
						}
						needsZSTD = needsZSTD || shape.columnDataZSTD
						if e = nativeTypedCheckedAdd(&source.TypedSourceDecodedBytes, shape.decodedBytes); e != nil {
							break
						}
						if e = nativeTypedCheckedAdd(&source.DictionarySlots, shape.dictionarySlots); e != nil {
							break
						}
						read := scratch
						if e = nativeTypedCheckedAdd(&read, extra); e != nil {
							break
						}
						source.TypedSourceReadPeak = max(source.TypedSourceReadPeak, read)
					}
				}
			}
			it.Next()
		}
		e = errors.Join(e, it.Error(), it.Close())
		if e != nil {
			return 0, 0, 0, 0, e
		}
	}
	// The supplied vector reader skips metadata decompression. Keep ALL stored
	// and decoded source cohorts above, and reserve the original fixed decoder
	// whenever a captured data section or the bound pending producer needs it.
	if needsZSTD {
		source.TypedSourceDecoderBytes, err = nativeTypedDecoderFixedCapacity()
		if err != nil {
			return 0, 0, 0, 0, err
		}
	}
	plan, err := nativeOrdinaryColumnGraphPhaseCapacity(g.probe.meta, def, rootDir, construction, source)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	// Ordinary construction already owns the complete manifest and creates
	// stableColumnManifestDurableRequirements from it. It does not construct the
	// cdir2 streaming reader or its two leaf slots. Keep both untrusted captured
	// TVIS slots independently of fresh-state slots, with all codec limits intact.
	decoded, err := nativeSourceRequirementBasisDecodedLengthV2(snap, root)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	basis := 2 * nativeSourceRequirementStateCapacityV2(decoded)
	if err = nativeTypedCheckedAdd(&plan.TotalBytes, basis); err != nil {
		return 0, 0, 0, 0, err
	}
	// The earlier bootstrap remains on the original stage owner. A SEPARATE
	// complete envelope in this original graph promise covers the later exact
	// producer-option validation while all source/pending promises coexist.
	capacity, err := nativeOrdinaryGraphBuildCapacityWithPendingValidation(plan.TotalBytes, bootstrap, pendingCodec)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return capacity, construction.Rows, workers, pendingCodec, nil
}

// nativeOrdinaryVectorGraphConstructionCensus visits the captured physical IDs
// and the immutable pending row superset. Replaced/deleted IDs can only increase
// the row/level charge; no materialized merged corpus or fake carrier is needed.
func nativeOrdinaryVectorGraphConstructionCensus(tx *NativeJSONTxn, primary *nativeDomainState, def VectorIndexDefinition, workers int) (nativeColumnGraphConstructionPlan, uint64, error) {
	var rows, pendingBuild uint64
	if primary == nil {
		return nativeColumnGraphConstructionPlan{}, 0, backenddb.ErrNativeSemanticUnsupported
	}
	walk := func(visit func([]byte) error) error {
		if primary.baseRoot != 0 {
			it, err := tx.core.Snapshot().IteratorAtRoot(primary.baseRoot, nil, nil)
			if err != nil {
				return err
			}
			for it.Valid() {
				if !it.IsDeleted() {
					if err = visit(it.UnsafeKey()); err != nil {
						break
					}
				}
				it.Next()
			}
			err = errors.Join(err, it.Error(), it.Close())
			if err != nil {
				return err
			}
		}
		for d := primary.typed; d != nil; d = d.previous {
			if d.data != nil && d.data.graph != nil {
				continue
			}
			for _, row := range d.rows {
				if err := visit(row.ID); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(func([]byte) error { return nativeTypedCheckedAdd(&rows, 1) }); err != nil {
		return nativeColumnGraphConstructionPlan{}, 0, err
	}
	for d := primary.typed; d != nil; d = d.previous {
		if d.data != nil && d.data.graph != nil {
			continue
		}
		if err := nativeTypedCheckedAdd(&pendingBuild, d.buildCapacity); err != nil {
			return nativeColumnGraphConstructionPlan{}, 0, err
		}
	}
	plan, err := nativeColumnGraphConstructionCapacityForWorkers(def, rows, workers, walk)
	return plan, pendingBuild, err
}

// Pending assets do not yet have physical lengths. Keep their complete original
// construction promises (sixteen coexisting views, as for physical refs above),
// in addition to unchanged graph row/ID geometry. This deliberately preserves
// the existing conservative refusal until the complete promise is selected.
func nativeOrdinaryVectorGraphConservativeCapacity(tx *NativeJSONTxn, g *nativeJSONTxnBinding, primary *nativeDomainState, root uint64, def VectorIndexDefinition) (uint64, uint64, error) {
	var physical, pending, pendingBytes, idBytes uint64
	if primary == nil {
		return 0, 0, backenddb.ErrNativeSemanticUnsupported
	}
	if primary.baseRoot != 0 {
		it, err := tx.core.Snapshot().IteratorAtRoot(primary.baseRoot, nil, nil)
		if err != nil {
			return 0, 0, err
		}
		for it.Valid() {
			if !it.IsDeleted() {
				if err = nativeTypedCheckedAdd(&physical, 1); err != nil {
					break
				}
			}
			it.Next()
		}
		err = errors.Join(err, it.Error(), it.Close())
		if err != nil {
			return 0, 0, err
		}
	}
	for d := primary.typed; d != nil; d = d.previous {
		if d.data != nil && d.data.graph != nil {
			continue
		}
		if err := nativeTypedCheckedAdd(&pending, uint64(len(d.rows))); err != nil {
			return 0, 0, err
		}
		if err := nativeTypedCheckedAdd(&pendingBytes, d.buildCapacity); err != nil {
			return 0, 0, err
		}
		for _, row := range d.rows {
			if err := nativeTypedCheckedAdd(&idBytes, uint64(len(row.ID))); err != nil {
				return 0, 0, err
			}
		}
	}
	// No row-producing prefix means no initial ordinary manifest can be
	// produced before the graph marker. Refuse before retaining/adopting a
	// recipe or assigning a command; a large budget must not turn this known
	// unsupported finalizer input into an accepted maintenance failure.
	if root == 0 && pending == 0 {
		return 0, 0, fmt.Errorf("collections: ordinary graph requires an initial manifest or row-producing pending prefix: %w", backenddb.ErrNativeSemanticUnsupported)
	}
	rows := physical
	if err := nativeTypedCheckedAdd(&rows, pending); err != nil {
		return 0, 0, err
	}
	var total uint64
	var err error
	if root == 0 {
		total, err = nativeVectorGraphRowCapacity(g.probe.meta, def, 0)
	} else {
		identity := g.probe.meta.Options.ColumnStore.ActiveManifest
		if identity == nil {
			return 0, 0, backenddb.ErrNativeSemanticUnsupported
		}
		if err = validateColumnManifestIdentityAtRoot(tx.core.Snapshot(), root, *identity); err != nil {
			return 0, 0, err
		}
		total, err = nativeVectorGraphCapacity(g.probe.meta, tx.core.Snapshot(), root, tx.db.ColumnAssetRootDir(), def, columnSourceDirectoryHeaderV2{Rows: physical, Generation: identity.Generation}, func(n uint64) error { return tx.admitStage(g, n) })
	}
	if err != nil {
		return 0, 0, err
	}
	extra, err := nativeVectorGraphRowCapacity(g.probe.meta, def, pending)
	if err != nil {
		return 0, 0, err
	}
	for _, term := range []struct{ n, factor uint64 }{{extra, 1}, {pendingBytes, 16}, {idBytes, 64}} {
		n, e := nativeTypedCheckedMul(term.n, term.factor)
		if e != nil {
			return 0, 0, e
		}
		if e = nativeTypedCheckedAdd(&total, n); e != nil {
			return 0, 0, e
		}
	}
	return total, rows, nil
}

// Complete ordinary pending row materialization and graph emission share this
// original Work. The old basis authenticates the recipe; the actual source
// identity/locators come from the preceding successful row plans, not cdir2.
func (w *nativeTypedMaterialization) materializeOrdinaryVectorGraph(d *nativeTypedDelta) error {
	recipe := d.data.graph
	if !recipe.ordinary || !nativeOrdinaryColumnGraphMeta(w.meta) || recipe.sourceRoot != w.input.baseManifestRoot || d.appliedLSN == 0 {
		return fmt.Errorf("ordinary graph basis ordinary=%t meta_ordinary=%t recipe_root=%d build_root=%d lsn=%d: %w", recipe.ordinary, nativeOrdinaryColumnGraphMeta(w.meta), recipe.sourceRoot, w.input.baseManifestRoot, d.appliedLSN, backenddb.ErrNativeSemanticUnsupported)
	}
	def, ok := findVectorIndex(w.meta.VectorIndexes, recipe.def.Name)
	if !ok || !reflect.DeepEqual(def, recipe.def) {
		return fmt.Errorf("ordinary graph vector declaration found=%t equal=%t: %w", ok, reflect.DeepEqual(def, recipe.def), backenddb.ErrNativeSemanticUnsupported)
	}
	cfg := *w.meta.Options.ColumnStore
	if cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil || !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		return fmt.Errorf("ordinary graph manifest identities active=%t recovery=%t: %w", cfg.ActiveManifest != nil, cfg.RecoveryAuthoritativeManifest != nil, backenddb.ErrNativeSemanticUnsupported)
	}
	manifest, err := decodeColumnManifestRecords(w.records)
	if err != nil {
		return err
	}
	if err = validateColumnManifestSnapshot(manifest, w.records, cfg, *cfg.ActiveManifest, w.meta.Name, "native ordinary graph source"); err != nil {
		return err
	}
	rootDir, registry, err := w.input.group.NativeColumnAssetBuildContext()
	if err != nil {
		return err
	}
	roots := make(map[string]uint64, len(w.roots))
	for _, root := range w.roots {
		roots[w.meta.Name+"/"+root.name] = root.root
	}
	catalog := newCollectionCatalog(w.meta, roots)
	if w.graphSource != nil {
		if err = w.graphSource.Close(); err != nil {
			return err
		}
		w.graphSource = nil
	}
	if recipe.rows > uint64(int(^uint(0)>>1)) {
		return fmt.Errorf("ordinary graph row bound rows=%d: %w", recipe.rows, backenddb.ErrNativeSemanticUnsupported)
	}
	var rows []columnVectorGraphAssetRow
	var source *columnVectorGraphTypedColumnVectorSource
	var used bool
	// Determine whether the pair is the original basis or this group's output;
	// never retry a rejected group read against a different authority.
	token, ok := w.input.snapshot.StateToken()
	if !ok {
		return backenddb.ErrClosed
	}
	var base [2]uint64
	for i, name := range []string{"primary", "column/row-locator"} {
		raw, e := w.input.snapshot.GetAtRoot(token.SystemRootPageID, []byte(systemCollectionRootPrefix+w.meta.Name+"/"+name))
		if errors.Is(e, tree.ErrKeyNotFound) {
			continue
		}
		if e != nil {
			return e
		}
		base[i], e = decodeRootID(raw)
		if e != nil {
			return e
		}
	}
	if base[0] == w.roots[0].root && base[1] == w.roots[2].root {
		rows, source, used, err = columnVectorGraphRowsFromTypedColumnSnapshotWithRoots(nil, rootDir, w.input.snapshot, catalog, cfg, w.records, manifest, def, nil, nil, false, int(recipe.rows))
	} else {
		err = w.input.group.WithNativeConstructionRoots([]uint64{w.roots[0].root, w.roots[2].root}, func(it []iterator.UnsafeIterator) error {
			var e error
			rows, source, used, e = columnVectorGraphRowsFromTypedColumnSnapshotWithRoots(nil, rootDir, w.input.snapshot, catalog, cfg, w.records, manifest, def, it[0], it[1], true, int(recipe.rows))
			return e
		})
	}
	w.graphSource = source
	defer func() { clear(rows) }()
	if err != nil {
		return err
	}
	if !used || uint64(len(rows)) > recipe.rows {
		return fmt.Errorf("ordinary graph source used=%t rows=%d bound=%d: %w", used, len(rows), recipe.rows, backenddb.ErrNativeSemanticUnsupported)
	}
	if recipe.constructionWorkers > 0 {
		err = buildColumnVectorGraphAdjacencyWithWorkers(rows, def, recipe.constructionWorkers)
	} else {
		err = buildColumnVectorGraphAdjacency(rows, def)
	}
	if err != nil {
		return err
	}
	authority, err := newColumnVectorGraphStableResourceAccumulator(registry)
	if err != nil {
		return err
	}
	w.graphAuthority = authority
	capture, err := typedGraphBaseCaptureAdmission(w.meta)
	if err != nil {
		return err
	}
	prepared, records, identity, err := prepareColumnVectorGraphRebuildManifestWithAuthorityTimed(w.meta.Name, cfg, w.meta.VectorIndexes, def, manifest, w.records, d.appliedLSN, rows, rootDir, authority, source, nil, capture)
	if err != nil {
		return err
	}
	w.graphAuthority = nil
	w.resources = prepared.stableResources
	prepared.stableResources = nil
	if w.resources == nil {
		return fmt.Errorf("ordinary graph missing stable resources: %w", backenddb.ErrNativeSemanticUnsupported)
	}
	if err = w.resourceBuilder.Merge(w.resources); err != nil {
		return err
	}
	w.resources = nil
	identityRecord := encodeColumnManifestIdentityRecordArray(identity)
	it := columnStoreCompactionManifestDeltaIterator(identityRecord, w.records, records)
	policy, err := collectionRootStoragePolicy(w.meta, w.meta.Name+"/column/manifest")
	if err == nil {
		err = w.buildIterator(1, it, policy)
	}
	err = errors.Join(err, it.Close())
	if err != nil {
		return err
	}
	updated, err := columnGraphRebuildUpdatedMeta(w.meta, identity, d.appliedLSN)
	if err != nil {
		return err
	}
	requirements, err := stableColumnManifestDurableRequirements(records, identity.Generation, cfg.AssetManager.Namespace)
	if err != nil {
		return err
	}
	w.meta, w.records, w.requirements = updated, records, requirements
	return nil
}
