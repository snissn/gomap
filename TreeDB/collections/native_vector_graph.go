package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

// The recipe owns an original immutable catalog reference, not rows or an
// engine. Source completion and the catalog point dependency bind its physical
// basis before assignment. The original build executes it after assignment.
type nativeVectorGraphRecipe struct {
	schema   *nativeSchemaState
	def      VectorIndexDefinition
	identity ColumnManifestIdentity
	digest   [32]byte
	rows     uint64
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
	n, e := nativeTypedCheckedMul(h.Rows, perRow)
	if e != nil {
		return 0, e
	}
	if e = nativeTypedCheckedAdd(&total, n); e != nil {
		return 0, e
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
	if e = nativeTypedCheckedAdd(&total, 2*nativeSourceRequirementStateCapacityV2(columnVectorIndexStateMaxDecodedBytes)); e != nil {
		return 0, e
	}
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
	if !nativeSourceImportMetaV2(g.probe.meta) || tx.db.StableResourceMetadataOwner() == nil {
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
	if primary != nil && primary.typed != nil {
		return out, backenddb.ErrNativeSemanticUnsupported
	}
	manifest, ok := g.probe.collection.domains.Get("column/manifest")
	if !ok {
		return out, backenddb.ErrNativeSemanticUnsupported
	}
	snap := tx.core.Snapshot()
	h, err := nativeVectorGraphHeader(g.probe.meta, snap, manifest.baseRoot)
	if err != nil {
		return out, err
	}
	if err = tx.admitStage(g, nativeSourceRequirementCapacityV2()); err != nil {
		return out, fmt.Errorf("collections: native graph initial stage: %w", err)
	}
	capacity, err := nativeVectorGraphCapacity(g.probe.meta, snap, manifest.baseRoot, tx.db.ColumnAssetRootDir(), def, h, func(n uint64) error { return tx.admitStage(g, n) })
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
	g.typed.data.graph = &nativeVectorGraphRecipe{schema: schema, def: def, identity: *g.probe.meta.Options.ColumnStore.ActiveManifest, digest: h.Digest, rows: h.Rows}
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
	return c.nativeVectorGraphStatus(name)
}

func nativeVectorGraphRecords(snap *backenddb.Snapshot, root uint64) ([]columnManifestRecord, error) {
	it, err := snap.IteratorAtRoot(root, columnManifestPartRecordPrefixBytes, []byte(columnSourceDirectoryNodePrefixV2))
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
	h, err := nativeVectorGraphHeader(r.meta, r.capture.Snapshot(), manifest.baseRoot)
	if err != nil {
		return out, err
	}
	// A status inspection uses the same original read lease and exact record
	// census; it does not construct graph rows or retain an engine/cache.
	charge, err := nativeVectorGraphRecordCapacity(r.capture.Snapshot(), manifest.baseRoot)
	if err != nil {
		return out, err
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return out, err
	}
	defer lease.Close()
	// The maintained status validator opens inv-norm assets and can use heap
	// fallback. Admit the same captured source/asset/decoded-row bound before
	// its original decoder allocates, independently of record compression.
	capacity, err := nativeVectorGraphCapacity(r.meta, r.capture.Snapshot(), manifest.baseRoot, r.database.ColumnAssetRootDir(), def, h, func(n uint64) error { return lease.Resize(max(charge, n)) })
	if err != nil {
		return out, err
	}
	if err = nativeTypedCheckedAdd(&charge, capacity); err != nil {
		return out, err
	}
	if err = lease.Resize(charge); err != nil {
		return out, err
	}
	records, err := nativeVectorGraphRecords(r.capture.Snapshot(), manifest.baseRoot)
	if err != nil {
		return out, err
	}
	return columnGraphVectorIndexStatusForRecords(r.database.ColumnAssetRootDir(), r.meta.Name, r.meta.Options.ColumnStore, def, records, binary.BigEndian.Uint64(h.Digest[:8]), true)
}

func nativeVectorGraphRecordCapacity(snap *backenddb.Snapshot, root uint64) (uint64, error) {
	it, err := snap.IteratorAtRoot(root, columnManifestPartRecordPrefixBytes, []byte(columnSourceDirectoryNodePrefixV2))
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
	if err = nativeTypedCheckedAdd(&total, nativeSourceRequirementCapacityV2()); err != nil {
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
	records, err := nativeVectorGraphRecords(w.input.snapshot, w.input.baseManifestRoot)
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
