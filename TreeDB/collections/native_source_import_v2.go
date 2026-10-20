package collections

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/sourcepartition"
	"github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

// A ColumnGraph declaration is source metadata, not a live ANN capability.
// The only admitted producer for this metadata is the immutable source import.
func nativeSourceImportMetaV2(meta CollectionMeta) bool {
	if !nativeTypedMeta(meta) || meta.Options.ColumnStore.RetainedPayload != ColumnRetainedPayloadNonColumn || len(meta.VectorIndexes) == 0 || len(meta.Indexes)+len(meta.TextIndexes) != 0 || normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatJSON || columnRetainedPayloadEffectiveEncoding(meta.Options.ColumnStore) != ColumnRetainedPayloadEncodingJSON {
		return false
	}
	for _, index := range meta.VectorIndexes {
		if index.Strategy != VectorIndexStrategyColumnGraph {
			return false
		}
		found := false
		for _, col := range meta.Options.ColumnStore.Columns {
			if col.Path == index.Field && col.ValueType == ColumnStoreValueFloat32Vector && col.Owner == TypedStorageOwnerColumnPart && columnStoreFloat32VectorElementsPerRow(col) == index.Dimensions {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	cfg := meta.Options.ColumnStore
	return cfg.ActiveManifest == nil || (cfg.ActiveManifest.Format == columnSourceDirectoryFormatV2 && cfg.ActiveManifest.Version == 2)
}

// The descriptor and all original source bytes share this immutable row edge.
// It owns no engine, snapshot or application callback.
type nativeSourceImportDeltaV2 struct {
	publication sourceImportPublicationV2
	refund      *cowbtree.OwnedMapRefund
}

func (d *nativeSourceImportDeltaV2) release() cowbtree.OwnedMapCompletion {
	d.publication = sourceImportPublicationV2{}
	refund := d.refund
	d.refund = nil
	return refund.Completion()
}

// Source input already has a checked 512KiB cap. Its retained descriptor owns
// one leaf, fixed accumulator/proof frontier, keys and canonical binding. Stage
// work additionally covers canonical JSON escaping and the existing typed
// projection; the original typed constructor separately admits its own rows.
func nativeSourceImportCapacityV2(input VectorPartitionSourceImportChunkV2, ids, retained [][]byte, columns []TypedColumnBatch) (owned, work uint64, err error) {
	owned = uint64(unsafe.Sizeof(nativeSourceImportDeltaV2{})) + uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{}))
	var source uint64
	add := func(n uint64) error { return nativeTypedCheckedAdd(&source, n) }
	for _, id := range ids {
		if err = add(uint64(len(id))); err != nil {
			return
		}
	}
	for _, raw := range retained {
		if err = add(uint64(len(raw))); err != nil {
			return
		}
	}
	for _, col := range columns {
		if err = add(uint64(len(col.Name))); err != nil {
			return
		}
		for _, v := range col.Strings {
			if err = add(uint64(len(v))); err != nil {
				return
			}
		}
		for _, v := range col.Float32Vectors {
			var n uint64
			n, err = nativeTypedCheckedMul(uint64(len(v)), 4)
			if err != nil {
				return
			}
			if err = add(n); err != nil {
				return
			}
		}
	}
	// Each provenance record has a fixed digest/five ordinals and one original
	// bounded string; the same leaf encoder supplies these fields, without JSON.
	for _, origin := range input.LegacyOrigins {
		if origin != nil {
			if len(origin.CollectionScope) > 4096 {
				err = memtable.ErrCOWCapacity
				return
			}
			if err = add(uint64(len(origin.CollectionScope)) + 32 + 5*8 + 4); err != nil {
				return
			}
		}
	}
	// The source leaf holds independently owned IDs, vectors, revision/origin
	// controls and row headers in addition to its encoded bytes.
	headers, e := nativeTypedCheckedMul(uint64(len(ids)), uint64(unsafe.Sizeof(vectorpartition.SourceRowV2{}))+uint64(unsafe.Sizeof([]byte{}))+8)
	if e != nil {
		err = e
		return
	}
	if err = add(headers); err != nil {
		return
	}
	// Canonical binding is independently bounded by the original format cap.
	fixed := uint64(maxSourceImportBindingBytesV2) + uint64(columnSourceDirectoryHeaderBytesV2) + 128*uint64(unsafe.Sizeof(vectorpartition.SourceSnapshotNodeV2{}))
	if err = nativeTypedCheckedAdd(&owned, source); err != nil {
		return
	}
	if err = nativeTypedCheckedAdd(&owned, fixed); err != nil {
		return
	}
	owned = memtable.COWAllocationCharge(owned)
	// Existing typed canonical staging uses sixteen source copies for maps,
	// projection controls and coexisting canonical images. JSON source metadata
	// may escape six bytes per original ID; this is inside that same envelope.
	if err = nativeTypedCheckedAdd(&source, fixed); err != nil {
		return
	}
	work, err = nativeTypedCheckedMul(source, 16)
	if err == nil {
		work = memtable.COWAllocationCharge(work)
	}
	return
}

func (c *Collection) nativeImportVectorPartitionSourceChunkV2(ownership sourcepartition.ResolvedSourceShardMapV2, input VectorPartitionSourceImportChunkV2, ids, retained [][]byte, columns []TypedColumnBatch, stats *VectorPartitionSourceImportStatsV2) (out VectorPartitionSourceImportProgressV2, resultErr error) {
	if c.manager.isClosing() {
		return out, backenddb.ErrClosed
	}
	if len(ids) == 0 || len(ids) > vectorpartition.MaxSourceChunkRowsV2 || len(retained) != len(ids) || len(input.DocumentRevisions) != len(ids) || (len(input.LegacyOrigins) != 0 && len(input.LegacyOrigins) != len(ids)) {
		return out, errors.New("collections: source import row bounds")
	}
	if err := sourceImportInputBoundsV2(ids, retained, columns); err != nil {
		return out, err
	}
	tx, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		return out, fmt.Errorf("collections: native source begin: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	g := &tx.bindings[0]
	if !nativeSourceImportMetaV2(g.probe.meta) {
		return out, backenddb.ErrNativeSemanticUnsupported
	}
	if tx.db.StableResourceMetadataOwner() == nil {
		return out, backenddb.ErrNativeSemanticUnsupported
	}
	if err = nativeTypedProfile(tx.db, g.probe.meta); err != nil {
		return out, err
	}
	ref := ownership.Collection()
	if ref.Collection != g.name || input.Snapshot.CollectionScope != ref.Database+"/"+ref.Catalog+"/"+ref.Collection || input.Snapshot.SourceMapEpoch != ownership.Epoch() || hex.EncodeToString(input.Snapshot.SourceMapDigest[:]) != ownership.Digest() {
		return out, errors.New("collections: source import ownership identity")
	}
	owner, err := ownership.ResolveDocumentID(ids[0])
	if err != nil {
		return out, err
	}
	if input.GroupID == "" || input.GroupID != owner.GroupID {
		return out, errors.New("collections: source import nonowner group")
	}
	owned, scratch, err := nativeSourceImportCapacityV2(input, ids, retained, columns)
	if err != nil {
		return out, err
	}
	lease, err := tx.core.NativeAllocator().AdmitNativeBytes(owned)
	if err != nil {
		return out, fmt.Errorf("collections: native source owned bytes=%d: %w", owned, err)
	}
	defer func() {
		if lease != nil {
			lease.Close()
		}
	}()
	if err = tx.admitStage(g, scratch); err != nil {
		return out, fmt.Errorf("collections: native source stage bytes=%d: %w", scratch, err)
	}
	// ValidateImportIDs owns one temporary int per ID and sorts it in place.
	// Its scratch ends before projection/canonical construction; the admitted
	// per-row header term alone covers it within the existing stage envelope.
	if err = ownership.ValidateImportIDs(input.Snapshot.ShardID, ids); err != nil {
		return out, err
	}
	projection, err := newTrustedTypedProjection(g.probe.meta, ids, retained, columns)
	if err != nil {
		return out, err
	}
	ordered := make([][]byte, len(ids))
	for i, id := range ids {
		ordered[i] = bytes.Clone(id)
	}
	command := sourceImportCommandMetadataV2{Version: 2, OrderedIDs: ordered, Binding: sourceImportBindingV2{Snapshot: input.Snapshot, IndexName: input.IndexName, VectorColumn: input.VectorColumn, GroupID: input.GroupID, Start: owner.Start, End: owner.End}, ChunkIndex: input.ChunkIndex, DocumentRevisions: slices.Clone(input.DocumentRevisions), LegacyOrigins: input.LegacyOrigins}
	chunk, err := sourceImportChunkFromProjectionV2(g.probe.meta, command, ids, projection)
	if err != nil {
		return out, err
	}
	docs, err := collectionDocumentsFromBatchInput(ids, retained)
	if err != nil {
		return out, err
	}
	typed, err := typedCommandPayload(g.probe.meta, docs, projection)
	if err != nil {
		return out, err
	}
	metadata, err := json.Marshal(command)
	if err != nil {
		return out, err
	}
	payload, err := commitlog.EncodeCollectionSourceImportPayloadV2(commitlog.CollectionSourceImportPayloadV2{Metadata: metadata, Inserted: typed})
	if err != nil {
		return out, err
	}
	if stats != nil {
		stats.CommandPayloadBytes = uint64(len(payload))
		stats.SourceProgressReads += 4
	}
	binding, err := json.Marshal(sourceImportBindingRecordV2{Version: 2, Binding: command.Binding})
	if err != nil {
		return out, err
	}
	if len(binding) > maxSourceImportBindingBytesV2 {
		return out, errors.New("collections: source import binding bytes cap")
	}
	progressKey, receiptKey := sourceImportKeysV2(g.name, command.Binding, command.ChunkIndex)
	snap := tx.core.Snapshot()
	previous, present, e1 := getSystemValue(snap, progressKey)
	receipt, receiptPresent, e2 := getSystemValue(snap, receiptKey)
	oldBinding, bindingPresent, e3 := getSystemValue(snap, progressKey+":binding")
	seal, sealPresent, e4 := getSystemValue(snap, progressKey+":seal")
	if err = errors.Join(e1, e2, e3, e4); err != nil {
		return out, err
	}
	publication, result, retry, err := prepareSourceImportPublicationV2(command, chunk, binding, sourceImportReceiptV2(payload), progressKey, receiptKey, previous, present, receipt, receiptPresent, oldBinding, bindingPresent, seal, sealPresent, stats)
	if err != nil {
		return out, err
	}
	if retry {
		return result, nil
	}
	// Source calls require sealed physical completion. A captured pending source
	// would mean a preceding original call has not completed; never read past it.
	primary, _ := g.probe.collection.domains.Get("primary")
	if primary != nil && primary.typed != nil {
		return out, fmt.Errorf("collections: pending source import requires original completion: %w", backenddb.ErrNativeSemanticUnsupported)
	}
	g.allowSourceImport = true
	if _, _, err = tx.stageNativeTypedBatch(g, ids, retained, columns, nil, commitlog.CommandKindCollectionInsertBatchByID, false); err != nil {
		return out, fmt.Errorf("collections: native source typed stage: %w", err)
	}
	refund := new(cowbtree.OwnedMapRefund)
	refund.Init(lease)
	lease = nil
	g.typed.data.source = &nativeSourceImportDeltaV2{publication: *publication, refund: refund}
	// Directory rows/keys/proofs and fallback suffix coexist with the original
	// physical builder. Preserve the independent retained receipt and promise.
	extra, e := nativeSourceDirectoryDeltaCapacityV2(publication)
	if e != nil {
		return out, e
	}
	if e = nativeTypedCheckedAdd(&g.typed.buildCapacity, extra); e != nil {
		return out, e
	}
	apply := nativeCommandWALApplyCommit{frame: commandwalapply.LoweredFrame{Kind: commitlog.CommandKindCollectionReplaceSourceByID, Scope: commitlog.CommandScopeCollection, PayloadFormat: commitlog.PayloadFormatCollectionSourceImportV2, Payload: payload}, sync: true}
	if err = tx.commitNativeApply(true, &apply); err != nil {
		err = fmt.Errorf("collections: native source commit logical_installed=%t consumed=%t: %w", apply.result.LogicalInstalled, apply.result.Consumed, err)
		if apply.result.LogicalInstalled {
			err = commitAmbiguousError("source import sealed completion", err)
		}
		return out, err
	}
	return result, nil
}
