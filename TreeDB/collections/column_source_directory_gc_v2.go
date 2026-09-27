package collections

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/vectorpartition"
	"github.com/snissn/gomap/TreeDB/node"
)

// GC is an explicit V2 consumer. Ordinary legacy scans/builds still refuse this
// format. Discovery reads only asset-reference prefixes, never source leaf or
// proof records, and refuses incomplete discovery before any destructive work.
func (c *Collection) prepareColumnAssetReachabilitySnapshotV2(ctx context.Context, maxRecords int, maxBytes int64) (columnPhysicalScanSnapshotView, func(), error) {
	if c == nil || c.db == nil {
		return columnPhysicalScanSnapshotView{}, nil, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return columnPhysicalScanSnapshotView{}, nil, backenddb.ErrClosed
	}
	closeView := func() { _ = snap.Close() }
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		closeView()
		return columnPhysicalScanSnapshotView{}, nil, err
	}
	if catalog == nil || catalog.meta.Options.ColumnStore == nil {
		closeView()
		return columnPhysicalScanSnapshotView{}, nil, errCollectionNotFound
	}
	cfg := *catalog.meta.Options.ColumnStore
	if cfg.ActiveManifest == nil || cfg.ActiveManifest.Format != columnSourceDirectoryFormatV2 {
		closeView()
		return c.prepareColumnPhysicalScanSnapshotViewWithContextAndSidecarsAndBudget(ctx, columnManifestScanAllSidecars(), maxRecords, maxBytes)
	}
	view, err := c.prepareColumnSourceDirectoryReachabilityAtSnapshotV2(ctx, snap, catalog, catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name)), cfg, maxRecords, maxBytes)
	if err != nil {
		closeView()
		return view, nil, err
	}
	return view, closeView, nil
}

func (c *Collection) prepareColumnSourceDirectoryReachabilityAtSnapshotV2(ctx context.Context, snap *backenddb.Snapshot, catalog *collectionCatalog, root uint64, cfg ColumnStoreConfig, maxRecords int, maxBytes int64) (columnPhysicalScanSnapshotView, error) {
	var view columnPhysicalScanSnapshotView
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return view, err
	}
	if snap == nil || catalog == nil || root == 0 || !cfg.Enabled || cfg.AssetManager == nil || cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil || cfg.ActiveManifest.Format != columnSourceDirectoryFormatV2 || cfg.ActiveManifest.Version != 2 || !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		return view, errors.New("collections: incomplete source directory recovery identity")
	}
	// Hard discovery ceilings remain explicit even when the caller supplies no
	// limits. A refused/incomplete plan never authorizes deletion.
	const hardRecords = 1 << 20
	const hardBytes = 64 << 20
	if maxRecords <= 0 || maxRecords > hardRecords {
		maxRecords = hardRecords
	}
	if maxBytes <= 0 || maxBytes > hardBytes {
		maxBytes = hardBytes
	}
	if err := validateColumnManifestIdentityAtRoot(snap, root, *cfg.ActiveManifest); err != nil {
		return view, err
	}
	entry, err := snap.GetEntryAtRoot(root, []byte(columnSourceDirectoryHeaderKeyV2))
	if err != nil {
		return view, err
	}
	if entry.Flags&(node.FlagPointer|node.FlagTombstone) != 0 {
		return view, errors.New("collections: source directory header is not inline")
	}
	header, err := decodeColumnSourceDirectoryHeaderV2(entry.Value)
	if err != nil {
		return view, err
	}
	schema, err := VectorPartitionSourceSchemaDigestV2(cfg)
	if err != nil {
		return view, err
	}
	if header.Generation != cfg.ActiveManifest.Generation || header.CollectionDigest != sha256.Sum256([]byte(catalog.meta.Name)) || header.SchemaDigest != schema || header.AppliedCommandLSN != cfg.RecoveryAuthoritativeAppliedCommandLSN || binary.BigEndian.Uint64(header.Digest[:8]) != cfg.ActiveManifest.Checksum {
		return view, errors.New("collections: source directory GC identity mismatch")
	}
	if header.Parts > uint64(maxRecords) {
		return view, ErrColumnAssetReachabilityManifestLimit
	}
	state, ok := snap.StateToken()
	if !ok {
		return view, backenddb.ErrClosed
	}
	view = columnPhysicalScanSnapshotView{CollectionName: catalog.meta.Name, Catalog: catalog, Config: columnStoreRowAssetConfig(cfg), FullConfig: cfg, ColumnStoreEnabled: true, CommitSeq: state.CommitSeq, SystemRoot: state.SystemRootPageID, ColumnAssetRootDir: c.db.ColumnAssetRootDir(), AssetNamespace: cfg.AssetManager.Namespace, snapshot: snap}
	view.Diagnostics = columnPhysicalScanDiagnostics{ManifestRoot: root, ManifestRootName: collectionColumnManifestRootName(catalog.meta.Name), ManifestGeneration: header.Generation, ActiveManifestChecksum: cfg.ActiveManifest.Checksum, RecoveryManifestGeneration: header.Generation, RecoveryManifestChecksum: cfg.RecoveryAuthoritativeManifest.Checksum, AppliedCommandLSN: header.AppliedCommandLSN}
	records, size := 1, int64(len(columnSourceDirectoryHeaderKeyV2)+len(entry.Value))
	if size > maxBytes {
		return view, ErrColumnAssetReachabilityManifestLimit
	}
	scan := func(prefix []byte, visit func([]byte, []byte) error) error {
		it, err := snap.IteratorAtRoot(root, prefix, prefixEnd(prefix))
		if err != nil {
			return err
		}
		defer it.Close()
		for it.Valid() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := it.UnsafeKey()
			value, _, flags := it.UnsafeEntry()
			if flags&(node.FlagPointer|node.FlagTombstone) != 0 || len(value) > 3000 {
				return errors.New("collections: source directory asset record is not bounded inline data")
			}
			if records >= maxRecords || int64(len(key))+int64(len(value)) > maxBytes-size {
				return ErrColumnAssetReachabilityManifestLimit
			}
			records++
			size += int64(len(key) + len(value))
			if err := visit(key, value); err != nil {
				return err
			}
			it.Next()
		}
		return it.Error()
	}
	var live columnManifestLivePartRowsForScan
	var refs, rows uint64
	err = scan(columnManifestPartRecordPrefixBytes, func(key, value []byte) error {
		generation, partID, err := columnManifestPartKeyFromRecordKeyForScan(key)
		if err != nil {
			return err
		}
		ref, count, _, _, _, reason, err := decodeColumnManifestPartFieldsForScan(value, cfg.AssetManager.Namespace)
		if err != nil {
			return err
		}
		if generation == 0 || generation > header.Generation || ref.Generation != generation || ref.PartID != partID || count <= 0 || count > vectorpartition.MaxSourceChunkRowsV2 {
			return errors.New("collections: source directory GC part identity/bounds")
		}
		op, ok := columnPhysicalScanOperationFromBytes(reason)
		if !ok || op != ColumnPublishOperationInsert {
			return errors.New("collections: source directory GC refuses mutation part")
		}
		role, err := decodeColumnManifestPartRoleForScan(value, ref, reason)
		if err != nil {
			return err
		}
		sortKey, err := decodeColumnManifestPartSortKeyForScan(value)
		if err != nil {
			return err
		}
		part := columnManifestAssetRefForScan{Ref: ref, Rows: count, Reason: op, Role: role, SortKey: sortKey}
		switch ref.Kind {
		case ColumnAssetKindTCS1PartImage:
			view.AssetRefs = append(view.AssetRefs, part)
			rows += uint64(count)
		case ColumnAssetKindTCS1TypedColumnPart:
			view.TypedColumnPartRefs = append(view.TypedColumnPartRefs, part)
		default:
			return errors.New("collections: source directory GC unsupported asset kind")
		}
		live.add(generation, partID, count)
		refs++
		return nil
	})
	if err != nil {
		return view, err
	}
	for _, item := range []struct {
		prefix []byte
		visit  func([]byte, []byte) error
	}{
		{columnManifestAggregateMetadataRecordPrefixBytes, func(key, value []byte) error {
			r, e := decodeColumnManifestAggregateMetadataRecordForScan(key, value, cfg.AssetManager.Namespace, header.Generation, &live, "")
			if e == nil {
				view.AggregateMetadata = append(view.AggregateMetadata, r)
				refs++
			}
			return e
		}},
		{columnManifestDictionaryCodesRecordPrefixBytes, func(key, value []byte) error {
			r, e := decodeColumnManifestDictionaryCodesRecordForScan(key, value, cfg.AssetManager.Namespace, header.Generation, &live, "")
			if e == nil {
				view.DictionaryCodes = append(view.DictionaryCodes, r)
				refs++
			}
			return e
		}},
		{columnManifestInt64ValuesRecordPrefixBytes, func(key, value []byte) error {
			r, e := decodeColumnManifestInt64ValuesRecordForScan(key, value, cfg.AssetManager.Namespace, header.Generation, &live, "")
			if e == nil {
				view.Int64Values = append(view.Int64Values, r)
				refs++
			}
			return e
		}},
		{columnManifestSegmentOwnershipRecordPrefixBytes, func(key, value []byte) error {
			r, e := decodeColumnManifestSegmentOwnershipForScan(key, value, cfg.AssetManager.Namespace, header.Generation)
			if e == nil && r.Graph {
				return errors.New("collections: source directory GC unexpected graph ownership")
			}
			if e == nil {
				view.SegmentOwnership = append(view.SegmentOwnership, r)
			}
			return e
		}},
	} {
		if err := scan(item.prefix, item.visit); err != nil {
			return view, err
		}
	}
	for _, prefix := range [][]byte{columnManifestVectorGraphRecordPrefixBytes, columnVectorIndexStateRecordPrefixBytes} {
		if err := scan(prefix, func(_, _ []byte) error {
			return errors.New("collections: source directory GC refuses unsupported graph metadata")
		}); err != nil {
			return view, err
		}
	}
	if refs != header.Parts || rows != header.Rows {
		return view, errors.New("collections: source directory GC incomplete asset/row coverage")
	}
	view.Diagnostics.ManifestRecords = records
	view.Diagnostics.AssetRefs = len(view.AssetRefs)
	view.ManifestCatalogBytes = size
	return view, nil
}
