package collections

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestTypedGraphEncodedGeneratedMetadataBeforeAppend(t *testing.T) {
	meta := typedMinimaCollectionMeta()
	meta.TextIndexes = nil
	dir, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	// This fixture's previous row/table-only reservation fits 4KiB. Including
	// generated metadata does not, despite the tiny retained document.
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 8, Tombstones: 8, ValueSlots: 32, OwnedBytes: 4096, EncodedOutputBytes: 4 << 10}, typedGraphColdLimits{ManifestRecords: 128, ManifestBytes: 128 << 10, AssetBytes: 1 << 20, DecodedTermBytes: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	before := col.typedGraphPublicationSnapshot()
	frames := len(collectionCommandWALFrames(t, dir))
	var appends atomic.Int64
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.BeforeDependencyAppend {
			appends.Add(1)
		}
		return nil
	})
	defer restore()
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"a"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}}}
	_, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, columns)
	if !errors.Is(err, errTypedGraphOverlayFoldNeeded) || appends.Load() != 0 || len(collectionCommandWALFrames(t, dir)) != frames || col.typedGraphPublicationSnapshot() != before {
		t.Fatalf("metadata admission err=%v appends=%d", err, appends.Load())
	}
}

func TestTypedGraphEncodedControlBound(t *testing.T) {
	meta, err := normalizeCollectionMeta(typedMinimaCollectionMeta())
	if err != nil {
		t.Fatal(err)
	}
	bound, err := typedGraphControlEncodedBound(meta)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise absent-to-present metadata and the maximum growing counter.
	identity := ColumnManifestIdentity{Generation: ^uint64(0), Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: ^uint64(0)}
	plan := ColumnPublishPlan{Enabled: true, Operation: ColumnPublishOperationInsert, UpdatedActiveManifest: identity, RecoveryAuthoritativeManifest: identity, RecoveryAuthoritativeAppliedCommandLSN: ^uint64(0)}
	meta.Options.ColumnStore.PhysicalMutationParts = ^uint64(0)
	updated, err := columnPublishUpdatedMeta(meta, plan)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeNormalizedCollectionMeta(updated)
	if err != nil {
		t.Fatal(err)
	}
	table := newCollectionRunTable(16)
	table.Set([]byte(systemCollectionMetaKey(meta.Name)), raw)
	for _, name := range collectionRootNames(updated) {
		table.Set([]byte(systemCollectionRootKey(name)), encodeRootID(^uint64(0)))
	}
	table.Set([]byte(systemCollectionDocumentGenerationKey(meta.Name)), encodeRootID(^uint64(0)))
	table.Freeze()
	actual, err := typedGraphTableEncodedBound(table)
	if err != nil || bound < actual {
		t.Fatalf("control bound=%d actual=%d err=%v", bound, actual, err)
	}
}

func TestTypedGraphEncodedManifestBounds(t *testing.T) {
	meta, err := normalizeCollectionMeta(typedMinimaCollectionMeta())
	if err != nil {
		t.Fatal(err)
	}
	bounds, removal, err := typedGraphManifestEncodedBounds(meta)
	if err != nil {
		t.Fatal(err)
	}
	for i, operation := range [...]ColumnPublishOperation{ColumnPublishOperationInsert, ColumnPublishOperationUpdate, ColumnPublishOperationDelete} {
		for _, source := range []bool{false, true} {
			input := ColumnPublishManifestEncodeInput{Collection: meta.Name, ColumnStore: *meta.Options.ColumnStore, Operation: operation, AppliedCommandLSN: 7}
			row := testColumnPublishPreparedAssetM10A()
			row.Ref.Namespace = meta.Options.ColumnStore.AssetManager.Namespace
			row.Reason, row.PartRole = string(operation), columnManifestPartRoleForPublish(operation)
			input.Prepared.Assets = []ColumnPreparedAsset{row}
			if operation != ColumnPublishOperationDelete {
				typed := row
				typed.Ref.Kind, typed.Ref.PartID = ColumnAssetKindTCS1TypedColumnPart, 2
				keys, err := typedColumnPartPublicationSortKey(*meta.Options.ColumnStore, columnStoreTypedColumnPartFields(*meta.Options.ColumnStore))
				if err != nil {
					t.Fatal(err)
				}
				typed.SortKey = columnSortKeyMatchString(keys)
				input.Prepared.Assets = append(input.Prepared.Assets, typed)
			}
			want := bounds[i]
			if source {
				// Atomic source replacement uses insert-role assets even when
				// the combined command/header is an update. The update bound
				// conservatively includes the longer delta-role string.
				if operation != ColumnPublishOperationDelete {
					for j := range input.Prepared.Assets {
						input.Prepared.Assets[j].Reason = string(ColumnPublishOperationInsert)
						input.Prepared.Assets[j].PartRole = ColumnManifestPartRoleBase
					}
				}
				deleted := row
				deleted.Ref.PartID = 3
				deleted.Reason, deleted.PartRole = string(ColumnPublishOperationDelete), ColumnManifestPartRoleTombstone
				input.Prepared.Assets = append(input.Prepared.Assets, deleted)
				want += removal
			}
			header, err := encodeColumnManifestHeaderRecord(input, 7)
			if err != nil {
				t.Fatal(err)
			}
			table := newCollectionRunTable(5)
			table.Set(columnManifestHeaderRecordKeyBytes, header)
			identity := encodeColumnManifestIdentityRecordArray(ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 1})
			table.Set([]byte(columnManifestIdentityRecordKey), identity[:])
			for _, asset := range input.Prepared.Assets {
				raw, err := encodeColumnManifestPartRecord(asset)
				if err != nil {
					t.Fatal(err)
				}
				table.Set(columnManifestPartRecordKey(asset.Ref.Generation, asset.Ref.PartID), raw)
			}
			table.Freeze()
			actual, err := typedGraphTableEncodedBound(table)
			if err != nil || actual > want || (!source && actual != want) {
				t.Fatalf("operation=%s source=%t bound=%d actual=%d err=%v", operation, source, want, actual, err)
			}
		}
	}
}
