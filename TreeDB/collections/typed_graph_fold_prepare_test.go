package collections

import (
	"bytes"
	"context"
	"math"
	"reflect"
	"testing"
)

func TestTypedGraphCapturedAssetIdentity(t *testing.T) {
	col, _, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	state, closeState, err := col.loadColumnStoreCompactionState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeState()
	rows, _, err := col.materializeColumnStoreCompactionRows(context.Background(), state, "")
	if err != nil {
		t.Fatal(err)
	}
	producer, ok := any(col).(interface {
		prepareTypedGraphCapturedAssets(columnStoreCompactionState, []columnDeclaredRow, *typedGraphFoldAssetAdmission) (ColumnPublishPreparedAssets, error)
	})
	if !ok {
		t.Fatal("captured-frontier asset preparation unavailable; ordinary producer advances generation")
	}
	prepared, err := producer.prepareTypedGraphCapturedAssets(state, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.stableResources != nil {
		defer prepared.stableResources.Release()
	}
	var maxRowPart uint64
	for _, ref := range columnStoreCompactionRefsFromManifestRecords(state.records) {
		if ref.Generation == state.manifest.Generation && ref.Kind == ColumnAssetKindTCS1PartImage && ref.PartID > maxRowPart {
			maxRowPart = ref.PartID
		}
	}
	for _, asset := range prepared.Assets {
		if asset.Ref.Generation != state.manifest.Generation {
			t.Fatalf("asset generation=%d captured=%d", asset.Ref.Generation, state.manifest.Generation)
		}
		if asset.Ref.Kind == ColumnAssetKindTCS1PartImage && asset.Ref.PartID <= maxRowPart {
			t.Fatalf("row part=%d captured max=%d", asset.Ref.PartID, maxRowPart)
		}
	}
	if len(prepared.Assets) == 0 {
		t.Fatal("missing captured assets")
	}
	if prepared.stableResources == nil || !prepared.stableResourcesRequired {
		t.Fatal("captured assets lost ordinary stable-resource ownership")
	}
}

// This fixture assembles unpublished manifest records and locator values. It
// exercises the actual mixed-generation scan and point readers, not maintenance
// publication, graph reconstruction, or a public fold implementation.
func TestTypedGraphCapturedMixedGenerationReaders(t *testing.T) {
	col, _, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	state, closeState, err := col.loadColumnStoreCompactionState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeState()
	rows, _, err := col.materializeColumnStoreCompactionRows(context.Background(), state, "")
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := col.prepareTypedGraphCapturedAssets(state, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.stableResources != nil {
		defer prepared.stableResources.Release()
	}
	baseRecords := make([]columnManifestRecord, 0, len(prepared.Assets))
	var rowPart uint64
	for _, asset := range prepared.Assets {
		value, err := encodeColumnManifestPartRecord(asset)
		if err != nil {
			t.Fatal(err)
		}
		if asset.Ref.Kind != ColumnAssetKindTCS1PartImage && asset.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
			t.Fatalf("fixture unexpected sidecar %s", asset.Ref.Kind)
		}
		baseRecords = append(baseRecords, columnManifestRecord{key: columnManifestPartRecordKey(asset.Ref.Generation, asset.Ref.PartID), value: value})
		if asset.Ref.Kind == ColumnAssetKindTCS1PartImage {
			rowPart = asset.Ref.PartID
		}
	}
	captured := make(map[string]DocumentRowRef, len(rows))
	for i, row := range rows {
		captured[string(row.ID)] = DocumentRowRef{DocumentID: row.ID, Generation: state.manifest.Generation, PartID: rowPart, RowIndex: i, AppliedCommandLSN: state.manifest.AppliedCommandLSN}
	}
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"after-T"}}, {Name: "user", Strings: []string{"new-user"}}, {Name: "path", Strings: []string{"new-path"}}}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	if _, err := col.DeleteBatch(ids[1:3]); err != nil {
		t.Fatal(err)
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids[2:3], retained[2:3], changed); err != nil {
		t.Fatal(err)
	}
	if deleted, err := col.ReplaceTypedSourceByID(ids[3:5], ids[3:4], retained[3:4], changed); err != nil || deleted != 2 {
		t.Fatalf("unequal source replacement deleted=%d err=%v", deleted, err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	latest := loadColumnStoreCompactionManifestView1953(t, col.db, col)
	if latest.manifest.Generation <= state.manifest.Generation {
		t.Fatal("fixture did not advance frontier")
	}
	records := append([]columnManifestRecord(nil), baseRecords...)
	for _, record := range latest.records {
		if bytes.Equal(record.key, columnManifestHeaderRecordKeyBytes) {
			records = append(records, record)
			continue
		}
		if !bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			continue
		}
		generation, _, err := decodeColumnManifestPartRecordKey(record.key)
		if err != nil {
			t.Fatal(err)
		}
		if generation > state.manifest.Generation {
			records = append(records, record)
		}
	}
	sortColumnManifestRecords(records)
	manifest, refs, mutationParts, err := decodeColumnManifestSnapshotViewForScan(records, state.cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	full, err := decodeColumnManifestRecords(records)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range full.Parts {
		if part.AssetRef.Generation != latest.manifest.Generation {
			t.Fatal("header-generation Parts unexpectedly contains T")
		}
	}
	typedRefs, err := typedColumnPartRefsByGenerationFromManifestRecords(records, state.cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := typedRefs[state.manifest.Generation]; !ok {
		t.Fatal("all-record typed map lost compacted T")
	}
	view := columnPhysicalScanSnapshotView{CollectionName: state.meta.Name, Config: columnStoreRowAssetConfig(latest.cfg), FullConfig: latest.cfg, ColumnStoreEnabled: true, AssetRefs: refs, MutationParts: mutationParts, ColumnAssetRootDir: col.db.ColumnAssetRootDir(), AssetNamespace: state.cfg.AssetManager.Namespace}
	var visibility columnPhysicalVisibilityIndex
	if _, err := col.scanColumnPhysicalRowsInSnapshotView(view, columnPhysicalScanRequest{Visitor: func(row columnPhysicalScanRowView) error { visibility.upsert(row); return nil }}); err != nil {
		t.Fatal(err)
	}
	readView, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer readView.Close()
	lookup, err := readView.LookupDocumentRowRefsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cache, err := newColumnPhysicalAssetReadCacheWithIntegrity(col.db.ColumnAssetRootDir(), view.AssetNamespace, "")
	if err != nil {
		t.Fatal(err)
	}
	defer cache.close()
	typedCache := typedColumnPartReconstructionCache{ReadCache: &cache, Refs: typedRefs, RefsLoaded: true}
	pointView := CollectionReadView{rowAssetReadCache: &cache}
	projection, err := newColumnPhysicalScanProjection(view.Config, nil)
	if err != nil {
		t.Fatal(err)
	}
	var pointScratch columnPhysicalRowReaderScratch
	for i, result := range lookup.Results {
		var visible columnPhysicalVisibleRow
		for _, row := range visibility.rows {
			if bytes.Equal(row.ID, ids[i]) {
				visible = row
				break
			}
		}
		if !result.Found {
			if (i != 1 && i != 4) || !visible.Deleted {
				t.Fatalf("missing/deleted mismatch i=%d row=%+v", i, visible)
			}
			continue
		}
		ref := result.RowRef
		if ref.Generation <= state.manifest.Generation {
			ref = captured[string(ids[i])]
		}
		encoded := encodeColumnPrimaryRowLocator(ref)
		decoded, err := decodeColumnPrimaryRowLocator(ids[i], encoded)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateDocumentRowRefMatchesVisibleRow(decoded, visible); err != nil {
			t.Fatal(err)
		}
		point, err := pointView.fetchDocumentPointRow(view, decoded, projection, &pointScratch, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateDocumentRowRefMatchesVisibleRow(decoded, point); err != nil {
			t.Fatal(err)
		}
		values, err := col.typedColumnPartValuesForVisibleRowAtSnapshotWithCache(state.snap, 0, latest.cfg, visible, &typedCache)
		if err != nil {
			t.Fatal(err)
		}
		fullValues, err := mergeColumnReconstructionValuesInto(latest.cfg, point.Values, values.Values, nil)
		if err != nil {
			t.Fatal(err)
		}
		for j := range fullValues {
			if fullValues[j].StringBytes != nil {
				fullValues[j].String = string(fullValues[j].StringBytes)
				fullValues[j].StringBytes = nil
			}
		}
		want := rows[i].Values
		if i == 0 || i == 2 || i == 3 {
			if visible.Generation <= state.manifest.Generation {
				t.Fatal("post-T row overwritten by compacted base")
			}
			want = cloneColumnDeclaredValues(want)
			for j, column := range state.cfg.Columns {
				switch column.Name {
				case "embedding":
					want[j].Float32Vector = changed[0].Float32Vectors[0]
				case "content":
					want[j].String = "after-T"
				case "user":
					want[j].String = "new-user"
				case "path":
					want[j].String = "new-path"
				}
			}
		}
		if !reflect.DeepEqual(fullValues, want) {
			t.Fatalf("typed row %d got=%+v want=%+v", i, fullValues, want)
		}
	}
	t.Logf("captured generation=%d LSN=%d fresh row part=%d; latest generation=%d; row refs=%d typed generations=%d", state.manifest.Generation, state.manifest.AppliedCommandLSN, rowPart, manifest.Generation, len(refs), len(typedRefs))
}

func TestTypedGraphCapturedIdentityRejectsOverflow(t *testing.T) {
	col, _, _, _, _, _ := openTypedGraphQualityFixture(t, 1)
	state, done, err := col.loadColumnStoreCompactionState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	state.records = append(cloneColumnManifestRecords(state.records), columnManifestRecord{key: columnManifestPartRecordKey(state.manifest.Generation, math.MaxUint64)})
	if _, err := col.prepareTypedGraphCapturedAssets(state, nil, nil); err == nil {
		t.Fatal("accepted exhausted part ID before empty candidate")
	}
}

func TestTypedGraphCapturedEmptyAssets(t *testing.T) {
	col, _, _, _, _, _ := openTypedGraphQualityFixture(t, 0)
	state, done, err := col.loadColumnStoreCompactionState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	prepared, err := col.prepareTypedGraphCapturedAssets(state, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.Assets) != 0 || prepared.stableResources != nil || prepared.RowCount != 0 {
		t.Fatal("empty capture invented physical assets")
	}
	bad := state
	bad.manifest.AppliedCommandLSN++
	if _, err := col.prepareTypedGraphCapturedAssets(bad, nil, nil); err == nil {
		t.Fatal("mismatched LSN accepted for empty capture")
	}
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{vectorBenchmarkEmbedding(0, 8)}}, {Name: "content", Strings: []string{"first"}}, {Name: "user", Strings: []string{"user"}}, {Name: "path", Strings: []string{"path"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("first")}, [][]byte{[]byte(`{"id":"first"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	latest := loadColumnStoreCompactionManifestView1953(t, col.db, col)
	if latest.manifest.Generation <= state.manifest.Generation {
		t.Fatal("empty base suffix did not advance generation")
	}
	refs, err := typedColumnPartRefsByGenerationFromManifestRecords(latest.records, state.cfg.AssetManager.Namespace)
	if err != nil || len(refs) != 1 {
		t.Fatalf("empty base suffix typed refs=%v err=%v", refs, err)
	}
	visible, err := col.scanColumnPhysicalVisibleRows(nil)
	if err != nil || len(visible.Rows) != 1 || string(visible.Rows[0].ID) != "first" || visible.Rows[0].Generation <= state.manifest.Generation {
		t.Fatalf("empty base suffix visible=%+v err=%v", visible.Rows, err)
	}
}

func TestTypedGraphCapturedRowPartAdmission(t *testing.T) {
	asset := ColumnPreparedAsset{Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: "fixture", Generation: 1, PartID: 3, FileID: 1, Length: 64, Checksum: 1}, Rows: 1, Bytes: 64, PublishID: 1, GenerationID: 1, Reason: string(ColumnPublishOperationInsert), PartRole: ColumnManifestPartRoleBase}
	record := encodeColumnManifestPartRecordForTest1787(t, asset)
	for _, bad := range []string{"duplicate-live", "key", "reason", "role"} {
		t.Run(bad, func(t *testing.T) {
			records := cloneColumnManifestRecords([]columnManifestRecord{record})
			switch bad {
			case "duplicate-live":
				other := asset
				other.Ref.PartID = 4
				records = append(records, encodeColumnManifestPartRecordForTest1787(t, other))
			case "key":
				records[0].key = columnManifestPartRecordKey(2, 3)
			case "reason":
				records[0].value = bytes.Replace(records[0].value, []byte("insert"), []byte("broken"), 1)
			case "role":
				records[0].value = bytes.Replace(records[0].value, []byte("base"), []byte("oops"), 1)
			}
			if _, _, err := typedColumnPartSetsByGenerationFromManifestRecords(records, "fixture"); err == nil {
				t.Fatal("malformed/ambiguous live row part admitted")
			}
		})
	}
}
