package collections

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestTypedColumnPartSortKeyTimeUSPersistsAscendingReopen1948(t *testing.T) {
	events := []columnPhysicalJSONBenchParityEventP0{
		{ID: "e0", TimeUS: 30, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:3"},
		{ID: "e1", TimeUS: 10, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:1"},
		{ID: "e2", TimeUS: 20, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:2"},
		{ID: "e3", TimeUS: 10, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.repost", Did: "did:4"},
	}
	d, col, closeFn := openTypedColumnSortKeyFixture1948(t, []ColumnSortKey{{Column: "time_us"}}, events)
	defer closeFn()

	rows := typedColumnPartRowsForGeneration1778(t, d, col, 1)
	assertTypedColumnSortKeyPrimaryIDs1948(t, rows, []int64{1, 3, 2, 0})
	assertTypedColumnSortKeyManifestAndImage1948(t, d, col, []ColumnSortKey{{Column: "time_us"}}, []int64{1, 3, 2, 0})
	for _, event := range events {
		doc, err := col.Get([]byte(event.ID))
		if err != nil {
			t.Fatalf("Get(%s) after sorted reopen: %v", event.ID, err)
		}
		for _, needle := range [][]byte{
			[]byte(fmt.Sprintf(`"time_us":%d`, event.TimeUS)),
			[]byte(fmt.Sprintf(`"did":%q`, event.Did)),
		} {
			if !bytes.Contains(doc, needle) {
				t.Fatalf("Get(%s)=%s missing %s", event.ID, doc, needle)
			}
		}
	}

	q4a, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupMinInt64,
		GroupColumn: "did",
		ValueColumn: "time_us",
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
			{Column: "collection", Value: "app.bsky.feed.post"},
		},
	})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q4a sorted time_us): %v", err)
	}
	if q4a.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || q4a.Diagnostics.RowsScanned != len(events) {
		t.Fatalf("q4a diagnostics=%+v want typed-column full scan over sorted rows", q4a.Diagnostics)
	}
	got := map[string]int64{}
	for _, group := range q4a.Groups {
		got[group.Key] = group.Int64
	}
	if got["did:1"] != 10 || got["did:3"] != 30 || len(got) != 2 {
		t.Fatalf("q4a groups=%+v want did:1=10 did:3=30", q4a.Groups)
	}
}

func TestTypedColumnPartSortKeyBoolPersistsAscendingReopen1948(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open setup DB: %v", err)
	}
	mgr := NewCollectionManager(d)
	cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{
		{Name: "flag", Path: "flag", ValueType: ColumnStoreValueBool, Owner: TypedStorageOwnerColumnPart},
	}, SortKey: []ColumnSortKey{{Column: "flag"}}}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection setup: %v", err)
	}
	ids := [][]byte{[]byte("e0"), []byte("e1"), []byte("e2"), []byte("e3")}
	docs := [][]byte{[]byte(`{"flag":true}`), []byte(`{"flag":false}`), []byte(`{"flag":true}`), []byte(`{"flag":false}`)}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close before reopen: %v", err)
	}
	reopen, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open reopened DB: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	reopened, err := NewCollectionManager(reopen).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	rows := typedColumnPartRowsForGeneration1778(t, reopen, reopened, 1)
	assertTypedColumnSortKeyPrimaryIDs1948(t, rows, []int64{1, 3, 0, 2})
	assertTypedColumnSortKeyManifestAndImage1948(t, reopen, reopened, []ColumnSortKey{{Column: "flag"}}, []int64{1, 3, 0, 2})
	gotFlags := make([]bool, len(rows))
	for i, row := range rows {
		gotFlags[i] = row.Values["flag"].Bool
	}
	if fmt.Sprint(gotFlags) != "[false false true true]" {
		t.Fatalf("bool sort order=%v want false/false/true/true", gotFlags)
	}
}

func TestTypedColumnPartSortKeyInt64ScanMapsPrimaryIDsToDocuments1948(t *testing.T) {
	events := []columnPhysicalJSONBenchParityEventP0{
		{ID: "e0", TimeUS: 30, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:3"},
		{ID: "e1", TimeUS: 10, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:1"},
		{ID: "e2", TimeUS: 20, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:2"},
		{ID: "e3", TimeUS: 10, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.repost", Did: "did:4"},
	}
	_, col, closeFn := openTypedColumnSortKeyFixture1948(t, []ColumnSortKey{{Column: "time_us"}}, events)
	defer closeFn()

	result, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan: %v", err)
	}
	wantIDs := []string{"e1", "e3", "e2", "e0"}
	wantRows := []int{1, 3, 2, 0}
	if len(result.Rows) != len(wantIDs) {
		t.Fatalf("scan rows=%+v want %d", result.Rows, len(wantIDs))
	}
	for i, row := range result.Rows {
		if string(row.DocumentID) != wantIDs[i] || row.RowIndex != wantRows[i] || int(row.PrimaryID) != wantRows[i] {
			t.Fatalf("row[%d]=%+v want document_id=%q physical_row=%d", i, row, wantIDs[i], wantRows[i])
		}
	}
}

func TestTypedColumnPartSortKeyClickHouseLogicalOrder1948(t *testing.T) {
	sortKey := []ColumnSortKey{{Column: "kind"}, {Column: "operation"}, {Column: "collection"}, {Column: "did"}, {Column: "time_us"}}
	events := []columnPhysicalJSONBenchParityEventP0{
		// First occurrence order is intentionally not lexical; insertion-order dictionary codes would put this row too early.
		{ID: "e0", TimeUS: 50, Kind: "zeta", Operation: "z", Collection: "z.collection", Did: "did:z"},
		{ID: "e1", TimeUS: 40, Kind: "alpha", Operation: "z", Collection: "b.collection", Did: "did:b"},
		{ID: "e2", TimeUS: 30, Kind: "alpha", Operation: "alpha", Collection: "z.collection", Did: "did:c"},
		{ID: "e3", TimeUS: 20, Kind: "alpha", Operation: "alpha", Collection: "a.collection", Did: "did:b"},
		{ID: "e4", TimeUS: 10, Kind: "alpha", Operation: "alpha", Collection: "a.collection", Did: "did:a"},
		{ID: "e5", TimeUS: 10, Kind: "alpha", Operation: "alpha", Collection: "a.collection", Did: "did:a"},
	}
	d, col, closeFn := openTypedColumnSortKeyFixture1948(t, sortKey, events)
	defer closeFn()

	rows := typedColumnPartRowsForGeneration1778(t, d, col, 1)
	assertTypedColumnSortKeyPrimaryIDs1948(t, rows, []int64{4, 5, 3, 2, 1, 0})
	if !sort.SliceIsSorted(rows, func(i, j int) bool { return typedColumnSortKeyLess1948(rows[i], rows[j]) }) {
		t.Fatalf("typed rows are not in logical ClickHouse-style order: %+v", rows)
	}
	assertTypedColumnSortKeyManifestAndImage1948(t, d, col, sortKey, []int64{4, 5, 3, 2, 1, 0})

	q2, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{
		Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
		GroupColumn:    "collection",
		DistinctColumn: "did",
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "alpha"},
			{Column: "operation", Value: "alpha"},
		},
	})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2 sorted contract): %v", err)
	}
	counts := map[string]ColumnPhysicalQueryGroup{}
	for _, group := range q2.Groups {
		counts[group.Key] = group
	}
	if counts["a.collection"].Count != 3 || counts["a.collection"].DistinctCount != 2 || counts["z.collection"].Count != 1 || counts["z.collection"].DistinctCount != 1 {
		t.Fatalf("q2 groups=%+v want real predicate count/distinct over sorted typed-column part", q2.Groups)
	}
	if q2.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || q2.Diagnostics.PredicateCount != 2 || q2.Diagnostics.RowsScanned != len(events) {
		t.Fatalf("q2 diagnostics=%+v want typed source, real predicates, full scan", q2.Diagnostics)
	}
}

func TestTypedColumnPartSortKeyRejectsNullable1948(t *testing.T) {
	cfg := typedColumnSortKeyConfig1948([]ColumnSortKey{{Column: "kind"}})
	setTypedColumnSortKeyColumnNullable1948(t, cfg, "kind", true)
	_, err := normalizeColumnStoreConfig("events", cfg)
	if err == nil || !strings.Contains(err.Error(), "null/default ordering is not defined") {
		t.Fatalf("normalize nullable sort key err=%v want null/default ordering rejection", err)
	}
}

func TestTypedColumnPartSortKeyAllowsRowAssetDescendingFallback1948(t *testing.T) {
	cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{
		{Name: "row_sort", Path: "row_sort", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "typed_value", Path: "typed_value", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
	}, SortKey: []ColumnSortKey{{Column: "row_sort", Direction: ColumnSortDescending}}}
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalize row-asset descending SortKey: %v", err)
	}
	sortKey, err := typedColumnPartPublicationSortKey(*normalized, columnStoreTypedColumnPartFields(*normalized))
	if err != nil {
		t.Fatalf("typedColumnPartPublicationSortKey: %v", err)
	}
	if len(sortKey) != 0 {
		t.Fatalf("typed-column sort key=%+v want row-asset descending fallback", sortKey)
	}
}

func TestTypedColumnPartSortKeyAllowsMixedOwnerTypedDescendingFallback1948(t *testing.T) {
	cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{
		{Name: "row_sort", Path: "row_sort", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "typed_value", Path: "typed_value", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
	}, SortKey: []ColumnSortKey{{Column: "row_sort"}, {Column: "typed_value", Direction: ColumnSortDescending}}}
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalize mixed-owner typed descending SortKey: %v", err)
	}
	sortKey, err := typedColumnPartPublicationSortKey(*normalized, columnStoreTypedColumnPartFields(*normalized))
	if err != nil {
		t.Fatalf("typedColumnPartPublicationSortKey: %v", err)
	}
	if len(sortKey) != 0 {
		t.Fatalf("typed-column sort key=%+v want mixed-owner primary-id fallback", sortKey)
	}
}

func TestTypedColumnPartSortKeyRejectsDescending1948(t *testing.T) {
	cfg := typedColumnSortKeyConfig1948([]ColumnSortKey{{Column: "time_us", Direction: ColumnSortDescending}})
	_, err := normalizeColumnStoreConfig("events", cfg)
	if err == nil || !strings.Contains(err.Error(), "descending typed_column_part sort key") {
		t.Fatalf("normalize descending sort key err=%v want descending typed_column_part rejection", err)
	}
}

func TestTypedColumnPartSortKeyMixedOwnerWithUnsupportedTypedColumnFallsBack1948(t *testing.T) {
	// Mixed-owner key (row_asset + typed_col DESC): config validation must not
	// reject it even though the typed-column direction is descending, because the
	// key is not fully typed-column-owned and falls back to primary-id ordering.
	cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{
		{Name: "row_sort", Path: "row_sort", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "typed_value", Path: "typed_value", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
	}, SortKey: []ColumnSortKey{
		{Column: "row_sort", Direction: ColumnSortDescending},
		{Column: "typed_value", Direction: ColumnSortDescending},
	}}
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalize mixed-owner SortKey with descending typed column: %v", err)
	}
	sortKey, err := typedColumnPartPublicationSortKey(*normalized, columnStoreTypedColumnPartFields(*normalized))
	if err != nil {
		t.Fatalf("typedColumnPartPublicationSortKey: %v", err)
	}
	if len(sortKey) != 0 {
		t.Fatalf("typed-column sort key=%+v want mixed-owner primary-id fallback", sortKey)
	}
}

func TestTypedColumnPartSortKeyMixedOwnerFallsBackToPrimaryID1948(t *testing.T) {
	cfg := typedColumnSortKeyConfig1948([]ColumnSortKey{{Column: "time_us"}})
	setTypedColumnSortKeyColumnOwner1948(t, cfg, "time_us", TypedStorageOwnerRowAsset)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows := typedColumnDeclaredRows1948([]columnPhysicalJSONBenchParityEventP0{
		{ID: "e0", TimeUS: 30, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:3"},
		{ID: "e1", TimeUS: 10, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:1"},
	})
	raw, _, err := buildTypedColumnPartImageForDeclaredRows(*normalized, 1, typedColumnPartAssetPartID, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnPartImageForDeclaredRows: %v", err)
	}
	part, err := typedColumnAdapterPartFromBytesForReconstruction(typedColumnAdapterOptions{Fields: columnStoreTypedColumnPartFields(*normalized), SchemaVersion: uint32(normalized.SchemaHash)}, raw)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromBytesForReconstruction: %v", err)
	}
	gotRows, err := part.scanRows()
	if err != nil {
		t.Fatalf("scanRows: %v", err)
	}
	assertTypedColumnSortKeyPrimaryIDs1948(t, gotRows, []int64{0, 1})
	if got := columnSortKeysFromTypedColumnSortKeys(part.Part.Descriptor.SortKey); len(got) != 0 {
		t.Fatalf("mixed-owner typed-column sort metadata=%+v want empty primary-id fallback", got)
	}
}

func setTypedColumnSortKeyColumnNullable1948(tb testing.TB, cfg *ColumnStoreConfig, name string, nullable bool) {
	tb.Helper()
	for i := range cfg.Columns {
		if cfg.Columns[i].Name == name {
			cfg.Columns[i].Nullable = nullable
			return
		}
	}
	tb.Fatalf("missing column %q in test fixture", name)
}

func setTypedColumnSortKeyColumnOwner1948(tb testing.TB, cfg *ColumnStoreConfig, name string, owner TypedStorageFieldOwner) {
	tb.Helper()
	for i := range cfg.Columns {
		if cfg.Columns[i].Name == name {
			cfg.Columns[i].Owner = owner
			return
		}
	}
	tb.Fatalf("missing column %q in test fixture", name)
}

func TestColumnPreparedAssetSortKeyRejectsDuplicate1948(t *testing.T) {
	asset := ColumnPreparedAsset{
		Ref:      ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: "events_column_assets", Generation: 1, PartID: typedColumnPartAssetPartID, FileID: 1, Length: 1, Checksum: 1},
		Rows:     1,
		Bytes:    1,
		Reason:   string(ColumnPublishOperationInsert),
		PartRole: ColumnManifestPartRoleBase,
		SortKey:  columnSortKeyMatchString([]ColumnSortKey{{Column: "time_us"}, {Column: "time_us"}}),
	}
	if err := validateColumnPreparedAssetForPlan(asset); err == nil || !strings.Contains(err.Error(), "duplicate sort key column") {
		t.Fatalf("validateColumnPreparedAssetForPlan duplicate err=%v want duplicate sort key column", err)
	}
}

func TestColumnManifestPartFieldsValidateSortKeyTrailer1948(t *testing.T) {
	namespace := "events_column_assets"
	raw := encodeColumnManifestPartRecordWithRawSortKey1948(t, ColumnAssetKindTCS1PartImage, namespace, []ColumnSortKey{{Column: "time_us"}})
	records := []columnManifestRecord{{key: columnManifestPartRecordKey(1, 2), value: raw}}
	_, _, err := columnManifestAssetRefsFromRecordsForScan(records, 1, namespace)
	if err == nil || !strings.Contains(err.Error(), "sort key requires") {
		t.Fatalf("columnManifestAssetRefsFromRecordsForScan err=%v want shared sort-key trailer validation", err)
	}
}

func TestColumnAssetRewriteManifestPartValidatesSortKeyTrailer1948(t *testing.T) {
	namespace := "events_column_assets"
	raw := encodeColumnManifestPartRecordWithRawSortKey1948(t, ColumnAssetKindTCS1PartImage, namespace, []ColumnSortKey{{Column: "time_us"}})
	_, _, err := columnAssetRewriteManifestPartRefForPatch(raw, namespace)
	if err == nil || !strings.Contains(err.Error(), "sort key is only valid") {
		t.Fatalf("columnAssetRewriteManifestPartRefForPatch err=%v want sort-key trailer validation", err)
	}
}

func TestColumnManifestPartFieldsRejectsTypedSortKeyEngineCap1948(t *testing.T) {
	namespace := "events_column_assets"
	sortKeys := make([]ColumnSortKey, typedColumnPartSortKeyMaxColumns+1)
	for i := range sortKeys {
		sortKeys[i] = ColumnSortKey{Column: fmt.Sprintf("c%02d", i)}
	}
	raw := encodeColumnManifestPartRecordWithRawSortKey1948(t, ColumnAssetKindTCS1TypedColumnPart, namespace, sortKeys)
	if _, err := decodeColumnManifestPartSortKeyForScan(raw); err == nil || !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("decodeColumnManifestPartSortKeyForScan err=%v want typed-column engine cap rejection", err)
	}
}

func TestTypedColumnImageSortKeyMutationGuard1948(t *testing.T) {
	primary := typedcolumn.ColumnPartDescriptor{SortKey: []typedcolumn.SortKeyColumn{{Column: typedColumnAdapterPrimaryIDColumn, Direction: typedcolumn.SortKeyAsc}}}
	if typedColumnPartDescriptorHasLogicalSortKey(primary) {
		t.Fatalf("primary-id descriptor reported logical sort key")
	}
	logical := typedcolumn.ColumnPartDescriptor{SortKey: []typedcolumn.SortKeyColumn{{Column: "time_us", Direction: typedcolumn.SortKeyAsc}}}
	if !typedColumnPartDescriptorHasLogicalSortKey(logical) {
		t.Fatalf("logical descriptor did not report sort key")
	}
}

func encodeColumnManifestPartRecordWithRawSortKey1948(t testing.TB, kind ColumnAssetKind, namespace string, sortKey []ColumnSortKey) []byte {
	t.Helper()
	var b bytes.Buffer
	writeManifestUint32(&b, columnManifestPartMagic)
	writeManifestUint16(&b, columnManifestRecordVersion)
	writeManifestString(&b, string(kind))
	writeManifestString(&b, namespace)
	writeManifestUint64(&b, 1)
	writeManifestUint64(&b, 2)
	writeManifestUint64(&b, 1)
	writeManifestUint64(&b, 0)
	writeManifestUint64(&b, 1)
	writeManifestUint64(&b, 1)
	writeManifestUint64(&b, 1)
	writeManifestUint64(&b, 1)
	writeManifestUint64(&b, 1)
	writeManifestUint64(&b, 1)
	writeManifestString(&b, string(ColumnPublishOperationInsert))
	writeManifestString(&b, string(ColumnManifestPartRoleBase))
	writeColumnManifestSortKey(&b, sortKey)
	return b.Bytes()
}

func TestTypedColumnPartPublicationSortKeyRejectsUnknownColumn1948(t *testing.T) {
	cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
	}, SortKey: []ColumnSortKey{{Column: "missing"}}}
	_, err := typedColumnPartPublicationSortKey(*cfg, columnStoreTypedColumnPartFields(*cfg))
	if err == nil || !strings.Contains(err.Error(), "unknown column") {
		t.Fatalf("typedColumnPartPublicationSortKey unknown err=%v want unknown column", err)
	}
}

func TestTypedColumnPartSortKeyMixedOwnerNamePathCollisionFallsBack1948(t *testing.T) {
	cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{
		{Name: "row_sort", Path: "row_path", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "typed_value", Path: "row_sort", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
	}, SortKey: []ColumnSortKey{{Column: "row_sort"}}}
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	sortKey, err := typedColumnPartPublicationSortKey(*normalized, columnStoreTypedColumnPartFields(*normalized))
	if err != nil {
		t.Fatalf("typedColumnPartPublicationSortKey: %v", err)
	}
	if len(sortKey) != 0 {
		t.Fatalf("typed-column sort key=%+v want mixed-owner primary-id fallback", sortKey)
	}
}

func TestTypedColumnPartSortKeyRejectsEngineCap1948(t *testing.T) {
	columns := make([]ColumnStoreColumn, typedColumnPartSortKeyMaxColumns+1)
	sortKeys := make([]ColumnSortKey, len(columns))
	for i := range columns {
		name := fmt.Sprintf("c%02d", i)
		columns[i] = ColumnStoreColumn{Name: name, Path: name, ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart}
		sortKeys[i] = ColumnSortKey{Column: name}
	}
	cfg := &ColumnStoreConfig{Enabled: true, Columns: columns, SortKey: sortKeys}
	_, err := normalizeColumnStoreConfig("events", cfg)
	if err == nil || !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("normalize oversized typed-column sort key err=%v want exceeds cap", err)
	}
}

func TestColumnPreparedAssetSortKeyRejectsOversized1948(t *testing.T) {
	sortKeys := make([]ColumnSortKey, int(columnManifestSortKeyMaxColumns)+1)
	for i := range sortKeys {
		sortKeys[i] = ColumnSortKey{Column: fmt.Sprintf("c%02d", i)}
	}
	asset := ColumnPreparedAsset{
		Ref:      ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: "events_column_assets", Generation: 1, PartID: typedColumnPartAssetPartID, FileID: 1, Length: 1, Checksum: 1},
		Rows:     1,
		Bytes:    1,
		Reason:   string(ColumnPublishOperationInsert),
		PartRole: ColumnManifestPartRoleBase,
		SortKey:  columnSortKeyMatchString(sortKeys),
	}
	if err := validateColumnPreparedAssetForPlan(asset); err == nil || !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("validateColumnPreparedAssetForPlan oversized err=%v want exceeds cap", err)
	}
}

func TestColumnPreparedAssetSortKeyRejectsTypedEngineCap1948(t *testing.T) {
	sortKeys := make([]ColumnSortKey, typedColumnPartSortKeyMaxColumns+1)
	for i := range sortKeys {
		sortKeys[i] = ColumnSortKey{Column: fmt.Sprintf("c%02d", i)}
	}
	asset := ColumnPreparedAsset{
		Ref:      ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: "events_column_assets", Generation: 1, PartID: typedColumnPartAssetPartID, FileID: 1, Length: 1, Checksum: 1},
		Rows:     1,
		Bytes:    1,
		Reason:   string(ColumnPublishOperationInsert),
		PartRole: ColumnManifestPartRoleBase,
		SortKey:  columnSortKeyMatchString(sortKeys),
	}
	if err := validateColumnPreparedAssetForPlan(asset); err == nil || !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("validateColumnPreparedAssetForPlan typed engine cap err=%v want exceeds cap", err)
	}
}

func TestTypedColumnPartSortKeyQueryValidationFailsClosed1948(t *testing.T) {
	cfg := typedColumnSortKeyConfig1948(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows := typedColumnDeclaredRows1948([]columnPhysicalJSONBenchParityEventP0{{ID: "e0", TimeUS: 1, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:1"}})
	raw, rowCount, err := buildTypedColumnPartImageForDeclaredRows(*normalized, 1, typedColumnPartAssetPartID, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnPartImageForDeclaredRows: %v", err)
	}
	ref := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: normalized.AssetManager.Namespace, Generation: 1, PartID: typedColumnPartAssetPartID, FileID: 1, Length: int64(len(raw)), Checksum: 1}
	plan := columnTypedColumnPhysicalQueryPlan{
		Fields:           columnStoreTypedColumnPartFields(*normalized),
		Selected:         []bool{true, true, true, true, true},
		ProjectedColumns: []string{"time_us", "kind", "operation", "collection", "did"},
		SortKey:          []ColumnSortKey{{Column: "time_us"}},
	}
	_, err = decodeTypedColumnPhysicalQueryPart(plan, normalized.SchemaHash,
		columnManifestAssetRefForScan{Ref: ref, Rows: rowCount, SortKey: nil},
		columnManifestAssetRefForScan{Rows: rowCount}, raw, false)
	if err == nil || !strings.Contains(err.Error(), "sort metadata mismatch") {
		t.Fatalf("decodeTypedColumnPhysicalQueryPart err=%v want sort metadata mismatch", err)
	}
}

func TestTypedColumnPartSortKeyQueryRejectsUnexpectedMetadata1948(t *testing.T) {
	unexpected := []ColumnSortKey{{Column: "time_us"}}
	if err := validateTypedColumnPhysicalQuerySortMetadata(nil, unexpected, nil); err == nil || !strings.Contains(err.Error(), "sort metadata mismatch") {
		t.Fatalf("validate unexpected manifest metadata err=%v want sort metadata mismatch", err)
	}
	if err := validateTypedColumnPhysicalQuerySortMetadata(nil, nil, unexpected); err == nil || !strings.Contains(err.Error(), "sort metadata mismatch") {
		t.Fatalf("validate unexpected image metadata err=%v want sort metadata mismatch", err)
	}
}

func BenchmarkTypedColumnPartSortKeyPublication1948(b *testing.B) {
	events := make([]columnPhysicalJSONBenchParityEventP0, 4096)
	for i := range events {
		events[i] = columnPhysicalJSONBenchParityEventP0{
			ID:         fmt.Sprintf("e%06d", i),
			TimeUS:     int64(len(events) - i),
			Kind:       []string{"zeta", "alpha", "commit", "reply"}[i%4],
			Operation:  []string{"z", "alpha", "create"}[i%3],
			Collection: []string{"z.collection", "a.collection", "app.bsky.feed.post"}[i%3],
			Did:        fmt.Sprintf("did:%06d", i%1024),
		}
	}
	rows := typedColumnDeclaredRows1948(events)
	cases := []struct {
		name    string
		sortKey []ColumnSortKey
	}{
		{name: "primary_id", sortKey: nil},
		{name: "time_us", sortKey: []ColumnSortKey{{Column: "time_us"}}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			cfg, err := normalizeColumnStoreConfig("events", typedColumnSortKeyConfig1948(tc.sortKey))
			if err != nil {
				b.Fatalf("normalizeColumnStoreConfig: %v", err)
			}
			var imageBytes int
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				raw, rowCount, err := buildTypedColumnPartImageForDeclaredRows(*cfg, uint64(i+1), typedColumnPartAssetPartID, rows)
				if err != nil {
					b.Fatalf("buildTypedColumnPartImageForDeclaredRows: %v", err)
				}
				if rowCount != len(rows) {
					b.Fatalf("rowCount=%d want %d", rowCount, len(rows))
				}
				imageBytes = len(raw)
			}
			b.ReportMetric(float64(len(rows)), "rows/op")
			b.ReportMetric(float64(imageBytes), "typed_part_bytes/op")
		})
	}
}

func openTypedColumnSortKeyFixture1948(tb testing.TB, sortKey []ColumnSortKey, events []columnPhysicalJSONBenchParityEventP0) (*backenddb.DB, *Collection, func()) {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatalf("Open setup DB: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: typedColumnSortKeyConfig1948(sortKey)}}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection setup: %v", err)
	}
	if len(events) != 0 {
		ids := make([][]byte, len(events))
		docs := make([][]byte, len(events))
		for i, event := range events {
			ids[i] = []byte(event.ID)
			docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":%q,"operation":%q,"collection":%q,"did":%q}`, event.TimeUS, event.Kind, event.Operation, event.Collection, event.Did))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			_ = d.Close()
			tb.Fatalf("InsertBatch: %v", err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		tb.Fatalf("Checkpoint before reopen: %v", err)
	}
	if err := d.Close(); err != nil {
		tb.Fatalf("Close before reopen: %v", err)
	}
	reopen, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatalf("Open reopened DB: %v", err)
	}
	reopened, err := NewCollectionManager(reopen).OpenCollection("events")
	if err != nil {
		_ = reopen.Close()
		tb.Fatalf("OpenCollection reopened: %v", err)
	}
	return reopen, reopened, func() { _ = reopen.Close() }
}

func typedColumnSortKeyConfig1948(sortKey []ColumnSortKey) *ColumnStoreConfig {
	return &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "operation", Path: "operation", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "collection", Path: "collection", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
	}, SortKey: cloneColumnSortKeys(sortKey)}
}

func typedColumnDeclaredRows1948(events []columnPhysicalJSONBenchParityEventP0) []columnDeclaredRow {
	rows := make([]columnDeclaredRow, len(events))
	for i, event := range events {
		rows[i] = columnDeclaredRow{ID: []byte(event.ID), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: event.TimeUS},
			{Type: ColumnStoreValueString, Present: true, String: event.Kind},
			{Type: ColumnStoreValueString, Present: true, String: event.Operation},
			{Type: ColumnStoreValueString, Present: true, String: event.Collection},
			{Type: ColumnStoreValueString, Present: true, String: event.Did},
		}}
	}
	return rows
}

func assertTypedColumnSortKeyPrimaryIDs1948(t testing.TB, rows []typedColumnAdapterRow, want []int64) {
	t.Helper()
	if len(rows) != len(want) {
		t.Fatalf("typed rows=%d want %d", len(rows), len(want))
	}
	for i, row := range rows {
		if row.PrimaryID != want[i] {
			t.Fatalf("typed row[%d] primary_id=%d want %d rows=%+v", i, row.PrimaryID, want[i], rows)
		}
	}
}

func typedColumnSortKeyLess1948(left, right typedColumnAdapterRow) bool {
	lv, rv := left.Values, right.Values
	keys := []string{"kind", "operation", "collection", "did"}
	for _, key := range keys {
		if lv[key].String != rv[key].String {
			return lv[key].String < rv[key].String
		}
	}
	if lv["time_us"].Int64 != rv["time_us"].Int64 {
		return lv["time_us"].Int64 < rv["time_us"].Int64
	}
	return left.PrimaryID < right.PrimaryID
}

func assertTypedColumnSortKeyManifestAndImage1948(t testing.TB, d *backenddb.DB, col *Collection, wantSortKey []ColumnSortKey, wantPrimaryIDs []int64) {
	t.Helper()
	id, ok := col.ColumnStoreCacheIdentity()
	if !ok || id.ManifestRoot == 0 {
		t.Fatalf("ColumnStoreCacheIdentity=%+v ok=%v want manifest root", id, ok)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	records, err := loadColumnManifestRecordsFromRoot(snap, id.ManifestRoot)
	if err != nil {
		t.Fatalf("loadColumnManifestRecordsFromRoot: %v", err)
	}
	manifest, err := decodeColumnManifestRecords(records)
	if err != nil {
		t.Fatalf("decodeColumnManifestRecords: %v", err)
	}
	var typedPart *columnManifestPartSnapshot
	for i := range manifest.Parts {
		if manifest.Parts[i].AssetRef.Kind == ColumnAssetKindTCS1TypedColumnPart {
			typedPart = &manifest.Parts[i]
			break
		}
	}
	if typedPart == nil {
		t.Fatalf("manifest parts=%+v missing typed_column_part", manifest.Parts)
	}
	if !columnSortKeysEqual(typedPart.SortKey, wantSortKey) {
		t.Fatalf("manifest sort key=%+v want %+v", typedPart.SortKey, wantSortKey)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), typedPart.AssetRef)
	if err != nil {
		t.Fatalf("read typed-column part: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	part, err := typedcolumn.ColumnPartFromImage(image)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	gotImageSortKey := columnSortKeysFromTypedColumnSortKeys(part.Descriptor.SortKey)
	if !columnSortKeysEqual(gotImageSortKey, wantSortKey) {
		t.Fatalf("image sort key=%+v want %+v", gotImageSortKey, wantSortKey)
	}
	for partRow, primaryID := range wantPrimaryIDs {
		locator, ok := part.LocatePrimaryID(primaryID)
		if !ok {
			t.Fatalf("missing locator for primary_id=%d", primaryID)
		}
		if locator.PartRow != partRow || locator.RowInGranule < 0 {
			t.Fatalf("locator primary_id=%d got %+v want part_row=%d", primaryID, locator, partRow)
		}
	}
}
