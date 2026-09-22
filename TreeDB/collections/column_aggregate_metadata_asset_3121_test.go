package collections

import (
	"bytes"
	"reflect"
	"runtime"
	"testing"
)

func TestAggregateMetadataAssetsSingleRowPassMatchesPerAggregate3121(t *testing.T) {
	cfg := aggregateMetadataBatchConfig3121()
	rows := aggregateMetadataBatchRows3121()

	got, err := buildColumnAggregateMetadataAssets(cfg, rows, cfg.AggregateMetadata, "events", "events/column-assets", 7, 11, 13)
	if err != nil {
		t.Fatalf("buildColumnAggregateMetadataAssets: %v", err)
	}
	want := buildColumnAggregateMetadataAssetsSequential3121(t, cfg, rows, cfg.AggregateMetadata)
	assertColumnAggregateMetadataAssetsEqual3121(t, got, want)
}

func TestAggregateMetadataAssetsTypedGranulePassMatchesPerAggregate3121(t *testing.T) {
	cfg := aggregateMetadataBatchConfig3121()
	cfg.SortKey = []ColumnSortKey{{Column: "time_us"}}
	for idx := range cfg.Columns {
		cfg.Columns[idx].Owner = TypedStorageOwnerColumnPart
	}
	for _, aggregate := range cfg.AggregateMetadata {
		if !columnAggregateMetadataUsesTypedColumnGranules(cfg, aggregate) {
			t.Fatalf("aggregate %s did not select typed-granule metadata path", aggregate.Name)
		}
	}
	rows := aggregateMetadataBatchRows3121()

	got, err := buildColumnAggregateMetadataAssets(cfg, rows, cfg.AggregateMetadata, "events", "events/column-assets", 7, 11, 13)
	if err != nil {
		t.Fatalf("buildColumnAggregateMetadataAssets: %v", err)
	}
	want := buildColumnAggregateMetadataAssetsSequential3121(t, cfg, rows, cfg.AggregateMetadata)
	assertColumnAggregateMetadataAssetsEqual3121(t, got, want)
}

func TestAggregateMetadataAssetsTypedGranulePrimaryIDOrderUsesFallback3175(t *testing.T) {
	cfg := aggregateMetadataBatchConfig3121()
	cfg.AssetManager = &ColumnAssetManagerConfig{Namespace: "events/column-assets"}
	for idx := range cfg.Columns {
		cfg.Columns[idx].Path = cfg.Columns[idx].Name
		cfg.Columns[idx].Owner = TypedStorageOwnerColumnPart
	}
	rows := append([]columnDeclaredRow(nil), aggregateMetadataBatchRows3121()[:3]...)

	typedPart, err := buildTypedColumnPartImageForDeclaredRowsWithResult(cfg, 7, typedColumnPartAssetPartID, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnPartImageForDeclaredRowsWithResult: %v", err)
	}
	if typedPart.Rows != len(rows) {
		t.Fatalf("typed part rows=%d want %d", typedPart.Rows, len(rows))
	}
	if len(typedPart.TypedGranuleRowOrder) != 0 {
		t.Fatalf("typed granule row order=%v want empty primary-id fallback", typedPart.TypedGranuleRowOrder)
	}
}

func TestAggregateMetadataAssetsTypedGranulePrecomputedOrderMatchesFallback3175(t *testing.T) {
	cfg := aggregateMetadataBatchConfig3121()
	cfg.SortKey = []ColumnSortKey{{Column: "time_us"}}
	cfg.AssetManager = &ColumnAssetManagerConfig{Namespace: "events/column-assets"}
	for idx := range cfg.Columns {
		cfg.Columns[idx].Path = cfg.Columns[idx].Name
		cfg.Columns[idx].Owner = TypedStorageOwnerColumnPart
	}
	rows := append([]columnDeclaredRow(nil), aggregateMetadataBatchRows3121()[:3]...)

	typedPart, err := buildTypedColumnPartImageForDeclaredRowsWithResult(cfg, 7, typedColumnPartAssetPartID, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnPartImageForDeclaredRowsWithResult: %v", err)
	}
	if typedPart.Rows != len(rows) {
		t.Fatalf("typed part rows=%d want %d", typedPart.Rows, len(rows))
	}
	if want := []int{1, 0, 2}; !reflect.DeepEqual(typedPart.TypedGranuleRowOrder, want) {
		t.Fatalf("typed granule row order=%v want %v", typedPart.TypedGranuleRowOrder, want)
	}

	got, err := buildColumnAggregateMetadataAssetsWithOptions(cfg, rows, cfg.AggregateMetadata, "events", "events/column-assets", 7, 11, 13, columnAggregateMetadataAssetBuildOptions{
		TypedGranuleRowOrder: typedPart.TypedGranuleRowOrder,
	})
	if err != nil {
		t.Fatalf("buildColumnAggregateMetadataAssetsWithOptions: %v", err)
	}
	want, err := buildColumnAggregateMetadataAssets(cfg, rows, cfg.AggregateMetadata, "events", "events/column-assets", 7, 11, 13)
	if err != nil {
		t.Fatalf("buildColumnAggregateMetadataAssets: %v", err)
	}
	assertColumnAggregateMetadataAssetsEqual3121(t, got, want)
}

func TestAggregateMetadataAssetsTypedGranulePrecomputedOrderWithPredicatesMatchesSequential3235(t *testing.T) {
	cfg := aggregateMetadataBatchConfig3121()
	cfg.SortKey = []ColumnSortKey{{Column: "time_us"}}
	cfg.AssetManager = &ColumnAssetManagerConfig{Namespace: "events/column-assets"}
	for idx := range cfg.Columns {
		cfg.Columns[idx].Path = cfg.Columns[idx].Name
		cfg.Columns[idx].Owner = TypedStorageOwnerColumnPart
	}
	cfg.AggregateMetadata = []ColumnAggregateMetadata{{
		Name:        "like_time_min",
		Column:      "time_us",
		GroupColumn: "did",
		Kind:        ColumnAggregateMin,
		Predicates:  []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "like"}},
	}}
	rows := append([]columnDeclaredRow(nil), aggregateMetadataBatchRows3121()[:3]...)

	typedPart, err := buildTypedColumnPartImageForDeclaredRowsWithResult(cfg, 7, typedColumnPartAssetPartID, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnPartImageForDeclaredRowsWithResult: %v", err)
	}
	if want := []int{1, 0, 2}; !reflect.DeepEqual(typedPart.TypedGranuleRowOrder, want) {
		t.Fatalf("typed granule row order=%v want %v", typedPart.TypedGranuleRowOrder, want)
	}

	got, err := buildColumnAggregateMetadataAssetsWithOptions(cfg, rows, cfg.AggregateMetadata, "events", "events/column-assets", 7, 11, 13, columnAggregateMetadataAssetBuildOptions{
		TypedGranuleRowOrder: typedPart.TypedGranuleRowOrder,
	})
	if err != nil {
		t.Fatalf("buildColumnAggregateMetadataAssetsWithOptions: %v", err)
	}
	want := buildColumnAggregateMetadataAssetsSequential3121(t, cfg, rows, cfg.AggregateMetadata)
	assertColumnAggregateMetadataAssetsEqual3121(t, got, want)
}

func TestAggregateMetadataAssetsPredicateAccumulatorsRemainSeparate3235(t *testing.T) {
	cfg := aggregateMetadataBatchConfig3121()
	cfg.AggregateMetadata = []ColumnAggregateMetadata{
		{
			Name:        "like_time_min",
			Column:      "time_us",
			GroupColumn: "did",
			Kind:        ColumnAggregateMin,
			Predicates:  []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "like"}},
		},
		{
			Name:        "post_time_min",
			Column:      "time_us",
			GroupColumn: "did",
			Kind:        ColumnAggregateMin,
			Predicates:  []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "post"}},
		},
	}
	rows := aggregateMetadataBatchRows3121()

	got, err := buildColumnAggregateMetadataAssets(cfg, rows, cfg.AggregateMetadata, "events", "events/column-assets", 7, 11, 13)
	if err != nil {
		t.Fatalf("buildColumnAggregateMetadataAssets: %v", err)
	}
	want := buildColumnAggregateMetadataAssetsSequential3121(t, cfg, rows, cfg.AggregateMetadata)
	assertColumnAggregateMetadataAssetsEqual3121(t, got, want)
	if gotLen, wantLen := len(got), 2; gotLen != wantLen {
		t.Fatalf("got %d assets want %d", gotLen, wantLen)
	}
	if got[0].Entries[0].Group != "did:alpha" || got[0].Entries[0].Min != 10 || got[0].Entries[0].Max != 20 {
		t.Fatalf("like aggregate entries=%+v", got[0].Entries)
	}
	if got[1].Entries[0].Group != "did:beta" || got[1].Entries[0].Min != 5 || got[1].Entries[0].Max != 5 {
		t.Fatalf("post aggregate entries=%+v", got[1].Entries)
	}
}

func TestColumnAggregateMetadataPredicateAccumulatorKeySeparatesInListBoundaries3236(t *testing.T) {
	broadInList := []ColumnPhysicalQueryPredicate{{
		Column: "a_col",
		Kind:   ColumnPhysicalQueryPredicateInList,
		Values: []string{"a", "b", string(ColumnPhysicalQueryPredicateEqual), "z"},
	}}
	inListAndEqual := []ColumnPhysicalQueryPredicate{
		{
			Column: "a_col",
			Kind:   ColumnPhysicalQueryPredicateInList,
			Values: []string{"a"},
		},
		{
			Column: "b",
			Kind:   ColumnPhysicalQueryPredicateEqual,
			Value:  "z",
		},
	}

	broadKey := columnAggregateMetadataPredicateAccumulatorKey(broadInList)
	narrowKey := columnAggregateMetadataPredicateAccumulatorKey(inListAndEqual)
	if broadKey == narrowKey {
		t.Fatalf("ambiguous predicate accumulator keys: %q", broadKey)
	}
}

func TestAggregateMetadataAssetsTypedGranuleParallelRowOrderWithPredicatesMatchesSequential3235(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)
	cfg := aggregateMetadataBatchConfig3121()
	for idx := range cfg.Columns {
		cfg.Columns[idx].Owner = TypedStorageOwnerColumnPart
	}
	cfg.AggregateMetadata = []ColumnAggregateMetadata{
		{
			Name:        "like_time_min",
			Column:      "time_us",
			GroupColumn: "did",
			Kind:        ColumnAggregateMin,
			Predicates:  []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "like"}},
		},
		{
			Name:        "post_time_min",
			Column:      "time_us",
			GroupColumn: "did",
			Kind:        ColumnAggregateMin,
			Predicates:  []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "post"}},
		},
	}
	rows := make([]columnDeclaredRow, typedColumnDefaultRowsPerGranule()+3)
	order := make([]int, len(rows))
	for idx := range rows {
		kind := "like"
		if idx%3 == 0 {
			kind = "post"
		}
		rows[idx] = columnDeclaredRow{ID: []byte("row"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: int64(idx)},
			{Type: ColumnStoreValueString, Present: true, String: kind},
			{Type: ColumnStoreValueString, Present: true, String: "did"},
		}}
		order[idx] = idx
	}
	entrySets := columnAggregateMetadataEntrySetCount(len(rows), typedColumnDefaultRowsPerGranule())
	if workers := columnAggregateMetadataEntrySetWorkerCount(entrySets); workers < 2 {
		t.Fatalf("worker count=%d want parallel path for entry_sets=%d", workers, entrySets)
	}

	got, err := buildColumnAggregateMetadataAssetsWithOptions(cfg, rows, cfg.AggregateMetadata, "events", "events/column-assets", 7, 11, 13, columnAggregateMetadataAssetBuildOptions{
		TypedGranuleRowOrder: order,
	})
	if err != nil {
		t.Fatalf("buildColumnAggregateMetadataAssetsWithOptions: %v", err)
	}
	want := buildColumnAggregateMetadataAssetsSequential3121(t, cfg, rows, cfg.AggregateMetadata)
	assertColumnAggregateMetadataAssetsEqual3121(t, got, want)
}

func TestAggregateMetadataAssetsPredicateAggregatesMatchSequential3121(t *testing.T) {
	cfg := aggregateMetadataBatchConfig3121()
	cfg.AggregateMetadata = []ColumnAggregateMetadata{{
		Name:        "post_time_min",
		Column:      "time_us",
		GroupColumn: "did",
		Kind:        ColumnAggregateMin,
		Predicates:  []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "post"}},
	}}
	rows := aggregateMetadataBatchRows3121()

	got, err := buildColumnAggregateMetadataAssets(cfg, rows, cfg.AggregateMetadata, "events", "events/column-assets", 7, 11, 13)
	if err != nil {
		t.Fatalf("buildColumnAggregateMetadataAssets: %v", err)
	}
	want := buildColumnAggregateMetadataAssetsSequential3121(t, cfg, rows, cfg.AggregateMetadata)
	assertColumnAggregateMetadataAssetsEqual3121(t, got, want)
	if len(got) != 1 || len(got[0].Predicates) != 1 || got[0].Predicates[0].Column != "kind" {
		t.Fatalf("predicate coverage not preserved: %+v", got)
	}
}

func TestAggregateMetadataAssetsFallbackSkipsUnsupportedAggregates3121(t *testing.T) {
	cfg := aggregateMetadataBatchConfig3121()
	aggregates := append(cloneColumnAggregateMetadata(cfg.AggregateMetadata), ColumnAggregateMetadata{
		Name:        "sum_time_us",
		Column:      "time_us",
		GroupColumn: "did",
		Kind:        ColumnAggregateSum,
	})
	rows := aggregateMetadataBatchRows3121()

	got, err := buildColumnAggregateMetadataAssets(cfg, rows, aggregates, "events", "events/column-assets", 7, 11, 13)
	if err != nil {
		t.Fatalf("buildColumnAggregateMetadataAssets: %v", err)
	}
	want := buildColumnAggregateMetadataAssetsSequential3121(t, cfg, rows, aggregates)
	assertColumnAggregateMetadataAssetsEqual3121(t, got, want)
	if gotLen, wantLen := len(got), len(want); gotLen != wantLen {
		t.Fatalf("got %d assets want unsupported aggregate skipped with %d assets", gotLen, wantLen)
	}
}

func TestColumnRowSidecarAssetsFusedMatchesSeparateBuilders3137(t *testing.T) {
	cfg := aggregateMetadataBatchConfig3121()
	rows := aggregateMetadataBatchRows3121()[:4]

	got, ok, err := buildColumnRowSidecarAssets(cfg, rows, cfg.AggregateMetadata, "events", "events/column-assets", 7, 11, 13)
	if err != nil {
		t.Fatalf("buildColumnRowSidecarAssets: %v", err)
	}
	if !ok {
		t.Fatalf("buildColumnRowSidecarAssets did not use fused path")
	}
	wantDictionaries, err := buildColumnDictionaryCodesAssets(cfg, rows, "events", "events/column-assets", 7, 11, 13)
	if err != nil {
		t.Fatalf("buildColumnDictionaryCodesAssets: %v", err)
	}
	wantInt64, err := buildColumnInt64ValuesAssets(cfg, rows, "events", "events/column-assets", 7, 11, 13)
	if err != nil {
		t.Fatalf("buildColumnInt64ValuesAssets: %v", err)
	}
	wantMetadata, err := buildColumnAggregateMetadataAssets(cfg, rows, cfg.AggregateMetadata, "events", "events/column-assets", 7, 11, 13)
	if err != nil {
		t.Fatalf("buildColumnAggregateMetadataAssets: %v", err)
	}
	assertColumnDictionaryCodesAssetsEqual3137(t, got.DictionaryCodes, wantDictionaries)
	assertColumnInt64ValuesAssetsEqual3137(t, got.Int64Values, wantInt64)
	assertColumnAggregateMetadataAssetsEqual3121(t, got.AggregateMetadata, wantMetadata)
}

func TestColumnRowSidecarAssetsFallsBackForPredicateAggregates3137(t *testing.T) {
	cfg := aggregateMetadataBatchConfig3121()
	cfg.AggregateMetadata = []ColumnAggregateMetadata{{
		Name:        "post_time_min",
		Column:      "time_us",
		GroupColumn: "did",
		Kind:        ColumnAggregateMin,
		Predicates:  []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "post"}},
	}}
	rows := aggregateMetadataBatchRows3121()[:4]

	_, ok, err := buildColumnRowSidecarAssets(cfg, rows, cfg.AggregateMetadata, "events", "events/column-assets", 7, 11, 13)
	if err != nil {
		t.Fatalf("buildColumnRowSidecarAssets: %v", err)
	}
	if ok {
		t.Fatalf("predicate aggregate unexpectedly used fused path")
	}
}

func TestColumnRowSidecarAssetsFallsBackForDeletedRows3137(t *testing.T) {
	cfg := aggregateMetadataBatchConfig3121()
	rows := aggregateMetadataBatchRows3121()

	_, ok, err := buildColumnRowSidecarAssets(cfg, rows, cfg.AggregateMetadata, "events", "events/column-assets", 7, 11, 13)
	if err != nil {
		t.Fatalf("buildColumnRowSidecarAssets: %v", err)
	}
	if ok {
		t.Fatalf("deleted rows unexpectedly used fused path")
	}
}

func aggregateMetadataBatchConfig3121() ColumnStoreConfig {
	return ColumnStoreConfig{
		SchemaHash: 3121,
		Columns: []ColumnStoreColumn{
			{Name: "time_us", ValueType: ColumnStoreValueInt64},
			{Name: "kind", ValueType: ColumnStoreValueString, Dictionary: true},
			{Name: "did", ValueType: ColumnStoreValueString, Dictionary: true},
		},
		AggregateMetadata: []ColumnAggregateMetadata{
			{Name: "q1_kind_count", GroupColumn: "kind", Kind: ColumnAggregateCount},
			{Name: "q5_did_time_span_min", Column: "time_us", GroupColumn: "did", Kind: ColumnAggregateMin},
			{Name: "q5_did_time_span_max", Column: "time_us", GroupColumn: "did", Kind: ColumnAggregateMax},
		},
	}
}

func aggregateMetadataBatchRows3121() []columnDeclaredRow {
	return []columnDeclaredRow{
		{ID: []byte("alpha-10"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 10},
			{Type: ColumnStoreValueString, Present: true, String: "like"},
			{Type: ColumnStoreValueString, Present: true, StringBytes: []byte("did:alpha")},
		}},
		{ID: []byte("beta-5"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 5},
			{Type: ColumnStoreValueString, Present: true, String: "post"},
			{Type: ColumnStoreValueString, Present: true, String: "did:beta"},
		}},
		{ID: []byte("alpha-20"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 20},
			{Type: ColumnStoreValueString, Present: true, String: "like"},
			{Type: ColumnStoreValueString, Present: true, String: "did:alpha"},
		}},
		{ID: []byte("empty-kind"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 30},
			{Type: ColumnStoreValueString},
			{Type: ColumnStoreValueString, Present: true, String: "did:gamma"},
		}},
		{ID: []byte("deleted"), Deleted: true, Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 1},
			{Type: ColumnStoreValueString, Present: true, String: "deleted"},
			{Type: ColumnStoreValueString, Present: true, String: "did:deleted"},
		}},
	}
}

func assertColumnDictionaryCodesAssetsEqual3137(t *testing.T, got, want []columnDictionaryCodesAsset) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dictionary assets mismatch\ngot:  %+v\nwant: %+v", got, want)
	}
	for idx := range got {
		gotEncoded, err := encodeColumnDictionaryCodesAsset(got[idx])
		if err != nil {
			t.Fatalf("encode got dictionary[%d]: %v", idx, err)
		}
		wantEncoded, err := encodeColumnDictionaryCodesAsset(want[idx])
		if err != nil {
			t.Fatalf("encode want dictionary[%d]: %v", idx, err)
		}
		if !bytes.Equal(gotEncoded, wantEncoded) {
			t.Fatalf("encoded dictionary asset[%d] mismatch", idx)
		}
	}
}

func assertColumnInt64ValuesAssetsEqual3137(t *testing.T, got, want []columnInt64ValuesAsset) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("int64 assets mismatch\ngot:  %+v\nwant: %+v", got, want)
	}
	for idx := range got {
		gotEncoded, err := encodeColumnInt64ValuesAsset(got[idx])
		if err != nil {
			t.Fatalf("encode got int64[%d]: %v", idx, err)
		}
		wantEncoded, err := encodeColumnInt64ValuesAsset(want[idx])
		if err != nil {
			t.Fatalf("encode want int64[%d]: %v", idx, err)
		}
		if !bytes.Equal(gotEncoded, wantEncoded) {
			t.Fatalf("encoded int64 asset[%d] mismatch", idx)
		}
	}
}

func buildColumnAggregateMetadataAssetsSequential3121(t *testing.T, cfg ColumnStoreConfig, rows []columnDeclaredRow, aggregates []ColumnAggregateMetadata) []columnAggregateMetadataAsset {
	t.Helper()
	assets := make([]columnAggregateMetadataAsset, 0, len(aggregates))
	for _, aggregate := range aggregates {
		asset, ok, err := buildColumnAggregateMetadataAsset(cfg, rows, aggregate, "events", "events/column-assets", 7, 11, 13)
		if err != nil {
			t.Fatalf("buildColumnAggregateMetadataAsset(%s): %v", aggregate.Name, err)
		}
		if ok {
			assets = append(assets, asset)
		}
	}
	return assets
}

func assertColumnAggregateMetadataAssetsEqual3121(t *testing.T, got, want []columnAggregateMetadataAsset) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("assets mismatch\ngot:  %+v\nwant: %+v", got, want)
	}
	for idx := range got {
		gotEncoded, err := encodeColumnAggregateMetadataAsset(got[idx])
		if err != nil {
			t.Fatalf("encode got[%d]: %v", idx, err)
		}
		if size := columnAggregateMetadataEncodedSize(got[idx]); size != len(gotEncoded) {
			t.Fatalf("encoded aggregate metadata size[%d]=%d want len=%d", idx, size, len(gotEncoded))
		}
		wantEncoded, err := encodeColumnAggregateMetadataAsset(want[idx])
		if err != nil {
			t.Fatalf("encode want[%d]: %v", idx, err)
		}
		if !bytes.Equal(gotEncoded, wantEncoded) {
			t.Fatalf("encoded asset[%d] mismatch", idx)
		}
	}
}
