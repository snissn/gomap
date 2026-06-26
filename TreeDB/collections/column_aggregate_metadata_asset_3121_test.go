package collections

import (
	"bytes"
	"reflect"
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

func TestAggregateMetadataAssetsFallbackForPredicateAggregates3121(t *testing.T) {
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
	if gotLen, wantLen := len(got), len(cfg.AggregateMetadata); gotLen != wantLen {
		t.Fatalf("got %d assets want unsupported aggregate skipped with %d assets", gotLen, wantLen)
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
		wantEncoded, err := encodeColumnAggregateMetadataAsset(want[idx])
		if err != nil {
			t.Fatalf("encode want[%d]: %v", idx, err)
		}
		if !bytes.Equal(gotEncoded, wantEncoded) {
			t.Fatalf("encoded asset[%d] mismatch", idx)
		}
	}
}
