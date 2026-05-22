package colgranule

import "testing"

func TestColumnPartByteAccountingReconcilesCategories(t *testing.T) {
	opts := partTestOptions([]SortKeyColumn{{Column: "time_us"}})
	opts.AggregateMetadata = []AggregateMetadataDefinition{aggregateMetadataTestDefinition()}
	part, err := BuildColumnPart(23, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {5, 4, 3, 2, 1},
		"time_us":   {50, 40, 30, 20, 10},
		"value":     {500, 400, 300, 200, 100},
		"kind_code": {0, 1, 1, 2, 0},
		"has_reply": {1, 1, 0, 1, 0},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}

	accounting := part.ByteAccounting()
	if accounting.Rows != part.Descriptor.RowCount || accounting.Columns != len(accounting.ColumnsDetail) || accounting.Granules != len(part.Descriptor.Granules) {
		t.Fatalf("shape=%+v want rows=%d columns=%d granules=%d", accounting, part.Descriptor.RowCount, len(accounting.ColumnsDetail), len(part.Descriptor.Granules))
	}
	if accounting.PhysicalFiles != 0 {
		t.Fatalf("physical files=%d want 0 for in-memory experiment", accounting.PhysicalFiles)
	}
	if accounting.DeclaredColumnStoredBytes == 0 || accounting.MarkBytes == 0 || accounting.SortKeyMetadataBytes == 0 || accounting.DescriptorBytes == 0 || accounting.LocatorBytes == 0 {
		t.Fatalf("missing accounting categories: %+v", accounting)
	}
	if accounting.AggregateMetadataBytes == 0 {
		t.Fatalf("aggregate metadata bytes were not counted: %+v", accounting)
	}
	if got, want := accounting.TotalStoredBytes, accounting.CategoryBytes(); got != want {
		t.Fatalf("total bytes=%d category sum=%d accounting=%+v", got, want, accounting)
	}
	if accounting.BytesPerRow <= 0 {
		t.Fatalf("bytes per row=%f want positive", accounting.BytesPerRow)
	}
	if accounting.RetainedJSONPayload != "absent_declared_columns_only" {
		t.Fatalf("retained JSON label=%q", accounting.RetainedJSONPayload)
	}
	if len(accounting.ColumnsDetail) != 5 {
		t.Fatalf("column detail count=%d want 5", len(accounting.ColumnsDetail))
	}
	for _, column := range accounting.ColumnsDetail {
		if column.Rows != 5 || column.Blocks == 0 || column.LogicalValueBytes == 0 || column.StoredBytes == 0 {
			t.Fatalf("bad column accounting: %+v", column)
		}
	}
	if len(accounting.CompressionDetail) == 0 {
		t.Fatal("missing compression detail")
	}
}

func TestColumnPartByteAccountingRecomputeTotalsZeroRowsClearsBytesPerRow(t *testing.T) {
	accounting := ColumnPartByteAccounting{
		DescriptorBytes: 64,
		BytesPerRow:     123,
	}
	accounting.RecomputeTotals()
	if accounting.TotalStoredBytes != 64 {
		t.Fatalf("total bytes=%d want 64", accounting.TotalStoredBytes)
	}
	if accounting.BytesPerRow != 0 {
		t.Fatalf("bytes per row=%f want 0", accounting.BytesPerRow)
	}
}

func TestEstimateJSONBenchDictionaryBytes(t *testing.T) {
	ds, err := LoadJSONBenchColumns("testdata/jsonbench_sample.jsonl", 0)
	if err != nil {
		t.Fatalf("LoadJSONBenchColumns: %v", err)
	}
	bytes := EstimateJSONBenchDictionaryBytes(ds)
	if bytes <= 0 {
		t.Fatalf("dictionary bytes=%d want positive", bytes)
	}
}
