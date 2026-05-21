package colgranule

import (
	"os"
	"strconv"
	"testing"
)

func BenchmarkJSONBenchLocalColumns(b *testing.B) {
	path := os.Getenv("JSONBENCH_DATA")
	if path == "" {
		path = DefaultJSONBenchDir
	}
	if _, err := os.Stat(path); err != nil {
		b.Skipf("JSONBench data not present; set JSONBENCH_DATA or install %s", DefaultJSONBenchDir)
	}
	limit := 1000000
	ds, err := LoadJSONBenchColumns(path, limit)
	if err != nil {
		b.Fatalf("LoadJSONBenchColumns: %v", err)
	}
	b.Logf("loaded rows=%d columns=%d path=%s", ds.Rows, len(ds.Columns), path)
	for _, name := range ds.ColumnNames() {
		values := ds.Columns[name]
		for _, cfg := range DefaultJSONBenchConfigs() {
			b.Run(name+"/"+cfg.Encoding.String()+"/"+cfg.Compression.String(), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(values) * 8))
				for i := 0; i < b.N; i++ {
					summaries, err := SummarizeJSONBenchDataset(JSONBenchDataset{
						Rows:    ds.Rows,
						Columns: map[string][]int64{name: values},
					}, DefaultRowsPerGranule, []Config{cfg})
					if err != nil {
						b.Fatal(err)
					}
					benchSink += int64(summaries[0].StoredBytes)
				}
			})
		}
	}
}

func BenchmarkJSONBenchEncodedPartQueries(b *testing.B) {
	ds := syntheticJSONBenchDataset(DefaultRowsPerGranule * 100)
	part, err := BuildJSONBenchColumnPart(ds, DefaultRowsPerGranule)
	if err != nil {
		b.Fatalf("BuildJSONBenchColumnPart: %v", err)
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		b.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	queries := []struct {
		name string
		run  jsonBenchPartQueryRunner
	}{
		{"Q1_grouped_count", runJSONBenchPartQ1},
		{"Q2_group_count_distinct", runJSONBenchPartQ2},
		{"Q3_hourly_grouped_count", runJSONBenchPartQ3},
		{"Q4_min_by_user", runJSONBenchPartQ4},
		{"Q5_span_by_user", runJSONBenchPartQ5},
	}
	for _, q := range queries {
		b.Run(q.name, func(b *testing.B) {
			b.ReportAllocs()
			scratch := &jsonBenchPartQueryScratch{
				scanner:   part.NewScanner(),
				projected: make(map[string][]int64, 6),
			}
			rows, digest, diagnostics, err := q.run(part, codes, scratch)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(rows) + int64(digest)
			rowsScanned := diagnostics.RowsScanned
			if rowsScanned == 0 {
				rowsScanned = ds.Rows
			}
			valueBytes := int64(rowsScanned) * int64(len(diagnostics.ColumnsProjected)) * 8
			if valueBytes == 0 {
				valueBytes = int64(rowsScanned) * 8
			}
			b.SetBytes(valueBytes)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, digest, diagnostics, err = q.run(part, codes, scratch)
				if err != nil {
					b.Fatal(err)
				}
				benchSink += int64(rows) + int64(digest)
			}
			rowsScanned = diagnostics.RowsScanned
			if rowsScanned == 0 {
				rowsScanned = ds.Rows
			}
			reportGranuleBenchMetrics64(b, rowsScanned, valueBytes, int64(diagnostics.BytesDecoded))
		})
	}
}

func syntheticJSONBenchDataset(rows int) JSONBenchDataset {
	columns := map[string][]int64{
		"row_index":              make([]int64, rows),
		"time_us":                make([]int64, rows),
		"hour_of_day":            make([]int64, rows),
		"did_code":               make([]int64, rows),
		"kind_code":              make([]int64, rows),
		"commit_operation_code":  make([]int64, rows),
		"commit_collection_code": make([]int64, rows),
	}
	const didCardinality = 65536
	for i := 0; i < rows; i++ {
		columns["row_index"][i] = int64(i)
		columns["time_us"][i] = 1_700_000_000_000_000 + int64(i)*1_000_000
		columns["hour_of_day"][i] = unixMicroHour(columns["time_us"][i])
		columns["did_code"][i] = int64((i % didCardinality) + 1)
		columns["kind_code"][i] = 1
		columns["commit_operation_code"][i] = 1
		columns["commit_collection_code"][i] = int64((i % 3) + 1)
	}
	didDict := make(map[string]int64, didCardinality)
	for i := 1; i <= didCardinality; i++ {
		didDict[strconv.Itoa(i)] = int64(i)
	}
	return JSONBenchDataset{
		Rows:    rows,
		Columns: columns,
		Dictionaries: map[string]map[string]int64{
			"did_code": didDict,
			"kind_code": {
				"commit": 1,
			},
			"commit_operation_code": {
				"create": 1,
			},
			"commit_collection_code": {
				"app.bsky.feed.post":   1,
				"app.bsky.feed.repost": 2,
				"app.bsky.feed.like":   3,
			},
		},
	}
}
