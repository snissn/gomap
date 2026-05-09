package colgranule

import (
	"os"
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
