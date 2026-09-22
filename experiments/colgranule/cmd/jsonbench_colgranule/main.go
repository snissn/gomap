package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/snissn/gomap/experiments/colgranule"
)

func main() {
	data := flag.String("data", colgranule.DefaultJSONBenchDir, "JSONBench input file or directory with sorted .json/.json.gz files")
	limit := flag.Int("limit", 1_000_000, "maximum rows to load; <=0 means all rows")
	rowsPerGranule := flag.Int("rows-per-granule", colgranule.DefaultRowsPerGranule, "rows per encoded granule")
	flag.Parse()

	start := time.Now()
	ds, err := colgranule.LoadJSONBenchColumns(*data, *limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load JSONBench columns: %v\n", err)
		os.Exit(1)
	}
	loadDuration := time.Since(start)

	fmt.Printf("jsonbench_colgranule data=%s files=%d rows=%d columns=%d rows_per_granule=%d load_duration=%s\n",
		*data, len(ds.Files), ds.Rows, len(ds.Columns), *rowsPerGranule, loadDuration)
	for _, name := range ds.ColumnNames() {
		values := ds.Columns[name]
		min, max := int64(0), int64(0)
		if len(values) > 0 {
			min, max = minMaxForMain(values)
		}
		fmt.Printf("column=%s rows=%d min=%d max=%d\n", name, len(values), min, max)
	}

	summaries, err := colgranule.SummarizeJSONBenchDataset(ds, *rowsPerGranule, colgranule.DefaultJSONBenchConfigs())
	if err != nil {
		fmt.Fprintf(os.Stderr, "summarize JSONBench columns: %v\n", err)
		os.Exit(1)
	}
	for _, summary := range summaries {
		fmt.Println(colgranule.FormatColumnCodecSummary(summary))
	}
}

func minMaxForMain(values []int64) (int64, int64) {
	min, max := values[0], values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}
