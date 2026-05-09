package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/snissn/gomap/experiments/colgranule"
)

type clickHouseResult struct {
	System             string      `json:"system"`
	Version            string      `json:"version"`
	OS                 string      `json:"os"`
	Machine            string      `json:"machine"`
	DatasetSize        int         `json:"dataset_size"`
	NumLoadedDocuments int         `json:"num_loaded_documents"`
	TotalSize          int64       `json:"total_size"`
	DataSize           int64       `json:"data_size"`
	IndexSize          int64       `json:"index_size"`
	Result             [][]float64 `json:"result"`
}

type comparisonRaw struct {
	GeneratedAt       string                            `json:"generated_at"`
	DataPath          string                            `json:"data_path"`
	Limit             int                               `json:"limit"`
	Rows              int                               `json:"rows"`
	Files             []string                          `json:"files"`
	RowsPerGranule    int                               `json:"rows_per_granule"`
	LoadDuration      time.Duration                     `json:"load_duration"`
	ClickHouseLocal   clickHouseResult                  `json:"clickhouse_local"`
	ClickHouseAWS     clickHouseResult                  `json:"clickhouse_aws"`
	QueryTimings      []colgranule.JSONBenchQueryTiming `json:"query_timings"`
	ColumnSummaries   []colgranule.ColumnCodecSummary   `json:"column_summaries"`
	BestColumnStorage []bestColumnStorage               `json:"best_column_storage"`
}

type bestColumnStorage struct {
	Column               string                         `json:"column"`
	Encoding             colgranule.Encoding            `json:"encoding"`
	RequestedCompression colgranule.Compression         `json:"requested_compression"`
	StoredBytes          int                            `json:"stored_bytes"`
	ValueBytes           int                            `json:"value_bytes"`
	RatioVsValues        float64                        `json:"ratio_vs_values"`
	ActualCompressionMix map[colgranule.Compression]int `json:"actual_compression_mix"`
}

func main() {
	data := flag.String("data", colgranule.DefaultJSONBenchDir, "JSONBench input file or directory")
	limit := flag.Int("limit", 1_000_000, "maximum rows to load; <=0 means all rows")
	rowsPerGranule := flag.Int("rows-per-granule", colgranule.DefaultRowsPerGranule, "rows per encoded granule")
	attempts := flag.Int("attempts", 5, "query timing attempts")
	clickHouseLocalPath := flag.String("clickhouse-local", "/Users/michaelseiler/dev/snissn/JSONBench/clickhouse/local_results/fresh_1m_20260508_121356_clickhouse/result.json", "local ClickHouse JSONBench result")
	clickHouseAWSPath := flag.String("clickhouse-aws", "/Users/michaelseiler/dev/snissn/JSONBench/clickhouse/results/m6i.8xlarge_bluesky_1m.json", "checked ClickHouse JSONBench result")
	outJSON := flag.String("out-json", "experiments/colgranule/JSONBENCH_COMPARISON_RAW.json", "raw JSON output")
	outMarkdown := flag.String("out-md", "experiments/colgranule/JSONBENCH_COMPARISON_REPORT.md", "Markdown report output")
	flag.Parse()

	start := time.Now()
	ds, err := colgranule.LoadJSONBenchColumns(*data, *limit)
	must(err)
	loadDuration := time.Since(start)

	summaries, err := colgranule.SummarizeJSONBenchDataset(ds, *rowsPerGranule, colgranule.DefaultJSONBenchConfigs())
	must(err)
	timings, err := colgranule.RunJSONBenchQueries(ds, *attempts)
	must(err)

	raw := comparisonRaw{
		GeneratedAt:       time.Now().UTC().Format(time.RFC3339),
		DataPath:          *data,
		Limit:             *limit,
		Rows:              ds.Rows,
		Files:             ds.Files,
		RowsPerGranule:    *rowsPerGranule,
		LoadDuration:      loadDuration,
		ClickHouseLocal:   readClickHouseResult(*clickHouseLocalPath),
		ClickHouseAWS:     readClickHouseResult(*clickHouseAWSPath),
		QueryTimings:      timings,
		ColumnSummaries:   summaries,
		BestColumnStorage: bestColumns(summaries),
	}

	writeJSON(*outJSON, raw)
	writeMarkdown(*outMarkdown, raw)
}

func readClickHouseResult(path string) clickHouseResult {
	data, err := os.ReadFile(path)
	must(err)
	var result clickHouseResult
	must(json.Unmarshal(data, &result))
	return result
}

func writeJSON(path string, raw comparisonRaw) {
	data, err := json.MarshalIndent(raw, "", "  ")
	must(err)
	must(os.WriteFile(path, append(data, '\n'), 0o644))
}

func writeMarkdown(path string, raw comparisonRaw) {
	var b strings.Builder
	fmt.Fprintf(&b, "# JSONBench ClickHouse Comparison\n\n")
	fmt.Fprintf(&b, "Generated from `%s` with row limit `%d`; this local run read `%d` file(s) and `%d` rows. The comparison is a smoke-level column-kernel comparison, not a full database benchmark: TreeDB roots, collection WAL, query planning, persistence, and SQL execution are intentionally out of scope.\n\n", raw.DataPath, raw.Limit, len(raw.Files), raw.Rows)
	fmt.Fprintf(&b, "## Query Timing\n\n")
	fmt.Fprintf(&b, "| Query | Column-kernel best | ClickHouse local | ClickHouse AWS best | Notes |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---|\n")
	for i, timing := range raw.QueryTimings {
		local := clickHouseBest(raw.ClickHouseLocal, i)
		aws := clickHouseBest(raw.ClickHouseAWS, i)
		fmt.Fprintf(&b, "| %s | %.6fs | %.6fs | %.6fs | %s |\n", timing.Query, timing.Best.Seconds(), local, aws, timing.Description)
	}
	fmt.Fprintf(&b, "\n## Storage Footprint\n\n")
	fmt.Fprintf(&b, "ClickHouse local 1m size: total `%d` bytes, data `%d` bytes, index `%d` bytes.\n\n", raw.ClickHouseLocal.TotalSize, raw.ClickHouseLocal.DataSize, raw.ClickHouseLocal.IndexSize)
	fmt.Fprintf(&b, "The table below is one-column-at-a-time storage for the experimental granule codecs. It picks the smallest stored byte count observed for each derived `int64` column across raw, delta-varint, snappy, and lz4 combinations.\n\n")
	fmt.Fprintf(&b, "| Column | Best codec | Stored bytes | Ratio vs int64 values |\n")
	fmt.Fprintf(&b, "|---|---|---:|---:|\n")
	for _, col := range raw.BestColumnStorage {
		fmt.Fprintf(&b, "| `%s` | `%s` + `%s` | %d | %.6f |\n", col.Column, col.Encoding, col.RequestedCompression, col.StoredBytes, col.RatioVsValues)
	}
	fmt.Fprintf(&b, "\nBest-codec total for all derived columns: `%d` bytes.\n", bestTotal(raw.BestColumnStorage, nil))
	fmt.Fprintf(&b, "Best-codec total for ClickHouse indexed/query paths (`kind`, `commit.operation`, `commit.collection`, `did`, `time_us`): `%d` bytes.\n", bestTotal(raw.BestColumnStorage, map[string]bool{
		"kind_code": true, "commit_operation_code": true, "commit_collection_code": true, "did_code": true, "time_us": true,
	}))
	fmt.Fprintf(&b, "\n## Raw Data\n\n")
	fmt.Fprintf(&b, "Machine-readable raw data is in `experiments/colgranule/JSONBENCH_COMPARISON_RAW.json`.\n")
	must(os.WriteFile(path, []byte(b.String()), 0o644))
}

func bestColumns(summaries []colgranule.ColumnCodecSummary) []bestColumnStorage {
	byColumn := make(map[string]bestColumnStorage)
	for _, s := range summaries {
		cur, ok := byColumn[s.Column]
		if !ok || s.StoredBytes < cur.StoredBytes {
			byColumn[s.Column] = bestColumnStorage{
				Column:               s.Column,
				Encoding:             s.Encoding,
				RequestedCompression: s.RequestedCompression,
				StoredBytes:          s.StoredBytes,
				ValueBytes:           s.ValueBytes,
				RatioVsValues:        float64(s.StoredBytes) / float64(s.ValueBytes),
				ActualCompressionMix: s.ActualCompressionMix,
			}
		}
	}
	out := make([]bestColumnStorage, 0, len(byColumn))
	for _, s := range byColumn {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Column < out[j].Column })
	return out
}

func clickHouseBest(result clickHouseResult, i int) float64 {
	if i >= len(result.Result) || len(result.Result[i]) == 0 {
		return 0
	}
	best := result.Result[i][0]
	for _, v := range result.Result[i][1:] {
		if v < best {
			best = v
		}
	}
	return best
}

func bestTotal(cols []bestColumnStorage, include map[string]bool) int {
	var total int
	for _, col := range cols {
		if include != nil && !include[col.Column] {
			continue
		}
		total += col.StoredBytes
	}
	return total
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
