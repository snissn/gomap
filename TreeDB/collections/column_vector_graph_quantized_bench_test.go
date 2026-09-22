package collections

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/brq"
	"github.com/snissn/gomap/TreeDB/internal/rabitq"
)

const columnGraphScalarU8QuantizedBenchIndexName1926 = "embedding.scalar_u8.fast"
const columnGraphScalarU8AlphaQuantizedBenchIndexName2845 = "embedding.scalar_u8.alpha"

type columnGraphScalarU8QuantizedBenchShape1926 struct {
	rows           int
	dims           int
	m              int
	efConstruction int
	topK           int
	efSearch       int
	queryOrdinal   int
	queryCount     int
}

const (
	columnGraphScalarU8QuantizedBenchShapeEnv1926          = "TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_SHAPE"
	columnGraphScalarU8QuantizedBenchRowsEnv1926           = "TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_ROWS"
	columnGraphScalarU8QuantizedBenchDimsEnv1926           = "TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_DIMS"
	columnGraphScalarU8QuantizedBenchMEnv1926              = "TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_M"
	columnGraphScalarU8QuantizedBenchEfConstructionEnv1926 = "TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_EF_CONSTRUCTION"
	columnGraphScalarU8QuantizedBenchTopKEnv1926           = "TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_TOP_K"
	columnGraphScalarU8QuantizedBenchEfSearchEnv1926       = "TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_EF_SEARCH"
	columnGraphScalarU8QuantizedBenchQueryOrdinalEnv1926   = "TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_QUERY_ORDINAL"
	columnGraphScalarU8QuantizedBenchQueriesEnv1926        = "TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_QUERIES"

	columnGraphScalarU8QuantizedBenchVectorDocsEnv2591           = "TREEDB_VECTOR_BENCH_DOCS"
	columnGraphScalarU8QuantizedBenchVectorDimsEnv2591           = "TREEDB_VECTOR_BENCH_DIMS"
	columnGraphScalarU8QuantizedBenchVectorMEnv2591              = "TREEDB_VECTOR_BENCH_M"
	columnGraphScalarU8QuantizedBenchVectorEfConstructionEnv2591 = "TREEDB_VECTOR_BENCH_EF_CONSTRUCTION"
	columnGraphScalarU8QuantizedBenchVectorTopKEnv2591           = "TREEDB_VECTOR_BENCH_TOPK"
	columnGraphScalarU8QuantizedBenchVectorEfSearchEnv2591       = "TREEDB_VECTOR_BENCH_EF_SEARCH"
	columnGraphScalarU8QuantizedBenchVectorQueriesEnv2591        = "TREEDB_VECTOR_BENCH_QUERIES"

	columnGraphQuantizedHotCPUProfilePathEnv2541        = "TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_CPU_PROFILE_PATH"
	columnGraphQuantizedHotAllocsProfilePathEnv2541     = "TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_ALLOCS_PROFILE_PATH"
	columnGraphQuantizedHotAllocsBaseProfilePathEnv2541 = "TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_ALLOCS_BASE_PROFILE_PATH"
	columnGraphQuantizedHotMemProfileRateEnv2541        = "TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_MEM_PROFILE_RATE"
	defaultColumnGraphQuantizedHotMemProfileRate2541    = 1
)

func init() {
	if strings.TrimSpace(os.Getenv(columnGraphQuantizedHotAllocsProfilePathEnv2541)) != "" {
		runtime.MemProfileRate = 0
	}
}

type columnGraphQuantizedSearchLoopProfileHook2541 struct {
	cpuPath              string
	allocsPath           string
	allocsBasePath       string
	oldMemProfileRate    int
	memProfileRate       int
	memProfileSuppressed bool
	restored             bool
}

func newColumnGraphQuantizedSearchLoopProfileHook2541(b *testing.B) *columnGraphQuantizedSearchLoopProfileHook2541 {
	b.Helper()

	hook := &columnGraphQuantizedSearchLoopProfileHook2541{
		cpuPath:        strings.TrimSpace(os.Getenv(columnGraphQuantizedHotCPUProfilePathEnv2541)),
		allocsPath:     strings.TrimSpace(os.Getenv(columnGraphQuantizedHotAllocsProfilePathEnv2541)),
		allocsBasePath: strings.TrimSpace(os.Getenv(columnGraphQuantizedHotAllocsBaseProfilePathEnv2541)),
		memProfileRate: defaultColumnGraphQuantizedHotMemProfileRate2541,
	}
	if !hook.enabled() {
		return hook
	}
	if hook.allocsPath != "" {
		if raw := strings.TrimSpace(os.Getenv(columnGraphQuantizedHotMemProfileRateEnv2541)); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value <= 0 {
				b.Fatalf("%s=%q must be a positive integer", columnGraphQuantizedHotMemProfileRateEnv2541, raw)
			}
			hook.memProfileRate = value
		}
		hook.oldMemProfileRate = runtime.MemProfileRate
		runtime.MemProfileRate = 0
		hook.memProfileSuppressed = true
		runtime.GC()
	}
	return hook
}

func (hook *columnGraphQuantizedSearchLoopProfileHook2541) enabled() bool {
	return hook != nil && (hook.cpuPath != "" || hook.allocsPath != "")
}

func (hook *columnGraphQuantizedSearchLoopProfileHook2541) finish() {
	if hook == nil || !hook.memProfileSuppressed || hook.restored {
		return
	}
	runtime.MemProfileRate = hook.oldMemProfileRate
	hook.restored = true
}

func (hook *columnGraphQuantizedSearchLoopProfileHook2541) start(b *testing.B) func() {
	b.Helper()
	if !hook.enabled() {
		return func() {}
	}
	if hook.allocsPath != "" {
		runtime.GC()
		if hook.allocsBasePath != "" {
			// Serialize the baseline at the same sampling rate used for the raw
			// search-loop profile. Go pprof scales sampled allocations using the
			// current runtime.MemProfileRate at WriteTo time; using a different rate
			// here would leave setup samples in the later raw-minus-base diff when
			// HOT_MEM_PROFILE_RATE is greater than 1.
			func() {
				runtime.MemProfileRate = hook.memProfileRate
				defer func() {
					runtime.MemProfileRate = 0
				}()
				writeColumnGraphQuantizedHotProfile2541(b, "allocs", hook.allocsBasePath)
			}()
		}
	}

	var cpuFile *os.File
	if hook.cpuPath != "" {
		cpuFile = createColumnGraphQuantizedHotProfileFile2541(b, hook.cpuPath, "cpu")
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			_ = cpuFile.Close()
			b.Fatalf("start search-loop CPU profile: %v; do not also pass go test -cpuprofile", err)
		}
	}
	if hook.allocsPath != "" {
		runtime.MemProfileRate = hook.memProfileRate
	}

	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		if hook.allocsPath != "" {
			// Keep the collection sampling rate in effect until allocs WriteTo
			// completes. The pprof writer scales sampled allocations using the
			// current runtime.MemProfileRate, so disabling sampling before the raw
			// profile is serialized underreports runs that use rates greater than 1.
			// The harness filters pprof-writer noise from the published allocs.pprof
			// while retaining allocs_diff_raw.pprof for auditability.
			runtime.MemProfileRate = hook.memProfileRate
			defer func() {
				runtime.MemProfileRate = 0
			}()
		}
		if cpuFile != nil {
			pprof.StopCPUProfile()
			if err := cpuFile.Close(); err != nil {
				b.Errorf("close search-loop CPU profile: %v", err)
			}
		}
		if hook.allocsPath != "" {
			runtime.GC()
			runtime.GC()
			writeColumnGraphQuantizedHotProfile2541(b, "allocs", hook.allocsPath)
		}
	}
}

func createColumnGraphQuantizedHotProfileFile2541(b *testing.B, path, kind string) *os.File {
	b.Helper()
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			b.Fatalf("create search-loop %s profile dir: %v", kind, err)
		}
	}
	file, err := os.Create(path)
	if err != nil {
		b.Fatalf("create search-loop %s profile: %v", kind, err)
	}
	return file
}

func writeColumnGraphQuantizedHotProfile2541(b *testing.B, name, path string) {
	b.Helper()
	file := createColumnGraphQuantizedHotProfileFile2541(b, path, name)
	defer func() {
		if err := file.Close(); err != nil {
			b.Errorf("close search-loop %s profile: %v", name, err)
		}
	}()
	profile := pprof.Lookup(name)
	if profile == nil {
		b.Fatalf("runtime profile %q is unavailable", name)
	}
	if err := profile.WriteTo(file, 0); err != nil {
		b.Fatalf("write search-loop %s profile: %v", name, err)
	}
}

func defaultColumnGraphScalarU8QuantizedBenchShape1926() columnGraphScalarU8QuantizedBenchShape1926 {
	return columnGraphScalarU8QuantizedBenchShape1926{rows: 1024, dims: 128, m: 16, efConstruction: 128, topK: 10, efSearch: 128, queryOrdinal: 37, queryCount: 1}
}

func defaultColumnGraphQuantizedProductionBenchShape2591() columnGraphScalarU8QuantizedBenchShape1926 {
	return columnGraphScalarU8QuantizedBenchShape1926{rows: 10000, dims: 768, m: 16, efConstruction: 128, topK: 10, efSearch: 128, queryOrdinal: 3333, queryCount: 16}
}

func columnGraphScalarU8QuantizedBenchShapeFromEnv1926(tb testing.TB) columnGraphScalarU8QuantizedBenchShape1926 {
	tb.Helper()
	shape, err := columnGraphScalarU8QuantizedBenchShapeFromEnvValues1926(os.Getenv)
	if err != nil {
		tb.Fatalf("column graph quantized benchmark shape: %v", err)
	}
	return shape
}

func columnGraphQuantizedProductionBenchShapeFromEnv2591(tb testing.TB) columnGraphScalarU8QuantizedBenchShape1926 {
	tb.Helper()
	shape, err := columnGraphQuantizedProductionBenchShapeFromEnvValues2591(os.Getenv)
	if err != nil {
		tb.Fatalf("column graph quantized production benchmark shape: %v", err)
	}
	return shape
}

func columnGraphScalarU8QuantizedBenchShapeFromEnvValues1926(getenv func(string) string) (columnGraphScalarU8QuantizedBenchShape1926, error) {
	return columnGraphScalarU8QuantizedBenchShapeFromEnvValuesWithDefault1926(getenv, defaultColumnGraphScalarU8QuantizedBenchShape1926())
}

func columnGraphQuantizedProductionBenchShapeFromEnvValues2591(getenv func(string) string) (columnGraphScalarU8QuantizedBenchShape1926, error) {
	return columnGraphScalarU8QuantizedBenchShapeFromEnvValuesWithDefault1926(getenv, defaultColumnGraphQuantizedProductionBenchShape2591())
}

func columnGraphScalarU8QuantizedBenchShapeFromEnvValuesWithDefault1926(getenv func(string) string, shape columnGraphScalarU8QuantizedBenchShape1926) (columnGraphScalarU8QuantizedBenchShape1926, error) {
	if rawShape := strings.TrimSpace(getenv(columnGraphScalarU8QuantizedBenchShapeEnv1926)); rawShape != "" {
		rows, dims, err := parseColumnGraphScalarU8QuantizedBenchShapeToken1926(rawShape)
		if err != nil {
			return shape, err
		}
		shape.rows = rows
		shape.dims = dims
	}
	if err := applyPositiveColumnGraphScalarU8QuantizedBenchEnvAliases1926(getenv, []string{columnGraphScalarU8QuantizedBenchVectorDocsEnv2591, columnGraphScalarU8QuantizedBenchRowsEnv1926}, "rows", &shape.rows); err != nil {
		return shape, err
	}
	if err := applyPositiveColumnGraphScalarU8QuantizedBenchEnvAliases1926(getenv, []string{columnGraphScalarU8QuantizedBenchVectorDimsEnv2591, columnGraphScalarU8QuantizedBenchDimsEnv1926}, "dims", &shape.dims); err != nil {
		return shape, err
	}
	if err := applyPositiveColumnGraphScalarU8QuantizedBenchEnvAliases1926(getenv, []string{columnGraphScalarU8QuantizedBenchVectorMEnv2591, columnGraphScalarU8QuantizedBenchMEnv1926}, "m", &shape.m); err != nil {
		return shape, err
	}
	if err := applyPositiveColumnGraphScalarU8QuantizedBenchEnvAliases1926(getenv, []string{columnGraphScalarU8QuantizedBenchVectorEfConstructionEnv2591, columnGraphScalarU8QuantizedBenchEfConstructionEnv1926}, "efConstruction", &shape.efConstruction); err != nil {
		return shape, err
	}
	if err := applyPositiveColumnGraphScalarU8QuantizedBenchEnvAliases1926(getenv, []string{columnGraphScalarU8QuantizedBenchVectorTopKEnv2591, columnGraphScalarU8QuantizedBenchTopKEnv1926}, "topK", &shape.topK); err != nil {
		return shape, err
	}
	if err := applyPositiveColumnGraphScalarU8QuantizedBenchEnvAliases1926(getenv, []string{columnGraphScalarU8QuantizedBenchVectorEfSearchEnv2591, columnGraphScalarU8QuantizedBenchEfSearchEnv1926}, "efSearch", &shape.efSearch); err != nil {
		return shape, err
	}
	if err := applyPositiveColumnGraphScalarU8QuantizedBenchEnvAliases1926(getenv, []string{columnGraphScalarU8QuantizedBenchVectorQueriesEnv2591, columnGraphScalarU8QuantizedBenchQueriesEnv1926}, "queries", &shape.queryCount); err != nil {
		return shape, err
	}
	if err := applyNonNegativeColumnGraphScalarU8QuantizedBenchEnv1926(getenv, columnGraphScalarU8QuantizedBenchQueryOrdinalEnv1926, "queryOrdinal", &shape.queryOrdinal); err != nil {
		return shape, err
	}
	if shape.efConstruction < shape.m {
		return shape, fmt.Errorf("efConstruction=%d must be >= m=%d", shape.efConstruction, shape.m)
	}
	if shape.topK > shape.rows {
		return shape, fmt.Errorf("topK=%d exceeds rows=%d", shape.topK, shape.rows)
	}
	if shape.queryOrdinal >= shape.rows {
		if shape.queryCount <= 1 {
			return shape, fmt.Errorf("queryOrdinal=%d out of range rows=%d", shape.queryOrdinal, shape.rows)
		}
		shape.queryOrdinal = shape.rows / 3
	}
	if shape.queryCount > shape.rows {
		shape.queryCount = shape.rows
	}
	return shape, nil
}

func parseColumnGraphScalarU8QuantizedBenchShapeToken1926(raw string) (int, int, error) {
	defaultShape := defaultColumnGraphScalarU8QuantizedBenchShape1926()
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" || normalized == "default" || normalized == "claim_core" {
		return defaultShape.rows, defaultShape.dims, nil
	}
	normalized = strings.ReplaceAll(normalized, "_x_", "x")
	normalized = strings.ReplaceAll(normalized, "_", "")
	parts := strings.Split(normalized, "x")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("unsupported %s=%q (want default, 1024x128, 10k_x_768, 10k_x_1536, or <rows>x<dims>)", columnGraphScalarU8QuantizedBenchShapeEnv1926, raw)
	}
	rows, err := parseColumnGraphScalarU8QuantizedBenchPositiveToken1926(parts[0], "rows", true)
	if err != nil {
		return 0, 0, fmt.Errorf("unsupported %s=%q: %w", columnGraphScalarU8QuantizedBenchShapeEnv1926, raw, err)
	}
	dims, err := parseColumnGraphScalarU8QuantizedBenchPositiveToken1926(parts[1], "dims", true)
	if err != nil {
		return 0, 0, fmt.Errorf("unsupported %s=%q: %w", columnGraphScalarU8QuantizedBenchShapeEnv1926, raw, err)
	}
	return rows, dims, nil
}

func parseColumnGraphScalarU8QuantizedBenchPositiveToken1926(raw, name string, allowK bool) (int, error) {
	token := strings.TrimSpace(strings.ToLower(raw))
	multiplier := 1
	if strings.HasSuffix(token, "k") {
		if !allowK {
			return 0, fmt.Errorf("%s token %q may not use k suffix", name, raw)
		}
		multiplier = 1000
		token = strings.TrimSuffix(token, "k")
	}
	value, err := strconv.Atoi(token)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s token %q must be a positive integer", name, raw)
	}
	maxInt := int(^uint(0) >> 1)
	if value > maxInt/multiplier {
		return 0, fmt.Errorf("%s token %q overflows int", name, raw)
	}
	return value * multiplier, nil
}

func applyPositiveColumnGraphScalarU8QuantizedBenchEnv1926(getenv func(string) string, envName, field string, target *int) error {
	raw := strings.TrimSpace(getenv(envName))
	if raw == "" {
		return nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fmt.Errorf("%s=%q must be a positive integer for %s", envName, raw, field)
	}
	*target = value
	return nil
}

func applyPositiveColumnGraphScalarU8QuantizedBenchEnvAliases1926(getenv func(string) string, envNames []string, field string, target *int) error {
	for _, envName := range envNames {
		if err := applyPositiveColumnGraphScalarU8QuantizedBenchEnv1926(getenv, envName, field, target); err != nil {
			return err
		}
	}
	return nil
}

func applyNonNegativeColumnGraphScalarU8QuantizedBenchEnv1926(getenv func(string) string, envName, field string, target *int) error {
	raw := strings.TrimSpace(getenv(envName))
	if raw == "" {
		return nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return fmt.Errorf("%s=%q must be a non-negative integer for %s", envName, raw, field)
	}
	*target = value
	return nil
}

func columnGraphScalarU8QuantizedBenchInsertBatchSize1926(shape columnGraphScalarU8QuantizedBenchShape1926) int {
	if shape.rows <= 1024 && shape.dims <= 128 {
		return max(shape.rows, 1)
	}
	return 64
}

func insertColumnGraphScalarU8QuantizedBenchRows1926(tb testing.TB, col *Collection, shape columnGraphScalarU8QuantizedBenchShape1926, rows []columnGraphRebuildInputRowV2A) {
	tb.Helper()
	batchSize := columnGraphScalarU8QuantizedBenchInsertBatchSize1926(shape)
	for start := 0; start < len(rows); start += batchSize {
		end := min(start+batchSize, len(rows))
		ids := make([][]byte, end-start)
		docs := make([][]byte, end-start)
		for i, row := range rows[start:end] {
			raw, err := json.Marshal(map[string]any{
				"time_us":   int64(start + i + 1),
				"kind":      "vector",
				"did":       row.id,
				"embedding": row.vector,
			})
			if err != nil {
				tb.Fatalf("json.Marshal row %q: %v", row.id, err)
			}
			ids[i] = []byte(row.id)
			docs[i] = raw
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			tb.Fatalf("InsertBatch rows [%d,%d): %v", start, end, err)
		}
	}
	if err := col.Flush(); err != nil {
		tb.Fatalf("Flush: %v", err)
	}
}

func TestColumnGraphScalarU8QuantizedBenchShapeFromEnvValues1926(t *testing.T) {
	defaultShape := defaultColumnGraphScalarU8QuantizedBenchShape1926()
	cases := []struct {
		name    string
		env     map[string]string
		want    columnGraphScalarU8QuantizedBenchShape1926
		wantErr string
	}{
		{name: "default", want: defaultShape},
		{name: "named 10k x 768", env: map[string]string{columnGraphScalarU8QuantizedBenchShapeEnv1926: "10k_x_768"}, want: columnGraphScalarU8QuantizedBenchShape1926{rows: 10000, dims: 768, m: 16, efConstruction: 128, topK: 10, efSearch: 128, queryOrdinal: 37, queryCount: 1}},
		{name: "named 10k x 1536", env: map[string]string{columnGraphScalarU8QuantizedBenchShapeEnv1926: "10k_x_1536"}, want: columnGraphScalarU8QuantizedBenchShape1926{rows: 10000, dims: 1536, m: 16, efConstruction: 128, topK: 10, efSearch: 128, queryOrdinal: 37, queryCount: 1}},
		{name: "generic shape", env: map[string]string{columnGraphScalarU8QuantizedBenchShapeEnv1926: "2048x256"}, want: columnGraphScalarU8QuantizedBenchShape1926{rows: 2048, dims: 256, m: 16, efConstruction: 128, topK: 10, efSearch: 128, queryOrdinal: 37, queryCount: 1}},
		{name: "explicit overrides", env: map[string]string{
			columnGraphScalarU8QuantizedBenchShapeEnv1926:          "10k_x_1536",
			columnGraphScalarU8QuantizedBenchRowsEnv1926:           "4096",
			columnGraphScalarU8QuantizedBenchDimsEnv1926:           "384",
			columnGraphScalarU8QuantizedBenchMEnv1926:              "12",
			columnGraphScalarU8QuantizedBenchEfConstructionEnv1926: "144",
			columnGraphScalarU8QuantizedBenchTopKEnv1926:           "7",
			columnGraphScalarU8QuantizedBenchEfSearchEnv1926:       "96",
			columnGraphScalarU8QuantizedBenchQueryOrdinalEnv1926:   "5",
			columnGraphScalarU8QuantizedBenchQueriesEnv1926:        "3",
		}, want: columnGraphScalarU8QuantizedBenchShape1926{rows: 4096, dims: 384, m: 12, efConstruction: 144, topK: 7, efSearch: 96, queryOrdinal: 5, queryCount: 3}},
		{name: "standard vector env", env: map[string]string{
			columnGraphScalarU8QuantizedBenchVectorDocsEnv2591:           "10000",
			columnGraphScalarU8QuantizedBenchVectorDimsEnv2591:           "768",
			columnGraphScalarU8QuantizedBenchVectorMEnv2591:              "16",
			columnGraphScalarU8QuantizedBenchVectorEfConstructionEnv2591: "128",
			columnGraphScalarU8QuantizedBenchVectorTopKEnv2591:           "10",
			columnGraphScalarU8QuantizedBenchVectorEfSearchEnv2591:       "128",
			columnGraphScalarU8QuantizedBenchVectorQueriesEnv2591:        "16",
		}, want: columnGraphScalarU8QuantizedBenchShape1926{rows: 10000, dims: 768, m: 16, efConstruction: 128, topK: 10, efSearch: 128, queryOrdinal: 37, queryCount: 16}},
		{name: "bad shape", env: map[string]string{columnGraphScalarU8QuantizedBenchShapeEnv1926: "wide"}, wantErr: "unsupported"},
		{name: "ef construction below m", env: map[string]string{columnGraphScalarU8QuantizedBenchMEnv1926: "16", columnGraphScalarU8QuantizedBenchEfConstructionEnv1926: "8"}, wantErr: "efConstruction=8 must be >= m=16"},
		{name: "topk exceeds rows", env: map[string]string{columnGraphScalarU8QuantizedBenchRowsEnv1926: "4", columnGraphScalarU8QuantizedBenchTopKEnv1926: "5"}, wantErr: "topK=5 exceeds rows=4"},
		{name: "query ordinal out of range", env: map[string]string{columnGraphScalarU8QuantizedBenchRowsEnv1926: "4", columnGraphScalarU8QuantizedBenchTopKEnv1926: "3", columnGraphScalarU8QuantizedBenchQueryOrdinalEnv1926: "4"}, wantErr: "queryOrdinal=4 out of range rows=4"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := columnGraphScalarU8QuantizedBenchShapeFromEnvValues1926(func(key string) string { return tc.env[key] })
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err=%v want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tc.want {
				t.Fatalf("shape=%+v want %+v", got, tc.want)
			}
		})
	}
}

func TestColumnGraphQuantizedProductionBenchShapeFromEnvValues2591(t *testing.T) {
	shape, err := columnGraphQuantizedProductionBenchShapeFromEnvValues2591(func(key string) string { return "" })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := defaultColumnGraphQuantizedProductionBenchShape2591()
	if shape != want {
		t.Fatalf("production shape=%+v want %+v", shape, want)
	}
	shape, err = columnGraphQuantizedProductionBenchShapeFromEnvValues2591(func(key string) string {
		switch key {
		case columnGraphScalarU8QuantizedBenchVectorDocsEnv2591:
			return "4"
		case columnGraphScalarU8QuantizedBenchVectorTopKEnv2591:
			return "4"
		case columnGraphScalarU8QuantizedBenchQueryOrdinalEnv1926:
			return "0"
		case columnGraphScalarU8QuantizedBenchVectorQueriesEnv2591:
			return "16"
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatalf("unexpected clamp err: %v", err)
	}
	if shape.rows != 4 || shape.queryCount != 4 {
		t.Fatalf("clamped production shape=%+v want rows=4 queryCount=4", shape)
	}
}

func TestColumnGraphScalarU8QuantizedBenchQueriesHonorMultiQueryOrdinal2591(t *testing.T) {
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 8, dims: 2, m: 4, efConstruction: 4, topK: 2, efSearch: 4, queryOrdinal: 3, queryCount: 3}
	rows := make([]columnGraphRebuildInputRowV2A, shape.rows)
	for i := range rows {
		rows[i] = columnGraphRebuildInputRowV2A{id: fmt.Sprintf("doc-%d", i), vector: []float32{float32(i), float32(i + 1)}}
	}

	queries, ordinals := columnGraphScalarU8QuantizedBenchQueries1926(t, shape, rows)
	wantOrdinals := []int{3, 2, 1}
	if !reflect.DeepEqual(ordinals, wantOrdinals) {
		t.Fatalf("query ordinals=%v want %v", ordinals, wantOrdinals)
	}
	for i, ordinal := range wantOrdinals {
		if !reflect.DeepEqual(queries[i], rows[ordinal].vector) {
			t.Fatalf("query %d=%v want row %d vector %v", i, queries[i], ordinal, rows[ordinal].vector)
		}
	}
}

func TestColumnGraphQuantizedProductionGateRouteAssertions2591(t *testing.T) {
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 128, dims: 32, m: 16, efConstruction: 128, topK: 10, efSearch: 64, queryOrdinal: 0, queryCount: 4}

	exactFixture := openColumnGraphScalarU8QuantizedBenchFixture1926(t, shape, false)
	defer exactFixture.close()
	query := exactFixture.queries[0]
	searcher, err := exactFixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: exactFixture.definition.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher exact: %v", err)
	}
	var buffer VectorIndexSearchBuffer
	exact, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeExact, TopK: shape.topK, EfSearch: shape.efSearch, StatsMode: VectorIndexSearchStatsModeFullDiagnostics}, &buffer)
	_ = searcher.Close()
	if err != nil || len(exact.Results) == 0 {
		t.Fatalf("exact SearchWithBuffer results=%d err=%v", len(exact.Results), err)
	}
	assertColumnGraphQuantizedProductionExactSearchStats2591(t, exact.Stats)
	collectionExact, err := exactFixture.collection.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: exactFixture.definition.Name, Query: query, QueryMode: VectorIndexQueryModeExact, TopK: shape.topK, EfSearch: shape.efSearch, MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeFullDiagnostics}, &buffer)
	if err != nil || len(collectionExact.Results) == 0 {
		t.Fatalf("exact collection SearchVectorIndexWithBuffer results=%d err=%v", len(collectionExact.Results), err)
	}
	assertColumnGraphQuantizedProductionExactSearchStats2591(t, collectionExact.Stats)

	scalarFixture := openColumnGraphScalarU8QuantizedBenchFixture1926(t, shape, true)
	defer scalarFixture.close()
	query = scalarFixture.queries[0]
	searcher, err = scalarFixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: scalarFixture.definition.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher scalar_u8: %v", err)
	}
	buffer.Reset()
	scalarOnlyOpts := VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926, TopK: shape.topK, EfSearch: shape.efSearch, StatsMode: VectorIndexSearchStatsModeFullDiagnostics}
	scalarOnly, err := searcher.SearchWithBuffer(scalarOnlyOpts, &buffer)
	_ = searcher.Close()
	if err != nil || len(scalarOnly.Results) == 0 {
		t.Fatalf("scalar_u8 SearchWithBuffer results=%d err=%v", len(scalarOnly.Results), err)
	}
	assertColumnGraphScalarU8QuantizedSearchWithBufferGuardrails2414(t, scalarOnly.Stats, scalarOnlyOpts, scalarFixture.definition.Dimensions)
	buffer.Reset()
	scalarRerankOpts := VectorIndexSearchOptions{IndexName: scalarFixture.definition.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926, QuantizedRerankCandidates: 32, TopK: shape.topK, EfSearch: shape.efSearch, MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeFullDiagnostics}
	scalarRerank, err := scalarFixture.collection.SearchVectorIndexWithBuffer(scalarRerankOpts, &buffer)
	if err != nil || len(scalarRerank.Results) == 0 {
		t.Fatalf("scalar_u8 collection rerank results=%d err=%v", len(scalarRerank.Results), err)
	}
	assertColumnGraphScalarU8QuantizedCollectionWithBufferGuardrails2415(t, scalarRerank.Stats, scalarRerankOpts, scalarFixture.definition.Dimensions)

	alphaFixture := openColumnGraphScalarU8AlphaQuantizedBenchFixture2845(t, shape)
	defer alphaFixture.close()
	query = alphaFixture.queries[0]
	searcher, err = alphaFixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: alphaFixture.definition.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher scalar_u8 alpha: %v", err)
	}
	buffer.Reset()
	alphaOnlyOpts := VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: columnGraphScalarU8AlphaQuantizedBenchIndexName2845, TopK: shape.topK, EfSearch: shape.efSearch, StatsMode: VectorIndexSearchStatsModeFullDiagnostics}
	alphaOnly, err := searcher.SearchWithBuffer(alphaOnlyOpts, &buffer)
	_ = searcher.Close()
	if err != nil || len(alphaOnly.Results) == 0 {
		t.Fatalf("scalar_u8 alpha SearchWithBuffer results=%d err=%v", len(alphaOnly.Results), err)
	}
	assertColumnGraphScalarU8AlphaQuantizedSearchWithBufferGuardrails2845(t, alphaOnly.Stats, alphaOnlyOpts, alphaFixture.definition.Dimensions)
	buffer.Reset()
	alphaRerankOpts := VectorIndexSearchOptions{IndexName: alphaFixture.definition.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: columnGraphScalarU8AlphaQuantizedBenchIndexName2845, QuantizedRerankCandidates: 32, TopK: shape.topK, EfSearch: shape.efSearch, MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeFullDiagnostics}
	alphaRerank, err := alphaFixture.collection.SearchVectorIndexWithBuffer(alphaRerankOpts, &buffer)
	if err != nil || len(alphaRerank.Results) == 0 {
		t.Fatalf("scalar_u8 alpha collection rerank results=%d err=%v", len(alphaRerank.Results), err)
	}
	assertColumnGraphScalarU8AlphaQuantizedCollectionWithBufferGuardrails2845(t, alphaRerank.Stats, alphaRerankOpts, alphaFixture.definition.Dimensions)

	rabitqFixture := openColumnGraphRabitQQuantizedBenchFixture2451(t, shape)
	defer rabitqFixture.close()
	query = rabitqFixture.queries[0]
	searcher, err = rabitqFixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: rabitqFixture.definition.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher rabitq_1bit: %v", err)
	}
	buffer.Reset()
	rabitqOnlyOpts := VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: columnGraphRabitQQuantizedIndexName2450, TopK: shape.topK, EfSearch: shape.efSearch, StatsMode: VectorIndexSearchStatsModeFullDiagnostics}
	rabitqOnly, err := searcher.SearchWithBuffer(rabitqOnlyOpts, &buffer)
	_ = searcher.Close()
	if err != nil || len(rabitqOnly.Results) == 0 {
		t.Fatalf("rabitq_1bit SearchWithBuffer results=%d err=%v", len(rabitqOnly.Results), err)
	}
	assertColumnGraphRabitQQuantizedSearchWithBufferGuardrails2451(t, rabitqOnly.Stats, rabitqOnlyOpts, rabitqFixture.definition.Dimensions, rabitqFixture.quantizedCodeBytesPerVector)
	buffer.Reset()
	rabitqRerankOpts := VectorIndexSearchOptions{IndexName: rabitqFixture.definition.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: columnGraphRabitQQuantizedIndexName2450, QuantizedRerankCandidates: 32, TopK: shape.topK, EfSearch: shape.efSearch, MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeFullDiagnostics}
	rabitqRerank, err := rabitqFixture.collection.SearchVectorIndexWithBuffer(rabitqRerankOpts, &buffer)
	if err != nil || len(rabitqRerank.Results) == 0 {
		t.Fatalf("rabitq_1bit collection rerank results=%d err=%v", len(rabitqRerank.Results), err)
	}
	assertCollectionBufferedRabitQQuantizedRouteStats2452(t, rabitqRerank.Stats, columnVectorGraphNativeSearchQueryModeQuantizedRerank, rabitqRerankOpts, rabitqFixture.definition.Dimensions, rabitqFixture.quantizedCodeBytesPerVector)
}

func TestColumnGraphQuantizedProductionGateMissingAssetFailClosed2591(t *testing.T) {
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 64, dims: 16, m: 16, efConstruction: 128, topK: 5, efSearch: 32, queryOrdinal: 0, queryCount: 1}
	fixture := openColumnGraphScalarU8QuantizedBenchFixture1926(t, shape, false)
	defer fixture.close()
	searcher, err := fixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.definition.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	var buffer VectorIndexSearchBuffer
	got, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: fixture.query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926, TopK: shape.topK, EfSearch: shape.efSearch, StatsMode: VectorIndexSearchStatsModeFullDiagnostics}, &buffer)
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) {
		t.Fatalf("missing quantized SearchWithBuffer err=%v want ErrVectorIndexSearchUnavailable", err)
	}
	if len(got.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("missing quantized results=%d buffer.results=%d idBytes=%d want reset empty views", len(got.Results), len(buffer.results), len(buffer.idBytes))
	}
	assertQuantizedUnavailableGuardrailStats2416(t, got.Stats, columnVectorGraphNativeSearchQueryModeQuantizedOnly, columnVectorGraphQuantizedAssetHealthMissing)
}

func TestColumnGraphScalarU8QuantizedBenchInsertBatchSize1926(t *testing.T) {
	if got := columnGraphScalarU8QuantizedBenchInsertBatchSize1926(defaultColumnGraphScalarU8QuantizedBenchShape1926()); got != 1024 {
		t.Fatalf("default batch size=%d want 1024", got)
	}
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 10000, dims: 1536, m: 16, efConstruction: 128, topK: 10, efSearch: 128, queryOrdinal: 37, queryCount: 1}
	if got := columnGraphScalarU8QuantizedBenchInsertBatchSize1926(shape); got != 64 {
		t.Fatalf("large-shape batch size=%d want 64", got)
	}
}

type columnGraphScalarU8QuantizedBenchFixture1926 struct {
	close                       func()
	collection                  *Collection
	definition                  VectorIndexDefinition
	query                       []float32
	queries                     [][]float32
	queryOrdinals               []int
	shape                       columnGraphScalarU8QuantizedBenchShape1926
	quantizedAssetBytes         int64
	quantizedCodeAssetBytes     int64
	quantizedAlphaAssetBytes    int64
	quantizedCodeBytesPerVector int
	scalarU8Alpha               bool
}

func columnGraphScalarU8AlphaQuantizedBenchIndexDefinition2845() QuantizedVectorIndexDefinition {
	return QuantizedVectorIndexDefinition{
		Name:  columnGraphScalarU8AlphaQuantizedBenchIndexName2845,
		Codec: QuantizedVectorCodecScalarU8,
		ScalarU8Calibration: &ScalarU8CalibrationConfig{
			Mode:     ScalarU8CalibrationModePerGranuleAlpha,
			Grouping: ScalarU8CalibrationGroupingStorageLayoutGranule,
			AlphaPolicy: ScalarU8AlphaPolicy{
				Name: ScalarU8AlphaPolicyMaxAbs,
			},
		},
	}
}

type columnGraphScalarU8QuantizedSearchWithBufferBenchCase2414 struct {
	name             string
	mode             VectorIndexQueryMode
	rerankCandidates int
	concurrency      int
}

func columnGraphScalarU8QuantizedSearchWithBufferBenchCases2414() []columnGraphScalarU8QuantizedSearchWithBufferBenchCase2414 {
	return []columnGraphScalarU8QuantizedSearchWithBufferBenchCase2414{
		{name: "route=quantized_only/c=1", mode: VectorIndexQueryModeQuantizedOnly, concurrency: 1},
		{name: "route=quantized_only/c=8", mode: VectorIndexQueryModeQuantizedOnly, concurrency: 8},
		{name: "route=quantized_rerank/candidates=32/c=1", mode: VectorIndexQueryModeQuantizedRerank, rerankCandidates: 32, concurrency: 1},
		{name: "route=quantized_rerank/candidates=32/c=8", mode: VectorIndexQueryModeQuantizedRerank, rerankCandidates: 32, concurrency: 8},
	}
}

type columnGraphScalarU8QuantizedCollectionWithBufferBenchCase2415 struct {
	name             string
	mode             VectorIndexQueryMode
	rerankCandidates int
	concurrency      int
}

func columnGraphScalarU8QuantizedCollectionWithBufferBenchCases2415() []columnGraphScalarU8QuantizedCollectionWithBufferBenchCase2415 {
	return []columnGraphScalarU8QuantizedCollectionWithBufferBenchCase2415{
		{name: "route=quantized_only/c=1", mode: VectorIndexQueryModeQuantizedOnly, concurrency: 1},
		{name: "route=quantized_only/c=8", mode: VectorIndexQueryModeQuantizedOnly, concurrency: 8},
		{name: "route=quantized_rerank/candidates=32/c=1", mode: VectorIndexQueryModeQuantizedRerank, rerankCandidates: 32, concurrency: 1},
		{name: "route=quantized_rerank/candidates=32/c=8", mode: VectorIndexQueryModeQuantizedRerank, rerankCandidates: 32, concurrency: 8},
	}
}

func columnGraphRabitQQuantizedCollectionWithBufferBenchCases2452() []columnGraphScalarU8QuantizedCollectionWithBufferBenchCase2415 {
	return columnGraphScalarU8QuantizedCollectionWithBufferBenchCases2415()
}

func BenchmarkCollectionVectorQuantizedProductionGate2591(b *testing.B) {
	shape := columnGraphQuantizedProductionBenchShapeFromEnv2591(b)

	b.Run("exact_fp32", func(b *testing.B) {
		fixture := openColumnGraphScalarU8QuantizedBenchFixture1926(b, shape, false)
		defer fixture.close()
		exactSets := columnGraphScalarU8QuantizedBenchmarkExactSets1926(b, fixture)
		for _, concurrency := range []int{1, 8} {
			concurrency := concurrency
			b.Run(fmt.Sprintf("SearchWithBuffer/c=%d", concurrency), func(b *testing.B) {
				runColumnGraphQuantizedProductionExactSearchWithBuffer2591(b, fixture, concurrency, exactSets)
			})
			b.Run(fmt.Sprintf("CollectionSearchVectorIndexWithBuffer/c=%d", concurrency), func(b *testing.B) {
				runColumnGraphQuantizedProductionExactCollectionWithBuffer2591(b, fixture, concurrency, exactSets)
			})
		}
	})

	b.Run("scalar_u8", func(b *testing.B) {
		fixture := openColumnGraphScalarU8QuantizedBenchFixture1926(b, shape, true)
		defer fixture.close()
		exactSets := columnGraphScalarU8QuantizedBenchmarkExactSets1926(b, fixture)
		for _, tc := range columnGraphScalarU8QuantizedSearchWithBufferBenchCases2414() {
			tc := tc
			b.Run("SearchWithBuffer/"+tc.name, func(b *testing.B) {
				opts := VectorIndexSearcherSearchOptions{
					QueryMode:                 tc.mode,
					QuantizedIndexName:        columnGraphScalarU8QuantizedBenchIndexName1926,
					QuantizedRerankCandidates: tc.rerankCandidates,
					TopK:                      fixture.shape.topK,
					EfSearch:                  fixture.shape.efSearch,
					StatsMode:                 VectorIndexSearchStatsModeProduction,
				}
				runColumnGraphQuantizedProductionSearchWithBuffer2591(b, fixture, opts, tc.concurrency, exactSets, func(tb testing.TB, stats VectorIndexSearchStats, opts VectorIndexSearcherSearchOptions) {
					assertColumnGraphScalarU8QuantizedSearchWithBufferGuardrails2414(tb, stats, opts, fixture.definition.Dimensions)
				}, "scalar_u8_row", nil)
			})
		}
		for _, tc := range columnGraphScalarU8QuantizedCollectionWithBufferBenchCases2415() {
			tc := tc
			b.Run("CollectionSearchVectorIndexWithBuffer/"+tc.name, func(b *testing.B) {
				opts := VectorIndexSearchOptions{
					IndexName:                 fixture.definition.Name,
					QueryMode:                 tc.mode,
					QuantizedIndexName:        columnGraphScalarU8QuantizedBenchIndexName1926,
					QuantizedRerankCandidates: tc.rerankCandidates,
					TopK:                      fixture.shape.topK,
					EfSearch:                  fixture.shape.efSearch,
					MaxDecodedBlocks:          1,
					StatsMode:                 VectorIndexSearchStatsModeProduction,
				}
				runColumnGraphQuantizedProductionCollectionWithBuffer2591(b, fixture, opts, tc.concurrency, exactSets, func(tb testing.TB, stats VectorIndexSearchStats, opts VectorIndexSearchOptions) {
					assertColumnGraphScalarU8QuantizedCollectionWithBufferGuardrails2415(tb, stats, opts, fixture.definition.Dimensions)
				}, "scalar_u8_row", nil)
			})
		}
	})

	b.Run("scalar_u8_per_granule_alpha", func(b *testing.B) {
		fixture := openColumnGraphScalarU8AlphaQuantizedBenchFixture2845(b, shape)
		defer fixture.close()
		exactSets := columnGraphScalarU8QuantizedBenchmarkExactSets1926(b, fixture)
		for _, tc := range columnGraphScalarU8QuantizedSearchWithBufferBenchCases2414() {
			tc := tc
			b.Run("SearchWithBuffer/"+tc.name, func(b *testing.B) {
				opts := VectorIndexSearcherSearchOptions{
					QueryMode:                 tc.mode,
					QuantizedIndexName:        columnGraphScalarU8AlphaQuantizedBenchIndexName2845,
					QuantizedRerankCandidates: tc.rerankCandidates,
					TopK:                      fixture.shape.topK,
					EfSearch:                  fixture.shape.efSearch,
					StatsMode:                 VectorIndexSearchStatsModeProduction,
				}
				runColumnGraphQuantizedProductionSearchWithBuffer2591(b, fixture, opts, tc.concurrency, exactSets, func(tb testing.TB, stats VectorIndexSearchStats, opts VectorIndexSearcherSearchOptions) {
					assertColumnGraphScalarU8AlphaQuantizedSearchWithBufferGuardrails2845(tb, stats, opts, fixture.definition.Dimensions)
				}, "scalar_u8_alpha_row", nil)
			})
		}
		for _, tc := range columnGraphScalarU8QuantizedCollectionWithBufferBenchCases2415() {
			tc := tc
			b.Run("CollectionSearchVectorIndexWithBuffer/"+tc.name, func(b *testing.B) {
				opts := VectorIndexSearchOptions{
					IndexName:                 fixture.definition.Name,
					QueryMode:                 tc.mode,
					QuantizedIndexName:        columnGraphScalarU8AlphaQuantizedBenchIndexName2845,
					QuantizedRerankCandidates: tc.rerankCandidates,
					TopK:                      fixture.shape.topK,
					EfSearch:                  fixture.shape.efSearch,
					MaxDecodedBlocks:          1,
					StatsMode:                 VectorIndexSearchStatsModeProduction,
				}
				runColumnGraphQuantizedProductionCollectionWithBuffer2591(b, fixture, opts, tc.concurrency, exactSets, func(tb testing.TB, stats VectorIndexSearchStats, opts VectorIndexSearchOptions) {
					assertColumnGraphScalarU8AlphaQuantizedCollectionWithBufferGuardrails2845(tb, stats, opts, fixture.definition.Dimensions)
				}, "scalar_u8_alpha_row", nil)
			})
		}
	})

	b.Run("rabitq_1bit", func(b *testing.B) {
		hotProfile := newColumnGraphQuantizedSearchLoopProfileHook2541(b)
		defer hotProfile.finish()
		fixture := openColumnGraphRabitQQuantizedBenchFixture2451(b, shape)
		defer fixture.close()
		exactSets := columnGraphScalarU8QuantizedBenchmarkExactSets1926(b, fixture)
		for _, tc := range columnGraphScalarU8QuantizedSearchWithBufferBenchCases2414() {
			tc := tc
			b.Run("SearchWithBuffer/"+tc.name, func(b *testing.B) {
				opts := VectorIndexSearcherSearchOptions{
					QueryMode:                 tc.mode,
					QuantizedIndexName:        columnGraphRabitQQuantizedIndexName2450,
					QuantizedRerankCandidates: tc.rerankCandidates,
					TopK:                      fixture.shape.topK,
					EfSearch:                  fixture.shape.efSearch,
					StatsMode:                 VectorIndexSearchStatsModeProduction,
				}
				runColumnGraphQuantizedProductionSearchWithBuffer2591(b, fixture, opts, tc.concurrency, exactSets, func(tb testing.TB, stats VectorIndexSearchStats, opts VectorIndexSearcherSearchOptions) {
					assertColumnGraphRabitQQuantizedSearchWithBufferGuardrails2451(tb, stats, opts, fixture.definition.Dimensions, fixture.quantizedCodeBytesPerVector)
				}, "rabitq_1bit_row", hotProfile)
			})
		}
		for _, tc := range columnGraphRabitQQuantizedCollectionWithBufferBenchCases2452() {
			tc := tc
			b.Run("CollectionSearchVectorIndexWithBuffer/"+tc.name, func(b *testing.B) {
				opts := VectorIndexSearchOptions{
					IndexName:                 fixture.definition.Name,
					QueryMode:                 tc.mode,
					QuantizedIndexName:        columnGraphRabitQQuantizedIndexName2450,
					QuantizedRerankCandidates: tc.rerankCandidates,
					TopK:                      fixture.shape.topK,
					EfSearch:                  fixture.shape.efSearch,
					MaxDecodedBlocks:          1,
					StatsMode:                 VectorIndexSearchStatsModeProduction,
				}
				runColumnGraphQuantizedProductionCollectionWithBuffer2591(b, fixture, opts, tc.concurrency, exactSets, func(tb testing.TB, stats VectorIndexSearchStats, opts VectorIndexSearchOptions) {
					assertCollectionBufferedRabitQQuantizedRouteStats2452(tb, stats, columnVectorGraphNativeSearchQueryModeFromPublic2415(opts.QueryMode), opts, fixture.definition.Dimensions, fixture.quantizedCodeBytesPerVector)
				}, "rabitq_1bit_row", hotProfile)
			})
		}
	})
}

type columnGraphQuantizedProductionSearchWorker2591 struct {
	searcher *VectorIndexSearcher
	buffer   VectorIndexSearchBuffer
	stats    VectorIndexSearchStats
	sink     int64
}

func runColumnGraphQuantizedProductionExactSearchWithBuffer2591(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, concurrency int, exactSets []columnGraphScalarU8QuantizedBenchmarkExactSet1926) {
	b.Helper()
	if concurrency <= 0 {
		b.Fatalf("concurrency=%d must be positive", concurrency)
	}
	queries := columnGraphScalarU8QuantizedBenchFixtureQueries1926(fixture)
	workers := make([]columnGraphQuantizedProductionSearchWorker2591, concurrency)
	baseOpts := VectorIndexSearcherSearchOptions{QueryMode: VectorIndexQueryModeExact, TopK: fixture.shape.topK, EfSearch: fixture.shape.efSearch, StatsMode: VectorIndexSearchStatsModeProduction}
	for i := range workers {
		searcher, err := fixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.definition.Name, MaxDecodedBlocks: 1})
		if err != nil {
			b.Fatalf("OpenVectorIndexSearcher exact worker %d: %v", i, err)
		}
		defer func(searcher *VectorIndexSearcher) { _ = searcher.Close() }(searcher)
		workers[i].searcher = searcher
		warmOpts := baseOpts
		warmOpts.Query = queries[i%len(queries)]
		warm, err := searcher.SearchWithBuffer(warmOpts, &workers[i].buffer)
		if err != nil {
			b.Fatalf("warm exact SearchWithBuffer worker %d: %v", i, err)
		}
		if len(warm.Results) == 0 {
			b.Fatalf("warm exact SearchWithBuffer worker %d returned no results", i)
		}
		assertColumnGraphQuantizedProductionExactSearchStats2591(b, warm.Stats)
	}
	warmRecall := columnGraphQuantizedProductionWarmRecallSearchWithBuffer2591(b, workers[0].searcher, &workers[0].buffer, baseOpts, queries, exactSets, func(tb testing.TB, stats VectorIndexSearchStats, _ VectorIndexSearcherSearchOptions) {
		assertColumnGraphQuantizedProductionExactSearchStats2591(tb, stats)
	})
	runColumnGraphQuantizedProductionSearchWithBufferLoop2591(b, fixture, workers, baseOpts, queries, exactSets, warmRecall, "exact_fp32_row", nil)
}

func runColumnGraphQuantizedProductionSearchWithBuffer2591(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, opts VectorIndexSearcherSearchOptions, concurrency int, exactSets []columnGraphScalarU8QuantizedBenchmarkExactSet1926, assertStats func(testing.TB, VectorIndexSearchStats, VectorIndexSearcherSearchOptions), rowMetric string, hotProfile *columnGraphQuantizedSearchLoopProfileHook2541) {
	b.Helper()
	if concurrency <= 0 {
		b.Fatalf("concurrency=%d must be positive", concurrency)
	}
	queries := columnGraphScalarU8QuantizedBenchFixtureQueries1926(fixture)
	workers := make([]columnGraphQuantizedProductionSearchWorker2591, concurrency)
	for i := range workers {
		searcher, err := fixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.definition.Name, MaxDecodedBlocks: 1})
		if err != nil {
			b.Fatalf("OpenVectorIndexSearcher quantized worker %d: %v", i, err)
		}
		defer func(searcher *VectorIndexSearcher) { _ = searcher.Close() }(searcher)
		workers[i].searcher = searcher
		warmOpts := opts
		warmOpts.Query = queries[i%len(queries)]
		warm, err := searcher.SearchWithBuffer(warmOpts, &workers[i].buffer)
		if err != nil {
			b.Fatalf("warm quantized SearchWithBuffer worker %d: %v", i, err)
		}
		if len(warm.Results) == 0 {
			b.Fatalf("warm quantized SearchWithBuffer worker %d returned no results", i)
		}
		assertStats(b, warm.Stats, warmOpts)
	}
	warmRecall := columnGraphQuantizedProductionWarmRecallSearchWithBuffer2591(b, workers[0].searcher, &workers[0].buffer, opts, queries, exactSets, assertStats)
	runColumnGraphQuantizedProductionSearchWithBufferLoop2591(b, fixture, workers, opts, queries, exactSets, warmRecall, rowMetric, hotProfile)
}

func columnGraphQuantizedProductionWarmRecallSearchWithBuffer2591(b *testing.B, searcher *VectorIndexSearcher, buffer *VectorIndexSearchBuffer, opts VectorIndexSearcherSearchOptions, queries [][]float32, exactSets []columnGraphScalarU8QuantizedBenchmarkExactSet1926, assertStats func(testing.TB, VectorIndexSearchStats, VectorIndexSearcherSearchOptions)) float64 {
	b.Helper()
	var recallSum float64
	statsOpts := opts
	statsOpts.StatsMode = VectorIndexSearchStatsModeFullDiagnostics
	for i, query := range queries {
		statsOpts.Query = query
		response, err := searcher.SearchWithBuffer(statsOpts, buffer)
		if err != nil {
			b.Fatalf("measure SearchWithBuffer stats query %d: %v", i, err)
		}
		if len(response.Results) == 0 {
			b.Fatalf("measure SearchWithBuffer stats query %d returned no results", i)
		}
		assertStats(b, response.Stats, statsOpts)
		recallSum += columnGraphScalarU8QuantizedBenchmarkRecallAtK1926(response.Results, exactSets[i].ids, exactSets[i].count)
	}
	return recallSum / float64(len(queries))
}

func runColumnGraphQuantizedProductionSearchWithBufferLoop2591(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, workers []columnGraphQuantizedProductionSearchWorker2591, opts VectorIndexSearcherSearchOptions, queries [][]float32, exactSets []columnGraphScalarU8QuantizedBenchmarkExactSet1926, warmRecall float64, rowMetric string, hotProfile *columnGraphQuantizedSearchLoopProfileHook2541) {
	b.Helper()
	reportSearchWithBufferProductionGateRowMetrics2591(b, len(workers), rowMetric)
	stopHotProfile := func() {}
	if hotProfile != nil {
		stopHotProfile = hotProfile.start(b)
	}
	hotProfileActive := hotProfile != nil && hotProfile.enabled()
	defer func() {
		if hotProfileActive {
			stopHotProfile()
		}
	}()

	if len(workers) == 1 {
		worker := &workers[0]
		var localStats VectorIndexSearchStats
		var localSink int64
		localOpts := opts
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			localOpts.Query = queries[iteration%len(queries)]
			response, err := worker.searcher.SearchWithBuffer(localOpts, &worker.buffer)
			if err != nil {
				b.Fatalf("SearchWithBuffer: %v", err)
			}
			if len(response.Results) == 0 {
				b.Fatalf("SearchWithBuffer returned no results")
			}
			localSink += int64(response.Results[0].Ordinal)
			addColumnGraphScalarU8QuantizedBenchmarkStats1926(&localStats, response.Stats)
		}
		b.StopTimer()
		stopHotProfile()
		hotProfileActive = false
		worker.stats = localStats
		worker.sink = localSink
	} else {
		var next atomic.Uint64
		var failed atomic.Bool
		var firstErr atomic.Value
		recordErr := func(format string, args ...any) {
			if failed.CompareAndSwap(false, true) {
				firstErr.Store(fmt.Sprintf(format, args...))
			}
		}
		var wg sync.WaitGroup
		ready := make(chan struct{}, len(workers))
		start := make(chan struct{})
		wg.Add(len(workers))
		for i := range workers {
			worker := &workers[i]
			go func() {
				defer wg.Done()
				ready <- struct{}{}
				<-start
				var localStats VectorIndexSearchStats
				var localSink int64
				localOpts := opts
				for {
					iteration := int(next.Add(1)) - 1
					if iteration >= b.N {
						break
					}
					if failed.Load() {
						continue
					}
					localOpts.Query = queries[iteration%len(queries)]
					response, err := worker.searcher.SearchWithBuffer(localOpts, &worker.buffer)
					if err != nil {
						recordErr("SearchWithBuffer: %v", err)
						continue
					}
					if len(response.Results) == 0 {
						recordErr("SearchWithBuffer returned no results")
						continue
					}
					localSink += int64(response.Results[0].Ordinal)
					addColumnGraphScalarU8QuantizedBenchmarkStats1926(&localStats, response.Stats)
				}
				worker.stats = localStats
				worker.sink = localSink
			}()
		}
		for range workers {
			<-ready
		}
		b.ResetTimer()
		close(start)
		wg.Wait()
		b.StopTimer()
		stopHotProfile()
		hotProfileActive = false
		if errValue := firstErr.Load(); errValue != nil {
			b.Fatalf("%s", errValue.(string))
		}
	}

	var stats VectorIndexSearchStats
	var sink int64
	for i := range workers {
		addColumnGraphScalarU8QuantizedBenchmarkStats1926(&stats, workers[i].stats)
		sink += workers[i].sink
	}
	columnPhysicalScanBenchSum += sink
	recallSum := warmRecall * float64(b.N)
	reportColumnGraphScalarU8QuantizedScorePlaneMetrics1926(b, fixture, stats, recallSum)
}

func reportSearchWithBufferProductionGateRowMetrics2591(b *testing.B, concurrency int, rowMetric string) {
	b.Helper()
	b.ReportAllocs()
	b.ReportMetric(float64(concurrency), "concurrency")
	b.ReportMetric(1, "searchwithbuffer_buffered_row")
	b.ReportMetric(1, "reported_stats_mode_production")
	b.ReportMetric(2591, "production_gate_issue")
	if rowMetric != "" {
		b.ReportMetric(1, rowMetric)
	}
}

func runColumnGraphQuantizedProductionExactCollectionWithBuffer2591(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, concurrency int, exactSets []columnGraphScalarU8QuantizedBenchmarkExactSet1926) {
	b.Helper()
	opts := VectorIndexSearchOptions{IndexName: fixture.definition.Name, QueryMode: VectorIndexQueryModeExact, TopK: fixture.shape.topK, EfSearch: fixture.shape.efSearch, MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	runColumnGraphQuantizedProductionCollectionWithBuffer2591(b, fixture, opts, concurrency, exactSets, func(tb testing.TB, stats VectorIndexSearchStats, _ VectorIndexSearchOptions) {
		assertColumnGraphQuantizedProductionExactSearchStats2591(tb, stats)
	}, "exact_fp32_row", nil)
}

func runColumnGraphQuantizedProductionCollectionWithBuffer2591(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, opts VectorIndexSearchOptions, concurrency int, exactSets []columnGraphScalarU8QuantizedBenchmarkExactSet1926, assertStats func(testing.TB, VectorIndexSearchStats, VectorIndexSearchOptions), rowMetric string, hotProfile *columnGraphQuantizedSearchLoopProfileHook2541) {
	b.Helper()
	if concurrency <= 0 {
		b.Fatalf("concurrency=%d must be positive", concurrency)
	}
	queries := columnGraphScalarU8QuantizedBenchFixtureQueries1926(fixture)
	type benchWorker struct {
		buffer VectorIndexSearchBuffer
		stats  VectorIndexSearchStats
		sink   int64
	}
	if err := fixture.collection.closeCollectionVectorIndexPreparedSearchCache(); err != nil {
		b.Fatalf("close collection prepared cache before production gate row: %v", err)
	}
	workers := make([]benchWorker, concurrency)
	for i := range workers {
		warmOpts := opts
		warmOpts.Query = queries[i%len(queries)]
		warm, err := fixture.collection.SearchVectorIndexWithBuffer(warmOpts, &workers[i].buffer)
		if err != nil {
			b.Fatalf("warm SearchVectorIndexWithBuffer worker %d: %v", i, err)
		}
		if len(warm.Results) == 0 {
			b.Fatalf("warm SearchVectorIndexWithBuffer worker %d returned no results", i)
		}
		assertStats(b, warm.Stats, warmOpts)
	}
	statsOpts := opts
	statsOpts.StatsMode = VectorIndexSearchStatsModeFullDiagnostics
	var warmRecallSum float64
	for i, query := range queries {
		statsOpts.Query = query
		measured, err := fixture.collection.SearchVectorIndexWithBuffer(statsOpts, &workers[0].buffer)
		if err != nil {
			b.Fatalf("measure SearchVectorIndexWithBuffer stats query %d: %v", i, err)
		}
		if len(measured.Results) == 0 {
			b.Fatalf("measure SearchVectorIndexWithBuffer stats query %d returned no results", i)
		}
		assertStats(b, measured.Stats, statsOpts)
		warmRecallSum += columnGraphScalarU8QuantizedBenchmarkRecallAtK1926(measured.Results, exactSets[i].ids, exactSets[i].count)
	}
	if opts.QueryMode == VectorIndexQueryModeQuantizedOnly || opts.QueryMode == VectorIndexQueryModeQuantizedRerank {
		ensureColumnGraphScalarU8QuantizedCollectionReaderPool2415(b, fixture.collection, opts, concurrency)
	}
	warmRecall := warmRecallSum / float64(len(queries))
	var next atomic.Uint64
	var sink atomic.Int64
	var failed atomic.Bool
	var firstErr atomic.Value
	recordErr := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			firstErr.Store(fmt.Sprintf(format, args...))
		}
	}
	var wg sync.WaitGroup
	ready := make(chan struct{}, len(workers))
	start := make(chan struct{})
	wg.Add(len(workers))
	for i := range workers {
		worker := &workers[i]
		go func(workerIndex int) {
			defer wg.Done()
			localOpts := opts
			for warmIter := 0; warmIter < 8; warmIter++ {
				localOpts.Query = queries[(workerIndex+warmIter)%len(queries)]
				if warm, err := fixture.collection.SearchVectorIndexWithBuffer(localOpts, &worker.buffer); err != nil {
					recordErr("goroutine warm SearchVectorIndexWithBuffer: %v", err)
					break
				} else if len(warm.Results) == 0 {
					recordErr("goroutine warm SearchVectorIndexWithBuffer returned no results")
					break
				}
			}
			ready <- struct{}{}
			<-start
			var localStats VectorIndexSearchStats
			var localSink int64
			for {
				iteration := int(next.Add(1)) - 1
				if iteration >= b.N {
					break
				}
				if failed.Load() {
					continue
				}
				localOpts.Query = queries[iteration%len(queries)]
				response, err := fixture.collection.SearchVectorIndexWithBuffer(localOpts, &worker.buffer)
				if err != nil {
					recordErr("SearchVectorIndexWithBuffer: %v", err)
					continue
				}
				if len(response.Results) == 0 {
					recordErr("SearchVectorIndexWithBuffer returned no results")
					continue
				}
				localSink += int64(response.Results[0].Ordinal)
				addColumnGraphScalarU8QuantizedBenchmarkStats1926(&localStats, response.Stats)
			}
			worker.stats = localStats
			worker.sink = localSink
		}(i)
	}
	for range workers {
		<-ready
	}
	if errValue := firstErr.Load(); errValue != nil {
		close(start)
		wg.Wait()
		b.Fatalf("%s", errValue.(string))
	}
	cacheBeforeTimed := fixture.collection.collectionVectorIndexPreparedSearchCacheSnapshot()
	b.ReportAllocs()
	b.ReportMetric(float64(concurrency), "concurrency")
	b.ReportMetric(1, "collection_searchvectorindex_with_buffer_seam")
	b.ReportMetric(0, "open_searcher_calls/op")
	b.ReportMetric(0, "open_setup_in_timed_loop")
	b.ReportMetric(1, "reported_stats_mode_production")
	b.ReportMetric(2591, "production_gate_issue")
	if rowMetric != "" {
		b.ReportMetric(1, rowMetric)
	}
	stopHotProfile := func() {}
	if hotProfile != nil {
		stopHotProfile = hotProfile.start(b)
	}
	hotProfileActive := hotProfile != nil && hotProfile.enabled()
	defer func() {
		if hotProfileActive {
			stopHotProfile()
		}
	}()
	b.ResetTimer()
	close(start)
	wg.Wait()
	b.StopTimer()
	stopHotProfile()
	hotProfileActive = false
	if errValue := firstErr.Load(); errValue != nil {
		b.Fatalf("%s", errValue.(string))
	}
	var stats VectorIndexSearchStats
	for i := range workers {
		addColumnGraphScalarU8QuantizedBenchmarkStats1926(&stats, workers[i].stats)
		sink.Add(workers[i].sink)
	}
	columnPhysicalScanBenchSum += sink.Load()
	recallSum := warmRecall * float64(b.N)
	reportCollectionVectorIndexPreparedSearchBenchMetrics2415(b, cacheBeforeTimed, fixture.collection.collectionVectorIndexPreparedSearchCacheSnapshot())
	reportColumnGraphScalarU8QuantizedScorePlaneMetrics1926(b, fixture, stats, recallSum)
}

func assertColumnGraphQuantizedProductionExactSearchStats2591(tb testing.TB, stats VectorIndexSearchStats) {
	tb.Helper()
	if stats.SearchRouteHNSWSearchPack != 1 || stats.HNSWSearchPackActive != 1 || stats.HNSWSearchPackFallbacks != 0 {
		tb.Fatalf("exact production route stats=%+v want active hnsw_search_pack_v1", stats)
	}
	if stats.SearchRouteColumnGraphPrepared+stats.SearchRouteColumnGraphFallback != 0 || stats.SearchRouteQuantizedOnly+stats.SearchRouteQuantizedRerank != 0 || stats.QuantizedScorerActive != 0 {
		tb.Fatalf("exact production route stats=%+v want exact pack only", stats)
	}
	if stats.DocumentsFetched != 0 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 {
		tb.Fatalf("exact production buffered stats=%+v want no docs/materialization/fallback/scratch", stats)
	}
}

func BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926(b *testing.B) {
	shape := columnGraphScalarU8QuantizedBenchShapeFromEnv1926(b)
	fixture := openColumnGraphScalarU8QuantizedBenchFixture1926(b, shape, true)
	defer fixture.close()
	exactIDs, exactCount := columnGraphScalarU8QuantizedBenchmarkExactIDs1926(b, fixture)

	cases := []struct {
		name               string
		mode               VectorIndexQueryMode
		rerankCandidates   int
		quantizedIndexName string
	}{
		{name: "mode=exact", mode: VectorIndexQueryModeExact},
		{name: "mode=quantized_only", mode: VectorIndexQueryModeQuantizedOnly, quantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926},
		{name: "mode=quantized_rerank/candidates=10", mode: VectorIndexQueryModeQuantizedRerank, quantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926, rerankCandidates: 10},
		{name: "mode=quantized_rerank/candidates=32", mode: VectorIndexQueryModeQuantizedRerank, quantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926, rerankCandidates: 32},
		{name: "mode=quantized_rerank/candidates=128", mode: VectorIndexQueryModeQuantizedRerank, quantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926, rerankCandidates: 128},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			searcher, err := fixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.definition.Name, MaxDecodedBlocks: 1})
			if err != nil {
				b.Fatalf("OpenVectorIndexSearcher: %v", err)
			}
			defer func() { _ = searcher.Close() }()
			opts := VectorIndexSearcherSearchOptions{
				Query:                     fixture.query,
				QueryMode:                 tc.mode,
				QuantizedIndexName:        tc.quantizedIndexName,
				QuantizedRerankCandidates: tc.rerankCandidates,
				TopK:                      fixture.shape.topK,
				EfSearch:                  fixture.shape.efSearch,
			}
			var buffer VectorIndexSearchBuffer
			warm, err := searcher.SearchWithBuffer(opts, &buffer)
			if err != nil {
				b.Fatalf("warm SearchWithBuffer: %v", err)
			}
			if len(warm.Results) == 0 {
				b.Fatalf("warm SearchWithBuffer returned no results")
			}

			var stats VectorIndexSearchStats
			var recallSum float64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				response, err := searcher.SearchWithBuffer(opts, &buffer)
				if err != nil {
					b.Fatalf("SearchWithBuffer: %v", err)
				}
				if len(response.Results) == 0 {
					b.Fatalf("SearchWithBuffer returned no results")
				}
				columnPhysicalScanBenchSum += int64(response.Results[0].Ordinal)
				addColumnGraphScalarU8QuantizedBenchmarkStats1926(&stats, response.Stats)
				recallSum += columnGraphScalarU8QuantizedBenchmarkRecallAtK1926(response.Results, exactIDs, exactCount)
			}
			b.StopTimer()
			reportColumnGraphScalarU8QuantizedScorePlaneMetrics1926(b, fixture, stats, recallSum)
		})
	}
}

func BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414(b *testing.B) {
	shape := columnGraphScalarU8QuantizedBenchShapeFromEnv1926(b)
	fixture := openColumnGraphScalarU8QuantizedBenchFixture1926(b, shape, true)
	defer fixture.close()
	exactIDs, exactCount := columnGraphScalarU8QuantizedBenchmarkExactIDs1926(b, fixture)

	for _, tc := range columnGraphScalarU8QuantizedSearchWithBufferBenchCases2414() {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			opts := VectorIndexSearcherSearchOptions{
				Query:                     fixture.query,
				QueryMode:                 tc.mode,
				QuantizedIndexName:        columnGraphScalarU8QuantizedBenchIndexName1926,
				QuantizedRerankCandidates: tc.rerankCandidates,
				TopK:                      fixture.shape.topK,
				EfSearch:                  fixture.shape.efSearch,
				StatsMode:                 VectorIndexSearchStatsModeProduction,
			}
			runColumnGraphScalarU8QuantizedSearchWithBufferBench2414(b, fixture, opts, tc.concurrency, exactIDs, exactCount)
		})
	}
}

func BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedAlphaSearchWithBuffer2414(b *testing.B) {
	shape := columnGraphScalarU8QuantizedBenchShapeFromEnv1926(b)
	fixture := openColumnGraphScalarU8AlphaQuantizedBenchFixture2845(b, shape)
	defer fixture.close()
	exactIDs, exactCount := columnGraphScalarU8QuantizedBenchmarkExactIDs1926(b, fixture)

	for _, tc := range columnGraphScalarU8QuantizedSearchWithBufferBenchCases2414() {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			opts := VectorIndexSearcherSearchOptions{
				Query:                     fixture.query,
				QueryMode:                 tc.mode,
				QuantizedIndexName:        columnGraphScalarU8AlphaQuantizedBenchIndexName2845,
				QuantizedRerankCandidates: tc.rerankCandidates,
				TopK:                      fixture.shape.topK,
				EfSearch:                  fixture.shape.efSearch,
				StatsMode:                 VectorIndexSearchStatsModeProduction,
			}
			runColumnGraphScalarU8AlphaQuantizedSearchWithBufferBench2845(b, fixture, opts, tc.concurrency, exactIDs, exactCount)
		})
	}
}

func BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451(b *testing.B) {
	hotProfile := newColumnGraphQuantizedSearchLoopProfileHook2541(b)
	defer hotProfile.finish()
	shape := columnGraphScalarU8QuantizedBenchShapeFromEnv1926(b)
	fixture := openColumnGraphRabitQQuantizedBenchFixture2451(b, shape)
	defer fixture.close()
	exactIDs, exactCount := columnGraphScalarU8QuantizedBenchmarkExactIDs1926(b, fixture)

	for _, tc := range columnGraphScalarU8QuantizedSearchWithBufferBenchCases2414() {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			opts := VectorIndexSearcherSearchOptions{
				Query:                     fixture.query,
				QueryMode:                 tc.mode,
				QuantizedIndexName:        columnGraphRabitQQuantizedIndexName2450,
				QuantizedRerankCandidates: tc.rerankCandidates,
				TopK:                      fixture.shape.topK,
				EfSearch:                  fixture.shape.efSearch,
				StatsMode:                 VectorIndexSearchStatsModeProduction,
			}
			runColumnGraphRabitQQuantizedSearchWithBufferBench2451(b, fixture, opts, tc.concurrency, exactIDs, exactCount, hotProfile)
		})
	}
}

func BenchmarkVectorIndexSearcherColumnGraphBRQQuantizedSearchWithBuffer2481(b *testing.B) {
	shape := columnGraphScalarU8QuantizedBenchShapeFromEnv1926(b)
	fixture := openColumnGraphBRQQuantizedBenchFixture2481(b, shape)
	defer fixture.close()
	exactIDs, exactCount := columnGraphScalarU8QuantizedBenchmarkExactIDs1926(b, fixture)

	for _, tc := range columnGraphScalarU8QuantizedSearchWithBufferBenchCases2414() {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			opts := VectorIndexSearcherSearchOptions{
				Query:                     fixture.query,
				QueryMode:                 tc.mode,
				QuantizedIndexName:        columnGraphBRQQuantizedIndexName2481,
				QuantizedRerankCandidates: tc.rerankCandidates,
				TopK:                      fixture.shape.topK,
				EfSearch:                  fixture.shape.efSearch,
				StatsMode:                 VectorIndexSearchStatsModeProduction,
			}
			runColumnGraphBRQQuantizedSearchWithBufferBench2481(b, fixture, opts, tc.concurrency, exactIDs, exactCount)
		})
	}
}

func BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415(b *testing.B) {
	hotProfile := newColumnGraphQuantizedSearchLoopProfileHook2541(b)
	defer hotProfile.finish()
	shape := columnGraphScalarU8QuantizedBenchShapeFromEnv1926(b)
	fixture := openColumnGraphScalarU8QuantizedBenchFixture1926(b, shape, true)
	defer fixture.close()
	exactIDs, exactCount := columnGraphScalarU8QuantizedBenchmarkExactIDs1926(b, fixture)

	for _, tc := range columnGraphScalarU8QuantizedCollectionWithBufferBenchCases2415() {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			opts := VectorIndexSearchOptions{
				IndexName:                 fixture.definition.Name,
				Query:                     fixture.query,
				QueryMode:                 tc.mode,
				QuantizedIndexName:        columnGraphScalarU8QuantizedBenchIndexName1926,
				QuantizedRerankCandidates: tc.rerankCandidates,
				TopK:                      fixture.shape.topK,
				EfSearch:                  fixture.shape.efSearch,
				MaxDecodedBlocks:          1,
				StatsMode:                 VectorIndexSearchStatsModeProduction,
			}
			runColumnGraphScalarU8QuantizedCollectionWithBufferBench2415(b, fixture, opts, tc.concurrency, exactIDs, exactCount, hotProfile)
		})
	}
}

func BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8QuantizedAlpha2415(b *testing.B) {
	hotProfile := newColumnGraphQuantizedSearchLoopProfileHook2541(b)
	defer hotProfile.finish()
	shape := columnGraphScalarU8QuantizedBenchShapeFromEnv1926(b)
	fixture := openColumnGraphScalarU8AlphaQuantizedBenchFixture2845(b, shape)
	defer fixture.close()
	exactIDs, exactCount := columnGraphScalarU8QuantizedBenchmarkExactIDs1926(b, fixture)

	for _, tc := range columnGraphScalarU8QuantizedCollectionWithBufferBenchCases2415() {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			opts := VectorIndexSearchOptions{
				IndexName:                 fixture.definition.Name,
				Query:                     fixture.query,
				QueryMode:                 tc.mode,
				QuantizedIndexName:        columnGraphScalarU8AlphaQuantizedBenchIndexName2845,
				QuantizedRerankCandidates: tc.rerankCandidates,
				TopK:                      fixture.shape.topK,
				EfSearch:                  fixture.shape.efSearch,
				MaxDecodedBlocks:          1,
				StatsMode:                 VectorIndexSearchStatsModeProduction,
			}
			runColumnGraphScalarU8AlphaQuantizedCollectionWithBufferBench2845(b, fixture, opts, tc.concurrency, exactIDs, exactCount, hotProfile)
		})
	}
}

func BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452(b *testing.B) {
	hotProfile := newColumnGraphQuantizedSearchLoopProfileHook2541(b)
	defer hotProfile.finish()
	shape := columnGraphScalarU8QuantizedBenchShapeFromEnv1926(b)
	fixture := openColumnGraphRabitQQuantizedBenchFixture2451(b, shape)
	defer fixture.close()
	exactIDs, exactCount := columnGraphScalarU8QuantizedBenchmarkExactIDs1926(b, fixture)

	for _, tc := range columnGraphRabitQQuantizedCollectionWithBufferBenchCases2452() {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			opts := VectorIndexSearchOptions{
				IndexName:                 fixture.definition.Name,
				Query:                     fixture.query,
				QueryMode:                 tc.mode,
				QuantizedIndexName:        columnGraphRabitQQuantizedIndexName2450,
				QuantizedRerankCandidates: tc.rerankCandidates,
				TopK:                      fixture.shape.topK,
				EfSearch:                  fixture.shape.efSearch,
				MaxDecodedBlocks:          1,
				StatsMode:                 VectorIndexSearchStatsModeProduction,
			}
			runColumnGraphScalarU8QuantizedCollectionWithBufferBench2415(b, fixture, opts, tc.concurrency, exactIDs, exactCount, hotProfile)
		})
	}
}

func runColumnGraphScalarU8QuantizedCollectionWithBufferBench2415(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, opts VectorIndexSearchOptions, concurrency int, exactIDs map[string]struct{}, exactCount int, hotProfile *columnGraphQuantizedSearchLoopProfileHook2541) {
	b.Helper()
	if concurrency <= 0 {
		b.Fatalf("concurrency=%d must be positive", concurrency)
	}
	type benchWorker struct {
		buffer VectorIndexSearchBuffer
		stats  VectorIndexSearchStats
		sink   int64
	}
	if err := fixture.collection.closeCollectionVectorIndexPreparedSearchCache(); err != nil {
		b.Fatalf("close collection prepared cache before benchmark row: %v", err)
	}
	workers := make([]benchWorker, concurrency)
	for i := range workers {
		warmOpts := opts
		warmOpts.Query = fixture.query
		warm, err := fixture.collection.SearchVectorIndexWithBuffer(warmOpts, &workers[i].buffer)
		if err != nil {
			b.Fatalf("warm SearchVectorIndexWithBuffer worker %d: %v", i, err)
		}
		if len(warm.Results) == 0 {
			b.Fatalf("warm SearchVectorIndexWithBuffer worker %d returned no results", i)
		}
		assertColumnGraphScalarU8QuantizedCollectionWithBufferGuardrails2415(b, warm.Stats, opts, fixture.definition.Dimensions)
		assertColumnGraphScalarU8QuantizedBenchWarmCodecStats2844(b, warm.Stats, fixture)
	}
	statsOpts := opts
	statsOpts.StatsMode = VectorIndexSearchStatsModeFullDiagnostics
	measured, err := fixture.collection.SearchVectorIndexWithBuffer(statsOpts, &workers[0].buffer)
	if err != nil {
		b.Fatalf("measure SearchVectorIndexWithBuffer stats: %v", err)
	}
	assertColumnGraphScalarU8QuantizedCollectionWithBufferGuardrails2415(b, measured.Stats, statsOpts, fixture.definition.Dimensions)
	assertColumnGraphScalarU8QuantizedBenchWarmCodecStats2844(b, measured.Stats, fixture)
	ensureColumnGraphScalarU8QuantizedCollectionReaderPool2415(b, fixture.collection, opts, concurrency)
	warmRecall := columnGraphScalarU8QuantizedBenchmarkRecallAtK1926(measured.Results, exactIDs, exactCount)

	var next atomic.Uint64
	var sink atomic.Int64
	var failed atomic.Bool
	var firstErr atomic.Value
	recordErr := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			firstErr.Store(fmt.Sprintf(format, args...))
		}
	}
	var wg sync.WaitGroup
	ready := make(chan struct{}, len(workers))
	start := make(chan struct{})
	wg.Add(len(workers))
	for i := range workers {
		worker := &workers[i]
		go func() {
			defer wg.Done()
			localOpts := opts
			localOpts.Query = fixture.query
			for warmIter := 0; warmIter < 16; warmIter++ {
				if warm, err := fixture.collection.SearchVectorIndexWithBuffer(localOpts, &worker.buffer); err != nil {
					recordErr("goroutine warm SearchVectorIndexWithBuffer: %v", err)
					break
				} else if len(warm.Results) == 0 {
					recordErr("goroutine warm SearchVectorIndexWithBuffer returned no results")
					break
				}
			}
			ready <- struct{}{}
			<-start
			var localStats VectorIndexSearchStats
			var localSink int64
			for {
				iteration := int(next.Add(1)) - 1
				if iteration >= b.N {
					break
				}
				if failed.Load() {
					continue
				}
				response, err := fixture.collection.SearchVectorIndexWithBuffer(localOpts, &worker.buffer)
				if err != nil {
					recordErr("SearchVectorIndexWithBuffer: %v", err)
					continue
				}
				if len(response.Results) == 0 {
					recordErr("SearchVectorIndexWithBuffer returned no results")
					continue
				}
				localSink += int64(response.Results[0].Ordinal)
				addColumnGraphScalarU8QuantizedBenchmarkStats1926(&localStats, response.Stats)
			}
			worker.stats = localStats
			worker.sink = localSink
		}()
	}
	for range workers {
		<-ready
	}
	if errValue := firstErr.Load(); errValue != nil {
		close(start)
		wg.Wait()
		b.Fatalf("%s", errValue.(string))
	}
	cacheBeforeTimed := fixture.collection.collectionVectorIndexPreparedSearchCacheSnapshot()
	b.ReportAllocs()
	stopHotProfile := hotProfile.start(b)
	hotProfileActive := hotProfile.enabled()
	defer func() {
		if hotProfileActive {
			stopHotProfile()
		}
	}()
	b.ResetTimer()
	close(start)
	wg.Wait()
	b.StopTimer()
	stopHotProfile()
	hotProfileActive = false
	b.ReportMetric(float64(concurrency), "concurrency")
	b.ReportMetric(1, "collection_searchvectorindex_with_buffer_seam")
	b.ReportMetric(1, "collection_buffered_quantized_row")
	b.ReportMetric(0, "open_searcher_calls/op")
	b.ReportMetric(0, "open_setup_in_timed_loop")
	b.ReportMetric(1, "reported_stats_mode_production")
	if errValue := firstErr.Load(); errValue != nil {
		b.Fatalf("%s", errValue.(string))
	}
	var stats VectorIndexSearchStats
	for i := range workers {
		addColumnGraphScalarU8QuantizedBenchmarkStats1926(&stats, workers[i].stats)
		sink.Add(workers[i].sink)
	}
	columnPhysicalScanBenchSum += sink.Load()
	assertColumnGraphScalarU8QuantizedBenchAggregateCodecStats2844(b, stats, fixture, b.N)
	recallSum := warmRecall * float64(b.N)
	reportCollectionVectorIndexPreparedSearchBenchMetrics2415(b, cacheBeforeTimed, fixture.collection.collectionVectorIndexPreparedSearchCacheSnapshot())
	reportColumnGraphScalarU8QuantizedScorePlaneMetrics1926(b, fixture, stats, recallSum)
}

func ensureColumnGraphScalarU8QuantizedCollectionReaderPool2415(b *testing.B, col *Collection, opts VectorIndexSearchOptions, concurrency int) {
	b.Helper()
	queryMode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	if err != nil {
		b.Fatalf("normalize collection quantized benchmark query mode: %v", err)
	}
	prepared, _, _, err := col.acquireCollectionVectorIndexPreparedSearch(opts)
	if err != nil {
		b.Fatalf("acquire collection quantized prepared search for reader-pool warmup: %v", err)
	}
	prepared.mu.RLock()
	defer prepared.mu.RUnlock()
	if prepared.closed || prepared.searcher == nil || prepared.searcher.closed || prepared.searcher.snapshot == nil {
		b.Fatalf("collection quantized prepared search is not ready for reader-pool warmup")
	}
	for {
		prepared.quantizedReadersMu.Lock()
		have := len(prepared.quantizedReaders)
		prepared.quantizedReadersMu.Unlock()
		if have >= concurrency {
			return
		}
		reader, _, err := prepared.openCollectionVectorIndexPreparedQuantizedReader(opts, queryMode)
		if err != nil {
			b.Fatalf("open collection quantized prepared reader %d/%d: %v", have+1, concurrency, err)
		}
		prepared.quantizedReadersMu.Lock()
		idx := len(prepared.quantizedReaders)
		prepared.quantizedReaders = append(prepared.quantizedReaders, reader)
		prepared.quantizedAvailableReaders = append(prepared.quantizedAvailableReaders, idx)
		prepared.quantizedReadersMu.Unlock()
	}
}

func reportCollectionVectorIndexPreparedSearchBenchMetrics2415(b *testing.B, before, after collectionVectorIndexPreparedSearchCacheSnapshot) {
	b.Helper()
	iterations := float64(b.N)
	if iterations <= 0 {
		iterations = 1
	}
	cacheBuilds := after.CacheBuilds - before.CacheBuilds
	cacheMisses := after.CacheMisses - before.CacheMisses
	cacheHits := after.CacheHits - before.CacheHits
	cacheWaits := after.CacheWaits - before.CacheWaits
	invalidations := after.Invalidations - before.Invalidations
	closes := after.Closes - before.Closes
	errors := after.Errors - before.Errors
	b.ReportMetric(float64(after.Entries), "collection_prepared_cache_entries")
	b.ReportMetric(float64(after.BuildingEntries), "collection_prepared_cache_building_entries")
	b.ReportMetric(float64(cacheBuilds)/iterations, "collection_prepared_cache_builds/op")
	b.ReportMetric(float64(cacheMisses)/iterations, "collection_prepared_cache_misses/op")
	b.ReportMetric(float64(cacheHits)/iterations, "collection_prepared_cache_hits/op")
	b.ReportMetric(float64(cacheWaits)/iterations, "collection_prepared_cache_waits/op")
	b.ReportMetric(float64(invalidations)/iterations, "collection_prepared_cache_invalidations/op")
	b.ReportMetric(float64(closes)/iterations, "collection_prepared_cache_closes/op")
	b.ReportMetric(float64(errors)/iterations, "collection_prepared_cache_errors/op")
	if lookups := cacheHits + cacheMisses; lookups > 0 {
		b.ReportMetric(float64(cacheHits)/float64(lookups), "collection_prepared_cache_hit_ratio")
	}
	b.ReportMetric(float64(after.ActiveHandles), "collection_prepared_active_handles")
	b.ReportMetric(float64(after.ActiveMappedBytes), "collection_prepared_mapped_B")
	b.ReportMetric(float64(after.ActiveHeapCopyBytes), "collection_prepared_heap_copy_B")
	b.ReportMetric(float64(after.ActiveDerivedMetadataBytes), "collection_prepared_derived_metadata_B")
}

func assertColumnGraphScalarU8QuantizedCollectionWithBufferGuardrails2415(tb testing.TB, stats VectorIndexSearchStats, opts VectorIndexSearchOptions, dims int) {
	tb.Helper()
	queryMode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	if err != nil {
		tb.Fatalf("normalize query mode: %v", err)
	}
	if !vectorIndexSearchStatsAreBufferedNoDocumentQuantizedRoute(stats, queryMode, opts, dims) {
		tb.Fatalf("Collection.SearchVectorIndexWithBuffer quantized stats=%+v want no-document quantized route", stats)
	}
}

func assertColumnGraphScalarU8AlphaQuantizedCollectionWithBufferGuardrails2845(tb testing.TB, stats VectorIndexSearchStats, opts VectorIndexSearchOptions, dims int) {
	tb.Helper()
	assertColumnGraphScalarU8QuantizedCollectionWithBufferGuardrails2415(tb, stats, opts, dims)
	if stats.QuantizedScoreCodecScalarU8Alpha != 1 {
		tb.Fatalf("scalar_u8 alpha collection stats=%+v want quantized_score_codec_scalar_u8_alpha=1", stats)
	}
}

func runColumnGraphScalarU8AlphaQuantizedCollectionWithBufferBench2845(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, opts VectorIndexSearchOptions, concurrency int, exactIDs map[string]struct{}, exactCount int, hotProfile *columnGraphQuantizedSearchLoopProfileHook2541) {
	b.Helper()
	runColumnGraphScalarU8QuantizedCollectionWithBufferBench2415(b, fixture, opts, concurrency, exactIDs, exactCount, hotProfile)
}

func assertColumnGraphScalarU8QuantizedBenchWarmCodecStats2844(tb testing.TB, stats VectorIndexSearchStats, fixture columnGraphScalarU8QuantizedBenchFixture1926) {
	tb.Helper()
	if fixture.scalarU8Alpha {
		if stats.QuantizedScoreCodecScalarU8Alpha != 1 {
			tb.Fatalf("scalar_u8 alpha benchmark stats=%+v want QuantizedScoreCodecScalarU8Alpha=1", stats)
		}
		return
	}
	if stats.QuantizedScoreCodecScalarU8Alpha != 0 {
		tb.Fatalf("non-alpha quantized benchmark stats=%+v want QuantizedScoreCodecScalarU8Alpha=0", stats)
	}
}

func assertColumnGraphScalarU8QuantizedBenchAggregateCodecStats2844(tb testing.TB, stats VectorIndexSearchStats, fixture columnGraphScalarU8QuantizedBenchFixture1926, searches int) {
	tb.Helper()
	if searches < 0 {
		searches = 0
	}
	if fixture.scalarU8Alpha {
		if stats.QuantizedScoreCodecScalarU8Alpha != uint64(searches) {
			tb.Fatalf("scalar_u8 alpha benchmark aggregate stats=%+v want QuantizedScoreCodecScalarU8Alpha=%d", stats, searches)
		}
		return
	}
	if stats.QuantizedScoreCodecScalarU8Alpha != 0 {
		tb.Fatalf("non-alpha quantized benchmark aggregate stats=%+v want QuantizedScoreCodecScalarU8Alpha=0", stats)
	}
}

func runColumnGraphScalarU8QuantizedSearchWithBufferBench2414(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, opts VectorIndexSearcherSearchOptions, concurrency int, exactIDs map[string]struct{}, exactCount int) {
	b.Helper()
	if concurrency <= 0 {
		b.Fatalf("concurrency=%d must be positive", concurrency)
	}
	type benchWorker struct {
		searcher *VectorIndexSearcher
		buffer   VectorIndexSearchBuffer
		stats    VectorIndexSearchStats
		sink     int64
	}
	workers := make([]benchWorker, concurrency)
	for i := range workers {
		searcher, err := fixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.definition.Name, MaxDecodedBlocks: 1})
		if err != nil {
			b.Fatalf("OpenVectorIndexSearcher worker %d: %v", i, err)
		}
		defer func(searcher *VectorIndexSearcher) { _ = searcher.Close() }(searcher)
		workers[i].searcher = searcher
		warm, err := searcher.SearchWithBuffer(opts, &workers[i].buffer)
		if err != nil {
			b.Fatalf("warm SearchWithBuffer worker %d: %v", i, err)
		}
		if len(warm.Results) == 0 {
			b.Fatalf("warm SearchWithBuffer worker %d returned no results", i)
		}
		assertColumnGraphScalarU8QuantizedSearchWithBufferGuardrails2414(b, warm.Stats, opts, fixture.definition.Dimensions)
		assertColumnGraphScalarU8QuantizedBenchWarmCodecStats2844(b, warm.Stats, fixture)
	}
	warmRecall := columnGraphScalarU8QuantizedBenchmarkRecallAtK1926(workers[0].buffer.results, exactIDs, exactCount)

	var next atomic.Uint64
	var sink atomic.Int64
	var failed atomic.Bool
	var firstErr atomic.Value
	recordErr := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			firstErr.Store(fmt.Sprintf(format, args...))
		}
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(len(workers))
	for i := range workers {
		worker := &workers[i]
		go func() {
			defer wg.Done()
			<-start
			var localStats VectorIndexSearchStats
			var localSink int64
			for {
				iteration := int(next.Add(1)) - 1
				if iteration >= b.N {
					break
				}
				if failed.Load() {
					continue
				}
				response, err := worker.searcher.SearchWithBuffer(opts, &worker.buffer)
				if err != nil {
					recordErr("SearchWithBuffer: %v", err)
					continue
				}
				if len(response.Results) == 0 {
					recordErr("SearchWithBuffer returned no results")
					continue
				}
				localSink += int64(response.Results[0].Ordinal)
				addColumnGraphScalarU8QuantizedBenchmarkStats1926(&localStats, response.Stats)
			}
			worker.stats = localStats
			worker.sink = localSink
		}()
	}
	b.ReportAllocs()
	b.ReportMetric(float64(concurrency), "concurrency")
	b.ReportMetric(1, "searchwithbuffer_buffered_row")
	b.ReportMetric(1, "reported_stats_mode_production")
	b.ResetTimer()
	close(start)
	wg.Wait()
	b.StopTimer()
	if errValue := firstErr.Load(); errValue != nil {
		b.Fatalf("%s", errValue.(string))
	}
	var stats VectorIndexSearchStats
	for i := range workers {
		addColumnGraphScalarU8QuantizedBenchmarkStats1926(&stats, workers[i].stats)
		sink.Add(workers[i].sink)
	}
	columnPhysicalScanBenchSum += sink.Load()
	assertColumnGraphScalarU8QuantizedBenchAggregateCodecStats2844(b, stats, fixture, b.N)
	recallSum := warmRecall * float64(b.N)
	reportColumnGraphScalarU8QuantizedScorePlaneMetrics1926(b, fixture, stats, recallSum)
}

func assertColumnGraphScalarU8QuantizedSearchWithBufferGuardrails2414(tb testing.TB, stats VectorIndexSearchStats, opts VectorIndexSearcherSearchOptions, dims int) {
	tb.Helper()
	switch opts.QueryMode {
	case VectorIndexQueryModeQuantizedOnly:
		assertQuantizedOnlyGuardrailStats2416(tb, stats, dims)
	case VectorIndexQueryModeQuantizedRerank:
		assertScalarU8PackNativeQuantizedRerankNoDocumentGuardrailStats2657(tb, stats, opts.QuantizedRerankCandidates, dims)
	default:
		tb.Fatalf("unexpected SearchWithBuffer benchmark query mode %q", opts.QueryMode)
	}
	if stats.DocumentsFetched != 0 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 {
		tb.Fatalf("SearchWithBuffer buffered guardrails stats=%+v want no docs/materialization/fallback/scratch", stats)
	}
}

func assertColumnGraphScalarU8AlphaQuantizedSearchWithBufferGuardrails2845(tb testing.TB, stats VectorIndexSearchStats, opts VectorIndexSearcherSearchOptions, dims int) {
	tb.Helper()
	assertColumnGraphScalarU8QuantizedSearchWithBufferGuardrails2414(tb, stats, opts, dims)
	if stats.QuantizedScoreCodecScalarU8Alpha != 1 {
		tb.Fatalf("scalar_u8 alpha SearchWithBuffer stats=%+v want quantized_score_codec_scalar_u8_alpha=1", stats)
	}
}

func runColumnGraphScalarU8AlphaQuantizedSearchWithBufferBench2845(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, opts VectorIndexSearcherSearchOptions, concurrency int, exactIDs map[string]struct{}, exactCount int) {
	b.Helper()
	runColumnGraphScalarU8QuantizedSearchWithBufferBench2414(b, fixture, opts, concurrency, exactIDs, exactCount)
}

func runColumnGraphRabitQQuantizedSearchWithBufferBench2451(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, opts VectorIndexSearcherSearchOptions, concurrency int, exactIDs map[string]struct{}, exactCount int, hotProfile *columnGraphQuantizedSearchLoopProfileHook2541) {
	b.Helper()
	if concurrency <= 0 {
		b.Fatalf("concurrency=%d must be positive", concurrency)
	}
	type benchWorker struct {
		searcher *VectorIndexSearcher
		buffer   VectorIndexSearchBuffer
		stats    VectorIndexSearchStats
		sink     int64
	}
	workers := make([]benchWorker, concurrency)
	for i := range workers {
		searcher, err := fixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.definition.Name, MaxDecodedBlocks: 1})
		if err != nil {
			b.Fatalf("OpenVectorIndexSearcher worker %d: %v", i, err)
		}
		defer func(searcher *VectorIndexSearcher) { _ = searcher.Close() }(searcher)
		workers[i].searcher = searcher
		warm, err := searcher.SearchWithBuffer(opts, &workers[i].buffer)
		if err != nil {
			b.Fatalf("warm rabitq SearchWithBuffer worker %d: %v", i, err)
		}
		if len(warm.Results) == 0 {
			b.Fatalf("warm rabitq SearchWithBuffer worker %d returned no results", i)
		}
		assertColumnGraphRabitQQuantizedSearchWithBufferGuardrails2451(b, warm.Stats, opts, fixture.definition.Dimensions, fixture.quantizedCodeBytesPerVector)
	}
	warmRecall := columnGraphScalarU8QuantizedBenchmarkRecallAtK1926(workers[0].buffer.results, exactIDs, exactCount)

	var next atomic.Uint64
	var sink atomic.Int64
	var failed atomic.Bool
	var firstErr atomic.Value
	recordErr := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			firstErr.Store(fmt.Sprintf(format, args...))
		}
	}
	var wg sync.WaitGroup
	ready := make(chan struct{}, len(workers))
	start := make(chan struct{})
	wg.Add(len(workers))
	for i := range workers {
		worker := &workers[i]
		go func() {
			defer wg.Done()
			for warmIter := 0; warmIter < 8; warmIter++ {
				response, err := worker.searcher.SearchWithBuffer(opts, &worker.buffer)
				if err != nil {
					recordErr("rabitq goroutine warm SearchWithBuffer: %v", err)
					break
				}
				if len(response.Results) == 0 {
					recordErr("rabitq goroutine warm SearchWithBuffer returned no results")
					break
				}
			}
			ready <- struct{}{}
			<-start
			if failed.Load() {
				return
			}
			var localStats VectorIndexSearchStats
			var localSink int64
			for {
				iteration := int(next.Add(1)) - 1
				if iteration >= b.N {
					break
				}
				if failed.Load() {
					continue
				}
				response, err := worker.searcher.SearchWithBuffer(opts, &worker.buffer)
				if err != nil {
					recordErr("rabitq SearchWithBuffer: %v", err)
					continue
				}
				if len(response.Results) == 0 {
					recordErr("rabitq SearchWithBuffer returned no results")
					continue
				}
				localSink += int64(response.Results[0].Ordinal)
				addColumnGraphScalarU8QuantizedBenchmarkStats1926(&localStats, response.Stats)
			}
			worker.stats = localStats
			worker.sink = localSink
		}()
	}
	for range workers {
		<-ready
	}
	if errValue := firstErr.Load(); errValue != nil {
		close(start)
		wg.Wait()
		b.Fatalf("%s", errValue.(string))
	}
	b.ReportAllocs()
	b.ReportMetric(float64(concurrency), "concurrency")
	b.ReportMetric(1, "searchwithbuffer_buffered_row")
	b.ReportMetric(1, "rabitq_1bit_row")
	b.ReportMetric(1, "reported_stats_mode_production")
	stopHotProfile := hotProfile.start(b)
	hotProfileActive := hotProfile.enabled()
	defer func() {
		if hotProfileActive {
			stopHotProfile()
		}
	}()
	b.ResetTimer()
	close(start)
	wg.Wait()
	b.StopTimer()
	stopHotProfile()
	hotProfileActive = false
	if errValue := firstErr.Load(); errValue != nil {
		b.Fatalf("%s", errValue.(string))
	}
	var stats VectorIndexSearchStats
	for i := range workers {
		addColumnGraphScalarU8QuantizedBenchmarkStats1926(&stats, workers[i].stats)
		sink.Add(workers[i].sink)
	}
	columnPhysicalScanBenchSum += sink.Load()
	recallSum := warmRecall * float64(b.N)
	reportColumnGraphScalarU8QuantizedScorePlaneMetrics1926(b, fixture, stats, recallSum)
}

func assertColumnGraphRabitQQuantizedSearchWithBufferGuardrails2451(tb testing.TB, stats VectorIndexSearchStats, opts VectorIndexSearcherSearchOptions, dims int, bytesPerCode int) {
	tb.Helper()
	if bytesPerCode <= 0 {
		tb.Fatalf("rabitq bytes_per_code=%d", bytesPerCode)
	}
	switch opts.QueryMode {
	case VectorIndexQueryModeQuantizedOnly:
		if stats.SearchRouteQuantizedOnly != 1 || stats.SearchRouteQuantizedRerank != 0 || stats.QuantizedScorerActive != 1 {
			tb.Fatalf("rabitq quantized_only route stats=%+v", stats)
		}
		if stats.SearchRouteHNSWSearchPack != 1 || stats.HNSWSearchPackActive != 1 || stats.HNSWSearchPackFallbacks != 0 || stats.SearchRouteColumnGraphPrepared != 0 || stats.SearchRouteColumnGraphFallback != 0 {
			tb.Fatalf("rabitq quantized_only route stats=%+v want prepared hnsw_search_pack_v1 score-plane traversal", stats)
		}
		if stats.PreparedScoreCalls != 0 || stats.QuantizedRerankExactScoreCalls != 0 || stats.VectorBytesRead != 0 || stats.NormBytesRead != 0 {
			tb.Fatalf("rabitq quantized_only exact stats=%+v want none", stats)
		}
	case VectorIndexQueryModeQuantizedRerank:
		if stats.SearchRouteQuantizedOnly != 0 || stats.SearchRouteQuantizedRerank != 1 || stats.QuantizedScorerActive != 1 {
			tb.Fatalf("rabitq quantized_rerank route stats=%+v", stats)
		}
		if stats.SearchRouteHNSWSearchPack != 1 || stats.HNSWSearchPackActive != 1 || stats.HNSWSearchPackFallbacks != 0 || stats.SearchRouteColumnGraphPrepared != 0 || stats.SearchRouteColumnGraphFallback != 0 {
			tb.Fatalf("rabitq quantized_rerank route stats=%+v want prepared hnsw_search_pack_v1 score-plane traversal", stats)
		}
		if stats.QuantizedRerankCandidates != uint64(opts.QuantizedRerankCandidates) || stats.QuantizedRerankExactScoreCalls != uint64(opts.QuantizedRerankCandidates) {
			tb.Fatalf("rabitq quantized_rerank stats=%+v want shortlist=%d", stats, opts.QuantizedRerankCandidates)
		}
		if stats.VectorBytesRead != uint64(opts.QuantizedRerankCandidates*dims*4) || stats.NormBytesRead != 0 {
			tb.Fatalf("rabitq quantized_rerank exact bytes stats=%+v want prepared-pack vector=%d norm=0", stats, opts.QuantizedRerankCandidates*dims*4)
		}
	default:
		tb.Fatalf("unexpected rabitq SearchWithBuffer benchmark query mode %q", opts.QueryMode)
	}
	if stats.QuantizedScoreCalls == 0 || stats.QuantizedCodeBytesRead != stats.QuantizedScoreCalls*uint64(bytesPerCode) {
		tb.Fatalf("rabitq quantized code stats=%+v bytes_per_code=%d", stats, bytesPerCode)
	}
	if stats.DocumentsFetched != 0 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 {
		tb.Fatalf("rabitq SearchWithBuffer buffered guardrails stats=%+v want no docs/materialization/fallback/scratch", stats)
	}
}

func runColumnGraphBRQQuantizedSearchWithBufferBench2481(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, opts VectorIndexSearcherSearchOptions, concurrency int, exactIDs map[string]struct{}, exactCount int) {
	b.Helper()
	if concurrency <= 0 {
		b.Fatalf("concurrency=%d must be positive", concurrency)
	}
	type benchWorker struct {
		searcher *VectorIndexSearcher
		buffer   VectorIndexSearchBuffer
		stats    VectorIndexSearchStats
		sink     int64
	}
	workers := make([]benchWorker, concurrency)
	for i := range workers {
		searcher, err := fixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.definition.Name, MaxDecodedBlocks: 1})
		if err != nil {
			b.Fatalf("OpenVectorIndexSearcher worker %d: %v", i, err)
		}
		defer func(searcher *VectorIndexSearcher) { _ = searcher.Close() }(searcher)
		workers[i].searcher = searcher
		warm, err := searcher.SearchWithBuffer(opts, &workers[i].buffer)
		if err != nil {
			b.Fatalf("warm brq SearchWithBuffer worker %d: %v", i, err)
		}
		if len(warm.Results) == 0 {
			b.Fatalf("warm brq SearchWithBuffer worker %d returned no results", i)
		}
		assertColumnGraphBRQQuantizedSearchWithBufferGuardrails2481(b, warm.Stats, opts, fixture.definition.Dimensions, fixture.quantizedCodeBytesPerVector)
	}
	warmRecall := columnGraphScalarU8QuantizedBenchmarkRecallAtK1926(workers[0].buffer.results, exactIDs, exactCount)

	var next atomic.Uint64
	var sink atomic.Int64
	var failed atomic.Bool
	var firstErr atomic.Value
	recordErr := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			firstErr.Store(fmt.Sprintf(format, args...))
		}
	}
	var wg sync.WaitGroup
	ready := make(chan struct{}, len(workers))
	start := make(chan struct{})
	wg.Add(len(workers))
	for i := range workers {
		worker := &workers[i]
		go func() {
			defer wg.Done()
			for warmIter := 0; warmIter < 8; warmIter++ {
				response, err := worker.searcher.SearchWithBuffer(opts, &worker.buffer)
				if err != nil {
					recordErr("brq goroutine warm SearchWithBuffer: %v", err)
					break
				}
				if len(response.Results) == 0 {
					recordErr("brq goroutine warm SearchWithBuffer returned no results")
					break
				}
			}
			ready <- struct{}{}
			<-start
			if failed.Load() {
				return
			}
			var localStats VectorIndexSearchStats
			var localSink int64
			for {
				iteration := int(next.Add(1)) - 1
				if iteration >= b.N {
					break
				}
				if failed.Load() {
					continue
				}
				response, err := worker.searcher.SearchWithBuffer(opts, &worker.buffer)
				if err != nil {
					recordErr("brq SearchWithBuffer: %v", err)
					continue
				}
				if len(response.Results) == 0 {
					recordErr("brq SearchWithBuffer returned no results")
					continue
				}
				localSink += int64(response.Results[0].Ordinal)
				addColumnGraphScalarU8QuantizedBenchmarkStats1926(&localStats, response.Stats)
			}
			worker.stats = localStats
			worker.sink = localSink
		}()
	}
	for range workers {
		<-ready
	}
	if errValue := firstErr.Load(); errValue != nil {
		close(start)
		wg.Wait()
		b.Fatalf("%s", errValue.(string))
	}
	b.ReportAllocs()
	b.ReportMetric(float64(concurrency), "concurrency")
	b.ReportMetric(1, "searchwithbuffer_buffered_row")
	b.ReportMetric(1, "brq_1bit_row")
	b.ReportMetric(1, "reported_stats_mode_production")
	b.ResetTimer()
	close(start)
	wg.Wait()
	b.StopTimer()
	if errValue := firstErr.Load(); errValue != nil {
		b.Fatalf("%s", errValue.(string))
	}
	var stats VectorIndexSearchStats
	for i := range workers {
		addColumnGraphScalarU8QuantizedBenchmarkStats1926(&stats, workers[i].stats)
		sink.Add(workers[i].sink)
	}
	columnPhysicalScanBenchSum += sink.Load()
	recallSum := warmRecall * float64(b.N)
	reportColumnGraphScalarU8QuantizedScorePlaneMetrics1926(b, fixture, stats, recallSum)
}

func assertColumnGraphBRQQuantizedSearchWithBufferGuardrails2481(tb testing.TB, stats VectorIndexSearchStats, opts VectorIndexSearcherSearchOptions, dims int, bytesPerCode int) {
	tb.Helper()
	if bytesPerCode <= 0 {
		tb.Fatalf("brq bytes_per_code=%d", bytesPerCode)
	}
	switch opts.QueryMode {
	case VectorIndexQueryModeQuantizedOnly:
		if stats.SearchRouteQuantizedOnly != 1 || stats.SearchRouteQuantizedRerank != 0 || stats.QuantizedScorerActive != 1 {
			tb.Fatalf("brq quantized_only route stats=%+v", stats)
		}
		if stats.PreparedScoreCalls != 0 || stats.QuantizedRerankExactScoreCalls != 0 || stats.VectorBytesRead != 0 || stats.NormBytesRead != 0 {
			tb.Fatalf("brq quantized_only exact stats=%+v want none", stats)
		}
	case VectorIndexQueryModeQuantizedRerank:
		if stats.SearchRouteQuantizedOnly != 0 || stats.SearchRouteQuantizedRerank != 1 || stats.QuantizedScorerActive != 1 {
			tb.Fatalf("brq quantized_rerank route stats=%+v", stats)
		}
		if stats.QuantizedRerankCandidates != uint64(opts.QuantizedRerankCandidates) || stats.QuantizedRerankExactScoreCalls != uint64(opts.QuantizedRerankCandidates) {
			tb.Fatalf("brq quantized_rerank stats=%+v want shortlist=%d", stats, opts.QuantizedRerankCandidates)
		}
		if stats.VectorBytesRead != uint64(opts.QuantizedRerankCandidates*dims*4) || stats.NormBytesRead != uint64(opts.QuantizedRerankCandidates*4) {
			tb.Fatalf("brq quantized_rerank exact bytes stats=%+v want vector=%d norm=%d", stats, opts.QuantizedRerankCandidates*dims*4, opts.QuantizedRerankCandidates*4)
		}
	default:
		tb.Fatalf("unexpected brq SearchWithBuffer benchmark query mode %q", opts.QueryMode)
	}
	if stats.QuantizedScoreCalls == 0 || stats.QuantizedCodeBytesRead != stats.QuantizedScoreCalls*uint64(bytesPerCode) {
		tb.Fatalf("brq quantized code stats=%+v bytes_per_code=%d", stats, bytesPerCode)
	}
	if stats.QuantizedScoreCodecBRQ1Bit != 1 || stats.BRQ1BitQueryWeightBits != brq.QueryWeightBits || stats.BRQ1BitBitProductPasses != stats.QuantizedScoreCalls*2 || stats.BRQ1BitQueryWeightScale <= 0 {
		tb.Fatalf("brq codec stats=%+v", stats)
	}
	if stats.DocumentsFetched != 0 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 {
		tb.Fatalf("brq SearchWithBuffer buffered guardrails stats=%+v want no docs/materialization/fallback/scratch", stats)
	}
}

func BenchmarkColumnGraphScalarU8QuantizedTraversalCounters2271(b *testing.B) {
	shape := columnGraphScalarU8QuantizedBenchShapeFromEnv1926(b)
	fixture := openColumnGraphScalarU8QuantizedBenchFixture1926(b, shape, true)
	defer fixture.close()

	cases := []struct {
		name               string
		mode               VectorIndexQueryMode
		rerankCandidates   int
		quantizedIndexName string
	}{
		{name: "mode=exact", mode: VectorIndexQueryModeExact},
		{name: "mode=quantized_only", mode: VectorIndexQueryModeQuantizedOnly, quantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926},
		{name: "mode=quantized_rerank/candidates=32", mode: VectorIndexQueryModeQuantizedRerank, quantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926, rerankCandidates: 32},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			searcher, err := fixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.definition.Name, MaxDecodedBlocks: 1})
			if err != nil {
				b.Fatalf("OpenVectorIndexSearcher: %v", err)
			}
			defer func() { _ = searcher.Close() }()
			opts := VectorIndexSearcherSearchOptions{
				Query:                     fixture.query,
				QueryMode:                 tc.mode,
				QuantizedIndexName:        tc.quantizedIndexName,
				QuantizedRerankCandidates: tc.rerankCandidates,
				TopK:                      fixture.shape.topK,
				EfSearch:                  fixture.shape.efSearch,
				StatsMode:                 VectorIndexSearchStatsModeBenchmarkDebug,
			}
			var buffer VectorIndexSearchBuffer
			warm, err := searcher.SearchWithBuffer(opts, &buffer)
			if err != nil {
				b.Fatalf("warm SearchWithBuffer: %v", err)
			}
			if len(warm.Results) == 0 || warm.Stats.BenchmarkDebugSearches != 1 {
				b.Fatalf("warm response results=%d stats=%+v want benchmark_debug counters", len(warm.Results), warm.Stats)
			}

			var stats VectorIndexSearchStats
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				response, err := searcher.SearchWithBuffer(opts, &buffer)
				if err != nil {
					b.Fatalf("SearchWithBuffer: %v", err)
				}
				if len(response.Results) == 0 {
					b.Fatalf("SearchWithBuffer returned no results")
				}
				columnPhysicalScanBenchSum += int64(response.Results[0].Ordinal)
				addColumnGraphScalarU8QuantizedBenchmarkStats1926(&stats, response.Stats)
			}
			b.StopTimer()
			reportColumnGraphScalarU8QuantizedTraversalCounterMetrics2271(b, fixture, stats)
		})
	}
}

func BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926(b *testing.B) {
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 256, dims: 128, m: 16, efConstruction: 128, topK: 10, efSearch: 128, queryOrdinal: 37, queryCount: 1}
	for _, tc := range []struct {
		name  string
		qdefs []QuantizedVectorIndexDefinition
	}{
		{name: "mode=exact_assets"},
		{name: "mode=scalar_u8_assets", qdefs: []QuantizedVectorIndexDefinition{{Name: columnGraphScalarU8QuantizedBenchIndexName1926}}},
		{name: "mode=scalar_u8_per_granule_alpha_assets", qdefs: []QuantizedVectorIndexDefinition{columnGraphScalarU8AlphaQuantizedBenchIndexDefinition2845()}},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			_, d, col, def, _ := openColumnGraphScalarU8QuantizedBenchCollectionWithDefinitions2845(b, shape, tc.qdefs)
			defer func() { _ = d.Close() }()
			b.ReportAllocs()
			reportColumnGraphScalarU8QuantizedBenchShapeMetrics1926(b, shape)
			if len(tc.qdefs) > 0 {
				b.ReportMetric(float64(len(tc.qdefs)), "quantized_indexes")
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				status, err := col.RebuildVectorIndex(def.Name)
				if err != nil {
					b.Fatalf("RebuildVectorIndex: %v", err)
				}
				if !status.Loaded || status.RebuildNeeded {
					b.Fatalf("status=%+v, want loaded", status)
				}
				columnGraphRebuildBenchSinkV2A = status
			}
			b.StopTimer()
			if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
				b.ReportMetric(float64(b.N)/elapsed, "ops/sec")
			}
			reportColumnGraphScalarU8QuantizedStorageMetrics1926(b, d, def, shape)
		})
	}
}

func BenchmarkColumnGraphBRQQuantizedRebuildStorage2481(b *testing.B) {
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 256, dims: 128, m: 16, efConstruction: 128, topK: 10, efSearch: 128, queryOrdinal: 37, queryCount: 1}
	_, d, col, def, _ := openColumnGraphBRQQuantizedBenchCollection2481(b, shape)
	defer func() { _ = d.Close() }()
	b.ReportAllocs()
	reportColumnGraphScalarU8QuantizedBenchShapeMetrics1926(b, shape)
	b.ReportMetric(1, "quantized_indexes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status, err := col.RebuildVectorIndex(def.Name)
		if err != nil {
			b.Fatalf("RebuildVectorIndex: %v", err)
		}
		if !status.Loaded || status.RebuildNeeded {
			b.Fatalf("status=%+v, want loaded", status)
		}
		columnGraphRebuildBenchSinkV2A = status
	}
	b.StopTimer()
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "ops/sec")
	}
	reportColumnGraphBRQQuantizedStorageMetrics2481(b, d, def, shape)
}

func openColumnGraphScalarU8QuantizedBenchFixture1926(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926, quantized bool) columnGraphScalarU8QuantizedBenchFixture1926 {
	tb.Helper()
	var qdefs []QuantizedVectorIndexDefinition
	if quantized {
		qdefs = []QuantizedVectorIndexDefinition{{Name: columnGraphScalarU8QuantizedBenchIndexName1926}}
	}
	fixture := openColumnGraphScalarU8QuantizedBenchFixtureWithDefinitions2845(tb, shape, qdefs)
	if quantized {
		fixture.quantizedCodeBytesPerVector = shape.dims
	}
	return fixture
}

func openColumnGraphScalarU8AlphaQuantizedBenchFixture2845(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926) columnGraphScalarU8QuantizedBenchFixture1926 {
	tb.Helper()
	fixture := openColumnGraphScalarU8QuantizedBenchFixtureWithDefinitions2845(tb, shape, []QuantizedVectorIndexDefinition{columnGraphScalarU8AlphaQuantizedBenchIndexDefinition2845()})
	fixture.quantizedCodeBytesPerVector = shape.dims
	fixture.scalarU8Alpha = true
	return fixture
}

func openColumnGraphScalarU8QuantizedBenchFixtureWithDefinitions2845(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926, qdefs []QuantizedVectorIndexDefinition) columnGraphScalarU8QuantizedBenchFixture1926 {
	tb.Helper()
	_, d, col, def, rows := openColumnGraphScalarU8QuantizedBenchCollectionWithDefinitions2845(tb, shape, qdefs)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(tb, status, def.Name)
	queries, queryOrdinals := columnGraphScalarU8QuantizedBenchQueries1926(tb, shape, rows)
	fixture := columnGraphScalarU8QuantizedBenchFixture1926{
		close:         func() { _ = d.Close() },
		collection:    col,
		definition:    def,
		query:         append([]float32(nil), queries[0]...),
		queries:       queries,
		queryOrdinals: queryOrdinals,
		shape:         shape,
	}
	if len(def.QuantizedIndexes) > 0 {
		total, codes, alpha := columnGraphQuantizedAssetSetBytesForName2845(tb, d, def, def.QuantizedIndexes[0].Name)
		fixture.quantizedAssetBytes = total
		fixture.quantizedCodeAssetBytes = codes
		fixture.quantizedAlphaAssetBytes = alpha
	}
	return fixture
}

func openColumnGraphScalarU8AlphaQuantizedBenchFixture2844(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926) columnGraphScalarU8QuantizedBenchFixture1926 {
	tb.Helper()
	_, d, col, def, rows := openColumnGraphScalarU8AlphaQuantizedBenchCollection2844(tb, shape)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(tb, status, def.Name)
	queries, queryOrdinals := columnGraphScalarU8QuantizedBenchQueries1926(tb, shape, rows)
	return columnGraphScalarU8QuantizedBenchFixture1926{
		close:                       func() { _ = d.Close() },
		collection:                  col,
		definition:                  def,
		query:                       append([]float32(nil), queries[0]...),
		queries:                     queries,
		queryOrdinals:               queryOrdinals,
		shape:                       shape,
		quantizedAssetBytes:         columnGraphQuantizedAssetSetBytesForName2844(tb, d, def, columnGraphScalarU8QuantizedBenchIndexName1926),
		quantizedCodeBytesPerVector: shape.dims,
		scalarU8Alpha:               true,
	}
}

func columnGraphScalarU8AlphaQuantizedBenchIndex2844() QuantizedVectorIndexDefinition {
	return QuantizedVectorIndexDefinition{
		Name:  columnGraphScalarU8QuantizedBenchIndexName1926,
		Codec: QuantizedVectorCodecScalarU8,
		ScalarU8Calibration: &ScalarU8CalibrationConfig{
			Mode:     ScalarU8CalibrationModePerGranuleAlpha,
			Grouping: ScalarU8CalibrationGroupingStorageLayoutGranule,
			AlphaPolicy: ScalarU8AlphaPolicy{
				Name:        ScalarU8AlphaPolicyAbsQuantile,
				QuantilePPM: ScalarU8AlphaPolicyAbsQuantilePPM999,
			},
		},
	}
}

func openColumnGraphRabitQQuantizedBenchFixture2451(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926) columnGraphScalarU8QuantizedBenchFixture1926 {
	tb.Helper()
	_, d, col, def, rows := openColumnGraphRabitQQuantizedBenchCollection2450(tb, shape)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(tb, status, def.Name)
	queries, queryOrdinals := columnGraphScalarU8QuantizedBenchQueries1926(tb, shape, rows)
	plan, err := rabitq.NewPlan(shape.dims, rabitq.DefaultConfig())
	if err != nil {
		_ = d.Close()
		tb.Fatalf("rabitq.NewPlan: %v", err)
	}
	assetBytes := columnGraphQuantizedAssetBytesForName2451(tb, d, def, columnGraphRabitQQuantizedIndexName2450)
	return columnGraphScalarU8QuantizedBenchFixture1926{
		close:                       func() { _ = d.Close() },
		collection:                  col,
		definition:                  def,
		query:                       append([]float32(nil), queries[0]...),
		queries:                     queries,
		queryOrdinals:               queryOrdinals,
		shape:                       shape,
		quantizedAssetBytes:         assetBytes,
		quantizedCodeAssetBytes:     assetBytes,
		quantizedCodeBytesPerVector: plan.BytesPerCode(),
	}
}

func openColumnGraphBRQQuantizedBenchFixture2481(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926) columnGraphScalarU8QuantizedBenchFixture1926 {
	tb.Helper()
	_, d, col, def, rows := openColumnGraphBRQQuantizedBenchCollection2481(tb, shape)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(tb, status, def.Name)
	queries, queryOrdinals := columnGraphScalarU8QuantizedBenchQueries1926(tb, shape, rows)
	plan, err := brq.NewPlan(shape.dims, brq.DefaultConfig())
	if err != nil {
		_ = d.Close()
		tb.Fatalf("brq.NewPlan: %v", err)
	}
	assetBytes := columnGraphQuantizedAssetBytesForName2451(tb, d, def, columnGraphBRQQuantizedIndexName2481)
	return columnGraphScalarU8QuantizedBenchFixture1926{
		close:                       func() { _ = d.Close() },
		collection:                  col,
		definition:                  def,
		query:                       append([]float32(nil), queries[0]...),
		queries:                     queries,
		queryOrdinals:               queryOrdinals,
		shape:                       shape,
		quantizedAssetBytes:         assetBytes,
		quantizedCodeAssetBytes:     assetBytes,
		quantizedCodeBytesPerVector: plan.BytesPerCode(),
	}
}

func columnGraphScalarU8QuantizedBenchVectorIndexDefinition1926(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926) VectorIndexDefinition {
	tb.Helper()
	def := columnGraphRebuildVectorIndexDefinitionV2A(shape.dims, shape.m)
	def.EfConstruction = shape.efConstruction
	def.EfSearch = shape.efSearch
	normalized, err := normalizeVectorIndexDefinition(def)
	if err != nil {
		tb.Fatalf("normalize benchmark vector index definition: %v", err)
	}
	return normalized
}

func columnGraphScalarU8QuantizedBenchQueries1926(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926, rows []columnGraphRebuildInputRowV2A) ([][]float32, []int) {
	tb.Helper()
	if shape.queryCount <= 0 {
		tb.Fatalf("query count=%d must be positive", shape.queryCount)
	}
	queries := make([][]float32, shape.queryCount)
	ordinals := make([]int, shape.queryCount)
	for i := range queries {
		ordinal := shape.queryOrdinal
		if shape.queryCount > 1 && len(rows) > 0 {
			ordinal = (shape.queryOrdinal + i*7919) % len(rows)
		}
		if ordinal < 0 || ordinal >= len(rows) {
			tb.Fatalf("query ordinal=%d out of range rows=%d", ordinal, len(rows))
		}
		ordinals[i] = ordinal
		queries[i] = append([]float32(nil), rows[ordinal].vector...)
	}
	return queries, ordinals
}

func columnGraphScalarU8QuantizedBenchFixtureQueries1926(fixture columnGraphScalarU8QuantizedBenchFixture1926) [][]float32 {
	if len(fixture.queries) > 0 {
		return fixture.queries
	}
	return [][]float32{fixture.query}
}

func openColumnGraphBRQQuantizedBenchCollection2481(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926) (string, *backenddb.DB, *Collection, VectorIndexDefinition, []columnGraphRebuildInputRowV2A) {
	tb.Helper()
	if shape.rows <= 0 || shape.dims <= 0 {
		tb.Fatalf("invalid benchmark shape rows=%d dims=%d", shape.rows, shape.dims)
	}
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def := columnGraphScalarU8QuantizedBenchVectorIndexDefinition1926(tb, shape)
	def.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: columnGraphBRQQuantizedIndexName2481, Codec: brq.CodecName}}
	var err error
	def, err = normalizeVectorIndexDefinition(def)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    columnGraphRebuildColumnStoreConfigV2A(shape.dims),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	rows := columnGraphRebuildSyntheticRowsV2A(shape.rows, shape.dims)
	insertColumnGraphScalarU8QuantizedBenchRows1926(tb, col, shape, rows)
	return dir, d, col, def, rows
}

func openColumnGraphScalarU8AlphaQuantizedBenchCollection2844(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926) (string, *backenddb.DB, *Collection, VectorIndexDefinition, []columnGraphRebuildInputRowV2A) {
	tb.Helper()
	if shape.rows <= 0 || shape.dims <= 0 {
		tb.Fatalf("invalid benchmark shape rows=%d dims=%d", shape.rows, shape.dims)
	}
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def := columnGraphScalarU8QuantizedBenchVectorIndexDefinition1926(tb, shape)
	def.QuantizedIndexes = []QuantizedVectorIndexDefinition{columnGraphScalarU8AlphaQuantizedBenchIndex2844()}
	var err error
	def, err = normalizeVectorIndexDefinition(def)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    columnGraphRebuildColumnStoreConfigV2A(shape.dims),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	rows := columnGraphRebuildSyntheticRowsV2A(shape.rows, shape.dims)
	insertColumnGraphScalarU8QuantizedBenchRows1926(tb, col, shape, rows)
	return dir, d, col, def, rows
}

func openColumnGraphScalarU8QuantizedBenchCollection1926(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926, quantized bool) (string, *backenddb.DB, *Collection, VectorIndexDefinition, []columnGraphRebuildInputRowV2A) {
	tb.Helper()
	var qdefs []QuantizedVectorIndexDefinition
	if quantized {
		qdefs = []QuantizedVectorIndexDefinition{{Name: columnGraphScalarU8QuantizedBenchIndexName1926}}
	}
	return openColumnGraphScalarU8QuantizedBenchCollectionWithDefinitions2845(tb, shape, qdefs)
}

func openColumnGraphScalarU8AlphaQuantizedBenchCollection2845(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926) (string, *backenddb.DB, *Collection, VectorIndexDefinition, []columnGraphRebuildInputRowV2A) {
	tb.Helper()
	return openColumnGraphScalarU8QuantizedBenchCollectionWithDefinitions2845(tb, shape, []QuantizedVectorIndexDefinition{columnGraphScalarU8AlphaQuantizedBenchIndexDefinition2845()})
}

func openColumnGraphScalarU8QuantizedBenchCollectionWithDefinitions2845(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926, qdefs []QuantizedVectorIndexDefinition) (string, *backenddb.DB, *Collection, VectorIndexDefinition, []columnGraphRebuildInputRowV2A) {
	tb.Helper()
	if shape.rows <= 0 || shape.dims <= 0 {
		tb.Fatalf("invalid benchmark shape rows=%d dims=%d", shape.rows, shape.dims)
	}
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def := columnGraphScalarU8QuantizedBenchVectorIndexDefinition1926(tb, shape)
	if len(qdefs) > 0 {
		def.QuantizedIndexes = append([]QuantizedVectorIndexDefinition(nil), qdefs...)
		var err error
		def, err = normalizeVectorIndexDefinition(def)
		if err != nil {
			_ = d.Close()
			tb.Fatalf("normalizeVectorIndexDefinition: %v", err)
		}
	}
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    columnGraphRebuildColumnStoreConfigV2A(shape.dims),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	rows := columnGraphRebuildSyntheticRowsV2A(shape.rows, shape.dims)
	insertColumnGraphScalarU8QuantizedBenchRows1926(tb, col, shape, rows)
	return dir, d, col, def, rows
}

type columnGraphScalarU8QuantizedBenchmarkExactSet1926 struct {
	ids   map[string]struct{}
	count int
}

func columnGraphScalarU8QuantizedBenchmarkExactIDs1926(tb testing.TB, fixture columnGraphScalarU8QuantizedBenchFixture1926) (map[string]struct{}, int) {
	tb.Helper()
	exactSets := columnGraphScalarU8QuantizedBenchmarkExactSets1926(tb, fixture)
	return exactSets[0].ids, exactSets[0].count
}

func columnGraphScalarU8QuantizedBenchmarkExactSets1926(tb testing.TB, fixture columnGraphScalarU8QuantizedBenchFixture1926) []columnGraphScalarU8QuantizedBenchmarkExactSet1926 {
	tb.Helper()
	queries := columnGraphScalarU8QuantizedBenchFixtureQueries1926(fixture)
	searcher, err := fixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.definition.Name, MaxDecodedBlocks: 1})
	if err != nil {
		tb.Fatalf("OpenVectorIndexSearcher exact reference: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	sets := make([]columnGraphScalarU8QuantizedBenchmarkExactSet1926, len(queries))
	var buffer VectorIndexSearchBuffer
	for i, query := range queries {
		response, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeExact, TopK: fixture.shape.topK, EfSearch: fixture.shape.efSearch}, &buffer)
		if err != nil {
			tb.Fatalf("exact reference SearchWithBuffer query %d: %v", i, err)
		}
		if len(response.Results) == 0 {
			tb.Fatalf("exact reference query %d returned no results", i)
		}
		ids := make(map[string]struct{}, len(response.Results))
		for _, result := range response.Results {
			ids[string(result.ID)] = struct{}{}
		}
		sets[i] = columnGraphScalarU8QuantizedBenchmarkExactSet1926{ids: ids, count: len(response.Results)}
	}
	return sets
}

func columnGraphScalarU8QuantizedBenchmarkRecallAtK1926(results []VectorIndexSearchResult, exactIDs map[string]struct{}, exactCount int) float64 {
	if exactCount == 0 {
		return 0
	}
	overlap := 0
	for _, result := range results {
		if _, ok := exactIDs[string(result.ID)]; ok {
			overlap++
		}
	}
	return float64(overlap) / float64(exactCount)
}

func addColumnGraphScalarU8QuantizedBenchmarkStats1926(dst *VectorIndexSearchStats, src VectorIndexSearchStats) {
	dst.CandidateRows += src.CandidateRows
	dst.Candidates += src.Candidates
	dst.Edges += src.Edges
	dst.VisitedNodes += src.VisitedNodes
	dst.VisitedEdges += src.VisitedEdges
	dst.VectorBytesRead += src.VectorBytesRead
	dst.NormBytesRead += src.NormBytesRead
	dst.AdjacencyBytesRead += src.AdjacencyBytesRead
	dst.CandidateFetches += src.CandidateFetches
	dst.ExpansionFetches += src.ExpansionFetches
	dst.ResultFetches += src.ResultFetches
	dst.DocumentsFetched += src.DocumentsFetched
	dst.ScoreBatchCalls += src.ScoreBatchCalls
	dst.ScoreBatchCandidates += src.ScoreBatchCandidates
	if src.ScoreBatchMaxTileSize > dst.ScoreBatchMaxTileSize {
		dst.ScoreBatchMaxTileSize = src.ScoreBatchMaxTileSize
	}
	dst.ScoreBatchOptimizedCalls += src.ScoreBatchOptimizedCalls
	dst.ScoreBatchScalarFallbackCalls += src.ScoreBatchScalarFallbackCalls
	dst.PreparedScoreCalls += src.PreparedScoreCalls
	dst.QuantizedScoreCalls += src.QuantizedScoreCalls
	dst.QuantizedCodeBytesRead += src.QuantizedCodeBytesRead
	dst.QuantizedRerankCandidates += src.QuantizedRerankCandidates
	dst.QuantizedRerankExactScoreCalls += src.QuantizedRerankExactScoreCalls
	dst.QuantizedScorerActive += src.QuantizedScorerActive
	dst.QuantizedAssetMissing += src.QuantizedAssetMissing
	dst.QuantizedAssetInvalid += src.QuantizedAssetInvalid
	dst.QuantizedAssetStale += src.QuantizedAssetStale
	dst.QuantizedAssetClosed += src.QuantizedAssetClosed
	dst.QuantizedAssetUnavailable += src.QuantizedAssetUnavailable
	dst.QuantizedAssetMmapDirect += src.QuantizedAssetMmapDirect
	dst.QuantizedAssetHeapCopy += src.QuantizedAssetHeapCopy
	dst.QuantizedAssetOpenNanos += src.QuantizedAssetOpenNanos
	dst.QuantizedAssetMappedBytes += src.QuantizedAssetMappedBytes
	dst.QuantizedAssetHeapCopyBytes += src.QuantizedAssetHeapCopyBytes
	dst.QuantizedAssetActiveHandles += src.QuantizedAssetActiveHandles
	dst.QuantizedScoreCodecScalarU8Alpha += src.QuantizedScoreCodecScalarU8Alpha
	dst.QuantizedScoreCodecBRQ1Bit += src.QuantizedScoreCodecBRQ1Bit
	dst.BRQ1BitQueryWeightBits += src.BRQ1BitQueryWeightBits
	dst.BRQ1BitBitProductPasses += src.BRQ1BitBitProductPasses
	dst.BRQ1BitQueryWeightScale += src.BRQ1BitQueryWeightScale
	dst.ScoreFloat64Fallbacks += src.ScoreFloat64Fallbacks
	dst.PreparedGraphSearchViews += src.PreparedGraphSearchViews
	dst.GraphRowFallbacks += src.GraphRowFallbacks
	dst.SearchRouteColumnGraphPrepared += src.SearchRouteColumnGraphPrepared
	dst.SearchRouteColumnGraphFallback += src.SearchRouteColumnGraphFallback
	dst.SearchRouteHNSWSearchPack += src.SearchRouteHNSWSearchPack
	dst.SearchRouteQuantizedOnly += src.SearchRouteQuantizedOnly
	dst.SearchRouteQuantizedRerank += src.SearchRouteQuantizedRerank
	dst.HNSWSearchPackActive += src.HNSWSearchPackActive
	dst.HNSWSearchPackMissing += src.HNSWSearchPackMissing
	dst.HNSWSearchPackInvalid += src.HNSWSearchPackInvalid
	dst.HNSWSearchPackStale += src.HNSWSearchPackStale
	dst.HNSWSearchPackClosed += src.HNSWSearchPackClosed
	dst.HNSWSearchPackFallbacks += src.HNSWSearchPackFallbacks
	dst.HNSWSearchPackMmapDirect += src.HNSWSearchPackMmapDirect
	dst.HNSWSearchPackHeapCopy += src.HNSWSearchPackHeapCopy
	dst.HNSWSearchPackOpenNanos += src.HNSWSearchPackOpenNanos
	dst.HNSWSearchPackMappedBytes += src.HNSWSearchPackMappedBytes
	dst.HNSWSearchPackHeapCopyBytes += src.HNSWSearchPackHeapCopyBytes
	dst.HNSWSearchPackActiveHandles += src.HNSWSearchPackActiveHandles
	dst.TypedColumnFallbacks += src.TypedColumnFallbacks
	dst.VectorScratchDecodes += src.VectorScratchDecodes
	dst.AdjacencyLegacyFallbacks += src.AdjacencyLegacyFallbacks
	dst.AdjacencySourceFallbacks += src.AdjacencySourceFallbacks
	dst.ResultIDGraphFallbacks += src.ResultIDGraphFallbacks
	dst.ResultIDTypedBytesState += src.ResultIDTypedBytesState
	dst.RowRefVectorSourceState += src.RowRefVectorSourceState
	dst.RowRefVectorSourceLegacyGraphIDs += src.RowRefVectorSourceLegacyGraphIDs
	dst.BenchmarkDebugSearches += src.BenchmarkDebugSearches
	dst.NeighborTiles += src.NeighborTiles
	dst.NeighborTileNeighbors += src.NeighborTileNeighbors
	if src.NeighborTileMaxSize > dst.NeighborTileMaxSize {
		dst.NeighborTileMaxSize = src.NeighborTileMaxSize
	}
	dst.ScoredNeighbors += src.ScoredNeighbors
	dst.SkippedNeighbors += src.SkippedNeighbors
	dst.AlreadyVisitedSkips += src.AlreadyVisitedSkips
	dst.FilterSkips += src.FilterSkips
	dst.UpperLayerScores += src.UpperLayerScores
	dst.UpperLayerScoreTiles += src.UpperLayerScoreTiles
	dst.UpperLayerScoreTileCandidates += src.UpperLayerScoreTileCandidates
	if src.UpperLayerScoreTileMaxSize > dst.UpperLayerScoreTileMaxSize {
		dst.UpperLayerScoreTileMaxSize = src.UpperLayerScoreTileMaxSize
	}
	dst.UpperLayerAdjacencyLoads += src.UpperLayerAdjacencyLoads
	dst.UpperLayerAdjacencyNeighbors += src.UpperLayerAdjacencyNeighbors
	dst.UpperLayerEdgeVisits += src.UpperLayerEdgeVisits
	dst.Layer0Scores += src.Layer0Scores
	dst.Layer0ScoreTiles += src.Layer0ScoreTiles
	dst.Layer0ScoreTileCandidates += src.Layer0ScoreTileCandidates
	if src.Layer0ScoreTileMaxSize > dst.Layer0ScoreTileMaxSize {
		dst.Layer0ScoreTileMaxSize = src.Layer0ScoreTileMaxSize
	}
	dst.Layer0AdjacencyLoads += src.Layer0AdjacencyLoads
	dst.Layer0AdjacencyNeighbors += src.Layer0AdjacencyNeighbors
	dst.Layer0EdgeVisits += src.Layer0EdgeVisits
	dst.Layer0ScoredNeighbors += src.Layer0ScoredNeighbors
	dst.Layer0AlreadyVisitedSkips += src.Layer0AlreadyVisitedSkips
	dst.Layer0FilterSkips += src.Layer0FilterSkips
	dst.Layer0StopChecks += src.Layer0StopChecks
	dst.Layer0StopTrue += src.Layer0StopTrue
	dst.Layer0StopFalse += src.Layer0StopFalse
	dst.CandidateComparisons += src.CandidateComparisons
	dst.FrontierComparisons += src.FrontierComparisons
	dst.TopKComparisons += src.TopKComparisons
	dst.FrontierPushes += src.FrontierPushes
	dst.FrontierPops += src.FrontierPops
	dst.FrontierPopMisses += src.FrontierPopMisses
	dst.FrontierSiftUpCalls += src.FrontierSiftUpCalls
	dst.FrontierSiftDownCalls += src.FrontierSiftDownCalls
	dst.FrontierSiftUpSteps += src.FrontierSiftUpSteps
	dst.FrontierSiftDownSteps += src.FrontierSiftDownSteps
	dst.TopKInsertAttempts += src.TopKInsertAttempts
	dst.TopKInsertSuccesses += src.TopKInsertSuccesses
	dst.TopKInsertRejections += src.TopKInsertRejections
	dst.TopKHeapSiftSteps += src.TopKHeapSiftSteps
	dst.VisitedMarkChecks += src.VisitedMarkChecks
	dst.VisitedMarkHits += src.VisitedMarkHits
	dst.VisitedMarkMisses += src.VisitedMarkMisses
	dst.VisitedMarkInserts += src.VisitedMarkInserts
	dst.VisitedResetEpochAdvances += src.VisitedResetEpochAdvances
	dst.VisitedResetClearedRows += src.VisitedResetClearedRows
}

func reportColumnGraphScalarU8QuantizedBenchShapeMetrics1926(b *testing.B, shape columnGraphScalarU8QuantizedBenchShape1926) {
	b.Helper()
	b.ReportMetric(float64(shape.rows), "rows")
	b.ReportMetric(float64(shape.rows), "docs/index")
	b.ReportMetric(float64(shape.dims), "dims")
	b.ReportMetric(float64(shape.m), "degree")
	b.ReportMetric(float64(shape.m), "hnsw_m")
	b.ReportMetric(float64(shape.efConstruction), "ef_construction")
	b.ReportMetric(float64(shape.topK), "top_k")
	b.ReportMetric(float64(shape.efSearch), "ef_search")
	b.ReportMetric(float64(shape.queryCount), "queries")
}

func reportColumnGraphScalarU8QuantizedScorePlaneMetrics1926(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, stats VectorIndexSearchStats, recallSum float64) {
	b.Helper()
	reportColumnGraphScalarU8QuantizedBenchShapeMetrics1926(b, fixture.shape)
	if fixture.scalarU8Alpha {
		b.ReportMetric(1, "per_granule_alpha_row")
		b.ReportMetric(1, "scalar_u8_alpha_row")
	}
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "ops/sec")
	}
	denom := float64(b.N)
	if denom == 0 {
		denom = 1
	}
	b.ReportMetric((recallSum/denom)*100, "recall_at_k_pct")
	b.ReportMetric(float64(stats.CandidateRows)/denom, "candidate_rows/search")
	b.ReportMetric(float64(stats.Candidates)/denom, "candidates/search")
	b.ReportMetric(float64(stats.Edges)/denom, "edges/search")
	b.ReportMetric(float64(stats.VisitedNodes)/denom, "visited_nodes/search")
	b.ReportMetric(float64(stats.VisitedEdges)/denom, "visited_edges/search")
	b.ReportMetric(float64(stats.CandidateFetches)/denom, "candidate_fetches/search")
	b.ReportMetric(float64(stats.ExpansionFetches)/denom, "expansion_fetches/search")
	b.ReportMetric(float64(stats.ResultFetches)/denom, "result_fetches/search")
	b.ReportMetric(float64(stats.DocumentsFetched)/denom, "docs_fetched/search")
	b.ReportMetric(float64(stats.VectorBytesRead)/denom, "vector_B/search")
	b.ReportMetric(float64(stats.NormBytesRead)/denom, "norm_B/search")
	b.ReportMetric(float64(stats.AdjacencyBytesRead)/denom, "adjacency_B/search")
	b.ReportMetric(float64(stats.ScoreBatchCalls)/denom, "score_batch_calls/search")
	b.ReportMetric(float64(stats.ScoreBatchCandidates)/denom, "score_batch_candidates/search")
	b.ReportMetric(float64(stats.ScoreBatchMaxTileSize), "score_batch_max_tile_size")
	b.ReportMetric(float64(stats.ScoreBatchOptimizedCalls)/denom, "score_batch_optimized/search")
	b.ReportMetric(float64(stats.ScoreBatchScalarFallbackCalls)/denom, "score_batch_fallback/search")
	b.ReportMetric(float64(stats.PreparedScoreCalls)/denom, "prepared_score_calls/search")
	b.ReportMetric(float64(stats.QuantizedScoreCalls)/denom, "quantized_score_calls/search")
	b.ReportMetric(float64(stats.QuantizedCodeBytesRead)/denom, "quantized_code_B/search")
	b.ReportMetric(float64(stats.QuantizedRerankCandidates)/denom, "quantized_rerank_candidates/search")
	b.ReportMetric(float64(stats.QuantizedRerankExactScoreCalls)/denom, "quantized_rerank_exact_score_calls/search")
	b.ReportMetric(float64(stats.QuantizedScorerActive)/denom, "quantized_scorer_active/search")
	b.ReportMetric(float64(stats.QuantizedAssetMissing)/denom, "quantized_asset_missing/search")
	b.ReportMetric(float64(stats.QuantizedAssetInvalid)/denom, "quantized_asset_invalid/search")
	b.ReportMetric(float64(stats.QuantizedAssetStale)/denom, "quantized_asset_stale/search")
	b.ReportMetric(float64(stats.QuantizedAssetClosed)/denom, "quantized_asset_closed/search")
	b.ReportMetric(float64(stats.QuantizedAssetUnavailable)/denom, "quantized_asset_unavailable/search")
	b.ReportMetric(float64(stats.QuantizedAssetMmapDirect)/denom, "quantized_asset_mmap_direct/search")
	b.ReportMetric(float64(stats.QuantizedAssetHeapCopy)/denom, "quantized_asset_heap_copy/search")
	b.ReportMetric(float64(stats.QuantizedAssetOpenNanos)/denom, "quantized_asset_open_ns")
	b.ReportMetric(float64(stats.QuantizedAssetMappedBytes)/denom, "quantized_asset_mapped_B")
	b.ReportMetric(float64(stats.QuantizedAssetHeapCopyBytes)/denom, "quantized_asset_heap_copy_B")
	b.ReportMetric(float64(stats.QuantizedAssetActiveHandles)/denom, "quantized_asset_active_handles")
	b.ReportMetric(float64(stats.QuantizedScoreCodecScalarU8Alpha)/denom, "QuantizedScoreCodecScalarU8Alpha/search")
	b.ReportMetric(float64(stats.QuantizedScoreCodecScalarU8Alpha)/denom, "quantized_score_codec_scalar_u8_alpha/search")
	b.ReportMetric(float64(stats.QuantizedScoreCodecBRQ1Bit)/denom, "quantized_score_codec_brq_1bit/search")
	b.ReportMetric(float64(stats.BRQ1BitQueryWeightBits)/denom, "brq_1bit_query_weight_bits/search")
	b.ReportMetric(float64(stats.BRQ1BitBitProductPasses)/denom, "brq_1bit_bitproduct_passes/search")
	b.ReportMetric(stats.BRQ1BitQueryWeightScale/denom, "brq_1bit_query_weight_scale/search")
	b.ReportMetric(float64(stats.ScoreFloat64Fallbacks)/denom, "score_float64_fallbacks/search")
	b.ReportMetric(float64(stats.PreparedGraphSearchViews)/denom, "prepared_graph_search_views/search")
	b.ReportMetric(float64(stats.GraphRowFallbacks)/denom, "graph_row_fallbacks/search")
	b.ReportMetric(float64(stats.SearchRouteColumnGraphPrepared)/denom, "search_route_column_graph_prepared/search")
	b.ReportMetric(float64(stats.SearchRouteColumnGraphFallback)/denom, "search_route_column_graph_fallback/search")
	b.ReportMetric(float64(stats.SearchRouteHNSWSearchPack)/denom, "search_route_hnsw_search_pack/search")
	b.ReportMetric(float64(stats.SearchRouteQuantizedOnly)/denom, "search_route_quantized_only/search")
	b.ReportMetric(float64(stats.SearchRouteQuantizedRerank)/denom, "search_route_quantized_rerank/search")
	b.ReportMetric(float64(stats.HNSWSearchPackActive)/denom, "hnsw_search_pack_active/search")
	b.ReportMetric(float64(stats.HNSWSearchPackMissing)/denom, "hnsw_search_pack_missing/search")
	b.ReportMetric(float64(stats.HNSWSearchPackInvalid)/denom, "hnsw_search_pack_invalid/search")
	b.ReportMetric(float64(stats.HNSWSearchPackStale)/denom, "hnsw_search_pack_stale/search")
	b.ReportMetric(float64(stats.HNSWSearchPackClosed)/denom, "hnsw_search_pack_closed/search")
	b.ReportMetric(float64(stats.HNSWSearchPackFallbacks)/denom, "hnsw_search_pack_fallbacks/search")
	b.ReportMetric(float64(stats.HNSWSearchPackMmapDirect)/denom, "hnsw_search_pack_mmap_direct/search")
	b.ReportMetric(float64(stats.HNSWSearchPackHeapCopy)/denom, "hnsw_search_pack_heap_copy/search")
	b.ReportMetric(float64(stats.HNSWSearchPackOpenNanos)/denom, "hnsw_search_pack_open_ns")
	b.ReportMetric(float64(stats.HNSWSearchPackMappedBytes)/denom, "hnsw_search_pack_mapped_B")
	b.ReportMetric(float64(stats.HNSWSearchPackHeapCopyBytes)/denom, "hnsw_search_pack_heap_copy_B")
	b.ReportMetric(float64(stats.HNSWSearchPackActiveHandles)/denom, "hnsw_search_pack_active_handles")
	b.ReportMetric(float64(stats.TypedColumnFallbacks)/denom, "typed_column_vector_fallbacks/search")
	b.ReportMetric(float64(stats.VectorScratchDecodes)/denom, "vector_scratch_decodes/search")
	b.ReportMetric(float64(stats.AdjacencyLegacyFallbacks)/denom, "adjacency_legacy_fallbacks/search")
	b.ReportMetric(float64(stats.AdjacencySourceFallbacks)/denom, "adjacency_source_fallbacks/search")
	b.ReportMetric(float64(stats.ResultIDGraphFallbacks)/denom, "result_id_graph_fallbacks/search")
	b.ReportMetric(float64(stats.ResultIDTypedBytesState)/denom, "result_id_typed_bytes_state/search")
	b.ReportMetric(float64(stats.RowRefVectorSourceState)/denom, "row_ref_vector_source_state/search")
	b.ReportMetric(float64(stats.RowRefVectorSourceLegacyGraphIDs)/denom, "row_ref_vector_source_legacy_graph_ids/search")
	quantizedCodeBytes := fixture.quantizedCodeBytesPerVector
	if quantizedCodeBytes == 0 {
		quantizedCodeBytes = fixture.shape.dims
	}
	b.ReportMetric(float64(quantizedCodeBytes), "quantized_code_B/vector")
	b.ReportMetric(float64(fixture.shape.dims*4), "exact_vector_B/vector")
	b.ReportMetric(4, "exact_norm_B/vector")
	b.ReportMetric(float64(fixture.shape.dims*4+4), "exact_vector_norm_B/vector")
	b.ReportMetric(float64(fixture.quantizedAssetBytes), "quantized_asset_B_total")
	b.ReportMetric(float64(fixture.quantizedCodeAssetBytes), "quantized_code_asset_B_total")
	b.ReportMetric(float64(fixture.quantizedAlphaAssetBytes), "quantized_alpha_asset_B_total")
	if fixture.shape.rows > 0 {
		b.ReportMetric(float64(fixture.quantizedAssetBytes)/float64(fixture.shape.rows), "quantized_asset_B/vector")
		b.ReportMetric(float64(fixture.quantizedCodeAssetBytes)/float64(fixture.shape.rows), "quantized_code_asset_B/vector")
		b.ReportMetric(float64(fixture.quantizedAlphaAssetBytes)/float64(fixture.shape.rows), "quantized_alpha_asset_B/vector")
	}
}

func reportColumnGraphScalarU8QuantizedTraversalCounterMetrics2271(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, stats VectorIndexSearchStats) {
	b.Helper()
	reportColumnGraphScalarU8QuantizedBenchShapeMetrics1926(b, fixture.shape)
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "ops/sec")
	}
	denom := float64(b.N)
	if denom == 0 {
		denom = 1
	}
	b.ReportMetric(2271, "traversal_counter_issue")
	b.ReportMetric(float64(stats.BenchmarkDebugSearches)/denom, "benchmark_debug_searches/search")
	b.ReportMetric(float64(stats.Candidates)/denom, "candidates/search")
	b.ReportMetric(float64(stats.VisitedEdges)/denom, "visited_edges/search")
	b.ReportMetric(float64(stats.NeighborTiles)/denom, "adjacency_layer_loads/search")
	b.ReportMetric(float64(stats.NeighborTileNeighbors)/denom, "adjacency_loaded_neighbors/search")
	b.ReportMetric(float64(stats.NeighborTileMaxSize), "adjacency_layer_max_neighbors")
	b.ReportMetric(float64(stats.UpperLayerAdjacencyLoads)/denom, "upper_layer_adjacency_loads/search")
	b.ReportMetric(float64(stats.UpperLayerAdjacencyNeighbors)/denom, "upper_layer_adjacency_neighbors/search")
	b.ReportMetric(float64(stats.UpperLayerEdgeVisits)/denom, "upper_layer_neighbors_iterated/search")
	b.ReportMetric(float64(stats.Layer0AdjacencyLoads)/denom, "layer0_adjacency_loads/search")
	b.ReportMetric(float64(stats.Layer0AdjacencyNeighbors)/denom, "layer0_adjacency_neighbors/search")
	b.ReportMetric(float64(stats.Layer0EdgeVisits)/denom, "layer0_neighbors_iterated/search")
	b.ReportMetric(float64(stats.AlreadyVisitedSkips)/denom, "already_visited_skips/search")
	b.ReportMetric(float64(stats.Layer0AlreadyVisitedSkips)/denom, "layer0_already_visited_skips/search")
	b.ReportMetric(float64(stats.UpperLayerScoreTiles)/denom, "upper_layer_score_tiles/search")
	b.ReportMetric(float64(stats.UpperLayerScoreTileCandidates)/denom, "upper_layer_score_tile_candidates/search")
	b.ReportMetric(float64(stats.UpperLayerScoreTileMaxSize), "upper_layer_score_tile_max_size")
	b.ReportMetric(float64(stats.Layer0ScoreTiles)/denom, "layer0_score_tiles/search")
	b.ReportMetric(float64(stats.Layer0ScoreTileCandidates)/denom, "layer0_score_tile_candidates/search")
	b.ReportMetric(float64(stats.Layer0ScoreTileMaxSize), "layer0_score_tile_max_size")
	b.ReportMetric(float64(stats.CandidateComparisons)/denom, "candidate_comparisons/search")
	b.ReportMetric(float64(stats.FrontierComparisons)/denom, "frontier_comparisons/search")
	b.ReportMetric(float64(stats.TopKComparisons)/denom, "top_k_comparisons/search")
	b.ReportMetric(float64(stats.FrontierPushes)/denom, "frontier_pushes/search")
	b.ReportMetric(float64(stats.FrontierPops)/denom, "frontier_pops/search")
	b.ReportMetric(float64(stats.FrontierPopMisses)/denom, "frontier_pop_misses/search")
	b.ReportMetric(float64(stats.FrontierSiftUpCalls)/denom, "frontier_sift_up_calls/search")
	b.ReportMetric(float64(stats.FrontierSiftDownCalls)/denom, "frontier_sift_down_calls/search")
	b.ReportMetric(float64(stats.FrontierSiftUpSteps)/denom, "frontier_sift_up_steps/search")
	b.ReportMetric(float64(stats.FrontierSiftDownSteps)/denom, "frontier_sift_down_steps/search")
	b.ReportMetric(float64(stats.TopKInsertAttempts)/denom, "top_k_insert_attempts/search")
	b.ReportMetric(float64(stats.TopKInsertSuccesses)/denom, "top_k_insert_successes/search")
	b.ReportMetric(float64(stats.TopKInsertRejections)/denom, "top_k_insert_rejections/search")
	b.ReportMetric(float64(stats.TopKHeapSiftSteps)/denom, "top_k_heap_sift_steps/search")
	b.ReportMetric(float64(stats.VisitedMarkChecks)/denom, "visited_mark_checks/search")
	b.ReportMetric(float64(stats.VisitedMarkHits)/denom, "visited_mark_hits/search")
	b.ReportMetric(float64(stats.VisitedMarkMisses)/denom, "visited_mark_misses/search")
	b.ReportMetric(float64(stats.VisitedMarkInserts)/denom, "visited_mark_inserts/search")
	b.ReportMetric(float64(stats.VisitedResetEpochAdvances)/denom, "visited_reset_epoch_advances/search")
	b.ReportMetric(float64(stats.VisitedResetClearedRows)/denom, "visited_reset_cleared_rows/search")
	b.ReportMetric(float64(stats.Layer0StopChecks)/denom, "layer0_stop_checks/search")
	b.ReportMetric(float64(stats.Layer0StopTrue)/denom, "layer0_stop_true/search")
	b.ReportMetric(float64(stats.Layer0StopFalse)/denom, "layer0_stop_false/search")
	if stats.VisitedNodes > 0 {
		b.ReportMetric(float64(stats.VisitedEdges)/float64(stats.VisitedNodes), "edges_per_visited_node")
	}
}

func reportColumnGraphScalarU8QuantizedStorageMetrics1926(b *testing.B, d *backenddb.DB, def VectorIndexDefinition, shape columnGraphScalarU8QuantizedBenchShape1926) {
	b.Helper()
	graph, rows := loadAndScanColumnGraphRebuildRowsV2A(b, d, "docs", def)
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(b, d, "docs")
	state := columnVectorIndexStateFromRecords1987(b, records, def)
	b.ReportMetric(float64(graph.AssetBytes), "graph_asset_B/op")
	b.ReportMetric(float64(columnVectorIndexStateAssetsStorageBytes(state)), "state_assets_B/op")
	b.ReportMetric(float64(columnVectorGraphStorageBytesWithState(graph, state)), "graph_total_storage_B/op")
	b.ReportMetric(float64(shape.dims), "quantized_code_B/vector")
	b.ReportMetric(float64(shape.dims*4), "exact_vector_B/vector")
	b.ReportMetric(4, "exact_norm_B/vector")
	b.ReportMetric(float64(shape.dims*4+4), "exact_vector_norm_B/vector")
	var quantizedAssets, quantizedCodeAssets, quantizedAlphaAssets int
	var quantizedBytes, quantizedCodeBytes, quantizedAlphaBytes int64
	for _, asset := range state.Assets {
		switch asset.Role {
		case columnVectorIndexStateAssetRoleQuantizedCodes:
			quantizedAssets++
			quantizedCodeAssets++
			quantizedBytes += asset.AssetBytes
			quantizedCodeBytes += asset.AssetBytes
		case columnVectorIndexStateAssetRoleQuantizedAlpha:
			quantizedAssets++
			quantizedAlphaAssets++
			quantizedBytes += asset.AssetBytes
			quantizedAlphaBytes += asset.AssetBytes
		}
	}
	b.ReportMetric(float64(quantizedAssets), "quantized_assets/op")
	b.ReportMetric(float64(quantizedCodeAssets), "quantized_code_assets/op")
	b.ReportMetric(float64(quantizedAlphaAssets), "quantized_alpha_assets/op")
	b.ReportMetric(float64(quantizedBytes), "quantized_asset_B/op")
	b.ReportMetric(float64(quantizedCodeBytes), "quantized_code_asset_B/op")
	b.ReportMetric(float64(quantizedAlphaBytes), "quantized_alpha_asset_B/op")
	if shape.rows > 0 {
		b.ReportMetric(float64(quantizedBytes)/float64(shape.rows), "quantized_asset_B/vector")
		b.ReportMetric(float64(quantizedCodeBytes)/float64(shape.rows), "quantized_code_asset_B/vector")
		b.ReportMetric(float64(quantizedAlphaBytes)/float64(shape.rows), "quantized_alpha_asset_B/vector")
	}
	if metrics, ok := columnGraphScalarU8AlphaBenchMetrics2845(b, def, rows); ok {
		b.ReportMetric(float64(metrics.granules), "scalar_u8_alpha_granules")
		b.ReportMetric(metrics.alphaMin, "scalar_u8_alpha_min")
		b.ReportMetric(metrics.alphaMean, "scalar_u8_alpha_mean")
		b.ReportMetric(metrics.alphaMax, "scalar_u8_alpha_max")
		b.ReportMetric(metrics.codeBoundaryPct, "scalar_u8_alpha_code_boundary_pct")
	}
}

type columnGraphScalarU8AlphaBenchMetricSummary2845 struct {
	granules        int
	alphaMin        float64
	alphaMean       float64
	alphaMax        float64
	codeBoundaryPct float64
}

func columnGraphScalarU8AlphaBenchMetrics2845(tb testing.TB, def VectorIndexDefinition, rows []columnGraphRebuildScannedRowV2A) (columnGraphScalarU8AlphaBenchMetricSummary2845, bool) {
	tb.Helper()
	var q QuantizedVectorIndexDefinition
	found := false
	for _, candidate := range def.QuantizedIndexes {
		if candidate.Codec == QuantizedVectorCodecScalarU8 && !scalarU8CalibrationIsLegacy(candidate) {
			q = candidate
			found = true
			break
		}
	}
	if !found {
		return columnGraphScalarU8AlphaBenchMetricSummary2845{}, false
	}
	assetRows := make([]columnVectorGraphAssetRow, len(rows))
	for i, row := range rows {
		assetRows[i] = columnVectorGraphAssetRow{ID: []byte(row.id), Vector: row.vector, InvNorm: row.invNorm, Adjacency: row.adjacency}
	}
	metadata, err := buildColumnVectorGraphScalarU8AlphaMetadata(def, q, assetRows)
	if err != nil {
		tb.Fatalf("build scalar_u8 alpha benchmark metadata: %v", err)
	}
	if len(metadata.Alphas) == 0 {
		return columnGraphScalarU8AlphaBenchMetricSummary2845{}, false
	}
	metrics := columnGraphScalarU8AlphaBenchMetricSummary2845{granules: len(metadata.Alphas)}
	var alphaSum float64
	for i, alpha := range metadata.Alphas {
		value := float64(alpha)
		if i == 0 || value < metrics.alphaMin {
			metrics.alphaMin = value
		}
		if i == 0 || value > metrics.alphaMax {
			metrics.alphaMax = value
		}
		alphaSum += value
	}
	metrics.alphaMean = alphaSum / float64(len(metadata.Alphas))
	var rowStart int
	var boundary, total uint64
	for granule, count := range metadata.RowCounts {
		alpha := metadata.Alphas[granule]
		rowEnd := rowStart + int(count)
		for _, row := range rows[rowStart:rowEnd] {
			for dim := 0; dim < def.Dimensions; dim++ {
				code := columnVectorGraphScalarU8Code(row.vector[dim] * row.invNorm / alpha)
				if code == 0 || code == 255 {
					boundary++
				}
				total++
			}
		}
		rowStart = rowEnd
	}
	if total > 0 {
		metrics.codeBoundaryPct = (float64(boundary) / float64(total)) * 100
	}
	return metrics, true
}

func reportColumnGraphBRQQuantizedStorageMetrics2481(b *testing.B, d *backenddb.DB, def VectorIndexDefinition, shape columnGraphScalarU8QuantizedBenchShape1926) {
	b.Helper()
	graph, _ := loadAndScanColumnGraphRebuildRowsV2A(b, d, "docs", def)
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(b, d, "docs")
	state := columnVectorIndexStateFromRecords1987(b, records, def)
	b.ReportMetric(float64(graph.AssetBytes), "graph_asset_B/op")
	b.ReportMetric(float64(columnVectorIndexStateAssetsStorageBytes(state)), "state_assets_B/op")
	b.ReportMetric(float64(columnVectorGraphStorageBytesWithState(graph, state)), "graph_total_storage_B/op")
	plan, err := brq.NewPlan(shape.dims, brq.DefaultConfig())
	if err != nil {
		b.Fatalf("brq.NewPlan: %v", err)
	}
	b.ReportMetric(float64(plan.BytesPerCode()), "quantized_code_B/vector")
	b.ReportMetric(float64(shape.dims*4), "exact_vector_B/vector")
	b.ReportMetric(4, "exact_norm_B/vector")
	b.ReportMetric(float64(shape.dims*4+4), "exact_vector_norm_B/vector")
	var quantizedAssets int
	var quantizedBytes int64
	for _, asset := range state.Assets {
		if asset.Role == columnVectorIndexStateAssetRoleQuantizedCodes {
			quantizedAssets++
			quantizedBytes += asset.AssetBytes
		}
	}
	b.ReportMetric(float64(quantizedAssets), "quantized_assets/op")
	b.ReportMetric(float64(quantizedBytes), "quantized_asset_B/op")
	if shape.rows > 0 {
		b.ReportMetric(float64(quantizedBytes)/float64(shape.rows), "quantized_asset_B/vector")
	}
	if want := (plan.CodeDimensions() + 7) / 8; want != plan.BytesPerCode() {
		b.Fatalf("logical code bytes mismatch got ceil=%d bytes=%d", want, plan.BytesPerCode())
	}
}

func columnGraphScalarU8QuantizedAssetBytes1926(tb testing.TB, d *backenddb.DB, def VectorIndexDefinition) int64 {
	tb.Helper()
	return columnGraphQuantizedAssetBytesForName2451(tb, d, def, columnGraphScalarU8QuantizedBenchIndexName1926)
}

func columnGraphQuantizedAssetBytesForName2451(tb testing.TB, d *backenddb.DB, def VectorIndexDefinition, quantizedIndexName string) int64 {
	tb.Helper()
	_, codes, _ := columnGraphQuantizedAssetSetBytesForName2845(tb, d, def, quantizedIndexName)
	return codes
}

func columnGraphQuantizedAssetSetBytesForName2845(tb testing.TB, d *backenddb.DB, def VectorIndexDefinition, quantizedIndexName string) (total int64, codes int64, alpha int64) {
	tb.Helper()
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(tb, d, "docs")
	state := columnVectorIndexStateFromRecords1987(tb, records, def)
	sets := columnVectorGraphQuantizedAssetSetsByName(state, def)
	set, ok := sets[quantizedIndexName]
	if !ok || !set.HasCodes {
		tb.Fatalf("quantized asset %q missing from state assets: %+v", quantizedIndexName, state.Assets)
	}
	if set.Codes.AssetBytes <= 0 {
		tb.Fatalf("quantized asset %q code bytes=%d want positive", quantizedIndexName, set.Codes.AssetBytes)
	}
	codes = set.Codes.AssetBytes
	if set.HasAlpha {
		if set.Alpha.AssetBytes <= 0 {
			tb.Fatalf("quantized asset %q alpha bytes=%d want positive", quantizedIndexName, set.Alpha.AssetBytes)
		}
		alpha = set.Alpha.AssetBytes
	}
	return codes + alpha, codes, alpha
}

func columnGraphQuantizedAssetSetBytesForName2844(tb testing.TB, d *backenddb.DB, def VectorIndexDefinition, quantizedIndexName string) int64 {
	tb.Helper()
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(tb, d, "docs")
	state := columnVectorIndexStateFromRecords1987(tb, records, def)
	sets := columnVectorGraphQuantizedAssetSetsByName(state, def)
	set, ok := sets[quantizedIndexName]
	if !ok || !set.HasCodes {
		tb.Fatalf("quantized asset set %q missing codes from state assets: %+v", quantizedIndexName, state.Assets)
	}
	if set.Codes.AssetBytes <= 0 {
		tb.Fatalf("quantized asset set %q code bytes=%d want positive", quantizedIndexName, set.Codes.AssetBytes)
	}
	total := set.Codes.AssetBytes
	if set.HasAlpha {
		if set.Alpha.AssetBytes <= 0 {
			tb.Fatalf("quantized asset set %q alpha bytes=%d want positive", quantizedIndexName, set.Alpha.AssetBytes)
		}
		total += set.Alpha.AssetBytes
	}
	return total
}
