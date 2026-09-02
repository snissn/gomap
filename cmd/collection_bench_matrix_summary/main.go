package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
)

type config struct {
	matrixIndexPath     string
	outDir              string
	availableBenchmarks bool
}

type matrixRow struct {
	Cell                   string
	Engine                 string
	DocumentFormat         string
	DataOuterLeavesInVLog  string
	IndexOuterLeavesInVLog string
	PagerChunkSize         string
	PagerSyncConcurrency   string
	ReportMarkdownPath     string
	ReportJSONPath         string
}

type report struct {
	Status              string `json:"status"`
	DocumentFormat      string `json:"document_format,omitempty"`
	CollectionBatchSize int    `json:"collection_batch_size,omitempty"`
	Sections            []struct {
		Benchmarks []benchmarkAggregate `json:"benchmarks"`
	} `json:"sections"`
}

type benchmarkAggregate struct {
	Name            string             `json:"name"`
	MeanNsPerOp     float64            `json:"mean_ns_per_op"`
	MeanBytesPerOp  float64            `json:"mean_bytes_per_op"`
	MeanAllocsPerOp float64            `json:"mean_allocs_per_op"`
	MeanMetrics     map[string]float64 `json:"mean_metrics,omitempty"`
}

type summaryRow struct {
	matrixRow
	Benchmark             string
	NsPerOp               float64
	BytesPerOp            float64
	AllocsPerOp           float64
	CollectionBatchSize   int
	InsertNsPerDoc        *float64
	SyncNsPerDoc          *float64
	WriterDocsPerSec      *float64
	KeyFallbacks          *float64
	PrefixFallbacks       *float64
	IndexesPerDoc         *float64
	StoredDocs            *float64
	DiskTotalBytes        *float64
	DiskBytesPerDoc       *float64
	CollectionDiskBytes   *float64
	CollectionDiskBPerDoc *float64
	IndexDiskBytes        *float64
	IndexDiskBPerDoc      *float64
	VLogRewriteNs         *float64
	VLogRewriteBefore     *float64
	VLogRewriteAfter      *float64
	VLogRewriteDelta      *float64
	VLogRewriteAfterGC    *float64
	VLogRewriteDeltaGC    *float64
	VLogRewriteVacuumNs   *float64
	VLogRewriteAfterVac   *float64
	VLogRewriteDeltaVac   *float64
	VLogGCNs              *float64
	LeafGenPlanNs         *float64
	LeafGenPlanLive       *float64
	LeafGenPlanDead       *float64
	LeafGenPackNs         *float64
	LeafGenPackBefore     *float64
	LeafGenPackAfter      *float64
	LeafGenPackDelta      *float64
	LeafGenPackAfterGC    *float64
	LeafGenPackDeltaGC    *float64
	LeafGenPackVacuumNs   *float64
	LeafGenPackAfterVac   *float64
	LeafGenPackDeltaVac   *float64
	LeafGenPackFrames     *float64
	LeafGenPackMaxFrameK  *float64
	LeafGenGCNs           *float64
	SQLiteVacuumNs        *float64
	SQLiteVacuumBefore    *float64
	SQLiteVacuumAfter     *float64
	SQLiteVacuumDelta     *float64
}

type loadedReport struct {
	DocumentFormat      string
	CollectionBatchSize int
	Benchmarks          map[string]benchmarkAggregate
}

type userStoryRow struct {
	summaryRow
	Story      string
	DocsPerSec float64
	MSPerBatch float64
	BatchesSec float64
}

type diskUsageRow struct {
	summaryRow
	Story                      string
	SplitSource                string
	IndexesPerDocValue         float64
	StoredDocsValue            float64
	DiskTotalBytesValue        float64
	DiskBytesPerDocValue       float64
	CollectionDiskBytesValue   *float64
	CollectionDiskBPerDocValue *float64
	IndexDiskBytesValue        *float64
	IndexDiskBPerDocValue      *float64
}

type maintenanceRow struct {
	summaryRow
	Kind       string
	NsPerMaint *float64
	GCNs       *float64
	Before     *float64
	After      *float64
	Delta      *float64
	AfterGC    *float64
	DeltaGC    *float64
	VacuumNs   *float64
	AfterVac   *float64
	DeltaVac   *float64
	Frames     *float64
	MaxFrameK  *float64
}

var benchmarkOrder = []string{
	"BenchmarkCollectionOverheadIndexStateJSONExtraction",
	"BenchmarkCollectionOverheadIndexStateTemplateV1Extraction",
	"BenchmarkCollectionOverheadPlanIndexedTemplateV1",
	"BenchmarkCollectionOverheadPlanIndexedPrecomputedState",
	"BenchmarkCollectionShapeInsertBatch",
	"BenchmarkCollectionShapeInsertBatchSingleStringJSON",
	"BenchmarkCollectionInsertBatchWithSecondaryIndexes",
	"BenchmarkCollectionShapeInsertBatchCheckpoint",
	"BenchmarkCollectionShapeInsertBatchCheckpointSingleStringJSON",
	"BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes",
	"BenchmarkCollectionTimedProfileInsertBatchWithSecondaryIndexes",
	"BenchmarkCollectionTimedProfileInsertBatchCheckpointWithSecondaryIndexes",
	"BenchmarkCollectionShapeReadPrimary",
	"BenchmarkCollectionShapeReadPrimaryParallel",
	"BenchmarkCollectionShapeReadPrimaryInto",
	"BenchmarkCollectionShapeReadPrimaryIntoParallel",
	"BenchmarkCollectionMixedReadWritePrimary",
	"BenchmarkCollectionMixedReadWriteSecondaryUnique",
	"BenchmarkSQLiteShapeInsertBatchJSON",
	"BenchmarkSQLiteShapeInsertBatchNativeColumns",
	"BenchmarkSQLiteInsertBatchWithSecondaryIndexes",
	"BenchmarkSQLiteInsertBatchCheckpointWithSecondaryIndexes",
	"BenchmarkSQLiteNativeColumnsInsertBatchWithSecondaryIndexes",
	"BenchmarkSQLiteNativeColumnsInsertBatchCheckpointWithSecondaryIndexes",
	"BenchmarkSQLiteShapeInsertBatchCheckpointJSON",
	"BenchmarkSQLiteShapeInsertBatchCheckpointNativeColumns",
	"BenchmarkSQLiteShapeReadPrimaryJSON",
	"BenchmarkSQLiteShapeReadPrimaryNativeColumns",
	"BenchmarkSQLiteShapeSecondaryLookupJSON",
	"BenchmarkSQLiteShapeSecondaryLookupNativeColumns",
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_matrix_summary: %v\n", err)
		os.Exit(2)
	}
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_matrix_summary: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("collection_bench_matrix_summary", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.matrixIndexPath, "matrix-index", "", "Path to matrix_index.tsv")
	fs.StringVar(&cfg.outDir, "out-dir", "", "Output directory for summary artifacts")
	fs.BoolVar(&cfg.availableBenchmarks, "available-benchmarks", false, "Summarize all benchmark rows present in each report instead of requiring the default production benchmark set")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.matrixIndexPath == "" {
		return config{}, fmt.Errorf("-matrix-index is required")
	}
	if cfg.outDir == "" {
		cfg.outDir = filepath.Dir(cfg.matrixIndexPath)
	}
	return cfg, nil
}

func run(cfg config) error {
	rows, err := readMatrixIndex(cfg.matrixIndexPath)
	if err != nil {
		return err
	}
	summaryRows, err := buildSummaryRows(rows, cfg.availableBenchmarks)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		return fmt.Errorf("create out dir: %w", err)
	}
	tsvPath := filepath.Join(cfg.outDir, "collections_matrix_summary.tsv")
	if err := os.WriteFile(tsvPath, []byte(renderTSV(summaryRows, cfg.outDir)), 0o644); err != nil {
		return fmt.Errorf("write tsv: %w", err)
	}
	userStoryPath := filepath.Join(cfg.outDir, "collections_user_story_summary.tsv")
	if err := os.WriteFile(userStoryPath, []byte(renderUserStoryTSV(buildUserStoryRows(summaryRows), cfg.outDir)), 0o644); err != nil {
		return fmt.Errorf("write user story tsv: %w", err)
	}
	diskUsagePath := filepath.Join(cfg.outDir, "collections_disk_usage_summary.tsv")
	if err := os.WriteFile(diskUsagePath, []byte(renderDiskUsageTSV(buildDiskUsageRows(summaryRows), cfg.outDir)), 0o644); err != nil {
		return fmt.Errorf("write disk usage tsv: %w", err)
	}
	maintenancePath := filepath.Join(cfg.outDir, "collections_maintenance_summary.tsv")
	if err := os.WriteFile(maintenancePath, []byte(renderMaintenanceTSV(buildMaintenanceRows(summaryRows), cfg.outDir)), 0o644); err != nil {
		return fmt.Errorf("write maintenance tsv: %w", err)
	}
	mdPath := filepath.Join(cfg.outDir, "collections_matrix_summary.md")
	md, err := renderMarkdown(summaryRows, cfg.outDir)
	if err != nil {
		return err
	}
	if err := os.WriteFile(mdPath, []byte(md), 0o644); err != nil {
		return fmt.Errorf("write markdown: %w", err)
	}
	htmlPath := filepath.Join(cfg.outDir, "collections_matrix_summary.html")
	htmlDoc, err := markdownToHTMLDoc(md)
	if err != nil {
		return err
	}
	if err := os.WriteFile(htmlPath, htmlDoc, 0o644); err != nil {
		return fmt.Errorf("write html: %w", err)
	}
	fmt.Printf("wrote matrix summary tsv: %s\n", tsvPath)
	fmt.Printf("wrote user story tsv:     %s\n", userStoryPath)
	fmt.Printf("wrote disk usage tsv:     %s\n", diskUsagePath)
	fmt.Printf("wrote maintenance tsv:    %s\n", maintenancePath)
	fmt.Printf("wrote matrix summary md:  %s\n", mdPath)
	fmt.Printf("wrote matrix summary html: %s\n", htmlPath)
	return nil
}

func readMatrixIndex(path string) ([]matrixRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open matrix index: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read matrix index: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("matrix index is empty")
	}
	header := make(map[string]int, len(records[0]))
	for i, name := range records[0] {
		header[name] = i
	}
	required := []string{"cell", "engine", "data_outer_leaves_in_vlog", "index_outer_leaves_in_vlog", "pager_chunk_size", "pager_sync_concurrency", "report_md", "report_json"}
	for _, name := range required {
		if _, ok := header[name]; !ok {
			return nil, fmt.Errorf("matrix index missing %q column", name)
		}
	}

	documentFormatIdx, hasDocumentFormat := header["document_format"]
	baseDir, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("resolve matrix index directory: %w", err)
	}
	var rows []matrixRow
	for _, record := range records[1:] {
		if len(record) == 0 || strings.TrimSpace(record[0]) == "" {
			continue
		}
		documentFormat := ""
		if hasDocumentFormat {
			documentFormat = field(record, documentFormatIdx)
			if strings.TrimSpace(documentFormat) == "" {
				documentFormat = "json"
			}
		}
		cell := field(record, header["cell"])
		reportMarkdownPath, err := matrixIndexArtifactPath(baseDir, field(record, header["report_md"]))
		if err != nil {
			return nil, fmt.Errorf("matrix index cell %q report_md: %w", cell, err)
		}
		reportJSONPath, err := matrixIndexArtifactPath(baseDir, field(record, header["report_json"]))
		if err != nil {
			return nil, fmt.Errorf("matrix index cell %q report_json: %w", cell, err)
		}
		rows = append(rows, matrixRow{
			Cell:                   cell,
			Engine:                 field(record, header["engine"]),
			DocumentFormat:         documentFormat,
			DataOuterLeavesInVLog:  field(record, header["data_outer_leaves_in_vlog"]),
			IndexOuterLeavesInVLog: field(record, header["index_outer_leaves_in_vlog"]),
			PagerChunkSize:         field(record, header["pager_chunk_size"]),
			PagerSyncConcurrency:   field(record, header["pager_sync_concurrency"]),
			ReportMarkdownPath:     reportMarkdownPath,
			ReportJSONPath:         reportJSONPath,
		})
	}
	return rows, nil
}

func matrixIndexArtifactPath(baseDir, artifactPath string) (string, error) {
	artifactPath = strings.TrimSpace(artifactPath)
	if artifactPath == "" {
		return "", fmt.Errorf("artifact path is empty")
	}
	baseDir = filepath.Clean(baseDir)
	var joined string
	if filepath.IsAbs(artifactPath) {
		joined = filepath.Clean(artifactPath)
	} else {
		artifactPath = filepath.FromSlash(artifactPath)
		if filepath.VolumeName(artifactPath) != "" {
			return "", fmt.Errorf("volume-qualified artifact path %q is not allowed in matrix index", artifactPath)
		}
		joined = filepath.Clean(filepath.Join(baseDir, artifactPath))
	}
	if inside, err := pathWithinDir(baseDir, joined); err != nil {
		return "", fmt.Errorf("resolve artifact path %q: %w", artifactPath, err)
	} else if !inside {
		if resolvedInside, err := resolvedPathWithinDir(baseDir, joined); err != nil || !resolvedInside {
			return "", fmt.Errorf("artifact path %q escapes matrix directory", artifactPath)
		}
		return joined, nil
	}
	if resolvedInside, err := resolvedPathWithinDir(baseDir, joined); err != nil {
		return "", fmt.Errorf("resolve artifact path %q symlinks: %w", artifactPath, err)
	} else if !resolvedInside {
		return "", fmt.Errorf("resolved artifact path %q escapes matrix directory", artifactPath)
	}
	return joined, nil
}

func pathWithinDir(baseDir, target string) (bool, error) {
	rel, err := filepath.Rel(baseDir, target)
	if err != nil {
		return false, err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return false, nil
	}
	return true, nil
}

func resolvedPathWithinDir(baseDir, target string) (bool, error) {
	resolvedBase, err := filepath.EvalSymlinks(baseDir)
	if err != nil {
		return false, err
	}
	resolvedTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		return false, err
	}
	inside, err := pathWithinDir(resolvedBase, resolvedTarget)
	if err != nil {
		return false, err
	}
	return inside, nil
}

func field(record []string, idx int) string {
	if idx < 0 || idx >= len(record) {
		return ""
	}
	return record[idx]
}

func buildSummaryRows(rows []matrixRow, availableBenchmarks bool) ([]summaryRow, error) {
	out := make([]summaryRow, 0, len(rows)*len(benchmarkOrder))
	for _, row := range rows {
		report, err := loadBenchmarkReport(row.ReportJSONPath)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(row.DocumentFormat) == "" {
			row.DocumentFormat = report.DocumentFormat
			if strings.TrimSpace(row.DocumentFormat) == "" {
				row.DocumentFormat = "json"
			}
		}
		if availableBenchmarks {
			for _, name := range availableBenchmarkNames(report.Benchmarks) {
				benchmark := report.Benchmarks[name]
				out = append(out, buildSummaryRow(row, report.CollectionBatchSize, name, benchmark))
			}
			continue
		}
		for _, name := range requiredBenchmarkNames(row) {
			benchmark, ok := report.Benchmarks[name]
			if !ok {
				return nil, fmt.Errorf("report %s missing benchmark %q for matrix cell %q", row.ReportJSONPath, name, row.Cell)
			}
			out = append(out, buildSummaryRow(row, report.CollectionBatchSize, name, benchmark))
		}
		for _, name := range optionalBenchmarkNames(row) {
			benchmark, ok := report.Benchmarks[name]
			if !ok {
				continue
			}
			out = append(out, buildSummaryRow(row, report.CollectionBatchSize, name, benchmark))
		}
	}
	return out, nil
}

func availableBenchmarkNames(benchmarks map[string]benchmarkAggregate) []string {
	names := make([]string, 0, len(benchmarks))
	for name := range benchmarks {
		names = append(names, name)
	}
	order := make(map[string]int, len(benchmarkOrder))
	for i, name := range benchmarkOrder {
		order[name] = i
	}
	sortBenchmarkNames(names, order)
	return names
}

func sortBenchmarkNames(names []string, order map[string]int) {
	sort.Slice(names, func(i, j int) bool {
		leftBase := benchmarkBaseName(names[i])
		rightBase := benchmarkBaseName(names[j])
		leftOrder, leftOK := order[leftBase]
		rightOrder, rightOK := order[rightBase]
		switch {
		case leftOK && rightOK && leftOrder != rightOrder:
			return leftOrder < rightOrder
		case leftOK != rightOK:
			return leftOK
		default:
			return names[i] < names[j]
		}
	})
}

func benchmarkBaseName(name string) string {
	if slash := strings.IndexByte(name, '/'); slash > 0 {
		return name[:slash]
	}
	return name
}

func buildSummaryRow(row matrixRow, collectionBatchSize int, name string, benchmark benchmarkAggregate) summaryRow {
	benchmarkRow := row
	benchmarkRow.DocumentFormat = documentFormatForBenchmark(benchmarkRow.DocumentFormat, name)
	return summaryRow{
		matrixRow:             benchmarkRow,
		Benchmark:             name,
		NsPerOp:               benchmark.MeanNsPerOp,
		BytesPerOp:            benchmark.MeanBytesPerOp,
		AllocsPerOp:           benchmark.MeanAllocsPerOp,
		CollectionBatchSize:   collectionBatchSize,
		InsertNsPerDoc:        metricPtr(benchmark.MeanMetrics, "insert_ns/doc"),
		SyncNsPerDoc:          metricPtr(benchmark.MeanMetrics, "sync_ns/doc"),
		WriterDocsPerSec:      metricPtr(benchmark.MeanMetrics, "writer_docs/sec"),
		KeyFallbacks:          metricPtr(benchmark.MeanMetrics, "per_item_key_probe_fallback_count"),
		PrefixFallbacks:       metricPtr(benchmark.MeanMetrics, "per_item_prefix_probe_fallback_count"),
		IndexesPerDoc:         metricPtr(benchmark.MeanMetrics, "indexes/doc"),
		StoredDocs:            metricPtr(benchmark.MeanMetrics, "stored_docs"),
		DiskTotalBytes:        metricPtr(benchmark.MeanMetrics, "disk_total_bytes"),
		DiskBytesPerDoc:       metricPtr(benchmark.MeanMetrics, "disk_bytes/doc"),
		CollectionDiskBytes:   metricPtr(benchmark.MeanMetrics, "collection_disk_bytes"),
		CollectionDiskBPerDoc: metricPtr(benchmark.MeanMetrics, "collection_disk_bytes/doc"),
		IndexDiskBytes:        metricPtr(benchmark.MeanMetrics, "index_disk_bytes"),
		IndexDiskBPerDoc:      metricPtr(benchmark.MeanMetrics, "index_disk_bytes/doc"),
		VLogRewriteNs:         metricPtr(benchmark.MeanMetrics, "vlog_rewrite_ns/op"),
		VLogRewriteBefore:     metricPtr(benchmark.MeanMetrics, "vlog_rewrite_disk_total_bytes_before"),
		VLogRewriteAfter:      metricPtr(benchmark.MeanMetrics, "vlog_rewrite_disk_total_bytes_after"),
		VLogRewriteDelta:      metricPtr(benchmark.MeanMetrics, "vlog_rewrite_disk_total_bytes_delta"),
		VLogRewriteAfterGC:    metricPtr(benchmark.MeanMetrics, "vlog_rewrite_gc_disk_total_bytes_after"),
		VLogRewriteDeltaGC:    metricPtr(benchmark.MeanMetrics, "vlog_rewrite_gc_disk_total_bytes_delta"),
		VLogRewriteVacuumNs:   metricPtr(benchmark.MeanMetrics, "vlog_rewrite_gc_vacuum_ns/op"),
		VLogRewriteAfterVac:   metricPtr(benchmark.MeanMetrics, "vlog_rewrite_gc_vacuum_disk_total_bytes_after"),
		VLogRewriteDeltaVac:   metricPtr(benchmark.MeanMetrics, "vlog_rewrite_gc_vacuum_disk_total_bytes_delta"),
		VLogGCNs:              metricPtr(benchmark.MeanMetrics, "vlog_gc_ns/op"),
		LeafGenPlanNs:         metricPtr(benchmark.MeanMetrics, "leafgen_plan_ns/op"),
		LeafGenPlanLive:       metricPtr(benchmark.MeanMetrics, "leafgen_plan_candidate_bytes_live"),
		LeafGenPlanDead:       metricPtr(benchmark.MeanMetrics, "leafgen_plan_candidate_bytes_dead"),
		LeafGenPackNs:         metricPtr(benchmark.MeanMetrics, "leafgen_pack_ns/op"),
		LeafGenPackBefore:     metricPtr(benchmark.MeanMetrics, "leafgen_pack_disk_total_bytes_before"),
		LeafGenPackAfter:      metricPtr(benchmark.MeanMetrics, "leafgen_pack_disk_total_bytes_after"),
		LeafGenPackDelta:      metricPtr(benchmark.MeanMetrics, "leafgen_pack_disk_total_bytes_delta"),
		LeafGenPackAfterGC:    metricPtr(benchmark.MeanMetrics, "leafgen_pack_gc_disk_total_bytes_after"),
		LeafGenPackDeltaGC:    metricPtr(benchmark.MeanMetrics, "leafgen_pack_gc_disk_total_bytes_delta"),
		LeafGenPackVacuumNs:   metricPtr(benchmark.MeanMetrics, "leafgen_pack_gc_vacuum_ns/op"),
		LeafGenPackAfterVac:   metricPtr(benchmark.MeanMetrics, "leafgen_pack_gc_vacuum_disk_total_bytes_after"),
		LeafGenPackDeltaVac:   metricPtr(benchmark.MeanMetrics, "leafgen_pack_gc_vacuum_disk_total_bytes_delta"),
		LeafGenPackFrames:     metricPtr(benchmark.MeanMetrics, "leafgen_pack_leaf_frames_written"),
		LeafGenPackMaxFrameK:  metricPtr(benchmark.MeanMetrics, "leafgen_pack_max_leaf_frame_k"),
		LeafGenGCNs:           metricPtr(benchmark.MeanMetrics, "leafgen_gc_ns/op"),
		SQLiteVacuumNs:        metricPtr(benchmark.MeanMetrics, "sqlite_vacuum_ns/op"),
		SQLiteVacuumBefore:    metricPtr(benchmark.MeanMetrics, "sqlite_vacuum_disk_total_bytes_before"),
		SQLiteVacuumAfter:     metricPtr(benchmark.MeanMetrics, "sqlite_vacuum_disk_total_bytes_after"),
		SQLiteVacuumDelta:     metricPtr(benchmark.MeanMetrics, "sqlite_vacuum_disk_total_bytes_delta"),
	}
}

func requiredBenchmarkNames(row matrixRow) []string {
	if isSQLiteMatrixRow(row) {
		return []string{
			"BenchmarkSQLiteInsertBatchWithSecondaryIndexes",
			"BenchmarkSQLiteInsertBatchCheckpointWithSecondaryIndexes",
		}
	}
	return []string{
		"BenchmarkCollectionOverheadIndexStateJSONExtraction",
		"BenchmarkCollectionOverheadIndexStateTemplateV1Extraction",
		"BenchmarkCollectionOverheadPlanIndexedTemplateV1",
		"BenchmarkCollectionOverheadPlanIndexedPrecomputedState",
		"BenchmarkCollectionInsertBatchWithSecondaryIndexes",
		"BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes",
	}
}

func optionalBenchmarkNames(row matrixRow) []string {
	if isSQLiteMatrixRow(row) {
		return []string{
			"BenchmarkSQLiteNativeColumnsInsertBatchWithSecondaryIndexes",
			"BenchmarkSQLiteNativeColumnsInsertBatchCheckpointWithSecondaryIndexes",
		}
	}
	return nil
}

func isSQLiteMatrixRow(row matrixRow) bool {
	// The matrix runner normalizes SQLite cells to the sqlite_* namespace even
	// when TREEDB_COLLECTION_SQLITE_ENGINE uses a custom engine label.
	return row.Cell == "sqlite" || strings.HasPrefix(row.Cell, "sqlite_") || strings.HasPrefix(row.Engine, "sqlite")
}

func documentFormatForBenchmark(format, benchmark string) string {
	base := benchmarkBaseName(benchmark)
	if strings.HasPrefix(base, "BenchmarkSQLiteNativeColumns") ||
		(strings.HasPrefix(base, "BenchmarkSQLiteShape") && strings.Contains(base, "NativeColumns")) {
		return "native-columns"
	}
	if strings.TrimSpace(format) == "" {
		return "json"
	}
	return format
}

func loadBenchmarkReport(path string) (loadedReport, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return loadedReport{}, fmt.Errorf("read report %s: %w", path, err)
	}
	var rep report
	if err := json.Unmarshal(raw, &rep); err != nil {
		return loadedReport{}, fmt.Errorf("parse report %s: %w", path, err)
	}
	if rep.Status != "ok" {
		return loadedReport{}, fmt.Errorf("report %s status %q; matrix summary requires ok reports", path, rep.Status)
	}
	out := make(map[string]benchmarkAggregate)
	for _, section := range rep.Sections {
		for _, benchmark := range section.Benchmarks {
			out[benchmark.Name] = benchmark
		}
	}
	return loadedReport{DocumentFormat: rep.DocumentFormat, CollectionBatchSize: rep.CollectionBatchSize, Benchmarks: out}, nil
}

func metricPtr(metrics map[string]float64, name string) *float64 {
	value, ok := metrics[name]
	if !ok {
		return nil
	}
	return &value
}

func renderTSV(rows []summaryRow, outDir string) string {
	var sb strings.Builder
	sb.WriteString("cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\tbenchmark\tns_per_op\tops_per_sec\tbytes_per_op\tallocs_per_op\tinsert_ns/doc\tinsert_docs_per_sec\tsync_ns/doc\tsync_docs_per_sec\twriter_docs_per_sec\tper_item_key_probe_fallback_count\tper_item_prefix_probe_fallback_count\tindexes/doc\tstored_docs\tdisk_total_bytes\tdisk_bytes/doc\tcollection_disk_bytes\tcollection_disk_bytes/doc\tindex_disk_bytes\tindex_disk_bytes/doc\treport_md\n")
	for _, row := range rows {
		sb.WriteString(row.Cell)
		sb.WriteByte('\t')
		sb.WriteString(row.Engine)
		sb.WriteByte('\t')
		sb.WriteString(row.DocumentFormat)
		sb.WriteByte('\t')
		sb.WriteString(row.DataOuterLeavesInVLog)
		sb.WriteByte('\t')
		sb.WriteString(row.IndexOuterLeavesInVLog)
		sb.WriteByte('\t')
		sb.WriteString(row.PagerChunkSize)
		sb.WriteByte('\t')
		sb.WriteString(row.PagerSyncConcurrency)
		sb.WriteByte('\t')
		sb.WriteString(row.Benchmark)
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.NsPerOp))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(opsPerSecFromNs(row.NsPerOp)))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.BytesPerOp))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.AllocsPerOp))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.InsertNsPerDoc))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(optionalOpsPerSecFromNs(row.InsertNsPerDoc)))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.SyncNsPerDoc))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(optionalOpsPerSecFromNs(row.SyncNsPerDoc)))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.WriterDocsPerSec))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.KeyFallbacks))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.PrefixFallbacks))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.IndexesPerDoc))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.StoredDocs))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.DiskTotalBytes))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.DiskBytesPerDoc))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.CollectionDiskBytes))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.CollectionDiskBPerDoc))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.IndexDiskBytes))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.IndexDiskBPerDoc))
		sb.WriteByte('\t')
		sb.WriteString(relativeReportPath(outDir, row.ReportMarkdownPath))
		sb.WriteByte('\n')
	}
	return sb.String()
}

func renderUserStoryTSV(rows []userStoryRow, outDir string) string {
	var sb strings.Builder
	sb.WriteString("cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\tstory\tbenchmark\tdocs_per_batch\tns_per_doc\tdocs_per_sec\tms_per_batch\tinsert_ms_per_batch\tsync_ms_per_batch\tbatches_per_sec\treport_md\n")
	for _, row := range rows {
		sb.WriteString(row.Cell)
		sb.WriteByte('\t')
		sb.WriteString(row.Engine)
		sb.WriteByte('\t')
		sb.WriteString(row.DocumentFormat)
		sb.WriteByte('\t')
		sb.WriteString(row.DataOuterLeavesInVLog)
		sb.WriteByte('\t')
		sb.WriteString(row.IndexOuterLeavesInVLog)
		sb.WriteByte('\t')
		sb.WriteString(row.PagerChunkSize)
		sb.WriteByte('\t')
		sb.WriteString(row.PagerSyncConcurrency)
		sb.WriteByte('\t')
		sb.WriteString(row.Story)
		sb.WriteByte('\t')
		sb.WriteString(row.Benchmark)
		sb.WriteByte('\t')
		sb.WriteString(strconv.Itoa(row.CollectionBatchSize))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.NsPerOp))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.DocsPerSec))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.MSPerBatch))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(optionalMetricPerBatchMS(row.InsertNsPerDoc, row.CollectionBatchSize)))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(optionalMetricPerBatchMS(row.SyncNsPerDoc, row.CollectionBatchSize)))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.BatchesSec))
		sb.WriteByte('\t')
		sb.WriteString(relativeReportPath(outDir, row.ReportMarkdownPath))
		sb.WriteByte('\n')
	}
	return sb.String()
}

func renderDiskUsageTSV(rows []diskUsageRow, outDir string) string {
	var sb strings.Builder
	sb.WriteString("cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\tstory\tbenchmark\tindexes/doc\tstored_docs\tdisk_total_bytes\tdisk_bytes/doc\tcollection_disk_bytes\tcollection_disk_bytes/doc\tindex_disk_bytes\tindex_disk_bytes/doc\tsplit_source\treport_md\n")
	for _, row := range rows {
		sb.WriteString(row.Cell)
		sb.WriteByte('\t')
		sb.WriteString(row.Engine)
		sb.WriteByte('\t')
		sb.WriteString(row.DocumentFormat)
		sb.WriteByte('\t')
		sb.WriteString(row.DataOuterLeavesInVLog)
		sb.WriteByte('\t')
		sb.WriteString(row.IndexOuterLeavesInVLog)
		sb.WriteByte('\t')
		sb.WriteString(row.PagerChunkSize)
		sb.WriteByte('\t')
		sb.WriteString(row.PagerSyncConcurrency)
		sb.WriteByte('\t')
		sb.WriteString(row.Story)
		sb.WriteByte('\t')
		sb.WriteString(row.Benchmark)
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.IndexesPerDocValue))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.StoredDocsValue))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.DiskTotalBytesValue))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.DiskBytesPerDocValue))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.CollectionDiskBytesValue))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.CollectionDiskBPerDocValue))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.IndexDiskBytesValue))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.IndexDiskBPerDocValue))
		sb.WriteByte('\t')
		sb.WriteString(row.SplitSource)
		sb.WriteByte('\t')
		sb.WriteString(relativeReportPath(outDir, row.ReportMarkdownPath))
		sb.WriteByte('\n')
	}
	return sb.String()
}

func renderMaintenanceTSV(rows []maintenanceRow, outDir string) string {
	var sb strings.Builder
	sb.WriteString("cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\tmaintenance\tbenchmark\tns_per_op\tops_per_sec\tgc_ns_per_op\tgc_ops_per_sec\tvacuum_ns_per_op\tvacuum_ops_per_sec\tdisk_total_bytes_before\tdisk_total_bytes_after\tdisk_total_bytes_delta\tdisk_total_bytes_after_gc\tdisk_total_bytes_delta_after_gc\tdisk_total_bytes_after_gc_vacuum\tdisk_total_bytes_delta_after_gc_vacuum\tleaf_frames_written\tmax_leaf_frame_k\treport_md\n")
	for _, row := range rows {
		sb.WriteString(row.Cell)
		sb.WriteByte('\t')
		sb.WriteString(row.Engine)
		sb.WriteByte('\t')
		sb.WriteString(row.DocumentFormat)
		sb.WriteByte('\t')
		sb.WriteString(row.DataOuterLeavesInVLog)
		sb.WriteByte('\t')
		sb.WriteString(row.IndexOuterLeavesInVLog)
		sb.WriteByte('\t')
		sb.WriteString(row.PagerChunkSize)
		sb.WriteByte('\t')
		sb.WriteString(row.PagerSyncConcurrency)
		sb.WriteByte('\t')
		sb.WriteString(row.Kind)
		sb.WriteByte('\t')
		sb.WriteString(row.Benchmark)
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.NsPerMaint))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(optionalOpsPerSecFromNs(row.NsPerMaint)))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.GCNs))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(optionalOpsPerSecFromNs(row.GCNs)))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.VacuumNs))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(optionalOpsPerSecFromNs(row.VacuumNs)))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.Before))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.After))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.Delta))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.AfterGC))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.DeltaGC))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.AfterVac))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.DeltaVac))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.Frames))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.MaxFrameK))
		sb.WriteByte('\t')
		sb.WriteString(relativeReportPath(outDir, row.ReportMarkdownPath))
		sb.WriteByte('\n')
	}
	return sb.String()
}

func optionalMetricPerBatchMS(nsPerDoc *float64, batchSize int) *float64 {
	if nsPerDoc == nil || batchSize <= 0 {
		return nil
	}
	value := *nsPerDoc * float64(batchSize) / 1e6
	return &value
}

func opsPerSecFromNs(nsPerOp float64) float64 {
	if nsPerOp <= 0 {
		return math.NaN()
	}
	return 1e9 / nsPerOp
}

func optionalOpsPerSecFromNs(nsPerOp *float64) *float64 {
	if nsPerOp == nil || *nsPerOp <= 0 {
		return nil
	}
	value := 1e9 / *nsPerOp
	return &value
}

func formatOptionalTSVFloat(value *float64) string {
	if value == nil {
		return "-"
	}
	return formatTSVFloat(*value)
}

func formatTSVFloat(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "-"
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func renderMarkdown(rows []summaryRow, outDir string) (string, error) {
	var sb strings.Builder
	sb.WriteString("# Collections Benchmark Matrix Summary\n\n")
	userStoryRows := buildUserStoryRows(rows)
	diskUsageRows := buildDiskUsageRows(rows)
	maintenanceRows := buildMaintenanceRows(rows)
	diagnosticRows := buildDiagnosticRows(rows)
	if lines := buildExecutiveSummary(userStoryRows, diskUsageRows, maintenanceRows, diagnosticRows); len(lines) > 0 {
		sb.WriteString("## Executive Summary\n\n")
		for _, line := range lines {
			sb.WriteString("- ")
			sb.WriteString(line)
			sb.WriteByte('\n')
		}
		sb.WriteString("\n")
	}
	if len(userStoryRows) > 0 {
		sb.WriteString("## User-Facing Throughput\n\n")
		sb.WriteString("Batch benchmark ops are documents, so this section reports the indexed ingest story as docs/sec, batch latency, and batches/sec. Diagnostic JSON/planner rows are separated below.\n\n")
		sb.WriteString("| Cell | Engine | Format | Data vlog | Index vlog | Pager chunk | Pager sync | Story | Benchmark | Docs/batch | ns/doc | Docs/sec | ms/batch | insert ms/batch | sync ms/batch | batches/sec | Report |\n")
		sb.WriteString("| --- | --- | --- | ---: | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n")
		for _, row := range userStoryRows {
			reportPath := relativeReportPath(outDir, row.ReportMarkdownPath)
			sb.WriteString("| `")
			sb.WriteString(escapeTableCell(row.Cell))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.Engine))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.DocumentFormat))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.DataOuterLeavesInVLog))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.IndexOuterLeavesInVLog))
			sb.WriteString("` | ")
			sb.WriteString("`")
			sb.WriteString(escapeTableCell(row.PagerChunkSize))
			sb.WriteString("` | ")
			sb.WriteString("`")
			sb.WriteString(escapeTableCell(row.PagerSyncConcurrency))
			sb.WriteString("` | ")
			sb.WriteString(escapeTableCell(row.Story))
			sb.WriteString(" | `")
			sb.WriteString(escapeTableCell(row.Benchmark))
			sb.WriteString("` | ")
			sb.WriteString(formatIntWithCommas(int64(row.CollectionBatchSize)))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(row.NsPerOp))
			sb.WriteString(" | ")
			sb.WriteString(formatThroughput(row.DocsPerSec))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(row.MSPerBatch))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalFloat(optionalMetricPerBatchMS(row.InsertNsPerDoc, row.CollectionBatchSize)))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalFloat(optionalMetricPerBatchMS(row.SyncNsPerDoc, row.CollectionBatchSize)))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(row.BatchesSec))
			sb.WriteString(" | [report](")
			sb.WriteString(markdownLinkPath(reportPath))
			sb.WriteString(") |\n")
		}
		sb.WriteString("\n")
	}
	if len(diskUsageRows) > 0 {
		sb.WriteString("## Disk Usage\n\n")
		sb.WriteString("Disk rows use benchmark-reported end-of-run bytes after an untimed flush/checkpoint. Collection/index splits use engine-reported object bytes when available; otherwise they derive index bytes from the per-doc delta against the matching zero-index row.\n\n")
		sb.WriteString("| Cell | Engine | Format | Data vlog | Index vlog | Pager chunk | Pager sync | Story | Benchmark | Indexes/doc | Stored docs | Total disk | Total B/doc | Collection disk | Collection B/doc | Index disk | Index B/doc | Split | Report |\n")
		sb.WriteString("| --- | --- | --- | ---: | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |\n")
		for _, row := range diskUsageRows {
			reportPath := relativeReportPath(outDir, row.ReportMarkdownPath)
			sb.WriteString("| `")
			sb.WriteString(escapeTableCell(row.Cell))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.Engine))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.DocumentFormat))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.DataOuterLeavesInVLog))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.IndexOuterLeavesInVLog))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.PagerChunkSize))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.PagerSyncConcurrency))
			sb.WriteString("` | ")
			sb.WriteString(escapeTableCell(row.Story))
			sb.WriteString(" | `")
			sb.WriteString(escapeTableCell(row.Benchmark))
			sb.WriteString("` | ")
			sb.WriteString(formatFloat(row.IndexesPerDocValue))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(row.StoredDocsValue))
			sb.WriteString(" | ")
			sb.WriteString(formatByteCount(row.DiskTotalBytesValue))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(row.DiskBytesPerDocValue))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalByteCount(row.CollectionDiskBytesValue))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalFloat(row.CollectionDiskBPerDocValue))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalByteCount(row.IndexDiskBytesValue))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalFloat(row.IndexDiskBPerDocValue))
			sb.WriteString(" | `")
			sb.WriteString(escapeTableCell(row.SplitSource))
			sb.WriteString("` | [report](")
			sb.WriteString(markdownLinkPath(reportPath))
			sb.WriteString(") |\n")
		}
		sb.WriteString("\n")
	}
	if len(maintenanceRows) > 0 {
		sb.WriteString("## Maintenance Compaction\n\n")
		sb.WriteString("TreeDB `treedb_vlog_rewrite` rows measure value_vlog rewrite/GC followed by index vacuum when enabled. TreeDB `treedb_leafgen_pack_gc` rows measure leaf_vlog generation pack/GC followed by index vacuum when enabled. SQLite rows show total disk before and after full `VACUUM`.\n\n")
		sb.WriteString("| Cell | Engine | Format | Data vlog | Index vlog | Pager chunk | Pager sync | Maintenance | Benchmark | ns/op | ops/sec | GC ns/op | GC ops/sec | Vacuum ns/op | Vacuum ops/sec | Before | After | Delta | After GC | Delta after GC | After GC+vacuum | Delta after GC+vacuum | Frames | Max K | Report |\n")
		sb.WriteString("| --- | --- | --- | ---: | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n")
		for _, row := range maintenanceRows {
			reportPath := relativeReportPath(outDir, row.ReportMarkdownPath)
			sb.WriteString("| `")
			sb.WriteString(escapeTableCell(row.Cell))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.Engine))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.DocumentFormat))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.DataOuterLeavesInVLog))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.IndexOuterLeavesInVLog))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.PagerChunkSize))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.PagerSyncConcurrency))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.Kind))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.Benchmark))
			sb.WriteString("` | ")
			sb.WriteString(formatOptionalFloat(row.NsPerMaint))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalFloat(optionalOpsPerSecFromNs(row.NsPerMaint)))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalFloat(row.GCNs))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalFloat(optionalOpsPerSecFromNs(row.GCNs)))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalFloat(row.VacuumNs))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalFloat(optionalOpsPerSecFromNs(row.VacuumNs)))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalByteCount(row.Before))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalByteCount(row.After))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalByteCount(row.Delta))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalByteCount(row.AfterGC))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalByteCount(row.DeltaGC))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalByteCount(row.AfterVac))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalByteCount(row.DeltaVac))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalFloat(row.Frames))
			sb.WriteString(" | ")
			sb.WriteString(formatOptionalFloat(row.MaxFrameK))
			sb.WriteString(" | [report](")
			sb.WriteString(markdownLinkPath(reportPath))
			sb.WriteString(") |\n")
		}
		sb.WriteString("\n")
	}
	if len(diagnosticRows) > 0 {
		sb.WriteString("## Diagnostic Rows\n\n")
		sb.WriteString("These rows are not user stories. They isolate JSON/data extraction and non-JSON planner cost so future optimization can quarantine JSON work separately from TreeDB publish/index maintenance.\n\n")
		sb.WriteString("| Cell | Engine | Format | Pager chunk | Pager sync | Diagnostic | ns/doc | ops/sec | B/doc | allocs/doc | Report |\n")
		sb.WriteString("| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |\n")
		for _, row := range diagnosticRows {
			reportPath := relativeReportPath(outDir, row.ReportMarkdownPath)
			sb.WriteString("| `")
			sb.WriteString(escapeTableCell(row.Cell))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.Engine))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.DocumentFormat))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.PagerChunkSize))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.PagerSyncConcurrency))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.Benchmark))
			sb.WriteString("` | ")
			sb.WriteString(formatFloat(row.NsPerOp))
			sb.WriteString(" | ")
			sb.WriteString(formatThroughput(opsPerSecFromNs(row.NsPerOp)))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(row.BytesPerOp))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(row.AllocsPerOp))
			sb.WriteString(" | [report](")
			sb.WriteString(markdownLinkPath(reportPath))
			sb.WriteString(") |\n")
		}
		sb.WriteString("\n")
	}
	sb.WriteString("## Raw Matrix\n\n")
	sb.WriteString("| Cell | Engine | Format | Data vlog | Index vlog | Pager chunk | Pager sync | Benchmark | ns/op | ops/sec | B/op | allocs/op | insert ns/doc | insert docs/sec | sync ns/doc | sync docs/sec | writer docs/sec | Key fallbacks | Prefix fallbacks | indexes/doc | stored docs | disk total | disk B/doc | collection disk | collection B/doc | index disk | index B/doc | Report |\n")
	sb.WriteString("| --- | --- | --- | ---: | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n")
	for _, row := range rows {
		reportPath := relativeReportPath(outDir, row.ReportMarkdownPath)
		sb.WriteString("| `")
		sb.WriteString(escapeTableCell(row.Cell))
		sb.WriteString("` | `")
		sb.WriteString(escapeTableCell(row.Engine))
		sb.WriteString("` | `")
		sb.WriteString(escapeTableCell(row.DocumentFormat))
		sb.WriteString("` | `")
		sb.WriteString(escapeTableCell(row.DataOuterLeavesInVLog))
		sb.WriteString("` | `")
		sb.WriteString(escapeTableCell(row.IndexOuterLeavesInVLog))
		sb.WriteString("` | `")
		sb.WriteString(escapeTableCell(row.PagerChunkSize))
		sb.WriteString("` | `")
		sb.WriteString(escapeTableCell(row.PagerSyncConcurrency))
		sb.WriteString("` | `")
		sb.WriteString(escapeTableCell(row.Benchmark))
		sb.WriteString("` | ")
		sb.WriteString(formatFloat(row.NsPerOp))
		sb.WriteString(" | ")
		sb.WriteString(formatThroughput(opsPerSecFromNs(row.NsPerOp)))
		sb.WriteString(" | ")
		sb.WriteString(formatFloat(row.BytesPerOp))
		sb.WriteString(" | ")
		sb.WriteString(formatFloat(row.AllocsPerOp))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalFloat(row.InsertNsPerDoc))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalFloat(optionalOpsPerSecFromNs(row.InsertNsPerDoc)))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalFloat(row.SyncNsPerDoc))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalFloat(optionalOpsPerSecFromNs(row.SyncNsPerDoc)))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalFloat(row.WriterDocsPerSec))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalFloat(row.KeyFallbacks))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalFloat(row.PrefixFallbacks))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalFloat(row.IndexesPerDoc))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalFloat(row.StoredDocs))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalByteCount(row.DiskTotalBytes))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalFloat(row.DiskBytesPerDoc))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalByteCount(row.CollectionDiskBytes))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalFloat(row.CollectionDiskBPerDoc))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalByteCount(row.IndexDiskBytes))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalFloat(row.IndexDiskBPerDoc))
		sb.WriteString(" | [report](")
		sb.WriteString(markdownLinkPath(reportPath))
		sb.WriteString(") |\n")
	}
	return sb.String(), nil
}

func relativeReportPath(outDir, reportPath string) string {
	if rel, err := filepath.Rel(outDir, reportPath); err == nil {
		return filepath.ToSlash(rel)
	}
	return filepath.ToSlash(reportPath)
}

func buildExecutiveSummary(userRows []userStoryRow, diskRows []diskUsageRow, maintenanceRows []maintenanceRow, diagnosticRows []summaryRow) []string {
	var lines []string
	for _, story := range sortedUserStories(userRows) {
		best, ok := fastestUserStory(userRows, story)
		if !ok {
			continue
		}
		lines = append(lines, fmt.Sprintf("Fastest %s: `%s` / `%s` / `%s` at %s docs/sec (%s ns/doc, %s ms/batch, %s batches/sec).",
			story,
			best.Cell,
			best.DocumentFormat,
			best.Benchmark,
			formatThroughput(best.DocsPerSec),
			formatFloat(best.NsPerOp),
			formatFloat(best.MSPerBatch),
			formatFloat(best.BatchesSec),
		))
	}
	if best, ok := lowestTwoIndexDiskUsage(diskRows); ok {
		lines = append(lines, fmt.Sprintf("Smallest two-index bulk-insert footprint: `%s` / `%s` at %s total (%s B/doc; index estimate %s B/doc from `%s`).",
			best.Cell,
			best.DocumentFormat,
			formatByteCount(best.DiskTotalBytesValue),
			formatFloat(best.DiskBytesPerDocValue),
			formatOptionalFloat(best.IndexDiskBPerDocValue),
			best.SplitSource,
		))
	}
	for _, kind := range sortedMaintenanceKinds(maintenanceRows) {
		if best, ok := largestMaintenanceReduction(maintenanceRows, kind); ok {
			delta := best.Delta
			after := best.After
			rateNs := best.NsPerMaint
			if best.DeltaVac != nil {
				delta = best.DeltaVac
				after = best.AfterVac
				rateNs = best.VacuumNs
			} else if best.DeltaGC != nil {
				delta = best.DeltaGC
				after = best.AfterGC
				rateNs = best.GCNs
			}
			lines = append(lines, fmt.Sprintf("Largest %s disk reduction: `%s` / `%s` changed total disk by %s to %s (%s ops/sec maintenance rate).",
				kind,
				best.Cell,
				best.DocumentFormat,
				formatOptionalByteCount(delta),
				formatOptionalByteCount(after),
				formatOptionalFloat(optionalOpsPerSecFromNs(rateNs)),
			))
			continue
		}
		if best, ok := smallestMaintenanceDelta(maintenanceRows, kind); ok {
			delta := best.Delta
			after := best.After
			if best.DeltaVac != nil {
				delta = best.DeltaVac
				after = best.AfterVac
			} else if best.DeltaGC != nil {
				delta = best.DeltaGC
				after = best.AfterGC
			}
			lines = append(lines, fmt.Sprintf("No %s disk reduction was observed; smallest total-disk delta was %s to %s in `%s` / `%s`.",
				kind,
				formatOptionalByteCount(delta),
				formatOptionalByteCount(after),
				best.Cell,
				best.DocumentFormat,
			))
		}
	}
	if best, ok := fastestDiagnostic(diagnosticRows); ok {
		lines = append(lines, fmt.Sprintf("Fastest diagnostic row: `%s` in `%s` / `%s` at %s ops/sec (%s ns/op).",
			best.Benchmark,
			best.Cell,
			best.DocumentFormat,
			formatThroughput(opsPerSecFromNs(best.NsPerOp)),
			formatFloat(best.NsPerOp),
		))
	}
	return lines
}

func sortedUserStories(rows []userStoryRow) []string {
	seen := make(map[string]struct{})
	for _, row := range rows {
		seen[row.Story] = struct{}{}
	}
	stories := make([]string, 0, len(seen))
	for story := range seen {
		stories = append(stories, story)
	}
	sort.Strings(stories)
	return stories
}

func fastestUserStory(rows []userStoryRow, story string) (userStoryRow, bool) {
	var best userStoryRow
	var ok bool
	for _, row := range rows {
		if row.Story != story {
			continue
		}
		if !ok || row.DocsPerSec > best.DocsPerSec {
			best = row
			ok = true
		}
	}
	return best, ok
}

func lowestTwoIndexDiskUsage(rows []diskUsageRow) (diskUsageRow, bool) {
	var best diskUsageRow
	var ok bool
	for _, row := range rows {
		if row.Story != "bulk indexed insert" || row.IndexesPerDocValue != 2 {
			continue
		}
		if !ok || row.DiskBytesPerDocValue < best.DiskBytesPerDocValue {
			best = row
			ok = true
		}
	}
	return best, ok
}

func sortedMaintenanceKinds(rows []maintenanceRow) []string {
	seen := make(map[string]struct{})
	for _, row := range rows {
		seen[row.Kind] = struct{}{}
	}
	kinds := make([]string, 0, len(seen))
	for kind := range seen {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

func largestMaintenanceReduction(rows []maintenanceRow, kind string) (maintenanceRow, bool) {
	var best maintenanceRow
	var bestDelta float64
	var ok bool
	for _, row := range rows {
		if row.Kind != kind {
			continue
		}
		delta, hasDelta := maintenanceReductionDelta(row)
		if !hasDelta {
			continue
		}
		if delta >= 0 {
			continue
		}
		if !ok || delta < bestDelta {
			best = row
			bestDelta = delta
			ok = true
		}
	}
	return best, ok
}

func smallestMaintenanceDelta(rows []maintenanceRow, kind string) (maintenanceRow, bool) {
	var best maintenanceRow
	var bestDelta float64
	var ok bool
	for _, row := range rows {
		if row.Kind != kind {
			continue
		}
		delta, hasDelta := maintenanceReductionDelta(row)
		if !hasDelta {
			continue
		}
		if !ok || delta < bestDelta {
			best = row
			bestDelta = delta
			ok = true
		}
	}
	return best, ok
}

func maintenanceReductionDelta(row maintenanceRow) (float64, bool) {
	if row.DeltaVac != nil {
		return *row.DeltaVac, true
	}
	if row.DeltaGC != nil {
		return *row.DeltaGC, true
	}
	if row.Delta != nil {
		return *row.Delta, true
	}
	return 0, false
}

func fastestDiagnostic(rows []summaryRow) (summaryRow, bool) {
	var best summaryRow
	var ok bool
	for _, row := range rows {
		if row.NsPerOp <= 0 {
			continue
		}
		if !ok || row.NsPerOp < best.NsPerOp {
			best = row
			ok = true
		}
	}
	return best, ok
}

func buildUserStoryRows(rows []summaryRow) []userStoryRow {
	out := make([]userStoryRow, 0, len(rows))
	for _, row := range rows {
		story, ok := userStoryLabel(row.Benchmark)
		if !ok || row.NsPerOp <= 0 || row.CollectionBatchSize <= 0 {
			continue
		}
		batchSize := row.CollectionBatchSize
		docsPerSec := 1e9 / row.NsPerOp
		out = append(out, userStoryRow{
			summaryRow: row,
			Story:      story,
			DocsPerSec: docsPerSec,
			MSPerBatch: float64(batchSize) * row.NsPerOp / 1e6,
			BatchesSec: docsPerSec / float64(batchSize),
		})
	}
	return out
}

type diskUsageKey struct {
	Cell                   string
	Engine                 string
	DocumentFormat         string
	BenchmarkFamily        string
	DataOuterLeavesInVLog  string
	IndexOuterLeavesInVLog string
	PagerChunkSize         string
	PagerSyncConcurrency   string
	Story                  string
}

func buildDiskUsageRows(rows []summaryRow) []diskUsageRow {
	candidates := make([]summaryRow, 0, len(rows))
	baselines := make(map[diskUsageKey]summaryRow)
	for _, row := range rows {
		story, ok := userStoryLabel(row.Benchmark)
		if !ok || row.DiskTotalBytes == nil || row.StoredDocs == nil || row.IndexesPerDoc == nil || *row.StoredDocs <= 0 {
			continue
		}
		candidates = append(candidates, row)
		if *row.IndexesPerDoc == 0 {
			key := diskUsageGroupKey(row, story)
			rowBPerDoc, rowOK := summaryRowDiskBytesPerDoc(row)
			existing, hasExisting := baselines[key]
			existingBPerDoc, existingOK := summaryRowDiskBytesPerDoc(existing)
			if rowOK && (!hasExisting || !existingOK || rowBPerDoc < existingBPerDoc) {
				baselines[key] = row
			}
		}
	}

	out := make([]diskUsageRow, 0, len(candidates))
	for _, row := range candidates {
		story, _ := userStoryLabel(row.Benchmark)
		totalBytes := *row.DiskTotalBytes
		storedDocs := *row.StoredDocs
		totalBPerDoc := totalBytes / storedDocs
		if row.DiskBytesPerDoc != nil {
			totalBPerDoc = *row.DiskBytesPerDoc
		}
		usageRow := diskUsageRow{
			summaryRow:                 row,
			Story:                      story,
			IndexesPerDocValue:         *row.IndexesPerDoc,
			StoredDocsValue:            storedDocs,
			DiskTotalBytesValue:        totalBytes,
			DiskBytesPerDocValue:       totalBPerDoc,
			CollectionDiskBytesValue:   row.CollectionDiskBytes,
			CollectionDiskBPerDocValue: row.CollectionDiskBPerDoc,
			IndexDiskBytesValue:        row.IndexDiskBytes,
			IndexDiskBPerDocValue:      row.IndexDiskBPerDoc,
			SplitSource:                "reported",
		}
		if usageRow.CollectionDiskBytesValue == nil || usageRow.IndexDiskBytesValue == nil {
			baseline, ok := baselines[diskUsageGroupKey(row, story)]
			collectionBPerDoc, ok := summaryRowDiskBytesPerDoc(baseline)
			if !ok {
				usageRow.SplitSource = "total_only"
				out = append(out, usageRow)
				continue
			}
			indexBPerDoc := totalBPerDoc - collectionBPerDoc
			if indexBPerDoc < 0 {
				indexBPerDoc = 0
			}
			collectionBytes := collectionBPerDoc * storedDocs
			indexBytes := indexBPerDoc * storedDocs
			usageRow.CollectionDiskBytesValue = &collectionBytes
			usageRow.CollectionDiskBPerDocValue = &collectionBPerDoc
			usageRow.IndexDiskBytesValue = &indexBytes
			usageRow.IndexDiskBPerDocValue = &indexBPerDoc
			usageRow.SplitSource = "zero_index_delta"
		}
		out = append(out, usageRow)
	}
	return out
}

func buildMaintenanceRows(rows []summaryRow) []maintenanceRow {
	out := make([]maintenanceRow, 0, len(rows))
	for _, row := range rows {
		if row.VLogRewriteBefore != nil || row.VLogRewriteAfter != nil || row.VLogRewriteAfterGC != nil || row.VLogRewriteAfterVac != nil {
			out = append(out, maintenanceRow{
				summaryRow: row,
				Kind:       "treedb_vlog_rewrite",
				NsPerMaint: row.VLogRewriteNs,
				GCNs:       row.VLogGCNs,
				Before:     row.VLogRewriteBefore,
				After:      row.VLogRewriteAfter,
				Delta:      row.VLogRewriteDelta,
				AfterGC:    row.VLogRewriteAfterGC,
				DeltaGC:    row.VLogRewriteDeltaGC,
				VacuumNs:   row.VLogRewriteVacuumNs,
				AfterVac:   row.VLogRewriteAfterVac,
				DeltaVac:   row.VLogRewriteDeltaVac,
			})
		}
		if row.LeafGenPackBefore != nil || row.LeafGenPackAfter != nil || row.LeafGenPackAfterGC != nil || row.LeafGenPackAfterVac != nil {
			out = append(out, maintenanceRow{
				summaryRow: row,
				Kind:       "treedb_leafgen_pack_gc",
				NsPerMaint: row.LeafGenPackNs,
				GCNs:       row.LeafGenGCNs,
				Before:     row.LeafGenPackBefore,
				After:      row.LeafGenPackAfter,
				Delta:      row.LeafGenPackDelta,
				AfterGC:    row.LeafGenPackAfterGC,
				DeltaGC:    row.LeafGenPackDeltaGC,
				VacuumNs:   row.LeafGenPackVacuumNs,
				AfterVac:   row.LeafGenPackAfterVac,
				DeltaVac:   row.LeafGenPackDeltaVac,
				Frames:     row.LeafGenPackFrames,
				MaxFrameK:  row.LeafGenPackMaxFrameK,
			})
		}
		if row.SQLiteVacuumBefore != nil || row.SQLiteVacuumAfter != nil {
			out = append(out, maintenanceRow{
				summaryRow: row,
				Kind:       "sqlite_vacuum",
				NsPerMaint: row.SQLiteVacuumNs,
				Before:     row.SQLiteVacuumBefore,
				After:      row.SQLiteVacuumAfter,
				Delta:      row.SQLiteVacuumDelta,
			})
		}
	}
	return out
}

func summaryRowDiskBytesPerDoc(row summaryRow) (float64, bool) {
	if row.DiskBytesPerDoc != nil {
		return *row.DiskBytesPerDoc, true
	}
	if row.DiskTotalBytes == nil || row.StoredDocs == nil || *row.StoredDocs <= 0 {
		return 0, false
	}
	return *row.DiskTotalBytes / *row.StoredDocs, true
}

func diskUsageGroupKey(row summaryRow, story string) diskUsageKey {
	return diskUsageKey{
		Cell:                   row.Cell,
		Engine:                 row.Engine,
		DocumentFormat:         row.DocumentFormat,
		BenchmarkFamily:        benchmarkBaseName(row.Benchmark),
		DataOuterLeavesInVLog:  row.DataOuterLeavesInVLog,
		IndexOuterLeavesInVLog: row.IndexOuterLeavesInVLog,
		PagerChunkSize:         row.PagerChunkSize,
		PagerSyncConcurrency:   row.PagerSyncConcurrency,
		Story:                  story,
	}
}

func userStoryLabel(benchmark string) (string, bool) {
	switch benchmarkBaseName(benchmark) {
	case "BenchmarkCollectionInsertBatchWithSecondaryIndexes",
		"BenchmarkCollectionTimedProfileInsertBatchWithSecondaryIndexes",
		"BenchmarkCollectionShapeInsertBatch",
		"BenchmarkCollectionShapeInsertBatchSingleStringJSON",
		"BenchmarkSQLiteInsertBatchWithSecondaryIndexes",
		"BenchmarkSQLiteNativeColumnsInsertBatchWithSecondaryIndexes",
		"BenchmarkSQLiteShapeInsertBatchJSON",
		"BenchmarkSQLiteShapeInsertBatchNativeColumns":
		return "bulk indexed insert", true
	case "BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes",
		"BenchmarkCollectionTimedProfileInsertBatchCheckpointWithSecondaryIndexes",
		"BenchmarkCollectionShapeInsertBatchCheckpoint",
		"BenchmarkCollectionShapeInsertBatchCheckpointSingleStringJSON",
		"BenchmarkSQLiteInsertBatchCheckpointWithSecondaryIndexes",
		"BenchmarkSQLiteNativeColumnsInsertBatchCheckpointWithSecondaryIndexes",
		"BenchmarkSQLiteShapeInsertBatchCheckpointJSON",
		"BenchmarkSQLiteShapeInsertBatchCheckpointNativeColumns":
		return "checkpointed indexed insert", true
	default:
		return "", false
	}
}

func buildDiagnosticRows(rows []summaryRow) []summaryRow {
	out := make([]summaryRow, 0, len(rows))
	for _, row := range rows {
		if isDiagnosticBenchmark(row.Benchmark) {
			out = append(out, row)
		}
	}
	return out
}

func isDiagnosticBenchmark(benchmark string) bool {
	switch benchmarkBaseName(benchmark) {
	case "BenchmarkCollectionOverheadIndexStateJSONExtraction",
		"BenchmarkCollectionOverheadIndexStateTemplateV1Extraction",
		"BenchmarkCollectionOverheadPlanIndexedTemplateV1",
		"BenchmarkCollectionOverheadPlanIndexedPrecomputedState":
		return true
	default:
		return false
	}
}

func markdownToHTMLDoc(markdown string) ([]byte, error) {
	var body bytes.Buffer
	md := goldmark.New(goldmark.WithExtensions(extension.GFM))
	if err := md.Convert([]byte(markdown), &body); err != nil {
		return nil, fmt.Errorf("render markdown: %w", err)
	}
	const pageTmpl = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Collections Benchmark Matrix</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 2rem auto; max-width: 1280px; padding: 0 1rem; line-height: 1.5; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #d0d7de; padding: 0.45rem 0.65rem; vertical-align: top; }
    th { background: #f6f8fa; text-align: left; }
    code { background: #f6f8fa; padding: 0.1rem 0.25rem; border-radius: 4px; }
  </style>
</head>
<body>{{.Body}}</body>
</html>`
	tmpl, err := template.New("page").Parse(pageTmpl)
	if err != nil {
		return nil, err
	}
	var doc bytes.Buffer
	if err := tmpl.Execute(&doc, struct{ Body template.HTML }{Body: template.HTML(body.String())}); err != nil {
		return nil, err
	}
	return doc.Bytes(), nil
}

func markdownLinkPath(path string) string {
	return strings.ReplaceAll(path, " ", "%20")
}

func escapeTableCell(value string) string {
	return strings.ReplaceAll(value, "|", "\\|")
}

func formatOptionalFloat(value *float64) string {
	if value == nil {
		return "-"
	}
	return formatFloat(*value)
}

func formatOptionalByteCount(value *float64) string {
	if value == nil {
		return "-"
	}
	return formatByteCount(*value)
}

func formatByteCount(value float64) string {
	return formatFloat(value)
}

func formatThroughput(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "-"
	}
	return formatIntWithCommas(int64(math.Round(value)))
}

func formatFloat(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "-"
	}
	rounded := math.Round(value*100) / 100
	if math.Abs(rounded-math.Round(rounded)) < 0.005 {
		return formatIntWithCommas(int64(math.Round(rounded)))
	}
	return trimTrailingZeros(formatWithCommas(rounded))
}

func formatWithCommas(value float64) string {
	negative := value < 0
	if negative {
		value = -value
	}
	whole := int64(value)
	fraction := value - float64(whole)
	base := formatIntWithCommas(whole)
	if fraction == 0 {
		if negative {
			return "-" + base
		}
		return base
	}
	text := fmt.Sprintf("%s%.2f", "", fraction)
	text = strings.TrimPrefix(text, "0")
	if negative {
		return "-" + base + text
	}
	return base + text
}

func trimTrailingZeros(value string) string {
	value = strings.TrimRight(value, "0")
	value = strings.TrimRight(value, ".")
	return value
}

func formatIntWithCommas(value int64) string {
	negative := value < 0
	if negative {
		value = -value
	}
	raw := strconv.FormatInt(value, 10)
	if len(raw) <= 3 {
		if negative {
			return "-" + raw
		}
		return raw
	}
	var parts []string
	for len(raw) > 3 {
		parts = append(parts, raw[len(raw)-3:])
		raw = raw[:len(raw)-3]
	}
	parts = append(parts, raw)
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	result := strings.Join(parts, ",")
	if negative {
		return "-" + result
	}
	return result
}
