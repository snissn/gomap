package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/experiments/colgranule"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type clickHouseResult struct {
	System             string                       `json:"system"`
	Version            string                       `json:"version"`
	OS                 string                       `json:"os"`
	Machine            string                       `json:"machine"`
	DatasetSize        int                          `json:"dataset_size"`
	NumLoadedDocuments int                          `json:"num_loaded_documents"`
	TotalSize          int64                        `json:"total_size"`
	DataSize           int64                        `json:"data_size"`
	IndexSize          int64                        `json:"index_size"`
	Result             [][]float64                  `json:"result"`
	Q4Fairness         []clickHouseQ4FairnessResult `json:"q4_fairness"`
}

type clickHouseQ4FairnessResult struct {
	Query       string    `json:"query"`
	Description string    `json:"description"`
	Table       string    `json:"table"`
	SortKey     string    `json:"sort_key"`
	QueryShape  string    `json:"query_shape"`
	Attempts    []float64 `json:"attempts"`
	Best        float64   `json:"best"`
}

type comparisonRaw struct {
	GeneratedAt              string                                `json:"generated_at"`
	DataPath                 string                                `json:"data_path"`
	Limit                    int                                   `json:"limit"`
	Rows                     int                                   `json:"rows"`
	Files                    []string                              `json:"files"`
	InputBytes               int64                                 `json:"input_bytes"`
	RowsPerGranule           int                                   `json:"rows_per_granule"`
	LoadDuration             time.Duration                         `json:"load_duration"`
	ClickHouseLocal          clickHouseResult                      `json:"clickhouse_local"`
	RemainingTreeDB          remainingTreeDBResult                 `json:"remaining_treedb"`
	RemainingTreeDBJSON      remainingTreeDBResult                 `json:"remaining_treedb_json"`
	RemainingTreeDBTpl       remainingTreeDBResult                 `json:"remaining_treedb_template_v1"`
	ConservativeBSON         remainingTreeDBResult                 `json:"conservative_remaining_treedb_bson"`
	ConservativeJSON         remainingTreeDBResult                 `json:"conservative_remaining_treedb_json"`
	ConservativeTpl          remainingTreeDBResult                 `json:"conservative_remaining_treedb_template_v1"`
	RawTreeDBJSON            remainingTreeDBResult                 `json:"raw_treedb_json"`
	QueryTimings             []colgranule.JSONBenchQueryTiming     `json:"query_timings"`
	EncodedPartQueryTimings  []colgranule.JSONBenchPartQueryTiming `json:"encoded_part_query_timings"`
	EncodedPartQ4Fairness    []colgranule.JSONBenchPartQueryTiming `json:"encoded_part_q4_fairness_timings"`
	AggregateMetadataTimings []colgranule.JSONBenchPartQueryTiming `json:"aggregate_metadata_timings"`
	EncodedPartBuildReports  []colgranule.JSONBenchPartBuildReport `json:"encoded_part_build_reports"`
	ColumnSummaries          []colgranule.ColumnCodecSummary       `json:"column_summaries"`
	BestColumnStorage        []bestColumnStorage                   `json:"best_column_storage"`
}

type remainingTreeDBResult struct {
	Enabled              bool    `json:"enabled"`
	DocumentFormat       string  `json:"document_format"`
	StoragePolicy        string  `json:"storage_policy"`
	DBDir                string  `json:"db_dir"`
	Rows                 int     `json:"rows"`
	RawDocumentBytes     int64   `json:"raw_document_bytes"`
	LoadDuration         float64 `json:"load_seconds"`
	FlushDuration        float64 `json:"flush_seconds"`
	CheckpointDuration   float64 `json:"checkpoint_seconds"`
	BeforeCompactBytes   int64   `json:"before_compact_bytes"`
	BeforeCompactFiles   int     `json:"before_compact_files"`
	AfterCompactBytes    int64   `json:"after_compact_bytes"`
	AfterCompactFiles    int     `json:"after_compact_files"`
	CompactionDuration   float64 `json:"compaction_seconds"`
	FullyCompacted       bool    `json:"fully_compacted"`
	CompactedBytesPerRow float64 `json:"compacted_bytes_per_row"`
	RewriteDuration      float64 `json:"rewrite_duration_seconds"`
	RewriteRecordsCopied int     `json:"rewrite_records_copied"`
	RewriteValueBytes    int64   `json:"rewrite_value_bytes"`
	RewriteSourceBytes   int64   `json:"rewrite_source_bytes"`
	RewriteReclaimFiles  int     `json:"rewrite_reclaim_files"`
	RewriteReclaimBytes  int64   `json:"rewrite_reclaim_bytes"`
	StoredShape          string  `json:"stored_shape"`
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

type retainedPayloadOption struct {
	Label  string
	Result remainingTreeDBResult
}

type remainingShape string

const (
	remainingShapeClickHouseTyped remainingShape = "clickhouse_typed_paths_removed"
	remainingShapeConservative    remainingShape = "time_us_only_removed"
)

func main() {
	data := flag.String("data", colgranule.DefaultJSONBenchDir, "JSONBench input file or directory")
	limit := flag.Int("limit", 1_000_000, "maximum rows to load; <=0 means all rows")
	rowsPerGranule := flag.Int("rows-per-granule", colgranule.DefaultRowsPerGranule, "rows per encoded granule")
	attempts := flag.Int("attempts", 5, "query timing attempts")
	clickHouseLocalPath := flag.String("clickhouse-local", "", "optional local ClickHouse JSONBench result")
	remainingDBDir := flag.String("remaining-db-dir", "artifacts/colgranule_remaining_treedb", "temporary TreeDB directory prefix for remaining payload measurement")
	measureRemaining := flag.Bool("measure-remaining-treedb", true, "load remaining JSON shapes into JSON, BSON, and Template-v1 TreeDB collections, compact them, and include disk usage")
	retainedPayloadFromJSON := flag.String("retained-payload-from-json", "", "optional jsonbench_compare raw JSON file whose retained-payload measurements should be reused when -measure-remaining-treedb=false")
	outJSON := flag.String("out-json", "artifacts/colgranule/JSONBENCH_COMPARISON_RAW.json", "raw JSON output")
	outMarkdown := flag.String("out-md", "artifacts/colgranule/JSONBENCH_COMPARISON_REPORT.md", "Markdown report output")
	flag.Parse()
	if *measureRemaining && *retainedPayloadFromJSON != "" {
		must(errors.New("-retained-payload-from-json requires -measure-remaining-treedb=false"))
	}

	start := time.Now()
	ds, err := colgranule.LoadJSONBenchColumns(*data, *limit)
	must(err)
	loadDuration := time.Since(start)

	summaries, err := colgranule.SummarizeJSONBenchDataset(ds, *rowsPerGranule, colgranule.DefaultJSONBenchConfigs())
	must(err)
	timings, err := colgranule.RunJSONBenchQueries(ds, *attempts)
	must(err)
	encodedPartTimings, err := colgranule.RunJSONBenchPartQueries(ds, *rowsPerGranule, *attempts)
	must(err)
	encodedPartQ4Fairness, err := colgranule.RunJSONBenchPartQ4FairnessQueries(ds, *rowsPerGranule, *attempts)
	must(err)
	must(validateEncodedPartParity(timings, encodedPartTimings))
	aggregateMetadataTimings, err := colgranule.RunJSONBenchPartAggregateMetadataQueries(ds, *rowsPerGranule, *attempts)
	if err != nil {
		if strings.Contains(err.Error(), "rejected by admission") {
			fmt.Fprintf(os.Stderr, "skipping aggregate metadata timings: %v\n", err)
		} else {
			must(err)
		}
	}
	encodedPartBuildReports, err := colgranule.RunJSONBenchPartBuildReports(ds, *rowsPerGranule, *attempts)
	must(err)
	var remaining remainingTreeDBResult
	var remainingJSON remainingTreeDBResult
	var remainingTpl remainingTreeDBResult
	var conservativeBSON remainingTreeDBResult
	var conservativeJSON remainingTreeDBResult
	var conservativeTpl remainingTreeDBResult
	var rawTreeDBJSON remainingTreeDBResult
	if *measureRemaining {
		remaining, err = measureRemainingTreeDB(context.Background(), ds.Files, ds.Rows, *remainingDBDir+"-clickhouse-bson", collections.DocumentFormatBSON, remainingShapeClickHouseTyped)
		must(err)
		remainingJSON, err = measureRemainingTreeDB(context.Background(), ds.Files, ds.Rows, *remainingDBDir+"-clickhouse-json", collections.DocumentFormatJSON, remainingShapeClickHouseTyped)
		must(err)
		remainingTpl, err = measureRemainingTreeDB(context.Background(), ds.Files, ds.Rows, *remainingDBDir+"-clickhouse-template-v1", collections.DocumentFormatTemplateV1, remainingShapeClickHouseTyped)
		must(err)
		conservativeBSON, err = measureRemainingTreeDB(context.Background(), ds.Files, ds.Rows, *remainingDBDir+"-conservative-bson", collections.DocumentFormatBSON, remainingShapeConservative)
		must(err)
		conservativeJSON, err = measureRemainingTreeDB(context.Background(), ds.Files, ds.Rows, *remainingDBDir+"-conservative-json", collections.DocumentFormatJSON, remainingShapeConservative)
		must(err)
		conservativeTpl, err = measureRemainingTreeDB(context.Background(), ds.Files, ds.Rows, *remainingDBDir+"-conservative-template-v1", collections.DocumentFormatTemplateV1, remainingShapeConservative)
		must(err)
		rawTreeDBJSON, err = measureRawJSONTreeDB(context.Background(), ds.Files, ds.Rows, *remainingDBDir+"-raw-json")
		must(err)
	} else if *retainedPayloadFromJSON != "" {
		retainedRaw := readComparisonRaw(*retainedPayloadFromJSON)
		remaining = retainedRaw.RemainingTreeDB
		remainingJSON = retainedRaw.RemainingTreeDBJSON
		remainingTpl = retainedRaw.RemainingTreeDBTpl
		conservativeBSON = retainedRaw.ConservativeBSON
		conservativeJSON = retainedRaw.ConservativeJSON
		conservativeTpl = retainedRaw.ConservativeTpl
		rawTreeDBJSON = retainedRaw.RawTreeDBJSON
	}

	raw := comparisonRaw{
		GeneratedAt:              time.Now().UTC().Format(time.RFC3339),
		DataPath:                 *data,
		Limit:                    *limit,
		Rows:                     ds.Rows,
		Files:                    ds.Files,
		InputBytes:               inputBytes(ds.Files),
		RowsPerGranule:           *rowsPerGranule,
		LoadDuration:             loadDuration,
		ClickHouseLocal:          readClickHouseResult(*clickHouseLocalPath),
		RemainingTreeDB:          remaining,
		RemainingTreeDBJSON:      remainingJSON,
		RemainingTreeDBTpl:       remainingTpl,
		ConservativeBSON:         conservativeBSON,
		ConservativeJSON:         conservativeJSON,
		ConservativeTpl:          conservativeTpl,
		RawTreeDBJSON:            rawTreeDBJSON,
		QueryTimings:             timings,
		EncodedPartQueryTimings:  encodedPartTimings,
		EncodedPartQ4Fairness:    encodedPartQ4Fairness,
		AggregateMetadataTimings: aggregateMetadataTimings,
		EncodedPartBuildReports:  encodedPartBuildReports,
		ColumnSummaries:          summaries,
		BestColumnStorage:        bestColumns(summaries),
	}

	writeJSON(*outJSON, raw)
	writeMarkdown(*outMarkdown, raw)
}

func readClickHouseResult(path string) clickHouseResult {
	if path == "" {
		return clickHouseResult{System: "not provided"}
	}
	data, err := os.ReadFile(path)
	must(err)
	var result clickHouseResult
	must(json.Unmarshal(data, &result))
	return result
}

func readComparisonRaw(path string) comparisonRaw {
	data, err := os.ReadFile(path)
	must(err)
	var result comparisonRaw
	must(json.Unmarshal(data, &result))
	return result
}

func validateEncodedPartParity(reference []colgranule.JSONBenchQueryTiming, encoded []colgranule.JSONBenchPartQueryTiming) error {
	if len(encoded) == 0 {
		return nil
	}
	if len(reference) != len(encoded) {
		return fmt.Errorf("encoded part query count=%d want baseline count=%d", len(encoded), len(reference))
	}
	for i, got := range encoded {
		want := reference[i]
		if got.Query != want.Query {
			return fmt.Errorf("encoded part query[%d]=%s want baseline query %s", i, got.Query, want.Query)
		}
		if got.ResultRows != want.ResultRows || got.ResultDigest != want.ResultDigest {
			return fmt.Errorf("encoded part %s result rows/digest=(%d,%d) want baseline (%d,%d)", got.Query, got.ResultRows, got.ResultDigest, want.ResultRows, want.ResultDigest)
		}
		for attemptIndex, attempt := range got.Attempts {
			if attempt.ResultRows != want.ResultRows || attempt.ResultDigest != want.ResultDigest {
				return fmt.Errorf("encoded part %s attempt %d rows/digest=(%d,%d) want baseline (%d,%d)", got.Query, attemptIndex, attempt.ResultRows, attempt.ResultDigest, want.ResultRows, want.ResultDigest)
			}
		}
	}
	return nil
}

func measureRemainingTreeDB(ctx context.Context, files []string, rows int, dbDir string, format collections.DocumentFormat, shape remainingShape) (remainingTreeDBResult, error) {
	var out remainingTreeDBResult
	out.Enabled = true
	out.DocumentFormat = string(format)
	out.StoragePolicy = string(collections.RootStorageCompressed)
	out.DBDir = dbDir
	switch shape {
	case remainingShapeClickHouseTyped:
		out.StoredShape = fmt.Sprintf("original JSON object converted to %s with ClickHouse typed JSON paths removed", format)
	case remainingShapeConservative:
		out.StoredShape = fmt.Sprintf("original JSON object converted to %s with only top-level time_us removed", format)
	default:
		return out, fmt.Errorf("unknown remaining shape %q", shape)
	}
	if rows <= 0 {
		return out, errors.New("remaining TreeDB measurement requires positive row count")
	}
	if err := os.RemoveAll(dbDir); err != nil {
		return out, fmt.Errorf("reset remaining TreeDB dir: %w", err)
	}
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		return out, fmt.Errorf("create remaining TreeDB dir: %w", err)
	}

	opts := treedb.OptionsForBenchmark(treedb.ProfileBenchUnsafe, dbDir)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		return out, fmt.Errorf("open remaining TreeDB: %w", err)
	}

	loadStart := time.Now()
	loadErr := func() error {
		defer func() { _ = cleanup() }()
		mgr := collections.NewCollectionManager(backend)
		if _, err := mgr.CreateCollection(&collections.CollectionMeta{
			Name: "jsonbench_remaining_" + string(format),
			Options: collections.CollectionOptions{
				DocumentFormat:        format,
				DataRootStoragePolicy: collections.RootStorageCompressed,
			},
		}); err != nil {
			return fmt.Errorf("create remaining collection: %w", err)
		}
		col, err := mgr.OpenCollection("jsonbench_remaining_" + string(format))
		if err != nil {
			return fmt.Errorf("open remaining collection: %w", err)
		}
		if err := insertRemainingDocuments(files, rows, col, format, shape, &out); err != nil {
			return err
		}
		flushStart := time.Now()
		if err := col.Flush(); err != nil {
			return fmt.Errorf("flush remaining collection: %w", err)
		}
		if err := mgr.FlushAll(); err != nil {
			return fmt.Errorf("flush all remaining collection: %w", err)
		}
		out.FlushDuration = time.Since(flushStart).Seconds()
		checkpointStart := time.Now()
		if err := backend.Checkpoint(); err != nil {
			return fmt.Errorf("checkpoint remaining TreeDB: %w", err)
		}
		out.CheckpointDuration = time.Since(checkpointStart).Seconds()
		return nil
	}()
	out.LoadDuration = time.Since(loadStart).Seconds()
	if loadErr != nil {
		return out, loadErr
	}
	if out.Rows != rows {
		return out, fmt.Errorf("remaining TreeDB loaded %d rows, want %d", out.Rows, rows)
	}
	before, beforeFiles, err := directoryUsage(dbDir)
	if err != nil {
		return out, err
	}
	out.BeforeCompactBytes = before
	out.BeforeCompactFiles = beforeFiles

	compactStart := time.Now()
	maintenance, maintenanceCleanup, err := treedb.OpenBackend(opts)
	if err != nil {
		return out, fmt.Errorf("open remaining TreeDB for compaction: %w", err)
	}
	stats, compactErr := maintenance.CompactStorage(ctx, backenddb.CompactStorageOptions{
		SyncEachPhase:                   true,
		ValueLogRewriteBatchSize:        16_000,
		LeafPackMinExpectedReclaimBytes: 1,
		LeafPackMinReclaimPerCopyPPM:    1,
	})
	cleanupErr := maintenanceCleanup()
	if compactErr != nil {
		return out, fmt.Errorf("compact remaining TreeDB: %w", compactErr)
	}
	if cleanupErr != nil {
		return out, fmt.Errorf("close remaining TreeDB compaction handle: %w", cleanupErr)
	}
	out.CompactionDuration = time.Since(compactStart).Seconds()
	out.FullyCompacted = stats.FullyCompacted

	sourceFileIDs, err := valueLogFileIDs(dbDir)
	if err != nil {
		return out, err
	}
	if len(sourceFileIDs) > 0 {
		rewriteStart := time.Now()
		rewriteDB, err := treedb.Open(opts)
		if err != nil {
			return out, fmt.Errorf("open remaining TreeDB for value-log rewrite: %w", err)
		}
		rewriteStats, rewriteErr := rewriteDB.ValueLogRewriteOnline(ctx, treedb.ValueLogRewriteOnlineOptions{
			SourceFileIDs: sourceFileIDs,
			BatchSize:     16_000,
			SyncEachBatch: true,
		})
		rewriteCloseErr := rewriteDB.Close()
		if rewriteErr != nil {
			return out, fmt.Errorf("rewrite remaining TreeDB value log: %w", rewriteErr)
		}
		if rewriteCloseErr != nil {
			return out, fmt.Errorf("close remaining TreeDB rewrite handle: %w", rewriteCloseErr)
		}
		out.RewriteDuration = time.Since(rewriteStart).Seconds()
		out.RewriteRecordsCopied = rewriteStats.ValueRecordsCopied
		out.RewriteValueBytes = rewriteStats.ValueBytesCopied
		out.RewriteSourceBytes = rewriteStats.SourceBytesRequested
		out.RewriteReclaimFiles = rewriteStats.SourceSegmentsReclaimed
		out.RewriteReclaimBytes = rewriteStats.SourceBytesReclaimed
	}
	after, afterFiles, err := directoryUsage(dbDir)
	if err != nil {
		return out, err
	}
	out.AfterCompactBytes = after
	out.AfterCompactFiles = afterFiles
	out.CompactedBytesPerRow = float64(after) / float64(out.Rows)
	return out, nil
}

func measureRawJSONTreeDB(ctx context.Context, files []string, rows int, dbDir string) (remainingTreeDBResult, error) {
	var out remainingTreeDBResult
	out.Enabled = true
	out.DocumentFormat = "raw_json_value"
	out.StoragePolicy = "raw_key_value"
	out.DBDir = dbDir
	out.StoredShape = "TreeDB key/value rows with documentID(row) as key and original JSON line bytes as value"
	if rows <= 0 {
		return out, errors.New("raw TreeDB measurement requires positive row count")
	}
	if err := os.RemoveAll(dbDir); err != nil {
		return out, fmt.Errorf("reset raw TreeDB dir: %w", err)
	}
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		return out, fmt.Errorf("create raw TreeDB dir: %w", err)
	}

	opts := treedb.OptionsForBenchmark(treedb.ProfileBenchUnsafe, dbDir)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := treedb.Open(opts)
	if err != nil {
		return out, fmt.Errorf("open raw TreeDB: %w", err)
	}

	loadStart := time.Now()
	loadErr := func() error {
		defer func() { _ = db.Close() }()
		if err := insertRawJSONRows(files, rows, db, &out); err != nil {
			return err
		}
		checkpointStart := time.Now()
		if err := db.Checkpoint(); err != nil {
			return fmt.Errorf("checkpoint raw TreeDB: %w", err)
		}
		out.CheckpointDuration = time.Since(checkpointStart).Seconds()
		return nil
	}()
	out.LoadDuration = time.Since(loadStart).Seconds()
	if loadErr != nil {
		return out, loadErr
	}
	if out.Rows != rows {
		return out, fmt.Errorf("raw TreeDB loaded %d rows, want %d", out.Rows, rows)
	}
	before, beforeFiles, err := directoryUsage(dbDir)
	if err != nil {
		return out, err
	}
	out.BeforeCompactBytes = before
	out.BeforeCompactFiles = beforeFiles

	compactStart := time.Now()
	maintenance, err := treedb.Open(opts)
	if err != nil {
		return out, fmt.Errorf("open raw TreeDB for compaction: %w", err)
	}
	stats, compactErr := maintenance.CompactStorage(ctx, treedb.CompactStorageOptions{
		SyncEachPhase:                   true,
		ValueLogRewriteBatchSize:        16_000,
		LeafPackMinExpectedReclaimBytes: 1,
		LeafPackMinReclaimPerCopyPPM:    1,
	})
	closeErr := maintenance.Close()
	if compactErr != nil {
		return out, fmt.Errorf("compact raw TreeDB: %w", compactErr)
	}
	if closeErr != nil {
		return out, fmt.Errorf("close raw TreeDB compaction handle: %w", closeErr)
	}
	out.CompactionDuration = time.Since(compactStart).Seconds()
	out.FullyCompacted = stats.FullyCompacted

	sourceFileIDs, err := valueLogFileIDs(dbDir)
	if err != nil {
		return out, err
	}
	if len(sourceFileIDs) > 0 {
		rewriteStart := time.Now()
		rewriteDB, err := treedb.Open(opts)
		if err != nil {
			return out, fmt.Errorf("open raw TreeDB for value-log rewrite: %w", err)
		}
		rewriteStats, rewriteErr := rewriteDB.ValueLogRewriteOnline(ctx, treedb.ValueLogRewriteOnlineOptions{
			SourceFileIDs: sourceFileIDs,
			BatchSize:     16_000,
			SyncEachBatch: true,
		})
		rewriteCloseErr := rewriteDB.Close()
		if rewriteErr != nil {
			return out, fmt.Errorf("rewrite raw TreeDB value log: %w", rewriteErr)
		}
		if rewriteCloseErr != nil {
			return out, fmt.Errorf("close raw TreeDB rewrite handle: %w", rewriteCloseErr)
		}
		out.RewriteDuration = time.Since(rewriteStart).Seconds()
		out.RewriteRecordsCopied = rewriteStats.ValueRecordsCopied
		out.RewriteValueBytes = rewriteStats.ValueBytesCopied
		out.RewriteSourceBytes = rewriteStats.SourceBytesRequested
		out.RewriteReclaimFiles = rewriteStats.SourceSegmentsReclaimed
		out.RewriteReclaimBytes = rewriteStats.SourceBytesReclaimed
	}
	if err := validateRawJSONTreeDB(files, rows, dbDir); err != nil {
		return out, err
	}
	after, afterFiles, err := directoryUsage(dbDir)
	if err != nil {
		return out, err
	}
	out.AfterCompactBytes = after
	out.AfterCompactFiles = afterFiles
	out.CompactedBytesPerRow = float64(after) / float64(out.Rows)
	return out, nil
}

func insertRawJSONRows(files []string, rows int, db *treedb.DB, out *remainingTreeDBResult) error {
	const batchSize = 16_000
	batch := db.NewBatchWithSize(batchSize)
	defer func() { _ = batch.Close() }()
	flush := func() error {
		size, err := batch.GetByteSize()
		if err != nil {
			return fmt.Errorf("raw TreeDB batch size: %w", err)
		}
		if size == 0 {
			return nil
		}
		if err := batch.Write(); err != nil {
			return fmt.Errorf("write raw TreeDB batch ending row %d: %w", out.Rows, err)
		}
		batch.Close()
		batch = db.NewBatchWithSize(batchSize)
		return nil
	}
	for _, file := range files {
		if out.Rows >= rows {
			break
		}
		if err := scanJSONBenchFile(file, func(raw []byte) error {
			if out.Rows >= rows {
				return errStopScan
			}
			out.Rows++
			rawCopy := append([]byte(nil), raw...)
			out.RawDocumentBytes += int64(len(rawCopy))
			if err := batch.Set(documentID(uint64(out.Rows)), rawCopy); err != nil {
				return fmt.Errorf("set raw JSON row %d: %w", out.Rows, err)
			}
			if out.Rows%batchSize == 0 {
				return flush()
			}
			return nil
		}); err != nil {
			return fmt.Errorf("scan raw JSON %s: %w", file, err)
		}
	}
	return flush()
}

func validateRawJSONTreeDB(files []string, rows int, dbDir string) error {
	opts := treedb.OptionsForBenchmark(treedb.ProfileBenchUnsafe, dbDir)
	db, err := treedb.Open(opts)
	if err != nil {
		return fmt.Errorf("open raw TreeDB for validation: %w", err)
	}
	var checked int
	var scanErr error
	for _, file := range files {
		if checked >= rows {
			break
		}
		if err := scanJSONBenchFile(file, func(raw []byte) error {
			if checked >= rows {
				return errStopScan
			}
			checked++
			value, err := db.Get(documentID(uint64(checked)))
			if err != nil {
				return fmt.Errorf("get raw JSON row %d: %w", checked, err)
			}
			if !bytes.Equal(value, raw) {
				return fmt.Errorf("raw JSON row %d validation mismatch: source bytes=%d stored bytes=%d", checked, len(raw), len(value))
			}
			return nil
		}); err != nil {
			scanErr = fmt.Errorf("validate raw JSON %s: %w", file, err)
			break
		}
	}
	if scanErr == nil && checked != rows {
		scanErr = fmt.Errorf("validated %d raw JSON rows, want %d", checked, rows)
	}
	closeErr := db.Close()
	if scanErr != nil {
		if closeErr != nil {
			return errors.Join(scanErr, fmt.Errorf("close raw TreeDB validation DB: %w", closeErr))
		}
		return scanErr
	}
	if closeErr != nil {
		return fmt.Errorf("close raw TreeDB validation DB: %w", closeErr)
	}
	return nil
}

func valueLogFileIDs(dbDir string) ([]uint32, error) {
	valueDir := filepath.Join(dbDir, "maindb", "value_vlog")
	entries, err := os.ReadDir(valueDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var ids []uint32
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "value-l") || !strings.HasSuffix(name, ".log") {
			continue
		}
		parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(name, "value-l"), ".log"), "-")
		if len(parts) != 2 {
			continue
		}
		lane, err := strconv.ParseUint(parts[0], 10, 8)
		if err != nil {
			continue
		}
		seq, err := strconv.ParseUint(parts[1], 10, 23)
		if err != nil {
			continue
		}
		if lane == 255 {
			continue
		}
		segmentID := uint32(lane)<<23 | uint32(seq)
		ids = append(ids, segmentID|0x80000000)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func insertRemainingDocuments(files []string, rows int, col *collections.Collection, format collections.DocumentFormat, shape remainingShape, out *remainingTreeDBResult) error {
	const batchSize = 16_000
	batchLimit := batchSize
	var ids [][]byte
	var docs [][]byte
	var templateEncoder collections.TemplateV1Encoder
	flush := func() error {
		if len(ids) == 0 {
			return nil
		}
		var err error
		switch format {
		case collections.DocumentFormatBSON:
			_, err = col.InsertBatchValidatedBSON(ids, docs)
		case collections.DocumentFormatTemplateV1:
			_, err = col.InsertBatchWithTemplateV1Encoder(ids, docs, &templateEncoder)
		case collections.DocumentFormatJSON:
			_, err = col.InsertBatch(ids, docs)
		default:
			err = fmt.Errorf("unsupported remaining document format %q", format)
		}
		if err != nil {
			return fmt.Errorf("insert remaining %s batch ending row %d: %w", format, out.Rows, err)
		}
		ids = ids[:0]
		docs = docs[:0]
		return nil
	}
	for _, file := range files {
		if out.Rows >= rows {
			break
		}
		if err := scanJSONBenchFile(file, func(raw []byte) error {
			if out.Rows >= rows {
				return errStopScan
			}
			doc, err := remainingDocument(raw, format, shape)
			if err != nil {
				return err
			}
			out.RawDocumentBytes += int64(len(doc))
			out.Rows++
			ids = append(ids, documentID(uint64(out.Rows)))
			docs = append(docs, doc)
			if len(ids) >= batchLimit {
				return flush()
			}
			return nil
		}); err != nil {
			return fmt.Errorf("scan remaining JSON %s: %w", file, err)
		}
	}
	return flush()
}

func remainingDocument(raw []byte, format collections.DocumentFormat, shape remainingShape) ([]byte, error) {
	switch format {
	case collections.DocumentFormatBSON:
		return remainingBSONDocument(raw, shape)
	case collections.DocumentFormatJSON:
		return remainingJSONDocument(raw, shape)
	case collections.DocumentFormatTemplateV1:
		doc, err := remainingJSONDocument(raw, shape)
		if err != nil {
			return nil, err
		}
		return collections.EncodeTemplateV1DocumentJSON(doc)
	default:
		return nil, fmt.Errorf("unsupported remaining document format %q", format)
	}
}

func remainingBSONDocument(raw []byte, shape remainingShape) ([]byte, error) {
	var doc map[string]any
	if err := decodeJSONPreserveNumbers(raw, &doc); err != nil {
		return nil, fmt.Errorf("decode remaining JSON: %w", err)
	}
	removeRemainingPaths(doc, shape)
	encoded, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encode remaining BSON: %w", err)
	}
	if err := bson.Raw(encoded).Validate(); err != nil {
		return nil, fmt.Errorf("validate remaining BSON: %w", err)
	}
	return encoded, nil
}

func decodeJSONPreserveNumbers(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return normalizeJSONNumbers(v)
}

func normalizeJSONNumbers(v any) error {
	switch x := v.(type) {
	case map[string]any:
		for key, value := range x {
			normalized, err := normalizeJSONNumberValue(value)
			if err != nil {
				return err
			}
			x[key] = normalized
		}
	case []any:
		for i, value := range x {
			normalized, err := normalizeJSONNumberValue(value)
			if err != nil {
				return err
			}
			x[i] = normalized
		}
	}
	return nil
}

func normalizeJSONNumberValue(value any) (any, error) {
	switch x := value.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i, nil
		}
		f, err := x.Float64()
		if err != nil {
			return nil, err
		}
		return f, nil
	case map[string]any:
		if err := normalizeJSONNumbers(x); err != nil {
			return nil, err
		}
		return x, nil
	case []any:
		if err := normalizeJSONNumbers(x); err != nil {
			return nil, err
		}
		return x, nil
	default:
		return value, nil
	}
}

func remainingJSONDocument(raw []byte, shape remainingShape) ([]byte, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("decode remaining JSON: %w", err)
	}
	if err := removeRemainingRawPaths(doc, shape); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encode remaining JSON: %w", err)
	}
	return encoded, nil
}

func removeRemainingPaths(doc map[string]any, shape remainingShape) {
	delete(doc, "time_us")
	if shape == remainingShapeConservative {
		return
	}
	delete(doc, "kind")
	delete(doc, "did")
	commit, ok := doc["commit"].(map[string]any)
	if !ok {
		return
	}
	delete(commit, "operation")
	delete(commit, "collection")
}

func removeRemainingRawPaths(doc map[string]json.RawMessage, shape remainingShape) error {
	delete(doc, "time_us")
	if shape == remainingShapeConservative {
		return nil
	}
	delete(doc, "kind")
	delete(doc, "did")
	rawCommit, ok := doc["commit"]
	if !ok {
		return nil
	}
	var commit map[string]json.RawMessage
	if err := json.Unmarshal(rawCommit, &commit); err != nil {
		return fmt.Errorf("decode remaining commit JSON: %w", err)
	}
	delete(commit, "operation")
	delete(commit, "collection")
	encoded, err := json.Marshal(commit)
	if err != nil {
		return fmt.Errorf("encode remaining commit JSON: %w", err)
	}
	doc["commit"] = encoded
	return nil
}

var errStopScan = errors.New("stop scan")

func scanJSONBenchFile(path string, fn func(raw []byte) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	var reader io.Reader = file
	var gz *gzip.Reader
	if strings.HasSuffix(path, ".gz") {
		gz, err = gzip.NewReader(file)
		if err != nil {
			return err
		}
		defer func() { _ = gz.Close() }()
		reader = gz
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	for scanner.Scan() {
		raw := bytes.TrimSpace(scanner.Bytes())
		if len(raw) == 0 {
			continue
		}
		if err := fn(raw); err != nil {
			if errors.Is(err, errStopScan) {
				return nil
			}
			return err
		}
	}
	return scanner.Err()
}

func documentID(row uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, row)
	return out
}

func directoryUsage(dir string) (int64, int, error) {
	var total int64
	var count int
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		count++
		return nil
	})
	return total, count, err
}

func writeJSON(path string, raw comparisonRaw) {
	data, err := json.MarshalIndent(raw, "", "  ")
	must(err)
	must(os.MkdirAll(filepath.Dir(path), 0o755))
	must(os.WriteFile(path, append(data, '\n'), 0o644))
}

func writeMarkdown(path string, raw comparisonRaw) {
	var b strings.Builder
	fmt.Fprintf(&b, "# JSONBench ClickHouse Comparison\n\n")
	fmt.Fprintf(&b, "Generated from `%s` with row limit `%d`; this local run read `%d` file(s) and `%d` rows. The comparison is a smoke-level column-kernel comparison, not a full database benchmark: TreeDB roots, collection WAL, query planning, persistence, and SQL execution are intentionally out of scope.\n\n", raw.DataPath, raw.Limit, len(raw.Files), raw.Rows)
	fmt.Fprintf(&b, "The ClickHouse numbers below use the local JSONBench result `%s` `%s` on `%s`, so query timings and disk bytes are local-machine comparisons.\n\n", raw.ClickHouseLocal.System, raw.ClickHouseLocal.Version, raw.ClickHouseLocal.OS)
	fmt.Fprintf(&b, "## Query Timing\n\n")
	fmt.Fprintf(&b, "| Query | Column-kernel best | ClickHouse local | Kernel / ClickHouse | Notes |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---|\n")
	for i, timing := range raw.QueryTimings {
		local := clickHouseBest(raw.ClickHouseLocal, i)
		fmt.Fprintf(&b, "| %s | %.6fs | %.6fs | %.2fx | %s |\n", timing.Query, timing.Best.Seconds(), local, ratio(timing.Best.Seconds(), local), timing.Description)
	}
	if len(raw.EncodedPartQueryTimings) > 0 {
		fmt.Fprintf(&b, "\n## Encoded Part Query Timing\n\n")
		fmt.Fprintf(&b, "| Query | Best | Best cache | ClickHouse local | Part / ClickHouse | Kernel | Diagnostics |\n")
		fmt.Fprintf(&b, "|---|---:|---|---:|---:|---|---|\n")
		for i, timing := range raw.EncodedPartQueryTimings {
			local := clickHouseBest(raw.ClickHouseLocal, i)
			d := timing.Diagnostics
			fmt.Fprintf(&b, "| %s | %.6fs | %s | %.6fs | %.2fx | `%s` | rows=%d granules=%d decoded=%d blocks=%d bytes=%d columns=`%s` |\n",
				timing.Query,
				timing.Best.Seconds(),
				timing.BestCache,
				local,
				ratio(timing.Best.Seconds(), local),
				d.AggregateKernel,
				d.RowsScanned,
				d.GranulesConsidered,
				d.GranulesDecoded,
				d.BlocksDecoded,
				d.BytesDecoded,
				strings.Join(d.ColumnsProjected, ","))
		}
	}
	if len(raw.EncodedPartQ4Fairness) > 0 {
		fmt.Fprintf(&b, "\n## Q4 Sort-Order Fairness\n\n")
		fmt.Fprintf(&b, "Q4a compares time-ordered TreeDB against a time-ordered ClickHouse table when the local ClickHouse result includes `q4_fairness`. Q4b compares a TreeDB part ordered by `kind`, `operation`, `collection`, `did`, and `time_us` against the standard ClickHouse table order, without TreeDB's global `time_us` early-stop shortcut.\n\n")
		fmt.Fprintf(&b, "| Query | TreeDB best | Best cache | ClickHouse local | TreeDB / ClickHouse | Speedup | TreeDB sort key | Early stop | Kernel | Diagnostics | ClickHouse shape |\n")
		fmt.Fprintf(&b, "|---|---:|---|---:|---:|---:|---|---|---|---|---|\n")
		for _, timing := range raw.EncodedPartQ4Fairness {
			d := timing.Diagnostics
			local := clickHouseQ4FairnessBest(raw.ClickHouseLocal, timing.Query)
			localSeconds := local.Best
			localShape := local.QueryShape
			if localShape == "" {
				localShape = "not provided"
			}
			fmt.Fprintf(&b, "| %s | %.6fs | %s | %s | %s | %s | `%s` | %t | `%s` | rows=%d granules=%d skipped=%d decoded=%d blocks=%d bytes=%d | `%s` |\n",
				timing.Query,
				timing.Best.Seconds(),
				timing.BestCache,
				formatSeconds(localSeconds),
				formatRatio(timing.Best.Seconds(), localSeconds),
				formatSpeedup(timing.Best.Seconds(), localSeconds),
				strings.Join(d.SortKey, ","),
				d.EarlyStopAvailable,
				d.AggregateKernel,
				d.RowsScanned,
				d.GranulesConsidered,
				d.GranulesSkipped,
				d.GranulesDecoded,
				d.BlocksDecoded,
				d.BytesDecoded,
				localShape)
		}
	}
	if len(raw.AggregateMetadataTimings) > 0 {
		fmt.Fprintf(&b, "\n## Aggregate Metadata Prototype\n\n")
		fmt.Fprintf(&b, "These M1B timings use exact per-granule `did_code -> min(time_us), max(time_us), count` metadata for declared post/create rows. The prototype stores metadata uncompressed in memory and reports build cost plus estimated byte accounting so later file-backed work can decide admission and compression policies.\n\n")
		fmt.Fprintf(&b, "| Query | Metadata best | Best cache | Baseline | Speedup | Break-even | Kernel | Metadata rows | Entries | Entries/matched row | Est. bytes | Est. B/part row | Est. B/matched row | Build | Compression |\n")
		fmt.Fprintf(&b, "|---|---:|---|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---|\n")
		for _, timing := range raw.AggregateMetadataTimings {
			d := timing.Diagnostics
			baseline := aggregateMetadataBaselineSeconds(raw, timing.Query)
			fmt.Fprintf(&b, "| %s | %.6fs | %s | %s | %s | %s | `%s` | %d | %d | %.3f | %d | %.3f | %.3f | %.6fs | `%s` |\n",
				timing.Query,
				timing.Best.Seconds(),
				timing.BestCache,
				formatSeconds(baseline),
				formatSpeedup(timing.Best.Seconds(), baseline),
				formatBreakEvenQueries(d.AggregateMetadataBuildDuration.Seconds(), baseline, timing.Best.Seconds()),
				d.AggregateKernel,
				d.AggregateMetadataRows,
				d.AggregateMetadataEntries,
				ratio(float64(d.AggregateMetadataEntries), float64(d.AggregateMetadataRows)),
				d.AggregateMetadataBytes,
				d.AggregateMetadataBytesPerRow,
				ratio(float64(d.AggregateMetadataBytes), float64(d.AggregateMetadataRows)),
				d.AggregateMetadataBuildDuration.Seconds(),
				d.AggregateMetadataCompression)
		}
	}
	if len(raw.EncodedPartBuildReports) > 0 {
		fmt.Fprintf(&b, "\n## M1D Serialized Image Build and Size Accounting\n\n")
		fmt.Fprintf(&b, "These reports build the actual encoded JSONBench column part layouts with aggregate metadata enabled, then serialize the part into an exact in-memory image. The image contains a manifest, descriptors, declared column payload sections, sort-key metadata, marks, row locators, aggregate metadata, and part-local dictionaries when available. Retained/original JSON payload is labeled separately and is absent from the encoded-part total. Physical file count is `0` because the image is still in memory; file-backed `TCS1` work should replace this with real file/container counts.\n\n")
		fmt.Fprintf(&b, "| Layout | Files | Granules | Codec blocks | Best build | ns/row | Rows/s | Encoded MiB/s | Stored MiB/s | Alloc B/op | Allocs/op | Temp B/op | Output B/row |\n")
		fmt.Fprintf(&b, "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
		for _, report := range raw.EncodedPartBuildReports {
			fmt.Fprintf(&b, "| `%s` | %d | %d | %d | %.6fs | %.1f | %.0f | %.2f | %.2f | %d | %d | %d | %.3f |\n",
				report.Layout,
				report.Accounting.PhysicalFiles,
				report.Accounting.Granules,
				report.Accounting.CodecBlocks,
				report.Best.Duration.Seconds(),
				report.NanosPerRow,
				report.RowsPerSecond,
				report.EncodedMiBPerSecond,
				report.StoredMiBPerSecond,
				report.AllocatedBytesPerOp,
				report.AllocsPerOp,
				report.TemporaryBytes,
				report.Accounting.BytesPerRow)
		}
		fmt.Fprintf(&b, "\n| Layout | Total bytes | Serialized image | Manifest | vs raw JSON | vs source gzip | vs ClickHouse total | Declared columns | Dictionaries | Marks | Sort key | Aggregate metadata | Descriptors | Locators | Retained JSON |\n")
		fmt.Fprintf(&b, "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n")
		for _, report := range raw.EncodedPartBuildReports {
			accounting := report.Accounting
			fmt.Fprintf(&b, "| `%s` | %d | %d | %d | %s | %s | %s | %d | %d | %d | %d | %d | %d | %d | `%s` |\n",
				report.Layout,
				accounting.TotalStoredBytes,
				accounting.SerializedImageBytes,
				accounting.SerializedManifestBytes,
				formatPercent(float64(accounting.TotalStoredBytes), float64(report.RawJSONBytes)),
				formatSourceGzipPercent(raw, int64(accounting.TotalStoredBytes)),
				formatPercent(float64(accounting.TotalStoredBytes), float64(raw.ClickHouseLocal.TotalSize)),
				accounting.DeclaredColumnStoredBytes,
				accounting.DictionaryBytes,
				accounting.MarkBytes,
				accounting.SortKeyMetadataBytes,
				accounting.AggregateMetadataBytes,
				accounting.DescriptorBytes,
				accounting.LocatorBytes,
				accounting.RetainedJSONPayload)
		}
		retainedPayloads := retainedPayloadOptions(raw)
		if len(retainedPayloads) > 0 {
			fmt.Fprintf(&b, "\nFull-dataset retained-payload estimates add the current serialized column-part image total to a measured TreeDB payload collection containing the original JSON row with ClickHouse typed paths removed. This is the closest pre-M2 comparison to ClickHouse `total_size`: the column part is serialized but still in memory, while the retained payload is an actual compacted TreeDB directory measurement.\n\n")
			fmt.Fprintf(&b, "| Layout | Retained payload | Part bytes | Payload bytes | Total bytes | MiB | B/row | TreeDB / ClickHouse | Delta vs ClickHouse | Granules | Codec blocks | Part files | Payload files |\n")
			fmt.Fprintf(&b, "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
			for _, report := range raw.EncodedPartBuildReports {
				for _, payload := range retainedPayloads {
					total := int64(report.Accounting.TotalStoredBytes) + payload.Result.AfterCompactBytes
					fmt.Fprintf(&b, "| `%s` | `%s` | %d | %d | %d | %.2f | %.3f | %s | %s | %d | %d | %d | %d |\n",
						report.Layout,
						payload.Label,
						report.Accounting.TotalStoredBytes,
						payload.Result.AfterCompactBytes,
						total,
						mib(total),
						ratio(float64(total), float64(report.Accounting.Rows)),
						formatMultiplier(float64(total), float64(raw.ClickHouseLocal.TotalSize)),
						formatSignedMiB(total-raw.ClickHouseLocal.TotalSize),
						report.Accounting.Granules,
						report.Accounting.CodecBlocks,
						report.Accounting.PhysicalFiles,
						payload.Result.AfterCompactFiles)
				}
			}
		} else {
			fmt.Fprintf(&b, "\nFull-dataset retained-payload estimate: not measured in this run. Re-run with `-measure-remaining-treedb=true`, or reuse a prior retained-payload run with `-measure-remaining-treedb=false -retained-payload-from-json <raw-json>`.\n")
		}
		fmt.Fprintf(&b, "\nCompression/admission by column/substream:\n\n")
		fmt.Fprintf(&b, "| Layout | Column | Substream | Encoding | Requested | Actual | Blocks | Raw bytes | Stored bytes | Stored/raw | Attempts | Kept | Rejected | Fallback |\n")
		fmt.Fprintf(&b, "|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|\n")
		for _, report := range raw.EncodedPartBuildReports {
			for _, detail := range report.Accounting.CompressionDetail {
				fallback := detail.FallbackReason
				if fallback == "" {
					fallback = "kept"
				}
				fmt.Fprintf(&b, "| `%s` | `%s` | `%s` | `%s` | `%s` | `%s` | %d | %d | %d | %.3f | %d | %d | %d | `%s` |\n",
					report.Layout,
					detail.Column,
					detail.Substream,
					detail.Encoding,
					detail.RequestedCompression,
					detail.ActualCompression,
					detail.Blocks,
					detail.EncodedRawBytes,
					detail.StoredBytes,
					detail.StoredToEncodedRawRate,
					detail.CompressionAttempted,
					detail.CompressionKept,
					detail.CompressionRejected,
					fallback)
			}
		}
	}
	fmt.Fprintf(&b, "\n## Storage Footprint\n\n")
	allBest := bestTotal(raw.BestColumnStorage, nil)
	queryBest := bestTotal(raw.BestColumnStorage, queryPathColumns())
	combinedAllBSON := int64(allBest) + raw.RemainingTreeDB.AfterCompactBytes
	combinedQueryBSON := int64(queryBest) + raw.RemainingTreeDB.AfterCompactBytes
	combinedAllJSON := int64(allBest) + raw.RemainingTreeDBJSON.AfterCompactBytes
	combinedQueryJSON := int64(queryBest) + raw.RemainingTreeDBJSON.AfterCompactBytes
	combinedAllTpl := int64(allBest) + raw.RemainingTreeDBTpl.AfterCompactBytes
	combinedQueryTpl := int64(queryBest) + raw.RemainingTreeDBTpl.AfterCompactBytes
	conservativeAllBSON := int64(allBest) + raw.ConservativeBSON.AfterCompactBytes
	conservativeQueryBSON := int64(queryBest) + raw.ConservativeBSON.AfterCompactBytes
	conservativeAllJSON := int64(allBest) + raw.ConservativeJSON.AfterCompactBytes
	conservativeQueryJSON := int64(queryBest) + raw.ConservativeJSON.AfterCompactBytes
	conservativeAllTpl := int64(allBest) + raw.ConservativeTpl.AfterCompactBytes
	conservativeQueryTpl := int64(queryBest) + raw.ConservativeTpl.AfterCompactBytes
	rawKVQueryJSON := int64(queryBest) + raw.RawTreeDBJSON.AfterCompactBytes
	fmt.Fprintf(&b, "| Item | Bytes | MiB | Notes |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---|\n")
	fmt.Fprintf(&b, "| JSONBench compressed input files | %d | %.2f | Local `.json.gz` source bytes read by this run. |\n", raw.InputBytes, mib(raw.InputBytes))
	fmt.Fprintf(&b, "| ClickHouse local total | %d | %.2f | `total_size` from local ClickHouse JSONBench result. |\n", raw.ClickHouseLocal.TotalSize, mib(raw.ClickHouseLocal.TotalSize))
	fmt.Fprintf(&b, "| ClickHouse local data | %d | %.2f | `data_size` from local ClickHouse JSONBench result. |\n", raw.ClickHouseLocal.DataSize, mib(raw.ClickHouseLocal.DataSize))
	fmt.Fprintf(&b, "| ClickHouse local index | %d | %.2f | `index_size` from local ClickHouse JSONBench result. |\n", raw.ClickHouseLocal.IndexSize, mib(raw.ClickHouseLocal.IndexSize))
	for _, report := range raw.EncodedPartBuildReports {
		fmt.Fprintf(&b, "| Serialized column part `%s` total | %d | %.2f | Exact serialized in-memory image including declared columns, dictionaries, marks, descriptors, locators, and admitted aggregate metadata; retained JSON is `%s`. |\n", report.Layout, report.Accounting.TotalStoredBytes, mib(int64(report.Accounting.TotalStoredBytes)), report.Accounting.RetainedJSONPayload)
	}
	for _, report := range raw.EncodedPartBuildReports {
		for _, payload := range retainedPayloadOptions(raw) {
			total := int64(report.Accounting.TotalStoredBytes) + payload.Result.AfterCompactBytes
			fmt.Fprintf(&b, "| M1D serialized image `%s` + `%s` retained payload | %d | %.2f | Full-dataset estimate: %.2f%% of ClickHouse local total, delta %s; part files `%d`, payload files `%d`, granules `%d`. |\n", report.Layout, payload.Label, total, mib(total), ratio(float64(total)*100, float64(raw.ClickHouseLocal.TotalSize)), formatSignedMiB(total-raw.ClickHouseLocal.TotalSize), report.Accounting.PhysicalFiles, payload.Result.AfterCompactFiles, report.Accounting.Granules)
		}
	}
	fmt.Fprintf(&b, "| Legacy one-column best-codec lower bound, all derived columns | %d | %.2f | %.2f%% of ClickHouse local total; excludes manifest, locators, dictionaries, marks, aggregate metadata, and retained JSON. |\n", allBest, mib(int64(allBest)), ratio(float64(allBest)*100, float64(raw.ClickHouseLocal.TotalSize)))
	fmt.Fprintf(&b, "| Legacy one-column best-codec lower bound, query/index paths | %d | %.2f | %.2f%% of ClickHouse local total; excludes manifest, locators, dictionaries, marks, aggregate metadata, and retained JSON. |\n", queryBest, mib(int64(queryBest)), ratio(float64(queryBest)*100, float64(raw.ClickHouseLocal.TotalSize)))
	if raw.RemainingTreeDB.Enabled {
		fmt.Fprintf(&b, "| TreeDB BSON remaining fields after compaction + value-log rewrite | %d | %.2f | Stores original JSON minus ClickHouse typed paths as BSON in a compressed no-index collection. |\n", raw.RemainingTreeDB.AfterCompactBytes, mib(raw.RemainingTreeDB.AfterCompactBytes))
		fmt.Fprintf(&b, "| Granules all derived columns + TreeDB BSON remaining fields | %d | %.2f | %.2f%% of ClickHouse local total. |\n", combinedAllBSON, mib(combinedAllBSON), ratio(float64(combinedAllBSON)*100, float64(raw.ClickHouseLocal.TotalSize)))
		fmt.Fprintf(&b, "| Granules query/index paths + TreeDB BSON remaining fields | %d | %.2f | %.2f%% of ClickHouse local total. |\n", combinedQueryBSON, mib(combinedQueryBSON), ratio(float64(combinedQueryBSON)*100, float64(raw.ClickHouseLocal.TotalSize)))
	}
	if raw.RemainingTreeDBJSON.Enabled {
		fmt.Fprintf(&b, "| TreeDB JSON remaining fields after compaction + value-log rewrite | %d | %.2f | Stores original JSON minus ClickHouse typed paths as JSON in a compressed no-index collection. |\n", raw.RemainingTreeDBJSON.AfterCompactBytes, mib(raw.RemainingTreeDBJSON.AfterCompactBytes))
		fmt.Fprintf(&b, "| Granules all derived columns + TreeDB JSON remaining fields | %d | %.2f | %.2f%% of ClickHouse local total. |\n", combinedAllJSON, mib(combinedAllJSON), ratio(float64(combinedAllJSON)*100, float64(raw.ClickHouseLocal.TotalSize)))
		fmt.Fprintf(&b, "| Granules query/index paths + TreeDB JSON remaining fields | %d | %.2f | %.2f%% of ClickHouse local total. |\n", combinedQueryJSON, mib(combinedQueryJSON), ratio(float64(combinedQueryJSON)*100, float64(raw.ClickHouseLocal.TotalSize)))
	}
	if raw.RemainingTreeDBTpl.Enabled {
		fmt.Fprintf(&b, "| TreeDB Template-v1 remaining fields after compaction + value-log rewrite | %d | %.2f | Stores original JSON minus ClickHouse typed paths as Template-v1 in a compressed no-index collection. |\n", raw.RemainingTreeDBTpl.AfterCompactBytes, mib(raw.RemainingTreeDBTpl.AfterCompactBytes))
		fmt.Fprintf(&b, "| Granules all derived columns + TreeDB Template-v1 remaining fields | %d | %.2f | %.2f%% of ClickHouse local total. |\n", combinedAllTpl, mib(combinedAllTpl), ratio(float64(combinedAllTpl)*100, float64(raw.ClickHouseLocal.TotalSize)))
		fmt.Fprintf(&b, "| Granules query/index paths + TreeDB Template-v1 remaining fields | %d | %.2f | %.2f%% of ClickHouse local total. |\n", combinedQueryTpl, mib(combinedQueryTpl), ratio(float64(combinedQueryTpl)*100, float64(raw.ClickHouseLocal.TotalSize)))
	}
	if raw.ConservativeBSON.Enabled {
		fmt.Fprintf(&b, "| Conservative TreeDB BSON remaining fields after compaction + value-log rewrite | %d | %.2f | Stores original JSON minus only `time_us` as BSON in a compressed no-index collection. |\n", raw.ConservativeBSON.AfterCompactBytes, mib(raw.ConservativeBSON.AfterCompactBytes))
		fmt.Fprintf(&b, "| Granules all derived columns + conservative TreeDB BSON remaining fields | %d | %.2f | %.2f%% of ClickHouse local total. |\n", conservativeAllBSON, mib(conservativeAllBSON), ratio(float64(conservativeAllBSON)*100, float64(raw.ClickHouseLocal.TotalSize)))
		fmt.Fprintf(&b, "| Granules query/index paths + conservative TreeDB BSON remaining fields | %d | %.2f | %.2f%% of ClickHouse local total. |\n", conservativeQueryBSON, mib(conservativeQueryBSON), ratio(float64(conservativeQueryBSON)*100, float64(raw.ClickHouseLocal.TotalSize)))
	}
	if raw.ConservativeJSON.Enabled {
		fmt.Fprintf(&b, "| Conservative TreeDB JSON remaining fields after compaction + value-log rewrite | %d | %.2f | Stores original JSON minus only `time_us` as JSON in a compressed no-index collection. |\n", raw.ConservativeJSON.AfterCompactBytes, mib(raw.ConservativeJSON.AfterCompactBytes))
		fmt.Fprintf(&b, "| Granules all derived columns + conservative TreeDB JSON remaining fields | %d | %.2f | %.2f%% of ClickHouse local total. |\n", conservativeAllJSON, mib(conservativeAllJSON), ratio(float64(conservativeAllJSON)*100, float64(raw.ClickHouseLocal.TotalSize)))
		fmt.Fprintf(&b, "| Granules query/index paths + conservative TreeDB JSON remaining fields | %d | %.2f | %.2f%% of ClickHouse local total. |\n", conservativeQueryJSON, mib(conservativeQueryJSON), ratio(float64(conservativeQueryJSON)*100, float64(raw.ClickHouseLocal.TotalSize)))
	}
	if raw.ConservativeTpl.Enabled {
		fmt.Fprintf(&b, "| Conservative TreeDB Template-v1 remaining fields after compaction + value-log rewrite | %d | %.2f | Stores original JSON minus only `time_us` as Template-v1 in a compressed no-index collection. |\n", raw.ConservativeTpl.AfterCompactBytes, mib(raw.ConservativeTpl.AfterCompactBytes))
		fmt.Fprintf(&b, "| Granules all derived columns + conservative TreeDB Template-v1 remaining fields | %d | %.2f | %.2f%% of ClickHouse local total. |\n", conservativeAllTpl, mib(conservativeAllTpl), ratio(float64(conservativeAllTpl)*100, float64(raw.ClickHouseLocal.TotalSize)))
		fmt.Fprintf(&b, "| Granules query/index paths + conservative TreeDB Template-v1 remaining fields | %d | %.2f | %.2f%% of ClickHouse local total. |\n", conservativeQueryTpl, mib(conservativeQueryTpl), ratio(float64(conservativeQueryTpl)*100, float64(raw.ClickHouseLocal.TotalSize)))
	}
	if raw.RawTreeDBJSON.Enabled {
		fmt.Fprintf(&b, "| Raw TreeDB key/value JSON after compaction + value-log rewrite | %d | %.2f | Stores `documentID(row) -> original JSON line bytes` with no collection document encoding. |\n", raw.RawTreeDBJSON.AfterCompactBytes, mib(raw.RawTreeDBJSON.AfterCompactBytes))
		fmt.Fprintf(&b, "| Granules query/index paths + raw TreeDB key/value JSON | %d | %.2f | %.2f%% of ClickHouse local total. |\n", rawKVQueryJSON, mib(rawKVQueryJSON), ratio(float64(rawKVQueryJSON)*100, float64(raw.ClickHouseLocal.TotalSize)))
	}
	fmt.Fprintf(&b, "\n")
	if raw.RemainingTreeDB.Enabled {
		fmt.Fprintf(&b, "The ClickHouse-aligned remaining-fields TreeDB collections store each original JSON row after deleting the same explicitly typed JSON paths used by the local ClickHouse JSONBench schema: `time_us`, `kind`, `did`, `commit.operation`, and `commit.collection`. The removed paths are represented by granule columns in this experiment. Nested values such as `commit.rev`, `commit.rkey`, `commit.cid`, `commit.record.text`, `langs`, `reply`, and `subject` remain in the TreeDB payload. The conservative rows keep those string paths in the TreeDB payload and remove only `time_us`.\n\n")
		fmt.Fprintf(&b, "BSON remaining-fields compaction detail: before compact `%d` bytes across `%d` files; after compact plus value-log rewrite `%d` bytes across `%d` files; compaction wall time `%.3fs`; rewrite wall time `%.3fs`; rewritten records `%d`; rewritten value bytes `%d`; rewritten source bytes `%d`; reclaimed rewrite source files `%d`; reclaimed rewrite source bytes `%d`; BSON payload bytes before TreeDB storage `%d`.\n\n", raw.RemainingTreeDB.BeforeCompactBytes, raw.RemainingTreeDB.BeforeCompactFiles, raw.RemainingTreeDB.AfterCompactBytes, raw.RemainingTreeDB.AfterCompactFiles, raw.RemainingTreeDB.CompactionDuration, raw.RemainingTreeDB.RewriteDuration, raw.RemainingTreeDB.RewriteRecordsCopied, raw.RemainingTreeDB.RewriteValueBytes, raw.RemainingTreeDB.RewriteSourceBytes, raw.RemainingTreeDB.RewriteReclaimFiles, raw.RemainingTreeDB.RewriteReclaimBytes, raw.RemainingTreeDB.RawDocumentBytes)
	}
	if raw.RemainingTreeDBJSON.Enabled {
		fmt.Fprintf(&b, "JSON remaining-fields compaction detail: before compact `%d` bytes across `%d` files; after compact plus value-log rewrite `%d` bytes across `%d` files; compaction wall time `%.3fs`; rewrite wall time `%.3fs`; rewritten records `%d`; rewritten value bytes `%d`; rewritten source bytes `%d`; reclaimed rewrite source files `%d`; reclaimed rewrite source bytes `%d`; JSON payload bytes before TreeDB storage `%d`.\n\n", raw.RemainingTreeDBJSON.BeforeCompactBytes, raw.RemainingTreeDBJSON.BeforeCompactFiles, raw.RemainingTreeDBJSON.AfterCompactBytes, raw.RemainingTreeDBJSON.AfterCompactFiles, raw.RemainingTreeDBJSON.CompactionDuration, raw.RemainingTreeDBJSON.RewriteDuration, raw.RemainingTreeDBJSON.RewriteRecordsCopied, raw.RemainingTreeDBJSON.RewriteValueBytes, raw.RemainingTreeDBJSON.RewriteSourceBytes, raw.RemainingTreeDBJSON.RewriteReclaimFiles, raw.RemainingTreeDBJSON.RewriteReclaimBytes, raw.RemainingTreeDBJSON.RawDocumentBytes)
	}
	if raw.RemainingTreeDBTpl.Enabled {
		fmt.Fprintf(&b, "Template-v1 remaining-fields compaction detail: before compact `%d` bytes across `%d` files; after compact plus value-log rewrite `%d` bytes across `%d` files; compaction wall time `%.3fs`; rewrite wall time `%.3fs`; rewritten records `%d`; rewritten value bytes `%d`; rewritten source bytes `%d`; reclaimed rewrite source files `%d`; reclaimed rewrite source bytes `%d`; Template-v1 payload bytes before TreeDB storage `%d`.\n\n", raw.RemainingTreeDBTpl.BeforeCompactBytes, raw.RemainingTreeDBTpl.BeforeCompactFiles, raw.RemainingTreeDBTpl.AfterCompactBytes, raw.RemainingTreeDBTpl.AfterCompactFiles, raw.RemainingTreeDBTpl.CompactionDuration, raw.RemainingTreeDBTpl.RewriteDuration, raw.RemainingTreeDBTpl.RewriteRecordsCopied, raw.RemainingTreeDBTpl.RewriteValueBytes, raw.RemainingTreeDBTpl.RewriteSourceBytes, raw.RemainingTreeDBTpl.RewriteReclaimFiles, raw.RemainingTreeDBTpl.RewriteReclaimBytes, raw.RemainingTreeDBTpl.RawDocumentBytes)
		fmt.Fprintf(&b, "Template-v1 reuses one encoder across bounded insert batches, so template records and compact stored documents are learned across the whole measurement without retaining every row in memory. The rewritten record count includes template-root records as well as primary documents.\n\n")
	}
	if raw.ConservativeBSON.Enabled {
		fmt.Fprintf(&b, "Conservative BSON remaining-fields compaction detail: before compact `%d` bytes across `%d` files; after compact plus value-log rewrite `%d` bytes across `%d` files; compaction wall time `%.3fs`; rewrite wall time `%.3fs`; rewritten records `%d`; rewritten value bytes `%d`; rewritten source bytes `%d`; reclaimed rewrite source files `%d`; reclaimed rewrite source bytes `%d`; BSON payload bytes before TreeDB storage `%d`.\n\n", raw.ConservativeBSON.BeforeCompactBytes, raw.ConservativeBSON.BeforeCompactFiles, raw.ConservativeBSON.AfterCompactBytes, raw.ConservativeBSON.AfterCompactFiles, raw.ConservativeBSON.CompactionDuration, raw.ConservativeBSON.RewriteDuration, raw.ConservativeBSON.RewriteRecordsCopied, raw.ConservativeBSON.RewriteValueBytes, raw.ConservativeBSON.RewriteSourceBytes, raw.ConservativeBSON.RewriteReclaimFiles, raw.ConservativeBSON.RewriteReclaimBytes, raw.ConservativeBSON.RawDocumentBytes)
	}
	if raw.ConservativeJSON.Enabled {
		fmt.Fprintf(&b, "Conservative JSON remaining-fields compaction detail: before compact `%d` bytes across `%d` files; after compact plus value-log rewrite `%d` bytes across `%d` files; compaction wall time `%.3fs`; rewrite wall time `%.3fs`; rewritten records `%d`; rewritten value bytes `%d`; rewritten source bytes `%d`; reclaimed rewrite source files `%d`; reclaimed rewrite source bytes `%d`; JSON payload bytes before TreeDB storage `%d`.\n\n", raw.ConservativeJSON.BeforeCompactBytes, raw.ConservativeJSON.BeforeCompactFiles, raw.ConservativeJSON.AfterCompactBytes, raw.ConservativeJSON.AfterCompactFiles, raw.ConservativeJSON.CompactionDuration, raw.ConservativeJSON.RewriteDuration, raw.ConservativeJSON.RewriteRecordsCopied, raw.ConservativeJSON.RewriteValueBytes, raw.ConservativeJSON.RewriteSourceBytes, raw.ConservativeJSON.RewriteReclaimFiles, raw.ConservativeJSON.RewriteReclaimBytes, raw.ConservativeJSON.RawDocumentBytes)
	}
	if raw.ConservativeTpl.Enabled {
		fmt.Fprintf(&b, "Conservative Template-v1 remaining-fields compaction detail: before compact `%d` bytes across `%d` files; after compact plus value-log rewrite `%d` bytes across `%d` files; compaction wall time `%.3fs`; rewrite wall time `%.3fs`; rewritten records `%d`; rewritten value bytes `%d`; rewritten source bytes `%d`; reclaimed rewrite source files `%d`; reclaimed rewrite source bytes `%d`; Template-v1 payload bytes before TreeDB storage `%d`.\n\n", raw.ConservativeTpl.BeforeCompactBytes, raw.ConservativeTpl.BeforeCompactFiles, raw.ConservativeTpl.AfterCompactBytes, raw.ConservativeTpl.AfterCompactFiles, raw.ConservativeTpl.CompactionDuration, raw.ConservativeTpl.RewriteDuration, raw.ConservativeTpl.RewriteRecordsCopied, raw.ConservativeTpl.RewriteValueBytes, raw.ConservativeTpl.RewriteSourceBytes, raw.ConservativeTpl.RewriteReclaimFiles, raw.ConservativeTpl.RewriteReclaimBytes, raw.ConservativeTpl.RawDocumentBytes)
	}
	if raw.RawTreeDBJSON.Enabled {
		fmt.Fprintf(&b, "Raw TreeDB key/value JSON detail: before compact `%d` bytes across `%d` files; after compact plus value-log rewrite `%d` bytes across `%d` files; compaction wall time `%.3fs`; rewrite wall time `%.3fs`; rewritten records `%d`; rewritten value bytes `%d`; rewritten source bytes `%d`; reclaimed rewrite source files `%d`; reclaimed rewrite source bytes `%d`; raw JSON payload bytes before TreeDB storage `%d`.\n\n", raw.RawTreeDBJSON.BeforeCompactBytes, raw.RawTreeDBJSON.BeforeCompactFiles, raw.RawTreeDBJSON.AfterCompactBytes, raw.RawTreeDBJSON.AfterCompactFiles, raw.RawTreeDBJSON.CompactionDuration, raw.RawTreeDBJSON.RewriteDuration, raw.RawTreeDBJSON.RewriteRecordsCopied, raw.RawTreeDBJSON.RewriteValueBytes, raw.RawTreeDBJSON.RewriteSourceBytes, raw.RawTreeDBJSON.RewriteReclaimFiles, raw.RawTreeDBJSON.RewriteReclaimBytes, raw.RawTreeDBJSON.RawDocumentBytes)
		if raw.RawTreeDBJSON.RewriteReclaimBytes > 0 {
			fmt.Fprintf(&b, "Raw TreeDB key/value JSON uses the public cached key/value write path. Value-log rewrite reclaimed the observed source segments reported above, so this row's post-rewrite footprint excludes those old ingest source bytes.\n\n")
		} else if raw.RawTreeDBJSON.RewriteSourceBytes > 0 {
			fmt.Fprintf(&b, "Raw TreeDB key/value JSON uses the public cached key/value write path. In the inspected run, value-log rewrite produced a dictionary-compressed rewrite segment, but the original ingest value-log segments remained classified as active and therefore stayed in the measured directory footprint. Treat this row as a cached raw-key/value retention fixture, not as the lower bound for compressed raw JSON bytes.\n\n")
		}
	}
	fmt.Fprintf(&b, "The table below is one-column-at-a-time storage for the experimental granule codecs. It picks the smallest stored byte count observed for each derived `int64` column across raw, delta-varint, snappy, and lz4 combinations.\n\n")
	fmt.Fprintf(&b, "| Column | Best codec | Stored bytes | Ratio vs int64 values | Ratio vs ClickHouse total |\n")
	fmt.Fprintf(&b, "|---|---|---:|---:|---:|\n")
	for _, col := range raw.BestColumnStorage {
		fmt.Fprintf(&b, "| `%s` | `%s` + `%s` | %d | %.6f | %.4f%% |\n", col.Column, col.Encoding, col.RequestedCompression, col.StoredBytes, col.RatioVsValues, ratio(float64(col.StoredBytes)*100, float64(raw.ClickHouseLocal.TotalSize)))
	}
	fmt.Fprintf(&b, "\n## Raw Data\n\n")
	fmt.Fprintf(&b, "Machine-readable raw data is written beside this report when using the default output directory.\n")
	must(os.MkdirAll(filepath.Dir(path), 0o755))
	must(os.WriteFile(path, []byte(b.String()), 0o644))
}

func inputBytes(files []string) int64 {
	var total int64
	for _, file := range files {
		info, err := os.Stat(file)
		must(err)
		total += info.Size()
	}
	return total
}

func bestColumns(summaries []colgranule.ColumnCodecSummary) []bestColumnStorage {
	byColumn := make(map[string]bestColumnStorage)
	for _, s := range summaries {
		ratioVsValues := 0.0
		if s.ValueBytes > 0 {
			ratioVsValues = float64(s.StoredBytes) / float64(s.ValueBytes)
		}
		cur, ok := byColumn[s.Column]
		if !ok || s.StoredBytes < cur.StoredBytes {
			byColumn[s.Column] = bestColumnStorage{
				Column:               s.Column,
				Encoding:             s.Encoding,
				RequestedCompression: s.RequestedCompression,
				StoredBytes:          s.StoredBytes,
				ValueBytes:           s.ValueBytes,
				RatioVsValues:        ratioVsValues,
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

func clickHouseQ4FairnessBest(result clickHouseResult, query string) clickHouseQ4FairnessResult {
	prefix := strings.ToLower(query)
	var out clickHouseQ4FairnessResult
	for _, candidate := range result.Q4Fairness {
		if !strings.HasPrefix(strings.ToLower(candidate.Query), prefix) {
			continue
		}
		if candidate.Best == 0 {
			for i, attempt := range candidate.Attempts {
				if i == 0 || attempt < candidate.Best {
					candidate.Best = attempt
				}
			}
		}
		if candidate.Best == 0 {
			continue
		}
		if out.Best == 0 || candidate.Best < out.Best {
			out = candidate
		}
	}
	return out
}

func aggregateMetadataBaselineSeconds(raw comparisonRaw, query string) float64 {
	switch query {
	case "Q4b-meta":
		for _, timing := range raw.EncodedPartQ4Fairness {
			if timing.Query == "Q4b" {
				return timing.Best.Seconds()
			}
		}
	case "Q5-meta":
		for _, timing := range raw.EncodedPartQueryTimings {
			if timing.Query == "Q5" {
				return timing.Best.Seconds()
			}
		}
	}
	return 0
}

func queryPathColumns() map[string]bool {
	return map[string]bool{
		"kind_code": true, "commit_operation_code": true, "commit_collection_code": true, "did_code": true, "time_us": true,
	}
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

func retainedPayloadOptions(raw comparisonRaw) []retainedPayloadOption {
	options := make([]retainedPayloadOption, 0, 3)
	if raw.RemainingTreeDB.Enabled {
		options = append(options, retainedPayloadOption{Label: "BSON remaining", Result: raw.RemainingTreeDB})
	}
	if raw.RemainingTreeDBJSON.Enabled {
		options = append(options, retainedPayloadOption{Label: "JSON remaining", Result: raw.RemainingTreeDBJSON})
	}
	if raw.RemainingTreeDBTpl.Enabled {
		options = append(options, retainedPayloadOption{Label: "Template-v1 remaining", Result: raw.RemainingTreeDBTpl})
	}
	return options
}

func ratio(numerator, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	return numerator / denominator
}

func formatSeconds(seconds float64) string {
	if seconds == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.6fs", seconds)
}

func formatRatio(numerator, denominator float64) string {
	if denominator == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2fx", ratio(numerator, denominator))
}

func formatMultiplier(numerator, denominator float64) string {
	if denominator == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.3fx", ratio(numerator, denominator))
}

func formatSpeedup(numerator, denominator float64) string {
	if numerator == 0 || denominator == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2fx", ratio(denominator, numerator))
}

func formatPercent(numerator, denominator float64) string {
	if denominator == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2f%%", ratio(numerator*100, denominator))
}

func formatSourceGzipPercent(raw comparisonRaw, bytes int64) string {
	if raw.Limit > 0 && raw.Rows >= raw.Limit {
		return "n/a (row-limited)"
	}
	return formatPercent(float64(bytes), float64(raw.InputBytes))
}

func formatSignedMiB(bytes int64) string {
	sign := "+"
	if bytes < 0 {
		sign = "-"
		bytes = -bytes
	}
	return fmt.Sprintf("%s%.2f MiB", sign, mib(bytes))
}

func formatBreakEvenQueries(buildSeconds, baselineSeconds, metadataSeconds float64) string {
	saved := baselineSeconds - metadataSeconds
	if buildSeconds <= 0 || saved <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.1f", buildSeconds/saved)
}

func mib(bytes int64) float64 {
	return float64(bytes) / 1024 / 1024
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
