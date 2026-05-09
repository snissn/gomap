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
	GeneratedAt         string                            `json:"generated_at"`
	DataPath            string                            `json:"data_path"`
	Limit               int                               `json:"limit"`
	Rows                int                               `json:"rows"`
	Files               []string                          `json:"files"`
	InputBytes          int64                             `json:"input_bytes"`
	RowsPerGranule      int                               `json:"rows_per_granule"`
	LoadDuration        time.Duration                     `json:"load_duration"`
	ClickHouseLocal     clickHouseResult                  `json:"clickhouse_local"`
	RemainingTreeDB     remainingTreeDBResult             `json:"remaining_treedb"`
	RemainingTreeDBJSON remainingTreeDBResult             `json:"remaining_treedb_json"`
	RemainingTreeDBTpl  remainingTreeDBResult             `json:"remaining_treedb_template_v1"`
	ConservativeBSON    remainingTreeDBResult             `json:"conservative_remaining_treedb_bson"`
	ConservativeJSON    remainingTreeDBResult             `json:"conservative_remaining_treedb_json"`
	ConservativeTpl     remainingTreeDBResult             `json:"conservative_remaining_treedb_template_v1"`
	RawTreeDBJSON       remainingTreeDBResult             `json:"raw_treedb_json"`
	QueryTimings        []colgranule.JSONBenchQueryTiming `json:"query_timings"`
	ColumnSummaries     []colgranule.ColumnCodecSummary   `json:"column_summaries"`
	BestColumnStorage   []bestColumnStorage               `json:"best_column_storage"`
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
	}

	raw := comparisonRaw{
		GeneratedAt:         time.Now().UTC().Format(time.RFC3339),
		DataPath:            *data,
		Limit:               *limit,
		Rows:                ds.Rows,
		Files:               ds.Files,
		InputBytes:          inputBytes(ds.Files),
		RowsPerGranule:      *rowsPerGranule,
		LoadDuration:        loadDuration,
		ClickHouseLocal:     readClickHouseResult(*clickHouseLocalPath),
		RemainingTreeDB:     remaining,
		RemainingTreeDBJSON: remainingJSON,
		RemainingTreeDBTpl:  remainingTpl,
		ConservativeBSON:    conservativeBSON,
		ConservativeJSON:    conservativeJSON,
		ConservativeTpl:     conservativeTpl,
		RawTreeDBJSON:       rawTreeDBJSON,
		QueryTimings:        timings,
		ColumnSummaries:     summaries,
		BestColumnStorage:   bestColumns(summaries),
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

	opts := treedb.OptionsFor(treedb.ProfileBench, dbDir)
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
		rewriteDB, rewriteCleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
		if err != nil {
			return out, fmt.Errorf("open remaining TreeDB for value-log rewrite: %w", err)
		}
		rewriteStats, rewriteErr := rewriteDB.ValueLogRewriteOnline(ctx, backenddb.ValueLogRewriteOnlineOptions{
			SourceFileIDs: sourceFileIDs,
			BatchSize:     16_000,
			SyncEachBatch: true,
		})
		rewriteCleanupErr := rewriteCleanup()
		if rewriteErr != nil {
			return out, fmt.Errorf("rewrite remaining TreeDB value log: %w", rewriteErr)
		}
		if rewriteCleanupErr != nil {
			return out, fmt.Errorf("close remaining TreeDB rewrite handle: %w", rewriteCleanupErr)
		}
		out.RewriteDuration = time.Since(rewriteStart).Seconds()
		out.RewriteRecordsCopied = rewriteStats.ValueRecordsCopied
		out.RewriteValueBytes = rewriteStats.ValueBytesCopied
		out.RewriteSourceBytes = rewriteStats.SourceBytesRequested
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

	opts := treedb.OptionsFor(treedb.ProfileBench, dbDir)
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
			out.RawDocumentBytes += int64(len(raw))
			if err := batch.Set(documentID(uint64(out.Rows)), raw); err != nil {
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
	if err := json.Unmarshal(raw, &doc); err != nil {
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
	fmt.Fprintf(&b, "| Granule best-codec all derived columns | %d | %.2f | %.2f%% of ClickHouse local total. |\n", allBest, mib(int64(allBest)), ratio(float64(allBest)*100, float64(raw.ClickHouseLocal.TotalSize)))
	fmt.Fprintf(&b, "| Granule best-codec query/index paths | %d | %.2f | %.2f%% of ClickHouse local total. |\n", queryBest, mib(int64(queryBest)), ratio(float64(queryBest)*100, float64(raw.ClickHouseLocal.TotalSize)))
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
		fmt.Fprintf(&b, "BSON remaining-fields compaction detail: before compact `%d` bytes across `%d` files; after compact plus value-log rewrite `%d` bytes across `%d` files; compaction wall time `%.3fs`; rewrite wall time `%.3fs`; rewritten records `%d`; rewritten value bytes `%d`; rewritten source bytes `%d`; BSON payload bytes before TreeDB storage `%d`.\n\n", raw.RemainingTreeDB.BeforeCompactBytes, raw.RemainingTreeDB.BeforeCompactFiles, raw.RemainingTreeDB.AfterCompactBytes, raw.RemainingTreeDB.AfterCompactFiles, raw.RemainingTreeDB.CompactionDuration, raw.RemainingTreeDB.RewriteDuration, raw.RemainingTreeDB.RewriteRecordsCopied, raw.RemainingTreeDB.RewriteValueBytes, raw.RemainingTreeDB.RewriteSourceBytes, raw.RemainingTreeDB.RawDocumentBytes)
	}
	if raw.RemainingTreeDBJSON.Enabled {
		fmt.Fprintf(&b, "JSON remaining-fields compaction detail: before compact `%d` bytes across `%d` files; after compact plus value-log rewrite `%d` bytes across `%d` files; compaction wall time `%.3fs`; rewrite wall time `%.3fs`; rewritten records `%d`; rewritten value bytes `%d`; rewritten source bytes `%d`; JSON payload bytes before TreeDB storage `%d`.\n\n", raw.RemainingTreeDBJSON.BeforeCompactBytes, raw.RemainingTreeDBJSON.BeforeCompactFiles, raw.RemainingTreeDBJSON.AfterCompactBytes, raw.RemainingTreeDBJSON.AfterCompactFiles, raw.RemainingTreeDBJSON.CompactionDuration, raw.RemainingTreeDBJSON.RewriteDuration, raw.RemainingTreeDBJSON.RewriteRecordsCopied, raw.RemainingTreeDBJSON.RewriteValueBytes, raw.RemainingTreeDBJSON.RewriteSourceBytes, raw.RemainingTreeDBJSON.RawDocumentBytes)
	}
	if raw.RemainingTreeDBTpl.Enabled {
		fmt.Fprintf(&b, "Template-v1 remaining-fields compaction detail: before compact `%d` bytes across `%d` files; after compact plus value-log rewrite `%d` bytes across `%d` files; compaction wall time `%.3fs`; rewrite wall time `%.3fs`; rewritten records `%d`; rewritten value bytes `%d`; rewritten source bytes `%d`; Template-v1 payload bytes before TreeDB storage `%d`.\n\n", raw.RemainingTreeDBTpl.BeforeCompactBytes, raw.RemainingTreeDBTpl.BeforeCompactFiles, raw.RemainingTreeDBTpl.AfterCompactBytes, raw.RemainingTreeDBTpl.AfterCompactFiles, raw.RemainingTreeDBTpl.CompactionDuration, raw.RemainingTreeDBTpl.RewriteDuration, raw.RemainingTreeDBTpl.RewriteRecordsCopied, raw.RemainingTreeDBTpl.RewriteValueBytes, raw.RemainingTreeDBTpl.RewriteSourceBytes, raw.RemainingTreeDBTpl.RawDocumentBytes)
		fmt.Fprintf(&b, "Template-v1 reuses one encoder across bounded insert batches, so template records and compact stored documents are learned across the whole measurement without retaining every row in memory. The rewritten record count includes template-root records as well as primary documents.\n\n")
	}
	if raw.ConservativeBSON.Enabled {
		fmt.Fprintf(&b, "Conservative BSON remaining-fields compaction detail: before compact `%d` bytes across `%d` files; after compact plus value-log rewrite `%d` bytes across `%d` files; compaction wall time `%.3fs`; rewrite wall time `%.3fs`; rewritten records `%d`; rewritten value bytes `%d`; rewritten source bytes `%d`; BSON payload bytes before TreeDB storage `%d`.\n\n", raw.ConservativeBSON.BeforeCompactBytes, raw.ConservativeBSON.BeforeCompactFiles, raw.ConservativeBSON.AfterCompactBytes, raw.ConservativeBSON.AfterCompactFiles, raw.ConservativeBSON.CompactionDuration, raw.ConservativeBSON.RewriteDuration, raw.ConservativeBSON.RewriteRecordsCopied, raw.ConservativeBSON.RewriteValueBytes, raw.ConservativeBSON.RewriteSourceBytes, raw.ConservativeBSON.RawDocumentBytes)
	}
	if raw.ConservativeJSON.Enabled {
		fmt.Fprintf(&b, "Conservative JSON remaining-fields compaction detail: before compact `%d` bytes across `%d` files; after compact plus value-log rewrite `%d` bytes across `%d` files; compaction wall time `%.3fs`; rewrite wall time `%.3fs`; rewritten records `%d`; rewritten value bytes `%d`; rewritten source bytes `%d`; JSON payload bytes before TreeDB storage `%d`.\n\n", raw.ConservativeJSON.BeforeCompactBytes, raw.ConservativeJSON.BeforeCompactFiles, raw.ConservativeJSON.AfterCompactBytes, raw.ConservativeJSON.AfterCompactFiles, raw.ConservativeJSON.CompactionDuration, raw.ConservativeJSON.RewriteDuration, raw.ConservativeJSON.RewriteRecordsCopied, raw.ConservativeJSON.RewriteValueBytes, raw.ConservativeJSON.RewriteSourceBytes, raw.ConservativeJSON.RawDocumentBytes)
	}
	if raw.ConservativeTpl.Enabled {
		fmt.Fprintf(&b, "Conservative Template-v1 remaining-fields compaction detail: before compact `%d` bytes across `%d` files; after compact plus value-log rewrite `%d` bytes across `%d` files; compaction wall time `%.3fs`; rewrite wall time `%.3fs`; rewritten records `%d`; rewritten value bytes `%d`; rewritten source bytes `%d`; Template-v1 payload bytes before TreeDB storage `%d`.\n\n", raw.ConservativeTpl.BeforeCompactBytes, raw.ConservativeTpl.BeforeCompactFiles, raw.ConservativeTpl.AfterCompactBytes, raw.ConservativeTpl.AfterCompactFiles, raw.ConservativeTpl.CompactionDuration, raw.ConservativeTpl.RewriteDuration, raw.ConservativeTpl.RewriteRecordsCopied, raw.ConservativeTpl.RewriteValueBytes, raw.ConservativeTpl.RewriteSourceBytes, raw.ConservativeTpl.RawDocumentBytes)
	}
	if raw.RawTreeDBJSON.Enabled {
		fmt.Fprintf(&b, "Raw TreeDB key/value JSON detail: before compact `%d` bytes across `%d` files; after compact plus value-log rewrite `%d` bytes across `%d` files; compaction wall time `%.3fs`; rewrite wall time `%.3fs`; rewritten records `%d`; rewritten value bytes `%d`; rewritten source bytes `%d`; raw JSON payload bytes before TreeDB storage `%d`.\n\n", raw.RawTreeDBJSON.BeforeCompactBytes, raw.RawTreeDBJSON.BeforeCompactFiles, raw.RawTreeDBJSON.AfterCompactBytes, raw.RawTreeDBJSON.AfterCompactFiles, raw.RawTreeDBJSON.CompactionDuration, raw.RawTreeDBJSON.RewriteDuration, raw.RawTreeDBJSON.RewriteRecordsCopied, raw.RawTreeDBJSON.RewriteValueBytes, raw.RawTreeDBJSON.RewriteSourceBytes, raw.RawTreeDBJSON.RawDocumentBytes)
		fmt.Fprintf(&b, "Raw TreeDB key/value JSON uses the public cached key/value write path. In the inspected run, value-log rewrite produced a dictionary-compressed rewrite segment, but the original ingest value-log segments remained classified as active and therefore stayed in the measured directory footprint. Treat this row as a cached raw-key/value retention fixture, not as the lower bound for compressed raw JSON bytes.\n\n")
	}
	fmt.Fprintf(&b, "The table below is one-column-at-a-time storage for the experimental granule codecs. It picks the smallest stored byte count observed for each derived `int64` column across raw, delta-varint, snappy, and lz4 combinations.\n\n")
	fmt.Fprintf(&b, "| Column | Best codec | Stored bytes | Ratio vs int64 values | Ratio vs ClickHouse total |\n")
	fmt.Fprintf(&b, "|---|---|---:|---:|---:|\n")
	for _, col := range raw.BestColumnStorage {
		fmt.Fprintf(&b, "| `%s` | `%s` + `%s` | %d | %.6f | %.4f%% |\n", col.Column, col.Encoding, col.RequestedCompression, col.StoredBytes, col.RatioVsValues, ratio(float64(col.StoredBytes)*100, float64(raw.ClickHouseLocal.TotalSize)))
	}
	fmt.Fprintf(&b, "\n## Raw Data\n\n")
	fmt.Fprintf(&b, "Machine-readable raw data is in `experiments/colgranule/JSONBENCH_COMPARISON_RAW.json`.\n")
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

func ratio(numerator, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	return numerator / denominator
}

func mib(bytes int64) float64 {
	return float64(bytes) / 1024 / 1024
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
