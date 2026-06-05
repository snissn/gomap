package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/trace"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestColumnStoreSuiteRetainedPayloadPreservesJSONNumbersM13C(t *testing.T) {
	cfg := columnStoreSuiteConfig()
	retained, err := columnStoreSuiteRetainedPayloadFromDocument(
		[]byte(`{"time_us":9223372036854775807,"kind":"like","did":"d1","payload_id":9223372036854775806}`),
		cfg,
	)
	if err != nil {
		t.Fatalf("columnStoreSuiteRetainedPayloadFromDocument: %v", err)
	}
	if bytes.Contains(retained, []byte("9.223")) {
		t.Fatalf("retained payload used floating/scientific notation: %s", retained)
	}
	if !bytes.Contains(retained, []byte(`9223372036854775806`)) {
		t.Fatalf("retained payload lost integer fidelity: %s", retained)
	}
	audit, err := collections.AuditColumnRetainedPayloadPathsAbsent(*cfg, retained, []string{"time_us", "kind", "did"})
	if err != nil {
		t.Fatalf("retained payload path audit: %v audit=%+v retained=%s", err, audit, retained)
	}
	if bytes.Contains(retained, []byte("time_us")) || bytes.Contains(retained, []byte("kind")) || bytes.Contains(retained, []byte("did")) {
		t.Fatalf("retained payload still contains declared columns: %s", retained)
	}
}

func TestColumnStoreSuiteRetainedPayloadRejectsTrailingJSONM13C(t *testing.T) {
	cfg := columnStoreSuiteConfig()
	if _, err := columnStoreSuiteRetainedPayloadFromDocument([]byte(`{"payload":1} {"payload":2}`), cfg); err == nil {
		t.Fatal("columnStoreSuiteRetainedPayloadFromDocument accepted trailing JSON value")
	} else if !strings.Contains(err.Error(), "trailing JSON value") {
		t.Fatalf("columnStoreSuiteRetainedPayloadFromDocument err=%v want trailing JSON value", err)
	}
}

func TestRunColumnStoreSuiteRejectsRelaxedAssetReadIntegrityWithoutUnsafeM1634(t *testing.T) {
	for _, mode := range []string{
		string(collections.ColumnAssetReadIntegrityCachedVerify),
		string(collections.ColumnAssetReadIntegritySkipChecksums),
	} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			cfg := BenchConfig{
				Keys:      16,
				BatchSize: 8,
				DBsArg:    "treedb",
				Profile:   "durable",
				Progress:  false,
				SeedUsed:  1,
			}
			prevAllowUnsafe := *treedbAllowUnsafe
			prevIntegrity := *columnStoreSuiteAssetReadIntegrityArg
			*treedbAllowUnsafe = false
			*columnStoreSuiteAssetReadIntegrityArg = mode
			t.Cleanup(func() {
				*treedbAllowUnsafe = prevAllowUnsafe
				*columnStoreSuiteAssetReadIntegrityArg = prevIntegrity
			})
			if _, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
				ProfileDir:    dir,
				ExecutionPath: "native-fastpath",
				ForcedPath:    columnStorePathSerialColumnScan,
			}); err == nil || !strings.Contains(err.Error(), "requires -treedb-allow-unsafe") {
				t.Fatalf("runColumnStoreSuite relaxed read integrity err=%v want unsafe rejection", err)
			}
		})
	}
}

func TestRunColumnStoreSuiteReportsRelaxedAssetReadIntegrityM1634(t *testing.T) {
	for _, mode := range []collections.ColumnAssetReadIntegrity{
		collections.ColumnAssetReadIntegrityCachedVerify,
		collections.ColumnAssetReadIntegritySkipChecksums,
	} {
		t.Run(string(mode), func(t *testing.T) {
			dir := t.TempDir()
			cfg := BenchConfig{
				Keys:      16,
				BatchSize: 8,
				DBsArg:    "treedb",
				Profile:   "durable",
				Progress:  false,
				SeedUsed:  1,
			}
			prevAllowUnsafe := *treedbAllowUnsafe
			*treedbAllowUnsafe = true
			t.Cleanup(func() { *treedbAllowUnsafe = prevAllowUnsafe })
			if _, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
				ProfileDir:               dir,
				ExecutionPath:            "native-fastpath",
				ForcedPath:               columnStorePathSerialColumnScan,
				ColumnAssetReadIntegrity: mode,
			}); err != nil {
				t.Fatalf("runColumnStoreSuite relaxed read integrity: %v", err)
			}
			data, err := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
			if err != nil {
				t.Fatalf("read column_store_results.json: %v", err)
			}
			var report columnStoreSuiteReport
			if err := json.Unmarshal(data, &report); err != nil {
				t.Fatalf("unmarshal column_store_results.json: %v", err)
			}
			if got, want := report.ColumnAssetReadIntegrity, string(mode); got != want {
				t.Fatalf("column_asset_read_integrity=%q want %q", got, want)
			}
			if !report.BenchmarkOnlyRelaxed {
				t.Fatalf("BenchmarkOnlyRelaxed=false want true for %s", mode)
			}
		})
	}
}

func TestColumnStoreSuiteWALExcludedDurableStorageAccounting1954(t *testing.T) {
	dir := t.TempDir()
	writeSizedFile := func(rel string, size int) {
		path := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, bytes.Repeat([]byte("x"), size), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", rel, err)
		}
	}

	writeSizedFile("index.db", 100)
	writeSizedFile("vlog_ref_counts.meta", 5)
	writeSizedFile(filepath.Join("value_vlog", "value-l0-000001.log"), 11)
	writeSizedFile(filepath.Join("leaf_vlog", "value-l255-000001.log"), 12)
	writeSizedFile(filepath.Join("column_assets", "events", "segment-000001.bin"), 13)
	writeSizedFile(filepath.Join("wal", "commit-l0-000001.log"), 40)
	writeSizedFile(filepath.Join("wal", "commit-lane-readme.log"), 17)
	writeSizedFile(filepath.Join("wal", "commit-l+1-000001.log"), 23)
	writeSizedFile(filepath.Join("wal", "commit-l0-000000.log"), 19)
	writeSizedFile(filepath.Join("wal", "operator-note.txt"), 9)
	writeSizedFile(filepath.Join("wal", "recovery-artifacts", "artifact.bin"), 7)

	totalBytes, _, err := columnStoreSuiteDirUsage(dir)
	if err != nil {
		t.Fatalf("columnStoreSuiteDirUsage: %v", err)
	}
	walBytes, err := columnStoreSuiteCommandWALLogBytes(dir)
	if err != nil {
		t.Fatalf("columnStoreSuiteCommandWALLogBytes: %v", err)
	}
	primaryIndexBytes, err := columnStoreSuiteOptionalFileBytes(filepath.Join(dir, "index.db"))
	if err != nil {
		t.Fatalf("columnStoreSuiteOptionalFileBytes(index.db): %v", err)
	}
	manifestControlBytes, missing, err := columnStoreSuiteManifestControlUsage(dir)
	if err != nil {
		t.Fatalf("columnStoreSuiteManifestControlUsage: %v", err)
	}
	ordinaryValueLogBytes, err := columnStoreSuiteOptionalDirBytes(backenddb.ValueLogDirPath(dir))
	if err != nil {
		t.Fatalf("ordinary value-log bytes: %v", err)
	}
	leafLogBytes, err := columnStoreSuiteOptionalDirBytes(backenddb.LeafLogDirPath(dir))
	if err != nil {
		t.Fatalf("leaf-log bytes: %v", err)
	}
	columnAssetBytes, err := columnStoreSuiteColumnAssetUsage(dir)
	if err != nil {
		t.Fatalf("column asset bytes: %v", err)
	}

	if walBytes != 40 {
		t.Fatalf("wal bytes excluded=%d want only valid wal/commit-l<lane>-<seq>.log command WAL segment bytes", walBytes)
	}
	if got, want := primaryIndexBytes, int64(100); got != want {
		t.Fatalf("primary index bytes=%d want %d", got, want)
	}
	if got, want := manifestControlBytes, int64(5); got != want || len(missing) != 0 {
		t.Fatalf("manifest control bytes=%d missing=%v want %d and no missing", got, missing, want)
	}
	if ordinaryValueLogBytes != 11 || leafLogBytes != 12 || columnAssetBytes != 13 {
		t.Fatalf("durable split bytes ordinary=%d leaf=%d column=%d want 11/12/13", ordinaryValueLogBytes, leafLogBytes, columnAssetBytes)
	}
	wantDurable := totalBytes - walBytes
	if got := columnStoreSuiteDurableStorageBytesWALExcluded(totalBytes, walBytes); got != wantDurable {
		t.Fatalf("durable storage bytes WAL excluded=%d want %d", got, wantDurable)
	}
	if got := columnStoreSuiteDurableStorageBytesWALExcluded(10, 20); got != 0 {
		t.Fatalf("durable storage bytes should floor at zero when WAL exceeds total, got %d", got)
	}

	report := columnStoreSuiteReport{ByteAccounting: columnStoreByteAccounting{
		PrimaryIndexBytes:                  primaryIndexBytes,
		OrdinaryValueLogBytes:              ordinaryValueLogBytes,
		LeafLogBytes:                       leafLogBytes,
		ColumnAssetBytes:                   columnAssetBytes,
		ManifestControlBytes:               manifestControlBytes,
		WALBytesExcludedFromDurable:        walBytes,
		DurableStorageBytesWALExcluded:     wantDurable,
		DurableStorageBytesWALExcludedNote: columnStoreDurableStorageWALExcludedNote,
		DBTotalBytes:                       totalBytes,
	}}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("json.Marshal(report): %v", err)
	}
	for _, want := range []string{`"primary_index_bytes"`, `"wal_bytes_excluded_from_durable_storage"`, `"durable_storage_bytes_wal_excluded"`, `"durable_storage_bytes_wal_excluded_note"`, `"ordinary_value_vlog_bytes"`, `"leaf_vlog_bytes"`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("JSON missing %s:\n%s", want, data)
		}
	}
	if strings.Contains(string(data), `"command_wal_bytes":`) {
		t.Fatalf("JSON contains ambiguous command WAL label:\n%s", data)
	}
	md := renderColumnStoreSuiteMarkdown(report)
	for _, want := range []string{"primary_index_bytes", "wal_bytes_excluded_from_durable_storage", "durable_storage_bytes_wal_excluded", "value_vlog, leaf_vlog, index.db"} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q:\n%s", want, md)
		}
	}
}

func TestColumnStoreSuiteFormatPhysicalQueryLineM1634(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		key    string
		value  int64
		want   string
	}{
		{name: "count", prefix: "q1", key: "share", value: 12, want: "q1:share=12"},
		{name: "negative", prefix: "q4a", key: "d000001", value: -42, want: "q4a:d000001=-42"},
		{name: "min_int64", prefix: "q4a", key: "d000002", value: -1 << 63, want: "q4a:d000002=-9223372036854775808"},
		{name: "max_int64", prefix: "q4b", key: "d000003", value: 1<<63 - 1, want: "q4b:d000003=9223372036854775807"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := columnStoreSuiteFormatPhysicalQueryLine(tt.prefix, tt.key, tt.value); got != tt.want {
				t.Fatalf("columnStoreSuiteFormatPhysicalQueryLine=%q want %q", got, tt.want)
			}
		})
	}
}

func TestColumnStoreSuiteHashPhysicalQueryGroupsMatchesLineHashM1634(t *testing.T) {
	tests := []struct {
		name      string
		prefix    string
		queryName string
		groups    []collections.ColumnPhysicalQueryGroup
	}{
		{
			name:      "q1_unsorted",
			prefix:    "q1",
			queryName: columnStoreQueryQ1,
			groups: []collections.ColumnPhysicalQueryGroup{
				{Key: "app.bsky.feed.post", Count: 34},
				{Key: "app.bsky.feed.like", Count: 12},
				{Key: "app.bsky.graph.follow", Count: 56},
			},
		},
		{
			name:      "q1_prefix_key_and_decimal_lexical_order",
			prefix:    "q1",
			queryName: columnStoreQueryQ1,
			groups: []collections.ColumnPhysicalQueryGroup{
				{Key: "a", Count: 2},
				{Key: "a.b", Count: 1},
				{Key: "a", Count: 10},
			},
		},
		{
			name:      "q5_unsorted_boundaries",
			prefix:    "q5",
			queryName: columnStoreQueryQ5,
			groups: []collections.ColumnPhysicalQueryGroup{
				{Key: "d000002", Int64: 1<<63 - 1},
				{Key: "d000001", Int64: -1 << 63},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			groupsForLines := append([]collections.ColumnPhysicalQueryGroup(nil), tt.groups...)
			lines, err := columnStoreSuitePhysicalQueryLines(tt.prefix, tt.queryName, groupsForLines)
			if err != nil {
				t.Fatalf("columnStoreSuitePhysicalQueryLines: %v", err)
			}
			want := columnStoreHashLines(lines)
			groupsForHash := append([]collections.ColumnPhysicalQueryGroup(nil), tt.groups...)
			got, count, err := columnStoreSuiteHashPhysicalQueryGroups(tt.prefix, tt.queryName, groupsForHash)
			if err != nil {
				t.Fatalf("columnStoreSuiteHashPhysicalQueryGroups: %v", err)
			}
			if count != len(tt.groups) {
				t.Fatalf("result count=%d want %d", count, len(tt.groups))
			}
			if got != want {
				t.Fatalf("direct hash=%016x want line hash=%016x", got, want)
			}
		})
	}
}

func TestColumnStoreSuiteInheritsTreeDBDisableReadChecksumM1634(t *testing.T) {
	prevAllowUnsafe := *treedbAllowUnsafe
	prevDisableReadChecksum := *treedbDisableReadChecksum
	prevIntegrity := *columnStoreSuiteAssetReadIntegrityArg
	*treedbAllowUnsafe = true
	*treedbDisableReadChecksum = true
	*columnStoreSuiteAssetReadIntegrityArg = string(collections.ColumnAssetReadIntegrityVerify)
	t.Cleanup(func() {
		*treedbAllowUnsafe = prevAllowUnsafe
		*treedbDisableReadChecksum = prevDisableReadChecksum
		*columnStoreSuiteAssetReadIntegrityArg = prevIntegrity
	})

	got, err := columnStoreSuiteEffectiveAssetReadIntegrity("")
	if err != nil {
		t.Fatalf("columnStoreSuiteEffectiveAssetReadIntegrity: %v", err)
	}
	if got != collections.ColumnAssetReadIntegritySkipChecksums {
		t.Fatalf("column asset read integrity=%q want inherited %q", got, collections.ColumnAssetReadIntegritySkipChecksums)
	}
	got, err = columnStoreSuiteEffectiveAssetReadIntegrity(collections.ColumnAssetReadIntegrityVerify)
	if err != nil {
		t.Fatalf("explicit verify integrity: %v", err)
	}
	if got != collections.ColumnAssetReadIntegrityVerify {
		t.Fatalf("explicit column asset read integrity=%q want verify override", got)
	}
}

func TestColumnStoreSuiteEffectiveAssetReadIntegrityCachedVerifyAliasesM1634(t *testing.T) {
	prevAllowUnsafe := *treedbAllowUnsafe
	*treedbAllowUnsafe = true
	t.Cleanup(func() { *treedbAllowUnsafe = prevAllowUnsafe })

	for _, value := range []collections.ColumnAssetReadIntegrity{
		collections.ColumnAssetReadIntegrityCachedVerify,
		collections.ColumnAssetReadIntegrity("cached-verify"),
		collections.ColumnAssetReadIntegrity("verify_once"),
		collections.ColumnAssetReadIntegrity("verify-once"),
	} {
		got, err := columnStoreSuiteEffectiveAssetReadIntegrity(value)
		if err != nil {
			t.Fatalf("columnStoreSuiteEffectiveAssetReadIntegrity(%q): %v", value, err)
		}
		if got != collections.ColumnAssetReadIntegrityCachedVerify {
			t.Fatalf("columnStoreSuiteEffectiveAssetReadIntegrity(%q)=%q want %q", value, got, collections.ColumnAssetReadIntegrityCachedVerify)
		}
	}
}

func TestRunColumnStoreSuiteWritesArtifactsAndMetricsM11A(t *testing.T) {
	dir := t.TempDir()
	cfg := BenchConfig{
		Keys:                 64,
		BatchSize:            16,
		DBsArg:               "treedb",
		Profile:              "durable",
		Progress:             false,
		SeedUsed:             1,
		CPUProfile:           filepath.Join(dir, "cpu"),
		AllocsProfile:        filepath.Join(dir, "allocs"),
		AllocsProfileRate:    1,
		CheckpointCPUProfile: filepath.Join(dir, "checkpoint_cpu"),
		BlockProfile:         filepath.Join(dir, "block.pprof"),
		BlockProfileRate:     1,
		MutexProfile:         filepath.Join(dir, "mutex.pprof"),
		MutexProfileFraction: 1,
		TraceProfile:         filepath.Join(dir, "trace.out"),
	}

	out, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ProfileDir:    dir,
		ExecutionPath: "native-fastpath",
		ForcedPath:    columnStorePathRowStoreBaseline,
		RunBenchprof:  true,
	})
	if err != nil {
		t.Fatalf("runColumnStoreSuite: %v", err)
	}

	for _, want := range []string{
		"# unified_bench suite: column_store",
		"q1",
		"q4a",
		"q4b",
		"q5_metadata",
		"row_store_baseline",
		"Parity",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("suite output missing %q:\n%s", want, out)
		}
	}

	for _, name := range []string{
		"benchprof_results.json",
		"benchprof_results.md",
		"insights.md",
		"insights.json",
		"insights.html",
		"column_store_results.json",
		"column_store_results.md",
		"column_store_results.html",
		"cpu_column_store_treedb_column_store.pprof",
		"allocs_column_store_treedb_column_store.pprof",
		"checkpoint_cpu_checkpoint_column_store_treedb_column_store.pprof",
		"block.pprof",
		"mutex.pprof",
		"trace.out",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected %s: %v", name, err)
		}
	}

	var benchprof benchprofExport
	benchprofData, err := os.ReadFile(filepath.Join(dir, "benchprof_results.json"))
	if err != nil {
		t.Fatalf("read benchprof_results.json: %v", err)
	}
	if err := json.Unmarshal(benchprofData, &benchprof); err != nil {
		t.Fatalf("unmarshal benchprof_results.json: %v", err)
	}
	if len(benchprof.Runs) != 1 {
		t.Fatalf("benchprof runs=%d want 1", len(benchprof.Runs))
	}
	if got := benchprof.Runs[0].Results["column_store"]["TreeDB Column Store"]; got <= 0 {
		t.Fatalf("benchprof column_store aggregate rows/sec=%f want positive", got)
	}
	benchprofMarkdown, err := os.ReadFile(filepath.Join(dir, "benchprof_results.md"))
	if err != nil {
		t.Fatalf("read benchprof_results.md: %v", err)
	}
	if !strings.Contains(string(benchprofMarkdown), "Column store query phase") {
		t.Fatalf("benchprof markdown missing aggregate column_store display name:\n%s", benchprofMarkdown)
	}

	var report columnStoreSuiteReport
	data, err := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
	if err != nil {
		t.Fatalf("read column_store_results.json: %v", err)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal column_store_results.json: %v", err)
	}
	if got, want := report.Suite, "column_store"; got != want {
		t.Fatalf("suite=%q want %q", got, want)
	}
	if got, want := report.Profile, "durable"; got != want {
		t.Fatalf("profile=%q want %q", got, want)
	}
	if got, want := report.PathLabel, "native-fastpath"; got != want {
		t.Fatalf("path_label=%q want %q", got, want)
	}
	if got, want := report.ForcedPath, columnStorePathRowStoreBaseline; got != want {
		t.Fatalf("forced_path=%q want %q", got, want)
	}
	queryMetrics := assertColumnStoreQueryMetricCoverageM11A(t, report.Queries)
	for _, q := range report.Queries {
		assertColumnStoreCompressionAttributionM1952(t, q.Name, q.CompressionAttribution, true)
		if q.PlanLabel != columnStorePathRowStoreBaseline {
			t.Fatalf("query %s plan_label=%q want %q", q.Name, q.PlanLabel, columnStorePathRowStoreBaseline)
		}
		if q.DurationMS < 0 || q.RowsPerSecond < 0 {
			t.Fatalf("query %s has invalid timing metrics: %+v", q.Name, q)
		}
		if q.BytesRead <= 0 {
			t.Fatalf("query %s bytes_read=%d", q.Name, q.BytesRead)
		}
		if q.ScanDurationMS < 0 {
			t.Fatalf("query %s scan_duration_ms=%v", q.Name, q.ScanDurationMS)
		}
		if q.AdapterDurationMS < 0 {
			t.Fatalf("query %s adapter_duration_ms=%v", q.Name, q.AdapterDurationMS)
		}
		if q.PlannerCandidates == 0 || q.PlannerReason == "" {
			t.Fatalf("query %s missing planner diagnostics: %+v", q.Name, q)
		}
		if q.StorageSource != string(collections.ColumnPhysicalQueryStorageSourceRowScan) || q.FallbackReason != string(collections.ColumnPhysicalQueryFallbackNone) {
			t.Fatalf("query %s storage source/fallback=%q/%q want row scan/none", q.Name, q.StorageSource, q.FallbackReason)
		}
		if q.ManifestRootName == "" || q.ManifestRoot == 0 || q.ManifestGeneration == 0 || q.ActiveManifestChecksum == 0 {
			t.Fatalf("query %s missing manifest identity fields: %+v", q.Name, q)
		}
		if q.RowMaterializations != report.Rows {
			t.Fatalf("query %s row_materializations=%d want %d", q.Name, q.RowMaterializations, report.Rows)
		}
		if !strings.Contains(q.ThroughputInterpretation, "decode-bound") {
			t.Fatalf("query %s throughput_interpretation=%q want row-store decode-bound classification", q.Name, q.ThroughputInterpretation)
		}
	}
	if q := queryMetrics["q5_metadata"]; q.AliasOf != "q5" || q.ImplementationNote == "" {
		t.Fatalf("q5_metadata should be explicitly reported as a q5 alias placeholder: %+v", q)
	}
	if got, want := queryMetrics["q5_metadata"].ProductionHash, queryMetrics["q5"].ProductionHash; got != want {
		t.Fatalf("q5_metadata production hash=%016x want q5 hash=%016x", got, want)
	}
	if got, want := queryMetrics["q5_metadata"].RawHash, queryMetrics["q5"].RawHash; got != want {
		t.Fatalf("q5_metadata raw hash=%016x want q5 hash=%016x", got, want)
	}
	assertColumnStoreCodecLayoutCoverageM1952(t, report)
	assertColumnStoreParityCoverageM11A(t, report.Parity)
	for name, parity := range report.Parity {
		if !parity.Pass {
			t.Fatalf("parity %s failed: %+v", name, parity)
		}
		if parity.RawHash == 0 || parity.ProductionHash == 0 {
			t.Fatalf("parity %s has zero hash: %+v", name, parity)
		}
	}
	if report.ByteAccounting.CommandWALBytesBeforeCheckpoint == 0 {
		t.Fatalf("expected command WAL bytes in byte accounting: %+v", report.ByteAccounting)
	}
	if report.ByteAccounting.RetainedPayloadBytesNote == "" {
		t.Fatalf("expected retained-payload byte-accounting note: %+v", report.ByteAccounting)
	}
	if report.ByteAccounting.ColumnAssetBytes == 0 {
		t.Fatalf("expected measured M12A physical column asset bytes: %+v", report.ByteAccounting)
	}
	if report.ByteAccounting.ColumnAssetStoreBytes != report.ByteAccounting.ColumnAssetBytes {
		t.Fatalf("column_asset_store_bytes=%d want column_asset_bytes=%d", report.ByteAccounting.ColumnAssetStoreBytes, report.ByteAccounting.ColumnAssetBytes)
	}
	if report.ByteAccounting.OrdinaryValueLogBytes < 0 || report.ByteAccounting.LeafLogBytes < 0 {
		t.Fatalf("expected non-negative ordinary value_vlog and leaf_vlog byte splits: %+v", report.ByteAccounting)
	}
	if report.ByteAccounting.PrimaryIndexBytes == 0 {
		t.Fatalf("expected primary index bytes in byte accounting: %+v", report.ByteAccounting)
	}
	if got, want := report.ByteAccounting.DurableStorageBytesWALExcluded, columnStoreSuiteDurableStorageBytesWALExcluded(report.ByteAccounting.DBTotalBytes, report.ByteAccounting.WALBytesExcludedFromDurable); got != want {
		t.Fatalf("durable_storage_bytes_wal_excluded=%d want db_total-wal=%d (accounting=%+v)", got, want, report.ByteAccounting)
	}
	if report.ByteAccounting.DurableStorageBytesWALExcludedNote == "" || !strings.Contains(report.ByteAccounting.DurableStorageBytesWALExcludedNote, "value_vlog") || !strings.Contains(report.ByteAccounting.DurableStorageBytesWALExcludedNote, "leaf_vlog") || !strings.Contains(report.ByteAccounting.DurableStorageBytesWALExcludedNote, "index.db") {
		t.Fatalf("expected durable-storage WAL-excluded note to list durable included stores: %+v", report.ByteAccounting)
	}
	if got, want := report.ByteAccounting.TotalReconstructableBytes, report.ByteAccounting.RetainedPayloadBytes+report.ByteAccounting.ColumnAssetBytes+report.ByteAccounting.ManifestControlBytes; got != want {
		t.Fatalf("total_reconstructable_bytes=%d want retained+column+manifest=%d", got, want)
	}
	var sawReopenRecovery bool
	for _, stage := range report.Stages {
		if stage.Name == "reopen_recovery" {
			sawReopenRecovery = true
		}
		if stage.Name == "reopen" {
			t.Fatalf("stage name should use reopen_recovery, got legacy stage: %+v", stage)
		}
	}
	if !sawReopenRecovery {
		t.Fatalf("missing reopen_recovery stage: %+v", report.Stages)
	}
	if !strings.Contains(string(data), `"command_wal_bytes_before_checkpoint"`) {
		t.Fatalf("column store JSON missing before-checkpoint command WAL label:\n%s", data)
	}
	for _, want := range []string{`"primary_index_bytes"`, `"wal_bytes_excluded_from_durable_storage"`, `"durable_storage_bytes_wal_excluded"`, `"durable_storage_bytes_wal_excluded_note"`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("column store JSON missing WAL-excluded durable storage field %s:\n%s", want, data)
		}
	}
	for _, want := range []string{`"jsonbench_cells"`, `"cell_label"`, `"sort_layout"`, `"execution_mode"`, `"metadata_data_scan_path"`, `"mutation_mode"`, `"retained_payload_policy"`, `"retained_payload_encoding"`, `"retained_payload_encoding_status"`, `"retained_payload_compression"`, `"retained_payload_compression_policy"`, `"retained_payload_compression_status"`, `"typed_storage_owner"`, `"row_count"`, `"reconstruction_status"`, `"full_data_caveat"`, `"storage_accounting_caveat"`, `"external_jsonbench_status"`, `"colgranule_reuse_map"`, `"codec_layouts"`, `"compression_attribution"`, `"codec_layout_label"`, `"compression_policy_label"`, `"compressed_bytes"`, `"decompressed_bytes"`, `"raw_bytes"`, `"compression_ratio"`, `"compression_duration_source"`, `"decompression_duration_source"`, `"benchmark_b_per_op"`, `"benchmark_allocs_per_op"`, `"benchmark_allocation_source"`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("column store JSON missing reporting field %s:\n%s", want, data)
		}
	}
	assertColumnStoreJSONBenchCellShapeM1955(t, report, false)
	if !strings.Contains(string(data), `"column_asset_bytes"`) ||
		!strings.Contains(string(data), `"column_asset_store_bytes"`) ||
		!strings.Contains(string(data), `"ordinary_value_vlog_bytes"`) ||
		!strings.Contains(string(data), `"leaf_vlog_bytes"`) ||
		!strings.Contains(string(data), `"retained_payload_bytes_note"`) {
		t.Fatalf("column store JSON missing byte-accounting notes:\n%s", data)
	}
	if strings.Contains(string(data), `"command_wal_bytes":`) {
		t.Fatalf("column store JSON contains ambiguous command WAL label:\n%s", data)
	}
	columnMarkdown, err := os.ReadFile(filepath.Join(dir, "column_store_results.md"))
	if err != nil {
		t.Fatalf("read column_store_results.md: %v", err)
	}
	if !strings.Contains(string(columnMarkdown), "command_wal_bytes_before_checkpoint") {
		t.Fatalf("column store markdown missing before-checkpoint command WAL label:\n%s", columnMarkdown)
	}
	for _, want := range []string{"primary_index_bytes", "wal_bytes_excluded_from_durable_storage", "durable_storage_bytes_wal_excluded", "durable_storage_bytes_wal_excluded_note"} {
		if !strings.Contains(string(columnMarkdown), want) {
			t.Fatalf("column store markdown missing WAL-excluded durable storage field %q:\n%s", want, columnMarkdown)
		}
	}
	for _, want := range []string{"## Production JSONBench Synthetic Cells", "## Colgranule Reuse Map", "metadata/data path", "retained payload", "retained encoding", "retained compression", "typed owner", "full-data caveat", "## Query Compression And Allocation Attribution", "## Codec/Layout Matrix", "codec/layout", "compression policy", "compressed bytes", "decompressed bytes", "raw bytes", "B/op", "allocs/op", columnStoreCompressionPolicyOff, columnStoreCompressionPolicyDefault} {
		if !strings.Contains(string(columnMarkdown), want) {
			t.Fatalf("column store markdown missing reporting field %q:\n%s", want, columnMarkdown)
		}
	}
	if !strings.Contains(string(columnMarkdown), "column_asset_bytes") ||
		!strings.Contains(string(columnMarkdown), "column_asset_store_bytes") ||
		!strings.Contains(string(columnMarkdown), "primary_index_bytes") ||
		!strings.Contains(string(columnMarkdown), "ordinary_value_vlog_bytes") ||
		!strings.Contains(string(columnMarkdown), "leaf_vlog_bytes") ||
		!strings.Contains(string(columnMarkdown), "retained_payload_bytes_note") {
		t.Fatalf("column store markdown missing byte-accounting notes:\n%s", columnMarkdown)
	}
	if strings.Contains(string(columnMarkdown), "| `` |") {
		t.Fatalf("column store markdown should render empty notes as placeholder:\n%s", columnMarkdown)
	}
	if report.ByteAccounting.ManifestControlBytes == 0 || report.ByteAccounting.DBTotalBytes == 0 || report.ByteAccounting.DBTotalFiles == 0 {
		t.Fatalf("expected measured manifest/control and DB byte accounting: %+v", report.ByteAccounting)
	}
	if report.Manifest.ActiveGeneration == 0 || report.Manifest.ActiveChecksum == 0 || report.Manifest.AppliedCommandLSN == 0 || report.Manifest.ManifestRootName == "" || report.Manifest.ManifestRoot == 0 {
		t.Fatalf("expected active/recovery-authoritative manifest identity: %+v", report.Manifest)
	}
	if len(report.FailClosedForcedPaths) != 0 {
		t.Fatalf("unexpected fail-closed forced path labels after M14B routing: %+v", report.FailClosedForcedPaths)
	}
	if !columnStoreTestStringSliceContains(report.AcceptedForcedPaths, columnStorePathSerialColumnScan) {
		t.Fatalf("expected physical forced path labels to be recorded as accepted: %+v", report.AcceptedForcedPaths)
	}
	if report.Artifacts.CPUProfile == "" ||
		report.Artifacts.AllocsProfile == "" ||
		report.Artifacts.CheckpointCPUProfile == "" ||
		report.Artifacts.BlockProfile == "" ||
		report.Artifacts.MutexProfile == "" ||
		report.Artifacts.TraceProfile == "" {
		t.Fatalf("expected all configured profile artifacts to be recorded: %+v", report.Artifacts)
	}
	for label, path := range map[string]string{
		"block": report.Artifacts.BlockDeltaProfile,
		"mutex": report.Artifacts.MutexDeltaProfile,
	} {
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("reported %s delta artifact %s: %v", label, path, err)
		}
	}
}

func TestWriteColumnStoreSuiteArtifactsUsesRecordedColumnPathsM11A(t *testing.T) {
	dir := t.TempDir()
	defaultDir := filepath.Join(dir, "default")
	recordedDir := filepath.Join(dir, "recorded")
	report := columnStoreSuiteReport{
		Suite: "column_store",
		Artifacts: columnStoreArtifactPaths{
			ColumnJSON:        filepath.Join(recordedDir, "custom_column.json"),
			ColumnMarkdown:    filepath.Join(recordedDir, "custom_column.md"),
			ColumnHTML:        filepath.Join(recordedDir, "custom_column.html"),
			BenchprofJSON:     filepath.Join(recordedDir, "custom_benchprof.json"),
			BenchprofMarkdown: filepath.Join(recordedDir, "custom_benchprof.md"),
		},
	}
	run := BenchRun{
		Config: BenchConfig{Keys: 1, Profile: "durable"},
		Results: map[string]map[string]float64{
			"column_store": {columnStoreSuiteBenchDisplayName: 1},
		},
	}

	if err := writeColumnStoreSuiteArtifacts(defaultDir, "native-fastpath", report, "# column store", run); err != nil {
		t.Fatalf("writeColumnStoreSuiteArtifacts: %v", err)
	}
	for _, path := range []string{
		report.Artifacts.ColumnJSON,
		report.Artifacts.ColumnMarkdown,
		report.Artifacts.ColumnHTML,
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected recorded artifact path %s: %v", path, err)
		}
	}
	for _, name := range []string{
		"column_store_results.json",
		"column_store_results.md",
		"column_store_results.html",
	} {
		if _, err := os.Stat(filepath.Join(defaultDir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected no fallback column artifact %s, stat err=%v", name, err)
		}
	}
	for _, path := range []string{
		report.Artifacts.BenchprofJSON,
		report.Artifacts.BenchprofMarkdown,
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected recorded benchprof artifact path %s: %v", path, err)
		}
	}
	for _, name := range []string{
		"benchprof_results.json",
		"benchprof_results.md",
	} {
		if _, err := os.Stat(filepath.Join(defaultDir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected no fallback benchprof artifact %s, stat err=%v", name, err)
		}
	}
}

func TestColumnStoreBenchRunUsesDurationForAggregateM11A(t *testing.T) {
	run := columnStoreBenchRun(BenchConfig{}, "durable", t.TempDir(), columnStoreSuiteReport{
		Rows:      30,
		BatchSize: 10,
		Queries: []columnStoreQueryMetric{
			{Name: "q1", RowsProcessed: 10, RowMaterializations: 10, RowsPerSecond: 1, duration: 10 * time.Millisecond},
			{Name: "q2", RowsProcessed: 20, RowMaterializations: 20, RowsPerSecond: 1, duration: 20 * time.Millisecond},
		},
	}, nil, 0)

	got := run.Results[columnStoreSuiteBenchTestName][columnStoreSuiteBenchDisplayName]
	if math.Abs(got-1000) > 1e-6 {
		t.Fatalf("aggregate rows/sec=%f want 1000 from exact durations", got)
	}
}

func TestColumnStoreBenchRunUsesRowsProcessedForPhysicalAggregateM14B(t *testing.T) {
	run := columnStoreBenchRun(BenchConfig{}, "durable", t.TempDir(), columnStoreSuiteReport{
		Rows:      30,
		BatchSize: 10,
		Queries: []columnStoreQueryMetric{
			{Name: "q1", RowsProcessed: 10, RowMaterializations: 0, RowsPerSecond: 1, duration: 10 * time.Millisecond},
			{Name: "q2", RowsProcessed: 20, RowMaterializations: 0, RowsPerSecond: 1, duration: 20 * time.Millisecond},
		},
	}, nil, 0)

	got := run.Results[columnStoreSuiteBenchTestName][columnStoreSuiteBenchDisplayName]
	if math.Abs(got-1000) > 1e-6 {
		t.Fatalf("aggregate rows/sec=%f want 1000 from physical rows processed", got)
	}
}

func TestColumnStoreBenchRunFallsBackToRowMaterializationsForLegacyArtifactsM14B(t *testing.T) {
	run := columnStoreBenchRun(BenchConfig{}, "durable", t.TempDir(), columnStoreSuiteReport{
		Rows:      30,
		BatchSize: 10,
		Queries: []columnStoreQueryMetric{
			{Name: "q1", Rows: 10, RowMaterializations: 10, RowsPerSecond: 1, duration: 10 * time.Millisecond},
			{Name: "q2", Rows: 20, RowMaterializations: 20, RowsPerSecond: 1, duration: 20 * time.Millisecond},
		},
	}, nil, 0)

	got := run.Results[columnStoreSuiteBenchTestName][columnStoreSuiteBenchDisplayName]
	if math.Abs(got-1000) > 1e-6 {
		t.Fatalf("aggregate rows/sec=%f want 1000 from legacy rows fallback", got)
	}
}

func TestColumnStoreBenchRunDoesNotFallbackForZeroProcessedPhysicalRowsM14B(t *testing.T) {
	run := columnStoreBenchRun(BenchConfig{}, "durable", t.TempDir(), columnStoreSuiteReport{
		Rows:      30,
		BatchSize: 10,
		Queries: []columnStoreQueryMetric{
			{Name: "q1", Rows: 30, RowsProcessed: 0, RowsProcessedKnown: true, RowMaterializations: 0, RowsPerSecond: 0, duration: 30 * time.Millisecond},
		},
	}, nil, 0)

	got := run.Results[columnStoreSuiteBenchTestName][columnStoreSuiteBenchDisplayName]
	if got != 0 {
		t.Fatalf("aggregate rows/sec=%f want zero without legacy row materialization fallback", got)
	}
}

func TestColumnStoreBenchRunFiltersSelectedQueryOrderM1634(t *testing.T) {
	run := columnStoreBenchRun(BenchConfig{}, "durable", t.TempDir(), columnStoreSuiteReport{
		Rows:      30,
		BatchSize: 10,
		Queries: []columnStoreQueryMetric{
			{Name: columnStoreQueryQ3, RowsProcessed: 30, RowsPerSecond: 300, duration: 10 * time.Millisecond},
			{Name: columnStoreQueryQ5, RowsProcessed: 30, RowsPerSecond: 500, duration: 10 * time.Millisecond},
		},
	}, nil, 0)

	wantOrder := []string{
		columnStoreSuiteBenchTestName,
		columnStoreSuiteBenchMetricPrefix + columnStoreQueryQ3,
		columnStoreSuiteBenchMetricPrefix + columnStoreQueryQ5,
	}
	if !slices.Equal(run.TestOrder, wantOrder) {
		t.Fatalf("TestOrder=%v want %v", run.TestOrder, wantOrder)
	}
	if columnStoreTestCommaListContains(run.Config.TestsArg, columnStoreSuiteBenchMetricPrefix+columnStoreQueryQ1) {
		t.Fatalf("TestsArg includes unselected q1: %q", run.Config.TestsArg)
	}
	if _, ok := run.Results[columnStoreSuiteBenchMetricPrefix+columnStoreQueryQ1]; ok {
		t.Fatalf("results include unselected q1: %+v", run.Results)
	}
	if _, ok := run.DisplayNames[columnStoreSuiteBenchMetricPrefix+columnStoreQueryQ3]; !ok {
		t.Fatalf("display names missing selected q3: %+v", run.DisplayNames)
	}
}

func TestColumnStoreBenchRunIncludesAliasesOnlyWhenSourceQuerySelectedM1634(t *testing.T) {
	run := columnStoreBenchRun(BenchConfig{}, "durable", t.TempDir(), columnStoreSuiteReport{
		Rows:      30,
		BatchSize: 10,
		Queries: []columnStoreQueryMetric{
			{Name: columnStoreQueryQ1, RowsProcessed: 30, RowsPerSecond: 100, duration: 10 * time.Millisecond},
			{Name: columnStoreQueryQ4A, RowsProcessed: 30, RowsPerSecond: 400, duration: 10 * time.Millisecond},
		},
	}, nil, 0)

	if !slices.Contains(run.TestOrder, columnStoreSuiteAliasFullScanQ1) {
		t.Fatalf("TestOrder missing q1 alias: %v", run.TestOrder)
	}
	if !slices.Contains(run.TestOrder, columnStoreSuiteAliasPrefixQ4A) {
		t.Fatalf("TestOrder missing q4a alias: %v", run.TestOrder)
	}
	if _, ok := run.Results[columnStoreSuiteAliasFullScanQ1]; !ok {
		t.Fatalf("results missing q1 alias: %+v", run.Results)
	}
	if _, ok := run.Results[columnStoreSuiteAliasPrefixQ4A]; !ok {
		t.Fatalf("results missing q4a alias: %+v", run.Results)
	}
}

func TestColumnStoreSuiteThroughputInterpretationM14C(t *testing.T) {
	tests := []struct {
		name string
		q    columnStoreQueryMetric
		want []string
	}{
		{
			name: "row store decode baseline",
			q: columnStoreQueryMetric{
				Name:                columnStoreQueryQ1,
				PlanLabel:           columnStorePathRowStoreBaseline,
				Rows:                1024,
				RowsProcessed:       1024,
				RowMaterializations: 1024,
				BytesRead:           128 << 10,
			},
			want: []string{"decode-bound", "row materialization", "mark-pruning not active", "effective_rows_processed=1024", "row_materializations=1024/1024", "bytes_read=131072"},
		},
		{
			name: "btree decode baseline",
			q: columnStoreQueryMetric{
				Name:                columnStoreQueryQ2,
				PlanLabel:           columnStorePathBTreeIndexBaseline,
				Rows:                1024,
				RowsProcessed:       1024,
				RowMaterializations: 1024,
				BytesRead:           128 << 10,
			},
			want: []string{"decode-bound", "B-tree baseline", "row materialization", "mark-pruning not active", "effective_rows_processed=1024", "row_materializations=1024/1024"},
		},
		{
			name: "serial physical scan",
			q: columnStoreQueryMetric{
				Name:                columnStoreQueryQ3,
				PlanLabel:           columnStorePathSerialColumnScan,
				Rows:                1024,
				RowsProcessed:       1024,
				RowMaterializations: 0,
				BytesRead:           96 << 10,
				ScheduledGranules:   2,
			},
			want: []string{"physical serial scan", "TCPA decode", "aggregation", "memory-bandwidth", "mark-pruning not active", "effective_rows_processed=1024", "scheduled_granules=2"},
		},
		{
			name: "missing source row count falls back to processed rows",
			q: columnStoreQueryMetric{
				Name:                columnStoreQueryQ3,
				PlanLabel:           columnStorePathSerialColumnScan,
				RowsProcessed:       1024,
				RowMaterializations: 0,
			},
			want: []string{"physical serial scan", "row_materializations=0/1024"},
		},
		{
			name: "legacy materialized row evidence",
			q: columnStoreQueryMetric{
				Name:                columnStoreQueryQ1,
				PlanLabel:           columnStorePathRowStoreBaseline,
				Rows:                1024,
				RowMaterializations: 1024,
			},
			want: []string{"decode-bound", "effective_rows_processed=1024", "row_materializations=1024/1024"},
		},
		{
			name: "aggregate metadata fallback without metadata hits",
			q: columnStoreQueryMetric{
				Name:                columnStoreQueryQ5Metadata,
				PlanLabel:           columnStorePathAggregateMetadata,
				Rows:                1024,
				RowsProcessed:       1024,
				RowMaterializations: 0,
				BytesRead:           96 << 10,
				MetadataHits:        0,
			},
			want: []string{"fallback-bound", "metadata hits reported", "physical scan/reroute", "mark-pruning not active", "metadata_hits=0"},
		},
		{
			name: "metadata hit future path",
			q: columnStoreQueryMetric{
				Name:                columnStoreQueryQ5Metadata,
				PlanLabel:           columnStorePathAggregateMetadata,
				Rows:                1024,
				RowsProcessed:       1024,
				RowMaterializations: 0,
				MetadataHits:        4,
			},
			want: []string{"metadata-bound", "metadata hits", "mark-pruning not active", "metadata_hits=4"},
		},
		{
			name: "granule pruning future path",
			q: columnStoreQueryMetric{
				Name:                columnStoreQueryQ1,
				PlanLabel:           columnStorePathSerialColumnScan,
				Rows:                1024,
				RowsProcessed:       1024,
				RowMaterializations: 0,
				SkippedGranules:     3,
			},
			want: []string{"physical serial scan", "mark-pruning active", "skipped_granules=3"},
		},
		{
			name: "parallel physical scan",
			q: columnStoreQueryMetric{
				Name:                columnStoreQueryQ4A,
				PlanLabel:           columnStorePathParallelColumnScan,
				Rows:                1024,
				RowsProcessed:       1024,
				RowMaterializations: 0,
				BytesRead:           96 << 10,
				WorkerCount:         2,
			},
			want: []string{"parallel physical scan", "manifest-ref partition", "overhead-bound", "memory-bandwidth", "mark-pruning not active"},
		},
		{
			name: "parallel invalid worker count",
			q: columnStoreQueryMetric{
				Name:          columnStoreQueryQ4A,
				PlanLabel:     columnStorePathParallelColumnScan,
				Rows:          1024,
				RowsProcessed: 1024,
				WorkerCount:   0,
			},
			want: []string{"parallel physical scan", "invalid reported worker_count=0", "mark-pruning not active", "effective_rows_processed=1024"},
		},
		{
			name: "unknown effective rows are explicit",
			q: columnStoreQueryMetric{
				Name:      columnStoreQueryQ1,
				PlanLabel: columnStorePathSerialColumnScan,
			},
			want: []string{"physical serial scan", "effective_rows_processed=unknown", "row_materializations=0/unknown"},
		},
		{
			name: "known zero effective rows are explicit",
			q: columnStoreQueryMetric{
				Name:               columnStoreQueryQ1,
				PlanLabel:          columnStorePathSerialColumnScan,
				RowsProcessedKnown: true,
			},
			want: []string{"physical serial scan", "effective_rows_processed=0", "row_materializations=0/unknown"},
		},
		{
			name: "unknown plan includes labels",
			q: columnStoreQueryMetric{
				Name:          "q_custom",
				PlanLabel:     "unknown_plan",
				Rows:          1024,
				RowsProcessed: 1024,
			},
			want: []string{"fallback/error-bound", "unknown_plan", "q_custom", "mark-pruning not active"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := columnStoreQueryThroughputInterpretation(tc.q)
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Fatalf("interpretation %q missing %q", got, want)
				}
			}
		})
	}
}

func TestColumnStoreSuiteMarkdownRendersThroughputInterpretationM14C(t *testing.T) {
	report := columnStoreSuiteReport{
		Suite:      "column_store",
		Profile:    "durable",
		Fixture:    "synthetic",
		ForcedPath: columnStorePathSerialColumnScan,
		Rows:       1024,
		Queries: []columnStoreQueryMetric{
			{
				Name:                     columnStoreQueryQ1,
				PlanLabel:                columnStorePathSerialColumnScan,
				RowsProcessed:            1024,
				ThroughputInterpretation: "physical serial scan: TCPA decode plus reducer aggregation over declared columns; memory-bandwidth bound on asset bytes; mark-pruning not active",
			},
			{
				Name:                     columnStoreQueryQ5Metadata,
				PlanLabel:                columnStorePathAggregateMetadata,
				RowsProcessed:            1024,
				ThroughputInterpretation: "fallback-bound aggregate metadata label: no metadata hits reported, so evidence must be treated as a physical scan/reroute rather than the metadata-asset fast path; mark-pruning not active",
			},
			{
				Name:                     "q|pipe`tick",
				PlanLabel:                "plan|pipe`tick",
				ThroughputInterpretation: "line1\r\nline2|pipe <b>&bad</b>",
				ImplementationNote:       "note|pipe`tick",
			},
			{
				Name:      "q_empty_interpretation",
				PlanLabel: columnStorePathSerialColumnScan,
			},
			{
				Name:                     "-",
				PlanLabel:                "-",
				ThroughputInterpretation: "-",
			},
		},
		Parity: map[string]columnStoreParity{
			columnStoreQueryQ1:         {Pass: true},
			columnStoreQueryQ5Metadata: {Pass: true},
		},
	}

	md := renderColumnStoreSuiteMarkdown(report)
	for _, want := range []string{
		"## Throughput Interpretation",
		"physical serial scan",
		"fallback-bound aggregate metadata label",
		"mark-pruning not active",
		"adapter ms",
		"``q\\|pipe`tick``",
		"``plan\\|pipe`tick``",
		"``note\\|pipe`tick``",
		"line1 line2\\|pipe &lt;b&gt;&amp;bad&lt;/b&gt;",
		"| `q_empty_interpretation` | `serial_column_scan` | (empty) |",
		"| `-` | `-` | - |",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q:\n%s", want, md)
		}
	}
}

func TestColumnStoreQueryCompressionAttributionKeepsPhysicalAggregateRerouteM1952(t *testing.T) {
	attr := columnStoreQueryCompressionAttribution(
		columnStorePathSerialColumnScan,
		string(collections.ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset),
		string(collections.ColumnPhysicalQueryFallbackAggregateMetadataUnsupported),
		128,
	)
	if attr.SupportState != columnStoreCompressionSupportSupported || attr.CodecLayoutLabel != columnStoreCodecLayoutCurrent || attr.CompressionPolicyLabel != columnStoreCompressionPolicyDefault {
		t.Fatalf("aggregate metadata physical reroute attribution=%+v, want supported current codec layout", attr)
	}
	if attr.SupportReason != string(collections.ColumnPhysicalQueryFallbackAggregateMetadataUnsupported) || strings.Contains(attr.RawBytesSource, "fallback_no_column_codec") || strings.Contains(attr.DecompressedBytesSource, "fallback_no_column_codec") {
		t.Fatalf("aggregate metadata physical reroute sources=%+v, want column codec attribution with reroute reason", attr)
	}

	rowFallback := columnStoreQueryCompressionAttribution(
		columnStorePathSerialColumnScan,
		string(collections.ColumnPhysicalQueryStorageSourceRowScan),
		string(collections.ColumnPhysicalQueryFallbackDirectAssetReduceUnsupported),
		128,
	)
	if rowFallback.SupportState != columnStoreCompressionSupportFallback || rowFallback.CompressionPolicyLabel != "not_applicable_fallback" || !strings.Contains(rowFallback.RawBytesSource, "fallback") {
		t.Fatalf("row fallback attribution=%+v, want not-applicable fallback", rowFallback)
	}
}

func TestColumnStoreSuiteMarkdownRendersCodecLayoutUnsupportedRowsM1952(t *testing.T) {
	report := columnStoreSuiteReport{
		Suite:                 "column_store",
		Profile:               "durable",
		Fixture:               "synthetic",
		ForcedPath:            columnStorePathSerialColumnScan,
		CompressionMatrixNote: "future unsupported rows remain reportable without enabling runtime support",
		Queries: []columnStoreQueryMetric{{
			Name:                   columnStoreQueryQ1,
			PlanLabel:              columnStorePathSerialColumnScan,
			CompressionAttribution: columnStoreQueryCompressionAttribution(columnStorePathSerialColumnScan, string(collections.ColumnPhysicalQueryStorageSourceTypedColumnPartSection), string(collections.ColumnPhysicalQueryFallbackNone), 128),
		}},
		CodecLayouts: []columnStoreCodecLayoutMetric{{
			columnStoreCompressionAttribution: columnStoreCompressionAttribution{
				CodecLayoutLabel:            "future_layout/zstd_dict",
				CompressionPolicyLabel:      "alternative_zstd_dict",
				RequestedCompression:        "zstd_dict",
				ActualCompression:           "none",
				SupportState:                "unsupported",
				SupportReason:               "zstd_dict runtime support is intentionally not widened in slice 1",
				CompressedBytesSource:       "not_available_unsupported",
				RawBytesSource:              "not_available_unsupported",
				DecompressedBytesSource:     "not_available_unsupported",
				CompressionRatioSource:      "not_available_unsupported",
				CompressionDurationSource:   "not_available_unsupported",
				DecompressionDurationSource: "not_available_unsupported",
				BenchmarkAllocationSource:   "not_available_unsupported",
			},
			Rows:    1024,
			Columns: 3,
		}},
	}

	md := renderColumnStoreSuiteMarkdown(report)
	for _, want := range []string{
		"## Query Compression And Allocation Attribution",
		"## Codec/Layout Matrix",
		"future_layout/zstd_dict",
		"alternative_zstd_dict",
		"unsupported: zstd_dict runtime support is intentionally not widened in slice 1",
		"B/op",
		"allocs/op",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q:\n%s", want, md)
		}
	}
}

func TestMarkdownCodeTableTextPreservesBoundaryWhitespaceM14C(t *testing.T) {
	got := markdownCodeTableText(" value|with`tick ")
	want := "``  value\\|with`tick  ``"
	if got != want {
		t.Fatalf("markdownCodeTableText=%q want %q", got, want)
	}
}

func TestMarkdownTableTextHandlesEscapedPipesAndBlankNewlinesM14C(t *testing.T) {
	if got, want := markdownTableText(`a\|b|c\\|d`), `a\|b\|c\\\|d`; got != want {
		t.Fatalf("markdownTableText escaped pipes=%q want %q", got, want)
	}
	if got, want := markdownTableText(`a\\\\\\\\|b|c`), `a\\\\\\\\\|b\|c`; got != want {
		t.Fatalf("markdownTableText long backslash run=%q want %q", got, want)
	}
	if got, want := markdownTableText("uses `prepared` runner"), "uses \\`prepared\\` runner"; got != want {
		t.Fatalf("markdownTableText escaped backticks=%q want %q", got, want)
	}
	if got, want := markdownCodeTableText("\r\n"), "`"+markdownTableEmptyCell+"`"; got != want {
		t.Fatalf("markdownCodeTableText blank newline=%q want empty marker", got)
	}
}

func TestRenderColumnStoreSuiteMarkdownCodeListsM11A(t *testing.T) {
	md := renderColumnStoreSuiteMarkdown(columnStoreSuiteReport{
		Profile:                "durable",
		Fixture:                "fixture",
		ForcedPath:             columnStorePathRowStoreBaseline,
		StageSeparatedBoundary: "boundary",
		ByteAccounting: columnStoreByteAccounting{
			ManifestControlMissing: []string{"manifest", "dictionary"},
		},
		AcceptedForcedPaths:   []string{"row_store_baseline", "physical_column"},
		FailClosedForcedPaths: []string{"planner_skipscan", "aggregate_metadata"},
	})

	for _, want := range []string{
		"- manifest_control_missing: `manifest`, `dictionary`",
		"- accepted labels are CLI/planner labels, not a promise that every run has the required physical assets",
		"- accepted: `row_store_baseline`, `physical_column`",
		"- fail-closed: `planner_skipscan`, `aggregate_metadata`",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q:\n%s", want, md)
		}
	}
}

func TestColumnStoreSuiteRuntimeProfilesDoNotEnableOnCreateFailureM11A(t *testing.T) {
	prevMutexFraction := runtime.SetMutexProfileFraction(0)
	t.Cleanup(func() {
		runtime.SetMutexProfileFraction(prevMutexFraction)
	})

	_, err := startColumnStoreSuiteRuntimeProfiles(BenchConfig{
		MutexProfile:         filepath.Join(t.TempDir(), "missing", "mutex.pprof"),
		MutexProfileFraction: 1,
	})
	if err == nil {
		t.Fatal("expected mutex profile create failure")
	}
	if !strings.Contains(err.Error(), "mutexprofile") {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotPrev := runtime.SetMutexProfileFraction(0); gotPrev != 0 {
		t.Fatalf("mutex profiler was left enabled after create failure: previous fraction=%d", gotPrev)
	}
}

func TestColumnStoreSuiteRuntimeProfilesRemoveStartedArtifactsOnLaterCreateFailureM11A(t *testing.T) {
	dir := t.TempDir()
	blockPath := filepath.Join(dir, "block.pprof")
	_, err := startColumnStoreSuiteRuntimeProfiles(BenchConfig{
		BlockProfile:         blockPath,
		BlockProfileRate:     1,
		MutexProfile:         filepath.Join(dir, "missing", "mutex.pprof"),
		MutexProfileFraction: 1,
	})
	if err == nil {
		t.Fatal("expected mutex profile create failure")
	}
	if !strings.Contains(err.Error(), "mutexprofile") {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, statErr := os.Stat(blockPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected started block profile artifact to be removed, stat err=%v", statErr)
	}
}

func TestColumnStoreSuiteRuntimeProfilesDoNotChangeMemRateWhenFilteredM11A(t *testing.T) {
	prevRate := runtime.MemProfileRate
	t.Cleanup(func() {
		runtime.MemProfileRate = prevRate
	})
	runtime.MemProfileRate = 4096

	finish, err := startColumnStoreSuiteRuntimeProfiles(BenchConfig{
		AllocsProfile:      filepath.Join(t.TempDir(), "allocs"),
		AllocsProfileRate:  1,
		AllocsProfileTests: map[string]struct{}{"not_column_store": {}},
	})
	if err != nil {
		t.Fatalf("start runtime profiles: %v", err)
	}
	if err := finish(); err != nil {
		t.Fatalf("finish runtime profiles: %v", err)
	}
	if got := runtime.MemProfileRate; got != 4096 {
		t.Fatalf("MemProfileRate changed despite allocs test filter: got %d", got)
	}
}

func TestColumnStoreSuiteCPUProfileStartsAfterProfileBaselinesM11A(t *testing.T) {
	var events []string
	profileTmpDir := t.TempDir()
	newProfilePath := func(prefix string) (string, error) {
		f, err := os.CreateTemp(profileTmpDir, prefix+"_*.pprof")
		if err != nil {
			return "", err
		}
		path := f.Name()
		if err := f.Close(); err != nil {
			_ = os.Remove(path)
			return "", err
		}
		return path, nil
	}
	profileHooks := &benchmarkProfileHooks{
		startCPUProfile: func(_ io.Writer) error {
			events = append(events, "cpu_start")
			return nil
		},
		stopCPUProfile: func() {
			events = append(events, "cpu_stop")
		},
		writeAllocsSnapshotTemp: func(prefix string) (string, error) {
			events = append(events, prefix)
			return newProfilePath(prefix)
		},
		writeRuntimeProfileSnapshotTemp: func(prefix, profileName string) (string, error) {
			events = append(events, prefix)
			return newProfilePath(prefix)
		},
		writeAllocsDeltaProfile: func(basePath, afterPath, outPath string) error {
			events = append(events, "alloc_delta")
			return nil
		},
		writeRuntimeProfileDeltaProfile: func(basePath, afterPath, outPath string) (bool, error) {
			events = append(events, filepath.Base(outPath))
			return true, nil
		},
	}

	fixture, _ := buildColumnStoreSyntheticFixture(8, 1)
	dir := t.TempDir()
	db, err := openColumnStoreSuiteDB(dir)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "events",
		Options: collections.CollectionOptions{
			DocumentFormat:               collections.DocumentFormatJSON,
			DisableIndexedWriteMemtables: true,
			ColumnStore:                  columnStoreSuiteConfig(),
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, fixture, 4); err != nil {
		t.Fatalf("insert fixture: %v", err)
	}
	rawHashes, err := columnStoreReferenceHashes(fixture)
	if err != nil {
		t.Fatalf("reference hashes: %v", err)
	}

	_, parity, parityErr, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{
		CPUProfile:           filepath.Join(profileTmpDir, "cpu"),
		AllocsProfile:        filepath.Join(profileTmpDir, "allocs"),
		AllocsProfileRate:    1,
		BlockProfile:         filepath.Join(profileTmpDir, "block.pprof"),
		MutexProfile:         filepath.Join(profileTmpDir, "mutex.pprof"),
		MutexProfileFraction: 1,
		profileHooks:         profileHooks,
	}, collection, len(fixture), rawHashes, columnStorePathRowStoreBaseline, collections.ColumnAssetReadIntegrityVerify, nil)
	if err != nil {
		t.Fatalf("profiled queries: %v", err)
	}
	if parityErr != nil {
		t.Fatalf("parity: %v", parityErr)
	}
	assertColumnStoreParityCoverageM11A(t, parity)

	indexOf := func(target string) int {
		for i, event := range events {
			if event == target {
				return i
			}
		}
		return -1
	}
	cpuStartIdx := indexOf("cpu_start")
	cpuStopIdx := indexOf("cpu_stop")
	if cpuStartIdx < 0 || cpuStopIdx < 0 {
		t.Fatalf("missing CPU profile events: %v", events)
	}
	for _, baseline := range []string{
		"unified_bench_column_store_allocs_base",
		"unified_bench_column_store_block_base",
		"unified_bench_column_store_mutex_base",
	} {
		idx := indexOf(baseline)
		if idx < 0 {
			t.Fatalf("missing baseline event %s: %v", baseline, events)
		}
		if idx > cpuStartIdx {
			t.Fatalf("expected %s before cpu_start: %v", baseline, events)
		}
	}
	for _, after := range []string{
		"unified_bench_column_store_allocs_after",
		"unified_bench_column_store_block_after",
		"unified_bench_column_store_mutex_after",
	} {
		idx := indexOf(after)
		if idx < 0 {
			t.Fatalf("missing after event %s: %v", after, events)
		}
		if cpuStopIdx > idx {
			t.Fatalf("expected cpu_stop before %s: %v", after, events)
		}
	}
}

func TestColumnStoreSuiteCheckpointCPUProfileUsesResolvedHooksM11A(t *testing.T) {
	var events []string
	profileHooks := benchmarkProfileHooks{
		startCPUProfile: func(_ io.Writer) error {
			events = append(events, "checkpoint_cpu_start")
			return nil
		},
		stopCPUProfile: func() {
			events = append(events, "checkpoint_cpu_stop")
		},
	}
	cfg := BenchConfig{
		CheckpointCPUProfile: filepath.Join(t.TempDir(), "checkpoint_cpu"),
	}

	f, err := startCheckpointCPUProfile(cfg, profileHooks, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	if err != nil {
		t.Fatalf("start checkpoint CPU profile: %v", err)
	}
	profileHooks.stopCPUProfile()
	if err := f.Close(); err != nil {
		t.Fatalf("close checkpoint CPU profile: %v", err)
	}

	if got, want := strings.Join(events, ","), "checkpoint_cpu_start,checkpoint_cpu_stop"; got != want {
		t.Fatalf("checkpoint CPU profile hooks = %s, want %s", got, want)
	}
}

func TestColumnStoreSuiteCheckpointCPUProfileStartFailureRemovesArtifactM11A(t *testing.T) {
	profileHooks := benchmarkProfileHooks{
		startCPUProfile: func(_ io.Writer) error {
			return errors.New("checkpoint start failed")
		},
	}
	cfg := BenchConfig{
		CheckpointCPUProfile: filepath.Join(t.TempDir(), "checkpoint_cpu"),
	}

	f, err := startCheckpointCPUProfile(cfg, profileHooks, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	if err == nil {
		if f != nil {
			_ = f.Close()
		}
		t.Fatal("expected checkpoint CPU profile start failure")
	}
	msg := err.Error()
	if !strings.Contains(msg, columnStoreSuiteBenchTestName) ||
		!strings.Contains(msg, columnStoreSuiteBenchDBName) ||
		!strings.Contains(msg, cfg.CheckpointCPUProfile) {
		t.Fatalf("error=%v, want checkpoint CPU start failure test/db/path context", err)
	}
	profilePath := fmt.Sprintf("%s_checkpoint_%s_%s.pprof", cfg.CheckpointCPUProfile, sanitizeProfileSegment(columnStoreSuiteBenchTestName), sanitizeProfileSegment(columnStoreSuiteBenchDBName))
	if _, statErr := os.Stat(profilePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected failed checkpoint CPU profile artifact to be removed, stat err=%v", statErr)
	}
}

func TestColumnStoreSuiteCheckpointCPUProfileNilStartHookReturnsErrorM11A(t *testing.T) {
	cfg := BenchConfig{
		CheckpointCPUProfile: filepath.Join(t.TempDir(), "checkpoint_cpu"),
	}

	f, err := startCheckpointCPUProfile(cfg, benchmarkProfileHooks{}, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	if err == nil {
		if f != nil {
			_ = f.Close()
		}
		t.Fatal("expected checkpoint CPU profile nil hook error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "start hook is nil") ||
		!strings.Contains(msg, columnStoreSuiteBenchTestName) ||
		!strings.Contains(msg, columnStoreSuiteBenchDBName) {
		t.Fatalf("error=%v, want nil hook test/db context", err)
	}
	profilePath := fmt.Sprintf("%s_checkpoint_%s_%s.pprof", cfg.CheckpointCPUProfile, sanitizeProfileSegment(columnStoreSuiteBenchTestName), sanitizeProfileSegment(columnStoreSuiteBenchDBName))
	if _, statErr := os.Stat(profilePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected failed checkpoint CPU profile artifact to be removed, stat err=%v", statErr)
	}
}

func TestColumnStoreSuiteCPUProfileStartFailureRemovesArtifactM11A(t *testing.T) {
	profileHooks := &benchmarkProfileHooks{
		startCPUProfile: func(_ io.Writer) error {
			return errors.New("start failed")
		},
	}

	collection, events, rawHashes := newColumnStoreSuiteTestCollectionM11A(t, 8, 4)
	profilePrefix := filepath.Join(t.TempDir(), "cpu")
	_, _, parityErr, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{
		CPUProfile:   profilePrefix,
		profileHooks: profileHooks,
	}, collection, len(events), rawHashes, columnStorePathRowStoreBaseline, collections.ColumnAssetReadIntegrityVerify, nil)
	if err == nil {
		t.Fatal("expected CPU profile start failure")
	}
	if parityErr != nil {
		t.Fatalf("hard CPU profile error returned as parity error: %v", parityErr)
	}
	profilePath := fmt.Sprintf("%s_%s_%s.pprof", profilePrefix, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	if _, statErr := os.Stat(profilePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected failed CPU profile artifact to be removed, stat err=%v", statErr)
	}
}

func TestColumnStoreSuiteRuntimeTraceStartFailureRemovesArtifactM11A(t *testing.T) {
	activeTracePath := filepath.Join(t.TempDir(), "active_trace.out")
	activeTrace, err := os.Create(activeTracePath)
	if err != nil {
		t.Fatalf("create active trace: %v", err)
	}
	if err := trace.Start(activeTrace); err != nil {
		_ = activeTrace.Close()
		t.Skipf("runtime trace unavailable: %v", err)
	}
	defer func() {
		trace.Stop()
		_ = activeTrace.Close()
	}()

	tracePath := filepath.Join(t.TempDir(), "trace.out")
	finish, err := startColumnStoreSuiteRuntimeProfiles(BenchConfig{TraceProfile: tracePath})
	if err == nil {
		if finish != nil {
			_ = finish()
		}
		t.Fatal("expected trace start failure while trace is already active")
	}
	if _, statErr := os.Stat(tracePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected failed trace artifact to be removed, stat err=%v", statErr)
	}
}

func TestColumnStoreSuiteHardQueryFailureRemovesCPUArtifactM11A(t *testing.T) {
	collection, events, rawHashes := newColumnStoreSuiteTestCollectionM11A(t, 4, 2)
	profileDir := t.TempDir()
	profilePrefix := filepath.Join(profileDir, "cpu")
	profileHooks := &benchmarkProfileHooks{
		startCPUProfile: func(_ io.Writer) error { return nil },
		stopCPUProfile:  func() {},
	}
	queries, parity, parityErr, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{
		CPUProfile:   profilePrefix,
		profileHooks: profileHooks,
	}, collection, len(events)+1, rawHashes, columnStorePathRowStoreBaseline, collections.ColumnAssetReadIntegrityVerify, nil)
	if err == nil {
		t.Fatal("expected hard query error")
	}
	if parityErr != nil {
		t.Fatalf("hard query error returned as parity error: %v", parityErr)
	}
	if queries != nil || parity != nil {
		t.Fatalf("expected no query/parity output on hard failure, queries=%v parity=%v", queries, parity)
	}
	profilePath := fmt.Sprintf("%s_%s_%s.pprof", profilePrefix, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	if _, statErr := os.Stat(profilePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected hard-failure CPU profile artifact to be removed, stat err=%v", statErr)
	}
}

func TestColumnStoreSuiteRuntimeDeltaSkipsEmptyOutputM11A(t *testing.T) {
	collection, events, rawHashes := newColumnStoreSuiteTestCollectionM11A(t, 8, 4)
	profileDir := t.TempDir()
	blockPath := filepath.Join(profileDir, "block.pprof")
	profileHooks := &benchmarkProfileHooks{
		writeRuntimeProfileSnapshotTemp: func(prefix, profileName string) (string, error) {
			path := filepath.Join(profileDir, prefix+".pprof")
			return path, os.WriteFile(path, []byte("snapshot"), 0o644)
		},
		writeRuntimeProfileDeltaProfile: func(basePath, afterPath, outPath string) (bool, error) {
			return false, nil
		},
	}
	_, _, _, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{
		BlockProfile: blockPath,
		profileHooks: profileHooks,
	}, collection, len(events), rawHashes, columnStorePathRowStoreBaseline, collections.ColumnAssetReadIntegrityVerify, nil)
	if err != nil {
		t.Fatalf("empty block delta output should be skipped: %v", err)
	}
	outPath := contentionProfilePath(blockPath, "block", columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	if _, statErr := os.Stat(outPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected empty block delta artifact to be omitted, stat err=%v", statErr)
	}
}

func TestColumnStoreSuiteRuntimeProfilesTrimPaddedPathsM11A(t *testing.T) {
	dir := t.TempDir()
	blockPath := filepath.Join(dir, "block.pprof")
	mutexPath := filepath.Join(dir, "mutex.pprof")
	tracePath := filepath.Join(dir, "trace.out")

	finish, err := startColumnStoreSuiteRuntimeProfiles(BenchConfig{
		BlockProfile:         " \t" + blockPath + "\t ",
		BlockProfileRate:     1,
		MutexProfile:         " \t" + mutexPath + "\t ",
		MutexProfileFraction: 1,
		TraceProfile:         " \t" + tracePath + "\t ",
	})
	if err != nil {
		t.Fatalf("startColumnStoreSuiteRuntimeProfiles: %v", err)
	}
	if err := finish(); err != nil {
		t.Fatalf("finish runtime profiles: %v", err)
	}
	for _, path := range []string{blockPath, mutexPath, tracePath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected trimmed profile path %q to exist: %v", path, err)
		}
	}
}

func TestColumnStoreSuiteProfiledQueriesSkipWhitespaceOnlyRuntimeProfilePathsM11A(t *testing.T) {
	collection, events, rawHashes := newColumnStoreSuiteTestCollectionM11A(t, 8, 4)
	profileHooks := &benchmarkProfileHooks{
		writeRuntimeProfileSnapshotTemp: func(prefix, profileName string) (string, error) {
			t.Fatalf("runtime profile snapshot should not run for whitespace-only %s path", profileName)
			return "", nil
		},
		writeRuntimeProfileDeltaProfile: func(basePath, afterPath, outPath string) (bool, error) {
			t.Fatalf("runtime profile delta should not run for whitespace-only path %q", outPath)
			return false, nil
		},
	}
	queries, parity, parityErr, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{
		BlockProfile: " \t ",
		MutexProfile: " \n ",
		profileHooks: profileHooks,
	}, collection, len(events), rawHashes, columnStorePathRowStoreBaseline, collections.ColumnAssetReadIntegrityVerify, nil)
	if err != nil || parityErr != nil {
		t.Fatalf("whitespace-only runtime profiles should be ignored: err=%v parityErr=%v", err, parityErr)
	}
	assertColumnStoreQueryMetricCoverageM11A(t, queries)
	assertColumnStoreParityCoverageM11A(t, parity)
}

func TestColumnStoreSuiteProfiledQueriesTrimAllocsDeltaPathM11A(t *testing.T) {
	collection, events, rawHashes := newColumnStoreSuiteTestCollectionM11A(t, 8, 4)
	dir := t.TempDir()
	allocsPrefix := filepath.Join(dir, "allocs")
	var gotOutPath string
	profileHooks := &benchmarkProfileHooks{
		writeAllocsSnapshotTemp: func(prefix string) (string, error) {
			path := filepath.Join(dir, prefix+".pprof")
			return path, os.WriteFile(path, []byte("snapshot"), 0o644)
		},
		writeAllocsDeltaProfile: func(basePath, afterPath, outPath string) error {
			gotOutPath = outPath
			return os.WriteFile(outPath, []byte("delta"), 0o644)
		},
	}
	queries, parity, parityErr, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{
		AllocsProfile:     " \t" + allocsPrefix + "\t ",
		AllocsProfileRate: 1,
		profileHooks:      profileHooks,
	}, collection, len(events), rawHashes, columnStorePathRowStoreBaseline, collections.ColumnAssetReadIntegrityVerify, nil)
	if err != nil || parityErr != nil {
		t.Fatalf("profiled queries failed: err=%v parityErr=%v", err, parityErr)
	}
	assertColumnStoreQueryMetricCoverageM11A(t, queries)
	assertColumnStoreParityCoverageM11A(t, parity)
	wantOutPath := fmt.Sprintf("%s_%s_%s.pprof", allocsPrefix, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	if gotOutPath != wantOutPath {
		t.Fatalf("allocs delta out path=%q want %q", gotOutPath, wantOutPath)
	}
	if _, err := os.Stat(wantOutPath); err != nil {
		t.Fatalf("expected trimmed allocs delta artifact: %v", err)
	}
}

func TestColumnStoreSuiteArtifactsTrimRuntimeProfilePathsM11A(t *testing.T) {
	dir := t.TempDir()
	cpuPath := filepath.Join(dir, "cpu")
	allocsPath := filepath.Join(dir, "allocs")
	checkpointPath := filepath.Join(dir, "checkpoint_cpu")
	blockPath := filepath.Join(dir, "block.pprof")
	mutexPath := filepath.Join(dir, "mutex.pprof")
	tracePath := filepath.Join(dir, "trace.out")

	paths := columnStoreArtifactPathsForProfileDir(dir, BenchConfig{
		CPUProfile:           " \t" + cpuPath + "\t ",
		AllocsProfile:        " \t" + allocsPath + "\t ",
		CheckpointCPUProfile: " \t" + checkpointPath + "\t ",
		BlockProfile:         " \t" + blockPath + "\t ",
		MutexProfile:         " \t" + mutexPath + "\t ",
		TraceProfile:         " \t" + tracePath + "\t ",
	}, true)
	if paths.CPUProfile != fmt.Sprintf("%s_%s_%s.pprof", cpuPath, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName) {
		t.Fatalf("cpu artifact path was not trimmed: %+v", paths)
	}
	if paths.AllocsProfile != fmt.Sprintf("%s_%s_%s.pprof", allocsPath, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName) {
		t.Fatalf("allocs artifact path was not trimmed: %+v", paths)
	}
	if paths.CheckpointCPUProfile != fmt.Sprintf("%s_checkpoint_%s_%s.pprof", checkpointPath, sanitizeProfileSegment(columnStoreSuiteBenchTestName), sanitizeProfileSegment(columnStoreSuiteBenchDBName)) {
		t.Fatalf("checkpoint artifact path was not trimmed: %+v", paths)
	}
	if paths.BlockProfile != blockPath || paths.MutexProfile != mutexPath || paths.TraceProfile != tracePath {
		t.Fatalf("runtime artifact paths were not trimmed: %+v", paths)
	}
	if paths.BlockDeltaProfile != contentionProfilePath(blockPath, "block", columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName) {
		t.Fatalf("block delta path=%q", paths.BlockDeltaProfile)
	}
	if paths.MutexDeltaProfile != contentionProfilePath(mutexPath, "mutex", columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName) {
		t.Fatalf("mutex delta path=%q", paths.MutexDeltaProfile)
	}
	if paths.InsightsMarkdown == "" || paths.InsightsJSON == "" || paths.InsightsHTML == "" {
		t.Fatalf("runBenchprof=true should advertise insights artifacts: %+v", paths)
	}
	withoutBenchprof := columnStoreArtifactPathsForProfileDir(dir, BenchConfig{}, false)
	if withoutBenchprof.InsightsMarkdown != "" || withoutBenchprof.InsightsJSON != "" || withoutBenchprof.InsightsHTML != "" {
		t.Fatalf("runBenchprof=false should not advertise insights artifacts: %+v", withoutBenchprof)
	}
}

func TestColumnStoreSuiteArtifactsOmitMissingRuntimeDeltaPathsM11A(t *testing.T) {
	dir := t.TempDir()
	blockDeltaPath := filepath.Join(dir, "block_delta.pprof")
	mutexDeltaPath := filepath.Join(dir, "mutex_delta.pprof")
	blockPath := filepath.Join(dir, "block.pprof")
	mutexPath := filepath.Join(dir, "mutex.pprof")
	if err := os.WriteFile(blockDeltaPath, []byte("delta"), 0o644); err != nil {
		t.Fatalf("write block delta: %v", err)
	}

	paths := columnStoreSuitePruneMissingRuntimeDeltaArtifacts(columnStoreArtifactPaths{
		BlockDeltaProfile: blockDeltaPath,
		MutexDeltaProfile: mutexDeltaPath,
		BlockProfile:      blockPath,
		MutexProfile:      mutexPath,
	})
	if paths.BlockDeltaProfile != blockDeltaPath {
		t.Fatalf("block delta path=%q want %q", paths.BlockDeltaProfile, blockDeltaPath)
	}
	if paths.MutexDeltaProfile != "" {
		t.Fatalf("missing mutex delta path should be omitted, got %q", paths.MutexDeltaProfile)
	}
	if paths.BlockProfile != blockPath || paths.MutexProfile != mutexPath {
		t.Fatalf("base runtime profile artifacts should remain advertised: %+v", paths)
	}
}

func TestColumnStoreSuiteArtifactsPreserveRuntimeDeltaStatErrorsM11A(t *testing.T) {
	dir := t.TempDir()
	notDir := filepath.Join(dir, "not-dir")
	if err := os.WriteFile(notDir, []byte("file"), 0o644); err != nil {
		t.Fatalf("write not-dir marker: %v", err)
	}
	blockDeltaPath := filepath.Join(notDir, "block_delta.pprof")
	paths := columnStoreSuitePruneMissingRuntimeDeltaArtifacts(columnStoreArtifactPaths{
		BlockDeltaProfile: blockDeltaPath,
		MutexDeltaProfile: " \t ",
	})
	if paths.BlockDeltaProfile != blockDeltaPath {
		t.Fatalf("stat-error optional delta path should be preserved, got %+v", paths)
	}
	if paths.MutexDeltaProfile != "" {
		t.Fatalf("blank optional delta path should be omitted: %+v", paths)
	}
}

func TestColumnStoreSuiteProfiledQueriesReturnHardErrorsSeparatelyM11A(t *testing.T) {
	collection, events, rawHashes := newColumnStoreSuiteTestCollectionM11A(t, 4, 2)
	queries, parity, parityErr, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{}, collection, len(events)+1, rawHashes, columnStorePathRowStoreBaseline, collections.ColumnAssetReadIntegrityVerify, nil)
	if err == nil {
		t.Fatal("expected hard query error")
	}
	if parityErr != nil {
		t.Fatalf("hard query error returned as parity error: %v", parityErr)
	}
	if queries != nil || parity != nil {
		t.Fatalf("expected no query/parity output on hard failure, queries=%v parity=%v", queries, parity)
	}
}

func TestColumnStoreQueryHashRejectsUnknownQueryM11A(t *testing.T) {
	_, _, err := columnStoreQueryHash("missing_query", nil)
	if err == nil {
		t.Fatal("expected unknown query to fail")
	}
	if !strings.Contains(err.Error(), "unknown column_store query") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreQueryHashCanonicalizesQ5MetadataAliasM11A(t *testing.T) {
	events := []columnStoreDecodedEvent{
		{TimeUS: 10, Kind: "create", Did: "did:example:a"},
		{TimeUS: 40, Kind: "update", Did: "did:example:a"},
		{TimeUS: 25, Kind: "create", Did: "did:example:b"},
		{TimeUS: 55, Kind: "delete", Did: "did:example:b"},
	}
	q5Hash, q5Count, err := columnStoreQueryHash("q5", events)
	if err != nil {
		t.Fatalf("q5 hash: %v", err)
	}
	aliasHash, aliasCount, err := columnStoreQueryHash("q5_metadata", events)
	if err != nil {
		t.Fatalf("q5_metadata hash: %v", err)
	}
	if q5Hash != aliasHash || q5Count != aliasCount {
		t.Fatalf("q5_metadata hash/count=(%016x,%d) want q5=(%016x,%d)", aliasHash, aliasCount, q5Hash, q5Count)
	}
}

func TestColumnStoreSuiteExecutesForcedSerialPhysicalPathM14B(t *testing.T) {
	dir := t.TempDir()
	cfg := BenchConfig{Keys: 16, BatchSize: 8, DBsArg: "treedb", Profile: "durable", SeedUsed: 1}
	_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ProfileDir:    dir,
		ExecutionPath: "native-fastpath",
		ForcedPath:    columnStorePathSerialColumnScan,
	})
	if err != nil {
		t.Fatalf("runColumnStoreSuite serial physical: %v", err)
	}

	var report columnStoreSuiteReport
	data, err := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
	if err != nil {
		t.Fatalf("read column_store_results.json: %v", err)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal column_store_results.json: %v", err)
	}
	if got, want := report.ForcedPath, columnStorePathSerialColumnScan; got != want {
		t.Fatalf("forced_path=%q want %q", got, want)
	}
	if !columnStoreTestStringSliceContains(report.AcceptedForcedPaths, columnStorePathSerialColumnScan) {
		t.Fatalf("serial physical path not reported as accepted: %+v", report.AcceptedForcedPaths)
	}
	if report.ByteAccounting.ColumnAssetBytes <= 0 {
		t.Fatalf("physical run did not report column assets: %+v", report.ByteAccounting)
	}
	assertColumnStoreJSONBenchCellShapeM1955(t, report, true)
	queryMetrics := assertColumnStoreQueryMetricCoverageM11A(t, report.Queries)
	for _, q := range report.Queries {
		if q.PlanLabel != columnStorePathSerialColumnScan {
			t.Fatalf("query %s plan_label=%q want %q", q.Name, q.PlanLabel, columnStorePathSerialColumnScan)
		}
		if q.RowsProcessed != report.Rows {
			t.Fatalf("query %s rows_processed=%d want %d", q.Name, q.RowsProcessed, report.Rows)
		}
		if q.RowMaterializations != 0 {
			t.Fatalf("query %s row_materializations=%d want zero physical row materialization", q.Name, q.RowMaterializations)
		}
		if q.BytesRead <= 0 || q.RowsPerSecond <= 0 || q.NsPerRow <= 0 {
			t.Fatalf("query %s missing physical throughput metrics: %+v", q.Name, q)
		}
		if q.StorageSource != string(collections.ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset) || q.FallbackReason != string(collections.ColumnPhysicalQueryFallbackNone) {
			t.Fatalf("query %s storage source/fallback=%q/%q want compatibility sidecar/none", q.Name, q.StorageSource, q.FallbackReason)
		}
		if q.ManifestRootName == "" || q.ManifestRoot == 0 || q.ManifestGeneration == 0 || q.ActiveManifestChecksum == 0 {
			t.Fatalf("query %s missing per-row manifest identity: %+v", q.Name, q)
		}
		if q.Name == columnStoreQueryQ1 || q.Name == columnStoreQueryQ2 {
			if q.DictionaryCodeHits == 0 {
				t.Fatalf("query %s dictionary_code_hits=%d want sidecar hits", q.Name, q.DictionaryCodeHits)
			}
			if !strings.Contains(q.ThroughputInterpretation, "dictionary-code sidecar serial path") {
				t.Fatalf("query %s throughput_interpretation=%q want dictionary sidecar classification", q.Name, q.ThroughputInterpretation)
			}
			continue
		}
		if q.Name == columnStoreQueryQ3 {
			if q.Int64ValueHits == 0 {
				t.Fatalf("query %s int64_value_hits=%d want sidecar hits", q.Name, q.Int64ValueHits)
			}
			if !strings.Contains(q.ThroughputInterpretation, "int64-value sidecar serial path") {
				t.Fatalf("query %s throughput_interpretation=%q want int64 sidecar classification", q.Name, q.ThroughputInterpretation)
			}
			continue
		}
		if q.Name == columnStoreQueryQ4A || q.Name == columnStoreQueryQ4B || q.Name == columnStoreQueryQ5 || q.Name == columnStoreQueryQ5Metadata {
			if q.DictionaryCodeHits == 0 || q.Int64ValueHits == 0 {
				t.Fatalf("query %s dictionary_code_hits=%d int64_value_hits=%d want dictionary+int64 sidecar hits", q.Name, q.DictionaryCodeHits, q.Int64ValueHits)
			}
			if !strings.Contains(q.ThroughputInterpretation, "dictionary-code and int64-value sidecar serial path") {
				t.Fatalf("query %s throughput_interpretation=%q want dictionary+int64 sidecar classification", q.Name, q.ThroughputInterpretation)
			}
			continue
		}
		if !strings.Contains(q.ThroughputInterpretation, "physical serial scan") {
			t.Fatalf("query %s throughput_interpretation=%q want serial physical classification", q.Name, q.ThroughputInterpretation)
		}
	}
	if got, want := queryMetrics[columnStoreQueryQ5Metadata].ProductionHash, queryMetrics[columnStoreQueryQ5].ProductionHash; got != want {
		t.Fatalf("q5_metadata production hash=%016x want q5 hash=%016x", got, want)
	}
	for name, parity := range report.Parity {
		if !parity.Pass {
			t.Fatalf("parity %s failed: %+v", name, parity)
		}
	}
}

func TestColumnStoreSuiteExecutesForcedAggregateAndParallelPhysicalPathsM14B(t *testing.T) {
	tests := []struct {
		name       string
		forcedPath string
		q5Plan     string
		wantWorker int
	}{
		{name: "aggregate metadata", forcedPath: columnStorePathAggregateMetadata, q5Plan: columnStorePathAggregateMetadata, wantWorker: 1},
		{name: "parallel", forcedPath: columnStorePathParallelColumnScan, q5Plan: columnStorePathParallelColumnScan, wantWorker: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := BenchConfig{Keys: 24, BatchSize: 6, DBsArg: "treedb", Profile: "durable", SeedUsed: 1}
			_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
				ProfileDir:    dir,
				ExecutionPath: "native-fastpath",
				ForcedPath:    tc.forcedPath,
			})
			if err != nil {
				t.Fatalf("runColumnStoreSuite %s: %v", tc.forcedPath, err)
			}

			var report columnStoreSuiteReport
			data, err := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
			if err != nil {
				t.Fatalf("read column_store_results.json: %v", err)
			}
			if err := json.Unmarshal(data, &report); err != nil {
				t.Fatalf("unmarshal column_store_results.json: %v", err)
			}
			if !columnStoreTestStringSliceContains(report.AcceptedForcedPaths, tc.forcedPath) {
				t.Fatalf("%s not reported as accepted: %+v", tc.forcedPath, report.AcceptedForcedPaths)
			}
			cellMetrics := assertColumnStoreJSONBenchCellShapeM1955(t, report, tc.forcedPath == columnStorePathAggregateMetadata)
			if tc.forcedPath == columnStorePathAggregateMetadata {
				q4bCells := cellMetrics[columnStoreQueryQ4B]
				if q4bCells[columnStoreJSONBenchCellColumnDirectMetadata+"/"+columnStoreJSONBenchModeDirect].MetadataDataScanPath != columnStoreJSONBenchScanPathMetadata {
					t.Fatalf("q4b missing direct metadata cell: %+v", q4bCells)
				}
				if q4bCells[columnStoreJSONBenchCellColumnPreparedMetadata+"/"+columnStoreJSONBenchModePrepared].MetadataDataScanPath != columnStoreJSONBenchScanPathMetadata {
					t.Fatalf("q4b missing prepared metadata cell: %+v", q4bCells)
				}
			}
			queryMetrics := assertColumnStoreQueryMetricCoverageM11A(t, report.Queries)
			for _, q := range report.Queries {
				metadataFastPath := q.Name == columnStoreQueryQ4B || q.Name == columnStoreQueryQ5Metadata
				if tc.forcedPath == columnStorePathAggregateMetadata && !metadataFastPath {
					if q.PlanLabel != columnStorePathSerialColumnScan {
						t.Fatalf("query %s plan_label=%q want %q under aggregate_metadata forced path", q.Name, q.PlanLabel, columnStorePathSerialColumnScan)
					}
					if !strings.Contains(q.ImplementationNote, "rerouted_to_serial_column_scan") {
						t.Fatalf("query %s missing aggregate reroute implementation note: %+v", q.Name, q)
					}
				}
				if q.RowMaterializations != 0 {
					t.Fatalf("query %s row_materializations=%d want zero physical row materialization", q.Name, q.RowMaterializations)
				}
				if q.RowsProcessed != report.Rows {
					t.Fatalf("query %s rows_processed=%d want %d", q.Name, q.RowsProcessed, report.Rows)
				}
				metadataHitAggregate := tc.forcedPath == columnStorePathAggregateMetadata && q.MetadataHits > 0
				if !metadataHitAggregate && q.BytesRead <= 0 {
					t.Fatalf("query %s bytes_read=%d want physical bytes", q.Name, q.BytesRead)
				}
				if q.ThroughputInterpretation == "" {
					t.Fatalf("query %s missing throughput_interpretation", q.Name)
				}
				if q.StorageSource == "" || q.FallbackReason == "" {
					t.Fatalf("query %s missing storage source/fallback diagnostics: %+v", q.Name, q)
				}
				if tc.forcedPath == columnStorePathAggregateMetadata {
					if q.MetadataHits > 0 {
						if q.StorageSource != string(collections.ColumnPhysicalQueryStorageSourceAggregateMetadata) || q.FallbackReason != string(collections.ColumnPhysicalQueryFallbackNone) {
							t.Fatalf("query %s metadata storage/fallback=%q/%q want aggregate metadata/none", q.Name, q.StorageSource, q.FallbackReason)
						}
					} else if q.StorageSource != string(collections.ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset) || q.FallbackReason != string(collections.ColumnPhysicalQueryFallbackAggregateMetadataUnsupported) {
						t.Fatalf("query %s aggregate reroute storage/fallback=%q/%q want compatibility sidecar/aggregate unsupported", q.Name, q.StorageSource, q.FallbackReason)
					}
				}
				if tc.forcedPath == columnStorePathParallelColumnScan {
					if q.StorageSource != string(collections.ColumnPhysicalQueryStorageSourceTypedRowAsset) || q.FallbackReason != string(collections.ColumnPhysicalQueryFallbackNone) {
						t.Fatalf("query %s parallel storage/fallback=%q/%q want typed-row asset/none", q.Name, q.StorageSource, q.FallbackReason)
					}
				}
				if tc.forcedPath == columnStorePathParallelColumnScan && q.WorkerCount != tc.wantWorker {
					t.Fatalf("query %s worker_count=%d want %d for parallel path", q.Name, q.WorkerCount, tc.wantWorker)
				}
				if tc.forcedPath == columnStorePathParallelColumnScan && !strings.Contains(q.ThroughputInterpretation, "parallel physical scan") {
					t.Fatalf("query %s throughput_interpretation=%q want parallel physical classification", q.Name, q.ThroughputInterpretation)
				}
			}
			if tc.forcedPath == columnStorePathAggregateMetadata {
				q4b := queryMetrics[columnStoreQueryQ4B]
				if q4b.PlanLabel != columnStorePathAggregateMetadata {
					t.Fatalf("q4b plan_label=%q want %q", q4b.PlanLabel, columnStorePathAggregateMetadata)
				}
				if q4b.MetadataHits == 0 {
					t.Fatalf("q4b metadata_hits=0 want aggregate metadata fast path: %+v", q4b)
				}
				if !strings.Contains(q4b.ThroughputInterpretation, "metadata-bound") {
					t.Fatalf("q4b throughput_interpretation=%q want metadata-bound aggregate classification", q4b.ThroughputInterpretation)
				}
				interpretation := queryMetrics[columnStoreQueryQ5Metadata].ThroughputInterpretation
				if queryMetrics[columnStoreQueryQ5Metadata].MetadataHits > 0 {
					if !strings.Contains(interpretation, "metadata-bound") {
						t.Fatalf("q5_metadata throughput_interpretation=%q want metadata-bound aggregate classification", interpretation)
					}
				} else if !strings.Contains(interpretation, "fallback-bound") {
					t.Fatalf("q5_metadata throughput_interpretation=%q want aggregate fallback classification", interpretation)
				}
			}
			if got := queryMetrics[columnStoreQueryQ5Metadata].PlanLabel; got != tc.q5Plan {
				t.Fatalf("q5_metadata plan_label=%q want %q", got, tc.q5Plan)
			}
			if got, want := queryMetrics[columnStoreQueryQ5Metadata].ProductionHash, queryMetrics[columnStoreQueryQ5].ProductionHash; got != want {
				t.Fatalf("q5_metadata production hash=%016x want q5 hash=%016x", got, want)
			}
			for name, parity := range report.Parity {
				if !parity.Pass {
					t.Fatalf("parity %s failed: %+v", name, parity)
				}
			}
		})
	}
}

func TestColumnStoreSuiteRejectsOraclePathLabelM11B(t *testing.T) {
	_, err := runColumnStoreSuite(BenchConfig{Keys: 8, BatchSize: 4, DBsArg: "treedb", Profile: "durable", SeedUsed: 1}, columnStoreSuiteOptions{
		ProfileDir:    t.TempDir(),
		ExecutionPath: "oracle",
		ForcedPath:    columnStorePathRowStoreBaseline,
	})
	if err == nil {
		t.Fatal("expected oracle path-label to fail closed")
	}
	if !strings.Contains(err.Error(), "requires -path-label native-fastpath") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreJSONBenchPreparedParityMismatchIsFatal1955(t *testing.T) {
	cell := columnStoreJSONBenchCell{
		Query:               columnStoreQueryQ4B,
		PlanLabel:           columnStorePathSerialColumnScan,
		ExecutionMode:       columnStoreJSONBenchModePrepared,
		CompatibilityStatus: "available",
		RawHash:             0x1111,
		ResultHash:          0x2222,
		ParityWithRowScan:   false,
	}
	err := columnStoreValidatePreparedJSONBenchCellParity(cell)
	if err == nil || !strings.Contains(err.Error(), "prepared JSONBench cell") || !strings.Contains(err.Error(), "parity mismatch") {
		t.Fatalf("expected prepared parity mismatch error, got %v", err)
	}
}

func TestColumnStoreJSONBenchCellFromQueryMetricUsesDirectDiagnostics1955(t *testing.T) {
	q := columnStoreQueryMetric{
		Name:               "q4a",
		PlanLabel:          columnStorePathSerialColumnScan,
		StorageSource:      string(collections.ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset),
		FallbackReason:     string(collections.ColumnPhysicalQueryFallbackNone),
		Rows:               7,
		RowsProcessed:      5,
		RowsProcessedKnown: true,
		ResultCount:        2,
		RawHash:            11,
		ProductionHash:     11,
		ScanDurationMS:     1.5,
		ReduceDurationMS:   2.5,
		AdapterDurationMS:  0.75,
		hotRunDuration:     6 * time.Millisecond,
		RowsScanned:        13,
		RowsMatched:        3,
		ReduceRows:         2,
		DecodedGranules:    4,
		CompressionAttribution: columnStoreCompressionAttribution{
			CompressionPolicyLabel: "default",
			RequestedCompression:   "snappy",
			ActualCompression:      "snappy",
		},
	}

	cell := columnStoreJSONBenchCellFromQueryMetric(q, &collections.ColumnStoreConfig{}, 0)
	if got, want := cell.HotRunDurationMS, 6.0; got != want {
		t.Fatalf("hot_run_duration_ms=%v want %v", got, want)
	}
	if got, want := cell.RowsScanned, q.RowsScanned; got != want {
		t.Fatalf("rows_scanned=%d want %d", got, want)
	}
	if got, want := cell.RowsMatched, q.RowsMatched; got != want {
		t.Fatalf("rows_matched=%d want %d", got, want)
	}
	if got, want := cell.ReduceRows, q.ReduceRows; got != want {
		t.Fatalf("reduce_rows=%d want %d", got, want)
	}
	if got, want := cell.DecodedGranules, q.DecodedGranules; got != want {
		t.Fatalf("decoded_granules=%d want %d", got, want)
	}
}

func TestColumnStorePhaseDurationsUsesMeasuredHotRun1955(t *testing.T) {
	diag := collections.ColumnPhysicalQueryDiagnostics{
		ScanNanos:        int64(9 * time.Millisecond),
		ReduceNanos:      int64(2 * time.Millisecond),
		ResultShapeNanos: int64(3 * time.Millisecond),
	}
	scan, reduce, resultShape := columnStorePhaseDurations(10*time.Millisecond, diag)
	if got, want := scan, 5*time.Millisecond; got != want {
		t.Fatalf("scan duration=%v want %v", got, want)
	}
	if got, want := reduce, 2*time.Millisecond; got != want {
		t.Fatalf("reduce duration=%v want %v", got, want)
	}
	if got, want := resultShape, 3*time.Millisecond; got != want {
		t.Fatalf("result shape duration=%v want %v", got, want)
	}

	scan, reduce, resultShape = columnStorePhaseDurations(4*time.Millisecond, diag)
	if scan != 0 {
		t.Fatalf("scan duration must clamp at zero when subphases exceed total: %v", scan)
	}
	if got, want := reduce, 2*time.Millisecond; got != want {
		t.Fatalf("reduce duration=%v want %v", got, want)
	}
	if got, want := resultShape, 3*time.Millisecond; got != want {
		t.Fatalf("result shape duration=%v want %v", got, want)
	}
}

func TestColumnStoreJSONBenchUnavailablePreparedCellDoesNotClaimPreparedHash1955(t *testing.T) {
	direct := columnStoreQueryMetric{
		Name:           columnStoreQueryQ4B,
		PlanLabel:      columnStorePathAggregateMetadata,
		AliasOf:        columnStoreQueryAliasOf(columnStoreQueryQ4B, columnStorePathAggregateMetadata),
		StorageSource:  string(collections.ColumnPhysicalQueryStorageSourceAggregateMetadata),
		FallbackReason: string(collections.ColumnPhysicalQueryFallbackNone),
		Rows:           9,
		RawHash:        0x1111,
		ProductionHash: 0x1111,
		CompressionAttribution: columnStoreCompressionAttribution{
			CompressionPolicyLabel: columnStoreCompressionPolicyDefault,
			RequestedCompression:   columnStoreCompressionNoneLabel,
			ActualCompression:      columnStoreCompressionNoneLabel,
		},
	}

	exec := columnStoreQueryExecution{
		StorageSource:  string(collections.ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset),
		FallbackReason: string(collections.ColumnPhysicalQueryFallbackAggregateMetadataUnsupported),
		SetupDuration:  3 * time.Millisecond,
	}
	cell := columnStoreJSONBenchUnavailablePreparedCell(columnStoreQueryQ4B, direct, exec, &collections.ColumnStoreConfig{}, 0, errors.New("prepare unavailable"))
	if cell.CompatibilityStatus != "unavailable" || !strings.Contains(cell.CompatibilityStatusReason, "prepare unavailable") {
		t.Fatalf("unexpected unavailable status: %+v", cell)
	}
	if cell.CellLabel != columnStoreJSONBenchCellColumnPrepared || cell.StorageSource != exec.StorageSource || cell.FallbackReason != exec.FallbackReason || cell.MetadataDataScanPath != columnStoreJSONBenchScanPathData || cell.PreparedSetupDurationMS <= 0 {
		t.Fatalf("unavailable prepared cell did not preserve actual fallback diagnostics: %+v", cell)
	}
	if cell.RawHash != direct.RawHash {
		t.Fatalf("raw_hash=%016x want %016x", cell.RawHash, direct.RawHash)
	}
	if cell.ResultHash != 0 || cell.ParityWithRowScan {
		t.Fatalf("unavailable prepared cell must not claim prepared result hash/parity: %+v", cell)
	}
}

func TestColumnStoreSuiteRejectsInvalidKeysAndBatchSizeM11A(t *testing.T) {
	tests := []struct {
		name    string
		cfg     BenchConfig
		wantErr string
	}{
		{
			name:    "zero keys",
			cfg:     BenchConfig{Keys: 0, BatchSize: 4, DBsArg: "treedb", Profile: "durable", SeedUsed: 1},
			wantErr: "invalid keys: 0",
		},
		{
			name:    "negative keys",
			cfg:     BenchConfig{Keys: -1, BatchSize: 4, DBsArg: "treedb", Profile: "durable", SeedUsed: 1},
			wantErr: "invalid keys: -1",
		},
		{
			name:    "zero batch",
			cfg:     BenchConfig{Keys: 4, BatchSize: 0, DBsArg: "treedb", Profile: "durable", SeedUsed: 1},
			wantErr: "invalid batchsize: 0",
		},
		{
			name:    "negative batch",
			cfg:     BenchConfig{Keys: 4, BatchSize: -1, DBsArg: "treedb", Profile: "durable", SeedUsed: 1},
			wantErr: "invalid batchsize: -1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runColumnStoreSuite(tc.cfg, columnStoreSuiteOptions{
				ForcedPath: columnStorePathRowStoreBaseline,
			})
			if err == nil {
				t.Fatal("expected invalid column_store config to fail")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("unexpected error %q, want substring %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestColumnStoreSuitePlanKindMapsKnownPathsM11B(t *testing.T) {
	cases := []struct {
		path string
		want collections.ColumnQueryPlanKind
	}{
		{columnStorePathRowStoreBaseline, collections.ColumnQueryPlanRowStoreBaseline},
		{columnStorePathBTreeIndexBaseline, collections.ColumnQueryPlanBTreeIndexBaseline},
		{columnStorePathSerialColumnScan, collections.ColumnQueryPlanSerialColumnScan},
		{columnStorePathAggregateMetadata, collections.ColumnQueryPlanAggregateMetadata},
		{columnStorePathParallelColumnScan, collections.ColumnQueryPlanParallelColumnScan},
	}
	for _, tc := range cases {
		if tc.path != string(tc.want) {
			t.Fatalf("path constant %q diverged from planner kind %q", tc.path, tc.want)
		}
		got, err := columnStoreSuitePlanKind(tc.path)
		if err != nil {
			t.Fatalf("columnStoreSuitePlanKind(%q): %v", tc.path, err)
		}
		if got != tc.want {
			t.Fatalf("columnStoreSuitePlanKind(%q)=%q want %q", tc.path, got, tc.want)
		}
	}
	if _, err := columnStoreSuitePlanKind("future_alias"); err == nil {
		t.Fatal("expected unknown path to fail")
	} else {
		msg := err.Error()
		for _, want := range []string{
			"future_alias",
			"accepted=",
			"aliases=",
			"serial-column-scan",
			"aggregate-metadata",
			"fail_closed=",
			"-column-store-path",
		} {
			if !strings.Contains(msg, want) {
				t.Fatalf("unknown path error %q missing %q", msg, want)
			}
		}
	}
}

func TestColumnStoreSuitePathFlagDocumentsAliasesM11B(t *testing.T) {
	f := flag.Lookup("column-store-path")
	if f == nil {
		t.Fatal("missing column-store-path flag")
	}
	for _, want := range []string{"aliases:", "executable:", "row-store-baseline", "b-tree-index-baseline", "serial-column-scan", "aggregate-metadata", "parallel-column-scan"} {
		if !strings.Contains(f.Usage, want) {
			t.Fatalf("column-store-path help missing %q:\n%s", want, f.Usage)
		}
	}
}

func TestColumnStoreSuiteRunsBTreeIndexBaselineM11B(t *testing.T) {
	dir := t.TempDir()
	cfg := BenchConfig{
		Keys:      64,
		BatchSize: 16,
		DBsArg:    "treedb",
		Profile:   "durable",
		Progress:  false,
		SeedUsed:  1,
	}

	_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ProfileDir:    dir,
		ExecutionPath: "native-fastpath",
		ForcedPath:    columnStorePathBTreeIndexBaseline,
		RunBenchprof:  false,
	})
	if err != nil {
		t.Fatalf("runColumnStoreSuite btree baseline: %v", err)
	}

	var report columnStoreSuiteReport
	data, err := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
	if err != nil {
		t.Fatalf("read column_store_results.json: %v", err)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal column_store_results.json: %v", err)
	}
	queryMetrics := assertColumnStoreQueryMetricCoverageM11A(t, report.Queries)
	for _, q := range report.Queries {
		if q.PlanLabel != columnStorePathBTreeIndexBaseline {
			t.Fatalf("query %s plan_label=%q want %q", q.Name, q.PlanLabel, columnStorePathBTreeIndexBaseline)
		}
		if q.PlannerReason == "" || q.PlannerCandidates == 0 {
			t.Fatalf("query %s missing planner diagnostics: %+v", q.Name, q)
		}
		if q.WorkerCount != 1 {
			t.Fatalf("query %s worker_count=%d want 1 for caller-thread B-tree baseline", q.Name, q.WorkerCount)
		}
		if !strings.Contains(q.PlannerReason, "full-scan B-tree baseline") {
			t.Fatalf("query %s planner_reason=%q want full-scan baseline disclosure", q.Name, q.PlannerReason)
		}
		if q.RowMaterializations != report.Rows {
			t.Fatalf("query %s row_materializations=%d want %d", q.Name, q.RowMaterializations, report.Rows)
		}
		if q.Name != "q5_metadata" && !strings.Contains(q.ImplementationNote, "no_predicate_pushdown") {
			t.Fatalf("query %s missing B-tree baseline implementation note: %+v", q.Name, q)
		}
	}
	if q := queryMetrics["q5_metadata"]; q.AliasOf != "q5" || !strings.Contains(q.ImplementationNote, "no_predicate_pushdown") {
		t.Fatalf("q5_metadata should report both q5 aliasing and B-tree baseline scan semantics: %+v", q)
	}
	if got, want := queryMetrics["q5_metadata"].ProductionHash, queryMetrics["q5"].ProductionHash; got != want {
		t.Fatalf("q5_metadata production hash=%016x want q5 hash=%016x", got, want)
	}
	for name, parity := range report.Parity {
		if !parity.Pass {
			t.Fatalf("parity %s failed: %+v", name, parity)
		}
	}
}

func TestColumnStoreSuiteQueriesNormalizeForcedPathAliasesM11B(t *testing.T) {
	const rows = 16
	events, _ := buildColumnStoreSyntheticFixture(rows, 1)
	dir := t.TempDir()
	db, err := openColumnStoreSuiteDB(dir)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(columnStoreSuiteCollectionMeta(columnStorePathRowStoreBaseline)); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, events, 8); err != nil {
		t.Fatalf("insert fixture: %v", err)
	}

	rawHashes, err := columnStoreReferenceHashes(events)
	if err != nil {
		t.Fatalf("reference hashes: %v", err)
	}
	queries, parity, err := runColumnStoreSuiteQueries(collection, rows, rawHashes, "row", collections.ColumnAssetReadIntegrityVerify, nil)
	if err != nil {
		t.Fatalf("runColumnStoreSuiteQueries row alias: %v", err)
	}
	if len(queries) == 0 || len(parity) == 0 {
		t.Fatalf("queries=%d parity=%d want non-empty", len(queries), len(parity))
	}
	for _, q := range queries {
		if q.PlanLabel != columnStorePathRowStoreBaseline {
			t.Fatalf("query %s plan_label=%q want %q", q.Name, q.PlanLabel, columnStorePathRowStoreBaseline)
		}
		if q.WorkerCount != 1 {
			t.Fatalf("query %s worker_count=%d want 1 for caller-thread row baseline", q.Name, q.WorkerCount)
		}
		if q.ThroughputInterpretation != "" {
			t.Fatalf("query %s raw query loop throughput_interpretation=%q want empty until report/artifact rendering", q.Name, q.ThroughputInterpretation)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint before physical aliases: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before physical aliases: %v", err)
	}
	db, err = openColumnStoreSuiteDB(dir)
	if err != nil {
		t.Fatalf("reopen before physical aliases: %v", err)
	}
	manager = collections.NewCollectionManager(db)
	collection, err = manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("reopen collection before physical aliases: %v", err)
	}

	physicalAliases := map[string]collections.ColumnQueryPlanKind{
		"serial":               collections.ColumnQueryPlanSerialColumnScan,
		"serial-column-scan":   collections.ColumnQueryPlanSerialColumnScan,
		"metadata":             collections.ColumnQueryPlanAggregateMetadata,
		"aggregate-metadata":   collections.ColumnQueryPlanAggregateMetadata,
		"parallel":             collections.ColumnQueryPlanParallelColumnScan,
		"parallel-column-scan": collections.ColumnQueryPlanParallelColumnScan,
	}
	for alias, want := range physicalAliases {
		normalized := normalizeColumnStoreSuitePath(alias)
		got, err := columnStoreSuitePlanKind(normalized)
		if err != nil {
			t.Fatalf("columnStoreSuitePlanKind(%q): %v", alias, err)
		}
		if got != want {
			t.Fatalf("columnStoreSuitePlanKind(%q)=%q want %q", alias, got, want)
		}
		queries, parity, err := runColumnStoreSuiteQueries(collection, rows, rawHashes, alias, collections.ColumnAssetReadIntegrityVerify, nil)
		if err != nil {
			t.Fatalf("physical alias %q: %v", alias, err)
		}
		if len(queries) != len(columnStoreQueryNameList) || len(parity) != len(columnStoreQueryNameList) {
			t.Fatalf("physical alias %q queries=%d parity=%d want %d", alias, len(queries), len(parity), len(columnStoreQueryNameList))
		}
		for name, result := range parity {
			if !result.Pass {
				t.Fatalf("physical alias %q parity %s failed: %+v", alias, name, result)
			}
		}
	}
}

func TestColumnStoreSuitePhysicalPathFailsClosedOnMissingAssetsM14B(t *testing.T) {
	const rows = 16
	events, _ := buildColumnStoreSyntheticFixture(rows, 1)
	dir := t.TempDir()
	db, err := openColumnStoreSuiteDB(dir)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(columnStoreSuiteCollectionMeta(columnStorePathSerialColumnScan)); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, events, 8); err != nil {
		t.Fatalf("insert fixture: %v", err)
	}
	rawHashes, err := columnStoreReferenceHashes(events)
	if err != nil {
		t.Fatalf("reference hashes: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before recovery-authoritative reopen: %v", err)
	}
	db, err = openColumnStoreSuiteDB(dir)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	manager = collections.NewCollectionManager(db)
	collection, err = manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("reopen collection: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before asset removal: %v", err)
	}
	db = nil
	if err := os.RemoveAll(backenddb.ColumnAssetRootDirPath(dir)); err != nil {
		t.Fatalf("remove column asset root: %v", err)
	}
	db, err = openColumnStoreSuiteDB(dir)
	if err != nil {
		t.Fatalf("reopen after asset removal: %v", err)
	}
	manager = collections.NewCollectionManager(db)
	collection, err = manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("reopen collection after asset removal: %v", err)
	}
	_, _, err = runColumnStoreSuiteQueries(collection, rows, rawHashes, columnStorePathSerialColumnScan, collections.ColumnAssetReadIntegrityVerify, nil)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing physical assets err=%v want os.ErrNotExist without row fallback", err)
	}
}

func TestColumnStoreSuiteQ3ReferenceUsesPhysicalHourSemanticsM14B(t *testing.T) {
	events := []columnStoreDecodedEvent{
		{TimeUS: -1},
		{TimeUS: -3_600_000_000},
		{TimeUS: -3_600_000_001},
		{TimeUS: 0},
	}
	lines, err := columnStoreQueryLines(columnStoreQueryQ3, events)
	if err != nil {
		t.Fatalf("columnStoreQueryLines q3: %v", err)
	}
	got := strings.Join(lines, "\n")
	for _, want := range []string{
		"q3:hour_23=2",
		"q3:hour_22=1",
		"q3:hour_00=1",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("q3 lines missing %q: %v", want, lines)
		}
	}
	if strings.Contains(got, "hour_-") {
		t.Fatalf("q3 lines used truncating negative hour bucket: %v", lines)
	}
}

func TestColumnStoreSuitePhysicalQueryLinesFailsClosedOnUnknownMappingM14B(t *testing.T) {
	if _, err := columnStoreSuitePhysicalQueryLines("future", "future_query", nil); err == nil {
		t.Fatal("expected unknown physical query line mapping to fail")
	} else if !strings.Contains(err.Error(), "future_query") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreSuiteIndexCandidateCoverageM11B(t *testing.T) {
	for _, name := range columnStoreQueryNames() {
		if got := columnStoreSuiteQueryIndexCandidates(name); len(got) == 0 {
			t.Fatalf("query %s has no B-tree baseline candidate columns", name)
		}
	}
}

func TestColumnStoreSuiteBTreeIndexScanMaterializesOneEntryPerDocumentM11B(t *testing.T) {
	const rows = 2048
	events, _ := buildColumnStoreSyntheticFixture(rows, 1)
	db, err := openColumnStoreSuiteDB(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(columnStoreSuiteCollectionMeta(columnStorePathBTreeIndexBaseline)); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, events, 256); err != nil {
		t.Fatalf("insert fixture: %v", err)
	}

	decoded, materialized, _, err := scanColumnStoreSuiteEventsByIndex(collection, rows, "q1", "kind_idx")
	if err != nil {
		t.Fatalf("scanColumnStoreSuiteEventsByIndex: %v", err)
	}
	if materialized != rows || len(decoded) != rows {
		t.Fatalf("index scan materialized=%d decoded=%d want %d", materialized, len(decoded), rows)
	}
}

func TestColumnStoreSuiteReportsParityMismatchM11A(t *testing.T) {
	dir := t.TempDir()
	cfg := BenchConfig{Keys: 16, BatchSize: 8, DBsArg: "treedb", Profile: "durable", SeedUsed: 1}
	_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ProfileDir:              dir,
		ExecutionPath:           "native-fastpath",
		ForcedPath:              columnStorePathRowStoreBaseline,
		CorruptReferenceForTest: "q1",
	})
	if err == nil {
		t.Fatal("expected parity mismatch")
	}
	if !strings.Contains(err.Error(), "parity mismatch") || !strings.Contains(err.Error(), "q1") {
		t.Fatalf("unexpected parity error: %v", err)
	}

	var report columnStoreSuiteReport
	data, readErr := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
	if readErr != nil {
		t.Fatalf("expected column_store_results.json after mismatch: %v", readErr)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	assertColumnStoreParityCoverageM11A(t, report.Parity)
	q1, ok := report.Parity["q1"]
	if !ok {
		t.Fatal("missing q1 parity entry")
	}
	if q1.Pass {
		t.Fatalf("expected q1 parity to be recorded as failed: %+v", q1)
	}
}

func TestColumnStoreSuiteReportsPreparedCellParityMismatchAfterArtifacts1955(t *testing.T) {
	dir := t.TempDir()
	cfg := BenchConfig{Keys: 16, BatchSize: 8, DBsArg: "treedb", Profile: "durable", SeedUsed: 1}
	_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ProfileDir:              dir,
		ExecutionPath:           "native-fastpath",
		ForcedPath:              columnStorePathSerialColumnScan,
		QueryNames:              []string{columnStoreQueryQ1},
		CorruptReferenceForTest: columnStoreQueryQ1,
	})
	if err == nil || !strings.Contains(err.Error(), "prepared JSONBench cell") || !strings.Contains(err.Error(), "parity mismatch") {
		t.Fatalf("expected prepared parity mismatch error, got %v", err)
	}

	var report columnStoreSuiteReport
	data, readErr := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
	if readErr != nil {
		t.Fatalf("expected column_store_results.json after prepared mismatch: %v", readErr)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	var prepared *columnStoreJSONBenchCell
	for i := range report.JSONBenchCells {
		cell := &report.JSONBenchCells[i]
		if cell.Query == columnStoreQueryQ1 && cell.ExecutionMode == columnStoreJSONBenchModePrepared {
			prepared = cell
			break
		}
	}
	if prepared == nil {
		t.Fatalf("missing prepared q1 JSONBench cell: %+v", report.JSONBenchCells)
	}
	if prepared.ParityWithRowScan || prepared.RawHash == prepared.ResultHash || prepared.ResultHash == 0 {
		t.Fatalf("prepared mismatch cell did not preserve failing hashes: %+v", *prepared)
	}
}

func TestColumnStoreSuiteRejectsUnknownCorruptReferenceM11A(t *testing.T) {
	cfg := BenchConfig{Keys: 8, BatchSize: 4, DBsArg: "treedb", Profile: "durable", SeedUsed: 1}
	_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ForcedPath:              columnStorePathRowStoreBaseline,
		CorruptReferenceForTest: "missing_query",
	})
	if err == nil {
		t.Fatal("expected unknown corrupt reference query to fail")
	}
	if !strings.Contains(err.Error(), "unknown corrupt reference query") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreSuiteRejectsMixedDBSelectionM11A(t *testing.T) {
	err := validateColumnStoreSuiteDBSelection("treedb,leveldb", "")
	if err == nil {
		t.Fatal("expected mixed DB selection to fail")
	}
	if !strings.Contains(err.Error(), "only supports TreeDB") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreSuiteAppliesDBExclusionsBeforeMixedCheckM11A(t *testing.T) {
	if err := validateColumnStoreSuiteDBSelection("treedb,leveldb", "leveldb"); err != nil {
		t.Fatalf("expected excluded non-TreeDB selection to pass: %v", err)
	}
	if err := validateColumnStoreSuiteDBSelection("treedbcached", ""); err != nil {
		t.Fatalf("expected TreeDB alias selection to pass: %v", err)
	}
	if err := validateColumnStoreSuiteDBSelection("treedb,leveldb", "leveldb,treedbcached"); err == nil || !strings.Contains(err.Error(), "excludes it") {
		t.Fatalf("expected TreeDB alias exclusion to reject suite, got %v", err)
	}
	err := validateColumnStoreSuiteDBSelection("leveldb", "leveldb")
	if err == nil {
		t.Fatal("expected all selected DBs excluded to fail")
	}
	if !strings.Contains(err.Error(), "requires TreeDB") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreSuiteRejectsExcludedTreeDBM11A(t *testing.T) {
	err := validateColumnStoreSuiteDBSelection("all", "treedb")
	if err == nil {
		t.Fatal("expected TreeDB exclusion to fail")
	}
	if !strings.Contains(err.Error(), "excludes it") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreSuiteReportsKeptDataDirM11A(t *testing.T) {
	dir := t.TempDir()
	cfg := BenchConfig{
		Keys:      8,
		BatchSize: 4,
		DBsArg:    "treedb",
		Profile:   "durable",
		Progress:  false,
		SeedUsed:  1,
		KeepDir:   true,
	}
	out, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ProfileDir:    dir,
		ExecutionPath: "native-fastpath",
		ForcedPath:    columnStorePathRowStoreBaseline,
	})
	if err != nil {
		t.Fatalf("runColumnStoreSuite: %v", err)
	}

	var report columnStoreSuiteReport
	data, err := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
	if err != nil {
		t.Fatalf("read column_store_results.json: %v", err)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	if report.DataDir == "" {
		t.Fatalf("expected kept data dir in report: %+v", report)
	}
	if _, err := os.Stat(report.DataDir); err != nil {
		t.Fatalf("expected kept data dir to exist: %v", err)
	}
	if !strings.Contains(out, "data-dir") {
		t.Fatalf("markdown output missing kept data-dir:\n%s", out)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(report.DataDir)
	})
}

func columnStoreTestStringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func columnStoreTestCommaListContains(values string, want string) bool {
	for _, value := range strings.Split(values, ",") {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if value == want {
			return true
		}
	}
	return false
}

func TestColumnStoreSuiteConfigUsesExplicitAggregateMetadataNamesM11A(t *testing.T) {
	cfg := columnStoreSuiteConfig()
	var names []string
	for _, agg := range cfg.AggregateMetadata {
		names = append(names, agg.Name)
	}
	got := strings.Join(names, ",")
	if !strings.Contains(got, columnStoreSuiteQ5AggregateMin) || !strings.Contains(got, columnStoreSuiteQ5AggregateMax) {
		t.Fatalf("aggregate metadata names=%q", got)
	}
	if strings.Contains(got, "q5_did_time_span,") || strings.HasSuffix(got, "q5_did_time_span") {
		t.Fatalf("aggregate metadata min name is ambiguous: %q", got)
	}
}

func TestColumnStoreSuiteTypedBenchmarkFlagsPublishTypedColumnPartOwnersM1952(t *testing.T) {
	withColumnStoreTypedBenchmarkPolicyFlags(t, "snappy", "raw_int64", 16)
	cfg := columnStoreSuiteConfig()
	if got, want := cfg.ProfileSupport, collections.ColumnStoreProfileBenchmarkRelaxed; got != want {
		t.Fatalf("profile_support=%q want %q", got, want)
	}
	for _, col := range cfg.Columns {
		if col.Owner != collections.TypedStorageOwnerColumnPart {
			t.Fatalf("column %q owner=%q want %q", col.Name, col.Owner, collections.TypedStorageOwnerColumnPart)
		}
	}
}

func TestRunColumnStoreSuiteTypedCompressionSurfacesTypedColumnPartCodecRowsM1952(t *testing.T) {
	withColumnStoreTypedBenchmarkPolicyFlags(t, "snappy", "raw_int64", 16)
	dir := t.TempDir()
	cfg := BenchConfig{Keys: 64, BatchSize: 16, DBsArg: "treedb", Profile: "durable", Progress: false, SeedUsed: 1}
	_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ProfileDir:    dir,
		ExecutionPath: "native-fastpath",
		ForcedPath:    columnStorePathSerialColumnScan,
		QueryNames:    []string{columnStoreQueryQ1},
	})
	if err != nil {
		t.Fatalf("runColumnStoreSuite typed compression: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
	if err != nil {
		t.Fatalf("read column_store_results.json: %v", err)
	}
	var report columnStoreSuiteReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal column_store_results.json: %v", err)
	}
	for _, q := range report.Queries {
		if q.Name != columnStoreQueryQ1 {
			t.Fatalf("unexpected query metric in q1-only typed compression run: %+v", q)
		}
		if q.StorageSource != string(collections.ColumnPhysicalQueryStorageSourceTypedColumnPartSection) || q.FallbackReason != string(collections.ColumnPhysicalQueryFallbackNone) {
			t.Fatalf("compressed q1 query storage/fallback=%q/%q want typed-column part section/no fallback", q.StorageSource, q.FallbackReason)
		}
	}
	for _, cell := range report.JSONBenchCells {
		if cell.Query != columnStoreQueryQ1 {
			t.Fatalf("unexpected JSONBench cell in q1-only typed compression run: %+v", cell)
		}
		if cell.StorageSource != string(collections.ColumnPhysicalQueryStorageSourceTypedColumnPartSection) || cell.FallbackReason != string(collections.ColumnPhysicalQueryFallbackNone) || cell.CompatibilityStatus != "available" || cell.MetadataDataScanPath != columnStoreJSONBenchScanPathData {
			t.Fatalf("compressed q1 cell diagnostics=%+v want typed-column data path/no fallback", cell)
		}
	}
	found := false
	foundLocatorSection := false
	foundDictionaryTarget := false
	for _, row := range report.CodecLayouts {
		attr := row.columnStoreCompressionAttribution
		if strings.HasPrefix(attr.CodecLayoutLabel, "typed_column_part/section/locators/") && attr.CompressionPolicyLabel == "requested_snappy" {
			foundLocatorSection = true
			if attr.RequestedCompression != "snappy" || attr.ActualCompression != "snappy" || attr.SupportState != columnStoreCompressionSupportSupported {
				t.Fatalf("locator section attribution=%+v want requested/actual snappy supported", attr)
			}
			if attr.CompressedBytes <= 0 || attr.RawBytes <= attr.CompressedBytes || attr.DecompressedBytes != attr.RawBytes || attr.CompressionRatio <= 0 || attr.CompressionRatio >= 1 {
				t.Fatalf("locator section bytes=%+v want compressed smaller than raw", attr)
			}
		}
		if strings.HasPrefix(attr.CodecLayoutLabel, "typed_column_part/section/dictionaries/") && attr.CompressionPolicyLabel == "requested_snappy" {
			foundDictionaryTarget = true
			if attr.RequestedCompression != "snappy" || attr.ActualCompression != "snappy" || attr.SupportState != columnStoreCompressionSupportSupported {
				t.Fatalf("dictionary section attribution=%+v want requested/actual snappy supported", attr)
			}
			if attr.CompressedBytes <= 0 || attr.RawBytes <= attr.CompressedBytes || attr.DecompressedBytes != attr.RawBytes || attr.CompressionRatio <= 0 || attr.CompressionRatio >= 1 {
				t.Fatalf("dictionary section bytes=%+v want compressed smaller than raw", attr)
			}
		}
		if !strings.HasPrefix(attr.CodecLayoutLabel, "typed_column_part/") || strings.Contains(attr.CodecLayoutLabel, "/section/") || attr.CompressionPolicyLabel != "requested_snappy" {
			continue
		}
		found = true
		if attr.RequestedCompression != "snappy" || attr.SupportState != columnStoreCompressionSupportSupported {
			t.Fatalf("typed codec row attribution=%+v want requested snappy supported", attr)
		}
		if attr.CompressedBytes <= 0 || attr.RawBytes <= 0 || attr.DecompressedBytes <= 0 || attr.CompressionRatio <= 0 {
			t.Fatalf("typed codec row missing bytes: %+v", attr)
		}
		if attr.CompressedBytesSource != "typed_column_part_byte_accounting.compression_detail.stored_bytes" || attr.RawBytesSource != "typed_column_part_byte_accounting.compression_detail.encoded_raw_bytes" {
			t.Fatalf("typed codec row sources=%+v", attr)
		}
	}
	if !found {
		t.Fatalf("missing requested_snappy typed_column_part codec row in %+v", report.CodecLayouts)
	}
	if !foundLocatorSection {
		t.Fatalf("missing requested_snappy locator section codec row in %+v", report.CodecLayouts)
	}
	if !foundDictionaryTarget {
		t.Fatalf("missing requested_snappy dictionary section codec row in %+v", report.CodecLayouts)
	}
}

func withColumnStoreTypedBenchmarkPolicyFlags(t *testing.T, compression, int64Encoding string, rowsPerGranule int) {
	t.Helper()
	prevCompression := *columnStoreSuiteTypedCompressionArg
	prevInt64Encoding := *columnStoreSuiteTypedInt64EncodingArg
	prevRowsPerGranule := *columnStoreSuiteTypedRowsPerGranuleArg
	prevAdaptive := *columnStoreSuiteTypedAdaptiveArg
	prevAdaptiveTargetBytes := *columnStoreSuiteTypedAdaptiveTargetBytesArg
	prevAdaptiveMinRows := *columnStoreSuiteTypedAdaptiveMinRowsArg
	prevAdaptiveMaxRows := *columnStoreSuiteTypedAdaptiveMaxRowsArg
	*columnStoreSuiteTypedCompressionArg = compression
	*columnStoreSuiteTypedInt64EncodingArg = int64Encoding
	*columnStoreSuiteTypedRowsPerGranuleArg = rowsPerGranule
	*columnStoreSuiteTypedAdaptiveArg = false
	*columnStoreSuiteTypedAdaptiveTargetBytesArg = 0
	*columnStoreSuiteTypedAdaptiveMinRowsArg = 0
	*columnStoreSuiteTypedAdaptiveMaxRowsArg = 0
	t.Cleanup(func() {
		*columnStoreSuiteTypedCompressionArg = prevCompression
		*columnStoreSuiteTypedInt64EncodingArg = prevInt64Encoding
		*columnStoreSuiteTypedRowsPerGranuleArg = prevRowsPerGranule
		*columnStoreSuiteTypedAdaptiveArg = prevAdaptive
		*columnStoreSuiteTypedAdaptiveTargetBytesArg = prevAdaptiveTargetBytes
		*columnStoreSuiteTypedAdaptiveMinRowsArg = prevAdaptiveMinRows
		*columnStoreSuiteTypedAdaptiveMaxRowsArg = prevAdaptiveMaxRows
	})
}

func TestColumnStoreSuiteAggregateMetadataRequestUsesRegisteredNameM11B(t *testing.T) {
	cfg := columnStoreSuiteConfig()
	registered := make(map[string]bool, len(cfg.AggregateMetadata))
	for _, agg := range cfg.AggregateMetadata {
		registered[agg.Name] = true
	}
	name := columnStoreSuiteAggregateMetadataName(columnStoreQueryQ5Metadata)
	if name == "" {
		t.Fatal("q5_metadata did not request aggregate metadata")
	}
	if !registered[name] {
		t.Fatalf("q5_metadata requested aggregate metadata %q outside registered names", name)
	}
	name = columnStoreSuiteAggregateMetadataName(columnStoreQueryQ4B)
	if name == "" {
		t.Fatal("q4b did not request aggregate metadata")
	}
	if !registered[name] {
		t.Fatalf("q4b requested aggregate metadata %q outside registered names", name)
	}
}

func TestColumnStoreSuiteDirUsageFailsOnMissingPathM11A(t *testing.T) {
	_, _, err := columnStoreSuiteDirUsage(filepath.Join(t.TempDir(), "missing"))
	if err == nil {
		t.Fatal("expected missing path to fail")
	}
}

func TestColumnStoreSuiteManifestControlUsageTreatsMissingFilesAsZeroM11A(t *testing.T) {
	bytes, missing, err := columnStoreSuiteManifestControlUsage(t.TempDir())
	if err != nil {
		t.Fatalf("manifest/control usage: %v", err)
	}
	if bytes != 0 {
		t.Fatalf("manifest/control bytes=%d want 0", bytes)
	}
	if len(missing) != len(columnStoreSuiteManifestControlFiles) {
		t.Fatalf("missing manifest/control files=%v want %v", missing, columnStoreSuiteManifestControlFiles)
	}
}

func TestColumnStoreSuiteREADMEDocumentsCommandM11A(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("README.md"))
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	text := string(data)
	for _, want := range []string{
		"-suite column_store",
		"-column-store-path row_store_baseline",
		"column_store_results.html",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("README missing %q", want)
		}
	}
}

func BenchmarkColumnStoreSuiteRowBaselineQueriesM11B(b *testing.B) {
	benchmarkColumnStoreSuiteQueriesM11B(b, columnStorePathRowStoreBaseline, collections.ColumnAssetReadIntegrityVerify)
}

func BenchmarkColumnStoreSuiteBTreeIndexBaselineQueriesM11B(b *testing.B) {
	benchmarkColumnStoreSuiteQueriesM11B(b, columnStorePathBTreeIndexBaseline, collections.ColumnAssetReadIntegrityVerify)
}

func BenchmarkColumnStoreSuiteSerialPhysicalQueriesM14C(b *testing.B) {
	benchmarkColumnStoreSuiteQueriesM11B(b, columnStorePathSerialColumnScan, collections.ColumnAssetReadIntegrityVerify)
}

func BenchmarkColumnStoreSuiteSerialPhysicalQueriesCachedVerifyM1634(b *testing.B) {
	benchmarkColumnStoreSuiteQueriesM11B(b, columnStorePathSerialColumnScan, collections.ColumnAssetReadIntegrityCachedVerify)
}

func BenchmarkColumnStoreSuiteSerialPhysicalQueriesSkipChecksumsM1634(b *testing.B) {
	benchmarkColumnStoreSuiteQueriesM11B(b, columnStorePathSerialColumnScan, collections.ColumnAssetReadIntegritySkipChecksums)
}

func BenchmarkColumnStoreSuiteAggregateMetadataQueriesM14C(b *testing.B) {
	benchmarkColumnStoreSuiteQueriesM11B(b, columnStorePathAggregateMetadata, collections.ColumnAssetReadIntegrityVerify)
}

func BenchmarkColumnStoreSuiteAggregateMetadataQueriesCachedVerifyM1634(b *testing.B) {
	benchmarkColumnStoreSuiteQueriesM11B(b, columnStorePathAggregateMetadata, collections.ColumnAssetReadIntegrityCachedVerify)
}

func BenchmarkColumnStoreSuiteAggregateMetadataQueriesSkipChecksumsM1634(b *testing.B) {
	benchmarkColumnStoreSuiteQueriesM11B(b, columnStorePathAggregateMetadata, collections.ColumnAssetReadIntegritySkipChecksums)
}

func BenchmarkColumnStoreSuiteParallelPhysicalQueriesM14C(b *testing.B) {
	benchmarkColumnStoreSuiteQueriesM11B(b, columnStorePathParallelColumnScan, collections.ColumnAssetReadIntegrityVerify)
}

func benchmarkColumnStoreSuiteQueriesM11B(b *testing.B, path string, readIntegrity collections.ColumnAssetReadIntegrity) {
	const rows = 10_000
	const batchSize = 1_000

	events, sourceBytes := buildColumnStoreSyntheticFixture(rows, 1)
	dir := b.TempDir()
	db, err := openColumnStoreSuiteDB(dir)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(columnStoreSuiteCollectionMeta(path)); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, events, batchSize); err != nil {
		b.Fatalf("insert fixture: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		b.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}

	db, err = openColumnStoreSuiteDB(dir)
	if err != nil {
		b.Fatalf("reopen db: %v", err)
	}
	defer db.Close()
	collection, err = collections.NewCollectionManager(db).OpenCollection("events")
	if err != nil {
		b.Fatalf("reopen collection: %v", err)
	}

	rawHashes, err := columnStoreReferenceHashes(events)
	if err != nil {
		b.Fatalf("reference hashes: %v", err)
	}
	queryCount := len(columnStoreQueryNames())
	b.ReportAllocs()
	b.SetBytes(sourceBytes * int64(queryCount))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		queries, parity, err := runColumnStoreSuiteQueries(collection, rows, rawHashes, path, readIntegrity, nil)
		if err != nil {
			b.Fatalf("queries: %v", err)
		}
		assertColumnStoreQueryMetricCoverageM11A(b, queries)
		assertColumnStoreParityCoverageM11A(b, parity)
		for name, p := range parity {
			if !p.Pass {
				b.Fatalf("parity failed for %s: %+v", name, p)
			}
		}
	}
	b.ReportMetric(float64(rows*queryCount), "rows/op")
}

func assertColumnStoreJSONBenchCellShapeM1955(t testing.TB, report columnStoreSuiteReport, wantPrepared bool) map[string]map[string]columnStoreJSONBenchCell {
	t.Helper()
	if len(report.JSONBenchCells) == 0 {
		t.Fatal("missing JSONBench synthetic cell rows")
	}
	if report.ExternalJSONBenchStatus == "" || len(report.ReportCaveats) == 0 || len(report.ColgranuleReuseMap) == 0 {
		t.Fatalf("missing report status/caveat/reuse fields: external=%q caveats=%v reuse=%v", report.ExternalJSONBenchStatus, report.ReportCaveats, report.ColgranuleReuseMap)
	}
	byQueryMode := make(map[string]map[string]columnStoreJSONBenchCell)
	for _, cell := range report.JSONBenchCells {
		if cell.Query == "" || cell.CellLabel == "" || cell.SortLayout == "" || cell.PlanLabel == "" || cell.StorageSource == "" || cell.FallbackReason == "" || cell.ExecutionMode == "" || cell.MetadataDataScanPath == "" || cell.CompressionMode == "" || cell.MutationMode == "" || cell.RetainedPayloadPolicy == "" || cell.RetainedPayloadEncoding == "" || cell.RetainedPayloadEncodingStatus == "" || cell.RetainedPayloadCompression == "" || cell.RetainedPayloadCompressionPolicy == "" || cell.RetainedPayloadCompressionStatus == "" || cell.TypedStorageOwner == "" || cell.RowCount != report.Rows || cell.ReconstructionStatus == "" || cell.FullDataCaveat == "" || cell.StorageAccountingCaveat == "" || cell.CompatibilityStatus == "" {
			t.Fatalf("incomplete JSONBench cell labels: %+v", cell)
		}
		if cell.FullDataCell {
			t.Fatalf("synthetic gomap cell should not claim full-data parity: %+v", cell)
		}
		if !strings.Contains(cell.FullDataCaveat, "#2117") || !strings.Contains(cell.StorageAccountingCaveat, "#2118") {
			t.Fatalf("cell missing dependency caveats: %+v", cell)
		}
		if len(cell.TypedStorageOwnerColumns) == 0 {
			t.Fatalf("cell missing per-column owner labels: %+v", cell)
		}
		if cell.CompatibilityStatus == "available" {
			if cell.RawHash == 0 || cell.ResultHash == 0 || !cell.ParityWithRowScan {
				t.Fatalf("available JSONBench cell parity/hash invalid: %+v", cell)
			}
		}
		m := byQueryMode[cell.Query]
		if m == nil {
			m = make(map[string]columnStoreJSONBenchCell)
			byQueryMode[cell.Query] = m
		}
		key := cell.CellLabel + "/" + cell.ExecutionMode
		if _, exists := m[key]; exists {
			t.Fatalf("duplicate JSONBench cell for query=%s key=%s", cell.Query, key)
		}
		m[key] = cell
	}
	for _, query := range columnStoreQueryNames() {
		m := byQueryMode[query]
		if len(m) == 0 {
			t.Fatalf("missing JSONBench cell for %s in %+v", query, report.JSONBenchCells)
		}
		if wantPrepared {
			var hasDirect, hasPrepared bool
			for _, cell := range m {
				hasDirect = hasDirect || cell.ExecutionMode == columnStoreJSONBenchModeDirect
				hasPrepared = hasPrepared || cell.ExecutionMode == columnStoreJSONBenchModePrepared
			}
			if !hasDirect || !hasPrepared {
				t.Fatalf("query %s direct/prepared coverage direct=%t prepared=%t cells=%+v", query, hasDirect, hasPrepared, m)
			}
		}
	}
	return byQueryMode
}

func assertColumnStoreQueryMetricCoverageM11A(t testing.TB, queries []columnStoreQueryMetric) map[string]columnStoreQueryMetric {
	t.Helper()
	names := columnStoreQueryNames()
	if len(queries) != len(names) {
		t.Fatalf("query metrics=%d want %d", len(queries), len(names))
	}
	byName := make(map[string]columnStoreQueryMetric, len(queries))
	for _, q := range queries {
		if _, exists := byName[q.Name]; exists {
			t.Fatalf("duplicate query metric for %s", q.Name)
		}
		byName[q.Name] = q
	}
	for _, name := range names {
		if _, ok := byName[name]; !ok {
			t.Fatalf("missing query metric for %s", name)
		}
	}
	if len(byName) != len(names) {
		t.Fatalf("query metrics include unexpected names: %+v", byName)
	}
	return byName
}

func assertColumnStoreCodecLayoutCoverageM1952(t testing.TB, report columnStoreSuiteReport) {
	t.Helper()
	if report.CompressionMatrixNote == "" {
		t.Fatal("missing compression matrix note")
	}
	if len(report.CodecLayouts) < 4 {
		t.Fatalf("codec layout rows=%d want compression_off, current_default_none, zstd unsupported, zstd_dict unsupported", len(report.CodecLayouts))
	}
	seen := make(map[string]string, len(report.CodecLayouts))
	for _, row := range report.CodecLayouts {
		assertColumnStoreCompressionAttributionM1952(t, "codec_layout", row.columnStoreCompressionAttribution, false)
		if row.Rows != report.Rows || row.Columns == 0 {
			t.Fatalf("codec layout row dimensions rows/columns=(%d,%d) want rows=%d and columns>0: %+v", row.Rows, row.Columns, report.Rows, row)
		}
		seen[row.CompressionPolicyLabel] = row.SupportState
	}
	for _, want := range []string{columnStoreCompressionPolicyOff, columnStoreCompressionPolicyDefault} {
		if seen[want] != columnStoreCompressionSupportSupported {
			t.Fatalf("missing supported codec layout policy %q in %+v", want, report.CodecLayouts)
		}
	}
	for _, want := range []string{"requested_zstd", "requested_zstd_dict"} {
		if seen[want] != columnStoreCompressionSupportUnsupported {
			t.Fatalf("missing unsupported codec layout policy %q in %+v", want, report.CodecLayouts)
		}
	}
}

func assertColumnStoreCompressionAttributionM1952(t testing.TB, label string, attr columnStoreCompressionAttribution, queryRow bool) {
	t.Helper()
	if attr.CodecLayoutLabel == "" || attr.CompressionPolicyLabel == "" || attr.RequestedCompression == "" || attr.ActualCompression == "" || attr.SupportState == "" {
		t.Fatalf("%s compression labels=%+v", label, attr)
	}
	if queryRow && (attr.RequestedCompression != columnStoreCompressionNoneLabel || attr.ActualCompression != columnStoreCompressionNoneLabel) {
		t.Fatalf("%s query compression labels=%+v want requested/actual none", label, attr)
	}
	if !queryRow {
		switch attr.SupportState {
		case columnStoreCompressionSupportSupported:
			if attr.CodecLayoutLabel != columnStoreCodecLayoutCurrent && !strings.HasPrefix(attr.CodecLayoutLabel, "typed_column_part/") {
				t.Fatalf("%s supported codec layout row labels=%+v", label, attr)
			}
		case columnStoreCompressionSupportDeferred:
			if attr.SupportReason == "" || attr.CompressedBytes <= 0 || attr.RawBytes <= 0 || attr.DecompressedBytes <= 0 || attr.CompressionRatio <= 0 {
				t.Fatalf("%s deferred codec layout row attribution=%+v", label, attr)
			}
		case columnStoreCompressionSupportUnsupported:
			if attr.SupportReason == "" || attr.CompressedBytes != 0 || attr.RawBytes != 0 || attr.DecompressedBytes != 0 || attr.CompressionRatio != 0 {
				t.Fatalf("%s unsupported codec layout row attribution=%+v", label, attr)
			}
			if attr.CompressedBytesSource == "" || attr.RawBytesSource == "" || attr.DecompressedBytesSource == "" || attr.CompressionRatioSource == "" || attr.CompressionDurationSource == "" || attr.DecompressionDurationSource == "" || attr.BenchmarkAllocationSource == "" {
				t.Fatalf("%s unsupported codec layout row sources=%+v", label, attr)
			}
			return
		default:
			t.Fatalf("%s unexpected codec layout support state=%q attribution=%+v", label, attr.SupportState, attr)
		}
	}
	if queryRow {
		switch attr.SupportState {
		case columnStoreCompressionSupportSupported:
			if attr.CodecLayoutLabel != columnStoreCodecLayoutCurrent || attr.CompressionPolicyLabel != columnStoreCompressionPolicyDefault {
				t.Fatalf("%s supported query attribution=%+v", label, attr)
			}
		case columnStoreCompressionSupportNotApplicable, columnStoreCompressionSupportFallback:
			if strings.Contains(attr.CodecLayoutLabel, columnStoreCodecLayoutCurrent) || attr.CompressionPolicyLabel == columnStoreCompressionPolicyDefault {
				t.Fatalf("%s non-column/fallback query attribution mislabels current codec layout: %+v", label, attr)
			}
		default:
			t.Fatalf("%s unexpected query support state=%q attribution=%+v", label, attr.SupportState, attr)
		}
	}
	if attr.CompressedBytes <= 0 || attr.RawBytes <= 0 || attr.DecompressedBytes <= 0 || attr.CompressionRatio <= 0 {
		t.Fatalf("%s compression byte attribution=%+v", label, attr)
	}
	if attr.CompressedBytesSource == "" || attr.RawBytesSource == "" || attr.DecompressedBytesSource == "" || attr.CompressionRatioSource == "" {
		t.Fatalf("%s compression byte sources=%+v", label, attr)
	}
	if attr.CompressionDurationSource == "" || attr.DecompressionDurationSource == "" {
		t.Fatalf("%s compression duration sources=%+v", label, attr)
	}
	if attr.BenchmarkBPerOp != 0 || attr.BenchmarkAllocsPerOp != 0 || attr.BenchmarkAllocationSource == "" {
		t.Fatalf("%s allocation attribution=%+v", label, attr)
	}
}

func TestColumnStoreQueryNamesReturnsDefensiveCopyM11A(t *testing.T) {
	names := columnStoreQueryNames()
	if len(names) == 0 {
		t.Fatal("columnStoreQueryNames returned no names")
	}
	names[0] = "mutated"
	names = append(names, "extra")

	fresh := columnStoreQueryNames()
	if got, want := fresh[0], "q1"; got != want {
		t.Fatalf("fresh query names first entry = %q, want %q", got, want)
	}
	if len(fresh) != len(columnStoreQueryNameList) {
		t.Fatalf("fresh query names len = %d, want %d", len(fresh), len(columnStoreQueryNameList))
	}
}

func TestColumnStoreSuiteParseQueryNamesM1634(t *testing.T) {
	t.Run("default all", func(t *testing.T) {
		got, err := columnStoreSuiteParseQueryNames("")
		if err != nil {
			t.Fatalf("parse default: %v", err)
		}
		if len(got) != len(columnStoreQueryNameList) {
			t.Fatalf("default len=%d want %d", len(got), len(columnStoreQueryNameList))
		}
	})
	t.Run("subset trims whitespace and normalizes case", func(t *testing.T) {
		got, err := columnStoreSuiteParseQueryNames(" Q3, q5 ")
		if err != nil {
			t.Fatalf("parse subset: %v", err)
		}
		want := []string{columnStoreQueryQ3, columnStoreQueryQ5}
		if !slices.Equal(got, want) {
			t.Fatalf("subset=%v want %v", got, want)
		}
	})
	t.Run("all normalizes case", func(t *testing.T) {
		got, err := columnStoreSuiteParseQueryNames(" All ")
		if err != nil {
			t.Fatalf("parse all: %v", err)
		}
		if len(got) != len(columnStoreQueryNameList) {
			t.Fatalf("all len=%d want %d", len(got), len(columnStoreQueryNameList))
		}
	})
	for _, value := range []string{"missing", "q3,q3", "all,q3", "q3,"} {
		t.Run(value, func(t *testing.T) {
			if _, err := columnStoreSuiteParseQueryNames(value); err == nil {
				t.Fatalf("parse %q succeeded, want error", value)
			}
		})
	}
}

func TestColumnStoreSuiteQueriesCanSelectSingleRoutedPhysicalQueryM1634(t *testing.T) {
	const rows = 64
	events, _ := buildColumnStoreSyntheticFixture(rows, 1)
	dir := t.TempDir()
	db, err := openColumnStoreSuiteDB(dir)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(columnStoreSuiteCollectionMeta(columnStorePathSerialColumnScan)); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, events, 16); err != nil {
		t.Fatalf("insert fixture: %v", err)
	}
	rawHashes, err := columnStoreReferenceHashes(events)
	if err != nil {
		t.Fatalf("reference hashes: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before physical query: %v", err)
	}
	db, err = openColumnStoreSuiteDB(dir)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = db.Close() }()
	collection, err = collections.NewCollectionManager(db).OpenCollection("events")
	if err != nil {
		t.Fatalf("reopen collection: %v", err)
	}

	queries, parity, err := runColumnStoreSuiteQueries(collection, rows, rawHashes, columnStorePathSerialColumnScan, collections.ColumnAssetReadIntegrityVerify, []string{columnStoreQueryQ3})
	if err != nil {
		t.Fatalf("selected physical query: %v", err)
	}
	if len(queries) != 1 || queries[0].Name != columnStoreQueryQ3 {
		t.Fatalf("queries=%+v want only q3", queries)
	}
	if len(parity) != 1 || !parity[columnStoreQueryQ3].Pass {
		t.Fatalf("parity=%+v want passing q3 only", parity)
	}
	if queries[0].PlanLabel != columnStorePathSerialColumnScan {
		t.Fatalf("plan_label=%q want %q", queries[0].PlanLabel, columnStorePathSerialColumnScan)
	}
	if queries[0].RowMaterializations != 0 {
		t.Fatalf("row_materializations=%d want 0 for physical q3", queries[0].RowMaterializations)
	}
}

func assertColumnStoreParityCoverageM11A(t testing.TB, parity map[string]columnStoreParity) {
	t.Helper()
	names := columnStoreQueryNames()
	if len(parity) != len(names) {
		t.Fatalf("parity entries=%d want %d", len(parity), len(names))
	}
	for _, name := range names {
		if _, ok := parity[name]; !ok {
			t.Fatalf("missing parity entry for %s", name)
		}
	}
}

func newColumnStoreSuiteTestCollectionM11A(t testing.TB, rows, batchSize int) (*collections.Collection, []columnStoreFixtureEvent, map[string]uint64) {
	t.Helper()
	events, _ := buildColumnStoreSyntheticFixture(rows, 1)
	dir := t.TempDir()
	db, err := openColumnStoreSuiteDB(dir)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "events",
		Options: collections.CollectionOptions{
			DocumentFormat:               collections.DocumentFormatJSON,
			DisableIndexedWriteMemtables: true,
			ColumnStore:                  columnStoreSuiteConfig(),
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, events, batchSize); err != nil {
		t.Fatalf("insert fixture: %v", err)
	}
	rawHashes, err := columnStoreReferenceHashes(events)
	if err != nil {
		t.Fatalf("reference hashes: %v", err)
	}
	return collection, events, rawHashes
}
