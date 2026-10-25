package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestDefaultDocumentOpenerActivatesNativeCOW(t *testing.T) {
	cfg := defaultConfig()
	cfg.Dir = filepath.Join(t.TempDir(), "db")
	database, err := openDemoBackend(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	}()
	capture, err := database.backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatalf("default document opener has no active shared COW owner: %v", err)
	}
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	before := database.stats()["treedb.cache.cow.txn.logical_seq"]
	if _, err := database.manager.CreateCollection(collectionMetaForMode(cfg.Mode)); err != nil {
		t.Fatal(err)
	}
	col, err := database.manager.OpenCollection(collectionName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("a")}, [][]byte{[]byte(`{"time_us":1}`)}); err != nil {
		t.Fatal(err)
	}
	held, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if _, err := col.InsertBatch([][]byte{[]byte("b")}, [][]byte{[]byte(`{"time_us":2}`)}); err != nil {
		t.Fatal(err)
	}
	if got, err := col.Get([]byte("b")); err != nil || string(got) != `{"time_us":2}` {
		t.Fatalf("logical read before checkpoint: %s %v", got, err)
	}
	if stats := database.stats(); stats["treedb.cache.memtable_mode"] != "cow_btree" || stats["treedb.cache.cow.txn.logical_seq"] == before {
		t.Fatalf("no actual native publication: %v", stats)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	old, err := held.FetchDocumentsByID([][]byte{[]byte("a"), []byte("b")}, collections.DocumentFetchOptions{})
	if err != nil || len(old.Results) != 2 || !old.Results[0].Found || old.Results[1].Found {
		t.Fatalf("held read across publication/checkpoint: %+v %v", old.Results, err)
	}
}

func TestParseConfigRejectsPositionals(t *testing.T) {
	_, err := parseConfig([]string{"unexpected"})
	if err == nil || !strings.Contains(err.Error(), "unexpected positional arguments") {
		t.Fatalf("err=%v want unexpected positional rejection", err)
	}
}

func TestParseConfigPresetOverride(t *testing.T) {
	cfg, err := parseConfig([]string{"-preset", "event-analytics", "-rows", "123", "-batch-size", "7", "-mode", "document"})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Preset != "event-analytics" || cfg.Workload != workloadRangeAggregate {
		t.Fatalf("preset not applied: %+v", cfg)
	}
	if cfg.Rows != 123 || cfg.BatchSize != 7 || cfg.Mode != modeDocument {
		t.Fatalf("explicit flags did not override preset: %+v", cfg)
	}
}

func TestGenerateFixtureRowsDeterministic(t *testing.T) {
	cfg := defaultConfig()
	cfg.Rows = 8
	cfg.PayloadBytes = 17
	cfg.ExtraFields = 3
	cfg.Int64Distribution = "random"
	cfg.Seed = 99

	first := generateFixtureRows(cfg)
	second := generateFixtureRows(cfg)
	if len(first) != len(second) {
		t.Fatalf("length mismatch")
	}
	for i := range first {
		if !bytes.Equal(first[i].ID, second[i].ID) || !bytes.Equal(first[i].Doc, second[i].Doc) || first[i].TimeUS != second[i].TimeUS {
			t.Fatalf("row %d differs:\n%+v\n%+v", i, first[i], second[i])
		}
	}
}

func TestDeterministicPayloadHandlesNegativeSeed(t *testing.T) {
	got := deterministicPayload(64, -99, 3)
	if len(got) != 64 {
		t.Fatalf("payload len=%d want 64", len(got))
	}
	if got != deterministicPayload(64, -99, 3) {
		t.Fatalf("negative-seed payload is not deterministic")
	}
}

func TestAggregateParityAcrossModes(t *testing.T) {
	modes := []string{modeDocument, modeTypedRow, modeTypedColumn, modeHybridDocumentRow, modeHybridDocumentCol, modeHybridRowColumn}
	var wantCount, wantSum int64
	for i, mode := range modes {
		cfg := defaultConfig()
		cfg.Mode = mode
		cfg.Rows = 48
		cfg.BatchSize = 16
		cfg.PayloadBytes = 8
		cfg.ExtraFields = 1
		cfg.Dir = filepath.Join(t.TempDir(), "db")
		got, err := runDemo(cfg)
		if err != nil {
			t.Fatalf("runDemo mode=%s: %v", mode, err)
		}
		wantOwner := "backend_roots"
		if mode == modeDocument {
			wantOwner = "native_semantic"
		}
		if got.PublicationOwner != wantOwner || (mode == modeDocument && got.MemtableMode != "cow_btree") {
			t.Fatalf("mode=%s route=%s/%s want owner=%s", mode, got.PublicationOwner, got.MemtableMode, wantOwner)
		}
		if i == 0 {
			wantCount, wantSum = got.Aggregate.Count, got.Aggregate.Sum
			continue
		}
		if got.Aggregate.Count != wantCount || got.Aggregate.Sum != wantSum {
			t.Fatalf("mode=%s aggregate count=%d sum=%d want count=%d sum=%d", mode, got.Aggregate.Count, got.Aggregate.Sum, wantCount, wantSum)
		}
	}
}

func TestReopenReadSmoke(t *testing.T) {
	for _, mode := range []string{modeDocument, modeTypedColumn} {
		t.Run(mode, func(t *testing.T) {
			cfg := defaultConfig()
			cfg.Mode = mode
			cfg.Workload = workloadReopenRead
			cfg.Rows = 64
			cfg.BatchSize = 32
			cfg.PayloadBytes = 8
			cfg.Dir = filepath.Join(t.TempDir(), "db")
			got, err := runDemo(cfg)
			if err != nil {
				t.Fatalf("runDemo reopen-read: %v", err)
			}
			if got.Matches == 0 || got.Aggregate.Count == 0 {
				t.Fatalf("unexpected reopen-read result: %+v", got)
			}
		})
	}
}

func TestDocumentWorkloadsAndDefaultShapes(t *testing.T) {
	for _, workload := range []string{workloadInsert, workloadPointGet, workloadRangeFilter, workloadRangeAggregate, workloadFullAggregate, workloadMixedRead, workloadReopenRead} {
		t.Run(workload, func(t *testing.T) {
			cfg := defaultConfig()
			cfg.Workload, cfg.Rows, cfg.BatchSize = workload, 32, 16
			cfg.Dir = filepath.Join(t.TempDir(), "db")
			got, err := runDemo(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if got.PublicationOwner != "native_semantic" || got.MemtableMode != "cow_btree" {
				t.Fatalf("default route: %+v", got)
			}
			if workload == workloadInsert {
				if got.Ops != int64(cfg.Rows) {
					t.Fatalf("insert ops=%d", got.Ops)
				}
				return
			}
			if workload == workloadPointGet {
				if got.Matches != int64(cfg.Rows) {
					t.Fatalf("point matches=%d", got.Matches)
				}
				return
			}
			rows := generateFixtureRows(cfg)
			low, high := queryRange(rows, workload == workloadFullAggregate)
			var count, sum int64
			for _, row := range rows {
				if row.TimeUS >= low && row.TimeUS <= high {
					count++
					sum += row.TimeUS
				}
			}
			if got.Matches != count {
				t.Fatalf("matches=%d want %d", got.Matches, count)
			}
			if workload != workloadRangeFilter && (got.Aggregate.Count != count || got.Aggregate.Sum != sum) {
				t.Fatalf("aggregate=%+v want count=%d sum=%d", got.Aggregate, count, sum)
			}
		})
	}
	for _, args := range [][]string{nil, {"-preset", "document-app"}} {
		cfg, err := parseConfig(args)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Dir = filepath.Join(t.TempDir(), "db")
		got, err := runDemo(cfg)
		if err != nil {
			t.Fatalf("actual default/preset %v: %v", args, err)
		}
		if got.PublicationOwner != "native_semantic" || got.Rows != cfg.Rows || got.Matches == 0 {
			t.Fatalf("actual default/preset %v: %+v", args, got)
		}
	}
}

func TestDocumentCapacityRefusalAndCleanup(t *testing.T) {
	cfg := defaultConfig()
	cfg.Dir = filepath.Join(t.TempDir(), "db")
	database, err := openDemoBackend(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.manager.CreateCollection(collectionMetaForMode(cfg.Mode)); err != nil {
		t.Fatal(err)
	}
	col, err := database.manager.OpenCollection(collectionName)
	if err != nil {
		t.Fatal(err)
	}
	// Retain the current cut so checkpointing cannot retire its setup receipts
	// and create accidental headroom while the real admission owner is full.
	pin, err := database.backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	allocator := pin.NativeAllocator()
	limit := allocator.Limits().MaxInFlightBytes
	_, capacityErr := allocator.AdmitNativeBytes(limit + 1)
	if capacityErr == nil {
		t.Fatal("finite limit admitted over-limit reservation")
	}
	blocker, err := allocator.AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	reserved, err := strconv.ParseUint(database.stats()["treedb.cache.cow.reserved_bytes"], 10, 64)
	if err != nil || reserved >= limit {
		t.Fatalf("reserved=%d limit=%d error=%v", reserved, limit, err)
	}
	if err := blocker.Resize(limit - reserved); err != nil {
		t.Fatal(err)
	}
	ids, docs := [][]byte{[]byte("a")}, [][]byte{[]byte(`{"time_us":1}`)}
	if _, err := col.InsertBatch(ids, docs); !errors.Is(err, capacityErr) {
		t.Fatalf("full native owner must refuse without fallback: %v", err)
	}
	blocker.Close()
	if err := pin.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := col.Get(ids[0]); err != nil || got != nil {
		t.Fatalf("refused insert became visible: %s %v", got, err)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("insert after releasing blocker: %v", err)
	}
	registry := database.backend.ValueLogIdentityPinRegistry()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
	if _, err := col.Get(ids[0]); err == nil {
		t.Fatal("closed manager remained readable")
	}
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("closed owner pins: %+v", s)
	}
	reopened, err := openDemoBackend(cfg)
	if err != nil {
		t.Fatalf("closed owner retained directory ownership: %v", err)
	}
	defer reopened.Close()
	col, err = reopened.manager.OpenCollection(collectionName)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := col.Get(ids[0]); err != nil || !bytes.Equal(got, docs[0]) {
		t.Fatalf("reopen after refusal: %s %v", got, err)
	}
}

func TestDocumentDurableCrashRecovery(t *testing.T) {
	const childDir = "GOMAP_COLLECTION_DEMO_CRASH_DIR"
	cfg := defaultConfig()
	cfg.Rows = 4
	if dir := os.Getenv(childDir); dir != "" {
		cfg.Dir = dir
		database, err := openDemoBackend(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.manager.CreateCollection(collectionMetaForMode(cfg.Mode)); err != nil {
			t.Fatal(err)
		}
		col, err := database.manager.OpenCollection(collectionName)
		if err != nil {
			t.Fatal(err)
		}
		if err := insertRows(col, generateFixtureRows(cfg), cfg.BatchSize); err != nil {
			t.Fatal(err)
		}
		// Durable InsertBatch has returned. Skip all Close/Checkpoint work.
		os.Exit(0)
	}
	cfg.Dir = filepath.Join(t.TempDir(), "db")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDocumentDurableCrashRecovery$", "-test.count=1")
	cmd.Env = append(os.Environ(), childDir+"="+cfg.Dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash child: %v\n%s", err, output)
	}
	database, err := openDemoBackend(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	col, err := database.manager.OpenCollection(collectionName)
	if err != nil {
		t.Fatal(err)
	}
	rows := generateFixtureRows(cfg)
	for _, row := range rows {
		if got, err := col.Get(row.ID); err != nil || !bytes.Equal(got, row.Doc) {
			t.Fatalf("replay document %s: %s %v", row.ID, got, err)
		}
	}
	result, err := runRangeFilter(col, rows, cfg)
	low, high := queryRange(rows, false)
	var want int64
	for _, row := range rows {
		if row.TimeUS >= low && row.TimeUS <= high {
			want++
		}
	}
	if err != nil || result.Matches != want {
		t.Fatalf("replay index matches=%d want=%d error=%v", result.Matches, want, err)
	}
}

func TestInsertWorkloadReportsSetupThroughput(t *testing.T) {
	cfg := defaultConfig()
	cfg.Mode = modeTypedColumn
	cfg.Workload = workloadInsert
	cfg.Rows = 32
	cfg.BatchSize = 16
	cfg.PayloadBytes = 8
	cfg.Dir = filepath.Join(t.TempDir(), "db")
	got, err := runDemo(cfg)
	if err != nil {
		t.Fatalf("runDemo insert: %v", err)
	}
	if got.Ops != int64(cfg.Rows) || got.OpsSec <= 0 || got.RowsSec <= 0 {
		t.Fatalf("insert counters not populated from setup timing: %+v", got)
	}
	if got.QueryMS < 0 {
		t.Fatalf("query_ms should not be negative: %+v", got)
	}
}

func TestExplicitDirMustBeFresh(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("keep"), 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	cfg := defaultConfig()
	cfg.Rows = 8
	cfg.Dir = dir
	if _, err := runDemo(cfg); err == nil {
		t.Fatal("runDemo accepted non-empty explicit dir")
	}
	if got, err := os.ReadFile(filepath.Join(dir, "keep.txt")); err != nil || string(got) != "keep" {
		t.Fatalf("sentinel changed: got=%q err=%v", got, err)
	}
}

func TestValidateFreshDemoDirRejectsRawParentTraversal(t *testing.T) {
	if _, err := validateFreshDemoDir("safe/../other"); err == nil {
		t.Fatal("validateFreshDemoDir accepted raw parent traversal")
	}
}

func TestValidateFreshDemoDirAllowsShortExplicitPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("absolute /tmp/db shape is Unix-specific")
	}
	got, err := validateFreshDemoDir("/tmp/db")
	if err != nil {
		t.Fatalf("validateFreshDemoDir rejected short explicit path: %v", err)
	}
	if got != "/tmp/db" {
		t.Fatalf("path=%q want /tmp/db", got)
	}
}

func TestProfileArtifactsCreated(t *testing.T) {
	profileDir := t.TempDir()
	cfg := defaultConfig()
	cfg.Mode = modeTypedColumn
	cfg.Rows = 128
	cfg.BatchSize = 64
	cfg.PayloadBytes = 8
	cfg.Dir = filepath.Join(t.TempDir(), "db")
	cfg.ProfileDir = profileDir
	if _, err := runDemo(cfg); err != nil {
		t.Fatalf("runDemo profile: %v", err)
	}
	for _, name := range []string{"cpu.pprof", "allocs.pprof", "summary.json", "summary.md"} {
		path := filepath.Join(profileDir, name)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("missing profile artifact %s: %v", name, err)
		}
		if info.Size() == 0 {
			t.Fatalf("profile artifact %s is empty", name)
		}
	}
	t.Run("document-summary-error-closes-owner", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.Rows = 4
		cfg.Dir = filepath.Join(t.TempDir(), "db")
		cfg.ProfileDir = t.TempDir()
		if err := os.Mkdir(filepath.Join(cfg.ProfileDir, "summary.json"), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := runDemo(cfg); err == nil {
			t.Fatal("summary output failure was not reported")
		}
		reopened, err := openDemoBackend(cfg)
		if err != nil {
			t.Fatalf("failed command retained database ownership: %v", err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
