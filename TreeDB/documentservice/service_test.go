package documentservice

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestVectorIndexUnavailablePreservesDiagnosticCause(t *testing.T) {
	cause := fmt.Errorf("%w: stale document coverage", collections.ErrVectorIndexSearchUnavailable)
	err := mapVectorIndexSearchError("native ann vector search", cause)
	if ErrorCodeOf(err) != CodeIndexUnavailable || !strings.Contains(err.Error(), "stale document coverage") {
		t.Fatalf("err=%v code=%s", err, ErrorCodeOf(err))
	}
}

func TestCollectMatchingIDsUsesDeclaredScalarEqualityConjunction(t *testing.T) {
	ctx := context.Background()
	svc, db := newTestService(t)
	defer db.Close()
	create := CreateIndexRequest{
		Name:      "scalar_delete",
		Dimension: 2,
		Metric:    MetricCosine,
		ScalarFields: []ScalarFieldDeclaration{
			{Field: "meta.user_id", ValueType: ScalarFieldString},
			{Field: "meta.fpath", ValueType: ScalarFieldString},
		},
	}
	info, err := svc.CreateIndex(ctx, create)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	documents := []Document{
		{ID: "a", Embedding: []float32{1, 0}, Meta: map[string]any{"user_id": "shared", "fpath": "/target"}},
		{ID: "b", Embedding: []float32{1, 0}, Meta: map[string]any{"user_id": "shared", "fpath": "/other"}},
		{ID: "c", Embedding: []float32{1, 0}, Meta: map[string]any{"user_id": "other", "fpath": "/target"}},
	}
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: documents}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	col, _, err := svc.openIndex(ctx, create.Name, 0)
	if err != nil {
		t.Fatalf("openIndex: %v", err)
	}
	filter := &Filter{Operator: "AND", Conditions: []Filter{
		{Field: "meta.user_id", Operator: "==", Value: "shared"},
		{Field: "meta.fpath", Operator: "==", Value: "/target"},
	}}
	ids, indexed, err := collectMatchingIDsFromScalarIndexes(ctx, col, filter, newScalarSchema(info.ScalarFields))
	if err != nil || !indexed || !reflect.DeepEqual(ids, []string{"a"}) {
		t.Fatalf("ids=%v indexed=%v err=%v", ids, indexed, err)
	}
}

func TestServiceNilMutationReceiverFailsClosed(t *testing.T) {
	var svc *Service
	ctx := context.Background()
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("UpsertDocuments err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.DeleteDocuments(ctx, "docs", DeleteDocumentsRequest{}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("DeleteDocuments err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.ResetIndex(ctx, "docs", ResetIndexRequest{}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("ResetIndex err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.OptimizeIndex(ctx, "docs", OptimizeIndexRequest{}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("OptimizeIndex err=%v code=%s", err, ErrorCodeOf(err))
	}
}

func TestServiceRawBackendReadsAdvanceCachedForegroundActivity(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.BackgroundIndexVacuumInterval = -1
	backend, cleanup, stats, maintenance, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = cleanup() })
	manager := collections.NewCollectionManager(backend)
	svc := NewWithDeferredVectorBuildMaintenance(manager, maintenance)
	ctx := context.Background()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2, Metric: MetricCosine}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	const key = "treedb.cache.vlog_generation.maintenance.foreground_last_read_unix_nano"
	readTimestamp := func() int64 {
		t.Helper()
		var timestamp int64
		if _, err := fmt.Sscan(stats()[key], &timestamp); err != nil {
			t.Fatalf("parse %s: %v", key, err)
		}
		return timestamp
	}
	assertAdvances := func(name string, read func() error) {
		t.Helper()
		before := readTimestamp()
		for range 2 * 64 {
			if err := read(); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if after := readTimestamp(); after > before {
				return
			}
		}
		t.Fatalf("%s did not advance cached foreground activity from %d", name, before)
	}

	assertAdvances("document-service count", func() error {
		count, err := svc.CountDocuments(ctx, "docs", CountDocumentsRequest{})
		if err == nil && count.Count != 0 {
			return fmt.Errorf("count=%d want 0", count.Count)
		}
		return err
	})
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatalf("warm cached collection open: %v", err)
	}
	assertAdvances("cached collection open", func() error {
		_, err := manager.OpenCollection("docs")
		return err
	})
	assertAdvances("bounded collection list", func() error {
		_, _, err := manager.ListCollectionsBounded(1)
		return err
	})
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{
		Documents: []Document{{ID: "long-scan", Embedding: []float32{1, 0}}},
	}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	readActive := func() int64 {
		t.Helper()
		const activeKey = "treedb.cache.vlog_generation.maintenance.foreground_active_iterators"
		var active int64
		if _, err := fmt.Sscan(stats()[activeKey], &active); err != nil {
			t.Fatalf("parse %s: %v", activeKey, err)
		}
		return active
	}
	assertLongRawScan := func(name string, scan func(func() (bool, error)) error) {
		t.Helper()
		entered := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseScan := func() { releaseOnce.Do(func() { close(release) }) }
		defer releaseScan()
		done := make(chan error, 1)
		go func() {
			done <- scan(func() (bool, error) {
				close(entered)
				<-release
				return true, nil
			})
		}()
		select {
		case <-entered:
		case err := <-done:
			t.Fatalf("%s ended before callback: %v", name, err)
		case <-time.After(time.Second):
			t.Fatalf("%s did not reach callback", name)
		}
		if got := readActive(); got != 1 {
			releaseScan()
			<-done
			t.Fatalf("%s active foreground reads=%d want 1", name, got)
		}
		time.Sleep(300 * time.Millisecond)
		if got := readActive(); got != 1 {
			releaseScan()
			<-done
			t.Fatalf("%s active foreground reads after read timestamp aged=%d want 1", name, got)
		}
		releaseScan()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s did not finish", name)
		}
		if got := readActive(); got != 0 {
			t.Fatalf("%s active foreground reads after completion=%d want 0", name, got)
		}
	}
	assertLongRawScan("ScanDocumentIDsFunc", func(block func() (bool, error)) error {
		_, err := col.ScanDocumentIDsFunc(1, func([]byte) (bool, error) { return block() })
		return err
	})
	assertLongRawScan("ScanDocumentsFunc", func(block func() (bool, error)) error {
		_, err := col.ScanDocumentsFunc(1, func(collections.DocumentRecord) (bool, error) { return block() })
		return err
	})
}

func TestServiceDeferredVectorBuildMaintenanceLifecycle(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.BackgroundIndexVacuumInterval = -1
	backend, cleanup, stats, maintenance, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = cleanup() })
	svc := NewWithDeferredVectorBuildMaintenance(collections.NewCollectionManager(backend), maintenance)
	ctx := context.Background()
	for _, name := range []string{"docs", "other", "generation", "commitfail", "rejected", "optimized"} {
		if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: name, Dimension: 2, Metric: MetricCosine}); err != nil {
			t.Fatalf("CreateIndex(%s): %v", name, err)
		}
	}
	deferInsert := func(index, id string) {
		t.Helper()
		if _, err := svc.UpsertDocuments(ctx, index, UpsertDocumentsRequest{
			Documents:               []Document{{ID: id, Embedding: []float32{1, 0}}},
			DeferVectorIndexRebuild: true,
		}); err != nil {
			t.Fatalf("deferred upsert %s/%s: %v", index, id, err)
		}
	}
	assertActive := func(want bool) {
		t.Helper()
		if got := stats()["treedb.bg_vacuum.deferred_vector_build.active"]; got != fmt.Sprint(want) {
			t.Fatalf("deferred epoch active=%q want %t; stats=%v", got, want, stats())
		}
	}
	assertCleanGraph := func(index string, wantLive int) {
		t.Helper()
		col, _, err := svc.openIndex(ctx, index, 0)
		if err != nil {
			t.Fatalf("open %s: %v", index, err)
		}
		status, err := col.VectorIndexStatus(defaultVectorIndexName)
		if err != nil {
			t.Fatalf("vector status %s: %v", index, err)
		}
		if !status.Loaded || status.RebuildNeeded || status.Stats.RebuildNeeded || status.Stats.SnapshotDirty || status.Stats.LiveDocs != wantLive {
			t.Fatalf("vector status %s=%+v want clean graph with %d live docs", index, status, wantLive)
		}
	}

	deferInsert("docs", "a")
	assertActive(true)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := svc.CreateIndex(canceled, CreateIndexRequest{Name: "canceled", Dimension: 2, Metric: MetricCosine}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled CreateIndex error=%v want context.Canceled", err)
	}
	assertActive(true)
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{
		Documents:               []Document{{ID: "bad", Embedding: []float32{1}}},
		DeferVectorIndexRebuild: true,
	}); err == nil {
		t.Fatal("malformed deferred upsert succeeded")
	}
	assertActive(true)

	deferInsert("docs", "a2")
	assertActive(true)
	deferInsert("other", "b")
	assertActive(false)
	assertCleanGraph("other", 1)

	generationInfo, err := svc.OpenIndex(ctx, "generation")
	if err != nil {
		t.Fatalf("OpenIndex(generation): %v", err)
	}
	competingGenerationEpoch := svc.deferredVectorBuildMaintenance.AdmitInsert(context.Background(), "generation", generationInfo.Generation+1)
	if competingGenerationEpoch == 0 || !svc.deferredVectorBuildMaintenance.CommitInsert(competingGenerationEpoch) {
		t.Fatal("establish competing-generation owner")
	}
	deferInsert("generation", "generation-fallback")
	assertActive(false)
	assertCleanGraph("generation", 1)

	// Invalidation after admission but before commit must fall back to the
	// ordinary rebuild policy, never return a deferred success with a dirty graph.
	svc.deferredMaintenanceBeforeCommit = svc.deferredVectorBuildMaintenance.End
	deferInsert("rejected", "commit-rejected")
	svc.deferredMaintenanceBeforeCommit = nil
	assertActive(false)
	assertCleanGraph("rejected", 1)

	deferInsert("docs", "c")
	assertActive(true)
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "c", Embedding: []float32{0, 1}}}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatalf("deferred update: %v", err)
	}
	assertActive(false)

	deferInsert("docs", "d")
	if _, err := svc.DeleteDocuments(ctx, "docs", DeleteDocumentsRequest{IDs: []string{"d"}}); err != nil {
		t.Fatalf("DeleteDocuments: %v", err)
	}
	assertActive(false)

	deferInsert("docs", "missing-delete-guard")
	if _, err := svc.DeleteDocuments(ctx, "missing", DeleteDocumentsRequest{IDs: []string{"missing"}}); ErrorCodeOf(err) != CodeIndexNotFound {
		t.Fatalf("missing-index DeleteDocuments err=%v code=%s", err, ErrorCodeOf(err))
	}
	assertActive(false)

	deferInsert("docs", "stale-delete-guard")
	docsInfo, err := svc.OpenIndex(ctx, "docs")
	if err != nil {
		t.Fatalf("OpenIndex(docs): %v", err)
	}
	if _, err := svc.DeleteDocuments(ctx, "docs", DeleteDocumentsRequest{IDs: []string{"stale-delete-guard"}, ExpectedGeneration: docsInfo.Generation + 1}); ErrorCodeOf(err) != CodeIndexStale {
		t.Fatalf("stale-generation DeleteDocuments err=%v code=%s", err, ErrorCodeOf(err))
	}
	assertActive(false)

	deferInsert("optimized", "e")
	if _, err := svc.OptimizeIndex(nil, "optimized", OptimizeIndexRequest{}); err != nil {
		t.Fatalf("OptimizeIndex deferred finalizer: %v", err)
	}
	assertActive(false)
	assertCleanGraph("optimized", 1)

	svc.deferredMaintenanceBeforeCommit = svc.deferredVectorBuildMaintenance.End
	deferInsert("commitfail", "commit-fallback")
	svc.deferredMaintenanceBeforeCommit = nil
	assertActive(false)
	assertCleanGraph("commitfail", 1)

	deferInsert("docs", "f")
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "normal", Embedding: []float32{1, 0}}}}); err != nil {
		t.Fatalf("non-deferred UpsertDocuments: %v", err)
	}
	assertActive(false)

	deferInsert("docs", "g")
	// Reset support is independent of the fail-back transition being tested.
	_, _ = svc.ResetIndex(ctx, "docs", ResetIndexRequest{DropOld: true})
	assertActive(false)

	deferInsert("docs", "h")
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertActive(false)
}

func TestServiceDeferredVectorBuildMaintenanceDoesNotSpanManagers(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.BackgroundIndexVacuumInterval = -1
	backend, cleanup, stats, maintenance, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = cleanup() })
	first := NewWithDeferredVectorBuildMaintenance(collections.NewCollectionManager(backend), maintenance)
	second := NewWithDeferredVectorBuildMaintenance(collections.NewCollectionManager(backend), maintenance)
	ctx := context.Background()
	if _, err := first.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2, Metric: MetricCosine}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	request := func(id string) UpsertDocumentsRequest {
		return UpsertDocumentsRequest{Documents: []Document{{ID: id, Embedding: []float32{1, 0}}}, DeferVectorIndexRebuild: true}
	}
	if _, err := first.UpsertDocuments(ctx, "docs", request("first")); err != nil {
		t.Fatalf("first deferred upsert: %v", err)
	}
	if got := stats()["treedb.bg_vacuum.deferred_vector_build.active"]; got != "true" {
		t.Fatalf("first service epoch active=%q want true", got)
	}
	if _, err := second.UpsertDocuments(ctx, "docs", request("second")); err != nil {
		t.Fatalf("second deferred upsert: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("close second service: %v", err)
	}
	if got := stats()["treedb.bg_vacuum.deferred_vector_build.active"]; got != "true" {
		t.Fatalf("separate manager cleared first service epoch: active=%q", got)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first service: %v", err)
	}
	if got := stats()["treedb.bg_vacuum.deferred_vector_build.active"]; got != "false" {
		t.Fatalf("owning service left epoch active=%q", got)
	}
}

func TestServiceDeferredColumnGraphInsertAdmissionIsSharedAndLifecycleIsBarrier(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.BackgroundIndexVacuumInterval = -1
	backend, cleanup, _, maintenance, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = cleanup() })
	svc := NewWithDeferredVectorBuildMaintenance(collections.NewCollectionManager(backend), maintenance)
	ctx := context.Background()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{
		Name:      "docs",
		Dimension: 2,
		Metric:    MetricCosine,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyColumnGraph,
		},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if _, err := svc.OptimizeIndex(ctx, "docs", OptimizeIndexRequest{}); err != nil {
		t.Fatalf("OptimizeIndex: %v", err)
	}
	svc.benchmarkSearchCacheMu.RLock()
	cachedCollection := svc.benchmarkSearchCache["docs"].collection
	svc.benchmarkSearchCacheMu.RUnlock()
	if cachedCollection == nil {
		t.Fatal("OptimizeIndex did not prime the prepared search cache")
	}

	firstAtCommit := make(chan struct{})
	releaseFirst := make(chan struct{})
	var firstHook atomic.Bool
	svc.deferredMaintenanceBeforeCommit = func() {
		if firstHook.CompareAndSwap(false, true) {
			close(firstAtCommit)
			<-releaseFirst
		}
	}
	request := func(id string) UpsertDocumentsRequest {
		return UpsertDocumentsRequest{
			Documents:               []Document{{ID: id, Embedding: []float32{1, 0}}},
			DeferVectorIndexRebuild: true,
		}
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := svc.UpsertDocuments(ctx, "docs", request("first"))
		firstDone <- err
	}()
	select {
	case <-firstAtCommit:
	case <-time.After(5 * time.Second):
		t.Fatal("first deferred insert did not reach pre-commit hook")
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := svc.UpsertDocuments(ctx, "docs", request("second"))
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second deferred insert: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second deferred insert could not pass a paused independent insert")
	}

	deleteDone := make(chan error, 1)
	go func() {
		_, err := svc.DeleteDocuments(ctx, "docs", DeleteDocumentsRequest{IDs: []string{"second"}})
		deleteDone <- err
	}()
	select {
	case err := <-deleteDone:
		t.Fatalf("delete crossed an admitted deferred insert: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first deferred insert: %v", err)
	}
	if got := cachedCollection.LastInsertStats().Documents; got != 0 {
		t.Fatalf("shared inserts reused cached diagnostics handle: Documents=%d want 0", got)
	}
	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteDocuments: %v", err)
	}
	count, err := svc.CountDocuments(ctx, "docs", CountDocumentsRequest{})
	if err != nil || count.Count != 1 {
		t.Fatalf("CountDocuments=%+v err=%v want 1", count, err)
	}

	firstAtCommit = make(chan struct{})
	releaseFirst = make(chan struct{})
	firstHook.Store(false)
	svc.deferredMaintenanceBeforeCommit = func() {
		if firstHook.CompareAndSwap(false, true) {
			close(firstAtCommit)
			<-releaseFirst
		}
	}
	firstDone = make(chan error, 1)
	go func() {
		_, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{
			Documents:               []Document{{ID: "collision", Content: "first", Embedding: []float32{1, 0}}},
			DeferVectorIndexRebuild: true,
		})
		firstDone <- err
	}()
	select {
	case <-firstAtCommit:
	case <-time.After(5 * time.Second):
		t.Fatal("first collision insert did not reach pre-commit hook")
	}
	updateDone := make(chan error, 1)
	go func() {
		_, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{
			Documents:               []Document{{ID: "collision", Content: "second", Embedding: []float32{0, 1}}},
			DeferVectorIndexRebuild: true,
		})
		updateDone <- err
	}()
	select {
	case err := <-updateDone:
		t.Fatalf("colliding upsert crossed the insert-only shared boundary: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first collision insert: %v", err)
	}
	if err := <-updateDone; err != nil {
		t.Fatalf("colliding upsert retry: %v", err)
	}
	svc.deferredMaintenanceBeforeCommit = nil
	filtered, err := svc.FilterDocuments(ctx, "docs", FilterDocumentsRequest{Limit: 10})
	if err != nil {
		t.Fatalf("FilterDocuments: %v", err)
	}
	found := false
	for _, document := range filtered.Documents {
		if document.ID == "collision" {
			found = document.Content == "second"
		}
	}
	if !found {
		t.Fatalf("collision document was not exclusively updated: %+v", filtered.Documents)
	}
}

func TestServiceDeferredVectorBuildOptimizeCheckpointCrashReopen(t *testing.T) {
	const helper = "TREEDB_DEFERRED_OPTIMIZE_CRASH_HELPER"
	dir := os.Getenv(helper)
	if dir != "" {
		opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
		opts.BackgroundIndexVacuumInterval = -1
		backend, _, _, maintenance, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
		if err != nil {
			t.Fatalf("open helper: %v", err)
		}
		svc := NewWithDeferredVectorBuildMaintenance(collections.NewCollectionManager(backend), maintenance)
		ctx := context.Background()
		if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2, Metric: MetricCosine}); err != nil {
			t.Fatalf("create helper index: %v", err)
		}
		if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "durable", Embedding: []float32{1, 0}}}, DeferVectorIndexRebuild: true}); err != nil {
			t.Fatalf("deferred helper upsert: %v", err)
		}
		if _, err := svc.OptimizeIndex(ctx, "docs", OptimizeIndexRequest{}); err != nil {
			t.Fatalf("optimize helper index: %v", err)
		}
		os.Exit(0)
	}

	dir = t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestServiceDeferredVectorBuildOptimizeCheckpointCrashReopen$")
	cmd.Env = append(os.Environ(), helper+"="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v\n%s", err, output)
	}
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.BackgroundIndexVacuumInterval = -1
	backend, cleanup, _, maintenance, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = cleanup() })
	svc := NewWithDeferredVectorBuildMaintenance(collections.NewCollectionManager(backend), maintenance)
	col, _, err := svc.openIndex(context.Background(), "docs", 0)
	if err != nil {
		t.Fatalf("open optimized index after crash: %v", err)
	}
	if got, err := col.Get([]byte("durable")); err != nil || got == nil {
		t.Fatalf("get after crash got=%v err=%v", got, err)
	}
	status, err := col.VectorIndexStatus(defaultVectorIndexName)
	if err != nil || !status.Loaded || status.RebuildNeeded || status.Stats.RebuildNeeded || status.Stats.SnapshotDirty {
		t.Fatalf("vector status after crash=%+v err=%v", status, err)
	}
}

func TestServiceStandardOptimizeCheckpointCrashReopen(t *testing.T) {
	const helper = "TREEDB_STANDARD_OPTIMIZE_CRASH_HELPER"
	dir := os.Getenv(helper)
	if dir != "" {
		db, err := backenddb.Open(testBackendOptions(dir))
		if err != nil {
			t.Fatalf("open helper: %v", err)
		}
		svc := New(collections.NewCollectionManager(db))
		ctx := context.Background()
		if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2, Metric: MetricCosine}); err != nil {
			t.Fatalf("create helper index: %v", err)
		}
		if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "durable", Embedding: []float32{1, 0}}}, DeferVectorIndexRebuild: true}); err != nil {
			t.Fatalf("deferred helper upsert: %v", err)
		}
		if _, err := svc.OptimizeIndex(ctx, "docs", OptimizeIndexRequest{}); err != nil {
			t.Fatalf("optimize helper index: %v", err)
		}
		os.Exit(0)
	}

	dir = t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestServiceStandardOptimizeCheckpointCrashReopen$")
	cmd.Env = append(os.Environ(), helper+"="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v\n%s", err, output)
	}
	db, err := backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	svc := New(collections.NewCollectionManager(db))
	col, _, err := svc.openIndex(context.Background(), "docs", 0)
	if err != nil {
		t.Fatalf("open optimized index after crash: %v", err)
	}
	if got, err := col.Get([]byte("durable")); err != nil || got == nil {
		t.Fatalf("get after crash got=%v err=%v", got, err)
	}
	status, err := col.VectorIndexStatus(defaultVectorIndexName)
	if err != nil || !status.Loaded || status.RebuildNeeded || status.Stats.RebuildNeeded || status.Stats.SnapshotDirty {
		t.Fatalf("vector status after crash=%+v err=%v", status, err)
	}
}

func TestServiceDeferredVectorBuildFailedFinalizeReopensDurableAndStale(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.BackgroundIndexVacuumInterval = -1
	backend, cleanup, _, maintenance, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	svc := NewWithDeferredVectorBuildMaintenance(collections.NewCollectionManager(backend), maintenance)
	ctx := context.Background()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2, Metric: MetricCosine}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "prior", Embedding: []float32{1, 0}}}}); err != nil {
		t.Fatalf("seed prior graph: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{
		Documents:               []Document{{ID: "deferred", Embedding: []float32{0, 1}}},
		DeferVectorIndexRebuild: true,
	}); err != nil {
		t.Fatalf("deferred upsert: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := svc.OptimizeIndex(canceled, "docs", OptimizeIndexRequest{}); !errors.Is(err, context.Canceled) || ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("canceled OptimizeIndex err=%v code=%s", err, ErrorCodeOf(err))
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("service close: %v", err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopenedBackend, reopenedCleanup, _, reopenedMaintenance, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopenedCleanup() })
	reopened := NewWithDeferredVectorBuildMaintenance(collections.NewCollectionManager(reopenedBackend), reopenedMaintenance)
	count, err := reopened.CountDocuments(ctx, "docs", CountDocumentsRequest{})
	if err != nil || count.Count != 2 {
		t.Fatalf("reopen count=%+v err=%v want 2 durable documents", count, err)
	}
	col, _, err := reopened.openIndex(ctx, "docs", 0)
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	status, err := col.VectorIndexStatus(defaultVectorIndexName)
	if err != nil {
		t.Fatalf("reopen vector status: %v", err)
	}
	if status.Loaded || !status.RebuildNeeded {
		t.Fatalf("reopen vector status=%+v want fail-closed rebuild-needed status", status)
	}
}

func TestServiceSchemaValidationAndUnsupportedFilterErrors(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()

	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 0}); ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("CreateIndex dimension err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2, Metric: "manhattan"}); ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("CreateIndex metric err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "missing", Content: "missing embedding"}}}); ErrorCodeOf(err) != CodeInvalidRequest || !strings.Contains(err.Error(), "embedding") {
		t.Fatalf("missing embedding err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "bad", Embedding: []float32{1}}}}); ErrorCodeOf(err) != CodeInvalidRequest || !strings.Contains(err.Error(), "dimension") {
		t.Fatalf("dimension mismatch err=%v code=%s", err, ErrorCodeOf(err))
	}
	_, err := svc.CountDocuments(ctx, "docs", CountDocumentsRequest{Filter: &Filter{Field: "meta.repo", Operator: "LIKE", Value: "gomap"}})
	if ErrorCodeOf(err) != CodeInvalidRequest || !strings.Contains(err.Error(), "unsupported filter operator") {
		t.Fatalf("unsupported filter err=%v code=%s", err, ErrorCodeOf(err))
	}
	_, err = svc.CountDocuments(ctx, "docs", CountDocumentsRequest{Filter: &Filter{Field: "meta.repo", Operator: "in", Value: "gomap"}})
	if ErrorCodeOf(err) != CodeInvalidRequest || !strings.Contains(err.Error(), "array") {
		t.Fatalf("unsupported in operand err=%v code=%s", err, ErrorCodeOf(err))
	}
	_, err = svc.CountDocuments(ctx, "docs", CountDocumentsRequest{Filter: &Filter{Field: "meta.version", Operator: ">", Value: []any{1.0}}})
	if ErrorCodeOf(err) != CodeInvalidRequest || !strings.Contains(err.Error(), "numeric or string") {
		t.Fatalf("unsupported comparison operand on empty index err=%v code=%s", err, ErrorCodeOf(err))
	}
}

func TestServiceUpsertDeleteCountFilterRoundTrip(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	upsert, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{
		{ID: "a", Content: "alpha", Embedding: []float32{1, 0}, Meta: map[string]any{"repo": "gomap", "language": "go", "start_line": 10.0, "tags": []any{"core", "api"}}},
		{ID: "b", Content: "beta", Embedding: []float32{0, 1}, Meta: map[string]any{"repo": "gomap", "language": "python", "start_line": 30.0}},
		{ID: "c", Content: "gamma", Embedding: []float32{0.7, 0.7}, Meta: map[string]any{"repo": "other", "language": "go", "start_line": 50.0}},
	}})
	if err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	if upsert.Upserted != 3 || upsert.Inserted != 3 || upsert.Updated != 0 || !reflect.DeepEqual(upsert.IDs, []string{"a", "b", "c"}) {
		t.Fatalf("upsert=%+v", upsert)
	}
	if upsert.Index.Generation != info.Generation {
		t.Fatalf("generation changed: upsert=%d create=%d", upsert.Index.Generation, info.Generation)
	}
	count, err := svc.CountDocuments(ctx, "docs", CountDocumentsRequest{})
	if err != nil || count.Count != 3 {
		t.Fatalf("count all=%+v err=%v", count, err)
	}
	filteredCount, err := svc.CountDocuments(ctx, "docs", CountDocumentsRequest{Filter: &Filter{Operator: "AND", Conditions: []Filter{
		{Field: "meta.repo", Operator: "==", Value: "gomap"},
		{Field: "meta.start_line", Operator: ">=", Value: 20.0},
	}}})
	if err != nil || filteredCount.Count != 1 {
		t.Fatalf("filtered count=%+v err=%v", filteredCount, err)
	}
	listed, err := svc.FilterDocuments(ctx, "docs", FilterDocumentsRequest{Filter: &Filter{Field: "meta.language", Operator: "in", Value: []any{"go"}}, ReturnEmbedding: false})
	if err != nil {
		t.Fatalf("FilterDocuments: %v", err)
	}
	if listed.MatchedCount != 2 || len(listed.Documents) != 2 || listed.Documents[0].ID != "a" || listed.Documents[1].ID != "c" {
		t.Fatalf("listed=%+v", listed)
	}
	if listed.Documents[0].Embedding != nil {
		t.Fatalf("embedding returned despite return_embedding=false: %+v", listed.Documents[0].Embedding)
	}
	if _, ok := listed.Documents[0].Meta["repo"]; !ok {
		t.Fatalf("metadata missing from listed document: %+v", listed.Documents[0].Meta)
	}
	updated, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Content: "alpha updated", Embedding: []float32{1, 0}, Meta: map[string]any{"repo": "gomap", "language": "go", "start_line": 11.0}}}})
	if err != nil {
		t.Fatalf("Upsert update: %v", err)
	}
	if updated.Inserted != 0 || updated.Updated != 1 {
		t.Fatalf("updated=%+v", updated)
	}
	deleted, err := svc.DeleteDocuments(ctx, "docs", DeleteDocumentsRequest{IDs: []string{"b"}})
	if err != nil || deleted.Deleted != 1 || !reflect.DeepEqual(deleted.IDs, []string{"b"}) {
		t.Fatalf("delete id=%+v err=%v", deleted, err)
	}
	deleted, err = svc.DeleteDocuments(ctx, "docs", DeleteDocumentsRequest{Filter: &Filter{Field: "meta.repo", Operator: "!=", Value: "gomap"}})
	if err != nil || deleted.Deleted != 1 || !reflect.DeepEqual(deleted.IDs, []string{"c"}) {
		t.Fatalf("delete filter=%+v err=%v", deleted, err)
	}
	count, err = svc.CountDocuments(ctx, "docs", CountDocumentsRequest{})
	if err != nil || count.Count != 1 {
		t.Fatalf("final count=%+v err=%v", count, err)
	}
}

func TestServiceUnfilteredCountHonorsCancellation(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Embedding: []float32{1, 0}}}}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	// openIndex and the pre-scan guard consume two checks; the ID callback consumes the third.
	cancelOnScan := &cancelOnErrContext{Context: canceled, cancel: cancel, cancelAt: 3}
	if _, err := svc.CountDocuments(cancelOnScan, "docs", CountDocumentsRequest{}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("CountDocuments err=%v code=%s", err, ErrorCodeOf(err))
	}
	if cancelOnScan.checks != cancelOnScan.cancelAt {
		t.Fatalf("context checks=%d want=%d", cancelOnScan.checks, cancelOnScan.cancelAt)
	}
}

type cancelOnErrContext struct {
	context.Context
	checks   int
	cancel   context.CancelFunc
	cancelAt int
}

func (c *cancelOnErrContext) Err() error {
	c.checks++
	if c.checks == c.cancelAt {
		c.cancel()
	}
	return c.Context.Err()
}

func TestServiceUpsertInsertRaceFallsBackToReplace(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	col, _, err := svc.openIndex(ctx, "docs", 0)
	if err != nil {
		t.Fatalf("openIndex: %v", err)
	}
	first, err := prepareDocumentsForWrite([]Document{{ID: "race", Content: "first", Embedding: []float32{1, 0}}}, info)
	if err != nil {
		t.Fatalf("prepare first: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte(first[0].id)}, [][]byte{first[0].raw}); err != nil {
		t.Fatalf("InsertBatch first: %v", err)
	}
	raced, err := prepareDocumentsForWrite([]Document{{ID: "race", Content: "winner", Embedding: []float32{0, 1}, Meta: map[string]any{"repo": "gomap"}}}, info)
	if err != nil {
		t.Fatalf("prepare raced: %v", err)
	}
	inserted, updated, err := upsertPreparedDocument(ctx, col, raced[0], true)
	if err != nil {
		t.Fatalf("upsertPreparedDocument raced insert: %v", err)
	}
	if inserted || !updated {
		t.Fatalf("inserted=%v updated=%v, want replace fallback", inserted, updated)
	}
	listed, err := svc.FilterDocuments(ctx, "docs", FilterDocumentsRequest{ReturnEmbedding: true})
	if err != nil {
		t.Fatalf("FilterDocuments: %v", err)
	}
	if len(listed.Documents) != 1 || listed.Documents[0].Content != "winner" || !reflect.DeepEqual(listed.Documents[0].Embedding, []float32{0, 1}) {
		t.Fatalf("listed after raced upsert=%+v", listed)
	}
}

func TestServiceConcurrentSameIDUpsertsSucceed(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	const workers = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{
				ID:        "same-id",
				Content:   fmt.Sprintf("writer-%02d", i),
				Embedding: []float32{1, float32(i + 1)},
				Meta:      map[string]any{"writer": float64(i)},
			}}})
			if err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent upsert returned error: %v", err)
	}
	count, err := svc.CountDocuments(ctx, "docs", CountDocumentsRequest{})
	if err != nil {
		t.Fatalf("CountDocuments: %v", err)
	}
	if count.Count != 1 {
		t.Fatalf("count=%d want 1", count.Count)
	}
	search, err := svc.SearchDenseVector(ctx, "docs", DenseVectorSearchRequest{QueryEmbedding: []float32{1, 1}, TopK: 1, ReturnEmbedding: true, Route: RouteExact})
	if err != nil {
		t.Fatalf("SearchDenseVector: %v", err)
	}
	if len(search.Documents) != 1 || search.Documents[0].ID != "same-id" || len(search.Documents[0].Embedding) != 2 {
		t.Fatalf("search after concurrent upsert=%+v", search)
	}
}

func TestServiceConcurrentFilterDeleteAndUpsertPreservesReplacement(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	for i := 0; i < 25; i++ {
		id := fmt.Sprintf("race-%02d", i)
		if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{
			ID:        id,
			Content:   "matching before delete",
			Embedding: []float32{1, 0},
			Meta:      map[string]any{"repo": "gomap"},
		}}}); err != nil {
			t.Fatalf("initial UpsertDocuments: %v", err)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, err := svc.DeleteDocuments(ctx, "docs", DeleteDocumentsRequest{Filter: &Filter{Field: "meta.repo", Operator: "==", Value: "gomap"}})
			errs <- err
		}()
		go func() {
			defer wg.Done()
			<-start
			_, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{
				ID:        id,
				Content:   "replacement should survive",
				Embedding: []float32{0, 1},
				Meta:      map[string]any{"repo": "other"},
			}}})
			errs <- err
		}()
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent delete/upsert error: %v", err)
			}
		}

		listed, err := svc.FilterDocuments(ctx, "docs", FilterDocumentsRequest{Filter: &Filter{Field: "id", Operator: "==", Value: id}})
		if err != nil {
			t.Fatalf("FilterDocuments final: %v", err)
		}
		if len(listed.Documents) != 1 || listed.Documents[0].Meta["repo"] != "other" {
			t.Fatalf("final document for %s=%+v, want surviving replacement", id, listed.Documents)
		}
	}
}

func TestServiceDenseVectorSearchStableIDsScoresMetadataAndEmbeddingEcho(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	_, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{
		{ID: "doc-a", Content: "a", Embedding: []float32{1, 0}, Meta: map[string]any{"repo": "gomap", "path": "a.go"}},
		{ID: "doc-aa", Content: "aa", Embedding: []float32{1, 0}, Meta: map[string]any{"repo": "gomap", "path": "aa.go"}},
		{ID: "doc-b", Content: "b", Embedding: []float32{0.8, 0.2}, Meta: map[string]any{"repo": "gomap", "path": "b.go"}},
		{ID: "doc-c", Content: "c", Embedding: []float32{0, 1}, Meta: map[string]any{"repo": "other", "path": "c.go"}},
	}})
	if err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	res, err := svc.SearchDenseVector(ctx, "docs", DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 3, Filter: &Filter{Field: "meta.repo", Operator: "==", Value: "gomap"}, ReturnEmbedding: false})
	if err != nil {
		t.Fatalf("SearchDenseVector: %v", err)
	}
	if !res.Exact || res.Candidates != 3 || len(res.Documents) != 3 {
		t.Fatalf("search response=%+v", res)
	}
	gotIDs := []string{res.Documents[0].ID, res.Documents[1].ID, res.Documents[2].ID}
	wantIDs := []string{"doc-a", "doc-aa", "doc-b"}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("ids=%v want %v", gotIDs, wantIDs)
	}
	if res.Documents[0].Score == nil || math.Abs(*res.Documents[0].Score-1) > 1e-6 || res.Documents[1].Score == nil || math.Abs(*res.Documents[1].Score-1) > 1e-6 {
		t.Fatalf("top scores=%v %v", res.Documents[0].Score, res.Documents[1].Score)
	}
	if res.Documents[2].Score == nil || *res.Documents[2].Score >= 1 || *res.Documents[2].Score <= 0.9 {
		t.Fatalf("third score=%v", res.Documents[2].Score)
	}
	if res.Documents[0].Embedding != nil {
		t.Fatalf("embedding returned despite return_embedding=false")
	}
	if res.Documents[0].Meta["path"] != "a.go" {
		t.Fatalf("metadata=%+v", res.Documents[0].Meta)
	}
	withEmbedding, err := svc.SearchDenseVector(ctx, "docs", DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, ReturnEmbedding: true})
	if err != nil {
		t.Fatalf("SearchDenseVector echo: %v", err)
	}
	if !reflect.DeepEqual(withEmbedding.Documents[0].Embedding, []float32{1, 0}) {
		t.Fatalf("embedding echo=%v", withEmbedding.Documents[0].Embedding)
	}
}

func TestServiceKeywordSearchRankedLexicalResultsAndTieOrder(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if !info.Capabilities.KeywordSearch || !info.Capabilities.HybridSearch || info.TextIndexName != defaultTextIndexName || info.VectorIndexName != defaultVectorIndexName || info.Capabilities.KeywordMetadataFilters || info.Capabilities.HybridMetadataFilters {
		t.Fatalf("index capabilities/info=%+v", info)
	}
	_, err = svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{
		{ID: "doc-best", Content: "refund refund refund policy", Embedding: []float32{1, 0}, Meta: map[string]any{"repo": "gomap", "path": "best.go"}},
		{ID: "doc-ok", Content: "refund shipping", Embedding: []float32{0, 1}, Meta: map[string]any{"repo": "gomap", "path": "ok.go"}},
		{ID: "doc-other", Content: "shipping update", Embedding: []float32{0.5, 0.5}, Meta: map[string]any{"repo": "other"}},
		{ID: "tie-a", Content: "tie term", Embedding: []float32{1, 1}},
		{ID: "tie-b", Content: "tie term", Embedding: []float32{1, -1}},
	}})
	if err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	res, err := svc.SearchKeyword(ctx, "docs", KeywordSearchRequest{Query: "refund", TopK: 2})
	if err != nil {
		t.Fatalf("SearchKeyword: %v", err)
	}
	if len(res.Documents) != 2 || res.Documents[0].ID != "doc-best" || res.Documents[1].ID != "doc-ok" {
		t.Fatalf("keyword ids=%v want doc-best/doc-ok response=%+v", documentIDs(res.Documents), res)
	}
	if res.Documents[0].Score == nil || res.Documents[1].Score == nil || *res.Documents[0].Score <= *res.Documents[1].Score {
		t.Fatalf("keyword scores=%v %v want ranked lexical scores", res.Documents[0].Score, res.Documents[1].Score)
	}
	if res.Documents[0].Embedding != nil || res.Documents[0].Meta["path"] != "best.go" {
		t.Fatalf("keyword document mapping=%+v", res.Documents[0])
	}
	meta := searchMeta(t, res.Documents[0])
	if meta["type"] != "keyword" || meta["text_index"] != defaultTextIndexName || meta["rank"] != 1 {
		t.Fatalf("keyword explanation meta=%+v", meta)
	}
	if res.Stats.CandidatesReturned != 2 || res.Stats.PostingsScanned == 0 || res.Stats.FullDocumentScanFallbacks != 0 || res.Stats.FailClosed != 0 {
		t.Fatalf("keyword stats=%+v", res.Stats)
	}

	ties, err := svc.SearchKeyword(ctx, "docs", KeywordSearchRequest{Query: "tie", TopK: 2})
	if err != nil {
		t.Fatalf("SearchKeyword ties: %v", err)
	}
	if got := documentIDs(ties.Documents); !reflect.DeepEqual(got, []string{"tie-a", "tie-b"}) {
		t.Fatalf("tie ids=%v want stable ID order", got)
	}
	if limited, err := svc.SearchKeyword(ctx, "docs", KeywordSearchRequest{Query: "refund", TopK: 2, CandidateLimit: 1}); ErrorCodeOf(err) != CodeIndexUnavailable || limited.Stats.FailClosed == 0 {
		t.Fatalf("keyword candidate-limit response=%+v err=%v code=%s", limited, err, ErrorCodeOf(err))
	}
}

func TestServiceHybridSearchTextVectorAndOverlap(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	_, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{
		{ID: "shared", Content: "refund refund", Embedding: []float32{1, 0}, Meta: map[string]any{"kind": "shared"}},
		{ID: "text", Content: "refund policy", Embedding: []float32{0, 1}, Meta: map[string]any{"kind": "text"}},
		{ID: "vector", Content: "shipping update", Embedding: []float32{0.99, 0.01}, Meta: map[string]any{"kind": "vector"}},
		{ID: "background", Content: "other", Embedding: []float32{0, 1}, Meta: map[string]any{"kind": "background"}},
	}})
	if err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}

	textOnly, err := svc.SearchHybrid(ctx, "docs", HybridSearchRequest{Query: "refund", TopK: 2, TextCandidateLimit: 3})
	if err != nil {
		t.Fatalf("SearchHybrid text-only: %v", err)
	}
	if len(textOnly.Documents) != 2 || textOnly.Stats.VectorCandidatesReturned != 0 {
		t.Fatalf("text-only response=%+v stats=%+v", textOnly, textOnly.Stats)
	}
	if !searchMetaHasOnlySource(t, textOnly.Documents[0], "text") {
		t.Fatalf("text-only meta=%+v", searchMeta(t, textOnly.Documents[0]))
	}

	vectorOnly, err := svc.SearchHybrid(ctx, "docs", HybridSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 2, VectorCandidateLimit: 3, EfSearch: 8})
	if err != nil {
		t.Fatalf("SearchHybrid vector-only: %v", err)
	}
	if len(vectorOnly.Documents) != 2 || vectorOnly.Documents[0].ID != "shared" || vectorOnly.Stats.TextCandidatesReturned != 0 || vectorOnly.Stats.VectorCandidatesReturned == 0 {
		t.Fatalf("vector-only response=%+v stats=%+v", vectorOnly, vectorOnly.Stats)
	}
	if !searchMetaHasOnlySource(t, vectorOnly.Documents[0], "vector") {
		t.Fatalf("vector-only meta=%+v", searchMeta(t, vectorOnly.Documents[0]))
	}

	overlap, err := svc.SearchHybrid(ctx, "docs", HybridSearchRequest{Query: "refund", QueryEmbedding: []float32{1, 0}, TopK: 3, TextCandidateLimit: 3, VectorCandidateLimit: 3, EfSearch: 8})
	if err != nil {
		t.Fatalf("SearchHybrid overlap: %v", err)
	}
	if len(overlap.Documents) != 3 || overlap.Documents[0].ID != "shared" || overlap.Documents[0].Score == nil {
		t.Fatalf("overlap response=%+v", overlap)
	}
	meta := searchMeta(t, overlap.Documents[0])
	if meta["type"] != "hybrid" || meta["fusion_method"] != string(collections.HybridFusionMethodRRF) || !searchMetaHasSources(meta, "text", "vector") {
		t.Fatalf("overlap explanation meta=%+v", meta)
	}
	if overlap.Stats.FusionBoth == 0 || overlap.Stats.CollapseRejections != 0 || overlap.Stats.CollapseExhaustions != 0 || overlap.Stats.FullDocumentScanFallbacks != 0 || overlap.Stats.FailClosed != 0 || overlap.Plan.FusionMethod != collections.HybridFusionMethodRRF || overlap.Plan.MaxChunksPerParent != 0 {
		t.Fatalf("overlap stats=%+v plan=%+v", overlap.Stats, overlap.Plan)
	}
}

func TestServiceKeywordHybridFiltersAndMissingIndexesFailClosed(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Content: "refund", Embedding: []float32{1, 0}, Meta: map[string]any{"repo": "gomap"}}}}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	filter := &Filter{Field: "meta.repo", Operator: "==", Value: "gomap"}
	// v1alpha2: filters against undeclared meta fields fail closed with a
	// typed invalid_request instead of the legacy blanket 501.
	if _, err := svc.SearchKeyword(ctx, "docs", KeywordSearchRequest{Query: "refund", TopK: 1, Filter: filter}); ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("keyword filter err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.SearchHybrid(ctx, "docs", HybridSearchRequest{Query: "refund", TopK: 1, Filter: filter}); ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("hybrid filter err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Content: "refund updated", Embedding: []float32{0, 1}, Meta: map[string]any{"repo": "gomap"}}}}); err != nil {
		t.Fatalf("stale vector UpsertDocuments: %v", err)
	}
	if _, err := svc.SearchHybrid(ctx, "docs", HybridSearchRequest{QueryEmbedding: []float32{0, 1}, TopK: 1, EfSearch: 4}); ErrorCodeOf(err) != CodeIndexUnavailable && ErrorCodeOf(err) != CodeIndexStale {
		t.Fatalf("stale vector err=%v code=%s", err, ErrorCodeOf(err))
	}

	mgr := collections.NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "notext",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
			ColumnStore:    serviceColumnStoreConfig(2),
		},
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name:       defaultVectorIndexName,
			Field:      defaultEmbeddingField,
			Metric:     collections.VectorMetricCosine,
			Dimensions: 2,
			Strategy:   collections.VectorIndexStrategyColumnGraph,
		}},
	}); err != nil {
		t.Fatalf("CreateCollection notext: %v", err)
	}
	if _, err := svc.SearchKeyword(ctx, "notext", KeywordSearchRequest{Query: "refund", TopK: 1}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("missing text err=%v code=%s", err, ErrorCodeOf(err))
	}

	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name:    "novector",
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON},
		TextIndexes: []collections.TextIndexDefinition{{
			Name:   defaultTextIndexName,
			Fields: []collections.TextIndexField{{Field: defaultTextField}},
		}},
	}); err != nil {
		t.Fatalf("CreateCollection novector: %v", err)
	}
	if _, err := svc.SearchHybrid(ctx, "novector", HybridSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("missing vector err=%v code=%s", err, ErrorCodeOf(err))
	}
}

func TestServiceNonCosineHybridCapabilityFailsClosedButDenseAndKeywordWork(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "l2docs", Dimension: 2, Metric: MetricL2})
	if err != nil {
		t.Fatalf("CreateIndex l2docs: %v", err)
	}
	if info.Capabilities.HybridSearch {
		t.Fatalf("l2 capabilities=%+v want hybrid_search=false", info.Capabilities)
	}
	if _, err := svc.UpsertDocuments(ctx, "l2docs", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Content: "refund", Embedding: []float32{1, 0}}}}); err != nil {
		t.Fatalf("UpsertDocuments l2docs: %v", err)
	}
	keyword, err := svc.SearchKeyword(ctx, "l2docs", KeywordSearchRequest{Query: "refund", TopK: 1})
	if err != nil || len(keyword.Documents) != 1 || keyword.Documents[0].ID != "a" {
		t.Fatalf("SearchKeyword l2docs=%+v err=%v", keyword, err)
	}
	dense, err := svc.SearchDenseVector(ctx, "l2docs", DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1})
	if err != nil || len(dense.Documents) != 1 || dense.Documents[0].ID != "a" {
		t.Fatalf("SearchDenseVector l2docs=%+v err=%v", dense, err)
	}
	if _, err := svc.SearchHybrid(ctx, "l2docs", HybridSearchRequest{Query: "refund", TopK: 1}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("SearchHybrid l2 err=%v code=%s", err, ErrorCodeOf(err))
	}
}

func TestServiceOpenedIncompatibleTextVectorSchemasFailClosed(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	mgr := collections.NewCollectionManager(db)
	baseVector := collections.VectorIndexDefinition{
		Name:       defaultVectorIndexName,
		Field:      defaultEmbeddingField,
		Metric:     collections.VectorMetricCosine,
		Dimensions: 2,
		Encoding:   collections.VectorIndexEncodingFloat32,
		Strategy:   collections.VectorIndexStrategyNativeRuntime,
	}
	baseText := collections.TextIndexDefinition{
		Name:           defaultTextIndexName,
		Fields:         []collections.TextIndexField{{Field: defaultTextField}},
		Analyzer:       collections.TextAnalyzerSimple,
		StorePositions: true,
	}
	badText := baseText
	badText.Fields = []collections.TextIndexField{{Field: defaultTextField}, {Field: "title"}}
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name:          "badtext",
		Options:       collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON},
		VectorIndexes: []collections.VectorIndexDefinition{baseVector},
		TextIndexes:   []collections.TextIndexDefinition{badText},
	}); err != nil {
		t.Fatalf("CreateCollection badtext: %v", err)
	}
	if _, err := svc.OpenIndex(ctx, "badtext"); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("bad text schema err=%v code=%s", err, ErrorCodeOf(err))
	}

	badVector := baseVector
	badVector.Encoding = collections.VectorIndexEncodingInt8
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name:          "badvector",
		Options:       collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON},
		VectorIndexes: []collections.VectorIndexDefinition{badVector},
		TextIndexes:   []collections.TextIndexDefinition{baseText},
	}); err != nil {
		t.Fatalf("CreateCollection badvector: %v", err)
	}
	if _, err := svc.OpenIndex(ctx, "badvector"); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("bad vector schema err=%v code=%s", err, ErrorCodeOf(err))
	}
}

func TestServicePersistenceReopenDocumentsAndEmbeddings(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	svc := New(collections.NewCollectionManager(db))
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2}); err != nil {
		_ = db.Close()
		t.Fatalf("CreateIndex: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "persist", Content: "persistent", Embedding: []float32{1, 0}, Meta: map[string]any{"repo": "gomap"}}}}); err != nil {
		_ = db.Close()
		t.Fatalf("UpsertDocuments: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	reopened, err := backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()
	reopenedSvc := New(collections.NewCollectionManager(reopened))
	var scans atomic.Int64
	reopenedSvc.documentScanBefore = func() { scans.Add(1) }
	filtered, err := reopenedSvc.FilterDocuments(ctx, "docs", FilterDocumentsRequest{
		Filter:          &Filter{Field: "id", Operator: "==", Value: "persist"},
		Limit:           1,
		ReturnEmbedding: true,
	})
	if err != nil || scans.Load() != 0 || filtered.MatchedCount != 1 || len(filtered.Documents) != 1 ||
		filtered.Documents[0].ID != "persist" || !reflect.DeepEqual(filtered.Documents[0].Embedding, []float32{1, 0}) {
		t.Fatalf("reopened exact-ID filter=%+v scans=%d err=%v", filtered, scans.Load(), err)
	}
	count, err := reopenedSvc.CountDocuments(ctx, "docs", CountDocumentsRequest{})
	if err != nil || count.Count != 1 {
		t.Fatalf("reopened count=%+v err=%v", count, err)
	}
	search, err := reopenedSvc.SearchDenseVector(ctx, "docs", DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, ReturnEmbedding: true})
	if err != nil {
		t.Fatalf("reopened search: %v", err)
	}
	if len(search.Documents) != 1 || search.Documents[0].ID != "persist" || !reflect.DeepEqual(search.Documents[0].Embedding, []float32{1, 0}) {
		t.Fatalf("reopened search=%+v", search)
	}
}

func TestServiceErrorCasesDimensionStaleUnavailable(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Embedding: []float32{1, 0}}}}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	if _, err := svc.SearchDenseVector(ctx, "docs", DenseVectorSearchRequest{QueryEmbedding: []float32{1}, TopK: 1}); ErrorCodeOf(err) != CodeInvalidRequest || !strings.Contains(err.Error(), "dimension") {
		t.Fatalf("query dimension err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.CountDocuments(ctx, "docs", CountDocumentsRequest{ExpectedGeneration: info.Generation + 1}); ErrorCodeOf(err) != CodeIndexStale {
		t.Fatalf("stale err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.CountDocuments(ctx, "missing", CountDocumentsRequest{}); ErrorCodeOf(err) != CodeIndexNotFound {
		t.Fatalf("missing err=%v code=%s", err, ErrorCodeOf(err))
	}
	mgr := collections.NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "raw", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON}}); err != nil {
		t.Fatalf("CreateCollection raw: %v", err)
	}
	if _, err := svc.CountDocuments(ctx, "raw", CountDocumentsRequest{}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("unavailable err=%v code=%s", err, ErrorCodeOf(err))
	}
}

func TestServiceCreateIndexRejectsInvalidScalarU8Calibration2842(t *testing.T) {
	tests := []struct {
		name string
		q    QuantizedIndexInfo
	}{
		{
			name: "unsupported_mode",
			q: QuantizedIndexInfo{
				Name:  "embedding.scalar_u8.bad",
				Codec: collections.QuantizedVectorCodecScalarU8,
				ScalarU8Calibration: &collections.ScalarU8CalibrationConfig{
					Mode: "per_vector_alpha",
				},
			},
		},
		{
			name: "scalar_config_on_rabitq",
			q: QuantizedIndexInfo{
				Name:  "embedding.rabitq.bad",
				Codec: "rabitq_1bit",
				ScalarU8Calibration: &collections.ScalarU8CalibrationConfig{
					Mode: collections.ScalarU8CalibrationModeLegacy,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, db := newTestService(t)
			defer db.Close()
			_, err := svc.CreateIndex(context.Background(), CreateIndexRequest{
				Name:      "bench_" + tt.name,
				Dimension: 2,
				Metric:    MetricCosine,
				VectorIndexOptions: &BenchmarkVectorIndexOptions{
					Strategy:         collections.VectorIndexStrategyColumnGraph,
					QuantizedIndexes: []QuantizedIndexInfo{tt.q},
				},
			})
			if ErrorCodeOf(err) != CodeInvalidRequest || !strings.Contains(err.Error(), "scalar_u8_calibration") {
				t.Fatalf("CreateIndex err=%v code=%s want invalid_request scalar_u8_calibration", err, ErrorCodeOf(err))
			}
		})
	}
}

func TestServiceIndexCapabilitiesIncludeScalarU8PerGranuleAlphaRerank2844(t *testing.T) {
	base := collections.VectorIndexDefinition{
		Name:       defaultVectorIndexName,
		Field:      defaultEmbeddingField,
		Metric:     collections.VectorMetricCosine,
		Dimensions: 2,
		Encoding:   collections.VectorIndexEncodingFloat32,
		Strategy:   collections.VectorIndexStrategyColumnGraph,
	}
	alphaCalibration := &collections.ScalarU8CalibrationConfig{
		Mode:     collections.ScalarU8CalibrationModePerGranuleAlpha,
		Grouping: collections.ScalarU8CalibrationGroupingStorageLayoutGranule,
		AlphaPolicy: collections.ScalarU8AlphaPolicy{
			Name: collections.ScalarU8AlphaPolicyMaxAbs,
		},
	}
	tests := []struct {
		name                        string
		quantizedIndexes            []collections.QuantizedVectorIndexDefinition
		wantQuantizedRerank         bool
		wantScalarU8QuantizedRerank bool
	}{
		{
			name: "legacy_nil_config",
			quantizedIndexes: []collections.QuantizedVectorIndexDefinition{{
				Name:    "embedding.scalar_u8.fast",
				Codec:   collections.QuantizedVectorCodecScalarU8,
				Version: 1,
			}},
			wantQuantizedRerank:         true,
			wantScalarU8QuantizedRerank: true,
		},
		{
			name: "explicit_legacy_config",
			quantizedIndexes: []collections.QuantizedVectorIndexDefinition{{
				Name:    "embedding.scalar_u8.legacy",
				Codec:   collections.QuantizedVectorCodecScalarU8,
				Version: 1,
				ScalarU8Calibration: &collections.ScalarU8CalibrationConfig{
					Mode: collections.ScalarU8CalibrationModeLegacy,
				},
			}},
			wantQuantizedRerank:         true,
			wantScalarU8QuantizedRerank: true,
		},
		{
			name: "per_granule_alpha_scoring",
			quantizedIndexes: []collections.QuantizedVectorIndexDefinition{{
				Name:                "embedding.scalar_u8.alpha",
				Codec:               collections.QuantizedVectorCodecScalarU8,
				Version:             1,
				ScalarU8Calibration: alphaCalibration,
			}},
			wantQuantizedRerank:         true,
			wantScalarU8QuantizedRerank: true,
		},
		{
			name: "alpha_scalar_u8_and_rabitq_rerank",
			quantizedIndexes: []collections.QuantizedVectorIndexDefinition{
				{
					Name:                "embedding.scalar_u8.alpha",
					Codec:               collections.QuantizedVectorCodecScalarU8,
					Version:             1,
					ScalarU8Calibration: alphaCalibration,
				},
				{
					Name:    "embedding.rabitq_1bit.experimental",
					Codec:   "rabitq_1bit",
					Version: 1,
				},
			},
			wantQuantizedRerank:         true,
			wantScalarU8QuantizedRerank: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := base
			def.QuantizedIndexes = tt.quantizedIndexes
			got := indexCapabilities(def, true)
			if got.QuantizedRerank != tt.wantQuantizedRerank || got.ScalarU8QuantizedRerank != tt.wantScalarU8QuantizedRerank {
				t.Fatalf("capabilities=%+v want quantized_rerank=%t scalar_u8_quantized_rerank=%t", got, tt.wantQuantizedRerank, tt.wantScalarU8QuantizedRerank)
			}
		})
	}
}

func TestServiceUpsertScalarU8PerGranuleAlphaDefaultBuildsAssets2843(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	_, err := svc.CreateIndex(ctx, CreateIndexRequest{
		Name:      "bench_alpha_default",
		Dimension: 2,
		Metric:    MetricCosine,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyColumnGraph,
			QuantizedIndexes: []QuantizedIndexInfo{{
				Name:  "embedding.scalar_u8.alpha",
				Codec: collections.QuantizedVectorCodecScalarU8,
				ScalarU8Calibration: &collections.ScalarU8CalibrationConfig{
					Mode:     collections.ScalarU8CalibrationModePerGranuleAlpha,
					Grouping: collections.ScalarU8CalibrationGroupingStorageLayoutGranule,
					AlphaPolicy: collections.ScalarU8AlphaPolicy{
						Name: collections.ScalarU8AlphaPolicyMaxAbs,
					},
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("CreateIndex alpha: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "bench_alpha_default", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Embedding: []float32{1, 0}}}}); err != nil {
		t.Fatalf("default UpsertDocuments alpha: %v", err)
	}
	count, err := svc.CountDocuments(ctx, "bench_alpha_default", CountDocumentsRequest{})
	if err != nil || count.Count != 1 {
		t.Fatalf("CountDocuments after default alpha upsert=%+v err=%v want 1", count, err)
	}
}

func TestServiceOptimizeScalarU8PerGranuleAlphaBuildsAssets2843(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	_, err := svc.CreateIndex(ctx, CreateIndexRequest{
		Name:      "bench_alpha",
		Dimension: 2,
		Metric:    MetricCosine,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyColumnGraph,
			QuantizedIndexes: []QuantizedIndexInfo{{
				Name:  "embedding.scalar_u8.alpha",
				Codec: collections.QuantizedVectorCodecScalarU8,
				ScalarU8Calibration: &collections.ScalarU8CalibrationConfig{
					Mode:     collections.ScalarU8CalibrationModePerGranuleAlpha,
					Grouping: collections.ScalarU8CalibrationGroupingStorageLayoutGranule,
					AlphaPolicy: collections.ScalarU8AlphaPolicy{
						Name: collections.ScalarU8AlphaPolicyMaxAbs,
					},
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("CreateIndex alpha: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "bench_alpha", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Embedding: []float32{1, 0}}}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatalf("deferred UpsertDocuments alpha: %v", err)
	}
	count, err := svc.CountDocuments(ctx, "bench_alpha", CountDocumentsRequest{})
	if err != nil || count.Count != 1 {
		t.Fatalf("CountDocuments after deferred alpha upsert=%+v err=%v want 1", count, err)
	}
	if _, err := svc.OptimizeIndex(ctx, "bench_alpha", OptimizeIndexRequest{}); err != nil {
		t.Fatalf("OptimizeIndex alpha: %v", err)
	}
}

func TestReconcileOptimizeIndexTimingCoarseClock(t *testing.T) {
	timing := OptimizeIndexTiming{TotalNanos: 100, CacheInvalidateNanos: 1, RebuildNanos: 100, CachePrimeNanos: 1, CacheWarmNanos: 20}
	reconcileOptimizeIndexTiming(&timing, VectorIndexMaintenanceStatus{DurationNanos: 110})
	if timing.RebuildNanos != 110 || timing.TotalNanos != 132 {
		t.Fatalf("reconciled optimize timing=%+v want rebuild/total=110/132", timing)
	}
}

func TestServiceBenchmarkLifecycleResetOptimizeAndNoDocumentSearch(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	vectorOptions := &BenchmarkVectorIndexOptions{
		Strategy: collections.VectorIndexStrategyColumnGraph,
		M:        4,
		EfSearch: 8,
		QuantizedIndexes: []QuantizedIndexInfo{{
			Name:  "embedding.scalar_u8.fast",
			Codec: collections.QuantizedVectorCodecScalarU8,
		}},
	}

	brqOptions := &BenchmarkVectorIndexOptions{
		Strategy: collections.VectorIndexStrategyColumnGraph,
		QuantizedIndexes: []QuantizedIndexInfo{{
			Name:  "embedding.brq_1bit.experimental",
			Codec: "brq_1bit",
		}},
	}
	if _, err := svc.ResetIndex(ctx, "bench_brq", ResetIndexRequest{Dimension: 2, Metric: MetricCosine, DropOld: true, VectorIndexOptions: brqOptions}); ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("ResetIndex brq_1bit err=%v code=%s", err, ErrorCodeOf(err))
	}

	reset, err := svc.ResetIndex(ctx, "bench", ResetIndexRequest{Dimension: 2, Metric: MetricCosine, DropOld: true, VectorIndexOptions: vectorOptions})
	if err != nil {
		t.Fatalf("ResetIndex create: %v", err)
	}
	if !reset.Created || reset.Reset || reset.Index.VectorStrategy != collections.VectorIndexStrategyColumnGraph || !reset.Index.Capabilities.BenchmarkLifecycle || !reset.Index.Capabilities.ExactColumnGraphSearch || !reset.Index.Capabilities.ScalarU8QuantizedRerank {
		t.Fatalf("reset create response=%+v capabilities=%+v", reset, reset.Index.Capabilities)
	}
	loadBenchmarkDocs(t, svc, "bench", []Document{
		{ID: "old-a", Content: "old alpha", Embedding: []float32{1, 0}},
		{ID: "old-b", Content: "old beta", Embedding: []float32{0, 1}},
	})
	optimize, err := svc.OptimizeIndex(ctx, "bench", OptimizeIndexRequest{})
	if err != nil {
		t.Fatalf("OptimizeIndex first: %v", err)
	}
	if !optimize.Status.Loaded || optimize.Status.RebuildNeeded {
		t.Fatalf("optimize status=%+v", optimize.Status)
	}
	// Cache invalidation, priming, and warming can complete within one clock tick on
	// platforms with coarse monotonic-clock resolution.
	if optimize.Timing.TotalNanos == 0 || optimize.Timing.RebuildNanos == 0 {
		t.Fatalf("optimize timing=%+v want measurable total and rebuild stages", optimize.Timing)
	}
	build := optimize.Status.ColumnGraphBuild
	if build.TotalNanos == 0 || build.SnapshotNanos == 0 || build.RowExtractionNanos == 0 || build.AdjacencyBuildNanos == 0 || build.LocalityRemapNanos == 0 || build.AssetPreparationNanos == 0 || build.InvNormPreparationNanos == 0 || build.AdjacencyStatePreparationNanos == 0 || build.RowRefPreparationNanos == 0 || build.DocumentIDPreparationNanos == 0 || build.QuantizedPreparationNanos == 0 || build.SearchPackPreparationNanos == 0 || build.ManifestFinalizationNanos == 0 || build.FileSyncNanos == 0 || build.FileSyncCount == 0 || build.NamespaceSyncCount == 0 || build.PublicationNanos == 0 {
		t.Fatalf("column graph build timing=%+v want completed ordered stages", build)
	}
	assetChildren := build.InvNormPreparationNanos + build.AdjacencyStatePreparationNanos + build.RowRefPreparationNanos + build.DocumentIDPreparationNanos + build.QuantizedPreparationNanos + build.SearchPackPreparationNanos + build.ManifestFinalizationNanos
	if build.AssetPreparationNanos < assetChildren || build.PublicationNanos < build.AssetPreparationNanos {
		t.Fatalf("column graph nested timing=%+v asset_children=%d", build, assetChildren)
	}
	buildChildren := build.SnapshotNanos + build.RowExtractionNanos + build.AdjacencyBuildNanos + build.LocalityRemapNanos + build.PublicationNanos
	if build.TotalNanos < buildChildren || optimize.Status.DurationNanos != build.TotalNanos || optimize.Timing.RebuildNanos < build.TotalNanos {
		t.Fatalf("column graph timing=%+v optimize=%+v build_children=%d", build, optimize.Timing, buildChildren)
	}
	optimizeChildren := optimize.Timing.CacheInvalidateNanos + optimize.Timing.RebuildNanos + optimize.Timing.CachePrimeNanos + optimize.Timing.CacheWarmNanos
	if optimize.Timing.TotalNanos < optimizeChildren {
		t.Fatalf("optimize timing=%+v children=%d", optimize.Timing, optimizeChildren)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	failed, err := svc.OptimizeIndex(canceled, "bench", OptimizeIndexRequest{})
	if err == nil {
		t.Fatal("canceled OptimizeIndex err=nil")
	}
	if failed.Timing != (OptimizeIndexTiming{}) || failed.Status.Name != "" || failed.VectorIndexName != "" {
		t.Fatalf("canceled OptimizeIndex response=%+v must not report success", failed)
	}
	exact, err := svc.SearchBenchmarkVector(ctx, "bench", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 2, EfSearch: 8})
	if err != nil {
		t.Fatalf("SearchBenchmarkVector exact: %v", err)
	}
	if !exact.NoDocuments || exact.QueryMode != BenchmarkVectorQueryModeExact || len(exact.Results) != 2 || exact.Results[0].ID != "old-a" || exact.Stats.DocumentsFetched != 0 || exact.Diagnostics.Route != collections.VectorIndexSearchRouteExactHNSWSearchPackV1 || !exact.Diagnostics.NoDocumentGuardrailsOK {
		t.Fatalf("exact benchmark response=%+v stats=%+v diagnostics=%+v", exact, exact.Stats, exact.Diagnostics)
	}
	reranked, err := svc.SearchBenchmarkVector(ctx, "bench", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 2, EfSearch: 8, QueryMode: BenchmarkVectorQueryModeQuantizedRerank, QuantizedIndexName: "embedding.scalar_u8.fast", QuantizedRerankCandidates: 32})
	if err != nil {
		t.Fatalf("SearchBenchmarkVector quantized_rerank: %v", err)
	}
	if reranked.Diagnostics.Route != collections.VectorIndexSearchRouteQuantizedRerank || reranked.Stats.QuantizedScorerActive != 1 || reranked.Stats.QuantizedRerankExactScoreCalls == 0 || reranked.Stats.DocumentsFetched != 0 {
		t.Fatalf("quantized_rerank response=%+v stats=%+v diagnostics=%+v", reranked, reranked.Stats, reranked.Diagnostics)
	}

	if _, err := svc.ResetIndex(ctx, "bench", ResetIndexRequest{Dimension: 2, Metric: MetricCosine, DropOld: true, VectorIndexOptions: vectorOptions}); ErrorCodeOf(err) != CodeUnsupported {
		t.Fatalf("ResetIndex existing column_graph err=%v code=%s", err, ErrorCodeOf(err))
	}
}

// BenchmarkVectorIndexMaintenanceStatusTimingCopy bounds the response-only
// timing projection; rebuild work itself is intentionally outside this loop.
func BenchmarkVectorIndexMaintenanceStatusTimingCopy(b *testing.B) {
	status := collections.VectorIndexStatus{
		Name:     "embedding",
		Strategy: collections.VectorIndexStrategyColumnGraph,
		ColumnGraphBuild: collections.ColumnGraphBuildTiming{
			Total: time.Second, Snapshot: time.Nanosecond, RowExtraction: time.Nanosecond,
			AdjacencyBuild: time.Nanosecond, LocalityRemap: time.Nanosecond, AssetPreparation: time.Nanosecond,
			InvNormPreparation: time.Nanosecond, AdjacencyStatePreparation: time.Nanosecond,
			RowRefPreparation: time.Nanosecond, DocumentIDPreparation: time.Nanosecond,
			QuantizedPreparation: time.Nanosecond, SearchPackPreparation: time.Nanosecond,
			ManifestFinalization: time.Nanosecond, FileSync: time.Nanosecond, FileSyncCount: 1,
			NamespaceSync: time.Nanosecond, NamespaceSyncCount: 1, Publication: time.Nanosecond,
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = vectorIndexMaintenanceStatus(status)
	}
}

func TestServiceBenchmarkResetDeletesCompatibleNativeIndex(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	vectorOptions := &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime}
	if _, err := svc.ResetIndex(ctx, "nativebench", ResetIndexRequest{Dimension: 2, Metric: MetricCosine, DropOld: true, VectorIndexOptions: vectorOptions}); err != nil {
		t.Fatalf("ResetIndex create native: %v", err)
	}
	loadBenchmarkDocs(t, svc, "nativebench", []Document{
		{ID: "old-a", Content: "old alpha", Embedding: []float32{1, 0}},
		{ID: "old-b", Content: "old beta", Embedding: []float32{0, 1}},
	})
	reset, err := svc.ResetIndex(ctx, "nativebench", ResetIndexRequest{Dimension: 2, Metric: MetricCosine, DropOld: true, VectorIndexOptions: vectorOptions})
	if err != nil {
		t.Fatalf("ResetIndex existing native: %v", err)
	}
	if reset.Created || !reset.Reset || reset.DroppedDocuments != 2 {
		t.Fatalf("reset existing native response=%+v", reset)
	}
	count, err := svc.CountDocuments(ctx, "nativebench", CountDocumentsRequest{})
	if err != nil || count.Count != 0 {
		t.Fatalf("count after native reset=%+v err=%v", count, err)
	}
}

func TestServiceBenchmarkNativeRuntimeLiveMutationRoute(t *testing.T) {
	ctx := context.Background()
	svc, db := newTestService(t)
	defer db.Close()
	vectorOptions := &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime}
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "native", Dimension: 2, Metric: MetricCosine, VectorIndexOptions: vectorOptions})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if !info.Capabilities.NoDocumentVectorSearch {
		t.Fatalf("capabilities=%+v want native no-document search", info.Capabilities)
	}
	upsert := func(docs ...Document) {
		t.Helper()
		if _, err := svc.UpsertDocuments(ctx, "native", UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
			t.Fatalf("UpsertDocuments: %v: %v", err, errors.Unwrap(err))
		}
	}
	search := func(query []float32) BenchmarkVectorSearchResponse {
		t.Helper()
		got, err := svc.SearchBenchmarkVector(ctx, "native", BenchmarkVectorSearchRequest{QueryEmbedding: query, TopK: 2, EfSearch: 8, StatsMode: collections.VectorIndexSearchStatsModeProduction})
		if err != nil {
			t.Fatalf("SearchBenchmarkVector: %v", err)
		}
		if got.Diagnostics.Route != collections.VectorIndexSearchRouteNativeRuntime || !got.Diagnostics.LiveANN.Enabled || got.Diagnostics.LiveANN.ExactFallbacks != 0 || got.Diagnostics.LiveANN.FullRebuilds != 0 || !got.NoDocuments || got.Stats.DocumentsFetched != 0 {
			t.Fatalf("native response=%+v", got)
		}
		return got
	}

	upsert(Document{ID: "a", Embedding: []float32{1, 0}}, Document{ID: "b", Embedding: []float32{0, 1}})
	for _, mode := range []collections.VectorIndexSearchStatsMode{collections.VectorIndexSearchStatsModeDefault, collections.VectorIndexSearchStatsModeFullDiagnostics, collections.VectorIndexSearchStatsModeWorkAccounting} {
		if _, err := svc.SearchBenchmarkVector(ctx, "native", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, StatsMode: mode}); ErrorCodeOf(err) != CodeIndexUnavailable {
			t.Fatalf("native stats mode %q err=%v code=%s want unavailable until instrumented", mode, err, ErrorCodeOf(err))
		}
	}
	if got := search([]float32{1, 0}); len(got.Results) != 2 || got.Results[0].ID != "a" {
		t.Fatalf("insert results=%+v want a first", got.Results)
	}
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "native", Dimension: 2, Metric: MetricCosine, VectorIndexOptions: vectorOptions}); err != nil {
		t.Fatalf("compatible CreateIndex: %v", err)
	}
	if got := search([]float32{1, 0}); len(got.Results) != 2 || got.Results[0].ID != "a" {
		t.Fatalf("compatible create results=%+v want live handle preserved", got.Results)
	}
	upsert(Document{ID: "a", Embedding: []float32{-1, 0}})
	if got := search([]float32{1, 0}); len(got.Results) != 2 || got.Results[0].ID == "a" {
		t.Fatalf("update results=%+v want replacement excluded from old-vector top hit", got.Results)
	}
	if _, err := svc.DeleteDocuments(ctx, "native", DeleteDocumentsRequest{IDs: []string{"a"}}); err != nil {
		t.Fatalf("DeleteDocuments: %v", err)
	}
	if got := search([]float32{-1, 0}); len(got.Results) != 1 || got.Results[0].ID != "b" {
		t.Fatalf("delete results=%+v want only b", got.Results)
	}
}

func TestServiceDenseNativeRuntimeOrdinaryRouteLifecycleAndReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	open := func() (*Service, *backenddb.DB) {
		t.Helper()
		db, err := backenddb.Open(testBackendOptions(dir))
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		return New(collections.NewCollectionManager(db)), db
	}
	svc, db := open()
	create := CreateIndexRequest{
		Name:               "native_dense",
		Dimension:          2,
		Metric:             MetricCosine,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime},
	}
	info, err := svc.CreateIndex(ctx, create)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if !info.Capabilities.NoDocumentVectorSearch || info.Capabilities.ColumnGraphVectorSearch {
		t.Fatalf("native capabilities=%+v", info.Capabilities)
	}
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{
		Documents: []Document{
			{ID: "a", Content: "old-a", Embedding: []float32{1, 0}},
			{ID: "b", Content: "b", Embedding: []float32{0, 1}},
		},
		DeferVectorIndexRebuild: true,
	}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	assertSearch := func(route Route, query []float32, topK int, wantIDs ...string) DenseVectorSearchResponse {
		t.Helper()
		got, err := svc.SearchDenseVector(ctx, create.Name, DenseVectorSearchRequest{
			QueryEmbedding:  query,
			TopK:            topK,
			Route:           route,
			EfSearch:        8,
			ReturnEmbedding: true,
		})
		if err != nil {
			t.Fatalf("SearchDenseVector route=%q: %v", route, err)
		}
		if got.Route != RouteAnn || got.Exact || got.Candidates != len(got.Documents) || len(got.Documents) != len(wantIDs) {
			t.Fatalf("dense native response=%+v want ids=%v", got, wantIDs)
		}
		for i, want := range wantIDs {
			if got.Documents[i].ID != want || got.Documents[i].Score == nil || len(got.Documents[i].Embedding) != 2 {
				t.Fatalf("dense native document[%d]=%+v want id=%q score and embedding", i, got.Documents[i], want)
			}
		}
		return got
	}
	defaultRoute := assertSearch("", []float32{1, 0}, 2, "a", "b")
	explicitRoute := assertSearch(RouteAnn, []float32{1, 0}, 2, "a", "b")
	if math.Abs(*defaultRoute.Documents[0].Score-*explicitRoute.Documents[0].Score) > 1e-9 {
		t.Fatalf("default and explicit ANN score mismatch: default=%v explicit=%v", *defaultRoute.Documents[0].Score, *explicitRoute.Documents[0].Score)
	}
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{
		Documents:               []Document{{ID: "a", Content: "new-a", Embedding: []float32{-1, 0}}},
		DeferVectorIndexRebuild: true,
	}); err != nil {
		t.Fatalf("update a: %v", err)
	}
	updated := assertSearch("", []float32{-1, 0}, 1, "a")
	if updated.Documents[0].Content != "new-a" {
		t.Fatalf("updated document=%+v", updated.Documents[0])
	}
	if _, err := svc.DeleteDocuments(ctx, create.Name, DeleteDocumentsRequest{IDs: []string{"a"}}); err != nil {
		t.Fatalf("DeleteDocuments: %v", err)
	}
	assertSearch("", []float32{-1, 0}, 1, "b")
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("close service: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	svc, db = open()
	defer func() {
		_ = svc.Close()
		_ = db.Close()
	}()
	reopened := assertSearch("", []float32{0, 1}, 1, "b")
	if reopened.Documents[0].Content != "b" {
		t.Fatalf("reopened document=%+v", reopened.Documents[0])
	}
}

func TestServiceDenseNativeRuntimeSearchMaterializesOneStableSnapshot(t *testing.T) {
	ctx := context.Background()
	svc, db := newTestService(t)
	defer db.Close()
	create := CreateIndexRequest{
		Name:               "native_snapshot",
		Dimension:          2,
		Metric:             MetricCosine,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime},
	}
	info, err := svc.CreateIndex(ctx, create)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{
		Documents:               []Document{{ID: "a", Content: "v0", Embedding: []float32{1, 0}}},
		DeferVectorIndexRebuild: true,
	}); err != nil {
		t.Fatalf("initial UpsertDocuments: %v", err)
	}
	col, _, err := svc.openIndex(ctx, create.Name, 0)
	if err != nil {
		t.Fatalf("openIndex: %v", err)
	}
	replace := func(doc Document) error {
		prepared, err := prepareDocumentsForWrite([]Document{doc}, info)
		if err != nil {
			return err
		}
		matched, err := col.Replace([]byte(doc.ID), prepared[0].raw)
		if err == nil && !matched {
			return fmt.Errorf("document %q disappeared before replacement", doc.ID)
		}
		return err
	}
	hookCalls := 0
	svc.denseVectorNativeAfterSearch = func(attempt int, search collections.VectorIndexSearchResponse) error {
		hookCalls++
		diagnostics := search.Diagnostics()
		if attempt != 0 || diagnostics.Route != collections.VectorIndexSearchRouteNativeRuntime ||
			!diagnostics.LiveANN.Enabled || diagnostics.LiveANN.ExactFallbacks != 0 ||
			diagnostics.LiveANN.FullRebuilds != 0 || search.Stats.SearchRouteNativeRuntime != 1 {
			return fmt.Errorf("unexpected native attempt=%d response=%+v diagnostics=%+v", attempt, search, diagnostics)
		}
		return replace(Document{ID: "a", Content: "v1", Embedding: []float32{0, 1}})
	}
	got, err := svc.SearchDenseVector(ctx, create.Name, DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, ReturnEmbedding: true})
	if err != nil || hookCalls != 1 || len(got.Documents) != 1 ||
		got.Documents[0].Content != "v0" ||
		!reflect.DeepEqual(got.Documents[0].Embedding, []float32{1, 0}) ||
		got.Documents[0].Score == nil || math.Abs(*got.Documents[0].Score-1) > 1e-9 ||
		got.VisibilityMismatchCount != 0 || got.VisibilityRetryCount != 0 ||
		!got.NativeBasePlusLiveDelta || got.ExactFallbacks != 0 ||
		got.DocumentMaterializationRows != 1 {
		t.Fatalf("stable snapshot response=%+v err=%v hookCalls=%d", got, err, hookCalls)
	}

	svc.denseVectorNativeAfterSearch = nil
	updated, err := svc.SearchDenseVector(ctx, create.Name, DenseVectorSearchRequest{QueryEmbedding: []float32{0, 1}, TopK: 1, ReturnEmbedding: true})
	if err != nil || len(updated.Documents) != 1 || updated.Documents[0].Content != "v1" ||
		!reflect.DeepEqual(updated.Documents[0].Embedding, []float32{0, 1}) {
		t.Fatalf("updated response=%+v err=%v", updated, err)
	}
}

func TestServiceDenseNativeRuntimeCloseWaitsForPooledSearch(t *testing.T) {
	ctx := context.Background()
	svc, db := newTestService(t)
	defer db.Close()
	create := CreateIndexRequest{
		Name:               "native_close",
		Dimension:          2,
		Metric:             MetricCosine,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime},
	}
	if _, err := svc.CreateIndex(ctx, create); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{
		Documents:               []Document{{ID: "a", Content: "a", Embedding: []float32{1, 0}}},
		DeferVectorIndexRebuild: true,
	}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	searchEntered := make(chan struct{})
	releaseSearch := make(chan struct{})
	var enterOnce sync.Once
	svc.denseVectorNativeAfterSearch = func(int, collections.VectorIndexSearchResponse) error {
		enterOnce.Do(func() { close(searchEntered) })
		<-releaseSearch
		return nil
	}
	type searchResult struct {
		response DenseVectorSearchResponse
		err      error
	}
	searchDone := make(chan searchResult, 1)
	go func() {
		response, err := svc.SearchDenseVector(ctx, create.Name, DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1})
		searchDone <- searchResult{response: response, err: err}
	}()
	<-searchEntered

	closeStarted := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		close(closeStarted)
		closeDone <- svc.Close()
	}()
	<-closeStarted
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while native dense search held the pooled-search read lock: %v", err)
	default:
	}
	close(releaseSearch)
	searched := <-searchDone
	if searched.err != nil || len(searched.response.Documents) != 1 || searched.response.Documents[0].ID != "a" {
		t.Fatalf("in-flight search response=%+v err=%v", searched.response, searched.err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := svc.SearchDenseVector(ctx, create.Name, DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("post-close native dense search err=%v code=%s", err, ErrorCodeOf(err))
	}
}
func TestServiceBenchmarkNativeRuntimeCompatibleCreateAfterEmptyReopen(t *testing.T) {
	dir := t.TempDir()
	open := func() (*Service, *backenddb.DB) {
		t.Helper()
		db, err := backenddb.Open(testBackendOptions(dir))
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		return New(collections.NewCollectionManager(db)), db
	}
	ctx := context.Background()
	req := CreateIndexRequest{Name: "native_empty", Dimension: 2, Metric: MetricCosine, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime}}

	svc, db := open()
	if _, err := svc.CreateIndex(ctx, req); err != nil {
		t.Fatalf("initial CreateIndex: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("close initial service: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close initial db: %v", err)
	}

	svc, db = open()
	defer db.Close()
	if _, err := svc.CreateIndex(ctx, req); err != nil {
		t.Fatalf("compatible CreateIndex after reopen: %v", err)
	}
	empty, err := svc.SearchBenchmarkVector(ctx, req.Name, BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 8, StatsMode: collections.VectorIndexSearchStatsModeProduction})
	if err != nil || len(empty.Results) != 0 || empty.Diagnostics.Route != collections.VectorIndexSearchRouteNativeRuntime || empty.Diagnostics.LiveANN.FullRebuilds != 0 {
		t.Fatalf("empty native search response=%+v err=%v", empty, err)
	}
	if _, err := svc.UpsertDocuments(ctx, req.Name, UpsertDocumentsRequest{Documents: []Document{{ID: "a", Embedding: []float32{1, 0}}}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatalf("first UpsertDocuments after reopen: %v", err)
	}
	got, err := svc.SearchBenchmarkVector(ctx, req.Name, BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 8, StatsMode: collections.VectorIndexSearchStatsModeProduction})
	if err != nil || len(got.Results) != 1 || got.Results[0].ID != "a" || got.Diagnostics.LiveANN.FullRebuilds != 0 {
		t.Fatalf("native search after first upsert response=%+v err=%v", got, err)
	}
}

func TestServiceBenchmarkNativeRuntimeConcurrentFirstMutationsShareHandle(t *testing.T) {
	dir := t.TempDir()
	open := func() (*Service, *backenddb.DB) {
		t.Helper()
		db, err := backenddb.Open(testBackendOptions(dir))
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		return New(collections.NewCollectionManager(db)), db
	}
	ctx := context.Background()
	req := CreateIndexRequest{Name: "native_concurrent", Dimension: 2, Metric: MetricCosine, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime}}
	svc, db := open()
	if _, err := svc.CreateIndex(ctx, req); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("close initial service: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close initial db: %v", err)
	}

	svc, db = open()
	defer db.Close()
	if _, err := svc.CreateIndex(ctx, req); err != nil {
		t.Fatalf("compatible CreateIndex after reopen: %v", err)
	}
	if err := svc.invalidateBenchmarkSearchCache(req.Name); err != nil {
		t.Fatalf("invalidate native cache before concurrent mutations: %v", err)
	}
	const workers = 16
	svc.writeMu.Lock()
	ready := make(chan struct{}, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			_, err := svc.UpsertDocuments(ctx, req.Name, UpsertDocumentsRequest{Documents: []Document{{
				ID:        fmt.Sprintf("doc-%02d", i),
				Embedding: []float32{1, float32(i + 1)},
			}}, DeferVectorIndexRebuild: true})
			errs <- err
		}()
	}
	for range workers {
		<-ready
	}
	time.Sleep(50 * time.Millisecond)
	svc.writeMu.Unlock()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent upsert: %v", err)
		}
	}
	svc.benchmarkSearchCacheMu.RLock()
	entry := svc.benchmarkSearchCache[req.Name]
	if entry == nil || entry.collection == nil {
		svc.benchmarkSearchCacheMu.RUnlock()
		t.Fatal("concurrent mutations did not prime native cache")
	}
	var buffer collections.VectorIndexSearchBuffer
	got, err := entry.collection.SearchVectorIndexWithBuffer(collections.VectorIndexSearchOptions{
		IndexName: defaultVectorIndexName,
		Query:     []float32{1, 1},
		TopK:      workers,
		EfSearch:  workers * 2,
		StatsMode: collections.VectorIndexSearchStatsModeProduction,
	}, &buffer)
	svc.benchmarkSearchCacheMu.RUnlock()
	if err != nil || len(got.Results) != workers {
		t.Fatalf("native search after concurrent first mutations results=%d err=%v want %d", len(got.Results), err, workers)
	}
}

func TestServicesShareNativeRuntimeMutationsThroughManager(t *testing.T) {
	db, err := backenddb.Open(testBackendOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	manager := collections.NewCollectionManager(db)
	first := New(manager)
	second := New(manager)
	ctx := context.Background()
	req := CreateIndexRequest{Name: "native_shared", Dimension: 2, Metric: MetricCosine, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime}}
	if _, err := first.CreateIndex(ctx, req); err != nil {
		t.Fatalf("first CreateIndex: %v", err)
	}
	if _, err := first.UpsertDocuments(ctx, req.Name, UpsertDocumentsRequest{Documents: []Document{{ID: "a", Embedding: []float32{1, 0}}}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatalf("first UpsertDocuments: %v", err)
	}
	if _, err := first.OptimizeIndex(ctx, req.Name, OptimizeIndexRequest{}); err != nil {
		t.Fatalf("persist initial native graph: %v", err)
	}
	if _, err := first.UpsertDocuments(ctx, req.Name, UpsertDocumentsRequest{Documents: []Document{{ID: "b", Embedding: []float32{0, 1}}}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatalf("dirty first native graph: %v", err)
	}
	if _, err := second.CreateIndex(ctx, req); err != nil {
		t.Fatalf("second compatible CreateIndex: %v", err)
	}
	if _, err := second.UpsertDocuments(ctx, req.Name, UpsertDocumentsRequest{Documents: []Document{{ID: "c", Embedding: []float32{-1, 0}}}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatalf("second UpsertDocuments: %v", err)
	}
	for name, service := range map[string]*Service{"first": first, "second": second} {
		got, err := service.SearchBenchmarkVector(ctx, req.Name, BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 3, EfSearch: 8, StatsMode: collections.VectorIndexSearchStatsModeProduction})
		if err != nil || len(got.Results) != 3 {
			t.Fatalf("%s SearchBenchmarkVector results=%+v err=%v", name, got.Results, err)
		}
	}
}

func TestServiceOptimizeNativeRuntimeDoesNotWarmColumnGraph(t *testing.T) {
	db, err := backenddb.Open(testBackendOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	svc := New(collections.NewCollectionManager(db))
	ctx := context.Background()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "native_optimize", Dimension: 2, Metric: MetricCosine, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime}}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "native_optimize", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Embedding: []float32{1, 0}}}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	optimized, err := svc.OptimizeIndex(ctx, "native_optimize", OptimizeIndexRequest{})
	if err != nil {
		t.Fatalf("OptimizeIndex: %v", err)
	}
	if !optimized.Status.Loaded || optimized.Status.RootID == 0 || optimized.Status.RebuildNeeded {
		t.Fatalf("OptimizeIndex status=%+v want published clean native root", optimized.Status)
	}
	response, err := svc.SearchBenchmarkVector(ctx, "native_optimize", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 8, StatsMode: collections.VectorIndexSearchStatsModeProduction})
	if err != nil || len(response.Results) != 1 || response.Results[0].ID != "a" || response.Diagnostics.Route != collections.VectorIndexSearchRouteNativeRuntime {
		t.Fatalf("SearchBenchmarkVector response=%+v err=%v", response, err)
	}
}

func TestServiceBenchmarkVectorFailClosed(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "bench", Dimension: 2, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph}})
	if err != nil {
		t.Fatalf("CreateIndex bench: %v", err)
	}
	if _, err := svc.SearchBenchmarkVector(ctx, "bench", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 4}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("missing assets err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.SearchBenchmarkVector(ctx, "bench", BenchmarkVectorSearchRequest{ExpectedGeneration: info.Generation + 1, QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 4}); ErrorCodeOf(err) != CodeIndexStale {
		t.Fatalf("stale generation err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.SearchBenchmarkVector(ctx, "bench", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, QueryMode: "future_mode"}); ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("unsupported query mode err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.SearchBenchmarkVector(ctx, "bench", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, QuantizedIndexName: "embedding.scalar_u8.fast"}); ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("exact with quantized index err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.SearchBenchmarkVector(ctx, "bench", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, QueryMode: BenchmarkVectorQueryModeQuantizedOnly, QuantizedRerankCandidates: 32}); ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("quantized_only invalid shape err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.SearchBenchmarkVector(ctx, "bench", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, StatsMode: collections.VectorIndexSearchStatsModeBenchmarkDebug}); ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("benchmark_debug stats_mode err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.SearchBenchmarkVector(ctx, "bench", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, StatsMode: collections.VectorIndexSearchStatsMode("future")}); ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("unknown stats_mode err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.UpsertDocuments(ctx, "bench", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Embedding: []float32{1, 0}}}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatalf("deferred UpsertDocuments bench: %v", err)
	}
	if _, err := svc.SearchBenchmarkVector(ctx, "bench", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 4}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("pre-optimize benchmark vector search err=%v code=%s", err, ErrorCodeOf(err))
	}
	if _, err := svc.OptimizeIndex(ctx, "bench", OptimizeIndexRequest{}); err != nil {
		t.Fatalf("OptimizeIndex bench: %v", err)
	}
	work, err := svc.SearchBenchmarkVector(ctx, "bench", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 4, StatsMode: collections.VectorIndexSearchStatsModeWorkAccounting})
	if err != nil {
		t.Fatalf("SearchBenchmarkVector work accounting: %v", err)
	}
	if !work.NoDocuments || work.Stats.WorkAccountingSearches != 1 || work.Stats.FP32ScoreCalls == 0 || work.Stats.DistanceKernelNanos == 0 || work.Stats.ServiceResponseNanos == 0 {
		t.Fatalf("work-accounting benchmark response=%+v", work)
	}
	if _, err := svc.SearchBenchmarkVector(ctx, "bench", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, QueryMode: BenchmarkVectorQueryModeQuantizedRerank, QuantizedIndexName: "embedding.scalar_u8.missing", QuantizedRerankCandidates: 32}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("unsupported/missing quantized err=%v code=%s", err, ErrorCodeOf(err))
	}
	exact, err := svc.SearchBenchmarkVector(ctx, "bench", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 4})
	if err != nil {
		t.Fatalf("exact search after missing quantized asset: %v", err)
	}
	if len(exact.Results) != 1 || exact.Results[0].ID != "a" || exact.QueryMode != BenchmarkVectorQueryModeExact || exact.Diagnostics.Route != collections.VectorIndexSearchRouteExactHNSWSearchPackV1 || !exact.NoDocuments {
		t.Fatalf("exact search after missing quantized asset=%+v", exact)
	}
	dense, err := svc.SearchDenseVector(ctx, "bench", DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, Route: RouteExact})
	if err != nil || !dense.Exact || len(dense.Documents) != 1 || dense.Documents[0].ID != "a" {
		t.Fatalf("dense exact fallback contract changed dense=%+v err=%v", dense, err)
	}

	l2Info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "l2bench", Dimension: 2, Metric: MetricL2, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime}})
	if err != nil {
		t.Fatalf("CreateIndex l2bench: %v", err)
	}
	if l2Info.Capabilities.NoDocumentVectorSearch {
		t.Fatalf("l2 capabilities=%+v want no_document_vector_search=false", l2Info.Capabilities)
	}
	if _, err := svc.SearchBenchmarkVector(ctx, "l2bench", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("unsupported metric/strategy err=%v code=%s", err, ErrorCodeOf(err))
	}
}

func loadBenchmarkDocs(t *testing.T, svc *Service, index string, docs []Document) {
	t.Helper()
	if _, err := svc.UpsertDocuments(context.Background(), index, UpsertDocumentsRequest{Documents: docs}); err != nil {
		t.Fatalf("UpsertDocuments %s: %v", index, err)
	}
}

func documentIDs(docs []Document) []string {
	ids := make([]string, len(docs))
	for i := range docs {
		ids[i] = docs[i].ID
	}
	return ids
}

func searchMeta(t *testing.T, doc Document) map[string]any {
	t.Helper()
	meta, ok := doc.Meta[searchMetaKey].(map[string]any)
	if !ok {
		t.Fatalf("document %q missing %s metadata: %+v", doc.ID, searchMetaKey, doc.Meta)
	}
	return meta
}

func searchMetaHasOnlySource(t *testing.T, doc Document, source string) bool {
	t.Helper()
	return searchMetaHasSources(searchMeta(t, doc), source)
}

func searchMetaHasSources(meta map[string]any, sources ...string) bool {
	rawSources, ok := meta["sources"].([]map[string]any)
	if !ok {
		return false
	}
	if len(rawSources) != len(sources) {
		return false
	}
	want := make(map[string]bool, len(sources))
	for _, source := range sources {
		want[source] = false
	}
	for _, raw := range rawSources {
		source, ok := raw["source"].(string)
		if !ok {
			return false
		}
		if _, exists := want[source]; !exists {
			return false
		}
		want[source] = true
	}
	for _, found := range want {
		if !found {
			return false
		}
	}
	return true
}

func newTestService(t testing.TB) (*Service, *backenddb.DB) {
	t.Helper()
	db, err := backenddb.Open(testBackendOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return New(collections.NewCollectionManager(db)), db
}

func TestServiceFilterDocumentsCursorPagesWithoutRescanningPrefix(t *testing.T) {
	ctx := context.Background()
	svc, db := newTestService(t)
	defer func() {
		_ = svc.Close()
		_ = db.Close()
	}()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "cursor_docs", Dimension: 2, Metric: MetricCosine}); err != nil {
		t.Fatal(err)
	}
	docs := []Document{
		{ID: "a", Embedding: []float32{1, 0}},
		{ID: "b", Embedding: []float32{1, 0}},
		{ID: "c", Embedding: []float32{1, 0}},
		{ID: "d", Embedding: []float32{1, 0}},
		{ID: "e", Embedding: []float32{1, 0}},
	}
	if _, err := svc.UpsertDocuments(ctx, "cursor_docs", UpsertDocumentsRequest{Documents: docs}); err != nil {
		t.Fatal(err)
	}
	first, err := svc.FilterDocuments(ctx, "cursor_docs", FilterDocumentsRequest{Limit: 2, CursorPage: true})
	if err != nil || first.Exhausted || !first.Truncated || first.NextAfterID != "b" ||
		len(first.Documents) != 2 || first.Documents[0].ID != "a" || first.Documents[1].ID != "b" {
		t.Fatalf("first cursor page=%+v err=%v", first, err)
	}
	second, err := svc.FilterDocuments(ctx, "cursor_docs", FilterDocumentsRequest{Limit: 2, CursorPage: true, AfterID: first.NextAfterID})
	if err != nil || second.Exhausted || second.NextAfterID != "d" ||
		len(second.Documents) != 2 || second.Documents[0].ID != "c" || second.Documents[1].ID != "d" {
		t.Fatalf("second cursor page=%+v err=%v", second, err)
	}
	last, err := svc.FilterDocuments(ctx, "cursor_docs", FilterDocumentsRequest{Limit: 2, CursorPage: true, AfterID: second.NextAfterID})
	if err != nil || !last.Exhausted || last.Truncated || last.NextAfterID != "" ||
		len(last.Documents) != 1 || last.Documents[0].ID != "e" {
		t.Fatalf("last cursor page=%+v err=%v", last, err)
	}
	filter := &Filter{Field: "id", Operator: "==", Value: "e"}
	sparse, err := svc.FilterDocuments(ctx, "cursor_docs", FilterDocumentsRequest{Filter: filter, Limit: 1, CursorPage: true})
	if err != nil || sparse.Exhausted || len(sparse.Documents) != 0 || sparse.NextAfterID != "b" {
		t.Fatalf("first sparse cursor page=%+v err=%v", sparse, err)
	}
	sparse, err = svc.FilterDocuments(ctx, "cursor_docs", FilterDocumentsRequest{
		Filter: filter, Limit: 1, CursorPage: true, AfterID: sparse.NextAfterID,
	})
	if err != nil || sparse.Exhausted || len(sparse.Documents) != 0 || sparse.NextAfterID != "d" {
		t.Fatalf("second sparse cursor page=%+v err=%v", sparse, err)
	}
	sparse, err = svc.FilterDocuments(ctx, "cursor_docs", FilterDocumentsRequest{
		Filter: filter, Limit: 1, CursorPage: true, AfterID: sparse.NextAfterID,
	})
	if err != nil || !sparse.Exhausted || len(sparse.Documents) != 1 || sparse.Documents[0].ID != "e" {
		t.Fatalf("last sparse cursor page=%+v err=%v", sparse, err)
	}
	huge, err := svc.FilterDocuments(ctx, "cursor_docs", FilterDocumentsRequest{
		Limit: int(^uint(0) >> 1), CursorPage: true,
	})
	if err != nil || !huge.Exhausted || len(huge.Documents) != len(docs) {
		t.Fatalf("huge-limit cursor page=%+v err=%v", huge, err)
	}
}

func TestServiceFilterDocumentsExactIDDirectPath(t *testing.T) {
	ctx := context.Background()
	svc, db := newTestService(t)
	defer db.Close()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "exact_id", Dimension: 2, Metric: MetricCosine})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	documents := []Document{
		{ID: "a", Content: "first", Embedding: []float32{1, 0}},
		{ID: "target", Content: "selected", Embedding: []float32{0, 1}, Meta: map[string]any{"kind": "wanted"}},
		{ID: "z", Content: "last", Embedding: []float32{1, 0}},
	}
	if _, err := svc.UpsertDocuments(ctx, info.Name, UpsertDocumentsRequest{Documents: documents}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	var scans atomic.Int64
	svc.documentScanBefore = func() { scans.Add(1) }
	exact := &Filter{Field: " id ", Operator: " == ", Value: "target"}
	assertNoScan := func(name string, req FilterDocumentsRequest) FilterDocumentsResponse {
		t.Helper()
		before := scans.Load()
		got, err := svc.FilterDocuments(ctx, info.Name, req)
		if err != nil {
			t.Fatalf("%s: FilterDocuments: %v", name, err)
		}
		if after := scans.Load(); after != before {
			t.Fatalf("%s: document scans changed from %d to %d", name, before, after)
		}
		return got
	}

	got := assertNoScan("hit", FilterDocumentsRequest{Filter: exact, Limit: 1})
	if got.MatchedCount != 1 || got.Truncated || len(got.Documents) != 1 ||
		got.Documents[0].ID != "target" || got.Documents[0].Content != "selected" || got.Documents[0].Embedding != nil {
		t.Fatalf("hit=%+v", got)
	}
	got = assertNoScan("unlimited", FilterDocumentsRequest{Filter: exact})
	if got.MatchedCount != 1 || got.Truncated || len(got.Documents) != 1 || got.Documents[0].ID != "target" {
		t.Fatalf("unlimited hit=%+v", got)
	}
	got = assertNoScan("embedding", FilterDocumentsRequest{Filter: exact, Limit: 1, ReturnEmbedding: true})
	if len(got.Documents) != 1 || !reflect.DeepEqual(got.Documents[0].Embedding, []float32{0, 1}) {
		t.Fatalf("embedding hit=%+v", got)
	}
	got = assertNoScan("offset", FilterDocumentsRequest{Filter: exact, Limit: 1, Offset: 1})
	if got.MatchedCount != 1 || got.Truncated || got.Documents == nil || len(got.Documents) != 0 {
		t.Fatalf("offset hit=%+v", got)
	}
	miss := &Filter{Field: "id", Operator: "==", Value: "missing"}
	got = assertNoScan("miss", FilterDocumentsRequest{Filter: miss, Limit: 1})
	if got.MatchedCount != 0 || got.Truncated || got.Documents == nil || len(got.Documents) != 0 {
		t.Fatalf("miss=%+v", got)
	}
	emptyID := &Filter{Field: "id", Operator: "==", Value: ""}
	got = assertNoScan("empty ID", FilterDocumentsRequest{Filter: emptyID, Limit: 1})
	if got.MatchedCount != 0 || got.Truncated || got.Documents == nil || len(got.Documents) != 0 {
		t.Fatalf("empty-ID miss=%+v", got)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	before := scans.Load()
	if _, err := svc.FilterDocuments(canceled, info.Name, FilterDocumentsRequest{Filter: exact, Limit: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled exact-ID filter err=%v", err)
	}
	if scans.Load() != before {
		t.Fatal("canceled exact-ID filter scanned documents")
	}
	if _, err := svc.FilterDocuments(ctx, info.Name, FilterDocumentsRequest{
		ExpectedGeneration: info.Generation + 1, Filter: exact, Limit: 1,
	}); ErrorCodeOf(err) != CodeIndexStale {
		t.Fatalf("stale exact-ID filter err=%v code=%s", err, ErrorCodeOf(err))
	}
	if scans.Load() != before {
		t.Fatal("stale exact-ID filter scanned documents")
	}
	if _, err := svc.FilterDocuments(ctx, info.Name, FilterDocumentsRequest{
		Filter: &Filter{Field: "id", Operator: "and", Value: "target"}, Limit: 1,
	}); ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("malformed filter err=%v code=%s", err, ErrorCodeOf(err))
	}
	if scans.Load() != before {
		t.Fatal("malformed filter scanned documents")
	}

	fallbacks := []struct {
		name   string
		filter *Filter
	}{
		{name: "non-string", filter: &Filter{Field: "id", Operator: "==", Value: 1}},
		{name: "non-equality", filter: &Filter{Field: "id", Operator: "!=", Value: "target"}},
		{name: "composite", filter: &Filter{Operator: "and", Conditions: []Filter{
			{Field: "id", Operator: "==", Value: "target"},
			{Field: "meta.kind", Operator: "==", Value: "wanted"},
		}}},
	}
	for _, tc := range fallbacks {
		before := scans.Load()
		if _, err := svc.FilterDocuments(ctx, info.Name, FilterDocumentsRequest{Filter: tc.filter, Limit: 1}); err != nil {
			t.Fatalf("%s fallback: %v", tc.name, err)
		}
		if after := scans.Load(); after != before+1 {
			t.Fatalf("%s fallback scans=%d want %d", tc.name, after, before+1)
		}
	}
}
func TestServiceFilterDocumentsExactIDClosedBackendFailsClosed(t *testing.T) {
	ctx := context.Background()
	svc, db := newTestService(t)
	const name = "exact_id_closed"
	if _, err := svc.manager.CreateCollection(&collections.CollectionMeta{
		Name:    name,
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := svc.manager.OpenCollection(name)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.Insert([]byte("target"), []byte(`{"id":"target","embedding":[1,0],"meta":{}}`)); err != nil {
		t.Fatalf("insert target: %v", err)
	}
	info := IndexInfo{Name: name, Generation: 1}
	if err := svc.primeBenchmarkSearchCache(name, col, info); err != nil {
		t.Fatalf("primeBenchmarkSearchCache: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close backend: %v", err)
	}
	var scans atomic.Int64
	svc.documentScanBefore = func() { scans.Add(1) }
	for _, id := range []string{"", "target"} {
		_, err := svc.FilterDocuments(ctx, name, FilterDocumentsRequest{
			Filter: &Filter{Field: "id", Operator: "==", Value: id},
			Limit:  1,
		})
		if ErrorCodeOf(err) != CodeIndexUnavailable {
			t.Fatalf("closed backend id=%q err=%v code=%s", id, err, ErrorCodeOf(err))
		}
	}
	if scans.Load() != 0 {
		t.Fatalf("closed backend exact-ID filters scanned %d times", scans.Load())
	}
}

func BenchmarkServiceFilterDocumentsExactID(b *testing.B) {
	ctx := context.Background()
	svc, db := newTestService(b)
	defer db.Close()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "exact_id_bench", Dimension: 2, Metric: MetricCosine}); err != nil {
		b.Fatalf("CreateIndex: %v", err)
	}
	const documentCount = 5000
	documents := make([]Document, documentCount)
	for i := range documents {
		documents[i] = Document{
			ID:        fmt.Sprintf("doc-%06d", i),
			Content:   "benchmark payload",
			Embedding: []float32{1, 0},
			Meta:      map[string]any{"ordinal": i},
		}
	}
	if _, err := svc.UpsertDocuments(ctx, "exact_id_bench", UpsertDocumentsRequest{Documents: documents}); err != nil {
		b.Fatalf("UpsertDocuments: %v", err)
	}
	col, _, err := svc.openIndex(ctx, "exact_id_bench", 0)
	if err != nil {
		b.Fatalf("openIndex: %v", err)
	}
	hitID := documents[len(documents)-1].ID
	for _, tc := range []struct {
		name      string
		id        string
		wantCount int
	}{
		{name: "hit", id: hitID, wantCount: 1},
		{name: "miss", id: "missing", wantCount: 0},
	} {
		b.Run(tc.name, func(b *testing.B) {
			req := FilterDocumentsRequest{
				Filter: &Filter{Field: "id", Operator: "==", Value: tc.id},
				Limit:  1,
			}
			b.ReportAllocs()
			for b.Loop() {
				got, err := svc.FilterDocuments(ctx, "exact_id_bench", req)
				if err != nil || got.MatchedCount != tc.wantCount || len(got.Documents) != tc.wantCount {
					b.Fatalf("FilterDocuments=%+v err=%v", got, err)
				}
			}
		})
	}
	b.Run("get-materialize-hit", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			raw, err := col.Get([]byte(hitID))
			if err != nil {
				b.Fatalf("Get: %v", err)
			}
			jsonDoc, err := col.StoredDocumentJSON(raw)
			if err != nil {
				b.Fatalf("StoredDocumentJSON: %v", err)
			}
			if _, err := decodeStoredDocument([]byte(hitID), jsonDoc); err != nil {
				b.Fatalf("decodeStoredDocument: %v", err)
			}
		}
	})
}

func testBackendOptions(dir string) backenddb.Options {
	return backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true}
}
