package collections

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
)

func testColumnStoreCompactStableAbandonPreservesSameSizeReboundSegment(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2SortedBatchA1950()}
	_, d, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, typedColumnQ2ClickHouseSortKey1950(), batches)
	defer closeFn()
	updated := batches[0][0]
	updated.TimeUS++
	updateTypedColumnEvent1953(t, col, updated)

	injected := errors.New("injected post-prepare compaction failure")
	var segmentPath, rotatedPath string
	var replacement []byte
	restoreHook := setColumnStoreCompactionAfterPrepareTestHook(func(prepared ColumnPublishPreparedAssets) error {
		if !prepared.stableResourcesRequired || prepared.stableResources == nil {
			t.Fatalf("prepared stable authority required=%t resources=%v", prepared.stableResourcesRequired, prepared.stableResources)
		}
		if len(prepared.Assets) == 0 {
			t.Fatal("compaction prepared no assets")
		}
		var err error
		segmentPath, err = columnAssetSegmentPath(d.ColumnAssetRootDir(), prepared.Assets[0].Ref)
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(segmentPath)
		if err != nil {
			t.Fatal(err)
		}
		rotatedPath = segmentPath + ".stable-abandon-original"
		if err := os.Rename(segmentPath, rotatedPath); err != nil {
			t.Fatal(err)
		}
		replacement = bytes.Repeat([]byte{'R'}, int(info.Size()))
		if err := os.WriteFile(segmentPath, replacement, 0o600); err != nil {
			t.Fatal(err)
		}
		return injected
	})
	defer restoreHook()

	if _, err := col.ColumnStoreCompact(context.Background(), ColumnStoreCompactOptions{}); !errors.Is(err, injected) {
		t.Fatalf("ColumnStoreCompact error=%v want injected failure", err)
	}
	got, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, replacement) {
		t.Fatalf("same-size replacement mutated: bytes=%d want %d", len(got), len(replacement))
	}
	if info, err := os.Stat(rotatedPath); err != nil || info.Size() == 0 {
		t.Fatalf("retained original stat=%v info=%v", err, info)
	}
	if filepath.Dir(segmentPath) != filepath.Dir(rotatedPath) {
		t.Fatalf("rotated original escaped segment directory: %q %q", segmentPath, rotatedPath)
	}
}

func TestColumnStoreCompactQ2SortedLatestVisibleReopen1953(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2SortedBatchA1950(), typedColumnQ2SortedBatchB1950()}
	dir, d, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, typedColumnQ2ClickHouseSortKey1950(), batches)
	defer func() { closeFn() }()
	events := flattenColumnPhysicalEvents1950(batches)
	latest := latestEventMap1953(events)

	promoted := latest["a-kind-guard"]
	promoted.Kind = "commit"
	promoted.Operation = "create"
	promoted.Collection = "app.bsky.feed.like"
	promoted.Did = "did:promoted"
	promoted.TimeUS += 101
	updateTypedColumnEvent1953(t, col, promoted)
	latest[promoted.ID] = promoted

	demoted := latest["a-post-a-1"]
	demoted.Operation = "delete"
	demoted.TimeUS += 102
	updateTypedColumnEvent1953(t, col, demoted)
	latest[demoted.ID] = demoted

	moved := latest["a-post-shared"]
	moved.Collection = "app.bsky.feed.like"
	moved.TimeUS += 103
	updateTypedColumnEvent1953(t, col, moved)
	latest[moved.ID] = moved

	deleteTypedColumnEvent1953(t, col, "b-repost")
	delete(latest, "b-repost")

	live := latestEvents1953(latest)
	req := typedColumnQ2Request1950()
	before, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2 before compaction): %v", err)
	}
	if got, want := columnPhysicalJSONBenchQ2CountsP0(before.Groups), columnPhysicalJSONBenchQ2ReferenceCountsP0(live); !columnPhysicalJSONBenchQ2CountsEqualP0(got, want) {
		t.Fatalf("q2 before compaction counts=%v want %v groups=%+v", got, want, before.Groups)
	}
	if before.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || before.Diagnostics.FallbackReason != ColumnPhysicalQueryFallbackNone || before.Diagnostics.MutationParts == 0 || before.Diagnostics.VisibilityRows != len(live) || before.Diagnostics.DocumentMaterializations != 0 || before.Diagnostics.RowMaterializations != 0 {
		t.Fatalf("q2 before compaction diagnostics=%+v want latest-visible typed-column path", before.Diagnostics)
	}
	if _, err := col.PrepareColumnPhysicalQuery(req); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "insert-only") {
		t.Fatalf("PrepareColumnPhysicalQuery before compaction err=%v want insert-only guard", err)
	}

	beforeView := loadColumnStoreCompactionManifestView1953(t, d, col)
	oldRefs := columnStoreCompactionRefsFromRecordsForTest1953(t, beforeView.records)
	if beforeView.mutationParts == 0 || len(oldRefs) == 0 {
		t.Fatalf("before compaction manifest mutation_parts=%d refs=%d want mutation-bearing refs", beforeView.mutationParts, len(oldRefs))
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("settle pre-compaction publication before pin baseline: %v", err)
	}
	registry := d.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()

	stats, err := col.ColumnStoreCompact(context.Background(), ColumnStoreCompactOptions{})
	if err != nil {
		t.Fatalf("ColumnStoreCompact: %v", err)
	}
	if !stats.Compacted || stats.PreviousGeneration != beforeView.manifest.Generation || stats.NewGeneration <= stats.PreviousGeneration || stats.RowsCompacted != len(live) || stats.MutationPartsBefore == 0 || stats.MutationPartsAfter != 0 || stats.AssetsPublished == 0 {
		t.Fatalf("compaction stats=%+v want logical rewrite of latest-visible rows", stats)
	}
	if got := col.Meta().Options.ColumnStore.PhysicalMutationParts; got != 0 {
		t.Fatalf("PhysicalMutationParts=%d want reset after compaction", got)
	}
	assertColumnStoreCompactionDocumentsReconstruct1953(t, col, live)
	// Return guarantees the activated visible closure; the independently
	// recoverable durable closure may already be installed by the asynchronous
	// publisher. Require one of those two exact ownership states.
	visiblePins := baselinePins + uint64(stats.AssetsPublished)
	visibleAndDurablePins := baselinePins + 2*uint64(stats.AssetsPublished)
	if got := registry.ActivePins(); got != visiblePins && got != visibleAndDurablePins {
		t.Fatalf("stable asset pins after compaction publish=%d want visible %d or visible+durable %d", got, visiblePins, visibleAndDurablePins)
	}
	if got := registry.ActiveIdentities(); got != baselineIdentities {
		t.Fatalf("stable asset identities after compaction publish=%d want baseline %d", got, baselineIdentities)
	}
	if got := registry.ActiveStableNamespaceLinks(); ordinaryColumnStableAuthorityEnabled() && got == 0 {
		t.Fatal("successful compaction publish retained no exact namespace sync proof")
	} else if !ordinaryColumnStableAuthorityEnabled() && got != 0 {
		t.Fatalf("legacy pre-cutover compaction retained %d exact namespace sync proofs, want 0", got)
	}

	after, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2 after compaction): %v", err)
	}
	if got, want := columnPhysicalJSONBenchQ2CountsP0(after.Groups), columnPhysicalJSONBenchQ2ReferenceCountsP0(live); !columnPhysicalJSONBenchQ2CountsEqualP0(got, want) {
		t.Fatalf("q2 after compaction counts=%v want %v groups=%+v", got, want, after.Groups)
	}
	assertTypedColumnQ2SortedGroupedDistinctDiagnostics1950(t, "q2 after compaction", after.Diagnostics, len(live), columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", live), true)
	if after.Diagnostics.MutationParts != 0 || after.Diagnostics.VisibilityRows != 0 || after.Diagnostics.DeletedRows != 0 {
		t.Fatalf("q2 after compaction diagnostics=%+v want insert-only manifest", after.Diagnostics)
	}
	runner, err := col.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery after compaction: %v", err)
	}
	prepared, err := runner.Run()
	closeErr := runner.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("prepared q2 after compaction run=%v close=%v", err, closeErr)
	}
	if got, want := columnPhysicalJSONBenchQ2CountsP0(prepared.Groups), columnPhysicalJSONBenchQ2ReferenceCountsP0(live); !columnPhysicalJSONBenchQ2CountsEqualP0(got, want) {
		t.Fatalf("prepared q2 after compaction counts=%v want %v groups=%+v", got, want, prepared.Groups)
	}

	afterView := loadColumnStoreCompactionManifestView1953(t, d, col)
	assertColumnStoreCompactionManifestOnlyGeneration1953(t, afterView.records, stats.NewGeneration, false)
	assertColumnStoreCompactionRefsAbsent1953(t, afterView.records, oldRefs)
	assertColumnStoreCompactionRefsReclaimable1953(t, col, oldRefs)

	_, col, closeFn = checkpointAndReopenTypedColumnLatestVisibleFixture1953(t, dir, d, closeFn)
	assertColumnStoreCompactionDocumentsReconstruct1953(t, col, live)
	reopened, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2 after compaction reopen): %v", err)
	}
	if got, want := columnPhysicalJSONBenchQ2CountsP0(reopened.Groups), columnPhysicalJSONBenchQ2ReferenceCountsP0(live); !columnPhysicalJSONBenchQ2CountsEqualP0(got, want) {
		t.Fatalf("q2 after compaction reopen counts=%v want %v groups=%+v", got, want, reopened.Groups)
	}
	if reopened.Diagnostics.MutationParts != 0 || reopened.Diagnostics.VisibilityRows != 0 || reopened.Diagnostics.DocumentMaterializations != 0 || reopened.Diagnostics.RowMaterializations != 0 {
		t.Fatalf("q2 after compaction reopen diagnostics=%+v want insert-only typed-column path", reopened.Diagnostics)
	}
}

func assertColumnStoreCompactionDocumentsReconstruct1953(tb testing.TB, col *Collection, want []columnPhysicalJSONBenchParityEventP0) {
	tb.Helper()
	records, truncated, err := col.ScanDocuments(len(want) + 1)
	if err != nil {
		tb.Fatalf("ScanDocuments after compaction: %v", err)
	}
	if truncated || len(records) != len(want) {
		tb.Fatalf("ScanDocuments after compaction truncated=%t rows=%d want %d", truncated, len(records), len(want))
	}
	wantIDs := make(map[string]struct{}, len(want))
	for i := range want {
		wantIDs[want[i].ID] = struct{}{}
	}
	for i := range records {
		if _, ok := wantIDs[string(records[i].ID)]; !ok {
			tb.Fatalf("ScanDocuments after compaction returned unexpected id %q", records[i].ID)
		}
	}
}

func TestColumnStoreCompactRebuildsAggregateMetadata1953(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ5DenseBatchA1950(), columnPhysicalQ5DenseBatchB1950()}
	dir, d, col, closeFn := openColumnStoreCompactionFixture1953(t, predicateAggregateMetadataConfig1951(), batches)
	defer func() { closeFn() }()
	events := flattenColumnPhysicalEvents1950(batches)
	latest := latestEventMap1953(events)

	updated := latest["a-m-1"]
	updated.TimeUS += 1_000
	updateTypedColumnEvent1953(t, col, updated)
	latest[updated.ID] = updated

	demoted := latest["b-beta-2"]
	demoted.Operation = "delete"
	demoted.TimeUS += 2_000
	updateTypedColumnEvent1953(t, col, demoted)
	latest[demoted.ID] = demoted

	deleteTypedColumnEvent1953(t, col, "b-m-3")
	delete(latest, "b-m-3")

	metadataReq := columnPredicateAggregateMetadataQ5Request1951(true)
	fallbackReq := metadataReq
	fallbackReq.AggregateMetadataName = ""
	want, err := col.RunColumnPhysicalQuery(fallbackReq)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q5 latest-visible fallback before compaction): %v", err)
	}
	if want.Diagnostics.MutationParts == 0 || want.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || want.Diagnostics.DocumentMaterializations != 0 || want.Diagnostics.RowMaterializations != 0 {
		t.Fatalf("fallback before compaction diagnostics=%+v want latest-visible typed-column path", want.Diagnostics)
	}
	if _, err := col.RunColumnPhysicalQuery(metadataReq); !errors.Is(err, ErrColumnQueryPlanUnsupported) {
		t.Fatalf("RunColumnPhysicalQuery metadata before compaction err=%v want unsupported", err)
	}
	if _, err := col.PrepareColumnPhysicalQuery(metadataReq); !errors.Is(err, ErrColumnQueryPlanUnsupported) {
		t.Fatalf("PrepareColumnPhysicalQuery metadata before compaction err=%v want unsupported", err)
	}

	beforeView := loadColumnStoreCompactionManifestView1953(t, d, col)
	oldRefs := columnStoreCompactionRefsFromRecordsForTest1953(t, beforeView.records)
	oldMetadataRefs := columnStoreCompactionRefsWithKind1953(oldRefs, ColumnAssetKindTCS1AggregateMetadata)
	if beforeView.mutationParts == 0 || len(oldMetadataRefs) == 0 {
		t.Fatalf("before compaction mutation_parts=%d old metadata refs=%d want stale metadata lineage", beforeView.mutationParts, len(oldMetadataRefs))
	}

	stats, err := col.ColumnStoreCompact(context.Background(), ColumnStoreCompactOptions{})
	if err != nil {
		t.Fatalf("ColumnStoreCompact aggregate metadata: %v", err)
	}
	if !stats.Compacted || stats.RowsCompacted != len(latestEvents1953(latest)) || stats.AggregateMetadataPublished == 0 || stats.MutationPartsAfter != 0 {
		t.Fatalf("aggregate compaction stats=%+v want rebuilt insert-only metadata", stats)
	}

	after, err := col.RunColumnPhysicalQuery(metadataReq)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery metadata after compaction: %v", err)
	}
	if !reflect.DeepEqual(after.Groups, want.Groups) {
		t.Fatalf("metadata after compaction groups=%+v want latest-visible %+v", after.Groups, want.Groups)
	}
	assertColumnStoreCompactionAggregateMetadataDiagnostics1953(t, after.Diagnostics, want.Diagnostics.RowsMatched)

	runner, err := col.PrepareColumnPhysicalQuery(metadataReq)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery metadata after compaction: %v", err)
	}
	prepared, err := runner.Run()
	closeErr := runner.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("prepared metadata after compaction run=%v close=%v", err, closeErr)
	}
	if !reflect.DeepEqual(prepared.Groups, want.Groups) {
		t.Fatalf("prepared metadata after compaction groups=%+v want %+v", prepared.Groups, want.Groups)
	}
	assertColumnStoreCompactionAggregateMetadataDiagnostics1953(t, prepared.Diagnostics, want.Diagnostics.RowsMatched)

	afterView := loadColumnStoreCompactionManifestView1953(t, d, col)
	assertColumnStoreCompactionManifestOnlyGeneration1953(t, afterView.records, stats.NewGeneration, true)
	assertColumnStoreCompactionRefsAbsent1953(t, afterView.records, oldRefs)
	assertColumnStoreCompactionRefsReclaimable1953(t, col, oldMetadataRefs)

	_, col, closeFn = checkpointAndReopenTypedColumnLatestVisibleFixture1953(t, dir, d, closeFn)
	reopened, err := col.RunColumnPhysicalQuery(metadataReq)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery metadata after compaction reopen: %v", err)
	}
	if !reflect.DeepEqual(reopened.Groups, want.Groups) {
		t.Fatalf("metadata after compaction reopen groups=%+v want %+v", reopened.Groups, want.Groups)
	}
	assertColumnStoreCompactionAggregateMetadataDiagnostics1953(t, reopened.Diagnostics, want.Diagnostics.RowsMatched)
}

func TestColumnStoreCompactRebuildsRowAssetSidecars1953(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{{
		{ID: "a", TimeUS: 100, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:a"},
		{ID: "b", TimeUS: 200, Kind: "identity", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:b"},
		{ID: "c", TimeUS: 300, Kind: "commit", Operation: "delete", Collection: "app.bsky.feed.repost", Did: "did:c"},
	}}
	_, d, col, closeFn := openColumnStoreCompactionFixture1953(t, columnStoreCompactionMixedSidecarConfig1953(), batches)
	defer closeFn()
	events := flattenColumnPhysicalEvents1950(batches)
	latest := latestEventMap1953(events)

	updated := latest["b"]
	updated.Kind = "commit"
	updated.Collection = "app.bsky.feed.post"
	updated.TimeUS += 55
	updateTypedColumnEvent1953(t, col, updated)
	latest[updated.ID] = updated
	deleteTypedColumnEvent1953(t, col, "c")
	delete(latest, "c")

	beforeView := loadColumnStoreCompactionManifestView1953(t, d, col)
	oldRefs := columnStoreCompactionRefsFromRecordsForTest1953(t, beforeView.records)
	oldDictionaryRefs := columnStoreCompactionRefsWithKind1953(oldRefs, ColumnAssetKindTCS1DictionaryCodes)
	oldInt64Refs := columnStoreCompactionRefsWithKind1953(oldRefs, ColumnAssetKindTCS1Int64Values)
	if beforeView.mutationParts == 0 || len(oldDictionaryRefs) == 0 || len(oldInt64Refs) == 0 {
		t.Fatalf("before sidecar compaction mutation_parts=%d dictionary_refs=%d int64_refs=%d", beforeView.mutationParts, len(oldDictionaryRefs), len(oldInt64Refs))
	}

	stats, err := col.ColumnStoreCompact(context.Background(), ColumnStoreCompactOptions{})
	if err != nil {
		t.Fatalf("ColumnStoreCompact sidecars: %v", err)
	}
	if !stats.Compacted || stats.RowsCompacted != len(latestEvents1953(latest)) {
		t.Fatalf("sidecar compaction stats=%+v want compacted live rows", stats)
	}
	afterView := loadColumnStoreCompactionManifestView1953(t, d, col)
	afterRefs := columnStoreCompactionRefsFromRecordsForTest1953(t, afterView.records)
	if got := len(columnStoreCompactionRefsWithKind1953(afterRefs, ColumnAssetKindTCS1DictionaryCodes)); got == 0 {
		t.Fatalf("compacted sidecar manifest refs=%+v want dictionary sidecar", afterRefs)
	}
	if got := len(columnStoreCompactionRefsWithKind1953(afterRefs, ColumnAssetKindTCS1Int64Values)); got == 0 {
		t.Fatalf("compacted sidecar manifest refs=%+v want int64 values sidecar", afterRefs)
	}
	assertColumnStoreCompactionManifestOnlyGeneration1953(t, afterView.records, stats.NewGeneration, false)
	assertColumnStoreCompactionRefsAbsent1953(t, afterView.records, oldRefs)
	assertColumnStoreCompactionRefsReclaimable1953(t, col, append(oldDictionaryRefs, oldInt64Refs...))

	result, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery after sidecar compaction: %v", err)
	}
	if got, want := columnPhysicalGroupCountMap1953(result.Groups), collectionCounts1953(latestEvents1953(latest)); !reflect.DeepEqual(got, want) {
		t.Fatalf("sidecar compaction groups=%v want %v full=%+v", got, want, result.Groups)
	}
}

func TestColumnStoreCompactPostCompactionWritesAdvanceFromCompactedManifest1953(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{{
		{ID: "a", TimeUS: 100, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:a"},
		{ID: "b", TimeUS: 200, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:b"},
	}}
	dir, d, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, nil, batches)
	defer func() { closeFn() }()
	events := flattenColumnPhysicalEvents1950(batches)
	latest := latestEventMap1953(events)

	updated := latest["a"]
	updated.Collection = "app.bsky.feed.repost"
	updated.TimeUS += 10
	updateTypedColumnEvent1953(t, col, updated)
	latest[updated.ID] = updated
	deleteTypedColumnEvent1953(t, col, "b")
	delete(latest, "b")

	beforeView := loadColumnStoreCompactionManifestView1953(t, d, col)
	beforeLSN := d.State().AppliedCommandLSN
	stats, err := col.ColumnStoreCompact(context.Background(), ColumnStoreCompactOptions{})
	if err != nil {
		t.Fatalf("ColumnStoreCompact before post-write: %v", err)
	}
	compactedView := loadColumnStoreCompactionManifestView1953(t, d, col)
	if got := d.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("DB AppliedCommandLSN advanced during compaction: got %d want %d", got, beforeLSN)
	}
	if compactedView.manifest.AppliedCommandLSN != beforeView.cfg.RecoveryAuthoritativeAppliedCommandLSN || compactedView.cfg.RecoveryAuthoritativeAppliedCommandLSN != beforeLSN {
		t.Fatalf("compacted manifest/config LSN manifest=%d cfg=%d before_cfg=%d db=%d", compactedView.manifest.AppliedCommandLSN, compactedView.cfg.RecoveryAuthoritativeAppliedCommandLSN, beforeView.cfg.RecoveryAuthoritativeAppliedCommandLSN, beforeLSN)
	}

	inserted := columnPhysicalJSONBenchParityEventP0{ID: "c", TimeUS: 300, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:c"}
	if _, err := col.InsertBatch([][]byte{[]byte(inserted.ID)}, [][]byte{typedColumnEventDocument1953(inserted)}); err != nil {
		t.Fatalf("InsertBatch after compaction: %v", err)
	}
	latest[inserted.ID] = inserted
	insertView := loadColumnStoreCompactionManifestView1953(t, d, col)
	if insertView.manifest.Generation != stats.NewGeneration+1 || insertView.manifest.AppliedCommandLSN != beforeLSN+1 || d.State().AppliedCommandLSN != beforeLSN+1 {
		t.Fatalf("post-compaction insert manifest=%+v db_lsn=%d want generation=%d lsn=%d", insertView.manifest, d.State().AppliedCommandLSN, stats.NewGeneration+1, beforeLSN+1)
	}

	updated = latest["a"]
	updated.Collection = "app.bsky.feed.like"
	updated.TimeUS += 20
	updateTypedColumnEvent1953(t, col, updated)
	latest[updated.ID] = updated
	deleteTypedColumnEvent1953(t, col, "c")
	delete(latest, "c")
	mutationView := loadColumnStoreCompactionManifestView1953(t, d, col)
	if mutationView.manifest.Generation != stats.NewGeneration+3 || mutationView.manifest.AppliedCommandLSN != beforeLSN+3 || d.State().AppliedCommandLSN != beforeLSN+3 || mutationView.mutationParts == 0 {
		t.Fatalf("post-compaction mutation manifest=%+v mutation_parts=%d db_lsn=%d want generation=%d lsn=%d mutation parts", mutationView.manifest, mutationView.mutationParts, d.State().AppliedCommandLSN, stats.NewGeneration+3, beforeLSN+3)
	}

	result, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery after post-compaction writes: %v", err)
	}
	if got, want := columnPhysicalGroupCountMap1953(result.Groups), collectionCounts1953(latestEvents1953(latest)); !reflect.DeepEqual(got, want) {
		t.Fatalf("post-compaction write groups=%v want %v full=%+v", got, want, result.Groups)
	}

	_, col, closeFn = checkpointAndReopenTypedColumnLatestVisibleFixture1953(t, dir, d, closeFn)
	reopened, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery post-compaction writes reopen: %v", err)
	}
	if got, want := columnPhysicalGroupCountMap1953(reopened.Groups), collectionCounts1953(latestEvents1953(latest)); !reflect.DeepEqual(got, want) {
		t.Fatalf("post-compaction write reopen groups=%v want %v full=%+v", got, want, reopened.Groups)
	}
}

func TestColumnStoreCompactRejectsVectorManifestRecords1953(t *testing.T) {
	cases := []struct {
		name string
		key  []byte
	}{
		{name: "graph", key: columnVectorGraphManifestRecordKey("c5-unsupported-vector-graph")},
		{name: "state", key: columnVectorIndexStateRecordKey("c5-unsupported-vector-state")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			batches := [][]columnPhysicalJSONBenchParityEventP0{{
				{ID: "a", TimeUS: 100, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:a"},
			}}
			_, d, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, nil, batches)
			defer closeFn()

			installColumnStoreCompactionManifestRecord1953(t, col, columnManifestRecord{
				key:   tc.key,
				value: []byte("unsupported-vector-record-for-c5-compaction-guard"),
			})
			if _, err := col.ColumnStoreCompact(context.Background(), ColumnStoreCompactOptions{}); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "vector") {
				t.Fatalf("ColumnStoreCompact with vector manifest record err=%v want explicit unsupported vector failure", err)
			}

			// The failed compaction must leave the vector-bearing manifest readable.
			_ = loadColumnStoreCompactionManifestView1953(t, d, col)
		})
	}
}

func TestSystemTargetIteratorTombstoneEntry1953(t *testing.T) {
	it := &systemTargetIterator{entries: []systemTargetEntry{{
		key:   []byte("deleted"),
		value: []byte("stale-value"),
		flags: node.FlagTombstone,
	}}}
	if !it.Valid() || !it.IsDeleted() {
		t.Fatalf("iterator valid/deleted=%t/%t want tombstone", it.Valid(), it.IsDeleted())
	}
	if value := it.UnsafeValue(); value != nil {
		t.Fatalf("UnsafeValue for tombstone=%q want nil", value)
	}
	value, _, flags := it.UnsafeEntry()
	if value != nil || flags&node.FlagTombstone == 0 {
		t.Fatalf("UnsafeEntry value=%q flags=0x%x want tombstone", value, flags)
	}
	if copied := it.ValueCopy([]byte("keep")); len(copied) != 0 {
		t.Fatalf("ValueCopy tombstone=%q want empty copy", copied)
	}
}

func TestColumnStoreCompactAllRowsDeletedPublishesEmptyInsertOnlyManifest1953(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{{
		{ID: "a", TimeUS: 100, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:a"},
		{ID: "b", TimeUS: 200, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:b"},
	}}
	dir, d, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, nil, batches)
	defer func() { closeFn() }()

	deleteTypedColumnEvent1953(t, col, "a")
	deleteTypedColumnEvent1953(t, col, "b")
	beforeView := loadColumnStoreCompactionManifestView1953(t, d, col)
	oldRefs := columnStoreCompactionRefsFromRecordsForTest1953(t, beforeView.records)

	stats, err := col.ColumnStoreCompact(context.Background(), ColumnStoreCompactOptions{})
	if err != nil {
		t.Fatalf("ColumnStoreCompact all deleted: %v", err)
	}
	if !stats.Compacted || stats.RowsCompacted != 0 || stats.AssetsPublished != 0 || stats.MutationPartsBefore == 0 || stats.MutationPartsAfter != 0 {
		t.Fatalf("all-deleted compaction stats=%+v want empty insert-only generation", stats)
	}

	afterView := loadColumnStoreCompactionManifestView1953(t, d, col)
	if afterView.manifest.RowCount != 0 || afterView.manifest.ExpectedParts != 0 || afterView.mutationParts != 0 {
		t.Fatalf("all-deleted manifest=%+v mutation_parts=%d want empty insert-only manifest", afterView.manifest, afterView.mutationParts)
	}
	assertColumnStoreCompactionManifestOnlyGeneration1953(t, afterView.records, stats.NewGeneration, false)
	assertColumnStoreCompactionRefsAbsent1953(t, afterView.records, oldRefs)

	result, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery all deleted after compaction: %v", err)
	}
	if len(result.Groups) != 0 || result.Diagnostics.MutationParts != 0 || result.Diagnostics.VisibilityRows != 0 || result.Diagnostics.DocumentMaterializations != 0 {
		t.Fatalf("all-deleted query result=%+v diagnostics=%+v want empty insert-only typed query", result.Groups, result.Diagnostics)
	}

	_, col, closeFn = checkpointAndReopenTypedColumnLatestVisibleFixture1953(t, dir, d, closeFn)
	reopened, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery all deleted reopen: %v", err)
	}
	if len(reopened.Groups) != 0 || reopened.Diagnostics.MutationParts != 0 {
		t.Fatalf("all-deleted reopen result=%+v diagnostics=%+v want empty insert-only manifest", reopened.Groups, reopened.Diagnostics)
	}
}

type columnStoreCompactionManifestView1953 struct {
	cfg           ColumnStoreConfig
	manifest      columnManifestSnapshot
	records       []columnManifestRecord
	mutationParts int
	rootID        uint64
}

func columnStoreCompactionMixedSidecarConfig1953() *ColumnStoreConfig {
	return &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerRowAsset, Dictionary: true},
		{Name: "operation", Path: "operation", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "collection", Path: "collection", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
	}}
}

func installColumnStoreCompactionManifestRecord1953(tb testing.TB, col *Collection, record columnManifestRecord) {
	tb.Helper()
	state, err := col.loadColumnAssetRewriteManifestState()
	if err != nil {
		tb.Fatalf("loadColumnAssetRewriteManifestState: %v", err)
	}
	records := append(cloneColumnManifestRecords(state.records), columnManifestRecord{key: bytes.Clone(record.key), value: bytes.Clone(record.value)})
	sortColumnManifestRecords(records)
	identity, err := columnAssetRewriteUpdatedIdentity(state, records)
	if err != nil {
		tb.Fatalf("columnAssetRewriteUpdatedIdentity: %v", err)
	}
	updatedMeta, err := columnAssetRewriteUpdatedMeta(state.meta, identity)
	if err != nil {
		tb.Fatalf("columnAssetRewriteUpdatedMeta: %v", err)
	}
	newSystemRoot, rootIDs, err := col.publishColumnAssetRewriteManifestState(state, updatedMeta, identity, records)
	if err != nil {
		tb.Fatalf("publish manifest fixture record: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 || newSystemRoot == 0 {
		tb.Fatalf("publish manifest fixture roots system=%d roots=%v", newSystemRoot, rootIDs)
	}
	nextCatalog := cloneCatalogWithRootUpdates(state.catalog, updatedMeta, []string{state.rootName}, rootIDs)
	col.meta = updatedMeta
	col.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	col.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
}

func openColumnStoreCompactionFixture1953(tb testing.TB, cfg *ColumnStoreConfig, batches [][]columnPhysicalJSONBenchParityEventP0) (string, *backenddb.DB, *Collection, func()) {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatalf("Open setup DB: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection setup: %v", err)
	}
	for batchIdx, batch := range batches {
		ids := make([][]byte, len(batch))
		docs := make([][]byte, len(batch))
		for i, event := range batch {
			ids[i] = []byte(event.ID)
			docs[i] = typedColumnEventDocument1953(event)
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			_ = d.Close()
			tb.Fatalf("InsertBatch[%d]: %v", batchIdx, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		tb.Fatalf("Checkpoint setup: %v", err)
	}
	return dir, d, col, func() { _ = d.Close() }
}

func loadColumnStoreCompactionManifestView1953(tb testing.TB, d *backenddb.DB, col *Collection) columnStoreCompactionManifestView1953 {
	tb.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, col.Meta().Name)
	if err != nil {
		tb.Fatalf("loadCollectionCatalog: %v", err)
	}
	if catalog == nil {
		tb.Fatal("loadCollectionCatalog returned nil")
	}
	cfg := catalog.meta.Options.ColumnStore.copy()
	if cfg.ActiveManifest == nil {
		tb.Fatal("missing active column manifest")
	}
	rootName := collectionColumnManifestRootName(catalog.meta.Name)
	if cfg.ManifestRoot != nil && cfg.ManifestRoot.Name != "" {
		rootName = cfg.ManifestRoot.Name
	}
	rootID := catalog.rootID(rootName)
	if rootID == 0 {
		tb.Fatalf("missing manifest root %q", rootName)
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, rootID)
	if err != nil {
		tb.Fatalf("loadColumnManifestRecordsFromRoot: %v", err)
	}
	manifest, _, _, mutationParts, _, err := loadColumnManifestSnapshotViewForScanFromRoot(snap, rootID, cfg, *cfg.ActiveManifest, catalog.meta.Name, true, catalog.meta.VectorIndexes)
	if err != nil {
		tb.Fatalf("loadColumnManifestSnapshotViewForScanFromRoot: %v", err)
	}
	return columnStoreCompactionManifestView1953{cfg: cfg, manifest: cloneColumnStoreCompactionManifestSnapshot1953(manifest), records: cloneColumnManifestRecords(records), mutationParts: mutationParts, rootID: rootID}
}

func cloneColumnStoreCompactionManifestSnapshot1953(manifest columnManifestSnapshot) columnManifestSnapshot {
	clone := manifest
	clone.Parts = append([]columnManifestPartSnapshot(nil), manifest.Parts...)
	for i := range clone.Parts {
		clone.Parts[i].SortKey = cloneColumnSortKeys(manifest.Parts[i].SortKey)
	}
	clone.AggregateMetadata = append([]columnManifestAggregateMetadataSnapshot(nil), manifest.AggregateMetadata...)
	clone.DictionaryCodes = append([]columnManifestDictionaryCodesSnapshot(nil), manifest.DictionaryCodes...)
	clone.Int64Values = append([]columnManifestInt64ValuesSnapshot(nil), manifest.Int64Values...)
	return clone
}

func columnStoreCompactionRefsFromRecordsForTest1953(tb testing.TB, records []columnManifestRecord) []ColumnAssetRef {
	tb.Helper()
	refs := make([]ColumnAssetRef, 0, len(records))
	for _, record := range records {
		if !columnStoreCompactionRecordCanHoldRef1953(record.key) {
			continue
		}
		part, err := decodeColumnManifestPartRecord(record.value)
		if err != nil {
			tb.Fatalf("decodeColumnManifestPartRecord key=%q: %v", string(record.key), err)
		}
		refs = append(refs, part.AssetRef)
	}
	return refs
}

func columnStoreCompactionRecordCanHoldRef1953(key []byte) bool {
	return bytes.HasPrefix(key, columnManifestPartRecordPrefixBytes) ||
		bytes.HasPrefix(key, columnManifestAggregateMetadataRecordPrefixBytes) ||
		bytes.HasPrefix(key, columnManifestDictionaryCodesRecordPrefixBytes) ||
		bytes.HasPrefix(key, columnManifestInt64ValuesRecordPrefixBytes)
}

func columnStoreCompactionRefsWithKind1953(refs []ColumnAssetRef, kind ColumnAssetKind) []ColumnAssetRef {
	out := make([]ColumnAssetRef, 0, len(refs))
	for _, ref := range refs {
		if ref.Kind == kind {
			out = append(out, ref)
		}
	}
	return out
}

func assertColumnStoreCompactionManifestOnlyGeneration1953(tb testing.TB, records []columnManifestRecord, generation uint64, wantAggregateMetadata bool) {
	tb.Helper()
	aggregateRecords := 0
	for _, record := range records {
		switch {
		case bytes.Equal(record.key, columnManifestHeaderRecordKeyBytes):
			continue
		case bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes):
			keyGeneration, _, err := columnManifestPartKeyFromRecordKeyForScan(record.key)
			if err != nil {
				tb.Fatalf("columnManifestPartKeyFromRecordKeyForScan: %v", err)
			}
			part, err := decodeColumnManifestPartRecord(record.value)
			if err != nil {
				tb.Fatalf("decode part record: %v", err)
			}
			if keyGeneration != generation || part.AssetRef.Generation != generation || part.GenerationID != generation || part.Reason != string(ColumnPublishOperationInsert) || part.PartRole != ColumnManifestPartRoleBase {
				tb.Fatalf("part record key_generation=%d part=%+v want generation=%d insert/base", keyGeneration, part, generation)
			}
		case bytes.HasPrefix(record.key, columnManifestAggregateMetadataRecordPrefixBytes):
			aggregateRecords++
			keyGeneration, _, name, err := columnManifestAggregateMetadataKeyPartsFromRecordKey(record.key)
			if err != nil {
				tb.Fatalf("columnManifestAggregateMetadataKeyPartsFromRecordKey: %v", err)
			}
			part, err := decodeColumnManifestPartRecord(record.value)
			if err != nil {
				tb.Fatalf("decode aggregate metadata record: %v", err)
			}
			if keyGeneration != generation || part.AssetRef.Generation != generation || part.GenerationID != generation || part.AssetRef.Kind != ColumnAssetKindTCS1AggregateMetadata || part.Reason != string(name) {
				tb.Fatalf("aggregate metadata key_generation=%d name=%q part=%+v want generation=%d", keyGeneration, string(name), part, generation)
			}
		case bytes.HasPrefix(record.key, columnManifestDictionaryCodesRecordPrefixBytes):
			keyGeneration, _, columnName, err := columnManifestDictionaryCodesKeyPartsFromRecordKey(record.key)
			if err != nil {
				tb.Fatalf("columnManifestDictionaryCodesKeyPartsFromRecordKey: %v", err)
			}
			part, err := decodeColumnManifestPartRecord(record.value)
			if err != nil {
				tb.Fatalf("decode dictionary record: %v", err)
			}
			if keyGeneration != generation || part.AssetRef.Generation != generation || part.GenerationID != generation || part.AssetRef.Kind != ColumnAssetKindTCS1DictionaryCodes || part.Reason != string(columnName) {
				tb.Fatalf("dictionary key_generation=%d column=%q part=%+v want generation=%d", keyGeneration, string(columnName), part, generation)
			}
		case bytes.HasPrefix(record.key, columnManifestInt64ValuesRecordPrefixBytes):
			keyGeneration, _, columnName, err := columnManifestInt64ValuesKeyPartsFromRecordKey(record.key)
			if err != nil {
				tb.Fatalf("columnManifestInt64ValuesKeyPartsFromRecordKey: %v", err)
			}
			part, err := decodeColumnManifestPartRecord(record.value)
			if err != nil {
				tb.Fatalf("decode int64 values record: %v", err)
			}
			if keyGeneration != generation || part.AssetRef.Generation != generation || part.GenerationID != generation || part.AssetRef.Kind != ColumnAssetKindTCS1Int64Values || part.Reason != string(columnName) {
				tb.Fatalf("int64 values key_generation=%d column=%q part=%+v want generation=%d", keyGeneration, string(columnName), part, generation)
			}
		case bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes), bytes.HasPrefix(record.key, columnVectorIndexStateRecordPrefixBytes):
			tb.Fatalf("compacted manifest retained unsupported vector record key=%q", string(record.key))
		default:
			tb.Fatalf("unexpected manifest record key=%q", string(record.key))
		}
	}
	if wantAggregateMetadata && aggregateRecords == 0 {
		tb.Fatal("compacted manifest has no aggregate metadata records")
	}
	if !wantAggregateMetadata && aggregateRecords != 0 {
		tb.Fatalf("compacted manifest has %d aggregate metadata records, want none", aggregateRecords)
	}
}

func assertColumnStoreCompactionRefsAbsent1953(tb testing.TB, records []columnManifestRecord, refs []ColumnAssetRef) {
	tb.Helper()
	active := make(map[ColumnAssetRef]struct{})
	for _, ref := range columnStoreCompactionRefsFromRecordsForTest1953(tb, records) {
		active[ref] = struct{}{}
	}
	for _, ref := range refs {
		if _, ok := active[ref]; ok {
			tb.Fatalf("superseded ref %+v still present in compacted manifest", ref)
		}
	}
}

func assertColumnStoreCompactionRefsReclaimable1953(tb testing.TB, col *Collection, refs []ColumnAssetRef) {
	tb.Helper()
	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true, CandidateRefs: refs})
	if err != nil {
		tb.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	for _, ref := range refs {
		assertColumnAssetReachabilityEntry1755(tb, plan, ref, ColumnAssetReachabilityReclaimable, false)
	}
}

func assertColumnStoreCompactionAggregateMetadataDiagnostics1953(tb testing.TB, diag ColumnPhysicalQueryDiagnostics, matchedRows int) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceAggregateMetadata || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("aggregate metadata diagnostics=%+v want metadata source", diag)
	}
	if diag.MutationParts != 0 || diag.VisibilityRows != 0 || diag.RowsScanned != 0 || diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 {
		tb.Fatalf("aggregate metadata diagnostics=%+v want insert-only metadata without scans/materialization", diag)
	}
	if diag.MetadataHits == 0 || diag.MetadataEntries == 0 || diag.DecodedMetadataBytes == 0 || diag.PhysicalBytesScanned <= 0 {
		tb.Fatalf("aggregate metadata diagnostics=%+v want metadata hits/entries/bytes", diag)
	}
	if diag.RowsMatched != matchedRows || diag.ReduceRows != matchedRows {
		tb.Fatalf("aggregate metadata diagnostics=%+v want matched/reduced=%d", diag, matchedRows)
	}
}

func TestColumnStoreCompactRequiresColumnStore1953(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.ColumnStoreCompact(context.Background(), ColumnStoreCompactOptions{}); err == nil || !strings.Contains(err.Error(), "column_store") {
		t.Fatalf("ColumnStoreCompact without column store err=%v want explicit failure", err)
	}
}
