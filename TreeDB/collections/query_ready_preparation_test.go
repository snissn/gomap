package collections

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestPrepareQueryReadyColumnGenerationRoutesJSONBenchAfterReopen(t *testing.T) {
	if !typedcolumn.QueryReadyGenerationFileOpenSupported() {
		t.Skip("query-ready generation file open requires read-only mmap support")
	}
	events := columnPhysicalJSONBenchParityEventsP0()
	_, collection, closeFn, _ := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, events)
	defer closeFn()

	prepared, err := collection.PrepareQueryReadyColumnGeneration(context.Background(), QueryReadyColumnPreparationOptions{
		MaxWorkers: 1, MaxInFlightBytes: 64 << 20,
	})
	if err != nil {
		t.Fatalf("prepare query-ready generation: %v", err)
	}
	defer func() { _ = prepared.Close() }()
	files := prepared.Files()
	if files.Base.Path == "" || files.Base.Offset <= 0 || files.Base.Length <= 0 || files.Base.Generation == 0 || files.Base.Kind != QueryReadyColumnGenerationBase {
		t.Fatalf("invalid exact base descriptor: %+v", files.Base)
	}
	info, err := os.Stat(files.Base.Path)
	if err != nil {
		t.Fatal(err)
	}
	if end := files.Base.Offset + files.Base.Length; end > info.Size() {
		t.Fatalf("base descriptor end=%d exceeds file=%d", end, info.Size())
	}
	stats := prepared.Stats()
	if stats.SourceParts != 1 || stats.SourceRows != int64(len(events)) || stats.SourceBytes <= 0 || stats.OutputBytes != files.Base.Length || stats.AssetsProduced != 1 {
		t.Fatalf("preparation stats=%+v", stats)
	}
	wantIdentity, ok := collection.ColumnStoreCacheIdentity()
	if !ok {
		t.Fatal("reopened collection has no cache identity")
	}
	if got := prepared.SnapshotIdentity(); got != wantIdentity {
		t.Fatalf("snapshot identity=%+v want %+v", got, wantIdentity)
	}

	scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, collection, len(events))
	commitCreate := []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "commit"}, {Column: "operation", Value: "create"}}
	postCreate := append(append([]ColumnPhysicalQueryPredicate(nil), commitCreate...), ColumnPhysicalQueryPredicate{Column: "collection", Value: "app.bsky.feed.post"})
	feedCreate := append(append([]ColumnPhysicalQueryPredicate(nil), commitCreate...), ColumnPhysicalQueryPredicate{Column: "collection", Kind: ColumnPhysicalQueryPredicateInList, Values: []string{"app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like"}})
	cases := []struct {
		name string
		req  ColumnPhysicalQueryRequest
	}{
		{"q1", ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"}},
		{"q2", ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountAndDistinct, GroupColumn: "collection", DistinctColumn: "did", Predicates: commitCreate}},
		{"q3", ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupHourCount, GroupColumn: "collection", ValueColumn: "time_us", Predicates: feedCreate}},
		{"q4a", ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", Predicates: postCreate}},
		{"q5", ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", Predicates: postCreate}},
		{"sum_time_second_of_day_square", ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQuerySumSecondOfDaySquare, ValueColumn: "time_us"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runner, err := collection.PrepareQueryReadyColumnPhysicalQuery(files, tc.req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = runner.Close() }()
			for run := 0; run < 2; run++ {
				result, err := runner.Run()
				if err != nil {
					t.Fatal(err)
				}
				wantHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchReferenceLinesP0(tc.name, scanned))
				gotHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchPhysicalLinesP0(tc.name, result.Groups))
				if gotHash != wantHash {
					t.Fatalf("hash=%016x want=%016x groups=%+v", gotHash, wantHash, result.Groups)
				}
				diag := result.Diagnostics
				if diag.StorageSource != ColumnPhysicalQueryStorageSourceQueryReadyBaseDelta || diag.QueryReadyEncodedExecutions != 1 || diag.DocumentMaterializations != 0 || diag.QueryReadyLegacyFallbacks != 0 || diag.QueryReadyPrecomputedAnswers != 0 {
					t.Fatalf("routing diagnostics=%+v", diag)
				}
			}
		})
	}
	open := collection.collectionQueryReadyGenerationCacheSnapshot().Open
	if open.OpenAttempts == 0 || open.LogicalImageBytes != files.Base.Length {
		t.Fatalf("M3 exact-range open stats=%+v descriptor=%+v", open, files.Base)
	}
}

func TestPrepareQueryReadyColumnGenerationCancellationAndLifetime(t *testing.T) {
	if !typedcolumn.QueryReadyGenerationFileOpenSupported() {
		t.Skip("query-ready generation file open requires read-only mmap support")
	}
	_, collection, closeFn, _ := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, columnPhysicalJSONBenchParityEventsP0())
	defer closeFn()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := collection.PrepareQueryReadyColumnGeneration(ctx, QueryReadyColumnPreparationOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preparation err=%v", err)
	}

	prepared, err := collection.PrepareQueryReadyColumnGeneration(context.Background(), QueryReadyColumnPreparationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	files := prepared.Files()
	runner, err := collection.PrepareQueryReadyColumnPhysicalQuery(files, ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"})
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); !errors.Is(err, ErrQueryReadyColumnGenerationBusy) {
		t.Fatalf("close owner with active runner err=%v want busy", err)
	}
	secondRunner, err := collection.PrepareQueryReadyColumnPhysicalQuery(files, ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"})
	if err != nil {
		t.Fatalf("busy owner close changed file-set ownership: %v", err)
	}
	if _, err := secondRunner.Run(); err != nil {
		t.Fatalf("runner acquired after busy owner close: %v", err)
	}
	if err := secondRunner.Close(); err != nil {
		t.Fatalf("close runner acquired after busy owner close: %v", err)
	}
	if _, err := runner.Run(); err != nil {
		t.Fatalf("active runner lost lifetime after rejected owner close: %v", err)
	}
	if err := runner.Close(); err != nil {
		t.Fatalf("close final runner: %v", err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatalf("close owner after final runner: %v", err)
	}
	if _, err := collection.PrepareQueryReadyColumnPhysicalQuery(files, ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"}); err == nil {
		t.Fatal("prepared file set remained acquirable after owner close")
	}
	info, err := os.Stat(files.Base.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != files.Base.Offset {
		t.Fatalf("prepared tail retained after final lease: size=%d want %d", info.Size(), files.Base.Offset)
	}
}

func TestPreparedGenerationOwnerCloseRetiresOnlyItsFileSelection(t *testing.T) {
	if !typedcolumn.QueryReadyGenerationFileOpenSupported() {
		t.Skip("query-ready generation file open requires read-only mmap support")
	}
	_, collection, closeFn, _ := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, columnPhysicalJSONBenchParityEventsP0())
	defer closeFn()
	oldPrepared, err := collection.PrepareQueryReadyColumnGeneration(context.Background(), QueryReadyColumnPreparationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = oldPrepared.Close() }()
	newPrepared, err := collection.PrepareQueryReadyColumnGeneration(context.Background(), QueryReadyColumnPreparationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = newPrepared.Close() }()
	oldFiles, newFiles := oldPrepared.Files(), newPrepared.Files()
	if oldFiles.Base == newFiles.Base {
		t.Fatalf("preparations unexpectedly selected the same base descriptor: %+v", oldFiles.Base)
	}
	request := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"}
	oldRunner, err := collection.PrepareQueryReadyColumnPhysicalQuery(oldFiles, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldRunner.Run(); err != nil {
		t.Fatal(err)
	}
	if err := oldRunner.Close(); err != nil {
		t.Fatal(err)
	}
	newRunner, err := collection.PrepareQueryReadyColumnPhysicalQuery(newFiles, request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = newRunner.Close() }()
	if err := oldPrepared.Close(); err != nil {
		t.Fatalf("close old owner while replacement runner is active: %v", err)
	}
	if _, err := newRunner.Run(); err != nil {
		t.Fatalf("replacement runner after old owner close: %v", err)
	}
	if err := newRunner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := newPrepared.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareQueryReadyColumnGenerationExecutesCanonicalColumnStoreKinds(t *testing.T) {
	if !typedcolumn.QueryReadyGenerationFileOpenSupported() {
		t.Skip("query-ready generation file open requires read-only mmap support")
	}
	_, collection, closeFn, _ := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, columnPhysicalJSONBenchParityEventsP0())
	defer closeFn()
	prepared, err := collection.PrepareQueryReadyColumnGeneration(context.Background(), QueryReadyColumnPreparationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()

	for _, tc := range []struct {
		name string
		req  ColumnPhysicalQueryRequest
	}{
		{name: "q2_group_count_distinct", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"}},
		{name: "q3_hour_count", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us"}},
		{name: "q4b_group_max", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := collection.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("reference query: %v", err)
			}
			runner, err := collection.PrepareQueryReadyColumnPhysicalQuery(prepared.Files(), tc.req)
			if err != nil {
				t.Fatalf("prepare query-ready query: %v", err)
			}
			defer func() { _ = runner.Close() }()
			got, err := runner.Run()
			if err != nil {
				t.Fatalf("run query-ready query: %v", err)
			}
			if !slices.Equal(got.Groups, want.Groups) {
				t.Fatalf("groups=%+v want=%+v", got.Groups, want.Groups)
			}
			if got.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceQueryReadyBaseDelta || got.Diagnostics.QueryReadyEncodedExecutions != 1 || got.Diagnostics.DocumentMaterializations != 0 || got.Diagnostics.QueryReadyLegacyFallbacks != 0 {
				t.Fatalf("routing diagnostics=%+v", got.Diagnostics)
			}
		})
	}
}

func TestPrepareQueryReadyColumnGenerationRejectsCombinedSourceBuildBound(t *testing.T) {
	_, collection, closeFn, refs := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, columnPhysicalJSONBenchParityEventsP0())
	defer closeFn()
	if len(refs) != 1 || refs[0].Length <= 1 {
		t.Fatalf("source refs=%+v", refs)
	}
	before := len(collection.columnAssetLifecycleRegistrySnapshot())
	_, err := collection.PrepareQueryReadyColumnGeneration(context.Background(), QueryReadyColumnPreparationOptions{MaxWorkers: 1, MaxInFlightBytes: refs[0].Length - 1})
	var bound *QueryReadyColumnPreparationBoundError
	if !errors.As(err, &bound) || bound.RequiredBytes != refs[0].Length || bound.MaxBytes != refs[0].Length-1 {
		t.Fatalf("bound err=%v detail=%+v", err, bound)
	}
	bound = nil
	_, err = collection.PrepareQueryReadyColumnGeneration(context.Background(), QueryReadyColumnPreparationOptions{MaxWorkers: 1, MaxInFlightBytes: refs[0].Length})
	if !errors.As(err, &bound) || bound.RequiredBytes <= refs[0].Length || bound.MaxBytes != refs[0].Length {
		t.Fatalf("combined bound err=%v detail=%+v", err, bound)
	}
	if after := len(collection.columnAssetLifecycleRegistrySnapshot()); after != before {
		t.Fatalf("bound rejection registered prepared assets: before=%d after=%d", before, after)
	}
}

func TestPrepareQueryReadyColumnGenerationRejectsMutationAndTombstoneState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(testing.TB, *Collection, columnPhysicalJSONBenchParityEventP0)
	}{
		{"update", func(tb testing.TB, col *Collection, event columnPhysicalJSONBenchParityEventP0) {
			event.TimeUS++
			updateTypedColumnEvent1953(tb, col, event)
		}},
		{"tombstone", func(tb testing.TB, col *Collection, event columnPhysicalJSONBenchParityEventP0) {
			deleteTypedColumnEvent1953(tb, col, event.ID)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := columnPhysicalJSONBenchParityEventsP0()
			_, collection, closeFn, _ := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, events)
			defer closeFn()
			tc.mutate(t, collection, events[0])
			if _, err := collection.PrepareQueryReadyColumnGeneration(context.Background(), QueryReadyColumnPreparationOptions{}); !errors.Is(err, ErrColumnQueryPlanUnsupported) {
				t.Fatalf("mutation preparation err=%v", err)
			}
		})
	}
}
