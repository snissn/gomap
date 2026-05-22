package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestColumnPhysicalQueryAdapterExecutesJSONBenchShapesM13B(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(96)
	reopened, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()

	tests := []struct {
		name       string
		hashName   string
		req        ColumnPhysicalQueryRequest
		wantCount  int
		wantDirect bool
	}{
		{
			name:       "q1",
			hashName:   "q1",
			req:        ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"},
			wantCount:  4,
			wantDirect: true,
		},
		{
			name:       "q2",
			hashName:   "q2",
			req:        ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"},
			wantCount:  4,
			wantDirect: true,
		},
		{
			name:       "q3",
			hashName:   "q3",
			req:        ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us"},
			wantCount:  24,
			wantDirect: true,
		},
		{
			name:       "q4a",
			hashName:   "q4a",
			req:        ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us"},
			wantCount:  12,
			wantDirect: true,
		},
		{
			name:       "q4b",
			hashName:   "q4b",
			req:        ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us"},
			wantCount:  12,
			wantDirect: true,
		},
		{
			name:       "q5",
			hashName:   "q5",
			req:        ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"},
			wantCount:  12,
			wantDirect: true,
		},
		{
			name:       "q5_metadata",
			hashName:   "q5",
			req:        ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"},
			wantCount:  12,
			wantDirect: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := reopened.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("RunColumnPhysicalQuery(%s): %v", tc.name, err)
			}
			if got := len(result.Groups); got != tc.wantCount {
				t.Fatalf("result groups=%d want %d: %+v", got, tc.wantCount, result.Groups)
			}
			gotHash := columnPhysicalQueryHashLinesM13B(columnPhysicalQueryLinesM13B(tc.name, result.Groups))
			wantHash := columnPhysicalQueryReferenceHashM13B(tc.hashName, events)
			if gotHash != wantHash {
				t.Fatalf("%s hash=%016x want %016x lines=%v", tc.name, gotHash, wantHash, columnPhysicalQueryLinesM13B(tc.name, result.Groups))
			}
			if result.Diagnostics.RowMaterializations != 0 {
				t.Fatalf("%s row materializations=%d want zero for physical aggregate/projection adapter", tc.name, result.Diagnostics.RowMaterializations)
			}
			if result.Diagnostics.AssetRefs == 0 || result.Diagnostics.PhysicalBytesScanned <= 0 {
				t.Fatalf("%s missing physical diagnostics: %+v", tc.name, result.Diagnostics)
			}
			if result.Diagnostics.WorkerCount != 1 {
				t.Fatalf("%s worker count=%d want 1 diagnostics=%+v", tc.name, result.Diagnostics.WorkerCount, result.Diagnostics)
			}
			if tc.name != "q5_metadata" && result.Diagnostics.DecodedBlocks == 0 {
				t.Fatalf("%s decoded blocks=0 diagnostics=%+v", tc.name, result.Diagnostics)
			}
			if tc.wantDirect && result.Diagnostics.DirectReduceBlocks != result.Diagnostics.DecodedBlocks {
				t.Fatalf("%s direct reduce blocks=%d want decoded blocks=%d diagnostics=%+v", tc.name, result.Diagnostics.DirectReduceBlocks, result.Diagnostics.DecodedBlocks, result.Diagnostics)
			}
			if !tc.wantDirect && result.Diagnostics.DirectReduceBlocks != 0 {
				t.Fatalf("%s direct reduce blocks=%d want zero for non-vectorized shape diagnostics=%+v", tc.name, result.Diagnostics.DirectReduceBlocks, result.Diagnostics)
			}
			if tc.name == "q5_metadata" {
				if result.Diagnostics.MetadataHits == 0 {
					t.Fatalf("%s metadata hits=0 diagnostics=%+v", tc.name, result.Diagnostics)
				}
				if result.Diagnostics.RowsScanned != 0 || result.Diagnostics.DecodedBlocks != 0 {
					t.Fatalf("%s scanned physical rows on metadata path diagnostics=%+v", tc.name, result.Diagnostics)
				}
			}
			if result.Diagnostics.ReduceRows != len(events) {
				t.Fatalf("%s reduce rows=%d want %d", tc.name, result.Diagnostics.ReduceRows, len(events))
			}
		})
	}
}

func TestColumnPhysicalQueryAggregateMetadataRequiresRegisteredAssetM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(96)
	reopened, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()

	_, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{
		Kind:                  ColumnPhysicalQueryGroupInt64Span,
		GroupColumn:           "did",
		ValueColumn:           "time_us",
		AggregateMetadataName: "missing_metadata",
	})
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) {
		t.Fatalf("RunColumnPhysicalQuery missing aggregate metadata err=%v want unsupported", err)
	}
}

func TestColumnPhysicalQueryAdapterUsesFloorHourForNegativeTimeUSM13B(t *testing.T) {
	const hourUS = columnPhysicalQueryHourUS
	events := []columnPhysicalQueryEventM13B{
		{ID: "e1", TimeUS: -1, Kind: "like", Did: "d1"},
		{ID: "e2", TimeUS: -hourUS, Kind: "post", Did: "d2"},
		{ID: "e3", TimeUS: -hourUS - 1, Kind: "follow", Did: "d3"},
		{ID: "e4", TimeUS: 0, Kind: "like", Did: "d4"},
		{ID: "e5", TimeUS: hourUS, Kind: "post", Did: "d5"},
	}
	reopened, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()

	result, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery negative hours: %v", err)
	}
	got := make(map[string]int, len(result.Groups))
	for _, group := range result.Groups {
		got[group.Key] = group.Count
	}
	want := map[string]int{
		"hour_00": 1,
		"hour_01": 1,
		"hour_22": 1,
		"hour_23": 2,
	}
	if len(got) != len(want) {
		t.Fatalf("hour groups=%v want %v", got, want)
	}
	for key, count := range want {
		if got[key] != count {
			t.Fatalf("hour group %s=%d want %d groups=%v", key, got[key], count, got)
		}
	}
}

func TestColumnPhysicalQueryUTCHourM13B(t *testing.T) {
	const hourUS = columnPhysicalQueryHourUS
	tests := []struct {
		timeUS int64
		want   int
	}{
		{timeUS: -hourUS - 1, want: 22},
		{timeUS: -hourUS, want: 23},
		{timeUS: -1, want: 23},
		{timeUS: 0, want: 0},
		{timeUS: hourUS - 1, want: 0},
		{timeUS: hourUS, want: 1},
	}
	for _, tt := range tests {
		if got := columnPhysicalQueryUTCHour(tt.timeUS); got != tt.want {
			t.Fatalf("columnPhysicalQueryUTCHour(%d)=%d want %d", tt.timeUS, got, tt.want)
		}
	}
}

func TestColumnPhysicalQueryAdapterAppliesMutationVisibilityM13C(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		return []byte(`{"time_us":3,"kind":"share","did":"d1"}`), true, nil
	}); err != nil {
		_ = d.Close()
		t.Fatalf("Update: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("e2")}); err != nil || deleted != 1 {
		_ = d.Close()
		t.Fatalf("DeleteBatch deleted=%d err=%v, want one delete", deleted, err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	result, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupCount,
		GroupColumn: "kind",
	})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	if got, want := columnPhysicalQueryLinesM13B("q1", result.Groups), []string{"q1:share=1"}; !equalStringSets(got, want) {
		t.Fatalf("visibility query lines=%v want %v diagnostics=%+v", got, want, result.Diagnostics)
	}
	if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReconstructionRows != 0 {
		t.Fatalf("aggregate diagnostics materialized/reconstructed rows: %+v", result.Diagnostics)
	}
	if result.Diagnostics.ReduceRows != 1 || result.Diagnostics.DeletedRows != 1 {
		t.Fatalf("visibility diagnostics=%+v want one reduced live row and one tombstone", result.Diagnostics)
	}
	_, err = reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{
		Kind:                  ColumnPhysicalQueryGroupInt64Span,
		GroupColumn:           "did",
		ValueColumn:           "time_us",
		AggregateMetadataName: "min_time_us",
	})
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) {
		t.Fatalf("RunColumnPhysicalQuery metadata with mutation parts err=%v want unsupported", err)
	}
}

func TestColumnPhysicalQueryKeepsInsertOnlyMultiGenerationOnDirectPathM13C(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"alpha"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch e1: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("e2")}, [][]byte{
		[]byte(`{"time_us":2,"kind":"comment","did":"d2","payload":"beta"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch e2: %v", err)
	}
	if got := col.meta.Options.ColumnStore.ActiveManifest.Generation; got <= 1 {
		_ = d.Close()
		t.Fatalf("manifest generation=%d want > 1", got)
	}
	if got := col.meta.Options.ColumnStore.PhysicalMutationParts; got != 0 {
		_ = d.Close()
		t.Fatalf("physical mutation parts=%d want 0", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	result, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupCount,
		GroupColumn: "kind",
	})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	if result.Diagnostics.VisibilityRows != 0 || result.Diagnostics.ReconstructionRows != 0 {
		t.Fatalf("insert-only diagnostics used visibility/reconstruction: %+v", result.Diagnostics)
	}
	if result.Diagnostics.MutationParts != 0 || result.Diagnostics.RowsScanned != 2 {
		t.Fatalf("insert-only diagnostics=%+v want two direct rows and zero mutation parts", result.Diagnostics)
	}
	if got, want := columnPhysicalQueryLinesM13B("q1", result.Groups), []string{"q1:comment=1", "q1:like=1"}; !equalStringSets(got, want) {
		t.Fatalf("query lines=%v want %v diagnostics=%+v", got, want, result.Diagnostics)
	}
}

func TestColumnPhysicalQueryParallelMatchesSerialInsertOnlyM14B(t *testing.T) {
	reopened, closeFn := openColumnPhysicalInsertMultiGenerationFixtureM14B(t, 8)
	defer closeFn()

	tests := []struct {
		name string
		req  ColumnPhysicalQueryRequest
	}{
		{
			name: "count",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"},
		},
		{
			name: "count_distinct",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"},
		},
		{
			name: "hour_count",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us"},
		},
		{
			name: "min",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us"},
		},
		{
			name: "max",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us"},
		},
		{
			name: "span",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			serial, err := reopened.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("serial RunColumnPhysicalQuery: %v", err)
			}
			parallel, err := reopened.RunColumnPhysicalQueryParallel(tc.req, 4)
			if err != nil {
				t.Fatalf("parallel RunColumnPhysicalQuery: %v", err)
			}
			if !equalColumnPhysicalQueryGroups(parallel.Groups, serial.Groups) {
				t.Fatalf("parallel groups=%+v want serial %+v", parallel.Groups, serial.Groups)
			}
			if parallel.Diagnostics.RowMaterializations != 0 {
				t.Fatalf("parallel row materializations=%d want zero diagnostics=%+v", parallel.Diagnostics.RowMaterializations, parallel.Diagnostics)
			}
			if parallel.Diagnostics.DirectReduceBlocks != parallel.Diagnostics.DecodedBlocks {
				t.Fatalf("parallel direct reduce blocks=%d want decoded blocks=%d diagnostics=%+v", parallel.Diagnostics.DirectReduceBlocks, parallel.Diagnostics.DecodedBlocks, parallel.Diagnostics)
			}
			if got, want := parallel.Diagnostics.ReduceRows, serial.Diagnostics.ReduceRows; got != want {
				t.Fatalf("parallel reduce rows=%d want serial %d diagnostics=%+v", got, want, parallel.Diagnostics)
			}
			if got, want := parallel.Diagnostics.ScheduledGranules, serial.Diagnostics.ScheduledGranules; got != want {
				t.Fatalf("parallel scheduled granules=%d want serial %d diagnostics=%+v", got, want, parallel.Diagnostics)
			}
			if parallel.Diagnostics.PhysicalBytesScanned != serial.Diagnostics.PhysicalBytesScanned {
				t.Fatalf("parallel bytes=%d want serial %d diagnostics=%+v", parallel.Diagnostics.PhysicalBytesScanned, serial.Diagnostics.PhysicalBytesScanned, parallel.Diagnostics)
			}
			overPartitioned, err := reopened.RunColumnPhysicalQueryParallel(tc.req, 128)
			if err != nil {
				t.Fatalf("over-partitioned RunColumnPhysicalQueryParallel: %v", err)
			}
			if !equalColumnPhysicalQueryGroups(overPartitioned.Groups, serial.Groups) {
				t.Fatalf("over-partitioned groups=%+v want serial %+v", overPartitioned.Groups, serial.Groups)
			}
		})
	}
}

func equalColumnPhysicalQueryGroups(left, right []ColumnPhysicalQueryGroup) bool {
	left = append([]ColumnPhysicalQueryGroup(nil), left...)
	right = append([]ColumnPhysicalQueryGroup(nil), right...)
	sort.Slice(left, func(i, j int) bool {
		if left[i].Key != left[j].Key {
			return left[i].Key < left[j].Key
		}
		if left[i].Count != left[j].Count {
			return left[i].Count < left[j].Count
		}
		return left[i].Int64 < left[j].Int64
	})
	sort.Slice(right, func(i, j int) bool {
		if right[i].Key != right[j].Key {
			return right[i].Key < right[j].Key
		}
		if right[i].Count != right[j].Count {
			return right[i].Count < right[j].Count
		}
		return right[i].Int64 < right[j].Int64
	})
	return reflect.DeepEqual(left, right)
}

func TestColumnPhysicalQuerySnapshotViewSingleRefUsesPinnedManifestM14B(t *testing.T) {
	reopened, closeFn := openColumnPhysicalQueryFixtureM13B(t, columnPhysicalQueryFixtureEventsM13B(16))
	defer closeFn()

	view, closeView, err := reopened.prepareColumnPhysicalScanSnapshotView()
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	if got, want := len(view.AssetRefs), 1; got != want {
		t.Fatalf("asset refs=%d want single-ref fixture", got)
	}
	if _, err := reopened.InsertBatch([][]byte{[]byte("e_published_after_pin")}, [][]byte{
		[]byte(`{"time_us":1700000000000999,"kind":"after_pin","did":"did_after","payload":"ignored"}`),
	}); err != nil {
		t.Fatalf("InsertBatch after pinned view: %v", err)
	}

	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}
	pinned, err := reopened.runColumnPhysicalQueryInSnapshotView(view, req)
	if err != nil {
		t.Fatalf("runColumnPhysicalQueryInSnapshotView: %v", err)
	}
	latest, err := reopened.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("latest RunColumnPhysicalQuery: %v", err)
	}
	if got, want := pinned.Diagnostics.ReduceRows, 16; got != want {
		t.Fatalf("pinned reduce rows=%d want %d diagnostics=%+v", got, want, pinned.Diagnostics)
	}
	if got, want := latest.Diagnostics.ReduceRows, 17; got != want {
		t.Fatalf("latest reduce rows=%d want %d diagnostics=%+v", got, want, latest.Diagnostics)
	}
	if _, ok := columnPhysicalQueryGroupCountsM14B(pinned.Groups)["after_pin"]; ok {
		t.Fatalf("pinned view included post-pin row: groups=%+v diagnostics=%+v", pinned.Groups, pinned.Diagnostics)
	}
	if got := columnPhysicalQueryGroupCountsM14B(latest.Groups)["after_pin"]; got != 1 {
		t.Fatalf("latest view after_pin count=%d want 1 groups=%+v diagnostics=%+v", got, latest.Groups, latest.Diagnostics)
	}
}

func TestColumnPhysicalQueryParallelFailsClosedForSerialShapesM14B(t *testing.T) {
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}
	multiRef, closeMulti := openColumnPhysicalInsertMultiGenerationFixtureM14B(t, 4)
	defer closeMulti()
	if _, err := multiRef.RunColumnPhysicalQueryParallel(req, 1); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "at least two workers") {
		t.Fatalf("RunColumnPhysicalQueryParallel one worker err=%v want fail-closed worker-count error", err)
	}

	singleRef, closeSingle := openColumnPhysicalQueryFixtureM13B(t, columnPhysicalQueryFixtureEventsM13B(16))
	defer closeSingle()
	if _, err := singleRef.RunColumnPhysicalQueryParallel(req, 4); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "more than one asset ref") {
		t.Fatalf("RunColumnPhysicalQueryParallel one ref err=%v want fail-closed asset-ref error", err)
	}
}

func TestColumnPhysicalQueryParallelFailsClosedForMutationVisibilityM14B(t *testing.T) {
	reopened, closeFn, _ := openColumnPhysicalMutationFixtureM13C(t, 64)
	defer closeFn()

	for _, workers := range []int{1, 4} {
		_, err := reopened.RunColumnPhysicalQueryParallel(ColumnPhysicalQueryRequest{
			Kind:        ColumnPhysicalQueryGroupCount,
			GroupColumn: "kind",
		}, workers)
		if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "partitioned visibility execution") {
			t.Fatalf("RunColumnPhysicalQueryParallel workers=%d err=%v want fail-closed partitioned visibility error", workers, err)
		}
	}
}

func columnPhysicalQueryGroupCountsM14B(groups []ColumnPhysicalQueryGroup) map[string]int {
	out := make(map[string]int, len(groups))
	for _, group := range groups {
		out[group.Key] = group.Count
	}
	return out
}

func TestColumnPhysicalScanHonorsCancellationBeforeSchedulingRefsM14B(t *testing.T) {
	reopened, closeFn := openColumnPhysicalInsertMultiGenerationFixtureM14B(t, 4)
	defer closeFn()

	view, closeView, err := reopened.prepareColumnPhysicalScanSnapshotView()
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	diag, err := reopened.scanColumnPhysicalRowsInSnapshotView(view, columnPhysicalScanRequest{
		ProjectedColumns: []string{"kind"},
		Visitor: func(columnPhysicalScanRowView) error {
			t.Fatal("visitor should not run after cancellation")
			return nil
		},
		RequireInsertOnly: true,
		ShouldCancel:      func() bool { return true },
	})
	if !errors.Is(err, errColumnPhysicalScanCancelled) {
		t.Fatalf("scan err=%v want cancellation", err)
	}
	if diag.ScheduledGranules != 0 || diag.DecodedBlocks != 0 || diag.RowsScanned != 0 {
		t.Fatalf("cancelled scan scheduled work: %+v", diag)
	}
}

func TestColumnPhysicalScanRejectsRemainderWithoutModuloM14B(t *testing.T) {
	reopened, closeFn := openColumnPhysicalInsertMultiGenerationFixtureM14B(t, 4)
	defer closeFn()

	view, closeView, err := reopened.prepareColumnPhysicalScanSnapshotView()
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	_, err = reopened.scanColumnPhysicalRowsInSnapshotView(view, columnPhysicalScanRequest{
		ProjectedColumns:    []string{"kind"},
		Visitor:             func(columnPhysicalScanRowView) error { return nil },
		RequireInsertOnly:   true,
		RefOrdinalRemainder: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "requires non-zero modulo") {
		t.Fatalf("scan err=%v want remainder without modulo rejection", err)
	}
}

func TestMergeColumnPhysicalQueryDiagnosticsTreatsMutationPartsAsViewLevelM14B(t *testing.T) {
	left := ColumnPhysicalQueryDiagnostics{MutationParts: 2, DecodedBlocks: 1, SegmentFileCacheHits: 2, SegmentFileCacheMisses: 3}
	right := ColumnPhysicalQueryDiagnostics{MutationParts: 2, DecodedBlocks: 3, SegmentFileCacheHits: 5, SegmentFileCacheMisses: 7}
	merged := mergeColumnPhysicalQueryDiagnostics(left, right)
	if got, want := merged.MutationParts, 2; got != want {
		t.Fatalf("mutation parts=%d want view-level max %d", got, want)
	}
	if got, want := merged.DecodedBlocks, 4; got != want {
		t.Fatalf("decoded blocks=%d want summed work %d", got, want)
	}
	if got, want := merged.SegmentFileCacheHits, uint64(7); got != want {
		t.Fatalf("cache hits=%d want summed %d", got, want)
	}
	if got, want := merged.SegmentFileCacheMisses, uint64(10); got != want {
		t.Fatalf("cache misses=%d want summed %d", got, want)
	}
}

func TestColumnStoreGetReconstructsRetainedPayloadM13C(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"alpha","nested":{"keep":true}}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	got, err := reopened.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":1,"kind":"like","did":"d1","payload":"alpha","nested":{"keep":true}}`))

	raw := readRawPrimaryDocumentForTestM13C(t, reopen, "events", []byte("e1"))
	if strings.Contains(string(raw), "time_us") || strings.Contains(string(raw), "kind") || strings.Contains(string(raw), "did") {
		t.Fatalf("raw retained payload still duplicates declared fields: %s", raw)
	}
	if !strings.Contains(string(raw), "payload") {
		t.Fatalf("raw retained payload lost non-column field: %s", raw)
	}
}

func TestColumnStoreRetainedPayloadRejectsCreateIndexOnDeclaredColumnM13C(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "kind_idx", Field: "kind", ValueType: IndexValueString}); err == nil || !strings.Contains(err.Error(), "retained-payload column field") {
		t.Fatalf("CreateIndex on declared column err=%v want retained-payload column rejection", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "payload_idx", Field: "payload", ValueType: IndexValueString}); err != nil {
		t.Fatalf("CreateIndex on retained payload field: %v", err)
	}
}

func TestColumnStoreRetainedPayloadNoneRejectsCreateIndexOnAnyFieldM13C(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	cfg := testColumnStoreConfig(nil)
	cfg.RetainedPayload = ColumnRetainedPayloadNone
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: cfg},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "payload_idx", Field: "payload", ValueType: IndexValueString}); err == nil || !strings.Contains(err.Error(), "retained-payload-none") {
		t.Fatalf("CreateIndex on retained-payload-none field err=%v want rejection", err)
	}
}

func TestColumnStoreRetainedPayloadNoneAllowsIndexWhenColumnStoreDisabledM13C(t *testing.T) {
	meta := CollectionMeta{
		Name: "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			RetainedPayload: ColumnRetainedPayloadNone,
		}},
	}
	err := rejectCreateIndexOnRetainedColumnField(meta, IndexDefinition{Name: "payload_idx", Field: "payload", ValueType: IndexValueString})
	if err != nil {
		t.Fatalf("disabled column_store CreateIndex rejection err=%v want nil", err)
	}
}

func TestColumnStoreRetainedPayloadRejectsCreateIndexOnColumnSubtreeM13C(t *testing.T) {
	meta := CollectionMeta{
		Name: "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled:         true,
			RetainedPayload: ColumnRetainedPayloadNonColumn,
			Columns: []ColumnStoreColumn{
				{Name: "repo", Path: "commit.repo", ValueType: ColumnStoreValueString},
				{Name: "author", Path: "author", ValueType: ColumnStoreValueString},
			},
		}},
	}
	cases := []struct {
		name  string
		field string
		want  bool
	}{
		{name: "exact", field: "commit.repo", want: true},
		{name: "descendant", field: "commit.repo.id", want: true},
		{name: "ancestor", field: "commit", want: true},
		{name: "root descendant", field: "author.name", want: true},
		{name: "sibling prefix", field: "commit.repository", want: false},
		{name: "retained payload", field: "payload.repo", want: false},
	}
	for _, tc := range cases {
		err := rejectCreateIndexOnRetainedColumnField(meta, IndexDefinition{Name: tc.name + "_idx", Field: tc.field, ValueType: IndexValueString})
		if tc.want && (err == nil || !strings.Contains(err.Error(), "retained-payload column field")) {
			t.Fatalf("%s CreateIndex err=%v want retained-payload column rejection", tc.name, err)
		}
		if !tc.want && err != nil {
			t.Fatalf("%s CreateIndex err=%v want nil", tc.name, err)
		}
	}
}

func TestColumnStoreRetainedPayloadDisablesDirectBufferedUpdateM13C(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	col := &Collection{
		db:          d,
		writeDomain: &collectionWriteDomain{},
	}
	opts := collectionOptions{documentFormat: DocumentFormatBSON}
	changed := []preparedBatchUpdate{{
		documentID:         []byte("e1"),
		document:           []byte("full-document"),
		primaryDocument:    []byte("retained-document"),
		hasPrimaryDocument: true,
	}}
	noColumnMeta := CollectionMeta{Name: "events"}
	if !col.shouldUseDirectBufferedUpdatePlan(noColumnMeta, opts, true, updateBatchModeNoSecondaryUniqueIndexChanges, updateBatchSecondaryIndexChangeSummary{}, changed, true) {
		t.Fatal("control metadata did not use direct buffered update plan")
	}
	columnMeta, err := normalizeCollectionMeta(CollectionMeta{
		Name: "events",
		Options: CollectionOptions{
			ColumnStore: testColumnStoreConfig(nil),
		},
	})
	if err != nil {
		t.Fatalf("normalize column meta: %v", err)
	}
	if !columnStoreNeedsRetainedPayloadTransform(columnMeta) {
		t.Fatalf("column metadata does not require retained payload transform: %+v", columnMeta.Options.ColumnStore)
	}
	if col.shouldUseDirectBufferedUpdatePlan(columnMeta, opts, true, updateBatchModeNoSecondaryUniqueIndexChanges, updateBatchSecondaryIndexChangeSummary{}, changed, true) {
		t.Fatal("retained-payload column store used direct buffered update plan")
	}
}

func TestPreparedBatchUpdatePrimaryDocumentPreservesEmptyRetainedPayloadM13C(t *testing.T) {
	update := preparedBatchUpdate{
		document:           []byte(`{"time_us":1}`),
		primaryDocument:    []byte{},
		hasPrimaryDocument: true,
	}
	if got := preparedBatchUpdatePrimaryDocument(update); got == nil || len(got) != 0 {
		t.Fatalf("preparedBatchUpdatePrimaryDocument len=%d nil=%v, want explicit empty retained payload", len(got), got == nil)
	}
	update.hasPrimaryDocument = false
	if got := preparedBatchUpdatePrimaryDocument(update); !bytes.Equal(got, update.document) {
		t.Fatalf("preparedBatchUpdatePrimaryDocument fallback=%s want full document", got)
	}
}

func TestColumnStoreRetainedPayloadDisablesBufferedUpdateReadsM13C(t *testing.T) {
	columnMeta, err := normalizeCollectionMeta(CollectionMeta{
		Name: "events",
		Options: CollectionOptions{
			ColumnStore: testColumnStoreConfig(nil),
		},
	})
	if err != nil {
		t.Fatalf("normalize column meta: %v", err)
	}
	if !columnStoreNeedsRetainedPayloadTransform(columnMeta) {
		t.Fatalf("column metadata does not require retained payload transform: %+v", columnMeta.Options.ColumnStore)
	}
	const baseSystemRoot = 7
	domain := &collectionWriteDomain{
		loaded:                 true,
		meta:                   columnMeta,
		catalog:                &collectionCatalog{meta: columnMeta},
		baseSystemRoot:         baseSystemRoot,
		count:                  1,
		primaryOverlay:         newBufferedPrimaryOverlay(1),
		primaryCache:           newBufferedPrimaryOverlay(1),
		primaryCacheSystemRoot: baseSystemRoot,
		primaryCacheCollection: columnMeta.Name,
	}
	entry := directBufferedRootEntry{key: []byte("e1"), value: []byte(`{"payload":"retained"}`)}
	domain.primaryOverlay.addEntry(entry)
	domain.primaryCache.addEntry(entry)
	items := []updateBatchItem{{UpdateBatchItem: UpdateBatchItem{DocumentID: []byte("e1")}}}

	if cached := snapshotUpdateBatchPrimaryCache(domain, columnMeta, baseSystemRoot, items); cached.enabled {
		defer putUpdateBatchBufferedEntries(cached.primaryEntries, cached.primaryBuffer)
		t.Fatal("retained-payload column store used primary cache for update planning")
	}
	read, templateRuns, blocked, stale, needIndex, err := snapshotUpdateBatchBufferedReadLocked(domain, columnMeta, 1, baseSystemRoot, items, DocumentFormatJSON, false)
	defer resetCollectionTables(templateRuns)
	defer putUpdateBatchBufferedEntries(read.primaryEntries, read.primaryBuffer)
	if err != nil {
		t.Fatalf("snapshotUpdateBatchBufferedReadLocked: %v", err)
	}
	if read.enabled || !blocked || stale || needIndex {
		t.Fatalf("retained-payload column store did not fail closed on buffered read: enabled=%v blocked=%v stale=%v needIndex=%v", read.enabled, blocked, stale, needIndex)
	}
}

func TestColumnStoreReconstructionPreservesMissingNullableColumnsM13C(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: ColumnRetainedPayloadNonColumn,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
		Columns: []ColumnStoreColumn{
			{Name: "nested", Path: "a.b", ValueType: ColumnStoreValueString, Nullable: true},
			{Name: "top", Path: "top", ValueType: ColumnStoreValueString, Nullable: true},
		},
	}
	tests := []struct {
		name string
		doc  []byte
	}{
		{
			name: "missing nested under scalar ancestor stays omitted",
			doc:  []byte(`{"a":"keep","payload":1}`),
		},
		{
			name: "explicit nested null stays explicit",
			doc:  []byte(`{"a":{"b":null},"payload":1}`),
		},
		{
			name: "explicit top-level null stays explicit",
			doc:  []byte(`{"top":null,"payload":1}`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			retained, err := columnRetainedPayloadFromJSONDocument(cfg, tt.doc)
			if err != nil {
				t.Fatalf("columnRetainedPayloadFromJSONDocument: %v", err)
			}
			rows, err := extractColumnDeclaredRowsFromJSONDocuments(cfg, []columnWriteDocument{{
				ID:       []byte("e1"),
				Document: tt.doc,
			}})
			if err != nil {
				t.Fatalf("extractColumnDeclaredRowsFromJSONDocuments: %v", err)
			}
			encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
				Collection:        "events",
				Namespace:         "events/column-assets",
				Generation:        1,
				PartID:            1,
				AppliedCommandLSN: 1,
				Operation:         ColumnPublishOperationInsert,
				Columns:           cfg.Columns,
				Rows:              rows,
			})
			if err != nil {
				t.Fatalf("encodeColumnPhysicalAsset: %v", err)
			}
			asset, err := decodeColumnPhysicalAsset(encoded)
			if err != nil {
				t.Fatalf("decodeColumnPhysicalAsset: %v", err)
			}
			got, err := reconstructColumnJSONDocument(cfg, retained, asset.Rows[0].Values)
			if err != nil {
				t.Fatalf("reconstructColumnJSONDocument: %v", err)
			}
			assertJSONEqualM13C(t, got, tt.doc)
		})
	}
}

func TestColumnStoreReconstructionFailsClosedOnScalarAncestorM13C(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: ColumnRetainedPayloadNonColumn,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
		Columns: []ColumnStoreColumn{
			{Name: "nested", Path: "a.b", ValueType: ColumnStoreValueString, Nullable: true},
		},
	}
	retained := []byte(`{"a":"keep","payload":1}`)
	values := []columnDeclaredValue{{
		Type:    ColumnStoreValueString,
		Present: true,
		Null:    false,
		String:  "value",
	}}
	if _, err := reconstructColumnJSONDocument(cfg, retained, values); err == nil || !strings.Contains(err.Error(), `non-object ancestor "a"`) {
		t.Fatalf("reconstruct scalar ancestor err=%v want non-object ancestor failure", err)
	}
}

func TestColumnStoreScanDocumentsReconstructsRetainedPayloadM13C(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"alpha"}`),
		[]byte(`{"time_us":2,"kind":"comment","did":"d2","payload":"beta"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, modified, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		assertJSONEqualM13C(t, current, []byte(`{"time_us":1,"kind":"like","did":"d1","payload":"alpha"}`))
		return []byte(`{"time_us":3,"kind":"share","did":"d1","payload":"alpha2"}`), true, nil
	}); err != nil || !modified {
		_ = d.Close()
		t.Fatalf("Update modified=%t err=%v", modified, err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("e2")}); err != nil || deleted != 1 {
		_ = d.Close()
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	records, truncated, err := reopened.ScanDocuments(10)
	if err != nil {
		t.Fatalf("ScanDocuments: %v", err)
	}
	if truncated {
		t.Fatalf("ScanDocuments truncated")
	}
	if len(records) != 1 {
		t.Fatalf("ScanDocuments records=%d want 1", len(records))
	}
	if got, want := string(records[0].ID), "e1"; got != want {
		t.Fatalf("ScanDocuments id=%q want %q", got, want)
	}
	assertJSONEqualM13C(t, records[0].Document, []byte(`{"time_us":3,"kind":"share","did":"d1","payload":"alpha2"}`))

	raw := readRawPrimaryDocumentForTestM13C(t, reopen, "events", []byte("e1"))
	if strings.Contains(string(raw), "time_us") || strings.Contains(string(raw), "kind") || strings.Contains(string(raw), "did") {
		t.Fatalf("raw retained payload still duplicates declared fields: %s", raw)
	}
}

func TestColumnStoreScanDocumentsLimitFiltersVisibilityRowsM13C(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2"), []byte("e3")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"alpha"}`),
		[]byte(`{"time_us":2,"kind":"comment","did":"d2","payload":"beta"}`),
		[]byte(`{"time_us":3,"kind":"share","did":"d3","payload":"gamma"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	records, truncated, err := reopened.ScanDocuments(1)
	if err != nil {
		t.Fatalf("ScanDocuments: %v", err)
	}
	if !truncated {
		t.Fatalf("ScanDocuments truncated=false want true")
	}
	if len(records) != 1 {
		t.Fatalf("ScanDocuments records=%d want 1", len(records))
	}
	assertJSONEqualM13C(t, records[0].Document, []byte(`{"time_us":1,"kind":"like","did":"d1","payload":"alpha"}`))

	snap := reopen.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := reopened.catalogForSnapshot(snap)
	if err != nil {
		t.Fatalf("catalogForSnapshot: %v", err)
	}
	cfg := catalog.meta.Options.ColumnStore.copy()
	visible, err := reopened.scanColumnPhysicalVisibleRowsAtSnapshotForTargets(
		snap,
		catalog,
		catalog.meta.Name,
		catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name)),
		cfg,
		true,
		newColumnPhysicalVisibilityTargetIDs([][]byte{[]byte("e2")}),
		nil,
	)
	if err != nil {
		t.Fatalf("scanColumnPhysicalVisibleRowsAtSnapshotForTargets: %v", err)
	}
	if got, want := len(visible.Rows), 1; got != want {
		t.Fatalf("visible rows=%d want %d", got, want)
	}
	if got, want := string(visible.Rows[0].ID), "e2"; got != want {
		t.Fatalf("visible id=%q want %q", got, want)
	}
	if visible.Diagnostics.RowsScanned < 3 {
		t.Fatalf("diagnostic rows scanned=%d want at least 3", visible.Diagnostics.RowsScanned)
	}
}

func TestColumnStoreReconstructionPreservesDeclaredAndRetainedUpdatesM13C(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"alpha"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, modified, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		assertJSONEqualM13C(t, current, []byte(`{"time_us":1,"kind":"like","did":"d1","payload":"alpha"}`))
		return []byte(`{"time_us":1,"kind":"like","did":"d1","payload":"beta"}`), true, nil
	}); err != nil || !modified {
		_ = d.Close()
		t.Fatalf("retained Update modified=%t err=%v", modified, err)
	}
	if _, modified, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		assertJSONEqualM13C(t, current, []byte(`{"time_us":1,"kind":"like","did":"d1","payload":"beta"}`))
		return []byte(`{"time_us":3,"kind":"share","did":"d1","payload":"beta"}`), true, nil
	}); err != nil || !modified {
		_ = d.Close()
		t.Fatalf("declared Update modified=%t err=%v", modified, err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	got, err := reopened.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":3,"kind":"share","did":"d1","payload":"beta"}`))
	result, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	if got, want := columnPhysicalQueryLinesM13B("q1", result.Groups), []string{"q1:share=1"}; !equalStringSets(got, want) {
		t.Fatalf("query lines=%v want %v diagnostics=%+v", got, want, result.Diagnostics)
	}
}

func TestColumnStoreDeleteHidesRetainedAndDeclaredStateM13C(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"alpha"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("e1")}); err != nil || deleted != 1 {
		_ = d.Close()
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	if got, err := reopened.Get([]byte("e1")); err != nil || got != nil {
		t.Fatalf("Get deleted got=%s err=%v, want nil nil", got, err)
	}
	result, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	if len(result.Groups) != 0 || result.Diagnostics.ReduceRows != 0 || result.Diagnostics.DeletedRows != 1 {
		t.Fatalf("delete visibility result=%+v diagnostics=%+v", result.Groups, result.Diagnostics)
	}
}

func TestColumnStoreDeleteUpdatedRowM13C(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"alpha"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, modified, err := col.Update([]byte("e1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"time_us":2,"kind":"share","did":"d1","payload":"beta"}`), true, nil
	}); err != nil || !modified {
		_ = d.Close()
		t.Fatalf("Update modified=%t err=%v", modified, err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("e1")}); err != nil || deleted != 1 {
		_ = d.Close()
		t.Fatalf("DeleteBatch updated row deleted=%d err=%v", deleted, err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	if got, err := reopened.Get([]byte("e1")); err != nil || got != nil {
		t.Fatalf("Get deleted updated row got=%s err=%v, want nil nil", got, err)
	}
	result, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	if len(result.Groups) != 0 || result.Diagnostics.ReduceRows != 0 || result.Diagnostics.DeletedRows != 1 {
		t.Fatalf("delete updated visibility result=%+v diagnostics=%+v", result.Groups, result.Diagnostics)
	}
}

func TestColumnStoreDeleteUpdatedRowAfterManyMutationsM13C(t *testing.T) {
	const rows = 1024
	const deletedRows = 103 // delete every 10th index: 0,10,...,1020.
	col, closeFn, liveRows := openColumnPhysicalMutationFixtureM13C(t, rows)
	defer closeFn()
	if liveRows != rows-deletedRows {
		t.Fatalf("liveRows=%d want %d", liveRows, rows-deletedRows)
	}
	if got, err := col.Get([]byte("e000000")); err != nil || got != nil {
		t.Fatalf("Get deleted updated row got=%s err=%v, want nil nil", got, err)
	}
	result, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	if result.Diagnostics.DeletedRows != deletedRows {
		t.Fatalf("deleted rows=%d want %d diagnostics=%+v", result.Diagnostics.DeletedRows, deletedRows, result.Diagnostics)
	}
}

func TestColumnPhysicalVisibilityInspectorValidatesManifestPartsM13C(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"a"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2","payload":"b"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		assertJSONEqualM13C(t, current, []byte(`{"time_us":1,"kind":"like","did":"d1","payload":"a"}`))
		return []byte(`{"time_us":3,"kind":"share","did":"d1","payload":"a2"}`), true, nil
	}); err != nil {
		_ = d.Close()
		t.Fatalf("Update: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("e2")}); err != nil || deleted != 1 {
		_ = d.Close()
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("e2")}, [][]byte{
		[]byte(`{"time_us":4,"kind":"comment","did":"d2","payload":"b2"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("reinsert InsertBatch: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	inspection := inspectColumnPhysicalVisibilityM13C(t, reopen, reopened)
	if len(inspection.RawRows) != 5 {
		t.Fatalf("raw physical rows=%d want insert/update/delete/reinsert rows: %+v", len(inspection.RawRows), inspection.RawRows)
	}
	if len(inspection.Visible) != 2 {
		t.Fatalf("visible rows=%d want latest e1/e2 rows: %+v", len(inspection.Visible), inspection.Visible)
	}
	assertVisibleRowM13C(t, inspection.Visible["e1"], false, ColumnPublishOperationUpdate, int64(3), "share")
	assertVisibleRowM13C(t, inspection.Visible["e2"], false, ColumnPublishOperationInsert, int64(4), "comment")
	if inspection.Diagnostics.RowMaterializations != 0 {
		t.Fatalf("visibility inspector materialized rows=%d want zero", inspection.Diagnostics.RowMaterializations)
	}
}

func TestColumnStoreRandomizedMutationOracleM13C(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	rng := rand.New(rand.NewSource(1621))
	ids := []string{"e0", "e1", "e2", "e3", "e4", "e5"}
	model := make(map[string]columnStoreOracleDocM13C)
	touched := make(map[string]struct{})

	for i, id := range ids {
		doc := columnStoreOracleDocM13C{
			TimeUS:  1_700_000_000_000_000 + int64(i),
			Kind:    fmt.Sprintf("kind_%d", i%3),
			Did:     fmt.Sprintf("did_%d", i%2),
			Payload: fmt.Sprintf("seed_%d", i),
			Extra:   i,
		}
		if _, err := col.InsertBatch([][]byte{[]byte(id)}, [][]byte{doc.JSON()}); err != nil {
			_ = d.Close()
			t.Fatalf("seed InsertBatch %s: %v", id, err)
		}
		model[id] = doc
		touched[id] = struct{}{}
	}

	for step := 0; step < 80; step++ {
		id := ids[rng.Intn(len(ids))]
		current, alive := model[id]
		switch rng.Intn(5) {
		case 0:
			if alive {
				continue
			}
			doc := columnStoreOracleDocM13C{
				TimeUS:  1_700_000_100_000_000 + int64(step),
				Kind:    fmt.Sprintf("kind_%d", rng.Intn(4)),
				Did:     fmt.Sprintf("did_%d", rng.Intn(3)),
				Payload: fmt.Sprintf("reinsert_%02d", step),
				Extra:   step,
			}
			if _, err := col.InsertBatch([][]byte{[]byte(id)}, [][]byte{doc.JSON()}); err != nil {
				_ = d.Close()
				t.Fatalf("reinsert step=%d id=%s: %v", step, id, err)
			}
			model[id] = doc
			touched[id] = struct{}{}
		case 1:
			if !alive {
				continue
			}
			next := current
			next.Payload = fmt.Sprintf("payload_%02d_%d", step, rng.Intn(100))
			next.Extra = step
			if _, modified, err := col.Update([]byte(id), func(current []byte) ([]byte, bool, error) {
				assertJSONEqualM13C(t, current, model[id].JSON())
				return next.JSON(), true, nil
			}); err != nil || !modified {
				_ = d.Close()
				t.Fatalf("retained update step=%d id=%s modified=%t err=%v", step, id, modified, err)
			}
			model[id] = next
		case 2:
			if !alive {
				continue
			}
			next := current
			next.TimeUS += int64(1000 + step)
			next.Kind = fmt.Sprintf("kind_%d", rng.Intn(4))
			next.Did = fmt.Sprintf("did_%d", rng.Intn(3))
			if _, modified, err := col.Update([]byte(id), func(current []byte) ([]byte, bool, error) {
				assertJSONEqualM13C(t, current, model[id].JSON())
				return next.JSON(), true, nil
			}); err != nil || !modified {
				_ = d.Close()
				t.Fatalf("declared update step=%d id=%s modified=%t err=%v", step, id, modified, err)
			}
			model[id] = next
		case 3:
			if !alive {
				continue
			}
			if deleted, err := col.DeleteBatch([][]byte{[]byte(id)}); err != nil || deleted != 1 {
				_ = d.Close()
				t.Fatalf("delete step=%d id=%s deleted=%d err=%v", step, id, deleted, err)
			}
			delete(model, id)
		case 4:
			if err := d.Checkpoint(); err != nil {
				_ = d.Close()
				t.Fatalf("Checkpoint step=%d: %v", step, err)
			}
		}
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	for _, id := range ids {
		got, err := reopened.Get([]byte(id))
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		doc, alive := model[id]
		if !alive {
			if got != nil {
				t.Fatalf("Get %s=%s want deleted/missing nil", id, got)
			}
			continue
		}
		assertJSONEqualM13C(t, got, doc.JSON())
		raw := readRawPrimaryDocumentForTestM13C(t, reopen, "events", []byte(id))
		if strings.Contains(string(raw), "time_us") || strings.Contains(string(raw), "kind") || strings.Contains(string(raw), "did") {
			t.Fatalf("raw retained payload for %s duplicates declared fields: %s", id, raw)
		}
		if !strings.Contains(string(raw), "payload") {
			t.Fatalf("raw retained payload for %s lost retained field: %s", id, raw)
		}
	}
	result, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupCount,
		GroupColumn: "kind",
	})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	if got, want := columnPhysicalQueryLinesM13B("q1", result.Groups), columnStoreOracleGroupLinesM13C(model); !equalStringSets(got, want) {
		t.Fatalf("oracle q1 lines=%v want %v diagnostics=%+v", got, want, result.Diagnostics)
	}
	if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReconstructionRows != 0 {
		t.Fatalf("aggregate diagnostics materialized/reconstructed rows: %+v", result.Diagnostics)
	}
	inspection := inspectColumnPhysicalVisibilityM13C(t, reopen, reopened)
	if len(inspection.Visible) != len(touched) {
		t.Fatalf("visible latest rows=%d touched=%d", len(inspection.Visible), len(touched))
	}
}

func TestColumnStoreReplayReconstructsRetainedPayloadM13C(t *testing.T) {
	dir, baseLSN := prepareColumnStoreCommandWALDirM10B(t)
	insertPayload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("events", []commitlog.CollectionDocument{{
		ID:       []byte("e1"),
		Document: []byte(`{"time_us":1,"kind":"like","did":"d1","payload":"alpha"}`),
	}})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload insert: %v", err)
	}
	updatePayload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload("events", []commitlog.CollectionDocument{{
		ID:       []byte("e1"),
		Document: []byte(`{"time_us":2,"kind":"share","did":"d1","payload":"beta"}`),
	}})
	if err != nil {
		t.Fatalf("EncodeCollectionUpdateBatchByIDPayload update: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, baseLSN+1, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, insertPayload)
	writeCollectionCommandWALFrame(t, dir, baseLSN+2, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionUpdateBatchByIDV1, updatePayload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	assertColumnManifestStateM10B(t, reopened, 2, baseLSN+2)
	got, err := reopened.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get replayed e1: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":2,"kind":"share","did":"d1","payload":"beta"}`))
	raw := readRawPrimaryDocumentForTestM13C(t, reopen, "events", []byte("e1"))
	if strings.Contains(string(raw), "time_us") || strings.Contains(string(raw), "kind") || strings.Contains(string(raw), "did") || !strings.Contains(string(raw), "payload") {
		t.Fatalf("replayed raw retained payload=%s, want retained-only payload", raw)
	}
	result, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery replayed: %v", err)
	}
	if got, want := columnPhysicalQueryLinesM13B("q1", result.Groups), []string{"q1:share=1"}; !equalStringSets(got, want) {
		t.Fatalf("replayed q1 lines=%v want %v diagnostics=%+v", got, want, result.Diagnostics)
	}
}

func TestColumnStoreQueryAndReconstructionFailClosedMissingAssetM13C(t *testing.T) {
	dir, ref := prepareColumnPhysicalScannerCorruptionFixtureM13A(t)
	assetPath, err := columnAssetSegmentPath(backenddb.ColumnAssetRootDirPath(dir), ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	if err := os.Remove(assetPath); err != nil {
		t.Fatalf("Remove asset: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	if _, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("RunColumnPhysicalQuery missing asset err=%v want os.ErrNotExist", err)
	}
	if got, err := reopened.Get([]byte("e1")); !errors.Is(err, os.ErrNotExist) || got != nil {
		t.Fatalf("Get missing asset got=%s err=%v want fail-closed os.ErrNotExist", got, err)
	}
}

func TestColumnStoreQueryAndReconstructionFailClosedCorruptAssetM13C(t *testing.T) {
	dir, ref := prepareColumnPhysicalScannerCorruptionFixtureM13A(t)
	assetPath, err := columnAssetSegmentPath(backenddb.ColumnAssetRootDirPath(dir), ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	file, err := os.OpenFile(assetPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile asset: %v", err)
	}
	if _, err := file.WriteAt([]byte{0xff}, ref.Offset); err != nil {
		_ = file.Close()
		t.Fatalf("corrupt asset: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close corrupt asset: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	if _, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("RunColumnPhysicalQuery corrupt asset err=%v want checksum failure", err)
	}
	if got, err := reopened.Get([]byte("e1")); err == nil || !strings.Contains(err.Error(), "checksum") || got != nil {
		t.Fatalf("Get corrupt asset got=%s err=%v want fail-closed checksum failure", got, err)
	}
}

func TestColumnStoreQueryAndReconstructionFailClosedTruncatedAssetM13C(t *testing.T) {
	dir, ref := prepareColumnPhysicalScannerCorruptionFixtureM13A(t)
	assetPath, err := columnAssetSegmentPath(backenddb.ColumnAssetRootDirPath(dir), ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	if err := os.Truncate(assetPath, ref.Offset+ref.Length-1); err != nil {
		t.Fatalf("Truncate asset: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	if _, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("RunColumnPhysicalQuery truncated asset err=%v want io.ErrUnexpectedEOF", err)
	}
	if got, err := reopened.Get([]byte("e1")); !errors.Is(err, io.ErrUnexpectedEOF) || got != nil {
		t.Fatalf("Get truncated asset got=%s err=%v want fail-closed io.ErrUnexpectedEOF", got, err)
	}
}

func TestColumnPhysicalAssetScanRejectsWrongNamespaceM13C(t *testing.T) {
	normalized, rows := makeColumnPhysicalAssetBenchmarkRows(t, 4)
	encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        1,
		PartID:            1,
		AppliedCommandLSN: 1,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              rows,
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset: %v", err)
	}
	wrongNamespace := []byte("events/column-assetz")
	mutated := bytes.Replace(encoded, []byte(normalized.AssetManager.Namespace), wrongNamespace, 1)
	if bytes.Equal(mutated, encoded) {
		t.Fatalf("test failed to mutate encoded namespace %q", normalized.AssetManager.Namespace)
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  normalized.AssetManager.Namespace,
		Generation: 1,
		PartID:     1,
		FileID:     columnAssetM12ASegmentFileID,
		Length:     int64(len(mutated)),
		Checksum:   page.Checksum(mutated),
	}
	projection, err := newColumnPhysicalScanProjection(*normalized, nil)
	if err != nil {
		t.Fatalf("newColumnPhysicalScanProjection: %v", err)
	}
	if _, err := scanColumnPhysicalAssetRows(mutated, ref, "events", normalized, projection, nil); err == nil || !strings.Contains(err.Error(), "namespace") {
		t.Fatalf("scan wrong namespace err=%v want namespace failure", err)
	}
}

func equalStringSets(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	gotCopy := append([]string(nil), got...)
	wantCopy := append([]string(nil), want...)
	sort.Strings(gotCopy)
	sort.Strings(wantCopy)
	for i := range gotCopy {
		if gotCopy[i] != wantCopy[i] {
			return false
		}
	}
	return true
}

func assertJSONEqualM13C(t *testing.T, got, want []byte) {
	t.Helper()
	var gotValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("got invalid JSON %q: %v", got, err)
	}
	var wantValue any
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("want invalid JSON %q: %v", want, err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("JSON mismatch\ngot:  %s\nwant: %s", got, want)
	}
}

func readRawPrimaryDocumentForTestM13C(t *testing.T, d *backenddb.DB, collection string, id []byte) []byte {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collection)
	if err != nil {
		t.Fatalf("loadCollectionCatalog: %v", err)
	}
	if catalog == nil {
		t.Fatalf("collection %q not found", collection)
	}
	raw, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionPrimaryRootName(collection), id, nil)
	if err != nil {
		t.Fatalf("raw primary read: %v", err)
	}
	if !found {
		t.Fatalf("raw primary document %q not found", string(id))
	}
	return raw
}

type columnStoreOracleDocM13C struct {
	TimeUS  int64
	Kind    string
	Did     string
	Payload string
	Extra   int
}

func (d columnStoreOracleDocM13C) JSON() []byte {
	return []byte(fmt.Sprintf(`{"time_us":%d,"kind":"%s","did":"%s","payload":"%s","extra":%d}`,
		d.TimeUS, d.Kind, d.Did, d.Payload, d.Extra))
}

func columnStoreOracleGroupLinesM13C(model map[string]columnStoreOracleDocM13C) []string {
	counts := make(map[string]int)
	for _, doc := range model {
		counts[doc.Kind]++
	}
	return columnPhysicalQueryIntLinesM13B("q1", counts)
}

type columnPhysicalVisibilityInspectionM13C struct {
	RawRows     []columnPhysicalVisibleRow
	Visible     map[string]columnPhysicalVisibleRow
	Diagnostics columnPhysicalScanDiagnostics
}

func inspectColumnPhysicalVisibilityM13C(t *testing.T, d *backenddb.DB, col *Collection) columnPhysicalVisibilityInspectionM13C {
	t.Helper()
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.ActiveManifest == nil || cfg.AssetManager == nil {
		t.Fatalf("missing column store manifest metadata: %+v", cfg)
	}
	refs := columnManifestPhysicalAssetRefsForTestM1634(columnManifestAssetRefsForCollectionM12A(t, d, col))
	seenRefs := make(map[[2]uint64]struct{}, len(refs))
	for _, ref := range refs {
		if ref.Namespace != cfg.AssetManager.Namespace {
			t.Fatalf("manifest ref namespace=%q want %q: %+v", ref.Namespace, cfg.AssetManager.Namespace, ref)
		}
		if ref.Generation == 0 || ref.Generation > cfg.ActiveManifest.Generation || ref.PartID == 0 {
			t.Fatalf("manifest ref has invalid generation/part for active generation %d: %+v", cfg.ActiveManifest.Generation, ref)
		}
		key := [2]uint64{ref.Generation, ref.PartID}
		if _, ok := seenRefs[key]; ok {
			t.Fatalf("duplicate manifest ref generation/part: %+v", ref)
		}
		seenRefs[key] = struct{}{}
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref)
		if err != nil {
			t.Fatalf("readColumnPhysicalAssetFromManager(%+v): %v", ref, err)
		}
		if int64(len(raw)) != ref.Length {
			t.Fatalf("asset ref length=%d actual=%d for %+v", ref.Length, len(raw), ref)
		}
		if err := validateColumnPhysicalAssetForManifest(raw, ref, *cfg); err != nil {
			t.Fatalf("validateColumnPhysicalAssetForManifest(%+v): %v", ref, err)
		}
	}

	var rawRows []columnPhysicalVisibleRow
	expected := make(map[string]columnPhysicalVisibleRow)
	_, err := col.scanColumnPhysicalRows(columnPhysicalScanRequest{
		Visitor: func(row columnPhysicalScanRowView) error {
			copied := columnPhysicalVisibleRow{
				Generation:        row.Generation,
				PartID:            row.PartID,
				AppliedCommandLSN: row.AppliedCommandLSN,
				Operation:         row.Operation,
				RowIndex:          row.RowIndex,
				ID:                bytes.Clone(row.ID),
				Deleted:           row.Deleted,
			}
			if !row.Deleted {
				copied.Values = cloneColumnDeclaredValues(row.Values)
			}
			rawRows = append(rawRows, copied)
			key := string(row.ID)
			if existing, ok := expected[key]; !ok || columnPhysicalVisibleRowNewer(copied, existing) {
				expected[key] = copied
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("scanColumnPhysicalRows for visibility inspector: %v", err)
	}
	visible, err := col.scanColumnPhysicalVisibleRows(nil)
	if err != nil {
		t.Fatalf("scanColumnPhysicalVisibleRows: %v", err)
	}
	got := make(map[string]columnPhysicalVisibleRow, len(visible.Rows))
	for _, row := range visible.Rows {
		got[string(row.ID)] = row
	}
	if len(got) != len(expected) {
		t.Fatalf("visible latest rows=%d want %d", len(got), len(expected))
	}
	for id, want := range expected {
		assertVisibleRowsEquivalentM13C(t, id, got[id], want)
	}
	return columnPhysicalVisibilityInspectionM13C{
		RawRows:     rawRows,
		Visible:     got,
		Diagnostics: visible.Diagnostics,
	}
}

func assertVisibleRowsEquivalentM13C(t *testing.T, id string, got, want columnPhysicalVisibleRow) {
	t.Helper()
	if string(got.ID) != id || string(want.ID) != id {
		t.Fatalf("visible id mismatch key=%q got=%q want=%q", id, got.ID, want.ID)
	}
	if got.Generation != want.Generation || got.PartID != want.PartID ||
		got.AppliedCommandLSN != want.AppliedCommandLSN ||
		got.Operation != want.Operation || got.RowIndex != want.RowIndex ||
		got.Deleted != want.Deleted || len(got.Values) != len(want.Values) {
		t.Fatalf("visible row mismatch for %s\ngot:  %+v\nwant: %+v", id, got, want)
	}
	for i := range got.Values {
		if !columnDeclaredValuesEquivalentM13C(got.Values[i], want.Values[i]) {
			t.Fatalf("visible value[%d] mismatch for %s\ngot:  %+v\nwant: %+v", i, id, got.Values[i], want.Values[i])
		}
	}
}

func assertVisibleRowM13C(t *testing.T, row columnPhysicalVisibleRow, deleted bool, operation ColumnPublishOperation, timeUS int64, kind string) {
	t.Helper()
	if row.Deleted != deleted || row.Operation != operation {
		t.Fatalf("visible row=%+v want deleted=%v operation=%s", row, deleted, operation)
	}
	if deleted {
		return
	}
	if len(row.Values) < 2 {
		t.Fatalf("visible row values=%+v want time_us and kind", row.Values)
	}
	if row.Values[0].Type != ColumnStoreValueInt64 || row.Values[0].Int64 != timeUS {
		t.Fatalf("visible time_us=%+v want %d", row.Values[0], timeUS)
	}
	if columnPhysicalScanStringForTest(row.Values[1]) != kind {
		t.Fatalf("visible kind=%+v want %q", row.Values[1], kind)
	}
}

func columnDeclaredValuesEquivalentM13C(a, b columnDeclaredValue) bool {
	if a.Type != b.Type || a.Present != b.Present || a.Null != b.Null || a.Bool != b.Bool || a.Int64 != b.Int64 || a.Double != b.Double {
		return false
	}
	return columnPhysicalScanStringForTest(a) == columnPhysicalScanStringForTest(b)
}

func TestColumnPhysicalQueryAdapterRejectsUnsupportedShapeM13B(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(4)
	reopened, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()

	tests := []struct {
		name string
		req  ColumnPhysicalQueryRequest
		want string
	}{
		{
			name: "missing group",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount},
			want: "group column",
		},
		{
			name: "undeclared value",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "missing"},
			want: "undeclared column",
		},
		{
			name: "unknown kind",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryKind("unknown")},
			want: "unsupported physical column query kind",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := reopened.RunColumnPhysicalQuery(tc.req)
			if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("RunColumnPhysicalQuery err=%v want %q", err, tc.want)
			}
		})
	}
}

func TestColumnPhysicalQueryValueValidationM13B(t *testing.T) {
	var exec columnPhysicalQueryExecutor
	if got, err := exec.stringKey(columnDeclaredValue{Type: ColumnStoreValueString, StringBytes: []byte("alpha")}); err != nil || got != "alpha" {
		t.Fatalf("stringKey bytes got %q err=%v", got, err)
	}
	if got, err := exec.stringKey(columnDeclaredValue{Type: ColumnStoreValueString, String: "beta"}); err != nil || got != "beta" {
		t.Fatalf("stringKey string got %q err=%v", got, err)
	}
	if _, err := exec.stringKey(columnDeclaredValue{Type: ColumnStoreValueInt64, Int64: 7}); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "expected string") {
		t.Fatalf("wrong-type stringKey err=%v want expected string unsupported", err)
	}
	if _, err := exec.stringKey(columnDeclaredValue{Type: ColumnStoreValueString, Null: true}); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "null string") {
		t.Fatalf("null stringKey err=%v want null string unsupported", err)
	}
	if got, err := columnPhysicalQueryInt64Value(columnDeclaredValue{Type: ColumnStoreValueInt64, Int64: 42}); err != nil || got != 42 {
		t.Fatalf("int64 value got %d err=%v", got, err)
	}
	if _, err := columnPhysicalQueryInt64Value(columnDeclaredValue{Type: ColumnStoreValueString, String: "42"}); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "expected int64") {
		t.Fatalf("wrong-type int64 err=%v want expected int64 unsupported", err)
	}
	if _, err := columnPhysicalQueryInt64Value(columnDeclaredValue{Type: ColumnStoreValueInt64, Null: true}); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "null int64") {
		t.Fatalf("null int64 err=%v want null int64 unsupported", err)
	}
}

func TestColumnPhysicalQueryStringKeyFallbackAllocationsM13B(t *testing.T) {
	var exec columnPhysicalQueryExecutor
	bytesValue := columnDeclaredValue{Type: ColumnStoreValueString, StringBytes: []byte("alpha")}
	stringValue := columnDeclaredValue{Type: ColumnStoreValueString, String: "beta"}
	if _, err := exec.stringKey(bytesValue); err != nil {
		t.Fatalf("warm bytes stringKey: %v", err)
	}
	if _, err := exec.stringKey(stringValue); err != nil {
		t.Fatalf("warm string stringKey: %v", err)
	}

	if allocs := testing.AllocsPerRun(100, func() {
		got, err := exec.stringKey(bytesValue)
		if err != nil || got != "alpha" {
			panic(fmt.Sprintf("bytes stringKey got %q err=%v", got, err))
		}
	}); allocs != 0 {
		t.Fatalf("bytes stringKey allocs/run=%.2f want 0", allocs)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		got, err := exec.stringKey(stringValue)
		if err != nil || got != "beta" {
			panic(fmt.Sprintf("string stringKey got %q err=%v", got, err))
		}
	}); allocs != 0 {
		t.Fatalf("string fallback allocs/run=%.2f want 0", allocs)
	}
}

func TestColumnPhysicalQueryAdapterAllocationSlopeM13B(t *testing.T) {
	smallEvents := columnPhysicalQueryFixtureEventsM13B(128)
	small, closeSmall := openColumnPhysicalQueryFixtureM13B(t, smallEvents)
	defer closeSmall()
	largeEvents := columnPhysicalQueryFixtureEventsM13B(2048)
	large, closeLarge := openColumnPhysicalQueryFixtureM13B(t, largeEvents)
	defer closeLarge()
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}

	smallAllocs := testing.AllocsPerRun(5, func() {
		if _, err := small.RunColumnPhysicalQuery(req); err != nil {
			t.Fatalf("small RunColumnPhysicalQuery: %v", err)
		}
	})
	largeAllocs := testing.AllocsPerRun(5, func() {
		if _, err := large.RunColumnPhysicalQuery(req); err != nil {
			t.Fatalf("large RunColumnPhysicalQuery: %v", err)
		}
	})
	const maxExtraFixtureAllocs = 64 // permits manifest/fixture-scale setup drift while still rejecting row-linear allocation.
	if largeAllocs > smallAllocs+maxExtraFixtureAllocs {
		t.Fatalf("allocation slope looks row-linear: small=%.0f large=%.0f max_extra=%.0f", smallAllocs, largeAllocs, float64(maxExtraFixtureAllocs))
	}
}

func BenchmarkColumnPhysicalQueryAdapterM13B(b *testing.B) {
	for _, rows := range []int{1024, 8192} {
		cases := []struct {
			name string
			req  ColumnPhysicalQueryRequest
		}{
			{name: "q1", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}},
			{name: "q2", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"}},
			{name: "q3", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us"}},
			{name: "q4a", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us"}},
			{name: "q4b", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us"}},
			{name: "q5", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"}},
		}
		for _, tc := range cases {
			b.Run(fmt.Sprintf("%s_rows_%d", tc.name, rows), func(b *testing.B) {
				collection, closeFn := openColumnPhysicalQueryFixtureM13B(b, columnPhysicalQueryFixtureEventsM13B(rows))
				defer closeFn()
				preview, err := collection.RunColumnPhysicalQuery(tc.req)
				if err != nil {
					b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
				}
				b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
				b.ReportAllocs()
				b.ResetTimer()
				var reducedRows int64
				for i := 0; i < b.N; i++ {
					result, err := collection.RunColumnPhysicalQuery(tc.req)
					if err != nil {
						b.Fatalf("RunColumnPhysicalQuery: %v", err)
					}
					if len(result.Groups) == 0 {
						b.Fatal("empty result")
					}
					reducedRows += int64(result.Diagnostics.ReduceRows)
				}
				if reducedRows > 0 {
					elapsed := b.Elapsed()
					b.ReportMetric(float64(reducedRows)/elapsed.Seconds(), "rows/s")
					b.ReportMetric(float64(elapsed.Nanoseconds())/float64(reducedRows), "ns/row")
				}
			})
		}
	}
}

func BenchmarkColumnPhysicalQueryVisibilityM13C(b *testing.B) {
	for _, rows := range []int{1024, 8192} {
		b.Run(fmt.Sprintf("q1_rows_%d", rows), func(b *testing.B) {
			collection, closeFn, liveRows := openColumnPhysicalMutationFixtureM13C(b, rows)
			defer closeFn()
			req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}
			preview, err := collection.RunColumnPhysicalQuery(req)
			if err != nil {
				b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
			}
			if preview.Diagnostics.ReconstructionRows != 0 || preview.Diagnostics.RowMaterializations != 0 {
				b.Fatalf("preview diagnostics reconstructed/materialized rows: %+v", preview.Diagnostics)
			}
			b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
			b.ReportAllocs()
			b.ResetTimer()
			var reducedRows int64
			var visibilityRows int64
			var scanNanos int64
			var reduceNanos int64
			for i := 0; i < b.N; i++ {
				result, err := collection.RunColumnPhysicalQuery(req)
				if err != nil {
					b.Fatalf("RunColumnPhysicalQuery: %v", err)
				}
				if result.Diagnostics.ReduceRows != liveRows {
					b.Fatalf("ReduceRows=%d want live rows %d diagnostics=%+v", result.Diagnostics.ReduceRows, liveRows, result.Diagnostics)
				}
				reducedRows += int64(result.Diagnostics.ReduceRows)
				visibilityRows += int64(result.Diagnostics.VisibilityRows)
				scanNanos += result.Diagnostics.VisibilityNanos
				reduceNanos += result.Diagnostics.ReduceNanos
			}
			if reducedRows > 0 {
				elapsed := b.Elapsed()
				b.ReportMetric(float64(reducedRows)/elapsed.Seconds(), "rows/s")
				b.ReportMetric(float64(elapsed.Nanoseconds())/float64(reducedRows), "ns/row")
				b.ReportMetric(float64(visibilityRows)/float64(b.N), "visibility_rows/op")
				b.ReportMetric(float64(scanNanos)/float64(b.N), "visibility_ns/op")
				b.ReportMetric(float64(reduceNanos)/float64(b.N), "reduce_ns/op")
			}
		})
	}
}

func BenchmarkColumnStoreGetReconstructionM13C(b *testing.B) {
	for _, rows := range []int{1024, 8192} {
		b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
			collection, closeFn, _ := openColumnPhysicalMutationFixtureM13C(b, rows)
			defer closeFn()
			id := []byte("e000001")
			if got, err := collection.Get(id); err != nil || got == nil {
				b.Fatalf("preview Get got=%s err=%v", got, err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			var reconstructedRows int64
			for i := 0; i < b.N; i++ {
				got, err := collection.Get(id)
				if err != nil {
					b.Fatalf("Get: %v", err)
				}
				if len(got) == 0 {
					b.Fatal("empty reconstructed document")
				}
				reconstructedRows++
			}
			elapsed := b.Elapsed()
			b.ReportMetric(float64(reconstructedRows)/elapsed.Seconds(), "rows/s")
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(reconstructedRows), "ns/row")
			b.ReportMetric(float64(reconstructedRows)/float64(b.N), "reconstruction_rows/op")
		})
	}
}

type columnPhysicalQueryEventM13B struct {
	ID     string
	TimeUS int64
	Kind   string
	Did    string
}

func columnPhysicalQueryFixtureEventsM13B(rows int) []columnPhysicalQueryEventM13B {
	out := make([]columnPhysicalQueryEventM13B, rows)
	const baseTimeUS = int64(1_700_000_000_000_000)
	for i := 0; i < rows; i++ {
		out[i] = columnPhysicalQueryEventM13B{
			ID:     fmt.Sprintf("e%06d", i),
			TimeUS: baseTimeUS + int64(i%24)*3_600_000_000 + int64(i/24),
			Kind:   fmt.Sprintf("kind_%02d", i%4),
			Did:    fmt.Sprintf("did_%02d", i%12),
		}
	}
	return out
}

func openColumnPhysicalQueryFixtureM13B(tb testing.TB, events []columnPhysicalQueryEventM13B) (*Collection, func()) {
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
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		tb.Fatalf("Checkpoint setup DB: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection setup: %v", err)
	}
	ids := make([][]byte, len(events))
	docs := make([][]byte, len(events))
	for i, event := range events {
		ids[i] = []byte(event.ID)
		docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":"%s","did":"%s","payload":"ignored_%d"}`, event.TimeUS, event.Kind, event.Did, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		_ = d.Close()
		tb.Fatalf("InsertBatch: %v", err)
	}
	if err := d.Close(); err != nil {
		tb.Fatalf("Close before reopen: %v", err)
	}

	reopen, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatalf("Open reopened DB: %v", err)
	}
	reopened, err := NewCollectionManager(reopen).OpenCollection("events")
	if err != nil {
		_ = reopen.Close()
		tb.Fatalf("OpenCollection reopened: %v", err)
	}
	return reopened, func() { _ = reopen.Close() }
}

func openColumnPhysicalMutationFixtureM13C(tb testing.TB, rows int) (*Collection, func(), int) {
	tb.Helper()
	if rows < 2 {
		tb.Fatalf("rows=%d want at least two rows", rows)
	}
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatalf("Open mutation fixture DB: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection mutation fixture: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		tb.Fatalf("Checkpoint mutation fixture setup: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection mutation fixture: %v", err)
	}
	ids := make([][]byte, rows)
	docs := make([][]byte, rows)
	for i := 0; i < rows; i++ {
		ids[i] = []byte(fmt.Sprintf("e%06d", i))
		docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":"kind_%02d","did":"did_%02d","payload":"seed_%d"}`,
			1_700_000_000_000_000+int64(i), i%8, i%64, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		_ = d.Close()
		tb.Fatalf("InsertBatch mutation fixture: %v", err)
	}
	liveRows := rows
	for i := 0; i < rows; i += 4 {
		id := []byte(fmt.Sprintf("e%06d", i))
		doc := []byte(fmt.Sprintf(`{"time_us":%d,"kind":"kind_updated","did":"did_%02d","payload":"updated_%d"}`,
			1_700_001_000_000_000+int64(i), i%64, i))
		if _, modified, err := col.Update(id, func([]byte) ([]byte, bool, error) {
			return doc, true, nil
		}); err != nil || !modified {
			_ = d.Close()
			tb.Fatalf("Update mutation fixture id=%s modified=%t err=%v", id, modified, err)
		}
	}
	for i := 0; i < rows; i += 10 {
		id := []byte(fmt.Sprintf("e%06d", i))
		deleted, err := col.DeleteBatch([][]byte{id})
		if err != nil || deleted != 1 {
			_ = d.Close()
			tb.Fatalf("DeleteBatch mutation fixture id=%s deleted=%d err=%v", id, deleted, err)
		}
		liveRows--
	}
	if err := d.Close(); err != nil {
		tb.Fatalf("Close mutation fixture: %v", err)
	}
	reopen, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatalf("Open mutation fixture reopened DB: %v", err)
	}
	reopened, err := NewCollectionManager(reopen).OpenCollection("events")
	if err != nil {
		_ = reopen.Close()
		tb.Fatalf("OpenCollection mutation fixture reopened: %v", err)
	}
	return reopened, func() { _ = reopen.Close() }, liveRows
}

func columnPhysicalQueryReferenceHashM13B(name string, events []columnPhysicalQueryEventM13B) uint64 {
	return columnPhysicalQueryHashLinesM13B(columnPhysicalQueryReferenceLinesM13B(name, events))
}

func columnPhysicalQueryReferenceLinesM13B(name string, events []columnPhysicalQueryEventM13B) []string {
	switch name {
	case "q1":
		counts := make(map[string]int)
		for _, event := range events {
			counts[event.Kind]++
		}
		return columnPhysicalQueryIntLinesM13B(name, counts)
	case "q2":
		distinct := make(map[string]map[string]struct{})
		for _, event := range events {
			set := distinct[event.Kind]
			if set == nil {
				set = make(map[string]struct{})
				distinct[event.Kind] = set
			}
			set[event.Did] = struct{}{}
		}
		counts := make(map[string]int)
		for key, set := range distinct {
			counts[key] = len(set)
		}
		return columnPhysicalQueryIntLinesM13B(name, counts)
	case "q3":
		counts := make(map[string]int)
		for _, event := range events {
			counts[columnPhysicalQueryHourKey(columnPhysicalQueryUTCHour(event.TimeUS))]++
		}
		return columnPhysicalQueryIntLinesM13B(name, counts)
	case "q4a":
		values := make(map[string]int64)
		for _, event := range events {
			if cur, ok := values[event.Did]; !ok || event.TimeUS < cur {
				values[event.Did] = event.TimeUS
			}
		}
		return columnPhysicalQueryInt64LinesM13B(name, values)
	case "q4b":
		values := make(map[string]int64)
		for _, event := range events {
			if cur, ok := values[event.Did]; !ok || event.TimeUS > cur {
				values[event.Did] = event.TimeUS
			}
		}
		return columnPhysicalQueryInt64LinesM13B(name, values)
	case "q5":
		type span struct {
			min int64
			max int64
		}
		values := make(map[string]span)
		for _, event := range events {
			cur, ok := values[event.Did]
			if !ok {
				values[event.Did] = span{min: event.TimeUS, max: event.TimeUS}
				continue
			}
			if event.TimeUS < cur.min {
				cur.min = event.TimeUS
			}
			if event.TimeUS > cur.max {
				cur.max = event.TimeUS
			}
			values[event.Did] = cur
		}
		lines := make([]string, 0, len(values))
		for key, value := range values {
			lines = append(lines, fmt.Sprintf("%s:%s=%d", name, key, value.max-value.min))
		}
		return lines
	default:
		panic(name)
	}
}

func columnPhysicalQueryLinesM13B(name string, groups []ColumnPhysicalQueryGroup) []string {
	lines := make([]string, 0, len(groups))
	for _, group := range groups {
		switch name {
		case "q1", "q2", "q3":
			lines = append(lines, fmt.Sprintf("%s:%s=%d", name, group.Key, group.Count))
		case "q4a", "q4b", "q5", "q5_metadata":
			hashName := name
			if name == "q5_metadata" {
				hashName = "q5"
			}
			lines = append(lines, fmt.Sprintf("%s:%s=%d", hashName, group.Key, group.Int64))
		default:
			panic(name)
		}
	}
	return lines
}

func columnPhysicalQueryIntLinesM13B(prefix string, values map[string]int) []string {
	lines := make([]string, 0, len(values))
	for key, value := range values {
		lines = append(lines, fmt.Sprintf("%s:%s=%d", prefix, key, value))
	}
	return lines
}

func columnPhysicalQueryInt64LinesM13B(prefix string, values map[string]int64) []string {
	lines := make([]string, 0, len(values))
	for key, value := range values {
		lines = append(lines, fmt.Sprintf("%s:%s=%d", prefix, key, value))
	}
	return lines
}

func columnPhysicalQueryHashLinesM13B(lines []string) uint64 {
	sort.Strings(lines)
	h := fnv.New64a()
	for _, line := range lines {
		_, _ = h.Write([]byte(line))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}
