package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestColumnPhysicalQuerySumSecondOfDaySquareOverflowCheckedM3116(t *testing.T) {
	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "compatibility_row_scan",
			run: func() error {
				exec := &columnPhysicalQueryExecutor{
					kind:     ColumnPhysicalQuerySumSecondOfDaySquare,
					valueIdx: 0,
					int64Sum: typedColumnInt64PredicateAggregateMaxSum,
				}
				return exec.visitValues([]columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: 1_000_000}})
			},
		},
		{
			name: "direct_asset_scan",
			run: func() error {
				exec := &columnPhysicalQueryExecutor{
					kind:     ColumnPhysicalQuerySumSecondOfDaySquare,
					int64Sum: typedColumnInt64PredicateAggregateMaxSum,
				}
				return exec.visitDirectSumSecondOfDaySquare(1_000_000, true)
			},
		},
		{
			name: "parallel_merge",
			run: func() error {
				left := &columnPhysicalQueryExecutor{
					kind:         ColumnPhysicalQuerySumSecondOfDaySquare,
					int64Sum:     typedColumnInt64PredicateAggregateMaxSum,
					int64SumRows: 1,
				}
				right := &columnPhysicalQueryExecutor{
					kind:         ColumnPhysicalQuerySumSecondOfDaySquare,
					int64Sum:     1,
					int64SumRows: 1,
				}
				return left.mergeFrom(right)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			if err == nil || !strings.Contains(err.Error(), "sum overflow") {
				t.Fatalf("err=%v want sum overflow", err)
			}
		})
	}
}

func TestColumnPhysicalQuerySumSecondOfDaySquareResultKeyUsesValueColumnM3116(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "score_us", Path: "score_us", ValueType: ColumnStoreValueInt64},
		},
	}
	exec, err := newColumnPhysicalQueryExecutor(cfg, ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQuerySumSecondOfDaySquare,
		ValueColumn: "score_us",
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalQueryExecutor: %v", err)
	}
	if err := exec.addSumSecondOfDaySquareValue(1_000_000); err != nil {
		t.Fatalf("addSumSecondOfDaySquareValue: %v", err)
	}
	groups := exec.groups()
	if len(groups) != 1 {
		t.Fatalf("groups=%+v want one result", groups)
	}
	if groups[0].Key != "score_us_second_of_day_square" {
		t.Fatalf("result key=%q want %q", groups[0].Key, "score_us_second_of_day_square")
	}
}

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
			req:        ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "max_time_us"},
			wantCount:  12,
			wantDirect: false,
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
			wantMetadata := tc.name == "q4b" || tc.name == "q5_metadata"
			if !wantMetadata && result.Diagnostics.DecodedBlocks == 0 {
				t.Fatalf("%s decoded blocks=0 diagnostics=%+v", tc.name, result.Diagnostics)
			}
			if tc.wantDirect && result.Diagnostics.DirectReduceBlocks != result.Diagnostics.DecodedBlocks {
				t.Fatalf("%s direct reduce blocks=%d want decoded blocks=%d diagnostics=%+v", tc.name, result.Diagnostics.DirectReduceBlocks, result.Diagnostics.DecodedBlocks, result.Diagnostics)
			}
			if !tc.wantDirect && result.Diagnostics.DirectReduceBlocks != 0 {
				t.Fatalf("%s direct reduce blocks=%d want zero for non-vectorized shape diagnostics=%+v", tc.name, result.Diagnostics.DirectReduceBlocks, result.Diagnostics)
			}
			if wantMetadata {
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

func TestSortColumnPhysicalQueryGroupsByKeySmallM1634(t *testing.T) {
	tests := []struct {
		name string
		in   []ColumnPhysicalQueryGroup
		want []string
	}{
		{
			name: "reverse small",
			in: []ColumnPhysicalQueryGroup{
				{Key: "z", Count: 1},
				{Key: "m", Count: 2},
				{Key: "a", Count: 3},
			},
			want: []string{"a", "m", "z"},
		},
		{
			name: "already sorted small",
			in: []ColumnPhysicalQueryGroup{
				{Key: "a", Count: 1},
				{Key: "m", Count: 2},
				{Key: "z", Count: 3},
			},
			want: []string{"a", "m", "z"},
		},
		{
			name: "duplicates small",
			in: []ColumnPhysicalQueryGroup{
				{Key: "b", Count: 1},
				{Key: "a", Count: 2},
				{Key: "b", Count: 3},
			},
			want: []string{"a", "b", "b"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sortColumnPhysicalQueryGroupsByKey(tc.in)
			got := make([]string, len(tc.in))
			for i, group := range tc.in {
				got[i] = group.Key
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("sorted keys=%v want %v", got, tc.want)
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
			if tc.req.Kind == ColumnPhysicalQueryGroupCount ||
				tc.req.Kind == ColumnPhysicalQueryGroupCountDistinct ||
				tc.req.Kind == ColumnPhysicalQueryHourCount ||
				tc.req.Kind == ColumnPhysicalQueryGroupMinInt64 ||
				tc.req.Kind == ColumnPhysicalQueryGroupMaxInt64 ||
				tc.req.Kind == ColumnPhysicalQueryGroupInt64Span {
				if serial.Diagnostics.PhysicalBytesScanned <= 0 || serial.Diagnostics.PhysicalBytesScanned >= parallel.Diagnostics.PhysicalBytesScanned {
					t.Fatalf("serial sidecar bytes=%d want below parallel TCPA bytes=%d serial=%+v parallel=%+v", serial.Diagnostics.PhysicalBytesScanned, parallel.Diagnostics.PhysicalBytesScanned, serial.Diagnostics, parallel.Diagnostics)
				}
			} else if parallel.Diagnostics.PhysicalBytesScanned != serial.Diagnostics.PhysicalBytesScanned {
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

func TestColumnPhysicalQueryParallelWorkerCountCapsFanoutM14B(t *testing.T) {
	if got, want := columnPhysicalQueryParallelWorkerCount(1024, 8192), columnPhysicalQueryMaxParallelWorkers; got != want {
		t.Fatalf("worker count=%d want cap %d", got, want)
	}
	if got, want := columnPhysicalQueryParallelWorkerCount(1024, 8), 8; got != want {
		t.Fatalf("worker count=%d want asset refs %d", got, want)
	}
	if got, want := columnPhysicalQueryParallelWorkerCount(4, 8192), 4; got != want {
		t.Fatalf("worker count=%d want requested workers %d", got, want)
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
	left := ColumnPhysicalQueryDiagnostics{
		MutationParts: 2, DecodedBlocks: 1, MetadataMisses: 1, FallbackReads: 2,
		RowMaterializations: 3, DocumentMaterializations: 4, DecodedMetadataBytes: 5,
		MappedBytes: 6, HeapCopyBytes: 7, SegmentFileCacheHits: 2, SegmentFileCacheMisses: 3,
	}
	right := ColumnPhysicalQueryDiagnostics{
		MutationParts: 2, DecodedBlocks: 3, MetadataMisses: 10, FallbackReads: 20,
		RowMaterializations: 30, DocumentMaterializations: 40, DecodedMetadataBytes: 50,
		MappedBytes: 60, HeapCopyBytes: 70, SegmentFileCacheHits: 5, SegmentFileCacheMisses: 7,
	}
	merged := mergeColumnPhysicalQueryDiagnostics(left, right)
	if got, want := merged.MutationParts, 2; got != want {
		t.Fatalf("mutation parts=%d want view-level max %d", got, want)
	}
	if got, want := merged.DecodedBlocks, 4; got != want {
		t.Fatalf("decoded blocks=%d want summed work %d", got, want)
	}
	if got, want := merged.MetadataMisses, 11; got != want {
		t.Fatalf("metadata misses=%d want summed %d", got, want)
	}
	if got, want := merged.FallbackReads, 22; got != want {
		t.Fatalf("fallback reads=%d want summed %d", got, want)
	}
	if got, want := merged.RowMaterializations, 33; got != want {
		t.Fatalf("row materializations=%d want summed %d", got, want)
	}
	if got, want := merged.DocumentMaterializations, 44; got != want {
		t.Fatalf("document materializations=%d want summed %d", got, want)
	}
	if got, want := merged.DecodedMetadataBytes, uint64(55); got != want {
		t.Fatalf("decoded metadata bytes=%d want summed %d", got, want)
	}
	if got, want := merged.MappedBytes, uint64(66); got != want {
		t.Fatalf("mapped bytes=%d want summed %d", got, want)
	}
	if got, want := merged.HeapCopyBytes, uint64(77); got != want {
		t.Fatalf("heap copy bytes=%d want summed %d", got, want)
	}
	if got, want := merged.SegmentFileCacheHits, uint64(7); got != want {
		t.Fatalf("cache hits=%d want summed %d", got, want)
	}
	if got, want := merged.SegmentFileCacheMisses, uint64(10); got != want {
		t.Fatalf("cache misses=%d want summed %d", got, want)
	}
}

func TestMergeColumnPhysicalQueryDiagnosticsAddsQ2PostPrepareSplits3324(t *testing.T) {
	left := ColumnPhysicalQueryDiagnostics{
		TypedColumnPreparePostPrepareNanos:                    100,
		TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos:    10,
		TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos: 20,
		TypedColumnPrepareQ2GroupGlobalCodeRemapNanos:         30,
		TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos:      40,
	}
	right := ColumnPhysicalQueryDiagnostics{
		TypedColumnPreparePostPrepareNanos:                    1000,
		TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos:    1,
		TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos: 2,
		TypedColumnPrepareQ2GroupGlobalCodeRemapNanos:         3,
		TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos:      4,
	}

	merged := mergeColumnPhysicalQueryDiagnostics(left, right)
	if got, want := merged.TypedColumnPreparePostPrepareNanos, int64(1100); got != want {
		t.Fatalf("post prepare nanos=%d want %d", got, want)
	}
	if got, want := merged.TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos, int64(11); got != want {
		t.Fatalf("q2 group global dictionary/rank nanos=%d want %d", got, want)
	}
	if got, want := merged.TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos, int64(22); got != want {
		t.Fatalf("q2 distinct global dictionary/rank nanos=%d want %d", got, want)
	}
	if got, want := merged.TypedColumnPrepareQ2GroupGlobalCodeRemapNanos, int64(33); got != want {
		t.Fatalf("q2 group global-code remap nanos=%d want %d", got, want)
	}
	if got, want := merged.TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos, int64(44); got != want {
		t.Fatalf("q2 distinct global-code remap nanos=%d want %d", got, want)
	}
}

func TestMergeColumnPhysicalQueryDiagnosticsSortKeyNoneSentinel1949(t *testing.T) {
	tests := []struct {
		name                        string
		left                        ColumnPhysicalQueryDiagnostics
		right                       ColumnPhysicalQueryDiagnostics
		wantSortKeyFallback         string
		wantGroupedDistinctFallback string
	}{
		{
			name: "left none right actual",
			left: ColumnPhysicalQueryDiagnostics{
				SortKeyMarkFallbackReason:           columnSortKeyMarkFallbackNone,
				SortedGroupedDistinctFallbackReason: columnSortedGroupedDistinctFallbackNone,
			},
			right: ColumnPhysicalQueryDiagnostics{
				SortKeyMarkFallbackReason:           columnSortKeyMarkFallbackMissingMarks,
				SortedGroupedDistinctFallbackReason: columnSortedGroupedDistinctFallbackMissingPrefix,
			},
			wantSortKeyFallback:         columnSortKeyMarkFallbackMissingMarks,
			wantGroupedDistinctFallback: columnSortedGroupedDistinctFallbackMissingPrefix,
		},
		{
			name: "left actual right none",
			left: ColumnPhysicalQueryDiagnostics{
				SortKeyMarkFallbackReason:           columnSortKeyMarkFallbackMissingMarks,
				SortedGroupedDistinctFallbackReason: columnSortedGroupedDistinctFallbackMissingPrefix,
			},
			right: ColumnPhysicalQueryDiagnostics{
				SortKeyMarkFallbackReason:           columnSortKeyMarkFallbackNone,
				SortedGroupedDistinctFallbackReason: columnSortedGroupedDistinctFallbackNone,
			},
			wantSortKeyFallback:         columnSortKeyMarkFallbackMissingMarks,
			wantGroupedDistinctFallback: columnSortedGroupedDistinctFallbackMissingPrefix,
		},
		{
			name: "both none",
			left: ColumnPhysicalQueryDiagnostics{
				SortKeyMarkFallbackReason:           columnSortKeyMarkFallbackNone,
				SortedGroupedDistinctFallbackReason: columnSortedGroupedDistinctFallbackNone,
			},
			right: ColumnPhysicalQueryDiagnostics{
				SortKeyMarkFallbackReason:           columnSortKeyMarkFallbackNone,
				SortedGroupedDistinctFallbackReason: columnSortedGroupedDistinctFallbackNone,
			},
			wantSortKeyFallback:         columnSortKeyMarkFallbackNone,
			wantGroupedDistinctFallback: columnSortedGroupedDistinctFallbackNone,
		},
		{
			name: "conflicting actual reasons become mixed",
			left: ColumnPhysicalQueryDiagnostics{
				SortKeyMarkFallbackReason:           columnSortKeyMarkFallbackMissingMarks,
				SortedGroupedDistinctFallbackReason: columnSortedGroupedDistinctFallbackMissingPrefix,
			},
			right: ColumnPhysicalQueryDiagnostics{
				SortKeyMarkFallbackReason:           columnSortKeyMarkFallbackStaleMarks,
				SortedGroupedDistinctFallbackReason: columnSortedGroupedDistinctFallbackSortKeyLayout,
			},
			wantSortKeyFallback:         "mixed",
			wantGroupedDistinctFallback: "mixed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			merged := mergeColumnPhysicalQueryDiagnostics(tc.left, tc.right)
			if got := merged.SortKeyMarkFallbackReason; got != tc.wantSortKeyFallback {
				t.Fatalf("sort-key mark fallback=%q want %q", got, tc.wantSortKeyFallback)
			}
			if got := merged.SortedGroupedDistinctFallbackReason; got != tc.wantGroupedDistinctFallback {
				t.Fatalf("sorted grouped distinct fallback=%q want %q", got, tc.wantGroupedDistinctFallback)
			}
		})
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
	retainedJSON := readRawRetainedPayloadJSONForTestM13C(t, reopen, "events", raw)
	if strings.Contains(string(retainedJSON), "time_us") || strings.Contains(string(retainedJSON), "kind") || strings.Contains(string(retainedJSON), "did") {
		t.Fatalf("raw retained payload still duplicates declared fields: %s", retainedJSON)
	}
	if !strings.Contains(string(retainedJSON), "payload") {
		t.Fatalf("raw retained payload lost non-column field: %s", retainedJSON)
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
	read, templateRuns, blocked, stale, needIndex, err := snapshotUpdateBatchBufferedReadLocked(nil, domain, columnMeta, 1, baseSystemRoot, items, DocumentFormatJSON, false)
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

func TestColumnStoreReconstructionPreservesRetainedJSONNumberLiteralM13C(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: ColumnRetainedPayloadNonColumn,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
		Columns: []ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString},
			{Name: "did", Path: "did", ValueType: ColumnStoreValueString},
		},
	}
	doc := []byte(`{"time_us":9223372036854775807,"kind":"like","did":"d1","payload_id":9223372036854775806}`)
	retained, err := columnRetainedPayloadFromJSONDocument(cfg, doc)
	if err != nil {
		t.Fatalf("columnRetainedPayloadFromJSONDocument: %v", err)
	}
	rows, err := extractColumnDeclaredRowsFromJSONDocuments(cfg, []columnWriteDocument{{
		ID:       []byte("e1"),
		Document: doc,
	}})
	if err != nil {
		t.Fatalf("extractColumnDeclaredRowsFromJSONDocuments: %v", err)
	}
	got, err := reconstructColumnJSONDocument(cfg, retained, rows[0].Values)
	if err != nil {
		t.Fatalf("reconstructColumnJSONDocument: %v", err)
	}
	if strings.Contains(string(got), "9.223") || !strings.Contains(string(got), `"payload_id":9223372036854775806`) {
		t.Fatalf("reconstructed JSON number fidelity lost: %s", got)
	}
}

func TestColumnStoreReconstructionFailsClosedOnScalarAncestorM13C(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled:                 true,
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
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
	retainedJSON := readRawRetainedPayloadJSONForTestM13C(t, reopen, "events", raw)
	if strings.Contains(string(retainedJSON), "time_us") || strings.Contains(string(retainedJSON), "kind") || strings.Contains(string(retainedJSON), "did") {
		t.Fatalf("raw retained payload still duplicates declared fields: %s", retainedJSON)
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
		retainedJSON := readRawRetainedPayloadJSONForTestM13C(t, reopen, "events", raw)
		if strings.Contains(string(retainedJSON), "time_us") || strings.Contains(string(retainedJSON), "kind") || strings.Contains(string(retainedJSON), "did") {
			t.Fatalf("raw retained payload for %s duplicates declared fields: %s", id, retainedJSON)
		}
		if !strings.Contains(string(retainedJSON), "payload") {
			t.Fatalf("raw retained payload for %s lost retained field: %s", id, retainedJSON)
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
	retainedJSON := readRawRetainedPayloadJSONForTestM13C(t, reopen, "events", raw)
	if strings.Contains(string(retainedJSON), "time_us") || strings.Contains(string(retainedJSON), "kind") || strings.Contains(string(retainedJSON), "did") || !strings.Contains(string(retainedJSON), "payload") {
		t.Fatalf("replayed raw retained payload=%s, want retained-only payload", retainedJSON)
	}
	result, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery replayed: %v", err)
	}
	if got, want := columnPhysicalQueryLinesM13B("q1", result.Groups), []string{"q1:share=1"}; !equalStringSets(got, want) {
		t.Fatalf("replayed q1 lines=%v want %v diagnostics=%+v", got, want, result.Diagnostics)
	}
}

func TestColumnStoreQueryAndReconstructionUseSelectedClosureWhenOtherAssetMissingM13C(t *testing.T) {
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
	result, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"})
	if err != nil || len(result.Groups) != 1 || result.Groups[0].Key != "like" || result.Groups[0].Count != 1 {
		t.Fatalf("RunColumnPhysicalQuery after missing non-selected asset result=%+v err=%v want selected valid closure", result, err)
	}
	if got, err := reopened.Get([]byte("e1")); err != nil {
		t.Fatalf("Get after missing non-selected asset: %v", err)
	} else {
		assertJSONEqualM13C(t, got, []byte(`{"time_us":1,"kind":"like","did":"d1"}`))
	}
}

func TestColumnStoreQueryAndReconstructionUseSelectedClosureWhenOtherAssetCorruptM13C(t *testing.T) {
	dir, ref := prepareColumnPhysicalScannerCorruptionFixtureM13A(t)
	sidecarRef := columnPhysicalQueryInt64SidecarRefForCorruption(t, dir)
	assetPath, err := columnAssetSegmentPath(backenddb.ColumnAssetRootDirPath(dir), ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	sidecarPath, err := columnAssetSegmentPath(backenddb.ColumnAssetRootDirPath(dir), sidecarRef)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath sidecar: %v", err)
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
	sidecarFile, err := os.OpenFile(sidecarPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile sidecar asset: %v", err)
	}
	if _, err := sidecarFile.WriteAt([]byte{0xff}, sidecarRef.Offset); err != nil {
		_ = sidecarFile.Close()
		t.Fatalf("corrupt sidecar asset: %v", err)
	}
	if err := sidecarFile.Close(); err != nil {
		t.Fatalf("Close corrupt sidecar asset: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	result, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us"})
	if err != nil || len(result.Groups) != 1 || result.Groups[0].Key != "d1" || result.Groups[0].Int64 != 1 {
		t.Fatalf("RunColumnPhysicalQuery after corrupt non-selected asset result=%+v err=%v want selected valid closure", result, err)
	}
	if got, err := reopened.Get([]byte("e1")); err != nil {
		t.Fatalf("Get after corrupt non-selected asset: %v", err)
	} else {
		assertJSONEqualM13C(t, got, []byte(`{"time_us":1,"kind":"like","did":"d1"}`))
	}
}

func TestColumnStoreQueryAndReconstructionUseSelectedClosureWhenOtherAssetTruncatedM13C(t *testing.T) {
	dir, ref := prepareColumnPhysicalScannerCorruptionFixtureM13A(t)
	sidecarRef := columnPhysicalQueryInt64SidecarRefForCorruption(t, dir)
	assetPath, err := columnAssetSegmentPath(backenddb.ColumnAssetRootDirPath(dir), ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	sidecarPath, err := columnAssetSegmentPath(backenddb.ColumnAssetRootDirPath(dir), sidecarRef)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath sidecar: %v", err)
	}
	if err := os.Truncate(assetPath, ref.Offset+ref.Length-1); err != nil {
		t.Fatalf("Truncate asset: %v", err)
	}
	if err := os.Truncate(sidecarPath, sidecarRef.Offset+sidecarRef.Length-1); err != nil {
		t.Fatalf("Truncate sidecar asset: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	result, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us"})
	if err != nil || len(result.Groups) != 1 || result.Groups[0].Key != "d1" || result.Groups[0].Int64 != 1 {
		t.Fatalf("RunColumnPhysicalQuery after truncated non-selected asset result=%+v err=%v want selected valid closure", result, err)
	}
	if got, err := reopened.Get([]byte("e1")); err != nil {
		t.Fatalf("Get after truncated non-selected asset: %v", err)
	} else {
		assertJSONEqualM13C(t, got, []byte(`{"time_us":1,"kind":"like","did":"d1"}`))
	}
}

func columnPhysicalQueryInt64SidecarRefForCorruption(t *testing.T, dir string) ColumnAssetRef {
	t.Helper()
	db := openCollectionCommandWALDB(t, dir)
	defer func() { _ = db.Close() }()
	collection := openColumnStoreCollectionM10B(t, db)
	view, closeView, err := collection.prepareColumnPhysicalScanSnapshotView()
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	for _, ref := range view.Int64Values {
		if ref.ColumnName == "time_us" {
			return ref.AssetRef
		}
	}
	t.Fatalf("missing time_us int64 sidecar refs=%+v", view.Int64Values)
	return ColumnAssetRef{}
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

func TestColumnPhysicalDirectQueryValidatesUnselectedTypeTags(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}

	var raw bytes.Buffer
	writeManifestUint32(&raw, columnPhysicalAssetMagic)
	writeManifestUint16(&raw, columnPhysicalAssetVersion)
	writeManifestString(&raw, "events")
	writeManifestString(&raw, normalized.AssetManager.Namespace)
	writeManifestUint64(&raw, 1)
	writeManifestUint64(&raw, 1)
	writeManifestUint64(&raw, 1)
	writeManifestString(&raw, string(ColumnPublishOperationInsert))
	writeManifestUint64(&raw, normalized.SchemaHash)
	writeManifestUint64(&raw, uint64(len(normalized.Columns)))
	writeManifestUint64(&raw, 1)
	for _, col := range normalized.Columns {
		writeManifestString(&raw, col.Name)
		writeManifestString(&raw, col.Path)
		writeManifestString(&raw, string(col.ValueType))
		writeManifestBool(&raw, col.Nullable)
		writeManifestBool(&raw, col.Dictionary)
		writeManifestUint64(&raw, uint64(col.VectorDims))
	}
	writeManifestBytes(&raw, []byte("e1"))
	writeManifestBool(&raw, false)
	writeManifestString(&raw, string(ColumnStoreValueString))
	writeManifestBool(&raw, false)
	writeManifestBool(&raw, true)
	writeManifestUint64(&raw, 7)
	writeManifestString(&raw, string(ColumnStoreValueString))
	writeManifestBool(&raw, false)
	writeManifestBool(&raw, true)
	writeManifestString(&raw, "share")
	writeManifestString(&raw, string(ColumnStoreValueString))
	writeManifestBool(&raw, false)
	writeManifestBool(&raw, true)
	writeManifestString(&raw, "did:1")
	encoded := raw.Bytes()
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  normalized.AssetManager.Namespace,
		Generation: 1,
		PartID:     1,
		FileID:     columnAssetM12ASegmentFileID,
		Length:     int64(len(encoded)),
		Checksum:   page.Checksum(encoded),
	}

	verifyExec, err := newColumnPhysicalQueryExecutor(*normalized, ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"})
	if err != nil {
		t.Fatalf("newColumnPhysicalQueryExecutor verify: %v", err)
	}
	if _, err := reduceColumnPhysicalAssetDirect(encoded, ref, "events", normalized, ColumnPublishOperationInsert, verifyExec); err == nil || !strings.Contains(err.Error(), `column[0] type="string" want "int64"`) {
		t.Fatalf("reduce verify err=%v want unselected type-tag failure", err)
	}

	relaxedExec, err := newColumnPhysicalQueryExecutor(*normalized, ColumnPhysicalQueryRequest{
		Kind:                     ColumnPhysicalQueryGroupCount,
		GroupColumn:              "kind",
		ColumnAssetReadIntegrity: ColumnAssetReadIntegritySkipChecksums,
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalQueryExecutor relaxed: %v", err)
	}
	summary, err := reduceColumnPhysicalAssetDirect(encoded, ref, "events", normalized, ColumnPublishOperationInsert, relaxedExec)
	if err != nil {
		t.Fatalf("reduce relaxed: %v", err)
	}
	if summary.rows != 1 || relaxedExec.reduceRows != 1 {
		t.Fatalf("reduce relaxed summary rows=%d reduceRows=%d want 1", summary.rows, relaxedExec.reduceRows)
	}
	if got, want := columnPhysicalQueryLinesM13B("q1", relaxedExec.groups()), []string{"q1:share=1"}; !equalStringSets(got, want) {
		t.Fatalf("reduce relaxed groups=%v want %v", got, want)
	}
}

func TestColumnPhysicalDirectQuerySkipsUnselectedPrimitiveRowAssets1929(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = append(cfg.Columns,
		ColumnStoreColumn{Name: "i8", Path: "i8", ValueType: ColumnStoreValueInt8},
		ColumnStoreColumn{Name: "u8", Path: "u8", ValueType: ColumnStoreValueUint8},
		ColumnStoreColumn{Name: "i16", Path: "i16", ValueType: ColumnStoreValueInt16},
		ColumnStoreColumn{Name: "u16", Path: "u16", ValueType: ColumnStoreValueUint16},
		ColumnStoreColumn{Name: "i32", Path: "i32", ValueType: ColumnStoreValueInt32},
		ColumnStoreColumn{Name: "u32", Path: "u32", ValueType: ColumnStoreValueUint32},
		ColumnStoreColumn{Name: "u64", Path: "u64", ValueType: ColumnStoreValueUint64},
		ColumnStoreColumn{Name: "f16", Path: "f16", ValueType: ColumnStoreValueFloat16},
		ColumnStoreColumn{Name: "bf16", Path: "bf16", ValueType: ColumnStoreValueBFloat16},
	)
	cfg.SortKey = nil
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows := []columnDeclaredRow{{ID: []byte("e1"), Values: []columnDeclaredValue{
		{Type: ColumnStoreValueInt64, Present: true, Int64: 7},
		{Type: ColumnStoreValueString, Present: true, String: "share"},
		{Type: ColumnStoreValueString, Present: true, String: "did:1"},
		{Type: ColumnStoreValueInt8, Present: true, Int8: -8},
		{Type: ColumnStoreValueUint8, Present: true, Uint8: 8},
		{Type: ColumnStoreValueInt16, Present: true, Int16: -16},
		{Type: ColumnStoreValueUint16, Present: true, Uint16: 16},
		{Type: ColumnStoreValueInt32, Present: true, Int32: -32},
		{Type: ColumnStoreValueUint32, Present: true, Uint32: 32},
		{Type: ColumnStoreValueUint64, Present: true, Uint64: 64},
		{Type: ColumnStoreValueFloat16, Present: true, Float16: 0x3c00},
		{Type: ColumnStoreValueBFloat16, Present: true, BFloat16: 0x3f80},
	}}}
	raw, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
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
	ref := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: normalized.AssetManager.Namespace, Generation: 1, PartID: 1, FileID: columnAssetM12ASegmentFileID, Length: int64(len(raw)), Checksum: page.Checksum(raw)}
	exec, err := newColumnPhysicalQueryExecutor(*normalized, ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"})
	if err != nil {
		t.Fatalf("newColumnPhysicalQueryExecutor: %v", err)
	}
	summary, err := reduceColumnPhysicalAssetDirect(raw, ref, "events", normalized, ColumnPublishOperationInsert, exec)
	if err != nil {
		t.Fatalf("reduce direct with unselected primitive row assets: %v", err)
	}
	if summary.rows != 1 || exec.reduceRows != 1 {
		t.Fatalf("reduce summary rows=%d reduceRows=%d want 1", summary.rows, exec.reduceRows)
	}
	if got, want := columnPhysicalQueryLinesM13B("q1", exec.groups()), []string{"q1:share=1"}; !equalStringSets(got, want) {
		t.Fatalf("reduce groups=%v want %v", got, want)
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

func readRawRetainedPayloadJSONForTestM13C(t *testing.T, d *backenddb.DB, collection string, raw []byte) []byte {
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
	cfg := catalog.meta.Options.ColumnStore.copy()
	resolved, err := resolveColumnRetainedPayloadAtSnapshot(snap, catalog, cfg, raw)
	if err != nil {
		t.Fatalf("resolve raw retained payload: %v raw=%q", err, raw)
	}
	obj, err := decodeColumnRetainedPayloadObject(cfg, resolved, columnRetainedPayloadTemplateResolver(snap, catalog))
	if err != nil {
		t.Fatalf("decode raw retained payload: %v raw=%q resolved=%q", err, raw, resolved)
	}
	out, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal raw retained payload: %v", err)
	}
	return out
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

func TestColumnPhysicalQueryDictionaryPredicatesJSONBenchShapes1869(t *testing.T) {
	events := []columnPhysicalPredicateEvent1869{
		{ID: "e1", Kind: "commit", Operation: "create", Event: "app.bsky.feed.post", Did: "did_b", TimeUS: 1000},
		{ID: "e2", Kind: "commit", Operation: "create", Event: "app.bsky.feed.post", Did: "did_a", TimeUS: 2000},
		{ID: "e3", Kind: "commit", Operation: "create", Event: "app.bsky.feed.post", Did: "did_a", TimeUS: 500},
		{ID: "e4", Kind: "commit", Operation: "delete", Event: "app.bsky.feed.post", Did: "did_c", TimeUS: 10},
		{ID: "e5", Kind: "identity", Operation: "create", Event: "app.bsky.feed.post", Did: "did_d", TimeUS: 20},
		{ID: "e6", Kind: "commit", Operation: "create", Event: "app.bsky.feed.like", Did: "did_a", TimeUS: 3000},
		{ID: "e7", Kind: "commit", Operation: "create", Event: "app.bsky.feed.repost", Did: "did_b", TimeUS: 4000},
		{ID: "e8", Kind: "commit", Operation: "create", Event: "app.bsky.feed.post", Did: "did_b", TimeUS: 9000},
	}
	collection, closeFn := openColumnPhysicalPredicateFixture1869(t, events)
	defer closeFn()

	commitCreate := []ColumnPhysicalQueryPredicate{
		{Column: "kind", Kind: ColumnPhysicalQueryPredicateEqual, Value: "commit"},
		{Column: "operation", Kind: ColumnPhysicalQueryPredicateEqual, Value: "create"},
	}
	postCreate := append(append([]ColumnPhysicalQueryPredicate(nil), commitCreate...), ColumnPhysicalQueryPredicate{Column: "event", Kind: ColumnPhysicalQueryPredicateEqual, Value: "app.bsky.feed.post"})

	tests := []struct {
		name        string
		req         ColumnPhysicalQueryRequest
		wantCount   map[string]int
		wantInt64   map[string]int64
		wantMatched int
	}{
		{
			name: "q2 count by collection with kind operation predicates",
			req: ColumnPhysicalQueryRequest{
				Kind:        ColumnPhysicalQueryGroupCount,
				GroupColumn: "event",
				Predicates:  commitCreate,
			},
			wantCount: map[string]int{
				"app.bsky.feed.like":   1,
				"app.bsky.feed.post":   4,
				"app.bsky.feed.repost": 1,
			},
			wantMatched: 6,
		},
		{
			name: "q2 distinct users by collection with kind operation predicates",
			req: ColumnPhysicalQueryRequest{
				Kind:           ColumnPhysicalQueryGroupCountDistinct,
				GroupColumn:    "event",
				DistinctColumn: "did",
				Predicates:     commitCreate,
			},
			wantCount: map[string]int{
				"app.bsky.feed.like":   1,
				"app.bsky.feed.post":   2,
				"app.bsky.feed.repost": 1,
			},
			wantMatched: 6,
		},
		{
			name: "q4 min by user with post predicates",
			req: ColumnPhysicalQueryRequest{
				Kind:        ColumnPhysicalQueryGroupMinInt64,
				GroupColumn: "did",
				ValueColumn: "time_us",
				Predicates:  postCreate,
			},
			wantInt64:   map[string]int64{"did_a": 500, "did_b": 1000},
			wantMatched: 4,
		},
		{
			name: "q5 span by user with post predicates",
			req: ColumnPhysicalQueryRequest{
				Kind:        ColumnPhysicalQueryGroupInt64Span,
				GroupColumn: "did",
				ValueColumn: "time_us",
				Predicates:  postCreate,
			},
			wantInt64:   map[string]int64{"did_a": 1500, "did_b": 8000},
			wantMatched: 4,
		},
		{
			name: "in-list collection predicate",
			req: ColumnPhysicalQueryRequest{
				Kind:        ColumnPhysicalQueryGroupCount,
				GroupColumn: "event",
				Predicates: []ColumnPhysicalQueryPredicate{
					{Column: "kind", Value: "commit"},
					{Column: "operation", Value: "create"},
					{Column: "event", Kind: ColumnPhysicalQueryPredicateInList, Values: []string{"app.bsky.feed.like", "app.bsky.feed.repost"}},
				},
			},
			wantCount:   map[string]int{"app.bsky.feed.like": 1, "app.bsky.feed.repost": 1},
			wantMatched: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := collection.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("RunColumnPhysicalQuery: %v", err)
			}
			assertColumnPhysicalPredicateResult1869(t, result, len(events), tc.wantMatched, tc.wantCount, tc.wantInt64)

			runner, err := collection.PrepareColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
			}
			defer func() { _ = runner.Close() }()
			for i := 0; i < 2; i++ {
				prepared, err := runner.Run()
				if err != nil {
					t.Fatalf("prepared Run %d: %v", i, err)
				}
				assertColumnPhysicalPredicateResult1869(t, prepared, len(events), tc.wantMatched, tc.wantCount, tc.wantInt64)
			}
		})
	}
}

func TestColumnPhysicalQueryGroupHourCountQ31858(t *testing.T) {
	base := int64(1_700_000_000_000_000)
	events := []columnPhysicalPredicateEvent1869{
		{ID: "e1", Kind: "commit", Operation: "create", Event: "post", Did: "did_a", TimeUS: base + 0*columnPhysicalQueryHourUS},
		{ID: "e2", Kind: "commit", Operation: "create", Event: "post", Did: "did_b", TimeUS: base + 0*columnPhysicalQueryHourUS + 1},
		{ID: "e3", Kind: "commit", Operation: "create", Event: "post", Did: "did_c", TimeUS: base + 1*columnPhysicalQueryHourUS},
		{ID: "e4", Kind: "commit", Operation: "create", Event: "like", Did: "did_d", TimeUS: base + 1*columnPhysicalQueryHourUS},
		{ID: "e5", Kind: "commit", Operation: "delete", Event: "post", Did: "did_e", TimeUS: base + 2*columnPhysicalQueryHourUS},
		{ID: "e6", Kind: "identity", Operation: "create", Event: "post", Did: "did_f", TimeUS: base + 3*columnPhysicalQueryHourUS},
		{ID: "e7", Kind: "commit", Operation: "create", Event: "repost", Did: "did_g", TimeUS: base + 23*columnPhysicalQueryHourUS},
	}
	collection, closeFn := openColumnPhysicalPredicateFixture1869(t, events)
	defer closeFn()

	req := ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupHourCount,
		GroupColumn: "event",
		ValueColumn: "time_us",
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
			{Column: "event", Kind: ColumnPhysicalQueryPredicateInList, Values: []string{"post", "like"}},
		},
	}
	want := map[string]int{
		fmt.Sprintf("like/%02d", columnPhysicalQueryUTCHour(base+1*columnPhysicalQueryHourUS)): 1,
		fmt.Sprintf("post/%02d", columnPhysicalQueryUTCHour(base+0*columnPhysicalQueryHourUS)): 2,
		fmt.Sprintf("post/%02d", columnPhysicalQueryUTCHour(base+1*columnPhysicalQueryHourUS)): 1,
	}
	assertQ3 := func(t *testing.T, result ColumnPhysicalQueryResult) {
		t.Helper()
		got := make(map[string]int, len(result.Groups))
		for _, group := range result.Groups {
			got[fmt.Sprintf("%s/%02d", group.Key, group.Hour)] = group.Count
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("q3 groups=%v want %v full=%+v", got, want, result.Groups)
		}
		if result.Diagnostics.RowsScanned != len(events) || result.Diagnostics.RowsMatched != 4 || result.Diagnostics.ReduceRows != 4 || result.Diagnostics.DocumentMaterializations != 0 || result.Diagnostics.RowMaterializations != 0 {
			t.Fatalf("diagnostics=%+v want scanned=%d matched/reduced=4 no materialization", result.Diagnostics, len(events))
		}
		if result.Diagnostics.PredicateCount != 3 || result.Diagnostics.DictionaryCodeHits == 0 || result.Diagnostics.Int64ValueHits == 0 {
			t.Fatalf("q3 sidecar diagnostics=%+v", result.Diagnostics)
		}
	}

	t.Run("one-shot", func(t *testing.T) {
		result, err := collection.RunColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("RunColumnPhysicalQuery q3: %v", err)
		}
		assertQ3(t, result)
	})
	t.Run("prepared", func(t *testing.T) {
		runner, err := collection.PrepareColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("PrepareColumnPhysicalQuery q3: %v", err)
		}
		defer func() { _ = runner.Close() }()
		for i := 0; i < 2; i++ {
			result, err := runner.Run()
			if err != nil {
				t.Fatalf("prepared q3 run %d: %v", i, err)
			}
			assertQ3(t, result)
		}
	})
	for _, tc := range []struct {
		name string
		req  ColumnPhysicalQueryRequest
		want string
	}{
		{name: "missing group", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupHourCount, ValueColumn: "time_us"}, want: "group column is required"},
		{name: "missing value", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupHourCount, GroupColumn: "event"}, want: "value column is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := collection.PrepareColumnPhysicalQuery(tc.req); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("PrepareColumnPhysicalQuery err=%v want ErrColumnQueryPlanUnsupported containing %q", err, tc.want)
			}
		})
	}

	absent := req
	absent.Predicates = []ColumnPhysicalQueryPredicate{{Column: "event", Value: "missing"}}
	result, err := collection.RunColumnPhysicalQuery(absent)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery absent q3: %v", err)
	}
	if len(result.Groups) != 0 || result.Diagnostics.RowsScanned != len(events) || result.Diagnostics.RowsMatched != 0 || result.Diagnostics.ReduceRows != 0 {
		t.Fatalf("absent q3 result=%+v diagnostics=%+v", result.Groups, result.Diagnostics)
	}
	absentRunner, err := collection.PrepareColumnPhysicalQuery(absent)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery absent q3: %v", err)
	}
	defer func() { _ = absentRunner.Close() }()
	preparedAbsent, err := absentRunner.Run()
	if err != nil {
		t.Fatalf("prepared absent q3: %v", err)
	}
	if len(preparedAbsent.Groups) != 0 || preparedAbsent.Diagnostics.RowsScanned != len(events) || preparedAbsent.Diagnostics.RowsMatched != 0 || preparedAbsent.Diagnostics.ReduceRows != 0 {
		t.Fatalf("prepared absent q3 result=%+v diagnostics=%+v", preparedAbsent.Groups, preparedAbsent.Diagnostics)
	}
}

func TestColumnPhysicalQueryGroupCountAndDistinctFused1870(t *testing.T) {
	events := []columnPhysicalPredicateEvent1869{
		{ID: "e1", Kind: "commit", Operation: "create", Event: "post", Did: "did_a", TimeUS: 1},
		{ID: "e2", Kind: "commit", Operation: "create", Event: "post", Did: "did_a", TimeUS: 2},
		{ID: "e3", Kind: "commit", Operation: "create", Event: "post", Did: "did_b", TimeUS: 3},
		{ID: "e4", Kind: "commit", Operation: "create", Event: "like", Did: "did_a", TimeUS: 4},
		{ID: "e5", Kind: "identity", Operation: "create", Event: "post", Did: "did_c", TimeUS: 5},
	}
	collection, closeFn := openColumnPhysicalPredicateFixture1869(t, events)
	defer closeFn()

	req := ColumnPhysicalQueryRequest{
		Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
		GroupColumn:    "event",
		DistinctColumn: "did",
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
		},
	}
	wantCounts := map[string]int{"like": 1, "post": 3}
	wantDistinct := map[string]int{"like": 1, "post": 2}
	assertFused := func(t *testing.T, result ColumnPhysicalQueryResult) {
		t.Helper()
		gotCounts := make(map[string]int, len(result.Groups))
		gotDistinct := make(map[string]int, len(result.Groups))
		for _, group := range result.Groups {
			gotCounts[group.Key] = group.Count
			gotDistinct[group.Key] = group.DistinctCount
		}
		if !reflect.DeepEqual(gotCounts, wantCounts) || !reflect.DeepEqual(gotDistinct, wantDistinct) {
			t.Fatalf("fused groups counts=%v distinct=%v want counts=%v distinct=%v full=%+v", gotCounts, gotDistinct, wantCounts, wantDistinct, result.Groups)
		}
		if result.Diagnostics.RowsScanned != len(events) || result.Diagnostics.RowsMatched != 4 || result.Diagnostics.ReduceRows != 4 {
			t.Fatalf("diagnostics=%+v want scanned=%d matched/reduced=4", result.Diagnostics, len(events))
		}
	}

	t.Run("one-shot", func(t *testing.T) {
		result, err := collection.RunColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("one-shot fused q2: %v", err)
		}
		assertFused(t, result)
	})
	t.Run("parallel fail closed", func(t *testing.T) {
		_, err := collection.RunColumnPhysicalQueryParallel(req, 4)
		if !errors.Is(err, ErrColumnQueryPlanUnsupported) {
			t.Fatalf("RunColumnPhysicalQueryParallel fused q2 err=%v want ErrColumnQueryPlanUnsupported", err)
		}
	})
	t.Run("invalid requests fail with precise validation", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			req  ColumnPhysicalQueryRequest
			want string
		}{
			{name: "missing group", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountAndDistinct, DistinctColumn: "did"}, want: "group column is required"},
			{name: "missing distinct", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountAndDistinct, GroupColumn: "event"}, want: "distinct column is required"},
			{name: "same columns", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountAndDistinct, GroupColumn: "event", DistinctColumn: "event"}, want: "group and distinct columns must differ"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if _, err := collection.PrepareColumnPhysicalQuery(tc.req); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("PrepareColumnPhysicalQuery err=%v want ErrColumnQueryPlanUnsupported containing %q", err, tc.want)
				}
				if _, err := collection.RunColumnPhysicalQuery(tc.req); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("RunColumnPhysicalQuery err=%v want ErrColumnQueryPlanUnsupported containing %q", err, tc.want)
				}
			})
		}
	})
	t.Run("prepared", func(t *testing.T) {
		runner, err := collection.PrepareColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("PrepareColumnPhysicalQuery fused q2: %v", err)
		}
		defer func() { _ = runner.Close() }()
		for i := 0; i < 2; i++ {
			result, err := runner.Run()
			if err != nil {
				t.Fatalf("prepared fused q2 run %d: %v", i, err)
			}
			assertFused(t, result)
		}
	})
	t.Run("prepared no predicates", func(t *testing.T) {
		noPredicateReq := req
		noPredicateReq.Predicates = nil
		runner, err := collection.PrepareColumnPhysicalQuery(noPredicateReq)
		if err != nil {
			t.Fatalf("PrepareColumnPhysicalQuery no-predicate fused q2: %v", err)
		}
		defer func() { _ = runner.Close() }()
		result, err := runner.Run()
		if err != nil {
			t.Fatalf("prepared no-predicate fused q2: %v", err)
		}
		gotCounts := make(map[string]int, len(result.Groups))
		gotDistinct := make(map[string]int, len(result.Groups))
		for _, group := range result.Groups {
			gotCounts[group.Key] = group.Count
			gotDistinct[group.Key] = group.DistinctCount
		}
		if wantCounts := map[string]int{"like": 1, "post": 4}; !reflect.DeepEqual(gotCounts, wantCounts) {
			t.Fatalf("no-predicate counts=%v want %v full=%+v", gotCounts, wantCounts, result.Groups)
		}
		if wantDistinct := map[string]int{"like": 1, "post": 3}; !reflect.DeepEqual(gotDistinct, wantDistinct) {
			t.Fatalf("no-predicate distinct=%v want %v full=%+v", gotDistinct, wantDistinct, result.Groups)
		}
	})
}

func TestColumnPhysicalQueryDictionaryPredicatesAbsentLiteralEmpty1869(t *testing.T) {
	events := []columnPhysicalPredicateEvent1869{
		{ID: "e1", Kind: "commit", Operation: "create", Event: "app.bsky.feed.post", Did: "did_a", TimeUS: 1},
		{ID: "e2", Kind: "commit", Operation: "create", Event: "app.bsky.feed.like", Did: "did_b", TimeUS: 2},
	}
	collection, closeFn := openColumnPhysicalPredicateFixture1869(t, events)
	defer closeFn()

	req := ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupCount,
		GroupColumn: "event",
		Predicates:  []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "missing_literal"}},
	}
	result, err := collection.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery absent literal: %v", err)
	}
	assertColumnPhysicalPredicateResult1869(t, result, len(events), 0, map[string]int{}, nil)

	runner, err := collection.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery absent literal: %v", err)
	}
	defer func() { _ = runner.Close() }()
	prepared, err := runner.Run()
	if err != nil {
		t.Fatalf("prepared absent literal: %v", err)
	}
	assertColumnPhysicalPredicateResult1869(t, prepared, len(events), 0, map[string]int{}, nil)
}

func TestColumnPhysicalQueryPredicateValidationFailsClosed1869(t *testing.T) {
	cfg := ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Dictionary: true},
		{Name: "nullable_kind", Path: "nullable_kind", ValueType: ColumnStoreValueString, Dictionary: true, Nullable: true},
		{Name: "payload", Path: "payload", ValueType: ColumnStoreValueString},
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
		{Name: "typed_kind", Path: "typed_kind", ValueType: ColumnStoreValueString, Dictionary: true, Owner: TypedStorageOwnerColumnPart},
	}}
	tooMany := make([]string, columnPhysicalQueryMaxPredicateValues+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("v%d", i)
	}
	tests := []struct {
		name string
		req  ColumnPhysicalQueryRequest
		want string
	}{
		{name: "missing column", req: ColumnPhysicalQueryRequest{Predicates: []ColumnPhysicalQueryPredicate{{Value: "commit"}}}, want: "column is required"},
		{name: "undeclared", req: ColumnPhysicalQueryRequest{Predicates: []ColumnPhysicalQueryPredicate{{Column: "missing", Value: "commit"}}}, want: "undeclared column"},
		{name: "non string", req: ColumnPhysicalQueryRequest{Predicates: []ColumnPhysicalQueryPredicate{{Column: "time_us", Value: "1"}}}, want: "has type"},
		{name: "non dictionary", req: ColumnPhysicalQueryRequest{Predicates: []ColumnPhysicalQueryPredicate{{Column: "payload", Value: "x"}}}, want: "requires dictionary"},
		{name: "nullable", req: ColumnPhysicalQueryRequest{Predicates: []ColumnPhysicalQueryPredicate{{Column: "nullable_kind", Value: "x"}}}, want: "nullable"},
		{name: "typed owner", req: ColumnPhysicalQueryRequest{Predicates: []ColumnPhysicalQueryPredicate{{Column: "typed_kind", Value: "x"}}}, want: "owner"},
		{name: "empty in-list", req: ColumnPhysicalQueryRequest{Predicates: []ColumnPhysicalQueryPredicate{{Column: "kind", Kind: ColumnPhysicalQueryPredicateInList}}}, want: "at least one value"},
		{name: "too many in-list", req: ColumnPhysicalQueryRequest{Predicates: []ColumnPhysicalQueryPredicate{{Column: "kind", Kind: ColumnPhysicalQueryPredicateInList, Values: tooMany}}}, want: "exceeds limit"},
		{name: "unknown kind", req: ColumnPhysicalQueryRequest{Predicates: []ColumnPhysicalQueryPredicate{{Column: "kind", Kind: ColumnPhysicalQueryPredicateKind("prefix"), Value: "c"}}}, want: "unsupported physical predicate kind"},
		{name: "duplicate", req: ColumnPhysicalQueryRequest{Predicates: []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "commit"}, {Column: "kind", Value: "identity"}}}, want: "multiple physical predicates"},
		{name: "metadata", req: ColumnPhysicalQueryRequest{AggregateMetadataName: "min_time_us", Predicates: []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "commit"}}}, want: "aggregate metadata"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := columnPhysicalQueryPredicateSpecs(cfg, tc.req)
			if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("predicate validation err=%v want unsupported containing %q", err, tc.want)
			}
		})
	}
}

func BenchmarkColumnPhysicalQueryDictionaryPredicates1869(b *testing.B) {
	events := make([]columnPhysicalPredicateEvent1869, 8192)
	collections := []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.repost"}
	for i := range events {
		events[i] = columnPhysicalPredicateEvent1869{
			ID:        fmt.Sprintf("e%06d", i),
			Kind:      "commit",
			Operation: "create",
			Event:     collections[i%len(collections)],
			Did:       fmt.Sprintf("did_%04d", i%1024),
			TimeUS:    int64(1_700_000_000_000_000 + i*1000),
		}
		if i%11 == 0 {
			events[i].Kind = "identity"
		}
		if i%13 == 0 {
			events[i].Operation = "delete"
		}
	}
	collection, closeFn := openColumnPhysicalPredicateFixture1869(b, events)
	defer closeFn()
	commit := []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "commit"}}
	commitCreate := []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "commit"}, {Column: "operation", Value: "create"}}
	postCreate := append(append([]ColumnPhysicalQueryPredicate(nil), commitCreate...), ColumnPhysicalQueryPredicate{Column: "event", Value: "app.bsky.feed.post"})
	cases := []struct {
		name string
		req  ColumnPhysicalQueryRequest
	}{
		{name: "q2_count_one_predicate", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "event", Predicates: commit}},
		{name: "q2_count", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "event", Predicates: commitCreate}},
		{name: "q2_distinct", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "event", DistinctColumn: "did", Predicates: commitCreate}},
		{name: "q2_fused_count_distinct", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountAndDistinct, GroupColumn: "event", DistinctColumn: "did", Predicates: commitCreate}},
		{name: "q3_group_hour_count", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupHourCount, GroupColumn: "event", ValueColumn: "time_us", Predicates: postCreate}},
		{name: "q4_min", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", Predicates: postCreate}},
		{name: "q5_span", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", Predicates: postCreate}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			runner, err := collection.PrepareColumnPhysicalQuery(tc.req)
			if err != nil {
				b.Fatalf("PrepareColumnPhysicalQuery(%s): %v", tc.name, err)
			}
			defer func() { _ = runner.Close() }()
			if _, err := runner.Run(); err != nil {
				b.Fatalf("warm Run: %v", err)
			}
			var scanned, matched int64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := runner.Run()
				if err != nil {
					b.Fatalf("Run: %v", err)
				}
				scanned += int64(result.Diagnostics.RowsScanned)
				matched += int64(result.Diagnostics.RowsMatched)
			}
			elapsed := b.Elapsed()
			if elapsed > 0 && scanned > 0 {
				b.ReportMetric(float64(scanned)/elapsed.Seconds(), "scanned_rows/s")
			}
			if b.N > 0 {
				b.ReportMetric(float64(scanned)/float64(b.N), "scanned_rows/op")
				b.ReportMetric(float64(matched)/float64(b.N), "matched_rows/op")
			}
		})
	}
}

func TestColumnPhysicalQueryPredicatesFailClosedForUnsupportedStates1869(t *testing.T) {
	reopened, closeFn := openColumnPhysicalQueryFixtureM13B(t, columnPhysicalQueryFixtureEventsM13B(16))
	defer closeFn()
	metadataReq := ColumnPhysicalQueryRequest{
		Kind:                  ColumnPhysicalQueryGroupMinInt64,
		GroupColumn:           "did",
		ValueColumn:           "time_us",
		AggregateMetadataName: "min_time_us",
		Predicates:            []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "kind_00"}},
	}
	if _, err := reopened.RunColumnPhysicalQuery(metadataReq); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "aggregate metadata") {
		t.Fatalf("metadata predicate err=%v want fail closed", err)
	}
	if _, err := reopened.PrepareColumnPhysicalQuery(metadataReq); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "aggregate metadata") {
		t.Fatalf("prepared metadata predicate err=%v want fail closed", err)
	}

	mutated, closeMutated, _ := openColumnPhysicalMutationFixtureM13C(t, 32)
	defer closeMutated()
	mutationReq := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind", Predicates: []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "kind_00"}}}
	if _, err := mutated.RunColumnPhysicalQuery(mutationReq); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "insert-only") {
		t.Fatalf("mutation predicate err=%v want insert-only fail closed", err)
	}
	if _, err := mutated.PrepareColumnPhysicalQuery(mutationReq); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "insert-only") {
		t.Fatalf("prepared mutation predicate err=%v want insert-only fail closed", err)
	}

	multiRef, closeMulti := openColumnPhysicalInsertMultiGenerationFixtureM14B(t, 4)
	defer closeMulti()
	parallelReq := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind", Predicates: []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "kind_00"}}}
	if _, err := multiRef.RunColumnPhysicalQueryParallel(parallelReq, 4); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "parallel physical predicates") {
		t.Fatalf("parallel predicate err=%v want fail-closed unsupported", err)
	}

	hourReq := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us", Predicates: []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "kind_00"}}}
	if _, err := reopened.RunColumnPhysicalQuery(hourReq); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "hour-count physical predicates") {
		t.Fatalf("hour-count predicate err=%v want specific fail-closed unsupported", err)
	}
	if _, err := reopened.PrepareColumnPhysicalQuery(hourReq); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "hour-count physical predicates") {
		t.Fatalf("prepared hour-count predicate err=%v want specific fail-closed unsupported", err)
	}
}

func assertColumnPhysicalPredicateResult1869(t *testing.T, result ColumnPhysicalQueryResult, wantScanned, wantMatched int, wantCount map[string]int, wantInt64 map[string]int64) {
	t.Helper()
	if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.DocumentMaterializations != 0 {
		t.Fatalf("materializations row=%d document=%d diagnostics=%+v", result.Diagnostics.RowMaterializations, result.Diagnostics.DocumentMaterializations, result.Diagnostics)
	}
	if result.Diagnostics.RowsScanned != wantScanned || result.Diagnostics.ReduceRows != wantMatched || result.Diagnostics.RowsMatched != wantMatched {
		t.Fatalf("diagnostic rows scanned/matched/reduced=%d/%d/%d want %d/%d/%d diagnostics=%+v", result.Diagnostics.RowsScanned, result.Diagnostics.RowsMatched, result.Diagnostics.ReduceRows, wantScanned, wantMatched, wantMatched, result.Diagnostics)
	}
	if result.Diagnostics.PredicateCount == 0 || len(result.Diagnostics.PredicateColumns) != result.Diagnostics.PredicateCount {
		t.Fatalf("missing predicate diagnostics: %+v", result.Diagnostics)
	}
	if wantMatched > 0 && result.Diagnostics.PredicateDictionaryCodeHits == 0 {
		t.Fatalf("missing predicate dictionary code hits for matched rows: %+v", result.Diagnostics)
	}
	if len(wantCount) != 0 || wantCount != nil {
		got := make(map[string]int, len(result.Groups))
		for _, group := range result.Groups {
			got[group.Key] = group.Count
		}
		if !reflect.DeepEqual(got, wantCount) {
			t.Fatalf("count groups=%v want %v full=%+v", got, wantCount, result.Groups)
		}
		return
	}
	got := make(map[string]int64, len(result.Groups))
	for _, group := range result.Groups {
		got[group.Key] = group.Int64
	}
	if !reflect.DeepEqual(got, wantInt64) {
		t.Fatalf("int64 groups=%v want %v full=%+v", got, wantInt64, result.Groups)
	}
}

type columnPhysicalPredicateEvent1869 struct {
	ID        string
	Kind      string
	Operation string
	Event     string
	Did       string
	TimeUS    int64
}

func openColumnPhysicalPredicateFixture1869(tb testing.TB, events []columnPhysicalPredicateEvent1869) (*Collection, func()) {
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
	cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Dictionary: true},
		{Name: "operation", Path: "operation", ValueType: ColumnStoreValueString, Dictionary: true},
		{Name: "event", Path: "event", ValueType: ColumnStoreValueString, Dictionary: true},
		{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Dictionary: true},
	}}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
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
		docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":%q,"operation":%q,"event":%q,"did":%q}`, event.TimeUS, event.Kind, event.Operation, event.Event, event.Did))
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
	if collectionsRaceEnabled {
		t.Skip("exact allocation counts are not stable under race instrumentation")
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
	if _, err := small.RunColumnPhysicalQuery(req); err != nil {
		t.Fatalf("warm small RunColumnPhysicalQuery: %v", err)
	}
	if _, err := large.RunColumnPhysicalQuery(req); err != nil {
		t.Fatalf("warm large RunColumnPhysicalQuery: %v", err)
	}
	if collectionsRaceEnabled {
		t.Skip("exact allocation counts are not stable under race instrumentation")
	}

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

func TestColumnPhysicalQueryRunnerParityAndAllocationM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(2048)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}
	wantHash := columnPhysicalQueryReferenceHashM13B("q1", events)
	tcpBytes := columnPhysicalQueryTCPAAssetBytesM1634(t, collection)

	runner, err := collection.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Fatalf("runner Close: %v", err)
		}
	}()
	for i := 0; i < 3; i++ {
		result, err := runner.Run()
		if err != nil {
			t.Fatalf("runner Run warmup %d: %v", i, err)
		}
		if got := columnPhysicalQueryHashLinesM13B(columnPhysicalQueryLinesM13B("q1", result.Groups)); got != wantHash {
			t.Fatalf("runner q1 hash=%d want %d groups=%+v", got, wantHash, result.Groups)
		}
		if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReduceRows != len(events) {
			t.Fatalf("runner diagnostics=%+v want direct reduce over %d rows", result.Diagnostics, len(events))
		}
		if result.Diagnostics.PhysicalBytesScanned <= 0 || result.Diagnostics.PhysicalBytesScanned >= tcpBytes {
			t.Fatalf("runner physical bytes=%d want dictionary-code sidecar below TCPA bytes=%d", result.Diagnostics.PhysicalBytesScanned, tcpBytes)
		}
		if result.Diagnostics.SegmentFileCacheMisses != 0 {
			t.Fatalf("runner diagnostics=%+v want no per-run segment cache misses after prepared dictionary-code setup", result.Diagnostics)
		}
	}
	if collectionsRaceEnabled {
		t.Skip("exact allocation counts are not stable under race instrumentation")
	}

	allocs := testing.AllocsPerRun(20, func() {
		result, err := runner.Run()
		if err != nil {
			panic(fmt.Sprintf("runner Run: %v", err))
		}
		if len(result.Groups) != 4 {
			panic(fmt.Sprintf("runner groups=%d want 4", len(result.Groups)))
		}
	})
	if allocs != 0 {
		t.Fatalf("runner q1 warmed allocs/run=%.2f want 0", allocs)
	}
}

func TestColumnPhysicalQueryRunnerDistinctSidecarParityAndAllocationM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(2048)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"}
	wantHash := columnPhysicalQueryReferenceHashM13B("q2", events)
	tcpBytes := columnPhysicalQueryTCPAAssetBytesM1634(t, collection)

	runner, err := collection.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Fatalf("runner Close: %v", err)
		}
	}()
	for i := 0; i < 3; i++ {
		result, err := runner.Run()
		if err != nil {
			t.Fatalf("runner Run warmup %d: %v", i, err)
		}
		if got := columnPhysicalQueryHashLinesM13B(columnPhysicalQueryLinesM13B("q2", result.Groups)); got != wantHash {
			t.Fatalf("runner q2 hash=%d want %d groups=%+v", got, wantHash, result.Groups)
		}
		if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReduceRows != len(events) {
			t.Fatalf("runner diagnostics=%+v want direct reduce over %d rows", result.Diagnostics, len(events))
		}
		if result.Diagnostics.ProjectedColumns != 2 {
			t.Fatalf("runner projected columns=%d want kind+did sidecars", result.Diagnostics.ProjectedColumns)
		}
		if result.Diagnostics.PhysicalBytesScanned <= 0 || result.Diagnostics.PhysicalBytesScanned >= tcpBytes {
			t.Fatalf("runner physical bytes=%d want dictionary-code sidecars below TCPA bytes=%d", result.Diagnostics.PhysicalBytesScanned, tcpBytes)
		}
		if result.Diagnostics.SegmentFileCacheMisses != 0 {
			t.Fatalf("runner diagnostics=%+v want no per-run segment cache misses after prepared dictionary-code setup", result.Diagnostics)
		}
	}
	if collectionsRaceEnabled {
		t.Skip("exact allocation counts are not stable under race instrumentation")
	}

	allocs := testing.AllocsPerRun(20, func() {
		result, err := runner.Run()
		if err != nil {
			panic(fmt.Sprintf("runner Run: %v", err))
		}
		if len(result.Groups) != 4 {
			panic(fmt.Sprintf("runner groups=%d want 4", len(result.Groups)))
		}
	})
	if allocs != 0 {
		t.Fatalf("runner q2 warmed allocs/run=%.2f want 0", allocs)
	}
}

func TestColumnPhysicalQueryRunnerInt64ValueSidecarParityAndAllocationM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(2048)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us"}
	wantHash := columnPhysicalQueryReferenceHashM13B("q3", events)
	tcpBytes := columnPhysicalQueryTCPAAssetBytesM1634(t, collection)

	runner, err := collection.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Fatalf("runner Close: %v", err)
		}
	}()
	wantGroups := 0
	for i := 0; i < 3; i++ {
		result, err := runner.Run()
		if err != nil {
			t.Fatalf("runner Run warmup %d: %v", i, err)
		}
		if got := columnPhysicalQueryHashLinesM13B(columnPhysicalQueryLinesM13B("q3", result.Groups)); got != wantHash {
			t.Fatalf("runner q3 hash=%d want %d groups=%+v", got, wantHash, result.Groups)
		}
		if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReduceRows != len(events) {
			t.Fatalf("runner diagnostics=%+v want direct reduce over %d rows", result.Diagnostics, len(events))
		}
		if result.Diagnostics.ProjectedColumns != 1 {
			t.Fatalf("runner projected columns=%d want time_us sidecar", result.Diagnostics.ProjectedColumns)
		}
		if result.Diagnostics.Int64ValueHits == 0 || result.Diagnostics.Int64ValueHits != result.Diagnostics.ScheduledGranules {
			t.Fatalf("runner int64 hits=%d scheduled=%d diagnostics=%+v", result.Diagnostics.Int64ValueHits, result.Diagnostics.ScheduledGranules, result.Diagnostics)
		}
		if result.Diagnostics.PhysicalBytesScanned <= 0 || result.Diagnostics.PhysicalBytesScanned >= tcpBytes {
			t.Fatalf("runner physical bytes=%d want int64 sidecars below TCPA bytes=%d", result.Diagnostics.PhysicalBytesScanned, tcpBytes)
		}
		if result.Diagnostics.SegmentFileCacheMisses != 0 {
			t.Fatalf("runner diagnostics=%+v want no per-run segment cache misses after prepared int64 sidecar setup", result.Diagnostics)
		}
		wantGroups = len(result.Groups)
	}
	if collectionsRaceEnabled {
		t.Skip("exact allocation counts are not stable under race instrumentation")
	}

	allocs := testing.AllocsPerRun(20, func() {
		result, err := runner.Run()
		if err != nil {
			panic(fmt.Sprintf("runner Run: %v", err))
		}
		if len(result.Groups) != wantGroups {
			panic(fmt.Sprintf("runner groups=%d want %d", len(result.Groups), wantGroups))
		}
	})
	if allocs != 0 {
		t.Fatalf("runner q3 warmed allocs/run=%.2f want 0", allocs)
	}
}

func TestColumnPhysicalQueryRunnerDictInt64SidecarParityAndAllocationM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(2048)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()
	tcpBytes := columnPhysicalQueryTCPAAssetBytesM1634(t, collection)
	tests := []struct {
		name string
		req  ColumnPhysicalQueryRequest
	}{
		{
			name: "q4a",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us"},
		},
		{
			name: "q4b",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us"},
		},
		{
			name: "q5",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantHash := columnPhysicalQueryReferenceHashM13B(tc.name, events)
			runner, err := collection.PrepareColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
			}
			defer func() {
				if err := runner.Close(); err != nil {
					t.Fatalf("runner Close: %v", err)
				}
			}()
			wantGroups := 0
			for i := 0; i < 3; i++ {
				result, err := runner.Run()
				if err != nil {
					t.Fatalf("runner Run warmup %d: %v", i, err)
				}
				if got := columnPhysicalQueryHashLinesM13B(columnPhysicalQueryLinesM13B(tc.name, result.Groups)); got != wantHash {
					t.Fatalf("runner %s hash=%d want %d groups=%+v", tc.name, got, wantHash, result.Groups)
				}
				if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReduceRows != len(events) {
					t.Fatalf("runner diagnostics=%+v want direct reduce over %d rows", result.Diagnostics, len(events))
				}
				if result.Diagnostics.ProjectedColumns != 2 {
					t.Fatalf("runner projected columns=%d want did+time_us sidecars", result.Diagnostics.ProjectedColumns)
				}
				if result.Diagnostics.DictionaryCodeHits == 0 || result.Diagnostics.DictionaryCodeHits != result.Diagnostics.ScheduledGranules {
					t.Fatalf("runner dictionary hits=%d scheduled=%d diagnostics=%+v", result.Diagnostics.DictionaryCodeHits, result.Diagnostics.ScheduledGranules, result.Diagnostics)
				}
				if result.Diagnostics.Int64ValueHits == 0 || result.Diagnostics.Int64ValueHits != result.Diagnostics.ScheduledGranules {
					t.Fatalf("runner int64 hits=%d scheduled=%d diagnostics=%+v", result.Diagnostics.Int64ValueHits, result.Diagnostics.ScheduledGranules, result.Diagnostics)
				}
				if result.Diagnostics.PhysicalBytesScanned <= 0 || result.Diagnostics.PhysicalBytesScanned >= tcpBytes {
					t.Fatalf("runner physical bytes=%d want sidecars below TCPA bytes=%d", result.Diagnostics.PhysicalBytesScanned, tcpBytes)
				}
				if result.Diagnostics.SegmentFileCacheMisses != 0 {
					t.Fatalf("runner diagnostics=%+v want no per-run segment cache misses after prepared sidecar setup", result.Diagnostics)
				}
				wantGroups = len(result.Groups)
			}
			if collectionsRaceEnabled {
				t.Skip("exact allocation counts are not stable under race instrumentation")
			}
			allocs := testing.AllocsPerRun(20, func() {
				result, err := runner.Run()
				if err != nil {
					panic(fmt.Sprintf("runner Run: %v", err))
				}
				if len(result.Groups) != wantGroups {
					panic(fmt.Sprintf("runner groups=%d want %d", len(result.Groups), wantGroups))
				}
			})
			if allocs != 0 {
				t.Fatalf("runner %s warmed allocs/run=%.2f want 0", tc.name, allocs)
			}
		})
	}
}

func TestColumnPhysicalQuerySerialDictionarySidecarParityM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(2048)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()
	tcpBytes := columnPhysicalQueryTCPAAssetBytesM1634(t, collection)
	tests := []struct {
		name             string
		hashName         string
		req              ColumnPhysicalQueryRequest
		projectedColumns int
	}{
		{
			name:             "q1",
			hashName:         "q1",
			req:              ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"},
			projectedColumns: 1,
		},
		{
			name:             "q2",
			hashName:         "q2",
			req:              ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"},
			projectedColumns: 2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantHash := columnPhysicalQueryReferenceHashM13B(tc.hashName, events)
			result, err := collection.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("RunColumnPhysicalQuery: %v", err)
			}
			if got := columnPhysicalQueryHashLinesM13B(columnPhysicalQueryLinesM13B(tc.hashName, result.Groups)); got != wantHash {
				t.Fatalf("serial sidecar %s hash=%d want %d groups=%+v", tc.name, got, wantHash, result.Groups)
			}
			if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReduceRows != len(events) {
				t.Fatalf("serial sidecar diagnostics=%+v want direct reduce over %d rows", result.Diagnostics, len(events))
			}
			if result.Diagnostics.ProjectedColumns != tc.projectedColumns {
				t.Fatalf("serial sidecar projected columns=%d want %d", result.Diagnostics.ProjectedColumns, tc.projectedColumns)
			}
			if result.Diagnostics.DictionaryCodeHits == 0 || result.Diagnostics.DictionaryCodeHits != result.Diagnostics.ScheduledGranules {
				t.Fatalf("serial sidecar dictionary hits=%d scheduled=%d diagnostics=%+v", result.Diagnostics.DictionaryCodeHits, result.Diagnostics.ScheduledGranules, result.Diagnostics)
			}
			if result.Diagnostics.PhysicalBytesScanned <= 0 || result.Diagnostics.PhysicalBytesScanned >= tcpBytes {
				t.Fatalf("serial sidecar physical bytes=%d want dictionary-code sidecars below TCPA bytes=%d", result.Diagnostics.PhysicalBytesScanned, tcpBytes)
			}
			if result.Diagnostics.SegmentFileCacheMisses == 0 {
				t.Fatalf("serial sidecar diagnostics=%+v want one-shot sidecar read misses accounted", result.Diagnostics)
			}
		})
	}
}

func TestColumnDictionaryCodeGroupCountOneShotStreamsLargeDirectQ11890(t *testing.T) {
	const rows = columnDictionaryCodeOneShotMaxBytes/4 + 4096
	view := buildColumnDictionaryCodeGroupCountOneShotView1890(t, rows, true)
	if bytes := columnDictionaryCodeSnapshotBytes(view, columnDictionaryCodeSnapshotsByPart(view, "kind")); bytes <= columnDictionaryCodeOneShotMaxBytes {
		t.Fatalf("large q1 dictionary bytes=%d want > cap %d", bytes, columnDictionaryCodeOneShotMaxBytes)
	}

	req := ColumnPhysicalQueryRequest{
		Kind:                     ColumnPhysicalQueryGroupCount,
		GroupColumn:              "kind",
		ColumnAssetReadIntegrity: ColumnAssetReadIntegrityVerify,
	}
	copyCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity copy cache: %v", err)
	}
	defer func() {
		if err := copyCache.close(); err != nil {
			t.Fatalf("copy cache close: %v", err)
		}
	}()
	copyCache.returnViews = false
	oneShot, ok, err := runColumnDictionaryCodeGroupCountOneShot(view, req, &copyCache)
	if err != nil {
		t.Fatalf("large q1 one-shot: %v", err)
	}
	if !ok {
		t.Fatal("large q1 one-shot fell back; want streaming direct reducer")
	}
	assertColumnDictionaryCodeGroupCountLargeQ11890(t, oneShot, rows, view.DictionaryCodes[0].AssetRef.Length)
	want := map[string]int{"kind_00": rows / 4, "kind_01": rows / 4, "kind_02": rows / 4, "kind_03": rows / 4}
	if got := columnPhysicalQueryGroupCountsM14B(oneShot.Groups); !reflect.DeepEqual(got, want) {
		t.Fatalf("large q1 groups=%v want %v", got, want)
	}

	preparedCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity prepared cache: %v", err)
	}
	defer func() {
		if err := preparedCache.close(); err != nil {
			t.Fatalf("prepared cache close: %v", err)
		}
	}()
	prepared, err := prepareColumnDictionaryCodeGroupCountRunner(view, req, &preparedCache)
	if err != nil {
		t.Fatalf("prepare large q1 runner: %v", err)
	}
	if prepared == nil {
		t.Fatal("prepare large q1 runner returned nil")
	}
	preparedResult := prepared.run(view, req)
	if !equalColumnPhysicalQueryGroups(oneShot.Groups, preparedResult.Groups) {
		t.Fatalf("large q1 one-shot groups=%+v want prepared %+v", oneShot.Groups, preparedResult.Groups)
	}

	distinctReq := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"}
	distinctCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity distinct cache: %v", err)
	}
	defer func() {
		if err := distinctCache.close(); err != nil {
			t.Fatalf("distinct cache close: %v", err)
		}
	}()
	distinctCache.returnViews = false
	if _, ok, err := runColumnDictionaryCodeGroupCountDistinctOneShot(view, distinctReq, &distinctCache); err != nil || ok {
		t.Fatalf("large q2 one-shot ok=%v err=%v want unchanged cap fallback", ok, err)
	}
}

func TestColumnDictionaryCodeGroupCountOneShotLocalDictionaryReorderParityM1942(t *testing.T) {
	view, wantGroups, wantBytes := buildColumnDictionaryCodeGroupCountOneShotMultiPartViewM1942(t)
	req := ColumnPhysicalQueryRequest{
		Kind:                     ColumnPhysicalQueryGroupCount,
		GroupColumn:              "kind",
		ColumnAssetReadIntegrity: ColumnAssetReadIntegrityVerify,
	}
	cache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity one-shot cache: %v", err)
	}
	defer func() {
		if err := cache.close(); err != nil {
			t.Fatalf("one-shot cache close: %v", err)
		}
	}()
	ones, ok, err := runColumnDictionaryCodeGroupCountOneShot(view, req, &cache)
	if err != nil {
		t.Fatalf("one-shot q1 local dictionaries: %v", err)
	}
	if !ok {
		t.Fatal("one-shot q1 local dictionaries fell back")
	}
	if got := columnPhysicalQueryGroupCountsM14B(ones.Groups); !reflect.DeepEqual(got, wantGroups) {
		t.Fatalf("one-shot groups=%v want %v full=%+v", got, wantGroups, ones.Groups)
	}
	wantOrdered := []ColumnPhysicalQueryGroup{{Key: "alpha", Count: 3}, {Key: "beta", Count: 2}, {Key: "delta", Count: 2}, {Key: "gamma", Count: 4}}
	if !reflect.DeepEqual(ones.Groups, wantOrdered) {
		t.Fatalf("one-shot ordered groups=%+v want %+v", ones.Groups, wantOrdered)
	}
	if diag := ones.Diagnostics; diag.RowsScanned != 11 || diag.ReduceRows != 11 || diag.DictionaryCodeHits != 2 || diag.ScheduledGranules != 2 || diag.DecodedBlocks != 2 || diag.DirectReduceBlocks != 2 || diag.PhysicalBytesScanned != wantBytes || diag.ResultGroups != len(wantGroups) {
		t.Fatalf("one-shot diagnostics=%+v want rows/reduce=11 blocks=2 bytes=%d groups=%d", diag, wantBytes, len(wantGroups))
	}

	preparedCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity prepared cache: %v", err)
	}
	defer func() {
		if err := preparedCache.close(); err != nil {
			t.Fatalf("prepared cache close: %v", err)
		}
	}()
	prepared, err := prepareColumnDictionaryCodeGroupCountRunner(view, req, &preparedCache)
	if err != nil {
		t.Fatalf("prepare q1 local dictionaries: %v", err)
	}
	if prepared == nil {
		t.Fatal("prepare q1 local dictionaries returned nil")
	}
	preparedResult := prepared.run(view, req)
	if !equalColumnPhysicalQueryGroups(ones.Groups, preparedResult.Groups) {
		t.Fatalf("one-shot groups=%+v want prepared %+v", ones.Groups, preparedResult.Groups)
	}
}

func TestColumnDictionaryCodeGroupCountOneShotLocalDictionaryScratchOverStackCapM1942(t *testing.T) {
	view, wantRows, wantGroups, wantBytes := buildColumnDictionaryCodeGroupCountOneShotOverStackCapViewM1942(t)
	assertColumnDictionaryCodeGroupCountOneShotParityM1942(t, view, wantRows, wantGroups, wantBytes)
}

func assertColumnDictionaryCodeGroupCountOneShotParityM1942(t *testing.T, view columnPhysicalScanSnapshotView, wantRows, wantGroups int, wantBytes int64) {
	t.Helper()
	req := ColumnPhysicalQueryRequest{
		Kind:                     ColumnPhysicalQueryGroupCount,
		GroupColumn:              "kind",
		ColumnAssetReadIntegrity: ColumnAssetReadIntegrityVerify,
	}
	cache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity one-shot cache: %v", err)
	}
	defer func() {
		if err := cache.close(); err != nil {
			t.Fatalf("one-shot cache close: %v", err)
		}
	}()
	oneShot, ok, err := runColumnDictionaryCodeGroupCountOneShot(view, req, &cache)
	if err != nil {
		t.Fatalf("one-shot q1 local dictionaries: %v", err)
	}
	if !ok {
		t.Fatal("one-shot q1 local dictionaries fell back")
	}
	if diag := oneShot.Diagnostics; diag.RowsScanned != wantRows || diag.ReduceRows != wantRows || diag.DictionaryCodeHits != len(view.AssetRefs) || diag.ScheduledGranules != len(view.AssetRefs) || diag.DecodedBlocks != len(view.AssetRefs) || diag.DirectReduceBlocks != len(view.AssetRefs) || diag.PhysicalBytesScanned != wantBytes || diag.ResultGroups != wantGroups || len(oneShot.Groups) != wantGroups {
		t.Fatalf("one-shot diagnostics=%+v groups=%d want rows/reduce=%d blocks=%d bytes=%d groups=%d", diag, len(oneShot.Groups), wantRows, len(view.AssetRefs), wantBytes, wantGroups)
	}

	preparedCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity prepared cache: %v", err)
	}
	defer func() {
		if err := preparedCache.close(); err != nil {
			t.Fatalf("prepared cache close: %v", err)
		}
	}()
	prepared, err := prepareColumnDictionaryCodeGroupCountRunner(view, req, &preparedCache)
	if err != nil {
		t.Fatalf("prepare q1 local dictionaries: %v", err)
	}
	if prepared == nil {
		t.Fatal("prepare q1 local dictionaries returned nil")
	}
	preparedResult := prepared.run(view, req)
	if !equalColumnPhysicalQueryGroups(oneShot.Groups, preparedResult.Groups) {
		t.Fatalf("one-shot groups=%+v want prepared %+v", oneShot.Groups, preparedResult.Groups)
	}
}

func TestColumnDictionaryCodeGroupCountOneShotRejectsCorruptWideLocalCodeM1942(t *testing.T) {
	view := buildColumnDictionaryCodeGroupCountOneShotView1890(t, 16, false)
	snapshot := view.DictionaryCodes[0]
	cache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity read cache: %v", err)
	}
	defer func() {
		if err := cache.close(); err != nil {
			t.Fatalf("read cache close: %v", err)
		}
	}()
	raw, err := cache.read(snapshot.AssetRef, nil)
	if err != nil {
		t.Fatalf("read dictionary sidecar: %v", err)
	}
	dictCur, cardinality, rowCount, err := decodeColumnDictionaryCodesAssetHeader(raw, snapshot.AssetRef, view.Config, view.CollectionName, "kind", false)
	if err != nil {
		t.Fatalf("decode dictionary sidecar header: %v", err)
	}
	for localCode := 0; localCode < cardinality; localCode++ {
		_ = dictCur.stringBytes()
		if dictCur.err != nil {
			break
		}
	}
	if dictCur.err != nil {
		t.Fatalf("decode dictionary entries: %v", dictCur.err)
	}
	payload, err := columnDictionaryCodesPayloadAfterDictionary(raw, snapshot.AssetRef, &dictCur, rowCount)
	if err != nil {
		t.Fatalf("dictionary payload bounds: %v", err)
	}
	corrupt := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint32(corrupt[payload.offset:], uint32(cardinality))
	corruptRef, err := writeColumnDictionaryCodesAssetToManager(view.ColumnAssetRootDir, view.Config, corrupt, snapshot.AssetRef.Generation, snapshot.AssetRef.PartID)
	if err != nil {
		t.Fatalf("write corrupt dictionary sidecar: %v", err)
	}
	view.DictionaryCodes[0].AssetRef = corruptRef
	view.DictionaryCodes[0].Bytes = corruptRef.Length

	corruptCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity corrupt cache: %v", err)
	}
	defer func() {
		if err := corruptCache.close(); err != nil {
			t.Fatalf("corrupt cache close: %v", err)
		}
	}()
	_, ok, err := runColumnDictionaryCodeGroupCountOneShot(view, ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityVerify}, &corruptCache)
	if err == nil || !strings.Contains(err.Error(), "outside cardinality") {
		t.Fatalf("corrupt one-shot ok=%v err=%v want outside cardinality", ok, err)
	}
	if !ok {
		t.Fatalf("corrupt one-shot ok=false err=%v want direct path fail-closed error", err)
	}
}

func TestColumnDictionaryCodeGroupCountOneShotFallbacks1890(t *testing.T) {
	view := buildColumnDictionaryCodeGroupCountOneShotView1890(t, 128, false)
	cache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity: %v", err)
	}
	defer func() {
		if err := cache.close(); err != nil {
			t.Fatalf("cache close: %v", err)
		}
	}()
	tests := []struct {
		name string
		req  ColumnPhysicalQueryRequest
	}{
		{name: "predicate", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind", Predicates: []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "kind_00"}}}},
		{name: "non-dictionary", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "time_us"}},
		{name: "unsupported-kind", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok, err := runColumnDictionaryCodeGroupCountOneShot(view, tc.req, &cache); err != nil || ok {
				t.Fatalf("one-shot ok=%v err=%v want clean fallback", ok, err)
			}
		})
	}
}

func TestColumnDictionaryCodeGroupCountRunnerNoPredicateFastPath1912(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(256)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()

	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}
	direct, err := collection.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	runner, err := collection.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Fatalf("runner Close: %v", err)
		}
	}()
	prepared, err := runner.Run()
	if err != nil {
		t.Fatalf("runner Run: %v", err)
	}
	if !equalColumnPhysicalQueryGroups(direct.Groups, prepared.Groups) {
		t.Fatalf("direct groups=%+v want prepared %+v", direct.Groups, prepared.Groups)
	}
	wantCounts := map[string]int{"kind_00": 64, "kind_01": 64, "kind_02": 64, "kind_03": 64}
	if got := columnPhysicalQueryGroupCountsM14B(prepared.Groups); !reflect.DeepEqual(got, wantCounts) {
		t.Fatalf("prepared groups=%v want %v full=%+v", got, wantCounts, prepared.Groups)
	}
	diag := prepared.Diagnostics
	if diag.RowsScanned != len(events) || diag.ReduceRows != len(events) {
		t.Fatalf("prepared diagnostics rows scanned/reduced=%d/%d want %d/%d full=%+v", diag.RowsScanned, diag.ReduceRows, len(events), len(events), diag)
	}
	if diag.ScheduledGranules == 0 || diag.ScheduledGranules != diag.DecodedBlocks || diag.ScheduledGranules != diag.DirectReduceBlocks || diag.ScheduledGranules != diag.DictionaryCodeHits {
		t.Fatalf("prepared block/code diagnostics=%+v want scheduled=decoded=direct_reduce=dictionary_hits>0", diag)
	}
	if diag.PhysicalBytesScanned <= 0 || diag.PhysicalBytesScanned != direct.Diagnostics.PhysicalBytesScanned {
		t.Fatalf("prepared physical bytes=%d direct=%d diagnostics=%+v", diag.PhysicalBytesScanned, direct.Diagnostics.PhysicalBytesScanned, diag)
	}
	if diag.ResultGroups != len(prepared.Groups) || diag.ResultGroups != direct.Diagnostics.ResultGroups {
		t.Fatalf("prepared result group diagnostics=%d len=%d direct=%d", diag.ResultGroups, len(prepared.Groups), direct.Diagnostics.ResultGroups)
	}
	if diag.PredicateCount != 0 || len(diag.PredicateColumns) != 0 || diag.PredicateDictionaryCodeHits != 0 || diag.RowsMatched != 0 {
		t.Fatalf("prepared no-predicate diagnostics unexpectedly populated: %+v", diag)
	}
}

func TestColumnDictionaryCodeGroupCountRunnerPredicateDiagnostics1912(t *testing.T) {
	events := []columnPhysicalPredicateEvent1869{
		{ID: "e1", Kind: "commit", Operation: "create", Event: "post", Did: "did_b", TimeUS: 1000},
		{ID: "e2", Kind: "commit", Operation: "create", Event: "post", Did: "did_a", TimeUS: 2000},
		{ID: "e3", Kind: "commit", Operation: "create", Event: "post", Did: "did_a", TimeUS: 500},
		{ID: "e4", Kind: "commit", Operation: "delete", Event: "post", Did: "did_c", TimeUS: 10},
		{ID: "e5", Kind: "identity", Operation: "create", Event: "post", Did: "did_d", TimeUS: 20},
		{ID: "e6", Kind: "commit", Operation: "create", Event: "like", Did: "did_a", TimeUS: 3000},
		{ID: "e7", Kind: "commit", Operation: "create", Event: "repost", Did: "did_b", TimeUS: 4000},
		{ID: "e8", Kind: "commit", Operation: "create", Event: "post", Did: "did_b", TimeUS: 9000},
	}
	collection, closeFn := openColumnPhysicalPredicateFixture1869(t, events)
	defer closeFn()

	req := ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupCount,
		GroupColumn: "event",
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
		},
	}
	direct, err := collection.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	runner, err := collection.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Fatalf("runner Close: %v", err)
		}
	}()
	prepared, err := runner.Run()
	if err != nil {
		t.Fatalf("runner Run: %v", err)
	}
	if !equalColumnPhysicalQueryGroups(direct.Groups, prepared.Groups) {
		t.Fatalf("direct groups=%+v want prepared %+v", direct.Groups, prepared.Groups)
	}
	wantCounts := map[string]int{"like": 1, "post": 4, "repost": 1}
	if got := columnPhysicalQueryGroupCountsM14B(prepared.Groups); !reflect.DeepEqual(got, wantCounts) {
		t.Fatalf("prepared groups=%v want %v full=%+v", got, wantCounts, prepared.Groups)
	}

	assertPredicateGroupCountDiagnostics := func(t *testing.T, name string, result ColumnPhysicalQueryResult) {
		t.Helper()
		diag := result.Diagnostics
		if diag.RowsScanned != len(events) || diag.RowsMatched != 6 || diag.ReduceRows != 6 {
			t.Fatalf("%s rows scanned/matched/reduced=%d/%d/%d want %d/6/6 full=%+v", name, diag.RowsScanned, diag.RowsMatched, diag.ReduceRows, len(events), diag)
		}
		if diag.ScheduledGranules == 0 || diag.ScheduledGranules != diag.DecodedBlocks || diag.ScheduledGranules != diag.DirectReduceBlocks || diag.ScheduledGranules != diag.DictionaryCodeHits {
			t.Fatalf("%s block/code diagnostics=%+v want scheduled=decoded=direct_reduce=dictionary_hits>0", name, diag)
		}
		if diag.PredicateDictionaryCodeHits != diag.ScheduledGranules*len(req.Predicates) {
			t.Fatalf("%s predicate code hits=%d want scheduled(%d)*predicates(%d) diagnostics=%+v", name, diag.PredicateDictionaryCodeHits, diag.ScheduledGranules, len(req.Predicates), diag)
		}
		if diag.PhysicalBytesScanned <= 0 {
			t.Fatalf("%s physical bytes=%d diagnostics=%+v", name, diag.PhysicalBytesScanned, diag)
		}
		if diag.ResultGroups != len(result.Groups) || diag.ResultGroups != len(wantCounts) {
			t.Fatalf("%s result group diagnostics=%d len=%d want=%d", name, diag.ResultGroups, len(result.Groups), len(wantCounts))
		}
		if diag.PredicateCount != len(req.Predicates) || !reflect.DeepEqual(diag.PredicateColumns, []string{"kind", "operation"}) || !reflect.DeepEqual(diag.PredicateKinds, []string{string(ColumnPhysicalQueryPredicateEqual), string(ColumnPhysicalQueryPredicateEqual)}) || diag.PredicateLiterals != len(req.Predicates) {
			t.Fatalf("%s predicate diagnostics=%+v", name, diag)
		}
	}
	assertPredicateGroupCountDiagnostics(t, "direct", direct)
	assertPredicateGroupCountDiagnostics(t, "prepared", prepared)
	if prepared.Diagnostics.PhysicalBytesScanned != direct.Diagnostics.PhysicalBytesScanned || prepared.Diagnostics.PredicateDictionaryCodeHits != direct.Diagnostics.PredicateDictionaryCodeHits {
		t.Fatalf("prepared diagnostics=%+v direct diagnostics=%+v", prepared.Diagnostics, direct.Diagnostics)
	}
}

func TestColumnPhysicalQueryDirectQ1MatchesPrepared1890(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(4096)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}
	direct, err := collection.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	runner, err := collection.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Fatalf("runner Close: %v", err)
		}
	}()
	prepared, err := runner.Run()
	if err != nil {
		t.Fatalf("runner Run: %v", err)
	}
	if !equalColumnPhysicalQueryGroups(direct.Groups, prepared.Groups) {
		t.Fatalf("direct q1 groups=%+v want prepared %+v", direct.Groups, prepared.Groups)
	}
	if direct.Diagnostics.RowsScanned != len(events) || direct.Diagnostics.ReduceRows != len(events) || direct.Diagnostics.ResultGroups != len(prepared.Groups) {
		t.Fatalf("direct q1 diagnostics=%+v want scanned/reduced=%d result_groups=%d", direct.Diagnostics, len(events), len(prepared.Groups))
	}
}

func buildColumnDictionaryCodeGroupCountOneShotView1890(t testing.TB, rows int, includeDid bool) columnPhysicalScanSnapshotView {
	t.Helper()
	if rows <= 0 {
		t.Fatalf("rows=%d want positive", rows)
	}
	normalized, err := normalizeCollectionMeta(CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	})
	if err != nil {
		t.Fatalf("normalize collection meta: %v", err)
	}
	cfg := normalized.Options.ColumnStore.copy()
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 1
	root := t.TempDir()
	writeCodes := func(column string, columnIndex int, dictionary []string, modulo int) ColumnAssetRef {
		t.Helper()
		codes := make([]uint32, rows)
		for i := range codes {
			codes[i] = uint32(i % modulo)
		}
		encoded, err := encodeColumnDictionaryCodesAsset(columnDictionaryCodesAsset{
			Collection:        "events",
			Namespace:         cfg.AssetManager.Namespace,
			Generation:        7,
			PartID:            columnPhysicalRowAssetPartID,
			AppliedCommandLSN: 1,
			SchemaHash:        cfg.SchemaHash,
			ColumnName:        column,
			ColumnIndex:       columnIndex,
			Dictionary:        dictionary,
			Codes:             codes,
		})
		if err != nil {
			t.Fatalf("encode dictionary codes %s: %v", column, err)
		}
		ref, err := writeColumnDictionaryCodesAssetToManager(root, cfg, encoded, 7, columnPhysicalRowAssetPartID)
		if err != nil {
			t.Fatalf("write dictionary codes %s: %v", column, err)
		}
		return ref
	}
	kindRef := writeCodes("kind", 1, []string{"kind_00", "kind_01", "kind_02", "kind_03"}, 4)
	refs := []columnManifestDictionaryCodesSnapshot{{ColumnName: "kind", AssetRef: kindRef, Bytes: kindRef.Length}}
	if includeDid {
		didRef := writeCodes("did", 2, []string{"did_00", "did_01", "did_02", "did_03", "did_04", "did_05", "did_06", "did_07", "did_08", "did_09", "did_10", "did_11"}, 12)
		refs = append(refs, columnManifestDictionaryCodesSnapshot{ColumnName: "did", AssetRef: didRef, Bytes: didRef.Length})
	}
	return columnPhysicalScanSnapshotView{
		CollectionName:     "events",
		Config:             cfg,
		FullConfig:         cfg,
		ColumnStoreEnabled: true,
		AssetRefs: []columnManifestAssetRefForScan{{
			Ref:    ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: cfg.AssetManager.Namespace, Generation: 7, PartID: columnPhysicalRowAssetPartID},
			Reason: ColumnPublishOperationInsert,
			Rows:   rows,
		}},
		DictionaryCodes:    refs,
		ColumnAssetRootDir: root,
		AssetNamespace:     cfg.AssetManager.Namespace,
		Diagnostics: columnPhysicalScanDiagnostics{
			AssetRefs: 1,
		},
	}
}

func buildColumnDictionaryCodeGroupCountOneShotMultiPartViewM1942(t testing.TB) (columnPhysicalScanSnapshotView, map[string]int, int64) {
	t.Helper()
	parts := []columnDictionaryCodeGroupCountOneShotPartSpecM1942{
		{partID: 1, dictionary: []string{"alpha", "beta", "gamma"}, codes: []uint32{0, 1, 2, 0, 2}},
		{partID: 2, dictionary: []string{"gamma", "alpha", "beta", "delta"}, codes: []uint32{0, 1, 3, 3, 2, 0}},
	}
	view, bytes := buildColumnDictionaryCodeGroupCountOneShotPartsViewM1942(t, parts)
	return view, map[string]int{"alpha": 3, "beta": 2, "gamma": 4, "delta": 2}, bytes
}

func buildColumnDictionaryCodeGroupCountOneShotOverStackCapViewM1942(t testing.TB) (columnPhysicalScanSnapshotView, int, int, int64) {
	t.Helper()
	dictionary := make([]string, columnDictionaryCodeGroupCountOneShotLocalStackCap+1)
	codes := make([]uint32, len(dictionary))
	for i := range dictionary {
		dictionary[i] = fmt.Sprintf("kind_%03d", i)
		codes[i] = uint32(i)
	}
	firstKind := dictionary[0]
	lastKind := dictionary[len(dictionary)-1]
	parts := []columnDictionaryCodeGroupCountOneShotPartSpecM1942{
		{partID: 1, dictionary: dictionary, codes: codes},
		{partID: 2, dictionary: []string{lastKind, firstKind, "kind_extra"}, codes: []uint32{0, 1, 2, 0}},
	}
	view, bytes := buildColumnDictionaryCodeGroupCountOneShotPartsViewM1942(t, parts)
	return view, len(codes) + 4, len(dictionary) + 1, bytes
}

type columnDictionaryCodeGroupCountOneShotPartSpecM1942 struct {
	partID     uint64
	dictionary []string
	codes      []uint32
}

func buildColumnDictionaryCodeGroupCountOneShotPartsViewM1942(t testing.TB, parts []columnDictionaryCodeGroupCountOneShotPartSpecM1942) (columnPhysicalScanSnapshotView, int64) {
	t.Helper()
	view := buildColumnDictionaryCodeGroupCountOneShotView1890(t, 1, false)
	cfg := view.Config
	root := view.ColumnAssetRootDir
	assetRefs := make([]columnManifestAssetRefForScan, 0, len(parts))
	dictionaryRefs := make([]columnManifestDictionaryCodesSnapshot, 0, len(parts))
	var bytes int64
	for _, part := range parts {
		encoded, err := encodeColumnDictionaryCodesAsset(columnDictionaryCodesAsset{
			Collection:        "events",
			Namespace:         cfg.AssetManager.Namespace,
			Generation:        19,
			PartID:            part.partID,
			AppliedCommandLSN: 1,
			SchemaHash:        cfg.SchemaHash,
			ColumnName:        "kind",
			ColumnIndex:       1,
			Dictionary:        part.dictionary,
			Codes:             part.codes,
		})
		if err != nil {
			t.Fatalf("encode dictionary codes part=%d: %v", part.partID, err)
		}
		ref, err := writeColumnDictionaryCodesAssetToManager(root, cfg, encoded, 19, part.partID)
		if err != nil {
			t.Fatalf("write dictionary codes part=%d: %v", part.partID, err)
		}
		assetRefs = append(assetRefs, columnManifestAssetRefForScan{
			Ref:    ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: cfg.AssetManager.Namespace, Generation: 19, PartID: part.partID},
			Reason: ColumnPublishOperationInsert,
			Rows:   len(part.codes),
		})
		dictionaryRefs = append(dictionaryRefs, columnManifestDictionaryCodesSnapshot{ColumnName: "kind", AssetRef: ref, Bytes: ref.Length})
		bytes += ref.Length
	}
	view.AssetRefs = assetRefs
	view.DictionaryCodes = dictionaryRefs
	view.Diagnostics = columnPhysicalScanDiagnostics{AssetRefs: len(assetRefs)}
	return view, bytes
}

func assertColumnDictionaryCodeGroupCountLargeQ11890(t testing.TB, result ColumnPhysicalQueryResult, rows int, bytes int64) {
	t.Helper()
	diag := result.Diagnostics
	if diag.RowsScanned != rows || diag.ReduceRows != rows {
		t.Fatalf("large q1 rows scanned/reduced=%d/%d want %d diagnostics=%+v", diag.RowsScanned, diag.ReduceRows, rows, diag)
	}
	if diag.DictionaryCodeHits != 1 || diag.ScheduledGranules != 1 || diag.DecodedBlocks != 1 || diag.DirectReduceBlocks != 1 {
		t.Fatalf("large q1 sidecar diagnostics=%+v want one dictionary direct-reduce block", diag)
	}
	if diag.PhysicalBytesScanned != bytes || diag.PhysicalBytesScanned <= columnDictionaryCodeOneShotMaxBytes {
		t.Fatalf("large q1 physical bytes=%d want %d and > cap %d diagnostics=%+v", diag.PhysicalBytesScanned, bytes, columnDictionaryCodeOneShotMaxBytes, diag)
	}
	if diag.ResultGroups != 4 || len(result.Groups) != 4 {
		t.Fatalf("large q1 result groups diag/groups=%d/%d want 4 diagnostics=%+v", diag.ResultGroups, len(result.Groups), diag)
	}
	if diag.ProjectedColumns != 1 || diag.ColumnAssetReadIntegrity != string(ColumnAssetReadIntegrityVerify) {
		t.Fatalf("large q1 projected/integrity diagnostics=%+v", diag)
	}
}

func TestColumnPhysicalQuerySerialDictionaryDistinctOneShotRequiresViewBackedReadsM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(2048)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()

	view, closeView, err := collection.prepareColumnPhysicalScanSnapshotView()
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"}
	copyCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, ColumnAssetReadIntegritySkipChecksums)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity copy cache: %v", err)
	}
	defer func() {
		if err := copyCache.close(); err != nil {
			t.Fatalf("copy cache close: %v", err)
		}
	}()
	copyCache.returnViews = false
	if _, ok, err := runColumnDictionaryCodeGroupCountDistinctOneShot(view, req, &copyCache); err != nil || ok {
		t.Fatalf("copy-backed one-shot ok=%v err=%v want clean fallback", ok, err)
	}

	viewCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, ColumnAssetReadIntegritySkipChecksums)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity view cache: %v", err)
	}
	defer func() {
		if err := viewCache.close(); err != nil {
			t.Fatalf("view cache close: %v", err)
		}
	}()
	viewCache.returnViews = true
	result, ok, err := runColumnDictionaryCodeGroupCountDistinctOneShot(view, req, &viewCache)
	if err != nil {
		t.Fatalf("view-backed one-shot: %v", err)
	}
	if !ok {
		t.Skip("column asset mmap views are unavailable on this platform")
	}
	if got := columnPhysicalQueryHashLinesM13B(columnPhysicalQueryLinesM13B("q2", result.Groups)); got != columnPhysicalQueryReferenceHashM13B("q2", events) {
		t.Fatalf("view-backed one-shot q2 hash=%d want reference", got)
	}
}

func TestColumnPhysicalQuerySerialInt64ValueSidecarParityM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(2048)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()
	tcpBytes := columnPhysicalQueryTCPAAssetBytesM1634(t, collection)
	wantHash := columnPhysicalQueryReferenceHashM13B("q3", events)
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us"}
	result, err := collection.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	if got := columnPhysicalQueryHashLinesM13B(columnPhysicalQueryLinesM13B("q3", result.Groups)); got != wantHash {
		t.Fatalf("serial int64 sidecar q3 hash=%d want %d groups=%+v", got, wantHash, result.Groups)
	}
	if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReduceRows != len(events) {
		t.Fatalf("serial int64 sidecar diagnostics=%+v want direct reduce over %d rows", result.Diagnostics, len(events))
	}
	if result.Diagnostics.ProjectedColumns != 1 {
		t.Fatalf("serial int64 sidecar projected columns=%d want 1", result.Diagnostics.ProjectedColumns)
	}
	if result.Diagnostics.Int64ValueHits == 0 || result.Diagnostics.Int64ValueHits != result.Diagnostics.ScheduledGranules {
		t.Fatalf("serial int64 sidecar hits=%d scheduled=%d diagnostics=%+v", result.Diagnostics.Int64ValueHits, result.Diagnostics.ScheduledGranules, result.Diagnostics)
	}
	if result.Diagnostics.DictionaryCodeHits != 0 {
		t.Fatalf("serial int64 sidecar dictionary hits=%d want 0 diagnostics=%+v", result.Diagnostics.DictionaryCodeHits, result.Diagnostics)
	}
	if result.Diagnostics.PhysicalBytesScanned <= 0 || result.Diagnostics.PhysicalBytesScanned >= tcpBytes {
		t.Fatalf("serial int64 sidecar physical bytes=%d want int64 sidecars below TCPA bytes=%d", result.Diagnostics.PhysicalBytesScanned, tcpBytes)
	}
	if result.Diagnostics.SegmentFileCacheMisses == 0 {
		t.Fatalf("serial int64 sidecar diagnostics=%+v want one-shot sidecar read misses accounted", result.Diagnostics)
	}
}

func TestColumnPhysicalQuerySerialDictInt64SidecarParityM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(2048)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()
	tcpBytes := columnPhysicalQueryTCPAAssetBytesM1634(t, collection)
	tests := []struct {
		name string
		req  ColumnPhysicalQueryRequest
	}{
		{
			name: "q4a",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us"},
		},
		{
			name: "q4b",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us"},
		},
		{
			name: "q5",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantHash := columnPhysicalQueryReferenceHashM13B(tc.name, events)
			result, err := collection.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("RunColumnPhysicalQuery: %v", err)
			}
			if got := columnPhysicalQueryHashLinesM13B(columnPhysicalQueryLinesM13B(tc.name, result.Groups)); got != wantHash {
				t.Fatalf("serial sidecar %s hash=%d want %d groups=%+v", tc.name, got, wantHash, result.Groups)
			}
			if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReduceRows != len(events) {
				t.Fatalf("serial sidecar diagnostics=%+v want direct reduce over %d rows", result.Diagnostics, len(events))
			}
			if result.Diagnostics.ProjectedColumns != 2 {
				t.Fatalf("serial sidecar projected columns=%d want did+time_us sidecars", result.Diagnostics.ProjectedColumns)
			}
			if result.Diagnostics.DictionaryCodeHits == 0 || result.Diagnostics.DictionaryCodeHits != result.Diagnostics.ScheduledGranules {
				t.Fatalf("serial sidecar dictionary hits=%d scheduled=%d diagnostics=%+v", result.Diagnostics.DictionaryCodeHits, result.Diagnostics.ScheduledGranules, result.Diagnostics)
			}
			if result.Diagnostics.Int64ValueHits == 0 || result.Diagnostics.Int64ValueHits != result.Diagnostics.ScheduledGranules {
				t.Fatalf("serial sidecar int64 hits=%d scheduled=%d diagnostics=%+v", result.Diagnostics.Int64ValueHits, result.Diagnostics.ScheduledGranules, result.Diagnostics)
			}
			if result.Diagnostics.PhysicalBytesScanned <= 0 || result.Diagnostics.PhysicalBytesScanned >= tcpBytes {
				t.Fatalf("serial sidecar physical bytes=%d want dictionary+int64 sidecars below TCPA bytes=%d", result.Diagnostics.PhysicalBytesScanned, tcpBytes)
			}
			if result.Diagnostics.SegmentFileCacheMisses == 0 {
				t.Fatalf("serial sidecar diagnostics=%+v want one-shot sidecar read misses accounted", result.Diagnostics)
			}
		})
	}
}

// columnPhysicalQueryOneShotSnapshotHandleAllocsM1634 accounts for the unique
// cached and backend snapshot handles acquired by RunColumnPhysicalQuery. The
// handles intentionally cannot be pooled because a stale exported pointer must
// never become valid for a later snapshot.
const columnPhysicalQueryOneShotSnapshotHandleAllocsM1634 = 2

// columnPhysicalQueryOneShotRootIteratorHandleAllocsM1634 accounts for the
// three root iterators used to load a physical-query snapshot view. Root
// iterators are snapshot-bound handles so Close can invalidate them and defer
// snapshot cleanup until they are released.
const columnPhysicalQueryOneShotRootIteratorHandleAllocsM1634 = 3

func TestColumnPhysicalQuerySerialSidecarAllocationBudgetM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(2048)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()
	tests := []struct {
		name      string
		req       ColumnPhysicalQueryRequest
		maxAllocs float64
	}{
		{
			name:      "q1_dictionary_codes",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"},
			maxAllocs: 21,
		},
		{
			name:      "q2_dictionary_codes",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"},
			maxAllocs: 40,
		},
		{
			name:      "q3_int64_values",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us"},
			maxAllocs: 10,
		},
		{
			name:      "q4a_dictionary_int64",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us"},
			maxAllocs: 36,
		},
		{
			name:      "q4b_dictionary_int64",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us"},
			maxAllocs: 36,
		},
		{
			name:      "q5_dictionary_int64",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"},
			maxAllocs: 37,
		},
		{
			name:      "q4b_metadata",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "max_time_us"},
			maxAllocs: 34,
		},
		{
			name:      "q5_metadata",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"},
			maxAllocs: 34,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := collection.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("RunColumnPhysicalQuery preview: %v", err)
			}
			if result.Diagnostics.RowMaterializations != 0 {
				t.Fatalf("preview diagnostics=%+v want sidecar path", result.Diagnostics)
			}
			if collectionsRaceEnabled {
				t.Skip("exact allocation counts are not stable under race instrumentation")
			}
			allocs := testing.AllocsPerRun(20, func() {
				result, err := collection.RunColumnPhysicalQuery(tc.req)
				if err != nil {
					panic(fmt.Sprintf("RunColumnPhysicalQuery: %v", err))
				}
				if len(result.Groups) == 0 || result.Diagnostics.RowMaterializations != 0 {
					panic(fmt.Sprintf("bad sidecar result groups=%d diagnostics=%+v", len(result.Groups), result.Diagnostics))
				}
			})
			maxAllocs := tc.maxAllocs +
				columnPhysicalQueryOneShotSnapshotHandleAllocsM1634 +
				columnPhysicalQueryOneShotRootIteratorHandleAllocsM1634
			if runtime.GOOS == "windows" {
				// Windows CI carries extra runtime/path allocation noise in this
				// routed one-shot path; keep the stricter budget on Unix hosts
				// where the #1634 performance evidence is collected.
				maxAllocs += 32
			}
			if allocs > maxAllocs {
				t.Fatalf("%s routed sidecar allocs/run=%.2f want <= %.2f", tc.name, allocs, maxAllocs)
			}
		})
	}
}

func TestColumnPhysicalQueryManifestSidecarFilterM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(128)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()

	all, closeAll, err := collection.prepareColumnPhysicalScanSnapshotView()
	if closeAll != nil {
		defer closeAll()
	}
	if err != nil {
		t.Fatalf("prepare full snapshot view: %v", err)
	}
	if got, want := len(all.AssetRefs), 1; got != want {
		t.Fatalf("full asset refs=%d want %d", got, want)
	}
	if got, want := len(all.DictionaryCodes), 2; got != want {
		t.Fatalf("full dictionary sidecars=%d want %d", got, want)
	}
	if got, want := len(all.Int64Values), 1; got != want {
		t.Fatalf("full int64 sidecars=%d want %d", got, want)
	}
	if got, want := len(all.AggregateMetadata), 2; got != want {
		t.Fatalf("full aggregate sidecars=%d want %d", got, want)
	}

	tests := []struct {
		name           string
		req            ColumnPhysicalQueryRequest
		wantDictionary int
		wantInt64      int
		wantAggregate  int
	}{
		{
			name:           "q1_dictionary_only",
			req:            ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"},
			wantDictionary: 1,
		},
		{
			name:           "q2_dictionary_only",
			req:            ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"},
			wantDictionary: 2,
		},
		{
			name:      "q3_int64_only",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us"},
			wantInt64: 1,
		},
		{
			name:           "q4_dictionary_and_int64",
			req:            ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us"},
			wantDictionary: 1,
			wantInt64:      1,
		},
		{
			name:          "q5_metadata_aggregate_only",
			req:           ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"},
			wantAggregate: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			view, closeView, err := collection.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanSidecarsForPhysicalQuery(tc.req))
			if closeView != nil {
				defer closeView()
			}
			if err != nil {
				t.Fatalf("prepare filtered snapshot view: %v", err)
			}
			if got, want := len(view.AssetRefs), len(all.AssetRefs); got != want {
				t.Fatalf("filtered asset refs=%d want %d", got, want)
			}
			if got := len(view.DictionaryCodes); got != tc.wantDictionary {
				t.Fatalf("filtered dictionary sidecars=%d want %d", got, tc.wantDictionary)
			}
			if got := len(view.Int64Values); got != tc.wantInt64 {
				t.Fatalf("filtered int64 sidecars=%d want %d", got, tc.wantInt64)
			}
			if got := len(view.AggregateMetadata); got != tc.wantAggregate {
				t.Fatalf("filtered aggregate sidecars=%d want %d", got, tc.wantAggregate)
			}
		})
	}
}

func TestColumnPhysicalQueryRunnerDistinctSidecarSkipsIdenticalColumnsM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(128)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "kind"}

	runner, err := collection.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Fatalf("runner Close: %v", err)
		}
	}()
	result, err := runner.Run()
	if err != nil {
		t.Fatalf("runner Run: %v", err)
	}
	want := []string{"q2:kind_00=1", "q2:kind_01=1", "q2:kind_02=1", "q2:kind_03=1"}
	if got := columnPhysicalQueryLinesM13B("q2", result.Groups); !equalStringSets(got, want) {
		t.Fatalf("runner q2 identical-column lines=%v want %v", got, want)
	}
	if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReduceRows != len(events) {
		t.Fatalf("runner diagnostics=%+v want direct reduce over %d rows", result.Diagnostics, len(events))
	}
	if result.Diagnostics.ProjectedColumns != 1 {
		t.Fatalf("runner projected columns=%d want direct fallback projection for kind/kind", result.Diagnostics.ProjectedColumns)
	}
}

func TestColumnPhysicalQueryRunnerAggregateMetadataParityAndAllocationM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(2048)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()
	tests := []struct {
		name     string
		hashName string
		req      ColumnPhysicalQueryRequest
	}{
		{
			name:     "q4b",
			hashName: "q4b",
			req:      ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "max_time_us"},
		},
		{
			name:     "q5_metadata",
			hashName: "q5",
			req:      ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantHash := columnPhysicalQueryReferenceHashM13B(tc.hashName, events)
			baseline, err := collection.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("RunColumnPhysicalQuery baseline: %v", err)
			}
			if baseline.Diagnostics.MetadataHits == 0 || baseline.Diagnostics.RowsScanned != 0 || baseline.Diagnostics.DecodedBlocks != 0 {
				t.Fatalf("baseline diagnostics=%+v want metadata-only path", baseline.Diagnostics)
			}
			runner, err := collection.PrepareColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
			}
			defer func() {
				if err := runner.Close(); err != nil {
					t.Fatalf("runner Close: %v", err)
				}
			}()
			for i := 0; i < 3; i++ {
				result, err := runner.Run()
				if err != nil {
					t.Fatalf("runner Run warmup %d: %v", i, err)
				}
				if got := columnPhysicalQueryHashLinesM13B(columnPhysicalQueryLinesM13B(tc.name, result.Groups)); got != wantHash {
					t.Fatalf("runner %s hash=%d want %d groups=%+v", tc.name, got, wantHash, result.Groups)
				}
				if result.Diagnostics.MetadataHits == 0 || result.Diagnostics.RowsScanned != 0 || result.Diagnostics.DecodedBlocks != 0 {
					t.Fatalf("runner diagnostics=%+v want metadata-only hot path", result.Diagnostics)
				}
				if result.Diagnostics.SegmentFileCacheMisses != 0 {
					t.Fatalf("runner diagnostics=%+v want no per-run segment cache misses after metadata prepare", result.Diagnostics)
				}
				if result.Diagnostics.ReduceRows != len(events) {
					t.Fatalf("runner reduce rows=%d want %d diagnostics=%+v", result.Diagnostics.ReduceRows, len(events), result.Diagnostics)
				}
				if result.Diagnostics.PhysicalBytesScanned <= 0 || result.Diagnostics.PhysicalBytesScanned != baseline.Diagnostics.PhysicalBytesScanned {
					t.Fatalf("runner physical bytes=%d want metadata bytes=%d", result.Diagnostics.PhysicalBytesScanned, baseline.Diagnostics.PhysicalBytesScanned)
				}
			}
			if collectionsRaceEnabled {
				t.Skip("exact allocation counts are not stable under race instrumentation")
			}
			allocs := testing.AllocsPerRun(20, func() {
				result, err := runner.Run()
				if err != nil {
					panic(fmt.Sprintf("runner Run: %v", err))
				}
				if len(result.Groups) == 0 {
					panic("empty metadata result")
				}
			})
			if allocs != 0 {
				t.Fatalf("runner %s warmed allocs/run=%.2f want 0", tc.name, allocs)
			}
		})
	}
}

func TestColumnPhysicalQueryDictionaryCodesAreManifestReachableM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(128)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()
	view, closeView, err := collection.prepareColumnPhysicalScanSnapshotView()
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	if len(view.AssetRefs) != 1 {
		t.Fatalf("asset refs=%d want 1 physical part", len(view.AssetRefs))
	}
	if len(view.DictionaryCodes) != 2 {
		t.Fatalf("dictionary code refs=%d want kind+did sidecars", len(view.DictionaryCodes))
	}
	if len(view.Int64Values) != 1 {
		t.Fatalf("int64 value refs=%d want time_us sidecar", len(view.Int64Values))
	}
	byColumn := make(map[string]ColumnAssetRef, len(view.DictionaryCodes))
	for _, sidecar := range view.DictionaryCodes {
		byColumn[sidecar.ColumnName] = sidecar.AssetRef
		if sidecar.AssetRef.Kind != ColumnAssetKindTCS1DictionaryCodes {
			t.Fatalf("dictionary sidecar kind=%q want %q", sidecar.AssetRef.Kind, ColumnAssetKindTCS1DictionaryCodes)
		}
		if sidecar.AssetRef.Generation != view.AssetRefs[0].Ref.Generation || sidecar.AssetRef.PartID != view.AssetRefs[0].Ref.PartID {
			t.Fatalf("dictionary sidecar ref=%+v want same generation/part as %+v", sidecar.AssetRef, view.AssetRefs[0].Ref)
		}
	}
	if byColumn["kind"].Length == 0 || byColumn["did"].Length == 0 {
		t.Fatalf("dictionary sidecars missing expected columns: %+v", byColumn)
	}
	if view.Int64Values[0].ColumnName != "time_us" || view.Int64Values[0].AssetRef.Kind != ColumnAssetKindTCS1Int64Values {
		t.Fatalf("int64 sidecar=%+v want time_us %q", view.Int64Values[0], ColumnAssetKindTCS1Int64Values)
	}
	if view.Int64Values[0].Rows != len(events) {
		t.Fatalf("int64 sidecar rows=%d want %d", view.Int64Values[0].Rows, len(events))
	}
	if view.Int64Values[0].AssetRef.Generation != view.AssetRefs[0].Ref.Generation || view.Int64Values[0].AssetRef.PartID != view.AssetRefs[0].Ref.PartID {
		t.Fatalf("int64 sidecar ref=%+v want same generation/part as %+v", view.Int64Values[0].AssetRef, view.AssetRefs[0].Ref)
	}
}

func TestColumnInt64ValuesAssetCodecRejectsCorruptionM1634(t *testing.T) {
	normalized, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	normalized.RecoveryAuthoritativeAppliedCommandLSN = 42
	rows := []columnDeclaredRow{
		{ID: []byte("e1"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 1},
			{Type: ColumnStoreValueString, Present: true, String: "share"},
			{Type: ColumnStoreValueString, Present: true, String: "did_a"},
		}},
		{ID: []byte("e2"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: -2},
			{Type: ColumnStoreValueString, Present: true, String: "like"},
			{Type: ColumnStoreValueString, Present: true, String: "did_b"},
		}},
		{ID: []byte("e3"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 3},
			{Type: ColumnStoreValueString, Present: true, String: "share"},
			{Type: ColumnStoreValueString, Present: true, String: "did_c"},
		}},
	}
	assets, err := buildColumnInt64ValuesAssets(*normalized, rows, "events", normalized.AssetManager.Namespace, 7, 3, 42)
	if err != nil {
		t.Fatalf("buildColumnInt64ValuesAssets: %v", err)
	}
	if len(assets) != 1 || assets[0].ColumnName != "time_us" {
		t.Fatalf("assets=%+v want one time_us int64 sidecar", assets)
	}
	raw, err := encodeColumnInt64ValuesAsset(assets[0])
	if err != nil {
		t.Fatalf("encodeColumnInt64ValuesAsset: %v", err)
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1Int64Values,
		Namespace:  normalized.AssetManager.Namespace,
		Generation: assets[0].Generation,
		PartID:     assets[0].PartID,
		Length:     int64(len(raw)),
		Checksum:   page.Checksum(raw),
	}
	decoded, err := decodeColumnInt64ValuesAsset(raw, ref, *normalized, "events", "time_us", true)
	if err != nil {
		t.Fatalf("decodeColumnInt64ValuesAsset: %v", err)
	}
	if got, want := decoded.Values, []int64{1, -2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("values=%v want %v", got, want)
	}

	truncated := append([]byte(nil), raw[:len(raw)-8]...)
	truncatedRef := ref
	truncatedRef.Length = int64(len(truncated))
	truncatedRef.Checksum = page.Checksum(truncated)
	if _, err := decodeColumnInt64ValuesAsset(truncated, truncatedRef, *normalized, "events", "time_us", true); err == nil || !strings.Contains(err.Error(), "row count exceeds payload bytes") {
		t.Fatalf("truncated payload err=%v want row-count payload validation failure", err)
	}

	corrupt := append([]byte(nil), raw...)
	corrupt[len(corrupt)-1] ^= 0xff
	if _, err := decodeColumnInt64ValuesAsset(corrupt, ref, *normalized, "events", "time_us", true); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("checksum corruption err=%v want checksum failure", err)
	}
	mismatchedChecksumRef := ref
	mismatchedChecksumRef.Checksum ^= 1
	if _, err := decodeColumnInt64ValuesAsset(raw, mismatchedChecksumRef, *normalized, "events", "time_us", false); err != nil {
		t.Fatalf("skip-checksum decode err=%v want valid raw bytes to decode despite ref checksum mismatch", err)
	}

	wrongColumnRef := ref
	if _, err := decodeColumnInt64ValuesAsset(raw, wrongColumnRef, *normalized, "events", "kind", true); err == nil || !strings.Contains(err.Error(), "column=") {
		t.Fatalf("wrong column err=%v want column validation failure", err)
	}

	futureLSNCfg := *normalized
	futureLSNCfg.RecoveryAuthoritativeAppliedCommandLSN = 41
	if _, err := decodeColumnInt64ValuesAsset(raw, ref, futureLSNCfg, "events", "time_us", true); err == nil || !strings.Contains(err.Error(), "newer than recovery") {
		t.Fatalf("future lsn err=%v want recovery-authoritative LSN failure", err)
	}
}

func TestColumnDictionaryCodesAssetCodecRejectsCorruptionM1634(t *testing.T) {
	normalized, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	normalized.RecoveryAuthoritativeAppliedCommandLSN = 42
	rows := []columnDeclaredRow{
		{ID: []byte("e1"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 1},
			{Type: ColumnStoreValueString, Present: true, String: "share"},
			{Type: ColumnStoreValueString, Present: true, String: "did_a"},
		}},
		{ID: []byte("e2"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 2},
			{Type: ColumnStoreValueString, Present: true, String: "like"},
			{Type: ColumnStoreValueString, Present: true, String: "did_b"},
		}},
		{ID: []byte("e3"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 3},
			{Type: ColumnStoreValueString, Present: true, String: "share"},
			{Type: ColumnStoreValueString, Present: true, String: "did_c"},
		}},
	}
	assets, err := buildColumnDictionaryCodesAssets(*normalized, rows, "events", normalized.AssetManager.Namespace, 7, 3, 42)
	if err != nil {
		t.Fatalf("buildColumnDictionaryCodesAssets: %v", err)
	}
	var kindAsset columnDictionaryCodesAsset
	for _, asset := range assets {
		if asset.ColumnName == "kind" {
			kindAsset = asset
			break
		}
	}
	if kindAsset.ColumnName == "" {
		t.Fatalf("missing kind dictionary asset: %+v", assets)
	}
	raw, err := encodeColumnDictionaryCodesAsset(kindAsset)
	if err != nil {
		t.Fatalf("encodeColumnDictionaryCodesAsset: %v", err)
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1DictionaryCodes,
		Namespace:  normalized.AssetManager.Namespace,
		Generation: kindAsset.Generation,
		PartID:     kindAsset.PartID,
		Length:     int64(len(raw)),
		Checksum:   page.Checksum(raw),
	}
	decoded, err := decodeColumnDictionaryCodesAsset(raw, ref, *normalized, "events", "kind", true)
	if err != nil {
		t.Fatalf("decodeColumnDictionaryCodesAsset: %v", err)
	}
	if got, want := decoded.Dictionary, []string{"share", "like"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("dictionary=%v want %v", got, want)
	}
	if got, want := decoded.Codes, []uint32{0, 1, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("codes=%v want %v", got, want)
	}

	corrupt := append([]byte(nil), raw...)
	corrupt[len(corrupt)-1] ^= 0xff
	if _, err := decodeColumnDictionaryCodesAsset(corrupt, ref, *normalized, "events", "kind", true); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("checksum corruption err=%v want checksum failure", err)
	}
	mismatchedChecksumRef := ref
	mismatchedChecksumRef.Checksum ^= 1
	if _, err := decodeColumnDictionaryCodesAsset(raw, mismatchedChecksumRef, *normalized, "events", "kind", false); err != nil {
		t.Fatalf("skip-checksum decode err=%v want valid raw bytes to decode despite ref checksum mismatch", err)
	}

	badCode := append([]byte(nil), raw...)
	badCode[len(badCode)-1] = 9
	badRef := ref
	badRef.Checksum = page.Checksum(badCode)
	if _, err := decodeColumnDictionaryCodesAsset(badCode, badRef, *normalized, "events", "kind", true); err == nil || !strings.Contains(err.Error(), "outside cardinality") {
		t.Fatalf("bad code err=%v want outside cardinality failure", err)
	}

	wrongColumnRef := ref
	if _, err := decodeColumnDictionaryCodesAsset(raw, wrongColumnRef, *normalized, "events", "did", true); err == nil || !strings.Contains(err.Error(), "column=") {
		t.Fatalf("wrong column err=%v want column validation failure", err)
	}

	futureLSNCfg := *normalized
	futureLSNCfg.RecoveryAuthoritativeAppliedCommandLSN = 41
	if _, err := decodeColumnDictionaryCodesAsset(raw, ref, futureLSNCfg, "events", "kind", true); err == nil || !strings.Contains(err.Error(), "newer than recovery") {
		t.Fatalf("future lsn err=%v want recovery-authoritative LSN failure", err)
	}
}

func TestColumnDictionaryCodeIndexRejectsCorruptWideCodeM1634(t *testing.T) {
	if idx, ok := columnDictionaryCodeIndex(2, 3); !ok || idx != 2 {
		t.Fatalf("valid code idx=%d ok=%v want idx=2 ok=true", idx, ok)
	}
	if _, ok := columnDictionaryCodeIndex(^uint32(0), 3); ok {
		t.Fatalf("max uint32 code unexpectedly accepted for cardinality 3")
	}
}

func TestColumnDictionaryCodeSnapshotRowsM1634(t *testing.T) {
	view := columnPhysicalScanSnapshotView{
		AssetRefs: []columnManifestAssetRefForScan{
			{Ref: ColumnAssetRef{Generation: 2, PartID: 1}, Rows: 3},
			{Ref: ColumnAssetRef{Generation: 2, PartID: 2}, Rows: 5},
		},
	}
	byPart := map[[2]uint64]columnManifestDictionaryCodesSnapshot{
		{2, 1}: {AssetRef: ColumnAssetRef{Generation: 2, PartID: 1}},
		{2, 2}: {AssetRef: ColumnAssetRef{Generation: 2, PartID: 2}},
	}
	if rows, ok := columnDictionaryCodeSnapshotRows(view, byPart); rows != 8 || !ok {
		t.Fatalf("rows=%d ok=%v want 8/true", rows, ok)
	}
	delete(byPart, [2]uint64{2, 2})
	if rows, ok := columnDictionaryCodeSnapshotRows(view, byPart); rows != 0 || ok {
		t.Fatalf("missing part rows=%d ok=%v want 0/false", rows, ok)
	}
	byPart[[2]uint64{2, 2}] = columnManifestDictionaryCodesSnapshot{AssetRef: ColumnAssetRef{Generation: 2, PartID: 2}}
	view.AssetRefs[0].Rows = maxCollectionInt
	view.AssetRefs[1].Rows = 1
	if rows, ok := columnDictionaryCodeSnapshotRows(view, byPart); rows != 0 || ok {
		t.Fatalf("overflow rows=%d ok=%v want 0/false", rows, ok)
	}
}

func TestColumnDictionaryCodePreparedRunnersFallbackOnMixedSidecarCoverageM1634(t *testing.T) {
	normalized, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	ns := normalized.AssetManager.Namespace
	view := columnPhysicalScanSnapshotView{
		CollectionName: "events",
		Config:         *normalized,
		AssetRefs: []columnManifestAssetRefForScan{
			{Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: ns, Generation: 2, PartID: 1}, Reason: ColumnPublishOperationInsert, Rows: 3},
			{Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: ns, Generation: 2, PartID: 2}, Reason: ColumnPublishOperationInsert, Rows: 5},
		},
		DictionaryCodes: []columnManifestDictionaryCodesSnapshot{
			{ColumnName: "kind", AssetRef: ColumnAssetRef{Kind: ColumnAssetKindTCS1DictionaryCodes, Namespace: ns, Generation: 2, PartID: 1}},
			{ColumnName: "did", AssetRef: ColumnAssetRef{Kind: ColumnAssetKindTCS1DictionaryCodes, Namespace: ns, Generation: 2, PartID: 1}},
		},
	}
	readCache := &columnPhysicalAssetReadCache{namespace: ns}
	if runner, err := prepareColumnDictionaryCodeGroupCountRunner(view, ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}, readCache); err != nil || runner != nil {
		t.Fatalf("group-count runner=%T err=%v want clean fallback", runner, err)
	}
	if runner, err := prepareColumnDictionaryCodeGroupCountDistinctRunner(view, ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"}, readCache); err != nil || runner != nil {
		t.Fatalf("group-count-distinct runner=%T err=%v want clean fallback", runner, err)
	}
}

func TestColumnDictionaryCodePreparedRunnersRejectManifestRowMismatchM1634(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(2048)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()

	view, closeView, err := collection.prepareColumnPhysicalScanSnapshotView()
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	if len(view.AssetRefs) == 0 {
		t.Fatal("fixture produced no physical asset refs")
	}
	manifestRows := view.AssetRefs[0].Rows
	view.AssetRefs[0].Rows++
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, ColumnAssetReadIntegritySkipChecksums)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity: %v", err)
	}
	defer func() {
		if err := readCache.close(); err != nil {
			t.Fatalf("read cache close: %v", err)
		}
	}()
	if _, err := prepareColumnDictionaryCodeGroupCountRunner(view, ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}, &readCache); err == nil || !strings.Contains(err.Error(), "want manifest rows") {
		t.Fatalf("group-count err=%v want manifest row mismatch", err)
	}
	if _, err := prepareColumnDictionaryCodeGroupCountDistinctRunner(view, ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"}, &readCache); err == nil || !strings.Contains(err.Error(), "want manifest rows") {
		t.Fatalf("group-count-distinct err=%v want manifest row mismatch", err)
	}

	view.AssetRefs[0].Rows = manifestRows
	part := view.AssetRefs[0]
	partKey := [2]uint64{part.Ref.Generation, part.Ref.PartID}
	distinctSnapshot, ok := columnDictionaryCodeSnapshotsByPart(view, "did")[partKey]
	if !ok {
		t.Fatalf("missing distinct dictionary snapshot for part=%v", partKey)
	}
	distinctRaw, err := readCache.read(distinctSnapshot.AssetRef, nil)
	if err != nil {
		t.Fatalf("read distinct dictionary sidecar: %v", err)
	}
	distinctAsset, err := decodeColumnDictionaryCodesAsset(distinctRaw, distinctSnapshot.AssetRef, view.Config, view.CollectionName, "did", false)
	if err != nil {
		t.Fatalf("decode distinct dictionary sidecar: %v", err)
	}
	if len(distinctAsset.Dictionary) == 0 {
		t.Fatal("distinct dictionary sidecar has empty dictionary")
	}
	distinctAsset.Codes = append(distinctAsset.Codes, 0)
	encodedDistinct, err := encodeColumnDictionaryCodesAsset(distinctAsset)
	if err != nil {
		t.Fatalf("encode mismatched distinct dictionary sidecar: %v", err)
	}
	mismatchedDistinctRef, err := writeColumnDictionaryCodesAssetToManager(view.ColumnAssetRootDir, view.Config, encodedDistinct, distinctAsset.Generation, distinctAsset.PartID)
	if err != nil {
		t.Fatalf("write mismatched distinct dictionary sidecar: %v", err)
	}
	updatedDistinct := false
	for i := range view.DictionaryCodes {
		snapshot := &view.DictionaryCodes[i]
		if snapshot.ColumnName == "did" && snapshot.AssetRef.Generation == part.Ref.Generation && snapshot.AssetRef.PartID == part.Ref.PartID {
			snapshot.AssetRef = mismatchedDistinctRef
			updatedDistinct = true
			break
		}
	}
	if !updatedDistinct {
		t.Fatalf("failed to replace distinct dictionary snapshot for part=%v", partKey)
	}
	viewReadCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, ColumnAssetReadIntegritySkipChecksums)
	if err != nil {
		t.Fatalf("new view column asset read cache: %v", err)
	}
	viewReadCache.returnViews = true
	defer func() {
		if err := viewReadCache.close(); err != nil {
			t.Fatalf("view read cache close: %v", err)
		}
	}()
	if _, ok, err := runColumnDictionaryCodeGroupCountDistinctOneShot(view, ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"}, &viewReadCache); err == nil {
		if !ok {
			t.Skip("column asset mmap views are unavailable on this platform")
		}
		t.Fatalf("group-count-distinct one-shot ok=%v err=%v want distinct manifest row mismatch", ok, err)
	} else if !strings.Contains(err.Error(), "distinct dictionary codes asset row count") {
		t.Fatalf("group-count-distinct one-shot ok=%v err=%v want distinct manifest row mismatch", ok, err)
	}
}

func TestColumnDictionaryCodeDistinctSeenWordsRejectsOverflowM1634(t *testing.T) {
	wordsPerGroup, totalWords, ok, err := columnDictionaryCodeDistinctSeenWords(4, 129)
	if err != nil {
		t.Fatalf("columnDictionaryCodeDistinctSeenWords normal: %v", err)
	}
	if !ok {
		t.Fatal("columnDictionaryCodeDistinctSeenWords normal ok=false want true")
	}
	if wordsPerGroup != 3 || totalWords != 12 {
		t.Fatalf("wordsPerGroup=%d totalWords=%d want 3/12", wordsPerGroup, totalWords)
	}
	if _, _, ok, err := columnDictionaryCodeDistinctSeenWords(0, 1); err != nil || ok {
		t.Fatalf("empty group ok=%v err=%v want clean fallback", ok, err)
	}
	if _, _, ok, err := columnDictionaryCodeDistinctSeenWords(1, 0); err != nil || ok {
		t.Fatalf("empty distinct ok=%v err=%v want clean fallback", ok, err)
	}
	if _, _, ok, err := columnDictionaryCodeDistinctSeenWords(maxCollectionInt, 65); err != nil || ok {
		t.Fatalf("overflow ok=%v err=%v want clean fallback", ok, err)
	}
	if _, _, ok, err := columnDictionaryCodeDistinctSeenWords(columnDictionaryCodeDistinctMaxSeenWords+1, 1); err != nil || ok {
		t.Fatalf("oversized bitset ok=%v err=%v want clean fallback", ok, err)
	}
}

func columnPhysicalQueryTCPAAssetBytesM1634(tb testing.TB, collection *Collection) int64 {
	tb.Helper()
	view, closeView, err := collection.prepareColumnPhysicalScanSnapshotView()
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		tb.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	var bytes int64
	for _, ref := range view.AssetRefs {
		bytes += ref.Ref.Length
	}
	if bytes <= 0 {
		tb.Fatalf("TCPA asset bytes=%d want positive", bytes)
	}
	return bytes
}

func TestColumnPhysicalQueryRunnerFailsClosedForUnsupportedShapeM1634(t *testing.T) {
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, columnPhysicalQueryFixtureEventsM13B(128))
	defer closeFn()
	if _, err := collection.PrepareColumnPhysicalQuery(ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupCount,
		GroupColumn: "payload",
	}); !errors.Is(err, ErrColumnQueryPlanUnsupported) {
		t.Fatalf("PrepareColumnPhysicalQuery unsupported err=%v want ErrColumnQueryPlanUnsupported", err)
	}

	mutated, closeMutated, _ := openColumnPhysicalMutationFixtureM13C(t, 128)
	defer closeMutated()
	if _, err := mutated.PrepareColumnPhysicalQuery(ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupCount,
		GroupColumn: "kind",
	}); !errors.Is(err, ErrColumnQueryPlanUnsupported) {
		t.Fatalf("PrepareColumnPhysicalQuery mutation err=%v want ErrColumnQueryPlanUnsupported", err)
	}
}

func TestColumnPhysicalQueryGroupSortHybridM1634(t *testing.T) {
	groups := make([]ColumnPhysicalQueryGroup, 96)
	for i := range groups {
		groups[i] = ColumnPhysicalQueryGroup{Key: fmt.Sprintf("key_%03d", len(groups)-i)}
	}
	sortColumnPhysicalQueryGroupsByKey(groups)
	for i := 1; i < len(groups); i++ {
		if groups[i-1].Key > groups[i].Key {
			t.Fatalf("groups not sorted at %d: %q > %q", i, groups[i-1].Key, groups[i].Key)
		}
	}
}

func TestColumnPhysicalQueryExecutorResetClearsDistinctGroupsM1634(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	exec, err := newColumnPhysicalQueryExecutor(*cfg, ColumnPhysicalQueryRequest{
		Kind:           ColumnPhysicalQueryGroupCountDistinct,
		GroupColumn:    "kind",
		DistinctColumn: "did",
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalQueryExecutor: %v", err)
	}
	if err := exec.visitDirectGroupCountDistinct([]byte("kind_a"), true, []byte("did_a"), true); err != nil {
		t.Fatalf("visit first distinct: %v", err)
	}
	if got := exec.groups(); len(got) != 1 || got[0].Key != "kind_a" || got[0].Count != 1 {
		t.Fatalf("first groups=%+v want kind_a=1", got)
	}
	exec.resetForRun()
	if err := exec.visitDirectGroupCountDistinct([]byte("kind_b"), true, []byte("did_b"), true); err != nil {
		t.Fatalf("visit second distinct: %v", err)
	}
	got := exec.groups()
	if len(got) != 1 || got[0].Key != "kind_b" || got[0].Count != 1 {
		t.Fatalf("second groups=%+v want only kind_b=1", got)
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
			{name: "q4b_metadata", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "max_time_us"}},
			{name: "q5", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"}},
			{name: "q5_metadata", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"}},
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

func BenchmarkColumnPhysicalQueryRunnerM1634(b *testing.B) {
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
			{name: "q4b_metadata", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "max_time_us"}},
			{name: "q5", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"}},
			{name: "q5_metadata", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"}},
		}
		for _, tc := range cases {
			b.Run(fmt.Sprintf("%s_rows_%d", tc.name, rows), func(b *testing.B) {
				collection, closeFn := openColumnPhysicalQueryFixtureM13B(b, columnPhysicalQueryFixtureEventsM13B(rows))
				defer closeFn()
				runner, err := collection.PrepareColumnPhysicalQuery(tc.req)
				if err != nil {
					b.Fatalf("PrepareColumnPhysicalQuery: %v", err)
				}
				defer func() { _ = runner.Close() }()
				preview, err := runner.Run()
				if err != nil {
					b.Fatalf("preview runner Run: %v", err)
				}
				b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
				b.ReportAllocs()
				b.ResetTimer()
				var reducedRows int64
				for i := 0; i < b.N; i++ {
					result, err := runner.Run()
					if err != nil {
						b.Fatalf("runner Run: %v", err)
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
