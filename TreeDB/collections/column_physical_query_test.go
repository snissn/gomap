package collections

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestColumnPhysicalQueryAdapterExecutesJSONBenchShapesM13B(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(96)
	reopened, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()

	tests := []struct {
		name      string
		hashName  string
		req       ColumnPhysicalQueryRequest
		wantCount int
	}{
		{
			name:      "q1",
			hashName:  "q1",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"},
			wantCount: 4,
		},
		{
			name:      "q2",
			hashName:  "q2",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"},
			wantCount: 4,
		},
		{
			name:      "q3",
			hashName:  "q3",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us"},
			wantCount: 24,
		},
		{
			name:      "q4a",
			hashName:  "q4a",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us"},
			wantCount: 12,
		},
		{
			name:      "q4b",
			hashName:  "q4b",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us"},
			wantCount: 12,
		},
		{
			name:      "q5",
			hashName:  "q5",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"},
			wantCount: 12,
		},
		{
			name:      "q5_metadata",
			hashName:  "q5",
			req:       ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"},
			wantCount: 12,
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
			if result.Diagnostics.AssetRefs == 0 || result.Diagnostics.DecodedBlocks == 0 || result.Diagnostics.PhysicalBytesScanned <= 0 {
				t.Fatalf("%s missing physical diagnostics: %+v", tc.name, result.Diagnostics)
			}
			if result.Diagnostics.ReduceRows != len(events) {
				t.Fatalf("%s reduce rows=%d want %d", tc.name, result.Diagnostics.ReduceRows, len(events))
			}
		})
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

func TestColumnPhysicalQueryAdapterFailsClosedOnMutationRowsM13B(t *testing.T) {
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
		return []byte(`{"time_us":3,"kind":"like","did":"d1"}`), true, nil
	}); err != nil {
		_ = d.Close()
		t.Fatalf("Update: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	_, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupCount,
		GroupColumn: "kind",
	})
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "mutation visibility") {
		t.Fatalf("mutation query err=%v want fail-closed mutation visibility unsupported", err)
	}
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
	if largeAllocs > smallAllocs+64 {
		t.Fatalf("allocation slope looks row-linear: small=%.0f large=%.0f", smallAllocs, largeAllocs)
	}
}

func BenchmarkColumnPhysicalQueryAdapterM13B(b *testing.B) {
	for _, rows := range []int{1024, 8192} {
		b.Run(fmt.Sprintf("q1_rows_%d", rows), func(b *testing.B) {
			collection, closeFn := openColumnPhysicalQueryFixtureM13B(b, columnPhysicalQueryFixtureEventsM13B(rows))
			defer closeFn()
			req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}
			preview, err := collection.RunColumnPhysicalQuery(req)
			if err != nil {
				b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
			}
			b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
			b.ReportAllocs()
			b.ResetTimer()
			var reducedRows int64
			for i := 0; i < b.N; i++ {
				result, err := collection.RunColumnPhysicalQuery(req)
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
		b.Run(fmt.Sprintf("q5_rows_%d", rows), func(b *testing.B) {
			collection, closeFn := openColumnPhysicalQueryFixtureM13B(b, columnPhysicalQueryFixtureEventsM13B(rows))
			defer closeFn()
			req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"}
			preview, err := collection.RunColumnPhysicalQuery(req)
			if err != nil {
				b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
			}
			b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
			b.ReportAllocs()
			b.ResetTimer()
			var reducedRows int64
			for i := 0; i < b.N; i++ {
				result, err := collection.RunColumnPhysicalQuery(req)
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
