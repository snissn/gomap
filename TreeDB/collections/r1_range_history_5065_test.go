package collections

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

// Keep this public-range diagnostic fixture identical on the pre-fix and
// post-fix sources. Historical parts are retained deliberately, without GC.
func r1RangeHistoryFixture5065(tb testing.TB) (*Collection, [][]byte, [][]byte, int) {
	tb.Helper()
	_, d, col := openTypedMinimaCollectionMeta(tb, r1ReadCollectionMeta())
	tb.Cleanup(func() { _ = d.Close() })
	const liveRows = 32
	ids := make([][]byte, liveRows)
	retained := make([][]byte, liveRows)
	want := make([][]byte, liveRows)
	content, user := make([]string, liveRows), make([]string, liveRows)
	set := func(i, revision int) {
		ids[i] = []byte(fmt.Sprintf("row-%02d", i))
		optional := ""
		if i%2 == 0 {
			optional = `,"optional":null`
		}
		retained[i] = []byte(fmt.Sprintf(`{"id":"row-%02d","revision":%d%s}`, i, revision, optional))
		content[i], user[i] = fmt.Sprintf("complete-%02d-%d", i, revision), "u1"
		want[i] = []byte(fmt.Sprintf(`{"content":%q,"user":"u1","id":"row-%02d","revision":%d%s}`, content[i], i, revision, optional))
	}
	for i := range ids {
		set(i, 0)
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, r1ReadColumns(content, user)); err != nil {
		tb.Fatal(err)
	}
	for revision := 1; revision <= 127; revision++ {
		i := revision % liveRows
		set(i, revision)
		if n, err := col.UpsertTypedBatch(ids[i:i+1], retained[i:i+1], r1ReadColumns(content[i:i+1], user[i:i+1])); err != nil || n != 1 {
			tb.Fatalf("history upsert %d: n=%d err=%v", revision, n, err)
		}
	}
	if err := col.Flush(); err != nil {
		tb.Fatal(err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		tb.Fatal(err)
	}
	physical, err := view.materializerColumnSnapshotView(*view.catalog.meta.Options.ColumnStore)
	if err != nil {
		tb.Fatal(err)
	}
	parts := len(physical.AssetRefs)
	if err := view.Close(); err != nil {
		tb.Fatal(err)
	}
	if parts < 128 {
		tb.Fatalf("retained parts=%d want at least128", parts)
	}
	return col, ids, want, parts
}

func r1RangeHistoryOptions5065(limit int) IndexRangeOptions {
	return IndexRangeOptions{Lower: IndexRangeBound{Value: "u1", Inclusive: true}, Upper: IndexRangeBound{Value: "u1", Inclusive: true}, Limit: limit}
}

func TestR1TypedRowBoundedRangeHistory5065(t *testing.T) {
	col, ids, want, _ := r1RangeHistoryFixture5065(t)
	for _, limit := range []int{1, 32, 0} {
		got, cut, err := col.FindDocumentsByIndexRange("user", r1RangeHistoryOptions5065(limit))
		n := len(ids)
		if limit > 0 && limit < n {
			n = limit
		}
		if err != nil || cut != (n < len(ids)) || len(got) != n {
			t.Fatalf("limit%d: rows=%d cut=%t err=%v", limit, len(got), cut, err)
		}
		for i := range got {
			if !bytes.Equal(got[i].ID, ids[i]) {
				t.Fatalf("limit%d row%d ID=%q want=%q", limit, i, got[i].ID, ids[i])
			}
			r1RequireCompleteRow(t, got[i].Document, want[i])
		}
	}
	missing, cut, err := col.FindDocumentsByIndexRange("absent", r1RangeHistoryOptions5065(1))
	if err != nil || cut || missing != nil {
		t.Fatalf("missing index: rows=%v cut=%t err=%v", missing, cut, err)
	}
	got, cut, err := col.FindDocumentsByIndexRange("user", IndexRangeOptions{Lower: IndexRangeBound{Value: "no-match", Inclusive: true}, Upper: IndexRangeBound{Value: "no-match", Inclusive: true}, Limit: 1})
	if err != nil || cut || len(got) != 0 {
		t.Fatalf("missing rows: rows=%v cut=%t err=%v", got, cut, err)
	}
}

func BenchmarkR1TypedRowBoundedRangeHistory5065(b *testing.B) {
	col, ids, want, parts := r1RangeHistoryFixture5065(b)
	for _, limit := range []int{1, 32} {
		b.Run(fmt.Sprintf("limit%d", limit), func(b *testing.B) {
			opts := r1RangeHistoryOptions5065(limit)
			b.ReportAllocs()
			b.ReportMetric(float64(parts), "retained-parts")
			b.ReportMetric(float64(limit), "rows/op")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, cut, err := col.FindDocumentsByIndexRange("user", opts)
				if err != nil || cut != (limit < len(ids)) || len(got) != limit {
					b.Fatalf("rows=%d cut=%t err=%v", len(got), cut, err)
				}
			}
			b.StopTimer()
			got, _, err := col.FindDocumentsByIndexRange("user", opts)
			if err != nil {
				b.Fatal(err)
			}
			for i := range got {
				if !bytes.Equal(got[i].ID, ids[i]) {
					b.Fatalf("row%d ID=%q want=%q", i, got[i].ID, ids[i])
				}
				// Run the full-row oracle outside the diagnostic timer.
				var actual, expected any
				if err := json.Unmarshal(got[i].Document, &actual); err != nil {
					b.Fatal(err)
				}
				if err := json.Unmarshal(want[i], &expected); err != nil {
					b.Fatal(err)
				}
				if !reflect.DeepEqual(actual, expected) {
					b.Fatalf("row%d got=%s want=%s", i, got[i].Document, want[i])
				}
			}
		})
	}
}
