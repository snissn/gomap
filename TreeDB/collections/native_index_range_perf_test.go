package collections

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type nativeIndexRangeFixtureOpts struct {
	outerLeavesDict bool
	pointerThresh   int
	cowLimits       *treedb.COWMemtableLimits
	indexes         []IndexDefinition
}

func openNativeIndexRangeFixture(tb testing.TB, cfg nativeIndexRangeFixtureOpts) (*treedb.DB, *CollectionManager, *Collection) {
	tb.Helper()
	if cfg.outerLeavesDict && runtime.GOOS == "windows" {
		tb.Skip("outer-leaf dictionary value-log test skipped on windows")
	}
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, tb.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.LeafPrefixCompression = true
	opts.IndexColumnarLeaves = true
	if cfg.outerLeavesDict {
		opts.IndexOuterLeavesInValueLog = true
		opts.ValueLog.Compression = treedb.ValueLogCompressionDict
	}
	if cfg.pointerThresh > 0 {
		opts.ValueLog.PointerThreshold = cfg.pointerThresh
	}
	if cfg.cowLimits != nil {
		opts.COWMemtableLimits = *cfg.cowLimits
	} else {
		opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
	}
	owner, err := treedb.Open(opts)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := owner.Close(); err != nil {
			tb.Error(err)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		tb.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	tb.Cleanup(func() {
		if err := manager.Close(); err != nil {
			tb.Error(err)
		}
	})
	indexes := cfg.indexes
	if len(indexes) == 0 {
		indexes = []IndexDefinition{
			{Name: "age", Field: "age", ValueType: IndexValueBSONOrderedV2},
			{Name: "age_i64", Field: "age", ValueType: IndexValueInt64},
			{Name: "tags", Field: "tags", ValueType: IndexValueBSONOrderedV2, MultiKey: true},
		}
	}
	meta := &CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: indexes,
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		tb.Fatal(err)
	}
	col, err := manager.OpenCollection("users")
	if err != nil {
		tb.Fatal(err)
	}
	return owner, manager, col
}

func makeNativeRangeBSONDoc(tb testing.TB, age int64, tags []string, pad string) []byte {
	tb.Helper()
	tagArr := make(bson.A, len(tags))
	for i, tag := range tags {
		tagArr[i] = tag
	}
	doc, err := bson.Marshal(bson.D{
		{Key: "age", Value: age},
		{Key: "tags", Value: tagArr},
		{Key: "pad", Value: pad},
	})
	if err != nil {
		tb.Fatal(err)
	}
	return doc
}

func bsonRawInt64(tb testing.TB, v int64) bson.RawValue {
	tb.Helper()
	t, data, err := bson.MarshalValue(v)
	if err != nil {
		tb.Fatal(err)
	}
	return bson.RawValue{Type: t, Value: data}
}

func idsToStrings(raw [][]byte) []string {
	out := make([]string, len(raw))
	for i, id := range raw {
		out[i] = string(id)
	}
	return out
}

func docRecordIDs(recs []DocumentRecord) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r.ID)
	}
	return out
}

func TestScanIndexRangeWarmLaneParity(t *testing.T) {
	modes := []struct {
		name            string
		outerLeavesDict bool
	}{
		{name: "inline_leaves", outerLeavesDict: false},
		{name: "outer_leaves_dict", outerLeavesDict: true},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			if mode.outerLeavesDict && runtime.GOOS == "windows" {
				t.Skip("outer-leaf dictionary value-log test skipped on windows")
			}
			owner, _, col := openNativeIndexRangeFixture(t, nativeIndexRangeFixtureOpts{
				outerLeavesDict: mode.outerLeavesDict,
				pointerThresh:   64,
			})
			pad := string(bytes.Repeat([]byte("r"), 128))
			const total = 320
			ids := make([][]byte, total)
			docs := make([][]byte, total)
			docByID := make(map[string][]byte, total)
			for i := 0; i < total; i++ {
				id := fmt.Sprintf("u%04d", i)
				if i == 25 {
					// Oversized ID (> 64 bytes) to exercise dynamic collector admission.
					id = fmt.Sprintf("u%04d-%s", i, string(bytes.Repeat([]byte("k"), 96)))
				}
				ids[i] = []byte(id)
				// Every doc gets two tags in ["t010", "t090"] range to test dedupe.
				tags := []string{
					fmt.Sprintf("t%03d", i%100),
					fmt.Sprintf("t%03d", (i+1)%100),
				}
				docs[i] = makeNativeRangeBSONDoc(t, int64(i), tags, pad)
				docByID[id] = docs[i]
			}
			if _, err := col.InsertBatch(ids, docs); err != nil {
				t.Fatal(err)
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}

			// Create physical tombstones by deleting and checkpointing so
			// NativeScanLiveKeysAtRoot must skip physical tombstones too.
			deletedSet := map[int]bool{5: true, 12: true, 19: true, 260: true}
			for idx := range deletedSet {
				if err := col.Delete(ids[idx]); err != nil {
					t.Fatal(err)
				}
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}

			liveInRange := func(lo, hi int, loInc, hiInc, loUnbounded, hiUnbounded bool) []string {
				var out []string
				for i := 0; i < total; i++ {
					if deletedSet[i] {
						continue
					}
					if !loUnbounded {
						if loInc && i < lo {
							continue
						}
						if !loInc && i <= lo {
							continue
						}
					}
					if !hiUnbounded {
						if hiInc && i > hi {
							continue
						}
						if !hiInc && i >= hi {
							continue
						}
					}
					out = append(out, string(ids[i]))
				}
				return out
			}

			// Test bounds, inclusivity, empty range, and Limit values: 1, 10, 256, 257 (fallback), exact count.
			rangeCases := []struct {
				name        string
				lo, hi      int
				loInc       bool
				hiInc       bool
				loUnbounded bool
				hiUnbounded bool
				limit       int
			}{
				{name: "inclusive_limit1", lo: 10, hi: 50, loInc: true, hiInc: true, limit: 1},
				{name: "inclusive_limit10", lo: 0, hi: 50, loInc: true, hiInc: true, limit: 10},
				{name: "exclusive_bounds_limit10", lo: 10, hi: 30, loInc: false, hiInc: false, limit: 10},
				{name: "lower_only_limit10", lo: 300, loInc: true, hiUnbounded: true, limit: 10},
				{name: "upper_only_limit10", hi: 15, hiInc: false, loUnbounded: true, limit: 10},
				{name: "unbounded_limit10", loUnbounded: true, hiUnbounded: true, limit: 10},
				{name: "limit256_warm_boundary", lo: 0, hi: 319, loInc: true, hiInc: true, limit: 256},
				{name: "limit257_fallback_boundary", lo: 0, hi: 319, loInc: true, hiInc: true, limit: 257},
				{name: "exact_count_no_truncation", lo: 30, hi: 39, loInc: true, hiInc: true, limit: 10},
				{name: "exact_count_plus_one_truncated", lo: 30, hi: 40, loInc: true, hiInc: true, limit: 10},
				{name: "oversized_id_in_window", lo: 20, hi: 35, loInc: true, hiInc: true, limit: 10},
				{name: "empty_no_match", lo: 500, hi: 600, loInc: true, hiInc: true, limit: 10},
				{name: "empty_inverted", lo: 50, hi: 10, loInc: true, hiInc: true, limit: 10},
				{name: "empty_exclusive_equal", lo: 10, hi: 10, loInc: false, hiInc: false, limit: 10},
			}

			for _, tc := range rangeCases {
				t.Run(tc.name, func(t *testing.T) {
					wantAll := liveInRange(tc.lo, tc.hi, tc.loInc, tc.hiInc, tc.loUnbounded, tc.hiUnbounded)
					wantTrunc := len(wantAll) > tc.limit
					wantIDs := wantAll
					if len(wantIDs) > tc.limit {
						wantIDs = wantAll[:tc.limit]
					}
					if wantIDs == nil {
						wantIDs = []string{}
					}

					// Check IndexValueBSONOrderedV2 ("age") and IndexValueInt64 ("age_i64").
					for _, idxName := range []string{"age", "age_i64"} {
						makeBound := func(v int, inc, unbounded bool) IndexRangeBound {
							if unbounded {
								return IndexRangeBound{Unbounded: true}
							}
							if idxName == "age" {
								return IndexRangeBound{Value: bsonRawInt64(t, int64(v)), Inclusive: inc}
							}
							return IndexRangeBound{Value: int64(v), Inclusive: inc}
						}
						opts := IndexRangeOptions{
							Lower: makeBound(tc.lo, tc.loInc, tc.loUnbounded),
							Upper: makeBound(tc.hi, tc.hiInc, tc.hiUnbounded),
							Limit: tc.limit,
						}
						gotRaw, trunc, err := col.FindByIndexRange(idxName, opts)
						if err != nil {
							t.Fatalf("%s FindByIndexRange: %v", idxName, err)
						}
						if trunc != wantTrunc || !reflect.DeepEqual(idsToStrings(gotRaw), wantIDs) {
							t.Fatalf("%s FindByIndexRange: got=%v trunc=%v want=%v trunc=%v", idxName, idsToStrings(gotRaw), trunc, wantIDs, wantTrunc)
						}

						recs, docTrunc, err := col.FindDocumentsByIndexRange(idxName, opts)
						if err != nil {
							t.Fatalf("%s FindDocumentsByIndexRange: %v", idxName, err)
						}
						if docTrunc != wantTrunc || !reflect.DeepEqual(docRecordIDs(recs), wantIDs) {
							t.Fatalf("%s FindDocumentsByIndexRange: got=%v trunc=%v want=%v trunc=%v", idxName, docRecordIDs(recs), docTrunc, wantIDs, wantTrunc)
						}
						for _, rec := range recs {
							if !bytes.Equal(rec.Document, docByID[string(rec.ID)]) {
								t.Fatalf("%s doc mismatch for %q", idxName, rec.ID)
							}
						}
					}
				})
			}

			// Verify MultiKey dedupe parity against unbounded (fallback) scan.
			dedupeOptsAll := IndexRangeOptions{
				Lower: IndexRangeBound{Value: bsonRawString(t, "t010"), Inclusive: true},
				Upper: IndexRangeBound{Value: bsonRawString(t, "t025"), Inclusive: true},
				Limit: 0, // unbounded -> uses existing cursor path as oracle
			}
			oracleIDs, oracleTrunc, err := col.FindByIndexRange("tags", dedupeOptsAll)
			if err != nil || oracleTrunc || len(oracleIDs) <= 20 {
				t.Fatalf("dedupe oracle: len=%d trunc=%v err=%v", len(oracleIDs), oracleTrunc, err)
			}
			oracleStr := idsToStrings(oracleIDs)
			for _, lim := range []int{1, 10, len(oracleStr) - 1, len(oracleStr), len(oracleStr) + 5} {
				opts := dedupeOptsAll
				opts.Limit = lim
				wantTrunc := len(oracleStr) > lim
				want := oracleStr
				if len(want) > lim {
					want = oracleStr[:lim]
				}
				got, trunc, err := col.FindByIndexRange("tags", opts)
				if err != nil || trunc != wantTrunc || !reflect.DeepEqual(idsToStrings(got), want) {
					t.Fatalf("dedupe IDs limit=%d: got=%v trunc=%v want=%v trunc=%v err=%v", lim, idsToStrings(got), trunc, want, wantTrunc, err)
				}
				recs, docTrunc, err := col.FindDocumentsByIndexRange("tags", opts)
				if err != nil || docTrunc != wantTrunc || !reflect.DeepEqual(docRecordIDs(recs), want) {
					t.Fatalf("dedupe Docs limit=%d: got=%v trunc=%v want=%v trunc=%v err=%v", lim, docRecordIDs(recs), docTrunc, want, wantTrunc, err)
				}
			}

			// Early visitor stop (cont = false) must stop cleanly with truncated = false.
			for _, documents := range []bool{false, true} {
				var visited []string
				read, err := col.nativeJSON.Capture(col.name)
				if err != nil {
					t.Fatal(err)
				}
				trunc, found, err := read.scanIndexRange("age", IndexRangeOptions{
					Lower: IndexRangeBound{Value: bsonRawInt64(t, 0), Inclusive: true},
					Upper: IndexRangeBound{Value: bsonRawInt64(t, 50), Inclusive: true},
					Limit: 10,
				}, documents, func(id, doc []byte) (bool, error) {
					visited = append(visited, string(id))
					if documents && !bytes.Equal(doc, docByID[string(id)]) {
						return false, fmt.Errorf("doc mismatch for %q", id)
					}
					return len(visited) < 3, nil
				})
				if err := read.Close(); err != nil {
					t.Fatal(err)
				}
				want := liveInRange(0, 50, true, true, false, false)[:3]
				if err != nil || !found || trunc || !reflect.DeepEqual(visited, want) {
					t.Fatalf("early stop documents=%v: visited=%v trunc=%v found=%v err=%v want=%v", documents, visited, trunc, found, err, want)
				}
			}

			// Reentrant Close inside visit:
			// (a) cont == true on item 1 of Limit=10 -> ErrClosed, no deadlock
			// (b) cont == true on sole item of Limit=1 -> ErrClosed (exact parity with loop re-check)
			// (c) cont == false on item 1 -> nil error, read closed by defer
			for _, tc := range []struct {
				name      string
				limit     int
				documents bool
				cont      bool
				wantErr   error
			}{
				{name: "cont_true_docs_limit10", limit: 10, documents: true, cont: true, wantErr: backenddb.ErrClosed},
				{name: "cont_true_ids_limit10", limit: 10, documents: false, cont: true, wantErr: backenddb.ErrClosed},
				{name: "cont_true_limit1_last_item", limit: 1, documents: true, cont: true, wantErr: backenddb.ErrClosed},
				{name: "cont_false_docs_limit10", limit: 10, documents: true, cont: false, wantErr: nil},
			} {
				t.Run("reentrant_close_"+tc.name, func(t *testing.T) {
					rr, err := col.nativeJSON.Capture(col.name)
					if err != nil {
						t.Fatal(err)
					}
					calls := 0
					_, found, err := rr.scanIndexRange("age", IndexRangeOptions{
						Lower: IndexRangeBound{Value: bsonRawInt64(t, 0), Inclusive: true},
						Upper: IndexRangeBound{Value: bsonRawInt64(t, 50), Inclusive: true},
						Limit: tc.limit,
					}, tc.documents, func(id, doc []byte) (bool, error) {
						calls++
						if closeErr := rr.Close(); closeErr != nil {
							return false, closeErr
						}
						if _, getErr := rr.Get(id); !errors.Is(getErr, backenddb.ErrClosed) {
							return false, fmt.Errorf("expected ErrClosed after reentrant Close, got %v", getErr)
						}
						return tc.cont, nil
					})
					if !found || calls != 1 || rr.capture != nil {
						t.Fatalf("calls=%d found=%v captureOpen=%v err=%v", calls, found, rr.capture != nil, err)
					}
					if tc.wantErr != nil {
						if !errors.Is(err, tc.wantErr) {
							t.Fatalf("err=%v want %v", err, tc.wantErr)
						}
					} else if err != nil {
						t.Fatalf("unexpected err=%v", err)
					}
				})
			}

			// Capture settled pre-overlay cut, then apply pending overlay mutations
			// (no checkpoint) and verify overlay fallback vs pre-overlay isolation.
			preOverlay, err := col.nativeJSON.Capture(col.name)
			if err != nil {
				t.Fatal(err)
			}
			defer preOverlay.Close()

			if err := col.Delete(ids[2]); err != nil {
				t.Fatal(err)
			}
			newDoc := makeNativeRangeBSONDoc(t, 3, []string{"t003"}, pad)
			if _, err := col.Insert([]byte("u0003a"), newDoc); err != nil {
				t.Fatal(err)
			}

			overlayOpts := IndexRangeOptions{
				Lower: IndexRangeBound{Value: bsonRawInt64(t, 0), Inclusive: true},
				Upper: IndexRangeBound{Value: bsonRawInt64(t, 10), Inclusive: true},
				Limit: 10,
			}
			// Pre-overlay settled cut still sees u0002 and does NOT see u0003a.
			var preIDs []string
			preTrunc, preFound, err := preOverlay.scanIndexRange("age", overlayOpts, true, func(id, _ []byte) (bool, error) {
				preIDs = append(preIDs, string(id))
				return true, nil
			})
			wantPre := liveInRange(0, 10, true, true, false, false)
			if err != nil || !preFound || preTrunc || !reflect.DeepEqual(preIDs, wantPre) {
				t.Fatalf("pre-overlay settled scan: ids=%v trunc=%v err=%v want=%v", preIDs, preTrunc, err, wantPre)
			}

			// Current collection sees pending overlay (u0002 deleted, u0003a inserted).
			gotOverlay, overlayTrunc, err := col.FindDocumentsByIndexRange("age", overlayOpts)
			wantOverlay := []string{"u0000", "u0001", "u0003", "u0003a", "u0004", "u0006", "u0007", "u0008", "u0009", "u0010"}
			if err != nil || overlayTrunc || !reflect.DeepEqual(docRecordIDs(gotOverlay), wantOverlay) {
				t.Fatalf("overlay fallback scan: ids=%v trunc=%v err=%v want=%v", docRecordIDs(gotOverlay), overlayTrunc, err, wantOverlay)
			}
		})
	}
}

func TestScanIndexRangeWarmLaneOrphanFallback(t *testing.T) {
	owner, _, col := openNativeIndexRangeFixture(t, nativeIndexRangeFixtureOpts{
		outerLeavesDict: runtime.GOOS != "windows",
		pointerThresh:   64,
	})
	pad := string(bytes.Repeat([]byte("o"), 128))
	const total = 20
	ids := make([][]byte, total)
	docs := make([][]byte, total)
	docByID := make(map[string][]byte, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("d%02d", i)
		ids[i] = []byte(id)
		// Each doc has two tags so we can test dedupe orphan fallback too.
		tags := []string{
			fmt.Sprintf("k%02d", i),
			fmt.Sprintf("k%02d", i+1),
		}
		docs[i] = makeNativeRangeBSONDoc(t, int64(i), tags, pad)
		docByID[id] = docs[i]
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// settledRead has settled secondary indexes (baseRoot != 0, pending == nil, captured == nil)
	// containing all 20 docs.
	settledRead, err := col.nativeJSON.Capture(col.name)
	if err != nil {
		t.Fatal(err)
	}
	defer settledRead.Close()

	// Delete selected primary docs WITHOUT checkpointing so their primary domain
	// shares the same baseRoot but has pending tombstones for the orphan IDs:
	// - d01: orphan inside first Limit=3 entries for [0, 6]
	// - d06: orphan at (Limit+1)-th lookahead for [3, 6] (no live doc after d06 -> truncated=false)
	// - d07: second consecutive orphan at (Limit+2) for [3, 7] -> truncated=false, [3, 8] -> truncated=true (d08 live)
	// - d10, d11, d12: ALL Limit=3 collected IDs are orphans for [10, 15] -> must still collect d13, d14, d15!
	orphanIDs := []string{"d01", "d06", "d07", "d10", "d11", "d12"}
	for _, id := range orphanIDs {
		if err := col.Delete([]byte(id)); err != nil {
			t.Fatal(err)
		}
	}
	overlayRead, err := col.nativeJSON.Capture(col.name)
	if err != nil {
		t.Fatal(err)
	}
	defer overlayRead.Close()

	// Swap primary domain onto settledRead so its settled secondary index hits
	// the warm lane while getAdmittedLocked sees primary tombstones for orphanIDs.
	origDomain := settledRead.domain
	settledRead.domain = overlayRead.domain
	defer func() {
		settledRead.domain = origDomain
	}()

	runScan := func(indexName string, opts IndexRangeOptions) ([]string, bool) {
		t.Helper()
		var got []string
		trunc, found, err := settledRead.scanIndexRange(indexName, opts, true, func(id, doc []byte) (bool, error) {
			if !bytes.Equal(doc, docByID[string(id)]) {
				return false, fmt.Errorf("doc mismatch for %q", id)
			}
			got = append(got, string(id))
			return true, nil
		})
		if err != nil || !found {
			t.Fatalf("scanIndexRange(%q): found=%v err=%v", indexName, found, err)
		}
		return got, trunc
	}

	// (a) Missing primary doc within first Limit=3 entries ([0, 5]: d00, [d01 orphan], d02, d03, d04, d05)
	// Must emit [d00, d02, d03] without duplicates and return truncated=true (d04 live).
	got, trunc := runScan("age", IndexRangeOptions{
		Lower: IndexRangeBound{Value: bsonRawInt64(t, 0), Inclusive: true},
		Upper: IndexRangeBound{Value: bsonRawInt64(t, 5), Inclusive: true},
		Limit: 3,
	})
	if !trunc || !reflect.DeepEqual(got, []string{"d00", "d02", "d03"}) {
		t.Fatalf("orphan within Limit (truncated): got=%v trunc=%v want=[d00 d02 d03] true", got, trunc)
	}

	// Missing primary doc within first Limit=3 entries where fallback exhausts range ([0, 3]: d00, [d01], d02, d03)
	// Must emit [d00, d02, d03] and return truncated=false!
	got, trunc = runScan("age", IndexRangeOptions{
		Lower: IndexRangeBound{Value: bsonRawInt64(t, 0), Inclusive: true},
		Upper: IndexRangeBound{Value: bsonRawInt64(t, 3), Inclusive: true},
		Limit: 3,
	})
	if trunc || !reflect.DeepEqual(got, []string{"d00", "d02", "d03"}) {
		t.Fatalf("orphan within Limit (exact): got=%v trunc=%v want=[d00 d02 d03] false", got, trunc)
	}

	// ALL Limit=3 collected entries are orphans ([10, 16]: [d10, d11, d12 orphans], d13, d14, d15, d16)
	// Must emit [d13, d14, d15] with truncated=true!
	got, trunc = runScan("age", IndexRangeOptions{
		Lower: IndexRangeBound{Value: bsonRawInt64(t, 10), Inclusive: true},
		Upper: IndexRangeBound{Value: bsonRawInt64(t, 16), Inclusive: true},
		Limit: 3,
	})
	if !trunc || !reflect.DeepEqual(got, []string{"d13", "d14", "d15"}) {
		t.Fatalf("all-orphan initial batch: got=%v trunc=%v want=[d13 d14 d15] true", got, trunc)
	}

	// (b1) Orphan (Limit+1)-th lookahead key with NO subsequent live doc:
	// [3, 6]: d03, d04, d05 live; d06 orphan -> truncated MUST be false!
	got, trunc = runScan("age", IndexRangeOptions{
		Lower: IndexRangeBound{Value: bsonRawInt64(t, 3), Inclusive: true},
		Upper: IndexRangeBound{Value: bsonRawInt64(t, 6), Inclusive: true},
		Limit: 3,
	})
	if trunc || !reflect.DeepEqual(got, []string{"d03", "d04", "d05"}) {
		t.Fatalf("orphan lookahead no subsequent live (single): got=%v trunc=%v want=[d03 d04 d05] false", got, trunc)
	}

	// Multiple consecutive orphans after Limit with NO subsequent live doc:
	// [3, 7]: d03, d04, d05 live; d06, d07 orphans -> truncated MUST be false!
	got, trunc = runScan("age", IndexRangeOptions{
		Lower: IndexRangeBound{Value: bsonRawInt64(t, 3), Inclusive: true},
		Upper: IndexRangeBound{Value: bsonRawInt64(t, 7), Inclusive: true},
		Limit: 3,
	})
	if trunc || !reflect.DeepEqual(got, []string{"d03", "d04", "d05"}) {
		t.Fatalf("orphan lookahead no subsequent live (double): got=%v trunc=%v want=[d03 d04 d05] false", got, trunc)
	}

	// (b2) Orphan (Limit+1)-th and (Limit+2)-th lookahead keys WITH a subsequent live doc:
	// [3, 8]: d03, d04, d05 live; d06, d07 orphans; d08 live -> truncated MUST be true!
	got, trunc = runScan("age", IndexRangeOptions{
		Lower: IndexRangeBound{Value: bsonRawInt64(t, 3), Inclusive: true},
		Upper: IndexRangeBound{Value: bsonRawInt64(t, 8), Inclusive: true},
		Limit: 3,
	})
	if !trunc || !reflect.DeepEqual(got, []string{"d03", "d04", "d05"}) {
		t.Fatalf("orphan lookahead with subsequent live: got=%v trunc=%v want=[d03 d04 d05] true", got, trunc)
	}

	// (c) MultiKey dedupe + orphan within Limit:
	// tags in ["k00", "k05"]: d00(k00,k01), [d01(k01,k02) orphan], d02(k02,k03), d03(k03,k04), d04(k04,k05), d05(k05,k06)
	got, trunc = runScan("tags", IndexRangeOptions{
		Lower: IndexRangeBound{Value: bsonRawString(t, "k00"), Inclusive: true},
		Upper: IndexRangeBound{Value: bsonRawString(t, "k05"), Inclusive: true},
		Limit: 3,
	})
	if !trunc || !reflect.DeepEqual(got, []string{"d00", "d02", "d03"}) {
		t.Fatalf("dedupe orphan within Limit: got=%v trunc=%v want=[d00 d02 d03] true", got, trunc)
	}
}

func TestScanIndexRangeWarmLaneCOWBudgetExhausted(t *testing.T) {
	owner, _, col := openNativeIndexRangeFixture(t, nativeIndexRangeFixtureOpts{
		outerLeavesDict: false,
		pointerThresh:   64,
	})
	pad := string(bytes.Repeat([]byte("b"), 128))
	const total = 100
	ids := make([][]byte, total)
	docs := make([][]byte, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("u%04d", i)
		if i == 80 {
			// Oversized ID (2 KiB) to test mid-collection resize refusal under tight budget.
			id = fmt.Sprintf("u%04d-%s", i, string(bytes.Repeat([]byte("z"), 2048)))
		}
		ids[i] = []byte(id)
		docs[i] = makeNativeRangeBSONDoc(t, int64(i), []string{fmt.Sprintf("t%03d", i)}, pad)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	read, err := col.nativeJSON.Capture(col.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()

	warmOpts := IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(0), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(50), Inclusive: true},
		Limit: 10,
	}
	// Warm the read lanes first so lane workspace allocation is already settled.
	var warmGot []string
	trunc, found, err := read.scanIndexRange("age_i64", warmOpts, false, func(id, _ []byte) (bool, error) {
		warmGot = append(warmGot, string(id))
		return true, nil
	})
	if err != nil || !found || !trunc || len(warmGot) != 10 {
		t.Fatalf("warm baseline: len=%d trunc=%v found=%v err=%v", len(warmGot), trunc, found, err)
	}

	budget := read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)

	// Case 1: Headroom strictly LESS than OwnedPointerProjectionAllocationSize()
	// succeeds on the warm bounded path with zero lease leaks!
	projSize := memtable.COWAllocationCharge(tree.OwnedPointerProjectionAllocationSize())
	headroom := projSize / 2
	if headroom < 4096 {
		headroom = 4096
	}
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	avail := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	if err := blocker.Resize(avail - headroom); err != nil {
		blocker.Close()
		t.Fatal(err)
	}
	beforeTight := budget.Stats()
	var tightIDs []string
	trunc, found, err = read.scanIndexRange("age_i64", warmOpts, false, func(id, _ []byte) (bool, error) {
		tightIDs = append(tightIDs, string(id))
		return true, nil
	})
	afterTight := budget.Stats()
	blocker.Close()
	if err != nil || !found || !trunc || len(tightIDs) != 10 {
		t.Fatalf("tight-budget warm scan (headroom=%d < projSize=%d): len=%d trunc=%v found=%v err=%v", headroom, projSize, len(tightIDs), trunc, found, err)
	}
	if afterTight.ReservedBytes != beforeTight.ReservedBytes || afterTight.ExternalLeases != beforeTight.ExternalLeases {
		t.Fatalf("tight-budget warm scan leaked: before=%+v after=%+v", beforeTight, afterTight)
	}

	// Case 2: Completely full budget fails with ErrCOWCapacity and zero leaks.
	blockerFull, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := blockerFull.Resize(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes); err != nil {
		blockerFull.Close()
		t.Fatal(err)
	}
	beforeFull := budget.Stats()
	_, _, err = read.scanIndexRange("age_i64", warmOpts, true, func(_, _ []byte) (bool, error) {
		return true, nil
	})
	afterFull := budget.Stats()
	blockerFull.Close()
	if !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("full-budget warm scan err=%v want ErrCOWCapacity", err)
	}
	if afterFull.ReservedBytes != beforeFull.ReservedBytes || afterFull.ExternalLeases != beforeFull.ExternalLeases {
		t.Fatalf("full-budget warm scan leaked: before=%+v after=%+v", beforeFull, afterFull)
	}

	// Case 3: Oversized ID (2 KiB at age=80) exceeds preAdmitted and triggers
	// lease.Resize under tight headroom -> fails with ErrCOWCapacity, zero leaks.
	oversizedOpts := IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(80), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(82), Inclusive: true},
		Limit: 2,
	}
	blockerOversized, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	avail = budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	// Leave enough headroom for initial admission (~1.2 KiB), NOT enough for +2 KiB resize.
	if err := blockerOversized.Resize(avail - 1600); err != nil {
		blockerOversized.Close()
		t.Fatal(err)
	}
	beforeOver := budget.Stats()
	_, _, err = read.scanIndexRange("age_i64", oversizedOpts, false, func(_, _ []byte) (bool, error) {
		return true, nil
	})
	afterOver := budget.Stats()
	blockerOversized.Close()
	if !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("oversized-ID tight-budget warm scan err=%v want ErrCOWCapacity", err)
	}
	if afterOver.ReservedBytes != beforeOver.ReservedBytes || afterOver.ExternalLeases != beforeOver.ExternalLeases {
		t.Fatalf("oversized-ID warm scan leaked: before=%+v after=%+v", beforeOver, afterOver)
	}
}

func BenchmarkScanIndexRangeLimit10StandaloneCOW(b *testing.B) {
	owner, _, col := openNativeIndexRangeFixture(b, nativeIndexRangeFixtureOpts{
		outerLeavesDict: runtime.GOOS != "windows",
		pointerThresh:   64,
		indexes: []IndexDefinition{
			{Name: "age", Field: "age", ValueType: IndexValueBSONOrderedV2},
		},
	})
	const total = 4000
	pad := string(bytes.Repeat([]byte("a"), 192))
	ids := make([][]byte, total)
	docs := make([][]byte, total)
	for i := 0; i < total; i++ {
		ids[i] = []byte(fmt.Sprintf("doc%06d", i))
		docs[i] = makeNativeRangeBSONDoc(b, int64(i%100), nil, pad)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		b.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		b.Fatal(err)
	}

	const numRanges = 64
	rangeOpts := make([]IndexRangeOptions, numRanges)
	for i := 0; i < numRanges; i++ {
		lo := int64(i % 80)
		hi := lo + 15
		rangeOpts[i] = IndexRangeOptions{
			Lower: IndexRangeBound{Value: bsonRawInt64(b, lo), Inclusive: true},
			Upper: IndexRangeBound{Value: bsonRawInt64(b, hi), Inclusive: true},
			Limit: 10,
		}
	}

	// Warm caches and read lanes before timing.
	for i := 0; i < numRanges; i++ {
		recs, trunc, err := col.FindDocumentsByIndexRange("age", rangeOpts[i])
		if err != nil || !trunc || len(recs) != 10 {
			b.Fatalf("warmup FindDocumentsByIndexRange: len=%d trunc=%v err=%v", len(recs), trunc, err)
		}
	}

	b.Run("Serial", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			recs, trunc, err := col.FindDocumentsByIndexRange("age", rangeOpts[i%numRanges])
			if err != nil || !trunc || len(recs) != 10 {
				b.Fatalf("FindDocumentsByIndexRange: len=%d trunc=%v err=%v", len(recs), trunc, err)
			}
		}
	})

	b.Run("Parallel32", func(b *testing.B) {
		b.ReportAllocs()
		b.SetParallelism(32/max(runtime.GOMAXPROCS(0), 1) + 1)
		var counter atomic.Uint64
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				idx := counter.Add(1) % numRanges
				recs, trunc, err := col.FindDocumentsByIndexRange("age", rangeOpts[idx])
				if err != nil || !trunc || len(recs) != 10 {
					b.Errorf("FindDocumentsByIndexRange: len=%d trunc=%v err=%v", len(recs), trunc, err)
					return
				}
			}
		})
	})

	b.Run("IDsOnly_Serial", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			got, trunc, err := col.FindByIndexRange("age", rangeOpts[i%numRanges])
			if err != nil || !trunc || len(got) != 10 {
				b.Fatalf("FindByIndexRange: len=%d trunc=%v err=%v", len(got), trunc, err)
			}
		}
	})

	b.Run("IDsOnly_Parallel32", func(b *testing.B) {
		b.ReportAllocs()
		b.SetParallelism(32/max(runtime.GOMAXPROCS(0), 1) + 1)
		var counter atomic.Uint64
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				idx := counter.Add(1) % numRanges
				got, trunc, err := col.FindByIndexRange("age", rangeOpts[idx])
				if err != nil || !trunc || len(got) != 10 {
					b.Errorf("FindByIndexRange: len=%d trunc=%v err=%v", len(got), trunc, err)
					return
				}
			}
		})
	})
}
