package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestTypedGraphPreparedFilterSelectiveAND(t *testing.T) {
	const n = 1032
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, n)
	broad := "broad\x00" + strings.Repeat("b", 128)
	narrow := "narrow\x00" + strings.Repeat("x", 256)
	for i := range n {
		columns[2].Strings[i] = broad
		switch {
		case i < 5:
			columns[3].Strings[i] = narrow
		case i < 517:
			columns[3].Strings[i] = "p512"
		case i < 1030:
			columns[3].Strings[i] = "p513"
		default:
			columns[3].Strings[i] = "tail"
		}
	}
	if _, err := col.ReplaceTypedBatch(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	current, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	overlay, err := prepareTypedGraphOverlaySearch(base, current, typedGraphOverlayLimits{Rows: n, Tombstones: n, Bytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	limits := typedGraphFilterLimits{SourceIDs: 4 * n, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 1 << 20, InspectedEntries: 4 * n}
	conjunction := func(path string, reverse bool) HybridScalarFilter {
		leaves := []HybridScalarFilter{{IndexName: "user", Value: broad}, {IndexName: "path", Value: path}}
		if reverse {
			leaves[0], leaves[1] = leaves[1], leaves[0]
		}
		return HybridScalarFilter{And: leaves}
	}
	for _, fixture := range []struct {
		name, value              string
		count, source, inspected int
	}{
		{"narrow", narrow, 5, 517, 518},
		{"exact_probe_eof", "p512", 512, 1024, 1025},
		{"probe_fallback", "p513", 513, 2*512 + n + 513, 2*513 + n + 513},
	} {
		for _, reverse := range []bool{false, true} {
			t.Run(fixture.name+map[bool]string{false: "/forward", true: "/reverse"}[reverse], func(t *testing.T) {
				var work ColumnGraphFilterWork
				plan, err := prepareTypedGraphFilterWithWork(overlay, conjunction(fixture.value, reverse), limits, &work)
				setup := (2*len(broad) + 2) + (2*len(fixture.value) + 2)
				mapping := 7*setup + fixture.count*(len(broad)+strings.Count(broad, "\x00")+2+len(ids[0])+1)
				if fixture.count > 512 {
					mapping = 13 * setup
				}
				mapping += fixture.count * (bits.Len(n) + bits.Len(n) + 2)
				if err != nil || plan == nil || plan.count != fixture.count || work.SourceIDs != uint64(fixture.source) || work.SourceBytes != uint64(fixture.source*len(ids[0])) || work.InspectedEntries != uint64(fixture.inspected) || work.MappingWorkCharged != uint64(mapping) {
					t.Fatalf("selective work=%+v want count=%d source=%d inspected=%d mapping=%d err=%v", work, fixture.count, fixture.source, fixture.inspected, mapping, err)
				}
			})
		}
	}
	t.Run("all_leaves_validated", func(t *testing.T) {
		for _, leaf := range []HybridScalarFilter{{IndexName: "missing", Value: "x"}, {IndexName: "path", Value: 42}} {
			var work ColumnGraphFilterWork
			got, err := prepareTypedGraphFilterWithWork(overlay, HybridScalarFilter{And: []HybridScalarFilter{{IndexName: "user", Value: "absent"}, leaf}}, limits, &work)
			if !errors.Is(err, ErrHybridSearchIndexUnavailable) || got != nil || work.Completed || work.SourceIDs != 0 {
				t.Fatalf("invalid later leaf plan=%+v work=%+v err=%v", got, work, err)
			}
		}
	})
	t.Run("physical_cap_is_not_probe_stop", func(t *testing.T) {
		limited := limits
		limited.InspectedEntries = 512
		var work ColumnGraphFilterWork
		got, err := prepareTypedGraphFilterWithWork(overlay, conjunction(narrow, false), limited, &work)
		if !errors.Is(err, errTypedGraphSearchBudget) || got != nil || work.Completed || work.SourceIDs != 512 || work.InspectedEntries != 512 {
			t.Fatalf("physical budget plan=%+v work=%+v err=%v", got, work, err)
		}
	})

	setup := 7 * ((2*len(broad) + 2) + (2*len(narrow) + 2))
	keyBytes := 5 * (len(broad) + strings.Count(broad, "\x00") + 2 + len(ids[0]))
	ordinalWork := 5 * (bits.Len(n) + bits.Len(n) + 2)
	assertPrefix := func(t *testing.T, before workstats.GraphStats, work ColumnGraphFilterWork) {
		t.Helper()
		after := workstats.Read().Graph
		if !work.Attempted || work.Completed || after.Filters.Errors-before.Filters.Errors != 1 || after.FilterSourceIDs-before.FilterSourceIDs != work.SourceIDs || after.FilterSourceBytes-before.FilterSourceBytes != work.SourceBytes || after.FilterInspectedEntries-before.FilterInspectedEntries != work.InspectedEntries || after.FilterMappingWorkCharged-before.FilterMappingWorkCharged != work.MappingWorkCharged {
			t.Fatalf("local/process prefix mismatch work=%+v before=%+v after=%+v", work, before, after)
		}
	}
	t.Run("cumulative_budget_prefix", func(t *testing.T) {
		for _, tc := range []struct {
			name, path                 string
			limit                      func(*typedGraphFilterLimits)
			source, inspected, mapping int
		}{
			{"prefix_before_allocation", narrow, func(l *typedGraphFilterLimits) { l.MappingWork = 1 }, 0, 0, 0},
			{"long_second_prefix", narrow, func(l *typedGraphFilterLimits) { l.MappingWork = 2*len(broad) + 3 }, 0, 0, 2*len(broad) + 2},
			{"visitor_prefix", narrow, func(l *typedGraphFilterLimits) { l.MappingWork = (2*len(broad) + 2) + (2*len(narrow) + 2) }, 0, 0, (2*len(broad) + 2) + (2*len(narrow) + 2)},
			{"point_payload", narrow, func(l *typedGraphFilterLimits) { l.MappingWork = setup + keyBytes + 4 }, 517, 518, setup},
			{"final_ordinal", narrow, func(l *typedGraphFilterLimits) { l.MappingWork = setup + keyBytes + 5 + ordinalWork - 1 }, 517, 518, setup + keyBytes + 5},
			{"fallback_source", "p513", func(l *typedGraphFilterLimits) { l.SourceIDs = 2*512 + n + 512 }, 2*512 + n + 512, 2*513 + n + 513, 13 * ((2*len(broad) + 2) + (2*len("p513") + 2))},
			{"source_bytes", narrow, func(l *typedGraphFilterLimits) { l.SourceBytes = 3 * len(ids[0]) }, 3, 4, (2*len(narrow) + 2) + 7*(2*len(broad)+2)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				limited := limits
				tc.limit(&limited)
				var work ColumnGraphFilterWork
				before := workstats.Read().Graph
				got, err := prepareTypedGraphFilterWithWork(overlay, conjunction(tc.path, false), limited, &work)
				if got != nil || !errors.Is(err, errTypedGraphSearchBudget) || errors.Is(err, ErrHybridSearchIndexUnavailable) || work.SourceIDs != uint64(tc.source) || work.InspectedEntries != uint64(tc.inspected) || work.MappingWorkCharged != uint64(tc.mapping) || work.RetainedBytes != 0 {
					t.Fatalf("budget work=%+v want source=%d inspected=%d mapping=%d err=%v", work, tc.source, tc.inspected, tc.mapping, err)
				}
				assertPrefix(t, before, work)
			})
		}
	})
	t.Run("validated_empty", func(t *testing.T) {
		for _, reverse := range []bool{false, true} {
			var work ColumnGraphFilterWork
			got, err := prepareTypedGraphFilterWithWork(overlay, conjunction("absent", reverse), limits, &work)
			wantSource, wantInspected, wantMapping := 512, 513, 7*((2*len(broad)+2)+(2*len("absent")+2))
			if reverse {
				wantSource, wantInspected, wantMapping = 0, 0, (2*len(broad)+2)+7*(2*len("absent")+2)
			}
			if err != nil || got == nil || got.count != 0 || !work.Completed || work.SourceIDs != uint64(wantSource) || work.InspectedEntries != uint64(wantInspected) || work.MappingWorkCharged != uint64(wantMapping) {
				t.Fatalf("empty reverse=%t work=%+v err=%v", reverse, work, err)
			}
		}
	})
	t.Run("cancellation_construction_and_submitted_prefix", func(t *testing.T) {
		counted := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: int(^uint(0) >> 1)}
		filter := conjunction(narrow, false)
		if _, err := prepareTypedGraphFilterWithContext(counted, overlay, filter, limits, nil); err != nil {
			t.Fatal(err)
		}
		constructed, submitted := false, false
		for check := 1; check < counted.calls; check++ {
			var work ColumnGraphFilterWork
			before := workstats.Read().Graph
			got, err := prepareTypedGraphFilterWithContext(&cancelAfterErrContextV1{Context: context.Background(), cancelAfter: check}, overlay, filter, limits, &work)
			if got != nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("check %d err=%v", check, err)
			}
			assertPrefix(t, before, work)
			if work.RetainedBytes == 0 {
				constructed = constructed || work.MappingWorkCharged == uint64(setup+keyBytes)
				submitted = submitted || work.MappingWorkCharged == uint64(setup+keyBytes+5)
			}
		}
		if !constructed || !submitted {
			t.Fatalf("missing mid-work cancellation: constructed=%t submitted=%t checks=%d", constructed, submitted, counted.calls)
		}
		if _, err := prepareTypedGraphFilter(overlay, filter, limits); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("sticky_callback_missing_key_error", func(t *testing.T) {
		idx, _ := findIndex(current.catalog.meta.Indexes, "user")
		prefix, err := encodeIndexScalar(idx.ValueType, broad)
		if err != nil {
			t.Fatal(err)
		}
		allowed := hybridScalarAllowSet{}
		for _, id := range ids[:5] {
			allowed[string(id)] = struct{}{}
		}
		plan := &typedGraphPreparedFilter{overlay: overlay}
		// One entry check, five ID checks, five encoding checks, one submit
		// check: the next context check is the first storage callback.
		ctx := &typedGraphFilterErrorContext{Context: context.Background(), at: 13, err: tree.ErrKeyNotFound}
		err = refineTypedGraphAND(ctx, plan, allowed, idx, prefix, limits)
		if !errors.Is(err, tree.ErrKeyNotFound) || len(allowed) != 5 || plan.mappingWork != keyBytes+5 || ctx.calls != 13 {
			t.Fatalf("swallowed/repeated callback error=%v IDs=%d mapping=%d checks=%d", err, len(allowed), plan.mappingWork, ctx.calls)
		}
	})
	t.Run("captured_membership_after_writer", func(t *testing.T) {
		// Keep both captures open. Current secondary membership must remove the
		// replaced row, while the held capture retains its prior membership.
		changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"changed"}}, {Name: "user", Strings: []string{"outside"}}, {Name: "path", Strings: []string{narrow}}}
		if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
			t.Fatal(err)
		}
		if _, err := col.DeleteBatch(ids[2:3]); err != nil {
			t.Fatal(err)
		}
		latest, err := col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		defer latest.Close()
		fresh, err := prepareTypedGraphOverlaySearch(base, latest, typedGraphOverlayLimits{Rows: n + 2, Tombstones: n, Bytes: 8 << 20})
		if err != nil {
			t.Fatal(err)
		}
		for _, capture := range []struct {
			overlay *typedGraphOverlaySearch
			want    []string
		}{
			{overlay, []string{string(ids[0]), string(ids[1]), string(ids[2]), string(ids[3]), string(ids[4])}},
			{fresh, []string{string(ids[1]), string(ids[3]), string(ids[4])}},
		} {
			for _, reverse := range []bool{false, true} {
				plan, err := prepareTypedGraphFilter(capture.overlay, conjunction(narrow, reverse), limits)
				if err != nil {
					t.Fatal(err)
				}
				var got []string
				for _, ordinal := range plan.delta {
					got = append(got, string(capture.overlay.rows[ordinal].ID))
				}
				slices.Sort(got)
				if plan.count != len(capture.want) || !slices.Equal(got, capture.want) {
					t.Fatalf("captured membership IDs=%q want=%q", got, capture.want)
				}
			}
		}
		if err := latest.Close(); err != nil {
			t.Fatal(err)
		}
		if plan, err := prepareTypedGraphFilter(fresh, conjunction(narrow, false), limits); plan != nil || !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
			t.Fatalf("closed current accepted: %v", err)
		}
	})
	t.Run("public_owner_retry_fold_reopen", func(t *testing.T) {
		requireTypedGraphPublicServingTest(t)
		if err := current.Close(); err != nil {
			t.Fatal(err)
		}
		if err := base.Close(); err != nil {
			t.Fatal(err)
		}
		opts := typedGraphPublicTestOptions()
		opts.Publication.Rows, opts.Publication.Tombstones, opts.Publication.ValueSlots = 2048, 2048, 8192
		// Fold inspects initial + replacement parts and the later update/delete,
		// even though the latest live suffix has only n-1 rows.
		opts.Filter, opts.FoldRows, opts.SearchCandidates = limits, 2*n+2, 4096
		if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", opts); err != nil {
			t.Fatal(err)
		}
		filter := conjunction(narrow, false)
		q := VectorIndexSearchOptions{IndexName: "embedding_graph", Query: columns[0].Float32Vectors[0], TopK: 5, EfSearch: 128, StatsMode: VectorIndexSearchStatsModeMinimal, DeclaredScalarFilter: &filter}
		var buffer VectorIndexSearchBuffer
		checkResults := func(c *Collection) {
			t.Helper()
			response, view, err := c.SearchVectorIndexWithBufferReadView(q, &buffer)
			if err != nil {
				t.Fatal(err)
			}
			defer view.Close()
			docs, err := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, doc := range docs.Results {
				got = append(got, string(doc.ID))
				if !bytes.Contains(doc.Document, []byte(`"content":"content"`)) {
					t.Fatalf("wrong current payload: %s", doc.Document)
				}
			}
			slices.Sort(got)
			want := []string{string(ids[1]), string(ids[3]), string(ids[4])}
			if !slices.Equal(got, want) || response.Stats.ColumnGraphWork.Route != "typed_exact" || !response.Stats.ColumnGraphWork.Filter.Completed {
				t.Fatalf("public AND IDs=%q work=%+v", got, response.Stats.ColumnGraphWork)
			}
		}
		counted := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: int(^uint(0) >> 1)}
		q.Context = counted
		checkResults(col)
		submitted := false
		for check := 1; check < counted.calls; check++ {
			q.Context = &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: check}
			before := workstats.Read().Graph
			response, view, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
			if !errors.Is(err, context.Canceled) || view != nil || len(response.Results) != 0 || len(buffer.results) != 0 {
				t.Fatalf("public cancellation check=%d err=%v", check, err)
			}
			if owners := col.collectionSchemaCoordinator().typedGraphOwners.owners; owners != 0 {
				t.Fatalf("cancellation leaked %d owners", owners)
			}
			work := response.Stats.ColumnGraphWork.Filter
			if work.Attempted && !work.Completed {
				assertPrefix(t, before, work)
			}
			// Four live path IDs submit four broad-membership keys; one is
			// removed. Stop once cancellation occurs after this actual submit.
			if work.MappingWorkCharged == uint64(setup+4*(len(broad)+strings.Count(broad, "\x00")+2+len(ids[0])+1)) && work.RetainedBytes == 0 {
				submitted = true
				break
			}
		}
		if !submitted {
			t.Fatal("public cancellation never reached submitted membership")
		}
		q.Context = nil
		checkResults(col)
		if err := col.FoldColumnGraphServing(context.Background(), "embedding_graph"); err != nil {
			t.Fatal(err)
		}
		checkResults(col)
		dir := col.db.Dir()
		if err := col.db.Close(); err != nil {
			t.Fatal(err)
		}
		reopenedDB := openTypedMinimaDB(t, dir)
		defer reopenedDB.Close()
		reopened, err := NewCollectionManager(reopenedDB).OpenCollection(col.Name())
		if err != nil {
			t.Fatal(err)
		}
		if err := reopened.EnsureColumnGraphServing(context.Background(), "embedding_graph", opts); err != nil {
			t.Fatal(err)
		}
		checkResults(reopened)
	})
}

// Return an arbitrary callback error once, then appear live again. The consumer
// must retain the original callback error through GetMany's missing-root retry.
type typedGraphFilterErrorContext struct {
	context.Context
	at, calls int
	err       error
}

func (c *typedGraphFilterErrorContext) Err() error {
	c.calls++
	if c.calls == c.at {
		return c.err
	}
	return nil
}

func TestTypedGraphANDProbePhysicalTombstoneCap(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mgr := NewCollectionManager(db)
	_, err = mgr.CreateCollection(&CollectionMeta{Name: "physical", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, BufferedIndexedOverlayRoots: true, BufferedIndexedWriteMaxDocuments: 128, BufferedIndexedWriteMaxRootRuns: 1, DisableBufferedIndexedAsyncFlush: true}, Indexes: []IndexDefinition{{Name: "user", Field: "user", ValueType: IndexValueString}}})
	if err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("physical")
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := make([][]byte, 8), make([][]byte, 8)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("id-%02d", i))
		docs[i] = []byte(`{"user":"same"}`)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := col.CompactRootOverlays(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := col.DeleteBatch(ids); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	lookup := hybridScalarLookupView{context: context.Background(), snapshot: view.snapshot, catalog: view.catalog}
	limits := typedGraphFilterLimits{SourceIDs: 512, SourceBytes: 1024, MappingWork: 1024, InspectedEntries: 3}
	plan := &typedGraphPreparedFilter{}
	got, complete, err := probeTypedGraphANDLeaf(context.Background(), plan, &lookup, HybridScalarFilter{IndexName: "user", Value: "same"}, limits, 512, 10)
	if got != nil || complete || !errors.Is(err, errTypedGraphSearchBudget) || plan.sourceIDs != 0 || plan.inspectedEntries != 3 || plan.mappingWork != 60 {
		t.Fatalf("tombstone cap complete=%t source=%d inspected=%d mapping=%d err=%v", complete, plan.sourceIDs, plan.inspectedEntries, plan.mappingWork, err)
	}
	// With sufficient physical work, the same real deleted posting stream
	// completes empty. The prior cap was not a certified empty driver.
	limits.InspectedEntries = 32
	plan = &typedGraphPreparedFilter{}
	got, complete, err = probeTypedGraphANDLeaf(context.Background(), plan, &lookup, HybridScalarFilter{IndexName: "user", Value: "same"}, limits, 512, 10)
	if err != nil || !complete || len(got) != 0 || plan.sourceIDs != 0 || plan.inspectedEntries <= 3 {
		t.Fatalf("tombstone retry complete=%t source=%d inspected=%d err=%v", complete, plan.sourceIDs, plan.inspectedEntries, err)
	}
}
