package collections

import (
	"bytes"
	"fmt"
	"testing"
)

func typedGraphOverlapLimits() typedGraphReadOwnerLimits {
	return typedGraphReadOwnerLimits{Owners: 4, States: 4, StateBytes: 16 << 20, AssetBytes: 64 << 20, Cold: typedGraphColdLimits{ManifestRecords: 4096, ManifestBytes: 4 << 20, AssetBytes: 32 << 20, DecodedTermBytes: 32 << 20}}
}

func requireTypedGraphPreparedHolderTest(t testing.TB) {
	t.Helper()
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("captured prepared holder requires mmap_direct support")
	}
}

func TestTypedGraphReadOwnerPreparedOverlap(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	col, fixtureBase, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 1024)
	if err := fixtureBase.Close(); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphOverlapLimits()
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 512, Tombstones: 512, ValueSlots: 2048, OwnedBytes: 8 << 20}, limits.Cold); err != nil {
		t.Fatal(err)
	}
	anchor, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer anchor.Close()
	holder := anchor.overlay.base.reader.sharedPreparedSearch.holder
	initial := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	if initial.Entries != 1 || initial.Refs != 1 || holder == nil {
		t.Fatalf("anchor cache=%+v", initial)
	}
	var anchorBuffer VectorIndexSearchBuffer
	oldResults, _, err := anchor.overlay.search(columns[0].Float32Vectors[0], 10, 128, 4096, &anchorBuffer)
	if err != nil {
		t.Fatal(err)
	}
	oldIDs := make([]string, len(oldResults))
	for i, result := range oldResults {
		oldIDs[i] = string(result.ID)
	}
	previous := 0
	for _, d := range []int{0, 32, 256} {
		t.Run(fmt.Sprintf("suffix%d", d), func(t *testing.T) {
			if d > previous {
				changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[previous:d]}, {Name: "content", Strings: make([]string, d-previous)}, {Name: "user", Strings: columns[2].Strings[previous:d]}, {Name: "path", Strings: make([]string, d-previous)}}
				for i := range changed[1].Strings {
					changed[1].Strings[i], changed[3].Strings[i] = "changed content", "changed"
				}
				if _, err := col.ReplaceTypedBatch(ids[previous:d], retained[previous:d], changed); err != nil {
					t.Fatal(err)
				}
				if err := col.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			current, err := col.openTypedGraphReadOwner(limits)
			if err != nil {
				t.Fatal(err)
			}
			defer current.Close()
			cache := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
			if current.overlay.base.reader.sharedPreparedSearch.holder != holder || cache.CacheBuilds != initial.CacheBuilds || cache.CacheMisses != initial.CacheMisses || cache.Entries != 1 || cache.Refs != 2 || cache.CacheHits <= initial.CacheHits || cache.ActiveHandles != initial.ActiveHandles {
				t.Fatalf("D%d did not reuse holder: before=%+v current=%+v", d, initial, cache)
			}
			if len(current.overlay.rows) != d || (d > 0 && current.state == anchor.state) {
				t.Fatalf("D%d current state rows=%d", d, len(current.overlay.rows))
			}
			check := func(owner *typedGraphReadOwner, path, content string, count int, buffer *VectorIndexSearchBuffer) {
				t.Helper()
				baseFilter, err := prepareTypedGraphBaseFilter(owner.overlay.base, HybridScalarFilter{IndexName: "path", Value: path}, typedGraphBaseFilterLimits{typedGraphFilterLimits: typedGraphFilterLimits{SourceIDs: 2048, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 100000, InspectedEntries: 4096}, Clauses: 4, PredicateBytes: 1024})
				if err != nil {
					t.Fatal(err)
				}
				plan, err := bindTypedGraphBaseFilter(baseFilter, owner.overlay, typedGraphFilterBindLimits{Rows: 512, IDBytes: 1 << 20, ValueBytes: 1 << 20, MappingWork: 100000, PredicateWork: 100000, RetainedBytes: 1 << 20, ExactScanRows: 4096})
				if err != nil {
					t.Fatal(err)
				}
				if plan.count != count {
					t.Fatalf("path=%s eligible=%d want=%d", path, plan.count, count)
				}
				results, _, err := owner.overlay.searchPreparedFilter(plan, columns[0].Float32Vectors[0], 10, 128, 4096, buffer)
				if err != nil || len(results) != min(10, count) {
					t.Fatalf("path=%s count=%d err=%v", path, len(results), err)
				}
				fetched, err := owner.overlay.current.FetchDocumentsForVectorIndexSearchResults(results, DocumentFetchOptions{})
				if err != nil {
					t.Fatal(err)
				}
				for _, doc := range fetched.Results {
					if !doc.Found || !bytes.Contains(doc.Document, []byte(`"content":"`+content+`"`)) {
						t.Fatalf("path=%s incoherent document: %+v", path, doc)
					}
				}
			}
			var currentBuffer VectorIndexSearchBuffer // private mutable worker scratch
			check(current, "changed", "changed content", d, &currentBuffer)
			check(current, "source", "content", 1024-d, &currentBuffer)
			check(anchor, "changed", "changed content", 0, &anchorBuffer)
			check(anchor, "source", "content", 1024, &anchorBuffer)
			results, _, err := anchor.overlay.search(columns[0].Float32Vectors[0], 10, 128, 4096, &anchorBuffer)
			if err != nil || len(results) != len(oldIDs) {
				t.Fatal("old owner result changed", err)
			}
			for i, result := range results {
				if string(result.ID) != oldIDs[i] {
					t.Fatal("old owner order changed")
				}
			}
			previous = d
			t.Logf("D%d builds=%d hits=%d refs=%d handles=%d", d, cache.CacheBuilds-initial.CacheBuilds, cache.CacheHits-initial.CacheHits, cache.Refs, cache.ActiveHandles)
		})
	}
	if err := anchor.Close(); err != nil {
		t.Fatal(err)
	}
	if cache := col.columnVectorGraphSharedPreparedSearchCacheSnapshot(); cache.Entries != 0 || cache.Refs != 0 {
		t.Fatalf("last owner retained cache: %+v", cache)
	}
}
