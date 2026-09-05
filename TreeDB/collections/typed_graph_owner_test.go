package collections

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func TestTypedGraphOwnerCaptureCutoverDefaultGC(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"original"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	other, err := NewCollectionManager(db).OpenCollection(col.Name())
	if err != nil {
		t.Fatal(err)
	}
	captured, release := make(chan struct{}), make(chan struct{})
	typedGraphOwnerAfterSnapshotHook.Lock()
	typedGraphOwnerAfterSnapshotHook.fn = func(c *Collection) {
		if c == col {
			close(captured)
			<-release
		}
	}
	typedGraphOwnerAfterSnapshotHook.Unlock()
	defer func() {
		typedGraphOwnerAfterSnapshotHook.Lock()
		typedGraphOwnerAfterSnapshotHook.fn = nil
		typedGraphOwnerAfterSnapshotHook.Unlock()
	}()
	type opened struct {
		searcher *VectorIndexSearcher
		err      error
	}
	openDone := make(chan opened, 1)
	go func() {
		s, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding_graph"})
		openDone <- opened{s, err}
	}()
	<-captured
	writeDone := make(chan error, 1)
	go func() {
		for i := 0; i < 3; i++ {
			columns[0].Float32Vectors[0] = []float32{0, 1, float32(i), 0, 0, 0, 0, 0}
			columns[1].Strings[0] = "changed"
			if _, err := other.ReplaceTypedBatch([][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}, columns); err != nil {
				writeDone <- err
				return
			}
			if _, err := other.RebuildVectorIndex("embedding_graph"); err != nil {
				writeDone <- err
				return
			}
			if err := db.Checkpoint(); err != nil {
				writeDone <- err
				return
			}
		}
		_, err := other.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{})
		if err != nil {
			for _, pin := range mappedresource.GlobalPinSummary() {
				if columnAssetMappedResourcePinMatchesRoot(pin, db.ColumnAssetRootDir()) {
					if pin.Scope.ID == columnVectorGraphAdjacencyStateSourceScopeID {
						t.Logf("adjacency companion: %+v", pin)
					}
					if _, ok := columnAssetRefForMappedResourceKey(pin.Key); !ok {
						t.Logf("unconvertible owner pin: %+v", pin)
					}
				}
			}
		}
		writeDone <- err
	}()
	// Before the owner barrier, all cutovers and GC finish while opening is
	// paused. With the barrier they wait until capture has acquired its lease.
	var writeErr error
	completed := false
	select {
	case writeErr = <-writeDone:
		completed = true
	case <-time.After(2 * time.Second):
	}
	close(release)
	result := <-openDone
	if !completed {
		writeErr = <-writeDone
	}
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if result.err != nil {
		t.Fatalf("public graph owner lost captured assets: %v", result.err)
	}
	defer result.searcher.Close()
	if result.searcher.documentView != nil {
		t.Fatal("fixture unexpectedly materialized documents at open")
	}
	response, err := result.searcher.Search(VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0, 0, 0, 0, 0, 0}, TopK: 1, EfSearch: 8, IncludeDocuments: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 || response.Results[0].Score < .999 || !bytes.Contains(response.Results[0].Document, []byte(`"content":"original"`)) {
		t.Fatalf("old snapshot result changed after retirement: %+v", response.Results)
	}
}

func TestTypedGraphOwnerLeaseLifetime(t *testing.T) {
	for _, closeDB := range []bool{false, true} {
		t.Run(map[bool]string{false: "close", true: "db_close"}[closeDB], func(t *testing.T) {
			_, db, col := openTypedMinimaCollection(t)
			defer db.Close()
			columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"original"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
			if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}, columns); err != nil {
				t.Fatal(err)
			}
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatal(err)
			}
			other, err := NewCollectionManager(db).OpenCollection(col.Name())
			if err != nil {
				t.Fatal(err)
			}
			s, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding_graph"})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if s.lifecyclePin == nil || len(other.columnAssetLifecyclePinSetSnapshot()) != 1 {
				t.Fatal("owner lease not visible across managers")
			}
			requirements, _, err := s.catalog.typedGraphBase.requirementsAtSnapshot(s.snapshot)
			if err != nil {
				t.Fatal(err)
			}
			refs := s.lifecyclePin.Refs()
			if len(refs) != len(requirements.Obligations) {
				t.Fatalf("owner closure refs=%d want=%d", len(refs), len(requirements.Obligations))
			}
			owned := refs[0]
			probe, err := col.AcquireColumnAssetLifecyclePinSet(ColumnAssetLifecyclePinSetOptions{Source: ColumnAssetLifecyclePinSourcePreparedQuery, Owner: "copy-isolation", Refs: refs[:1]})
			if err != nil {
				t.Fatal(err)
			}
			refs[0].Checksum++
			out := probe.Refs()
			if out[0] != owned {
				t.Fatal("registry borrowed caller input")
			}
			out[0].Checksum++
			for _, record := range other.columnAssetLifecyclePinSetSnapshot() {
				if record.Owner == "copy-isolation" {
					record.Refs[0].Checksum++
				}
			}
			if probe.Refs()[0] != owned {
				t.Fatal("lease exposed immutable owned refs")
			}
			for _, record := range other.columnAssetLifecyclePinSetSnapshot() {
				if record.Owner == "copy-isolation" && record.Refs[0] != owned {
					t.Fatal("report exposed registry refs")
				}
			}
			if err := probe.Close(); err != nil {
				t.Fatal(err)
			}
			// Invalid opens cannot accumulate a second registry lease.
			if failed, err := other.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "absent"}); err == nil || failed != nil {
				t.Fatal("invalid index admitted")
			}
			if len(other.columnAssetLifecyclePinSetSnapshot()) != 1 {
				t.Fatal("failed open leaked lease")
			}
			if closeDB {
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				failedLease, err := col.AcquireColumnAssetLifecyclePinSet(ColumnAssetLifecyclePinSetOptions{Source: ColumnAssetLifecyclePinSourcePreparedQuery, Owner: "closed-register", Refs: []ColumnAssetRef{owned}})
				if err == nil || failedLease != nil {
					t.Fatal("closed backend accepted lease registration")
				}
				_, err = s.Search(VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0, 0, 0, 0, 0, 0}, TopK: 1, EfSearch: 8, IncludeDocuments: true})
				if err == nil {
					t.Fatal("search on closed DB did not fail closed")
				}
			} else if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if len(other.columnAssetLifecyclePinSetSnapshot()) != 0 {
				t.Fatal("owner lease not released")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTypedGraphOwnerEmptyAdjacencyPinClassification(t *testing.T) {
	key := mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: "n", Kind: string(ColumnAssetKindTCS1TypedColumnPart), Generation: 1, PartID: 4, FileID: 3, Offset: 2072, Version: 4, Encoding: "raw_uint32_offsets_list", Section: mappedresource.Section{Kind: "column_values", Name: "values", Category: "declared_column_values", Column: "adjacency"}}
	pin := mappedresource.Pin{Key: key, Scope: mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: columnVectorGraphAdjacencyStateSourceScopeID}, Source: mappedresource.SourceMapped, Root: "/root", Path: "/root/asset"}
	parent := pin
	parent.Key.Offset, parent.Key.Length, parent.Bytes = 2056, 16, 16
	parent.Key.Section.Kind, parent.Key.Section.Name = "column_offsets", "offsets"
	parent.Key.Section.Category = "declared_column_offsets"
	if ref, ok := columnAssetRefForEmptyAdjacencyPin(pin, []mappedresource.Pin{parent}); !ok || ref.Length != 16 || ref.Offset != 2056 {
		t.Fatal("valid empty section did not reuse real offsets extent")
	}
	for name, mutate := range map[string]func(*mappedresource.Pin){
		"unknown_kind":    func(p *mappedresource.Pin) { p.Key.Kind = "unknown" },
		"negative_offset": func(p *mappedresource.Pin) { p.Key.Offset = -1 },
		"nonzero_bytes":   func(p *mappedresource.Pin) { p.Bytes = 1 },
		"checksum":        func(p *mappedresource.Pin) { p.Key.Checksum = 1 },
		"derived":         func(p *mappedresource.Pin) { p.Source = mappedresource.SourceDerivedMetadata },
		"foreign_part":    func(p *mappedresource.Pin) { p.Key.PartID++ },
		"foreign_root":    func(p *mappedresource.Pin) { p.Root = "/other" },
		"foreign_path":    func(p *mappedresource.Pin) { p.Path = "/other" },
		"foreign_scope":   func(p *mappedresource.Pin) { p.Scope.ID = "other" },
		"unknown_section": func(p *mappedresource.Pin) { p.Key.Section.Column = "other" },
		"encoding":        func(p *mappedresource.Pin) { p.Key.Encoding = "other" },
		"ordinal":         func(p *mappedresource.Pin) { p.Key.Section.Ordinal++ },
		"overlap":         func(p *mappedresource.Pin) { p.Key.Offset = 2057 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := pin
			mutate(&bad)
			if _, ok := columnAssetRefForEmptyAdjacencyPin(bad, []mappedresource.Pin{parent}); ok {
				t.Fatal("malformed section admitted")
			}
		})
	}
	if _, ok := columnAssetRefForEmptyAdjacencyPin(pin, nil); ok {
		t.Fatal("unpaired empty section admitted")
	}
	badParent := parent
	badParent.Key.Offset = int64(^uint64(0) >> 1)
	if _, ok := columnAssetRefForEmptyAdjacencyPin(pin, []mappedresource.Pin{badParent}); ok {
		t.Fatal("overflowing parent extent admitted")
	}
	parent.Key.Length, parent.Bytes = 0, 0
	if _, ok := columnAssetRefForEmptyAdjacencyPin(pin, []mappedresource.Pin{parent}); ok {
		t.Fatal("zero-byte parent admitted")
	}
}

func TestTypedGraphOwnerPackOnlyLifetime(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"original"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	opts := VectorIndexSearchOptions{IndexName: "embedding_graph", Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal}
	// Use the actual cache owner builder, retaining its independently owned
	// result rather than requesting a new current-generation cache lookup.
	p, _, err := col.openCollectionVectorIndexPreparedSearch(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.searcher != nil || p.pack == nil {
		t.Fatal("exact pack unexpectedly owns a lazy snapshot consumer")
	}
	for i := 0; i < 3; i++ {
		if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
			t.Fatal(err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{}); err != nil {
		t.Fatal(err)
	}
	var buffer VectorIndexSearchBuffer
	response, err := p.SearchWithBuffer(opts, columnVectorGraphNativeSearchStatsModeMinimal, &buffer)
	if err != nil || len(response.Results) != 1 || !bytes.Equal(response.Results[0].ID, []byte("base")) {
		t.Fatalf("retired pack result=%+v err=%v", response.Results, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.SearchWithBuffer(opts, columnVectorGraphNativeSearchStatsModeMinimal, &buffer); err == nil {
		t.Fatal("closed pack remained usable")
	}
}
