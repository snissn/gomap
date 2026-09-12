package collections

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestTypedGraphReadOwnerPublishedSuffix(t *testing.T) {
	col, _, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 32)
	limits := typedGraphPublicationLimits{Rows: 64, Tombstones: 64, ValueSlots: 256, OwnedBytes: 1 << 20}
	cold := typedGraphColdLimits{ManifestRecords: 1024, ManifestBytes: 1 << 20, AssetBytes: 8 << 20, DecodedTermBytes: 8 << 20}
	if err := col.reconcileTypedGraphPublication(limits, cold); err != nil {
		t.Fatal(err)
	}
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"changed"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	owner, err := col.openTypedGraphReadOwner(typedGraphReadOwnerLimits{Owners: 4, States: 2, StateBytes: 8 << 20, AssetBytes: 16 << 20, Cold: cold})
	if err != nil {
		t.Fatalf("coherent owner after accepted typed replacement: %v", err)
	}
	defer owner.Close()
	state := col.typedGraphPublicationSnapshot()
	if len(owner.overlay.rows) != 1 || &owner.overlay.rows[0] != &state.rows[0] || &owner.overlay.invNorms[0] != &state.invNorms[0] {
		t.Fatal("owner copied or cold-reconstructed publication rows/norms")
	}
	var buffer VectorIndexSearchBuffer
	results, _, err := owner.overlay.search(columns[0].Float32Vectors[0], 4, 16, 128, &buffer)
	if err != nil || len(results) != 4 {
		t.Fatalf("results=%v err=%v", results, err)
	}
	check := func(value, content string, count int) {
		t.Helper()
		cold, err := prepareTypedGraphBaseFilter(owner.overlay.base, HybridScalarFilter{IndexName: "path", Value: value}, typedGraphBaseFilterLimits{typedGraphFilterLimits: typedGraphFilterLimits{SourceIDs: 128, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 10000, InspectedEntries: 256}, Clauses: 4, PredicateBytes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		plan, err := bindTypedGraphBaseFilter(cold, owner.overlay, typedGraphFilterBindLimits{Rows: 64, IDBytes: 10000, ValueBytes: 10000, MappingWork: 10000, PredicateWork: 10000, RetainedBytes: 1 << 20, ExactScanRows: 4096})
		if err != nil {
			t.Fatal(err)
		}
		results, _, err := owner.overlay.searchPreparedFilter(plan, columns[0].Float32Vectors[0], count, 16, 128, &buffer)
		if err != nil || len(results) != count {
			t.Fatalf("filter %s results=%d err=%v", value, len(results), err)
		}
		fetched, err := owner.overlay.current.FetchDocumentsForVectorIndexSearchResults(results, DocumentFetchOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for _, doc := range fetched.Results {
			if !doc.Found || !bytes.Contains(doc.Document, []byte(`"content":"`+content+`"`)) {
				t.Fatalf("current document: %+v", doc)
			}
		}
	}
	check("new", "changed", 1)
	check("source", "content", 4)
	// The empty-base exception must not admit a nonempty base missing its
	// persisted reverse mapping. Remove only this private reader's binding.
	inverse := owner.overlay.base.reader.rowRefSource
	owner.overlay.base.reader.rowRefSource = nil
	_, inverseErr := prepareTypedGraphFilter(owner.overlay, HybridScalarFilter{IndexName: "path", Value: "source"}, typedGraphFilterLimits{SourceIDs: 128, SourceBytes: 10000, RetainedBytes: 10000, MappingWork: 10000, InspectedEntries: 256})
	owner.overlay.base.reader.rowRefSource = inverse
	if !errors.Is(inverseErr, errTypedGraphInverseRequired) {
		t.Fatalf("nonempty missing inverse admitted: %v", inverseErr)
	}
}

func TestTypedGraphReadOwnerStateObjectCharge(t *testing.T) {
	col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	limits := typedGraphOverlapLimits()
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 32, Tombstones: 32, ValueSlots: 128, OwnedBytes: 1 << 20}, limits.Cold); err != nil {
		t.Fatal(err)
	}
	first, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	state := first.state
	want := int64(reflect.TypeFor[typedGraphPublicationState]().Size()) + state.admittedPayloadBytes + int64(cap(state.rows))*int64(reflect.TypeFor[columnPhysicalVisibleRow]().Size()) + int64(state.valueSlots)*int64(reflect.TypeFor[columnDeclaredValue]().Size()) + int64(cap(state.invNorms))*4
	if first.stateBytes != want {
		t.Fatalf("state charge=%d want%d including object=%d", first.stateBytes, want, reflect.TypeFor[typedGraphPublicationState]().Size())
	}
	second, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	a := &col.collectionSchemaCoordinator().typedGraphOwners
	if first.state != second.state || a.stateBytes != want+first.descriptorBytes+first.backingBytes+second.descriptorBytes+second.backingBytes {
		t.Fatal("shared state object must be charged once")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if a.stateBytes != want+second.descriptorBytes+second.backingBytes {
		t.Fatal("state charge released while another owner retained it")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if a.stateBytes != 0 {
		t.Fatal("state charge retained after last owner")
	}
}

func TestTypedGraphReadOwnerOtherManagerPendingAndInstallGap(t *testing.T) {
	col, _, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 32)
	limits := typedGraphReadOwnerLimits{Owners: 4, States: 4, StateBytes: 8 << 20, AssetBytes: 16 << 20, Cold: typedGraphColdLimits{ManifestRecords: 1024, ManifestBytes: 1 << 20, AssetBytes: 8 << 20, DecodedTermBytes: 8 << 20}}
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 64, Tombstones: 64, ValueSlots: 256, OwnedBytes: 1 << 20}, limits.Cold); err != nil {
		t.Fatal(err)
	}
	other, err := NewCollectionManager(col.db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"other-pending"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
	if _, _, err := other.InsertTypedBatchWithStats([][]byte{[]byte("new")}, [][]byte{[]byte(`{"id":"new"}`)}, changed); err != nil {
		t.Fatal(err)
	}
	if col.typedGraphPublicationSnapshot().physicalRows != 0 {
		t.Fatal("fixture did not retain acknowledged pending input")
	}
	coord := col.collectionSchemaCoordinator()
	coord.typedPublicationDebtMu.Lock()
	buffered := coord.typedPublicationBuffered
	coord.typedPublicationDebtMu.Unlock()
	if buffered != 1 {
		t.Fatalf("buffered receipts=%d want=1", buffered)
	}
	owner, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	if owner.state.physicalRows != 1 {
		t.Fatal("owner omitted other-manager acknowledged pending input")
	}
	coord.typedPublicationDebtMu.Lock()
	buffered = coord.typedPublicationBuffered
	coord.typedPublicationDebtMu.Unlock()
	if buffered != 0 {
		t.Fatalf("drained buffered receipts=%d want=0", buffered)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	typedGraphPublicationAfterAcceptedHook.Lock()
	typedGraphPublicationAfterAcceptedHook.fn = func(p *typedGraphPublicationCandidate) {
		if p.coord == col.collectionSchemaCoordinator() && once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}
	typedGraphPublicationAfterAcceptedHook.Unlock()
	defer func() {
		typedGraphPublicationAfterAcceptedHook.Lock()
		typedGraphPublicationAfterAcceptedHook.fn = nil
		typedGraphPublicationAfterAcceptedHook.Unlock()
	}()
	written := make(chan error, 1)
	go func() { _, err := other.ReplaceTypedBatch(ids[:1], retained[:1], changed); written <- err }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("accepted-root gap not reached")
	}
	type opened struct {
		owner *typedGraphReadOwner
		err   error
	}
	done, started := make(chan opened, 1), make(chan struct{})
	go func() { close(started); owner, err := col.openTypedGraphReadOwner(limits); done <- opened{owner, err} }()
	<-started
	var early *opened
	select {
	case result := <-done:
		early = &result
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if early != nil {
		if early.owner != nil {
			_ = early.owner.Close()
			t.Fatal("accepted-root gap returned stale successful owner")
		}
		if !errors.Is(early.err, ErrVectorIndexSnapshotMismatch) {
			t.Fatalf("gap error=%v", early.err)
		}
	} else {
		result := <-done
		if result.err != nil {
			t.Fatal(result.err)
		}
		defer result.owner.Close()
		if result.owner.state.physicalRows != 2 {
			t.Fatal("serialized owner missed installed replacement")
		}
	}
}

func TestTypedGraphReadOwnerSmallBase(t *testing.T) {
	for _, n := range []int{0, 1} {
		t.Run(map[int]string{0: "empty", 1: "singleton"}[n], func(t *testing.T) {
			_, db, col := openTypedMinimaCollection(t)
			defer db.Close()
			columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"before"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}}}
			if n == 1 {
				if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}, columns); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatal(err)
			}
			limits := typedGraphReadOwnerLimits{Owners: 2, States: 2, StateBytes: 1 << 20, AssetBytes: 8 << 20, Cold: typedGraphColdLimits{ManifestRecords: 128, ManifestBytes: 128 << 10, AssetBytes: 4 << 20, DecodedTermBytes: 4 << 20}}
			if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 8, Tombstones: 8, ValueSlots: 32, OwnedBytes: 4096}, limits.Cold); err != nil {
				t.Fatal(err)
			}
			if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("delta")}, [][]byte{[]byte(`{"id":"delta"}`)}, columns); err != nil {
				t.Fatal(err)
			}
			owner, err := col.openTypedGraphReadOwner(limits)
			if err != nil {
				t.Fatal(err)
			}
			defer owner.Close()
			// Both an empty source and a shared singleton reject before acquiring
			// any mapped source. The reader computes its key only once internally.
			before := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
			snap := db.AcquireSnapshot()
			catalog, err := loadCollectionCatalog(snap, col.collectionName())
			if err != nil {
				t.Fatal(err)
			}
			graph, sourceView, err := catalog.typedGraphBase.readerView(col, snap)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			r, admissionErr := col.openColumnVectorGraphPhysicalRowReaderFromView(snap, catalog.meta.VectorIndexes[0], graph, sourceView, columnVectorGraphPhysicalRowReaderOptions{admitSources: func(keyBytes int) error {
				calls++
				if (keyBytes == 0) != (n == 0) {
					t.Errorf("unexpected key length %d for rows %d", keyBytes, n)
				}
				return errTypedGraphOwnerBudget
			}})
			if err := snap.Close(); err != nil {
				t.Fatal(err)
			}
			if r != nil || calls != 1 || !errors.Is(admissionErr, errTypedGraphOwnerBudget) {
				t.Fatalf("source admission reader=%v calls=%d err=%v", r, calls, admissionErr)
			}
			after := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
			if before.CacheBuilds != after.CacheBuilds || before.Refs != after.Refs {
				t.Fatal("source admission rejection acquired resources")
			}
			if _, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{}); err != nil {
				for _, pin := range mappedresource.GlobalPinSummary() {
					if columnAssetMappedResourcePinMatchesRoot(pin, db.ColumnAssetRootDir()) {
						if _, ok := columnAssetRefForMappedResourceKey(pin.Key); !ok {
							t.Logf("unconvertible empty-base pin: %+v", pin)
						}
					}
				}
				t.Fatal(err)
			}
			var buffer VectorIndexSearchBuffer
			results, _, err := owner.overlay.search(columns[0].Float32Vectors[0], 2, 8, 32, &buffer)
			if err != nil || len(results) != n+1 {
				t.Fatalf("small-base results=%v err=%v", results, err)
			}
			cold, err := prepareTypedGraphBaseFilter(owner.overlay.base, HybridScalarFilter{IndexName: "path", Value: "p"}, typedGraphBaseFilterLimits{typedGraphFilterLimits: typedGraphFilterLimits{SourceIDs: 32, SourceBytes: 10000, RetainedBytes: 10000, MappingWork: 10000, InspectedEntries: 64}, Clauses: 4, PredicateBytes: 1024})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := bindTypedGraphBaseFilter(cold, owner.overlay, typedGraphFilterBindLimits{Rows: 8, IDBytes: 10000, ValueBytes: 10000, MappingWork: 10000, PredicateWork: 10000, RetainedBytes: 10000, ExactScanRows: 4096})
			if err != nil {
				t.Fatal(err)
			}
			results, stats, err := owner.overlay.searchPreparedFilter(plan, columns[0].Float32Vectors[0], 2, 8, 32, &buffer)
			if err != nil || len(results) != n+1 || !stats.FilteredExact || stats.Base.Edges != 0 {
				t.Fatalf("small-base exact results=%v stats=%+v err=%v", results, stats, err)
			}
			fetched, err := owner.overlay.current.FetchDocumentsForVectorIndexSearchResults(results, DocumentFetchOptions{})
			if err != nil || len(fetched.Results) != n+1 {
				t.Fatalf("small-base documents=%+v err=%v", fetched, err)
			}
		})
	}
}

func TestTypedGraphReadOwnerReopenRetirementAndLimits(t *testing.T) {
	col, fixtureBase, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 32)
	dir := col.db.Dir()
	// Persist a real nonempty suffix before normal Open. No recovery hook or
	// test state seeding creates the publication state used by this owner.
	persisted := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"persisted-before-reopen"}}, {Name: "user", Strings: columns[2].Strings[:1]}, {Name: "path", Strings: columns[3].Strings[:1]}}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], persisted); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := fixtureBase.Close(); err != nil {
		t.Fatal(err)
	}
	if err := col.db.Close(); err != nil {
		t.Fatal(err)
	}
	db := openTypedMinimaDB(t, dir)
	defer db.Close()
	col, err := NewCollectionManager(db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	publication := typedGraphPublicationLimits{Rows: 64, Tombstones: 64, ValueSlots: 256, OwnedBytes: 1 << 20}
	limits := typedGraphReadOwnerLimits{Owners: 3, States: 2, StateBytes: 8 << 20, AssetBytes: 16 << 20, Cold: typedGraphColdLimits{ManifestRecords: 1024, ManifestBytes: 1 << 20, AssetBytes: 8 << 20, DecodedTermBytes: 8 << 20}}
	if owner, err := col.openTypedGraphReadOwner(limits); owner != nil || !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("Open silently bootstrapped: %v", err)
	}
	if err := col.reconcileTypedGraphPublication(publication, limits.Cold); err != nil {
		t.Fatal(err)
	}
	if state := col.typedGraphPublicationSnapshot(); state.physicalRows != 1 || len(state.rows) != 1 || state.rows[0].Values[1].String != "persisted-before-reopen" {
		t.Fatal("normal Open bootstrap omitted persisted suffix")
	}
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"pinned-current"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
	write := func(content string) {
		t.Helper()
		changed[1].Strings[0] = content
		if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
			t.Fatal(err)
		}
		if err := col.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	write("pinned-current")
	first, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	other, err := NewCollectionManager(db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	second, err := other.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	a := &col.collectionSchemaCoordinator().typedGraphOwners
	if a.owners != 2 || len(a.states) != 1 || a.stateBytes != first.stateBytes+first.descriptorBytes+first.backingBytes+second.descriptorBytes+second.backingBytes || first.state != second.state {
		t.Fatalf("same-state accounting: owners=%d states=%d bytes=%d", a.owners, len(a.states), a.stateBytes)
	}
	write("second-state")
	third, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	if owner, err := col.openTypedGraphReadOwner(limits); owner != nil || !errors.Is(err, errTypedGraphOwnerBudget) {
		t.Fatalf("owner cap: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	write("third-state")
	if owner, err := col.openTypedGraphReadOwner(limits); owner != nil || !errors.Is(err, errTypedGraphOwnerBudget) {
		t.Fatalf("retired-state cap: %v", err)
	}
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
	// Actual cutovers and default GC retire the current snapshot's ordinary
	// typed columns before its first document fetch; no caller protection refs.
	for i := 0; i < 3; i++ {
		if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
			t.Fatal(err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if err := col.reconcileTypedGraphPublication(publication, limits.Cold); err != nil {
			t.Fatal(err)
		}
		write("after-cutover")
	}
	if stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{}); !rootpublication.StableRelativeNamespaceSupported() {
		if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) || stats.SegmentsDeleted != 0 {
			t.Fatalf("unsupported destructive GC: %+v err=%v", stats, err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	fetched, err := first.overlay.current.FetchDocumentsByID(ids[:1], DocumentFetchOptions{})
	if err != nil || len(fetched.Results) != 1 || !bytes.Contains(fetched.Results[0].Document, []byte(`"content":"pinned-current"`)) {
		t.Fatalf("lazy retired current fetch: %+v err=%v", fetched, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if a.owners != 0 || len(a.states) != 0 || a.stateBytes != 0 || a.assetBytes != 0 {
		t.Fatal("closed owners retain accounting")
	}
	write("budget-state")
	for _, term := range []string{"state", "asset", "metadata", "decoded"} {
		bounded := limits
		wantErr := errTypedGraphOverlayFoldNeeded
		switch term {
		case "state":
			bounded.StateBytes = 1
			wantErr = errTypedGraphOwnerBudget
		case "asset":
			bounded.AssetBytes = 1
			wantErr = errTypedGraphOwnerBudget
		case "metadata":
			bounded.Cold.ManifestBytes = 1
		case "decoded":
			bounded.Cold.DecodedTermBytes = 1
		}
		if owner, err := col.openTypedGraphReadOwner(bounded); owner != nil || !errors.Is(err, wantErr) {
			t.Fatalf("%s limit: %v", term, err)
		}
		if a.owners != 0 || len(a.states) != 0 {
			t.Fatal("failed open retained owner")
		}
	}
	last, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	filter, err := prepareTypedGraphFilter(last.overlay, HybridScalarFilter{IndexName: "path", Value: "new"}, typedGraphFilterLimits{SourceIDs: 128, SourceBytes: 10000, RetainedBytes: 10000, MappingWork: 10000, InspectedEntries: 256})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var buffer VectorIndexSearchBuffer
	if results, _, err := last.overlay.search(columns[0].Float32Vectors[0], 4, 16, 128, &buffer); err == nil || len(results) != 0 {
		t.Fatalf("DB-close did not invalidate reader: %v", err)
	}
	if results, _, err := last.overlay.searchPreparedFilter(filter, columns[0].Float32Vectors[0], 4, 16, 128, &buffer); err == nil || len(results) != 0 {
		t.Fatalf("DB-close allowed prepared search: %v", err)
	}
	if _, err := last.overlay.current.FetchDocumentsByID(ids[:1], DocumentFetchOptions{}); err == nil {
		t.Fatal("DB-close allowed materialization")
	}
	if _, err := prepareTypedGraphFilter(last.overlay, HybridScalarFilter{IndexName: "path", Value: "new"}, typedGraphFilterLimits{SourceIDs: 128, SourceBytes: 10000, RetainedBytes: 10000, MappingWork: 10000, InspectedEntries: 256}); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("DB-close allowed filter preparation: %v", err)
	}
	if err := last.Close(); err != nil {
		t.Fatal(err)
	}
	if a.owners != 0 {
		t.Fatal("DB-close/owner-close leaked accounting")
	}
}

func TestTypedGraphReadOwnerEmptyDocumentIDPin(t *testing.T) {
	key := mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: "n", Kind: string(ColumnAssetKindTCS1TypedColumnPart), Generation: 1, PartID: 2, FileID: 3, Offset: 72, Version: 4, Encoding: "raw_bytes_offsets", Section: mappedresource.Section{Kind: "column_values", Name: "values", Category: "declared_column_values", Column: columnVectorGraphDocumentIDStateColumnName}}
	pin := mappedresource.Pin{Key: key, Scope: mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: columnVectorGraphDocumentIDStateScopeID}, Source: mappedresource.SourceMapped, Root: "/root", Path: "/root/asset"}
	parent := pin
	parent.Key.Offset, parent.Key.Length, parent.Bytes = 64, 8, 8
	parent.Key.Section.Kind, parent.Key.Section.Name, parent.Key.Section.Category = "column_offsets", "offsets", "declared_column_offsets"
	if ref, ok := columnAssetRefForEmptyGraphValuesPin(pin, []mappedresource.Pin{parent}); !ok || ref.Length != 8 {
		t.Fatal("empty document IDs did not use real offsets extent")
	}
	if _, ok := columnAssetRefForEmptyGraphValuesPin(pin, nil); ok {
		t.Fatal("unpaired empty document IDs admitted")
	}
	for _, mutate := range []func(*mappedresource.Pin){func(p *mappedresource.Pin) { p.Scope.ID = columnVectorGraphAdjacencyStateSourceScopeID }, func(p *mappedresource.Pin) { p.Source = mappedresource.SourceDerivedMetadata }, func(p *mappedresource.Pin) { p.Key.Section.Column = "unknown" }} {
		bad := pin
		mutate(&bad)
		if _, ok := columnAssetRefForEmptyGraphValuesPin(bad, []mappedresource.Pin{parent}); ok {
			t.Fatal("unknown empty document ID tuple admitted")
		}
	}
}
