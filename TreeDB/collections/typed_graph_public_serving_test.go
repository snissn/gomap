package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// The pause is after the old captured holder exists and its snapshot/barrier
// have closed, but before it is installed in this handle's cache.
func TestTypedGraphPublicEnsureStaleCapturedKeeper(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	dir, name, index := col.db.Dir(), col.Name(), base.indexName
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	if err := col.db.Close(); err != nil {
		t.Fatal(err)
	}
	db := openTypedMinimaDB(t, dir)
	defer db.Close()
	old, err := NewCollectionManager(db).OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	current, err := NewCollectionManager(db).OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	opts := typedGraphPublicTestOptions()
	captured := make(chan *collectionVectorIndexPreparedSearch, 1)
	release := make(chan struct{})
	var released, paused atomic.Bool
	defer func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}()
	collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
	collectionVectorIndexPreparedSearchBuildHookForTest.afterBuild = func(p *collectionVectorIndexPreparedSearch) {
		if p != nil && p.collection == old && paused.CompareAndSwap(false, true) {
			captured <- p
			<-release
		}
	}
	collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
	defer func() {
		collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
		collectionVectorIndexPreparedSearchBuildHookForTest.afterBuild = nil
		collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
	}()
	done := make(chan error, 1)
	go func() { done <- old.EnsureColumnGraphServing(context.Background(), index, opts) }()
	var rejected *collectionVectorIndexPreparedSearch
	select {
	case rejected = <-captured:
	case <-time.After(10 * time.Second):
		t.Fatal("no post-capture pause")
	}
	resources := rejected.capturedBase
	if err := current.EnsureColumnGraphServing(context.Background(), index, opts); err != nil {
		t.Fatal(err)
	}
	query := VectorIndexSearchOptions{IndexName: index, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal}
	var buffer VectorIndexSearchBuffer
	response, held, err := current.SearchVectorIndexWithBufferReadView(query, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"accepted-suffix"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
	if _, err := current.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	if err := current.FoldColumnGraphServing(context.Background(), index); err != nil {
		t.Fatal(err)
	}
	accounting := resources.accounting
	accounting.Lock()
	ownersBefore, bytesBefore := accounting.baseOwners, accounting.baseAssetBytes
	descriptorsBefore, backingBefore := accounting.baseDescriptorBytes, accounting.baseBackingBytes
	accounting.Unlock()
	released.Store(true)
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, ErrConcurrentMutation) {
			t.Fatalf("stale ensure=%v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stale ensure blocked")
	}
	if !rejected.closed || resources.accounting != nil || resources.pin != nil || resources.ref != nil {
		t.Fatal("rejected captured keeper retained its pin/accounting")
	}
	accounting.Lock()
	releasedExactly := accounting.baseOwners == ownersBefore-1 && accounting.baseAssetBytes == bytesBefore-resources.assetBytes && accounting.baseDescriptorBytes == descriptorsBefore-resources.descriptorBytes && accounting.baseBackingBytes == backingBefore-resources.backingBytes
	accounting.Unlock()
	if !releasedExactly {
		t.Fatal("stale ensure failed to release exactly its own accounted keeper")
	}
	fetched, err := held.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
	if err != nil || len(fetched.Results) != 1 || bytes.Contains(fetched.Results[0].Document, []byte("accepted-suffix")) {
		t.Fatalf("held old owner=%+v err=%v", fetched.Results, err)
	}
	query.DeclaredScalarFilter = &HybridScalarFilter{IndexName: "path", Value: "new"}
	var latestBuffer VectorIndexSearchBuffer
	latest, view, err := current.SearchVectorIndexWithBufferReadView(query, &latestBuffer)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	docs, err := view.FetchDocumentsForVectorIndexSearchResults(latest.Results, DocumentFetchOptions{})
	if err != nil || len(docs.Results) != 1 || !bytes.Equal(docs.Results[0].ID, ids[0]) || !bytes.Contains(docs.Results[0].Document, []byte("accepted-suffix")) {
		t.Fatalf("current owner=%+v err=%v", docs.Results, err)
	}
}

func typedGraphPublicTestOptions() ColumnGraphServingOptions {
	opts := ColumnGraphServingOptions{Publication: ColumnGraphPublicationLimits{Rows: 128, Tombstones: 128, ValueSlots: 512, OwnedBytes: 4 << 20, EncodedOutputBytes: 1 << 20}, Owners: typedGraphOverlapLimits(), CandidateOutput: typedGraphFoldTestAssetLimits(), Maintenance: typedGraphTestWorkEpochLimits(), Filter: ColumnGraphFilterLimits{SourceIDs: 1024, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 100000, InspectedEntries: 1024}, FoldRows: 128, SearchCandidates: 1024}
	opts.Owners.StateBytes = 128 << 20
	opts.Maintenance.RetainedBytes = 256 << 20
	return opts
}

func requireTypedGraphPublicServingTest(t testing.TB) {
	t.Helper()
	requireTypedGraphPreparedHolderTest(t)
	requireColumnAssetExactDestructiveGCTest(t)
}

func TestTypedGraphPublicUnsupportedPreparedAdmission(t *testing.T) {
	if columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("exercises hosts without mmap_direct prepared holders")
	}
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, typedGraphPublicTestOptions()); !errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
		t.Fatalf("unsupported prepared admission: %v", err)
	}
	coord := col.collectionSchemaCoordinator()
	if state := coord.typedPublication.Load(); state != nil && state.servingAdmitted {
		t.Fatal("unsupported prepared holder admitted serving")
	}
	seq, system := dbCommitSeqAndSystemRoot(col.db)
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"reject"}}, {Name: "user", Strings: []string{"reject"}}, {Name: "path", Strings: []string{"reject"}}}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("unsupported admission allowed mutation: %v", err)
	}
	if next, root := dbCommitSeqAndSystemRoot(col.db); next != seq || root != system {
		t.Fatal("rejected write changed authority")
	}
	account := &coord.typedGraphOwners
	account.Lock()
	defer account.Unlock()
	if account.baseOwners != 0 || account.baseAssetBytes != 0 || account.baseDescriptorBytes != 0 || account.baseBackingBytes != 0 {
		t.Fatal("unsupported prepared holder retained accounting")
	}
}

func TestTypedGraphPublicFoldPublicationAvailability(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, typedGraphPublicTestOptions()); err != nil {
		t.Fatal(err)
	}
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"before-fold"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	var oldBuffer VectorIndexSearchBuffer
	oldResults, oldView, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal, DeclaredScalarFilter: &HybridScalarFilter{IndexName: "path", Value: "new"}}, &oldBuffer)
	if err != nil {
		t.Fatal(err)
	}
	defer oldView.Close()
	beforeFold := workstats.Read().Fold
	var observed bool
	typedGraphPublicationAfterAcceptedHook.Lock()
	typedGraphPublicationAfterAcceptedHook.foldAfterInstall = func(c *Collection) {
		if c != col {
			return
		}
		observed = true
		atInstall := workstats.Read().Fold
		if atInstall.Publications-beforeFold.Publications != 1 || atInstall.Public.Completed != beforeFold.Public.Completed {
			t.Errorf("publication and return conflated: before=%+v atInstall=%+v", beforeFold, atInstall)
		}
		query := VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal, DeclaredScalarFilter: &HybridScalarFilter{IndexName: "path", Value: "new"}}
		var buffer VectorIndexSearchBuffer
		response, view, err := c.SearchVectorIndexWithBufferReadView(query, &buffer)
		if err != nil {
			t.Errorf("public read between install and checkpoint: %v", err)
		} else {
			assertTypedGraphMaterializerReuse(t, view, true)
			fetched, fetchErr := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
			closeErr := view.Close()
			if fetchErr != nil || closeErr != nil || len(fetched.Results) != 1 || !bytes.Contains(fetched.Results[0].Document, []byte("before-fold")) {
				t.Errorf("post-install owner=%+v fetch=%v close=%v", fetched.Results, fetchErr, closeErr)
			}
		}
		changed[1].Strings[0] = "after-install"
		if _, err := c.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
			t.Errorf("public write between install and checkpoint: %v", err)
		}
	}
	typedGraphPublicationAfterAcceptedHook.Unlock()
	defer func() {
		typedGraphPublicationAfterAcceptedHook.Lock()
		typedGraphPublicationAfterAcceptedHook.foldAfterInstall = nil
		typedGraphPublicationAfterAcceptedHook.Unlock()
	}()
	if err := col.FoldColumnGraphServing(context.Background(), base.indexName); err != nil {
		t.Fatal(err)
	}
	afterFold := workstats.Read().Fold
	if afterFold.Public.Completed-beforeFold.Public.Completed != 1 || afterFold.Public.Errors != beforeFold.Public.Errors || afterFold.Renew.Completed-beforeFold.Renew.Completed != 1 {
		t.Fatalf("fold completion=%+v before=%+v", afterFold, beforeFold)
	}
	if !observed {
		t.Fatal("missing actual post-install boundary")
	}
	doc, err := col.Get(ids[0])
	if err != nil || !bytes.Contains(doc, []byte("after-install")) {
		t.Fatalf("post-install replacement not visible: %s err=%v", doc, err)
	}
	oldDocs, err := oldView.FetchDocumentsForVectorIndexSearchResults(oldResults.Results, DocumentFetchOptions{})
	if err != nil || len(oldDocs.Results) != 1 || !bytes.Contains(oldDocs.Results[0].Document, []byte("before-fold")) {
		t.Fatalf("old held owner=%+v err=%v", oldDocs.Results, err)
	}
}

func TestTypedGraphPublicFoldPostInstallAckCrash(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	const childEnv = "GOMAP_PUBLIC_FOLD_POST_INSTALL_ACK_DIR"
	if dir := os.Getenv(childEnv); dir != "" {
		var indexedJSON atomic.Uint64
		setColumnVectorGraphCanonicalRowsTestHook(func() { indexedJSON.Add(1) })
		db := openTypedMinimaDB(t, dir)
		col, err := NewCollectionManager(db).OpenCollection("minima")
		if err != nil {
			t.Fatal(err)
		}
		if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", typedGraphPublicTestOptions()); err != nil {
			t.Fatal(err)
		}
		columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"before-fold"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
		if _, err := col.ReplaceTypedBatch([][]byte{[]byte("row-00000")}, [][]byte{[]byte(`{"id":"row-00000"}`)}, columns); err != nil {
			t.Fatal(err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		blocked := make(chan struct{})
		durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Root == dir && event.Resource == durabilitycut.ResourceSeal && event.Point == durabilitycut.BeforePublicationSealWrite {
				<-blocked
			}
			return nil
		})
		typedGraphPublicationAfterAcceptedHook.Lock()
		typedGraphPublicationAfterAcceptedHook.foldAfterInstall = func(c *Collection) {
			columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"post-install-ack"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
			if _, err := c.ReplaceTypedBatch([][]byte{[]byte("row-00000")}, [][]byte{[]byte(`{"id":"row-00000"}`)}, columns); err != nil {
				t.Fatal(err)
			}
			if indexedJSON.Load() != 0 {
				t.Fatal("post-install ack entered indexed JSON extraction")
			}
			os.Exit(76) // durable public acknowledgement, before fold Checkpoint/Close
		}
		typedGraphPublicationAfterAcceptedHook.Unlock()
		if err := col.FoldColumnGraphServing(context.Background(), "embedding_graph"); err != nil {
			t.Fatal(err)
		}
		t.Fatal("missing post-install process cut")
	}
	col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	dir := col.db.Dir()
	state, closeState, err := col.loadColumnStoreCompactionState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	oldBaseLSN := state.catalog.typedGraphBase.meta.Options.ColumnStore.RecoveryAuthoritativeAppliedCommandLSN
	closeState()
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	if err := col.db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := col.db.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestTypedGraphPublicFoldPostInstallAckCrash$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), childEnv+"="+dir)
	output, err := cmd.CombinedOutput()
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.ExitCode() != 76 {
		t.Fatalf("post-install ack child: %v\n%s", err, output)
	}
	var indexedJSON atomic.Uint64
	restore := setColumnVectorGraphCanonicalRowsTestHook(func() { indexedJSON.Add(1) })
	defer restore()
	db := openTypedMinimaDB(t, dir)
	defer db.Close()
	reopened, err := NewCollectionManager(db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	state, closeState, err = reopened.loadColumnStoreCompactionState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	recoveredBaseLSN := state.catalog.typedGraphBase.meta.Options.ColumnStore.RecoveryAuthoritativeAppliedCommandLSN
	closeState()
	if recoveredBaseLSN != oldBaseLSN {
		t.Fatalf("recovered base LSN=%d want pre-fold%d", recoveredBaseLSN, oldBaseLSN)
	}
	if err := reopened.EnsureColumnGraphServing(context.Background(), "embedding_graph", typedGraphPublicTestOptions()); err != nil {
		t.Fatal(err)
	}
	var buffer VectorIndexSearchBuffer
	results, view, err := reopened.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: "embedding_graph", Query: []float32{1, 0, 0, 0, 0, 0, 0, 0}, TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal, DeclaredScalarFilter: &HybridScalarFilter{IndexName: "path", Value: "new"}}, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	docs, err := view.FetchDocumentsForVectorIndexSearchResults(results.Results, DocumentFetchOptions{})
	if err != nil || len(docs.Results) != 1 || string(docs.Results[0].ID) != "row-00000" || !bytes.Contains(docs.Results[0].Document, []byte("post-install-ack")) {
		t.Fatalf("post-install ack recovery=%+v err=%v", docs.Results, err)
	}
	if indexedJSON.Load() != 0 {
		t.Fatal("post-install replay/search entered indexed JSON extraction")
	}
}

func TestTypedGraphPublicHealthyEnsureConcurrentOwner(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, _, _, columns, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	opts := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, opts); err != nil {
		t.Fatal(err)
	}
	drained, releaseRead := make(chan struct{}), make(chan struct{})
	admitted, releaseEnsure := make(chan struct{}), make(chan struct{})
	var readReleased, ensureReleased atomic.Bool
	defer func() {
		if readReleased.CompareAndSwap(false, true) {
			close(releaseRead)
		}
		if ensureReleased.CompareAndSwap(false, true) {
			close(releaseEnsure)
		}
	}()
	typedGraphOwnerAfterSnapshotHook.Lock()
	typedGraphOwnerAfterSnapshotHook.afterDrain = func(c *Collection) {
		if c == col {
			close(drained)
			<-releaseRead
		}
	}
	typedGraphOwnerAfterSnapshotHook.Unlock()
	typedGraphPublicationAfterAcceptedHook.Lock()
	typedGraphPublicationAfterAcceptedHook.reconcileBeforeCapture = func(c *Collection) {
		if c == col {
			close(admitted)
			<-releaseEnsure
		}
	}
	typedGraphPublicationAfterAcceptedHook.Unlock()
	defer func() {
		typedGraphOwnerAfterSnapshotHook.Lock()
		typedGraphOwnerAfterSnapshotHook.afterDrain = nil
		typedGraphOwnerAfterSnapshotHook.Unlock()
		typedGraphPublicationAfterAcceptedHook.Lock()
		typedGraphPublicationAfterAcceptedHook.reconcileBeforeCapture = nil
		typedGraphPublicationAfterAcceptedHook.Unlock()
	}()
	readDone := make(chan error, 1)
	go func() {
		var buffer VectorIndexSearchBuffer
		response, view, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal}, &buffer)
		if err == nil {
			_, err = view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
			err = errors.Join(err, view.Close())
		}
		readDone <- err
	}()
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("owner did not drain")
	}
	ensureDone := make(chan error, 1)
	go func() { ensureDone <- col.EnsureColumnGraphServing(context.Background(), base.indexName, opts) }()
	select {
	case <-admitted:
	case <-time.After(10 * time.Second):
		t.Fatal("ensure did not reach exact capture boundary")
	}
	readReleased.Store(true)
	close(releaseRead)
	select {
	case err := <-readDone:
		if err != nil {
			t.Errorf("healthy same-authority ensure rejected concurrent owner: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("concurrent owner blocked")
	}
	ensureReleased.Store(true)
	close(releaseEnsure)
	select {
	case err := <-ensureDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ensure blocked")
	}
}

func TestTypedGraphPublicFoldStateInstallConflictFencesWrites(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	opts := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, opts); err != nil {
		t.Fatal(err)
	}
	coord := col.collectionSchemaCoordinator()
	typedGraphPublicationAfterAcceptedHook.Lock()
	typedGraphPublicationAfterAcceptedHook.foldBeforeStateInstall = func(c *Collection) {
		old := coord.typedPublication.Load()
		replacement := *old
		coord.typedPublication.Store(&replacement)
	}
	typedGraphPublicationAfterAcceptedHook.Unlock()
	defer func() {
		typedGraphPublicationAfterAcceptedHook.Lock()
		typedGraphPublicationAfterAcceptedHook.foldBeforeStateInstall = nil
		typedGraphPublicationAfterAcceptedHook.Unlock()
	}()
	if err := col.FoldColumnGraphServing(context.Background(), base.indexName); !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("state install conflict=%v", err)
	}
	if state := coord.typedPublication.Load(); state == nil || !state.invalid {
		t.Fatal("applied publication conflict left write admission healthy")
	}
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"rejected"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err == nil {
		t.Fatal("write admitted against incompatible post-apply state")
	}
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, opts); err != nil {
		t.Fatalf("explicit recovery: %v", err)
	}
}

func TestTypedGraphPublicFoldPostCaptureSuffixAndDebt(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, typedGraphPublicTestOptions()); err != nil {
		t.Fatal(err)
	}
	var expected typedGraphPublicationCost
	var encoded, candidateBytes int64
	var attempts int64
	coord := col.collectionSchemaCoordinator()
	capturedColumns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[7:8]}, {Name: "content", Strings: []string{"captured-debt"}}, {Name: "user", Strings: []string{"captured"}}, {Name: "path", Strings: []string{"captured"}}}
	if _, err := col.ReplaceTypedBatch(ids[7:8], retained[7:8], capturedColumns); err != nil {
		t.Fatal(err)
	}
	unlock := col.lockCollectionSchemaWrite()
	drainErr := col.flushCollectionWriteDomainsForSchemaMutation()
	unlock()
	if drainErr != nil {
		t.Fatal(drainErr)
	}
	capturedCost := typedGraphStateCost(coord.typedPublication.Load())
	if capturedCost.rows == 0 || capturedCost.bytes == 0 {
		t.Fatal("fixture lacks nonzero captured debt")
	}
	typedGraphPublicationAfterAcceptedHook.Lock()
	typedGraphPublicationAfterAcceptedHook.foldAfterCapture = func(c *Collection) {
		changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"post-capture"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
		if _, err := c.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
			t.Fatal(err)
		}
		if err := c.Delete(ids[1]); err != nil {
			t.Fatal(err)
		}
		if err := c.Delete(ids[2]); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.InsertTypedBatchWithStats(ids[2:3], retained[2:3], changed); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.InsertTypedBatchWithStats([][]byte{[]byte("growing")}, [][]byte{[]byte(`{"id":"growing"}`)}, changed); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.InsertTypedBatchWithStats([][]byte{[]byte("growing-again")}, [][]byte{[]byte(`{"id":"growing-again"}`)}, changed); err != nil {
			t.Fatal(err)
		}
		// Drain exactly as ordinary acquisition does before recording live cost.
		unlock := c.lockCollectionSchemaWrite()
		err := c.flushCollectionWriteDomainsForSchemaMutation()
		unlock()
		if err != nil {
			t.Fatal(err)
		}
		expected = typedGraphStateCost(coord.typedPublication.Load())
		expected.subtract(capturedCost)
	}
	typedGraphPublicationAfterAcceptedHook.foldBeforeStateInstall = func(c *Collection) {
		coord.typedPublicationDebtMu.Lock()
		encoded, candidateBytes, attempts = coord.typedPublicationEncodedBytes, coord.typedGraphCandidateBytes, coord.typedGraphCandidateAttempts
		coord.typedPublicationDebtMu.Unlock()
	}
	typedGraphPublicationAfterAcceptedHook.foldAfterInstall = func(c *Collection) {
		coord.typedPublicationDebtMu.Lock()
		defer coord.typedPublicationDebtMu.Unlock()
		if coord.typedPublicationDebt != expected || coord.typedPublicationPending != (typedGraphPublicationCost{}) || coord.typedPublicationEncodedBytes != encoded || coord.typedGraphCandidateBytes != candidateBytes || coord.typedGraphCandidateAttempts != attempts {
			t.Error("fold installation changed pending/attempt debt or lost post-capture logical cost")
		}
	}
	typedGraphPublicationAfterAcceptedHook.Unlock()
	defer func() {
		typedGraphPublicationAfterAcceptedHook.Lock()
		typedGraphPublicationAfterAcceptedHook.foldAfterCapture = nil
		typedGraphPublicationAfterAcceptedHook.foldAfterInstall = nil
		typedGraphPublicationAfterAcceptedHook.foldBeforeStateInstall = nil
		typedGraphPublicationAfterAcceptedHook.Unlock()
	}()
	if err := col.FoldColumnGraphServing(context.Background(), base.indexName); err != nil {
		t.Fatal(err)
	}
	var buffer VectorIndexSearchBuffer
	response, view, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 4, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal, DeclaredScalarFilter: &HybridScalarFilter{IndexName: "path", Value: "new"}}, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	assertTypedGraphMaterializerReuse(t, view, true)
	docs, err := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
	want := map[string]bool{string(ids[0]): true, string(ids[2]): true, "growing": true, "growing-again": true}
	if err != nil || len(docs.Results) != len(want) {
		t.Fatalf("post-capture results=%+v err=%v", docs.Results, err)
	}
	for _, doc := range docs.Results {
		if !want[string(doc.ID)] || !bytes.Contains(doc.Document, []byte("post-capture")) {
			t.Fatalf("unexpected suffix document=%+v", doc)
		}
		delete(want, string(doc.ID))
	}
	if doc, err := col.Get(ids[1]); err != nil || doc != nil {
		t.Fatalf("deleted ID revived: %s err=%v", doc, err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	typedGraphPublicationAfterAcceptedHook.Lock()
	typedGraphPublicationAfterAcceptedHook.foldAfterCapture = nil
	typedGraphPublicationAfterAcceptedHook.foldAfterInstall = nil
	typedGraphPublicationAfterAcceptedHook.foldBeforeStateInstall = nil
	typedGraphPublicationAfterAcceptedHook.Unlock()
	if err := col.FoldColumnGraphServing(context.Background(), base.indexName); err != nil {
		t.Fatal(err)
	}
	state := coord.typedPublication.Load()
	if state.servingBase.graph.RowCount != 9 || state.controlEncodedBytes <= 0 || state.deletePartEncodedBytes <= 0 || state.manifestEncodedBytes[0] <= 0 || state.manifestEncodedBytes[1] <= 0 || state.manifestEncodedBytes[2] <= 0 {
		t.Fatalf("grown base rows=%d or invalid encoded bounds", state.servingBase.graph.RowCount)
	}
	var grownBuffer VectorIndexSearchBuffer
	grown, grownView, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 16, EfSearch: 16, StatsMode: VectorIndexSearchStatsModeMinimal}, &grownBuffer)
	if err != nil {
		t.Fatal(err)
	}
	defer grownView.Close()
	grownDocs, err := grownView.FetchDocumentsForVectorIndexSearchResults(grown.Results, DocumentFetchOptions{})
	if err != nil || len(grownDocs.Results) != 9 {
		t.Fatalf("grown base results=%+v err=%v", grownDocs.Results, err)
	}
	for _, doc := range grownDocs.Results {
		if bytes.Equal(doc.ID, ids[1]) || len(doc.Document) == 0 {
			t.Fatalf("grown base invalid document=%+v", doc)
		}
	}
}

func TestTypedGraphPublicSameOwnerServing(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 32)
	defer base.Close()
	opts := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, opts); err != nil {
		t.Fatal(err)
	}
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"new-content"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	var buffer VectorIndexSearchBuffer
	response, view, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 16, StatsMode: VectorIndexSearchStatsModeMinimal, DeclaredScalarFilter: &HybridScalarFilter{IndexName: "path", Value: "new"}}, &buffer)
	if err != nil {
		t.Fatalf("public mutable filtered search: %v", err)
	}
	defer view.Close()
	assertTypedGraphMaterializerReuse(t, view, true)
	if len(response.Results) != 1 || !bytes.Equal(response.Results[0].ID, ids[0]) {
		t.Fatalf("results=%+v", response.Results)
	}
	changed[1].Strings[0] = "later-content"
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	fetched, err := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
	if err != nil || len(fetched.Results) != 1 || !bytes.Contains(fetched.Results[0].Document, []byte(`"content":"new-content"`)) {
		t.Fatalf("same-owner full fetch=%+v err=%v", fetched.Results, err)
	}
	owner := view.typedGraphOwner
	if owner == nil {
		t.Fatal("returned view lost owner")
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if !owner.closed {
		t.Fatal("view close retained owner")
	}
	if err := col.FoldColumnGraphServing(context.Background(), base.indexName); err != nil {
		t.Fatal(err)
	}
	epoch := col.collectionSchemaCoordinator().typedGraphWorkEpoch
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, opts); err != nil {
		t.Fatal(err)
	}
	if col.collectionSchemaCoordinator().typedGraphWorkEpoch != epoch {
		t.Fatal("idempotent ensure renewed work debt")
	}
	if renewed, err := col.RenewColumnGraphServing(context.Background(), base.indexName); err != nil || renewed.Epoch != epoch+1 {
		t.Fatalf("public renewal=%+v err=%v", renewed, err)
	}
	second, err := NewCollectionManager(col.db).OpenCollection(col.Name())
	if err != nil {
		t.Fatal(err)
	}
	query := VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 16, StatsMode: VectorIndexSearchStatsModeMinimal, DeclaredScalarFilter: &HybridScalarFilter{IndexName: "path", Value: "new"}}
	checkLatest := func(handle *Collection) {
		t.Helper()
		owned, err := handle.SearchVectorIndex(query)
		if err != nil || len(owned.Results) != 1 || !bytes.Equal(owned.Results[0].ID, ids[0]) {
			t.Fatalf("plain search=%+v err=%v", owned.Results, err)
		}
		res, read, err := handle.SearchVectorIndexWithBufferReadView(query, &buffer)
		if err != nil {
			t.Fatal(err)
		}
		defer read.Close()
		assertTypedGraphMaterializerReuse(t, read, true)
		docs, err := read.FetchDocumentsForVectorIndexSearchResults(res.Results, DocumentFetchOptions{})
		if err != nil || len(docs.Results) != 1 || !bytes.Contains(docs.Results[0].Document, []byte(`"content":"later-content"`)) {
			t.Fatalf("latest fetch=%+v err=%v", docs.Results, err)
		}
		buffer.Reset()
		if !bytes.Equal(owned.Results[0].ID, ids[0]) {
			t.Fatal("plain ID backing reused")
		}
	}
	checkLatest(second)
	bad := opts
	bad.SearchCandidates++
	if err := second.EnsureColumnGraphServing(context.Background(), base.indexName, bad); !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("changed limits: %v", err)
	}
	dir := col.db.Dir()
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	if err := col.db.Close(); err != nil {
		t.Fatal(err)
	}
	db := openTypedMinimaDB(t, dir)
	defer db.Close()
	reopened, err := NewCollectionManager(db).OpenCollection(col.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.EnsureColumnGraphServing(context.Background(), query.IndexName, opts); err != nil {
		t.Fatalf("reopen ensure: %v", err)
	}
	checkLatest(reopened)
}

func TestTypedGraphPublicServingPressureAndOptions(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, _, _, columns, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	opts := typedGraphPublicTestOptions()
	opts.Owners.Owners = 2 // one warm base keeper and one returned read owner
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, opts); err != nil {
		t.Fatal(err)
	}
	query := VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal}
	var buffer VectorIndexSearchBuffer
	_, read, err := col.SearchVectorIndexWithBufferReadView(query, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	owner := read.typedGraphOwner
	// A newly opened handle must propagate keeper admission failure without
	// changing the already admitted authority or leaking the failed warm.
	other, err := NewCollectionManager(col.db).OpenCollection(col.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer other.CloseVectorIndexPreparedSearchCache()
	admitted := col.collectionSchemaCoordinator().typedPublication.Load()
	if err := other.EnsureColumnGraphServing(context.Background(), base.indexName, opts); !errors.Is(err, ErrColumnGraphOwnerBudget) {
		t.Fatalf("new handle keeper pressure=%v", err)
	}
	if col.collectionSchemaCoordinator().typedPublication.Load() != admitted {
		t.Fatal("failed warm changed admitted authority")
	}
	var secondBuffer VectorIndexSearchBuffer
	if _, err := col.SearchVectorIndexWithBuffer(query, &secondBuffer); !errors.Is(err, ErrColumnGraphOwnerBudget) {
		t.Fatalf("held owner pressure=%v", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := col.SearchVectorIndexWithBuffer(query, &secondBuffer); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*VectorIndexSearchOptions){
		func(q *VectorIndexSearchOptions) { q.StatsMode = "" },
		func(q *VectorIndexSearchOptions) { q.QueryMode = VectorIndexQueryModeQuantizedOnly },
		func(q *VectorIndexSearchOptions) { q.QuantizedIndexName = "unknown" },
		func(q *VectorIndexSearchOptions) { q.QuantizedRerankCandidates = 2 },
		func(q *VectorIndexSearchOptions) { q.FetchMultiplier = 2 },
		func(q *VectorIndexSearchOptions) { q.ExactFilterMaxDocs = 2 },
		func(q *VectorIndexSearchOptions) { q.DisableExactFallback = true },
		func(q *VectorIndexSearchOptions) { q.MaxDecodedBlocks = 1 },
		func(q *VectorIndexSearchOptions) { q.IncludeDocuments = true },
	} {
		bad := query
		mutate(&bad)
		if _, _, err := col.SearchVectorIndexWithBufferReadView(bad, &secondBuffer); err == nil {
			t.Fatalf("unsupported options accepted: %+v", bad)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	query.Context = ctx
	if _, err := col.SearchVectorIndex(query); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	if got := col.collectionSchemaCoordinator().typedGraphOwners.owners; got != 0 {
		t.Fatalf("error leaked %d owners", got)
	}
	query.Context = nil
	coord := col.collectionSchemaCoordinator()
	ready := coord.typedPublication.Load()
	missing := *ready
	missing.servingBase = nil
	coord.typedPublication.Store(&missing)
	if _, err := col.SearchVectorIndex(query); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("not-ready state fell back: %v", err)
	}
	if coord.typedPublication.Load() != &missing {
		t.Fatal("query performed reconciliation")
	}
	coord.typedPublication.Store(ready)
}

func BenchmarkTypedGraphPublicServing(b *testing.B) {
	for _, rows := range []int{128, 1024} {
		for _, filtered := range []bool{false, true} {
			name := "search-fetch"
			if filtered {
				name = "filter-search-fetch"
			}
			b.Run(fmt.Sprintf("rows%d/%s", rows, name), func(b *testing.B) {
				col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(b, rows)
				defer base.Close()
				opts := typedGraphPublicTestOptions()
				if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, opts); err != nil {
					b.Fatal(err)
				}
				if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"updated"}}, {Name: "user", Strings: []string{"updated"}}, {Name: "path", Strings: []string{"source"}}}); err != nil {
					b.Fatal(err)
				}
				if _, err := col.DeleteBatch(ids[1:2]); err != nil {
					b.Fatal(err)
				}
				query := VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 4, EfSearch: 16, StatsMode: VectorIndexSearchStatsModeMinimal}
				if filtered {
					query.DeclaredScalarFilter = &HybridScalarFilter{IndexName: "path", Value: "source"}
				}
				var buffer VectorIndexSearchBuffer
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					res, view, err := col.SearchVectorIndexWithBufferReadView(query, &buffer)
					if err != nil {
						b.Fatal(err)
					}
					fetched, err := view.FetchDocumentsForVectorIndexSearchResults(res.Results, DocumentFetchOptions{})
					if err != nil || len(fetched.Results) != 4 {
						b.Fatalf("fetch=%d err=%v", len(fetched.Results), err)
					}
					if err := view.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestTypedGraphPublicEnsureCancellationAndFailedAdmission(t *testing.T) {
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	root, err := canonicalVectorPartitionStorageRootV1(col.db.Dir())
	if err != nil {
		t.Fatal(err)
	}
	held, release := make(chan struct{}), make(chan struct{})
	holder := make(chan error, 1)
	go func() {
		holder <- WithVectorPartitionStorageBarrierV1(root, func() error { close(held); <-release; return nil })
	}()
	<-held
	defer func() {
		close(release)
		if err := <-holder; err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := typedGraphPublicTestOptions()
	done := make(chan error, 1)
	go func() { done <- col.EnsureColumnGraphServing(ctx, base.indexName, opts) }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		vectorPartitionStorageBarriersV1.Lock()
		entry := vectorPartitionStorageBarriersV1.entries[root]
		waiting := entry != nil && entry.refs == 2
		vectorPartitionStorageBarriersV1.Unlock()
		if waiting {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("ensure exited before barrier: %v", err)
		case <-deadline.C:
			t.Fatal("no barrier waiter")
		case <-tick.C:
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ensure cancellation=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ensure ignored cancellation")
	}
	// Failed setup binds policy but never grants feature-off write admission.
	other, err := NewCollectionManager(col.db).OpenCollection(col.Name())
	if err != nil {
		t.Fatal(err)
	}
	seq, system := dbCommitSeqAndSystemRoot(col.db)
	if _, err := other.ReplaceTypedBatch(ids[:1], retained[:1], []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"reject"}}, {Name: "user", Strings: []string{"reject"}}, {Name: "path", Strings: []string{"reject"}}}); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("partial ensure write bypass=%v", err)
	}
	if next, root := dbCommitSeqAndSystemRoot(col.db); next != seq || root != system {
		t.Fatal("rejected write changed authority")
	}
}

func TestTypedGraphPublicNoWriteVacuumOwner(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	index := base.indexName
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureColumnGraphServing(context.Background(), index, typedGraphPublicTestOptions()); err != nil {
		t.Fatal(err)
	}
	search := func() (string, error) {
		var buffer VectorIndexSearchBuffer
		response, view, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: index, Query: columns[0].Float32Vectors[0], TopK: 3, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction, DeclaredScalarFilter: &HybridScalarFilter{IndexName: "path", Value: "source"}}, &buffer)
		if err != nil {
			return "", fmt.Errorf("snapshot=%+v results=%d mismatch=%t: %w", response.Stats.ColumnGraphWork.Snapshot, len(response.Results), errors.Is(err, ErrVectorIndexSnapshotMismatch), err)
		}
		defer view.Close()
		docs, err := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
		if err != nil {
			return "", err
		}
		if len(response.Results) != 3 || docs.Stats.DocumentsMissing != 0 || docs.Stats.DocumentsFetched != 3 {
			return "", fmt.Errorf("incomplete results=%d fetched=%d missing=%d", len(response.Results), docs.Stats.DocumentsFetched, docs.Stats.DocumentsMissing)
		}
		signature := ""
		for i, hit := range response.Results {
			signature += fmt.Sprintf("%q:%g:%q;", hit.ID, hit.Score, docs.Results[i].Document)
		}
		return signature, nil
	}
	want, err := search()
	if err != nil {
		t.Fatal(err)
	}
	// Keep a real read owner across relocation and a subsequent accepted write.
	var heldBuffer VectorIndexSearchBuffer
	heldResponse, held, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: index, Query: columns[0].Float32Vectors[0], TopK: 3, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction}, &heldBuffer)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	oldDocs, err := held.FetchDocumentsForVectorIndexSearchResults(heldResponse.Results, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	before := col.collectionSchemaCoordinator().typedPublication.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := col.db.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("actual vacuum rejected or blocked: %v", err)
	}
	got, err := search()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("changed result/document signature got=%s want=%s", got, want)
	}
	after := col.collectionSchemaCoordinator().typedPublication.Load()
	if after == before || after.catalog.pager == before.catalog.pager || before.servingBase == after.servingBase {
		t.Fatal("vacuum did not replace immutable publication coordinates")
	}
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"after-vacuum"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"source"}}}
	// The write has already committed backend roots but has not installed its
	// derived state. Maintenance must defer without disrupting that accepted write.
	var cutoverErr error
	var reachedInstall bool
	typedGraphPublicationAfterAcceptedHook.Lock()
	typedGraphPublicationAfterAcceptedHook.fn = func(p *typedGraphPublicationCandidate) {
		if p.coord == col.collectionSchemaCoordinator() {
			reachedInstall = true
			cutoverErr = col.db.VacuumIndexOnline(ctx)
		}
	}
	typedGraphPublicationAfterAcceptedHook.Unlock()
	defer func() {
		typedGraphPublicationAfterAcceptedHook.Lock()
		typedGraphPublicationAfterAcceptedHook.fn = nil
		typedGraphPublicationAfterAcceptedHook.Unlock()
	}()
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	typedGraphPublicationAfterAcceptedHook.Lock()
	typedGraphPublicationAfterAcceptedHook.fn = nil
	typedGraphPublicationAfterAcceptedHook.Unlock()
	if !reachedInstall || !errors.Is(cutoverErr, rootpublication.ErrResourcePinned) {
		t.Fatalf("accepted publication cutover: reached=%t err=%v", reachedInstall, cutoverErr)
	}
	if _, err := search(); err != nil {
		t.Fatal(err)
	}
	if err := col.FoldColumnGraphServing(context.Background(), index); err != nil {
		t.Fatal(err)
	}
	if _, err := search(); err != nil {
		t.Fatal(err)
	}
	stillOld, err := held.FetchDocumentsForVectorIndexSearchResults(heldResponse.Results, DocumentFetchOptions{})
	if err != nil || len(stillOld.Results) != len(oldDocs.Results) {
		t.Fatalf("held fetch: %v", err)
	}
	for i := range oldDocs.Results {
		if !bytes.Equal(stillOld.Results[i].Document, oldDocs.Results[i].Document) {
			t.Fatal("held owner changed after relocation/write/fold")
		}
	}
	// A real logical mutation made the pre-write state stale. Even though its
	// captured-base roots remain reachable, relocation must not grant authority.
	coord := col.collectionSchemaCoordinator()
	coord.typedPublication.Store(after)
	if err := col.db.VacuumIndexOnline(ctx); err != nil {
		t.Fatal(err)
	}
	if coord.typedPublication.Load() != after {
		t.Fatal("relocation repaired stale logical authority")
	}
	if _, err := search(); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("stale authority: %v", err)
	}

}

// Ensure may finish warming after vacuum has replaced its prepared authority.
func TestTypedGraphPublicEnsureAcrossVacuum(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, _, _, columns, _ := openTypedGraphQualityFixture(t, 8)
	index := base.indexName
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	captured, release := make(chan struct{}, 1), make(chan struct{})
	var paused, released atomic.Bool
	defer func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}()
	collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
	collectionVectorIndexPreparedSearchBuildHookForTest.afterBuild = func(p *collectionVectorIndexPreparedSearch) {
		if p != nil && p.collection == col && paused.CompareAndSwap(false, true) {
			captured <- struct{}{}
			<-release
		}
	}
	collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
	defer func() {
		collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
		collectionVectorIndexPreparedSearchBuildHookForTest.afterBuild = nil
		collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
	}()
	done := make(chan error, 1)
	go func() {
		done <- col.EnsureColumnGraphServing(context.Background(), index, typedGraphPublicTestOptions())
	}()
	select {
	case <-captured:
	case <-time.After(10 * time.Second):
		t.Fatal("Ensure did not reach post-capture boundary")
	}
	before := col.collectionSchemaCoordinator().typedPublication.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := col.db.VacuumIndexOnline(ctx); err != nil {
		t.Fatal(err)
	}
	after := col.collectionSchemaCoordinator().typedPublication.Load()
	if after == before || after.catalog.pager == before.catalog.pager {
		t.Fatal("no actual relocation")
	}
	released.Store(true)
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, ErrConcurrentMutation) {
			t.Fatalf("late Ensure: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("late Ensure blocked")
	}
	if col.collectionSchemaCoordinator().typedPublication.Load() != after {
		t.Fatal("late Ensure replaced relocated authority")
	}
	if err := col.EnsureColumnGraphServing(ctx, index, typedGraphPublicTestOptions()); err != nil {
		t.Fatal(err)
	}
	var buffer VectorIndexSearchBuffer
	response, view, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: index, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	if len(response.Results) != 1 {
		t.Fatalf("results=%d", len(response.Results))
	}
}
