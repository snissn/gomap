package collections

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// The original public4096 service fixture remains the default-budget product
// discriminator. This small sufficiently funded fixture isolates serving
// custody/identity from the separately tested construction promise.
func TestNativeOrdinaryColumnGraphCapturedPreparedSearch(t *testing.T) {
	owner, _, manager, col := openNativeOrdinaryGraphExecutionFixture(t)
	docs := [][]byte{[]byte(`{"embedding":[1,0],"body":"first"}`), []byte(`{"embedding":[0,1],"body":"second"}`)}
	if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection([][]byte{[]byte("a"), []byte("b")}, docs, "embedding", VectorMetricCosine, [][]float32{{1, 0}, {0, 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("vec"); err != nil {
		t.Fatal(err)
	}
	if col.db != nil {
		t.Fatal("native facade acquired a legacy database")
	}
	opts := VectorIndexSearchOptions{IndexName: "vec", Query: []float32{1, 0}, TopK: 2, StatsMode: VectorIndexSearchStatsModeProduction}
	if _, err := col.WarmVectorIndexPreparedSearch(opts); err != nil {
		t.Fatal(err)
	}
	warm, err := col.WarmVectorIndexPreparedSearch(opts)
	if err != nil {
		t.Fatal(err)
	}
	if warm.Stats.HNSWSearchPackCacheHits != 1 {
		t.Fatalf("warm did not reuse shared cache: %+v", warm.Stats)
	}
	var buffer VectorIndexSearchBuffer
	response, view, err := col.SearchVectorIndexWithBufferReadView(opts, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	defer buffer.Reset()
	if len(response.Results) != 2 || string(response.Results[0].ID) != "a" || string(response.Results[1].ID) != "b" || response.Stats.HNSWSearchPackActive == 0 {
		t.Fatalf("not exact prepared pack: %+v", response)
	}
	read := view.nativeRead
	prepared, _, _, err := col.acquireNativeColumnGraphPreparedSearchAtRead(opts, read)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.nativeLease == nil || prepared.collection != col || prepared.collection.db != nil {
		t.Fatal("cached pack lost independent receipt/facade custody")
	}
	identity := prepared.nativeIdentity
	if identity.incarnation != read.collection.incarnation || identity.documentGeneration != read.collection.documentGeneration || identity.logicalSequence != read.capture.LogicalSequence() {
		t.Fatal("cache identity not bound to held native cut")
	}
	fetched, err := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range fetched.Results {
		if !fetched.Results[i].Found || !bytes.Equal(fetched.Results[i].Document, docs[i]) {
			t.Fatalf("held fetch %d: %+v", i, fetched.Results[i])
		}
	}
	// A valid two-row graph still has M=16. Exact scalar filtering must bound
	// its scratch by rows, including when the allowed ID is below global top-k.
	var filtered VectorIndexSearchBuffer
	defer filtered.Reset()
	filteredResponse, err := col.searchNativeColumnGraphAtReadView(opts, &filtered, view, hybridScalarAllowSet{"b": {}})
	if err != nil {
		t.Fatal(err)
	}
	if len(filteredResponse.Results) != 1 || string(filteredResponse.Results[0].ID) != "b" || cap(filtered.searchScratch.scoreTileScores) > len(docs) || cap(filtered.searchScratch.scoreTileRowIDs) > len(docs) || cap(filtered.searchScratch.segmentedScoreRowIDs) > len(docs) || cap(filtered.searchScratch.scoreTileDots) > len(docs) {
		t.Fatalf("filtered result/scratch exceeded corpus: response=%+v scores=%d rowIDs=%d segmented=%d dots=%d", filteredResponse, cap(filtered.searchScratch.scoreTileScores), cap(filtered.searchScratch.scoreTileRowIDs), cap(filtered.searchScratch.segmentedScoreRowIDs), cap(filtered.searchScratch.scoreTileDots))
	}
	filtered.Reset()
	if _, _, err = col.InsertBatchWithStatsValidatedFloat32Projection([][]byte{[]byte("c")}, [][]byte{[]byte(`{"embedding":[1,1]}`)}, "embedding", VectorMetricCosine, [][]float32{{1, 1}}); err != nil {
		t.Fatal(err)
	}
	// A new pending typed row cannot be served by the old physical graph.
	var fresh VectorIndexSearchBuffer
	if _, err = col.SearchVectorIndexWithBuffer(opts, &fresh); !errors.Is(err, ErrVectorIndexSearchUnavailable) {
		t.Fatalf("pending source was silently ignored: %v", err)
	}
	fresh.Reset()
	// The held complete cut continues to own the old graph and documents.
	old, err := col.searchNativeColumnGraphAtReadView(opts, &buffer, view, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(old.Results) != 2 || string(old.Results[0].ID) != "a" {
		t.Fatalf("held graph changed: %+v", old)
	}
	buffer.Reset()
	if err = view.Close(); err != nil {
		t.Fatal(err)
	}
	if err = col.CloseVectorIndexPreparedSearchCache(); err != nil {
		t.Fatal(err)
	}
	if prepared.nativeLease != nil || prepared.pack != nil || !prepared.closed {
		t.Fatal("cache Close retained pack custody")
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNativePreparedSearchResponseConcurrentClose(t *testing.T) {
	_, _, _, col := openNativeOrdinaryGraphExecutionFixture(t)
	read, err := col.nativeJSON.Capture(col.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(4096)
	if err != nil {
		t.Fatal(err)
	}
	p := &collectionVectorIndexPreparedSearch{nativeLease: lease, response: VectorIndexSearchResponse{IndexName: "vec"}}
	defer p.Close()
	start := make(chan struct{})
	var active, finished sync.WaitGroup
	const workers = 4
	active.Add(workers)
	finished.Add(workers)
	for range workers {
		go func() {
			defer finished.Done()
			<-start
			for i := 0; i < 1000; i++ {
				_ = p.responseForSearch()
				if i == 0 {
					active.Done()
				}
			}
		}()
	}
	close(start)
	active.Wait()
	if err := p.Close(); err != nil {
		t.Error(err)
	}
	finished.Wait()
	if p.nativeLease != nil || p.responseForSearch().IndexName != "" {
		t.Fatal("Close retained native response custody")
	}
}

func TestNativeOrdinaryColumnGraphSearchAdmissionAndCancel(t *testing.T) {
	owner, _, manager, col := openNativeOrdinaryGraphExecutionFixture(t)
	if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection([][]byte{[]byte("a"), []byte("b")}, [][]byte{[]byte(`{"embedding":[1,0]}`), []byte(`{"embedding":[0,1]}`)}, "embedding", VectorMetricCosine, [][]float32{{1, 0}, {0, 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("vec"); err != nil {
		t.Fatal(err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	budget := view.nativeRead.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	opts := VectorIndexSearchOptions{IndexName: "vec", Query: []float32{1, 0}, TopK: 2, StatsMode: VectorIndexSearchStatsModeProduction}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opts.Context = ctx
	before := budget.Stats()
	var buffer VectorIndexSearchBuffer
	if _, err = col.searchNativeColumnGraphAtReadView(opts, &buffer, view, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	after := budget.Stats()
	if after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases || len(buffer.results) != 0 || buffer.nativeLease != nil {
		t.Fatalf("canceled work retained custody: before=%+v after=%+v", before, after)
	}
	// A pre-output refusal must not consume/read the asset after admission fails.
	sentinel := errors.New("output admission refused")
	_, err = nativeColumnGraphAssetReadCapacity("not-an-existing-asset-root", columnVectorIndexStateAssetSnapshot{Ref: ColumnAssetRef{Length: 1024}}, func(uint64) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("asset read happened before admission: %v", err)
	}
	if _, err = nativeColumnGraphPackCapacity(columnVectorIndexStateAssetSnapshot{Ref: ColumnAssetRef{Length: int64(^uint64(0) >> 1)}}); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("unchecked pack capacity: %v", err)
	}
	if err = view.Close(); err != nil {
		t.Fatal(err)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	if got := budget.Stats(); got.TotalBytes != 0 || got.ExternalLeases != 0 {
		t.Fatalf("full Close retained custody: %+v", got)
	}
}

// Actual raw document-ID validation must fit without reserving an unused generic
// decoder. The same original owner remains finitely pressured for both warm and
// search; the independent default-budget 4096 lifecycle remains unchanged.
func TestNativeOrdinaryColumnGraphRawWarmFiniteHeadroom(t *testing.T) {
	_, _, _, col := openNativeOrdinaryGraphExecutionFixture(t)
	if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection([][]byte{[]byte("a"), []byte("b")}, [][]byte{[]byte(`{"embedding":[1,0]}`), []byte(`{"embedding":[0,1]}`)}, "embedding", VectorMetricCosine, [][]float32{{1, 0}, {0, 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("vec"); err != nil {
		t.Fatal(err)
	}
	read, err := col.nativeJSON.Capture(col.name)
	if err != nil {
		t.Fatal(err)
	}
	budget := read.capture.NativeAllocator().(*memtable.COWBudget)
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	fixed, err := nativeTypedDecoderFixedCapacity()
	if err != nil {
		t.Fatal(err)
	}
	headroom := fixed / 2
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	before := budget.Stats()
	if before.ReservedBytes+headroom+overhead >= budget.Limits().MaxInFlightBytes {
		t.Fatalf("fixture has no finite pressure room: %+v", before)
	}
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - headroom - overhead)
	if err != nil {
		t.Fatal(err)
	}
	defer pressure.Close()
	opts := VectorIndexSearchOptions{IndexName: "vec", Query: []float32{1, 0}, TopK: 2, StatsMode: VectorIndexSearchStatsModeProduction}
	warm, err := col.WarmVectorIndexPreparedSearch(opts)
	if err != nil {
		t.Fatalf("raw warm with %d bytes of actual original-owner headroom: %v", headroom, err)
	}
	if warm.Stats.HNSWSearchPackCacheBuilds != 1 {
		t.Fatalf("warm did not build maintained pack: %+v", warm.Stats)
	}
	var buffer VectorIndexSearchBuffer
	defer buffer.Reset()
	response, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil || len(response.Results) != 2 || string(response.Results[0].ID) != "a" || string(response.Results[1].ID) != "b" {
		t.Fatalf("pressured exact search: response=%+v err=%v", response, err)
	}
	buffer.Reset()
	if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
		t.Fatal(err)
	}
}
