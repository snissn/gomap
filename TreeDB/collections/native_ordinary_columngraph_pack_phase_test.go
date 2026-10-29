package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// The original finite 4096-row service regression remains the public gate.
// This existing funded fixture isolates constructor-versus-retained custody.
func TestNativeOrdinaryColumnGraphPackEndsConstructionPromise(t *testing.T) {
	owner, _, manager, col := openNativeOrdinaryGraphExecutionFixture(t)
	const count = 512
	ids, docs, vectors := make([][]byte, count), make([][]byte, count), make([][]float32, count)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("p%04d", i))
		docs[i] = []byte(`{"embedding":[1,0],"body":"original payload"}`)
		vectors[i] = []float32{1, 0}
	}
	if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids, docs, "embedding", VectorMetricCosine, vectors); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("vec"); err != nil {
		t.Fatal(err)
	}
	read, err := col.nativeJSON.Capture(col.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	budget := read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	opts := VectorIndexSearchOptions{IndexName: "vec", Query: []float32{1, 0}, TopK: 10, StatsMode: VectorIndexSearchStatsModeProduction}
	p, _, _, err := col.acquireNativeColumnGraphPreparedSearchAtRead(opts, read)
	if err != nil {
		t.Fatal(err)
	}
	// A zero-byte operation cannot change the receipt. It only asks whether
	// that receipt still protects the old complete image construction floor.
	oldImage := uint64(16) * memtable.COWAllocationCharge(p.pack.Header.TotalLength)
	constructorEnded := errors.Is(p.nativeLease.ShrinkNativeBytes(0, oldImage), memtable.ErrCOWCapacity)
	var buffer VectorIndexSearchBuffer
	response, view, err := col.SearchVectorIndexWithBufferReadView(opts, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 10 {
		t.Fatalf("results=%d", len(response.Results))
	}
	fetched, err := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range fetched.Results {
		if !r.Found || !bytes.Equal(r.Document, docs[0]) {
			t.Fatalf("fetch %d: %+v", i, r)
		}
	}
	warm, err := col.WarmVectorIndexPreparedSearch(opts)
	if err != nil || warm.Stats.HNSWSearchPackCacheHits != 1 {
		t.Fatalf("warm reuse: %+v %v", warm.Stats, err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	buffer.Reset()
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
		t.Fatal(err)
	}
	if p.nativeLease != nil || p.pack != nil || !p.closed {
		t.Fatal("cache Close retained pack custody")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if s := budget.Stats(); s.TotalBytes != 0 || s.ReservedBytes != 0 || s.ExternalLeases != 0 {
		t.Fatalf("Close retained ownership: %+v", s)
	}
	if !constructorEnded {
		t.Fatal("successful prepared pack retains its full image construction promise")
	}
}
