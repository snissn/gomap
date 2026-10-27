package collections

import (
	"bytes"
	"errors"
	"testing"
	"time"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// This test controls the real boundary between original off-gate preparation
// and gate acquisition without a global hook or a retained opening transaction.
func TestNativeProducerGateRevalidatesStalePreparation(t *testing.T) {
	opts := nativeTypedGroupOptions(t.TempDir())
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	meta := &CollectionMeta{Name: "docs", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON, Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: TypedStorageOwnerColumnPart}}}}, VectorIndexes: []VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}}}
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := backend.PrepareNativeProducerObservation()
	if err != nil {
		t.Fatal(err)
	}
	// The contender cannot enter its original body until explicitly released.
	entered, release := make(chan struct{}), make(chan struct{})
	type result struct {
		err  error
		held bool
	}
	done := make(chan result, 1)
	go func() {
		close(entered)
		<-release
		unlock, held, err := c.lockNativeProducerAfterPreparation(prepared)
		if err == nil {
			unlock()
		}
		done <- result{err, held}
	}()
	<-entered
	released, joined := false, false
	defer func() {
		if !released {
			close(release)
		}
		if !joined {
			<-done
		}
	}()
	document := []byte(`{"embedding":[1,0],"marker":"intervening"}`)
	if _, err := c.Insert([]byte("first"), document); err != nil {
		t.Fatal(err)
	}
	read, err := c.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	allocator := read.capture.NativeAllocator()
	budget := allocator.(*memtable.COWBudget)
	if read.domain.domain.typed == nil {
		read.Close()
		t.Fatal("intervening public mutation did not leave pending typed work")
	}
	beforeBasis, valid := read.capture.Snapshot().StateToken()
	beforeSequence := read.capture.LogicalSequence()
	beforeGeneration := read.collection.documentGeneration
	beforePrimary := read.domain.domain.baseRoot
	beforeManifest, ok := read.collection.domains.Get("column/manifest")
	if !valid || !ok {
		read.Close()
		t.Fatal("pending source has no original basis/manifest domain")
	}
	beforeManifestRoot := beforeManifest.baseRoot
	beforeDeltas, beforeRows, beforeDeletes := 0, 0, 0
	for delta := read.domain.domain.typed; delta != nil; delta = delta.previous {
		beforeDeltas++
		beforeRows += len(delta.rows)
		beforeDeletes += len(delta.deleted)
	}
	if beforeRows != 1 || beforeDeletes != 0 {
		read.Close()
		t.Fatalf("pending typed source deltas=%d rows=%d deletes=%d", beforeDeltas, beforeRows, beforeDeletes)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	// Add a real separately owned reservation so the queued observation is now
	// pressured. This changes no watermark, capacity or retained reader policy.
	stats, limits := budget.Stats(), budget.Limits()
	// Leave the same genuine reservation headroom as the held-pressure control.
	// The original exactly-at-75% fixture could fall below the advisory watermark
	// when previously queued cleanup completed before gated observation.
	target := limits.MaxInFlightBytes - limits.MaxInFlightBytes/8
	if stats.ReservedBytes >= target {
		t.Fatal("fixture began above pressure watermark")
	}
	filler, err := budget.AcquireExternal(target - stats.ReservedBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer filler.Close()
	pressured, err := backend.ObserveNativeProducer()
	if err != nil || !pressured.UnderPressure || pressured == prepared {
		t.Fatalf("fresh pressure=%+v err=%v", pressured, err)
	}
	next := backend.CommandWALNextLSN()
	// Reobserve immediately before releasing the contender. No body/transaction
	// has opened, and queued cleanup has at least 1/16 of the original budget's
	// headroom before this fixture can slip below the actual 75% watermark.
	pressured, err = backend.ObserveNativeProducer()
	watermark := limits.MaxInFlightBytes - limits.MaxInFlightBytes/4
	if err != nil || !pressured.UnderPressure || pressured.ReservedBytes < watermark+limits.MaxInFlightBytes/16 || pressured.LogicalSequence != beforeSequence {
		t.Fatalf("release pressure=%+v err=%v before_sequence=%d watermark=%d", pressured, err, beforeSequence, watermark)
	}
	close(release)
	released = true
	select {
	case got := <-done:
		joined = true
		if got.err != nil || !got.held {
			t.Fatalf("revalidated gate: %+v", got)
		}
	case <-time.After(10 * time.Second):
		filler.Close()
		t.Fatal("stale preflight did not complete its off-gate drain")
	}
	// A real original checkpoint materialized the intervening logical source;
	// observation alone cannot remove its typed delta. No application body/WAL
	// operation was assigned by the helper itself.
	read, err = c.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	afterBasis, valid := read.capture.Snapshot().StateToken()
	afterDeltas, afterRows, afterDeletes := 0, 0, 0
	for delta := read.domain.domain.typed; delta != nil; delta = delta.previous {
		afterDeltas++
		afterRows += len(delta.rows)
		afterDeletes += len(delta.deleted)
	}
	afterManifest, hasManifest := read.collection.domains.Get("column/manifest")
	cfg := read.meta.Options.ColumnStore
	if read.domain.domain.typed != nil || !valid || !hasManifest ||
		afterBasis.CommitSeq <= beforeBasis.CommitSeq || afterBasis.AppliedCommandLSN+1 != next ||
		read.domain.domain.baseRoot == 0 || read.domain.domain.baseRoot == beforePrimary ||
		afterManifest.baseRoot == 0 || afterManifest.baseRoot == beforeManifestRoot ||
		read.capture.LogicalSequence() != beforeSequence || read.collection.documentGeneration != beforeGeneration ||
		cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil ||
		!columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		primary := read.domain.domain.baseRoot
		manifest := uint64(0)
		if hasManifest {
			manifest = afterManifest.baseRoot
		}
		sequence, generation := read.capture.LogicalSequence(), read.collection.documentGeneration
		read.Close()
		t.Fatalf("stale preflight did not materialize exact prefix: before basis=%+v primary=%d manifest=%d typed=%d/%d/%d sequence=%d generation=%d; after basis=%+v valid=%v primary=%d manifest=%d typed=%d/%d/%d sequence=%d generation=%d next=%d release_pressure=%+v", beforeBasis, beforePrimary, beforeManifestRoot, beforeDeltas, beforeRows, beforeDeletes, beforeSequence, beforeGeneration, afterBasis, valid, primary, manifest, afterDeltas, afterRows, afterDeletes, sequence, generation, next, pressured)
	}
	got, err := read.Get([]byte("first"))
	closeErr := read.Close()
	if err != nil || closeErr != nil || !bytes.Equal(got, document) || backend.CommandWALNextLSN() != next {
		t.Fatalf("drain changed logical work: %s %v/%v", got, err, closeErr)
	}
	filler.Close()
	callbacks := 0
	_, _, err = c.Update([]byte("first"), func([]byte) ([]byte, bool, error) {
		callbacks++
		return []byte(`{"embedding":[0,1],"marker":"once"}`), true, nil
	})
	if err != nil || callbacks != 1 {
		t.Fatalf("body callbacks=%d err=%v", callbacks, err)
	}
}

// Unreclaimable owners must terminate the pre-opening loop and preserve the
// original exact refusal. Revalidation does not grant an admission allowance.
func TestNativeProducerGateUnchangedHeldPressureRefuses(t *testing.T) {
	c, closeAll, _, _, _ := openHNSWPublicFixture(t, "native", 2, 2)
	defer closeAll()
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	allocator := read.capture.NativeAllocator()
	budget := allocator.(*memtable.COWBudget)
	original, err := read.Get([]byte("row-00000"))
	if err != nil {
		t.Fatal(err)
	}
	leaseCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	stats := budget.Stats()
	filler, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - budget.Limits().MaxInFlightBytes/8 - stats.ReservedBytes - leaseCharge)
	if err != nil {
		t.Fatal(err)
	}
	defer filler.Close()
	prepared, err := c.nativeJSON.db.PrepareNativeProducerObservation()
	if err != nil || !prepared.UnderPressure {
		t.Fatalf("held pressure preparation=%+v err=%v", prepared, err)
	}
	type result struct {
		unlock func()
		held   bool
		err    error
	}
	done := make(chan result, 1)
	go func() {
		unlock, held, err := c.lockNativeProducerAfterPreparation(prepared)
		done <- result{unlock, held, err}
	}()
	var got result
	select {
	case got = <-done:
	case <-time.After(10 * time.Second):
		filler.Close()
		got = <-done
		if got.unlock != nil {
			got.unlock()
		}
		t.Fatal("unchanged reader pressure spun instead of reaching finite admission")
	}
	if got.err != nil || !got.held {
		t.Fatalf("held-pressure gate=%+v", got)
	}
	defer got.unlock()
	next := c.nativeJSON.db.CommandWALNextLSN()
	// The completed drain remains pressured, but may not reclaim this reader or
	// the external owner. A further genuine owner occupies the remaining bytes
	// so the original transaction opening must retain its exact finite refusal.
	stats = budget.Stats()
	blocker, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - stats.ReservedBytes - leaseCharge)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	tx, err := c.nativeJSON.db.BeginPreparedNativeConditionalTxn()
	if tx != nil {
		tx.Close()
	}
	if !errors.Is(err, memtable.ErrCOWCapacity) || backenddb.CommitPublicationAccepted(err) || c.nativeJSON.db.CommandWALNextLSN() != next {
		t.Fatalf("held-pressure opening=%v err=%v", tx, err)
	}
	blocker.Close()
	current, err := read.Get([]byte("row-00000"))
	if err != nil || !bytes.Equal(current, original) {
		t.Fatalf("held row changed: %v", err)
	}
}
