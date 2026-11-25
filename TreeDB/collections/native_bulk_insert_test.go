package collections

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func openNativeBulkInsertTestDB(t *testing.T, limits treedb.COWMemtableLimits) (*treedb.DB, *backenddb.DB, *CollectionManager, *Collection) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = limits
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open treedb: %v", err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Errorf("close treedb: %v", err)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatalf("native semantic backend: %v", err)
	}
	manager := NewCollectionManager(backend)
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close collection manager: %v", err)
		}
	})
	meta := &CollectionMeta{
		Name: "bulk_docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{
			{Name: "age_1", Field: "age", ValueType: IndexValueInt64},
			{Name: "email_1", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city_1", Field: "city", ValueType: IndexValueString},
		},
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := manager.OpenCollection("bulk_docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	return owner, backend, manager, col
}

func buildNativeBulkBSONBatch(t *testing.T, producer, count, padBytes int) ([][]byte, [][]byte) {
	t.Helper()
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	cities := []string{"austin", "boston", "chicago", "denver", "seattle", "miami", "portland", "atlanta"}
	pad := string(bytes.Repeat([]byte("x"), padBytes))
	for i := 0; i < count; i++ {
		idStr := fmt.Sprintf("p%02d-doc-%07d", producer, i)
		ids[i] = []byte(idStr)
		doc, err := bson.Marshal(bson.D{
			{Key: "_id", Value: idStr},
			{Key: "age", Value: int64(18 + ((producer*count + i) % 70))},
			{Key: "email", Value: fmt.Sprintf("user-%02d-%07d@example.com", producer, i)},
			{Key: "city", Value: cities[(producer+i)%len(cities)]},
			{Key: "note", Value: pad},
		})
		if err != nil {
			t.Fatalf("marshal bson doc %d: %v", i, err)
		}
		docs[i] = doc
	}
	return ids, docs
}

func parseStatUint64(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	raw := stats[key]
	if raw == "" {
		t.Fatalf("missing stat %q", key)
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse stat %q=%q: %v", key, raw, err)
	}
	return v
}

func TestNativeBulkInsertConcurrentBatch10000NoCapacityFallback(t *testing.T) {
	const (
		producers = 8
		batchSize = 10000
	)
	owner, backend, _, col := openNativeBulkInsertTestDB(t, treedb.StandaloneCOWMemtableLimits())

	batchIDs := make([][][]byte, producers)
	batchDocs := make([][][]byte, producers)
	for p := 0; p < producers; p++ {
		batchIDs[p], batchDocs[p] = buildNativeBulkBSONBatch(t, p, batchSize, 64)
	}

	if err := backend.JoinNativeSemanticCleanupForTest(); err != nil {
		t.Fatalf("join cleanup before: %v", err)
	}
	pubBefore := parseStatUint64(t, owner.Stats(), "treedb.cache.cow.publications_total")

	start := make(chan struct{})
	errs := make([]error, producers)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			<-start
			_, errs[pid] = col.InsertBatchValidatedBSON(batchIDs[pid], batchDocs[pid])
		}(p)
	}
	close(start)
	wg.Wait()

	for p, err := range errs {
		if err != nil {
			t.Errorf("producer %d failed: %v", p, err)
		}
	}
	if t.Failed() {
		stats := owner.Stats()
		t.Fatalf("concurrent p=%d batch=%d failed; pub=%s roll=%s peak=%s ext=%s",
			producers, batchSize,
			stats["treedb.cache.cow.publications_total"],
			stats["treedb.cache.cow.rollovers_total"],
			stats["treedb.cache.cow.peak_bytes"],
			stats["treedb.cache.cow.external_bytes"],
		)
	}

	pubAfter := parseStatUint64(t, owner.Stats(), "treedb.cache.cow.publications_total")
	if delta := pubAfter - pubBefore; delta > 25 {
		t.Fatalf("unexpectedly high COW publications=%d (want <= 25)", delta)
	}

	// Spot-check documents from every producer.
	for p := 0; p < producers; p++ {
		for _, idx := range []int{0, batchSize / 2, batchSize - 1} {
			got, err := col.Get(batchIDs[p][idx])
			if err != nil || !bytes.Equal(got, batchDocs[p][idx]) {
				t.Fatalf("producer %d doc %d mismatch: gotLen=%d wantLen=%d err=%v", p, idx, len(got), len(batchDocs[p][idx]), err)
			}
		}
	}

	// Verify no leaked external leases after checkpoint and cleanup drain.
	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := backend.JoinNativeSemanticCleanupForTest(); err != nil {
		t.Fatalf("join cleanup after: %v", err)
	}
	afterObs, err := backend.ObserveNativeProducer()
	if err != nil {
		t.Fatalf("observe after: %v", err)
	}
	if afterObs.UnderPressure {
		t.Fatalf("unexpected residual pressure after checkpoint+cleanup: %+v", afterObs)
	}
}

func TestNativeBulkInsertDirectManagerConcurrentBatch10000(t *testing.T) {
	const (
		producers = 8
		batchSize = 10000
	)
	owner, _, _, col := openNativeBulkInsertTestDB(t, treedb.StandaloneCOWMemtableLimits())

	batchIDs := make([][][]byte, producers)
	batchDocs := make([][][]byte, producers)
	for p := 0; p < producers; p++ {
		batchIDs[p], batchDocs[p] = buildNativeBulkBSONBatch(t, p, batchSize, 64)
	}

	pubBefore := parseStatUint64(t, owner.Stats(), "treedb.cache.cow.publications_total")
	start := make(chan struct{})
	errs := make([]error, producers)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			<-start
			errs[pid] = col.nativeJSON.InsertBatch(col.name, batchIDs[pid], batchDocs[pid])
		}(p)
	}
	close(start)
	wg.Wait()

	for p, err := range errs {
		if err != nil {
			t.Errorf("direct manager producer %d failed: %v", p, err)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
	pubAfter := parseStatUint64(t, owner.Stats(), "treedb.cache.cow.publications_total")
	if delta := pubAfter - pubBefore; delta > 25 {
		t.Fatalf("direct manager publications=%d want <= 25", delta)
	}
}

func TestNativeBulkInsertTightLimitsFailsClosedNoLeaseLeak(t *testing.T) {
	tight := treedb.DefaultCOWMemtableLimits()
	tight.MaxInFlightBytes = 8 << 20
	tight.MaxTotalBytes = 16 << 20
	tight.MaxRetiredBytes = 16 << 20
	tight.MaxGenerationBytes = 2 << 20
	_, backend, _, col := openNativeBulkInsertTestDB(t, tight)

	if err := backend.JoinNativeSemanticCleanupForTest(); err != nil {
		t.Fatalf("join cleanup before: %v", err)
	}
	beforeObs, err := backend.ObserveNativeProducer()
	if err != nil {
		t.Fatalf("observe before: %v", err)
	}
	beforeLSN := backend.CommandWALNextLSN()

	// 2500 × ~2KB BSON docs (~5 MiB raw + 3 indexes) exceeds the 8 MiB in-flight limit.
	ids, docs := buildNativeBulkBSONBatch(t, 0, 2500, 2048)
	_, err = col.InsertBatchValidatedBSON(ids, docs)
	if !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("expected ErrCOWCapacity under tight limits, got %v", err)
	}
	if backenddb.CommitPublicationAccepted(err) || errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("expected clean pre-publication refusal, got %v", err)
	}

	if err := backend.JoinNativeSemanticCleanupForTest(); err != nil {
		t.Fatalf("join cleanup after: %v", err)
	}
	afterObs, err := backend.ObserveNativeProducer()
	if err != nil {
		t.Fatalf("observe after: %v", err)
	}
	if backend.CommandWALNextLSN() != beforeLSN {
		t.Fatalf("LSN advanced on refused batch: before=%d after=%d", beforeLSN, backend.CommandWALNextLSN())
	}
	if afterObs.ReservedBytes != beforeObs.ReservedBytes {
		t.Fatalf("leaked COW reservation on capacity refusal: before=%d after=%d", beforeObs.ReservedBytes, afterObs.ReservedBytes)
	}
}

func TestNativeBulkInsertZeroValueCoordinatorSlotDoesNotDeadlock(t *testing.T) {
	var coord collectionSchemaCoordinator
	releases := make([]func(), 0, nativeBulkPreimageMaxConcurrent)
	for i := 0; i < nativeBulkPreimageMaxConcurrent; i++ {
		releases = append(releases, coord.acquireBulkPreimageSlot())
	}
	acquiredExtra := make(chan func(), 1)
	go func() {
		acquiredExtra <- coord.acquireBulkPreimageSlot()
	}()
	select {
	case rel := <-acquiredExtra:
		rel()
		t.Fatalf("acquired more than nativeBulkPreimageMaxConcurrent=%d slots", nativeBulkPreimageMaxConcurrent)
	default:
	}
	// Idempotent double-release must not over-fill the token channel.
	releases[0]()
	releases[0]()
	extraRelease := <-acquiredExtra
	extraRelease()
	for _, rel := range releases[1:] {
		rel()
	}
	// Nil coordinator must return a no-op release func.
	var nilCoord *collectionSchemaCoordinator
	nilCoord.acquireBulkPreimageSlot()()
}

func TestNativeBulkInsertCapacityDrainRunsOutsideVectorGate(t *testing.T) {
	tight := treedb.DefaultCOWMemtableLimits()
	tight.MaxInFlightBytes = 16 << 20
	tight.MaxTotalBytes = 32 << 20
	tight.MaxRetiredBytes = 32 << 20
	tight.MaxGenerationBytes = 8 << 20
	_, backend, _, col := openNativeBulkInsertTestDB(t, tight)
	coord := col.collectionSchemaCoordinator()
	if coord == nil {
		t.Fatal("expected non-nil collectionSchemaCoordinator")
	}

	if err := backend.JoinNativeSemanticCleanupForTest(); err != nil {
		t.Fatalf("join cleanup: %v", err)
	}
	// Capture the COWBudget and hold an external filler lease so the first
	// preimage attempt fails with memtable.ErrCOWCapacity while leaving enough
	// room for the retry after the drain hook releases the filler.
	read, err := col.nativeJSON.Capture(col.name)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	budget := read.capture.NativeAllocator().(*memtable.COWBudget)
	if err := read.Close(); err != nil {
		t.Fatalf("close capture: %v", err)
	}

	stats := budget.Stats()
	limits := budget.Limits()
	// Leave only 128 KiB free so a 200-doc BSON batch preimage trips ErrCOWCapacity.
	holdBytes := limits.MaxInFlightBytes - stats.ReservedBytes - (128 << 10)
	filler, err := budget.AcquireExternal(holdBytes)
	if err != nil {
		t.Fatalf("acquire filler (%d bytes): %v", holdBytes, err)
	}
	var fillerOnce sync.Once
	releaseFiller := func() {
		fillerOnce.Do(func() { filler.Close() })
	}
	defer releaseFiller()

	var hookCalls int
	var vectorUnlockedInHook bool
	var slotCountInHook int
	restore := setNativeBulkCapacityDrainHookForTest(func() {
		hookCalls++
		if coord.vectorDDLMutationMu.TryLock() {
			vectorUnlockedInHook = true
			coord.vectorDDLMutationMu.Unlock()
		}
		slotCountInHook = len(coord.nativeBulkPreimageTokens)
		// Release transient pressure so the serialized pre==nil retry succeeds.
		releaseFiller()
	})
	defer restore()

	ids, docs := buildNativeBulkBSONBatch(t, 0, 200, 512)
	gotIDs, err := col.InsertBatchValidatedBSON(ids, docs)
	if err != nil {
		t.Fatalf("expected capacity drain retry to succeed, got: %v", err)
	}
	if len(gotIDs) != len(ids) {
		t.Fatalf("inserted len=%d want %d", len(gotIDs), len(ids))
	}
	if hookCalls != 1 {
		t.Fatalf("capacity drain hook calls=%d want 1", hookCalls)
	}
	if !vectorUnlockedInHook {
		t.Fatal("PrepareNativeProducer capacity drain ran while vectorDDLMutationMu (unlockVector) was still held")
	}
	if slotCountInHook != nativeBulkPreimageMaxConcurrent {
		t.Fatalf("bulk preimage slot was still held during PrepareNativeProducer drain: available=%d want %d", slotCountInHook, nativeBulkPreimageMaxConcurrent)
	}
	for _, idx := range []int{0, 100, 199} {
		got, err := col.Get(ids[idx])
		if err != nil || !bytes.Equal(got, docs[idx]) {
			t.Fatalf("doc %d mismatch after retry: gotLen=%d wantLen=%d err=%v", idx, len(got), len(docs[idx]), err)
		}
	}
}
