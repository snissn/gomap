package collections

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
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

func parseStatUint64(tb testing.TB, stats map[string]string, key string) uint64 {
	tb.Helper()
	raw := stats[key]
	if raw == "" {
		tb.Fatalf("missing stat %q", key)
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		tb.Fatalf("parse stat %q=%q: %v", key, raw, err)
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

func buildNativeBulkBSONIdx2Batch(tb testing.TB, producer, count, padBytes int) ([][]byte, [][]byte) {
	tb.Helper()
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	cities := []string{"austin", "boston", "chicago", "denver", "seattle", "miami", "portland", "atlanta"}
	pad := string(bytes.Repeat([]byte("z"), padBytes))
	for i := 0; i < count; i++ {
		idStr := fmt.Sprintf("p%02d-doc-%07d", producer, i)
		ids[i] = []byte(idStr)
		doc, err := bson.Marshal(bson.D{
			{Key: "_id", Value: idStr},
			{Key: "age", Value: int64(20 + ((producer*count + i) % 60))},
			{Key: "city", Value: cities[(producer+i)%len(cities)]},
			{Key: "payload", Value: pad},
		})
		if err != nil {
			tb.Fatalf("marshal bson doc %d: %v", i, err)
		}
		docs[i] = doc
	}
	return ids, docs
}

func TestNativeBulkInsertParallelStandaloneCOWIdx2DurabilityAndTightLimits(t *testing.T) {
	if nativeBulkPreimageMaxConcurrent < 4 {
		t.Fatalf("nativeBulkPreimageMaxConcurrent=%d want >= 4", nativeBulkPreimageMaxConcurrent)
	}
	cases := []struct {
		name      string
		limits    treedb.COWMemtableLimits
		producers int
		batchSize int
		padBytes  int
	}{
		{
			name:      "standalone_default_p8_idx2",
			limits:    treedb.StandaloneCOWMemtableLimits(),
			producers: 8,
			batchSize: 2000,
			padBytes:  64,
		},
		{
			name: "tight_limits_p8_idx2",
			limits: func() treedb.COWMemtableLimits {
				l := treedb.DefaultCOWMemtableLimits()
				l.MaxInFlightBytes = 32 << 20
				l.MaxTotalBytes = 64 << 20
				l.MaxRetiredBytes = 64 << 20
				l.MaxGenerationBytes = 8 << 20
				return l
			}(),
			producers: 8,
			batchSize: 500,
			padBytes:  64,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
			opts.MemtableMode = "cow_btree"
			opts.COWMemtableLimits = tc.limits

			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatalf("open treedb: %v", err)
			}
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				_ = owner.Close()
				t.Fatalf("native semantic backend: %v", err)
			}
			manager := NewCollectionManager(backend)
			meta := &CollectionMeta{
				Name: "bulk_idx2",
				Options: CollectionOptions{
					DocumentFormat: DocumentFormatBSON,
				},
				Indexes: []IndexDefinition{
					{Name: "age_1", Field: "age", ValueType: IndexValueInt64},
					{Name: "city_1", Field: "city", ValueType: IndexValueString},
				},
			}
			if _, err := manager.CreateCollection(meta); err != nil {
				_ = manager.Close()
				_ = owner.Close()
				t.Fatalf("create collection: %v", err)
			}
			col, err := manager.OpenCollection("bulk_idx2")
			if err != nil {
				_ = manager.Close()
				_ = owner.Close()
				t.Fatalf("open collection: %v", err)
			}

			if err := owner.Checkpoint(); err != nil {
				t.Fatalf("checkpoint before: %v", err)
			}
			if err := backend.JoinNativeSemanticCleanupForTest(); err != nil {
				t.Fatalf("join cleanup before: %v", err)
			}
			beforeExt := parseStatUint64(t, owner.Stats(), "treedb.cache.cow.external_bytes")

			batchIDs := make([][][]byte, tc.producers)
			batchDocs := make([][][]byte, tc.producers)
			for p := 0; p < tc.producers; p++ {
				batchIDs[p], batchDocs[p] = buildNativeBulkBSONIdx2Batch(t, p, tc.batchSize, tc.padBytes)
			}

			start := make(chan struct{})
			errs := make([]error, tc.producers)
			var wg sync.WaitGroup
			for p := 0; p < tc.producers; p++ {
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
				t.FailNow()
			}

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
			afterExt := parseStatUint64(t, owner.Stats(), "treedb.cache.cow.external_bytes")
			// Bounded txnhistory retention for newly committed keys accounts for
			// ~1 MiB under standalone limits; any leaked bulk preimage lease
			// would retain multiple MiB per batch.
			if afterExt > beforeExt+(2<<20) {
				t.Fatalf("leaked COW external lease bytes: before=%d after=%d (delta=%d)", beforeExt, afterExt, afterExt-beforeExt)
			}

			if err := manager.Close(); err != nil {
				t.Fatalf("close manager: %v", err)
			}
			if err := owner.Close(); err != nil {
				t.Fatalf("close owner: %v", err)
			}

			// Reopen and verify 100% document and secondary-index durability.
			reopened, err := treedb.Open(opts)
			if err != nil {
				t.Fatalf("reopen treedb: %v", err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Errorf("close reopened: %v", err)
				}
			}()
			reBackend, err := reopened.NativeSemanticBackend()
			if err != nil {
				t.Fatalf("reopened backend: %v", err)
			}
			reManager := NewCollectionManager(reBackend)
			defer func() {
				if err := reManager.Close(); err != nil {
					t.Errorf("close reManager: %v", err)
				}
			}()
			reCol, err := reManager.OpenCollection("bulk_idx2")
			if err != nil {
				t.Fatalf("reopen collection: %v", err)
			}

			totalDocs := tc.producers * tc.batchSize
			for p := 0; p < tc.producers; p++ {
				for i := 0; i < tc.batchSize; i++ {
					got, err := reCol.Get(batchIDs[p][i])
					if err != nil || !bytes.Equal(got, batchDocs[p][i]) {
						t.Fatalf("reopened p=%d doc=%d mismatch: gotLen=%d wantLen=%d err=%v", p, i, len(got), len(batchDocs[p][i]), err)
					}
				}
			}

			// Verify every secondary index entry across both indexes.
			cities := []string{"austin", "boston", "chicago", "denver", "seattle", "miami", "portland", "atlanta"}
			cityTotal := 0
			for _, city := range cities {
				gotIDs, err := reCol.FindByIndex("city_1", city)
				if err != nil {
					t.Fatalf("FindByIndex(city_1, %q): %v", city, err)
				}
				cityTotal += len(gotIDs)
			}
			if cityTotal != totalDocs {
				t.Fatalf("city_1 total indexed docs=%d want %d", cityTotal, totalDocs)
			}
			ageTotal := 0
			for age := int64(20); age < 80; age++ {
				gotIDs, err := reCol.FindByIndexValue("age_1", age)
				if err != nil {
					t.Fatalf("FindByIndexValue(age_1, %d): %v", age, err)
				}
				ageTotal += len(gotIDs)
			}
			if ageTotal != totalDocs {
				t.Fatalf("age_1 total indexed docs=%d want %d", ageTotal, totalDocs)
			}
		})
	}
}

func BenchmarkBulkInsertParallelStandaloneCOW(b *testing.B) {
	const (
		producers = 8
		batchSize = 2500
	)
	batchIDs := make([][][]byte, producers)
	batchDocs := make([][][]byte, producers)
	for p := 0; p < producers; p++ {
		batchIDs[p], batchDocs[p] = buildNativeBulkBSONIdx2Batch(b, p, batchSize, 64)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		b.StopTimer()
		opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, b.TempDir())
		opts.MemtableMode = "cow_btree"
		opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
		owner, err := treedb.Open(opts)
		if err != nil {
			b.Fatalf("open treedb: %v", err)
		}
		backend, err := owner.NativeSemanticBackend()
		if err != nil {
			_ = owner.Close()
			b.Fatalf("native semantic backend: %v", err)
		}
		manager := NewCollectionManager(backend)
		meta := &CollectionMeta{
			Name: "bench_bulk_idx2",
			Options: CollectionOptions{
				DocumentFormat: DocumentFormatBSON,
			},
			Indexes: []IndexDefinition{
				{Name: "age_1", Field: "age", ValueType: IndexValueInt64},
				{Name: "city_1", Field: "city", ValueType: IndexValueString},
			},
		}
		if _, err := manager.CreateCollection(meta); err != nil {
			_ = manager.Close()
			_ = owner.Close()
			b.Fatalf("create collection: %v", err)
		}
		col, err := manager.OpenCollection("bench_bulk_idx2")
		if err != nil {
			_ = manager.Close()
			_ = owner.Close()
			b.Fatalf("open collection: %v", err)
		}
		b.StartTimer()

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
		if err := owner.Checkpoint(); err != nil {
			b.Fatalf("checkpoint: %v", err)
		}

		b.StopTimer()
		for p, err := range errs {
			if err != nil {
				b.Fatalf("producer %d failed: %v", p, err)
			}
		}
		if err := manager.Close(); err != nil {
			b.Fatalf("close manager: %v", err)
		}
		if err := owner.Close(); err != nil {
			b.Fatalf("close owner: %v", err)
		}
	}
}

func copyDirTreeForCrashTest(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if filepath.Base(path) == "LOCK" {
			return nil
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = out.Close()
			return err
		}
		return out.Close()
	})
	if err != nil {
		t.Fatalf("copy crash snapshot dir: %v", err)
	}
}

func TestNativeBulkInsertDurableGroupCommitOverlapAndCrashRecovery(t *testing.T) {
	const (
		producers = 8
		waves     = 4
		batchSize = 200
	)
	dir := t.TempDir()
	opts := treedb.OptionsForSemantic(treedb.ProfileCommandWALDurable, dir)
	opts.BackgroundCheckpointInterval = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open durable treedb: %v", err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		_ = owner.Close()
		t.Fatalf("native semantic backend: %v", err)
	}
	manager := NewCollectionManager(backend)
	meta := &CollectionMeta{
		Name: "durable_bulk",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{
			{Name: "age_1", Field: "age", ValueType: IndexValueInt64},
			{Name: "email_1", Field: "email", ValueType: IndexValueString, Unique: true},
		},
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		_ = manager.Close()
		_ = owner.Close()
		t.Fatalf("create collection: %v", err)
	}
	col, err := manager.OpenCollection("durable_bulk")
	if err != nil {
		_ = manager.Close()
		_ = owner.Close()
		t.Fatalf("open collection: %v", err)
	}

	// Prebuild disjoint batches for each producer and wave.
	batchIDs := make([][][][]byte, producers)
	batchDocs := make([][][][]byte, producers)
	for p := 0; p < producers; p++ {
		batchIDs[p] = make([][][]byte, waves)
		batchDocs[p] = make([][][]byte, waves)
		for w := 0; w < waves; w++ {
			batchIDs[p][w], batchDocs[p][w] = buildNativeBulkBSONBatch(t, p*waves+w, batchSize, 64)
		}
	}

	statsBefore := owner.Stats()
	syncsBefore := parseStatUint64(t, statsBefore, "treedb.command_wal.group_commit.syncs_total")

	// Deterministically overlap concurrent InsertMany batches at the CommandWAL
	// sync boundary: hold the first physical WAL sync until at least 3 other
	// concurrent batches have appended while it is in flight (bounded by 200ms
	// so a lock-hold regression cannot deadlock). This proves workers release
	// nativeBulkInsertMu before awaiting durable sync and coalesce into a shared
	// group-commit wave even under GOMAXPROCS=1 or slow runners.
	var (
		firstSyncHeld atomic.Bool
		appendsInSync atomic.Int32
		releaseOnce   sync.Once
		releaseSync   = make(chan struct{})
	)
	restoreCut := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		switch e.Point {
		case durabilitycut.BeforeDependencyFileSync:
			if firstSyncHeld.CompareAndSwap(false, true) {
				timer := time.NewTimer(200 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-releaseSync:
				case <-timer.C:
				}
			}
		case durabilitycut.AfterDependencyAppend:
			if firstSyncHeld.Load() && appendsInSync.Add(1) >= 3 {
				releaseOnce.Do(func() { close(releaseSync) })
			}
		}
		return nil
	}))
	defer restoreCut()

	start := make(chan struct{})
	errs := make([]error, producers)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			<-start
			for w := 0; w < waves; w++ {
				if (pid+w)%2 == 0 {
					if err := col.InsertBatchValidatedBSONNoResultIDs(batchIDs[pid][w], batchDocs[pid][w]); err != nil {
						errs[pid] = err
						return
					}
				} else {
					if _, err := col.InsertBatchValidatedBSON(batchIDs[pid][w], batchDocs[pid][w]); err != nil {
						errs[pid] = err
						return
					}
				}
			}
		}(p)
	}
	close(start)
	wg.Wait()
	restoreCut()

	for p, err := range errs {
		if err != nil {
			t.Fatalf("producer %d failed: %v", p, err)
		}
	}

	statsAfter := owner.Stats()
	syncsAfter := parseStatUint64(t, statsAfter, "treedb.command_wal.group_commit.syncs_total")
	maxGroup := parseStatUint64(t, statsAfter, "treedb.command_wal.group_commit.group_size_max")
	durableLSN := parseStatUint64(t, statsAfter, "treedb.command_wal.durable_wal_lsn")
	maxLSN := parseStatUint64(t, statsAfter, "treedb.command_wal.max_lsn")
	totalBatches := uint64(producers * waves)
	syncDelta := syncsAfter - syncsBefore
	if durableLSN < maxLSN {
		t.Fatalf("post-ACK durable_wal_lsn=%d < max_lsn=%d", durableLSN, maxLSN)
	}
	if syncDelta >= totalBatches || maxGroup < 2 {
		t.Fatalf("expected concurrent durable InsertMany batches to group-commit: batches=%d syncs=%d max_group=%d", totalBatches, syncDelta, maxGroup)
	}

	// Simulate an abrupt crash immediately after ACK (before any Checkpoint or
	// graceful Close) by snapshotting the live directory and reopening the copy.
	crashDir := t.TempDir()
	copyDirTreeForCrashTest(t, dir, crashDir)

	if err := manager.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatalf("close owner: %v", err)
	}

	crashOpts := treedb.OptionsForSemantic(treedb.ProfileCommandWALDurable, crashDir)
	crashOpts.BackgroundCheckpointInterval = -1
	recovered, err := treedb.Open(crashOpts)
	if err != nil {
		t.Fatalf("reopen crash snapshot: %v", err)
	}
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Errorf("close recovered: %v", err)
		}
	}()
	recBackend, err := recovered.NativeSemanticBackend()
	if err != nil {
		t.Fatalf("recovered backend: %v", err)
	}
	recMgr := NewCollectionManager(recBackend)
	defer func() {
		if err := recMgr.Close(); err != nil {
			t.Errorf("close recovered manager: %v", err)
		}
	}()
	recCol, err := recMgr.OpenCollection("durable_bulk")
	if err != nil {
		t.Fatalf("open recovered collection: %v", err)
	}
	for p := 0; p < producers; p++ {
		for w := 0; w < waves; w++ {
			for _, idx := range []int{0, batchSize / 2, batchSize - 1} {
				got, err := recCol.Get(batchIDs[p][w][idx])
				if err != nil || !bytes.Equal(got, batchDocs[p][w][idx]) {
					t.Fatalf("crash-recovered p=%d w=%d idx=%d mismatch: gotLen=%d wantLen=%d err=%v", p, w, idx, len(got), len(batchDocs[p][w][idx]), err)
				}
			}
		}
	}
	// Verify unique index survived crash replay.
	sampleEmail := fmt.Sprintf("user-%02d-%07d@example.com", 0, 0)
	gotIDs, err := recCol.FindByIndex("email_1", sampleEmail)
	if err != nil || len(gotIDs) != 1 || !bytes.Equal(gotIDs[0], batchIDs[0][0][0]) {
		t.Fatalf("crash-recovered email_1 lookup=%q ids=%v err=%v", sampleEmail, gotIDs, err)
	}
}
