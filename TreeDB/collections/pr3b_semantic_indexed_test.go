package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestPR3bSemanticRawRecordsSurviveMutableQueuedActiveRequeued(t *testing.T) {
	d, mgr, col := pr3bSemanticTestCollection(t)
	defer func() { _ = d.Close() }()
	pr3bSeedSemanticUser(t, col)

	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update:     setJSONCity("sea"),
	}}); err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatal("city update was not buffered")
	}

	col.writeDomain.mu.RLock()
	mutableRecords := cloneIndexedSemanticRecords(col.writeDomain.indexedSemanticRecords)
	queuedBeforeRotate := len(col.writeDomain.indexedFlushUnits)
	publishingBeforeRotate := len(col.writeDomain.indexedPublishingUnits)
	col.writeDomain.mu.RUnlock()
	if queuedBeforeRotate != 0 || publishingBeforeRotate != 0 {
		t.Fatalf("pre-rotate queued=%d publishing=%d want 0/0", queuedBeforeRotate, publishingBeforeRotate)
	}
	pr3bRequireCitySemanticRecord(t, mutableRecords, "hnl", "sea")
	pr3bRequireIndexIDs(t, col, "city", "sea", "u1")
	pr3bRequireIndexIDs(t, col, "city", "hnl")

	stats := mgr.StatsSnapshot()
	if got := stats.IndexedSemanticRawRecords; got != 1 {
		t.Fatalf("raw semantic records after stage=%d want 1", got)
	}
	if got := stats.IndexedSemanticRawIndexDeltas; got != 1 {
		t.Fatalf("raw semantic index deltas after stage=%d want 1", got)
	}
	if got := stats.IndexedSemanticFallbackRecords; got != 1 {
		t.Fatalf("fallback semantic records after stage=%d want 1", got)
	}
	if got := stats.IndexedSemanticEffectiveRecords; got != 0 {
		t.Fatalf("effective semantic records after stage=%d want 0", got)
	}
	if got := stats.PendingIndexedSemanticRecords; got != 1 {
		t.Fatalf("pending semantic records after stage=%d want 1", got)
	}
	pr3bRequireSemanticMetricKeys(t, mgr)

	col.writeDomain.mu.Lock()
	if !rotateIndexedMutableToFlushUnitLocked(col.writeDomain) {
		col.writeDomain.mu.Unlock()
		t.Fatal("rotate indexed mutable state returned false")
	}
	queuedRecords := cloneIndexedSemanticRecords(col.writeDomain.indexedFlushUnits[0].semanticRecords)
	mutableAfterRotate := len(col.writeDomain.indexedSemanticRecords)
	col.writeDomain.mu.Unlock()
	if mutableAfterRotate != 0 {
		t.Fatalf("mutable semantic records after rotate=%d want 0", mutableAfterRotate)
	}
	pr3bRequireCitySemanticRecord(t, queuedRecords, "hnl", "sea")
	pr3bRequireIndexIDs(t, col, "city", "sea", "u1")

	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare async publish returned nil work")
	}
	defer collectionTestCloseIndexedFlushWork(work)
	if got := work.batch.state; got != coalescedFlushBatchActive {
		t.Fatalf("prepared batch state=%d want active", got)
	}
	pr3bRequireCitySemanticRecord(t, work.batch.semanticRecords, "hnl", "sea")
	col.writeDomain.mu.RLock()
	activeRecords := cloneIndexedSemanticRecords(col.writeDomain.indexedPublishingUnits[0].semanticRecords)
	queuedAfterPrepare := len(col.writeDomain.indexedFlushUnits)
	col.writeDomain.mu.RUnlock()
	if queuedAfterPrepare != 0 {
		t.Fatalf("queued units after prepare=%d want 0", queuedAfterPrepare)
	}
	pr3bRequireCitySemanticRecord(t, activeRecords, "hnl", "sea")

	injectedErr := errors.New("injected PR3b publish failure")
	if err := col.completePreparedIndexedFlush(work, 0, nil, injectedErr, 0, 0, 0); !errors.Is(err, injectedErr) {
		t.Fatalf("complete failure err=%v want injected error", err)
	}
	if got := work.batch.state; got != coalescedFlushBatchRequeued {
		t.Fatalf("failed batch state=%d want requeued", got)
	}
	col.writeDomain.mu.RLock()
	requeuedRecords := cloneIndexedSemanticRecords(col.writeDomain.indexedFlushUnits[0].semanticRecords)
	publishingAfterFailure := len(col.writeDomain.indexedPublishingUnits)
	col.writeDomain.mu.RUnlock()
	if publishingAfterFailure != 0 {
		t.Fatalf("publishing units after failure=%d want 0", publishingAfterFailure)
	}
	pr3bRequireCitySemanticRecord(t, requeuedRecords, "hnl", "sea")
	pr3bRequireIndexIDs(t, col, "city", "sea", "u1")

	stats = mgr.StatsSnapshot()
	if got := stats.IndexedSemanticRawRecords; got != 1 {
		t.Fatalf("raw semantic records after requeue=%d want 1", got)
	}
	if got := stats.PendingIndexedSemanticRecords; got != 1 {
		t.Fatalf("pending semantic records after requeue=%d want 1", got)
	}
	if got := stats.IndexedSemanticEffectiveRecords; got != 0 {
		t.Fatalf("effective semantic records after requeue=%d want 0", got)
	}

	if err := col.Flush(); err != nil {
		t.Fatalf("flush requeued semantic update: %v", err)
	}
	pr3bRequireIndexIDs(t, col, "city", "sea", "u1")
	stats = mgr.StatsSnapshot()
	if got := stats.PendingIndexedSemanticRecords; got != 0 {
		t.Fatalf("pending semantic records after flush=%d want 0", got)
	}
	if got := stats.IndexedSemanticRawRecords; got != 1 {
		t.Fatalf("raw semantic records after flush=%d want 1", got)
	}
	if got := stats.IndexedSemanticEffectiveRecords; got != 0 {
		t.Fatalf("effective semantic records after flush=%d want 0", got)
	}
}

func TestPR3bRootDeltaCoalescingSkippedSecondaryRootsUseUniqueRoots(t *testing.T) {
	raw := collectionRootDeltaPlanStats{
		secondaryRoots:       2,
		secondaryUniqueRoots: 1,
		entries:              6,
		secondaryDetail: collectionRootDeltaKindStats{
			entries: 6,
		},
	}
	final := collectionRootDeltaPlanStats{
		secondaryRoots:       1,
		secondaryUniqueRoots: 1,
		entries:              4,
		secondaryDetail: collectionRootDeltaKindStats{
			entries: 4,
		},
	}

	domain := &collectionWriteDomain{}
	domain.observeRootDeltaPlanCoalescing(raw, final)
	if got := domain.indexedSemanticSkippedSecondaryRoots.Load(); got != 0 {
		t.Fatalf("skipped secondary roots=%d want 0 for repeated raw units that still publish the root", got)
	}
	domain = &collectionWriteDomain{}
	domain.observeRootDeltaPlanCoalescing(raw, collectionRootDeltaPlanStats{})
	if got := domain.indexedSemanticSkippedSecondaryRoots.Load(); got != 1 {
		t.Fatalf("skipped secondary roots for net-zero plan=%d want 1 unique root", got)
	}
}

func TestPR3bAsyncNetZeroCoalescingRecordsAcceptanceCounters(t *testing.T) {
	d, mgr, col := pr3bSemanticTestCollection(t)
	defer func() { _ = d.Close() }()
	pr3bSeedSemanticUser(t, col)

	const (
		netZeroDocs  = 2
		netZeroBytes = 32
	)
	before := mgr.StatsSnapshot()
	col.writeDomain.mu.Lock()
	col.writeDomain.indexedFlushUnits = []indexedFlushUnit{{
		docCount:     netZeroDocs,
		byteCount:    netZeroBytes,
		rootRunCount: 1,
	}}
	col.writeDomain.count = netZeroDocs
	col.writeDomain.bufferedBytes = netZeroBytes
	col.writeDomain.mu.Unlock()

	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async net-zero publish: %v", err)
	}
	if work != nil {
		collectionTestCloseIndexedFlushWork(work)
		t.Fatal("prepare async net-zero publish returned work")
	}

	stats := mgr.StatsSnapshot()
	if got := stats.PendingDocuments; got != 0 {
		t.Fatalf("pending docs after async net-zero prepare=%d want 0", got)
	}
	if got := stats.CoalescedFlushBatches - before.CoalescedFlushBatches; got != 1 {
		t.Fatalf("coalesced flush batches after async net-zero prepare=%d want 1", got)
	}
	if got := stats.CoalescedFlushBatchUnits - before.CoalescedFlushBatchUnits; got != 1 {
		t.Fatalf("coalesced flush batch units after async net-zero prepare=%d want 1", got)
	}
	if got := stats.CoalescedFlushBatchDocs - before.CoalescedFlushBatchDocs; got != netZeroDocs {
		t.Fatalf("coalesced flush batch docs after async net-zero prepare=%d want %d", got, netZeroDocs)
	}
	if got := stats.CoalescedFlushBatchBytes - before.CoalescedFlushBatchBytes; got != netZeroBytes {
		t.Fatalf("coalesced flush batch bytes after async net-zero prepare=%d want %d", got, netZeroBytes)
	}
	if got := stats.CoalescedFlushNetZeroBatches - before.CoalescedFlushNetZeroBatches; got != 1 {
		t.Fatalf("coalesced flush net-zero batches after async net-zero prepare=%d want 1", got)
	}
}

func TestPR3bSemanticRepeatedSameDocumentUpdatesSerialEquivalent(t *testing.T) {
	d, mgr, col := pr3bSemanticTestCollection(t)
	defer func() { _ = d.Close() }()
	pr3bSeedSemanticUser(t, col)

	firstCalls := 0
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			firstCalls++
			if !bytes.Contains(current, []byte(`"city":"hnl"`)) {
				return nil, false, fmt.Errorf("first callback current=%s want city hnl", current)
			}
			return []byte(`{"email":"a@example.com","city":"sea","score":1}`), true, nil
		},
	}}); err != nil {
		t.Fatalf("first UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatal("first update was not buffered")
	}

	secondCalls := 0
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			secondCalls++
			if !bytes.Contains(current, []byte(`"city":"sea"`)) || !bytes.Contains(current, []byte(`"score":1`)) {
				return nil, false, fmt.Errorf("second callback current=%s want buffered city sea score 1", current)
			}
			return []byte(`{"email":"a@example.com","city":"hnl","score":2}`), true, nil
		},
	}}); err != nil {
		t.Fatalf("second UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatal("second update was not buffered")
	}
	if firstCalls != 1 || secondCalls != 1 {
		t.Fatalf("callback calls first=%d second=%d want 1/1", firstCalls, secondCalls)
	}

	pr3bRequireIndexIDs(t, col, "city", "hnl", "u1")
	pr3bRequireIndexIDs(t, col, "city", "sea")
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get buffered u1: %v", err)
	}
	if !bytes.Contains(got, []byte(`"city":"hnl"`)) || !bytes.Contains(got, []byte(`"score":2`)) {
		t.Fatalf("buffered u1=%s want city hnl score 2", got)
	}

	stats := mgr.StatsSnapshot()
	if got := stats.IndexedSemanticRawRecords; got != 2 {
		t.Fatalf("raw semantic records before flush=%d want 2", got)
	}
	if got := stats.IndexedSemanticRawIndexDeltas; got != 2 {
		t.Fatalf("raw semantic index deltas before flush=%d want 2", got)
	}
	if got := stats.IndexedSemanticEffectiveRecords; got != 0 {
		t.Fatalf("effective semantic records before flush=%d want 0", got)
	}
	if got := stats.PendingIndexedSemanticRecords; got != 2 {
		t.Fatalf("pending semantic records before flush=%d want 2", got)
	}

	if err := col.Flush(); err != nil {
		t.Fatalf("flush buffered serial updates: %v", err)
	}
	pr3bRequireIndexIDs(t, col, "city", "hnl", "u1")
	pr3bRequireIndexIDs(t, col, "city", "sea")
	stats = mgr.StatsSnapshot()
	if got := stats.PendingIndexedSemanticRecords; got != 0 {
		t.Fatalf("pending semantic records after flush=%d want 0", got)
	}
	if got := stats.IndexedSemanticRawRecords; got != 2 {
		t.Fatalf("raw semantic records after flush=%d want 2", got)
	}
	if got := stats.IndexedSemanticEffectiveRecords; got != 0 {
		t.Fatalf("effective semantic records after flush=%d want 0", got)
	}
}

func TestPR3bSemanticNonUniqueChangeChangeBackFallsBackRawOnly(t *testing.T) {
	d, mgr, col := pr3bSemanticTestCollection(t)
	defer func() { _ = d.Close() }()
	pr3bSeedSemanticUser(t, col)

	for _, city := range []string{"sea", "hnl"} {
		if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
			DocumentID: []byte("u1"),
			Update:     setJSONCity(city),
		}}); err != nil {
			t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges city=%s: %v", city, err)
		} else if !batched {
			t.Fatalf("city=%s update was not buffered", city)
		}
	}

	col.writeDomain.mu.RLock()
	records := cloneIndexedSemanticRecords(col.writeDomain.indexedSemanticRecords)
	col.writeDomain.mu.RUnlock()
	if got := len(records); got != 2 {
		t.Fatalf("mutable semantic records=%d want 2", got)
	}
	for i, record := range records {
		if record.fallback != indexedSemanticFallbackRawOnly {
			t.Fatalf("record %d fallback=%d want raw-only", i, record.fallback)
		}
	}
	pr3bRequireCitySemanticRecord(t, records[:1], "hnl", "sea")
	pr3bRequireCitySemanticRecord(t, records[1:], "sea", "hnl")
	pr3bRequireIndexIDs(t, col, "city", "hnl", "u1")
	pr3bRequireIndexIDs(t, col, "city", "sea")

	if err := col.Flush(); err != nil {
		t.Fatalf("flush change-change-back updates: %v", err)
	}
	pr3bRequireIndexIDs(t, col, "city", "hnl", "u1")
	stats := mgr.StatsSnapshot()
	if got := stats.IndexedSemanticRawRecords; got != 2 {
		t.Fatalf("raw semantic records=%d want 2", got)
	}
	if got := stats.IndexedSemanticFallbackRecords; got != 2 {
		t.Fatalf("fallback semantic records=%d want 2", got)
	}
	if got := stats.IndexedSemanticEffectiveRecords; got != 0 {
		t.Fatalf("effective semantic records=%d want 0", got)
	}
}

func TestPR3bSemanticUniqueHandoffFallsBackToMechanicalPath(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"email":"a@example.com"}`), []byte(`{"email":"b@example.com"}`)},
	); err != nil {
		t.Fatalf("insert users: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush seed users: %v", err)
	}

	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONEmail("c@example.com")},
		{DocumentID: []byte("u2"), Update: setJSONEmail("a@example.com")},
	})
	if err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges unique handoff: %v", err)
	}
	if batched {
		t.Fatalf("unique handoff batched=%v results=%+v want explicit fallback", batched, results)
	}
	stats := mgr.StatsSnapshot()
	if got := stats.IndexedSemanticRawRecords; got != 0 {
		t.Fatalf("raw semantic records after unique fallback=%d want 0", got)
	}
	if got := stats.PendingIndexedSemanticRecords; got != 0 {
		t.Fatalf("pending semantic records after unique fallback=%d want 0", got)
	}
	pr3bRequireIndexIDs(t, col, "email", "a@example.com", "u1")
	pr3bRequireIndexIDs(t, col, "email", "b@example.com", "u2")

	published, err := col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONEmail("c@example.com")},
		{DocumentID: []byte("u2"), Update: setJSONEmail("a@example.com")},
	})
	if err != nil {
		t.Fatalf("mechanical UpdateBatch unique handoff: %v", err)
	}
	if len(published) != 2 || !published[0].Modified || !published[1].Modified {
		t.Fatalf("mechanical handoff results=%+v want two modified rows", published)
	}
	pr3bRequireIndexIDs(t, col, "email", "a@example.com", "u2")
	pr3bRequireIndexIDs(t, col, "email", "c@example.com", "u1")
	stats = mgr.StatsSnapshot()
	if got := stats.IndexedSemanticRawRecords; got != 0 {
		t.Fatalf("raw semantic records after mechanical handoff=%d want 0", got)
	}
	if got := stats.PendingIndexedSemanticRecords; got != 0 {
		t.Fatalf("pending semantic records after mechanical handoff=%d want 0", got)
	}
}

func pr3bSemanticTestCollection(tb testing.TB) (*backenddb.DB, *CollectionManager, *Collection) {
	tb.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 8,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		_ = d.Close()
		tb.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("open collection: %v", err)
	}
	return d, mgr, col
}

func pr3bSeedSemanticUser(tb testing.TB, col *Collection) {
	tb.Helper()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"a@example.com","city":"hnl","score":0}`)},
	); err != nil {
		tb.Fatalf("insert seed user: %v", err)
	}
	if err := col.Flush(); err != nil {
		tb.Fatalf("flush seed user: %v", err)
	}
}

func pr3bRequireCitySemanticRecord(tb testing.TB, records []indexedSemanticRecord, oldCity, newCity string) {
	tb.Helper()
	if len(records) != 1 {
		tb.Fatalf("semantic records=%d want 1", len(records))
	}
	record := records[0]
	if record.kind != indexedSemanticRecordUpdate {
		tb.Fatalf("semantic record kind=%d want update", record.kind)
	}
	if !bytes.Equal(record.documentID, []byte("u1")) {
		tb.Fatalf("semantic record documentID=%q want u1", record.documentID)
	}
	if record.fallback != indexedSemanticFallbackRawOnly {
		tb.Fatalf("semantic record fallback=%d want raw-only", record.fallback)
	}
	if len(record.indexDeltas) != 1 {
		tb.Fatalf("semantic index deltas=%d want 1", len(record.indexDeltas))
	}
	delta := record.indexDeltas[0]
	if delta.indexName != "city" || delta.rootName != collectionSecondaryRootName("users", "city") || delta.unique {
		tb.Fatalf("city delta identity index=%q root=%q unique=%v", delta.indexName, delta.rootName, delta.unique)
	}
	wantOld, err := encodeIndexScalar(IndexValueString, oldCity)
	if err != nil {
		tb.Fatalf("encode old city %q: %v", oldCity, err)
	}
	wantNew, err := encodeIndexScalar(IndexValueString, newCity)
	if err != nil {
		tb.Fatalf("encode new city %q: %v", newCity, err)
	}
	if !pr3bSemanticValueSetsEqual(delta.oldValues, [][]byte{wantOld}) {
		tb.Fatalf("old city values=%q want %q", delta.oldValues, [][]byte{wantOld})
	}
	if !pr3bSemanticValueSetsEqual(delta.newValues, [][]byte{wantNew}) {
		tb.Fatalf("new city values=%q want %q", delta.newValues, [][]byte{wantNew})
	}
}

func pr3bSemanticValueSetsEqual(left, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !bytes.Equal(left[i], right[i]) {
			return false
		}
	}
	return true
}

func pr3bRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
	tb.Helper()
	ids, err := col.FindByIndexValue(indexName, value)
	if err != nil {
		tb.Fatalf("find index %s=%v: %v", indexName, value, err)
	}
	if len(ids) != len(want) {
		tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
	}
	for i := range want {
		if !bytes.Equal(ids[i], []byte(want[i])) {
			tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
		}
	}
}

func pr3bRequireSemanticMetricKeys(tb testing.TB, mgr *CollectionManager) {
	tb.Helper()
	exported := mgr.Stats()
	for _, key := range []string{
		"treedb.collections.write_domain.pending_indexed_semantic_records",
		"treedb.collections.write_domain.indexed_semantic.raw_records_total",
		"treedb.collections.write_domain.indexed_semantic.raw_index_deltas_total",
		"treedb.collections.write_domain.indexed_semantic.fallback_records_total",
		"treedb.collections.write_domain.indexed_semantic.effective_records_total",
		"treedb.collections.write_domain.indexed_semantic.skipped_secondary_roots_total",
		"treedb.collections.write_domain.indexed_semantic.duplicate_primary_ids_coalesced_total",
		"treedb.collections.write_domain.coalesced_flush_batch.batches_total",
		"treedb.collections.write_domain.coalesced_flush_batch.units_total",
		"treedb.collections.write_domain.coalesced_flush_batch.docs_total",
		"treedb.collections.write_domain.coalesced_flush_batch.bytes_total",
		"treedb.collections.write_domain.coalesced_flush_batch.net_zero_batches_total",
		"treedb.collections.write_domain.root_delta_plan.raw_unit.primary.entries_total",
		"treedb.collections.write_domain.root_delta_plan.final.primary.entries_total",
		"treedb.collections.write_domain.root_delta_plan.squashed_entries_total",
		"treedb.collections.write_domain.root_delta_plan.net_zero_plans_total",
		"treedb.collections.write_domain.primary_only.duplicate_ids_coalesced_total",
		"treedb.collections.write_domain.primary_only.drains_total",
	} {
		if exported[key] == "" {
			tb.Fatalf("exported stats missing %s from %#v", key, exported)
		}
	}
}
