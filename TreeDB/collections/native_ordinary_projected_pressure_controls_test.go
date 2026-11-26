package collections

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeOrdinaryProjectedGenerationControls(t *testing.T) {
	const rollovers = "treedb.cache.cow.rollovers_total"
	const refusals = "treedb.cache.cow.native_projected_refusals_total"
	opts := treedb.OptionsForBenchmark(treedb.ProfileCommandWALRelaxed, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
	opts.COWMemtableLimits.MaxGenerationBytes = 64 << 20
	// Keep the seeded generation active until the explicit relief request.
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.BackgroundIndexVacuumInterval = -1
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
	const name = "docs"
	if _, err = manager.CreateCollection(&CollectionMeta{Name: name, Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := bson.Marshal(bson.D{{Key: "name", Value: "same"}, {Key: "pad", Value: string(bytes.Repeat([]byte("a"), 440))}})
	if err != nil {
		t.Fatal(err)
	}
	batch := func(start, count int) ([][]byte, [][]byte) {
		ids, docs := make([][]byte, count), make([][]byte, count)
		for i := range ids {
			ids[i], docs[i] = []byte(fmt.Sprintf("u-%09d", start+i)), doc
		}
		return ids, docs
	}
	// Prepare a real 64k-row active tail below the advisory generation watermark
	// whose next 16k batch projects past MaxGenerationBytes under StandaloneCOWMemtableLimits.
	const size = 16000
	const seedBatches = 4
	for i := 0; i < seedBatches; i++ {
		ids, docs := batch(i*size, size)
		if stored, err := col.InsertBatch(ids, docs); err != nil || len(stored) != size {
			t.Fatalf("seed %d: %d IDs, %v", i, len(stored), err)
		}
	}
	ids, docs := batch(seedBatches*size, size)
	checkCapacity := func(t *testing.T, fn func() error) {
		t.Helper()
		if err := backend.JoinNativeSemanticCleanupForTest(); err != nil {
			t.Fatal(err)
		}
		before, err := backend.ObserveNativeProducer()
		if err != nil || before.UnderPressure {
			t.Fatalf("expected below-advisory active tail: %+v err=%v", before, err)
		}
		stats := owner.Stats()
		next := backend.CommandWALNextLSN()
		err = fn()
		if !errors.Is(err, memtable.ErrCOWCapacity) || backenddb.CommitPublicationAccepted(err) || errors.Is(err, ErrCommitAmbiguous) {
			t.Fatalf("expected preaccept capacity refusal, got %v", err)
		}
		if err := backend.JoinNativeSemanticCleanupForTest(); err != nil {
			t.Fatal(err)
		}
		after, err := backend.ObserveNativeProducer()
		afterStats := owner.Stats()
		if err != nil || next != backend.CommandWALNextLSN() || before.LogicalSequence != after.LogicalSequence ||
			stats[rollovers] != afterStats[rollovers] || stats[refusals] != afterStats[refusals] || before.ReservedBytes != after.ReservedBytes {
			t.Fatalf("capacity refusal changed state: before=%+v after=%+v next=%d..%d rollovers=%s..%s refusals=%s..%s observeErr=%v",
				before, after, next, backend.CommandWALNextLSN(), stats[rollovers], afterStats[rollovers], stats[refusals], afterStats[refusals], err)
		}
	}
	t.Run("validated_bson", func(t *testing.T) {
		checkCapacity(t, func() error {
			_, err := col.InsertBatchValidatedBSON(ids, docs)
			return err
		})
	})
	t.Run("gateway_with_result", func(t *testing.T) {
		checkCapacity(t, func() error {
			_, err := col.InsertBatchFromWire(ids, docs)
			return err
		})
	})
	t.Run("gateway_no_result", func(t *testing.T) {
		checkCapacity(t, func() error { return col.NativewireInsertBatchNoResultIDs(ids, docs, true) })
	})
	t.Run("manager_one_shot", func(t *testing.T) {
		checkCapacity(t, func() error { return col.nativeJSON.InsertBatch(name, ids, docs) })
	})
	t.Run("explicit_transaction", func(t *testing.T) {
		checkCapacity(t, func() error {
			tx, err := col.nativeJSON.Begin(name)
			if err != nil {
				return err
			}
			defer func() {
				if err := tx.Close(); err != nil {
					t.Error(err)
				}
			}()
			if err := tx.InsertBatch(ids, docs); err != nil {
				return err
			}
			return tx.Commit()
		})
	})
	t.Run("request_larger_than_fresh_generation", func(t *testing.T) {
		// 90k 470B rows require >64 MiB on a fresh empty generation (~47.5 MiB
		// payload + ~24 MiB COW node bound), so even with compact bulk staging
		// it genuinely exceeds MaxGenerationBytes without tripping MaxInFlightBytes.
		oversizedIDs, oversizedDocs := batch(seedBatches*size, 90000)
		checkCapacity(t, func() error {
			_, err := col.InsertBatch(oversizedIDs, oversizedDocs)
			return err
		})
	})

	var refusal *nativeInsertProjectedRefusal
	checkCapacity(t, func() error {
		_, expired, err := col.nativeInsertPreparedOnce(ids, docs, nil, false, nil, &refusal)
		if expired || refusal == nil || refusal.pressure.Domain != "primary" {
			t.Fatalf("expected scoped primary generation refusal: expired=%t refusal=%v error=%v", expired, refusal, err)
		}
		return err
	})
	projection := refusal.pressure.Refusal
	limit := opts.COWMemtableLimits.MaxGenerationBytes
	if projection.HistoryBytes >= limit-limit/4 || projection.HistoryBytes+projection.PendingBytes+projection.DemandBytes <= limit ||
		projection.FreshHistoryBytes > limit {
		t.Fatalf("unexpected exact reservation terms: %+v limit=%d", projection, limit)
	}

	// The original producer gate has returned. Completion of one genuine relief
	// request leaves the logical/WAL frontier unchanged. A second caller with
	// the same receipt must see the new tail and avoid a redundant rollover.
	before := owner.Stats()
	lsn := backend.CommandWALNextLSN()
	if err = backend.PrepareNativeProjectedGeneration(refusal.pressure); err != nil {
		t.Fatal(err)
	}
	turned := owner.Stats()
	prev, _ := strconv.ParseUint(before[rollovers], 10, 64)
	next, _ := strconv.ParseUint(turned[rollovers], 10, 64)
	if next != prev+1 || backend.CommandWALNextLSN() != lsn {
		t.Fatalf("expected one physical-only rollover: rollovers=%d..%d LSN=%d..%d", prev, next, lsn, backend.CommandWALNextLSN())
	}
	if err = backend.PrepareNativeProjectedGeneration(refusal.pressure); err != nil {
		t.Fatal(err)
	}
	if after := owner.Stats(); after[rollovers] != turned[rollovers] || backend.CommandWALNextLSN() != lsn {
		t.Fatalf("stale refusal redrained after generation advanced: %s -> %s", turned[rollovers], after[rollovers])
	}

	if got, err := col.Insert(ids[0], docs[0]); err != nil || !bytes.Equal(got, ids[0]) {
		t.Fatalf("intervening conflicting insert: ID=%q err=%v", got, err)
	}
	lsn = backend.CommandWALNextLSN()
	retry := refusal
	_, expired, err := col.nativeInsertPreparedOnce(ids, docs, nil, false, nil, &retry)
	if !errors.Is(err, ErrDocumentExists) || expired || retry != nil || backend.CommandWALNextLSN() != lsn {
		t.Fatalf("fresh attempt bypassed duplicate validation: err=%v expired=%t retry=%v", err, expired, retry)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "name_idx", Field: "name", ValueType: IndexValueString}); err != nil {
		t.Fatal(err)
	}
	lsn = backend.CommandWALNextLSN()
	retry = refusal
	_, expired, err = col.nativeInsertPreparedOnce(ids[1:], docs[1:], nil, false, nil, &retry)
	if !errors.Is(err, backenddb.ErrConcurrentModification) || expired || retry != nil || backend.CommandWALNextLSN() != lsn {
		t.Fatalf("fresh attempt crossed catalog change: err=%v expired=%t retry=%v", err, expired, retry)
	}
	for _, id := range ids[1:] {
		if got, err := col.Get(id); err != nil || got != nil {
			t.Fatalf("refused request leaked %q: bytes=%d err=%v", id, len(got), err)
		}
	}
}
