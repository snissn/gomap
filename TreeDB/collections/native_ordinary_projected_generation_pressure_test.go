package collections_test

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// These are the two canonical BSON0 cells, with the original standalone caps
// and no test-driven maintenance while inserting.
func TestNativeOrdinaryProjectedGenerationPressure(t *testing.T) {
	for _, batchSize := range []int{1000, 16000} {
		t.Run(strconv.Itoa(batchSize), func(t *testing.T) {
			testNativeOrdinaryProjectedGenerationPressure(t, batchSize)
		})
	}
}

func testNativeOrdinaryProjectedGenerationPressure(t *testing.T, batchSize int) {
	const rows = 100000
	const publications = "treedb.cache.cow.publications_total"
	const rollovers = "treedb.cache.cow.rollovers_total"
	const reserved = "treedb.cache.cow.reserved_bytes"
	const refusals = "treedb.cache.cow.native_projected_refusals_total"
	const reliefNs = "treedb.cache.cow.native_projected_relief_ns"
	const checkpointCalls = "treedb.public.checkpoint.calls_total"
	const acceptedLSN = "treedb.command_wal.live_accepted_max_lsn"
	const appliedLSN = "treedb.command_wal.applied_lsn"
	opts := treedb.OptionsForBenchmark(treedb.ProfileCommandWALRelaxed, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
	opts.IndexOuterLeavesInValueLog = true
	opts.IndexInternalBaseDelta = false
	opts.BackgroundIndexVacuumInterval = -1
	t.Setenv("TREEDB_COLLECTION_DOCUMENT_FORMAT", "bson")

	var owner *treedb.DB
	var backend *backenddb.DB
	var manager *collections.CollectionManager
	var c *collections.Collection
	var held *collections.CollectionReadView
	var budget interface{ Stats() memtable.COWStats }
	open := func(create bool) {
		t.Helper()
		var err error
		owner, err = treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err = owner.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		capture, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		limits := capture.NativeAllocator().Limits()
		budget = capture.NativeAllocator().(interface{ Stats() memtable.COWStats })
		if err := capture.Close(); err != nil {
			t.Fatal(err)
		}
		if limits != opts.COWMemtableLimits {
			t.Fatalf("live capacity changed: %+v want %+v", limits, opts.COWMemtableLimits)
		}
		manager = collections.NewCollectionManager(backend)
		if !manager.UsesNativeCommandWALApply() {
			t.Fatal("fixture did not select native command WAL owner")
		}
		if create {
			_, err = manager.CreateCollection(&collections.CollectionMeta{
				Name: "bench_shape_insert_0",
				Options: collections.CollectionOptions{
					DocumentFormat:          collections.DocumentFormatBSON,
					DataRootStoragePolicy:   collections.RootStorageCompressed,
					IndexStateStoragePolicy: collections.RootStorageCompressed,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		c, err = manager.OpenCollection("bench_shape_insert_0")
		if err != nil {
			t.Fatal(err)
		}
	}
	closeOwner := func() {
		t.Helper()
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
			held = nil
		}
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
			manager = nil
		}
		if owner == nil {
			return
		}
		err := owner.Close()
		owner = nil
		if err != nil {
			t.Errorf("original Close: %v", err)
		}
		if budget != nil {
			if s := budget.Stats(); s.TotalBytes != 0 || s.ReservedBytes != 0 || s.Views != 0 || s.ExternalLeases != 0 || s.Generations != 0 || s.Sources != 0 {
				t.Errorf("original COW budget after Close: %+v", s)
			}
		}
		pins := backend.ValueLogIdentityPinRegistry()
		if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 || s.ActiveStableNamespaceLinks != 0 || pins.CleanupError() != nil {
			t.Errorf("original stable custody after Close: %+v: %v", s, pins.CleanupError())
		}
	}
	t.Cleanup(closeOwner)
	open(true)
	stat := func(stats map[string]string, key string) uint64 {
		t.Helper()
		n, err := strconv.ParseUint(stats[key], 10, 64)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		return n
	}
	initial := owner.Stats()
	acknowledged, underWatermarkRefusals := 0, 0
	for acknowledged < rows {
		count := min(rows-acknowledged, batchSize)
		ids, documents := benchmarkDocumentBatch(t, acknowledged, count, true)
		before, err := backend.ObserveNativeProducer()
		if err != nil {
			t.Fatal(err)
		}
		beforeStats := owner.Stats()
		next := backend.CommandWALNextLSN()
		got, insertErr := c.InsertBatch(ids, documents)
		after, observeErr := backend.ObserveNativeProducer()
		afterStats := owner.Stats()
		if observeErr != nil {
			t.Fatal(observeErr)
		}
		if insertErr != nil {
			if !errors.Is(insertErr, memtable.ErrCOWCapacity) ||
				backenddb.CommitPublicationAccepted(insertErr) || len(got) != 0 || before.LogicalSequence != after.LogicalSequence ||
				next != backend.CommandWALNextLSN() || stat(beforeStats, publications) != stat(afterStats, publications) ||
				stat(afterStats, reserved) > stat(beforeStats, reserved) {
				t.Fatalf("unclean refusal at %d: err=%v IDs=%d logical=%d..%d next LSN=%d..%d reserved=%d..%d",
					acknowledged, insertErr, len(got), before.LogicalSequence, after.LogicalSequence, next, backend.CommandWALNextLSN(),
					stat(beforeStats, reserved), stat(afterStats, reserved))
			}
			t.Errorf("public InsertBatch stopped after %d/%d ACKs: below advisory=%t history=%d reserve=%d: %v",
				acknowledged, rows, !before.UnderPressure, stat(beforeStats, "treedb.cache.cow.history_bytes"), stat(beforeStats, reserved), insertErr)
			break
		}
		if len(got) != count || after.LogicalSequence != before.LogicalSequence+1 ||
			backend.CommandWALNextLSN() != next+1 || stat(afterStats, publications) != stat(beforeStats, publications)+1 ||
			stat(afterStats, reserved) > opts.COWMemtableLimits.MaxInFlightBytes {
			t.Fatalf("batch at %d: IDs=%d want=%d logical=%d..%d LSN=%d..%d publications=%d..%d reserved=%d..%d",
				acknowledged, len(got), count, before.LogicalSequence, after.LogicalSequence, next, backend.CommandWALNextLSN(),
				stat(beforeStats, publications), stat(afterStats, publications), stat(beforeStats, reserved), stat(afterStats, reserved))
		}
		reclaimed := stat(afterStats, refusals) - stat(beforeStats, refusals)
		if reclaimed > 1 {
			t.Fatalf("batch at %d initiated %d projected relief operations", acknowledged, reclaimed)
		}
		if reclaimed == 1 && !before.Semantic.GenerationUnderPressure {
			underWatermarkRefusals++
		}
		for i := range got {
			if !bytes.Equal(got[i], ids[i]) {
				t.Fatalf("acknowledged ID at %d is %q, want %q", acknowledged+i, got[i], ids[i])
			}
		}
		if held == nil {
			held, err = c.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
		}
		acknowledged += count
	}
	final := owner.Stats()
	if acknowledged == rows {
		// Under the tight non-delete node bound, 1,000-row batches demand well
		// below limit/4 (16 MiB) and fit in a single 64 MiB generation without
		// a below-watermark projected refusal, whereas 16,000-row batches trigger
		// a projected refusal below the advisory watermark and auto-rollover.
		if batchSize >= 16000 && (stat(final, rollovers) <= stat(initial, rollovers) || underWatermarkRefusals == 0) {
			t.Error("no automatic physical rollover from a projected refusal below the advisory watermark")
		}
	}
	if stat(final, checkpointCalls) != stat(initial, checkpointCalls) {
		t.Error("load invoked the public checkpoint")
	}
	heldRead := func() {
		t.Helper()
		if held == nil {
			t.Fatal("cannot verify first acknowledged row: no held read")
		}
		results, err := held.FetchDocumentsByID([][]byte{benchmarkDocumentID(0)}, collections.DocumentFetchOptions{})
		if err != nil || len(results.Results) != 1 || !results.Results[0].Found ||
			!bytes.Equal(results.Results[0].Document, benchmarkBSONDocument(t, 0, true)) {
			t.Fatalf("held first acknowledgement changed: %+v err=%v", results, err)
		}
	}
	census := func() {
		t.Helper()
		count := 0
		truncated, err := c.ScanDocumentsFunc(rows, func(record collections.DocumentRecord) (bool, error) {
			if !bytes.Equal(record.ID, benchmarkDocumentID(count)) ||
				!bytes.Equal(record.Document, benchmarkBSONDocument(t, count, true)) {
				return false, fmt.Errorf("unexpected row at %d: ID=%q bytes=%d", count, record.ID, len(record.Document))
			}
			count++
			return true, nil
		})
		if err != nil || truncated || count != acknowledged {
			t.Fatalf("census=%d want=%d truncated=%t err=%v", count, acknowledged, truncated, err)
		}
	}
	heldRead()
	checkpointStart := time.Now()
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkpointElapsed := time.Since(checkpointStart)
	checkpointed := owner.Stats()
	if stat(checkpointed, checkpointCalls) != stat(final, checkpointCalls)+1 ||
		stat(checkpointed, appliedLSN) < stat(final, acceptedLSN) {
		t.Fatal("explicit checkpoint did not cover the accepted WAL frontier")
	}
	heldRead()
	census()
	closeOwner()
	open(false)
	census()
	closeOwner()
	t.Logf("exact lifecycle: acknowledged=%d rollovers=%d projected_refusals=%d below_watermark=%d foreground_relief=%s checkpoint=%s reserve=%d peak=%d",
		acknowledged, stat(final, rollovers)-stat(initial, rollovers), stat(final, refusals)-stat(initial, refusals), underWatermarkRefusals,
		time.Duration(stat(final, reliefNs)-stat(initial, reliefNs)), checkpointElapsed, stat(final, reserved), stat(final, "treedb.cache.cow.peak_bytes"))
}
