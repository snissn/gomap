package collections

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestTypedGraphEncodedAdmissionRetainedBeforeAppend(t *testing.T) {
	dir, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{
		Rows: 8, Tombstones: 8, ValueSlots: 32, OwnedBytes: 4096, EncodedOutputBytes: 8192,
	}, typedGraphColdLimits{ManifestRecords: 128, ManifestBytes: 128 << 10, AssetBytes: 1 << 20, DecodedTermBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	before := col.typedGraphPublicationSnapshot()
	frames := len(collectionCommandWALFrames(t, dir))
	var vlogAppends atomic.Int64
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceValueLog && event.Point == durabilitycut.BeforeDependencyAppend {
			vlogAppends.Add(1)
		}
		return nil
	})
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}},
		{Name: "content", Strings: []string{"a"}},
		{Name: "user", Strings: []string{"u"}},
		{Name: "path", Strings: []string{"p"}},
	}
	retained := []byte(`{"id":"a","remainder":"` + strings.Repeat("x", 64<<10) + `"}`)
	_, _, err = col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{retained}, columns)
	restore()
	coord := col.collectionSchemaCoordinator()
	coord.typedPublicationDebtMu.Lock()
	debt, pending := coord.typedPublicationDebt, coord.typedPublicationPending
	coord.typedPublicationDebtMu.Unlock()
	gotFrames := len(collectionCommandWALFrames(t, dir))
	if !errors.Is(err, errTypedGraphOverlayFoldNeeded) || vlogAppends.Load() != 0 || gotFrames != frames || col.typedGraphPublicationSnapshot() != before || debt != (typedGraphPublicationCost{}) || pending != (typedGraphPublicationCost{}) {
		t.Fatalf("encoded admission err=%v vlog appends=%d WAL frames=%d->%d debt=%+v pending=%+v", err, vlogAppends.Load(), frames, gotFrames, debt, pending)
	}
}

func TestTypedGraphTextEncodedBoundOverflow(t *testing.T) {
	def := TextIndexDefinition{Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "content"}}}
	docs := []columnWriteDocument{{ID: []byte("a")}}
	states := []textDocumentStateValue{{}}
	for _, tail := range []int64{math.MaxInt64, math.MaxInt64 - 1} {
		if _, err := typedGraphTextInsertEncodedBound(def, docs, states, tail, 0); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
			t.Fatalf("overflow tail=%d err=%v", tail, err)
		}
	}
	if got, err := typedGraphTextInsertEncodedBound(def, nil, nil, 0, 0); got != 0 || err != nil {
		t.Fatalf("empty receipt=%d err=%v", got, err)
	}
	def.Version = TextIndexVersionV1
	if _, err := typedGraphTextInsertEncodedBound(def, docs, states, 0, 0); !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("unsupported version err=%v", err)
	}
}

func TestTypedGraphEncodedReceiptAttempts(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphPublicationLimits{Rows: 8, Tombstones: 8, ValueSlots: 32, OwnedBytes: 4096, EncodedOutputBytes: 600}
	cold := typedGraphColdLimits{ManifestRecords: 128, ManifestBytes: 128 << 10, AssetBytes: 1 << 20, DecodedTermBytes: 1 << 20}
	if err := col.reconcileTypedGraphPublication(limits, cold); err != nil {
		t.Fatal(err)
	}
	coord := col.collectionSchemaCoordinator()
	logical := typedGraphPublicationCost{rows: 1, slots: 4, bytes: 36}
	for _, proof := range [][]typedGraphEncodedCost{nil, {{}}} {
		if r, err := col.reserveTypedGraphPublication(logical, proof...); r != nil || !errors.Is(err, ErrHybridSearchUnsupported) {
			t.Fatalf("missing encoded authority admitted: receipt=%v err=%v", r, err)
		}
	}
	reserve := func() *typedGraphPublicationReceipt {
		t.Helper()
		r, err := col.reserveTypedGraphPublication(logical, typedGraphEncodedCost{primary: 100, flush: 200})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	check := func(want int64) {
		t.Helper()
		coord.typedPublicationDebtMu.Lock()
		got := coord.typedPublicationEncodedBytes
		coord.typedPublicationDebtMu.Unlock()
		if got != want {
			t.Fatalf("encoded reserved/attempted=%d want %d", got, want)
		}
	}
	r := reserve()
	check(300)
	r.rejectBeforeAppend()
	r.rejectBeforeAppend()
	check(0)
	r = reserve()
	for _, want := range []int64{300, 400, 500, 600} {
		if err := beginTypedGraphEncodedAttempt([]*typedGraphPublicationReceipt{r}, true); err != nil {
			t.Fatal(err)
		}
		check(want)
	}
	if err := beginTypedGraphEncodedAttempt([]*typedGraphPublicationReceipt{r}, true); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("retry without capacity err=%v", err)
	}
	check(600)
	if err := beginTypedGraphEncodedAttempt([]*typedGraphPublicationReceipt{r, r}, false); !errors.Is(err, ErrVectorIndexSnapshotMismatch) || r.flushAttempted {
		t.Fatalf("duplicate receipt changed phase: err=%v", err)
	}
	// Proven WAL rejection refunds unattempted flush output, not the four
	// attempted primary appends, even if the appender released all pointer pins.
	r.rejectBeforeAppend()
	check(400)
	// Derived-only invalidation exercises real reconciliation's ledger handling;
	// this is not a simulated backend recovery/append failure claim.
	before := coord.typedPublication.Load()
	invalid := *before
	invalid.invalid = true
	if !coord.typedPublication.CompareAndSwap(before, &invalid) {
		t.Fatal("unexpected publication")
	}
	if err := col.reconcileTypedGraphPublication(limits, cold); err != nil {
		t.Fatal(err)
	}
	check(400)
}

func TestTypedGraphEncodedBufferedFlushAndFailedAppend(t *testing.T) {
	for _, mode := range []struct{ cut, async bool }{{}, {cut: true}, {async: true}, {cut: true, async: true}} {
		t.Run(fmt.Sprintf("cut=%v/async=%v", mode.cut, mode.async), func(t *testing.T) {
			meta := typedMinimaCollectionMeta()
			if mode.async {
				meta.Options.DisableBufferedIndexedAsyncFlush = false
				meta.Options.BufferedIndexedWriteMaxDocuments = 1
			}
			dir, db, col := openTypedMinimaCollectionMeta(t, meta)
			defer db.Close()
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatal(err)
			}
			limits := typedGraphPublicationLimits{Rows: 32, Tombstones: 8, ValueSlots: 128, OwnedBytes: 1 << 20, EncodedOutputBytes: 4 << 20}
			cold := typedGraphColdLimits{ManifestRecords: 128, ManifestBytes: 128 << 10, AssetBytes: 4 << 20, DecodedTermBytes: 4 << 20}
			if err := col.reconcileTypedGraphPublication(limits, cold); err != nil {
				t.Fatal(err)
			}
			coord := col.collectionSchemaCoordinator()
			columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"alpha alpha beta"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}}}
			insert := func(id string) error {
				_, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte(id)}, [][]byte{[]byte(fmt.Sprintf(`{"id":%q,"remainder":%q}`, id, strings.Repeat("x", 64<<10)))}, columns)
				return err
			}
			var spent int64
			if mode.cut {
				injected := errors.New("after primary append")
				var fired atomic.Bool
				frames := len(collectionCommandWALFrames(t, dir))
				restore := durabilitycut.Install(func(event durabilitycut.Event) error {
					if event.Resource == durabilitycut.ResourceValueLog && event.Point == durabilitycut.AfterDependencyAppend && fired.CompareAndSwap(false, true) {
						return injected
					}
					return nil
				})
				err := insert("a")
				restore()
				coord.typedPublicationDebtMu.Lock()
				spent = coord.typedPublicationEncodedBytes
				logical := coord.typedPublicationDebt
				coord.typedPublicationDebtMu.Unlock()
				if !errors.Is(err, injected) || !fired.Load() || spent <= 64<<10 || logical != (typedGraphPublicationCost{}) || len(collectionCommandWALFrames(t, dir)) != frames {
					t.Fatalf("failed append err=%v fired=%v spent=%d logical=%+v", err, fired.Load(), spent, logical)
				}
			}
			for _, id := range []string{"a", "b", "c"} {
				if err := insert(id); err != nil {
					t.Fatal(err)
				}
			}
			coord.typedPublicationDebtMu.Lock()
			before := coord.typedPublicationEncodedBytes
			coord.typedPublicationDebtMu.Unlock()
			if before <= spent+3*(64<<10) {
				t.Fatalf("successful retry did not reserve new output: before=%d spent=%d", before, spent)
			}
			if err := col.Flush(); err != nil {
				t.Fatal(err)
			}
			coord.typedPublicationDebtMu.Lock()
			after, pending := coord.typedPublicationEncodedBytes, coord.typedPublicationPending
			coord.typedPublicationDebtMu.Unlock()
			if before != after || pending != (typedGraphPublicationCost{}) {
				t.Fatalf("flush transfer changed encoded debt: before=%d after=%d pending=%+v", before, after, pending)
			}
			if err := col.reconcileTypedGraphPublication(limits, cold); err != nil {
				t.Fatal(err)
			}
			if coord.typedPublicationEncodedBytes != before {
				t.Fatal("reconcile erased encoded attempts")
			}
		})
	}
}

func TestTypedGraphTextEncodedBoundActualTables(t *testing.T) {
	meta := typedMinimaCollectionMeta()
	meta.TextIndexes[0].StorePositions, meta.TextIndexes[0].StoreOffsets = true, true
	_, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()
	longID := []byte(strings.Repeat("long-existing-id", 32))
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"alpha"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{longID}, [][]byte{[]byte(fmt.Sprintf(`{"id":%q}`, longID))}, columns); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	snap := db.AcquireSnapshot()
	defer snap.Close()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	def := catalog.meta.TextIndexes[0]
	opts, err := collectionPlannerOptionsForDB(db, catalog.meta)
	if err != nil {
		t.Fatal(err)
	}
	tail, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionTextV2DocMapRootName(catalog.meta.Name, def.Name), encodeTextV2BlockKey(1), nil)
	if err != nil || !found {
		t.Fatalf("existing tail: found=%v err=%v", found, err)
	}
	for _, n := range []int{1, 8, 32, 127, 128, 129, 256} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			docs := make([]columnWriteDocument, n)
			states := make([]textDocumentStateValue, n)
			mutations := make([]textDocumentMutation, n)
			for i := range docs {
				docs[i].ID = []byte(fmt.Sprintf("new-%04d-%s", i, strings.Repeat("z", i%73)))
				states[i] = textDocumentStateValue{Fields: []textDocumentFieldState{{Field: "content", Length: 1, Terms: []textDocumentTermState{{Term: "alpha", Frequency: 1, Positions: []uint32{0}, Offsets: []textTokenOffset{{Start: 0, End: 5}}}}}}}
				mutations[i] = textDocumentMutation{documentID: docs[i].ID, preparedNew: &states[i], setNew: true}
			}
			var roots []string
			var policies []backenddb.OrderedRootStoragePolicy
			var tables []memtable.Table
			defer func() { resetCollectionTables(tables) }()
			if err := appendSingleTextV2IndexMutationDeltas(snap, catalog, opts, def, mutations, &roots, make(map[string]uint64), &policies, &tables); err != nil {
				t.Fatal(err)
			}
			if len(tables) != 7 {
				t.Fatalf("expected actual seven-table producer, got %d", len(tables))
			}
			var actual int64
			for _, table := range tables {
				actual += table.Size() + int64(table.Len())*typedGraphRootEntryFrameBytes
			}
			bound, err := typedGraphTextInsertEncodedBound(def, docs, states, int64(len(tail)), 0)
			if err != nil || bound < actual {
				t.Fatalf("one receipt reserved=%d actual=%d err=%v", bound, actual, err)
			}
			var summed, pendingIDs int64
			for start := 0; start < n; start += 8 {
				end := min(start+8, n)
				part, err := typedGraphTextInsertEncodedBound(def, docs[start:end], states[start:end], int64(len(tail)), pendingIDs)
				if err != nil {
					t.Fatal(err)
				}
				summed += part
				for _, doc := range docs[start:end] {
					pendingIDs += int64(len(doc.ID))
				}
			}
			if summed < actual {
				t.Fatalf("eight-row receipts reserved=%d actual=%d", summed, actual)
			}
			t.Logf("rows=%d actual-encoded=%d one-receipt=%d ratio=%.2f eight-row-receipts=%d ratio=%.2f", n, actual, bound, float64(bound)/float64(actual), summed, float64(summed)/float64(actual))
		})
	}
}
