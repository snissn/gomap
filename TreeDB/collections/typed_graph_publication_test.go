package collections

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestTypedGraphPublicationColdReopen(t *testing.T) {
	dir, db, col := openTypedMinimaCollection(t)
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{0, 2, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"after base"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	columns[1].Strings[0] = "replacement"
	if _, err := col.ReplaceTypedBatch([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.DeleteBatch([][]byte{[]byte("a")}); err != nil {
		t.Fatal(err)
	}
	columns[0].Float32Vectors[0] = []float32{0, 0, 3, 0, 0, 0, 0, 0}
	columns[1].Strings[0] = "reinsert"
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Normal Open has no replay seeding hook. Bootstrap must use the captured
	// alias and the actual current typed suffix, not require an empty suffix.
	db = openTypedMinimaDB(t, dir)
	defer db.Close()
	col, err := NewCollectionManager(db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	limits := typedGraphPublicationLimits{Rows: 8, Tombstones: 8, ValueSlots: 32, OwnedBytes: 4096}
	cold := typedGraphColdLimits{ManifestRecords: 128, ManifestBytes: 128 << 10, AssetBytes: 1 << 20, DecodedTermBytes: 1 << 20}
	for _, term := range []string{"records", "metadata", "assets", "decoded"} {
		t.Run(term, func(t *testing.T) {
			bounded := cold
			switch term {
			case "records":
				bounded.ManifestRecords = 1
			case "metadata":
				bounded.ManifestBytes = 1
			case "assets":
				bounded.AssetBytes = 1
			case "decoded":
				bounded.DecodedTermBytes = 1
			}
			if err := col.reconcileTypedGraphPublication(limits, bounded); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
				t.Fatalf("cold budget did not reject: %v", err)
			}
			if state := col.typedGraphPublicationSnapshot(); state == nil || !state.invalid || state.reconciling != nil {
				t.Fatal("failed bootstrap disabled limits or retained drain authority")
			}
		})
	}
	if err := col.reconcileTypedGraphPublication(limits, cold); err != nil {
		t.Fatalf("cold typed bootstrap after normal Open: %v", err)
	}
	state := col.typedGraphPublicationSnapshot()
	if state.physicalRows != 4 || state.tombstones != 1 || state.valueSlots != 12 || len(state.rows) != 1 || state.rows[0].Values[0].Float32Vector[2] != 3 || state.rows[0].Values[1].String != "reinsert" {
		t.Fatalf("cold suffix state: %+v", state)
	}
}

func TestTypedGraphPublicationReconcilePendingFailure(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphPublicationLimits{Rows: 8, Tombstones: 8, ValueSlots: 32, OwnedBytes: 4096}
	cold := typedGraphColdLimits{ManifestRecords: 128, ManifestBytes: 128 << 10, AssetBytes: 1 << 20, DecodedTermBytes: 1 << 20}
	if err := col.reconcileTypedGraphPublication(limits, cold); err != nil {
		t.Fatal(err)
	}
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"pending"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("reconcile pending physical preparation cut")
	restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(ColumnPublishPreparedAssets) error { return injected })
	err := col.Flush()
	restore()
	if err == nil {
		t.Fatal("physical failure not injected")
	}
	state := col.typedGraphPublicationSnapshot()
	if !state.invalid {
		t.Fatalf("uncertain staged publication did not invalidate: %v", err)
	}
	coord := col.collectionSchemaCoordinator()
	coord.typedPublicationDebtMu.Lock()
	pending := coord.typedPublicationPending.rows
	coord.typedPublicationDebtMu.Unlock()
	if pending != 1 {
		t.Fatalf("lost pending debt: %d", pending)
	}
	if err := col.reconcileTypedGraphPublication(limits, cold); !errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("recovery fence bypassed: %v", err)
	}
	coord.typedPublicationDebtMu.Lock()
	debt, remaining := coord.typedPublicationDebt, coord.typedPublicationPending
	coord.typedPublicationDebtMu.Unlock()
	if remaining.rows != 1 || debt.rows != 1 {
		t.Fatalf("failed reconciliation lost debt: %+v / %+v", debt, remaining)
	}
	if state := col.typedGraphPublicationSnapshot(); !state.invalid || state.reconciling != nil {
		t.Fatal("failed reconciliation left authority token or enabled state")
	}
}

func TestTypedGraphPublicationReconcileInvalidPending(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphPublicationLimits{Rows: 8, Tombstones: 8, ValueSlots: 32, OwnedBytes: 4096}
	cold := typedGraphColdLimits{ManifestRecords: 128, ManifestBytes: 128 << 10, AssetBytes: 1 << 20, DecodedTermBytes: 1 << 20}
	if err := col.reconcileTypedGraphPublication(limits, cold); err != nil {
		t.Fatal(err)
	}
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"pending"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	coord := col.collectionSchemaCoordinator()
	col.writeDomain.mu.Lock()
	documents := append([]columnWriteDocument(nil), col.writeDomain.columnDocuments...)
	receipts := append([]*typedGraphPublicationReceipt(nil), col.writeDomain.typedReceipts...)
	col.writeDomain.mu.Unlock()
	input := columnWritePublishInput{operation: ColumnPublishOperationInsert, documents: documents, typedReceipts: receipts}
	cost, err := typedGraphPublicationInputCost(input, limits)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateTypedGraphReceiptInput(coord, input, cost); err != nil {
		t.Fatal(err)
	}
	documents[0].ID = []byte("b") // identical cost is not authority for a different ID
	if err := validateTypedGraphReceiptInput(coord, input, cost); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("forged receipt input accepted: %v", err)
	}
	documents[0] = receipts[0].documents[0]
	input.declaredRowsReady = true
	input.declaredRows = []columnDeclaredRow{{Values: append([]columnDeclaredValue(nil), documents[0].declaredValues...)}}
	if err := validateTypedGraphReceiptInput(coord, input, cost); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("substituted prepared values accepted: %v", err)
	}
	input.declaredRows[0].Values = documents[0].declaredValues
	if err := validateTypedGraphReceiptInput(coord, input, cost); err != nil {
		t.Fatal(err)
	}
	input.declaredRowsReady, input.declaredRows = false, nil
	duplicate := columnWritePublishInput{operation: ColumnPublishOperationInsert, documents: append(documents, documents[0]), typedReceipts: append(receipts, receipts[0])}
	duplicateCost := cost
	duplicateCost.add(cost)
	if err := validateTypedGraphReceiptInput(coord, duplicate, duplicateCost); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("duplicate receipt accepted: %v", err)
	}
	// Derived-only invalidation is an internal readiness condition, not an
	// attempt to clear the backend's independently tested recovery fence.
	before := coord.typedPublication.Load()
	invalid := *before
	invalid.invalid = true
	if !coord.typedPublication.CompareAndSwap(before, &invalid) {
		t.Fatal("fixture invalidation raced")
	}
	if err := col.Flush(); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("ordinary flush bypassed invalid state: %v", err)
	}
	tooSmall := cold
	tooSmall.ManifestRecords = 1
	if err := col.reconcileTypedGraphPublication(limits, tooSmall); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("post-drain cold budget: %v", err)
	}
	coord.typedPublicationDebtMu.Lock()
	debt, pending := coord.typedPublicationDebt, coord.typedPublicationPending
	coord.typedPublicationDebtMu.Unlock()
	if pending.rows != 0 || debt.rows != 1 {
		t.Fatalf("partial reconcile lost installed charge: %+v / %+v", debt, pending)
	}
	if state := col.typedGraphPublicationSnapshot(); !state.invalid || state.reconciling != nil {
		t.Fatal("failed cold phase left ready state or drain authority")
	}
	if err := col.reconcileTypedGraphPublication(limits, cold); err != nil {
		t.Fatal(err)
	}
	state := col.typedGraphPublicationSnapshot()
	if state.invalid || state.physicalRows != 1 || len(state.rows) != 1 {
		t.Fatalf("reconciled state: %+v", state)
	}
	if err := validateTypedGraphReceiptInput(coord, input, cost); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("consumed receipt reused: %v", err)
	}
	coord.typedPublicationDebtMu.Lock()
	defer coord.typedPublicationDebtMu.Unlock()
	if coord.typedPublicationPending.rows != 0 || coord.typedPublicationDebt.rows != 1 {
		t.Fatalf("debt after reconciliation: total=%+v pending=%+v", coord.typedPublicationDebt, coord.typedPublicationPending)
	}
}

func TestTypedGraphPublicationReconcileCapsAndManagers(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphPublicationLimits{Rows: 8, Tombstones: 8, ValueSlots: 32, OwnedBytes: 4096}
	cold := typedGraphColdLimits{ManifestRecords: 1, ManifestBytes: 128 << 10, AssetBytes: 1 << 20, DecodedTermBytes: 1 << 20}
	if err := col.reconcileTypedGraphPublication(limits, cold); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("metadata budget: %v", err)
	}
	failed := col.typedGraphPublicationSnapshot()
	if failed == nil || !failed.invalid || failed.reconciling != nil {
		t.Fatal("initial failed bootstrap did not stay fail-closed")
	}
	cold.ManifestRecords = 128
	other, err := NewCollectionManager(db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, c := range []*Collection{col, other} {
		go func(c *Collection) { <-start; results <- c.reconcileTypedGraphPublication(limits, cold) }(c)
	}
	close(start)
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("concurrent reconcile blocked")
		}
	}
	ready := col.typedGraphPublicationSnapshot()
	if ready == nil || ready.invalid || other.typedGraphPublicationSnapshot() != ready {
		t.Fatal("managers did not converge")
	}
	if err := other.reconcileTypedGraphPublication(limits, cold); err != nil {
		t.Fatal(err)
	}
	if col.typedGraphPublicationSnapshot() != ready {
		t.Fatal("same frontier was rebuilt")
	}
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"cutover"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}}}
	if _, _, err := other.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	if err := other.Flush(); err != nil {
		t.Fatal(err)
	}
	ready = col.typedGraphPublicationSnapshot()
	if _, err := other.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	if err := col.reconcileTypedGraphPublication(limits, cold); err != nil {
		t.Fatal(err)
	}
	if col.typedGraphPublicationSnapshot() == ready || col.typedGraphPublicationSnapshot().physicalRows != 0 {
		t.Fatal("base cutover reused old frontier")
	}
}

func TestTypedGraphPublicationInstrumentedReplay(t *testing.T) {
	const childEnv = "GOMAP_TYPED_PUBLICATION_REPLAY_DIR"
	if dir := os.Getenv(childEnv); dir != "" {
		db := openTypedMinimaDB(t, dir)
		col, err := NewCollectionManager(db).OpenCollection("minima")
		if err != nil {
			t.Fatal(err)
		}
		entered, blocked := make(chan struct{}), make(chan struct{})
		var fired atomic.Bool
		durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Root == dir && event.Resource == durabilitycut.ResourceSeal && event.Point == durabilitycut.BeforePublicationSealWrite && fired.CompareAndSwap(false, true) {
				close(entered)
				<-blocked
			}
			return nil
		})
		ids := [][]byte{[]byte("a"), []byte("b")}
		retained := [][]byte{[]byte(`{"id":"a"}`), []byte(`{"id":"b"}`)}
		columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"alpha", "beta"}}, {Name: "user", Strings: []string{"u", "u"}}, {Name: "path", Strings: []string{"p", "p"}}}
		if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
			t.Fatal(err)
		}
		if err := col.Flush(); err != nil {
			t.Fatal(err)
		}
		columns[0].Float32Vectors = columns[0].Float32Vectors[:1]
		columns[0].Float32Vectors[0] = []float32{0, 0, 2, 0, 0, 0, 0, 0}
		for i := 1; i < len(columns); i++ {
			columns[i].Strings = columns[i].Strings[:1]
		}
		columns[1].Strings[0] = "changed"
		if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], columns); err != nil {
			t.Fatal(err)
		}
		if _, err := col.DeleteBatch(ids[1:]); err != nil {
			t.Fatal(err)
		}
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("seal cut not reached")
		}
		os.Exit(0)
	}
	dir, db, col := openTypedMinimaCollection(t)
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestTypedGraphPublicationInstrumentedReplay$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), childEnv+"="+dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash child: %v\n%s", err, out)
	}
	var seeded atomic.Bool
	typedGraphPublicationReplayOpenHook.Lock()
	typedGraphPublicationReplayOpenHook.fn = func(c *Collection) error {
		if !seeded.CompareAndSwap(false, true) {
			return nil
		}
		snap := c.db.AcquireSnapshot()
		if snap == nil {
			return ErrVectorIndexSnapshotMismatch
		}
		defer snap.Close()
		catalog, err := loadCollectionCatalog(snap, "minima")
		if err != nil {
			return err
		}
		if catalog == nil || catalog.typedGraphBase == nil || !collectionMetaValuesEqual(catalog.meta, catalog.typedGraphBase.meta) {
			return fmt.Errorf("replay fixture is not captured empty base")
		}
		if err := assertTypedGraphCapturedRootContents(snap, catalog); err != nil {
			return err
		}
		// Recovery is the sole owner here; do not recursively take the public
		// schema/drain admission lock. This is instrumentation, not bootstrap.
		if !c.collectionSchemaCoordinator().typedPublication.CompareAndSwap(nil, &typedGraphPublicationState{catalog: catalog, limits: typedGraphPublicationLimits{Rows: 16, Tombstones: 8, ValueSlots: 64, OwnedBytes: 4096}}) {
			return ErrVectorIndexSnapshotMismatch
		}
		return nil
	}
	typedGraphPublicationReplayOpenHook.Unlock()
	defer func() {
		typedGraphPublicationReplayOpenHook.Lock()
		typedGraphPublicationReplayOpenHook.fn = nil
		typedGraphPublicationReplayOpenHook.Unlock()
	}()
	db = openTypedMinimaDB(t, dir)
	defer db.Close()
	col, err := NewCollectionManager(db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	state := col.typedGraphPublicationSnapshot()
	if !seeded.Load() || state == nil || state.invalid || state.physicalRows != 4 || state.tombstones != 1 || len(state.rows) != 2 || state.rows[0].Values[0].Float32Vector[2] != 2 || !state.rows[1].Deleted {
		t.Fatalf("instrumented replay state=%+v", state)
	}
	doc, err := col.Get([]byte("a"))
	if err != nil || !bytes.Contains(doc, []byte("changed")) {
		t.Fatalf("replayed output=%s %v", doc, err)
	}
}

func TestTypedGraphPublicationAcceptedGapAcrossManagers(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	snap := db.AcquireSnapshot()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.initializeTypedGraphPublication(catalog, typedGraphPublicationLimits{Rows: 8, Tombstones: 8, ValueSlots: 32, OwnedBytes: 4096}); err != nil {
		t.Fatal(err)
	}
	_ = snap.Close()
	other, err := NewCollectionManager(db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	if other.collectionSchemaCoordinator() != col.collectionSchemaCoordinator() {
		t.Fatal("managers do not share frontier")
	}
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"a"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	typedGraphPublicationAfterAcceptedHook.Lock()
	typedGraphPublicationAfterAcceptedHook.fn = func(p *typedGraphPublicationCandidate) {
		if p.coord == col.collectionSchemaCoordinator() && once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}
	typedGraphPublicationAfterAcceptedHook.Unlock()
	defer func() {
		typedGraphPublicationAfterAcceptedHook.Lock()
		typedGraphPublicationAfterAcceptedHook.fn = nil
		typedGraphPublicationAfterAcceptedHook.Unlock()
	}()
	done := make(chan error, 1)
	go func() { done <- col.Flush() }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("publication hook not reached")
	}
	current := db.AcquireSnapshot()
	latest, loadErr := loadCollectionCatalog(current, "minima")
	_ = current.Close()
	if loadErr != nil {
		close(release)
		t.Fatal(loadErr)
	}
	if col.typedGraphPublicationSnapshot().matches(latest) {
		close(release)
		t.Fatal("old frontier admitted accepted-new-root view")
	}
	started, second := make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		_, _, err := other.InsertTypedBatchWithStats([][]byte{[]byte("b")}, [][]byte{[]byte(`{"id":"b"}`)}, columns)
		second <- err
	}()
	<-started
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if err := other.Flush(); err != nil {
		t.Fatal(err)
	}
	state := other.typedGraphPublicationSnapshot()
	if state.invalid || state.physicalRows != 2 || len(state.rows) != 2 {
		t.Fatalf("cross-manager frontier=%+v", state)
	}
	current = db.AcquireSnapshot()
	latest, err = loadCollectionCatalog(current, "minima")
	_ = current.Close()
	if err != nil || !state.matches(latest) {
		t.Fatalf("final frontier mismatch %v", err)
	}
}

func TestTypedGraphPublicationPendingReceipt(t *testing.T) {
	for _, cut := range []string{"none", "before_append", "after_sync"} {
		t.Run(cut, func(t *testing.T) {
			path, db, col := openTypedMinimaCollection(t)
			defer db.Close()
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatal(err)
			}
			snap := db.AcquireSnapshot()
			catalog, err := col.catalogForSnapshot(snap)
			if err != nil {
				t.Fatal(err)
			}
			if err := col.initializeTypedGraphPublication(catalog, typedGraphPublicationLimits{Rows: 1, Tombstones: 1, ValueSlots: 4, OwnedBytes: 1024}); err != nil {
				t.Fatal(err)
			}
			_ = snap.Close()
			injected := errors.New("typed reservation cut")
			var fired atomic.Bool
			restore := func() {}
			if cut != "none" {
				point := durabilitycut.BeforeDependencyAppend
				if cut == "after_sync" {
					point = durabilitycut.AfterDependencyFileSync
				}
				restore = durabilitycut.Install(func(event durabilitycut.Event) error {
					if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == point && fired.CompareAndSwap(false, true) {
						return injected
					}
					return nil
				})
			}
			columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"a"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}}}
			_, _, err = col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, columns)
			restore()
			coord := col.collectionSchemaCoordinator()
			coord.typedPublicationDebtMu.Lock()
			debt, pending := coord.typedPublicationDebt, coord.typedPublicationPending
			coord.typedPublicationDebtMu.Unlock()
			if cut == "before_append" {
				if !errors.Is(err, injected) || errors.Is(err, ErrCommitAmbiguous) || debt.rows != 0 || pending.rows != 0 {
					t.Fatalf("rejected err=%v debt=%+v pending=%+v", err, debt, pending)
				}
				return
			}
			if debt.rows != 1 || pending.rows != 1 {
				t.Fatalf("accepted pending debt=%+v pending=%+v", debt, pending)
			}
			if cut == "after_sync" {
				if !errors.Is(err, ErrCommitAmbiguous) || !col.typedGraphPublicationSnapshot().invalid {
					t.Fatalf("ambiguous err=%v state=%+v", err, col.typedGraphPublicationSnapshot())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			frames := len(collectionCommandWALFrames(t, path))
			_, _, err = col.InsertTypedBatchWithStats([][]byte{[]byte("b")}, [][]byte{[]byte(`{"id":"b"}`)}, columns)
			if !errors.Is(err, errTypedGraphOverlayFoldNeeded) || len(collectionCommandWALFrames(t, path)) != frames {
				t.Fatalf("pending bound err=%v", err)
			}
			if err := col.Flush(); err != nil {
				t.Fatal(err)
			}
			coord.typedPublicationDebtMu.Lock()
			debt, pending = coord.typedPublicationDebt, coord.typedPublicationPending
			coord.typedPublicationDebtMu.Unlock()
			if debt.rows != 1 || pending.rows != 0 || col.typedGraphPublicationSnapshot().physicalRows != 1 {
				t.Fatalf("transfer debt=%+v pending=%+v", debt, pending)
			}
		})
	}
}

func TestTypedGraphPublicationAdmissionBounds(t *testing.T) {
	for _, bound := range []string{"rows", "values", "payload"} {
		t.Run(bound, func(t *testing.T) {
			path, db, col := openTypedMinimaCollection(t)
			defer db.Close()
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatal(err)
			}
			snap := db.AcquireSnapshot()
			catalog, err := col.catalogForSnapshot(snap)
			if err != nil {
				t.Fatal(err)
			}
			limits := typedGraphPublicationLimits{Rows: 8, Tombstones: 8, ValueSlots: 32, OwnedBytes: 1024}
			switch bound {
			case "rows":
				limits.Rows = 1
			case "values":
				limits.ValueSlots = 1
			case "payload":
				limits.OwnedBytes = 1
			}
			if err := col.initializeTypedGraphPublication(catalog, limits); err != nil {
				t.Fatal(err)
			}
			_ = snap.Close()
			before := col.typedGraphPublicationSnapshot()
			frames := len(collectionCommandWALFrames(t, path))
			columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}, {1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"a", "b"}}, {Name: "user", Strings: []string{"u", "u"}}, {Name: "path", Strings: []string{"p", "p"}}}
			_, _, err = col.InsertTypedBatchWithStats([][]byte{[]byte("a"), []byte("b")}, [][]byte{[]byte(`{"id":"a"}`), []byte(`{"id":"b"}`)}, columns)
			if !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
				t.Fatalf("admission err=%v", err)
			}
			if col.typedGraphPublicationSnapshot() != before || len(collectionCommandWALFrames(t, path)) != frames {
				t.Fatal("rejected admission changed debt or WAL")
			}
		})
	}
}

func TestTypedGraphPublicationSourceCoordinates(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	snap := db.AcquireSnapshot()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.initializeTypedGraphPublication(catalog, typedGraphPublicationLimits{Rows: 16, Tombstones: 8, ValueSlots: 64, OwnedBytes: 4096}); err != nil {
		t.Fatal(err)
	}
	_ = snap.Close()
	a, b := []byte("a"), []byte("b")
	docA := []byte(`{"id":"a","embedding":[1,0,0,0,0,0,0,0],"content":"alpha","meta":{"user_id":"u","fpath":"p"}}`)
	docB := []byte(`{"id":"b","embedding":[0,1,0,0,0,0,0,0],"content":"beta","meta":{"user_id":"u","fpath":"p"}}`)
	if _, err := col.replaceSourceDocumentsWithCommandWALIntent(nil, [][]byte{a, b}, [][]byte{docA, docB}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := col.replaceSourceDocumentsWithCommandWALIntent([][]byte{a, b}, [][]byte{a}, [][]byte{docA}, nil, nil); err != nil {
		t.Fatal(err)
	}
	state := col.typedGraphPublicationSnapshot()
	if state.physicalRows != 5 || state.tombstones != 2 || len(state.rows) != 2 || state.rows[0].Deleted || !state.rows[1].Deleted {
		t.Fatalf("source frontier=%+v", state)
	}
	if state.rows[0].PartID != columnPhysicalRowAssetPartID+(1<<32) || state.rows[0].RowIndex != 0 || state.rows[1].PartID != columnPhysicalRowAssetPartID || state.rows[1].RowIndex != 1 {
		t.Fatalf("source coordinates=%+v", state.rows)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	refs, err := view.LookupDocumentRowRefsByID([][]byte{a, b}, DocumentFetchOptions{})
	_ = view.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(refs.Results) != 2 || !refs.Results[0].Found || refs.Results[1].Found {
		t.Fatalf("source locators=%+v", refs)
	}
	ref := refs.Results[0].RowRef
	row := state.rows[0]
	if ref.Generation != row.Generation || ref.PartID != row.PartID || ref.RowIndex != row.RowIndex || ref.AppliedCommandLSN != row.AppliedCommandLSN {
		t.Fatalf("locator=%+v derived=%+v", ref, row)
	}
	if _, err := col.replaceSourceDocumentsWithCommandWALIntent([][]byte{a}, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	state = col.typedGraphPublicationSnapshot()
	if state.physicalRows != 6 || state.tombstones != 3 || !state.rows[0].Deleted {
		t.Fatalf("source delete=%+v", state)
	}
}

func TestTypedGraphPublicationAsyncFailureRetainsDebt(t *testing.T) {
	meta := typedMinimaCollectionMeta()
	meta.Options.DisableBufferedIndexedAsyncFlush = false
	meta.Options.BufferedIndexedWriteMaxDocuments = 1
	path, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	snap := db.AcquireSnapshot()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.initializeTypedGraphPublication(catalog, typedGraphPublicationLimits{Rows: 1, Tombstones: 1, ValueSlots: 4, OwnedBytes: 4096}); err != nil {
		t.Fatal(err)
	}
	_ = snap.Close()
	injected := errors.New("async asset prepare failed")
	entered := make(chan struct{}, 1)
	restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(ColumnPublishPreparedAssets) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		return injected
	})
	defer restore()
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"a"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}}}
	_, _, writeErr := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, columns)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("async preparation not reached")
	}
	flushErr := col.Flush()
	if writeErr != nil && !errors.Is(writeErr, injected) {
		t.Fatal(writeErr)
	}
	if !errors.Is(flushErr, injected) {
		t.Fatalf("flush failure=%v", flushErr)
	}
	coord := col.collectionSchemaCoordinator()
	coord.typedPublicationDebtMu.Lock()
	debt, pending := coord.typedPublicationDebt, coord.typedPublicationPending
	coord.typedPublicationDebtMu.Unlock()
	if debt.rows != 1 || pending.rows != 1 {
		t.Fatalf("failed accepted debt=%+v pending=%+v", debt, pending)
	}
	frames := len(collectionCommandWALFrames(t, path))
	_, _, err = col.InsertTypedBatchWithStats([][]byte{[]byte("b")}, [][]byte{[]byte(`{"id":"b"}`)}, columns)
	if err == nil || len(collectionCommandWALFrames(t, path)) != frames {
		t.Fatalf("accepted debt bypassed after async failure: %v", err)
	}
}

func TestTypedGraphPublicationDirectGroup(t *testing.T) {
	meta := typedMinimaCollectionMeta()
	// Without text indexes the indexed insert buffer is ineligible: this
	// exercises the public insert's ordered-root group publication sibling.
	meta.TextIndexes = nil
	dir, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	snap := db.AcquireSnapshot()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.initializeTypedGraphPublication(catalog, typedGraphPublicationLimits{Rows: 1, Tombstones: 1, ValueSlots: 4, OwnedBytes: 4096}); err != nil {
		t.Fatal(err)
	}
	_ = snap.Close()
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"a"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}}}
	// The projection owns these headers and vectors; asset publication only
	// reads them. Derived state may share them without cloning each row.
	projection, err := newTrustedTypedProjection(catalog.meta, [][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, columns)
	if err != nil {
		t.Fatal(err)
	}
	values := projection.typedRows["a"]
	input := columnWritePublishInput{meta: catalog.meta, catalog: catalog, operation: ColumnPublishOperationInsert, rows: 1, documents: []columnWriteDocument{{ID: []byte("a")}}, declaredRowsReady: true, declaredRows: []columnDeclaredRow{{ID: []byte("a"), Values: values}}}
	candidate, err := col.prepareTypedGraphPublication(input)
	if err != nil {
		t.Fatal(err)
	}
	if &candidate.next.rows[0].Values[0] != &values[0] {
		t.Fatal("owning value headers were copied")
	}
	candidate.rejectBeforeAppend()
	borrowed := []byte("borrowed")
	values[1].StringBytes = borrowed
	candidate, err = col.prepareTypedGraphPublication(input)
	if err != nil {
		t.Fatal(err)
	}
	borrowed[0] = 'X'
	if candidate.next.rows[0].Values[1].String != "borrowed" || candidate.next.rows[0].Values[1].StringBytes != nil || values[1].StringBytes == nil {
		t.Fatal("borrowed normalization changed input or retained borrowed bytes")
	}
	candidate.rejectBeforeAppend()
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	if state := col.typedGraphPublicationSnapshot(); state.physicalRows != 1 || len(state.rows) != 1 {
		t.Fatalf("direct state before Flush: %+v", state)
	}
	frames := len(collectionCommandWALFrames(t, dir))
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("b")}, [][]byte{[]byte(`{"id":"b"}`)}, columns); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("second insert: %v", err)
	}
	if got := len(collectionCommandWALFrames(t, dir)); got != frames {
		t.Fatalf("rejected direct write appended WAL: %d -> %d", frames, got)
	}
}

func TestTypedGraphPublicationAcceptedMutation(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	snap := db.AcquireSnapshot()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.initializeTypedGraphPublication(catalog, typedGraphPublicationLimits{Rows: 16, Tombstones: 8, ValueSlots: 64, OwnedBytes: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	_ = snap.Close()
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"accepted"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("new")}, [][]byte{[]byte(`{"id":"new"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	state := col.typedGraphPublicationSnapshot()
	if len(state.rows) != 1 || state.physicalRows != 1 {
		t.Fatalf("accepted typed mutation missing from derived frontier: rows=%d physical=%d", len(state.rows), state.physicalRows)
	}
	if err := col.initializeTypedGraphPublication(catalog, state.limits); err == nil || col.typedGraphPublicationSnapshot() != state {
		t.Fatal("stale initialization replaced installed frontier")
	}
	result, err := col.ReplaceTypedBatch([][]byte{[]byte("new")}, [][]byte{[]byte(`{"id":"new"}`)}, columns)
	if err != nil || len(result) != 1 || result[0].Modified {
		t.Fatalf("identical replacement: %v %v", result, err)
	}
	if col.typedGraphPublicationSnapshot().physicalRows != 1 {
		t.Fatal("no-op added physical debt")
	}
	columns[0].Float32Vectors[0] = []float32{0, 2, 0, 0, 0, 0, 0, 0}
	result, err = col.ReplaceTypedBatch([][]byte{[]byte("new")}, [][]byte{[]byte(`{"id":"new"}`)}, columns)
	if err != nil || !result[0].Modified {
		t.Fatalf("replacement: %v %v", result, err)
	}
	state = col.typedGraphPublicationSnapshot()
	if state.physicalRows != 2 || state.rows[0].Values[0].Float32Vector[1] != 2 || state.invNorms[0] != .5 {
		t.Fatalf("replacement frontier: %+v", state)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("new")}); err != nil || deleted != 1 {
		t.Fatalf("delete: %d %v", deleted, err)
	}
	state = col.typedGraphPublicationSnapshot()
	if state.physicalRows != 3 || state.tombstones != 1 || !state.rows[0].Deleted {
		t.Fatalf("delete frontier: %+v", state)
	}
}
