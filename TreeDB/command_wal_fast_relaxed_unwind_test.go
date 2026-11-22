package treedb

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestPublicCOWFastRelaxedCutSwapUnwindReturnsOperationGate(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := indexedSliceOptions(ProfileCommandWALRelaxed, t.TempDir())
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closeStarted := false
	defer func() {
		if !closeStarted {
			_ = database.Close()
		}
	}()
	if database.ResolvedProfile() != ProfileCommandWALRelaxed || !database.commandWALCached || !database.cached.COWMode() {
		t.Fatal("fixture requires the public relaxed COW command WAL route")
	}

	anchor, stable := []byte("durable-anchor"), []byte("stable")
	if err := database.SetSync(anchor, stable); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	next := database.backend.CommandWALNextLSN()
	fast := database.NewBatch().(*commandWALPublicBatch)
	id := []byte("relaxed-unacknowledged")
	if err := fast.Set(id, []byte("assigned")); err != nil {
		_ = fast.Close()
		t.Fatal(err)
	}

	type cutObservation struct {
		ticket, assignedLSN uint64
		awaiting, installed bool
	}
	var cut cutObservation
	fault := errors.New("after the genuine fast relaxed command was assigned and before its COW cut swaps")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWCutSwap && e.Resource == durabilitycut.ResourceAuxiliary {
			registration := fast.cowState.registration
			receipt := fast.inner.(*caching.Batch).COWReceipt()
			cut = cutObservation{registration.ticket, receipt.AssignedLSN, registration.awaiting, receipt.LogicalInstalled}
			panic(fault)
		}
		return nil
	})
	result := make(chan any, 1)
	go func() {
		defer func() { result <- recover() }()
		_ = fast.Write()
	}()
	var unwind any
	select {
	case unwind = <-result:
		restore()
	case <-time.After(5 * time.Second):
		closeStarted = true // bounded failure, never join an unexpectedly stuck owner
		t.Fatal("the public relaxed write did not unwind")
	}
	if err := fast.Close(); err != nil {
		t.Error(err)
	}
	if unwind != fault || cut.ticket != publicCommandWALFastRelaxedAppendedTicket ||
		cut.assignedLSN != next || cut.awaiting || cut.installed || database.backend.CommandWALNextLSN() != next+1 {
		t.Errorf("unwind=%v cut=%+v next=%d→%d", unwind, cut, next, database.backend.CommandWALNextLSN())
	}
	t.Logf("genuine public fast relaxed register: ticket=%d assigned=%d awaiting=%t installed=%t",
		cut.ticket, cut.assignedLSN, cut.awaiting, cut.installed)
	if err := database.backend.CheckCommandWALPublishReady(); !errors.Is(err, ErrRecoveryRequired) {
		t.Errorf("assigned unwind lost recovery requirement: %v", err)
	}
	if database.commandWALPublicOperationGate.TryLock() {
		database.commandWALPublicOperationGate.Unlock()
	} else {
		t.Error("fast relaxed assigned unwind retained the public operation gate")
	}

	closed := make(chan error, 1)
	closeStarted = true
	go func() { closed <- database.Close() }()
	select {
	case err := <-closed:
		t.Logf("checked original Close completed with %v", err)
		if !errors.Is(err, ErrRecoveryRequired) {
			t.Errorf("original Close lost recovery requirement: %v", err)
		}
	case <-time.After(3 * time.Second):
		buf := make([]byte, 256<<10)
		n := runtime.Stack(buf, true)
		t.Fatalf("checked original Close blocked after the assigned unwind:\n%s", buf[:n])
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	requireRawKVValue(t, reopened, anchor, stable)
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
