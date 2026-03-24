package treedbtrace

import (
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func resetTracePhaseBusForTest() {
	phaseBus.init()
	phaseBus.set(tracePhaseDefault)
	phaseBus.mu.Lock()
	phaseBus.dbs = make(map[*treedb.DB]int)
	phaseBus.registerBeforeApply = nil
	phaseBus.mu.Unlock()
}

func TestTracePhaseBridge_PropagatesToWrappedTreeDB(t *testing.T) {
	resetTracePhaseBusForTest()
	defer resetTracePhaseBusForTest()

	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	wrapped := Wrap(db)
	defer wrapped.Close()

	if got := db.MaintenancePhase(); got != treedb.MaintenancePhaseSteady {
		t.Fatalf("initial maintenance phase=%v want steady", got)
	}

	SetTracePhase("restore")
	if got := CurrentTracePhase(); got != "restore" {
		t.Fatalf("trace phase=%q want restore", got)
	}
	if got := db.MaintenancePhase(); got != treedb.MaintenancePhaseRestore {
		t.Fatalf("maintenance phase=%v want restore", got)
	}

	SetTracePhase("catchup")
	if got := db.MaintenancePhase(); got != treedb.MaintenancePhaseCatchUp {
		t.Fatalf("maintenance phase=%v want catchup", got)
	}

	SetTracePhase("steady")
	if got := db.MaintenancePhase(); got != treedb.MaintenancePhaseSteady {
		t.Fatalf("maintenance phase=%v want steady", got)
	}
}

func TestTracePhaseBridge_AppliesCurrentPhaseOnWrap(t *testing.T) {
	resetTracePhaseBusForTest()
	defer resetTracePhaseBusForTest()

	SetTracePhase("restore")

	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	wrapped := Wrap(db)
	defer wrapped.Close()

	if got := db.MaintenancePhase(); got != treedb.MaintenancePhaseRestore {
		t.Fatalf("maintenance phase=%v want restore after wrap", got)
	}
}

func TestTracePhaseBridge_AppliesLatestPhaseDuringWrapRace(t *testing.T) {
	resetTracePhaseBusForTest()
	defer resetTracePhaseBusForTest()

	phaseBus.mu.Lock()
	phaseBus.registerBeforeApply = func() {
		SetTracePhase("restore")
	}
	phaseBus.mu.Unlock()

	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	wrapped := Wrap(db)
	defer wrapped.Close()

	if got := CurrentTracePhase(); got != "restore" {
		t.Fatalf("trace phase=%q want restore", got)
	}
	if got := db.MaintenancePhase(); got != treedb.MaintenancePhaseRestore {
		t.Fatalf("maintenance phase=%v want restore after raced wrap", got)
	}
}

func TestTracePhaseBridge_UnregistersOnClose(t *testing.T) {
	resetTracePhaseBusForTest()
	defer resetTracePhaseBusForTest()

	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	wrapped := Wrap(db)

	phaseBus.mu.Lock()
	if got := phaseBus.dbs[db]; got != 1 {
		phaseBus.mu.Unlock()
		t.Fatalf("registration count=%d want 1", got)
	}
	phaseBus.mu.Unlock()

	if err := wrapped.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	phaseBus.mu.Lock()
	defer phaseBus.mu.Unlock()
	if _, ok := phaseBus.dbs[db]; ok {
		t.Fatalf("registration still present after close; expected unregistered")
	}
}
