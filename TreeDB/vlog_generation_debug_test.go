package treedb

import (
	"reflect"
	"testing"
	"time"
)

func TestDebugValueLogGenerationStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	observedAt := time.Now().Add(-2 * time.Second).UnixNano()
	ledger := []ValueLogRewritePlanSegment{
		{FileID: 7, BytesTotal: 100, BytesLive: 40, BytesStale: 60, StaleRatio: 0.6},
		{FileID: 9, BytesTotal: 200, BytesLive: 20, BytesStale: 180, StaleRatio: 0.9},
	}
	if err := db.DebugSetValueLogGenerationRewriteLedger(ledger, true, observedAt); err != nil {
		t.Fatalf("DebugSetValueLogGenerationRewriteLedger: %v", err)
	}

	state, err := db.DebugValueLogGenerationState()
	if err != nil {
		t.Fatalf("DebugValueLogGenerationState: %v", err)
	}
	if !reflect.DeepEqual(state.RewriteSourceFileIDs, []uint32{7, 9}) {
		t.Fatalf("queue=%v want [7 9]", state.RewriteSourceFileIDs)
	}
	if !reflect.DeepEqual(state.RewriteDebtLedger, ledger) {
		t.Fatalf("ledger=%v want %v", state.RewriteDebtLedger, ledger)
	}
	if !state.RewriteStagePending {
		t.Fatalf("stage pending=false want true")
	}
	if state.RewriteStageObservedAtNS != observedAt {
		t.Fatalf("stage observed=%d want %d", state.RewriteStageObservedAtNS, observedAt)
	}

	if err := db.DebugSetValueLogGenerationRewriteQueue([]uint32{11, 13}); err != nil {
		t.Fatalf("DebugSetValueLogGenerationRewriteQueue: %v", err)
	}
	state, err = db.DebugValueLogGenerationState()
	if err != nil {
		t.Fatalf("DebugValueLogGenerationState after queue set: %v", err)
	}
	if !reflect.DeepEqual(state.RewriteSourceFileIDs, []uint32{11, 13}) {
		t.Fatalf("queue=%v want [11 13]", state.RewriteSourceFileIDs)
	}
	if len(state.RewriteDebtLedger) != 0 {
		t.Fatalf("ledger len=%d want 0", len(state.RewriteDebtLedger))
	}
	if state.RewriteStagePending {
		t.Fatalf("stage pending=true want false")
	}
	if state.RewriteStageObservedAtNS != 0 {
		t.Fatalf("stage observed=%d want 0", state.RewriteStageObservedAtNS)
	}
}
