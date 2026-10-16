package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"testing"
)

// Exercises the real DB guard without claiming connected native publication.
func TestOwnedFinishFirstCallbackCloseRefusesBeforeHooksV6(t *testing.T) {
	db, e := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	r := db.rootPublication
	if r == nil {
		t.Fatal("missing actual publisher")
	}
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	entered, e := r.EnterOwnedFinishCallbackV6(w)
	if e != nil || !entered {
		t.Fatalf("scope %v %v", entered, e)
	}
	// This is FIRST Close: no terminal owner or user-hook drain has started.
	if e = db.Close(); !errors.Is(e, errPublicationCallbackCloseV6) {
		t.Fatalf("callback close %v", e)
	}
	if db.closeHooksClosed || db.closing.Load() {
		t.Fatal("refusal began teardown or hook drain")
	}
	r.LeaveOwnedFinishCallbackV6()
	if e = db.SetSync([]byte("live"), []byte("value")); e != nil {
		t.Fatal(e)
	}
	if db.ownedFinishCallerV6 != 0 {
		t.Fatal("callback marker retained")
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
}
