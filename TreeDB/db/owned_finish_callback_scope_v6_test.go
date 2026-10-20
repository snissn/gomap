package db

import (
	"errors"
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
	if e = r.EnterOwnedFinishCallbackV6(); e != nil {
		t.Fatal(e)
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
	if db.ownedFinishGuardV6.caller.Load() != 0 {
		t.Fatal("callback marker retained")
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
}

func TestOwnedFinishConcurrentCleanupAllowsActualDATAPublicationV6(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	entered, release, done := make(chan error, 1), make(chan struct{}), make(chan struct{})
	go func() {
		err := db.ownedFinishGuardV6.EnterCleanup()
		entered <- err
		if err == nil {
			<-release
			db.ownedFinishGuardV6.LeaveCleanup()
		}
		close(done)
	}()
	if err := <-entered; err != nil {
		t.Fatal(err)
	}
	defer func() { close(release); <-done }()
	before := db.State().CommitSeq
	if err := db.SetSync([]byte("real-data"), []byte("while-cleanup-held")); err != nil {
		t.Fatal(err)
	}
	if db.State().CommitSeq <= before {
		t.Fatal("actual publication did not advance")
	}
	if db.ownedFinishGuardV6.cleanup.Load() == 0 || db.ownedFinishGuardV6.caller.Load() != 0 {
		t.Fatal("independent callback lane marker lost")
	}
}
