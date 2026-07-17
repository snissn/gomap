package db

import (
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestCommitAtStateRejectsAdvancedBasis(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = database.Close() }()

	basis, ok := database.StateToken()
	if !ok {
		t.Fatal("initial state token unavailable")
	}
	if err := database.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("advance visible root: %v", err)
	}

	err = database.CommitAtState(basis.RootPageID, basis)
	if !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("stale CommitAtState error=%v want %v", err, ErrConcurrentModification)
	}
	value, err := database.Get([]byte("key"))
	if err != nil || string(value) != "value" {
		t.Fatalf("value after stale rejection=%q err=%v want value", string(value), err)
	}

	current, ok := database.StateToken()
	if !ok {
		t.Fatal("current state token unavailable")
	}
	if err := database.CommitAtState(current.RootPageID, current); err != nil {
		t.Fatalf("CommitAtState at current basis: %v", err)
	}
}

func TestCommitAtStateMapsPublicationRaceToConcurrentModification(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = database.Close() }()

	basis, ok := database.StateToken()
	if !ok {
		t.Fatal("initial state token unavailable")
	}

	predecessorReleased := make(chan struct{})
	releasePredecessor := make(chan struct{})
	database.testAfterFinalizeRootSerializationReleaseHook = func() {
		close(predecessorReleased)
		<-releasePredecessor
	}
	defer func() { database.testAfterFinalizeRootSerializationReleaseHook = nil }()

	system := mustFrozenSystemMemtable(t, "sys/manual-root-race", "preserved")
	predecessorErr := make(chan error, 1)
	go func() {
		_, _, publishErr := database.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
			return system.NewIterator(nil, nil), nil
		})
		predecessorErr <- publishErr
	}()
	select {
	case <-predecessorReleased:
	case <-time.After(5 * time.Second):
		t.Fatal("predecessor did not release root serialization")
	}
	database.testAfterFinalizeRootSerializationReleaseHook = nil

	manualReady := make(chan struct{})
	database.testBeforeFinalizeCommitHook = func() { close(manualReady) }
	defer func() { database.testBeforeFinalizeCommitHook = nil }()

	manualErr := make(chan error, 1)
	go func() {
		manualErr <- database.CommitAtState(basis.RootPageID, basis)
	}()
	select {
	case <-manualReady:
	case <-time.After(5 * time.Second):
		t.Fatal("CommitAtState did not reach finalization")
	}
	database.testBeforeFinalizeCommitHook = nil

	close(releasePredecessor)
	select {
	case err = <-predecessorErr:
	case <-time.After(5 * time.Second):
		t.Fatal("predecessor did not finish")
	}
	if err != nil {
		t.Fatalf("predecessor publication: %v", err)
	}
	select {
	case err = <-manualErr:
	case <-time.After(5 * time.Second):
		t.Fatal("CommitAtState did not return after racing publication")
	}
	if !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("CommitAtState race error=%v want %v", err, ErrConcurrentModification)
	}
	if errors.Is(err, errDurableRootCandidateStale) {
		t.Fatalf("CommitAtState exposed internal stale-candidate error: %v", err)
	}

	snap := database.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot after concurrent-modification rejection is nil")
	}
	defer func() { _ = snap.Close() }()
	state, ok := snap.StateToken()
	if !ok {
		t.Fatal("snapshot state unavailable")
	}
	entry, err := snap.GetEntryAtRoot(state.SystemRootPageID, []byte("sys/manual-root-race"))
	if err != nil || string(entry.Value) != "preserved" {
		t.Fatalf("racing publication value=%q err=%v want preserved", string(entry.Value), err)
	}
}

func TestForceCommitRebasesAfterPublicationRace(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = database.Close() }()

	basis, ok := database.StateToken()
	if !ok {
		t.Fatal("initial state token unavailable")
	}

	predecessorReleased := make(chan struct{})
	releasePredecessor := make(chan struct{})
	database.testAfterFinalizeRootSerializationReleaseHook = func() {
		close(predecessorReleased)
		<-releasePredecessor
	}
	defer func() { database.testAfterFinalizeRootSerializationReleaseHook = nil }()

	system := mustFrozenSystemMemtable(t, "sys/force-root-race", "preserved")
	predecessorErr := make(chan error, 1)
	go func() {
		_, _, publishErr := database.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
			return system.NewIterator(nil, nil), nil
		})
		predecessorErr <- publishErr
	}()
	select {
	case <-predecessorReleased:
	case <-time.After(5 * time.Second):
		t.Fatal("predecessor did not release root serialization")
	}
	database.testAfterFinalizeRootSerializationReleaseHook = nil

	manualReady := make(chan struct{})
	database.testBeforeFinalizeCommitHook = func() { close(manualReady) }
	defer func() { database.testBeforeFinalizeCommitHook = nil }()

	manualErr := make(chan error, 1)
	go func() {
		manualErr <- database.ForceCommit(basis.RootPageID)
	}()
	select {
	case <-manualReady:
	case <-time.After(5 * time.Second):
		t.Fatal("ForceCommit did not reach finalization")
	}
	database.testBeforeFinalizeCommitHook = nil

	close(releasePredecessor)
	select {
	case err = <-predecessorErr:
	case <-time.After(5 * time.Second):
		t.Fatal("predecessor did not finish")
	}
	if err != nil {
		t.Fatalf("predecessor publication: %v", err)
	}
	select {
	case err = <-manualErr:
	case <-time.After(5 * time.Second):
		t.Fatal("ForceCommit did not return after racing publication")
	}
	if err != nil {
		t.Fatalf("ForceCommit after racing publication: %v", err)
	}

	snap := database.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot after force publication is nil")
	}
	defer func() { _ = snap.Close() }()
	state, ok := snap.StateToken()
	if !ok {
		t.Fatal("snapshot state unavailable")
	}
	if state.RootPageID != basis.RootPageID {
		t.Fatalf("forced user root=%d want %d", state.RootPageID, basis.RootPageID)
	}
	entry, err := snap.GetEntryAtRoot(state.SystemRootPageID, []byte("sys/force-root-race"))
	if err != nil || string(entry.Value) != "preserved" {
		t.Fatalf("racing publication value=%q err=%v want preserved", string(entry.Value), err)
	}
}
