package db

import (
	"context"
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/lifecycle"
)

func TestMinPinnedReaderCommitSeqKeepsRetiredReaderAndPageRetention(t *testing.T) {
	oldIndex := &indexGen{registry: lifecycle.NewReaderRegistry()}
	oldIndex.id = 1
	oldIndex.refs.Store(1)
	newIndex := &indexGen{registry: lifecycle.NewReaderRegistry()}
	newIndex.id = 2
	newIndex.refs.Store(1)
	database := &DB{snapPool: NewSnapshotPool()}
	database.idx.Store(oldIndex)
	database.trackIndex(oldIndex)
	database.trackIndex(newIndex)
	state := &DBState{CommitSeq: 10, RootPageID: 11}
	database.state.Store(state)
	database.publishSnapshotView(oldIndex, state, nil, nil)
	page := oldIndex.registry.RegisterPageRetention(5)
	defer oldIndex.registry.Unregister(page)
	snapshot := database.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() {
		if snapshot != nil {
			if err := snapshot.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	state = &DBState{CommitSeq: 20, RootPageID: 22}
	database.idx.Store(newIndex)
	database.state.Store(state)
	database.publishSnapshotView(newIndex, state, nil, nil)
	if got := database.MinPinnedReaderCommitSeq(); got != 10 {
		t.Fatalf("retired snapshot reader minimum=%d want 10", got)
	}
	if got := database.MinPinnedSnapshotCommitSeq(); got != 5 {
		t.Fatalf("reuse minimum ignored directory page retention: %d", got)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot = nil
	if got := database.MinPinnedReaderCommitSeq(); got != math.MaxUint64 {
		t.Fatalf("page-only pin still claims an asset reader: %d", got)
	}
	if got := database.MinPinnedSnapshotCommitSeq(); got != 5 {
		t.Fatalf("reader close lost page reuse protection: %d", got)
	}
}

func TestMinPinnedReaderCommitSeqProtectsInFlightAndCompletedAcquire(t *testing.T) {
	index := &indexGen{registry: lifecycle.NewReaderRegistry()}
	index.id = 1
	index.refs.Store(1)
	database := &DB{snapPool: NewSnapshotPool()}
	database.idx.Store(index)
	database.trackIndex(index)
	state := &DBState{CommitSeq: 313, RootPageID: 11}
	database.state.Store(state)
	database.publishSnapshotView(index, state, nil, nil)
	page := index.registry.RegisterPageRetention(1)
	defer index.registry.Unregister(page)
	database.snapshotAcquireRO[0].Store(1)
	if got := database.MinPinnedReaderCommitSeq(); got != 0 {
		t.Fatalf("in-flight reader minimum=%d want conservative 0", got)
	}
	database.snapshotAcquireRO[0].Store(0)

	var snapshot *Snapshot
	hookCalls := 0
	minPinnedSnapshotCommitSeqAfterScanForTestingMu.Lock()
	previous := minPinnedSnapshotCommitSeqAfterScanForTesting
	minPinnedSnapshotCommitSeqAfterScanForTesting = func() {
		hookCalls++
		if hookCalls == 1 {
			snapshot = database.AcquireSnapshot()
			if snapshot == nil {
				t.Fatal("AcquireSnapshot during scan returned nil")
			}
		}
	}
	minPinnedSnapshotCommitSeqAfterScanForTestingMu.Unlock()
	defer func() {
		minPinnedSnapshotCommitSeqAfterScanForTestingMu.Lock()
		minPinnedSnapshotCommitSeqAfterScanForTesting = previous
		minPinnedSnapshotCommitSeqAfterScanForTestingMu.Unlock()
		if snapshot != nil {
			if err := snapshot.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	if got := database.MinPinnedReaderCommitSeq(); got != state.CommitSeq {
		t.Fatalf("completed-acquire reader minimum=%d want %d", got, state.CommitSeq)
	}
	if hookCalls < 2 {
		t.Fatalf("reader minimum did not rescan changed acquisition epoch: %d", hookCalls)
	}
}

func TestMinPinnedReaderCommitSeqIncludesCapturedRecoverableRootSet(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, key := range []string{"one", "two"} {
		if err := database.SetSync([]byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	set, err := database.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if set != nil {
			if err := set.ReleaseWithError(); err != nil {
				t.Error(err)
			}
		}
	}()
	want := database.MinPinnedReaderCommitSeq()
	if want == math.MaxUint64 || set.oldestRegistryID == 0 {
		t.Fatal("captured recoverable roots have no reader-capable pin")
	}
	page := set.idx.registry.RegisterPageRetention(1)
	defer set.idx.registry.Unregister(page)
	// A stale set still owns its captured roots and backing until Release, even
	// though revalidation may refuse new traversals after the live slots advance.
	for _, key := range []string{"three", "four"} {
		if err := database.SetSync([]byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if got := database.MinPinnedReaderCommitSeq(); got != want {
		t.Fatalf("captured root reader minimum after slot advance=%d want %d", got, want)
	}
	if err := set.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	set = nil
	if got := database.MinPinnedReaderCommitSeq(); got != math.MaxUint64 {
		t.Fatalf("released root set still holds reader minimum: %d", got)
	}
	if got := database.MinPinnedSnapshotCommitSeq(); got != 1 {
		t.Fatalf("root set release lost remaining page retention: %d", got)
	}
}
