package db

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// These backend tests bind scalar coverage as existing command_wal_publish_test
// does. The public selection fixture separately proves actual grouped WAL ACK,
// C1 publication, PRIMARY checkpoint, Close and reopen through ordinary options.
func primaryFallbackDatabaseV6(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	database, err := Open(Options{Dir: dir, CommandWAL: true, IndexPrimaryDirectory: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	for n := uint64(1); n <= 2; n++ {
		state := database.State()
		if err := database.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, n, []CommandWALLSNRange{{First: n, Last: n}}, true); err != nil {
			t.Fatal(err)
		}
		if err := database.rootPublication.coordinator.WaitThrough(context.Background(), database.State().CommitSeq); err != nil {
			t.Fatal(err)
		}
	}
	return database
}

func TestRefreshCommandWALPrimaryCheckpointFallbackV6(t *testing.T) {
	database := primaryFallbackDatabaseV6(t)
	before := database.State()
	nextLSN := database.CommandWALNextLSN()
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	if database.State().AppliedCommandLSN != before.AppliedCommandLSN || database.CommandWALNextLSN() != nextLSN {
		t.Fatal("fallback refresh changed command coverage or appended a frame")
	}
	database.durablePublishMu.Lock()
	defer database.durablePublishMu.Unlock()
	slot := database.durableRoot.slot
	selected, fallback := database.durableRoot.slotRecord[slot], database.durableRoot.slotRecord[slot^1]
	if selected.UserRootPageID == fallback.UserRootPageID {
		t.Fatal("V6 physical slot addresses unexpectedly equal")
	}
	if !database.commandWALCheckpointRootsEqualLocked(selected, fallback) || database.commandWALCheckpointFallbackNeedsRefreshLocked(before.AppliedCommandLSN, selected, fallback) {
		t.Fatal("same complete logical directories did not converge")
	}
	if selected.AppliedCommandLSN != before.AppliedCommandLSN || fallback.AppliedCommandLSN != before.AppliedCommandLSN {
		t.Fatal("scalar coverage not refreshed")
	}
}

func TestCommandWALPrimaryCheckpointFallbackV6RefusesIncompleteOperands(t *testing.T) {
	database := primaryFallbackDatabaseV6(t)
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	database.durablePublishMu.Lock()
	defer database.durablePublishMu.Unlock()
	p := database.durableRoot.primary
	slot := database.durableRoot.slot
	selected, fallback := database.durableRoot.slotRecord[slot], database.durableRoot.slotRecord[slot^1]
	if !database.commandWALCheckpointRootsEqualLocked(selected, fallback) {
		t.Fatal("fixture lacks identical logical directory")
	}
	t.Run("changed-system", func(t *testing.T) {
		changed := fallback
		changed.SystemRootPageID++
		if database.commandWALCheckpointRootsEqualLocked(selected, changed) {
			t.Fatal("changed system root falsely converged")
		}
	})
	t.Run("changed-slot-record", func(t *testing.T) {
		changed := fallback
		changed.MaxEntryRevision++
		if database.commandWALCheckpointRootsEqualLocked(selected, changed) {
			t.Fatal("mismatched actual slot record falsely converged")
		}
	})
	t.Run("wrong-coverage", func(t *testing.T) {
		changed := fallback
		changed.AppliedCommandLSN--
		if !database.commandWALCheckpointFallbackNeedsRefreshLocked(selected.AppliedCommandLSN, selected, changed) {
			t.Fatal("different coverage suppressed refresh")
		}
	})
	t.Run("missing-view", func(t *testing.T) {
		saved := p.capsuleViews[slot^1]
		p.capsuleViews[slot^1] = rootpublication.PrimaryCapsuleViewV6{}
		defer func() { p.capsuleViews[slot^1] = saved }()
		if database.commandWALCheckpointRootsEqualLocked(selected, fallback) {
			t.Fatal("missing complete view falsely converged")
		}
	})
	t.Run("missing-image", func(t *testing.T) {
		saved := p.capsuleImages[slot^1]
		p.capsuleImages[slot^1] = nil
		defer func() { p.capsuleImages[slot^1] = saved }()
		if database.commandWALCheckpointRootsEqualLocked(selected, fallback) {
			t.Fatal("missing retained image falsely converged")
		}
	})
	t.Run("corrupt-directory", func(t *testing.T) {
		savedView, savedImage := p.capsuleViews[slot^1], p.capsuleImages[slot^1]
		copy := savedView.CopyOwned()
		copy.Directory()[page.PageHeaderSize+1] ^= 1
		p.capsuleViews[slot^1], p.capsuleImages[slot^1] = copy, copy.Image()
		defer func() { p.capsuleViews[slot^1], p.capsuleImages[slot^1] = savedView, savedImage }()
		if database.commandWALCheckpointRootsEqualLocked(selected, fallback) {
			t.Fatal("malformed complete view falsely converged")
		}
	})
}

func TestCommandWALPrimaryCheckpointFallbackV6ChangedDirectory(t *testing.T) {
	database := primaryFallbackDatabaseV6(t)
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	b := database.NewPhysicalBatch().(*Batch)
	if err := b.Set([]byte("changed-key"), []byte("changed-value")); err != nil {
		t.Fatal(err)
	}
	if err := b.SetCommandWALPublish(3, []CommandWALLSNRange{{First: 3, Last: 3}}); err != nil {
		t.Fatal(err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	if err := database.rootPublication.coordinator.WaitThrough(context.Background(), database.State().CommitSeq); err != nil {
		t.Fatal(err)
	}
	database.durablePublishMu.Lock()
	slot := database.durableRoot.slot
	selected, fallback := database.durableRoot.slotRecord[slot], database.durableRoot.slotRecord[slot^1]
	a, c := database.durableRoot.primary.capsuleViews[slot].Directory(), database.durableRoot.primary.capsuleViews[slot^1].Directory()
	if bytes.Equal(a[12:], c[12:]) || database.commandWALCheckpointRootsEqualLocked(selected, fallback) {
		t.Fatal("actual changed key directory falsely converged")
	}
	database.durablePublishMu.Unlock()
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	if got, err := database.Get([]byte("changed-key")); err != nil || string(got) != "changed-value" {
		t.Fatal("refresh lost genuine changed directory", err)
	}
}

func TestRefreshCommandWALPrimaryCheckpointFallbackV6ActualAuthorityRefusal(t *testing.T) {
	database := primaryFallbackDatabaseV6(t)
	sentinel := errors.New("actual PRIMARY authority refusal")
	seen := false
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Root == database.dir && event.Point == durabilitycut.BeforeMetaWrite {
			if event.StablePath == nil || event.Length != 3*int64(page.PageSize) {
				t.Fatal("refusal omitted actual physical authority")
			}
			seen = true
			return sentinel
		}
		return nil
	})
	nextLSN := database.CommandWALNextLSN()
	err := database.RefreshCommandWALCheckpointFallback()
	restore()
	if !seen || !errors.Is(err, sentinel) {
		t.Fatalf("actual refusal lost seen=%v err=%v", seen, err)
	}
	if database.CommandWALNextLSN() != nextLSN {
		t.Fatal("refused refresh appended command WAL")
	}
	database.durablePublishMu.Lock()
	slot := database.durableRoot.slot
	selected, fallback := database.durableRoot.slotRecord[slot], database.durableRoot.slotRecord[slot^1]
	if !database.commandWALCheckpointFallbackNeedsRefreshLocked(selected.AppliedCommandLSN, selected, fallback) {
		t.Fatal("refused authority falsely converged")
	}
	database.durablePublishMu.Unlock()
}
