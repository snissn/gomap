package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestRecoverableRootSetIncludesBothDurableSlotsAndRevalidates(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()

	if err := database.SetSync([]byte("one"), []byte("1")); err != nil {
		t.Fatalf("SetSync one: %v", err)
	}
	if err := database.SetSync([]byte("two"), []byte("2")); err != nil {
		t.Fatalf("SetSync two: %v", err)
	}

	set, err := database.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatalf("CaptureRecoverableRootSet: %v", err)
	}
	defer set.Release()
	if err := set.Revalidate(); err != nil {
		t.Fatalf("Revalidate unchanged set: %v", err)
	}

	roots := set.Roots()
	for slot, record := range database.durableRoot.slotRecord {
		if record.CommitSeq == 0 {
			t.Fatalf("durable slot %d is empty after two synchronous writes", slot)
		}
		found := false
		for _, root := range roots {
			if root.CommitSeq == record.CommitSeq && root.UserRootPageID == record.UserRootPageID && root.SystemRootPageID == record.SystemRootPageID && root.Durable {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("durable slot %d record %+v missing from roots %+v", slot, record, roots)
		}
	}
	for _, root := range roots {
		snapshot := set.AcquireSnapshotForRoot(root)
		if snapshot == nil {
			t.Fatalf("AcquireSnapshotForRoot(%+v) returned nil", root)
		}
		state, ok := snapshot.StateToken()
		if !ok || state.CommitSeq != root.CommitSeq || state.RootPageID != root.UserRootPageID || state.SystemRootPageID != root.SystemRootPageID {
			_ = snapshot.Close()
			t.Fatalf("root snapshot state=%+v ok=%v want %+v", state, ok, root)
		}
		if err := snapshot.Close(); err != nil {
			t.Fatalf("Close root snapshot: %v", err)
		}
	}
}

func TestRecoverableRootSetKeepsDistinctReplayFrontiersForSamePages(t *testing.T) {
	durable := recoverableDurableBasis{slotRecord: [2]rootpublication.DurableRootRecordV1{
		{CommitSeq: 7, UserRootPageID: 11, SystemRootPageID: 13, AppliedCommandLSN: 3, MaxEntryRevision: 17},
	}}
	roots := recoverableRootsForBasis(StateToken{
		CommitSeq: 7, RootPageID: 11, SystemRootPageID: 13, AppliedCommandLSN: 5, MaxEntryRevision: 19,
	}, durable, rootpublication.ReachabilitySnapshot{})
	if len(roots) != 2 {
		t.Fatalf("roots=%+v want distinct durable and visible replay frontiers", roots)
	}
	set := &RecoverableRootSet{roots: roots}
	for _, root := range roots {
		if !set.containsRoot(root) {
			t.Fatalf("captured root rejected: %+v", root)
		}
	}
	mutated := roots[0]
	mutated.AppliedCommandLSN++
	if set.containsRoot(mutated) {
		t.Fatalf("uncaptured replay frontier accepted: %+v", mutated)
	}
}

func TestRecoverableRootSetRejectsWriteAfterCapture(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()

	set, err := database.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatalf("CaptureRecoverableRootSet: %v", err)
	}
	defer set.Release()
	if err := database.Set([]byte("after"), []byte("capture")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := set.Revalidate(); !errors.Is(err, ErrRecoverableRootSetStale) {
		t.Fatalf("Revalidate after write=%v want ErrRecoverableRootSetStale", err)
	}
}

func TestRecoverableRootSetRejectsDurableAdvanceAfterCapture(t *testing.T) {
	database, err := Open(Options{
		Dir:                       t.TempDir(),
		DisableBackgroundPrune:    true,
		rootPublicationFixedDelay: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()

	if err := database.Set([]byte("visible"), []byte("not-yet-durable")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	set, err := database.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatalf("CaptureRecoverableRootSet: %v", err)
	}
	defer set.Release()
	if err := set.Revalidate(); err != nil {
		t.Fatalf("Revalidate before durable advance: %v", err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := set.Revalidate(); !errors.Is(err, ErrRecoverableRootSetStale) {
		t.Fatalf("Revalidate after durable advance=%v want ErrRecoverableRootSetStale", err)
	}
}

func TestRecoverableRootSetExactFilePinBlocksDeleteUntilRelease(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()

	set, err := database.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatalf("CaptureRecoverableRootSet: %v", err)
	}
	path := filepath.Join(t.TempDir(), "segment")
	if err := os.WriteFile(path, []byte("stable"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open file: %v", err)
	}
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		_ = file.Close()
		t.Fatalf("StableIdentityFromFile: %v", err)
	}
	identity.Generation = 0
	if err := set.PinStableFile(file); err != nil {
		_ = file.Close()
		t.Fatalf("PinStableFile: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close file: %v", err)
	}

	registry := database.StableResourceIdentityPinRegistry()
	if _, err := registry.BeginDeleteAt(identity, "recoverable-root-set/test-segment"); !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatalf("BeginDeleteAt while pinned=%v want ErrResourcePinned", err)
	}
	set.Release()
	lease, err := registry.BeginDeleteAt(identity, "recoverable-root-set/test-segment")
	if err != nil {
		t.Fatalf("BeginDeleteAt after release: %v", err)
	}
	lease.Abort()
}
