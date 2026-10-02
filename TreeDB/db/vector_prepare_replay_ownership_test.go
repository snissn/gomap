package db

import (
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"testing"
)

func vectorPrepareReplayOwnershipDBV1(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureCommandWALV1}, DurabilityProfile: ProfileCommandWALDurable}); err != nil {
		t.Fatal(err)
	}
	d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true, ResolvedProfile: ProfileCommandWALDurable})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// Read the genuine assigned frame from its durable journal, then use the
// existing replay callback boundary to mint, expire and remint its authority.
// No test stores an active replay LSN/token or fabricates a publication guard.
func TestVectorPrepareReplayOwnershipActualFrameV1(t *testing.T) {
	d, other := vectorPrepareReplayOwnershipDBV1(t), vectorPrepareReplayOwnershipDBV1(t)
	payload, err := commitlog.EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := d.NewCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := d.LockCommandWALStagingGuardV1()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := guard.BorrowStableResourceCaptureLeaseV1(d, intent); err == nil {
		t.Fatal("unassigned guard admitted capture")
	}
	assigned, err := guard.Append(intent, true)
	if err != nil {
		guard.Release()
		t.Fatal(err)
	}
	copied := *guard
	borrowed, err := copied.BorrowStableResourceCaptureLeaseV1(d, intent)
	if err != nil {
		guard.Release()
		t.Fatal(err)
	}
	if err := borrowed.ValidateCommandWALStagingCaptureV1(d, intent); err != nil {
		guard.Release()
		t.Fatal(err)
	}
	// Publication ends the staged intent even while the actual guard remains
	// held. Borrow before publication, then prove that this completion boundary
	// revokes borrowing without weakening the guard's exact-intent check.
	if err := d.PublishStagedCommandWALNoop(intent, true); err != nil {
		guard.Release()
		t.Fatal(err)
	}
	if intent.StagedForPublish() {
		guard.Release()
		t.Fatal("published noop retained staged authority")
	}
	if err := borrowed.ValidateCommandWALStagingCaptureV1(d, intent); err == nil {
		guard.Release()
		t.Fatal("publication left borrowed staging authority live")
	}
	if _, err := copied.BorrowStableResourceCaptureLeaseV1(d, intent); err == nil {
		guard.Release()
		t.Fatal("live guard admitted capture after publication")
	}
	guard.Release()
	copied.Release() // Shared expiry makes this harmless; no second unlock.
	if err := borrowed.ValidateDBV1(d); err == nil {
		t.Fatal("copied handle outlived original guard")
	}
	if _, err := copied.BorrowStableResourceCaptureLeaseV1(d, intent); err == nil {
		t.Fatal("copied guard admitted expired capture")
	}
	cleanupCalled := false
	if err := borrowed.RetainStableResourceCaptureRecovery(func() error { cleanupCalled = true; return nil }); err == nil {
		t.Fatal("expired borrower transferred recovery")
	}
	if cleanupCalled {
		t.Fatal("expired borrower ran cleanup")
	}
	borrowed.Release()
	if err := d.ValidateCommandWALReplayOperationV1(intent); err == nil {
		t.Fatal("ordinary intent admitted as startup replay")
	}
	segments, err := listWALSegments(d.Dir())
	if err != nil {
		t.Fatal(err)
	}
	frames, err := readCommandWALV2PhysicalFrames(segments, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var env commitlog.CommandEnvelope
	for _, frame := range frames {
		if frame.Envelope.LSN == assigned {
			env = frame.Envelope
			break
		}
	}
	if env.LSN == 0 {
		t.Fatal("assigned frame absent from real journal")
	}
	if _, err := d.NewCommandWALReplayIntent(env); err == nil {
		t.Fatal("minted replay intent outside callback")
	}
	var retained *CommandWALIntent
	err = d.applyRegisteredCommandWALFrame(env, commandWALReplayHandlerRegistration{handler: func(active *DB, actual commitlog.CommandEnvelope) error {
		var err error
		retained, err = active.NewCommandWALReplayIntent(actual)
		if err != nil {
			return err
		}
		if err := active.ValidateCommandWALReplayOperationV1(retained); err != nil {
			return err
		}
		if retained.StagedForPublish() {
			t.Fatal("startup intent claimed assigned raw staging")
		}
		// The other DB has a genuine callback and may have the same numeric token.
		return other.applyRegisteredCommandWALFrame(actual, commandWALReplayHandlerRegistration{handler: func(foreignDB *DB, foreignFrame commitlog.CommandEnvelope) error {
			foreign, err := foreignDB.NewCommandWALReplayIntent(foreignFrame)
			if err != nil {
				return err
			}
			if err := foreignDB.ValidateCommandWALReplayOperationV1(foreign); err != nil {
				return err
			}
			if err := active.ValidateCommandWALReplayOperationV1(foreign); err == nil {
				t.Fatal("accepted another DB's replay capability")
			}
			if err := foreignDB.ValidateCommandWALReplayOperationV1(retained); err == nil {
				t.Fatal("accepted original DB capability in another DB")
			}
			return nil
		}})
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.ValidateCommandWALReplayOperationV1(retained); err == nil {
		t.Fatal("callback exit left capability live")
	}
	err = d.applyRegisteredCommandWALFrame(env, commandWALReplayHandlerRegistration{handler: func(active *DB, actual commitlog.CommandEnvelope) error {
		if err := active.ValidateCommandWALReplayOperationV1(retained); err == nil {
			t.Fatal("same-LSN later callback accepted old token")
		}
		current, err := active.NewCommandWALReplayIntent(actual)
		if err != nil {
			return err
		}
		return active.ValidateCommandWALReplayOperationV1(current)
	}})
	if err != nil {
		t.Fatal(err)
	}
	forged := &CommandWALStagingGuardV1{}
	if _, err := forged.BorrowStableResourceCaptureLeaseV1(d, intent); err == nil {
		t.Fatal("zero guard admitted capture")
	}
}
