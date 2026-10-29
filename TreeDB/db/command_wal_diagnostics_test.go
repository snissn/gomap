package db

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestInspectClosedCommandWALPreservesRetentionAndReopen(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.InspectClosedCommandWALV1(); err == nil {
		t.Fatal("accepted open handle")
	}
	batch := database.NewBatch()
	if err := batch.Set([]byte("retained"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.rootPublication.coordinator.WaitThrough(context.Background(), database.State().CommitSeq); err != nil {
		t.Fatal(err)
	}
	if database.commandWALDurableLSN.Load() >= database.State().AppliedCommandLSN {
		t.Fatal("fixture did not create unavailable cleanup proof")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	segments, err := listWALSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	before := make(map[string][32]byte)
	for _, segment := range segments {
		raw, err := os.ReadFile(segment.path)
		if err != nil {
			t.Fatal(err)
		}
		before[segment.path] = sha256.Sum256(raw)
	}
	out, err := database.InspectClosedCommandWALV1()
	if err != nil {
		t.Fatal(err)
	}
	if out.ProofError == "" || len(out.Segments) == 0 || out.CleanupCounters["treedb.command_wal.cleanup.retries_total"] == "0" {
		t.Fatalf("lost conservative retention evidence: %+v", out)
	}
	if out.Roots[out.SelectedSlot].AppliedCommandLSN != database.State().AppliedCommandLSN {
		t.Fatalf("slot identity=%+v", out)
	}
	for path, digest := range before {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if sha256.Sum256(raw) != digest {
			t.Fatalf("diagnostics changed %s", path)
		}
	}
	reopened, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if value, err := reopened.Get([]byte("retained")); err != nil || string(value) != "value" {
		t.Fatalf("reopen value=%q error=%v", value, err)
	}
}

func TestInspectClosedCommandWALUnavailableProofAfterEarlierCleanup(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.SetSync([]byte("covered"), []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := database.rootPublication.coordinator.WaitThrough(context.Background(), database.State().CommitSeq); err != nil {
		t.Fatal(err)
	}
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	coveredPath, _ := database.commandWALActiveSegmentSnapshot()
	if err := database.RotateCommandWALActiveSegment(true); err != nil {
		t.Fatal(err)
	}
	if err := database.CleanupCommandWALCoveredSegments(false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(coveredPath); !os.IsNotExist(err) {
		t.Fatalf("earlier covered WAL was not removed: %v", err)
	}
	durableLSN := database.commandWALDurableLSN.Load()
	if durableLSN == 0 {
		t.Fatal("fixture lacks a nonzero removed prefix")
	}
	// Publish a relaxed successor beyond the dependency-closed WAL frontier.
	if err := database.Set([]byte("retained-later"), []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := database.rootPublication.coordinator.WaitThrough(context.Background(), database.State().CommitSeq); err != nil {
		t.Fatal(err)
	}
	if database.State().AppliedCommandLSN <= database.commandWALDurableLSN.Load() {
		t.Fatal("fixture lacks unavailable cleanup proof")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	// The old diagnostic used cleanupThrough=0 and fabricated a replay gap for
	// the prefix which the earlier successful cleanup legitimately removed.
	decisions, oldErr := scanCommandWALSegmentsForCleanupProof(dir, 0, database.commandWALDurableLSN.Load(), database.walMaxSegmentBytes)
	closeCommandWALCleanupDecisions(decisions)
	if !errors.Is(oldErr, ErrCommandWALAppliedLSNNonContig) {
		t.Fatalf("control did not expose the zero-frontier diagnostic error: %v", oldErr)
	}
	out, err := database.InspectClosedCommandWALV1()
	if err != nil || out.ScanError != "" || out.ProofError == "" || out.ReplayLineageValidated {
		t.Fatalf("unavailable proof became fabricated scan failure: %+v; %v", out, err)
	}
	if len(out.Segments) == 0 {
		t.Fatal("lost remaining segment inventory")
	}
	reopened, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for key, want := range map[string]string{"covered": "first", "retained-later": "second"} {
		got, err := reopened.Get([]byte(key))
		if err != nil || string(got) != want {
			t.Fatalf("reopen %q=%q error=%v", key, got, err)
		}
	}
}

// The observer reads existing terminal results; recording diagnostics must not
// require fields or writes in the production close/cleanup paths.
func TestInspectClosedCommandWALRetainsExistingCloseErrors(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected terminal teardown failure")
	database.registerInternalTeardownHook(func() error { return failure })
	if err := database.Close(); !errors.Is(err, failure) {
		t.Fatalf("close error=%v", err)
	}
	out, err := database.InspectClosedCommandWALV1()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.CloseError, failure.Error()) || !strings.Contains(out.CloseCleanupError, failure.Error()) {
		t.Fatalf("lost existing terminal errors: %+v", out)
	}
}

func TestInspectClosedCommandWALPoisonPreventsReplayAuthority(t *testing.T) {
	for _, tc := range []struct {
		name        string
		fail        func(*DB) error
		wantError   error
		publication bool
	}{
		{"ambiguous metadata sync", func(db *DB) error {
			db.testFailSyncMeta.Store(true)
			defer db.testFailSyncMeta.Store(false)
			if err := db.SetSync([]byte("uncertain"), []byte("second")); err != nil {
				return err
			}
			// Command-WAL SetSync durably appends the journal but may return
			// before physical root publication. Keep the fault armed through
			// that exact publication instead of clearing it after admission.
			return db.rootPublication.coordinator.WaitThrough(context.Background(), db.State().CommitSeq)
		}, errTestSyncMetaFailpoint, true},
		{"post-append WAL flush", func(db *DB) error {
			db.testFailCommandWALFlush.Store(true)
			defer db.testFailCommandWALFlush.Store(false)
			return db.SetSync([]byte("uncertain"), []byte("second"))
		}, errTestCommandWALFlushFailpoint, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			enableCommandWALFormat(t, dir)
			database := openCommandWALDB(t, dir)
			defer database.Close()
			if err := database.SetSync([]byte("covered"), []byte("first")); err != nil {
				t.Fatal(err)
			}
			if err := database.rootPublication.coordinator.WaitThrough(context.Background(), database.State().CommitSeq); err != nil {
				t.Fatal(err)
			}
			if err := tc.fail(database); !errors.Is(err, tc.wantError) {
				t.Fatalf("fault did not execute: %v", err)
			}
			if tc.publication && !database.publicationPoisoned.Load() || !tc.publication && !database.commandWALFlushPoisoned.Load() {
				t.Fatal("fault did not poison the intended path")
			}
			_ = database.Close()
			if !database.closeTeardownCompleted {
				t.Fatal("close did not complete teardown")
			}
			// This is the old, insufficient check. After teardown its internally
			// consistent runtime slots can supply a proof despite persistent poison.
			if _, err := captureDurableWALCleanupProofFromRuntimeV1(database.durableRoot, database.commandWALDurableLSN.Load()); err != nil {
				t.Fatalf("control lacks an otherwise plausible runtime proof: %v", err)
			}
			out, err := database.InspectClosedCommandWALV1()
			if err != nil {
				t.Fatal(err)
			}
			if out.ProofError == "" || !strings.Contains(out.ProofError, ErrRecoveryRequired.Error()) || out.ReplayLineageValidated ||
				out.RootObservation != "closed_handle_runtime_not_persisted_slot_verification" || len(out.Segments) == 0 {
				t.Fatalf("poisoned runtime claimed recovery authority or lost inventory: %+v", out)
			}
			if tc.publication && !out.PublicationPoisoned || !tc.publication && !out.CommandWALFlushPoisoned {
				t.Fatalf("lost poison evidence: %+v", out)
			}
			for _, segment := range out.Segments {
				if segment.CoveredByRoots {
					t.Fatalf("poisoned roots claimed coverage: %+v", segment)
				}
			}
		})
	}
}
