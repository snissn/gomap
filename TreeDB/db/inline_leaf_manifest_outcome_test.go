package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestInlineLeafManifestCompatibilityFollowsDurableOutcome(t *testing.T) {
	for _, outcome := range []struct {
		name              string
		refuseBeforeMeta  bool
		refuseMetaSync    bool
		refuseAbandonSync bool
	}{
		{name: "pre-meta-retry-exhaustion", refuseBeforeMeta: true},
		{name: "pre-meta-rollback-fence-refusal", refuseBeforeMeta: true, refuseAbandonSync: true},
		{name: "ambiguous-meta-sync-refusal", refuseMetaSync: true},
		{name: "durable-meta-visible-install-refusal"},
	} {
		t.Run(outcome.name, func(t *testing.T) {
			dir := t.TempDir()
			database, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
			t.Cleanup(func() {
				if database != nil {
					closeLeafGenerationPackPublicationTestDB(t, database, leafLog)
				}
			})
			seed := prepareLeafGenerationPackTestCandidate(t, database, leafLog, 512)
			pack, err := database.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
				GenerationIDs: []uint64{seed.generation.GenerationID}, Force: true, Sync: true,
			})
			if err != nil || len(pack.CreatedFileIDs) == 0 {
				t.Fatalf("seed pack: %+v %v", pack, err)
			}
			disableRootPublicationForLegacyVacuumBenchmark(t, database)
			compatibilityPath := leafGenerationManifestPath(LeafLogDirPath(dir))
			if _, err := os.Stat(compatibilityPath); !os.IsNotExist(err) {
				t.Fatalf("stable open must not create manifest.json: %v", err)
			}
			readRevisions := func() []string {
				t.Helper()
				revisions, err := filepath.Glob(filepath.Join(LeafLogDirPath(dir), "manifest.durable.*.json"))
				if err != nil {
					t.Fatal(err)
				}
				return revisions
			}
			beforeRevisions := readRevisions()
			before, _ := database.StateToken()
			key := []byte("pack-concurrent-0000")
			originalValue, err := database.Get(key)
			if err != nil || len(originalValue) == 0 {
				t.Fatalf("predecessor value: %x %v", originalValue, err)
			}

			if err := leafLog.rotateLeaf(); err != nil {
				t.Fatal(err)
			}
			_, taggedFileID := currentLeafSegmentOrFatal(t, leafLog)
			rotatedFileID := page.ValueLogSegmentID(taggedFileID)
			refusal := errTestDurableRootVisibleInstallFailpoint
			attempts := 0
			switch {
			case outcome.refuseBeforeMeta:
				refusal = errors.New("persistent refusal before inline meta write")
			case outcome.refuseMetaSync:
				refusal = errTestSyncMetaFailpoint
				database.testFailSyncMeta.Store(true)
			default:
				database.testFailDurableRootVisibleInstall.Store(true)
			}
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if outcome.refuseBeforeMeta && event.Point == durabilitycut.BeforeMetaWrite && event.Resource == durabilitycut.ResourceMeta {
					attempts++
					return refusal
				}
				return nil
			})
			const changed = "inline replacement"
			err = database.SetSync(key, []byte(changed))
			restore()
			database.testFailSyncMeta.Store(false)
			database.testFailDurableRootVisibleInstall.Store(false)
			if !errors.Is(err, refusal) || !errors.Is(err, ErrRecoveryRequired) || CommitPublicationAccepted(err) {
				t.Fatalf("outcome did not fail closed: %v", err)
			}
			if current, _ := database.StateToken(); current != before {
				t.Fatalf("unadmitted replacement became visible: before=%+v current=%+v", before, current)
			}
			if err := database.SetSync(key, []byte("must fail closed")); !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("a later write advanced the failed handle: %v", err)
			}
			afterRevisions := readRevisions()
			if len(afterRevisions) != len(beforeRevisions)+1 {
				t.Fatalf("exact successor missing before terminal outcome: before=%v after=%v", beforeRevisions, afterRevisions)
			}
			inline := database.inlineLeafManifest
			if inline == nil || inline.candidate == nil || inline.update.prepared == nil {
				t.Fatal("failed publication lost the original prepared manifest custody")
			}
			switch {
			case outcome.refuseBeforeMeta:
				pending := database.durableRoot.pending
				if attempts != maxDurableRootPreMetaRetriesV1 || pending != inline.candidate || pending.metaMutated {
					t.Fatalf("unapplied candidate identity: attempts=%d pending=%v", attempts, pending)
				}
			case outcome.refuseMetaSync:
				ambiguous := database.durableRoot.ambiguous
				if len(ambiguous) != 1 || ambiguous[0] != inline.candidate || !inline.candidate.metaMutated {
					t.Fatal("meta mutation did not retain the original uncertain candidate")
				}
			default:
				if database.durableRoot.record.CommitSeq != before.CommitSeq+1 {
					t.Fatal("test failed before durable successor was installed")
				}
			}
			fenceErr := errors.New("manifest rollback deletion fence refused")
			fenceAttempts := 0
			restore = durabilitycut.Install(func(event durabilitycut.Event) error {
				if outcome.refuseAbandonSync && event.Point == durabilitycut.BeforeDeletionDirectorySync && event.Resource == durabilitycut.ResourceOuterLeaf {
					fenceAttempts++
					return fenceErr
				}
				return nil
			})
			closed := database
			closeErr := database.Close()
			var repeatCloseErr error
			if outcome.refuseAbandonSync {
				repeatCloseErr = database.Close()
			}
			restore()
			database = nil
			if err := leafLog.Close(); err != nil {
				t.Fatal(err)
			}
			if outcome.refuseAbandonSync {
				if !errors.Is(closeErr, fenceErr) || !errors.Is(closeErr, ErrRecoveryRequired) || fenceAttempts != 1 {
					t.Fatalf("Close lost the original rollback refusal: error=%v attempts=%d", closeErr, fenceAttempts)
				}
				if !errors.Is(repeatCloseErr, fenceErr) || fenceAttempts != 1 {
					t.Fatalf("second Close did not preserve the first rollback refusal: error=%v attempts=%d", repeatCloseErr, fenceAttempts)
				}
			} else if closeErr != nil {
				logOriginalCleanupDiagnostic(t, closeErr)
				t.Fatalf("Close: %v", closeErr)
			}
			if !outcome.refuseAbandonSync {
				if pins := closed.valueLogIdentityPins.ActivePins(); pins != 0 {
					t.Fatalf("Close retained %d identity pins", pins)
				}
			}
			if _, err := os.Stat(compatibilityPath); !os.IsNotExist(err) {
				t.Errorf("stable outcome unexpectedly created manifest.json: %v", err)
			}
			if outcome.refuseBeforeMeta {
				if !outcome.refuseAbandonSync && !slices.Equal(readRevisions(), beforeRevisions) {
					t.Errorf("Close did not remove the unapplied immutable successor")
				}
			} else if !slices.Equal(readRevisions(), afterRevisions) {
				t.Errorf("Close rolled back a potentially durable successor")
			}
			reopened, err := Open(leafGenerationPackPublicationTestOptions(dir))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
			recovered, _ := reopened.StateToken()
			wantSuccessor := !outcome.refuseBeforeMeta
			if outcome.refuseMetaSync {
				wantSuccessor = recovered.CommitSeq == before.CommitSeq+1
			}
			wantSeq, wantValue := before.CommitSeq, originalValue
			if wantSuccessor {
				wantSeq, wantValue = before.CommitSeq+1, []byte(changed)
			}
			if recovered.CommitSeq != wantSeq {
				t.Errorf("reopened commit %d, want %d", recovered.CommitSeq, wantSeq)
			}
			if value, err := reopened.Get(key); err != nil || !bytes.Equal(value, wantValue) {
				t.Errorf("reopen recovered wrong root: got %x want %x error=%v", value, wantValue, err)
			}
			view := reopened.state.Load().LeafGenerations
			inManifest := view != nil && view.FileToGeneration[rotatedFileID] != 0
			if inManifest != wantSuccessor {
				t.Errorf("reopened root and rotated file %d disagree: exact manifest membership=%t", rotatedFileID, inManifest)
			}
		})
	}
}
