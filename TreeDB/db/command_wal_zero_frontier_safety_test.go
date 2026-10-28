package db

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func zeroFrontierCleanupDBForTest(t *testing.T, dir string, durableLSN uint64) *DB {
	t.Helper()
	database := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable}
	installCommandWALCleanupTwoRootRuntimeForTest(database, 1, durableLSN)
	fallback := database.durableRoot.slot ^ 1
	record := database.durableRoot.slotRecord[fallback]
	record.AppliedCommandLSN = 0
	database.durableRoot.slotRecord[fallback] = record
	installCommandWALCleanupJournalOwnerForTest(t, database, durableLSN)
	return database
}

func TestCommandWALCleanupZeroFrontierSafety(t *testing.T) {
	for _, variant := range []string{"missing_replay", "duplicate_replay", "corrupt_reserve", "pinned", "stale_root", "stale_journal", "pending_rotation", "replaced_identity"} {
		t.Run(variant, func(t *testing.T) {
			if variant == "replaced_identity" && runtime.GOOS == "windows" {
				t.Skip("Windows cannot rebind the retained open scan handle")
			}
			dir := t.TempDir()
			durableLSN := uint64(1)
			if variant == "missing_replay" || variant == "duplicate_replay" {
				durableLSN = 2
			}
			firstLSN := durableLSN
			if variant == "duplicate_replay" {
				firstLSN = 1
			}
			writeCommandWALFrame(t, dir, 1, firstLSN)
			emptySeq := uint64(2)
			if variant == "duplicate_replay" {
				writeCommandWALFrame(t, dir, 2, 2)
				emptySeq++
			}
			empty := writeEmptyFixedCleanupSegment(t, dir, emptySeq, 8192)
			database := zeroFrontierCleanupDBForTest(t, dir, durableLSN)
			wantErr := error(ErrCommandWALAppliedLSNNonContig)
			switch variant {
			case "duplicate_replay":
				wantErr = commitlog.ErrCommandWALDuplicateLSN
				// Journal open independently rejects duplicate replay. Inject it
				// after open to exercise cleanup's own zero-frontier gate.
				image, err := os.ReadFile(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1)))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 2)), image, 0o600); err != nil {
					t.Fatal(err)
				}
			case "corrupt_reserve":
				wantErr = commitlog.ErrCorrupt
				f, err := os.OpenFile(empty, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteAt([]byte{1}, 8191)
				closeErr := f.Close()
				if err != nil || closeErr != nil {
					t.Fatalf("corrupt reserve: write=%v close=%v", err, closeErr)
				}
			case "pinned":
				wantErr = rootpublication.ErrResourcePinned
				f, err := os.Open(empty)
				if err != nil {
					t.Fatal(err)
				}
				identity, err := rootpublication.StableIdentityFromFile(f)
				_ = f.Close()
				if err != nil {
					t.Fatal(err)
				}
				proof, err := database.captureDurableWALCleanupProofV1()
				if err != nil {
					t.Fatal(err)
				}
				err = database.commandJournal.WithCleanupSnapshot(proof.journal, func(registry *rootpublication.IdentityPinRegistry, _ commitlog.CommandJournalCleanupSnapshot) (bool, error) {
					if err := registry.Observe(identity); err != nil {
						return false, err
					}
					pin, err := registry.Pin(identity)
					if err == nil {
						t.Cleanup(pin.Release)
					}
					return false, err
				})
				if err != nil {
					t.Fatal(err)
				}
			case "stale_root":
				wantErr = errDurableWALCleanupProofStale
				database.testCommandWALCleanupAfterScanHook = func() {
					database.durableRoot.pending = &durableRootPublishCandidateV1{}
				}
			case "stale_journal":
				wantErr = commitlog.ErrCommandWALCleanupSnapshotStale
				database.testCommandWALCleanupAfterScanHook = func() {
					if err := database.commandJournal.Close(); err != nil {
						t.Fatal(err)
					}
				}
			case "pending_rotation":
				wantErr = commitlog.ErrCommandWALCleanupSnapshotStale
				t.Cleanup(func() {
					rotations, err := database.commandJournal.TakePendingStableRotations()
					if err != nil {
						t.Error(err)
					}
					for _, rotation := range rotations {
						rotation.Release()
					}
				})
				database.testCommandWALCleanupAfterScanHook = func() {
					// Actual first-frame physical pressure creates pending stable
					// ownership; an explicit completed rotation is monotonic.
					payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
						Op: commitlog.RawKVOpSet, Key: []byte("pending"), Value: make([]byte, 5<<20),
					}})
					if err != nil {
						t.Fatal(err)
					}
					_, err = database.commandJournal.AppendCommand(commitlog.CommandEnvelope{
						Version: commitlog.CommandFrameVersionV2, DurabilityClass: commitlog.CommandDurabilityDurable,
						Kind: commitlog.CommandKindRawKVBatch, Scope: commitlog.CommandScopeRawKV,
						PayloadFormat: commitlog.PayloadFormatRawKVBatchV1, Payload: payload,
					})
					if err != nil {
						t.Fatal(err)
					}
				}
			case "replaced_identity":
				wantErr = ErrRecoveryRequired
				database.testCommandWALCleanupAfterScanHook = func() {
					if err := os.Rename(empty, empty+".old"); err != nil {
						t.Fatal(err)
					}
					writeEmptyFixedCleanupSegment(t, dir, emptySeq, 8192)
				}
			}
			err := database.CleanupCommandWALCoveredSegments(true)
			if !errors.Is(err, wantErr) {
				t.Fatalf("cleanup error=%v, want %v", err, wantErr)
			}
			if variant == "missing_replay" || variant == "duplicate_replay" {
				// Checkpoint and Close already normalize unavailable replay proof
				// to conservative retention, rather than failing teardown.
				if !errors.Is(err, errDurableWALCleanupProofUnavailable) || normalizeCommandWALCheckpointCleanupError(err) != nil {
					t.Fatalf("incomplete replay did not retain conservative availability: %v", err)
				}
			}
			if _, err := os.Stat(empty); err != nil {
				t.Fatalf("unsafe empty-file removal: %v", err)
			}
			if database.commandWALCleanupRemoved.Load() != 0 || database.commandWALCleanupNamespaceDirty.Load() {
				t.Fatal("refused cleanup changed durable deletion accounting")
			}
		})
	}
}

func TestCommandWALCleanupZeroFrontierNamespaceSync(t *testing.T) {
	for _, withEmpty := range []bool{false, true} {
		name := "no_op"
		if withEmpty {
			name = "sync_failure_and_retry"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeCommandWALFrame(t, dir, 1, 1)
			if withEmpty {
				writeEmptyFixedCleanupSegment(t, dir, 2, 8192)
			}
			database := zeroFrontierCleanupDBForTest(t, dir, 1)
			cutErr := errors.New("zero-frontier deletion sync cut")
			syncs := 0
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.BeforeDeletionDirectorySync {
					syncs++
					return cutErr
				}
				return nil
			})
			err := database.CleanupCommandWALCoveredSegments(true)
			restore()
			if !withEmpty {
				if err != nil || syncs != 0 || database.commandWALCleanupNamespaceSyncs.Load() != 0 {
					t.Fatalf("no-op cleanup synced namespace: err=%v syncs=%d", err, syncs)
				}
				return
			}
			if !errors.Is(err, cutErr) || !errors.Is(err, ErrRecoveryRequired) || syncs != 1 || !database.commandWALCleanupNamespaceDirty.Load() || database.commandWALCleanupRemoved.Load() != 0 || database.commandWALCleanupUnlinkedPending.Load() != 1 {
				t.Fatalf("failed directory sync lost deletion debt: err=%v syncs=%d", err, syncs)
			}
			if err := database.CleanupCommandWALCoveredSegments(true); err != nil {
				t.Fatal(err)
			}
			if database.commandWALCleanupNamespaceDirty.Load() || database.commandWALCleanupRemoved.Load() != 1 || database.commandWALCleanupBytes.Load() != 8192 {
				t.Fatal("directory-sync retry lost physical deletion accounting")
			}
		})
	}
}
