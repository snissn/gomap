package db

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Metadata discovery must not read reserve bytes; the exact-handle cleanup
// reader still validates every recognized non-value file, even outside the
// command lane. No classification survives to the next cleanup invocation.
func TestCommandWALCleanupSingleValidationRejectsChangedReserve(t *testing.T) {
	for _, name := range []string{"inactive", "active", "legacy"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeEmptyFixedCleanupSegment(t, dir, 1, 8192)
			if name != "active" {
				writeEmptyFixedCleanupSegment(t, dir, 2, 8192)
			}
			if name == "legacy" {
				target := filepath.Join(WALDirPath(dir), "wal-000001.log")
				if err := os.Rename(path, target); err != nil {
					t.Fatal(err)
				}
				path = target
			}
			decisions, err := scanCommandWALSegmentsForCleanupProof(dir, 0, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			closeCommandWALCleanupDecisions(decisions)
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.WriteAt([]byte{1}, 8191); err != nil {
				t.Fatal(err)
			}
			if err = f.Close(); err != nil {
				t.Fatal(err)
			}
			// Discovery preserves physical metadata only. The payload must still fail
			// the complete cleanup scan after an earlier successful invocation.
			if _, err = listSegmentsInDirWithLogicalSizes(WALDirPath(dir), false); err != nil {
				t.Fatal(err)
			}
			decisions, err = scanCommandWALSegmentsForCleanupProof(dir, 0, 0, 0)
			defer closeCommandWALCleanupDecisions(decisions)
			if !errors.Is(err, commitlog.ErrCorrupt) {
				t.Fatalf("changed reserve accepted: %v", err)
			}
			if _, err = os.Stat(path); err != nil {
				t.Fatalf("corrupt file removed: %v", err)
			}
		})
	}
}

// This isolated scan is also the syscall-count fixture: two fixed containers
// must each undergo one complete reserve validation, not listing plus reader.
func TestCommandWALCleanupSingleValidationLogicalAccounting(t *testing.T) {
	dir := t.TempDir()
	first := writeEmptyFixedCleanupSegment(t, dir, 1, 8192)
	second := writeEmptyFixedCleanupSegment(t, dir, 2, 8192)
	decisions, err := scanCommandWALSegmentsForCleanupProof(dir, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer closeCommandWALCleanupDecisions(decisions)
	for _, path := range []string{first, second} {
		d := emptyFixedCleanupDecision(t, decisions, path)
		if d.Size != 8192 || d.LogicalSize != 0 || !d.Covered {
			t.Fatalf("physical/logical accounting: %+v", d)
		}
		if d.Active != (path == second) {
			t.Fatalf("active classification: %+v", d)
		}
	}
}

func TestCommandWALCleanupCorruptReserveMissingLineageIsHardError(t *testing.T) {
	for _, operation := range []string{"explicit", "checkpoint", "prefix", "close"} {
		for _, advance := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/advance=%t", operation, advance), func(t *testing.T) {
				dir := t.TempDir()
				d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = d.Close() })
				requireZeroFrontierFixedContainer(t, d)
				if err = d.SetSync([]byte("k"), []byte("v")); err != nil {
					t.Fatal(err)
				}
				if operation != "checkpoint" || advance {
					if err = d.Checkpoint(); err != nil {
						t.Fatal(err)
					}
				}
				if advance {
					if err = d.SetSync([]byte("k"), []byte("next")); err != nil {
						t.Fatal(err)
					}
					if operation != "checkpoint" {
						if err = d.Checkpoint(); err != nil {
							t.Fatal(err)
						}
					}
				}
				proof, err := d.captureDurableWALCleanupProofV1()
				if err != nil {
					t.Fatal(err)
				}
				if advance && operation != "checkpoint" && proof.cleanupThrough == 0 {
					t.Fatal("expected nonzero cleanup frontier")
				}
				segments, err := listWALSegments(dir)
				if err != nil {
					t.Fatal(err)
				}
				var path string
				for _, s := range segments {
					if s.size > 0 && !s.valueLog {
						path = s.path
						break
					}
				}
				if path == "" {
					t.Fatal("missing framed segment")
				}
				f, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				info, err := f.Stat()
				if err != nil {
					t.Fatal(err)
				}
				if _, err = f.WriteAt([]byte{1}, info.Size()-1); err != nil {
					t.Fatal(err)
				}
				if err = f.Close(); err != nil {
					t.Fatal(err)
				}
				beforeScans := d.commandWALCleanupScans.Load()
				switch operation {
				case "explicit":
					err = d.CleanupCommandWALCoveredSegments(false)
				case "checkpoint":
					err = d.Checkpoint()
				case "prefix":
					_, err = d.CleanupCommandWALCoveredPrefix()
				case "close":
					err = d.Close()
				}
				if operation == "checkpoint" && advance {
					afterProof, e := d.captureDurableWALCleanupProofV1()
					if e != nil || afterProof.cleanupThrough == 0 {
						t.Fatalf("checkpoint did not establish nonzero frontier: %+v %v", afterProof, e)
					}
				}
				if d.commandWALCleanupScans.Load() <= beforeScans {
					t.Fatal("operation did not invoke cleanup scan")
				}
				if !errors.Is(err, commitlog.ErrCorrupt) || errors.Is(err, errDurableWALCleanupProofUnavailable) {
					t.Fatalf("corruption normalized away: %v", err)
				}
				if _, statErr := os.Stat(path); statErr != nil {
					t.Fatalf("corrupt path removed: %v", statErr)
				}
			})
		}
	}
}

func TestCommandWALCleanupErrorNormalizationPreservesMixedBranches(t *testing.T) {
	fault := errors.New("observer fault")
	for _, tc := range []struct {
		name string
		err  error
		safe bool
	}{
		{"unavailable", commandWALCleanupProofRefusal(errDurableWALCleanupProofUnavailable), true},
		{"raw_unavailable", errDurableWALCleanupProofUnavailable, false},
		{"lineage", &commandWALCleanupRefusal{cause: errors.Join(errDurableWALCleanupProofUnavailable, fmt.Errorf("gap: %w", ErrCommandWALAppliedLSNNonContig))}, true},
		{"duplicate", &commandWALCleanupRefusal{cause: errors.Join(errDurableWALCleanupProofUnavailable, commitlog.ErrCommandWALDuplicateLSN)}, true},
		{"registry_pin", &commandWALCleanupRefusal{cause: errors.Join(errDurableWALCleanupProofUnavailable, rootpublication.ErrResourcePinned)}, true},
		{"observer_pin", rootpublication.ErrResourcePinned, false},
		{"observer_joined_pin", errors.Join(rootpublication.ErrResourcePinned, fault), false},
		{"mixed_corrupt", errors.Join(errDurableWALCleanupProofUnavailable, commitlog.ErrCorrupt, ErrCommandWALAppliedLSNNonContig), false},
		{"mixed_observer", errors.Join(errDurableWALCleanupProofUnavailable, fault), false},
		{"stale_corrupt", errors.Join(errDurableWALCleanupProofStale, commitlog.ErrCorrupt), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeCommandWALCheckpointCleanupError(tc.err)
			if tc.safe {
				if got != nil {
					t.Fatalf("safe refusal not normalized: %v", got)
				}
			} else {
				if got == nil {
					t.Fatal("hard refusal swallowed")
				}
				if commandWALCleanupRetryableOnly(tc.err) {
					t.Fatal("hard refusal marked retryable")
				}
				if errors.Is(tc.err, commitlog.ErrCorrupt) && !errors.Is(got, commitlog.ErrCorrupt) {
					t.Fatal("corruption lost")
				}
			}
		})
	}
}

// Error matching is observer-controlled and cannot confer cleanup authority.
type cleanupObserverSpoofUnavailable struct{}

func (*cleanupObserverSpoofUnavailable) Error() string { return "observer spoofed unavailable" }
func (*cleanupObserverSpoofUnavailable) Unwrap() error { return rootpublication.ErrResourcePinned }
func (*cleanupObserverSpoofUnavailable) Is(target error) bool {
	return target == errDurableWALCleanupProofUnavailable
}

func TestCommandWALCleanupObserverCannotAuthorizeRetry(t *testing.T) {
	for _, operation := range []string{"explicit", "checkpoint", "actual_checkpoint", "prefix", "close"} {
		for _, kind := range []string{"custom_is", "joined_stale_pin", "replayed_refusal"} {
			t.Run(operation+"/"+kind, func(t *testing.T) {
				dir := t.TempDir()
				d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = d.Close() })
				requireZeroFrontierFixedContainer(t, d)
				if err = d.RotateCommandWALActiveSegment(true); err != nil {
					t.Fatal(err)
				}
				if operation == "actual_checkpoint" {
					if err = d.SetSync([]byte("pending"), []byte("value")); err != nil {
						t.Fatal(err)
					}
				}
				before := map[string][32]byte{}
				entries, err := os.ReadDir(WALDirPath(dir))
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if entry.IsDir() {
						continue
					}
					path := filepath.Join(WALDirPath(dir), entry.Name())
					raw, e := os.ReadFile(path)
					if e != nil {
						t.Fatal(e)
					}
					before[path] = sha256.Sum256(raw)
				}
				var observerErr error = &cleanupObserverSpoofUnavailable{}
				if kind == "joined_stale_pin" {
					observerErr = errors.Join(errDurableWALCleanupProofStale, rootpublication.ErrResourcePinned)
				}
				if kind == "replayed_refusal" {
					observerErr = commandWALCleanupProofRefusal(errDurableWALCleanupProofStale)
				}
				observed := false
				restore := durabilitycut.Install(func(event durabilitycut.Event) error {
					if event.Point == durabilitycut.BeforeWALOrAssetUnlink && event.Resource == durabilitycut.ResourceCommandWAL {
						observed = true
						return observerErr
					}
					return nil
				})
				defer restore()
				switch operation {
				case "explicit":
					err = d.CleanupCommandWALCoveredSegments(false)
				case "checkpoint":
					err = d.CleanupCommandWALCoveredSegmentsAtCheckpoint(false)
				case "actual_checkpoint":
					err = d.Checkpoint()
				case "prefix":
					_, err = d.CleanupCommandWALCoveredPrefix()
				case "close":
					err = d.Close()
				}
				if !observed || err == nil || !errors.Is(err, observerErr) {
					t.Fatalf("observer refusal swallowed: observed=%t err=%v", observed, err)
				}
				for path, digest := range before {
					raw, e := os.ReadFile(path)
					if e != nil || sha256.Sum256(raw) != digest {
						t.Fatalf("observer failure changed %s: %v", path, e)
					}
				}
			})
		}
	}
}

func TestCommandWALCleanupDispositionCannotEscapeHardPhase(t *testing.T) {
	stale := normalizeCommandWALCheckpointCleanupError(commandWALCleanupProofRefusal(errDurableWALCleanupProofStale))
	if !IsCommandWALCleanupProofStale(stale) || !errors.Is(stale, ErrDurableWALCleanupProofStale) {
		t.Fatalf("trusted stale lost: %v", stale)
	}
	for _, err := range []error{ErrDurableWALCleanupProofStale, errors.Join(stale, commitlog.ErrCorrupt), fmt.Errorf("wrapped: %w", stale), hardCommandWALCleanupFailure(stale)} {
		if IsCommandWALCleanupProofStale(err) {
			t.Fatalf("untrusted error authorized retry: %v", err)
		}
	}
	hard := hardCommandWALCleanupFailure(errors.Join(stale, rootpublication.ErrResourcePinned))
	for _, err := range []error{hard, fmt.Errorf("wrapped: %w", hard), errors.Join(hard, ErrVacuumConcurrentMutation)} {
		if !IsCommandWALCleanupHardFailure(err) || compactStorageIndexVacuumTransient(err) {
			t.Fatalf("hard cleanup classified transient: %v", err)
		}
	}
}

func TestCommandWALCleanupSyncObserverCannotAuthorizeRetry(t *testing.T) {
	for _, operation := range []string{"explicit", "checkpoint", "actual_checkpoint", "prefix", "close"} {
		for _, kind := range []string{"custom_is", "joined_stale_pin", "replayed_refusal"} {
			t.Run(operation+"/"+kind, func(t *testing.T) {
				dir := t.TempDir()
				d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = d.Close() })
				requireZeroFrontierFixedContainer(t, d)
				if err = d.RotateCommandWALActiveSegment(true); err != nil {
					t.Fatal(err)
				}
				if operation == "actual_checkpoint" {
					if err = d.SetSync([]byte("pending"), []byte("value")); err != nil {
						t.Fatal(err)
					}
				}
				var observerErr error = &cleanupObserverSpoofUnavailable{}
				if kind == "joined_stale_pin" {
					observerErr = errors.Join(errDurableWALCleanupProofStale, rootpublication.ErrResourcePinned)
				}
				if kind == "replayed_refusal" {
					observerErr = commandWALCleanupProofRefusal(errDurableWALCleanupProofStale)
				}
				observed := false
				restore := durabilitycut.Install(func(event durabilitycut.Event) error {
					if event.Point == durabilitycut.BeforeDeletionDirectorySync && event.Resource == durabilitycut.ResourceCommandWAL {
						observed = true
						return observerErr
					}
					return nil
				})
				defer restore()
				switch operation {
				case "explicit":
					err = d.CleanupCommandWALCoveredSegments(false)
				case "checkpoint":
					err = d.CleanupCommandWALCoveredSegmentsAtCheckpoint(false)
				case "actual_checkpoint":
					err = d.Checkpoint()
				case "prefix":
					_, err = d.CleanupCommandWALCoveredPrefix()
				case "close":
					err = d.Close()
				}
				if !observed || err == nil || !errors.Is(err, observerErr) {
					t.Fatalf("observer refusal swallowed: observed=%t err=%v", observed, err)
				}
			})
		}
	}
}
