package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// This control uses only predecessor APIs/hooks/counters so its unchanged body
// can prove placement on the predecessor without an observation-only code port.
func TestVacuumLateCatchupPreflushDeltaMovesOffGate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	d, err := Open(Options{Dir: t.TempDir(), ChunkSize: 64 << 10, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	if err := d.SetSync([]byte("base"), []byte("seed")); err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	var hookErr error
	d.vacuumPreflushHook = func() error {
		once.Do(func() { hookErr = d.Set([]byte("during-preflush"), []byte("accepted")) })
		return hookErr
	}
	firstPending, gates := -1, 0
	d.vacuumBeforeCutoverHook = func(int) {
		if gates == 0 {
			d.vacuum.mu.Lock()
			firstPending = len(d.vacuum.ops) + len(d.vacuum.ranges)
			d.vacuum.mu.Unlock()
		}
		gates++
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatal(err)
	}
	if gates == 0 || firstPending != 0 {
		t.Fatalf("preflush mutation remains for gated cut: first pending=%d gates=%d want 0/nonzero", firstPending, gates)
	}
	if stats := d.VacuumOnlineStats(); stats.UserTailMutations != 0 {
		t.Fatalf("preflush mutation charged to gated tail: %d want 0", stats.UserTailMutations)
	}
	if value, err := d.Get([]byte("during-preflush")); err != nil || string(value) != "accepted" {
		t.Fatalf("accepted preflush value=%q err=%v", value, err)
	}
}

func TestVacuumLateCatchupConcurrentResidualPreservesOrderAndReopens(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	for _, walOff := range []bool{false, true} {
		for _, invalidate := range []bool{false, true} {
			t.Run(fmt.Sprintf("wal-off=%t/catalog-invalidate=%t", walOff, invalidate), func(t *testing.T) {
				opts := Options{Dir: t.TempDir(), ChunkSize: 64 << 10, DisableBackgroundPrune: true}
				if walOff {
					opts.ResolvedProfile = ProfileNoWALFast
					opts.Durability = DurabilityWALOffRelaxed
				}
				d, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = d.Close() }()
				for _, key := range []string{"ordering/a", "ordering/b", "outside"} {
					if err := d.SetSync([]byte(key), []byte("seed")); err != nil {
						t.Fatal(err)
					}
				}
				catalogRoot, err := publishVacuumSnapshotCollectionVersion(d, 0, 0, 64)
				if err != nil {
					t.Fatal(err)
				}
				catalogRoot, err = publishVacuumSnapshotCollectionVersion(d, catalogRoot, 1, 0)
				if err != nil {
					t.Fatal(err)
				}
				if err := d.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				old := d.AcquireSnapshot()
				if old == nil {
					t.Fatal("missing retained old-generation snapshot")
				}
				defer func() {
					if old != nil {
						_ = old.Close()
					}
				}()
				var preflushOnce sync.Once
				var hookErr error
				d.vacuumPreflushHook = func() error {
					preflushOnce.Do(func() {
						b := d.NewBatch()
						defer func() { _ = b.Close() }()
						hookErr = b.DeleteRange([]byte("ordering/a"), []byte("ordering/c"))
						if hookErr == nil {
							hookErr = b.Set([]byte("ordering/a"), []byte("late-a"))
						}
						if hookErr == nil {
							hookErr = b.Set([]byte("ordering/b"), []byte("late-b"))
						}
						if hookErr == nil {
							hookErr = b.Write()
						}
					})
					return hookErr
				}
				reached, release := make(chan struct{}), make(chan struct{})
				var holdOnce, releaseOnce sync.Once
				d.vacuumLateCatchupHook = func(points, ranges int) {
					if points == 2 && ranges == 1 {
						holdOnce.Do(func() { close(reached); <-release })
					}
				}
				var afterDrainOnce sync.Once
				d.vacuumBeforeCutoverHook = func(int) {
					afterDrainOnce.Do(func() {
						// This genuine publication occurs AFTER coordinator Drain.
						// Its recorder entry must remain in the gated residual.
						hookErr = d.Set([]byte("ordering/d"), []byte("post-drain"))
						if hookErr == nil && invalidate {
							catalogRoot, hookErr = publishVacuumSnapshotCollectionVersion(d, catalogRoot, 2, 0)
						}
						if hookErr != nil {
							return
						}
						// Settle these deliberate post-drain publications using the same
						// coordinator, so the next attempt does not gain independent debt.
						publication := d.rootPublication
						if publication == nil || publication.coordinator == nil {
							hookErr = fmt.Errorf("missing post-drain publication coordinator")
							return
						}
						drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
						hookErr = publication.coordinator.Drain(drainCtx)
						drainCancel()
						if hookErr != nil {
							return
						}
						settled := publication.coordinator.Stats()
						if settled.PendingCommits != 0 || settled.VisibleCommitSeq == 0 || settled.DurableCommitSeq != settled.VisibleCommitSeq {
							hookErr = fmt.Errorf("post-drain publication not settled: pending=%d visible=%d durable=%d", settled.PendingCommits, settled.VisibleCommitSeq, settled.DurableCommitSeq)
						}
					})
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				vacuumDone, writerDone := make(chan error, 1), make(chan error, 1)
				var vacuumJoined, writerStarted, writerJoined bool
				defer func() {
					cancel()
					releaseOnce.Do(func() { close(release) })
					if !vacuumJoined {
						select {
						case <-vacuumDone:
						case <-time.After(5 * time.Second):
							t.Error("vacuum did not join after releasing late catch-up")
						}
					}
					if writerStarted && !writerJoined {
						select {
						case <-writerDone:
						case <-time.After(5 * time.Second):
							t.Error("writer did not join after releasing late catch-up")
						}
					}
				}()
				go func() { vacuumDone <- d.VacuumIndexOnline(ctx) }()
				waitVacuumTestSignal(t, reached, "actual detached late private delta")
				if !d.vacuum.Active() {
					t.Fatal("recorder stopped during private catch-up")
				}
				writerStarted = true
				go func() {
					b := d.NewBatch()
					defer func() { _ = b.Close() }()
					err := b.DeleteRange([]byte("ordering/a"), []byte("ordering/b"))
					if err == nil {
						err = b.Set([]byte("ordering/b"), []byte("residual-b"))
					}
					if err == nil {
						err = b.Set([]byte("ordering/c"), []byte("residual-c"))
					}
					if err == nil {
						err = b.Write()
					}
					writerDone <- err
				}()
				select {
				case err := <-writerDone:
					writerJoined = true
					if err != nil {
						t.Fatalf("writer during detached private catch-up: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("public writer blocked by private late catch-up")
				}
				releaseOnce.Do(func() { close(release) })
				select {
				case err := <-vacuumDone:
					vacuumJoined = true
					if err != nil {
						t.Fatalf("vacuum: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("vacuum did not finish after late catch-up release")
				}
				if hookErr != nil {
					t.Fatal(hookErr)
				}
				stats := d.VacuumOnlineStats()
				if stats.LateCatchupMutations != 3 || stats.LateCatchupPointMutations != 2 || stats.LateCatchupRangeMutations != 1 {
					t.Fatalf("detached late mutations=%d/%d/%d want 3/2/1", stats.LateCatchupMutations, stats.LateCatchupPointMutations, stats.LateCatchupRangeMutations)
				}
				if stats.UserTailMutations != 4 || stats.UserTailPointMutations != 3 || stats.UserTailRangeMutations != 1 {
					t.Fatalf("gated residual=%d/%d/%d want 4/3/1", stats.UserTailMutations, stats.UserTailPointMutations, stats.UserTailRangeMutations)
				}
				if stats.DeferredCutovers != 1 || stats.RecoverableSetRecaptures != 1 || stats.LateCatchupPasses != 2 {
					t.Fatalf("catch-up lost bounded basis/capability recapture: %+v", stats)
				}
				assertVacuumLateCatchupState(t, d)
				for _, key := range []string{"ordering/a", "ordering/b", "outside"} {
					if got, err := old.Get([]byte(key)); err != nil || string(got) != "seed" {
						t.Fatalf("retained old key %q=%q err=%v", key, got, err)
					}
				}
				wantVersion := 1
				if invalidate {
					wantVersion = 2
				}
				verifyVacuumSnapshotCollectionVersion(t, d, catalogRoot, wantVersion)
				if err := old.Close(); err != nil {
					t.Fatal(err)
				}
				old = nil
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = reopened.Close() }()
				assertVacuumLateCatchupState(t, reopened)
				verifyVacuumSnapshotCollectionVersion(t, reopened, 0, wantVersion)
			})
		}
	}
}

func assertVacuumLateCatchupState(t *testing.T, d *DB) {
	t.Helper()
	if present, err := d.Has([]byte("ordering/a")); err != nil || present {
		t.Fatalf("late put followed by residual range: present=%t err=%v", present, err)
	}
	for key, want := range map[string]string{"ordering/b": "residual-b", "ordering/c": "residual-c", "ordering/d": "post-drain", "outside": "seed"} {
		if got, err := d.Get([]byte(key)); err != nil || string(got) != want {
			t.Fatalf("range-before-later-put key %q=%q err=%v want=%q", key, got, err, want)
		}
	}
}

func TestVacuumLateCatchupCancellationDiscardsPrivatePlanAndReopens(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	opts := Options{Dir: t.TempDir(), ChunkSize: 64 << 10, DisableBackgroundPrune: true}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	if err := d.SetSync([]byte("seed"), []byte("retained")); err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	original := d.idx.Load()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.vacuumPreflushHook = func() error { return d.Set([]byte("accepted-before-cancel"), []byte("old-generation")) }
	var detached bool
	d.vacuumLateCatchupHook = func(points, ranges int) {
		if points == 1 && ranges == 0 {
			detached = true
			cancel()
		}
	}
	if err := d.VacuumIndexOnline(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("vacuum cancellation=%v", err)
	}
	if !detached || d.idx.Load() != original || d.vacuum.Active() || d.vacuumInProgress.Load() {
		t.Fatal("canceled detached plan changed generation or retained active ownership")
	}
	if stats := d.VacuumOnlineStats(); !stats.Canceled || stats.WorkCompleted || stats.LateCatchupMutations != 1 {
		t.Fatalf("canceled plan stats=%+v", stats)
	}
	for _, name := range []string{indexNewFileName, indexReadyFileName, indexBakFileName} {
		if _, err := os.Stat(filepath.Join(opts.Dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("private artifact %s remains: %v", name, err)
		}
	}
	if got, err := d.Get([]byte("accepted-before-cancel")); err != nil || string(got) != "old-generation" {
		t.Fatalf("accepted old-generation value=%q err=%v", got, err)
	}
	d.vacuumPreflushHook, d.vacuumLateCatchupHook = nil, nil
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer retryCancel()
	if err := d.VacuumIndexOnline(retryCtx); err != nil {
		t.Fatalf("fresh public vacuum after canceled attempt: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	for key, want := range map[string]string{"seed": "retained", "accepted-before-cancel": "old-generation"} {
		if got, err := reopened.Get([]byte(key)); err != nil || string(got) != want {
			t.Fatalf("reopened %s=%q err=%v", key, got, err)
		}
	}
}
