package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/lockfile"
)

func TestOriginalStartupCleanupPhysicalRefusalRetainsLock(t *testing.T) {
	dir := t.TempDir()
	operation := errors.New("original startup operation refusal")
	var original *indexGen
	old := testDBOpenHook
	testDBOpenHook = func(d *DB) error {
		original = d.idx.Load()
		if err := original.pager.WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
			return err
		}
		return operation
	}
	defer func() { testDBOpenHook = old }()
	d, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if d != nil || !errors.Is(err, operation) || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("lost original startup operation or physical cleanup refusal: db=%p err=%v", d, err)
	}
	var receipt *OpenRecoveryCleanupError
	if !errors.As(err, &receipt) {
		t.Fatalf("no original fatal Open receipt: %v", err)
	}
	if original == nil || original.metadataCharge == 0 {
		t.Fatal("actual failed original generation refunded")
	}
	other, lockErr := lockfile.Acquire(filepath.Join(dir, "LOCK"))
	if other != nil {
		_ = other.Close()
	}
	if !errors.Is(lockErr, lockfile.ErrLocked) {
		t.Fatalf("startup LOCK escaped actual cleanup refusal: %v", lockErr)
	}
	if receipt.Close() != receipt.Unwrap() || original.close() != &original.closeFailure {
		t.Fatal("fatal observation replayed physical action")
	}
}

func TestOriginalReadOnlyStartupCleanupPhysicalRefusalRetainsLock(t *testing.T) {
	for _, noLock := range []bool{false, true} {
		name := "shared-lock"
		if noLock {
			name = "caller-lock"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			seed, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			if err = seed.SetSync([]byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err = seed.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err = seed.Close(); err != nil {
				t.Fatal(err)
			}
			var original *indexGen
			old := testDBCloseHook
			testDBCloseHook = func(d *DB) {
				original = d.idx.Load()
				if err := original.pager.WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
					t.Errorf("physical fault: %v", err)
				}
			}
			defer func() { testDBCloseHook = old }()
			opts := Options{Dir: dir, ReadOnly: true, CommandWAL: true, IndexPrimaryDirectory: true, DisableBackgroundPrune: true}
			var d *DB
			if noLock {
				lock, lockErr := lockfile.Acquire(filepath.Join(dir, "LOCK"))
				if lockErr != nil {
					t.Fatal(lockErr)
				}
				d, err = openReadOnlyNoLockWithRegistryV6(opts, nil, lock)
			} else {
				d, err = Open(opts)
			}
			if d != nil || !errors.Is(err, os.ErrClosed) {
				t.Fatalf("lost actual read-only startup physical cleanup refusal: %v", err)
			}
			var receipt *OpenRecoveryCleanupError
			if !errors.As(err, &receipt) || receipt.generation != original || original.startupDB == nil ||
				original.startupLock == nil || original.metadataCharge == 0 || original.startupCustody == nil {
				t.Fatalf("lost original startup payload: %v", err)
			}
			if !recoveryCleanupOwnsLockV6(err, original.startupLock) {
				t.Fatal("caller fatal propagation lost LOCK ownership")
			}
			other, lockErr := lockfile.Acquire(filepath.Join(dir, "LOCK"))
			if other != nil {
				_ = other.Close()
			}
			if !errors.Is(lockErr, lockfile.ErrLocked) {
				t.Fatalf("lost actual namespace LOCK: %v", lockErr)
			}
			charged := original.metadata.Bytes()
			if receipt.Close() != receipt.Unwrap() || original.close() != &original.closeFailure || original.metadata.Bytes() != charged {
				t.Fatal("repeat retried or refunded refused cleanup")
			}
		})
	}
}

func TestOriginalStartupSuccessAndCleanRefusalConsumeOnlyOriginalPayload(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	g := d.idx.Load()
	if g.metadataCharge == 0 || g.startupDB != nil || g.startupManager != nil || g.startupLock != nil || g.startupCustody != nil {
		t.Fatal("success left startup-only alias or missed generation admission")
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	if g.metadataCharge != 0 || g.metadata != nil {
		t.Fatal("successful terminal original generation refund missed")
	}
	operation := errors.New("clean startup operation refusal")
	old := testDBOpenHook
	testDBOpenHook = func(d *DB) error { g = d.idx.Load(); return operation }
	defer func() { testDBOpenHook = old }()
	d, err = Open(Options{Dir: dir, DisableBackgroundPrune: true})
	var receipt *OpenRecoveryCleanupError
	if d != nil || !errors.Is(err, operation) || errors.As(err, &receipt) {
		t.Fatalf("clean refusal invented fatal physical debt: %v", err)
	}
	if g.metadataCharge != 0 || g.startupDB != nil || g.startupCustody != nil {
		t.Fatal("successful original cleanup retained/refunded before alias scrub")
	}
	lock, err := lockfile.Acquire(filepath.Join(dir, "LOCK"))
	if err != nil {
		t.Fatalf("successful cleanup retained LOCK: %v", err)
	}
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
}
