//go:build linux

package db

import (
	"bytes"
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestCommandWALDeferredExt4StreamCleanup(t *testing.T) {
	dir := t.TempDir()
	probe, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ext4 := commitlog.IsExt4WALFilesystem(probe)
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	if !ext4 {
		t.Skip("requires ext4")
	}
	opts := Options{
		Dir: dir, CommandWAL: true, DisableBackgroundPrune: true,
		ResolvedProfile: ProfileCommandWALRelaxed, Durability: DurabilityWALOnRelaxed,
		MemtableMode: "cow_btree", FreshRelaxedStandaloneWALStream: true,
	}
	opts.ValueLog.PointerThreshold = 4096
	database, err := OpenForNativeSemanticOwner(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	if actual := database.commandJournal.WriterBufferStats().Container; actual != "stream" {
		t.Fatalf("ext4 relaxed fixture opened %s WAL", actual)
	}

	key := []byte("covered")
	value := bytes.Repeat([]byte{'x'}, 3<<10)
	closedPath, _ := database.commandWALActiveSegmentSnapshot()
	for i := 0; i < 32768; i++ {
		if err := database.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.SetSync(key, []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := database.RotateCommandWALActiveSegment(true); err != nil {
		t.Fatal(err)
	}
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); err != nil || !pending {
		t.Fatalf("expected deferred cleanup debt: pending=%t err=%v", pending, err)
	}
	if err := database.SetSync([]byte("active-prefix"), []byte("still live")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(closedPath)
	if err != nil || info.Size() < minDetachedClosedWALBytes {
		t.Fatalf("fixture did not create a large closed stream: %v, %v", info, err)
	}
	activePath, activeBytes := database.commandWALActiveSegmentSnapshot()
	if activePath == closedPath || activeBytes <= 0 || activeBytes > maxDetachedActiveWALBytes {
		t.Fatalf("unexpected active stream %s: %d", activePath, activeBytes)
	}

	// Replace the name after validation. It must not be possible to unlink
	// the replacement, even though the retained original descriptor is valid.
	displaced := closedPath + ".displaced"
	var replacementErr error
	database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{afterScan: func() {
		replacementErr = os.Rename(closedPath, displaced)
		if replacementErr == nil {
			replacementErr = os.WriteFile(closedPath, []byte("replacement"), 0600)
		}
	}}
	complete, cleanupErr := database.CleanupCommandWALCoveredPrefix()
	database.testCommandWALCleanupHooks = nil
	if replacementErr != nil {
		t.Fatal(replacementErr)
	}
	if complete || !errors.Is(cleanupErr, ErrRecoveryRequired) {
		t.Errorf("replacement cleanup complete=%t, err=%v", complete, cleanupErr)
	}
	if got, err := os.ReadFile(closedPath); err != nil || string(got) != "replacement" {
		t.Errorf("replacement lost or changed: %q, %v", got, err)
	}
	if err := os.Rename(displaced, closedPath); err != nil {
		t.Fatal(err)
	}
	if t.Failed() {
		return
	}

	// A late same-inode symlink passes descriptor-only identity checks.
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point != durabilitycut.BeforeWALOrAssetUnlink || event.Path != closedPath {
			return nil
		}
		if err := os.Rename(closedPath, displaced); err != nil {
			return err
		}
		return os.Symlink(displaced, closedPath)
	})
	complete, cleanupErr = database.CleanupCommandWALCoveredPrefix()
	restore()
	if complete || !errors.Is(cleanupErr, ErrRecoveryRequired) {
		t.Errorf("late symlink cleanup complete=%t, err=%v", complete, cleanupErr)
	}
	if linked, err := os.Readlink(closedPath); err != nil || linked != displaced {
		t.Errorf("late symlink lost or changed: %q, %v", linked, err)
	}
	if err := os.Rename(displaced, closedPath); err != nil {
		t.Fatal(err)
	}
	if t.Failed() {
		return
	}

	held := database.AcquireSnapshot()
	if held == nil {
		t.Fatal("no held snapshot")
	}
	defer held.Close()
	scanned := testCommandWALCleanupScanOverlap(t, database, cleanupDeferredCommandWALPrefixForTest, closedPath, true, func() error {
		if err := database.SetSync(key, []byte("during")); err != nil {
			return err
		}
		return database.RotateCommandWALActiveSegment(true)
	})
	if !slices.Equal(scanned, []string{activePath, closedPath}) {
		t.Fatalf("scans=%q, want active prefix followed by detached predecessor", scanned)
	}
	if _, err := os.Stat(closedPath); !os.IsNotExist(err) {
		t.Fatalf("covered predecessor not reclaimed: %v", err)
	}
	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("original active stream must remain after concurrent rotation: %v", err)
	}
	if got, err := held.Get(key); err != nil || string(got) != "before" {
		t.Fatalf("held key=%q, err=%v", got, err)
	}
	if got, err := database.Get(key); err != nil || string(got) != "during" {
		t.Fatalf("current key=%q, err=%v", got, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	reopened, err := OpenForNativeSemanticOwner(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	if got, err := reopened.Get([]byte("active-prefix")); err != nil || string(got) != "still live" {
		t.Fatalf("reopened active prefix=%q, err=%v", got, err)
	}
	if got, err := reopened.Get(key); err != nil || string(got) != "during" {
		t.Fatalf("reopened key=%q, err=%v", got, err)
	}
}
