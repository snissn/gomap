package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func openDeferredWALStreamForTest(t *testing.T, closed int) (*DB, Options, []string, string, map[string]string) {
	t.Helper()
	opts := Options{
		Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true,
		Durability: DurabilityWALOnRelaxed, ResolvedProfile: ProfileCommandWALRelaxed,
		MemtableMode: "cow_btree", FreshRelaxedStandaloneWALStream: true,
	}
	opts.ValueLog.PointerThreshold = 4096
	database, err := OpenForNativeSemanticOwner(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})

	var closedPaths []string
	want := make(map[string]string)
	for i := range closed {
		if got := database.commandJournal.WriterBufferStats().Container; got != "stream" {
			t.Skipf("fixture requires a stream writer, got %q", got)
		}
		key := fmt.Sprintf("covered-%d", i)
		if err := database.SetSync([]byte(key), []byte("before")); err != nil {
			t.Fatal(err)
		}
		want[key] = "before"
		path, _ := database.commandWALActiveSegmentSnapshot()
		if err := database.RotateCommandWALActiveSegment(true); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil || info.Size() == 0 {
			t.Fatalf("closed stream has no recorded extent: path=%q info=%v error=%v", path, info, err)
		}
		closedPaths = append(closedPaths, path)
	}
	if err := database.rootPublication.coordinator.WaitThrough(context.Background(), database.State().CommitSeq); err != nil {
		t.Fatal(err)
	}
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	pending, err := database.PrepareCommandWALCoveredPrefixCleanup()
	if err != nil || !pending {
		t.Fatalf("deferred cleanup pending=%t error=%v", pending, err)
	}
	activePath, _ := database.commandWALActiveSegmentSnapshot()
	if slices.Contains(closedPaths, activePath) {
		t.Fatalf("fixture did not install an independent active stream: %v", activePath)
	}
	if got := database.commandJournal.WriterBufferStats().Container; got != "stream" {
		t.Fatalf("active journal switched containers: %q", got)
	}

	return database, opts, closedPaths, activePath, want
}

func TestCommandWALDeferredStreamClosedScanAllowsAppend(t *testing.T) {
	t.Run("closed", func(t *testing.T) { testDeferredStreamCleanupOverlap(t, 1, false, false) })
	t.Run("active", func(t *testing.T) { testDeferredStreamCleanupOverlap(t, 2, true, false) })
	t.Run("rotate", func(t *testing.T) { testDeferredStreamCleanupOverlap(t, 2, false, true) })
}

func testDeferredStreamCleanupOverlap(t *testing.T, closed int, gateActive, rotate bool) {
	t.Helper()
	database, opts, closedPaths, activePath, want := openDeferredWALStreamForTest(t, closed)
	want["suffix"] = "during"
	gatedPath, wantOverlap := closedPaths[0], !gateActive
	if gateActive {
		gatedPath = activePath
		if err := database.SetSync([]byte("active-before"), []byte("retained")); err != nil {
			t.Fatal(err)
		}
		want["active-before"] = "retained"
	}

	scanned := testCommandWALCleanupScanOverlap(t, database, cleanupDeferredCommandWALPrefixForTest, gatedPath, wantOverlap, func() error {
		if err := database.SetSync([]byte("suffix"), []byte("during")); err != nil {
			return err
		}
		if rotate {
			return database.RotateCommandWALActiveSegment(true)
		}
		return nil
	})
	expected := append(slices.Clone(closedPaths), activePath)
	if rotate {
		finalActive, _ := database.commandWALActiveSegmentSnapshot()
		if finalActive == activePath {
			t.Fatal("foreground rotation did not install a successor during scan")
		}
		expected = append(expected, finalActive)
	}
	if !slices.Equal(scanned, expected) {
		t.Fatalf("closed and active scans=%q, want %q", scanned, expected)
	}
	for _, path := range closedPaths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("covered file %q not reclaimed: %v", path, err)
		}
	}
	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("active stream must remain linked: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenForNativeSemanticOwner(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	for key, expected := range want {
		value, err := reopened.Get([]byte(key))
		if err != nil || string(value) != expected {
			t.Fatalf("reopened %s=%q err=%v", key, value, err)
		}
	}
}

func TestCommandWALDeferredStreamRechecksClosedExtent(t *testing.T) {
	for _, when := range []string{"after_capture", "before_unlink"} {
		t.Run(when, func(t *testing.T) {
			database, _, closed, _, _ := openDeferredWALStreamForTest(t, 2)
			target := closed[0]
			info, err := os.Stat(target)
			if err != nil {
				t.Fatal(err)
			}
			changed := false
			changeExtentWitness := func() error {
				changed = true
				return os.Chtimes(target, info.ModTime(), info.ModTime().Add(time.Hour))
			}
			var changeErr error
			if when == "after_capture" {
				database.testCommandWALCleanupBeforeSegmentScanHook = func(path string) {
					if path == target {
						changeErr = changeExtentWitness()
					}
				}
				defer func() { database.testCommandWALCleanupBeforeSegmentScanHook = nil }()
			} else {
				restore := durabilitycut.Install(func(event durabilitycut.Event) error {
					if event.Resource == durabilitycut.ResourceCommandWAL &&
						event.Point == durabilitycut.BeforeWALOrAssetUnlink && event.Path == target {
						changeErr = changeExtentWitness()
						return changeErr
					}
					return nil
				})
				defer restore()
			}
			complete, err := database.CleanupCommandWALCoveredPrefix()
			if !changed || changeErr != nil || complete || !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("changed=%t changeErr=%v completed=%t cleanup=%v", changed, changeErr, complete, err)
			}
			for _, path := range closed {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("failed proof unlinked %s: %v", path, err)
				}
			}
		})
	}
}
