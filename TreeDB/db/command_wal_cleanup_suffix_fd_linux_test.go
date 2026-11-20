package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCommandWALCleanupFixedSuffixDescriptorExhaustion(t *testing.T) {
	const childEnv = "TREEDB_TEST_FIXED_SUFFIX_FD"
	if child := os.Getenv(childEnv); child != "" {
		budgetText, closeFailure, _ := strings.Cut(child, "/")
		free, err := strconv.Atoi(budgetText)
		if err != nil {
			t.Fatal(err)
		}
		runCommandWALCleanupFixedSuffixFDChild(t, free, closeFailure == "close_failure")
		return
	}
	for _, child := range []string{"10", "11", "10/close_failure"} {
		t.Run(child, func(t *testing.T) {
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestCommandWALCleanupFixedSuffixDescriptorExhaustion$", "-test.v")
			cmd.Env = append(os.Environ(), childEnv+"="+child)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("FD subprocess: %v\n%s", err, output)
			}
			t.Logf("%s", output)
		})
	}
}

func runCommandWALCleanupFixedSuffixFDChild(t *testing.T, free int, closeFailure bool) {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	limit.Cur = min(limit.Cur, 160)
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	opts := Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if container := database.commandJournal.WriterBufferStats().Container; container != "fixed" {
		t.Skipf("requires fixed WAL, got %s", container)
	}

	// One more sealed file than the staging budget forces the locked suffix
	// to open another covered file before it attempts to open the active WAL.
	var sealed []string
	for i := range maxOpenClosedWALCleanupSegments + 1 {
		if err := database.SetSync([]byte(fmt.Sprintf("covered-%d", i)), []byte("before")); err != nil {
			t.Fatal(err)
		}
		path, _ := database.commandWALActiveSegmentSnapshot()
		sealed = append(sealed, path)
		if err := database.RotateCommandWALActiveSegment(true); err != nil {
			t.Fatal(err)
		}
	}
	active, _ := database.commandWALActiveSegmentSnapshot()
	if err := database.rootPublication.coordinator.WaitThrough(t.Context(), database.State().CommitSeq); err != nil {
		t.Fatal(err)
	}
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); !pending || err != nil {
		t.Fatalf("prepare deferred cleanup: pending=%t err=%v", pending, err)
	}

	var pads []*os.File
	releasePads := func(keep int) {
		for _, pad := range pads[keep:] {
			if err := pad.Close(); err != nil {
				t.Error(err)
			}
		}
		pads = pads[:keep]
	}
	t.Cleanup(func() { releasePads(0) })
	for {
		pad, err := os.Open(os.DevNull)
		if errors.Is(err, syscall.EMFILE) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		pads = append(pads, pad)
	}
	if len(pads) < free {
		t.Fatalf("unable to reserve %d descriptors with limit %d", free, limit.Cur)
	}
	releasePads(len(pads) - free)

	var staged []*os.File
	var scanned []string
	database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{
		openPrefix: func(path string) (*os.File, error) {
			file, err := os.Open(path)
			if err == nil {
				staged = append(staged, file)
			}
			return file, err
		},
		beforeSegment: func(path string) {
			scanned = append(scanned, path)
			switch len(scanned) {
			case 1:
				if !database.commandJournal.AppendLockAvailableForTesting() {
					t.Fatal("first scan still holds append exclusion")
				}
				prefix, _, err := database.CaptureCommandWALPrefix()
				if err != nil {
					t.Fatal(err)
				}
				// The capture caches the active WAL descriptor for the remainder
				// of this cleanup attempt. Its public Sync must still succeed.
				if _, err := prefix.Sync(); err != nil {
					t.Fatal(err)
				}
			case 2:
				if closeFailure {
					// The first file has already entered the deletion decisions.
					// Make its eventual checked close fail, preserving the budget.
					if err := staged[0].Close(); err != nil {
						t.Fatal(err)
					}
					pad, err := os.Open(os.DevNull)
					if err != nil {
						t.Fatal(err)
					}
					pads = append(pads, pad)
				}
			}
		},
	}
	complete, cleanupErr := database.CleanupCommandWALCoveredPrefix()
	database.testCommandWALCleanupHooks = nil
	releasePads(0)

	wantComplete := free == 11
	wantScanned := len(sealed)
	var wantRemoved uint64
	if wantComplete {
		wantScanned++
		wantRemoved = uint64(len(sealed))
	}
	if len(staged) != maxOpenClosedWALCleanupSegments || len(scanned) != wantScanned {
		t.Fatalf("unexpected scan path: staged=%d scanned=%q", len(staged), scanned)
	}
	if removed := database.commandWALCleanupRemoved.Load(); removed != wantRemoved {
		t.Errorf("removed=%d, want %d", removed, wantRemoved)
	}
	if complete != wantComplete || (!closeFailure && cleanupErr != nil) ||
		(closeFailure && (!errors.Is(cleanupErr, syscall.EMFILE) || !errors.Is(cleanupErr, os.ErrClosed))) {
		t.Errorf("cleanup complete=%t err=%v, want complete=%t close failure=%t", complete, cleanupErr, wantComplete, closeFailure)
	}
	for _, path := range sealed {
		_, err := os.Stat(path)
		if (wantComplete && !errors.Is(err, os.ErrNotExist)) || (!wantComplete && err != nil) {
			t.Errorf("covered segment %s after first try: %v", path, err)
		}
	}

	if complete, err := database.CleanupCommandWALCoveredPrefix(); !complete || err != nil {
		t.Fatalf("retry cleanup complete=%t err=%v", complete, err)
	}
	if removed := database.commandWALCleanupRemoved.Load(); removed != uint64(len(sealed)) {
		t.Errorf("removed=%d after retry, want %d", removed, len(sealed))
	}
	for _, path := range sealed {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("covered segment %s after retry: %v", path, err)
		}
	}
	if _, err := os.Stat(active); err != nil {
		t.Fatalf("active WAL disappeared: %v", err)
	}
	if err := database.SetSync([]byte("after"), []byte("after")); err != nil {
		t.Fatal(err)
	}
	err = database.Close()
	database = nil
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	for i := range sealed {
		key := fmt.Sprintf("covered-%d", i)
		if value, err := reopened.Get([]byte(key)); string(value) != "before" || err != nil {
			t.Errorf("reopen %s value=%q err=%v", key, value, err)
		}
	}
	if value, err := reopened.Get([]byte("after")); string(value) != "after" || err != nil {
		t.Errorf("reopen after value=%q err=%v", value, err)
	}
}
