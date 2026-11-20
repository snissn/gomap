package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"
)

func enterLimitedWALCleanupProcess(t *testing.T) bool {
	t.Helper()
	const child = "GOMAP_CLEANUP_FD_CHILD"
	if os.Getenv(child) != "1" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, executable, "-test.run=^"+t.Name()+"$", "-test.v")
		command.Env = append(os.Environ(), child+"=1")
		output, err := command.CombinedOutput()
		t.Logf("isolated descriptor limit:\n%s", output)
		if err != nil {
			t.Fatal(err)
		}
		return false
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	limit.Cur = min(limit.Cur, 128)
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	return true
}

func TestCommandWALCleanupFixedPrefixSecondInventoryDescriptorExhaustion(t *testing.T) {
	if !enterLimitedWALCleanupProcess(t) {
		return
	}
	for _, failClose := range []bool{false, true} {
		t.Run(fmt.Sprintf("discard_close_failure=%t", failClose), func(t *testing.T) {
			opts := Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true}
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := database.Close(); err != nil {
					t.Error(err)
				}
			})
			if container := database.commandJournal.WriterBufferStats().Container; container != "fixed" {
				t.Skipf("requires fixed WAL, got %s", container)
			}

			var closed []string
			for _, key := range []string{"covered", "requires-replay"} {
				if err := database.SetSync([]byte(key), []byte("value")); err != nil {
					t.Fatal(err)
				}
				if err := database.rootPublication.coordinator.WaitThrough(context.Background(), database.State().CommitSeq); err != nil {
					t.Fatal(err)
				}
				path, _ := database.commandWALActiveSegmentSnapshot()
				if err := database.RotateCommandWALActiveSegment(true); err != nil {
					t.Fatal(err)
				}
				closed = append(closed, path)
				if key == "covered" {
					if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); !pending || err != nil {
						t.Fatalf("frozen cleanup debt pending=%t: %v", pending, err)
					}
				}
			}
			active, _ := database.commandWALActiveSegmentSnapshot()
			proof, err := database.captureDurableWALCleanupProofV1()
			if err != nil || proof.cleanupThrough != 1 || proof.durableWALLSN != 2 {
				t.Fatalf("expected first predecessor covered and second required for replay: through=%d durable=%d err=%v", proof.cleanupThrough, proof.durableWALLSN, err)
			}

			var padding []*os.File
			releasePadding := func() {
				for _, file := range padding {
					if err := file.Close(); err != nil {
						t.Error(err)
					}
				}
				padding = nil
			}
			defer releasePadding()
			var opened []*os.File
			var scans []string
			database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{
				openPrefix: func(path string) (*os.File, error) {
					file, err := os.Open(path)
					if err == nil {
						opened = append(opened, file)
					}
					return file, err
				},
				beforeSegment: func(path string) {
					scans = append(scans, path)
					switch len(scans) {
					case 2:
						if path != closed[1] || len(opened) != 2 {
							t.Fatalf("second scan started before both predecessors were staged: scans=%q staged=%d", scans, len(opened))
						}
						for _, file := range opened {
							if _, err := file.Seek(0, io.SeekCurrent); err != nil {
								t.Fatal(err)
							}
						}
						if failClose {
							// This file is already retained by the first deletion
							// decision. The discard must report its close error.
							if err := opened[0].Close(); err != nil {
								t.Fatal(err)
							}
						}
						for {
							file, err := os.Open(os.DevNull)
							if errors.Is(err, syscall.EMFILE) {
								break
							}
							if err != nil {
								t.Fatal(err)
							}
							padding = append(padding, file)
						}
						if _, err := os.ReadDir(filepath.Dir(active)); !errors.Is(err, syscall.EMFILE) {
							t.Fatalf("kernel did not refuse a directory reader while both predecessors were staged: %v", err)
						}
						t.Logf("both predecessors staged before real ReadDir EMFILE at the process file limit, padding=%d", len(padding))
					case 3:
						for _, file := range opened {
							if _, err := file.Seek(0, io.SeekCurrent); !errors.Is(err, os.ErrClosed) {
								t.Errorf("locked scan started before staged descriptor %q was released: %v", file.Name(), err)
							}
						}
					}
				},
			}
			defer func() { database.testCommandWALCleanupHooks = nil }()
			removed := database.commandWALCleanupRemoved.Load()
			complete, err := database.CleanupCommandWALCoveredPrefix()
			database.testCommandWALCleanupHooks = nil
			releasePadding()
			if failClose {
				if complete || !errors.Is(err, syscall.EMFILE) || !errors.Is(err, os.ErrClosed) || !slices.Equal(scans, closed) ||
					database.commandWALCleanupRemoved.Load() != removed {
					t.Fatalf("unchecked discard complete=%t scans=%q removed=%d err=%v", complete, scans, database.commandWALCleanupRemoved.Load()-removed, err)
				}
				if _, err := os.Stat(closed[0]); err != nil {
					t.Fatalf("failed discard retired covered predecessor: %v", err)
				}
				complete, err = database.CleanupCommandWALCoveredPrefix()
			} else if !slices.Equal(scans, append(append(slices.Clone(closed), closed...), active)) {
				t.Fatalf("expected one full scan after releasing overlapped descriptors: scans=%q complete=%t err=%v", scans, complete, err)
			}
			if !complete || err != nil || database.commandWALCleanupRemoved.Load() != removed+1 {
				t.Fatalf("completed cleanup=%t removed=%d err=%v", complete, database.commandWALCleanupRemoved.Load()-removed, err)
			}
			if _, err := os.Stat(closed[0]); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("covered predecessor still linked: %v", err)
			}
			for _, path := range []string{closed[1], active} {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("cleanup removed required WAL %q: %v", path, err)
				}
			}
			if err := database.Close(); err != nil {
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
			for _, key := range []string{"covered", "requires-replay"} {
				value, err := reopened.Get([]byte(key))
				if string(value) != "value" || err != nil {
					t.Fatalf("checked reopen %q value=%q err=%v", key, value, err)
				}
			}
		})
	}
}
