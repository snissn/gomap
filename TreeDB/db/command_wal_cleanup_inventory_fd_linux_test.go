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

func runWALCleanupDescriptorLimitChild(t *testing.T) (uint64, bool) {
	t.Helper()
	const child = "GOMAP_CLEANUP_INVENTORY_FD_CHILD"
	if os.Getenv(child) != "1" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, executable, "-test.run=^"+t.Name()+"$", "-test.v")
		command.Env = append(os.Environ(), child+"=1")
		output, err := command.CombinedOutput()
		t.Logf("isolated descriptor limit:\n%s", output)
		if err != nil {
			t.Fatal(err)
		}
		return 0, false
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	limit.Cur = min(limit.Cur, 128)
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	return limit.Cur, true
}

func TestCommandWALCleanupFixedPrefixSecondInventoryDescriptorExhaustion(t *testing.T) {
	limit, child := runWALCleanupDescriptorLimitChild(t)
	if !child {
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
						t.Logf("both predecessors staged before real ReadDir EMFILE at RLIMIT_NOFILE=%d, padding=%d", limit, len(padding))
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

func TestCommandWALCleanupSplitStreamSecondInventoryDescriptorExhaustion(t *testing.T) {
	limit, child := runWALCleanupDescriptorLimitChild(t)
	if !child {
		return
	}
	database, proof, sealed := prepareSplitStreamCleanupTest(t, 2, 2)
	var padding []*os.File
	defer func() {
		for _, file := range padding {
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	var visited []string
	scan := newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, database.walMaxSegmentBytes, func(path string) {
		visited = append(visited, path)
		if len(visited) != len(sealed) {
			return
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
		if _, err := os.ReadDir(filepath.Dir(path)); !errors.Is(err, syscall.EMFILE) {
			t.Fatalf("a new inventory reader should be refused at RLIMIT_NOFILE=%d: %v", limit, err)
		}
		t.Logf("both covered descriptors retained under real ReadDir EMFILE at RLIMIT_NOFILE=%d", limit)
	}, &proof.journal)
	attempted, scanErr := database.scanSplitCommandWALCleanupPrefix(proof, scan, os.Open, 1)
	decisions, err := scan.finish(scanErr)
	defer closeCommandWALCleanupDecisions(decisions)
	if !attempted || err != nil || !slices.Equal(visited, append(slices.Clone(sealed), proof.journal.ActivePath)) {
		t.Fatalf("cleanup attempted=%t visited=%q err=%v", attempted, visited, err)
	}
	if len(decisions) < len(sealed) || decisions[0].file == nil || decisions[1].file == nil {
		t.Fatalf("inventory refresh did not retain both covered decisions: %+v", decisions)
	}
}

func TestCommandWALCleanupSplitStreamRotationDescriptorExhaustion(t *testing.T) {
	limit, child := runWALCleanupDescriptorLimitChild(t)
	if !child {
		return
	}
	database := &DB{dir: t.TempDir(), commandWAL: true, durability: DurabilityDurable}
	for seq := uint64(1); seq <= 2; seq++ {
		writeCommandWALFrame(t, database.dir, seq, seq)
	}
	installCommandWALCleanupTwoRootRuntimeForTest(database, 1, 1)
	database.commandWALDurableLSN.Store(2)
	opts := commandWALCleanupJournalOptionsForTest(t, database, 2)
	opts.PreferExt4Stream = true
	installCommandWALCleanupJournalOptionsForTest(t, database, opts, true)
	proof, err := database.captureDurableWALCleanupProofV1()
	if err != nil {
		t.Fatal(err)
	}
	if !proof.journal.Ext4Stream {
		t.Skip("requires an ext4 stream")
	}

	var padding []*os.File
	t.Cleanup(func() {
		for _, file := range padding {
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		}
	})
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
	const slots = 4
	if len(padding) < slots {
		t.Fatalf("RLIMIT_NOFILE=%d left only %d available slots", limit, len(padding))
	}
	for _, file := range padding[len(padding)-slots:] {
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	padding = padding[:len(padding)-slots]

	var opened []*os.File
	var visited []string
	scan := newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, database.walMaxSegmentBytes, func(path string) {
		visited = append(visited, path)
		if len(visited) != 1 {
			return
		}
		if !database.commandJournal.AppendLockAvailableForTesting() {
			t.Fatal("rotation must run while detached readers are staged")
		}
		if err := database.RotateCommandWALActiveSegment(false); err != nil {
			t.Fatalf("public rotation with %d free slots before staging: %v", slots, err)
		}
		checkClosedSplitStreamReaders(t, opened, 2)
	}, &proof.journal)
	attempted, err := database.scanSplitCommandWALCleanupPrefix(proof, scan, func(path string) (*os.File, error) {
		file, err := os.Open(path)
		if err == nil {
			opened = append(opened, file)
		}
		return file, err
	}, 1)
	if attempted || err != nil || len(visited) != 1 {
		t.Fatalf("rotation did not return staged cleanup to locked scan: attempted=%t visited=%q err=%v", attempted, visited, err)
	}
	decisions, err := scan.finish(database.commandJournal.WithCleanupScan(proof.journal, func() error {
		return scan.scanLockedDirectory(database.dir)
	}))
	defer closeCommandWALCleanupDecisions(decisions)
	successor, _ := database.commandWALActiveSegmentSnapshot()
	if err != nil || len(decisions) != 2 || !decisions[0].Covered || decisions[1].Covered ||
		decisions[0].file == nil || decisions[1].file != nil || successor == proof.journal.ActivePath {
		t.Fatalf("locked retry did not preserve the covered and replay ownership after rotation: %+v successor=%q err=%v", decisions, successor, err)
	}
	t.Logf("public rotation and locked retry succeeded with %d free descriptors before staging at RLIMIT_NOFILE=%d", slots, limit)
}
