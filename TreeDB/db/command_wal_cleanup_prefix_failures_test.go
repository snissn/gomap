package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func seedCommandWALCleanupFixedPrefix(t *testing.T) (*DB, Options, []string, string) {
	t.Helper()
	opts := Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if container := database.commandJournal.WriterBufferStats().Container; container != "fixed" {
		t.Skipf("requires fixed WAL, got %s", container)
	}
	var closed []string
	for i := range 2 {
		if err := database.SetSync([]byte(fmt.Sprintf("covered-%d", i)), []byte("before")); err != nil {
			t.Fatal(err)
		}
		path, _ := database.commandWALActiveSegmentSnapshot()
		if err := database.RotateCommandWALActiveSegment(true); err != nil {
			t.Fatal(err)
		}
		closed = append(closed, path)
	}
	active, _ := database.commandWALActiveSegmentSnapshot()
	if slices.Contains(closed, active) {
		t.Fatal("fixture did not rotate both predecessors")
	}
	if err := database.rootPublication.coordinator.WaitThrough(context.Background(), database.State().CommitSeq); err != nil {
		t.Fatal(err)
	}
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); !pending || err != nil {
		t.Fatalf("fixture did not leave deferred cleanup debt: pending=%t err=%v", pending, err)
	}
	return database, opts, closed, active
}

func requireCommandWALCleanupFixedPrefixReplay(t *testing.T, database *DB, opts Options, closed []string, active string) {
	t.Helper()
	for _, path := range closed {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("covered predecessor %q must be retired: %v", path, err)
		}
	}
	if _, err := os.Stat(active); err != nil {
		t.Fatalf("active WAL must stay linked: %v", err)
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
	for i := range 2 {
		value, err := reopened.Get([]byte(fmt.Sprintf("covered-%d", i)))
		if string(value) != "before" || err != nil {
			t.Fatalf("reopen %d value=%q err=%v", i, value, err)
		}
	}
}

func TestCommandWALCleanupFixedPrefixDescriptorExhaustion(t *testing.T) {
	for _, tc := range []struct {
		name     string
		openErr  error
		fallback bool
	}{
		{"process_limit", syscall.EMFILE, true},
		{"system_limit", syscall.ENFILE, true},
		{"permission", os.ErrPermission, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, opts, closed, active := seedCommandWALCleanupFixedPrefix(t)
			openCalls := 0
			var first *os.File
			var scans []string
			database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{
				openPrefix: func(path string) (*os.File, error) {
					openCalls++
					if path == closed[1] {
						return nil, &os.PathError{Op: "open", Path: path, Err: tc.openErr}
					}
					var err error
					first, err = os.Open(path)
					return first, err
				},
				beforeSegment: func(path string) {
					if len(scans) == 0 {
						if _, err := first.Seek(0, io.SeekCurrent); !errors.Is(err, os.ErrClosed) {
							t.Errorf("staged descriptor was not closed before restart: %v", err)
						}
					}
					scans = append(scans, path)
				},
			}
			removed := database.commandWALCleanupRemoved.Load()
			complete, err := database.CleanupCommandWALCoveredPrefix()
			database.testCommandWALCleanupHooks = nil
			if openCalls != 2 || first == nil {
				t.Fatalf("preopen fixture didn't stage one descriptor before refusal: calls=%d first=%v", openCalls, first)
			}
			if _, closeErr := first.Seek(0, io.SeekCurrent); !errors.Is(closeErr, os.ErrClosed) {
				t.Fatalf("staged descriptor not closed: %v", closeErr)
			}
			if tc.fallback {
				if err != nil || !complete || !slices.Equal(scans, append(closed, active)) {
					t.Fatalf("snapshot fallback complete=%t scans=%q err=%v", complete, scans, err)
				}
			} else {
				if complete || !errors.Is(err, tc.openErr) || len(scans) != 0 ||
					database.commandWALCleanupRemoved.Load() != removed {
					t.Fatalf("nonexhaustion open refusal complete=%t scans=%q err=%v", complete, scans, err)
				}
				for _, path := range closed {
					if _, err := os.Stat(path); err != nil {
						t.Fatalf("refusal lost covered predecessor %q: %v", path, err)
					}
				}
				if err := cleanupDeferredCommandWALPrefixForTest(database); err != nil {
					t.Fatal(err)
				}
			}
			requireCommandWALCleanupFixedPrefixReplay(t, database, opts, closed, active)
		})
	}
}

func TestCommandWALCleanupFixedPrefixRestartChecksDiscardClose(t *testing.T) {
	for _, failClose := range []bool{false, true} {
		t.Run(fmt.Sprintf("close_failure=%t", failClose), func(t *testing.T) {
			database, opts, closed, active := seedCommandWALCleanupFixedPrefix(t)
			unsupported := filepath.Join(filepath.Dir(active), "value-l1-000001.log")
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
					if path == closed[1] && len(scans) == 2 {
						// An alternate-format name makes the second inventory
						// choose the original scanner, which ignores this file.
						if err := os.WriteFile(unsupported, nil, 0600); err != nil {
							t.Fatal(err)
						}
						if failClose {
							// The first covered file is already in the original
							// decision list. Force its eventual Close to fail.
							if err := opened[0].Close(); err != nil {
								t.Fatal(err)
							}
						}
					}
				},
			}
			removed := database.commandWALCleanupRemoved.Load()
			namespaceDirty := database.commandWALCleanupNamespaceDirty.Load()
			complete, err := database.CleanupCommandWALCoveredPrefix()
			database.testCommandWALCleanupHooks = nil
			if len(opened) != 2 {
				t.Fatalf("did not preopen both original predecessors: %d", len(opened))
			}
			for _, file := range opened {
				if _, err := file.Seek(0, io.SeekCurrent); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("discard retained a staged descriptor: file=%q err=%v", file.Name(), err)
				}
			}
			if failClose {
				if complete || !errors.Is(err, os.ErrClosed) || !slices.Equal(scans, closed) ||
					database.commandWALCleanupRemoved.Load() != removed ||
					database.commandWALCleanupNamespaceDirty.Load() != namespaceDirty {
					t.Fatalf("discard close failure complete=%t scans=%q err=%v", complete, scans, err)
				}
				for _, path := range closed {
					if _, err := os.Stat(path); err != nil {
						t.Fatalf("discard close failure lost covered predecessor %q: %v", path, err)
					}
				}
			} else if !complete || err != nil || !slices.Equal(scans, append(append(slices.Clone(closed), closed...), active)) {
				t.Fatalf("clean discard restart complete=%t scans=%q err=%v", complete, scans, err)
			}
			if err := os.Remove(unsupported); err != nil {
				t.Fatal(err)
			}
			if failClose {
				if err := cleanupDeferredCommandWALPrefixForTest(database); err != nil {
					t.Fatal(err)
				}
			}
			requireCommandWALCleanupFixedPrefixReplay(t, database, opts, closed, active)
		})
	}
}

func TestCommandWALCleanupFixedPrefixFinishChecksDiscardClose(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"stale_snapshot", commitlog.ErrCommandWALCleanupSnapshotStale, commitlog.ErrCommandWALCleanupSnapshotStale},
		{"missing_lineage", nil, errDurableWALCleanupProofUnavailable},
	} {
		for _, failClose := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/close_failure=%t", tc.name, failClose), func(t *testing.T) {
				file, err := os.CreateTemp(t.TempDir(), "decision")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = file.Close() })
				if failClose {
					if err := file.Close(); err != nil {
						t.Fatal(err)
					}
				}
				scan := newCommandWALCleanupScan(1, 2, 0, nil, nil)
				scan.decisions = []commandWALSegmentCleanupDecision{{file: file}}
				decisions, err := scan.finish(tc.err)
				if failClose {
					if !errors.Is(err, os.ErrClosed) || errors.Is(err, tc.want) {
						t.Fatalf("close failure must reach checked cleanup without a deferrable classification: %v", err)
					}
				} else if !errors.Is(err, tc.want) {
					t.Fatalf("clean discard changed refusal classification: got %v, want %v", err, tc.want)
				}
				if tc.err == nil && !errors.Is(err, ErrCommandWALAppliedLSNNonContig) {
					t.Fatalf("missing retained replay detail: %v", err)
				}
				if decisions[0].file != nil {
					t.Fatal("discard should consume the decision handle")
				}
				if _, err := file.Seek(0, io.SeekCurrent); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("discard retained a live descriptor: %v", err)
				}
			})
		}
	}
}
