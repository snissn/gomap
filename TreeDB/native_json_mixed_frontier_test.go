package treedb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/tree"
)

// The external-package lifecycle regression remains separate. This companion
// uses the same public entry and operations; only the existing durability observer
// pauses the captured frontier before its physical publication becomes durable.
func TestNativeJSONPublicMixedOrdersCapturedFrontier(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		for _, order := range []string{"native-first-raw-last", "raw-first-native-last"} {
			t.Run(string(profile)+"/"+order, func(t *testing.T) {
				opts := OptionsFor(profile, t.TempDir())
				opts.MemtableMode = "cow_btree"
				opts.FlushThreshold = 1 << 30
				opts.BackgroundCheckpointInterval = -1
				database, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				closed := false
				t.Cleanup(func() {
					if !closed {
						if err := database.Close(); err != nil {
							t.Errorf("cleanup public Close: %v", err)
						}
					}
				})
				handle, err := database.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				manager, err := collections.NewNativeJSONManager(handle)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := manager.CreateCollection(collections.CollectionMeta{Name: "rows"}); err != nil {
					t.Fatal(err)
				}
				stat := func(stats map[string]string, key string) uint64 {
					t.Helper()
					n, err := strconv.ParseUint(stats[key], 10, 64)
					if err != nil {
						t.Fatalf("stat %s=%q: %v", key, stats[key], err)
					}
					return n
				}
				writePair := func(id, value, raw string) {
					t.Helper()
					insert := func() {
						if err := manager.InsertBatch("rows", [][]byte{[]byte(id)}, [][]byte{[]byte(value)}); err != nil {
							t.Fatalf("native %s: %v", id, err)
						}
					}
					set := func() {
						if err := database.Set([]byte("raw"), []byte(raw)); err != nil {
							t.Fatalf("raw %s: %v", raw, err)
						}
					}
					if order == "native-first-raw-last" {
						insert()
						set()
					} else {
						set()
						insert()
					}
				}
				writePair("a", `{"n":1}`, "raw-old")
				prefixSequence := stat(database.Stats(), "treedb.cache.cow.txn.logical_seq")
				var prefixLSN uint64
				if profile != ProfileNoWALFast {
					prefixLSN = stat(handle.Stats(), "treedb.command_wal.live_accepted_max_lsn")
					if prefixLSN == 0 {
						t.Fatal("missing assigned old-prefix LSN")
					}
				}
				held, err := manager.Capture("rows")
				if err != nil {
					t.Fatal(err)
				}
				rawHeld := database.AcquireSnapshot()
				if rawHeld == nil {
					t.Fatal("nil public held raw Snapshot")
				}
				heldClosed := false
				t.Cleanup(func() {
					if !heldClosed {
						if err := errors.Join(held.Close(), rawHeld.Close()); err != nil {
							t.Errorf("held cleanup: %v", err)
						}
					}
				})
				if profile != ProfileNoWALFast {
					// Acquire before starting the physical publisher: it may hold
					// valueLogPublicationMu exclusively at the durability pause.
					// This immutable Snapshot still precedes capture and the suffix.
					beforeDisk := handle.AcquireSnapshot()
					if beforeDisk == nil {
						t.Fatal("nil pre-publication physical Snapshot")
					}
					beforeToken, ok := beforeDisk.StateToken()
					if !ok || beforeToken.AppliedCommandLSN >= prefixLSN {
						t.Fatalf("prefix published before hook: %+v prefix=%d", beforeToken, prefixLSN)
					}
					beforeDiskClosed := false
					t.Cleanup(func() {
						if !beforeDiskClosed {
							if err := beforeDisk.Close(); err != nil {
								t.Errorf("pre-publication Snapshot cleanup: %v", err)
							}
						}
					})
					resume := startNativeJSONPausedCheckpoint(t, database, opts.Dir)
					writePair("b", `{"n":2}`, "raw-new")
					suffixLSN := stat(handle.Stats(), "treedb.command_wal.live_accepted_max_lsn")
					if suffixLSN != prefixLSN+2 || stat(database.Stats(), "treedb.cache.cow.txn.logical_seq") != prefixSequence+2 {
						t.Fatalf("suffix did not add two complete commands: prefixLSN=%d suffixLSN=%d", prefixLSN, suffixLSN)
					}
					if checkpointErr := resume(); checkpointErr != nil {
						t.Fatalf("first captured mixed checkpoint: %v", checkpointErr)
					}
					disk := handle.AcquireSnapshot()
					if disk == nil {
						t.Fatal("nil captured-prefix physical Snapshot")
					}
					checkDisk := func() {
						token, ok := disk.StateToken()
						if !ok || token.AppliedCommandLSN != prefixLSN || token.AppliedCommandLSN >= suffixLSN {
							t.Fatalf("physical applied must equal OLD complete prefix: token=%+v prefix=%d suffix=%d", token, prefixLSN, suffixLSN)
						}
						if got, err := disk.Get([]byte("raw")); err != nil || string(got) != "raw-old" {
							t.Fatalf("physical old raw %q: %v", got, err)
						}
						root, err := disk.GetAtRoot(token.SystemRootPageID, []byte("collections/root/rows/primary"))
						if err != nil || len(root) != 8 {
							t.Fatalf("physical primary root %x: %v", root, err)
						}
						rootID := binary.BigEndian.Uint64(root)
						if got, err := disk.GetAtRoot(rootID, []byte("a")); err != nil || string(got) != `{"n":1}` {
							t.Fatalf("physical old native %q: %v", got, err)
						}
						if _, err := disk.GetAtRoot(rootID, []byte("b")); !errors.Is(err, tree.ErrKeyNotFound) {
							t.Fatalf("physical suffix exclusion: %v", err)
						}
					}
					// Ensure failed assertions still close the exact physical owner.
					func() {
						defer func() {
							if err := disk.Close(); err != nil {
								t.Errorf("physical prefix Close: %v", err)
							}
						}()
						checkDisk()
					}()
					if got, ok := beforeDisk.StateToken(); !ok || got != beforeToken {
						t.Fatalf("held pre-publication physical token changed: %+v -> %+v", beforeToken, got)
					}
					beforeDiskClosed = true
					if err := beforeDisk.Close(); err != nil {
						t.Fatal(err)
					}
					t.Logf("OLD physical prefix LSN=%d excludes newer suffix LSN=%d; logical prefix=%d suffix=%d", prefixLSN, suffixLSN, prefixSequence, prefixSequence+2)
				} else {
					// NoWAL blocks post-frontier admission. Preserve both ordering
					// lifecycle cells without pretending they exercise WAL overlap.
					if err := database.Checkpoint(); err != nil {
						t.Fatal(err)
					}
					writePair("b", `{"n":2}`, "raw-new")
				}
				for _, row := range []struct{ id, value string }{{"a", `{"n":1}`}, {"b", `{"n":2}`}} {
					if got, err := manager.Get("rows", []byte(row.id)); err != nil || string(got) != row.value {
						t.Fatalf("latest native %s %q: %v", row.id, got, err)
					}
				}
				if got, err := database.Get([]byte("raw")); err != nil || string(got) != "raw-new" {
					t.Fatalf("latest raw %q: %v", got, err)
				}
				if got, err := held.Get([]byte("a")); err != nil || string(got) != `{"n":1}` {
					t.Fatalf("held native %q: %v", got, err)
				}
				if _, err := held.Get([]byte("b")); !errors.Is(err, tree.ErrKeyNotFound) {
					t.Fatalf("held suffix absence: %v", err)
				}
				if got, err := rawHeld.Get([]byte("raw")); err != nil || string(got) != "raw-old" {
					t.Fatalf("held raw %q: %v", got, err)
				}
				heldClosed = true
				if err := errors.Join(held.Close(), rawHeld.Close()); err != nil {
					t.Fatal(err)
				}
				if err := database.Checkpoint(); err != nil {
					t.Fatalf("drain newer suffix: %v", err)
				}
				if profile != ProfileNoWALFast {
					token, ok := handle.StateToken()
					if !ok || token.AppliedCommandLSN != prefixLSN+2 {
						t.Fatalf("drained exact suffix LSN: %+v", token)
					}
				}
				closed = true
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
				database, err = Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				closed = false
				handle, err = database.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				manager, err = collections.NewNativeJSONManager(handle)
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range []struct{ id, value string }{{"a", `{"n":1}`}, {"b", `{"n":2}`}} {
					if got, err := manager.Get("rows", []byte(row.id)); err != nil || string(got) != row.value {
						t.Fatalf("reopened native %s %q: %v", row.id, got, err)
					}
				}
				if got, err := database.Get([]byte("raw")); err != nil || string(got) != "raw-new" {
					t.Fatalf("reopened raw %q: %v", got, err)
				}
				closed = true
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// startNativeJSONPausedCheckpoint uses the existing serialized durability test
// observer, not the obsolete public range provider. The exact PRIMARY data-sync
// event occurs after the cache has captured/rotated its frontier and released
// writeMu/writerMu, but before the accepted physical root is durable or handed
// off. Ordinary WAL suffix writes do not sync this physical file. The CAS avoids
// making an unrelated observer invocation wait on a blocked sync.Once callback.
func startNativeJSONPausedCheckpoint(t *testing.T, database *DB, dir string) func() error {
	t.Helper()
	reached, release := make(chan struct{}), make(chan struct{})
	var armed atomic.Bool
	root := filepath.Join(dir, "maindb")
	path := filepath.Join(root, "index.db.primary")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point != durabilitycut.BeforeIndexDataSync || event.Resource != durabilitycut.ResourceIndex || event.Root != root || event.Path != path || !armed.CompareAndSwap(false, true) {
			return nil
		}
		close(reached)
		select {
		case <-release:
			return nil
		case <-time.After(publicCommandWALCheckpointTestTimeout):
			return fmt.Errorf("native JSON physical checkpoint pause timed out: %s", path)
		}
	})
	done := make(chan error, 1)
	go func() { done <- database.Checkpoint() }()
	var resumeOnce sync.Once
	var checkpointErr error
	resume := func() error {
		resumeOnce.Do(func() {
			close(release)
			select {
			case checkpointErr = <-done:
			case <-time.After(publicCommandWALCheckpointTestTimeout):
				checkpointErr = errors.New("native JSON checkpoint did not finish after physical pause release")
			}
			restore()
		})
		return checkpointErr
	}
	t.Cleanup(func() {
		if err := resume(); err != nil {
			t.Errorf("paused checkpoint cleanup: %v", err)
		}
	})
	if err := waitForPublicCommandWALCheckpointPhase(reached, "native JSON captured PRIMARY data sync"); err != nil {
		t.Fatal(err)
	}
	return resume
}
