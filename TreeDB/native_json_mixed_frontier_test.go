package treedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"github.com/snissn/gomap/TreeDB/tree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeBSONRootCoveredPublication(t *testing.T) {
	if dir := os.Getenv("GOMAP_ROOT_COVERED_BSON_CRASH_DIR"); dir != "" {
		testNativeBSONRootCoveredPublication(t, ProfileCommandWALRelaxed, dir, false, false, 1, true)
		return
	}
	for _, tc := range []struct {
		name    string
		profile Profile
		indexed bool
		generic bool
		entries int
	}{
		{name: "covered", profile: ProfileCommandWALRelaxed, entries: 1},
		{name: "durable", profile: ProfileCommandWALDurable, entries: 3},
		{name: "indexed", profile: ProfileCommandWALRelaxed, indexed: true, entries: 3},
		{name: "generic-update", profile: ProfileCommandWALRelaxed, generic: true, entries: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testNativeBSONRootCoveredPublication(t, tc.profile, t.TempDir(), tc.indexed, tc.generic, tc.entries, false)
		})
	}
}

func nativeBSONRootCoveredOptions(profile Profile, dir string) Options {
	opts := OptionsFor(profile, dir)
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	return opts
}

func testNativeBSONRootCoveredPublication(t *testing.T, profile Profile, dir string, indexed, generic bool, entries int, crash bool) {
	t.Helper()
	opts := nativeBSONRootCoveredOptions(profile, dir)
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	cache := database.cached
	t.Cleanup(func() {
		if database != nil {
			if err := database.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}
	})
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	prefix, _, err := backend.PrivateCOWAcceptedPrefix(database.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	meta := collections.CollectionMeta{Name: "rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}
	if indexed {
		meta.Indexes = []collections.IndexDefinition{{Name: "n", Field: "n", ValueType: collections.IndexValueBSONOrderedV2}}
	}
	if _, err := manager.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	read, err := collections.NewNativeJSONManager(backend)
	if err != nil {
		t.Fatal(err)
	}
	id := []byte("row")
	document, err := bson.Marshal(bson.D{{Key: "_id", Value: "row"}, {Key: "n", Value: int64(0)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.InsertBatchValidatedBSON([][]byte{id}, [][]byte{document}); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	start := prefix.LogicalFrontiers()
	write := func(value int64) {
		t.Helper()
		if generic {
			matched, modified, err := collection.Update(id, func([]byte) ([]byte, bool, error) {
				replacement, err := bson.Marshal(bson.D{{Key: "_id", Value: "row"}, {Key: "n", Value: value}})
				return replacement, true, err
			})
			if err != nil || !matched || !modified {
				t.Fatalf("generic BSON update %d: matched=%t modified=%t err=%v", value, matched, modified, err)
			}
			return
		}
		results, complete, err := collection.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]collections.BSONSetUpdateBatchItem{{
			DocumentID: id,
			Fields:     []collections.BSONSetField{{Key: "n", Value: nativeBSONQuery(t, value)}},
		}})
		if err != nil || !complete || len(results) != 1 || !results[0].Matched || !results[0].Modified {
			t.Fatalf("structured BSON update %d: results=%+v complete=%t err=%v", value, results, complete, err)
		}
	}
	checkValue := func(value []byte, err error, want int64) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := bson.Raw(value).Lookup("n").Int64OK(); !ok || got != want {
			t.Fatalf("BSON value %d/%t, want %d", got, ok, want)
		}
	}
	write(1)
	held, err := read.Capture("rows")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	write(2)
	write(3)
	old := prefix.LogicalFrontiers()
	if old.V != start.V+3 || old.C != start.C+3 || old.S != start.S {
		t.Fatalf("independent accepted commands: start=%+v old=%+v", start, old)
	}
	if count := prefix.AcceptedPendingCount(); count != entries {
		t.Errorf("accepted prefix has %d entries for three BSON commands, want %d", count, entries)
	}
	if crash {
		// The uncovered frames contain only inline BSON bytes. Sync those exact
		// WAL bytes so process termination exercises replay from the older S.
		if err := backend.FlushCommandWAL(true); err != nil {
			t.Fatal(err)
		}
		if applied := backend.State().AppliedCommandLSN; applied != start.C || t.Failed() {
			t.Fatalf("expected an unapplied root-covered prefix: base=%d tip=%d applied=%d", start.C, old.C, applied)
		}
		fmt.Printf("ROOT-COVERED BSON UNAPPLIED base=%d tip=%d\n", start.C, old.C)
		os.Exit(97)
	}
	resume := startNativeJSONPausedCheckpoint(t, database, opts.Dir)
	write(4)
	write(5)
	write(6)
	if count := prefix.AcceptedPendingCount(); count != 2*entries {
		t.Errorf("held old seal plus accepted suffix occupies %d entries, want %d", count, 2*entries)
	}
	value, err := read.Get("rows", id)
	checkValue(value, err, 6)
	if err := resume(); err != nil {
		t.Fatal(err)
	}
	if got := prefix.LogicalFrontiers(); got.S.LogicalSequence != old.V || got.S.CanonicalLSN != old.C || got.C != old.C+3 || got.V != old.V+3 {
		t.Fatalf("first seal must end at the exact older tip: old=%+v current=%+v", old, got)
	}
	disk := backend.AcquireSnapshot()
	if disk == nil {
		t.Fatal("missing first physical seal")
	}
	func() {
		defer func() {
			if err := disk.Close(); err != nil {
				t.Error(err)
			}
		}()
		state, ok := disk.StateToken()
		if !ok || state.AppliedCommandLSN != old.C {
			t.Fatalf("wrong physical frontier: %+v want LSN %d", state, old.C)
		}
		root, err := disk.GetAtRoot(state.SystemRootPageID, []byte("collections/root/rows/primary"))
		if err != nil || len(root) != 8 {
			t.Fatalf("physical primary root %x: %v", root, err)
		}
		value, err := disk.GetAtRoot(binary.BigEndian.Uint64(root), id)
		checkValue(value, err, 3)
	}()
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got := prefix.LogicalFrontiers(); got.S.LogicalSequence != old.V+3 || got.S.CanonicalLSN != old.C+3 || prefix.AcceptedPendingCount() != 0 {
		t.Fatalf("second seal must drain exactly through the suffix: %+v", got)
	}
	value, err = held.Get(id)
	checkValue(value, err, 1)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	requirePublicCOWFinalZero(t, cache)
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	read, err = collections.NewNativeJSONManager(backend)
	if err != nil {
		t.Fatal(err)
	}
	value, err = read.Get("rows", id)
	checkValue(value, err, 6)
}

func TestNativeBSONRootCoveredUnappliedReplay(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNativeBSONRootCoveredPublication$", "-test.v")
	cmd.Env = append(os.Environ(), "GOMAP_ROOT_COVERED_BSON_CRASH_DIR="+dir)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte("ROOT-COVERED BSON UNAPPLIED base=2 tip=5")) || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("abrupt child: %v\n%s", err, output)
	}
	for _, expected := range []uint64{3, 0} {
		before := workstats.Read().Replay
		database, err := Open(nativeBSONRootCoveredOptions(ProfileCommandWALRelaxed, dir))
		if err != nil {
			t.Fatal(err)
		}
		after := workstats.Read().Replay
		cache := database.cached
		func() {
			defer func() {
				if err := database.Close(); err != nil {
					t.Error(err)
				}
			}()
			if after.FramesApplied-before.FramesApplied != expected || after.FrameErrors != before.FrameErrors {
				t.Fatalf("canonical recovery applied %d frames, want %d: %+v -> %+v", after.FramesApplied-before.FramesApplied, expected, before, after)
			}
			backend, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			if applied, next := backend.State().AppliedCommandLSN, backend.CommandWALNextLSN(); applied != 5 || next != 6 {
				t.Fatalf("original command identity: applied=%d next=%d", applied, next)
			}
			reader, err := collections.NewNativeJSONManager(backend)
			if err != nil {
				t.Fatal(err)
			}
			doc, err := reader.Get("rows", []byte("row"))
			if err != nil || bson.Raw(doc).Lookup("n").Int64() != 3 {
				t.Fatalf("replayed BSON: %v %v", bson.Raw(doc), err)
			}
		}()
		requirePublicCOWFinalZero(t, cache)
	}
}

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
// observer, not the obsolete public range provider. The exact DATA META-write
// event occurs after the cache has captured/rotated its frontier and released
// writeMu/writerMu, but before the accepted physical root is durable or handed
// off. Ordinary WAL suffix writes do not install DATA META. The CAS avoids
// making an unrelated observer invocation wait on a blocked sync.Once callback.
func startNativeJSONPausedCheckpoint(t *testing.T, database *DB, dir string) func() error {
	t.Helper()
	reached, release := make(chan struct{}), make(chan struct{})
	var armed atomic.Bool
	root := filepath.Join(dir, "maindb")
	path := filepath.Join(root, "index.db")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point != durabilitycut.BeforeMetaWrite || event.Resource != durabilitycut.ResourceMeta || event.Root != root || event.Path != path || !armed.CompareAndSwap(false, true) {
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
	var checkpointCompleted bool
	resume := func() error {
		resumeOnce.Do(func() {
			close(release)
			if !checkpointCompleted {
				select {
				case checkpointErr = <-done:
					checkpointCompleted = true
				case <-time.After(publicCommandWALCheckpointTestTimeout):
					checkpointErr = errors.New("native JSON checkpoint did not finish after physical pause release")
				}
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
	select {
	case <-reached:
	case checkpointErr = <-done:
		checkpointCompleted = true
		if checkpointErr != nil {
			t.Fatalf("native JSON checkpoint failed before captured DATA META write: %v", checkpointErr)
		}
		t.Fatal("native JSON checkpoint completed without captured DATA META write pause")
	case <-time.After(publicCommandWALCheckpointTestTimeout):
		t.Fatal("timed out waiting for native JSON captured DATA META write")
	}
	return resume
}
