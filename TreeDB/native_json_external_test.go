package treedb_test

import (
	"errors"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/tree"
)

// External callers enter through the ordinary cached DB. The returned semantic
// capability belongs to that same owner; the original DB owns its Close.
func TestNativeJSONExternalPublicOwnerLifecycle(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.FlushThreshold = 1 << 30
			opts.BackgroundCheckpointInterval = -1
			database, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					if err := database.Close(); err != nil {
						t.Errorf("public cleanup Close: %v", err)
					}
				}
			})
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatalf("public native capability: %v", err)
			}
			manager, err := collections.NewNativeJSONManager(handle)
			if err != nil {
				t.Fatal(err)
			}
			logical := func() uint64 {
				t.Helper()
				raw := database.Stats()["treedb.cache.cow.txn.logical_seq"]
				n, err := strconv.ParseUint(raw, 10, 64)
				if err != nil {
					t.Fatalf("public logical sequence %q: %v", raw, err)
				}
				return n
			}
			before := logical()
			if _, err := manager.CreateCollection(collections.CollectionMeta{Name: "rows"}); err != nil {
				t.Fatal(err)
			}
			if got := logical(); got != before+1 {
				t.Fatalf("native Create public logical advance: %d -> %d", before, got)
			}
			if err := manager.InsertBatch("rows", [][]byte{[]byte("a")}, [][]byte{[]byte(`{"n":1}`)}); err != nil {
				t.Fatal(err)
			}
			if err := database.Set([]byte("raw"), []byte("raw-v1")); err != nil {
				t.Fatalf("public raw Set after native: %v", err)
			}
			if got := logical(); got != before+3 {
				t.Fatalf("native/raw public logical sequence: want %d got %d", before+3, got)
			}
			upper, err := database.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			lower, err := handle.CaptureNativeSemanticState()
			if err != nil {
				if closeErr := upper.Close(); closeErr != nil {
					t.Errorf("upper Close: %v", closeErr)
				}
				t.Fatal(err)
			}
			same := upper.State() != nil && upper.State() == lower.State() && upper.Snapshot() == lower.Snapshot() && upper.LogicalSequence() == lower.LogicalSequence() && upper.LogicalSequence() == logical()
			closeErr := errors.Join(upper.Close(), lower.Close())
			upper, lower = nil, nil
			if !same || closeErr != nil {
				t.Fatalf("public/backend capture identity: same=%t close=%v", same, closeErr)
			}
			held, err := manager.Capture("rows")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := held.Close(); err != nil {
					t.Errorf("native held cleanup: %v", err)
				}
			})
			rawHeld := database.AcquireSnapshot()
			if rawHeld == nil {
				t.Fatal("nil public raw capture")
			}
			t.Cleanup(func() {
				if err := rawHeld.Close(); err != nil {
					t.Errorf("raw held cleanup: %v", err)
				}
			})
			repeated, err := database.NativeSemanticBackend()
			if err != nil || repeated != handle {
				t.Fatalf("repeat same-owner capability: %p != %p: %v", repeated, handle, err)
			}
			if err := manager.InsertBatch("rows", [][]byte{[]byte("b")}, [][]byte{[]byte(`{"n":2}`)}); err != nil {
				t.Fatal(err)
			}
			if err := database.Set([]byte("raw"), []byte("raw-v2")); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatalf("public mixed checkpoint: %v", err)
			}
			if got, err := held.Get([]byte("a")); err != nil || string(got) != `{"n":1}` {
				t.Fatalf("held native row %q: %v", got, err)
			}
			if _, err := held.Get([]byte("b")); !errors.Is(err, tree.ErrKeyNotFound) {
				t.Fatalf("held native absence: %v", err)
			}
			if got, err := rawHeld.Get([]byte("raw")); err != nil || string(got) != "raw-v1" {
				t.Fatalf("held public raw value %q: %v", got, err)
			}
			if err := errors.Join(held.Close(), rawHeld.Close()); err != nil {
				t.Fatal(err)
			}
			registry := handle.ValueLogIdentityPinRegistry()
			closed = true
			if err := database.Close(); err != nil {
				t.Fatalf("public owner Close: %v", err)
			}
			if got := registry.Stats(); got.ActivePins != 0 || got.ActiveIdentities != 0 || got.ActiveStableNamespaceLinks != 0 || registry.CleanupError() != nil {
				t.Fatalf("closed original pins: %+v cleanup=%v", got, registry.CleanupError())
			}
			if _, err := collections.NewNativeJSONManager(handle); !errors.Is(err, backenddb.ErrClosed) {
				t.Fatalf("capability after public owner Close: %v", err)
			}
			database, err = treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			closed = false
			if profile != treedb.ProfileNoWALFast {
				lsn, err := strconv.ParseUint(database.Stats()["treedb.command_wal.applied_lsn"], 10, 64)
				if err != nil || lsn == 0 {
					t.Fatalf("recovered canonical LSN=%d: %v", lsn, err)
				}
			}
			handle, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatalf("recovered public capability: %v", err)
			}
			manager, err = collections.NewNativeJSONManager(handle)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range []struct{ id, value string }{{"a", `{"n":1}`}, {"b", `{"n":2}`}} {
				if got, err := manager.Get("rows", []byte(row.id)); err != nil || string(got) != row.value {
					t.Fatalf("recovered native %s %q: %v", row.id, got, err)
				}
			}
			if got, err := database.Get([]byte("raw")); err != nil || string(got) != "raw-v2" {
				t.Fatalf("recovered public raw %q: %v", got, err)
			}
			if err := manager.InsertBatch("rows", [][]byte{[]byte("c")}, [][]byte{[]byte(`{"n":3}`)}); err != nil {
				t.Fatalf("first recovered descriptor mutation: %v", err)
			}
			if got, err := manager.Get("rows", []byte("c")); err != nil || string(got) != `{"n":3}` {
				t.Fatalf("recovered new native row %q: %v", got, err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			registry = handle.ValueLogIdentityPinRegistry()
			closed = true
			if err := database.Close(); err != nil {
				t.Fatalf("recovered public owner Close: %v", err)
			}
			if got := registry.Stats(); got.ActivePins != 0 || got.ActiveIdentities != 0 || got.ActiveStableNamespaceLinks != 0 || registry.CleanupError() != nil {
				t.Fatalf("recovered original pins: %+v cleanup=%v", got, registry.CleanupError())
			}
		})
	}
}
