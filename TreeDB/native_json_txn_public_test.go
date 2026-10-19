package treedb

import (
	"errors"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestNativeJSONPublicConditionalTransactions(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
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
						t.Errorf("cleanup Close: %v", err)
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
			if _, err = manager.CreateCollection(collections.CollectionMeta{Name: "rows"}); err != nil {
				t.Fatal(err)
			}
			if err = manager.InsertBatch("rows", [][]byte{[]byte("seed")}, [][]byte{[]byte(`{"n":0}`)}); err != nil {
				t.Fatal(err)
			}
			begin := func() *collections.NativeJSONTxn {
				t.Helper()
				tx, err := manager.Begin("rows")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := tx.Close(); err != nil {
						t.Errorf("txn cleanup: %v", err)
					}
				})
				return tx
			}
			stage := func(tx *collections.NativeJSONTxn, id, value string) {
				t.Helper()
				if err := tx.InsertBatch([][]byte{[]byte(id)}, [][]byte{[]byte(value)}); err != nil {
					t.Fatal(err)
				}
			}
			read := func(tx *collections.NativeJSONTxn, id, want string) {
				t.Helper()
				if value, err := tx.Get([]byte(id)); err != nil || string(value) != want {
					t.Fatalf("txn Get %s=%q want %q: %v", id, value, want, err)
				}
			}
			absent := func(tx *collections.NativeJSONTxn, id string) {
				t.Helper()
				if _, err := tx.Get([]byte(id)); !errors.Is(err, tree.ErrKeyNotFound) {
					t.Fatalf("txn absent %s: %v", id, err)
				}
			}
			assigned := func() uint64 {
				t.Helper()
				if profile == ProfileNoWALFast {
					return 0
				}
				v, err := strconv.ParseUint(handle.Stats()["treedb.command_wal.live_accepted_max_lsn"], 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				return v
			}
			reject := func(tx *collections.NativeJSONTxn, label string) {
				t.Helper()
				before := assigned()
				err := tx.Commit()
				if !errors.Is(err, backenddb.ErrConcurrentModification) {
					t.Fatalf("%s conflict: %v", label, err)
				}
				if after := assigned(); after != before {
					t.Fatalf("%s assigned before refusal: %d -> %d", label, before, after)
				}
			}

			// Both real P1 openings precede either body or commit.
			a, b := begin(), begin()
			stage(a, "a", `{"n":1}`)
			ids, docs := [][]byte{[]byte("b")}, [][]byte{[]byte(`{"n":2}`)}
			if err := b.InsertBatch(ids, docs); err != nil {
				t.Fatal(err)
			}
			if err := b.InsertBatch([][]byte{[]byte("second")}, [][]byte{[]byte(`{"n":99}`)}); err != nil {
				t.Fatalf("second ordered stage: %v", err)
			}
			ids[0][0] = 'z'
			docs[0][5] = '9' // staging owns input bytes
			read(a, "seed", `{"n":0}`)
			read(b, "seed", `{"n":0}`)
			read(a, "a", `{"n":1}`)
			read(b, "b", `{"n":2}`)
			read(b, "second", `{"n":99}`)
			absent(a, "never")
			absent(b, "never")
			if err := a.Commit(); err != nil {
				t.Fatalf("disjoint A: %v", err)
			}
			if err := b.Commit(); err != nil {
				t.Fatalf("disjoint B without reopen/retry: %v", err)
			}
			t.Log("two original disjoint openings committed without reopening or body retry")

			a, b = begin(), begin()
			stage(a, "same", `{"n":3}`)
			stage(b, "same", `{"n":4}`)
			if err := a.Commit(); err != nil {
				t.Fatal(err)
			}
			reject(b, "write/write")
			a, b = begin(), begin()
			absent(a, "x")
			stage(b, "x", `{"n":5}`)
			if err := b.Commit(); err != nil {
				t.Fatal(err)
			}
			stage(a, "y", `{"n":6}`)
			reject(a, "missing-read/write")

			handoff := begin()
			stage(handoff, "handoff", `{"n":7}`)
			read(handoff, "a", `{"n":1}`)
			if err := database.Set([]byte("raw-handoff"), []byte("v")); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatalf("equivalent handoff: %v", err)
			}
			if err := handoff.Commit(); err != nil {
				t.Fatalf("commit after physical handoff: %v", err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			check := func(m *collections.NativeJSONManager) {
				t.Helper()
				for id, want := range map[string]string{"seed": `{"n":0}`, "a": `{"n":1}`, "b": `{"n":2}`, "second": `{"n":99}`, "same": `{"n":3}`, "x": `{"n":5}`, "handoff": `{"n":7}`} {
					if got, err := m.Get("rows", []byte(id)); err != nil || string(got) != want {
						t.Fatalf("Get %s=%q want %q: %v", id, got, want, err)
					}
				}
				if _, err := m.Get("rows", []byte("y")); !errors.Is(err, tree.ErrKeyNotFound) {
					t.Fatalf("rejected y visible: %v", err)
				}
			}
			check(manager)
			registry := handle.ValueLogIdentityPinRegistry()
			cache := database.cached
			closed = true
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			if got := cache.COWMemoryStats(); got.TotalBytes != 0 || got.Sources != 0 || got.Views != 0 || got.ExternalLeases != 0 || got.Generations != 0 {
				t.Fatalf("original final accounting: %+v", got)
			}
			if got := registry.Stats(); got.ActivePins != 0 || got.ActiveIdentities != 0 || got.ActiveStableNamespaceLinks != 0 || registry.CleanupError() != nil {
				t.Fatalf("original pin cleanup: %+v %v", got, registry.CleanupError())
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			h, err := reopened.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			m, err := collections.NewNativeJSONManager(h)
			if err != nil {
				t.Fatal(err)
			}
			check(m)
			registry = h.ValueLogIdentityPinRegistry()
			cache = reopened.cached
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if got := cache.COWMemoryStats(); got.TotalBytes != 0 || got.Sources != 0 || got.Views != 0 || got.ExternalLeases != 0 || got.Generations != 0 {
				t.Fatalf("reopened final accounting: %+v", got)
			}
			if got := registry.Stats(); got.ActivePins != 0 || got.ActiveIdentities != 0 || got.ActiveStableNamespaceLinks != 0 || registry.CleanupError() != nil {
				t.Fatalf("reopened pin cleanup: %+v %v", got, registry.CleanupError())
			}

		})
	}
}
