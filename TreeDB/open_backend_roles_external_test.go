package treedb_test

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func backendRoleOptions(t *testing.T, profile treedb.Profile) treedb.Options {
	t.Helper()
	opts := treedb.OptionsFor(profile, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.MemtableShards = 2
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.ValueLog.PointerThreshold = 512
	return opts
}

// Use only exported APIs: these backend-root callers must never acquire a
// competing logical cache cut, even when the requested storage policy is COW.
func TestBackendFactoryRolesLegacyLifecycle(t *testing.T) {
	type factory func(treedb.Options) (*backenddb.DB, func() error, func() map[string]string, error)
	factories := []struct {
		name string
		open factory
	}{
		{"basic", func(opts treedb.Options) (*backenddb.DB, func() error, func() map[string]string, error) {
			backend, close, err := treedb.OpenBackendWithCachedLeafLog(opts)
			return backend, close, nil, err
		}},
		{"stats", treedb.OpenBackendWithCachedLeafLogStats},
		{"maintenance", func(opts treedb.Options) (*backenddb.DB, func() error, func() map[string]string, error) {
			backend, close, stats, control, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
			if err == nil && (control == nil || control.Scoped() == nil) {
				t.Error("missing original deferred maintenance control")
			}
			return backend, close, stats, err
		}},
	}
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, factory := range factories {
			t.Run(fmt.Sprintf("%s/%s", profile, factory.name), func(t *testing.T) {
				opts := backendRoleOptions(t, profile)

				backend, cleanup, stats, err := factory.open(opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if cleanup != nil {
						if err := cleanup(); err != nil {
							t.Error(err)
						}
					}
				})
				if backend.HasNativeSemanticBridge() {
					t.Fatal("backend factory bound logical COW bridge")
				}
				if err := backend.ActivateNativeSemanticOwner(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
					t.Fatalf("backend factory activation=%v", err)
				}
				if stats != nil {
					if got := stats()["treedb.cache.memtable_mode"]; got != "btree" {
						t.Fatalf("actual cache mode=%q", got)
					}
				}
				manager := collections.NewCollectionManager(backend)
				if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "legacy"}); err != nil {
					t.Fatal(err)
				}
				collection, err := manager.OpenCollection("legacy")
				if err != nil {
					t.Fatal(err)
				}
				document := []byte(`{"body":"` + string(bytes.Repeat([]byte("p"), 8192)) + `"}`)
				if _, err := collection.Insert([]byte("one"), document); err != nil {
					t.Fatal(err)
				}
				if err := manager.FlushAll(); err != nil {
					t.Fatal(err)
				}
				value := bytes.Repeat([]byte("raw-pointer"), 1024)
				ptrs, err := backend.AppendValueLogValues([][]byte{value})
				if err != nil {
					t.Fatal(err)
				}
				batch := backend.NewBatch()
				pointerBatch, ok := batch.(interface {
					SetPointer([]byte, page.ValuePtr) error
				})
				if !ok {
					t.Fatal("backend batch missing SetPointer")
				}
				if err := pointerBatch.SetPointer([]byte("backend-only"), ptrs[0]); err != nil {
					t.Fatal(err)
				}
				if err := batch.WriteSync(); err != nil {
					t.Fatal(err)
				}
				if err := batch.Close(); err != nil {
					t.Fatal(err)
				}
				if err := backend.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				cfg, ok, err := backenddb.LoadFormatConfig(filepath.Join(opts.Dir, "maindb"))
				if err != nil || !ok || !cfg.IndexOuterLeavesInValueLog {
					t.Fatalf("outer-leaf physical policy=%+v,%v,%v", cfg, ok, err)
				}
				snapshot := backend.AcquireSnapshot()
				entry, err := snapshot.GetEntry([]byte("backend-only"))
				if err != nil || entry.Flags&node.FlagPointer == 0 || entry.ValuePtr.FileID == 0 {
					t.Fatalf("persistent pointer=%+v,%v", entry, err)
				}
				if profile != treedb.ProfileNoWALFast {
					state, ok := snapshot.StateToken()
					if !ok {
						t.Fatal("missing physical state")
					}
					root, err := snapshot.Pager().Get(state.RootPageID)
					if err != nil {
						t.Fatal(err)
					}
					if got := page.PageType(page.DecodeHeader(root).Flags); got != page.PageTypeLeaf && got != page.PageTypeInternal {
						t.Fatalf("actual DATA root has invalid type: %v", got)
					}
				}
				snapshot.Close()
				closeOriginal := cleanup
				cleanup = nil
				if err := closeOriginal(); err != nil {
					t.Fatalf("legacy close=%v", err)
				}
				reopened, closeReopened, _, err := factory.open(opts)
				if err != nil {
					t.Fatal(err)
				}
				cleanup = closeReopened
				if got, err := reopened.Get([]byte("backend-only")); err != nil || !bytes.Equal(got, value) {
					t.Fatalf("reopened pointer len=%d,%v", len(got), err)
				}
				opened, err := collections.NewCollectionManager(reopened).OpenCollection("legacy")
				if err != nil {
					t.Fatal(err)
				}
				if got, err := opened.Get([]byte("one")); err != nil || !bytes.Equal(got, document) {
					t.Fatalf("reopened collection len=%d,%v", len(got), err)
				}
			})
		}
	}
}

func TestNativeSemanticBackendExportedLifecycle(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := backendRoleOptions(t, profile)
			database, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					if err := database.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			if err := database.Set([]byte("pending"), []byte("original")); err != nil {
				t.Fatal(err)
			}
			if backend, err := database.NativeSemanticBackend(); backend != nil || !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("pending activation=%v,%v", backend != nil, err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			snapshot := database.AcquireSnapshot()
			if backend, err := database.NativeSemanticBackend(); backend != nil || !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("held opening activation=%v,%v", backend != nil, err)
			}
			snapshot.Close()
			backend, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			if !backend.HasNativeSemanticBridge() {
				t.Fatal("ordinary Open lost original bridge")
			}
			before, ok := backend.StateToken()
			if !ok {
				t.Fatal("missing activated basis")
			}
			next := backend.CommandWALNextLSN()
			if err := backend.Set([]byte("alias"), []byte("blocked")); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("backend alias=%v", err)
			}
			if after, _ := backend.StateToken(); after != before || backend.CommandWALNextLSN() != next {
				t.Fatal("refused alias changed physical roots/identity")
			}
			if release, err := backend.AdmitLegacyCommandProducer(); release != nil || !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("legacy admission=%v", err)
			}
			if err := database.Set([]byte("suffix"), []byte("same owner")); err != nil {
				t.Fatal(err)
			}
			if repeated, err := database.NativeSemanticBackend(); repeated != backend || err != nil {
				t.Fatalf("repeat activation=%v,%v", repeated == backend, err)
			}
			closed = true
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			if handle, err := database.NativeSemanticBackend(); handle != nil || !errors.Is(err, treedb.ErrClosed) {
				t.Fatalf("closed activation=%v,%v", handle != nil, err)
			}
			reopened, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := reopened.NativeSemanticBackend(); err != nil {
				t.Fatalf("recovered activation=%v", err)
			}
			if got, err := reopened.Get([]byte("suffix")); err != nil || string(got) != "same owner" {
				t.Fatalf("recovered same-owner suffix=%q,%v", got, err)
			}
		})
	}
	var nilDB *treedb.DB
	if backend, err := nilDB.NativeSemanticBackend(); backend != nil || !errors.Is(err, treedb.ErrClosed) {
		t.Fatalf("nil activation=%v", err)
	}
}
