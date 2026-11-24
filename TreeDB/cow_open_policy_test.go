package treedb_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestOpenMemtablePolicyDefaultRemainsAdaptive(t *testing.T) {
	for _, mode := range []string{"", "auto"} {
		t.Run("mode="+mode, func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, t.TempDir())
			opts.MemtableMode = mode
			database, err := treedb.Open(opts)
			if err != nil {
				t.Fatalf("Open(default, mode=%q): %v", mode, err)
			}
			defer func() {
				if err := database.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
			}()
			if _, err := database.NativeSemanticBackend(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("NativeSemanticBackend(default, mode=%q) err=%v, want ErrNativeSemanticUnsupported", mode, err)
			}
			if got := database.Stats()["treedb.cache.memtable_mode_config"]; got != "adaptive" {
				t.Fatalf("default mode=%q memtable_mode_config=%q, want adaptive", mode, got)
			}
		})
	}
}

func TestOpenMemtablePolicySemanticSelectsCOWBTree(t *testing.T) {
	for _, mode := range []string{"", "auto"} {
		t.Run("mode="+mode, func(t *testing.T) {
			opts := treedb.OptionsForSemantic(treedb.ProfileCommandWALRelaxed, t.TempDir())
			opts.MemtableMode = mode
			database, err := treedb.Open(opts)
			if err != nil {
				t.Fatalf("Open(semantic, mode=%q): %v", mode, err)
			}
			backend, err := database.NativeSemanticBackend()
			if err != nil {
				_ = database.Close()
				t.Fatalf("NativeSemanticBackend(semantic, mode=%q): %v", mode, err)
			}
			manager := collections.NewCollectionManager(backend)
			if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "docs"}); err != nil {
				_ = manager.Close()
				_ = database.Close()
				t.Fatalf("CreateCollection: %v", err)
			}
			col, err := manager.OpenCollection("docs")
			if err != nil {
				_ = manager.Close()
				_ = database.Close()
				t.Fatalf("OpenCollection: %v", err)
			}
			if _, err := col.Insert([]byte("doc-1"), []byte(`{"ok":true}`)); err != nil {
				_ = manager.Close()
				_ = database.Close()
				t.Fatalf("Insert: %v", err)
			}
			got, err := col.Get([]byte("doc-1"))
			if err != nil || string(got) != `{"ok":true}` {
				_ = manager.Close()
				_ = database.Close()
				t.Fatalf("Get: got=%q err=%v", got, err)
			}
			stats := database.Stats()
			if stats["treedb.cache.memtable_mode"] != "cow_btree" {
				_ = manager.Close()
				_ = database.Close()
				t.Fatalf("expected cow_btree mode stat, got %q", stats["treedb.cache.memtable_mode"])
			}
			if err := manager.Close(); err != nil {
				_ = database.Close()
				t.Fatalf("manager.Close: %v", err)
			}
			if err := database.Close(); err != nil {
				t.Fatalf("database.Close: %v", err)
			}
		})
	}
}

func TestOpenMemtablePolicySemanticPreservesExplicitOverrides(t *testing.T) {
	custom := memtable.DefaultCOWLimits()
	custom.MaxInFlightBytes = 16 << 20
	custom.MaxTotalBytes = 64 << 20
	custom.MaxRetiredBytes = 64 << 20
	custom.MaxGenerationBytes = 8 << 20

	opts := treedb.OptionsForSemantic(treedb.ProfileCommandWALRelaxed, t.TempDir())
	opts.COWMemtableLimits = custom
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Open(semantic, custom limits): %v", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()
	if _, err := database.NativeSemanticBackend(); err != nil {
		t.Fatalf("NativeSemanticBackend: %v", err)
	}

	// Explicit mutable MemtableMode override still wins over MemtableWorkloadSemantic.
	btreeOpts := treedb.OptionsForSemantic(treedb.ProfileCommandWALRelaxed, t.TempDir())
	btreeOpts.MemtableMode = "btree"
	btreeDB, err := treedb.Open(btreeOpts)
	if err != nil {
		t.Fatalf("Open(semantic, explicit btree): %v", err)
	}
	defer func() {
		if err := btreeDB.Close(); err != nil {
			t.Fatalf("Close btreeDB: %v", err)
		}
	}()
	if _, err := btreeDB.NativeSemanticBackend(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("explicit btree should not activate COW: err=%v", err)
	}
	if got := btreeDB.Stats()["treedb.cache.memtable_mode"]; got != "btree" {
		t.Fatalf("explicit btree memtable_mode=%q, want btree", got)
	}
}

func TestOpenMemtablePolicySemanticReadOnlyAndBackendOnlySafe(t *testing.T) {
	dir := t.TempDir()
	seedOpts := treedb.OptionsForSemantic(treedb.ProfileCommandWALRelaxed, dir)
	seedDB, err := treedb.Open(seedOpts)
	if err != nil {
		t.Fatalf("seed Open: %v", err)
	}
	if err := seedDB.Set([]byte("k"), []byte("v")); err != nil {
		_ = seedDB.Close()
		t.Fatalf("seed Set: %v", err)
	}
	if err := seedDB.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}

	// ReadOnly with MemtableWorkloadSemantic must not force cow_btree into ErrCOWUnsupported.
	roOpts := treedb.OptionsForSemantic(treedb.ProfileCommandWALRelaxed, dir)
	roOpts.ReadOnly = true
	roDB, err := treedb.Open(roOpts)
	if err != nil {
		t.Fatalf("ReadOnly Open(semantic): %v", err)
	}
	got, err := roDB.Get([]byte("k"))
	if err != nil || string(got) != "v" {
		_ = roDB.Close()
		t.Fatalf("ReadOnly Get: got=%q err=%v", got, err)
	}
	if err := roDB.Close(); err != nil {
		t.Fatalf("ReadOnly Close: %v", err)
	}

	// backendOnly (OpenBackendWithCachedLeafLog) with MemtableWorkloadSemantic must succeed.
	beOpts := treedb.OptionsForSemantic(treedb.ProfileCommandWALRelaxed, t.TempDir())
	beOpts.IndexOuterLeavesInValueLog = true
	backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(beOpts)
	if err != nil {
		t.Fatalf("OpenBackendWithCachedLeafLog(semantic): %v", err)
	}
	if backend == nil {
		_ = cleanup()
		t.Fatal("expected non-nil backend")
	}
	if err := cleanup(); err != nil {
		t.Fatalf("backend cleanup: %v", err)
	}
}

func TestOpenMemtablePolicyRejectsInvalidWorkloadAndAdaptiveCOWBeforeStorage(t *testing.T) {
	badDir := filepath.Join(t.TempDir(), "invalid-workload")
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, badDir)
	opts.MemtableWorkload = treedb.MemtableWorkload(99)
	if _, err := treedb.Open(opts); err == nil {
		t.Fatal("expected error for invalid MemtableWorkload")
	}
	if _, err := os.Stat(badDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid MemtableWorkload created directory %q: err=%v", badDir, err)
	}

	for _, mode := range []string{"adaptive:cow_btree", "adaptive:cow_ordered"} {
		dir := filepath.Join(t.TempDir(), mode)
		adaptiveOpts := treedb.OptionsForSemantic(treedb.ProfileCommandWALRelaxed, dir)
		adaptiveOpts.MemtableMode = mode
		if _, err := treedb.Open(adaptiveOpts); !errors.Is(err, caching.ErrCOWUnsupported) {
			t.Fatalf("Open(%q) err=%v, want ErrCOWUnsupported", mode, err)
		}
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rejected %q created directory %q: err=%v", mode, dir, err)
		}
	}
}
