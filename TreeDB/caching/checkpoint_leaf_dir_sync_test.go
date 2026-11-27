package caching

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

func TestCachingCheckpointSharesSingleLeafLogDirSyncWithManifest(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("deferred leaf_vlog dir sync requires stable relative namespace support")
	}
	t.Setenv(envDisableVlogGenerationLoop, "1")
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
		DisableBackgroundPrune:     true,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		_ = backend.Close()
		t.Fatalf("bind private COW owner: %v", err)
	}
	cached, err := Open(dir, backend, Options{
		TransactionOwner:           owner,
		IndexOuterLeavesInValueLog: true,
		MemtableMode:               "cow_btree",
		DisableWAL:                 true,
		RelaxedSync:                true,
		AllowUnsafe:                true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	if err := backend.BindNativeSemanticBridge(owner, cached); err != nil {
		_ = cached.Close()
		t.Fatalf("bind native semantic bridge: %v", err)
	}

	leafDir := filepath.Clean(backenddb.LeafLogDirPath(dir))
	var leafDirSyncs atomic.Int64
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeNewFileDirectorySync &&
			e.Resource == durabilitycut.ResourceOuterLeaf &&
			filepath.Clean(e.Path) == leafDir {
			leafDirSyncs.Add(1)
		}
		return nil
	})

	const numKeys = 128
	val := bytes.Repeat([]byte("x"), 128)
	for i := 0; i < numKeys; i++ {
		if err := cached.Set([]byte(fmt.Sprintf("k-%04d", i)), val); err != nil {
			restore()
			_ = cached.Close()
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := cached.Checkpoint(); err != nil {
		restore()
		_ = cached.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	restore()

	// Segment creation (value-l255-000001.log) defers its leaf_vlog dir sync
	// and shares the single leaf_vlog dir sync performed by immutable manifest
	// replacement before meta publication.
	if got := leafDirSyncs.Load(); got != 1 {
		_ = cached.Close()
		t.Fatalf("leaf_vlog directory syncs across initial segment creation + Checkpoint = %d, want 1", got)
	}
	if err := cached.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify full data durability.
	reopened, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
		DisableBackgroundPrune:     true,
	})
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	defer func() {
		if closeErr := reopened.Close(); closeErr != nil {
			t.Errorf("close reopened: %v", closeErr)
		}
	}()
	for i := 0; i < numKeys; i++ {
		got, err := reopened.Get([]byte(fmt.Sprintf("k-%04d", i)))
		if err != nil || !bytes.Equal(got, val) {
			t.Fatalf("reopen Get(k-%04d) = (%d bytes, %v), want %d bytes", i, len(got), err, len(val))
		}
	}
}
