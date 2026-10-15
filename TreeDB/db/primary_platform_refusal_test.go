package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Exercise the actual unavailable operation and its no-publication boundary.
// Supported platforms continue into the fault/success oracle in each caller.
func primaryOnlinePlatformRefusal(t testing.TB, d *DB, ctx context.Context) bool {
	t.Helper()
	if runtime.GOOS != "windows" {
		return false
	}
	idx, state := d.idx.Load(), d.state.Load()
	before := primaryPairImages(t, d.dir)
	if err := d.VacuumIndexOnline(ctx); !errors.Is(err, ErrVacuumUnsupported) {
		t.Fatalf("online platform refusal: %v", err)
	}
	if d.idx.Load() != idx || d.state.Load() != state {
		t.Fatal("unsupported online vacuum published state")
	}
	requirePrimaryPairImages(t, d.dir, before)
	requireNoPrivatePrimaryPair(t, d.dir)
	return true
}

func primaryOfflineScopePlatformRefusal(t testing.TB, d *DB) bool {
	t.Helper()
	if rootpublication.StableRelativeNamespaceSupported() {
		return false
	}
	idx, state := d.idx.Load(), d.state.Load()
	before := primaryPairImages(t, d.dir)
	charge := idx.primary.MetadataOwner().Bytes()
	scope, err := d.captureOfflinePrimaryScopeV6()
	if scope != nil || !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("offline scope platform refusal: scope=%p err=%v", scope, err)
	}
	if d.idx.Load() != idx || d.state.Load() != state || idx.primary.MetadataOwner().Bytes() != charge {
		t.Fatal("unsupported offline scope changed custody/state")
	}
	requirePrimaryPairImages(t, d.dir, before)
	requireNoPrivatePrimaryPair(t, d.dir)
	return true
}

func primaryPairImages(t testing.TB, dir string) [2][]byte {
	t.Helper()
	var images [2][]byte
	for i, name := range []string{indexFileName, primaryIndexFileName} {
		var err error
		images[i], err = os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
	}
	return images
}
func requirePrimaryPairImages(t testing.TB, dir string, before [2][]byte) {
	t.Helper()
	after := primaryPairImages(t, dir)
	for i := range before {
		if !bytes.Equal(before[i], after[i]) {
			t.Fatalf("platform refusal mutated pair member %d", i)
		}
	}
}
func requireNoPrivatePrimaryPair(t testing.TB, dir string) {
	t.Helper()
	for _, name := range []string{indexNewFileName, primaryNewFileName, indexReadyFileName} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("platform refusal created %s: %v", name, err)
		}
	}
}
