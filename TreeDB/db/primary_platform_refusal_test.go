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

// Closed-source maintenance must refuse before mutating either authoritative
// pair member or deleting any persistent value-log/cache input.
func primaryOfflinePlatformRefusal(t testing.TB, opts Options, rewrite bool) bool {
	t.Helper()
	if rootpublication.StableRelativeNamespaceSupported() {
		return false
	}
	pair := primaryPairImages(t, opts.Dir)
	var files []string
	err := filepath.WalkDir(opts.Dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && entry.Name() != "LOCK" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	before := make(map[string][]byte, len(files))
	for _, path := range files {
		image, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = image
	}
	if rewrite {
		_, err = ValueLogRewriteOffline(opts)
	} else {
		err = VacuumIndexOffline(opts)
	}
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("offline platform refusal rewrite=%v: %v", rewrite, err)
	}
	requirePrimaryPairImages(t, opts.Dir, pair)
	requireNoPrivatePrimaryPair(t, opts.Dir)
	for path, image := range before {
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(image, after) {
			t.Fatalf("unsupported maintenance changed persistent input %s: %v", path, err)
		}
	}
	return true
}
