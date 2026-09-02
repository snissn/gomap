package treedb

import (
	"errors"
	"io/fs"
	"path/filepath"
	"reflect"
	"testing"
)

func TestOpenFreshCompositeEnsuresPublicLayoutDependencies(t *testing.T) {
	root := filepath.Join(t.TempDir(), "db")
	var gotMode fs.FileMode
	var gotProof string
	var gotPaths []string
	previous := ensureOpenStorageLayoutDirs
	ensureOpenStorageLayoutDirs = func(mode fs.FileMode, proof string, paths ...string) error {
		gotMode = mode
		gotProof = proof
		gotPaths = append([]string(nil), paths...)
		return previous(mode, proof, paths...)
	}
	t.Cleanup(func() {
		ensureOpenStorageLayoutDirs = previous
	})

	database, err := Open(Options{Dir: root})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	want := []string{
		root,
		filepath.Join(root, "maindb"),
		filepath.Join(root, "dictdb"),
	}
	wantProof := filepath.Join(root, "maindb", "index.db")
	if gotMode != 0o755 || gotProof != wantProof || !reflect.DeepEqual(gotPaths, want) {
		t.Fatalf("public layout mode=%#o proof=%q paths=%v, want mode=%#o proof=%q paths=%v", gotMode, gotProof, gotPaths, fs.FileMode(0o755), wantProof, want)
	}
}

func TestOpenFreshFlatEnsuresOuterRootDependency(t *testing.T) {
	root := filepath.Join(t.TempDir(), "db")
	var gotPaths []string
	previous := ensureOpenStorageLayoutDirs
	ensureOpenStorageLayoutDirs = func(mode fs.FileMode, proof string, paths ...string) error {
		gotPaths = append([]string(nil), paths...)
		return previous(mode, proof, paths...)
	}
	t.Cleanup(func() {
		ensureOpenStorageLayoutDirs = previous
	})

	database, err := Open(Options{Dir: root, DisableSideStores: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if want := []string{root}; !reflect.DeepEqual(gotPaths, want) {
		t.Fatalf("flat public layout paths=%v, want %v", gotPaths, want)
	}
}

func TestOpenPropagatesPublicLayoutNamespaceFailures(t *testing.T) {
	for _, name := range []string{"outer-parent", "root", "nested-parent"} {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "db")
			wantErr := errors.New("injected " + name + " sync failure")
			previous := ensureOpenStorageLayoutDirs
			ensureOpenStorageLayoutDirs = func(fs.FileMode, string, ...string) error {
				return wantErr
			}
			t.Cleanup(func() {
				ensureOpenStorageLayoutDirs = previous
			})

			if _, err := Open(Options{Dir: root}); !errors.Is(err, wantErr) {
				t.Fatalf("Open error=%v, want %v", err, wantErr)
			}
		})
	}
}

func TestOpenPreservesTypedUnsupportedNamespaceStatus(t *testing.T) {
	root := filepath.Join(t.TempDir(), "db")
	previous := ensureOpenStorageLayoutDirs
	ensureOpenStorageLayoutDirs = func(fs.FileMode, string, ...string) error {
		return ErrNamespacePersistenceUnsupported
	}
	t.Cleanup(func() {
		ensureOpenStorageLayoutDirs = previous
	})

	if _, err := Open(Options{Dir: root}); !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("Open error=%v, want ErrNamespacePersistenceUnsupported", err)
	}
}

func TestOpenFreshCompositeCloseReopenPreservesValue(t *testing.T) {
	root := filepath.Join(t.TempDir(), "db")
	key := []byte("layout/identity")
	value := []byte("preserved")

	database, err := Open(Options{Dir: root})
	if err != nil {
		t.Fatalf("Open fresh: %v", err)
	}
	if err := database.SetSync(key, value); err != nil {
		_ = database.Close()
		t.Fatalf("SetSync: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close fresh: %v", err)
	}

	reopened, err := Open(Options{Dir: root})
	if err != nil {
		t.Fatalf("Open existing: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.Get(key)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !reflect.DeepEqual(got, value) {
		t.Fatalf("Get after reopen=%q, want %q", got, value)
	}
}
