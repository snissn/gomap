package powerlossoracle

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/atomicfile"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestProductionAtomicReplacePreservesInodeUntilDirectorySync(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "manifest")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	var operations []durabilitycut.NamespaceOperation
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace != "" {
			operations = append(operations, event.Namespace)
		}
		return model.Observe(root, event)
	})
	if err := atomicfile.Write(path, []byte("new"), 0o600); err != nil {
		restore()
		t.Fatal(err)
	}
	restore()
	if want := []durabilitycut.NamespaceOperation{durabilitycut.NamespaceCreate, durabilitycut.NamespaceRename}; !reflect.DeepEqual(operations, want) {
		t.Fatalf("namespace operations=%v want=%v", operations, want)
	}

	assertStableFile(t, model, "manifest", "old")
	// The new inode's bytes are stable, but the old inode remains reachable
	// through the stable destination name until its directory is synced.
	assertStableFile(t, model, "manifest", "old")
	if err := model.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	assertStableFile(t, model, "manifest", "new")
}

func TestProductionAtomicCreateFailureReportsCleanupUnlink(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "manifest")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("stop after create")
	var operations []durabilitycut.NamespaceOperation
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == "" {
			return nil
		}
		operations = append(operations, event.Namespace)
		if err := model.Observe(root, event); err != nil {
			return err
		}
		if event.Namespace == durabilitycut.NamespaceCreate {
			return injected
		}
		return nil
	})
	err = atomicfile.Write(path, []byte("new"), 0o600)
	restore()
	if !errors.Is(err, injected) {
		t.Fatalf("atomic write err=%v want injected create failure", err)
	}
	if want := []durabilitycut.NamespaceOperation{durabilitycut.NamespaceCreate, durabilitycut.NamespaceUnlink}; !reflect.DeepEqual(operations, want) {
		t.Fatalf("namespace operations=%v want=%v", operations, want)
	}
	if got, want := model.VolatilePaths(), []string{"manifest"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("volatile paths after cleanup=%v want=%v", got, want)
	}
	assertStableFile(t, model, "manifest", "old")
}

func TestProductionPagerSyncPromotesOnlyAtAfterCut(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "index.db")
	chunkSize := int64(64 * 1024)
	p, err := pager.Open(path, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if _, err := p.Alloc(1); err != nil {
		t.Fatal(err)
	}
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, page.PageSize)
	data[0] = 0x7f
	if err := p.Write(0, data); err != nil {
		t.Fatal(err)
	}
	var points []durabilitycut.Point
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point != durabilitycut.BeforeIndexDataSync && event.Point != durabilitycut.AfterIndexDataSync {
			return nil
		}
		points = append(points, event.Point)
		return model.Observe(root, event)
	})
	if err := p.SyncIndexData(); err != nil {
		restore()
		t.Fatal(err)
	}
	restore()
	if want := []durabilitycut.Point{durabilitycut.BeforeIndexDataSync, durabilitycut.AfterIndexDataSync}; !reflect.DeepEqual(points, want) {
		t.Fatalf("pager cut points=%v want=%v", points, want)
	}
	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(crashDir, "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != int(chunkSize) || got[0] != 0x7f {
		t.Fatalf("stable pager bytes len=%d first=%#x", len(got), got[0])
	}
}

func assertStableFile(t *testing.T, model *Model, path, want string) {
	t.Helper()
	dir := t.TempDir()
	if err := model.MaterializeStable(dir); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, path))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("stable %s=%q want=%q", path, got, want)
	}
}
