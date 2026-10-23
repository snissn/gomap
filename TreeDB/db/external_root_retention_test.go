package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestRetainedPhysicalRootsRestoreTornSlotsAndRetry(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, DisableSideStores: true, DisableBackgroundPrune: true}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.SetSync([]byte("key"), []byte("previous")); err != nil {
		t.Fatal(err)
	}
	previousCut, err := database.CapturePhysicalSnapshotCutV1(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer previousCut.Close()
	previous, err := previousCut.RetainedRootV1()
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetSync([]byte("key"), []byte("current")); err != nil {
		t.Fatal(err)
	}
	currentCut, err := database.CapturePhysicalSnapshotCutV1(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer currentCut.Close()
	current, err := currentCut.RetainedRootV1()
	if err != nil {
		t.Fatal(err)
	}
	if err := RestoreRetainedPhysicalRootsV1(dir, current, &previous); !errors.Is(err, lockfile.ErrLocked) {
		t.Fatalf("restore admitted live writer: %v", err)
	}
	for i := 0; i < 16; i++ {
		if err := database.SetSync([]byte("key"), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := currentCut.Close(); err != nil {
		t.Fatal(err)
	}
	if err := previousCut.Close(); err != nil {
		t.Fatal(err)
	}
	for _, slot := range []int{0, 1} {
		for _, prefix := range []int{1, page.PageHeaderSize, page.PageSize / 2, page.PageSize} {
			// Model arbitrary power-loss persistence of the ONLY overwritten
			// bytes: a partial/torn selectable meta image. Root/dependency pages
			// and immutable certificates were stable before restore began.
			file, err := os.OpenFile(filepath.Join(dir, "index.db"), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, writeErr := file.WriteAt(bytes.Repeat([]byte{0x89}, prefix), int64(slot*page.PageSize))
			if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("restore interrupted before sync")
			err = restoreRetainedPhysicalRootsV1(dir, current, &previous, func(at int) error {
				if at == slot {
					return injected
				}
				return nil
			})
			if !errors.Is(err, injected) {
				t.Fatal(err)
			}
			if err := RestoreRetainedPhysicalRootsV1(dir, current, &previous); err != nil {
				t.Fatal(err)
			}
		}
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	got, err := database.Get([]byte("key"))
	if err != nil || string(got) != "current" {
		t.Fatalf("current=%q err=%v", got, err)
	}
	roots, err := database.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer roots.Release()
	found := false
	for _, root := range roots.Roots() {
		found = found || root.CommitSeq == previous.State.CommitSeq
	}
	if !found {
		t.Fatal("previous closure not reacquired")
	}
}

func TestRetainedPhysicalRootsRejectInodeAndNamespaceReplacement(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, DisableSideStores: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	cut, err := database.CapturePhysicalSnapshotCutV1(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	root, err := cut.RetainedRootV1()
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "index.db")
	image, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, image, 0600); err != nil {
		t.Fatal(err)
	}
	if err := RestoreRetainedPhysicalRootsV1(dir, root, nil); err == nil {
		t.Fatal("replacement inode admitted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".original", path); err != nil {
		t.Fatal(err)
	}
	other := t.TempDir()
	if err := os.Rename(path, filepath.Join(other, "index.db")); err != nil {
		t.Fatal(err)
	}
	if err := RestoreRetainedPhysicalRootsV1(other, root, nil); err == nil {
		t.Fatal("replacement namespace admitted")
	}
}
