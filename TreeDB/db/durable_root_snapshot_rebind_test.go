package db

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestRebindDurableRootSnapshotV1PreservesBothSlotsAndExactTargetIdentity(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("snapshot rebind requires durable rename and removal namespaces")
	}
	source := filepath.Join(t.TempDir(), "source")
	database, err := Open(Options{Dir: source, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if err != nil {
		t.Fatal(err)
	}
	values := [][]byte{[]byte("first-value-log-value"), []byte("second-value-log-value")}
	pointers := appendPointersInNewSegment(t, source, 0, 1, 10_000, len(values), func(index int) []byte { return values[index] })
	if err := database.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	for index, key := range []string{"first", "second"} {
		batch := database.NewBatch().(*Batch)
		if err := batch.SetPointer([]byte(key), pointers[index]); err != nil {
			t.Fatal(err)
		}
		if err := batch.WriteSync(); err != nil {
			t.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(t.TempDir(), "target")
	if err := os.CopyFS(target, os.DirFS(source)); err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(target, indexFileName)
	installedBeforeCut, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	cutErr := errors.New("snapshot rebind cut before meta")
	restoreCut := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.BeforeMetaWrite && event.Resource == durabilitycut.ResourceMeta {
			return cutErr
		}
		return nil
	})
	err = RebindDurableRootSnapshotV1(target)
	restoreCut()
	if !errors.Is(err, cutErr) {
		t.Fatalf("cut rebind error=%v, want %v", err, cutErr)
	}
	installedAfterCut, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(installedAfterCut, installedBeforeCut) {
		t.Fatal("pre-install rebind cut mutated the installed snapshot index")
	}
	temporaries, err := filepath.Glob(filepath.Join(target, ".durable-root-rebind-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temporaries) != 0 {
		t.Fatalf("pre-install rebind cut left temporary indexes: %v", temporaries)
	}
	if copied, err := Open(Options{Dir: target, ReadOnly: true}); err == nil {
		_ = copied.Close()
		t.Fatal("ordinary open accepted a copied durable dependency before snapshot rebind")
	} else if !errors.Is(err, ErrNoRecoverableMeta) {
		t.Fatalf("copied dependency open error=%v, want %v", err, ErrNoRecoverableMeta)
	}
	if err := RebindDurableRootSnapshotV1(target); err != nil {
		t.Fatal(err)
	}

	index, err := os.OpenFile(indexPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newSnapshotIndexPageStoreV1(index)
	if err != nil {
		_ = index.Close()
		t.Fatal(err)
	}
	selected, err := selectDurableRootV1(store, store.pageCount, nil)
	if err != nil {
		_ = index.Close()
		t.Fatal(err)
	}
	if selected.SlotCommits[0] == 0 || selected.SlotCommits[1] == 0 || selected.SlotCommits[0] == selected.SlotCommits[1] {
		_ = index.Close()
		t.Fatalf("rebound slot commits=%v, want two distinct recoverable generations", selected.SlotCommits)
	}
	newestSlot := selected.Slot
	olderSlot := newestSlot ^ 1
	olderManifest, err := rootpublication.LoadDependencyManifestV1(store, selected.SlotRecords[olderSlot].Manifest)
	if err != nil {
		_ = index.Close()
		t.Fatal(err)
	}
	entries := olderManifest.Entries()
	if len(entries) == 0 {
		_ = index.Close()
		t.Fatal("older rebound slot has no external dependency")
	}

	reopened, err := Open(Options{Dir: target, ReadOnly: true})
	if err != nil {
		_ = index.Close()
		t.Fatal(err)
	}
	for key, want := range map[string]string{"first": "first-value-log-value", "second": "second-value-log-value"} {
		got, err := reopened.Get([]byte(key))
		if err != nil || string(got) != want {
			_ = reopened.Close()
			_ = index.Close()
			t.Fatalf("Get(%q)=(%q,%v), want %q", key, got, err, want)
		}
	}
	if err := reopened.Close(); err != nil {
		_ = index.Close()
		t.Fatal(err)
	}

	newestMeta, err := store.ReadPage(newestSlot)
	if err != nil {
		_ = index.Close()
		t.Fatal(err)
	}
	newestMeta[page.PageHeaderSize+page.DurableMetaV1BodySize-1] ^= 0xff
	if err := store.WritePage(newestSlot, newestMeta); err != nil {
		_ = index.Close()
		t.Fatal(err)
	}
	if err := index.Sync(); err != nil {
		_ = index.Close()
		t.Fatal(err)
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}

	fallback, err := Open(Options{Dir: target, ReadOnly: true})
	if err != nil {
		t.Fatalf("open older rebound slot: %v", err)
	}
	got, err := fallback.Get([]byte("first"))
	if err != nil || string(got) != "first-value-log-value" {
		_ = fallback.Close()
		t.Fatalf("fallback Get(first)=(%q,%v)", got, err)
	}
	if err := fallback.Close(); err != nil {
		t.Fatal(err)
	}

	dependencyPath, err := durableDependencyPathV1(target, entries[0].DiagnosticPath)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(dependencyPath)
	if err != nil {
		t.Fatal(err)
	}
	replacement := dependencyPath + ".replacement"
	if err := os.WriteFile(replacement, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dependencyPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, dependencyPath); err != nil {
		t.Fatal(err)
	}
	if replaced, err := Open(Options{Dir: target, ReadOnly: true}); err == nil {
		_ = replaced.Close()
		t.Fatal("ordinary open accepted byte-identical dependency replacement after snapshot rebind")
	} else if !errors.Is(err, ErrNoRecoverableMeta) {
		t.Fatalf("replacement open error=%v, want %v", err, ErrNoRecoverableMeta)
	}
}
