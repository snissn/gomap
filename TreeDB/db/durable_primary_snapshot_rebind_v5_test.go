package db

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"os"
	"path/filepath"
	"testing"
)

func TestPrimarySnapshotRebindBothFilesDependenciesAndOlderSlot(t *testing.T) {
	for _, directory := range []bool{false, true} {
		name := "manifest"
		if directory {
			name = "directory"
		}
		t.Run(name, func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "source")
			if directory {
				if err := os.MkdirAll(source, 0755); err != nil {
					t.Fatal(err)
				}
				if err := SaveFormatConfig(source, FormatConfig{RequiredFeatures: []string{RequiredFeatureDependencyDirectoryV2}}); err != nil {
					t.Fatal(err)
				}
			}
			database, err := Open(Options{Dir: source, IndexPrimaryDirectory: true, ValueLog: ValueLogOptions{PointerThreshold: 1}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if database != nil {
					_ = database.Close()
				}
			}()
			values := [][]byte{bytes.Repeat([]byte("first"), 64), bytes.Repeat([]byte("second"), 64)}
			pointers := appendPointersInNewSegment(t, source, 0, 1, 10000, 2, func(i int) []byte { return values[i] })
			if err = database.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			for i, key := range []string{"first", "second"} {
				batch := database.NewBatch().(*Batch)
				if err = batch.SetPointer([]byte(key), pointers[i]); err != nil {
					t.Fatal(err)
				}
				if err = batch.WriteSync(); err != nil {
					t.Fatal(err)
				}
				if err = batch.Close(); err != nil {
					t.Fatal(err)
				}
			}
			commits := database.durableRoot.slotCommit
			newest := database.durableRoot.meta.RootRecordPageID
			if commits[0] == 0 || commits[1] == 0 || commits[0] == commits[1] {
				t.Fatalf("source slots %v", commits)
			}
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			database = nil
			target := filepath.Join(t.TempDir(), "target")
			if err = os.CopyFS(target, os.DirFS(source)); err != nil {
				t.Fatal(err)
			}
			var original [2][]byte
			paths := [2]string{filepath.Join(target, indexFileName), filepath.Join(target, primaryIndexFileName)}
			for i, path := range paths {
				original[i], err = os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
			}
			cutErr := errors.New("primary rebind before meta")
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if (e.Point == durabilitycut.BeforeMetaWrite && e.Resource == durabilitycut.ResourceMeta) || (e.Point == durabilitycut.BeforePublicationSealWrite && e.Resource == durabilitycut.ResourceSeal) {
					return cutErr
				}
				return nil
			})
			err = RebindDurableRootSnapshotV1(target)
			restore()
			if !errors.Is(err, cutErr) {
				t.Fatalf("rebind cut %v", err)
			}
			for i, path := range paths {
				actual, e := os.ReadFile(path)
				if e != nil || !bytes.Equal(actual, original[i]) {
					t.Fatalf("cut changed file %d: %v", i, e)
				}
			}
			temporary, e := filepath.Glob(filepath.Join(target, ".primary-root-rebind-*"))
			if e != nil || len(temporary) != 0 {
				t.Fatalf("temporary files %v %v", temporary, e)
			}
			if copied, e := Open(Options{Dir: target, ReadOnly: true}); e == nil {
				_ = copied.Close()
				t.Fatal("copied dependency admitted without rebind")
			}
			if err = RebindDurableRootSnapshotV1(target); err != nil {
				t.Fatal(err)
			}
			rebound, err := Open(Options{Dir: target, ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			if rebound.durableRoot.slotCommit != commits {
				t.Fatalf("slot preservation %v want %v", rebound.durableRoot.slotCommit, commits)
			}
			for i, key := range []string{"first", "second"} {
				got, e := rebound.Get([]byte(key))
				if e != nil || !bytes.Equal(got, values[i]) {
					t.Fatalf("rebound %s %d %v", key, len(got), e)
				}
			}
			if err = rebound.Close(); err != nil {
				t.Fatal(err)
			}
			// Corrupt only the newest record, leaving the older independently complete
			// slot and its rebound proof/dependency closure as the recovery authority.
			file, err := os.OpenFile(paths[1], os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, err = file.WriteAt([]byte{0xff}, int64(primaryarena.Local(newest))*4096+1000)
			if err = errors.Join(err, file.Sync(), file.Close()); err != nil {
				t.Fatal(err)
			}
			database, err = Open(Options{Dir: target, IndexPrimaryDirectory: true})
			if err != nil {
				t.Fatal(err)
			}
			if database.meta.CommitSeq != min(commits[0], commits[1]) {
				t.Fatalf("older recovery commit %d want %d", database.meta.CommitSeq, min(commits[0], commits[1]))
			}
			if got, e := database.Get([]byte("second")); e != nil || got != nil {
				t.Fatalf("older root exposed newest key %q %v", got, e)
			}
			if got, e := database.Get([]byte("first")); e != nil || !bytes.Equal(got, values[0]) {
				t.Fatalf("older slot pointer %d %v", len(got), e)
			}
			if err = database.SetSync([]byte("after"), []byte("restored")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
