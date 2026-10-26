//go:build treedb_test

package mvcc

import (
	"fmt"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// This test uses only preexisting public APIs and observation seams so its
// unchanged bytes can also run on the two-fence source as a causal control.
func TestCommitAtDurablePrimarySinglePhysicalFenceV6(t *testing.T) {
	d, err := treedb.Open(primaryReplacementOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	store := New(d)
	if err := store.CommitAt(1, []Mutation{{Key: []byte("key"), Value: []byte("A")}}, CommitDurable); err != nil {
		t.Fatal(err)
	}
	for ts := uint64(2); ts <= 3; ts++ {
		primary, data := 0, 0
		staged := false
		restore := durabilitycut.Install(func(e durabilitycut.Event) error {
			if filepath.Base(e.Path) == "index.db.primary" {
				if e.Point == durabilitycut.AfterMetaWrite {
					staged = true
				}
				if e.Point == durabilitycut.AfterIndexDataSync {
					if e.StablePath == nil {
						return fmt.Errorf("PRIMARY fence has no retained physical handle")
					}
					primary++
				}
			}
			if filepath.Base(e.Path) == "index.db" && e.Point == durabilitycut.AfterIndexDataSync {
				data++
			}
			return nil
		})
		err := store.CommitAt(ts, []Mutation{{Key: []byte("key"), Value: []byte(fmt.Sprintf("value-%d", ts))}}, CommitDurable)
		restore()
		if err != nil {
			t.Fatal(err)
		}
		if primary != 1 || data != 0 || !staged {
			t.Fatalf("public durable commit %d: physical PRIMARY=%d want1 DATA=%d want0 capsule_staged=%t", ts, primary, data, staged)
		}
		requireResult(t, store, []byte("key"), ts, Present, ts, []byte(fmt.Sprintf("value-%d", ts)))
	}
}
