//go:build treedb_test

package mvcc

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestPrimaryReplacementReusesDurableRawLeafPrefix(t *testing.T) {
	dir := t.TempDir()
	d, err := treedb.Open(primaryReplacementOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	st := New(d)
	commit := func(ts uint64, ms []Mutation) {
		t.Helper()
		if err := st.CommitAt(ts, ms, CommitDurable); err != nil {
			t.Fatal(err)
		}
	}
	generation := func() uint64 {
		t.Helper()
		n, e := strconv.ParseUint(d.Stats()["treedb.durable_root.freelist.generation"], 10, 64)
		if e != nil || n == 0 {
			t.Fatal(n, e)
		}
		return n
	}
	commit(1, []Mutation{{Key: []byte("k000"), Value: []byte("old")}})
	commit(2, []Mutation{{Key: []byte("k000"), Value: []byte("keep")}})
	seed := make([]Mutation, 31)
	for i := range seed {
		seed[i] = Mutation{Key: []byte(fmt.Sprintf("k%03d", i+1)), Value: []byte("before")}
	}
	commit(100, seed)
	bootstrap := generation()
	seed = make([]Mutation, 64)
	for i := range seed {
		seed[i] = Mutation{Key: []byte(fmt.Sprintf("seed%03d", i)), Value: []byte("seed")}
	}
	commit(101, seed)
	before := generation()
	if before <= bootstrap {
		t.Fatal("fixture did not materialize DATA")
	}
	leaf, primary := 0, 0
	restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.AfterDependencyFileSync {
			for _, p := range e.Paths {
				if filepath.Base(filepath.Dir(p)) == "leaf_vlog" {
					leaf++
				}
			}
		}
		if e.Point == durabilitycut.AfterIndexDataSync && filepath.Base(e.Path) == "index.db.primary" {
			primary++
		}
		return nil
	}))
	defer restore()
	commit(100, []Mutation{{Key: []byte("k031"), Value: []byte("after")}})
	restore()
	if after := generation(); after != before {
		t.Fatalf("PRIMARY replacement changed DATA generation %d -> %d", before, after)
	}
	if leaf != 0 || primary != 2 {
		t.Fatalf("unchanged leaf physical witnesses=%d want0; mutable PRIMARY fences=%d want2", leaf, primary)
	}
	// Growing the SAME raw-leaf inode must fall back to fresh capture and sync.
	files, err := filepath.Glob(filepath.Join(dir, "leaf_vlog", "*.log"))
	if err != nil || len(files) == 0 {
		t.Fatalf("leaf files=%v err=%v", files, err)
	}
	sizes := make(map[string]int64, len(files))
	for _, path := range files {
		info, e := os.Stat(path)
		if e != nil {
			t.Fatal(e)
		}
		sizes[path] = info.Size()
	}
	leaf, primary = 0, 0
	restore = sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.AfterDependencyFileSync {
			for _, p := range e.Paths {
				if filepath.Base(filepath.Dir(p)) == "leaf_vlog" {
					leaf++
				}
			}
		}
		if e.Point == durabilitycut.AfterIndexDataSync && filepath.Base(e.Path) == "index.db.primary" {
			primary++
		}
		return nil
	}))
	seed = make([]Mutation, 64)
	for i := range seed {
		seed[i] = Mutation{Key: []byte(fmt.Sprintf("growth%03d", i)), Value: []byte("new-leaf")}
	}
	defer restore()
	commit(200, seed)
	restore()
	grew := false
	for path, size := range sizes {
		info, e := os.Stat(path)
		if e == nil && info.Size() > size {
			grew = true
		}
	}
	if !grew || leaf == 0 || primary != 2 {
		t.Fatalf("same-inode growth=%v leaf witnesses=%d PRIMARY fences=%d", grew, leaf, primary)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	d, err = treedb.Open(primaryReplacementOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	st = New(d)
	got, err := st.GetAt([]byte("k031"), 100)
	if err != nil || string(got.Value) != "after" {
		t.Fatalf("recovered result=%+v err=%v", got, err)
	}
	// Recovery validates the old root but must not invent a live completion
	// receipt. Its first new publication still performs dependency I/O.
	leaf = 0
	restore = sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.AfterDependencyFileSync {
			for _, p := range e.Paths {
				if filepath.Base(filepath.Dir(p)) == "leaf_vlog" {
					leaf++
				}
			}
		}
		return nil
	}))
	defer restore()
	commit(100, []Mutation{{Key: []byte("k031"), Value: []byte("after-reopen")}})
	restore()
	if leaf == 0 {
		t.Fatal("open minted raw-leaf publication completion authority")
	}
}
