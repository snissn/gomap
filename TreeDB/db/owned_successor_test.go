package db

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
)

func TestOwnedBackendSuccessorCapturedRootLeafPointerAndReuse(t *testing.T) {
	for _, outer := range []bool{false, true} {
		t.Run(fmt.Sprintf("outer=%t", outer), func(t *testing.T) {
			dir := t.TempDir()
			d := openOrderedRootSpanNativeTestDB(t, dir, outer, 1)
			defer d.Close()
			if outer {
				log, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionBlock, BlockCodec: ValueLogBlockLZ4})
				if err != nil {
					t.Fatal(err)
				}
				defer log.Close()
				d.SetLeafPageLog(log)
			}
			value := bytes.Repeat([]byte("owned-pointer"), 400)
			ptrs := appendPointersInNewSegment(t, dir, 0, 83, 830000, 1, func(int) []byte { return value })
			if err := d.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			delta := newOrderedRootSpanNativeBatch(t, 600, "old")
			defer delta.Close()
			key := []byte("key-000599")
			if err := delta.SetPointerWithRevision(key, ptrs[0], 37); err != nil {
				t.Fatal(err)
			}
			policy := OrderedRootStoragePagerLeaves
			if outer {
				policy = OrderedRootStorageValueLogLeaves
			}
			root := publishOrderedRootSpanNativeBatch(t, d, 0, delta, policy)
			held := d.AcquireSnapshot()
			if held == nil {
				t.Fatal("missing original root capture")
			}
			defer held.Close()
			next := newOrderedRootSpanNativeBatch(t, 600, "new")
			defer next.Close()
			nextRoot := publishOrderedRootSpanNativeBatch(t, d, root, next, policy)
			if root == nextRoot {
				t.Fatal("fixture did not replace physical root")
			}
			first, found, err := d.SeekGEOwnedAtRoot(root, key, []byte("key-000600"))
			if err != nil || !found || !bytes.Equal(first.Key, key) || !bytes.Equal(first.Value, value) || first.Flags&node.FlagPointer == 0 || first.ValuePtr != ptrs[0] || first.Revision != 37 {
				t.Fatalf("original pointer root: entry=%+v found=%t err=%v", first, found, err)
			}
			// Cross-leaf descent, exclusive end and repeated use of
			// the original private capture must not expose its fixed scratch/output.
			for range 20 {
				got, ok, err := d.SeekGEOwnedAtRoot(root, []byte("key-000299x"), []byte("key-000302"))
				if err != nil || !ok || string(got.Key) != "key-000300" || string(got.Value) != "old-000300" {
					t.Fatal(got, ok, err)
				}
				got.Key[0], got.Value[0] = 'X', 'X'
				if _, ok, err := d.SeekGEOwnedAtRoot(root, key, key); err != nil || ok {
					t.Fatal("exclusive bound", ok, err)
				}
			}
			current, ok, err := d.SeekGEOwnedAtRoot(nextRoot, key, nil)
			if err != nil || !ok || string(current.Value) != "new-000599" {
				t.Fatal("replacement root", current, ok, err)
			}
			if !bytes.Equal(first.Key, key) || !bytes.Equal(first.Value, value) {
				t.Fatal("later capture overwrote owned output")
			}
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			if d.MinPinnedSnapshotCommitSeq() != math.MaxUint64 {
				t.Fatal("owned successor leaked reader pin")
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(first.Value, value) {
				t.Fatal("Close invalidated owned pointer value")
			}
			if _, _, err := d.SeekGEOwnedAtRoot(root, key, nil); err == nil {
				t.Fatal("read accepted after Close")
			}
		})
	}
}

func TestOwnedBackendSuccessorPrimaryAndConcurrentCaptures(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.Set([]byte("key"), []byte("stable")); err != nil {
		t.Fatal(err)
	}
	held := d.AcquireSnapshot()
	if held == nil {
		t.Fatal("missing original capture")
	}
	defer held.Close()
	root := held.state.RootPageID
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				entry, found, err := d.SeekGEOwnedAtRoot(root, []byte("key"), []byte("kez"))
				if err != nil || !found || string(entry.Key) != "key" || string(entry.Value) != "stable" {
					t.Errorf("captured DATA: %q %q %t %v", entry.Key, entry.Value, found, err)
					return
				}
				entry.Key[0], entry.Value[0] = 'X', 'X'
			}
		}()
	}
	for i := range 20 {
		if err := d.Set([]byte("new"), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	entry, found, err := d.SeekGEOwnedAtRoot(0, []byte("new"), nil)
	if err != nil || !found || !bytes.Equal(entry.Value, []byte{19}) {
		t.Fatal(entry, found, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if d.MinPinnedSnapshotCommitSeq() != math.MaxUint64 {
		t.Fatal("concurrent successor leaked reader pin")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	var nilDB *DB
	if _, _, err := nilDB.SeekGEOwnedAtRoot(0, nil, nil); err != ErrClosed {
		t.Fatal(err)
	}
}
