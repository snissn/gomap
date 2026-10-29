package collections

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/node"
	"reflect"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestNativeTextIteratorSuccessorBacking(t *testing.T) {
	owner, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	var ids []string
	for i, size := range []int{15, 16, 31, 32, 63, 64} {
		id := fmt.Sprintf("%03d", i) + strings.Repeat("x", size-3)
		ids = append(ids, id)
		if _, err := c.Insert([]byte(id), []byte(`{"content":"policy","tenant":"one"}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	old, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if err := c.Delete([]byte(ids[3])); err != nil {
		t.Fatal(err)
	}
	current, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for _, read := range []*NativeJSONRead{old, current} {
		for _, deleted := range []bool{false, true} {
			func() {
				lease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
				if err != nil {
					t.Fatal(err)
				}
				defer lease.Close()
				var charged uint64
				scope := nativeTextCatalog{read: read, admit: func(n uint64) error {
					if n > ^uint64(0)-charged {
						return memtable.ErrCOWCapacity
					}
					if err := lease.Resize(charged + n); err != nil {
						return err
					}
					charged += n
					return nil
				}}
				it, err := scope.iterator(c.name+"/primary", nil, nil, deleted, 0, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer it.Close()
				var got, want []string
				for _, id := range ids {
					if read == old || deleted || id != ids[3] {
						want = append(want, id)
					}
				}
				for it.Valid() {
					key := it.UnsafeKey()
					if cap(key) < len(key)+1 {
						t.Fatalf("successor would need another backing: len=%d cap=%d", len(key), cap(key))
					}
					got = append(got, string(key))
					it.Next()
				}
				if err := it.Error(); err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("captured iterator got=%q want=%q err=%v", got, want, err)
				}
				if err := it.Close(); err != nil {
					t.Fatal(err)
				}
				lease.Close()
			}()
		}
	}
}

func TestNativeTextScanWorkspace(t *testing.T) {
	owner, manager, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	ids := []string{"00" + strings.Repeat("x", 62), "01" + strings.Repeat("x", 62), "02", "03" + strings.Repeat("x", 126), "04"}
	for i, id := range ids {
		if _, err := c.Insert([]byte(id), []byte(fmt.Sprintf(`{"content":"policy %d","tenant":"one"}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	budget := read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	defer func() {
		lease.Close()
		if err := read.Close(); err != nil {
			t.Error(err)
		}
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
		if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
			t.Errorf("workspace full Close retained receipts: %+v", stats)
		}
	}()
	var charged, refusedCharge uint64
	catalog, err := newNativeTextCatalog(read, func(n uint64) error {
		if refusedCharge != 0 && n == refusedCharge {
			return memtable.ErrCOWCapacity
		}
		if n > ^uint64(0)-charged {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charged + n); err != nil {
			return err
		}
		charged += n
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := catalog.nativeText
	root := c.name + "/primary"
	t.Run("generalCallback", func(t *testing.T) {
		entry, err := scope.entry(root, []byte(ids[0]))
		if err != nil {
			t.Fatal(err)
		}
		var value []byte
		allocs := testing.AllocsPerRun(20, func() {
			value, err = scope.value(node.LeafEntry{Value: entry.Value, Flags: node.FlagInline}, root, entry.Key)
			if err != nil {
				panic(err)
			}
		})
		if !bytes.Equal(value, entry.Value) {
			t.Fatal("value changed")
		}
		// The independent source copy remains; the general callback is catalog-owned.
		if allocs > 1 {
			t.Fatalf("general value allocated %.0f objects; want only the independent value copy", allocs)
		}
	})
	t.Run("callbackRefusal", func(t *testing.T) {
		private := nativeTextCatalog{read: read, admit: func(uint64) error { return memtable.ErrCOWCapacity }}
		before := budget.Stats()
		_, err := private.value(node.LeafEntry{Value: []byte("value"), Flags: node.FlagInline}, root, nil)
		after := budget.Stats()
		if !errors.Is(err, memtable.ErrCOWCapacity) || private.recordAdmit != nil || after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
			t.Fatalf("callback refusal err=%v before=%+v after=%+v", err, before, after)
		}
	})
	t.Run("growthRefusal", func(t *testing.T) {
		it, err := scope.iterator(root, nil, nil, false, 0, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()
		it.Next()
		it.Next()
		if !it.Valid() || string(it.UnsafeKey()) != ids[2] {
			t.Fatal("missing pre-growth key")
		}
		workspace := it.(*nativeTextIterator).keyWorkspace
		beforeCharge, before := charged, budget.Stats()
		refusedCharge = memtable.COWAllocationCharge(uint64(len(ids[3]) + 1))
		it.Next()
		refusedCharge = 0
		after := budget.Stats()
		current := it.(*nativeTextIterator).keyWorkspace
		if it.Valid() || !errors.Is(it.Error(), memtable.ErrCOWCapacity) || charged != beforeCharge || cap(current) != cap(workspace) || &current[0] != &workspace[0] || after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
			t.Fatalf("growth refusal err=%v charges=%d/%d before=%+v after=%+v", it.Error(), beforeCharge, charged, before, after)
		}
	})

	t.Run("keyBacking", func(t *testing.T) {
		var admittedKeys int
		scope.admitKey = func(uint64) error { admittedKeys++; return nil }
		defer func() { scope.admitKey = nil }()
		it, err := scope.iterator(root, nil, nil, false, 0, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()
		original := &it.UnsafeKey()[0]
		savedKey, savedValue := it.KeyCopy(nil), it.UnsafeValue()
		firstValue := bytes.Clone(savedValue)
		it.Next()
		if !it.Valid() || string(it.UnsafeKey()) != ids[1] {
			t.Fatalf("next: key=%q err=%v", it.UnsafeKey(), it.Error())
		}
		if &it.UnsafeKey()[0] != original {
			t.Fatal("same-sized next key allocated another backing")
		}
		it.Next()
		if !it.Valid() || string(it.UnsafeKey()) != ids[2] || &it.UnsafeKey()[0] != original {
			t.Fatal("smaller key did not reuse backing")
		}
		it.Next()
		if !it.Valid() || string(it.UnsafeKey()) != ids[3] {
			t.Fatal("larger key missing")
		}
		larger := &it.UnsafeKey()[0]
		if larger == original {
			t.Fatal("larger key did not grow")
		}
		// Arbitrary backwards Seek and an aliased target preserve the input until merge ends.
		it.Seek([]byte(ids[0]))
		if !it.Valid() || &it.UnsafeKey()[0] != larger {
			t.Fatal("backward Seek did not reuse larger workspace")
		}
		it.Seek(it.UnsafeKey())
		if !it.Valid() || string(it.UnsafeKey()) != ids[0] {
			t.Fatal("aliased Seek changed target")
		}
		if admittedKeys != 6 {
			t.Fatalf("rewrite key cohorts admitted %d times, want 6", admittedKeys)
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
		closed := it.(*nativeTextIterator)
		if closed.scope != nil || closed.readLeaf != nil || closed.keyWorkspace != nil {
			t.Fatal("closed iterator retained workspace/owner")
		}
		if !bytes.Equal(savedKey, []byte(ids[0])) || !bytes.Equal(savedValue, firstValue) {
			t.Fatal("caller-owned key/retained value changed after movement/Close")
		}
	})
}
