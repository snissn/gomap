package collections

import (
	"fmt"
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
