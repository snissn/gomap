package collections

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestNativeTextPointReaderCapturedMerge(t *testing.T) {
	owner, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	for i := 0; i < 80; i++ {
		id := fmt.Sprintf("d-%04d", i*16)
		doc := []byte(fmt.Sprintf(`{"content":%q,"tenant":"one"}`, "old policy "+strings.Repeat("payload ", i%3*100)))
		if _, err := c.Insert([]byte(id), doc); err != nil {
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
	if _, err := c.UpdateBatch([]UpdateBatchItem{{DocumentID: []byte("d-0016"), Update: func([]byte) ([]byte, bool, error) {
		return []byte(`{"content":"new policy","tenant":"one"}`), true, nil
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete([]byte("d-0032")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Insert([]byte("d-0024"), []byte(`{"content":"new middle policy","tenant":"one"}`)); err != nil {
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
	for _, r := range []*NativeJSONRead{old, current} {
		func() {
			lease, err := r.capture.NativeAllocator().AdmitNativeBytes(0)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			var charged uint64
			admit := func(n uint64) error {
				if n > ^uint64(0)-charged {
					return memtable.ErrCOWCapacity
				}
				if err := lease.Resize(charged + n); err != nil {
					return err
				}
				charged += n
				return nil
			}
			scope := nativeTextCatalog{read: r, admit: admit}
			points, err := scope.pointReader(c.name + "/primary")
			if err != nil || points == nil {
				t.Fatal("point reader construction", err)
			}
			defer points.close()
			// Include sparse absent points, all logical effect kinds, distant
			// leaves, exhaustion and backward repositioning on the same owner.
			for _, key := range []string{"d-0000", "d-0001", "d-0016", "d-0024", "d-0032", "d-0048", "d-0500", "d-1264", "z", "d-0016", "d-0000"} {
				want, found, err := scope.get(c.name+"/primary", []byte(key), nil)
				if err != nil {
					t.Fatal(err)
				}
				got, ok, err := points.get([]byte(key))
				if err != nil || ok != found || !bytes.Equal(got, want) {
					t.Fatalf("key%q got=%q want=%q found=%v/%v err=%v", key, got, want, ok, found, err)
				}
			}
			if err := points.close(); err != nil || points.scope != nil || points.readLeaf != nil || points.cursor.read.domain != nil || points.cursor.physical != nil {
				t.Fatal("point reader retained aliases after close", err)
			}
			if _, _, err := points.get([]byte("d-0000")); !errors.Is(err, backenddb.ErrClosed) {
				t.Fatal("closed point reader", err)
			}
			// Refuse wrapper storage before construction and physical storage
			// before its first read. The original enclosing cut remains usable.
			blocked := nativeTextCatalog{read: r, admit: func(uint64) error { return memtable.ErrCOWCapacity }}
			if p, err := blocked.pointReader(c.name + "/primary"); !errors.Is(err, memtable.ErrCOWCapacity) || p != nil {
				t.Fatal("unadmitted point reader", err)
			}
			p, err := scope.pointReader(c.name + "/primary")
			if err != nil {
				t.Fatal(err)
			}
			defer p.close()
			p.scope = &blocked
			if _, _, err := p.get([]byte("d-1264")); !errors.Is(err, memtable.ErrCOWCapacity) || p.cursor.physical != nil {
				t.Fatal("unadmitted physical point reader", err)
			}
			closedRead, err := scope.pointReader(c.name + "/primary")
			if err != nil {
				t.Fatal(err)
			}
			defer closedRead.close()
			if _, ok, err := closedRead.get([]byte("d-1264")); err != nil || !ok {
				t.Fatal("populate physical point before closing read", err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := closedRead.get([]byte("d-1264")); !errors.Is(err, backenddb.ErrClosed) {
				t.Fatal("cached point survived enclosing read close", err)
			}
		}()
	}
}
