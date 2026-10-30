package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestNativeProjectionWorkspaceLending(t *testing.T) {
	owner, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	for i := 0; i < 16; i++ {
		if _, err := c.Insert([]byte(fmt.Sprintf("d%02d", i)), []byte(`{"content":"policy refund","tenant":"one"}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
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
	var charged uint64
	projectionCharges := 0
	deny := false
	catalog, err := newNativeTextCatalog(read, func(n uint64) error {
		if n == memtable.COWAllocationCharge(tree.OwnedPointerProjectionAllocationSize()) {
			if deny {
				return memtable.ErrCOWCapacity
			}
			projectionCharges++
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
	scope.singleTermFields = 1
	root := c.name + "/primary"
	first, err := scope.iterator(root, nil, nil, false, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	a := first.(*nativeTextIterator)
	if !a.cursor.borrowedProjection || !scope.projection.InUse() {
		t.Fatal("first cursor did not borrow operation storage")
	}
	physical := a.cursor.physical
	raw := first.UnsafeValue()
	saved := bytes.Clone(raw)
	second, err := scope.iterator(root, nil, nil, false, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	b := second.(*nativeTextIterator)
	if b.cursor.borrowedProjection || b.cursor.physical == physical || projectionCharges != 2 {
		t.Fatal("overlap did not admit independent storage")
	}
	second.Next()
	if !bytes.Equal(raw, saved) {
		t.Fatal("overlap changed value borrowed outside read mutex")
	}
	before := charged
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if scope.projection.InUse() || charged != before || a.cursor.physical != nil || a.readLeaf != nil {
		t.Fatal("Close retained aliases or refunded original admission")
	}
	point, err := scope.pointReader(root)
	if err != nil {
		t.Fatal(err)
	}
	defer point.close()
	value, ok, err := point.get([]byte("d00"))
	if err != nil || !ok || !bytes.Equal(value, saved) {
		t.Fatalf("point reuse: %q %v", value, err)
	}
	if !point.cursor.borrowedProjection || point.cursor.physical != physical || projectionCharges != 2 {
		t.Fatal("closed iterator storage was not reused by next phase")
	}
	second.Next()
	if !bytes.Equal(value, saved) {
		t.Fatal("point output aliases independently moving sibling")
	}
	if err := point.close(); err != nil {
		t.Fatal(err)
	}
	third, err := scope.iterator(root, nil, nil, false, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	visits := read.visits
	before = charged
	deny = true
	failed, err := scope.iterator(root, nil, nil, false, 0, nil, nil)
	deny = false
	if !errors.Is(err, memtable.ErrCOWCapacity) || failed != nil || read.visits != visits || charged <= before || !third.Valid() {
		t.Fatalf("overlap refusal err=%v visits=%d/%d", err, read.visits, visits)
	}
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if scope.projection.InUse() || read.visits != 0 {
		t.Fatal("loan/visit survived all cursor closes")
	}
	again, err := scope.iterator(root, []byte("d10"), nil, false, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(again.UnsafeKey()) != "d10" || again.(*nativeTextIterator).cursor.physical != physical {
		t.Fatal("reused root/bounds were not reinitialized")
	}
	if err := again.Close(); err != nil {
		t.Fatal(err)
	}
	// The first term can borrow the retained workspace. Refusal of the next
	// simultaneously live term must close the first, preserving primary error.
	scope.singleTermFields = 0
	for _, operator := range []TextSearchOperator{TextSearchOperatorAND, TextSearchOperatorOR} {
		deny = true
		_, err := c.searchNativeTextAtCatalog(TextSearchOptions{IndexName: "content", Query: "policy refund", TopK: 4, Operator: operator}, textSearchResultFull, read, TextSearchResponse{}, catalog)
		deny = false
		if !errors.Is(err, memtable.ErrCOWCapacity) || scope.projection.InUse() || read.visits != 0 {
			t.Fatalf("later-term refusal operator=%s err=%v loan=%v visits=%d", operator, err, scope.projection.InUse(), read.visits)
		}
	}
}
