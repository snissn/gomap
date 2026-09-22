package node

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestInternalBaseDeltaEmptyFirstKeyRoundTrip(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(10)
	if err := b.AddInternalChild([]byte{}, 100); err != nil {
		t.Fatalf("AddInternalChild(empty): %v", err)
	}
	if err := b.AddInternalChild([]byte("m"), 101); err != nil {
		t.Fatalf("AddInternalChild(m): %v", err)
	}
	n := b.Finish()
	if !n.InternalBaseDeltaEnabled() {
		t.Fatalf("internal base-delta not enabled")
	}

	key, child, err := n.GetInternalEntryRefView(0)
	if err != nil {
		t.Fatalf("GetInternalEntryRefView(0): %v", err)
	}
	if key == nil || len(key) != 0 {
		t.Fatalf("first key=%v len=%d, want non-nil empty", key, len(key))
	}
	if child.Kind != page.ChildRefPage || child.Page != 100 {
		t.Fatalf("first child=%+v want page 100", child)
	}

	got, found, err := n.SearchInternalChildID([]byte{})
	if err != nil {
		t.Fatalf("SearchInternalChildID(empty): %v", err)
	}
	if !found || got != 100 {
		t.Fatalf("SearchInternalChildID(empty)=(%d,%v), want (100,true)", got, found)
	}
}
