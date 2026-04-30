package leafrefscan

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestWalk_ZeroRootIsNoOp(t *testing.T) {
	if err := Walk(context.Background(), 0, nil, nil, nil); err != nil {
		t.Fatalf("Walk(zero root): %v", err)
	}
}

func TestWalk_NilGetReturnsError(t *testing.T) {
	err := Walk(context.Background(), 1, nil, nil, func(page.LeafLogPtr) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "get function is nil") {
		t.Fatalf("Walk(nil get) err=%v, want nil-get error", err)
	}
}

func TestWalk_NilVisitReturnsError(t *testing.T) {
	err := Walk(context.Background(), 1, func(uint64) ([]byte, error) { return nil, nil }, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "visit function is nil") {
		t.Fatalf("Walk(nil visit) err=%v, want nil-visit error", err)
	}
}

func TestWalk_RootLeafRefVisitsPointer(t *testing.T) {
	want := page.LeafLogPtr{FileID: 7, Offset: 42}
	rootPage := make([]byte, page.PageSize)
	b := node.NewBuilder(rootPage, page.PageTypeInternal)
	b.SetPageID(1)
	if err := b.AddInternalChildRef([]byte{}, page.LeafLogChildRef(want)); err != nil {
		t.Fatalf("AddInternalChildRef: %v", err)
	}
	b.FinishNoNode()

	var got []page.LeafLogPtr
	err := Walk(context.Background(), 1, func(pageID uint64) ([]byte, error) {
		if pageID != 1 {
			t.Fatalf("get page=%d want 1", pageID)
		}
		return rootPage, nil
	}, nil, func(ptr page.LeafLogPtr) error {
		got = append(got, ptr)
		return nil
	})
	if err != nil {
		t.Fatalf("Walk(root leafref): %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("visited=%v want one pointer", got)
	}
	if got[0].FileID != want.FileID || got[0].Offset != want.Offset {
		t.Fatalf("visited=%v want file=%d offset=%d", got, want.FileID, want.Offset)
	}
}

func TestWalk_InternalMixedChildrenVisitsOnlyLeafRefs(t *testing.T) {
	childLeaf := make([]byte, page.PageSize)
	leafNode := node.NewNode(childLeaf)
	leafNode.SetType(page.PageTypeLeaf)
	leafNode.SetPageID(10)

	rootPage := make([]byte, page.PageSize)
	wantPtr := page.LeafLogPtr{FileID: 9, Offset: 77}
	b := node.NewBuilder(rootPage, page.PageTypeInternal)
	b.SetPageID(1)
	if err := b.AddInternalChildRef([]byte("b"), page.LeafLogChildRef(wantPtr)); err != nil {
		t.Fatalf("AddInternalChildRef leafref: %v", err)
	}
	b.FinishNoNode()

	pages := map[uint64][]byte{
		1: rootPage,
	}
	var visited []page.LeafLogPtr
	err := Walk(context.Background(), 1, func(pageID uint64) ([]byte, error) {
		data, ok := pages[pageID]
		if !ok {
			return nil, errors.New("missing page")
		}
		return data, nil
	}, nil, func(ptr page.LeafLogPtr) error {
		visited = append(visited, ptr)
		return nil
	})
	if err != nil {
		t.Fatalf("Walk(mixed children): %v", err)
	}
	if len(visited) != 1 {
		t.Fatalf("visited=%v want one pointer", visited)
	}
	if visited[0].FileID != wantPtr.FileID || visited[0].Offset != wantPtr.Offset {
		t.Fatalf("visited=%v want file=%d offset=%d", visited, wantPtr.FileID, wantPtr.Offset)
	}
}

func TestWalk_InvalidPageTypeReturnsError(t *testing.T) {
	rootPage := make([]byte, page.PageSize)
	rootNode := node.NewNode(rootPage)
	rootNode.SetType(page.PageType(99))
	rootNode.SetPageID(1)

	err := Walk(context.Background(), 1, func(pageID uint64) ([]byte, error) {
		if pageID != 1 {
			t.Fatalf("unexpected pageID %d", pageID)
		}
		return rootPage, nil
	}, nil, func(page.LeafLogPtr) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "invalid page type") {
		t.Fatalf("Walk(invalid type) err=%v want invalid page type", err)
	}
}

func TestWalkRoots_MultipleRootsDeduplicatesPagerPages(t *testing.T) {
	leafPage := make([]byte, page.PageSize)
	leafNode := node.NewNode(leafPage)
	leafNode.SetType(page.PageTypeLeaf)
	leafNode.SetPageID(10)

	root1Page := make([]byte, page.PageSize)
	root2Page := make([]byte, page.PageSize)

	ptr1 := page.LeafLogPtr{FileID: 9, Offset: 77}
	ptr2 := page.LeafLogPtr{FileID: 9, Offset: 88}
	b1 := node.NewBuilder(root1Page, page.PageTypeInternal)
	b1.SetPageID(1)
	if err := b1.AddInternalChildRef([]byte("b"), page.LeafLogChildRef(ptr1)); err != nil {
		t.Fatalf("root1 AddInternalChildRef leafref: %v", err)
	}
	b1.FinishNoNode()
	b2 := node.NewBuilder(root2Page, page.PageTypeInternal)
	b2.SetPageID(2)
	if err := b2.AddInternalChildRef([]byte("b"), page.LeafLogChildRef(ptr2)); err != nil {
		t.Fatalf("root2 AddInternalChildRef leafref: %v", err)
	}
	b2.FinishNoNode()

	pages := map[uint64][]byte{1: root1Page, 2: root2Page, 10: leafPage}
	getCalls := 0
	var visited []page.LeafLogPtr
	err := WalkRoots(context.Background(), []uint64{1, 2}, func(pageID uint64) ([]byte, error) {
		getCalls++
		data, ok := pages[pageID]
		if !ok {
			return nil, errors.New("missing page")
		}
		return data, nil
	}, nil, func(ptr page.LeafLogPtr) error {
		visited = append(visited, ptr)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkRoots: %v", err)
	}
	if got, want := getCalls, 2; got != want {
		t.Fatalf("get calls=%d want %d", got, want)
	}
	if got, want := len(visited), 2; got != want {
		t.Fatalf("visited=%v want %d pointers", visited, want)
	}
	if visited[0] != ptr2 && visited[1] != ptr2 {
		t.Fatalf("visited=%v missing ptr2 %+v", visited, ptr2)
	}
	if visited[0] != ptr1 && visited[1] != ptr1 {
		t.Fatalf("visited=%v missing ptr1 %+v", visited, ptr1)
	}
}
