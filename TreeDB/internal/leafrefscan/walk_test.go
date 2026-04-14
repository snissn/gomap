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
	rootID, err := page.EncodeLeafRef(want)
	if err != nil {
		t.Fatalf("EncodeLeafRef: %v", err)
	}

	var got []page.LeafLogPtr
	err = Walk(context.Background(), rootID, func(uint64) ([]byte, error) {
		t.Fatal("get should not be called for root leafref")
		return nil, nil
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
	rootNode := node.NewNode(rootPage)
	rootNode.SetType(page.PageTypeInternal)
	rootNode.SetPageID(1)

	wantPtr := page.LeafLogPtr{FileID: 9, Offset: 77}
	leafRefID, err := page.EncodeLeafRef(wantPtr)
	if err != nil {
		t.Fatalf("EncodeLeafRef: %v", err)
	}
	if err := rootNode.AddInternalChild([]byte("a"), 10); err != nil {
		t.Fatalf("AddInternalChild regular: %v", err)
	}
	if err := rootNode.AddInternalChild([]byte("b"), leafRefID); err != nil {
		t.Fatalf("AddInternalChild leafref: %v", err)
	}

	pages := map[uint64][]byte{
		1:  rootPage,
		10: childLeaf,
	}
	var visited []page.LeafLogPtr
	err = Walk(context.Background(), 1, func(pageID uint64) ([]byte, error) {
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
