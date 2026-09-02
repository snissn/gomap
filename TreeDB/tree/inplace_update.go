package tree

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// UpdateValuePtrInPlace rewrites a pointer value in-place inside the current
// tree by updating the leaf page that contains key.
//
// This is intended for maintenance operations (e.g. value-log compaction) that want
// to avoid full copy-on-write path rewrites. Callers must ensure snapshot safety
// (i.e. no readers are pinned to the current index generation) before using it.
//
// Returns (updated=true, leafPageID) when the entry existed as a pointer and
// matched oldPtr. If the key is missing or has changed since oldPtr, it returns
// updated=false with a nil error.
func (t *Tree) UpdateValuePtrInPlace(key []byte, oldPtr, newPtr page.ValuePtr) (updated bool, leafPageID uint64, err error) {
	if t == nil || t.pager == nil {
		return false, 0, errors.New("missing pager")
	}

	currRef := page.PageChildRef(t.rootPageID)
	verifyAlways := t.pager.VerifyOnRead()

	for depth := 0; depth < maxTraversalDepth; depth++ {
		if currRef.Kind == page.ChildRefLeafLog {
			return false, 0, errors.New("cannot update value pointer in-place with value-log-backed leaf pages")
		}
		currID := currRef.Page
		data, err := t.pager.Get(currID)
		if err != nil {
			return false, 0, err
		}

		n := node.NewNode(data)
		if verifyAlways || !t.pager.IsVerified(currID) {
			if !n.VerifyChecksum() {
				return false, 0, fmt.Errorf("checksum mismatch on page %d", currID)
			}
			if !verifyAlways {
				t.pager.MarkVerified(currID)
			}
		}

		switch n.Type() {
		case page.PageTypeInternal:
			idx, _ := n.SearchInternal(key)
			childRef, err := n.GetInternalChildRef(idx)
			if err != nil {
				return false, 0, err
			}
			currRef = childRef

		case page.PageTypeLeaf:
			idx, found, err := n.SearchLeaf(key)
			if err != nil {
				return false, 0, err
			}
			if !found {
				return false, 0, nil
			}

			_, _, curPtr, flags, err := n.GetLeafEntryView(idx)
			if err != nil {
				return false, 0, err
			}
			if flags&node.FlagTombstone != 0 || flags&node.FlagPointer == 0 || curPtr != oldPtr {
				return false, 0, nil
			}

			wData, err := t.pager.GetForWrite(currID)
			if err != nil {
				return false, 0, err
			}
			wn := node.NewNode(wData)

			// Re-search defensively in case of unexpected inconsistencies.
			wIdx, wFound, err := wn.SearchLeaf(key)
			if err != nil {
				return false, 0, err
			}
			if !wFound {
				return false, 0, nil
			}

			updated, err := wn.UpdateLeafValuePtr(wIdx, oldPtr, newPtr)
			if err != nil {
				return false, 0, err
			}
			if !updated {
				return false, 0, nil
			}

			// We wrote the checksum, so the page is verified by construction.
			t.pager.MarkVerified(currID)
			return true, currID, nil

		default:
			return false, 0, fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
		}
	}

	return false, 0, errors.New("tree too deep")
}
