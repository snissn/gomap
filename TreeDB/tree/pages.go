package tree

import (
	"errors"
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// WalkPages visits each page reachable from the Tree's current root exactly
// once. It is primarily intended for diagnostics, vacuuming, and tests.
func (t *Tree) WalkPages(fn func(pageID uint64, n node.Node) error) error {
	if t.rootPageID == 0 {
		return nil
	}

	stack := make([]uint64, 0, 128)
	stack = append(stack, t.rootPageID)

	visited := make(map[uint64]struct{}, 1024)

	for len(stack) > 0 {
		// pop
		pageID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if _, ok := visited[pageID]; ok {
			continue
		}
		visited[pageID] = struct{}{}

		data, err := t.pager.Get(pageID)
		if err != nil {
			return err
		}
		n := node.NewNode(data)
		if !t.pager.IsVerified(pageID) {
			if !n.VerifyChecksum() {
				return fmt.Errorf("checksum mismatch on page %d", pageID)
			}
			t.pager.MarkVerified(pageID)
		}

		if err := fn(pageID, *n); err != nil {
			return err
		}

		switch n.Type() {
		case page.PageTypeLeaf:
			// no children
		case page.PageTypeInternal:
			count := n.Count()
			for i := uint16(0); i < count; i++ {
				childID, err := n.GetInternalChildID(i)
				if err != nil {
					return err
				}
				stack = append(stack, childID)
			}
		default:
			return errors.New("invalid page type")
		}
	}

	return nil
}

// CollectPageIDs returns the set of page IDs reachable from the Tree root.
// The returned slice is sorted.
func (t *Tree) CollectPageIDs() ([]uint64, error) {
	out := make([]uint64, 0, 1024)
	if err := t.WalkPages(func(pageID uint64, _ node.Node) error {
		out = append(out, pageID)
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}
