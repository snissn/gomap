package tree

import (
	"errors"
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
	if t.pager == nil {
		return errors.New("missing pager")
	}

	stack := make([]uint64, 0, 128)
	stack = append(stack, t.rootPageID)

	visited := make(map[uint64]struct{}, 1024)
	verifyAlways := t.pager.VerifyOnRead()

	for len(stack) > 0 {
		// pop
		pageID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// LeafRef ids encode value-log pointers for leaf pages; they are not
		// pager-backed pages and must not be returned to callers that retire/free
		// index.db page IDs.
		if _, ok := page.DecodeLeafRef(pageID); ok {
			continue
		}

		if _, ok := visited[pageID]; ok {
			continue
		}
		visited[pageID] = struct{}{}

		n, err := t.loadNodeView(pageID, verifyAlways)
		if err != nil {
			return err
		}

		if err := fn(pageID, n); err != nil {
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
				if _, ok := page.DecodeLeafRef(childID); ok {
					continue
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
