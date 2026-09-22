package tree

import (
	"errors"
	"sort"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// WalkPages visits each pager-backed page reachable from the Tree's current
// root exactly once. Leaf pages stored in the value log (typed leaf-log child
// refs) are
// skipped. It is primarily intended for diagnostics, vacuuming, and tests.
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
			if err := n.WalkInternalChildren(&stack, nil); err != nil {
				return err
			}
		default:
			return errors.New("invalid page type")
		}
	}

	return nil
}

// CollectPageIDs returns the sorted set of pager-backed page IDs reachable from
// the Tree root. Leaf pages stored in the value log are excluded.
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
