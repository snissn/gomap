package caching

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type pageGetter interface {
	Get(pageID uint64) ([]byte, error)
}

func collectLeafRefValueLogLiveIDs(p pageGetter, rootID uint64, live map[uint32]struct{}) error {
	if p == nil || rootID == 0 || live == nil {
		return nil
	}
	if ptr, ok := page.DecodeLeafRef(rootID); ok {
		live[ptr.FileID] = struct{}{}
		return nil
	}

	stack := make([]uint64, 0, 128)
	stack = append(stack, rootID)
	visited := make(map[uint64]struct{}, 1024)

	for len(stack) > 0 {
		pageID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if _, ok := visited[pageID]; ok {
			continue
		}
		visited[pageID] = struct{}{}

		data, err := p.Get(pageID)
		if err != nil {
			return err
		}
		n := node.NewNodeView(data)
		if !n.VerifyChecksum() {
			return fmt.Errorf("checksum mismatch on page %d", pageID)
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
				if ptr, ok := page.DecodeLeafRef(childID); ok {
					live[ptr.FileID] = struct{}{}
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
