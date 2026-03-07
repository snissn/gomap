package leafrefscan

import (
	"context"
	"fmt"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type GetFunc func(pageID uint64) ([]byte, error)
type VerifyFunc func(pageID uint64, n node.Node) error
type VisitFunc func(ptr page.ValuePtr) error

// Walk visits every leaf-ref pointer reachable from rootID.
func Walk(ctx context.Context, rootID uint64, get GetFunc, verify VerifyFunc, visit VisitFunc) error {
	if rootID == 0 {
		return nil
	}
	if get == nil {
		return fmt.Errorf("leafrefscan.Walk: get function is nil")
	}
	if visit == nil {
		return fmt.Errorf("leafrefscan.Walk: visit function is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ptr, ok := page.DecodeLeafRef(rootID); ok {
		return visit(ptr)
	}

	stack := make([]uint64, 0, 128)
	stack = append(stack, rootID)
	visited := make(map[uint64]struct{}, 1024)

	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		pageID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := visited[pageID]; ok {
			continue
		}
		visited[pageID] = struct{}{}

		data, err := get(pageID)
		if err != nil {
			return err
		}
		n := node.NewNodeView(data)
		if verify != nil {
			if err := verify(pageID, n); err != nil {
				return err
			}
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
					if err := visit(ptr); err != nil {
						return err
					}
					continue
				}
				stack = append(stack, childID)
			}
		default:
			return fmt.Errorf("invalid page type %d on page %d", n.Type(), pageID)
		}
	}
	return nil
}
