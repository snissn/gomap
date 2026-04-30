package leafrefscan

import (
	"context"
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type GetFunc func(pageID uint64) ([]byte, error)
type VerifyFunc func(pageID uint64, n node.Node) error
type VisitFunc func(ptr page.LeafLogPtr) error

func walkRootsCommon(ctx context.Context, rootIDs []uint64, get GetFunc, verify VerifyFunc, visit VisitFunc, state *walkState) error {
	if len(rootIDs) == 0 {
		return nil
	}
	if get == nil {
		return fmt.Errorf("leafrefscan: get function is nil")
	}
	if visit == nil {
		return fmt.Errorf("leafrefscan: visit function is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if state == nil {
		state = getWalkState()
		defer putWalkState(state)
	} else {
		state.stack = state.stack[:0]
		clear(state.visited)
	}
	stack := state.stack
	visited := state.visited
	for _, rootID := range rootIDs {
		if rootID == 0 {
			continue
		}
		stack = append(stack, rootID)
	}

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
			if err := n.WalkInternalChildren(&stack, visit); err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid page type %d on page %d", n.Type(), pageID)
		}
	}
	state.stack = stack
	return nil
}

type walkState struct {
	stack   []uint64
	visited map[uint64]struct{}
}

var walkStatePool = sync.Pool{
	New: func() any {
		return &walkState{
			stack:   make([]uint64, 0, 128),
			visited: make(map[uint64]struct{}, 1024),
		}
	},
}

func getWalkState() *walkState {
	if v := walkStatePool.Get(); v != nil {
		if s, ok := v.(*walkState); ok && s != nil {
			s.stack = s.stack[:0]
			clear(s.visited)
			return s
		}
	}
	return &walkState{
		stack:   make([]uint64, 0, 128),
		visited: make(map[uint64]struct{}, 1024),
	}
}

func putWalkState(s *walkState) {
	if s == nil {
		return
	}
	if cap(s.stack) > 1<<16 {
		s.stack = make([]uint64, 0, 128)
	} else {
		s.stack = s.stack[:0]
	}
	// Avoid retaining pathological maps; keep the common-size cache hot.
	if len(s.visited) > 1<<15 {
		s.visited = make(map[uint64]struct{}, 1024)
	} else {
		clear(s.visited)
	}
	walkStatePool.Put(s)
}

// Walk visits every leaf-ref pointer reachable from rootID.
func Walk(ctx context.Context, rootID uint64, get GetFunc, verify VerifyFunc, visit VisitFunc) error {
	if rootID == 0 {
		return nil
	}
	return WalkRoots(ctx, []uint64{rootID}, get, verify, visit)
}

// WalkRoots visits every leaf-ref pointer reachable from the provided roots.
// Pager-backed pages are deduplicated across the whole root set.
func WalkRoots(ctx context.Context, rootIDs []uint64, get GetFunc, verify VerifyFunc, visit VisitFunc) error {
	return walkRootsCommon(ctx, rootIDs, get, verify, visit, nil)
}
