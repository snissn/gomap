package tree

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap-gemini/TreeDB/node"
	"github.com/snissn/gomap-gemini/TreeDB/page"
	"github.com/snissn/gomap-gemini/TreeDB/pager"
	"github.com/snissn/gomap-gemini/TreeDB/slab"
)

var ErrKeyNotFound = errors.New("key not found")

type Tree struct {
	pager       *pager.Pager
	slabManager *slab.SlabManager
	rootPageID  uint64
}

func New(p *pager.Pager, sm *slab.SlabManager, root uint64) *Tree {
	return &Tree{
		pager:       p,
		slabManager: sm,
		rootPageID:  root,
	}
}

// SetRoot updates the root page ID.
func (t *Tree) SetRoot(root uint64) {
	t.rootPageID = root
}

func (t *Tree) Get(key []byte) ([]byte, error) {
	currID := t.rootPageID
	
	// Max depth check to prevent infinite loops on cycles
	for depth := 0; depth < 50; depth++ {
		// Read Page
		data, err := t.pager.Get(currID)
		if err != nil {
			return nil, err
		}
		
		n := node.NewNode(data)
		if !n.VerifyChecksum() {
			return nil, fmt.Errorf("checksum mismatch on page %d", currID)
		}
		
		switch n.Type() {
		case page.PageTypeInternal:
			idx, _ := n.SearchInternal(key)
			// SearchInternal returns the index of the child to follow
			
			entry, err := n.GetInternalEntry(idx)
			if err != nil {
				return nil, err
			}
			currID = entry.ChildPageID
			
		case page.PageTypeLeaf:
			idx, found := n.SearchLeaf(key)
			if !found {
				return nil, ErrKeyNotFound
			}
			
			entry, err := n.GetLeafEntry(idx)
			if err != nil {
				return nil, err
			}
			
			if entry.Flags & node.FlagTombstone != 0 {
				return nil, ErrKeyNotFound
			}
			
			if entry.Flags & node.FlagPointer != 0 {
				// Fetch from slab
				val, err := t.slabManager.Read(entry.ValuePtr)
				if err != nil {
					return nil, err
				}
				return val, nil
			}
			
			// Inline
			return entry.Value, nil
			
		default:
			return nil, fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
		}
	}
	
	return nil, errors.New("tree too deep (cycle detection)")
}
