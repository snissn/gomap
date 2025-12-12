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

// GetEntry returns the raw leaf entry (useful for compaction/CAS).
// CAUTION: Returned entry Key/Value might point directly to mmap memory.
// Do not modify or hold reference for long.
func (t *Tree) GetEntry(key []byte) (node.LeafEntry, error) {
	currID := t.rootPageID
	
	for depth := 0; depth < 50; depth++ {
		// Use ReadPage (Copy) instead of Get
		data, err := t.pager.ReadPage(currID)
		if err != nil {
			return node.LeafEntry{}, err
		}
		
		n := node.NewNode(data)
		// VerifyChecksum is fast (CRC32C hardware accelerated).
		// We still verify it to detect corruption.
		if !n.VerifyChecksum() {
			return node.LeafEntry{}, fmt.Errorf("checksum mismatch on page %d", currID)
		}
		
		switch n.Type() {
		case page.PageTypeInternal:
			idx, _ := n.SearchInternal(key)
			entry, err := n.GetInternalEntry(idx)
			if err != nil {
				return node.LeafEntry{}, err
			}
			currID = entry.ChildPageID
			
		case page.PageTypeLeaf:
			idx, found := n.SearchLeaf(key)
			if !found {
				return node.LeafEntry{}, ErrKeyNotFound
			}
			return n.GetLeafEntry(idx)
			
		default:
			return node.LeafEntry{}, fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
		}
	}
	return node.LeafEntry{}, errors.New("tree too deep")
}

func (t *Tree) Get(key []byte) ([]byte, error) {
	entry, err := t.GetEntry(key)
	if err != nil {
		return nil, err
	}
	
	if entry.Flags & node.FlagTombstone != 0 {
		return nil, ErrKeyNotFound
	}
	
	if entry.Flags & node.FlagPointer != 0 {
		val, err := t.slabManager.Read(entry.ValuePtr)
		if err != nil {
			return nil, err
		}
		return val, nil
	}
	
	// Copy value before returning to user
	val := make([]byte, len(entry.Value))
	copy(val, entry.Value)
	return val, nil
}
