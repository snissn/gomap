package tree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

var ErrKeyNotFound = errors.New("key not found")

func compareTreeKey(a, b []byte) int {
	if len(a) == 8 && len(b) == 8 {
		av := binary.BigEndian.Uint64(a)
		bv := binary.BigEndian.Uint64(b)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	}
	return bytes.Compare(a, b)
}

type SlabReader interface {
	Read(ptr page.ValuePtr) ([]byte, error)
	ReadUnsafe(ptr page.ValuePtr) ([]byte, error)
}

type Tree struct {
	pager      *pager.Pager
	slabReader SlabReader
	rootPageID uint64
}

func New(p *pager.Pager, sr SlabReader, root uint64) *Tree {
	return &Tree{
		pager:      p,
		slabReader: sr,
		rootPageID: root,
	}
}

// Reset re-initializes the tree with new parameters for reuse.
func (t *Tree) Reset(p *pager.Pager, sr SlabReader, root uint64) {
	t.pager = p
	t.slabReader = sr
	t.rootPageID = root
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
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	for depth := 0; depth < 50; depth++ {
		// Use Get (mmap) instead of ReadPage (Copy)
		data, err := t.pager.Get(currID)
		if err != nil {
			return node.LeafEntry{}, err
		}

		n := node.NewNodeView(data) // VerifyChecksum is fast (CRC32C hardware accelerated).
		// We use Verified Cache to skip it if already checked.
		if verifyAlways || !t.pager.IsVerified(currID) {
			if !n.VerifyChecksum() {
				return node.LeafEntry{}, fmt.Errorf("checksum mismatch on page %d", currID)
			}
			if !verifyAlways {
				t.pager.MarkVerified(currID)
			}
		}

		switch n.Type() {
		case page.PageTypeInternal:
			if depth == 0 {
				if low, high, ok, err := n.InternalFenceBounds(); err != nil {
					return node.LeafEntry{}, err
				} else if ok {
					if len(low) > 0 && compareTreeKey(key, low) < 0 {
						return node.LeafEntry{}, ErrKeyNotFound
					}
					if len(high) > 0 && compareTreeKey(key, high) >= 0 {
						return node.LeafEntry{}, ErrKeyNotFound
					}
				}
			}
			childID, _, err := n.SearchInternalChildID(key)
			if err != nil {
				return node.LeafEntry{}, err
			}
			currID = childID

		case page.PageTypeLeaf:
			idx, found, err := n.SearchLeaf(key)
			if err != nil {
				return node.LeafEntry{}, err
			}
			if !found {
				return node.LeafEntry{}, ErrKeyNotFound
			}

			// Zero-copy view
			k, v, ptr, flags, err := n.GetLeafEntryView(idx)
			if err != nil {
				return node.LeafEntry{}, err
			}

			return node.LeafEntry{
				Key:      k,
				Value:    v,
				ValuePtr: ptr,
				Flags:    flags,
			}, nil

		default:
			return node.LeafEntry{}, fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
		}
	}
	return node.LeafEntry{}, errors.New("tree too deep")
}

func (t *Tree) GetUnsafe(key []byte) ([]byte, error) {
	currID := t.rootPageID
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	for depth := 0; depth < 50; depth++ {
		data, err := t.pager.Get(currID)
		if err != nil {
			return nil, err
		}

		n := node.NewNodeView(data)
		if verifyAlways || !t.pager.IsVerified(currID) {
			if !n.VerifyChecksum() {
				return nil, fmt.Errorf("checksum mismatch on page %d", currID)
			}
			if !verifyAlways {
				t.pager.MarkVerified(currID)
			}
		}

		switch n.Type() {
		case page.PageTypeInternal:
			if depth == 0 {
				if low, high, ok, err := n.InternalFenceBounds(); err != nil {
					return nil, err
				} else if ok {
					if len(low) > 0 && compareTreeKey(key, low) < 0 {
						return nil, ErrKeyNotFound
					}
					if len(high) > 0 && compareTreeKey(key, high) >= 0 {
						return nil, ErrKeyNotFound
					}
				}
			}
			childID, _, err := n.SearchInternalChildID(key)
			if err != nil {
				return nil, err
			}
			currID = childID

		case page.PageTypeLeaf:
			idx, found, err := n.SearchLeaf(key)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, ErrKeyNotFound
			}

			val, ptr, flags, err := n.GetLeafValueView(idx)
			if err != nil {
				return nil, err
			}
			if flags&node.FlagTombstone != 0 {
				return nil, ErrKeyNotFound
			}
			if flags&node.FlagPointer != 0 {
				out, err := t.slabReader.ReadUnsafe(ptr)
				if err != nil {
					return nil, err
				}
				return out, nil
			}
			return val, nil

		default:
			return nil, fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
		}
	}

	return nil, errors.New("tree too deep")
}

func (t *Tree) Get(key []byte) ([]byte, error) {
	val, err := t.GetUnsafe(key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	// Copy value before returning to user
	cpy := make([]byte, len(val))
	copy(cpy, val)
	return cpy, nil
}

func (t *Tree) Has(key []byte) (bool, error) {
	entry, err := t.GetEntry(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	return entry.Flags&node.FlagTombstone == 0, nil
}
