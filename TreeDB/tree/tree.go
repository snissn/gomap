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

// Optional fast path for append-style pointer reads that can reuse caller
// buffers and avoid extra allocations.
type slabUnsafeAppender interface {
	ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error)
}

// Optional fast path for batched append-style pointer reads.
type slabUnsafeBatchAppender interface {
	ReadUnsafeAppendBatch(ptrs []page.ValuePtr, dst [][]byte) ([][]byte, error)
}

// Optional key-aware pointer reads for outer-leaf block payloads.
type slabUnsafeKeyReader interface {
	ReadUnsafeForKey(ptr page.ValuePtr, key []byte) ([]byte, error)
}

// Optional fence-key lookup reads for fence-only outer-leaf mode.
// found=false indicates the key does not exist in the referenced block.
type slabUnsafeFenceKeyReader interface {
	ReadUnsafeFenceForKey(ptr page.ValuePtr, key []byte) ([]byte, bool, error)
}

// Optional key-aware append-style pointer reads.
type slabUnsafeKeyAppender interface {
	ReadUnsafeAppendForKey(ptr page.ValuePtr, key []byte, dst []byte) ([]byte, error)
}

// Optional key-aware batched append-style pointer reads.
type slabUnsafeKeyBatchAppender interface {
	ReadUnsafeAppendBatchForKeys(ptrs []page.ValuePtr, keys [][]byte, dst [][]byte) ([][]byte, error)
}

type Tree struct {
	pager           *pager.Pager
	slabReader      SlabReader
	slabAppender    slabUnsafeAppender
	slabKeyReader   slabUnsafeKeyReader
	slabFenceReader slabUnsafeFenceKeyReader
	slabKeyAppender slabUnsafeKeyAppender
	rootPageID      uint64
}

func New(p *pager.Pager, sr SlabReader, root uint64) *Tree {
	t := &Tree{
		pager:      p,
		slabReader: sr,
		rootPageID: root,
	}
	if app, ok := sr.(slabUnsafeAppender); ok {
		t.slabAppender = app
	}
	if keyReader, ok := sr.(slabUnsafeKeyReader); ok {
		t.slabKeyReader = keyReader
	}
	if fenceReader, ok := sr.(slabUnsafeFenceKeyReader); ok {
		t.slabFenceReader = fenceReader
	}
	if keyAppender, ok := sr.(slabUnsafeKeyAppender); ok {
		t.slabKeyAppender = keyAppender
	}
	return t
}

// Reset re-initializes the tree with new parameters for reuse.
func (t *Tree) Reset(p *pager.Pager, sr SlabReader, root uint64) {
	t.pager = p
	t.slabReader = sr
	if app, ok := sr.(slabUnsafeAppender); ok {
		t.slabAppender = app
	} else {
		t.slabAppender = nil
	}
	if keyReader, ok := sr.(slabUnsafeKeyReader); ok {
		t.slabKeyReader = keyReader
	} else {
		t.slabKeyReader = nil
	}
	if fenceReader, ok := sr.(slabUnsafeFenceKeyReader); ok {
		t.slabFenceReader = fenceReader
	} else {
		t.slabFenceReader = nil
	}
	if keyAppender, ok := sr.(slabUnsafeKeyAppender); ok {
		t.slabKeyAppender = keyAppender
	} else {
		t.slabKeyAppender = nil
	}
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
				if val, ok, err := t.lookupFenceValueView(&n, idx, key); err != nil {
					return node.LeafEntry{}, err
				} else if ok {
					return node.LeafEntry{
						Key:   append([]byte(nil), key...),
						Value: val,
						Flags: 0,
					}, nil
				}
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

func (t *Tree) lookupFenceValueView(n *node.Node, idx uint16, key []byte) ([]byte, bool, error) {
	if t.slabFenceReader == nil || n == nil || idx == 0 {
		return nil, false, nil
	}
	_, _, ptr, flags, err := n.GetLeafEntryView(idx - 1)
	if err != nil {
		return nil, false, err
	}
	if flags&node.FlagTombstone != 0 || flags&node.FlagPointer == 0 {
		return nil, false, nil
	}
	val, found, err := t.slabFenceReader.ReadUnsafeFenceForKey(ptr, key)
	if err != nil {
		return nil, false, err
	}
	return val, found, nil
}

func (t *Tree) lookupLeafValueView(key []byte) ([]byte, page.ValuePtr, byte, error) {
	currID := t.rootPageID
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	for depth := 0; depth < 50; depth++ {
		data, err := t.pager.Get(currID)
		if err != nil {
			return nil, page.ValuePtr{}, 0, err
		}

		n := node.NewNodeView(data)
		if verifyAlways || !t.pager.IsVerified(currID) {
			if !n.VerifyChecksum() {
				return nil, page.ValuePtr{}, 0, fmt.Errorf("checksum mismatch on page %d", currID)
			}
			if !verifyAlways {
				t.pager.MarkVerified(currID)
			}
		}

		switch n.Type() {
		case page.PageTypeInternal:
			if depth == 0 {
				if low, high, ok, err := n.InternalFenceBounds(); err != nil {
					return nil, page.ValuePtr{}, 0, err
				} else if ok {
					if len(low) > 0 && compareTreeKey(key, low) < 0 {
						return nil, page.ValuePtr{}, 0, ErrKeyNotFound
					}
					if len(high) > 0 && compareTreeKey(key, high) >= 0 {
						return nil, page.ValuePtr{}, 0, ErrKeyNotFound
					}
				}
			}
			childID, _, err := n.SearchInternalChildID(key)
			if err != nil {
				return nil, page.ValuePtr{}, 0, err
			}
			currID = childID

		case page.PageTypeLeaf:
			idx, found, err := n.SearchLeaf(key)
			if err != nil {
				return nil, page.ValuePtr{}, 0, err
			}
			if !found {
				if val, ok, err := t.lookupFenceValueView(&n, idx, key); err != nil {
					return nil, page.ValuePtr{}, 0, err
				} else if ok {
					// Fence-only mode resolves user keys from outer blocks.
					// Return as inline to avoid a second pointer lookup in callers.
					return val, page.ValuePtr{}, 0, nil
				}
				return nil, page.ValuePtr{}, 0, ErrKeyNotFound
			}

			val, ptr, flags, err := n.GetLeafValueView(idx)
			if err != nil {
				return nil, page.ValuePtr{}, 0, err
			}
			return val, ptr, flags, nil

		default:
			return nil, page.ValuePtr{}, 0, fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
		}
	}

	return nil, page.ValuePtr{}, 0, errors.New("tree too deep")
}

func (t *Tree) GetUnsafe(key []byte) ([]byte, error) {
	val, ptr, flags, err := t.lookupLeafValueView(key)
	if err != nil {
		return nil, err
	}
	if flags&node.FlagTombstone != 0 {
		return nil, ErrKeyNotFound
	}
	if flags&node.FlagPointer != 0 {
		if t.slabKeyReader != nil {
			out, err := t.slabKeyReader.ReadUnsafeForKey(ptr, key)
			if err != nil {
				return nil, err
			}
			return out, nil
		}
		out, err := t.slabReader.ReadUnsafe(ptr)
		if err != nil {
			return nil, err
		}
		return out, nil
	}
	return val, nil
}

// GetAppend appends the value for key to dst and returns the grown slice.
// If key is missing/tombstoned, it returns dst and ErrKeyNotFound.
func (t *Tree) GetAppend(key, dst []byte) ([]byte, error) {
	val, ptr, flags, err := t.lookupLeafValueView(key)
	if err != nil {
		return dst, err
	}
	if flags&node.FlagTombstone != 0 {
		return dst, ErrKeyNotFound
	}
	if flags&node.FlagPointer != 0 {
		if t.slabKeyAppender != nil {
			oldLen := len(dst)
			tail, err := t.slabKeyAppender.ReadUnsafeAppendForKey(ptr, key, dst[oldLen:oldLen])
			if err != nil {
				return dst, err
			}
			if oldLen == 0 {
				return tail, nil
			}
			if len(tail) == 0 {
				return dst[:oldLen], nil
			}
			if cap(dst) > oldLen {
				base := dst[:cap(dst):cap(dst)]
				if &tail[0] == &base[oldLen] {
					return dst[:oldLen+len(tail)], nil
				}
			}
			return append(dst[:oldLen], tail...), nil
		}
		if t.slabKeyReader != nil {
			out, err := t.slabKeyReader.ReadUnsafeForKey(ptr, key)
			if err != nil {
				return dst, err
			}
			if out == nil {
				return dst, nil
			}
			return append(dst, out...), nil
		}
		if t.slabAppender != nil {
			oldLen := len(dst)
			tail, err := t.slabAppender.ReadUnsafeAppend(ptr, dst[oldLen:oldLen])
			if err != nil {
				return dst, err
			}
			if oldLen == 0 {
				return tail, nil
			}
			if len(tail) == 0 {
				return dst[:oldLen], nil
			}
			if cap(dst) > oldLen {
				base := dst[:cap(dst):cap(dst)]
				if &tail[0] == &base[oldLen] {
					return dst[:oldLen+len(tail)], nil
				}
			}
			return append(dst[:oldLen], tail...), nil
		}
		out, err := t.slabReader.ReadUnsafe(ptr)
		if err != nil {
			return dst, err
		}
		if out == nil {
			return dst, nil
		}
		return append(dst, out...), nil
	}
	if val == nil {
		return dst, nil
	}
	return append(dst, val...), nil
}

func (t *Tree) Get(key []byte) ([]byte, error) {
	return t.GetAppend(key, nil)
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
