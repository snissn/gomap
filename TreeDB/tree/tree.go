package tree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

var ErrKeyNotFound = errors.New("key not found")

type leafRefPageScratch struct {
	buf []byte
}

var leafRefPageScratchPool = sync.Pool{
	New: func() any {
		return &leafRefPageScratch{buf: make([]byte, 0, page.PageSize)}
	},
}

type ReadPathStats struct {
	GetAppendInlineHitsTotal   uint64
	GetAppendInlineBytesTotal  uint64
	GetAppendPointerHitsTotal  uint64
	GetAppendPointerBytesTotal uint64
}

var treeGetAppendInlineHitsTotal atomic.Uint64
var treeGetAppendInlineBytesTotal atomic.Uint64
var treeGetAppendPointerHitsTotal atomic.Uint64
var treeGetAppendPointerBytesTotal atomic.Uint64

func ReadPathStatsSnapshot() ReadPathStats {
	return ReadPathStats{
		GetAppendInlineHitsTotal:   treeGetAppendInlineHitsTotal.Load(),
		GetAppendInlineBytesTotal:  treeGetAppendInlineBytesTotal.Load(),
		GetAppendPointerHitsTotal:  treeGetAppendPointerHitsTotal.Load(),
		GetAppendPointerBytesTotal: treeGetAppendPointerBytesTotal.Load(),
	}
}

func getLeafRefPageScratch() *leafRefPageScratch {
	scratch, _ := leafRefPageScratchPool.Get().(*leafRefPageScratch)
	if scratch == nil || cap(scratch.buf) != page.PageSize {
		return &leafRefPageScratch{buf: make([]byte, 0, page.PageSize)}
	}
	scratch.buf = scratch.buf[:0]
	return scratch
}

func putLeafRefPageScratch(scratch *leafRefPageScratch) {
	if scratch == nil || cap(scratch.buf) != page.PageSize {
		return
	}
	scratch.buf = scratch.buf[:0]
	leafRefPageScratchPool.Put(scratch)
}

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

// Optional fast path for scratch-based unsafe reads. Callers can provide a
// reusable dst buffer to avoid allocating decode scratch and extra copies.
type slabUnsafeToReader interface {
	ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error)
}

// Optional key-aware pointer reads for outer-leaf block payloads.
type slabUnsafeKeyReader interface {
	ReadUnsafeForKey(ptr page.ValuePtr, key []byte) ([]byte, error)
}

// Optional key-aware append-style pointer reads.
type slabUnsafeKeyAppender interface {
	ReadUnsafeAppendForKey(ptr page.ValuePtr, key []byte, dst []byte) ([]byte, error)
}

// Optional key-aware batched append-style pointer reads.
type slabUnsafeKeyBatchAppender interface {
	ReadUnsafeAppendBatchForKeys(ptrs []page.ValuePtr, keys [][]byte, dst [][]byte) ([][]byte, error)
}

// Optional capability gate for key-aware pointer read interfaces.
type slabKeyAwareCapability interface {
	KeyAwareEnabled() bool
}

// Optional capability gate for slab/leaf-ref checksum verification.
//
// Returning false allows tree leaf-ref readers to skip per-page checksum
// validation on hot paths (unsafe; intended for explicitly relaxed profiles).
type slabReadChecksumCapability interface {
	ReadChecksumEnabled() bool
}

func keyAwarePointerReadsEnabled(sr SlabReader) bool {
	if gate, ok := sr.(slabKeyAwareCapability); ok {
		return gate.KeyAwareEnabled()
	}
	return true
}

func slabReadChecksumEnabled(sr SlabReader) bool {
	if gate, ok := sr.(slabReadChecksumCapability); ok {
		return gate.ReadChecksumEnabled()
	}
	return true
}

type Tree struct {
	pager           *pager.Pager
	slabReader      SlabReader
	slabAppender    slabUnsafeAppender
	slabBatcher     slabUnsafeBatchAppender
	slabToReader    slabUnsafeToReader
	slabKeyReader   slabUnsafeKeyReader
	slabKeyAppender slabUnsafeKeyAppender
	slabKeyBatcher  slabUnsafeKeyBatchAppender
	rootPageID      uint64
}

func New(p *pager.Pager, sr SlabReader, root uint64) *Tree {
	t := &Tree{
		pager:      p,
		slabReader: sr,
		rootPageID: root,
	}
	keyAwareEnabled := keyAwarePointerReadsEnabled(sr)
	if app, ok := sr.(slabUnsafeAppender); ok {
		t.slabAppender = app
	}
	if batcher, ok := sr.(slabUnsafeBatchAppender); ok {
		t.slabBatcher = batcher
	}
	if toer, ok := sr.(slabUnsafeToReader); ok {
		t.slabToReader = toer
	}
	if keyAwareEnabled {
		if keyReader, ok := sr.(slabUnsafeKeyReader); ok {
			t.slabKeyReader = keyReader
		}
		if keyAppender, ok := sr.(slabUnsafeKeyAppender); ok {
			t.slabKeyAppender = keyAppender
		}
		if keyBatcher, ok := sr.(slabUnsafeKeyBatchAppender); ok {
			t.slabKeyBatcher = keyBatcher
		}
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
	if batcher, ok := sr.(slabUnsafeBatchAppender); ok {
		t.slabBatcher = batcher
	} else {
		t.slabBatcher = nil
	}
	if toer, ok := sr.(slabUnsafeToReader); ok {
		t.slabToReader = toer
	} else {
		t.slabToReader = nil
	}
	if keyAwarePointerReadsEnabled(sr) {
		if keyReader, ok := sr.(slabUnsafeKeyReader); ok {
			t.slabKeyReader = keyReader
		} else {
			t.slabKeyReader = nil
		}
		if keyAppender, ok := sr.(slabUnsafeKeyAppender); ok {
			t.slabKeyAppender = keyAppender
		} else {
			t.slabKeyAppender = nil
		}
		if keyBatcher, ok := sr.(slabUnsafeKeyBatchAppender); ok {
			t.slabKeyBatcher = keyBatcher
		} else {
			t.slabKeyBatcher = nil
		}
	} else {
		t.slabKeyReader = nil
		t.slabKeyAppender = nil
		t.slabKeyBatcher = nil
	}
	t.rootPageID = root
}

// SetRoot updates the root page ID.
func (t *Tree) SetRoot(root uint64) {
	t.rootPageID = root
}

func (t *Tree) shouldVerifyLeafRefChecksum() bool {
	if t == nil {
		return true
	}
	return slabReadChecksumEnabled(t.slabReader)
}

func (t *Tree) loadNodeView(pageID uint64, verifyAlways bool) (node.Node, error) {
	if t == nil {
		return node.Node{}, errors.New("missing tree")
	}
	if ptr, ok := page.DecodeLeafRef(pageID); ok {
		if t.slabReader == nil {
			return node.Node{}, errors.New("missing slab reader")
		}
		data, err := t.slabReader.ReadUnsafe(ptr.ValuePtr())
		if err != nil {
			return node.Node{}, err
		}
		if len(data) != page.PageSize {
			return node.Node{}, fmt.Errorf("invalid leaf page size %d for page %d", len(data), pageID)
		}
		n := node.NewNodeView(data)
		if t.shouldVerifyLeafRefChecksum() && !n.VerifyChecksum() {
			return node.Node{}, fmt.Errorf("checksum mismatch on page %d", pageID)
		}
		if n.Type() != page.PageTypeLeaf {
			return node.Node{}, fmt.Errorf("invalid page type %d at page %d", n.Type(), pageID)
		}
		return n, nil
	}
	if t.pager == nil {
		return node.Node{}, errors.New("missing pager")
	}
	// Use Get (mmap) instead of ReadPage (Copy).
	data, err := t.pager.Get(pageID)
	if err != nil {
		return node.Node{}, err
	}
	n := node.NewNodeView(data) // VerifyChecksum is fast (CRC32C hardware accelerated).
	// We use Verified Cache to skip it if already checked.
	if verifyAlways || !t.pager.IsVerified(pageID) {
		if !n.VerifyChecksum() {
			return node.Node{}, fmt.Errorf("checksum mismatch on page %d", pageID)
		}
		if !verifyAlways {
			t.pager.MarkVerified(pageID)
		}
	}
	return n, nil
}

// GetEntry returns the persisted leaf entry for key.
//
// CAUTION: Returned entry Key/Value might point directly to mmap memory.
// Do not modify or hold reference for long.
func (t *Tree) GetEntry(key []byte) (node.LeafEntry, error) {
	currID := t.rootPageID
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	for depth := 0; depth < 50; depth++ {
		n, err := t.loadNodeView(currID, verifyAlways)
		if err != nil {
			return node.LeafEntry{}, err
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

// GetEntryExact is an alias for GetEntry.
func (t *Tree) GetEntryExact(key []byte) (node.LeafEntry, error) {
	return t.GetEntry(key)
}

func (t *Tree) lookupLeafValueView(key []byte, dst []byte, appendMode bool) ([]byte, page.ValuePtr, byte, bool, error) {
	currID := t.rootPageID
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	for depth := 0; depth < 50; depth++ {
		var (
			n                node.Node
			leafScratch      *leafRefPageScratch
			leafScratchOwned bool
			loadedLeafRef    bool
		)

		if appendMode {
			if ptr, ok := page.DecodeLeafRef(currID); ok && t.slabReader != nil {
				leafScratch = getLeafRefPageScratch()
				var (
					data []byte
					err  error
				)
				if t.slabToReader != nil {
					var usedDst bool
					data, usedDst, err = t.slabToReader.ReadUnsafeTo(ptr.ValuePtr(), leafScratch.buf)
					if err != nil {
						putLeafRefPageScratch(leafScratch)
						return nil, page.ValuePtr{}, 0, false, err
					}
					loadedLeafRef = true
					leafScratchOwned = usedDst
					if !usedDst {
						putLeafRefPageScratch(leafScratch)
						leafScratch = nil
					}
				} else if t.slabAppender != nil {
					data, err = t.slabAppender.ReadUnsafeAppend(ptr.ValuePtr(), leafScratch.buf[:0])
					if err != nil {
						putLeafRefPageScratch(leafScratch)
						return nil, page.ValuePtr{}, 0, false, err
					}
					loadedLeafRef = true
					leafScratchOwned = true
				} else {
					putLeafRefPageScratch(leafScratch)
					leafScratch = nil
				}
				if loadedLeafRef {
					if len(data) != page.PageSize {
						if leafScratchOwned {
							putLeafRefPageScratch(leafScratch)
						}
						return nil, page.ValuePtr{}, 0, false, fmt.Errorf("invalid leaf page size %d for page %d", len(data), currID)
					}
					n = node.NewNodeView(data)
					if t.shouldVerifyLeafRefChecksum() && !n.VerifyChecksum() {
						if leafScratchOwned {
							putLeafRefPageScratch(leafScratch)
						}
						return nil, page.ValuePtr{}, 0, false, fmt.Errorf("checksum mismatch on page %d", currID)
					}
					if n.Type() != page.PageTypeLeaf {
						if leafScratchOwned {
							putLeafRefPageScratch(leafScratch)
						}
						return nil, page.ValuePtr{}, 0, false, fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
					}
				}
			}
		}

		if !loadedLeafRef {
			var err error
			n, err = t.loadNodeView(currID, verifyAlways)
			if err != nil {
				return nil, page.ValuePtr{}, 0, false, err
			}
		}

		switch n.Type() {
		case page.PageTypeInternal:
			if depth == 0 {
				if low, high, ok, err := n.InternalFenceBounds(); err != nil {
					return nil, page.ValuePtr{}, 0, false, err
				} else if ok {
					if len(low) > 0 && compareTreeKey(key, low) < 0 {
						return nil, page.ValuePtr{}, 0, false, ErrKeyNotFound
					}
					if len(high) > 0 && compareTreeKey(key, high) >= 0 {
						return nil, page.ValuePtr{}, 0, false, ErrKeyNotFound
					}
				}
			}
			childID, _, err := n.SearchInternalChildID(key)
			if err != nil {
				return nil, page.ValuePtr{}, 0, false, err
			}
			currID = childID

		case page.PageTypeLeaf:
			idx, found, err := n.SearchLeaf(key)
			if err != nil {
				if leafScratchOwned {
					putLeafRefPageScratch(leafScratch)
				}
				return nil, page.ValuePtr{}, 0, false, err
			}
			if !found {
				if leafScratchOwned {
					putLeafRefPageScratch(leafScratch)
				}
				return nil, page.ValuePtr{}, 0, false, ErrKeyNotFound
			}

			val, ptr, flags, err := n.GetLeafValueView(idx)
			if err != nil {
				if leafScratchOwned {
					putLeafRefPageScratch(leafScratch)
				}
				return nil, page.ValuePtr{}, 0, false, err
			}
			if appendMode && flags&(node.FlagPointer|node.FlagTombstone) == 0 {
				out := dst
				if val != nil {
					out = append(dst, val...)
				}
				if leafScratchOwned {
					putLeafRefPageScratch(leafScratch)
				}
				return out, ptr, flags, true, nil
			}
			if leafScratchOwned {
				putLeafRefPageScratch(leafScratch)
			}
			return val, ptr, flags, false, nil

		default:
			if leafScratchOwned {
				putLeafRefPageScratch(leafScratch)
			}
			return nil, page.ValuePtr{}, 0, false, fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
		}
	}

	return nil, page.ValuePtr{}, 0, false, errors.New("tree too deep")
}

func (t *Tree) GetUnsafe(key []byte) ([]byte, error) {
	val, ptr, flags, _, err := t.lookupLeafValueView(key, nil, false)
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
	val, ptr, flags, appendedDirect, err := t.lookupLeafValueView(key, dst, true)
	if err != nil {
		return dst, err
	}
	if appendedDirect {
		treeGetAppendInlineHitsTotal.Add(1)
		if n := len(val) - len(dst); n > 0 {
			treeGetAppendInlineBytesTotal.Add(uint64(n))
		}
		return val, nil
	}
	if flags&node.FlagTombstone != 0 {
		return dst, ErrKeyNotFound
	}
	if flags&node.FlagPointer != 0 {
		treeGetAppendPointerHitsTotal.Add(1)
		if t.slabKeyAppender != nil {
			oldLen := len(dst)
			tail, err := t.slabKeyAppender.ReadUnsafeAppendForKey(ptr, key, dst[oldLen:oldLen])
			if err != nil {
				return dst, err
			}
			if oldLen == 0 {
				if len(tail) > 0 {
					treeGetAppendPointerBytesTotal.Add(uint64(len(tail)))
				}
				return tail, nil
			}
			if len(tail) == 0 {
				return dst[:oldLen], nil
			}
			if cap(dst) > oldLen {
				base := dst[:cap(dst):cap(dst)]
				if &tail[0] == &base[oldLen] {
					treeGetAppendPointerBytesTotal.Add(uint64(len(tail)))
					return dst[:oldLen+len(tail)], nil
				}
			}
			treeGetAppendPointerBytesTotal.Add(uint64(len(tail)))
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
			treeGetAppendPointerBytesTotal.Add(uint64(len(out)))
			return append(dst, out...), nil
		}
		if t.slabAppender != nil {
			oldLen := len(dst)
			tail, err := t.slabAppender.ReadUnsafeAppend(ptr, dst[oldLen:oldLen])
			if err != nil {
				return dst, err
			}
			if oldLen == 0 {
				if len(tail) > 0 {
					treeGetAppendPointerBytesTotal.Add(uint64(len(tail)))
				}
				return tail, nil
			}
			if len(tail) == 0 {
				return dst[:oldLen], nil
			}
			if cap(dst) > oldLen {
				base := dst[:cap(dst):cap(dst)]
				if &tail[0] == &base[oldLen] {
					treeGetAppendPointerBytesTotal.Add(uint64(len(tail)))
					return dst[:oldLen+len(tail)], nil
				}
			}
			treeGetAppendPointerBytesTotal.Add(uint64(len(tail)))
			return append(dst[:oldLen], tail...), nil
		}
		out, err := t.slabReader.ReadUnsafe(ptr)
		if err != nil {
			return dst, err
		}
		if out == nil {
			return dst, nil
		}
		treeGetAppendPointerBytesTotal.Add(uint64(len(out)))
		return append(dst, out...), nil
	}
	if val == nil {
		treeGetAppendInlineHitsTotal.Add(1)
		return dst, nil
	}
	treeGetAppendInlineHitsTotal.Add(1)
	if len(val) > 0 {
		treeGetAppendInlineBytesTotal.Add(uint64(len(val)))
	}
	return append(dst, val...), nil
}

// Get returns the value for key.
//
// When the stored value is non-empty, Get returns an owned, mutable copy of
// that value.
//
// If the key is missing or tombstoned, Get returns ErrKeyNotFound. For
// compatibility with the long-standing TreeDB API, when the stored value is
// zero-length but the key is present, Get returns (nil, nil).
func (t *Tree) Get(key []byte) ([]byte, error) {
	out, err := t.GetAppend(key, nil)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		// Preserve long-standing TreeDB API behavior: empty-but-present values
		// return (nil, nil) instead of a 0-length slice.
		return nil, nil
	}
	// GetAppend(key, nil) usually returns an exact-sized owned slice. Only copy
	// when extra capacity would otherwise retain oversized backing arrays.
	if cap(out) == len(out) {
		return out, nil
	}
	owned := make([]byte, len(out))
	copy(owned, out)
	return owned, nil
}

func (t *Tree) Has(key []byte) (bool, error) {
	currID := t.rootPageID
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	for depth := 0; depth < 50; depth++ {
		n, err := t.loadNodeView(currID, verifyAlways)
		if err != nil {
			return false, err
		}

		switch n.Type() {
		case page.PageTypeInternal:
			if depth == 0 {
				if low, high, ok, err := n.InternalFenceBounds(); err != nil {
					return false, err
				} else if ok {
					if len(low) > 0 && compareTreeKey(key, low) < 0 {
						return false, nil
					}
					if len(high) > 0 && compareTreeKey(key, high) >= 0 {
						return false, nil
					}
				}
			}
			childID, _, err := n.SearchInternalChildID(key)
			if err != nil {
				return false, err
			}
			currID = childID
		case page.PageTypeLeaf:
			idx, found, err := n.SearchLeaf(key)
			if err != nil {
				return false, err
			}
			if found {
				_, _, flags, err := n.GetLeafValueView(idx)
				if err != nil {
					return false, err
				}
				return flags&node.FlagTombstone == 0, nil
			}
			return false, nil
		default:
			return false, fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
		}
	}

	return false, errors.New("tree too deep")
}
