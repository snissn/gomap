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

// Global fence fallback scans can become quadratic under large missing-key
// sweeps. Limit probe depth so miss-heavy workloads stay bounded while still
// covering nearby cross-leaf anchors.
const fenceGlobalFallbackScanLimit = 32

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

// FenceBlockEntry is one logical key/value entry expanded from a fence-pointer block.
type FenceBlockEntry struct {
	Key   []byte
	Value []byte
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

// Optional append-style fence-key lookup reads.
type slabUnsafeFenceKeyAppender interface {
	ReadUnsafeFenceAppendForKey(ptr page.ValuePtr, key []byte, dst []byte) ([]byte, bool, error)
}

// Optional capability probe for fence-key lookup mode.
// When false, tree miss paths can skip fence fallback scans entirely.
type slabFenceLookupMode interface {
	FenceLookupEnabled() bool
}

// Optional block expansion reads for fence-only outer-leaf mode.
// ok=false means the reader is not in fence expansion mode for this pointer.
type slabUnsafeFenceBlockReader interface {
	ReadUnsafeFenceBlock(ptr page.ValuePtr) (entries []FenceBlockEntry, ok bool, err error)
}

// Optional block expansion reads for fence-only outer-leaf mode that reuse a
// caller-provided destination slice to avoid per-call allocations.
// ok=false means the reader is not in fence expansion mode for this pointer.
type slabUnsafeFenceBlockIntoReader interface {
	ReadUnsafeFenceBlockInto(ptr page.ValuePtr, dst []FenceBlockEntry) (entries []FenceBlockEntry, ok bool, err error)
}

// FenceBlockLease provides lifecycle ownership for expanded fence block
// storage. Implementations may back entries with pooled buffers.
type FenceBlockLease interface {
	Release()
}

// Optional block expansion reads for fence-only outer-leaf mode that also
// return a lease for entry storage lifetime.
// ok=false means the reader is not in fence expansion mode for this pointer.
type slabUnsafeFenceBlockLeaseIntoReader interface {
	ReadUnsafeFenceBlockLeaseInto(ptr page.ValuePtr, dst []FenceBlockEntry) (entries []FenceBlockEntry, lease FenceBlockLease, ok bool, err error)
}

// Optional key-only block expansion reads for fence-only outer-leaf mode.
// ok=false means the reader is not in fence expansion mode for this pointer.
type slabUnsafeFenceBlockKeyReader interface {
	ReadUnsafeFenceBlockKeys(ptr page.ValuePtr) (keys [][]byte, ok bool, err error)
}

// Optional bounded key-only fence expansion reads.
// Implementations may decode only keys in [lower, upper) to avoid
// materializing full fence key vectors for short bounded scans.
// ok=false means the reader is not in fence expansion mode for this pointer.
type slabUnsafeFenceBlockRangeKeyReader interface {
	ReadUnsafeFenceBlockKeysRange(ptr page.ValuePtr, lower []byte, upper []byte) (keys [][]byte, ok bool, err error)
}

// FenceKeysLease provides lifecycle ownership for fence key vectors.
// Keys returned by Keys must be treated as immutable views; implementations
// may alias shared cache state. Callers must Release when done.
type FenceKeysLease interface {
	Keys() [][]byte
	Release()
}

// Optional bounded key-only fence expansion reads with explicit lease
// ownership for decoded keys.
type slabUnsafeFenceBlockRangeKeyLeaseReader interface {
	ReadUnsafeFenceBlockKeysRangeLease(ptr page.ValuePtr, lower []byte, upper []byte) (lease FenceKeysLease, ok bool, err error)
}

// Optional seek-oriented fence reader. Implementations can avoid full key
// materialization for out-of-range predecessor probes by returning only
// lower-bound classification, and provide keys only when the probe key falls
// within the block.
type slabUnsafeFenceBlockSeekReader interface {
	ReadUnsafeFenceBlockSeek(ptr page.ValuePtr, key []byte) (pos int, below bool, above bool, keys [][]byte, ok bool, err error)
}

// Optional seek-oriented fence reader with explicit lease ownership for
// decoded keys returned on in-range matches.
type slabUnsafeFenceBlockSeekLeaseReader interface {
	ReadUnsafeFenceBlockSeekLease(ptr page.ValuePtr, key []byte) (pos int, below bool, above bool, lease FenceKeysLease, ok bool, err error)
}

// Optional fast classifier for whether a pointer is expected to reference a
// fence-expandable outer-leaf block. Returning false is definitive for
// non-grouped pointers; grouped pointers may still be probed because grouped
// records do not carry a dedicated non-colliding fence marker bit.
type slabFencePointerClassifier interface {
	FencePointerLikelyBlock(ptr page.ValuePtr) bool
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

func keyAwarePointerReadsEnabled(sr SlabReader) bool {
	if gate, ok := sr.(slabKeyAwareCapability); ok {
		return gate.KeyAwareEnabled()
	}
	return true
}

func fencePointerLookupsEnabled(sr SlabReader) bool {
	if gate, ok := sr.(slabFenceLookupMode); ok {
		return gate.FenceLookupEnabled()
	}
	return true
}

func (t *Tree) fencePointerLikelyBlock(ptr page.ValuePtr) bool {
	// Explicit fence markers are authoritative and must never be skipped.
	if page.ValuePtrIsFenceOuter(ptr) {
		return true
	}
	// Grouped pointers can still carry fence blocks in v2_fenceptr mode even
	// when they do not expose an explicit fence marker.
	if page.ValuePtrIsGrouped(ptr) {
		return true
	}
	if t != nil && t.slabFencePtrCls != nil {
		return t.slabFencePtrCls.FencePointerLikelyBlock(ptr)
	}
	// Readers that do not provide a classifier keep permissive behavior.
	return true
}

type Tree struct {
	pager             *pager.Pager
	slabReader        SlabReader
	slabAppender      slabUnsafeAppender
	slabBatcher       slabUnsafeBatchAppender
	slabKeyReader     slabUnsafeKeyReader
	slabFenceReader   slabUnsafeFenceKeyReader
	slabFenceAppender slabUnsafeFenceKeyAppender
	fenceLookupMode   bool
	slabFenceBlocks   slabUnsafeFenceBlockReader
	slabFenceBlocksI  slabUnsafeFenceBlockIntoReader
	slabFenceBlocksL  slabUnsafeFenceBlockLeaseIntoReader
	slabFenceKeys     slabUnsafeFenceBlockKeyReader
	slabFenceRange    slabUnsafeFenceBlockRangeKeyReader
	slabFenceRangeL   slabUnsafeFenceBlockRangeKeyLeaseReader
	slabFenceSeek     slabUnsafeFenceBlockSeekReader
	slabFenceSeekL    slabUnsafeFenceBlockSeekLeaseReader
	slabFencePtrCls   slabFencePointerClassifier
	slabKeyAppender   slabUnsafeKeyAppender
	slabKeyBatcher    slabUnsafeKeyBatchAppender
	rootPageID        uint64
}

func New(p *pager.Pager, sr SlabReader, root uint64) *Tree {
	t := &Tree{
		pager:      p,
		slabReader: sr,
		rootPageID: root,
	}
	keyAwareEnabled := keyAwarePointerReadsEnabled(sr)
	fenceEnabled := fencePointerLookupsEnabled(sr)
	if app, ok := sr.(slabUnsafeAppender); ok {
		t.slabAppender = app
	}
	if batcher, ok := sr.(slabUnsafeBatchAppender); ok {
		t.slabBatcher = batcher
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
	if fenceEnabled {
		if fenceReader, ok := sr.(slabUnsafeFenceKeyReader); ok {
			t.slabFenceReader = fenceReader
		}
		if fenceAppender, ok := sr.(slabUnsafeFenceKeyAppender); ok {
			t.slabFenceAppender = fenceAppender
		}
		if fenceBlocks, ok := sr.(slabUnsafeFenceBlockReader); ok {
			t.slabFenceBlocks = fenceBlocks
		}
		if fenceBlocks, ok := sr.(slabUnsafeFenceBlockIntoReader); ok {
			t.slabFenceBlocksI = fenceBlocks
		}
		if fenceBlocks, ok := sr.(slabUnsafeFenceBlockLeaseIntoReader); ok {
			t.slabFenceBlocksL = fenceBlocks
		}
		if fenceKeys, ok := sr.(slabUnsafeFenceBlockKeyReader); ok {
			t.slabFenceKeys = fenceKeys
		}
		if fenceRange, ok := sr.(slabUnsafeFenceBlockRangeKeyReader); ok {
			t.slabFenceRange = fenceRange
		}
		if fenceRangeLease, ok := sr.(slabUnsafeFenceBlockRangeKeyLeaseReader); ok {
			t.slabFenceRangeL = fenceRangeLease
		}
		if fenceSeek, ok := sr.(slabUnsafeFenceBlockSeekReader); ok {
			t.slabFenceSeek = fenceSeek
		}
		if fenceSeekLease, ok := sr.(slabUnsafeFenceBlockSeekLeaseReader); ok {
			t.slabFenceSeekL = fenceSeekLease
		}
		if cls, ok := sr.(slabFencePointerClassifier); ok {
			t.slabFencePtrCls = cls
		}
	}
	t.fenceLookupMode = fenceEnabled && (t.slabFenceReader != nil || t.slabFenceAppender != nil)
	return t
}

// Reset re-initializes the tree with new parameters for reuse.
func (t *Tree) Reset(p *pager.Pager, sr SlabReader, root uint64) {
	t.pager = p
	t.slabReader = sr
	keyAwareEnabled := keyAwarePointerReadsEnabled(sr)
	fenceEnabled := fencePointerLookupsEnabled(sr)
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
	if keyAwareEnabled {
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
	if fenceEnabled {
		if fenceReader, ok := sr.(slabUnsafeFenceKeyReader); ok {
			t.slabFenceReader = fenceReader
		} else {
			t.slabFenceReader = nil
		}
		if fenceAppender, ok := sr.(slabUnsafeFenceKeyAppender); ok {
			t.slabFenceAppender = fenceAppender
		} else {
			t.slabFenceAppender = nil
		}
		if fenceBlocks, ok := sr.(slabUnsafeFenceBlockReader); ok {
			t.slabFenceBlocks = fenceBlocks
		} else {
			t.slabFenceBlocks = nil
		}
		if fenceBlocks, ok := sr.(slabUnsafeFenceBlockIntoReader); ok {
			t.slabFenceBlocksI = fenceBlocks
		} else {
			t.slabFenceBlocksI = nil
		}
		if fenceBlocks, ok := sr.(slabUnsafeFenceBlockLeaseIntoReader); ok {
			t.slabFenceBlocksL = fenceBlocks
		} else {
			t.slabFenceBlocksL = nil
		}
		if fenceKeys, ok := sr.(slabUnsafeFenceBlockKeyReader); ok {
			t.slabFenceKeys = fenceKeys
		} else {
			t.slabFenceKeys = nil
		}
		if fenceRange, ok := sr.(slabUnsafeFenceBlockRangeKeyReader); ok {
			t.slabFenceRange = fenceRange
		} else {
			t.slabFenceRange = nil
		}
		if fenceRangeLease, ok := sr.(slabUnsafeFenceBlockRangeKeyLeaseReader); ok {
			t.slabFenceRangeL = fenceRangeLease
		} else {
			t.slabFenceRangeL = nil
		}
		if fenceSeek, ok := sr.(slabUnsafeFenceBlockSeekReader); ok {
			t.slabFenceSeek = fenceSeek
		} else {
			t.slabFenceSeek = nil
		}
		if fenceSeekLease, ok := sr.(slabUnsafeFenceBlockSeekLeaseReader); ok {
			t.slabFenceSeekL = fenceSeekLease
		} else {
			t.slabFenceSeekL = nil
		}
		if cls, ok := sr.(slabFencePointerClassifier); ok {
			t.slabFencePtrCls = cls
		} else {
			t.slabFencePtrCls = nil
		}
	} else {
		t.slabFenceReader = nil
		t.slabFenceAppender = nil
		t.slabFenceBlocks = nil
		t.slabFenceBlocksI = nil
		t.slabFenceBlocksL = nil
		t.slabFenceKeys = nil
		t.slabFenceRange = nil
		t.slabFenceRangeL = nil
		t.slabFenceSeek = nil
		t.slabFenceSeekL = nil
		t.slabFencePtrCls = nil
	}
	t.fenceLookupMode = fenceEnabled && (t.slabFenceReader != nil || t.slabFenceAppender != nil)
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

// GetEntryExact returns only exact persisted leaf entries for key.
//
// Unlike GetEntry, this method does not perform fence-pointer fallback on leaf
// misses. Callers that need to distinguish exact entries from logical
// fence-resolved keys should use this API.
func (t *Tree) GetEntryExact(key []byte) (node.LeafEntry, error) {
	currID := t.rootPageID
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	for depth := 0; depth < 50; depth++ {
		data, err := t.pager.Get(currID)
		if err != nil {
			return node.LeafEntry{}, err
		}

		n := node.NewNodeView(data)
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

// LookupFencePointerSource returns the predecessor fence pointer record that
// currently resolves key when key has no exact leaf entry.
//
// The boolean return is true only when a fence pointer source is found.
func (t *Tree) LookupFencePointerSource(key []byte) (page.ValuePtr, bool, error) {
	currID := t.rootPageID
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	for depth := 0; depth < 50; depth++ {
		data, err := t.pager.Get(currID)
		if err != nil {
			return page.ValuePtr{}, false, err
		}

		n := node.NewNodeView(data)
		if verifyAlways || !t.pager.IsVerified(currID) {
			if !n.VerifyChecksum() {
				return page.ValuePtr{}, false, fmt.Errorf("checksum mismatch on page %d", currID)
			}
			if !verifyAlways {
				t.pager.MarkVerified(currID)
			}
		}

		switch n.Type() {
		case page.PageTypeInternal:
			if depth == 0 {
				if low, high, ok, err := n.InternalFenceBounds(); err != nil {
					return page.ValuePtr{}, false, err
				} else if ok {
					if len(low) > 0 && compareTreeKey(key, low) < 0 {
						return page.ValuePtr{}, false, nil
					}
					if len(high) > 0 && compareTreeKey(key, high) >= 0 {
						return page.ValuePtr{}, false, nil
					}
				}
			}
			childID, _, err := n.SearchInternalChildID(key)
			if err != nil {
				return page.ValuePtr{}, false, err
			}
			currID = childID

		case page.PageTypeLeaf:
			idx, found, err := n.SearchLeaf(key)
			if err != nil {
				return page.ValuePtr{}, false, err
			}
			if found {
				return page.ValuePtr{}, false, nil
			}
			ptr, ok, err := t.lookupFencePointerSourcePtr(&n, idx, key)
			if err != nil {
				return page.ValuePtr{}, false, err
			}
			if ok {
				return ptr, true, nil
			}
			return t.lookupFencePointerSourceGlobalPtr(key)

		default:
			return page.ValuePtr{}, false, fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
		}
	}
	return page.ValuePtr{}, false, errors.New("tree too deep")
}

// LookupFencePointerOrigin returns the predecessor key and pointer record that
// currently resolve key when key has no exact leaf entry.
//
// The boolean return is true only when a fence pointer source is found.
func (t *Tree) LookupFencePointerOrigin(key []byte) ([]byte, page.ValuePtr, bool, error) {
	currID := t.rootPageID
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	for depth := 0; depth < 50; depth++ {
		data, err := t.pager.Get(currID)
		if err != nil {
			return nil, page.ValuePtr{}, false, err
		}

		n := node.NewNodeView(data)
		if verifyAlways || !t.pager.IsVerified(currID) {
			if !n.VerifyChecksum() {
				return nil, page.ValuePtr{}, false, fmt.Errorf("checksum mismatch on page %d", currID)
			}
			if !verifyAlways {
				t.pager.MarkVerified(currID)
			}
		}

		switch n.Type() {
		case page.PageTypeInternal:
			if depth == 0 {
				if low, high, ok, err := n.InternalFenceBounds(); err != nil {
					return nil, page.ValuePtr{}, false, err
				} else if ok {
					if len(low) > 0 && compareTreeKey(key, low) < 0 {
						return nil, page.ValuePtr{}, false, nil
					}
					if len(high) > 0 && compareTreeKey(key, high) >= 0 {
						return nil, page.ValuePtr{}, false, nil
					}
				}
			}
			childID, _, err := n.SearchInternalChildID(key)
			if err != nil {
				return nil, page.ValuePtr{}, false, err
			}
			currID = childID

		case page.PageTypeLeaf:
			idx, found, err := n.SearchLeaf(key)
			if err != nil {
				return nil, page.ValuePtr{}, false, err
			}
			if found {
				return nil, page.ValuePtr{}, false, nil
			}
			srcKey, ptr, ok, err := t.lookupFencePointerSource(&n, idx, key)
			if err != nil {
				return nil, page.ValuePtr{}, false, err
			}
			if ok {
				return srcKey, ptr, true, nil
			}
			return t.lookupFencePointerSourceGlobal(key)

		default:
			return nil, page.ValuePtr{}, false, fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
		}
	}
	return nil, page.ValuePtr{}, false, errors.New("tree too deep")
}

func (t *Tree) lookupFenceValueView(n *node.Node, idx uint16, key []byte) ([]byte, bool, error) {
	if !t.fenceLookupMode || n == nil || idx == 0 {
		return nil, false, nil
	}
	for scan := idx; scan > 0; scan-- {
		// Fence lookup only needs pointer+flags. Avoid full leaf-entry decode
		// (key reconstruction) on every probe in columnar-prefix leaves.
		_, ptr, flags, err := n.GetLeafValueView(scan - 1)
		if err != nil {
			return nil, false, err
		}
		if flags&node.FlagTombstone != 0 || flags&node.FlagPointer == 0 {
			continue
		}
		if !t.fencePointerLikelyBlock(ptr) {
			continue
		}
		if t.slabFenceReader != nil {
			val, found, err := t.slabFenceReader.ReadUnsafeFenceForKey(ptr, key)
			if err != nil {
				return nil, false, err
			}
			if found {
				return val, true, nil
			}
			continue
		}
		if t.slabFenceAppender != nil {
			val, found, err := t.slabFenceAppender.ReadUnsafeFenceAppendForKey(ptr, key, nil)
			if err != nil {
				return nil, false, err
			}
			if found {
				return val, true, nil
			}
			continue
		}
		return nil, false, nil
	}
	return nil, false, nil
}

func (t *Tree) lookupFencePointerSource(n *node.Node, idx uint16, key []byte) ([]byte, page.ValuePtr, bool, error) {
	if !t.fenceLookupMode || n == nil || idx == 0 {
		return nil, page.ValuePtr{}, false, nil
	}
	for scan := idx; scan > 0; scan-- {
		_, ptr, flags, err := n.GetLeafValueView(scan - 1)
		if err != nil {
			return nil, page.ValuePtr{}, false, err
		}
		if flags&node.FlagTombstone != 0 || flags&node.FlagPointer == 0 {
			continue
		}
		if !t.fencePointerLikelyBlock(ptr) {
			continue
		}
		if t.slabFenceReader != nil {
			_, found, err := t.slabFenceReader.ReadUnsafeFenceForKey(ptr, key)
			if err != nil {
				return nil, page.ValuePtr{}, false, err
			}
			if found {
				srcKey, _, _, _, err := n.GetLeafEntryView(scan - 1)
				if err != nil {
					return nil, page.ValuePtr{}, false, err
				}
				return append([]byte(nil), srcKey...), ptr, true, nil
			}
			continue
		}
		if t.slabFenceAppender != nil {
			_, found, err := t.slabFenceAppender.ReadUnsafeFenceAppendForKey(ptr, key, nil)
			if err != nil {
				return nil, page.ValuePtr{}, false, err
			}
			if found {
				srcKey, _, _, _, err := n.GetLeafEntryView(scan - 1)
				if err != nil {
					return nil, page.ValuePtr{}, false, err
				}
				return append([]byte(nil), srcKey...), ptr, true, nil
			}
			continue
		}
		return nil, page.ValuePtr{}, false, nil
	}
	return nil, page.ValuePtr{}, false, nil
}

func (t *Tree) lookupFencePointerSourcePtr(n *node.Node, idx uint16, key []byte) (page.ValuePtr, bool, error) {
	if !t.fenceLookupMode || n == nil || idx == 0 {
		return page.ValuePtr{}, false, nil
	}
	for scan := idx; scan > 0; scan-- {
		_, ptr, flags, err := n.GetLeafValueView(scan - 1)
		if err != nil {
			return page.ValuePtr{}, false, err
		}
		if flags&node.FlagTombstone != 0 || flags&node.FlagPointer == 0 {
			continue
		}
		if !t.fencePointerLikelyBlock(ptr) {
			continue
		}
		if t.slabFenceReader != nil {
			_, found, err := t.slabFenceReader.ReadUnsafeFenceForKey(ptr, key)
			if err != nil {
				return page.ValuePtr{}, false, err
			}
			if found {
				return ptr, true, nil
			}
			continue
		}
		if t.slabFenceAppender != nil {
			_, found, err := t.slabFenceAppender.ReadUnsafeFenceAppendForKey(ptr, key, nil)
			if err != nil {
				return page.ValuePtr{}, false, err
			}
			if found {
				return ptr, true, nil
			}
			continue
		}
		return page.ValuePtr{}, false, nil
	}
	return page.ValuePtr{}, false, nil
}

func (t *Tree) lookupFencePointerSourceGlobal(key []byte) ([]byte, page.ValuePtr, bool, error) {
	if !t.fenceLookupMode {
		return nil, page.ValuePtr{}, false, nil
	}
	it := t.ReverseIteratorWithOptions(nil, key, IteratorOptions{Mode: IteratorModePointerProjection})
	defer func() { _ = it.Close() }()
	scanned := 0
	for it.Valid() {
		if scanned >= fenceGlobalFallbackScanLimit {
			break
		}
		scanned++
		k, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 && flags&node.FlagTombstone == 0 {
			if t.fencePointerLikelyBlock(ptr) {
				if t.slabFenceReader != nil {
					_, found, err := t.slabFenceReader.ReadUnsafeFenceForKey(ptr, key)
					if err != nil {
						return nil, page.ValuePtr{}, false, err
					}
					if found {
						return append([]byte(nil), k...), ptr, true, nil
					}
				} else if t.slabFenceAppender != nil {
					_, found, err := t.slabFenceAppender.ReadUnsafeFenceAppendForKey(ptr, key, nil)
					if err != nil {
						return nil, page.ValuePtr{}, false, err
					}
					if found {
						return append([]byte(nil), k...), ptr, true, nil
					}
				} else {
					return nil, page.ValuePtr{}, false, nil
				}
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, page.ValuePtr{}, false, err
	}
	return nil, page.ValuePtr{}, false, nil
}

func (t *Tree) lookupFencePointerSourceGlobalPtr(key []byte) (page.ValuePtr, bool, error) {
	if !t.fenceLookupMode {
		return page.ValuePtr{}, false, nil
	}
	it := t.ReverseIteratorWithOptions(nil, key, IteratorOptions{Mode: IteratorModePointerProjection})
	defer func() { _ = it.Close() }()
	scanned := 0
	for it.Valid() {
		if scanned >= fenceGlobalFallbackScanLimit {
			break
		}
		scanned++
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 && flags&node.FlagTombstone == 0 {
			if !t.fencePointerLikelyBlock(ptr) {
				it.Next()
				continue
			}
			if t.slabFenceReader != nil {
				_, found, err := t.slabFenceReader.ReadUnsafeFenceForKey(ptr, key)
				if err != nil {
					return page.ValuePtr{}, false, err
				}
				if found {
					return ptr, true, nil
				}
			} else if t.slabFenceAppender != nil {
				_, found, err := t.slabFenceAppender.ReadUnsafeFenceAppendForKey(ptr, key, nil)
				if err != nil {
					return page.ValuePtr{}, false, err
				}
				if found {
					return ptr, true, nil
				}
			} else {
				return page.ValuePtr{}, false, nil
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return page.ValuePtr{}, false, err
	}
	return page.ValuePtr{}, false, nil
}

func (t *Tree) lookupFenceValueViewGlobal(key []byte) ([]byte, bool, error) {
	if !t.fenceLookupMode {
		return nil, false, nil
	}
	it := t.ReverseIteratorWithOptions(nil, key, IteratorOptions{Mode: IteratorModePointerProjection})
	defer func() { _ = it.Close() }()
	scanned := 0
	for it.Valid() {
		if scanned >= fenceGlobalFallbackScanLimit {
			break
		}
		scanned++
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 && flags&node.FlagTombstone == 0 {
			if !t.fencePointerLikelyBlock(ptr) {
				it.Next()
				continue
			}
			if t.slabFenceReader != nil {
				val, found, err := t.slabFenceReader.ReadUnsafeFenceForKey(ptr, key)
				if err != nil {
					return nil, false, err
				}
				if found {
					return val, true, nil
				}
			} else if t.slabFenceAppender != nil {
				val, found, err := t.slabFenceAppender.ReadUnsafeFenceAppendForKey(ptr, key, nil)
				if err != nil {
					return nil, false, err
				}
				if found {
					return val, true, nil
				}
			} else {
				return nil, false, nil
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, false, err
	}
	return nil, false, nil
}

func (t *Tree) lookupFenceValueViewAppendGlobal(key []byte, dst []byte) ([]byte, bool, error) {
	if !t.fenceLookupMode {
		return dst, false, nil
	}
	it := t.ReverseIteratorWithOptions(nil, key, IteratorOptions{Mode: IteratorModePointerProjection})
	defer func() { _ = it.Close() }()
	scanned := 0
	for it.Valid() {
		if scanned >= fenceGlobalFallbackScanLimit {
			break
		}
		scanned++
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 && flags&node.FlagTombstone == 0 {
			if !t.fencePointerLikelyBlock(ptr) {
				it.Next()
				continue
			}
			if t.slabFenceAppender != nil {
				oldLen := len(dst)
				tail, found, err := t.slabFenceAppender.ReadUnsafeFenceAppendForKey(ptr, key, dst[oldLen:oldLen])
				if err != nil {
					return dst, false, err
				}
				if !found {
					it.Next()
					continue
				}
				if oldLen == 0 {
					return tail, true, nil
				}
				if len(tail) == 0 {
					return dst[:oldLen], true, nil
				}
				if cap(dst) > oldLen {
					base := dst[:cap(dst):cap(dst)]
					if &tail[0] == &base[oldLen] {
						return dst[:oldLen+len(tail)], true, nil
					}
				}
				return append(dst[:oldLen], tail...), true, nil
			}
			if t.slabFenceReader != nil {
				val, found, err := t.slabFenceReader.ReadUnsafeFenceForKey(ptr, key)
				if err != nil {
					return dst, false, err
				}
				if found {
					return append(dst, val...), true, nil
				}
			} else {
				return dst, false, nil
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return dst, false, err
	}
	return dst, false, nil
}

func (t *Tree) lookupFenceValueViewAppend(n *node.Node, idx uint16, key []byte, dst []byte) ([]byte, bool, error) {
	if !t.fenceLookupMode || n == nil || idx == 0 {
		return nil, false, nil
	}
	for scan := idx; scan > 0; scan-- {
		_, ptr, flags, err := n.GetLeafValueView(scan - 1)
		if err != nil {
			return nil, false, err
		}
		if flags&node.FlagTombstone != 0 || flags&node.FlagPointer == 0 {
			continue
		}
		if !t.fencePointerLikelyBlock(ptr) {
			continue
		}
		if t.slabFenceAppender != nil {
			oldLen := len(dst)
			tail, found, err := t.slabFenceAppender.ReadUnsafeFenceAppendForKey(ptr, key, dst[oldLen:oldLen])
			if err != nil {
				return nil, false, err
			}
			if !found {
				continue
			}
			if oldLen == 0 {
				return tail, true, nil
			}
			if len(tail) == 0 {
				return dst[:oldLen], true, nil
			}
			if cap(dst) > oldLen {
				base := dst[:cap(dst):cap(dst)]
				if &tail[0] == &base[oldLen] {
					return dst[:oldLen+len(tail)], true, nil
				}
			}
			return append(dst[:oldLen], tail...), true, nil
		}
		if t.slabFenceReader != nil {
			val, found, err := t.slabFenceReader.ReadUnsafeFenceForKey(ptr, key)
			if err != nil {
				return nil, false, err
			}
			if !found {
				continue
			}
			return append(dst, val...), true, nil
		}
		return nil, false, nil
	}
	return nil, false, nil
}

func (t *Tree) lookupFenceHasKey(n *node.Node, idx uint16, key []byte) (found bool, ok bool, err error) {
	if !t.fenceLookupMode || n == nil || idx == 0 {
		return false, false, nil
	}
	for scan := idx; scan > 0; scan-- {
		_, ptr, flags, err := n.GetLeafValueView(scan - 1)
		if err != nil {
			return false, false, err
		}
		if flags&node.FlagTombstone != 0 || flags&node.FlagPointer == 0 {
			continue
		}
		if !t.fencePointerLikelyBlock(ptr) {
			continue
		}
		if t.slabFenceSeekL != nil {
			pos, below, above, lease, ok, err := t.slabFenceSeekL.ReadUnsafeFenceBlockSeekLease(ptr, key)
			if err != nil {
				if lease != nil {
					lease.Release()
				}
				return false, false, err
			}
			if !ok {
				if lease != nil {
					lease.Release()
				}
			} else {
				if below || above || lease == nil {
					if lease != nil {
						lease.Release()
					}
					continue
				}
				keys := lease.Keys()
				found = fenceSeekContainsKey(keys, pos, key)
				lease.Release()
				if found {
					return true, true, nil
				}
				continue
			}
		}
		if t.slabFenceSeek != nil {
			pos, below, above, keys, ok, err := t.slabFenceSeek.ReadUnsafeFenceBlockSeek(ptr, key)
			if err != nil {
				return false, false, err
			}
			if ok {
				if below || above {
					continue
				}
				if fenceSeekContainsKey(keys, pos, key) {
					return true, true, nil
				}
				continue
			}
		}
		if t.slabFenceReader != nil {
			_, found, err := t.slabFenceReader.ReadUnsafeFenceForKey(ptr, key)
			if err != nil {
				return false, false, err
			}
			if found {
				return true, true, nil
			}
			continue
		}
		if t.slabFenceAppender != nil {
			_, found, err := t.slabFenceAppender.ReadUnsafeFenceAppendForKey(ptr, key, nil)
			if err != nil {
				return false, false, err
			}
			if found {
				return true, true, nil
			}
			continue
		}
		return false, false, nil
	}
	return false, false, nil
}

func fenceSeekContainsKey(keys [][]byte, pos int, key []byte) bool {
	if pos < 0 || pos >= len(keys) {
		return false
	}
	return bytes.Equal(keys[pos], key)
}

func (t *Tree) lookupLeafValueView(key []byte, dst []byte, appendMode bool) ([]byte, page.ValuePtr, byte, bool, error) {
	currID := t.rootPageID
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	for depth := 0; depth < 50; depth++ {
		data, err := t.pager.Get(currID)
		if err != nil {
			return nil, page.ValuePtr{}, 0, false, err
		}

		n := node.NewNodeView(data)
		if verifyAlways || !t.pager.IsVerified(currID) {
			if !n.VerifyChecksum() {
				return nil, page.ValuePtr{}, 0, false, fmt.Errorf("checksum mismatch on page %d", currID)
			}
			if !verifyAlways {
				t.pager.MarkVerified(currID)
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
				return nil, page.ValuePtr{}, 0, false, err
			}
			if !found {
				if appendMode {
					if val, ok, err := t.lookupFenceValueViewAppend(&n, idx, key, dst); err != nil {
						return nil, page.ValuePtr{}, 0, false, err
					} else if ok {
						// Value already appended to dst by fence append path.
						return val, page.ValuePtr{}, 0, true, nil
					}
					if val, ok, err := t.lookupFenceValueViewAppendGlobal(key, dst); err != nil {
						return nil, page.ValuePtr{}, 0, false, err
					} else if ok {
						return val, page.ValuePtr{}, 0, true, nil
					}
				} else if val, ok, err := t.lookupFenceValueView(&n, idx, key); err != nil {
					return nil, page.ValuePtr{}, 0, false, err
				} else if ok {
					// Fence-only mode resolves user keys from outer blocks.
					// Return as inline to avoid a second pointer lookup in callers.
					return val, page.ValuePtr{}, 0, false, nil
				} else if val, ok, err := t.lookupFenceValueViewGlobal(key); err != nil {
					return nil, page.ValuePtr{}, 0, false, err
				} else if ok {
					return val, page.ValuePtr{}, 0, false, nil
				}
				return nil, page.ValuePtr{}, 0, false, ErrKeyNotFound
			}

			val, ptr, flags, err := n.GetLeafValueView(idx)
			if err != nil {
				return nil, page.ValuePtr{}, 0, false, err
			}
			return val, ptr, flags, false, nil

		default:
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
		return val, nil
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
	currID := t.rootPageID
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	for depth := 0; depth < 50; depth++ {
		data, err := t.pager.Get(currID)
		if err != nil {
			return false, err
		}

		n := node.NewNodeView(data)
		if verifyAlways || !t.pager.IsVerified(currID) {
			if !n.VerifyChecksum() {
				return false, fmt.Errorf("checksum mismatch on page %d", currID)
			}
			if !verifyAlways {
				t.pager.MarkVerified(currID)
			}
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
			if fenceFound, ok, err := t.lookupFenceHasKey(&n, idx, key); err != nil {
				return false, err
			} else if ok {
				return fenceFound, nil
			}
			if _, ok, err := t.lookupFencePointerSourceGlobalPtr(key); err != nil {
				return false, err
			} else if ok {
				return true, nil
			}
			return false, nil
		default:
			return false, fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
		}
	}

	return false, errors.New("tree too deep")
}
