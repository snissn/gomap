package tree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
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

type GetManyReadStats struct {
	CallsTotal          uint64
	GroupedCallsTotal   uint64
	FallbackCallsTotal  uint64
	LeafGroupsTotal     uint64
	LeafGroupItemsTotal uint64
	LeafLoadsSavedTotal uint64
}

type ProbeFallbackStats struct {
	FallbackCalls uint64
	FallbackItems uint64
}

const (
	getManyLeafGroupMinKeys = 64
	// maxTraversalDepth is a corruption guard for point/probe tree walks. Healthy
	// B+Tree roots should be much shallower; hitting this limit indicates a cycle
	// or structural balance bug that should be investigated rather than masked.
	maxTraversalDepth = 50
)

var treeGetAppendInlineHitsTotal atomic.Uint64
var treeGetAppendInlineBytesTotal atomic.Uint64
var treeGetAppendPointerHitsTotal atomic.Uint64
var treeGetAppendPointerBytesTotal atomic.Uint64
var treeGetManyCallsTotal atomic.Uint64
var treeGetManyGroupedCallsTotal atomic.Uint64
var treeGetManyFallbackCallsTotal atomic.Uint64
var treeGetManyLeafGroupsTotal atomic.Uint64
var treeGetManyLeafGroupItemsTotal atomic.Uint64
var treeGetManyLeafLoadsSavedTotal atomic.Uint64
var treeHotReadStatsEnabled = os.Getenv("TREEDB_HOT_PATH_STATS") != ""

var treeGetManyEmptyValue = []byte{}

func ReadPathStatsSnapshot() ReadPathStats {
	return ReadPathStats{
		GetAppendInlineHitsTotal:   treeGetAppendInlineHitsTotal.Load(),
		GetAppendInlineBytesTotal:  treeGetAppendInlineBytesTotal.Load(),
		GetAppendPointerHitsTotal:  treeGetAppendPointerHitsTotal.Load(),
		GetAppendPointerBytesTotal: treeGetAppendPointerBytesTotal.Load(),
	}
}

func GetManyReadStatsSnapshot() GetManyReadStats {
	return GetManyReadStats{
		CallsTotal:          treeGetManyCallsTotal.Load(),
		GroupedCallsTotal:   treeGetManyGroupedCallsTotal.Load(),
		FallbackCallsTotal:  treeGetManyFallbackCallsTotal.Load(),
		LeafGroupsTotal:     treeGetManyLeafGroupsTotal.Load(),
		LeafGroupItemsTotal: treeGetManyLeafGroupItemsTotal.Load(),
		LeafLoadsSavedTotal: treeGetManyLeafLoadsSavedTotal.Load(),
	}
}

func recordTreeGetAppendInline(n int) {
	if !treeHotReadStatsEnabled {
		return
	}
	treeGetAppendInlineHitsTotal.Add(1)
	if n > 0 {
		treeGetAppendInlineBytesTotal.Add(uint64(n))
	}
}

func recordTreeGetAppendPointer(n int) {
	if !treeHotReadStatsEnabled {
		return
	}
	treeGetAppendPointerHitsTotal.Add(1)
	if n > 0 {
		treeGetAppendPointerBytesTotal.Add(uint64(n))
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

// Optional fast path for leaf-log page reads. Unlike SlabReader, this receives
// the typed leaf-log pointer so implementations can safely use page-only caches
// without confusing ordinary value-log payloads with B-tree leaf pages.
type leafLogPageUnsafeToReader interface {
	ReadLeafLogPageUnsafeTo(ptr page.LeafLogPtr, dst []byte) ([]byte, bool, error)
}

// LeafLogPageReadState describes validation state carried by a leaf-log page
// read. RecordChecksumVerified is true when the bytes were produced by a
// checksum-enabled value-log read. CacheEntryPresent is true when the reader has
// an entry for this leaf pointer that may be marked after successful page
// checksum validation. PageChecksumVerified is true only when cached bytes had
// the B-tree page checksum validated successfully before being cached as
// verified.
type LeafLogPageReadState struct {
	RecordChecksumVerified bool
	CacheEntryPresent      bool
	PageChecksumVerified   bool
}

// Optional fast path for leaf-log page reads that can report cached validation
// state to let the tree skip repeated page checksum work without weakening
// durable read-integrity semantics.
type leafLogPageUnsafeToStateReader interface {
	ReadLeafLogPageUnsafeToWithState(ptr page.LeafLogPtr, dst []byte) ([]byte, bool, LeafLogPageReadState, error)
}

// LeafLogPageViewLease releases a read-only leaf-log page view.
type LeafLogPageViewLease interface {
	ReleaseLeafLogPageView()
}

// Optional fast path for read-only leaf-log page views. The lease must be
// released before the caller lets the returned page slice escape.
type leafLogPageUnsafeViewReader interface {
	ReadLeafLogPageUnsafeView(ptr page.LeafLogPtr) ([]byte, LeafLogPageViewLease, bool, error)
}

// Optional read-only leaf-log page view with cached validation state.
type leafLogPageUnsafeViewStateReader interface {
	ReadLeafLogPageUnsafeViewWithState(ptr page.LeafLogPtr) ([]byte, LeafLogPageViewLease, bool, LeafLogPageReadState, error)
}

// LeafLogPageViewChecksumMarker can be implemented by a view lease to mark the
// leased cache entry as page-checksum verified while the view lock is still held.
type LeafLogPageViewChecksumMarker interface {
	MarkLeafLogPageViewChecksumVerified()
}

// Optional marker used after a checksum-enabled tree validation succeeds. Cache
// implementations must fail closed and only mark entries that were populated
// from checksum-verified value-log bytes.
type leafLogPageChecksumVerifyMarker interface {
	MarkLeafLogPageChecksumVerified(ptr page.LeafLogPtr)
}

// Optional capability gate for key-aware pointer read interfaces.
type slabKeyAwareCapability interface {
	KeyAwareEnabled() bool
}

// GetManyViewFunc receives one GetManyView result. The value slice is a
// read-only view that is valid only until the callback returns; callers must
// copy it before retaining it. Missing/tombstoned keys are reported with
// found=false and value=nil.
type GetManyViewFunc func(index int, key []byte, value []byte, found bool) error

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
	pager               *pager.Pager
	slabReader          SlabReader
	slabAppender        slabUnsafeAppender
	slabBatcher         slabUnsafeBatchAppender
	slabToReader        slabUnsafeToReader
	slabKeyReader       slabUnsafeKeyReader
	slabKeyAppender     slabUnsafeKeyAppender
	slabKeyBatcher      slabUnsafeKeyBatchAppender
	leafLogToReader     leafLogPageUnsafeToReader
	leafLogToState      leafLogPageUnsafeToStateReader
	leafLogView         leafLogPageUnsafeViewReader
	leafLogViewState    leafLogPageUnsafeViewStateReader
	leafLogVerifyMarker leafLogPageChecksumVerifyMarker
	rootPageID          uint64
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
	if leafToReader, ok := sr.(leafLogPageUnsafeToReader); ok {
		t.leafLogToReader = leafToReader
	}
	if leafToState, ok := sr.(leafLogPageUnsafeToStateReader); ok {
		t.leafLogToState = leafToState
	}
	if leafView, ok := sr.(leafLogPageUnsafeViewReader); ok {
		t.leafLogView = leafView
	}
	if leafViewState, ok := sr.(leafLogPageUnsafeViewStateReader); ok {
		t.leafLogViewState = leafViewState
	}
	if marker, ok := sr.(leafLogPageChecksumVerifyMarker); ok {
		t.leafLogVerifyMarker = marker
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
	if leafToReader, ok := sr.(leafLogPageUnsafeToReader); ok {
		t.leafLogToReader = leafToReader
	} else {
		t.leafLogToReader = nil
	}
	if leafToState, ok := sr.(leafLogPageUnsafeToStateReader); ok {
		t.leafLogToState = leafToState
	} else {
		t.leafLogToState = nil
	}
	if leafView, ok := sr.(leafLogPageUnsafeViewReader); ok {
		t.leafLogView = leafView
	} else {
		t.leafLogView = nil
	}
	if leafViewState, ok := sr.(leafLogPageUnsafeViewStateReader); ok {
		t.leafLogViewState = leafViewState
	} else {
		t.leafLogViewState = nil
	}
	if marker, ok := sr.(leafLogPageChecksumVerifyMarker); ok {
		t.leafLogVerifyMarker = marker
	} else {
		t.leafLogVerifyMarker = nil
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
	return t.loadNodeViewWithLoadKind(pageID, verifyAlways, false)
}

func (t *Tree) loadLeafLogNodeView(ptr page.LogRecordRef, iterator bool) (node.Node, error) {
	var n node.Node
	if err := t.loadLeafLogNodeViewInto(&n, ptr, iterator); err != nil {
		return node.Node{}, err
	}
	return n, nil
}

func (t *Tree) loadLeafLogNodeViewInto(dst *node.Node, ptr page.LogRecordRef, iterator bool) error {
	if t.slabReader == nil {
		return errors.New("missing slab reader")
	}
	var (
		data  []byte
		state LeafLogPageReadState
		err   error
	)
	if t.leafLogToState != nil {
		data, _, state, err = t.leafLogToState.ReadLeafLogPageUnsafeToWithState(ptr, nil)
	} else if t.leafLogToReader != nil {
		data, _, err = t.leafLogToReader.ReadLeafLogPageUnsafeTo(ptr, nil)
	} else {
		data, err = t.slabReader.ReadUnsafe(ptr.ValuePtr())
	}
	if err != nil {
		return err
	}
	verifiedNow, err := validateLeafLogNodeIntoWithState(dst, data, ptr, t.shouldVerifyLeafRefChecksum(), iterator, state)
	if err != nil {
		return err
	}
	if verifiedNow && state.RecordChecksumVerified && state.CacheEntryPresent {
		t.markLeafLogPageChecksumVerified(ptr)
	}
	return nil
}

func validateLeafLogNode(data []byte, ptr page.LogRecordRef, verifyChecksum bool, iterator bool) (node.Node, error) {
	var n node.Node
	if err := validateLeafLogNodeInto(&n, data, ptr, verifyChecksum, iterator); err != nil {
		return node.Node{}, err
	}
	return n, nil
}

func validateLeafLogNodeInto(dst *node.Node, data []byte, ptr page.LogRecordRef, verifyChecksum bool, iterator bool) error {
	_, err := validateLeafLogNodeIntoWithState(dst, data, ptr, verifyChecksum, iterator, LeafLogPageReadState{})
	return err
}

func validateLeafLogNodeIntoWithState(dst *node.Node, data []byte, ptr page.LogRecordRef, verifyChecksum bool, iterator bool, state LeafLogPageReadState) (verifiedNow bool, err error) {
	if len(data) != page.PageSize {
		return false, fmt.Errorf("invalid leaf page size %d for leaf-log ref file=%d offset=%d", len(data), ptr.FileID, ptr.Offset)
	}
	node.InitFreshNodeView(dst, data)
	if verifyChecksum {
		if state.PageChecksumVerified && state.RecordChecksumVerified {
			noteOuterLeafChecksumSkipped()
		} else {
			noteOuterLeafChecksumVerified()
			if !dst.VerifyChecksum() {
				return false, fmt.Errorf("checksum mismatch on leaf-log ref file=%d offset=%d", ptr.FileID, ptr.Offset)
			}
			verifiedNow = true
		}
	}
	if dst.Type() != page.PageTypeLeaf {
		return false, fmt.Errorf("invalid page type %d at leaf-log ref file=%d offset=%d", dst.Type(), ptr.FileID, ptr.Offset)
	}
	noteOuterLeafLoad(ptr.ValuePtr(), len(data), iterator)
	return verifiedNow, nil
}

func (t *Tree) markLeafLogPageChecksumVerified(ptr page.LeafLogPtr) {
	if t == nil || t.leafLogVerifyMarker == nil {
		return
	}
	t.leafLogVerifyMarker.MarkLeafLogPageChecksumVerified(ptr)
}

func (t *Tree) loadChildRefView(ref page.ChildRef, verifyAlways bool, iterator bool) (node.Node, error) {
	var n node.Node
	if err := t.loadChildRefViewInto(&n, ref, verifyAlways, iterator); err != nil {
		return node.Node{}, err
	}
	return n, nil
}

func (t *Tree) loadChildRefViewInto(dst *node.Node, ref page.ChildRef, verifyAlways bool, iterator bool) error {
	if ref.Kind == page.ChildRefLeafLog {
		return t.loadLeafLogNodeViewInto(dst, ref.Log, iterator)
	}
	return t.loadNodeViewWithLoadKindInto(dst, ref.Page, verifyAlways, iterator)
}

func (t *Tree) loadNodeViewWithLoadKind(pageID uint64, verifyAlways bool, iterator bool) (node.Node, error) {
	var n node.Node
	if err := t.loadNodeViewWithLoadKindInto(&n, pageID, verifyAlways, iterator); err != nil {
		return node.Node{}, err
	}
	return n, nil
}

func (t *Tree) loadNodeViewWithLoadKindInto(dst *node.Node, pageID uint64, verifyAlways bool, iterator bool) error {
	if t == nil {
		return errors.New("missing tree")
	}
	if t.pager == nil {
		return errors.New("missing pager")
	}
	// Use Get (mmap) instead of ReadPage (Copy).
	data, err := t.pager.Get(pageID)
	if err != nil {
		return err
	}
	node.InitFreshNodeView(dst, data) // VerifyChecksum is fast (CRC-32/IEEE hardware accelerated).
	// We use Verified Cache to skip it if already checked.
	if verifyAlways || !t.pager.IsVerified(pageID) {
		if !dst.VerifyChecksum() {
			return fmt.Errorf("checksum mismatch on page %d", pageID)
		}
		if !verifyAlways {
			t.pager.MarkVerified(pageID)
		}
	}
	return nil
}

// GetEntry returns the persisted leaf entry for key.
//
// CAUTION: Returned entry Key/Value might point directly to mmap memory.
// Do not modify or hold reference for long.
func (t *Tree) GetEntry(key []byte) (node.LeafEntry, error) {
	currRef := page.PageChildRef(t.rootPageID)
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	for depth := 0; depth < maxTraversalDepth; depth++ {
		var n node.Node
		if err := t.loadChildRefViewInto(&n, currRef, verifyAlways, false); err != nil {
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
			if n.InternalLeafLogRefsEnabled() {
				childRef, _, err := n.SearchInternalChildRef(key)
				if err != nil {
					return node.LeafEntry{}, err
				}
				currRef = childRef
			} else {
				childID, _, err := n.SearchInternalChildID(key)
				if err != nil {
					return node.LeafEntry{}, err
				}
				currRef = page.PageChildRef(childID)
			}

		case page.PageTypeLeaf:
			idx, found, err := n.SearchLeaf(key)
			if err != nil {
				return node.LeafEntry{}, err
			}
			if !found {
				return node.LeafEntry{}, ErrKeyNotFound
			}

			// Zero-copy view
			k, v, ptr, flags, revision, err := n.GetLeafEntryViewWithRevision(idx)
			if err != nil {
				return node.LeafEntry{}, err
			}

			return node.LeafEntry{
				Key:      k,
				Value:    v,
				ValuePtr: ptr,
				Flags:    flags,
				Revision: revision,
			}, nil

		default:
			return node.LeafEntry{}, fmt.Errorf("invalid page type %d", n.Type())
		}
	}
	return node.LeafEntry{}, errors.New("tree too deep")
}

// GetEntryExact is an alias for GetEntry.
func (t *Tree) GetEntryExact(key []byte) (node.LeafEntry, error) {
	return t.GetEntry(key)
}

func (t *Tree) lookupLeafValueView(key []byte, dst []byte, appendMode bool) ([]byte, page.ValuePtr, byte, page.EntryRevision, bool, error) {
	currRef := page.PageChildRef(t.rootPageID)
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	for depth := 0; depth < maxTraversalDepth; depth++ {
		var (
			n                node.Node
			leafScratch      *leafRefPageScratch
			leafScratchOwned bool
			leafViewLease    LeafLogPageViewLease
			loadedLeafRef    bool
		)

		if appendMode {
			if currRef.Kind == page.ChildRefLeafLog && t.slabReader != nil {
				ptr := currRef.Log
				var (
					data  []byte
					state LeafLogPageReadState
					err   error
				)
				if t.leafLogViewState != nil {
					var ok bool
					data, leafViewLease, ok, state, err = t.leafLogViewState.ReadLeafLogPageUnsafeViewWithState(ptr)
					if err != nil {
						return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, err
					}
					if ok {
						loadedLeafRef = true
					}
				} else if t.leafLogView != nil {
					var ok bool
					data, leafViewLease, ok, err = t.leafLogView.ReadLeafLogPageUnsafeView(ptr)
					if err != nil {
						return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, err
					}
					if ok {
						loadedLeafRef = true
					}
				}
				if !loadedLeafRef {
					leafScratch = getLeafRefPageScratch()
					if t.leafLogToState != nil {
						var usedDst bool
						data, usedDst, state, err = t.leafLogToState.ReadLeafLogPageUnsafeToWithState(ptr, leafScratch.buf)
						if err != nil {
							putLeafRefPageScratch(leafScratch)
							return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, err
						}
						loadedLeafRef = true
						leafScratchOwned = usedDst
						if !usedDst {
							putLeafRefPageScratch(leafScratch)
							leafScratch = nil
						}
					} else if t.leafLogToReader != nil {
						var usedDst bool
						data, usedDst, err = t.leafLogToReader.ReadLeafLogPageUnsafeTo(ptr, leafScratch.buf)
						if err != nil {
							putLeafRefPageScratch(leafScratch)
							return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, err
						}
						loadedLeafRef = true
						leafScratchOwned = usedDst
						if !usedDst {
							putLeafRefPageScratch(leafScratch)
							leafScratch = nil
						}
					} else if t.slabToReader != nil {
						var usedDst bool
						data, usedDst, err = t.slabToReader.ReadUnsafeTo(ptr.ValuePtr(), leafScratch.buf)
						if err != nil {
							putLeafRefPageScratch(leafScratch)
							return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, err
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
							return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, err
						}
						loadedLeafRef = true
						leafScratchOwned = true
					} else {
						putLeafRefPageScratch(leafScratch)
						leafScratch = nil
					}
				}
				if loadedLeafRef {
					verifiedNow, err := validateLeafLogNodeIntoWithState(&n, data, ptr, t.shouldVerifyLeafRefChecksum(), false, state)
					if err != nil {
						if leafViewLease != nil {
							leafViewLease.ReleaseLeafLogPageView()
						}
						if leafScratchOwned {
							putLeafRefPageScratch(leafScratch)
						}
						return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, err
					}
					if verifiedNow && state.RecordChecksumVerified && state.CacheEntryPresent {
						if leafViewLease != nil {
							if marker, ok := leafViewLease.(LeafLogPageViewChecksumMarker); ok {
								marker.MarkLeafLogPageViewChecksumVerified()
							}
						} else {
							t.markLeafLogPageChecksumVerified(ptr)
						}
					}
				}
			}
		}

		if !loadedLeafRef {
			if err := t.loadChildRefViewInto(&n, currRef, verifyAlways, false); err != nil {
				return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, err
			}
		}

		switch n.Type() {
		case page.PageTypeInternal:
			if depth == 0 {
				if low, high, ok, err := n.InternalFenceBounds(); err != nil {
					return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, err
				} else if ok {
					if len(low) > 0 && compareTreeKey(key, low) < 0 {
						return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, ErrKeyNotFound
					}
					if len(high) > 0 && compareTreeKey(key, high) >= 0 {
						return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, ErrKeyNotFound
					}
				}
			}
			if n.InternalLeafLogRefsEnabled() {
				childRef, _, err := n.SearchInternalChildRef(key)
				if err != nil {
					return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, err
				}
				currRef = childRef
			} else {
				childID, _, err := n.SearchInternalChildID(key)
				if err != nil {
					return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, err
				}
				currRef = page.PageChildRef(childID)
			}

		case page.PageTypeLeaf:
			idx, found, err := n.SearchLeaf(key)
			if err != nil {
				if leafViewLease != nil {
					leafViewLease.ReleaseLeafLogPageView()
				}
				if leafScratchOwned {
					putLeafRefPageScratch(leafScratch)
				}
				return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, err
			}
			if !found {
				if leafViewLease != nil {
					leafViewLease.ReleaseLeafLogPageView()
				}
				if leafScratchOwned {
					putLeafRefPageScratch(leafScratch)
				}
				return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, ErrKeyNotFound
			}

			_, val, ptr, flags, revision, err := n.GetLeafEntryViewWithRevision(idx)
			if err != nil {
				if leafViewLease != nil {
					leafViewLease.ReleaseLeafLogPageView()
				}
				if leafScratchOwned {
					putLeafRefPageScratch(leafScratch)
				}
				return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, err
			}
			if appendMode && flags&(node.FlagPointer|node.FlagTombstone) == 0 {
				out := dst
				if val != nil {
					out = append(dst, val...)
				}
				if leafViewLease != nil {
					leafViewLease.ReleaseLeafLogPageView()
				}
				if leafScratchOwned {
					putLeafRefPageScratch(leafScratch)
				}
				return out, ptr, flags, revision, true, nil
			}
			if leafViewLease != nil {
				leafViewLease.ReleaseLeafLogPageView()
			}
			if leafScratchOwned {
				putLeafRefPageScratch(leafScratch)
			}
			return val, ptr, flags, revision, false, nil

		default:
			if leafViewLease != nil {
				leafViewLease.ReleaseLeafLogPageView()
			}
			if leafScratchOwned {
				putLeafRefPageScratch(leafScratch)
			}
			return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, fmt.Errorf("invalid page type %d", n.Type())
		}
	}

	return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, errors.New("tree too deep")
}

func (t *Tree) GetUnsafe(key []byte) ([]byte, error) {
	val, ptr, flags, _, _, err := t.lookupLeafValueView(key, nil, false)
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

func (t *Tree) appendPointerValueForKey(key []byte, ptr page.ValuePtr, dst []byte) ([]byte, error) {
	if t.slabKeyAppender != nil {
		oldLen := len(dst)
		tail, err := t.slabKeyAppender.ReadUnsafeAppendForKey(ptr, key, dst[oldLen:oldLen])
		if err != nil {
			return dst, err
		}
		if oldLen == 0 {
			recordTreeGetAppendPointer(len(tail))
			return tail, nil
		}
		if len(tail) == 0 {
			recordTreeGetAppendPointer(0)
			return dst[:oldLen], nil
		}
		if cap(dst) > oldLen {
			base := dst[:cap(dst):cap(dst)]
			if &tail[0] == &base[oldLen] {
				recordTreeGetAppendPointer(len(tail))
				return dst[:oldLen+len(tail)], nil
			}
		}
		recordTreeGetAppendPointer(len(tail))
		return append(dst[:oldLen], tail...), nil
	}
	if t.slabKeyReader != nil {
		out, err := t.slabKeyReader.ReadUnsafeForKey(ptr, key)
		if err != nil {
			return dst, err
		}
		if out == nil {
			recordTreeGetAppendPointer(0)
			return dst, nil
		}
		recordTreeGetAppendPointer(len(out))
		return append(dst, out...), nil
	}
	if t.slabAppender != nil {
		oldLen := len(dst)
		tail, err := t.slabAppender.ReadUnsafeAppend(ptr, dst[oldLen:oldLen])
		if err != nil {
			return dst, err
		}
		if oldLen == 0 {
			recordTreeGetAppendPointer(len(tail))
			return tail, nil
		}
		if len(tail) == 0 {
			recordTreeGetAppendPointer(0)
			return dst[:oldLen], nil
		}
		if cap(dst) > oldLen {
			base := dst[:cap(dst):cap(dst)]
			if &tail[0] == &base[oldLen] {
				recordTreeGetAppendPointer(len(tail))
				return dst[:oldLen+len(tail)], nil
			}
		}
		recordTreeGetAppendPointer(len(tail))
		return append(dst[:oldLen], tail...), nil
	}
	out, err := t.slabReader.ReadUnsafe(ptr)
	if err != nil {
		return dst, err
	}
	if out == nil {
		recordTreeGetAppendPointer(0)
		return dst, nil
	}
	recordTreeGetAppendPointer(len(out))
	return append(dst, out...), nil
}

// GetAppend appends the value for key to dst and returns the grown slice.
// If key is missing/tombstoned, it returns dst and ErrKeyNotFound.
func (t *Tree) GetAppend(key, dst []byte) ([]byte, error) {
	val, ptr, flags, _, appendedDirect, err := t.lookupLeafValueView(key, dst, true)
	if err != nil {
		return dst, err
	}
	if appendedDirect {
		recordTreeGetAppendInline(len(val) - len(dst))
		return val, nil
	}
	if flags&node.FlagTombstone != 0 {
		return dst, ErrKeyNotFound
	}
	if flags&node.FlagPointer != 0 {
		return t.appendPointerValueForKey(key, ptr, dst)
	}
	if val == nil {
		recordTreeGetAppendInline(0)
		return dst, nil
	}
	recordTreeGetAppendInline(len(val))
	return append(dst, val...), nil
}

// GetVersionedAppend appends the value for key to dst and returns the entry
// revision stored beside the value/pointer metadata. Missing keys return
// ErrKeyNotFound and LegacyEntryRevision; tombstones return ErrKeyNotFound with
// their stored tombstone revision.
func (t *Tree) GetVersionedAppend(key, dst []byte) ([]byte, page.EntryRevision, error) {
	val, ptr, flags, revision, appendedDirect, err := t.lookupLeafValueView(key, dst, true)
	if err != nil {
		return dst, revision, err
	}
	if appendedDirect {
		recordTreeGetAppendInline(len(val) - len(dst))
		return val, revision, nil
	}
	if flags&node.FlagTombstone != 0 {
		return dst, revision, ErrKeyNotFound
	}
	if flags&node.FlagPointer != 0 {
		out, err := t.appendPointerValueForKey(key, ptr, dst)
		return out, revision, err
	}
	if val == nil {
		recordTreeGetAppendInline(0)
		return dst, revision, nil
	}
	recordTreeGetAppendInline(len(val))
	return append(dst, val...), revision, nil
}

type getManyLeafProbe struct {
	key      []byte
	outIndex int
	next     int
}

type getManyLeafGroup struct {
	ref   page.ChildRef
	first int
	count int
}

type getManyLeafNodeLease struct {
	scratch *leafRefPageScratch
	lease   LeafLogPageViewLease
}

type getManyScratch struct {
	probes        []getManyLeafProbe
	groups        []getManyLeafGroup
	present       []bool
	groupByRef    map[page.ChildRef]int
	groupByRefCap int
}

const getManyScratchMaxReuseKeys = 8192

var getManyScratchPool sync.Pool

func (l getManyLeafNodeLease) Release() {
	if l.lease != nil {
		l.lease.ReleaseLeafLogPageView()
	}
	if l.scratch != nil {
		putLeafRefPageScratch(l.scratch)
	}
}

func getGetManyScratch(keyCount int) *getManyScratch {
	scratch, _ := getManyScratchPool.Get().(*getManyScratch)
	if scratch == nil {
		scratch = &getManyScratch{}
	}
	if cap(scratch.probes) < keyCount || cap(scratch.probes) > getManyScratchMaxReuseKeys {
		scratch.probes = make([]getManyLeafProbe, 0, keyCount)
	} else {
		scratch.probes = scratch.probes[:0]
	}
	if cap(scratch.groups) < keyCount || cap(scratch.groups) > getManyScratchMaxReuseKeys {
		scratch.groups = make([]getManyLeafGroup, 0, keyCount)
	} else {
		scratch.groups = scratch.groups[:0]
	}
	if cap(scratch.present) < keyCount || cap(scratch.present) > getManyScratchMaxReuseKeys {
		scratch.present = make([]bool, keyCount)
	} else {
		scratch.present = scratch.present[:keyCount]
		clear(scratch.present)
	}
	if scratch.groupByRef == nil || keyCount > getManyScratchMaxReuseKeys || scratch.groupByRefCap < keyCount {
		scratch.groupByRef = make(map[page.ChildRef]int, keyCount)
		scratch.groupByRefCap = keyCount
	} else {
		clear(scratch.groupByRef)
	}
	return scratch
}

func putGetManyScratch(scratch *getManyScratch) {
	if scratch == nil {
		return
	}
	if len(scratch.probes) > 0 {
		clear(scratch.probes)
	}
	if len(scratch.groups) > 0 {
		clear(scratch.groups)
	}
	if len(scratch.present) > 0 {
		clear(scratch.present)
	}
	if scratch.groupByRef != nil {
		clear(scratch.groupByRef)
	}
	if cap(scratch.probes) > getManyScratchMaxReuseKeys || cap(scratch.groups) > getManyScratchMaxReuseKeys || cap(scratch.present) > getManyScratchMaxReuseKeys {
		return
	}
	scratch.probes = scratch.probes[:0]
	scratch.groups = scratch.groups[:0]
	scratch.present = scratch.present[:0]
	getManyScratchPool.Put(scratch)
}

// GetManyAppend resolves keys into out using arena for returned safe-copy
// values. Missing keys leave nil entries in out. Present empty values use a
// shared zero-length, capacity-zero slice.
func (t *Tree) GetManyAppend(keys [][]byte, out [][]byte, arena []byte) ([]byte, error) {
	treeGetManyCallsTotal.Add(1)
	if len(out) < len(keys) {
		return arena, fmt.Errorf("GetManyAppend: out len %d < keys len %d", len(out), len(keys))
	}
	if len(keys) == 0 {
		return arena, nil
	}
	if t == nil {
		return arena, errors.New("missing tree")
	}
	if len(keys) < getManyLeafGroupMinKeys {
		treeGetManyFallbackCallsTotal.Add(1)
		return t.getManyAppendFallback(keys, out, arena)
	}
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	scratch := getGetManyScratch(len(keys))
	defer putGetManyScratch(scratch)
	for i, key := range keys {
		ref, groupable, err := t.findLeafRefForGetMany(key, verifyAlways)
		if err == ErrKeyNotFound {
			out[i] = nil
			continue
		}
		if err != nil {
			return arena, err
		}
		if !groupable {
			treeGetManyFallbackCallsTotal.Add(1)
			return t.getManyAppendFallback(keys, out, arena)
		}

		groupIdx, ok := scratch.groupByRef[ref]
		if !ok {
			groupIdx = len(scratch.groups)
			scratch.groupByRef[ref] = groupIdx
			scratch.groups = append(scratch.groups, getManyLeafGroup{ref: ref, first: -1})
		}
		probeIdx := len(scratch.probes)
		scratch.probes = append(scratch.probes, getManyLeafProbe{
			key:      key,
			outIndex: i,
			next:     scratch.groups[groupIdx].first,
		})
		scratch.groups[groupIdx].first = probeIdx
		scratch.groups[groupIdx].count++
	}
	if len(scratch.groups) == 0 {
		return arena, nil
	}

	treeGetManyGroupedCallsTotal.Add(1)
	treeGetManyLeafGroupsTotal.Add(uint64(len(scratch.groups)))
	treeGetManyLeafGroupItemsTotal.Add(uint64(len(scratch.probes)))
	if saved := len(scratch.probes) - len(scratch.groups); saved > 0 {
		treeGetManyLeafLoadsSavedTotal.Add(uint64(saved))
	}

	for i := range scratch.groups {
		g := &scratch.groups[i]
		var n node.Node
		lease, err := t.loadLeafNodeForGetMany(&n, g.ref, verifyAlways)
		if err != nil {
			lease.Release()
			return arena, err
		}
		for probeIdx := g.first; probeIdx >= 0; probeIdx = scratch.probes[probeIdx].next {
			probe := scratch.probes[probeIdx]
			var err error
			arena, err = t.appendLeafValueFromNode(&n, probe.key, out, probe.outIndex, arena)
			if err != nil {
				lease.Release()
				return arena, err
			}
		}
		lease.Release()
	}
	return arena, nil
}

// GetManyView resolves keys and calls fn once per input key with a read-only
// value view. The callback may be invoked in any order, but the index argument
// always identifies the input key. Value views are valid only until fn returns;
// callers must copy values they need after the callback. Missing or tombstoned
// keys are reported with found=false and value=nil.
func (t *Tree) GetManyView(keys [][]byte, fn GetManyViewFunc) error {
	treeGetManyCallsTotal.Add(1)
	if fn == nil {
		return errors.New("GetManyView: nil callback")
	}
	if len(keys) == 0 {
		return nil
	}
	if t == nil {
		return errors.New("missing tree")
	}
	if len(keys) < getManyLeafGroupMinKeys {
		treeGetManyFallbackCallsTotal.Add(1)
		return t.getManyViewFallback(keys, fn)
	}
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	scratch := getGetManyScratch(len(keys))
	defer putGetManyScratch(scratch)
	for i, key := range keys {
		ref, groupable, err := t.findLeafRefForGetMany(key, verifyAlways)
		if err == ErrKeyNotFound {
			continue
		}
		if err != nil {
			return err
		}
		if !groupable {
			treeGetManyFallbackCallsTotal.Add(1)
			return t.getManyViewFallback(keys, fn)
		}

		groupIdx, ok := scratch.groupByRef[ref]
		if !ok {
			groupIdx = len(scratch.groups)
			scratch.groupByRef[ref] = groupIdx
			scratch.groups = append(scratch.groups, getManyLeafGroup{ref: ref, first: -1})
		}
		probeIdx := len(scratch.probes)
		scratch.probes = append(scratch.probes, getManyLeafProbe{
			key:      key,
			outIndex: i,
			next:     scratch.groups[groupIdx].first,
		})
		scratch.groups[groupIdx].first = probeIdx
		scratch.groups[groupIdx].count++
		scratch.present[i] = true
	}

	if len(scratch.groups) > 0 {
		treeGetManyGroupedCallsTotal.Add(1)
		treeGetManyLeafGroupsTotal.Add(uint64(len(scratch.groups)))
		treeGetManyLeafGroupItemsTotal.Add(uint64(len(scratch.probes)))
		if saved := len(scratch.probes) - len(scratch.groups); saved > 0 {
			treeGetManyLeafLoadsSavedTotal.Add(uint64(saved))
		}

		for i := range scratch.groups {
			g := &scratch.groups[i]
			var n node.Node
			lease, err := t.loadLeafNodeForGetMany(&n, g.ref, verifyAlways)
			if err != nil {
				lease.Release()
				return err
			}
			for probeIdx := g.first; probeIdx >= 0; probeIdx = scratch.probes[probeIdx].next {
				probe := scratch.probes[probeIdx]
				if err := t.visitLeafValueFromNode(&n, probe.key, probe.outIndex, fn); err != nil {
					lease.Release()
					return err
				}
			}
			lease.Release()
		}
	}

	for i, key := range keys {
		if scratch.present[i] {
			continue
		}
		if err := fn(i, key, nil, false); err != nil {
			return err
		}
	}
	return nil
}

func (t *Tree) getManyViewFallback(keys [][]byte, fn GetManyViewFunc) error {
	var scratch []byte
	for i, key := range keys {
		out, err := t.GetAppend(key, scratch[:0])
		if err == ErrKeyNotFound {
			if err := fn(i, key, nil, false); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		val := out
		if len(val) == 0 {
			val = treeGetManyEmptyValue
		}
		if err := fn(i, key, val, true); err != nil {
			return err
		}
		if cap(out) <= 1<<20 {
			scratch = out[:0]
		} else {
			scratch = nil
		}
	}
	return nil
}

func (t *Tree) getManyAppendFallback(keys [][]byte, out [][]byte, arena []byte) ([]byte, error) {
	for i, key := range keys {
		start := len(arena)
		nextArena, err := t.GetAppend(key, arena)
		if err == ErrKeyNotFound {
			out[i] = nil
			continue
		}
		if err != nil {
			return arena, err
		}
		arena = nextArena
		if len(arena) == start {
			out[i] = treeGetManyEmptyValue
			continue
		}
		out[i] = arena[start:len(arena):len(arena)]
	}
	return arena, nil
}

func (t *Tree) findLeafRefForGetMany(key []byte, verifyAlways bool) (page.ChildRef, bool, error) {
	if t == nil {
		return page.ChildRef{}, false, errors.New("missing tree")
	}
	currRef := page.PageChildRef(t.rootPageID)
	for depth := 0; depth < maxTraversalDepth; depth++ {
		if currRef.Kind == page.ChildRefLeafLog {
			return currRef, true, nil
		}
		var n node.Node
		if err := t.loadChildRefViewInto(&n, currRef, verifyAlways, false); err != nil {
			return page.ChildRef{}, false, err
		}
		switch n.Type() {
		case page.PageTypeInternal:
			if depth == 0 {
				if low, high, ok, err := n.InternalFenceBounds(); err != nil {
					return page.ChildRef{}, false, err
				} else if ok {
					if len(low) > 0 && compareTreeKey(key, low) < 0 {
						return page.ChildRef{}, false, ErrKeyNotFound
					}
					if len(high) > 0 && compareTreeKey(key, high) >= 0 {
						return page.ChildRef{}, false, ErrKeyNotFound
					}
				}
			}
			if n.InternalLeafLogRefsEnabled() {
				childRef, _, err := n.SearchInternalChildRef(key)
				if err != nil {
					return page.ChildRef{}, false, err
				}
				currRef = childRef
			} else {
				childID, _, err := n.SearchInternalChildID(key)
				if err != nil {
					return page.ChildRef{}, false, err
				}
				currRef = page.PageChildRef(childID)
			}
		case page.PageTypeLeaf:
			return currRef, false, nil
		default:
			return page.ChildRef{}, false, fmt.Errorf("invalid page type %d", n.Type())
		}
	}
	return page.ChildRef{}, false, errors.New("tree too deep")
}

func (t *Tree) loadLeafNodeForGetMany(dst *node.Node, ref page.ChildRef, verifyAlways bool) (getManyLeafNodeLease, error) {
	var lease getManyLeafNodeLease
	if ref.Kind != page.ChildRefLeafLog {
		if err := t.loadChildRefViewInto(dst, ref, verifyAlways, false); err != nil {
			return lease, err
		}
		if dst.Type() != page.PageTypeLeaf {
			return lease, fmt.Errorf("invalid page type %d", dst.Type())
		}
		return lease, nil
	}
	if t.slabReader == nil {
		return lease, errors.New("missing slab reader")
	}

	ptr := ref.Log
	var (
		data        []byte
		state       LeafLogPageReadState
		leafScratch *leafRefPageScratch
		leafLease   LeafLogPageViewLease
	)
	if t.leafLogViewState != nil {
		var ok bool
		var err error
		data, leafLease, ok, state, err = t.leafLogViewState.ReadLeafLogPageUnsafeViewWithState(ptr)
		if err != nil {
			return lease, err
		}
		if ok {
			lease.lease = leafLease
			verifiedNow, err := validateLeafLogNodeIntoWithState(dst, data, ptr, t.shouldVerifyLeafRefChecksum(), false, state)
			if err != nil {
				lease.Release()
				return getManyLeafNodeLease{}, err
			}
			if verifiedNow && state.RecordChecksumVerified && state.CacheEntryPresent {
				if marker, ok := leafLease.(LeafLogPageViewChecksumMarker); ok {
					marker.MarkLeafLogPageViewChecksumVerified()
				}
			}
			return lease, nil
		}
	} else if t.leafLogView != nil {
		var ok bool
		var err error
		data, leafLease, ok, err = t.leafLogView.ReadLeafLogPageUnsafeView(ptr)
		if err != nil {
			return lease, err
		}
		if ok {
			lease.lease = leafLease
			if err := validateLeafLogNodeInto(dst, data, ptr, t.shouldVerifyLeafRefChecksum(), false); err != nil {
				lease.Release()
				return getManyLeafNodeLease{}, err
			}
			return lease, nil
		}
	}

	leafScratch = getLeafRefPageScratch()
	var usedScratch bool
	if t.leafLogToState != nil {
		var err error
		data, usedScratch, state, err = t.leafLogToState.ReadLeafLogPageUnsafeToWithState(ptr, leafScratch.buf)
		if err != nil {
			putLeafRefPageScratch(leafScratch)
			return lease, err
		}
	} else if t.leafLogToReader != nil {
		var err error
		data, usedScratch, err = t.leafLogToReader.ReadLeafLogPageUnsafeTo(ptr, leafScratch.buf)
		if err != nil {
			putLeafRefPageScratch(leafScratch)
			return lease, err
		}
	} else if t.slabToReader != nil {
		var err error
		data, usedScratch, err = t.slabToReader.ReadUnsafeTo(ptr.ValuePtr(), leafScratch.buf)
		if err != nil {
			putLeafRefPageScratch(leafScratch)
			return lease, err
		}
	} else if t.slabAppender != nil {
		var err error
		data, err = t.slabAppender.ReadUnsafeAppend(ptr.ValuePtr(), leafScratch.buf[:0])
		if err != nil {
			putLeafRefPageScratch(leafScratch)
			return lease, err
		}
		usedScratch = true
	} else {
		putLeafRefPageScratch(leafScratch)
		leafScratch = nil
		var err error
		data, err = t.slabReader.ReadUnsafe(ptr.ValuePtr())
		if err != nil {
			return lease, err
		}
	}
	if leafScratch != nil {
		if usedScratch {
			lease.scratch = leafScratch
		} else {
			putLeafRefPageScratch(leafScratch)
			leafScratch = nil
		}
	}
	verifiedNow, err := validateLeafLogNodeIntoWithState(dst, data, ptr, t.shouldVerifyLeafRefChecksum(), false, state)
	if err != nil {
		lease.Release()
		return getManyLeafNodeLease{}, err
	}
	if verifiedNow && state.RecordChecksumVerified && state.CacheEntryPresent {
		t.markLeafLogPageChecksumVerified(ptr)
	}
	return lease, nil
}

func (t *Tree) pointerValueViewForKey(key []byte, ptr page.ValuePtr) ([]byte, error) {
	if t.slabKeyReader != nil {
		return t.slabKeyReader.ReadUnsafeForKey(ptr, key)
	}
	if t.slabReader == nil {
		return nil, errors.New("missing slab reader")
	}
	return t.slabReader.ReadUnsafe(ptr)
}

func (t *Tree) visitLeafValueFromNode(n *node.Node, key []byte, outIndex int, fn GetManyViewFunc) error {
	idx, found, err := n.SearchLeaf(key)
	if err != nil {
		return err
	}
	if !found {
		return fn(outIndex, key, nil, false)
	}
	val, ptr, flags, err := n.GetLeafValueView(idx)
	if err != nil {
		return err
	}
	if flags&node.FlagTombstone != 0 {
		return fn(outIndex, key, nil, false)
	}
	if flags&node.FlagPointer != 0 {
		val, err = t.pointerValueViewForKey(key, ptr)
		if err != nil {
			return err
		}
		if len(val) == 0 {
			val = treeGetManyEmptyValue
		}
		return fn(outIndex, key, val, true)
	}
	if val == nil {
		val = treeGetManyEmptyValue
	}
	return fn(outIndex, key, val, true)
}

func (t *Tree) appendLeafValueFromNode(n *node.Node, key []byte, out [][]byte, outIndex int, arena []byte) ([]byte, error) {
	idx, found, err := n.SearchLeaf(key)
	if err != nil {
		return arena, err
	}
	if !found {
		out[outIndex] = nil
		return arena, nil
	}
	val, ptr, flags, err := n.GetLeafValueView(idx)
	if err != nil {
		return arena, err
	}
	if flags&node.FlagTombstone != 0 {
		out[outIndex] = nil
		return arena, nil
	}
	start := len(arena)
	if flags&node.FlagPointer != 0 {
		arena, err = t.appendPointerValueForKey(key, ptr, arena)
		if err != nil {
			return arena, err
		}
	} else if val != nil {
		recordTreeGetAppendInline(len(val))
		arena = append(arena, val...)
	} else {
		recordTreeGetAppendInline(0)
	}
	if len(arena) == start {
		out[outIndex] = treeGetManyEmptyValue
		return arena, nil
	}
	out[outIndex] = arena[start:len(arena):len(arena)]
	return arena, nil
}

// Get returns the value for key.
//
// When the stored value is non-empty, Get returns an owned, mutable copy of
// that value.
//
// If the key is missing or tombstoned, Get returns ErrKeyNotFound. When the
// stored value is zero-length but the key is present, Get returns a non-nil
// zero-length slice.
func (t *Tree) Get(key []byte) ([]byte, error) {
	out, err := t.GetAppend(key, nil)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return []byte{}, nil
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
	currRef := page.PageChildRef(t.rootPageID)
	verifyAlways := false
	if t.pager != nil {
		verifyAlways = t.pager.VerifyOnRead()
	}

	for depth := 0; depth < maxTraversalDepth; depth++ {
		var n node.Node
		if err := t.loadChildRefViewInto(&n, currRef, verifyAlways, false); err != nil {
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
			if n.InternalLeafLogRefsEnabled() {
				childRef, _, err := n.SearchInternalChildRef(key)
				if err != nil {
					return false, err
				}
				currRef = childRef
			} else {
				childID, _, err := n.SearchInternalChildID(key)
				if err != nil {
					return false, err
				}
				currRef = page.PageChildRef(childID)
			}
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
			return false, fmt.Errorf("invalid page type %d", n.Type())
		}
	}

	return false, errors.New("tree too deep")
}

func (t *Tree) HasMany(keys [][]byte) ([]bool, error) {
	out := make([]bool, len(keys))
	for i, key := range keys {
		ok, err := t.Has(key)
		if err != nil {
			return nil, err
		}
		out[i] = ok
	}
	return out, nil
}

func (t *Tree) HasAnySorted(keys [][]byte) (bool, error) {
	ok, _, err := t.HasAnySortedWithStats(keys)
	return ok, err
}

func (t *Tree) HasAnySortedWithStats(keys [][]byte) (bool, ProbeFallbackStats, error) {
	var stats ProbeFallbackStats
	if len(keys) == 0 {
		return false, stats, nil
	}
	for i := 1; i < len(keys); i++ {
		if compareTreeKey(keys[i-1], keys[i]) > 0 {
			return false, stats, errors.New("HasAnySorted: keys must be sorted in ascending compareTreeKey order (8-byte keys are compared as big-endian uint64)")
		}
	}

	scanLimit := len(keys) * 4
	if scanLimit < 1024 {
		scanLimit = 1024
	}

	it := t.IteratorWithOptions(keys[0], nil, IteratorOptions{Mode: IteratorModeKeysOnly})
	defer func() { _ = it.Close() }()
	targetIdx := 0
	scanned := 0
	limitExceeded := false
	for targetIdx < len(keys) {
		if !it.Valid() {
			if err := it.Error(); err != nil {
				return false, stats, err
			}
			return false, stats, nil
		}
		curr := it.UnsafeKey()
		switch cmp := compareTreeKey(curr, keys[targetIdx]); {
		case cmp == 0:
			if !it.IsDeleted() {
				return true, stats, it.Error()
			}
			target := keys[targetIdx]
			for targetIdx < len(keys) && compareTreeKey(keys[targetIdx], target) == 0 {
				targetIdx++
			}
			it.Next()
		case cmp < 0:
			scanned++
			if scanned > scanLimit {
				limitExceeded = true
				goto fallback
			}
			it.Next()
		default:
			for targetIdx < len(keys) && compareTreeKey(keys[targetIdx], curr) < 0 {
				targetIdx++
			}
		}
	}

fallback:
	if err := it.Error(); err != nil {
		return false, stats, err
	}
	if !limitExceeded {
		return false, stats, nil
	}
	stats.FallbackCalls = 1
	for targetIdx < len(keys) {
		target := keys[targetIdx]
		stats.FallbackItems++
		ok, err := t.Has(target)
		if err != nil {
			return false, stats, err
		}
		if ok {
			return true, stats, nil
		}
		targetIdx++
		for targetIdx < len(keys) && compareTreeKey(keys[targetIdx], target) == 0 {
			targetIdx++
		}
	}
	return false, stats, nil
}

func (t *Tree) HasPrefixes(prefixes [][]byte) ([]bool, error) {
	out, _, err := t.HasPrefixesWithStats(prefixes)
	return out, err
}

func (t *Tree) HasPrefixesWithStats(prefixes [][]byte) ([]bool, ProbeFallbackStats, error) {
	var stats ProbeFallbackStats
	out := make([]bool, len(prefixes))
	if len(prefixes) == 0 {
		return out, stats, nil
	}
	if len(prefixes) == 1 {
		found, err := t.hasPrefix(prefixes[0])
		if err != nil {
			return nil, stats, err
		}
		out[0] = found
		return out, stats, nil
	}

	type prefixProbeRef struct {
		prefix []byte
		idx    int
	}
	refs := make([]prefixProbeRef, len(prefixes))
	for i, prefix := range prefixes {
		refs[i] = prefixProbeRef{prefix: prefix, idx: i}
	}
	sort.Slice(refs, func(i, j int) bool {
		return compareTreeKey(refs[i].prefix, refs[j].prefix) < 0
	})

	it := t.IteratorWithOptions(refs[0].prefix, nil, IteratorOptions{Mode: IteratorModeKeysOnly})
	defer func() { _ = it.Close() }()
	for start := 0; start < len(refs); {
		prefix := refs[start].prefix
		end := start + 1
		for end < len(refs) && bytes.Equal(refs[end].prefix, prefix) {
			end++
		}

		it.Seek(prefix)
		found := false
		for {
			if !it.Valid() {
				if err := it.Error(); err != nil {
					return nil, stats, err
				}
				return out, stats, nil
			}
			curr := it.UnsafeKey()
			if compareTreeKey(curr, prefix) < 0 {
				it.Seek(prefix)
				continue
			}
			if !bytes.HasPrefix(curr, prefix) {
				break
			}
			if !it.IsDeleted() {
				found = true
				break
			}
			it.Next()
		}
		if found {
			for _, ref := range refs[start:end] {
				out[ref.idx] = true
			}
		}
		start = end
	}
	if err := it.Error(); err != nil {
		return nil, stats, err
	}
	return out, stats, nil
}

func (t *Tree) hasPrefix(prefix []byte) (bool, error) {
	it := t.IteratorWithOptions(prefix, nil, IteratorOptions{Mode: IteratorModeKeysOnly})
	defer func() { _ = it.Close() }()
	for {
		if !it.Valid() {
			if err := it.Error(); err != nil {
				return false, err
			}
			return false, nil
		}
		curr := it.UnsafeKey()
		if compareTreeKey(curr, prefix) < 0 {
			it.Seek(prefix)
			continue
		}
		if !bytes.HasPrefix(curr, prefix) {
			return false, it.Error()
		}
		if !it.IsDeleted() {
			return true, it.Error()
		}
		it.Next()
	}
}
