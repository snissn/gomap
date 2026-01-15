package zipper

import (
	"bytes"
	"errors"
	"log"
	"runtime"
	"sync"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type PageAllocator interface {
	Alloc(hint uint64) (uint64, error)
}

type Zipper struct {
	pager     *pager.Pager
	allocator PageAllocator

	leafReserveBytes          int
	internalReserveBytes      int
	piggybackCompaction       bool
	leafPrefixCompression     bool
	maintenanceDeleteMinRatio float64
}

type Split struct {
	Key    []byte
	NodeID uint64
}

func shortestSeparator(left, right []byte) []byte {
	if len(right) == 0 {
		return nil
	}
	if len(left) == 0 {
		return append([]byte(nil), right...)
	}
	if bytes.Compare(left, right) >= 0 {
		// If keys are equal or left is greater, there is no shorter valid separator.
		// Return a copy of right to preserve ordering without special-casing callers.
		return append([]byte(nil), right...)
	}

	n := len(left)
	if len(right) < n {
		n = len(right)
	}
	i := 0
	for i < n && left[i] == right[i] {
		i++
	}
	if i == n {
		return append([]byte(nil), right...)
	}
	if left[i]+1 < right[i] {
		sep := make([]byte, i+1)
		copy(sep, left[:i])
		sep[i] = left[i] + 1
		return sep
	}
	if left[i]+1 == right[i] && i+1 < len(right) {
		return append([]byte(nil), right[:i+1]...)
	}
	return append([]byte(nil), right...)
}

type internalEntry struct {
	key   []byte
	child uint64
}

type childWork struct {
	key       []byte
	childID   uint64
	ops       []batch.Entry
	newChild  uint64
	splits    []Split
	retired   []uint64
	childStat adaptive.Metrics
}

const maxChildWorkCap = 1 << 14

var childWorkPool sync.Pool

func getChildWorkSlice(capacity int) []childWork {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > maxChildWorkCap {
		return make([]childWork, 0, capacity)
	}
	if v := childWorkPool.Get(); v != nil {
		s := v.([]childWork)
		if cap(s) >= capacity {
			return s[:0]
		}
	}
	return make([]childWork, 0, capacity)
}

func putChildWorkSlice(children []childWork) {
	if cap(children) > maxChildWorkCap {
		return
	}
	for i := range children {
		children[i] = childWork{}
	}
	childWorkPool.Put(children[:0])
}

func New(p *pager.Pager, a PageAllocator) *Zipper {
	return &Zipper{
		pager:     p,
		allocator: a,
	}
}

// CloneWithAllocator returns a zipper that shares config/pager with z but uses
// the provided allocator.
func (z *Zipper) CloneWithAllocator(a PageAllocator) *Zipper {
	return &Zipper{
		pager:                     z.pager,
		allocator:                 a,
		leafReserveBytes:          z.leafReserveBytes,
		internalReserveBytes:      z.internalReserveBytes,
		piggybackCompaction:       z.piggybackCompaction,
		leafPrefixCompression:     z.leafPrefixCompression,
		maintenanceDeleteMinRatio: z.maintenanceDeleteMinRatio,
	}
}

// SetFillTargets configures soft-full thresholds for newly written pages.
// Targets are in parts-per-million where 1_000_000 means "allow full pages".
func (z *Zipper) SetFillTargets(leafPPM, internalPPM uint32) {
	z.leafReserveBytes = reserveBytesFromPPM(leafPPM)
	z.internalReserveBytes = reserveBytesFromPPM(internalPPM)
}

func (z *Zipper) SetPiggybackCompaction(enabled bool) {
	z.piggybackCompaction = enabled
}

func (z *Zipper) SetLeafPrefixCompression(enabled bool) {
	z.leafPrefixCompression = enabled
}

// SetMaintenanceDeleteMinRatio sets a delete ratio threshold before triggering
// maintenance merges on write (0 keeps existing behavior).
func (z *Zipper) SetMaintenanceDeleteMinRatio(ratio float64) {
	if ratio < 0 {
		ratio = 0
	}
	z.maintenanceDeleteMinRatio = ratio
}

func (z *Zipper) LeafPrefixCompression() bool {
	return z.leafPrefixCompression
}

func (z *Zipper) newLeafBuilder(data []byte) *node.Builder {
	if z != nil && z.leafPrefixCompression {
		return node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{LeafPrefixCompression: true})
	}
	return node.NewBuilder(data, page.PageTypeLeaf)
}

func (z *Zipper) newBuilderForType(data []byte, typ page.PageType) *node.Builder {
	if typ == page.PageTypeLeaf {
		return z.newLeafBuilder(data)
	}
	return node.NewBuilder(data, typ)
}

func reserveBytesFromPPM(ppm uint32) int {
	if ppm >= 1_000_000 {
		return 0
	}
	// Reserve a fixed fraction of the page size.
	reserve := int((uint64(page.PageSize) * uint64(1_000_000-ppm)) / 1_000_000)
	if reserve < 0 {
		return 0
	}
	return reserve
}

func (z *Zipper) leafSoftFull(b *node.Builder, entrySize int) bool {
	if z.leafReserveBytes <= 0 || b == nil || b.Count() == 0 {
		return false
	}
	return b.FreeSpace() < entrySize+node.DirectoryEntrySize+z.leafReserveBytes
}

func (z *Zipper) internalSoftFull(b *node.Builder, entrySize int) bool {
	if z.internalReserveBytes <= 0 || b == nil || b.Count() == 0 {
		return false
	}
	return b.FreeSpace() < entrySize+node.DirectoryEntrySize+z.internalReserveBytes
}

// Apply applies the batch to the tree rooted at rootID.
// Returns the new root page ID, list of retired pages, and commit metrics.
func (z *Zipper) Apply(rootID uint64, b *batch.Batch) (uint64, []uint64, adaptive.Metrics, error) {
	var metrics adaptive.Metrics
	ops := b.SortedEntries()
	if len(ops) == 0 {
		return rootID, nil, metrics, nil
	}

	// Underfull merge/rebalance maintenance is only beneficial when:
	//   - the batch includes deletes (can create empty/underfull pages), or
	//   - the caller configured soft-full targets (reserve bytes), which implies
	//     a preference for more balanced/less churny pages even on updates.
	deleteCount := 0
	for _, op := range ops {
		if op.Type == batch.OpDelete {
			deleteCount++
		}
	}
	hasDeletes := deleteCount > 0
	if hasDeletes && z.maintenanceDeleteMinRatio > 0 {
		deleteRatio := float64(deleteCount) / float64(len(ops))
		if deleteRatio < z.maintenanceDeleteMinRatio {
			hasDeletes = false
		}
	}
	maintenance := hasDeletes || z.leafReserveBytes > 0 || z.internalReserveBytes > 0 || z.piggybackCompaction

	newRoot, splits, retired, err := z.writeRecursive(rootID, ops, maintenance, &metrics, 0)
	if err != nil {
		return 0, nil, metrics, err
	}

	if len(splits) > 0 {
		// Root split!
		// The children for the next level are:
		// 1. The new version of the old root (newRoot) with Key=[] (effectively min key)
		// 2. The splits (siblings) generated from it.

		currentLevelNodes := []Split{{Key: []byte{}, NodeID: newRoot}}
		currentLevelNodes = append(currentLevelNodes, splits...)

		// Iteratively build levels up until all nodes fit in one root.
		for {
			// If we only have 1 node left, that is our new root.
			if len(currentLevelNodes) == 1 {
				return currentLevelNodes[0].NodeID, retired, metrics, nil
			}

			var nextLevelNodes []Split

			// Allocate a node for the current batch of children
			var currentBuilder *node.Builder

			// We need to track the "Start Key" of the current builder to promote it.
			var currentStartKey []byte

			for i, child := range currentLevelNodes {
				if currentBuilder == nil {
					// Start new node
					pid, err := z.allocator.Alloc(newRoot)
					if err != nil {
						return 0, nil, metrics, err
					}
					data, err := z.pager.GetForWrite(pid)
					if err != nil {
						return 0, nil, metrics, err
					}

					currentBuilder = node.NewBuilder(data, page.PageTypeInternal)
					currentBuilder.SetPageID(pid)

					currentStartKey = child.Key
				}

				// Add child
				childSize := 2 + 8 + len(child.Key)
				var err error
				if z.internalSoftFull(currentBuilder, childSize) {
					err = node.ErrNodeFull
				} else {
					err = currentBuilder.AddInternalChild(child.Key, child.NodeID)
				}
				if err == node.ErrNodeFull {
					// Finish current
					_ = currentBuilder.Finish()
					// Promote
					nextLevelNodes = append(nextLevelNodes, Split{Key: currentStartKey, NodeID: currentBuilder.PageID()})

					// Start new for THIS child (retry)
					pid, err := z.allocator.Alloc(currentBuilder.PageID())
					if err != nil {
						return 0, nil, metrics, err
					}
					data, err := z.pager.GetForWrite(pid)
					if err != nil {
						return 0, nil, metrics, err
					}
					currentBuilder = node.NewBuilder(data, page.PageTypeInternal)
					currentBuilder.SetPageID(pid)
					currentStartKey = child.Key

					if err := currentBuilder.AddInternalChild(child.Key, child.NodeID); err != nil {
						return 0, nil, metrics, err // Should fit in empty node
					}
				} else if err != nil {
					return 0, nil, metrics, err
				}

				// If this was the last child, finish
				if i == len(currentLevelNodes)-1 {
					_ = currentBuilder.Finish()
					nextLevelNodes = append(nextLevelNodes, Split{Key: currentStartKey, NodeID: currentBuilder.PageID()})
					currentBuilder = nil
				}
			}

			// Move up
			currentLevelNodes = nextLevelNodes
		}
	}

	return newRoot, retired, metrics, nil
}

// writeRecursive handles the COW merge.
// Returns: newPageID, splits, retiredPages, error.
func (z *Zipper) writeRecursive(pageID uint64, ops []batch.Entry, maintenance bool, metrics *adaptive.Metrics, depth int) (uint64, []Split, []uint64, error) {
	var retired []uint64
	for {
		oldData, err := z.pager.Get(pageID)
		if err != nil {
			return 0, nil, nil, err
		}
		oldNode := node.NewNodeView(oldData)
		if oldNode.Type() != page.PageTypeInternal || oldNode.Count() != 1 {
			break
		}
		childID, err := oldNode.GetInternalChildID(0)
		if err != nil {
			break
		}
		if childID == pageID {
			return 0, nil, nil, errors.New("zipper: detected self-referential child")
		}
		if pageID != 0 {
			retired = append(retired, pageID)
		}
		pageID = childID
	}
	if depth > 50 {
		var pageType page.PageType
		var count uint16
		var headerID uint64
		if data, err := z.pager.Get(pageID); err == nil {
			n := node.NewNodeView(data)
			pageType = n.Type()
			count = n.Count()
			headerID = n.PageID()
		}
		log.Printf("treedb: zipper depth limit hit page_id=%d header_id=%d depth=%d ops=%d page_type=%v count=%d",
			pageID,
			headerID,
			depth,
			len(ops),
			pageType,
			count,
		)
		return 0, nil, nil, errors.New("zipper: tree too deep or cycle detected")
	}

	// Read Page
	// Hint: Try to stay near the old page to preserve general tree locality structure
	newPageID, err := z.allocator.Alloc(pageID)
	if err != nil {
		return 0, nil, nil, err
	}

	newData, err := z.pager.GetForWrite(newPageID)
	if err != nil {
		return 0, nil, nil, err
	}

	// Zero-Copy Read: Use Get instead of ReadPage
	oldData, err := z.pager.Get(pageID)
	if err != nil {
		return 0, nil, nil, err
	}
	oldNode := node.NewNodeView(oldData)

	// Create Builder for new page
	builder := z.newBuilderForType(newData, oldNode.Type())
	builder.SetPageID(newPageID)

	// Track retired page
	if pageID != 0 {
		retired = append(retired, pageID)
	}

	if oldNode.Type() == page.PageTypeLeaf {
		// Merge Leaf
		nr, splits, err := z.mergeLeaf(oldNode, builder, ops, metrics)
		if err == nil {
			n := builder.Finish() // Finish writes header/checksum
			// Update Metrics
			metrics.IndexWriteBytes += page.PageSize
			metrics.LeafFill += float64(page.PageSize-n.FreeSpace()) / float64(page.PageSize)
		}
		return nr, splits, retired, err
	} else if oldNode.Type() == page.PageTypeInternal {
		// Internal merge
		nr, splits, childRetired, err := z.mergeInternal(oldNode, builder, ops, maintenance, metrics, depth)
		if err != nil {
			return 0, nil, nil, err
		}

		n := builder.Finish()
		metrics.IndexWriteBytes += page.PageSize

		retired = append(retired, childRetired...)

		// If this internal page collapsed to a single child, skip writing the
		// redundant level by returning the child directly.
		if n.Count() == 1 {
			childID, err := n.GetInternalChildID(0)
			if err == nil {
				retired = append(retired, nr)
				return childID, splits, retired, nil
			}
		}
		splits, retired, err = z.collapseSingleChildSplits(splits, retired)
		if err != nil {
			return 0, nil, nil, err
		}
		return nr, splits, retired, nil
	} else {
		// Handle Page 0 / Empty / New Tree case
		if oldNode.Type() == 0 {
			// Reuse builder, set type
			builder = z.newLeafBuilder(newData)
			builder.SetPageID(newPageID)

			nr, splits, err := z.mergeLeaf(oldNode, builder, ops, metrics)
			if err == nil {
				n := builder.Finish()
				metrics.IndexWriteBytes += page.PageSize
				metrics.LeafFill += float64(page.PageSize-n.FreeSpace()) / float64(page.PageSize)
			}
			return nr, splits, retired, err
		}
		return 0, nil, nil, page.ErrInvalidPageType
	}
}

func (z *Zipper) mergeLeaf(oldNode node.Node, builder *node.Builder, ops []batch.Entry, metrics *adaptive.Metrics) (uint64, []Split, error) {
	oldIdx := uint16(0)
	oldCount := oldNode.Count()
	opIdx := 0
	oldLoaded := false
	var oldKey, oldVal []byte
	var oldPtr page.ValuePtr
	var oldFlags byte
	var lastKey []byte

	var splits []Split

	// Current target builder
	target := builder

	for {
		// Pick next key: min(oldNode[oldIdx], ops[opIdx])
		var useBatch bool

		if oldIdx >= oldCount && opIdx >= len(ops) {
			break
		}

		if oldIdx >= oldCount {
			useBatch = true
		} else if opIdx >= len(ops) {
			// useOld = true
		} else {
			// Compare
			// Optimization: GetLeafEntryView (Zero Copy)
			if !oldLoaded {
				var err error
				oldKey, oldVal, oldPtr, oldFlags, err = oldNode.GetLeafEntryView(oldIdx)
				if err != nil {
					return 0, nil, err
				}
				oldLoaded = true
			}
			batchKey := ops[opIdx].Key

			cmp := bytes.Compare(oldKey, batchKey)
			if cmp < 0 {
				// useOld = true
			} else if cmp > 0 {
				useBatch = true
			} else {
				// Equal: Update (Batch wins)
				// The old entry is being overwritten or deleted.
				// If it was a pointer, track it as dead bytes.
				if oldFlags&node.FlagPointer != 0 {
					metrics.SlabDeadBytes += int(oldPtr.Length)
					if metrics.SlabDeadBytesByFile == nil {
						metrics.SlabDeadBytesByFile = make(map[uint32]int64, 4)
					}
					metrics.SlabDeadBytesByFile[oldPtr.FileID] += int64(oldPtr.Length)
				}

				useBatch = true
				oldIdx++ // Skip old
				oldLoaded = false
			}
		}

		// Key/Val to insert
		var key, val []byte
		var flags byte
		var valPtr page.ValuePtr

		if useBatch {
			op := ops[opIdx]
			opIdx++
			if op.Type == batch.OpDelete {
				continue // Skip insert
			}
			key = op.Key
			if op.Flags != 0 {
				flags = op.Flags
				if op.IsPtr {
					valPtr = op.ValuePtr
				} else {
					val = op.Value
				}
			} else if op.IsPtr {
				flags = node.FlagPointer
				valPtr = op.ValuePtr
			} else {
				flags = node.FlagInline
				val = op.Value
			}
		} else {
			// useOld
			// Optimization: View
			if !oldLoaded {
				var err error
				oldKey, oldVal, oldPtr, oldFlags, err = oldNode.GetLeafEntryView(oldIdx)
				if err != nil {
					return 0, nil, err
				}
				oldLoaded = true
			}
			oldIdx++
			oldLoaded = false
			key = oldKey
			if oldFlags&node.FlagTombstone != 0 {
				continue // Skip tombstones
			}
			flags = oldFlags
			if oldFlags&node.FlagPointer != 0 {
				valPtr = oldPtr
			} else {
				val = oldVal
			}
		}

		// Insert into target builder
		entrySize, prefixLen, suffixLen := target.LeafEntrySizeWithPrefix(key, val, flags)
		var err error
		if z.leafSoftFull(target, entrySize) {
			err = node.ErrNodeFull
		} else {
			err = target.AddLeafEntryWithPrefix(key, val, flags, valPtr, entrySize, prefixLen, suffixLen)
		}
		if err == node.ErrNodeFull {
			// SPLIT!

			// 1. Finish current target (writes header/checksum)
			if target != builder {
				n := target.Finish()
				metrics.IndexWriteBytes += page.PageSize
				metrics.LeafFill += float64(page.PageSize-n.FreeSpace()) / float64(page.PageSize)
				metrics.Splits++
			}

			// 2. Allocate NEW split node
			sid, err := z.allocator.Alloc(builder.PageID())
			if err != nil {
				return 0, nil, err
			}

			sdata, err := z.pager.GetForWrite(sid)
			if err != nil {
				return 0, nil, err
			}

			// New Builder
			splitBuilder := z.newLeafBuilder(sdata)
			splitBuilder.SetPageID(sid)

			// Record split
			splitKey := append([]byte(nil), key...) // Deep copy
			if len(lastKey) > 0 {
				splitKey = shortestSeparator(lastKey, key)
			}
			splits = append(splits, Split{Key: splitKey, NodeID: sid})

			target = splitBuilder
			lastKey = lastKey[:0]

			// Retry insert
			entrySize, prefixLen, suffixLen = target.LeafEntrySizeWithPrefix(key, val, flags)
			err = target.AddLeafEntryWithPrefix(key, val, flags, valPtr, entrySize, prefixLen, suffixLen)
			if err != nil {
				return 0, nil, err
			}
			lastKey = append(lastKey[:0], key...)
		} else if err != nil {
			return 0, nil, err
		} else {
			lastKey = append(lastKey[:0], key...)
		}
	}

	// Finalize last split node
	if target != builder {
		n := target.Finish()
		metrics.IndexWriteBytes += page.PageSize
		metrics.LeafFill += float64(page.PageSize-n.FreeSpace()) / float64(page.PageSize)
		metrics.Splits++
	}

	// 'builder' is finalized by caller.

	return builder.PageID(), splits, nil
}

func (z *Zipper) mergeInternal(oldNode node.Node, builder *node.Builder, ops []batch.Entry, maintenance bool, metrics *adaptive.Metrics, depth int) (uint64, []Split, []uint64, error) {
	count := oldNode.Count()

	var splits []Split

	var retired []uint64

	var err error

	opIdx := 0

	children := getChildWorkSlice(int(count))
	defer putChildWorkSlice(children)

	parentID := oldNode.PageID()
	for i := uint16(0); i < count; i++ {
		// Optimization: Use View to avoid alloc
		key, childID, err := oldNode.GetInternalEntryView(i)
		if err != nil {
			return 0, nil, nil, err
		}
		if childID == parentID {
			log.Printf("treedb: zipper self-child detected page_id=%d entry=%d", parentID, i)
		}

		// Determine End Key for this child
		var endKey []byte
		if i+1 < count {
			nextKey, _, err := oldNode.GetInternalEntryView(i + 1)
			if err != nil {
				return 0, nil, nil, err
			}
			endKey = nextKey
		}

		// Identify ops range for this child
		// ops[opIdx] ... until op.Key >= endKey
		startOpIdx := opIdx
		for opIdx < len(ops) {
			if endKey == nil || bytes.Compare(ops[opIdx].Key, endKey) < 0 {
				opIdx++
			} else {
				break
			}
		}
		childOps := ops[startOpIdx:opIdx]

		children = append(children, childWork{
			key:     key,
			childID: childID,
			ops:     childOps,
		})
	}

	const (
		minParallelChildren = 4
		minParallelOps      = 1024
	)

	useParallel := len(children) >= minParallelChildren && len(ops) >= minParallelOps && runtime.GOMAXPROCS(0) > 1

	if useParallel {
		maxParallel := runtime.GOMAXPROCS(0)
		if maxParallel < 1 {
			maxParallel = 1
		}
		jobs := make(chan int, len(children))
		for i := range children {
			if len(children[i].ops) == 0 {
				children[i].newChild = children[i].childID
				continue
			}
			jobs <- i
		}
		close(jobs)
		var wg sync.WaitGroup
		var firstErr error
		var errOnce sync.Once
		worker := func() {
			defer wg.Done()
			for i := range jobs {
				var childMetrics adaptive.Metrics
				ncID, cs, childRet, err := z.writeRecursive(children[i].childID, children[i].ops, maintenance, &childMetrics, depth+1)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					continue
				}
				children[i].newChild = ncID
				children[i].splits = cs
				children[i].retired = childRet
				children[i].childStat = childMetrics
			}
		}
		for i := 0; i < maxParallel; i++ {
			wg.Add(1)
			go worker()
		}
		wg.Wait()
		if firstErr != nil {
			return 0, nil, nil, firstErr
		}
		for i := range children {
			if len(children[i].ops) == 0 {
				continue
			}
			mergeMetrics(metrics, &children[i].childStat)
			if len(children[i].retired) > 0 {
				retired = append(retired, children[i].retired...)
			}
		}
	} else {
		for i := range children {
			if len(children[i].ops) > 0 {
				ncID, cs, childRet, err := z.writeRecursive(children[i].childID, children[i].ops, maintenance, metrics, depth+1)
				if err != nil {
					return 0, nil, nil, err
				}
				children[i].newChild = ncID
				children[i].splits = cs
				children[i].retired = childRet
				retired = append(retired, childRet...)
			} else {
				children[i].newChild = children[i].childID
			}
		}
	}

	entries := make([]internalEntry, 0, len(children)+4)
	for i := range children {
		child := children[i]
		if child.newChild >= z.pager.PageCount() {
			return 0, nil, nil, errors.New("zipper: detected OOB child ID")
		}
		entries = append(entries, internalEntry{key: append([]byte(nil), child.key...), child: child.newChild})

		// Add sibling splits
		for _, s := range child.splits {
			if s.NodeID >= z.pager.PageCount() {
				return 0, nil, nil, errors.New("zipper: detected OOB child ID (split)")
			}
			entries = append(entries, internalEntry{key: append([]byte(nil), s.Key...), child: s.NodeID})
		}
	}

	coalesced := entries
	if maintenance {
		var extraRetired []uint64
		coalesced, extraRetired, err = z.coalesceLeafChildren(entries, metrics)
		if err != nil {
			return 0, nil, nil, err
		}
		if len(extraRetired) > 0 {
			retired = append(retired, extraRetired...)
		}

		coalesced, extraRetired, err = z.coalesceInternalChildren(coalesced, metrics)
		if err != nil {
			return 0, nil, nil, err
		}
		if len(extraRetired) > 0 {
			retired = append(retired, extraRetired...)
		}
	}

	// Write final internal entries, splitting if needed.
	target := builder
	for i, e := range coalesced {
		if i == 0 && (e.key == nil) {
			e.key = []byte{}
		}
		entrySize := 2 + 8 + len(e.key)
		if z.internalSoftFull(target, entrySize) {
			err = node.ErrNodeFull
		} else {
			err = target.AddInternalChild(e.key, e.child)
		}
		if err == node.ErrNodeFull {
			target, err = z.createNewSplitInternal(target, builder, &splits, e.key, e.child, metrics)
			if err != nil {
				return 0, nil, nil, err
			}
		} else if err != nil {
			return 0, nil, nil, err
		}
	}

	// Finalize last split node
	if target != builder {
		_ = target.Finish()
		metrics.IndexWriteBytes += page.PageSize
	}

	// builder finalized by caller.
	return builder.PageID(), splits, retired, nil
}

func (z *Zipper) collapseSingleChildSplits(splits []Split, retired []uint64) ([]Split, []uint64, error) {
	if len(splits) == 0 {
		return splits, retired, nil
	}
	for i := range splits {
		data, err := z.pager.Get(splits[i].NodeID)
		if err != nil {
			return nil, nil, err
		}
		n := node.NewNodeView(data)
		if n.Type() != page.PageTypeInternal || n.Count() != 1 {
			continue
		}
		childID, err := n.GetInternalChildID(0)
		if err != nil {
			return nil, nil, err
		}
		retired = append(retired, splits[i].NodeID)
		splits[i].NodeID = childID
	}
	return splits, retired, nil
}

func mergeMetrics(dst, src *adaptive.Metrics) {
	if dst == nil || src == nil {
		return
	}
	dst.LeafFill += src.LeafFill
	dst.Splits += src.Splits
	dst.IndexWriteBytes += src.IndexWriteBytes
	dst.SlabWriteBytes += src.SlabWriteBytes
	dst.SlabDeadBytes += src.SlabDeadBytes

	if src.SlabWriteBytesByFile != nil {
		if dst.SlabWriteBytesByFile == nil {
			dst.SlabWriteBytesByFile = make(map[uint32]int64, len(src.SlabWriteBytesByFile))
		}
		for id, n := range src.SlabWriteBytesByFile {
			dst.SlabWriteBytesByFile[id] += n
		}
	}
	if src.SlabDeadBytesByFile != nil {
		if dst.SlabDeadBytesByFile == nil {
			dst.SlabDeadBytesByFile = make(map[uint32]int64, len(src.SlabDeadBytesByFile))
		}
		for id, n := range src.SlabDeadBytesByFile {
			dst.SlabDeadBytesByFile[id] += n
		}
	}
}

func (z *Zipper) coalesceLeafChildren(entries []internalEntry, metrics *adaptive.Metrics) ([]internalEntry, []uint64, error) {
	if len(entries) < 2 {
		return entries, nil, nil
	}

	var retired []uint64

	loadLeaf := func(id uint64) (node.Node, bool, error) {
		data, err := z.pager.Get(id)
		if err != nil {
			return node.Node{}, false, err
		}
		n := node.NewNodeView(data)
		if n.Type() != page.PageTypeLeaf {
			return node.Node{}, false, nil
		}
		return n, true, nil
	}

	// First pass: prune empty leaf children (except keep the first slot).
	out := entries[:0]
	for i, e := range entries {
		if i == 0 {
			out = append(out, e)
			continue
		}
		n, ok, err := loadLeaf(e.child)
		if err != nil {
			return nil, nil, err
		}
		if ok && n.Count() == 0 {
			retired = append(retired, e.child)
			continue
		}
		out = append(out, e)
	}
	entries = out
	if len(entries) < 2 {
		return entries, retired, nil
	}

	const underfullPPM = 350_000
	pageCap := page.PageSize - node.NodeHeaderSize

	leafEntryBytes := func(key, val []byte, ptr page.ValuePtr, flags byte) int {
		entrySize := 7 + len(key)
		if flags&node.FlagPointer != 0 {
			entrySize += page.ValuePtrSize
		} else {
			entrySize += len(val)
		}
		return entrySize + node.DirectoryEntrySize
	}

	leafRequiredBytes := func(n node.Node) (int, error) {
		sum := 0
		for i := uint16(0); i < n.Count(); i++ {
			k, v, ptr, flags, err := n.GetLeafEntryView(i)
			if err != nil {
				return 0, err
			}
			if flags&node.FlagTombstone != 0 {
				continue
			}
			sum += leafEntryBytes(k, v, ptr, flags)
			if sum > pageCap {
				return sum, nil
			}
		}
		return sum, nil
	}

	fillPPM := func(n node.Node) uint32 {
		used := page.PageSize - n.FreeSpace()
		return uint32((used * 1_000_000) / page.PageSize)
	}

	buildMergedLeaf := func(left, right node.Node) (uint64, bool, error) {
		pid, err := z.allocator.Alloc(left.PageID())
		if err != nil {
			return 0, false, err
		}
		data, err := z.pager.GetForWrite(pid)
		if err != nil {
			return 0, false, err
		}
		b := z.newLeafBuilder(data)
		b.SetPageID(pid)

		addAll := func(n node.Node) error {
			for i := uint16(0); i < n.Count(); i++ {
				k, v, ptr, flags, err := n.GetLeafEntryView(i)
				if err != nil {
					return err
				}
				if flags&node.FlagTombstone != 0 {
					continue
				}
				if err := b.AddLeafEntry(k, v, flags, ptr); err != nil {
					return err
				}
			}
			return nil
		}

		if err := addAll(left); err != nil {
			retired = append(retired, pid)
			if err == node.ErrNodeFull {
				return 0, false, nil
			}
			return 0, false, err
		}
		if err := addAll(right); err != nil {
			retired = append(retired, pid)
			if err == node.ErrNodeFull {
				return 0, false, nil
			}
			return 0, false, err
		}

		n := b.Finish()
		metrics.IndexWriteBytes += page.PageSize
		metrics.LeafFill += float64(page.PageSize-n.FreeSpace()) / float64(page.PageSize)
		return pid, true, nil
	}

	copyLeaf := func(id uint64, hint uint64) (uint64, error) {
		n, ok, err := loadLeaf(id)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, errors.New("copyLeaf: not a leaf")
		}

		pid, err := z.allocator.Alloc(hint)
		if err != nil {
			return 0, err
		}
		data, err := z.pager.GetForWrite(pid)
		if err != nil {
			return 0, err
		}
		b := z.newLeafBuilder(data)
		b.SetPageID(pid)

		for i := uint16(0); i < n.Count(); i++ {
			k, v, ptr, flags, err := n.GetLeafEntryView(i)
			if err != nil {
				return 0, err
			}
			if flags&node.FlagTombstone != 0 {
				continue
			}
			if err := b.AddLeafEntry(k, v, flags, ptr); err != nil {
				return 0, err
			}
		}
		b.Finish()
		metrics.IndexWriteBytes += page.PageSize
		return pid, nil
	}

	rebalanceLeaves := func(left, right node.Node) (leftID uint64, rightID uint64, rightStart []byte, ok bool, err error) {
		var (
			lid uint64
			rid uint64
		)
		if allocMany, ok := z.allocator.(interface {
			AllocMany(count int, hint uint64) ([]uint64, error)
		}); ok {
			ids, err := allocMany.AllocMany(2, left.PageID())
			if err != nil {
				if len(ids) > 0 {
					retired = append(retired, ids...)
				}
				return 0, 0, nil, false, err
			}
			if len(ids) < 2 {
				return 0, 0, nil, false, errors.New("rebalanceLeaves: insufficient pages allocated")
			}
			lid, rid = ids[0], ids[1]
		} else {
			lid, err = z.allocator.Alloc(left.PageID())
			if err != nil {
				return 0, 0, nil, false, err
			}
			rid, err = z.allocator.Alloc(lid)
			if err != nil {
				retired = append(retired, lid)
				return 0, 0, nil, false, err
			}
		}
		ldata, err := z.pager.GetForWrite(lid)
		if err != nil {
			return 0, 0, nil, false, err
		}
		lb := z.newLeafBuilder(ldata)
		lb.SetPageID(lid)

		rdata, err := z.pager.GetForWrite(rid)
		if err != nil {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, err
		}

		// Collect combined entries in-order without copying.
		type ev struct {
			k     []byte
			v     []byte
			ptr   page.ValuePtr
			flags byte
			size  int
		}
		combined := make([]ev, 0, int(left.Count()+right.Count()))
		for _, src := range []node.Node{left, right} {
			for i := uint16(0); i < src.Count(); i++ {
				k, v, ptr, flags, err := src.GetLeafEntryView(i)
				if err != nil {
					retired = append(retired, lid, rid)
					return 0, 0, nil, false, err
				}
				if flags&node.FlagTombstone != 0 {
					continue
				}
				if z.leafPrefixCompression {
					k = append([]byte(nil), k...)
				}
				combined = append(combined, ev{k: k, v: v, ptr: ptr, flags: flags, size: leafEntryBytes(k, v, ptr, flags)})
			}
		}
		if len(combined) < 2 {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, nil
		}

		// Choose a split point by bytes (closest to 50/50) to avoid repeated
		// underfull siblings and to guarantee the rebalance fits.
		prefixBytes := make([]int, len(combined)+1)
		for i, e := range combined {
			prefixBytes[i+1] = prefixBytes[i] + e.size
		}
		totalBytes := prefixBytes[len(combined)]

		// Enforce the configured soft-full reserve (if any) by ensuring the
		// constructed pages leave at least leafReserveBytes free space.
		cap := pageCap
		if z.leafReserveBytes > 0 && z.leafReserveBytes < pageCap {
			cap = pageCap - z.leafReserveBytes
		}

		bestSplitAt := -1
		bestDelta := int(^uint(0) >> 1) // MaxInt
		for splitAt := 1; splitAt < len(combined); splitAt++ {
			leftBytes := prefixBytes[splitAt]
			rightBytes := totalBytes - leftBytes
			if leftBytes > cap || rightBytes > cap {
				continue
			}
			delta := leftBytes - rightBytes
			if delta < 0 {
				delta = -delta
			}
			if delta < bestDelta {
				bestDelta = delta
				bestSplitAt = splitAt
			}
		}
		if bestSplitAt <= 0 || bestSplitAt >= len(combined) {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, nil
		}

		rb := z.newLeafBuilder(rdata)
		rb.SetPageID(rid)

		for i := 0; i < bestSplitAt; i++ {
			if err := lb.AddLeafEntry(combined[i].k, combined[i].v, combined[i].flags, combined[i].ptr); err != nil {
				retired = append(retired, lid, rid)
				return 0, 0, nil, false, err
			}
		}
		rightStart = append([]byte(nil), combined[bestSplitAt].k...)
		for i := bestSplitAt; i < len(combined); i++ {
			if err := rb.AddLeafEntry(combined[i].k, combined[i].v, combined[i].flags, combined[i].ptr); err != nil {
				retired = append(retired, lid, rid)
				return 0, 0, nil, false, err
			}
		}

		ln := lb.Finish()
		rn := rb.Finish()
		metrics.IndexWriteBytes += 2 * page.PageSize
		metrics.LeafFill += float64(page.PageSize-ln.FreeSpace()) / float64(page.PageSize)
		metrics.LeafFill += float64(page.PageSize-rn.FreeSpace()) / float64(page.PageSize)
		return lid, rid, rightStart, true, nil
	}

	// Second pass: attempt sibling merge/rebalance for underfull adjacent leaves.
	i := 0
	for i < len(entries)-1 {
		leftID := entries[i].child
		rightID := entries[i+1].child

		left, okL, err := loadLeaf(leftID)
		if err != nil {
			return nil, nil, err
		}
		right, okR, err := loadLeaf(rightID)
		if err != nil {
			return nil, nil, err
		}
		if !okL || !okR {
			i++
			continue
		}

		if left.Count() == 0 {
			// If this is a non-first child it would have been pruned already.
			i++
			continue
		}

		leftFill := fillPPM(left)
		rightFill := fillPPM(right)
		if leftFill >= underfullPPM && rightFill >= underfullPPM {
			// If not merging/rebalancing, check piggyback
			if z.piggybackCompaction {
				const distanceThreshold = 2500 // ~10MB
				dist := int64(leftID) - int64(rightID)
				if dist < 0 {
					dist = -dist
				}

				if dist > distanceThreshold {
					// Move the "older" one (lower ID) towards the newer one.
					if leftID < rightID {
						newID, err := copyLeaf(leftID, rightID)
						if err == nil {
							retired = append(retired, leftID)
							entries[i].child = newID
						}
					} else {
						newID, err := copyLeaf(rightID, leftID)
						if err == nil {
							retired = append(retired, rightID)
							entries[i+1].child = newID
						}
					}
				}
			}
			i++
			continue
		}

		leftBytes, err := leafRequiredBytes(left)
		if err != nil {
			return nil, nil, err
		}
		rightBytes, err := leafRequiredBytes(right)
		if err != nil {
			return nil, nil, err
		}

		mergeCap := pageCap
		if z.leafReserveBytes > 0 && z.leafReserveBytes < pageCap {
			mergeCap = pageCap - z.leafReserveBytes
		}
		if leftBytes+rightBytes <= mergeCap {
			mergedID, ok, err := buildMergedLeaf(left, right)
			if err != nil {
				return nil, nil, err
			}
			if ok {
				retired = append(retired, leftID, rightID)
				entries[i].child = mergedID
				copy(entries[i+1:], entries[i+2:])
				entries = entries[:len(entries)-1]
				if i > 0 {
					i--
				}
				continue
			}
		}

		// If merge isn't possible, attempt a bounded rebalance.
		leftNewID, rightNewID, rightStart, ok, err := rebalanceLeaves(left, right)
		if err != nil {
			return nil, nil, err
		}
		if ok && len(rightStart) > 0 {
			retired = append(retired, leftID, rightID)
			entries[i].child = leftNewID
			entries[i+1].child = rightNewID
			entries[i+1].key = rightStart
		}
		i++
	}

	return entries, retired, nil
}

func (z *Zipper) coalesceInternalChildren(entries []internalEntry, metrics *adaptive.Metrics) ([]internalEntry, []uint64, error) {
	if len(entries) < 2 {
		return entries, nil, nil
	}

	var retired []uint64

	loadInternal := func(id uint64) (node.Node, bool, error) {
		data, err := z.pager.Get(id)
		if err != nil {
			return node.Node{}, false, err
		}
		n := node.NewNodeView(data)
		if n.Type() != page.PageTypeInternal {
			return node.Node{}, false, nil
		}
		return n, true, nil
	}

	fillPPM := func(n node.Node) uint32 {
		used := page.PageSize - n.FreeSpace()
		return uint32((used * 1_000_000) / page.PageSize)
	}

	pageCap := page.PageSize - node.NodeHeaderSize
	internalEntryBytes := func(key []byte) int {
		if key == nil {
			key = []byte{}
		}
		// Internal entry: keylen(uint16) + child(uint64) + key bytes + directory entry.
		return (2 + 8 + len(key)) + node.DirectoryEntrySize
	}
	internalRequiredBytes := func(n node.Node) (int, error) {
		sum := 0
		for i := uint16(0); i < n.Count(); i++ {
			k, _, err := n.GetInternalEntryView(i)
			if err != nil {
				return 0, err
			}
			sum += internalEntryBytes(k)
			if sum > pageCap {
				return sum, nil
			}
		}
		return sum, nil
	}

	buildMergedInternal := func(left, right node.Node) (uint64, bool, error) {
		pid, err := z.allocator.Alloc(left.PageID())
		if err != nil {
			return 0, false, err
		}
		data, err := z.pager.GetForWrite(pid)
		if err != nil {
			return 0, false, err
		}
		b := node.NewBuilder(data, page.PageTypeInternal)
		b.SetPageID(pid)

		addAll := func(n node.Node) error {
			for i := uint16(0); i < n.Count(); i++ {
				k, child, err := n.GetInternalEntryView(i)
				if err != nil {
					return err
				}
				if k == nil {
					k = []byte{}
				}
				entrySize := 2 + 8 + len(k)
				if z.internalSoftFull(b, entrySize) {
					return node.ErrNodeFull
				}
				if err := b.AddInternalChild(k, child); err != nil {
					return err
				}
			}
			return nil
		}

		if err := addAll(left); err != nil {
			retired = append(retired, pid)
			if err == node.ErrNodeFull {
				return 0, false, nil
			}
			return 0, false, err
		}
		if err := addAll(right); err != nil {
			retired = append(retired, pid)
			if err == node.ErrNodeFull {
				return 0, false, nil
			}
			return 0, false, err
		}

		_ = b.Finish()
		metrics.IndexWriteBytes += page.PageSize
		return pid, true, nil
	}

	rebalanceInternals := func(left, right node.Node) (leftID uint64, rightID uint64, rightStart []byte, ok bool, err error) {
		var (
			lid uint64
			rid uint64
		)
		if allocMany, ok := z.allocator.(interface {
			AllocMany(count int, hint uint64) ([]uint64, error)
		}); ok {
			ids, err := allocMany.AllocMany(2, left.PageID())
			if err != nil {
				if len(ids) > 0 {
					retired = append(retired, ids...)
				}
				return 0, 0, nil, false, err
			}
			if len(ids) < 2 {
				return 0, 0, nil, false, errors.New("rebalanceInternals: insufficient pages allocated")
			}
			lid, rid = ids[0], ids[1]
		} else {
			lid, err = z.allocator.Alloc(left.PageID())
			if err != nil {
				return 0, 0, nil, false, err
			}
			rid, err = z.allocator.Alloc(lid)
			if err != nil {
				retired = append(retired, lid)
				return 0, 0, nil, false, err
			}
		}
		ldata, err := z.pager.GetForWrite(lid)
		if err != nil {
			return 0, 0, nil, false, err
		}
		rdata, err := z.pager.GetForWrite(rid)
		if err != nil {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, err
		}

		combined := make([]internalEntry, 0, int(left.Count()+right.Count()))
		for _, src := range []node.Node{left, right} {
			for i := uint16(0); i < src.Count(); i++ {
				k, child, err := src.GetInternalEntryView(i)
				if err != nil {
					retired = append(retired, lid, rid)
					return 0, 0, nil, false, err
				}
				combined = append(combined, internalEntry{key: k, child: child})
			}
		}
		if len(combined) < 2 {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, nil
		}

		splitAt := len(combined) / 2
		if splitAt < 1 {
			splitAt = 1
		}
		if splitAt >= len(combined) {
			splitAt = len(combined) - 1
		}

		build := func(b *node.Builder, list []internalEntry) error {
			for i, e := range list {
				k := e.key
				if k == nil {
					k = []byte{}
				}
				entrySize := 2 + 8 + len(k)
				if i > 0 && z.internalSoftFull(b, entrySize) {
					return node.ErrNodeFull
				}
				if err := b.AddInternalChild(k, e.child); err != nil {
					return err
				}
			}
			return nil
		}

		try := func(splitAt int) ([]byte, bool, error) {
			lb2 := node.NewBuilder(ldata, page.PageTypeInternal)
			lb2.SetPageID(lid)
			rb2 := node.NewBuilder(rdata, page.PageTypeInternal)
			rb2.SetPageID(rid)

			if err := build(lb2, combined[:splitAt]); err != nil {
				if err == node.ErrNodeFull {
					return nil, false, nil
				}
				return nil, false, err
			}
			if err := build(rb2, combined[splitAt:]); err != nil {
				if err == node.ErrNodeFull {
					return nil, false, nil
				}
				return nil, false, err
			}
			lb2.Finish()
			rb2.Finish()
			rs := combined[splitAt].key
			if rs == nil {
				rs = []byte{}
			}
			return append([]byte(nil), rs...), true, nil
		}

		// Adjust split point until both sides fit.
		rightStart, ok, err = try(splitAt)
		if err != nil {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, err
		}
		if !ok {
			for d := 1; d < len(combined)-1; d++ {
				if splitAt-d >= 1 {
					if rs, ok2, err2 := try(splitAt - d); err2 != nil {
						retired = append(retired, lid, rid)
						return 0, 0, nil, false, err2
					} else if ok2 {
						rightStart = rs
						ok = true
						break
					}
				}
				if splitAt+d < len(combined) {
					if rs, ok2, err2 := try(splitAt + d); err2 != nil {
						retired = append(retired, lid, rid)
						return 0, 0, nil, false, err2
					} else if ok2 {
						rightStart = rs
						ok = true
						break
					}
				}
			}
			if !ok {
				retired = append(retired, lid, rid)
				return 0, 0, nil, false, nil
			}
		}

		metrics.IndexWriteBytes += 2 * page.PageSize
		return lid, rid, rightStart, true, nil
	}

	const underfullPPM = 350_000

	i := 0
	for i < len(entries)-1 {
		leftID := entries[i].child
		rightID := entries[i+1].child

		left, okL, err := loadInternal(leftID)
		if err != nil {
			return nil, nil, err
		}
		right, okR, err := loadInternal(rightID)
		if err != nil {
			return nil, nil, err
		}
		if !okL || !okR {
			i++
			continue
		}

		leftFill := fillPPM(left)
		rightFill := fillPPM(right)
		if leftFill >= underfullPPM && rightFill >= underfullPPM {
			i++
			continue
		}

		leftBytes, err := internalRequiredBytes(left)
		if err != nil {
			return nil, nil, err
		}
		rightBytes, err := internalRequiredBytes(right)
		if err != nil {
			return nil, nil, err
		}

		// Attempt a full sibling merge when the combined entries should fit in one page
		// while still respecting the configured soft-full reserve (if any).
		if leftBytes+rightBytes <= pageCap-z.internalReserveBytes {
			mergedID, ok, err := buildMergedInternal(left, right)
			if err != nil {
				return nil, nil, err
			}
			if ok {
				retired = append(retired, leftID, rightID)
				entries[i].child = mergedID
				copy(entries[i+1:], entries[i+2:])
				entries = entries[:len(entries)-1]
				if i > 0 {
					i--
				}
				continue
			}
		}

		leftNewID, rightNewID, rightStart, ok, err := rebalanceInternals(left, right)
		if err != nil {
			return nil, nil, err
		}
		if ok && len(rightStart) > 0 {
			retired = append(retired, leftID, rightID)
			entries[i].child = leftNewID
			entries[i+1].child = rightNewID
			entries[i+1].key = rightStart
		}
		i++
	}

	return entries, retired, nil
}

func (z *Zipper) createNewSplitInternal(currentTarget, rootBuilder *node.Builder, splits *[]Split, key []byte, val uint64, metrics *adaptive.Metrics) (*node.Builder, error) {
	// 1. Finish current (if not rootBuilder)
	if currentTarget != rootBuilder {
		_ = currentTarget.Finish()
		metrics.IndexWriteBytes += page.PageSize
	}

	// 2. Alloc new
	sid, err := z.allocator.Alloc(currentTarget.PageID())
	if err != nil {
		return nil, err
	}

	sdata, err := z.pager.GetForWrite(sid)
	if err != nil {
		return nil, err
	}

	sb := node.NewBuilder(sdata, page.PageTypeInternal)
	sb.SetPageID(sid)

	*splits = append(*splits, Split{Key: append([]byte(nil), key...), NodeID: sid})

	// Retry insert
	if err := sb.AddInternalChild(key, val); err != nil {
		return nil, err
	}

	return sb, nil
}
