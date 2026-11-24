package cowbtree

import "unsafe"

// SetSortedBytes inserts or replaces a strictly ascending sequence of n
// byte-slice keys in a single range-partitioned pass over tr.
//
// Callers must ensure keys are strictly ascending:
//
//	bytes.Compare(keyAt(i-1), keyAt(i)) < 0 for all 0 < i < n.
//
// For each inserted key, ownKey(i) is invoked once to obtain its owned immutable
// string key. For each replaced key, the existing owned string key in the tree
// is preserved and onReplace(i, previousValue) is called if non-nil.
//
// All node and slice allocations flow through the same accounting hooks as
// Map.Set / SetBytes, while eliminating per-key root-to-leaf descents and
// intermediate copy/grow churn on bulk sorted runs.
func SetSortedBytes[V any](
	tr *Map[string, V],
	n int,
	keyAt func(int) []byte,
	valAt func(int) V,
	ownKey func(int) string,
	onReplace func(int, V),
) {
	if tr.sealed {
		panic("cowbtree: mutation of sealed immutable map")
	}
	if n <= 0 {
		return
	}
	if tr.copyValues || tr.isoCopyValues {
		for i := 0; i < n; i++ {
			prev, replaced := SetBytes(tr, keyAt(i), valAt(i))
			if replaced && onReplace != nil {
				onReplace(i, prev)
			}
		}
		return
	}
	if n == 1 {
		prev, replaced := SetBytes(tr, keyAt(0), valAt(0))
		if replaced && onReplace != nil {
			onReplace(0, prev)
		}
		return
	}
	if tr.root == nil {
		tr.init(0)
		nodes, seps := bulkBuildLeavesFromSorted(tr, 0, n, valAt, ownKey)
		tr.root = bulkBuildRootFromLevel(tr, nodes, seps)
		tr.count = n
		return
	}
	single, nodes, seps, added := nodeSetSortedBytes(tr, tr.root, 0, n, keyAt, valAt, ownKey, onReplace)
	if single != nil {
		tr.root = single
	} else {
		tr.root = bulkBuildRootFromLevel(tr, nodes, seps)
	}
	tr.count += added
}

// bulkBuildLeavesFromSorted builds balanced B-tree leaves (each with tr.min..tr.max
// items, or < tr.min when only 1 leaf is needed) and separating items from a
// strictly ascending input range [lo, hi).
func bulkBuildLeavesFromSorted[V any](
	tr *Map[string, V],
	lo, hi int,
	valAt func(int) V,
	ownKey func(int) string,
) ([]*mapNode[string, V], []mapPair[string, V]) {
	total := hi - lo
	if total <= tr.max {
		leaf := tr.newNode(true)
		leaf.items = tr.allocItems(total, total)
		for i := 0; i < total; i++ {
			idx := lo + i
			leaf.items[i] = mapPair[string, V]{key: ownKey(idx), value: valAt(idx)}
		}
		leaf.count = total
		return []*mapNode[string, V]{leaf}, nil
	}
	stride := tr.max + 1
	m := (total + stride) / stride
	remaining := total - (m - 1)
	base := remaining / m
	rem := remaining % m

	nodes := make([]*mapNode[string, V], m)
	seps := make([]mapPair[string, V], m-1)
	pos := lo
	for j := 0; j < m; j++ {
		leafLen := base
		if j < rem {
			leafLen++
		}
		leaf := tr.newNode(true)
		leaf.items = tr.allocItems(leafLen, leafLen)
		for k := 0; k < leafLen; k++ {
			leaf.items[k] = mapPair[string, V]{key: ownKey(pos), value: valAt(pos)}
			pos++
		}
		leaf.count = leafLen
		nodes[j] = leaf
		if j < m-1 {
			seps[j] = mapPair[string, V]{key: ownKey(pos), value: valAt(pos)}
			pos++
		}
	}
	return nodes, seps
}

// bulkSplitPairs splits a flat sorted slice of pairs into balanced leaf nodes
// (each with tr.min..tr.max items) and separating items. Caller ensures
// len(all) > tr.max.
func bulkSplitLeafPairs[V any](
	tr *Map[string, V],
	all []mapPair[string, V],
) ([]*mapNode[string, V], []mapPair[string, V]) {
	total := len(all)
	stride := tr.max + 1
	m := (total + stride) / stride
	remaining := total - (m - 1)
	base := remaining / m
	rem := remaining % m

	nodes := make([]*mapNode[string, V], m)
	seps := make([]mapPair[string, V], m-1)
	pos := 0
	for j := 0; j < m; j++ {
		leafLen := base
		if j < rem {
			leafLen++
		}
		leaf := tr.newNode(true)
		leaf.items = tr.allocItems(leafLen, leafLen)
		copy(leaf.items, all[pos:pos+leafLen])
		leaf.count = leafLen
		nodes[j] = leaf
		pos += leafLen
		if j < m-1 {
			seps[j] = all[pos]
			pos++
		}
	}
	return nodes, seps
}

// bulkSplitInternal splits a flat sequence of items and children (len(children) == len(items)+1)
// into balanced internal nodes and separating items when len(items) > tr.max.
func bulkSplitInternal[V any](
	tr *Map[string, V],
	allItems []mapPair[string, V],
	allChildren []*mapNode[string, V],
) ([]*mapNode[string, V], []mapPair[string, V]) {
	total := len(allItems)
	stride := tr.max + 1
	m := (total + stride) / stride
	remaining := total - (m - 1)
	base := remaining / m
	rem := remaining % m

	nodes := make([]*mapNode[string, V], m)
	seps := make([]mapPair[string, V], m-1)
	itemPos := 0
	childPos := 0
	for j := 0; j < m; j++ {
		cnt := base
		if j < rem {
			cnt++
		}
		node := tr.newNode(false)
		node.items = tr.allocItems(cnt, cnt)
		copy(node.items, allItems[itemPos:itemPos+cnt])
		children := make([]*mapNode[string, V], cnt+1)
		tr.recordAllocation(uint64(cap(children)) * uint64(unsafe.Sizeof((*mapNode[string, V])(nil))))
		copy(children, allChildren[childPos:childPos+cnt+1])
		*node.children = children
		node.updateCount()
		nodes[j] = node
		itemPos += cnt
		childPos += cnt + 1
		if j < m-1 {
			seps[j] = allItems[itemPos]
			itemPos++
		}
	}
	return nodes, seps
}

// bulkBuildRootFromLevel builds parent internal levels bottom-up until a single
// root node remains. len(nodes) must equal len(seps)+1 and len(nodes) >= 1.
func bulkBuildRootFromLevel[V any](
	tr *Map[string, V],
	nodes []*mapNode[string, V],
	seps []mapPair[string, V],
) *mapNode[string, V] {
	for len(nodes) > 1 {
		if len(seps) <= tr.max {
			root := tr.newNode(false)
			root.items = tr.allocItems(len(seps), len(seps))
			copy(root.items, seps)
			children := make([]*mapNode[string, V], len(nodes))
			tr.recordAllocation(uint64(cap(children)) * uint64(unsafe.Sizeof((*mapNode[string, V])(nil))))
			copy(children, nodes)
			*root.children = children
			root.updateCount()
			return root
		}
		nodes, seps = bulkSplitInternal(tr, seps, nodes)
	}
	return nodes[0]
}

// nodeSetSortedBytes applies sorted entries [lo, hi) to subtree n and returns
// either a single replacement node (single != nil, nodes == nil, seps == nil)
// or M split nodes + M-1 separators (single == nil), plus the net number of
// newly inserted keys.
func nodeSetSortedBytes[V any](
	tr *Map[string, V],
	n *mapNode[string, V],
	lo, hi int,
	keyAt func(int) []byte,
	valAt func(int) V,
	ownKey func(int) string,
	onReplace func(int, V),
) (*mapNode[string, V], []*mapNode[string, V], []mapPair[string, V], int) {
	if n.leaf() {
		return leafSetSortedBytes(tr, n, lo, hi, keyAt, valAt, ownKey, onReplace)
	}
	return internalSetSortedBytes(tr, n, lo, hi, keyAt, valAt, ownKey, onReplace)
}

func leafSetSortedBytes[V any](
	tr *Map[string, V],
	n *mapNode[string, V],
	lo, hi int,
	keyAt func(int) []byte,
	valAt func(int) V,
	ownKey func(int) string,
	onReplace func(int, V),
) (*mapNode[string, V], []*mapNode[string, V], []mapPair[string, V], int) {
	oldItems := n.items
	oldLen := len(oldItems)
	inCount := hi - lo

	// Fast path: pure right-edge append (all new keys > last existing key).
	if oldLen > 0 && compareStringBytes(oldItems[oldLen-1].key, keyAt(lo)) < 0 {
		total := oldLen + inCount
		if total <= tr.max {
			var leaf *mapNode[string, V]
			if n.isoid == tr.isoid && cap(oldItems) >= total {
				leaf = n
				leaf.items = oldItems[:total]
			} else if n.isoid == tr.isoid {
				leaf = n
				grown := tr.allocItems(total, total)
				copy(grown, oldItems)
				leaf.items = grown
			} else {
				leaf = tr.newNode(true)
				leaf.items = tr.allocItems(total, total)
				copy(leaf.items, oldItems)
			}
			for i := 0; i < inCount; i++ {
				idx := lo + i
				leaf.items[oldLen+i] = mapPair[string, V]{key: ownKey(idx), value: valAt(idx)}
			}
			leaf.count = total
			return leaf, nil, nil, inCount
		}
		// Right-edge append exceeding tr.max: stream directly into balanced
		// leaves without allocating an intermediate merged slice.
		stride := tr.max + 1
		m := (total + stride) / stride
		remaining := total - (m - 1)
		base := remaining / m
		rem := remaining % m

		nodes := make([]*mapNode[string, V], m)
		seps := make([]mapPair[string, V], m-1)
		oldPos := 0
		inPos := lo
		nextPair := func() mapPair[string, V] {
			if oldPos < oldLen {
				p := oldItems[oldPos]
				oldPos++
				return p
			}
			idx := inPos
			inPos++
			return mapPair[string, V]{key: ownKey(idx), value: valAt(idx)}
		}
		for j := 0; j < m; j++ {
			leafLen := base
			if j < rem {
				leafLen++
			}
			leaf := tr.newNode(true)
			leaf.items = tr.allocItems(leafLen, leafLen)
			k := 0
			if oldPos < oldLen {
				take := min(leafLen, oldLen-oldPos)
				copy(leaf.items[:take], oldItems[oldPos:oldPos+take])
				oldPos += take
				k = take
			}
			for ; k < leafLen; k++ {
				leaf.items[k] = mapPair[string, V]{key: ownKey(inPos), value: valAt(inPos)}
				inPos++
			}
			leaf.count = leafLen
			nodes[j] = leaf
			if j < m-1 {
				seps[j] = nextPair()
			}
		}
		return nil, nodes, seps, inCount
	}

	// General two-pointer merge of oldItems and sorted input [lo, hi).
	// First count replacements so we know exact finalLen before allocating.
	replaces := 0
	oi := 0
	ii := lo
	for oi < oldLen && ii < hi {
		c := compareStringBytes(oldItems[oi].key, keyAt(ii))
		if c < 0 {
			oi++
		} else if c > 0 {
			ii++
		} else {
			replaces++
			oi++
			ii++
		}
	}
	added := inCount - replaces
	finalLen := oldLen + added

	if finalLen <= tr.max {
		// Common case: single leaf holds all merged items.
		var leaf *mapNode[string, V]
		var dst []mapPair[string, V]
		inPlace := n.isoid == tr.isoid && replaces == inCount
		if inPlace {
			// Pure replacement on an already private leaf: zero allocation.
			leaf = n
			oi, ii = 0, lo
			for oi < oldLen && ii < hi {
				c := compareStringBytes(oldItems[oi].key, keyAt(ii))
				if c < 0 {
					oi++
				} else if c == 0 {
					if onReplace != nil {
						onReplace(ii, oldItems[oi].value)
					}
					oldItems[oi].value = valAt(ii)
					oi++
					ii++
				} else {
					ii++
				}
			}
			return leaf, nil, nil, 0
		}
		if n.isoid == tr.isoid {
			leaf = n
			dst = tr.allocItems(finalLen, finalLen)
		} else {
			leaf = tr.newNode(true)
			dst = tr.allocItems(finalLen, finalLen)
		}
		mergeLeafPairsInto(dst, oldItems, lo, hi, keyAt, valAt, ownKey, onReplace)
		leaf.items = dst
		leaf.count = finalLen
		return leaf, nil, nil, added
	}

	// Overflow: merge into a stack buffer when small (avoiding heap alloc) and
	// split into balanced leaves.
	var mergedStack [256]mapPair[string, V]
	var merged []mapPair[string, V]
	if finalLen <= len(mergedStack) {
		merged = mergedStack[:finalLen]
	} else {
		merged = make([]mapPair[string, V], finalLen)
	}
	mergeLeafPairsInto(merged, oldItems, lo, hi, keyAt, valAt, ownKey, onReplace)
	nodes, seps := bulkSplitLeafPairs(tr, merged)
	return nil, nodes, seps, added
}

func mergeLeafPairsInto[V any](
	dst []mapPair[string, V],
	oldItems []mapPair[string, V],
	lo, hi int,
	keyAt func(int) []byte,
	valAt func(int) V,
	ownKey func(int) string,
	onReplace func(int, V),
) {
	oi := 0
	ii := lo
	di := 0
	for oi < len(oldItems) && ii < hi {
		c := compareStringBytes(oldItems[oi].key, keyAt(ii))
		if c < 0 {
			dst[di] = oldItems[oi]
			oi++
		} else if c > 0 {
			dst[di] = mapPair[string, V]{key: ownKey(ii), value: valAt(ii)}
			ii++
		} else {
			if onReplace != nil {
				onReplace(ii, oldItems[oi].value)
			}
			dst[di] = mapPair[string, V]{key: oldItems[oi].key, value: valAt(ii)}
			oi++
			ii++
		}
		di++
	}
	if oi < len(oldItems) {
		copy(dst[di:], oldItems[oi:])
	} else {
		for ; ii < hi; ii++ {
			dst[di] = mapPair[string, V]{key: ownKey(ii), value: valAt(ii)}
			di++
		}
	}
}

// lowerBoundInput finds the smallest index in [lo, hi) with keyAt(idx) >= target.
func lowerBoundInput(lo, hi int, keyAt func(int) []byte, target string) int {
	l, r := lo, hi
	for l < r {
		mid := int(uint(l+r) >> 1)
		if compareStringBytes(target, keyAt(mid)) > 0 {
			l = mid + 1
		} else {
			r = mid
		}
	}
	return l
}

func internalSetSortedBytes[V any](
	tr *Map[string, V],
	n *mapNode[string, V],
	lo, hi int,
	keyAt func(int) []byte,
	valAt func(int) V,
	ownKey func(int) string,
	onReplace func(int, V),
) (*mapNode[string, V], []*mapNode[string, V], []mapPair[string, V], int) {
	oldItems := n.items
	oldChildren := *n.children
	k := len(oldItems)

	// Fast path: all entries fall into the rightmost child (key > last separator).
	// Very common for sequential / append-only inserts.
	if compareStringBytes(oldItems[k-1].key, keyAt(lo)) < 0 {
		childSingle, childNodes, childSeps, added := nodeSetSortedBytes(tr, oldChildren[k], lo, hi, keyAt, valAt, ownKey, onReplace)
		if childSingle != nil {
			node := n
			if node.isoid != tr.isoid {
				node = tr.copy(n)
			}
			(*node.children)[k] = childSingle
			node.count += added
			return node, nil, nil, added
		}
		newItemCount := k + len(childSeps)
		if newItemCount <= tr.max {
			var node *mapNode[string, V]
			if n.isoid == tr.isoid {
				node = n
				items := tr.allocItems(newItemCount, newItemCount)
				copy(items, oldItems)
				copy(items[k:], childSeps)
				node.items = items
				children := make([]*mapNode[string, V], newItemCount+1)
				tr.recordAllocation(uint64(cap(children)) * uint64(unsafe.Sizeof((*mapNode[string, V])(nil))))
				copy(children, oldChildren[:k])
				copy(children[k:], childNodes)
				*node.children = children
			} else {
				node = tr.newNode(false)
				node.items = tr.allocItems(newItemCount, newItemCount)
				copy(node.items, oldItems)
				copy(node.items[k:], childSeps)
				children := make([]*mapNode[string, V], newItemCount+1)
				tr.recordAllocation(uint64(cap(children)) * uint64(unsafe.Sizeof((*mapNode[string, V])(nil))))
				copy(children, oldChildren[:k])
				copy(children[k:], childNodes)
				*node.children = children
			}
			node.count = n.count + added
			return node, nil, nil, added
		}
		allItems := make([]mapPair[string, V], newItemCount)
		copy(allItems, oldItems)
		copy(allItems[k:], childSeps)
		allChildren := make([]*mapNode[string, V], newItemCount+1)
		copy(allChildren, oldChildren[:k])
		copy(allChildren[k:], childNodes)
		nodes, seps := bulkSplitInternal(tr, allItems, allChildren)
		return nil, nodes, seps, added
	}

	// General internal node range partition across children and separators.
	// Find the first separator >= keyAt(lo) so untouched leading children are skipped in O(log k).
	firstIdx, _ := searchStringBytes(n, keyAt(lo))
	cur := lo
	totalAdded := 0

	// Track whether any child split (changing item count).
	type childResult struct {
		childIdx int
		single   *mapNode[string, V]
		nodes    []*mapNode[string, V]
		seps     []mapPair[string, V]
	}
	var childResultsInline [8]childResult
	childResults := childResultsInline[:0]
	var itemReplacements []struct {
		itemIdx int
		val     V
	}
	extraSeps := 0

	for j := firstIdx; j <= k && cur < hi; j++ {
		var childEnd int
		var sepMatch bool
		var sepInputIdx int
		if j < k {
			sepKey := oldItems[j].key
			if compareStringBytes(sepKey, keyAt(cur)) == 0 {
				childEnd = cur
				sepMatch = true
				sepInputIdx = cur
			} else if compareStringBytes(sepKey, keyAt(hi-1)) < 0 {
				pos := lowerBoundInput(cur, hi, keyAt, sepKey)
				childEnd = pos
				if pos < hi && compareStringBytes(sepKey, keyAt(pos)) == 0 {
					sepMatch = true
					sepInputIdx = pos
				}
			} else if compareStringBytes(sepKey, keyAt(hi-1)) == 0 {
				childEnd = hi - 1
				sepMatch = true
				sepInputIdx = hi - 1
			} else {
				childEnd = hi
			}
		} else {
			childEnd = hi
		}

		if cur < childEnd {
			cSingle, cNodes, cSeps, added := nodeSetSortedBytes(tr, oldChildren[j], cur, childEnd, keyAt, valAt, ownKey, onReplace)
			totalAdded += added
			extraSeps += len(cSeps)
			childResults = append(childResults, childResult{childIdx: j, single: cSingle, nodes: cNodes, seps: cSeps})
		}

		if sepMatch {
			if onReplace != nil {
				onReplace(sepInputIdx, oldItems[j].value)
			}
			itemReplacements = append(itemReplacements, struct {
				itemIdx int
				val     V
			}{itemIdx: j, val: valAt(sepInputIdx)})
			cur = sepInputIdx + 1
		} else {
			cur = childEnd
		}
	}

	if extraSeps == 0 {
		// No child split: isolate n once and update modified child pointers & replaced items.
		node := n
		if node.isoid != tr.isoid {
			node = tr.copy(n)
		}
		for _, rep := range itemReplacements {
			node.items[rep.itemIdx].value = rep.val
		}
		for _, cr := range childResults {
			(*node.children)[cr.childIdx] = cr.single
		}
		node.count += totalAdded
		return node, nil, nil, totalAdded
	}

	// One or more children split: build the merged (items, children) sequence.
	newItemCount := k + extraSeps
	buildMerged := func(dstItems []mapPair[string, V], dstChildren []*mapNode[string, V]) {
		repIdx := 0
		crIdx := 0
		di := 0
		dc := 0
		for j := 0; j <= k; j++ {
			if crIdx < len(childResults) && childResults[crIdx].childIdx == j {
				cr := childResults[crIdx]
				crIdx++
				if cr.single != nil {
					dstChildren[dc] = cr.single
					dc++
				} else {
					copy(dstChildren[dc:], cr.nodes)
					dc += len(cr.nodes)
					copy(dstItems[di:], cr.seps)
					di += len(cr.seps)
				}
			} else {
				dstChildren[dc] = oldChildren[j]
				dc++
			}
			if j < k {
				item := oldItems[j]
				if repIdx < len(itemReplacements) && itemReplacements[repIdx].itemIdx == j {
					item.value = itemReplacements[repIdx].val
					repIdx++
				}
				dstItems[di] = item
				di++
			}
		}
	}

	if newItemCount <= tr.max {
		var node *mapNode[string, V]
		if n.isoid == tr.isoid {
			node = n
		} else {
			node = tr.newNode(false)
		}
		node.items = tr.allocItems(newItemCount, newItemCount)
		children := make([]*mapNode[string, V], newItemCount+1)
		tr.recordAllocation(uint64(cap(children)) * uint64(unsafe.Sizeof((*mapNode[string, V])(nil))))
		buildMerged(node.items, children)
		*node.children = children
		node.count = n.count + totalAdded
		return node, nil, nil, totalAdded
	}

	allItems := make([]mapPair[string, V], newItemCount)
	allChildren := make([]*mapNode[string, V], newItemCount+1)
	buildMerged(allItems, allChildren)
	nodes, seps := bulkSplitInternal(tr, allItems, allChildren)
	return nil, nodes, seps, totalAdded
}
