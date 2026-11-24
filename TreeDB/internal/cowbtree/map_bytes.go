package cowbtree

// SetBytes sets or replaces value for a byte-slice key in a string-keyed Map
// in a single B-tree traversal. On replacement, the existing owned string key
// in the tree is preserved with zero key allocation (including under
// -tags treedb_safe). On insertion, string(key) is allocated only at the final
// leaf write, never before split retries. Node isolation and AllocationCharge
// match Map.Set bit-for-bit.
func SetBytes[V any](tr *Map[string, V], key []byte, value V) (previous V, replaced bool) {
	if tr.sealed {
		panic("cowbtree: mutation of sealed immutable map")
	}
	if tr.root == nil {
		tr.init(0)
		items, _ := tr.growItems(nil, 1, nil)
		root := tr.newNode(true)
		root.items = append(items, mapPair[string, V]{key: string(key), value: value})
		root.count = 1
		tr.root = root
		tr.count = 1
		return tr.empty.value, false
	}
	prev, replaced, split := nodeSetBytes(tr, &tr.root, key, value)
	if split {
		left := tr.root
		items, _ := tr.growItems(nil, 1, nil)
		children, _ := tr.growChildren(nil, 2, nil)
		right, median := tr.nodeSplit(left)
		root := tr.newNode(false)
		*root.children = append(children, left, right)
		root.items = append(items, median)
		root.updateCount()
		tr.root = root
		return SetBytes(tr, key, value)
	}
	if replaced {
		return prev, true
	}
	tr.count++
	return tr.empty.value, false
}

func searchStringBytes[V any](n *mapNode[string, V], key []byte) (int, bool) {
	items := n.items
	lo, hi := 0, len(items)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		c := compareStringBytes(items[mid].key, key)
		if c < 0 {
			lo = mid + 1
		} else if c > 0 {
			hi = mid
		} else {
			return mid, true
		}
	}
	return lo, false
}

// GetEntryBytes looks up a byte-slice key in a string-keyed Map without
// mutating the tree, allocating a string key (including under -tags treedb_safe),
// or invoking isolation/work-accounting machinery. On a hit it returns the
// tree's existing owned immutable string key alongside its value.
func GetEntryBytes[V any](tr *Map[string, V], key []byte) (string, V, bool) {
	if tr == nil {
		var zero V
		return "", zero, false
	}
	for n := tr.root; n != nil; {
		i, found := searchStringBytes(n, key)
		if found {
			return n.items[i].key, n.items[i].value, true
		}
		if n.leaf() {
			return "", tr.empty.value, false
		}
		n = (*n.children)[i]
	}
	return "", tr.empty.value, false
}

func nodeSetBytes[V any](tr *Map[string, V], pn **mapNode[string, V], key []byte, value V) (prev V, replaced bool, split bool) {
	n := *pn
	var i int
	var found bool
	if n.isoid != tr.isoid && n.leaf() && len(n.items) < tr.max && !tr.copyValues && !tr.isoCopyValues {
		i, found = searchStringBytes(n, key)
		if !found {
			*pn = tr.copyInsertLeaf(n, i, mapPair[string, V]{key: string(key), value: value})
			return tr.empty.value, false, false
		}
		n = tr.isoLoad(pn, true)
	} else {
		n = tr.isoLoad(pn, true)
		i, found = searchStringBytes(n, key)
	}
	if found {
		prev = n.items[i].value
		n.items[i].value = value
		return prev, true, false
	}
	if n.leaf() {
		if len(n.items) == tr.max {
			return tr.empty.value, false, true
		}
		items, _ := tr.growItems(n.items, 1, nil)
		n.items = append(items, tr.empty)
		copy(n.items[i+1:], n.items[i:])
		n.items[i] = mapPair[string, V]{key: string(key), value: value}
		n.count++
		return tr.empty.value, false, false
	}
	prev, replaced, split = nodeSetBytes(tr, &(*n.children)[i], key, value)
	if split {
		if len(n.items) == tr.max {
			return tr.empty.value, false, true
		}
		items, _ := tr.growItems(n.items, 1, nil)
		children, _ := tr.growChildren(*n.children, 1, nil)
		right, median := tr.nodeSplit((*n.children)[i])
		*n.children = append(children, nil)
		copy((*n.children)[i+1:], (*n.children)[i:])
		(*n.children)[i+1] = right
		n.items = append(items, tr.empty)
		copy(n.items[i+1:], n.items[i:])
		n.items[i] = median
		return nodeSetBytes(tr, &n, key, value)
	}
	if !replaced {
		n.count++
	}
	return prev, replaced, false
}
