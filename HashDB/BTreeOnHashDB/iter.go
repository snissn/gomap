package btreeonhashdb

import (
	"bytes"
	"fmt"
)

// Iter iterates over the B+Tree in ascending key order.
// It holds a read lock on the tree for its lifetime; callers must Close().
type Iter struct {
	t         *Tree
	currNode  *Node
	keys      [][]byte // Direct cache of currNode.Keys to avoid double indirection
	currIdx   int
	end       []byte // exclusive; nil for unbounded
	leafLimit int    // index one past the last valid key in the current leaf
	err       error
	valid     bool
	closed    bool
}

// RevIter iterates over the B+Tree in descending key order.
// It holds a read lock on the tree for its lifetime; callers must Close().
type RevIter struct {
	t             *Tree
	currNode      *Node
	keys          [][]byte // Direct cache of currNode.Keys
	currIdx       int
	start         []byte // inclusive lower bound
	end           []byte // exclusive upper bound; nil for unbounded
	lowerBoundIdx int    // stop when currIdx < lowerBoundIdx
	err           error
	valid         bool
	closed        bool
}

// Range returns an iterator over [start, end).
// start == nil begins from the smallest key; end == nil iterates through the largest key.
func (t *Tree) Range(start, end []byte) (*Iter, error) {
	t.mu.RLock()

	it := &Iter{
		t:   t,
		end: end,
	}

	if err := it.seek(start); err != nil {
		t.mu.RUnlock()
		return nil, err
	}

	return it, nil
}

// ScanAll is a helper for iterating the entire keyspace.
func (t *Tree) ScanAll() (*Iter, error) {
	return t.Range(nil, nil)
}

// ReverseRange returns a descending iterator over [start, end).
// start == nil iterates down to the smallest key; end == nil starts from the largest key.
func (t *Tree) ReverseRange(start, end []byte) (*RevIter, error) {
	t.mu.RLock()

	it := &RevIter{
		t:     t,
		start: start,
		end:   end,
	}

	if err := it.seek(); err != nil {
		t.mu.RUnlock()
		return nil, err
	}

	return it, nil
}

func (it *Iter) seek(start []byte) error {
	currID := it.t.meta.RootNodeID

	for {
		node, err := it.t.loadNode(currID)
		if err != nil {
			return err
		}

		if node.Type == NodeLeaf {
			it.currNode = node
			it.keys = node.Keys

			// Determine starting index
			if start == nil {
				it.currIdx = 0
			} else {
				it.currIdx = node.search(start)
			}

			it.initLeafBounds()

			// If we landed off the end of this leaf, we must advance to the next leaf.
			// This handles the edge case where 'start' > all keys in this leaf.
			if it.currIdx >= len(it.keys) {
				it.valid = true // Temporarily valid so nextLeaf can operate
				it.nextLeaf()
			} else if it.currIdx >= it.leafLimit {
				// We landed on a key, but it is >= end. The range is empty.
				it.valid = false
			} else {
				it.valid = true
			}
			return nil
		}

		// Internal node traversal
		idx := 0
		if start != nil {
			idx = node.search(start)
			if idx < len(node.Keys) && bytes.Equal(node.Keys[idx], start) {
				idx++
			}
		}

		if idx >= len(node.Children) {
			return fmt.Errorf("node %d child index %d out of range", node.ID, idx)
		}
		currID = node.Children[idx]
	}
}

func (it *RevIter) seek() error {
	// 1. Find the starting leaf (Rightmost if end==nil, else search for end)
	currID := it.t.meta.RootNodeID

	// If end is nil, we want the rightmost leaf.
	if it.end == nil {
		for {
			node, err := it.t.loadNode(currID)
			if err != nil {
				return err
			}
			if node.Type == NodeLeaf {
				it.currNode = node
				it.keys = node.Keys
				it.currIdx = len(node.Keys) - 1
				break
			}
			currID = node.Children[len(node.Children)-1]
		}
	} else {
		// Seek to the leaf containing 'end'
		for {
			node, err := it.t.loadNode(currID)
			if err != nil {
				return err
			}

			if node.Type == NodeLeaf {
				it.currNode = node
				it.keys = node.Keys
				// We want the largest key < end.
				// search(end) returns first key >= end. Subtract 1 to get < end.
				it.currIdx = node.search(it.end) - 1
				break
			}

			childIdx := node.search(it.end)
			if childIdx >= len(node.Children) {
				childIdx = len(node.Children) - 1
			}
			currID = node.Children[childIdx]
		}
	}

	// 2. Setup bounds and validate position
	it.initLowerBound()

	// Handle edge cases where seek landed us out of bounds
	if it.currIdx < 0 {
		// Valid indicates we can try to move to previous leaf
		it.valid = true
		it.prevLeaf()
	} else if it.currIdx < it.lowerBoundIdx {
		// We are within the leaf, but below the start bound. Range is empty.
		it.valid = false
	} else {
		it.valid = true
	}

	return nil
}

// Valid reports whether the iterator points to a valid key/value pair.
func (it *Iter) Valid() bool {
	return it.valid && it.err == nil
}

// Valid reports whether the iterator points to a valid key/value pair.
func (it *RevIter) Valid() bool {
	return it.valid && it.err == nil
}

// Key returns the current key or nil if invalid.
func (it *Iter) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.keys[it.currIdx]
}

// Key returns the current key or nil if invalid.
func (it *RevIter) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.keys[it.currIdx]
}

// Value returns the current value or nil if invalid.
func (it *Iter) Value() []byte {
	if !it.valid {
		return nil
	}
	key := it.keys[it.currIdx]
	val, err := it.t.kv.Get(key)
	if err != nil {
		it.err = err
		it.valid = false
		return nil
	}
	return val
}

// Value returns the current value or nil if invalid.
func (it *RevIter) Value() []byte {
	if !it.valid {
		return nil
	}
	key := it.keys[it.currIdx]
	val, err := it.t.kv.Get(key)
	if err != nil {
		it.err = err
		it.valid = false
		return nil
	}
	return val
}

// Next advances the iterator.
// Optimized for inlining: Keep this function small and simple.
func (it *Iter) Next() {
	if !it.valid {
		return
	}

	it.currIdx++

	// Hot Path: Compare directly against cached limit.
	// This avoids function calls and complex checks 99% of the time.
	if it.currIdx < it.leafLimit {
		return
	}

	// Cold Path: Switch leaves.
	it.nextLeaf()
}

// nextLeaf handles the complex logic of moving to the next node.
// It is explicitly separated to allow Next() to be inlined.
func (it *Iter) nextLeaf() {
	for {
		nextID := it.currNode.NextLeaf
		if nextID == 0 {
			it.valid = false
			return
		}

		nextNode, err := it.t.loadNode(nextID)
		if err != nil {
			it.err = err
			it.valid = false
			return
		}

		// Update cache
		it.currNode = nextNode
		it.keys = nextNode.Keys

		if len(it.keys) == 0 {
			// Skip empty leaves
			continue
		}

		it.currIdx = 0
		it.initLeafBounds()

		// If the very first key in this new node is already past our end bound, stop.
		// (initLeafBounds sets leafLimit=0 if keys[0] >= end)
		if it.leafLimit == 0 && len(it.keys) > 0 {
			// Double check: if leafLimit is 0 it usually means keys[0] >= end.
			// But if end==nil, leafLimit is len(keys).
			// We rely on initLeafBounds logic here.
			it.valid = false
			return
		}

		// If initLeafBounds found valid keys (leafLimit > 0), we are good.
		it.valid = true
		return
	}
}

// initLeafBounds computes the per-leaf limit for the current leaf.
// This turns the bound check in Next() into a simple integer comparison (O(1)).
func (it *Iter) initLeafBounds() {
	if it.end == nil {
		it.leafLimit = len(it.keys)
		return
	}

	// Optimization: If the last key in this node is strictly smaller than 'end',
	// we can iterate the entire node without per-key checks.
	// This avoids the binary search overhead on the vast majority of leaves.
	if len(it.keys) > 0 && bytes.Compare(it.keys[len(it.keys)-1], it.end) < 0 {
		it.leafLimit = len(it.keys)
		return
	}

	// 'end' falls within this leaf; find the exact cutoff.
	it.leafLimit = it.currNode.search(it.end)
}

// Next advances the iterator backwards.
// Optimized for inlining.
func (it *RevIter) Next() {
	if !it.valid {
		return
	}

	it.currIdx--

	// Hot Path: Compare against cached lower bound.
	if it.currIdx >= it.lowerBoundIdx {
		return
	}

	// Cold Path: Switch leaves
	it.prevLeaf()
}

// prevLeaf handles moving to the previous node.
func (it *RevIter) prevLeaf() {
	for {
		prevID := it.currNode.PrevLeaf
		if prevID == 0 {
			it.valid = false
			return
		}

		prevNode, err := it.t.loadNode(prevID)
		if err != nil {
			it.err = err
			it.valid = false
			return
		}

		// Update cache
		it.currNode = prevNode
		it.keys = prevNode.Keys

		if len(it.keys) == 0 {
			continue
		}

		it.currIdx = len(it.keys) - 1
		it.initLowerBound()

		// If the largest key in this new node is smaller than 'start',
		// then the entire node is out of bounds. We are done.
		if it.currIdx < it.lowerBoundIdx {
			it.valid = false
			return
		}

		it.valid = true
		return
	}
}

// initLowerBound computes the index below which we should stop iterating.
func (it *RevIter) initLowerBound() {
	if it.start == nil {
		it.lowerBoundIdx = 0
		return
	}

	// Optimization: If the first (smallest) key is >= start,
	// we can iterate all the way to 0. Avoids binary search.
	if len(it.keys) > 0 && bytes.Compare(it.keys[0], it.start) >= 0 {
		it.lowerBoundIdx = 0
		return
	}

	// 'start' falls within this leaf. Find the first key >= start.
	it.lowerBoundIdx = it.currNode.search(it.start)
}

// Close releases the tree read lock.
func (it *Iter) Close() {
	if it.closed {
		return
	}
	it.t.mu.RUnlock()
	it.closed = true
}

// Close releases the tree read lock.
func (it *RevIter) Close() {
	if it.closed {
		return
	}
	it.t.mu.RUnlock()
	it.closed = true
}

// Error returns any error encountered during iteration.
func (it *Iter) Error() error {
	return it.err
}

// Error returns any error encountered during iteration.
func (it *RevIter) Error() error {
	return it.err
}
