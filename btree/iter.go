package btree

import (
	"bytes"
	"fmt"
)

// Iter iterates over the B+Tree in ascending key order.
// It holds a read lock on the tree for its lifetime; callers must Close().
type Iter struct {
	t        *Tree
	currNode *Node
	currIdx  int
	end      []byte // exclusive; nil for unbounded
	err      error
	valid    bool
	closed   bool
}

// RevIter iterates over the B+Tree in descending key order.
// It holds a read lock on the tree for its lifetime; callers must Close().
type RevIter struct {
	t        *Tree
	currNode *Node
	currIdx  int
	start    []byte // inclusive lower bound
	end      []byte // exclusive upper bound; nil for unbounded
	err      error
	valid    bool
	closed   bool
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
			if start == nil {
				it.currIdx = 0
			} else {
				it.currIdx = node.search(start)
			}
			it.currNode = node
			it.adjust()
			return nil
		}

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
	if it.end == nil {
		// Seek to the rightmost leaf.
		currID := it.t.meta.RootNodeID
		for {
			node, err := it.t.loadNode(currID)
			if err != nil {
				return err
			}
			if node.Type == NodeLeaf {
				it.currNode = node
				it.currIdx = len(node.Keys) - 1
				it.adjust()
				return nil
			}
			currID = node.Children[len(node.Children)-1]
		}
	}

	// Seek to the leaf containing the largest key < end.
	currID := it.t.meta.RootNodeID
	for {
		node, err := it.t.loadNode(currID)
		if err != nil {
			return err
		}

		if node.Type == NodeLeaf {
			it.currNode = node
			it.currIdx = node.search(it.end) - 1
			it.adjust()
			return nil
		}

		childIdx := node.search(it.end)
		if childIdx >= len(node.Children) {
			childIdx = len(node.Children) - 1
		}
		currID = node.Children[childIdx]
	}
}

// adjust normalizes the iterator to the next valid position respecting bounds.
func (it *Iter) adjust() {
	if it.err != nil || it.currNode == nil {
		it.valid = false
		return
	}

	for it.currIdx >= len(it.currNode.Keys) {
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
		it.currNode = nextNode
		it.currIdx = 0
	}

	if len(it.currNode.Keys) == 0 {
		it.valid = false
		return
	}

	if it.end != nil && bytes.Compare(it.currNode.Keys[it.currIdx], it.end) >= 0 {
		it.valid = false
		return
	}

	it.valid = true
}

// adjust normalizes the reverse iterator to the next valid position respecting bounds.
func (it *RevIter) adjust() {
	if it.err != nil || it.currNode == nil {
		it.valid = false
		return
	}

	for it.currIdx < 0 {
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
		it.currNode = prevNode
		it.currIdx = len(prevNode.Keys) - 1
	}

	if it.currIdx < 0 || it.currIdx >= len(it.currNode.Keys) {
		it.valid = false
		return
	}

	key := it.currNode.Keys[it.currIdx]
	if it.end != nil && bytes.Compare(key, it.end) >= 0 {
		it.currIdx--
		it.adjust()
		return
	}
	if it.start != nil && bytes.Compare(key, it.start) < 0 {
		it.valid = false
		return
	}

	it.valid = true
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
	if !it.Valid() {
		return nil
	}
	return it.currNode.Keys[it.currIdx]
}

// Key returns the current key or nil if invalid.
func (it *RevIter) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.currNode.Keys[it.currIdx]
}

// Value returns the current value or nil if invalid.
func (it *Iter) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.currNode.Values[it.currIdx]
}

// Value returns the current value or nil if invalid.
func (it *RevIter) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.currNode.Values[it.currIdx]
}

// Next advances the iterator.
func (it *Iter) Next() {
	if !it.Valid() {
		return
	}
	it.currIdx++
	it.adjust()
}

// Next advances the iterator backwards.
func (it *RevIter) Next() {
	if !it.Valid() {
		return
	}
	it.currIdx--
	it.adjust()
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
