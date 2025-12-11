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

// Valid reports whether the iterator points to a valid key/value pair.
func (it *Iter) Valid() bool {
	return it.valid && it.err == nil
}

// Key returns the current key or nil if invalid.
func (it *Iter) Key() []byte {
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

// Next advances the iterator.
func (it *Iter) Next() {
	if !it.Valid() {
		return
	}
	it.currIdx++
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

// Error returns any error encountered during iteration.
func (it *Iter) Error() error {
	return it.err
}
