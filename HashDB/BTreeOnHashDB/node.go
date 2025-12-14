package btreeonhashdb

import (
	"bytes"
	"sort"
)

// Node represents an in-memory B+Tree node.
// Keys are kept sorted in ascending lexicographic order.
type Node struct {
	ID   NodeID
	Type NodeType

	// Common
	Keys [][]byte

	// Internal
	Children []NodeID // len(Children) == len(Keys)+1 when Type == NodeInternal

	// Leaf
	Values   [][]byte // len(Values) == len(Keys) when Type == NodeLeaf
	NextLeaf NodeID
	PrevLeaf NodeID
}

// search returns the index of the first key >= the provided key.
// If all keys are < key, it returns len(Keys).
func (n *Node) search(key []byte) int {
	return sort.Search(len(n.Keys), func(i int) bool {
		return bytes.Compare(n.Keys[i], key) >= 0
	})
}
