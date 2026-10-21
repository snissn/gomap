package cowbtree

// ResetTree rebinds a read-only iterator while retaining its admitted stack
// backing. It clears every old edge, including cells above the live length,
// before installing the next immutable tree. It allocates no memory. A nil
// tree leaves an empty reusable iterator, so singleton projections need not
// discard the stack needed by their following multi-version neighbor.
func (iter *MapIter[K, V]) ResetTree(tree *Map[K, V]) {
	stack := iter.stack
	clear(stack[:cap(stack)])
	*iter = MapIter[K, V]{tr: tree, stack: stack[:0]}
}
