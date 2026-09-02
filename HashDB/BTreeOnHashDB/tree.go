package btreeonhashdb

import (
	"bytes"
	"container/list"
	"fmt"
	"sync"
)

// Meta is the durable header describing the tree layout.
type Meta struct {
	RootNodeID NodeID
	Height     uint16
	NextNodeID NodeID
}

type kvBatcher interface {
	PutMany(keys [][]byte, values [][]byte) error
	Flush() error
}

// Tree implements a B+Tree on top of an abstract KV store.
// Concurrency is handled with a coarse-grained RWMutex.
type Tree struct {
	treeID string
	kv     KVStore

	mu        sync.RWMutex
	meta      Meta
	cacheMu   sync.Mutex
	nodeCache map[NodeID]*list.Element
	cacheList *list.List
	cacheCap  int
	metaDirty bool

	inBatch    bool
	batchNodes map[NodeID]*Node
}

// OpenTree loads an existing tree or initializes a new one if no metadata exists.
// Meta read errors are returned to the caller; only an absent meta entry triggers initialization.
func OpenTree(kv KVStore, treeID string, opts *Options) (*Tree, error) {
	cacheSize := 128
	if opts != nil && opts.CacheSize > 0 {
		cacheSize = opts.CacheSize
	}

	t := &Tree{
		treeID:    treeID,
		kv:        kv,
		cacheCap:  cacheSize,
		nodeCache: make(map[NodeID]*list.Element),
		cacheList: list.New(),
	}

	metaBytes, err := kv.Get(metaKey(treeID))
	if err != nil {
		return nil, fmt.Errorf("load meta: %w", err)
	}

	if len(metaBytes) > 0 {
		m, err := decodeMeta(metaBytes)
		if err != nil {
			return nil, fmt.Errorf("decode meta: %w", err)
		}
		t.meta = *m
		return t, nil
	}

	// Initialize a new tree with a single empty leaf root.
	root := &Node{
		ID:   1,
		Type: NodeLeaf,
	}

	t.meta = Meta{
		RootNodeID: 1,
		Height:     1,
		NextNodeID: 2,
	}
	t.metaDirty = true

	if err := t.saveNode(root); err != nil {
		return nil, err
	}
	if err := t.saveMeta(); err != nil {
		return nil, err
	}

	return t, nil
}

// Get returns the value for the given key or (nil, nil) if it does not exist.
func (t *Tree) Get(key []byte) ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Values live directly in the underlying KV keyed by the user key.
	return t.kv.Get(key)
}

// Put inserts or updates the given key/value pair.
func (t *Tree) Put(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// First write the value into the underlying KV keyed by the user key.
	if err := t.kv.Put(key, value); err != nil {
		return err
	}

	opCache := make(map[NodeID]*Node)
	split, splitKey, rightID, err := t.insertRecursiveCached(t.meta.RootNodeID, key, value, t.meta.Height, opCache)
	if err != nil {
		return err
	}

	if split {
		newRootID := t.meta.NextNodeID
		t.meta.NextNodeID++

		newRoot := &Node{
			ID:       newRootID,
			Type:     NodeInternal,
			Keys:     [][]byte{splitKey},
			Children: []NodeID{t.meta.RootNodeID, rightID},
		}
		if err := t.saveNode(newRoot); err != nil {
			return err
		}

		t.meta.RootNodeID = newRootID
		t.meta.Height++
		t.metaDirty = true
	}

	return t.saveMetaIfDirty()
}

// PutMany inserts multiple key/value pairs in a batch.
func (t *Tree) PutMany(keys, values [][]byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(keys) != len(values) {
		return fmt.Errorf("keys/values length mismatch")
	}

	// 1. Batch write user values to the underlying KV.
	if b, ok := t.kv.(kvBatcher); ok {
		if err := b.PutMany(keys, values); err != nil {
			return err
		}
	} else {
		for i := range keys {
			if err := t.kv.Put(keys[i], values[i]); err != nil {
				return err
			}
		}
	}

	// 2. Insert into tree structure with deferred node saves.
	t.inBatch = true
	t.batchNodes = make(map[NodeID]*Node)
	defer func() {
		t.inBatch = false
		t.batchNodes = nil
	}()

	opCache := make(map[NodeID]*Node)

	for i := range keys {
		// Clear opCache for each operation to avoid accumulating too many nodes.
		for k := range opCache {
			delete(opCache, k)
		}

		split, splitKey, rightID, err := t.insertRecursiveCached(t.meta.RootNodeID, keys[i], values[i], t.meta.Height, opCache)
		if err != nil {
			return err
		}

		if split {
			newRootID := t.meta.NextNodeID
			t.meta.NextNodeID++

			newRoot := &Node{
				ID:       newRootID,
				Type:     NodeInternal,
				Keys:     [][]byte{splitKey},
				Children: []NodeID{t.meta.RootNodeID, rightID},
			}
			if err := t.saveNode(newRoot); err != nil {
				return err
			}

			t.meta.RootNodeID = newRootID
			t.meta.Height++
			t.metaDirty = true
		}
	}

	// 3. Flush batch nodes
	if err := t.saveBatchNodes(); err != nil {
		return err
	}

	return t.saveMetaIfDirty()
}

func (t *Tree) saveBatchNodes() error {
	if len(t.batchNodes) == 0 {
		return nil
	}

	nodes := make([]*Node, 0, len(t.batchNodes))
	for _, n := range t.batchNodes {
		nodes = append(nodes, n)
	}

	// Temporarily disable inBatch so saveNodes actually writes them
	t.inBatch = false
	err := t.saveNodes(nodes...)
	t.inBatch = true // Restore
	return err
}

// Delete removes a key from the tree. It is a lazy delete and performs no rebalancing.
func (t *Tree) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	currID := t.meta.RootNodeID
	opCache := make(map[NodeID]*Node)

	for {
		node, err := t.loadNodeCached(currID, opCache)
		if err != nil {
			return err
		}

		idx := node.search(key)
		if node.Type == NodeLeaf {
			if idx < len(node.Keys) && bytes.Equal(node.Keys[idx], key) {
				node.Keys = append(node.Keys[:idx], node.Keys[idx+1:]...)
				if err := t.saveNode(node); err != nil {
					return err
				}
				// Remove value from KV as well.
				return t.kv.Delete(key)
			}
			// Key not present in tree; best-effort delete from KV.
			return t.kv.Delete(key)
		}

		childIdx := idx
		if idx < len(node.Keys) && bytes.Equal(node.Keys[idx], key) {
			childIdx = idx + 1
		}
		if childIdx >= len(node.Children) {
			return fmt.Errorf("node %d child index %d out of range", node.ID, childIdx)
		}
		currID = node.Children[childIdx]
	}
}

// insertRecursive inserts into the subtree rooted at nodeID.
// Returns whether the node split, the promoted key, and the new right node ID.
func (t *Tree) insertRecursive(nodeID NodeID, key, value []byte, level uint16) (bool, []byte, NodeID, error) {
	nodeCache := make(map[NodeID]*Node)
	return t.insertRecursiveCached(nodeID, key, value, level, nodeCache)
}

func (t *Tree) insertRecursiveCached(nodeID NodeID, key, value []byte, level uint16, opCache map[NodeID]*Node) (bool, []byte, NodeID, error) {
	node, err := t.loadNodeCached(nodeID, opCache)
	if err != nil {
		return false, nil, 0, err
	}

	if level == 0 {
		return false, nil, 0, fmt.Errorf("invalid tree level for node %d", nodeID)
	}

	idx := node.search(key)

	if node.Type == NodeLeaf {
		if level != 1 {
			return false, nil, 0, fmt.Errorf("leaf node %d encountered above leaf level", nodeID)
		}
		// Keys only in leaf; values are stored separately in the KV.
		if idx < len(node.Keys) && bytes.Equal(node.Keys[idx], key) {
			// Key already present; nothing to change in the leaf.
			return false, nil, 0, nil
		}

		node.Keys = insertBytes(node.Keys, idx, key)

		if len(node.Keys) <= MaxKeys {
			return false, nil, 0, t.saveNode(node)
		}

		return t.splitLeaf(node)
	}

	if level <= 1 {
		return false, nil, 0, fmt.Errorf("internal node %d encountered at leaf level", nodeID)
	}

	childIdx := idx
	if idx < len(node.Keys) && bytes.Equal(node.Keys[idx], key) {
		childIdx = idx + 1
	}
	if childIdx >= len(node.Children) {
		return false, nil, 0, fmt.Errorf("node %d child index %d out of range", node.ID, childIdx)
	}

	childSplit, splitKey, rightID, err := t.insertRecursiveCached(node.Children[childIdx], key, value, level-1, opCache)
	if err != nil {
		return false, nil, 0, err
	}

	if !childSplit {
		return false, nil, 0, nil
	}

	node.Keys = insertBytes(node.Keys, childIdx, splitKey)
	node.Children = insertNodeID(node.Children, childIdx+1, rightID)

	if len(node.Keys) <= MaxKeys {
		return false, nil, 0, t.saveNode(node)
	}

	return t.splitInternal(node)
}

// splitLeaf splits a full leaf node into two and links them.
func (t *Tree) splitLeaf(left *Node) (bool, []byte, NodeID, error) {
	mid := len(left.Keys) / 2

	rightID := t.meta.NextNodeID
	t.meta.NextNodeID++
	t.metaDirty = true

	right := &Node{
		ID:       rightID,
		Type:     NodeLeaf,
		Keys:     append([][]byte(nil), left.Keys[mid:]...),
		NextLeaf: left.NextLeaf,
		PrevLeaf: left.ID,
	}

	left.Keys = left.Keys[:mid]
	left.NextLeaf = rightID

	if right.NextLeaf != 0 {
		if next, err := t.loadNode(right.NextLeaf); err == nil {
			next.PrevLeaf = rightID
			_ = t.saveNode(next)
		}
	}

	if err := t.saveNodes(right, left); err != nil {
		return false, nil, 0, err
	}

	return true, right.Keys[0], rightID, nil
}

// splitInternal splits a full internal node and returns the promoted key.
func (t *Tree) splitInternal(left *Node) (bool, []byte, NodeID, error) {
	mid := len(left.Keys) / 2
	promoted := left.Keys[mid]

	rightID := t.meta.NextNodeID
	t.meta.NextNodeID++
	t.metaDirty = true

	right := &Node{
		ID:       rightID,
		Type:     NodeInternal,
		Keys:     append([][]byte(nil), left.Keys[mid+1:]...),
		Children: append([]NodeID(nil), left.Children[mid+1:]...),
	}

	left.Keys = left.Keys[:mid]
	left.Children = left.Children[:mid+1]

	if err := t.saveNodes(right, left); err != nil {
		return false, nil, 0, err
	}

	return true, promoted, rightID, nil
}

func (t *Tree) saveMeta() error {
	if b, ok := t.kv.(kvBatcher); ok {
		if err := b.PutMany([][]byte{metaKey(t.treeID)}, [][]byte{encodeMeta(&t.meta)}); err != nil {
			return err
		}
		if err := b.Flush(); err != nil {
			return err
		}
		t.metaDirty = false
		return nil
	}
	if err := t.kv.Put(metaKey(t.treeID), encodeMeta(&t.meta)); err != nil {
		return err
	}
	t.metaDirty = false
	return nil
}

func (t *Tree) loadNode(id NodeID) (*Node, error) {
	if t.inBatch && t.batchNodes != nil {
		if n, ok := t.batchNodes[id]; ok {
			return n, nil
		}
	}
	if n := t.cacheGet(id); n != nil {
		return n, nil
	}
	data, err := t.kv.Get(nodeKey(t.treeID, id))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("node %d not found", id)
	}
	n, err := decodeNode(id, data)
	if err != nil {
		return nil, err
	}
	t.cachePut(n)
	return n, nil
}

func (t *Tree) saveNode(n *Node) error {
	return t.saveNodes(n)
}

func insertBytes[T any](s []T, i int, v T) []T {
	if len(s) < cap(s) {
		s = s[:len(s)+1]
		copy(s[i+1:], s[i:])
		s[i] = v
		return s
	}
	ns := make([]T, len(s)+1)
	copy(ns, s[:i])
	ns[i] = v
	copy(ns[i+1:], s[i:])
	return ns
}

func insertNodeID(s []NodeID, i int, v NodeID) []NodeID {
	if len(s) < cap(s) {
		s = s[:len(s)+1]
		copy(s[i+1:], s[i:])
		s[i] = v
		return s
	}
	ns := make([]NodeID, len(s)+1)
	copy(ns, s[:i])
	ns[i] = v
	copy(ns[i+1:], s[i:])
	return ns
}

func (t *Tree) saveMetaIfDirty() error {
	if !t.metaDirty {
		return nil
	}
	return t.saveMeta()
}

func (t *Tree) saveNodes(nodes ...*Node) error {
	if len(nodes) == 0 {
		return nil
	}
	if t.inBatch && t.batchNodes != nil {
		for _, n := range nodes {
			t.batchNodes[n.ID] = n
			t.cachePut(n)
		}
		return nil
	}
	keys := make([][]byte, len(nodes))
	vals := make([][]byte, len(nodes))
	for i, n := range nodes {
		enc, err := encodeNode(n)
		if err != nil {
			return err
		}
		keys[i] = nodeKey(t.treeID, n.ID)
		vals[i] = enc
	}

	if b, ok := t.kv.(kvBatcher); ok {
		if err := b.PutMany(keys, vals); err != nil {
			return err
		}
		if err := b.Flush(); err != nil {
			return err
		}
	} else {
		for i := range nodes {
			if err := t.kv.Put(keys[i], vals[i]); err != nil {
				return err
			}
		}
	}
	for _, n := range nodes {
		t.cachePut(n)
	}
	return nil
}

func (t *Tree) cacheGet(id NodeID) *Node {
	if t.cacheCap == 0 {
		return nil
	}
	t.cacheMu.Lock()
	defer t.cacheMu.Unlock()
	if el, ok := t.nodeCache[id]; ok {
		t.cacheList.MoveToFront(el)
		if n, ok := el.Value.(*Node); ok {
			return n
		}
	}
	return nil
}

func (t *Tree) cachePut(n *Node) {
	if t.cacheCap == 0 {
		return
	}
	t.cacheMu.Lock()
	defer t.cacheMu.Unlock()
	if t.nodeCache == nil {
		t.nodeCache = make(map[NodeID]*list.Element)
		t.cacheList = list.New()
	}
	if el, ok := t.nodeCache[n.ID]; ok {
		el.Value = n
		t.cacheList.MoveToFront(el)
		return
	}
	el := t.cacheList.PushFront(n)
	t.nodeCache[n.ID] = el
	if t.cacheList.Len() > t.cacheCap {
		lru := t.cacheList.Back()
		if lru != nil {
			t.cacheList.Remove(lru)
			if ln, ok := lru.Value.(*Node); ok {
				delete(t.nodeCache, ln.ID)
			}
		}
	}
}

// loadNodeCached first checks an operation-local cache, then the tree cache, then storage.
func (t *Tree) loadNodeCached(id NodeID, opCache map[NodeID]*Node) (*Node, error) {
	if opCache != nil {
		if n := opCache[id]; n != nil {
			return n, nil
		}
	}
	n, err := t.loadNode(id)
	if err != nil {
		return nil, err
	}
	if opCache != nil {
		opCache[id] = n
	}
	return n, nil
}
