package rootpublication

import "unsafe"

// stableTable is the one mutable ordered table used by ordinary and accounted
// registry/builders. A node is one independently stamped allocation. Rotations,
// lookup, removal and traversal allocate nothing. The enclosing owner supplies
// synchronization and admission; no table may invoke a foreign ledger callback.
type stableTable[K any, V any] struct {
	root    *stableTableNode[K, V]
	count   int
	less    func(K, K) bool
	census  BackingCensus
	keyPlan func(K) (uint64, uint64, error)
	ownKey  func(K) K
}
type stableTableNode[K any, V any] struct {
	left, right              *stableTableNode[K, V]
	height                   int
	key                      K
	value                    V
	stamp                    backingStamp
	keyBytes, keyAllocations uint64
}
type stableTableInsert[K any, V any] struct {
	table    *stableTable[K, V]
	node     *stableTableNode[K, V]
	key      K
	value    V
	existing *stableTableNode[K, V]
}

func newStableTable[K any, V any](less func(K, K) bool) stableTable[K, V] {
	return stableTable[K, V]{less: less}
}
func (t *stableTable[K, V]) find(k K) *stableTableNode[K, V] {
	for n := t.root; n != nil; {
		if t.less(k, n.key) {
			n = n.left
		} else if t.less(n.key, k) {
			n = n.right
		} else {
			return n
		}
	}
	return nil
}
func (t *stableTable[K, V]) get(k K) V {
	if n := t.find(k); n != nil {
		return n.value
	}
	var z V
	return z
}
func (t *stableTable[K, V]) lookup(k K) (V, bool) {
	if n := t.find(k); n != nil {
		return n.value, true
	}
	var z V
	return z, false
}

// prepare reserves the complete new node before privately allocating it.
// Applying this plan does not allocate or call an account. The enclosing lock
// must remain held between Prepare and Apply.
func (t *stableTable[K, V]) prepare(k K, v V, a StableMetadataAccount) (stableTableInsert[K, V], error) {
	p := stableTableInsert[K, V]{table: t, key: k, value: v, existing: t.find(k)}
	if p.existing != nil {
		return p, nil
	}
	b, err := StableBackingClassBytes(uint64(unsafe.Sizeof(stableTableNode[K, V]{})), true)
	if err != nil {
		if a != nil {
			return p, err
		}
		b = uint64(unsafe.Sizeof(stableTableNode[K, V]{}))
	}
	var kb, ka uint64
	if t.keyPlan != nil {
		kb, ka, err = t.keyPlan(k)
		if err != nil {
			if a != nil {
				return p, err
			}
			kb = 0
			ka = 0
		}
	}
	total, err := finiteStableAdd(b, kb)
	if err != nil {
		return p, err
	}
	if a != nil {
		if err = a.ReserveStableMetadata(total); err != nil {
			return p, err
		}
	}
	stamp := backingStamp{identity: nextBackingIdentity.Add(1), classBytes: b}
	if stamp.identity == 0 {
		panic("stable backing allocation identity exhausted")
	}
	if t.ownKey != nil {
		k = t.ownKey(k)
	}
	p.node = &stableTableNode[K, V]{height: 1, key: k, value: v, stamp: stamp, keyBytes: kb, keyAllocations: ka}
	return p, nil
}
func (p stableTableInsert[K, V]) apply() {
	if p.existing != nil {
		p.existing.value = p.value
		return
	}
	p.table.root = stableTableInsertNode(p.table.root, p.node, p.table.less)
	p.table.count++
	p.table.census.add(p.node.stamp)
	p.table.census.addGroup(p.node.keyBytes, p.node.keyAllocations)
}
func (t *stableTable[K, V]) set(k K, v V) {
	p, e := t.prepare(k, v, nil)
	if e != nil {
		panic(e)
	}
	p.apply()
}
func stableTableHeight[K any, V any](n *stableTableNode[K, V]) int {
	if n == nil {
		return 0
	}
	return n.height
}
func stableTableUpdate[K any, V any](n *stableTableNode[K, V]) {
	h := stableTableHeight(n.left)
	if r := stableTableHeight(n.right); r > h {
		h = r
	}
	n.height = h + 1
}
func stableTableRotateLeft[K any, V any](n *stableTableNode[K, V]) *stableTableNode[K, V] {
	p := n.right
	n.right = p.left
	p.left = n
	stableTableUpdate(n)
	stableTableUpdate(p)
	return p
}
func stableTableRotateRight[K any, V any](n *stableTableNode[K, V]) *stableTableNode[K, V] {
	p := n.left
	n.left = p.right
	p.right = n
	stableTableUpdate(n)
	stableTableUpdate(p)
	return p
}
func stableTableBalance[K any, V any](n *stableTableNode[K, V]) *stableTableNode[K, V] {
	if n == nil {
		return nil
	}
	stableTableUpdate(n)
	if stableTableHeight(n.left)-stableTableHeight(n.right) > 1 {
		if stableTableHeight(n.left.left) < stableTableHeight(n.left.right) {
			n.left = stableTableRotateLeft(n.left)
		}
		return stableTableRotateRight(n)
	}
	if stableTableHeight(n.right)-stableTableHeight(n.left) > 1 {
		if stableTableHeight(n.right.right) < stableTableHeight(n.right.left) {
			n.right = stableTableRotateRight(n.right)
		}
		return stableTableRotateLeft(n)
	}
	return n
}
func stableTableInsertNode[K any, V any](n, p *stableTableNode[K, V], less func(K, K) bool) *stableTableNode[K, V] {
	if n == nil {
		return p
	}
	if less(p.key, n.key) {
		n.left = stableTableInsertNode(n.left, p, less)
	} else {
		n.right = stableTableInsertNode(n.right, p, less)
	}
	return stableTableBalance(n)
}
func stableTableRemoveMin[K any, V any](n *stableTableNode[K, V]) (*stableTableNode[K, V], *stableTableNode[K, V]) {
	if n.left == nil {
		return n.right, n
	}
	var p *stableTableNode[K, V]
	n.left, p = stableTableRemoveMin(n.left)
	return stableTableBalance(n), p
}
func stableTableRemove[K any, V any](n *stableTableNode[K, V], k K, less func(K, K) bool) (*stableTableNode[K, V], backingStamp, bool) {
	if n == nil {
		return nil, backingStamp{}, false
	}
	var s backingStamp
	var ok bool
	if less(k, n.key) {
		n.left, s, ok = stableTableRemove(n.left, k, less)
	} else if less(n.key, k) {
		n.right, s, ok = stableTableRemove(n.right, k, less)
	} else {
		s = n.stamp
		s.classBytes += n.keyBytes
		s.extraAllocations = n.keyAllocations
		ok = true
		if n.left == nil {
			return n.right, s, true
		}
		if n.right == nil {
			return n.left, s, true
		}
		var p *stableTableNode[K, V]
		n.right, p = stableTableRemoveMin(n.right)
		p.left = n.left
		p.right = n.right
		return stableTableBalance(p), s, true
	}
	return stableTableBalance(n), s, ok
}
func (t *stableTable[K, V]) remove(k K) {
	var s backingStamp
	var ok bool
	t.root, s, ok = stableTableRemove(t.root, k, t.less)
	if ok {
		t.count--
		t.census.remove(s)
	}
}
func (t *stableTable[K, V]) visit(fn func(K, V) bool) bool { return stableTableVisit(t.root, fn) }
func stableTableVisit[K any, V any](n *stableTableNode[K, V], fn func(K, V) bool) bool {
	if n == nil {
		return true
	}
	return stableTableVisit(n.left, fn) && fn(n.key, n.value) && stableTableVisit(n.right, fn)
}
func (t *stableTable[K, V]) clear() {
	t.root = nil
	t.count = 0
	t.census.LiveClassBytes = 0
	t.census.LiveAllocations = 0
}

// removeWhere visits each original node once, with no traversal scratch. Unlike
// deleting during Visit, this remains correct through balancing rotations.
func (t *stableTable[K, V]) removeWhere(pred func(K, V) bool) {
	t.root = stableTableRemoveWhereNode(t, t.root, pred)
}
func stableTableRemoveWhereNode[K any, V any](t *stableTable[K, V], n *stableTableNode[K, V], pred func(K, V) bool) *stableTableNode[K, V] {
	if n == nil {
		return nil
	}
	n.left = stableTableRemoveWhereNode(t, n.left, pred)
	n.right = stableTableRemoveWhereNode(t, n.right, pred)
	if !pred(n.key, n.value) {
		return stableTableJoin(n.left, n, n.right)
	}
	t.count--
	s := n.stamp
	s.classBytes += n.keyBytes
	s.extraAllocations = n.keyAllocations
	t.census.remove(s)
	if n.left == nil {
		return n.right
	}
	if n.right == nil {
		return n.left
	}
	var p *stableTableNode[K, V]
	n.right, p = stableTableRemoveMin(n.right)
	return stableTableJoin(n.left, p, n.right)
}

// join handles arbitrary height differences after a whole-table filter.
func stableTableJoin[K any, V any](left, center, right *stableTableNode[K, V]) *stableTableNode[K, V] {
	if stableTableHeight(left) > stableTableHeight(right)+1 {
		left.right = stableTableJoin(left.right, center, right)
		return stableTableBalance(left)
	}
	if stableTableHeight(right) > stableTableHeight(left)+1 {
		right.left = stableTableJoin(left, center, right.left)
		return stableTableBalance(right)
	}
	center.left = left
	center.right = right
	stableTableUpdate(center)
	return center
}

func (t *stableTable[K, V]) plannedInsertBytes(k K) (uint64, error) {
	if t.find(k) != nil {
		return 0, nil
	}
	b, e := StableBackingClassBytes(uint64(unsafe.Sizeof(stableTableNode[K, V]{})), true)
	if e != nil {
		return 0, e
	}
	if t.keyPlan != nil {
		keys, _, err := t.keyPlan(k)
		if err != nil {
			return 0, err
		}
		return finiteStableAdd(b, keys)
	}
	return b, nil
}
