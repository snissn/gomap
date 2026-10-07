package freelist

import (
	"math/bits"
	"unsafe"
)

// numericRadixV1 replaces the existing numeric maps in both ordinary and
// finite allocator paths. There is no hash directory or capacity growth.
// Internal branches select the first differing bit; leaves own exact keys.
type numericRadixLeafV1[K comparable, V comparable] struct {
	key    K
	value  V
	credit *allocationCreditLeaseV1
}
type numericRadixBranchV1 struct {
	bit    uint8
	child  [2]any
	credit *allocationCreditLeaseV1
}
type numericRadixV1[K comparable, V comparable] struct {
	root   any
	count  int
	bit    func(K, uint8) uint8
	diff   func(K, K) int
	credit *allocationCreditLeaseV1
}

func newPageRadixV1[V comparable]() *numericRadixV1[uint64, V] {
	return &numericRadixV1[uint64, V]{
		bit:  func(k uint64, b uint8) uint8 { return uint8(k>>(63-b)) & 1 },
		diff: func(a, b uint64) int { return bits.LeadingZeros64(a ^ b) },
	}
}
func newCandidateRadixV1[V comparable]() *numericRadixV1[CandidateIDV1, V] {
	return &numericRadixV1[CandidateIDV1, V]{
		bit: func(k CandidateIDV1, b uint8) uint8 { return (k[b/8] >> (7 - b%8)) & 1 },
		diff: func(a, b CandidateIDV1) int {
			for i := range a {
				if a[i] != b[i] {
					return i*8 + bits.LeadingZeros8(a[i]^b[i])
				}
			}
			return 128
		},
	}
}
func (m *numericRadixV1[K, V]) Len() int {
	if m == nil {
		return 0
	}
	return m.count
}
func (m *numericRadixV1[K, V]) Get(key K) (V, bool) {
	var zero V
	if m == nil {
		return zero, false
	}
	node := m.root
	for node != nil {
		switch n := node.(type) {
		case *numericRadixLeafV1[K, V]:
			if n.key == key {
				return n.value, true
			}
			return zero, false
		case *numericRadixBranchV1:
			node = n.child[m.bit(key, n.bit)]
		}
	}
	return zero, false
}
func (m *numericRadixV1[K, V]) Value(key K) V { value, _ := m.Get(key); return value }
func (m *numericRadixV1[K, V]) Put(key K, value V) error {
	return m.PutWithCredit(key, value, m.credit)
}
func (m *numericRadixV1[K, V]) PutWithCredit(key K, value V, credit *allocationCreditLeaseV1) error {
	return m.putAdmittedV1(key, value, credit, nil)
}

func (m *numericRadixV1[K, V]) putAdmittedV1(key K, value V, credit *allocationCreditLeaseV1, operation *allocationOperationV1) error {
	if m == nil {
		return ErrGenerationFormat
	}
	var found *numericRadixLeafV1[K, V]
	node := m.root
	for node != nil {
		switch n := node.(type) {
		case *numericRadixLeafV1[K, V]:
			found = n
			node = nil
		case *numericRadixBranchV1:
			node = n.child[m.bit(key, n.bit)]
		}
	}
	if found != nil && found.key == key {
		found.value = value
		return nil
	}
	charge := allocationClassV1(uint64(unsafe.Sizeof(numericRadixLeafV1[K, V]{})), true)
	if found != nil {
		charge += allocationClassV1(uint64(unsafe.Sizeof(numericRadixBranchV1{})), true)
	}
	refs := uint64(1)
	if found != nil {
		refs++
	}
	if err := reserveBirthV1(credit, charge, refs, operation); err != nil {
		return err
	}
	leaf := &numericRadixLeafV1[K, V]{key: key, value: value, credit: credit}
	if found == nil {
		m.root = leaf
		m.count++
		return nil
	}
	different := uint8(m.diff(key, found.key))
	link := &m.root
	for {
		branch, ok := (*link).(*numericRadixBranchV1)
		if !ok || branch.bit >= different {
			break
		}
		link = &branch.child[m.bit(key, branch.bit)]
	}
	branch := &numericRadixBranchV1{bit: different, credit: credit}
	side := m.bit(key, different)
	branch.child[side], branch.child[1-side] = leaf, *link
	*link = branch
	m.count++
	return nil
}

// Set is used by ordinary mutation sites. Finite constructors must pre-admit
// the complete operation, and use Put where a debit can fail.
func (m *numericRadixV1[K, V]) Set(key K, value V) {
	if err := m.Put(key, value); err != nil {
		panic(err)
	}
}
func (m *numericRadixV1[K, V]) Delete(key K) {
	if m == nil || m.root == nil {
		return
	}
	link := &m.root
	var parent *numericRadixBranchV1
	var parentLink *any
	var side uint8
	for {
		switch node := (*link).(type) {
		case *numericRadixBranchV1:
			parent, parentLink = node, link
			side = m.bit(key, node.bit)
			link = &node.child[side]
		case *numericRadixLeafV1[K, V]:
			if node.key != key {
				return
			}
			var zero V
			node.value = zero
			var zeroKey K
			node.key = zeroKey
			leafCredit := node.credit
			node.credit = nil
			if parent == nil {
				m.root = nil
			} else {
				*parentLink = parent.child[1-side]
				parent.child = [2]any{}
				branchCredit := parent.credit
				parent.credit = nil
				branchCredit.release()
			}
			m.count--
			leafCredit.release()
			return
		default:
			return
		}
	}
}
func (m *numericRadixV1[K, V]) Range(visit func(K, V) bool) {
	if m == nil {
		return
	}
	var walk func(any) bool
	walk = func(node any) bool {
		switch n := node.(type) {
		case *numericRadixLeafV1[K, V]:
			return visit(n.key, n.value)
		case *numericRadixBranchV1:
			return walk(n.child[0]) && walk(n.child[1])
		default:
			return true
		}
	}
	walk(m.root)
}
func (m *numericRadixV1[K, V]) CloneWithCredit(credit *allocationCreditLeaseV1) (*numericRadixV1[K, V], error) {
	if m == nil {
		return nil, nil
	}
	if err := credit.reserve(allocationClassV1(uint64(unsafe.Sizeof(numericRadixV1[K, V]{})), true), 1); err != nil {
		return nil, err
	}
	clone := &numericRadixV1[K, V]{bit: m.bit, diff: m.diff, credit: credit}
	var err error
	m.Range(func(key K, value V) bool { err = clone.Put(key, value); return err == nil })
	if err != nil {
		clone.Clear()
		return nil, err
	}
	return clone, nil
}
func (m *numericRadixV1[K, V]) Clone() *numericRadixV1[K, V] {
	clone, err := m.CloneWithCredit(m.credit)
	if err != nil {
		panic(err)
	}
	return clone
}

// Clear destroys node aliases before releasing the creating request references.
func (m *numericRadixV1[K, V]) Clear() {
	if m == nil {
		return
	}
	for m.root != nil {
		node := m.root
		for {
			if leaf, ok := node.(*numericRadixLeafV1[K, V]); ok {
				m.Delete(leaf.key)
				break
			}
			node = node.(*numericRadixBranchV1).child[0]
		}
	}
	credit := m.credit
	m.credit = nil
	credit.release()
}
func (m *numericRadixV1[K, V]) Equal(other *numericRadixV1[K, V]) bool {
	if m.Len() != other.Len() {
		return false
	}
	equal := true
	m.Range(func(key K, value V) bool { got, ok := other.Get(key); equal = ok && got == value; return equal })
	return equal
}

// Source-derived Go1.26.3 linux/amd64 class charge, including scanning headers
// and large-object page rounding. Supported finite admission pins that ABI.
func allocationClassV1(size uint64, scan bool) uint64 {
	if size == 0 {
		return 0
	}
	if !scan && size < 16 {
		return 16
	}
	if size <= 32760 {
		if scan && size > 512 {
			size += 8
		}
		for _, class := range allocationClassesV1 {
			if size <= uint64(class) {
				return uint64(class)
			}
		}
	}
	if size > ^uint64(0)-8191 {
		return ^uint64(0)
	}
	return (size + 8191) &^ uint64(8191)
}

var allocationClassesV1 = [...]uint16{8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256, 288, 320, 352, 384, 416, 448, 480, 512, 576, 640, 704, 768, 896, 1024, 1152, 1280, 1408, 1536, 1792, 2048, 2304, 2688, 3072, 3200, 3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912, 8192, 9472, 9728, 10240, 10880, 12288, 13568, 14336, 16384, 18432, 19072, 20480, 21760, 24576, 27264, 28672, 32768}

func (m *numericRadixV1[K, V]) residentBytesV1() uint64 {
	if m == nil {
		return 0
	}
	bytes := allocationClassV1(uint64(unsafe.Sizeof(*m)), true)
	var walk func(any)
	walk = func(node any) {
		switch n := node.(type) {
		case *numericRadixLeafV1[K, V]:
			bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(unsafe.Sizeof(*n)), true))
			// Conservative overlap: the same lease may be counted per node. This
			// upper bound needs no owner registry and never excludes live backing.
			if n.credit != nil {
				bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(unsafe.Sizeof(*n.credit)), true))
			}
		case *numericRadixBranchV1:
			bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(unsafe.Sizeof(*n)), true))
			if n.credit != nil {
				bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(unsafe.Sizeof(*n.credit)), true))
			}
			walk(n.child[0])
			walk(n.child[1])
		}
	}
	walk(m.root)
	return bytes
}

func newPageRadixWithCreditV1[V comparable](credit *allocationCreditLeaseV1) (*numericRadixV1[uint64, V], error) {
	if err := credit.reserve(allocationClassV1(uint64(unsafe.Sizeof(numericRadixV1[uint64, V]{})), true), 1); err != nil {
		return nil, err
	}
	result := newPageRadixV1[V]()
	result.credit = credit
	return result, nil
}
func newCandidateRadixWithCreditV1[V comparable](credit *allocationCreditLeaseV1) (*numericRadixV1[CandidateIDV1, V], error) {
	if err := credit.reserve(allocationClassV1(uint64(unsafe.Sizeof(numericRadixV1[CandidateIDV1, V]{})), true), 1); err != nil {
		return nil, err
	}
	result := newCandidateRadixV1[V]()
	result.credit = credit
	return result, nil
}

// insertionCapacityV1 plans new DISTINCT keys against the current radix, under
// its existing containing lock. No old/new directory or rehash backing exists.
func (m *numericRadixV1[K, V]) insertionCapacityV1(newKeys uint64) (uint64, uint64) {
	branches := newKeys
	if m.root == nil && branches != 0 {
		branches--
	}
	bytes := cowSaturatingMulV1(newKeys, allocationClassV1(uint64(unsafe.Sizeof(numericRadixLeafV1[K, V]{})), true))
	bytes = cowSaturatingAddV1(bytes, cowSaturatingMulV1(branches, allocationClassV1(uint64(unsafe.Sizeof(numericRadixBranchV1{})), true)))
	return bytes, cowSaturatingAddV1(newKeys, branches)
}
