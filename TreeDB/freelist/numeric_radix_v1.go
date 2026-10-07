package freelist

import (
	"math/bits"
	"unsafe"
)

// Slots are private to their containing radix. One intrinsic creating-credit
// edge owns the complete typed chunk, including its free capacity and links.
const numericRadixChunkSlotsV1 = 32

type numericRadixRefV1[K comparable, V comparable] struct {
	chunk *numericRadixChunkV1[K, V]
	slot  uint16
}
type numericRadixNodeV1[K comparable, V comparable] struct {
	key      K
	value    V
	child    [2]numericRadixRefV1[K, V]
	nextFree uint16
	bit      uint8
	leaf     bool
}
type numericRadixChunkV1[K comparable, V comparable] struct {
	nodes                        [numericRadixChunkSlotsV1]numericRadixNodeV1[K, V]
	prev, next                   *numericRadixChunkV1[K, V]
	availablePrev, availableNext *numericRadixChunkV1[K, V]
	credit                       *allocationCreditLeaseV1
	free                         uint16
	used                         uint16
}
type numericRadixV1[K comparable, V comparable] struct {
	root             numericRadixRefV1[K, V]
	head, available  *numericRadixChunkV1[K, V]
	count, freeSlots int
	bit              func(K, uint8) uint8
	diff             func(K, K) int
	credit           *allocationCreditLeaseV1
}

func (r numericRadixRefV1[K, V]) node() *numericRadixNodeV1[K, V] {
	return &r.chunk.nodes[r.slot-1]
}
func newPageRadixV1[V comparable]() *numericRadixV1[uint64, V] {
	return &numericRadixV1[uint64, V]{
		bit:  func(k uint64, b uint8) uint8 { return uint8((k >> (63 - b)) & 1) },
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
	ref := m.root
	for ref.chunk != nil {
		n := ref.node()
		if n.leaf {
			if n.key == key {
				return n.value, true
			}
			return zero, false
		}
		ref = n.child[m.bit(key, n.bit)]
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
func (m *numericRadixV1[K, V]) Set(key K, value V) {
	if err := m.Put(key, value); err != nil {
		panic(err)
	}
}

func (m *numericRadixV1[K, V]) addAvailableV1(c *numericRadixChunkV1[K, V]) {
	c.availablePrev = nil
	c.availableNext = m.available
	if m.available != nil {
		m.available.availablePrev = c
	}
	m.available = c
}
func (m *numericRadixV1[K, V]) removeAvailableV1(c *numericRadixChunkV1[K, V]) {
	if c.availablePrev != nil {
		c.availablePrev.availableNext = c.availableNext
	} else {
		m.available = c.availableNext
	}
	if c.availableNext != nil {
		c.availableNext.availablePrev = c.availablePrev
	}
	c.availablePrev, c.availableNext = nil, nil
}
func numericRadixChunkClassV1[K comparable, V comparable]() uint64 {
	return allocationClassV1(uint64(unsafe.Sizeof(numericRadixChunkV1[K, V]{})), true)
}

// The complete prospective backing is admitted before its first chunk birth.
// The operation receipt and this method consume the same whole-chunk classes.
func (m *numericRadixV1[K, V]) ensureSlotsV1(needed int, credit *allocationCreditLeaseV1, operation *allocationOperationV1) error {
	if needed <= m.freeSlots {
		return nil
	}
	deficit := uint64(needed - m.freeSlots)
	chunks := (deficit + numericRadixChunkSlotsV1 - 1) / numericRadixChunkSlotsV1
	charge := cowSaturatingMulV1(chunks, numericRadixChunkClassV1[K, V]())
	if err := reserveBirthV1(credit, charge, chunks, operation); err != nil {
		return err
	}
	for ; chunks != 0; chunks-- {
		c := &numericRadixChunkV1[K, V]{credit: credit, free: 1, next: m.head}
		for i := 0; i < numericRadixChunkSlotsV1-1; i++ {
			c.nodes[i].nextFree = uint16(i + 2)
		}
		if m.head != nil {
			m.head.prev = c
		}
		m.head = c
		m.addAvailableV1(c)
		m.freeSlots += numericRadixChunkSlotsV1
	}
	return nil
}
func (m *numericRadixV1[K, V]) allocateSlotV1() numericRadixRefV1[K, V] {
	c := m.available
	slot := c.free
	n := &c.nodes[slot-1]
	c.free = n.nextFree
	*n = numericRadixNodeV1[K, V]{}
	c.used++
	m.freeSlots--
	if c.free == 0 {
		m.removeAvailableV1(c)
	}
	return numericRadixRefV1[K, V]{chunk: c, slot: slot}
}
func (m *numericRadixV1[K, V]) putAdmittedV1(key K, value V, credit *allocationCreditLeaseV1, operation *allocationOperationV1) error {
	found := m.root
	for found.chunk != nil && !found.node().leaf {
		n := found.node()
		found = n.child[m.bit(key, n.bit)]
	}
	if found.chunk != nil && found.node().key == key {
		found.node().value = value
		return nil
	}
	needed := 1
	if found.chunk != nil {
		needed = 2
	}
	if err := m.ensureSlotsV1(needed, credit, operation); err != nil {
		return err
	}
	leaf := m.allocateSlotV1()
	*leaf.node() = numericRadixNodeV1[K, V]{key: key, value: value, leaf: true}
	if found.chunk == nil {
		m.root = leaf
		m.count++
		return nil
	}
	different := uint8(m.diff(key, found.node().key))
	link := &m.root
	for !link.node().leaf && link.node().bit < different {
		n := link.node()
		link = &n.child[m.bit(key, n.bit)]
	}
	branch := m.allocateSlotV1()
	n := branch.node()
	n.bit = different
	side := m.bit(key, different)
	n.child[side], n.child[1-side] = leaf, *link
	*link = branch
	m.count++
	return nil
}

// No slot refunds credit. An empty chunk is unlinked and completely cleared
// before its creating edge is released; no historic empty capacity is retained.
func (m *numericRadixV1[K, V]) freeSlotV1(ref numericRadixRefV1[K, V]) {
	c, slot := ref.chunk, ref.slot
	wasFull := c.free == 0
	c.nodes[slot-1] = numericRadixNodeV1[K, V]{nextFree: c.free}
	c.free = slot
	c.used--
	m.freeSlots++
	if wasFull {
		m.addAvailableV1(c)
	}
	if c.used != 0 {
		return
	}
	m.removeAvailableV1(c)
	if c.prev != nil {
		c.prev.next = c.next
	} else {
		m.head = c.next
	}
	if c.next != nil {
		c.next.prev = c.prev
	}
	m.freeSlots -= numericRadixChunkSlotsV1
	creator := c.credit
	*c = numericRadixChunkV1[K, V]{}
	ref = numericRadixRefV1[K, V]{}
	c = nil
	creator.release()
}
func (m *numericRadixV1[K, V]) Delete(key K) {
	if m == nil || m.root.chunk == nil {
		return
	}
	link := &m.root
	var parent numericRadixRefV1[K, V]
	var parentLink *numericRadixRefV1[K, V]
	var side uint8
	for !link.node().leaf {
		parent, parentLink = *link, link
		n := link.node()
		side = m.bit(key, n.bit)
		link = &n.child[side]
	}
	leaf := *link
	if leaf.node().key != key {
		return
	}
	if parent.chunk == nil {
		m.root = numericRadixRefV1[K, V]{}
	} else {
		sibling := parent.node().child[1-side]
		*parentLink = sibling
		link, parentLink = nil, nil
		m.freeSlotV1(parent)
		parent = numericRadixRefV1[K, V]{}
	}
	m.count--
	m.freeSlotV1(leaf)
}
func rangeNumericRadixV1[K comparable, V comparable](ref numericRadixRefV1[K, V], visit func(K, V) bool) bool {
	if ref.chunk == nil {
		return true
	}
	n := ref.node()
	if n.leaf {
		return visit(n.key, n.value)
	}
	return rangeNumericRadixV1(n.child[0], visit) && rangeNumericRadixV1(n.child[1], visit)
}
func (m *numericRadixV1[K, V]) Range(visit func(K, V) bool) {
	if m != nil {
		rangeNumericRadixV1(m.root, visit)
	}
}

// Clone copies Patricia structure directly into packed independent chunks.
// It never reconstructs the tree through per-key searches or Range/Put.
func (m *numericRadixV1[K, V]) copyNodeV1(source numericRadixRefV1[K, V]) numericRadixRefV1[K, V] {
	if source.chunk == nil {
		return numericRadixRefV1[K, V]{}
	}
	ref := m.allocateSlotV1()
	src, dst := source.node(), ref.node()
	dst.key, dst.value, dst.bit, dst.leaf = src.key, src.value, src.bit, src.leaf
	if !src.leaf {
		dst.child[0] = m.copyNodeV1(src.child[0])
		dst.child[1] = m.copyNodeV1(src.child[1])
	}
	return ref
}
func (m *numericRadixV1[K, V]) CloneWithCredit(credit *allocationCreditLeaseV1) (*numericRadixV1[K, V], error) {
	if m == nil {
		return nil, nil
	}
	if m.count > int(^uint(0)>>1)/2 {
		return nil, ErrAllocationCertificateIncompleteV1
	}
	nodes := 0
	if m.count != 0 {
		nodes = m.count*2 - 1
	}
	chunks := (uint64(nodes) + numericRadixChunkSlotsV1 - 1) / numericRadixChunkSlotsV1
	header := allocationClassV1(uint64(unsafe.Sizeof(*m)), true)
	bytes := cowSaturatingAddV1(header, cowSaturatingMulV1(chunks, numericRadixChunkClassV1[K, V]()))
	operation, err := admitAllocationOperationV1(credit, bytes, chunks+1)
	if err != nil {
		return nil, err
	}
	defer operation.close()
	if err := reserveBirthV1(credit, header, 1, &operation); err != nil {
		return nil, err
	}
	clone := &numericRadixV1[K, V]{bit: m.bit, diff: m.diff, credit: credit}
	if err := clone.ensureSlotsV1(nodes, credit, &operation); err != nil {
		clone.Clear()
		return nil, err
	}
	clone.root = clone.copyNodeV1(m.root)
	clone.count = m.count
	return clone, nil
}
func (m *numericRadixV1[K, V]) Clone() *numericRadixV1[K, V] {
	clone, err := m.CloneWithCredit(m.credit)
	if err != nil {
		panic(err)
	}
	return clone
}
func (m *numericRadixV1[K, V]) Clear() {
	if m == nil {
		return
	}
	c := m.head
	creator := m.credit
	m.root = numericRadixRefV1[K, V]{}
	m.head, m.available, m.credit = nil, nil, nil
	m.count, m.freeSlots = 0, 0
	// First destroy all cross-chunk node and available aliases. Releasing
	// any chunk earlier could leave a child reference in another live chunk.
	for block := c; block != nil; block = block.next {
		clear(block.nodes[:])
		block.prev, block.availablePrev, block.availableNext = nil, nil, nil
	}
	for c != nil {
		next, owner := c.next, c.credit
		*c = numericRadixChunkV1[K, V]{}
		c = next
		owner.release()
	}
	creator.release()
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
	if m.credit != nil {
		bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(unsafe.Sizeof(*m.credit)), true))
	}
	for c := m.head; c != nil; c = c.next {
		bytes = cowSaturatingAddV1(bytes, numericRadixChunkClassV1[K, V]())
		if c.credit != nil {
			bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(unsafe.Sizeof(*c.credit)), true))
		}
	}
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

// Prospective DISTINCT memberships use existing free slots first. Every new
// chunk is charged at full actual capacity, regardless of eventual occupancy.
func (m *numericRadixV1[K, V]) insertionCapacityV1(newKeys uint64) (uint64, uint64) {
	if newKeys == 0 {
		return 0, 0
	}
	nodes := cowSaturatingMulV1(newKeys, 2)
	if m.count == 0 && nodes != ^uint64(0) {
		nodes--
	}
	if nodes <= uint64(m.freeSlots) {
		return 0, 0
	}
	deficit := nodes - uint64(m.freeSlots)
	chunks := deficit / numericRadixChunkSlotsV1
	if deficit%numericRadixChunkSlotsV1 != 0 {
		chunks++
	}
	return cowSaturatingMulV1(chunks, numericRadixChunkClassV1[K, V]()), chunks
}
