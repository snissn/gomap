package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// registryTable is the backing of the existing physical-identity authority.
// All accesses occur under IdentityPinRegistry.mu. Rehash reserves the entire
// new backing before allocation; the old backing is cleared before its refund.
type registryCell[K comparable, V any] struct {
	key     K
	value   V
	status  uint8
	payload uint64
}
type registryTable[K comparable, V any] struct {
	cells       []registryCell[K, V]
	count, used int
	charge      uint64
	hash        func(K) uint64
	owner       *retainedalloc.Owner
	keyCharge   func(K) uint64
	cloneKey    func(K) K
}

func (t *registryTable[K, V]) lookup(key K) (V, bool) {
	var zero V
	if len(t.cells) == 0 {
		return zero, false
	}
	mask := len(t.cells) - 1
	for i := int(t.hash(key)) & mask; ; i = (i + 1) & mask {
		c := &t.cells[i]
		if c.status == 0 {
			return zero, false
		}
		if c.status == 1 && c.key == key {
			return c.value, true
		}
	}
}
func (t *registryTable[K, V]) get(key K) V { v, _ := t.lookup(key); return v }
func (t *registryTable[K, V]) insert(key K, value V, payload uint64) {
	mask := len(t.cells) - 1
	tomb := -1
	for i := int(t.hash(key)) & mask; ; i = (i + 1) & mask {
		c := &t.cells[i]
		if c.status == 1 && c.key == key {
			c.value = value
			return
		}
		if c.status == 2 && tomb < 0 {
			tomb = i
		}
		if c.status == 0 {
			if tomb >= 0 {
				i = tomb
			} else {
				t.used++
			}
			t.cells[i] = registryCell[K, V]{key: key, value: value, status: 1, payload: payload}
			t.count++
			return
		}
	}
}
func (t *registryTable[K, V]) set(key K, value V) error {
	if _, ok := t.lookup(key); ok {
		t.insert(key, value, 0)
		return nil
	}
	if len(t.cells) == 0 || (t.used+1)*2 >= len(t.cells) {
		capacity := len(t.cells)
		// Tombstones require rehashing, not growth when the live load is low.
		// This preserves a finite high-water envelope for repeated identities.
		if (t.count+1)*2 >= capacity {
			capacity *= 2
		}
		if capacity == 0 {
			capacity = 16
		}
		charge := retainedalloc.AllocationCharge(uint64(capacity) * uint64(unsafe.Sizeof(registryCell[K, V]{})))
		if err := t.owner.Add(charge); err != nil {
			return err
		}
		old, oldCharge := t.cells, t.charge
		t.cells = make([]registryCell[K, V], capacity)
		t.used = 0
		t.count = 0
		t.charge = charge
		for i := range old {
			if old[i].status == 1 {
				t.insert(old[i].key, old[i].value, old[i].payload)
			}
		}
		clear(old)
		old = nil
		t.owner.Remove(oldCharge)
	}
	payload := uint64(0)
	if t.keyCharge != nil {
		payload = t.keyCharge(key)
	}
	if err := t.owner.Add(payload); err != nil {
		return err
	}
	if t.cloneKey != nil {
		key = t.cloneKey(key)
	}
	t.insert(key, value, payload)
	return nil
}
func (t *registryTable[K, V]) remove(key K) {
	if len(t.cells) == 0 {
		return
	}
	mask := len(t.cells) - 1
	for i := int(t.hash(key)) & mask; ; i = (i + 1) & mask {
		c := &t.cells[i]
		if c.status == 0 {
			return
		}
		if c.status == 1 && c.key == key {
			payload := c.payload
			*c = registryCell[K, V]{status: 2}
			t.count--
			t.owner.Remove(payload)
			return
		}
	}
}
func (t *registryTable[K, V]) all(yield func(K, V) bool) {
	for i := range t.cells {
		c := &t.cells[i]
		if c.status == 1 && !yield(c.key, c.value) {
			return
		}
	}
}
func (t *registryTable[K, V]) clear() {
	payload := uint64(0)
	for i := range t.cells {
		payload += t.cells[i].payload
	}
	clear(t.cells)
	t.count = 0
	t.used = 0
	t.owner.Remove(payload)
}
func (t *registryTable[K, V]) close() {
	t.clear()
	t.cells = nil
	t.count = 0
	t.used = 0
	t.owner.Remove(t.charge)
	t.charge = 0
}
func registryStringHash(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h = (h ^ uint64(s[i])) * 1099511628211
	}
	return h
}
func registryIdentityHash(id StableIdentity) uint64 {
	h := id.VolumeID ^ registryStringHash(id.Platform)
	for _, v := range id.ObjectID {
		h = (h ^ uint64(v)) * 1099511628211
	}
	return h
}
func registryLinkHash(link stableNamespaceLink) uint64 {
	return registryIdentityHash(link.parent) ^ registryIdentityHash(link.child)*1099511628211 ^ registryStringHash(link.name)
}
