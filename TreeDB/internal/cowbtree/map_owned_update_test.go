package cowbtree

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func ownedUpdateMustPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("operation did not panic")
		}
	}()
	f()
}

func TestMapSealOwnedConsumesHeaderAndPreservesReceipt(t *testing.T) {
	m := NewMap[int, int](2)
	for i := range 128 {
		m.Set(i, i*3)
	}
	opening := m.ImmutableCopy()
	m.Set(17, -17)
	root, isoid, charge, count := m.root, m.isoid, m.AllocationCharge(), m.Len()
	if charge == 0 {
		t.Fatal("fixture did not own any node allocations")
	}
	ids := atomic.LoadUint64(&gisoid)
	sealed := m.SealOwned()
	if sealed != m || sealed.root != root || sealed.isoid != isoid || sealed.Len() != count || !sealed.sealed {
		t.Fatal("consuming seal changed header identity or tree state")
	}
	if sealed.AllocationCharge() != charge || atomic.LoadUint64(&gisoid) != ids {
		t.Fatal("consuming seal reset the receipt or rotated isolation identities")
	}
	if value, ok := opening.Get(17); !ok || value != 51 {
		t.Fatal("sealing private postimage changed the opening snapshot")
	}
	if value, ok := sealed.Get(17); !ok || value != -17 {
		t.Fatal("sealed postimage lost its update")
	}
	for name, mutation := range map[string]func(){
		"set":            func() { sealed.Set(17, 0) },
		"delete":         func() { sealed.Delete(17) },
		"clear":          func() { sealed.Clear() },
		"copy":           func() { sealed.Copy() },
		"immutable_copy": func() { sealed.ImmutableCopy() },
		"seal_again":     func() { sealed.SealOwned() },
		"update_hit":     func() { sealed.UpdateExisting(17, nil) },
		"update_miss":    func() { sealed.UpdateExisting(-1, nil) },
	} {
		t.Run(name, func(t *testing.T) { ownedUpdateMustPanic(t, mutation) })
	}
	private := sealed.ForkImmutable()
	if private.sealed || private.isoid == sealed.isoid || private.AllocationCharge() != 0 {
		t.Fatal("fork did not start an independent private allocation receipt")
	}
	previous, found := private.UpdateExisting(17, func(key, value int) (int, int) { return key, value - 1 })
	if !found || previous != -17 || private.AllocationCharge() == 0 {
		t.Fatal("private fork did not isolate its update")
	}
	if value, ok := sealed.Get(17); !ok || value != -17 || sealed.AllocationCharge() != charge {
		t.Fatal("fork update changed the consumed immutable header")
	}
	checkMutationCounts(t, opening)
	checkMutationCounts(t, sealed)
	checkMutationCounts(t, private)
}

func ownedUpdatePathCharge(m *Map[int, int], key int) uint64 {
	var charge uint64
	for n := m.root; n != nil; {
		if n.leaf() {
			charge += retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(*n)))
		} else {
			charge += retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(mapInternalNode[int, int]{})))
		}
		charge += retainedalloc.AllocationCharge(uint64(min(len(n.items)+1, cap(n.items))) * uint64(unsafe.Sizeof(mapPair[int, int]{})))
		i, found := m.search(n, key)
		if !n.leaf() {
			charge += retainedalloc.AllocationCharge(uint64(min(len(*n.children)+1, m.max+1)) * uint64(unsafe.Sizeof((*mapNode[int, int])(nil))))
		}
		if found || n.leaf() {
			return charge
		}
		n = (*n.children)[i]
	}
	return 0
}

func TestMapUpdateExistingIsolatesOnlyFoundPathBeforeCallback(t *testing.T) {
	for _, degree := range []int{2, 8, 32} {
		t.Run(fmt.Sprintf("degree_%d", degree), func(t *testing.T) {
			m := NewMap[int, int](degree)
			for i := range 4096 {
				m.Set(i, i*3)
			}
			opening := m.SealOwned()
			if opening.Height() < 3 {
				t.Fatal("fixture does not exercise a multi-level copy path")
			}
			targets := []int{opening.root.items[0].key, (*opening.root.children)[0].items[0].key}
			leaf := opening.root
			for !leaf.leaf() {
				leaf = (*leaf.children)[len(*leaf.children)-1]
			}
			targets = append(targets, leaf.items[len(leaf.items)-1].key)
			for _, key := range targets {
				private := opening.ForkImmutable()
				calls := 0
				previous, found := private.UpdateExisting(key, func(oldKey, oldValue int) (int, int) {
					calls++
					if oldKey != key || oldValue != key*3 {
						t.Fatalf("callback pair=(%d,%d) want=(%d,%d)", oldKey, oldValue, key, key*3)
					}
					for n := private.root; ; {
						if n.isoid != private.isoid {
							t.Fatal("callback ran before the complete found path was private")
						}
						i, hit := private.search(n, key)
						if hit {
							break
						}
						n = (*n.children)[i]
					}
					return oldKey, -oldValue - 1
				})
				if !found || previous != key*3 || calls != 1 {
					t.Fatalf("result previous=%d found=%v callback calls=%d", previous, found, calls)
				}
				if got, want := private.AllocationCharge(), ownedUpdatePathCharge(opening, key); got != want {
					t.Fatalf("path allocation receipt=%d want=%d", got, want)
				}
				if private.Len() != opening.Len() || private.Height() != opening.Height() {
					t.Fatal("replacement changed count or tree height")
				}
				if value, ok := opening.Get(key); !ok || value != key*3 {
					t.Fatal("replacement changed the immutable opening snapshot")
				}
				if value, ok := private.Get(key); !ok || value != -key*3-1 {
					t.Fatal("replacement value missing from private postimage")
				}
				charge, root := private.AllocationCharge(), private.root
				previous, found = private.UpdateExisting(key, func(k, v int) (int, int) { calls++; return k, v - 1 })
				if !found || previous != -key*3-1 || calls != 2 || private.AllocationCharge() != charge || private.root != root {
					t.Fatal("already private replacement recopied nodes or retried callback")
				}
				last, seen := -1, 0
				private.Scan(func(k, _ int) bool {
					if k <= last {
						t.Fatal("replacement changed key ordering")
					}
					last, seen = k, seen+1
					return true
				})
				if seen != opening.Len() {
					t.Fatal("replacement changed scan membership")
				}
				checkMutationCounts(t, private)
				checkMutationCounts(t, opening)
			}
		})
	}
}

func TestMapUpdateExistingMissDoesNotCopyOrInvoke(t *testing.T) {
	m := NewMap[int, int](2)
	for i := range 1024 {
		m.Set(i*2, i)
	}
	opening := m.SealOwned()
	private := opening.ForkImmutable()
	root, isoid, count := private.root, private.isoid, private.Len()
	for _, key := range []int{-1, 1, 511, 2048} {
		previous, found := private.UpdateExisting(key, func(int, int) (int, int) {
			t.Fatal("missing key invoked callback")
			return 0, 0
		})
		if found || previous != 0 || private.root != root || private.isoid != isoid || private.Len() != count || private.AllocationCharge() != 0 {
			t.Fatal("missing key changed private state or allocation receipt")
		}
	}
	if allocations := testing.AllocsPerRun(100, func() { private.UpdateExisting(511, nil) }); allocations != 0 {
		t.Fatalf("missing key allocated %v objects", allocations)
	}
	empty := NewMap[int, int](2)
	if previous, found := empty.UpdateExisting(1, nil); found || previous != 0 || empty.root != nil || empty.AllocationCharge() != 0 {
		t.Fatal("empty map update had an effect")
	}
}

func TestMapUpdateExistingAdoptsWholeReplacementKeyBacking(t *testing.T) {
	m := NewMap[string, string](2)
	key := strings.Clone("logical-key-with-history")
	m.Set(key, "old")
	opening := m.SealOwned()
	private := opening.ForkImmutable()
	backing := strings.Clone(key + "/new-immutable-payload")
	replacement := backing[:len(key)]
	if unsafe.StringData(replacement) == unsafe.StringData(key) {
		t.Fatal("fixture did not create a distinct equal-key backing")
	}
	previous, found := private.UpdateExisting(strings.Clone(key), func(oldKey, oldValue string) (string, string) {
		if unsafe.StringData(oldKey) != unsafe.StringData(key) || oldValue != "old" {
			t.Fatal("callback did not receive the stored old pair")
		}
		return replacement, backing[len(key):]
	})
	if !found || previous != "old" {
		t.Fatal("equal-key update did not replace the existing pair")
	}
	private.Scan(func(k, v string) bool {
		if unsafe.StringData(k) != unsafe.StringData(replacement) || v != "/new-immutable-payload" {
			t.Fatal("update retained the old key header or lost the replacement value")
		}
		return true
	})
	opening.Scan(func(k, v string) bool {
		if unsafe.StringData(k) != unsafe.StringData(key) || v != "old" {
			t.Fatal("replacement changed the old snapshot key/value backing")
		}
		return true
	})
}

func TestMapUpdateExistingRejectsDifferentKeyBeforeLogicalChange(t *testing.T) {
	m := NewMap[int, int](2)
	for i := range 128 {
		m.Set(i, i*3)
	}
	opening := m.SealOwned()
	private := opening.ForkImmutable()
	calls := 0
	ownedUpdateMustPanic(t, func() {
		private.UpdateExisting(17, func(k, v int) (int, int) { calls++; return k + 1, -v })
	})
	if calls != 1 || private.Len() != opening.Len() || private.Height() != opening.Height() {
		t.Fatal("invalid replacement retried callback or changed tree geometry")
	}
	private.Scan(func(k, v int) bool {
		if v != k*3 {
			t.Fatal("invalid replacement changed a logical pair")
		}
		return true
	})
	if private.AllocationCharge() != ownedUpdatePathCharge(opening, 17) {
		t.Fatal("invalid replacement lost its actual private path-copy receipt")
	}
	previous, found := private.UpdateExisting(17, func(k, v int) (int, int) { return k, -v })
	if !found || previous != 51 {
		t.Fatal("valid update after invalid replacement did not work")
	}
	checkMutationCounts(t, private)
	checkMutationCounts(t, opening)
}
