package cowbtree

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/page"
)

type mutationWorkBudget struct{ records, bytes uint64 }

func (w *mutationWorkBudget) Reserve(records, bytes uint64) bool {
	if records > w.records || bytes > w.bytes {
		return false
	}
	w.records -= records
	w.bytes -= bytes
	return true
}

type mutationWorkTrace struct {
	calls, failAt  int
	records, bytes uint64
	reject         func(uint64, uint64) bool
}

func (w *mutationWorkTrace) Reserve(records, bytes uint64) bool {
	w.calls++
	if w.calls == w.failAt || (w.reject != nil && w.reject(records, bytes)) {
		return false
	}
	w.records += records
	w.bytes += bytes
	return true
}

func mutationEntries[V any](m *Map[string, V]) map[string]V {
	entries := make(map[string]V)
	m.Scan(func(k string, v V) bool { entries[k] = v; return true })
	return entries
}

func checkMutationCounts[K ordered, V any](t *testing.T, m *Map[K, V]) {
	t.Helper()
	var walk func(*mapNode[K, V]) int
	walk = func(n *mapNode[K, V]) int {
		count := len(n.items)
		if !n.leaf() {
			if len(*n.children) != len(n.items)+1 {
				t.Fatalf("bad child geometry: %d items %d children", len(n.items), len(*n.children))
			}
			for _, child := range *n.children {
				count += walk(child)
			}
		}
		if n.count != count {
			t.Fatalf("node count=%d actual=%d", n.count, count)
		}
		return count
	}
	actual := 0
	if m.root != nil {
		actual = walk(m.root)
	}
	if m.Len() != actual {
		t.Fatalf("map count=%d actual=%d", m.Len(), actual)
	}
}

func TestSetWithWorkRefusalLeavesEntries(t *testing.T) {
	m := NewMap[string, int](2)
	m.Set("a", 1)
	old, replaced, ready, err := m.SetWithWork("b", 2, &mutationWorkBudget{})
	if err != nil || ready || replaced || old != 0 {
		t.Fatalf("refusal: old=%d replaced=%v ready=%v err=%v", old, replaced, ready, err)
	}
	if !reflect.DeepEqual(mutationEntries(m), map[string]int{"a": 1}) {
		t.Fatalf("refusal changed entries: %v", mutationEntries(m))
	}
	checkMutationCounts(t, m)
	// Refusal owns no pending target: an ordinary write may immediately proceed.
	m.Set("c", 3)
	if _, ok := m.Get("b"); ok {
		t.Fatal("refused target became visible after another write")
	}
	if got, ok := m.Get("c"); !ok || got != 3 {
		t.Fatal("ordinary write after refusal failed")
	}
}

func TestSetWithWorkEveryAdmissionRefusal(t *testing.T) {
	scenarios := []struct {
		name  string
		count int
		key   string
		fork  bool
	}{
		{"empty", 0, "k000", false}, {"replace", 8, "k003", false},
		{"suffix", 2, "j000", false}, {"root-split", 3, "k003", false},
		{"nested-split", 24, "j000", false}, {"fork-replace", 30, "k014", true},
		{"fork-insert", 30, "j000", true},
	}
	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			build := func() (*Map[string, int], *Map[string, int]) {
				m := NewMap[string, int](2)
				for i := 0; i < s.count; i++ {
					m.Set(fmt.Sprintf("k%03d", i), i+1)
				}
				if s.fork {
					return m, m.Copy()
				}
				return m, nil
			}
			successful, _ := build()
			trace := new(mutationWorkTrace)
			previous, replaced, ready, err := successful.SetWithWork(s.key, 900, trace)
			if err != nil || !ready {
				t.Fatalf("unlimited work failed: %v %v", ready, err)
			}
			old, existed := mutationEntriesBefore(s.count)[s.key]
			if replaced != existed || previous != old {
				t.Fatalf("previous=%d replaced=%v want %d %v", previous, replaced, old, existed)
			}
			checkMutationCounts(t, successful)
			for fail := 1; fail <= trace.calls; fail++ {
				m, fork := build()
				before := mutationEntries(m)
				w := &mutationWorkTrace{failAt: fail}
				_, _, done, err := m.SetWithWork(s.key, 900, w)
				if err != nil || done {
					t.Fatalf("call %d refusal returned %v %v", fail, done, err)
				}
				if !reflect.DeepEqual(mutationEntries(m), before) {
					t.Fatalf("call %d refusal changed logical entries", fail)
				}
				checkMutationCounts(t, m)
				if fork != nil && !reflect.DeepEqual(mutationEntries(fork), before) {
					t.Fatalf("call %d changed fork", fail)
				}
				// Retry must complete from whatever private structure was left, and the
				// retained snapshot must still expose precisely its original values.
				if _, _, done, err = m.SetWithWork(s.key, 900, new(mutationWorkTrace)); err != nil || !done {
					t.Fatalf("retry after call %d: %v %v", fail, done, err)
				}
				checkMutationCounts(t, m)
				if got, ok := m.Get(s.key); !ok || got != 900 {
					t.Fatalf("retry target absent after call %d", fail)
				}
				if fork != nil && !reflect.DeepEqual(mutationEntries(fork), before) {
					t.Fatalf("retry call %d changed fork", fail)
				}
			}
		})
	}
}
func mutationEntriesBefore(count int) map[string]int {
	m := make(map[string]int)
	for i := 0; i < count; i++ {
		m[fmt.Sprintf("k%03d", i)] = i + 1
	}
	return m
}

func TestSetWithWorkActualEngineDifferential(t *testing.T) {
	ordinary := NewMap[string, int](2)
	bounded := NewMap[string, int](2)
	var retained *Map[string, int]
	var before map[string]int
	for i := 0; i < 300; i++ {
		if i == 90 {
			retained = bounded.Copy()
			before = mutationEntries(retained)
		}
		key := fmt.Sprintf("%04d", (i*67)%173)
		wantPrevious, wantReplaced := ordinary.Set(key, i+1)
		previous, replaced, ready, err := bounded.SetWithWork(key, i+1, new(mutationWorkTrace))
		if err != nil || !ready || previous != wantPrevious || replaced != wantReplaced {
			t.Fatalf("step %d got %d %v %v %v", i, previous, replaced, ready, err)
		}
		checkMutationCounts(t, bounded)
		if !reflect.DeepEqual(mutationEntries(ordinary), mutationEntries(bounded)) {
			t.Fatalf("step %d differs", i)
		}
	}
	if !reflect.DeepEqual(mutationEntries(retained), before) {
		t.Fatal("retained fork changed")
	}
}

func TestSetWithWorkChargesStringBytesAndCapacity(t *testing.T) {
	type namedString string
	m := NewMap[namedString, int](2)
	key := namedString(strings.Repeat("x", 4096))
	m.Set(key, 1)
	// Metadata fits; repeated equal-key comparisons cannot bypass byte admission.
	budget := &mutationWorkBudget{records: 1000, bytes: 512}
	if _, _, ready, err := m.SetWithWork(key, 2, budget); ready || err != nil {
		t.Fatalf("long key escaped bytes: %v %v", ready, err)
	}
	if got, _ := m.Get(key); got != 1 {
		t.Fatal("long-key refusal published value")
	}

	shared := NewMap[string, int](8)
	for i := 0; i < 9; i++ {
		shared.Set(fmt.Sprint(i), i)
	}
	fork := shared.Copy()
	originalRoot := shared.root
	capacity := min(len(originalRoot.items)+1, cap(originalRoot.items))
	pairBytes := uint64(unsafe.Sizeof(mapPair[string, int]{}))
	rejected := false
	work := &mutationWorkTrace{reject: func(records, bytes uint64) bool {
		if records == uint64(capacity) && bytes == uint64(capacity)*pairBytes {
			rejected = true
			return true
		}
		return false
	}}
	if _, _, ready, err := shared.SetWithWork("5", 55, work); ready || err != nil || !rejected {
		t.Fatalf("allocated capacity refusal: ready=%v err=%v rejected=%v cap=%d", ready, err, rejected, capacity)
	}
	if shared.root != originalRoot {
		t.Fatal("copied node published before backing admission")
	}
	if got, _ := fork.Get("5"); got != 5 {
		t.Fatal("capacity refusal changed fork")
	}
}

type mutationCopyValue struct {
	value int
	calls *int
}

func (v mutationCopyValue) Copy() mutationCopyValue {
	(*v.calls)++
	return mutationCopyValue{v.value, v.calls}
}
func TestSetWithWorkRefusesOpaqueCopyBeforeCallback(t *testing.T) {
	calls := 0
	m := NewMap[string, mutationCopyValue](2)
	m.Set("a", mutationCopyValue{1, &calls})
	fork := m.Copy()
	if _, _, ready, err := m.SetWithWork("a", mutationCopyValue{2, &calls}, new(mutationWorkTrace)); ready || err != nil {
		t.Fatalf("opaque callback: %v %v", ready, err)
	}
	if calls != 0 {
		t.Fatal("opaque callback ran without a bound")
	}
	if got, _ := m.Get("a"); got.value != 1 {
		t.Fatal("opaque-copy refusal changed value")
	}
	m.Set("a", mutationCopyValue{3, &calls})
	if calls != 1 {
		t.Fatalf("nil-policy Copy callback count=%d", calls)
	}
	if got, _ := fork.Get("a"); got.value != 1 {
		t.Fatal("ordinary shared engine changed fork")
	}
}

// Match the existing memtable.cowValue field types/order for mutation byte
// receipts without importing its owner package or creating another map engine.
type mutationReceiptValue struct {
	value    string
	ptr      page.ValuePtr
	revision page.EntryRevision
	flags    byte
}

// These are mutation-engine receipts for concrete geometries, not a public
// prune/transaction acceptance claim or a promise that every node fits Q32.
func TestSetWithWorkGeometryReceipts(t *testing.T) {
	cases := []struct {
		name  string
		count int
		key   string
		fork  bool
	}{
		{"default-replace-small", 1, "k000", false},
		{"default-insert-small", 1, "k001", false},
		{"default-fork-replace-1", 1, "k000", true},
		{"default-fork-replace-2", 2, "k000", true},
		{"default-fork-replace-4", 4, "k000", true},
		{"default-root-split", 63, "k063", false},
		{"default-fork-replace-full", 63, "k031", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			build := func() *Map[string, mutationReceiptValue] {
				m := NewMap[string, mutationReceiptValue](0)
				for i := 0; i < c.count; i++ {
					m.Set(fmt.Sprintf("k%03d", i), mutationReceiptValue{value: "old"})
				}
				if c.fork {
					_ = m.Copy()
				}
				return m
			}
			target := mutationReceiptValue{flags: 1}
			m := build()
			trace := new(mutationWorkTrace)
			if _, _, ready, err := m.SetWithWork(c.key, target, trace); !ready || err != nil {
				t.Fatalf("funded: %v %v", ready, err)
			}
			checkMutationCounts(t, m)
			literal := build()
			before := mutationEntries(literal)
			const bytes = uint64(1 << 20)
			budget := &mutationWorkBudget{records: 32, bytes: bytes}
			_, _, ready, err := literal.SetWithWork(c.key, target, budget)
			if err != nil {
				t.Fatal(err)
			}
			if !ready && !reflect.DeepEqual(mutationEntries(literal), before) {
				t.Fatal("literal Q32 refusal changed entries")
			}
			if ready {
				if got, ok := literal.Get(c.key); !ok || got != target {
					t.Fatal("literal success omitted target")
				}
			}
			checkMutationCounts(t, literal)
			t.Logf("value_bytes=%d pair_bytes=%d key_bytes=4; funded records=%d bytes=%d calls=%d; literal records_cap=32 bytes_cap=%d ready=%v charged_records=%d charged_bytes=%d", unsafe.Sizeof(target), unsafe.Sizeof(mapPair[string, mutationReceiptValue]{}), trace.records, trace.bytes, trace.calls, bytes, ready, 32-budget.records, bytes-budget.bytes)
		})
	}
}
