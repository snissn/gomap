package cowbtree

import (
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	upstream "github.com/tidwall/btree"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func testCursor(t *testing.T, m *Map[string, int]) StringCursor[int] {
	t.Helper()
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	c, ok := NewStringCursor(m, 64, &w)
	if !ok {
		t.Fatal("constructor admission")
	}
	return c
}
func runCursor(t *testing.T, c *StringCursor[int], op Operation, key string) (string, int, bool, int) {
	t.Helper()
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if err := c.Begin(op, key, &w); err != nil {
		t.Fatal(err)
	}
	rounds := 1
	for {
		done, err := c.Step(&w)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
		if _, _, valid := c.Current(); valid {
			t.Fatal("published partial result")
		}
		rounds++
		if rounds > 10000 {
			t.Fatal("no progress")
		}
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	}
	k, v, ok := c.Current()
	return k, v, ok, rounds
}
func TestMapUpstreamMutationAndCOW(t *testing.T) {
	a := NewMap[string, int](32)
	b := upstream.NewMap[string, int](32)
	rng := rand.New(rand.NewSource(4878))
	var oldA *Map[string, int]
	var oldB *upstream.Map[string, int]
	for i := 0; i < 8000; i++ {
		key := fmt.Sprintf("%05d", rng.Intn(3000))
		if i%7 == 0 {
			av, ao := a.Delete(key)
			bv, bo := b.Delete(key)
			if av != bv || ao != bo {
				t.Fatal("delete mismatch", i)
			}
		} else {
			av, ao := a.Set(key, i)
			bv, bo := b.Set(key, i)
			if av != bv || ao != bo {
				t.Fatal("set mismatch", i)
			}
		}
		if i == 4000 {
			oldA = a.Copy()
			oldB = b.Copy()
		}
		if i%251 == 0 {
			if a.Len() != b.Len() || a.Height() != b.Height() || !reflect.DeepEqual(a.Keys(), b.Keys()) || !reflect.DeepEqual(a.Values(), b.Values()) {
				t.Fatal("mutation mismatch", i)
			}
		}
	}
	if !reflect.DeepEqual(oldA.Keys(), oldB.Keys()) || !reflect.DeepEqual(oldA.Values(), oldB.Values()) {
		t.Fatal("old COW root changed")
	}
}
func TestCursorDifferentialOrderingAndSeek(t *testing.T) {
	for _, n := range []int{0, 1, 63, 64, 4096} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			m := NewMap[string, int](32)
			ref := upstream.NewMap[string, int](32)
			for i := 0; i < n; i++ {
				k := fmt.Sprintf("%08d", i*2)
				m.Set(k, i)
				ref.Set(k, i)
			}
			c := testCursor(t, m)
			it := ref.Iter()
			expected := it.First()
			k, v, ok, _ := runCursor(t, &c, First, "")
			for {
				if ok != expected || (ok && (k != it.Key() || v != it.Value())) {
					t.Fatal("forward mismatch", k, v, ok, expected)
				}
				if !ok {
					break
				}
				expected = it.Next()
				k, v, ok, _ = runCursor(t, &c, Next, "")
			}
			// Reverse from the exhausted forward cursor must find the last item.
			expected = it.Last()
			k, v, ok, _ = runCursor(t, &c, Prev, "")
			for {
				if ok != expected || (ok && (k != it.Key() || v != it.Value())) {
					t.Fatal("reverse mismatch", k, v, ok, expected)
				}
				if !ok {
					break
				}
				expected = it.Prev()
				k, v, ok, _ = runCursor(t, &c, Prev, "")
			}
			for _, q := range []string{"", "00000000", "00000001", "00000126", "00009999", "z"} {
				expected = it.Seek(q)
				k, v, ok, _ = runCursor(t, &c, SeekGE, q)
				if ok != expected || (ok && (k != it.Key() || v != it.Value())) {
					t.Fatal("GE mismatch", q, k, v, ok, expected)
				}
				// Upstream has no SeekLE: derive the expected floor from its ordered map.
				ek, ev, eok := "", 0, false
				ref.Ascend("", func(key string, value int) bool {
					if key > q {
						return false
					}
					ek, ev, eok = key, value, true
					return true
				})
				k, v, ok, _ = runCursor(t, &c, SeekLE, q)
				if ok != eok || (ok && (k != ek || v != ev)) {
					t.Fatal("LE mismatch", q, k, v, ok, ek, ev, eok)
				}
			}
		})
	}
}
func TestCursorPartialLongKeyAndExactStop(t *testing.T) {
	prefix := strings.Repeat("x", 3<<20)
	m := NewMap[string, int](32)
	m.Set(prefix+"a", 1)
	m.Set(prefix+"c", 3)
	c := testCursor(t, m)
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if err := c.Begin(SeekGE, prefix+"b", &w); err != nil {
		t.Fatal(err)
	}
	done, err := c.Step(&w)
	if err != nil || done || c.phase != comparePhase || c.offset == 0 {
		t.Fatal("comparison did not pause", done, err, c.phase, c.offset)
	}
	before := c.offset
	empty := iterator.OrdinalScanWork{}
	if done, err = c.Step(&empty); done || err != nil || c.offset != before {
		t.Fatal("zero budget moved comparison")
	}
	rounds := 1
	for !done {
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		done, err = c.Step(&w)
		if err != nil {
			t.Fatal(err)
		}
		rounds++
		if rounds > 30 {
			t.Fatal("restarted long comparison")
		}
	}
	key, value, valid := c.Current()
	if !valid || key != prefix+"c" || value != 3 || rounds < 6 {
		t.Fatal("long seek", valid, value, rounds)
	}
	// Publishing a record ends the operation even when work remains.
	if c.phase != 0 || c.node != nil || c.Pending() {
		t.Fatal("advanced after publication")
	}
	k, v, ok, _ := runCursor(t, &c, Prev, "")
	if !ok || k != prefix+"a" || v != 1 {
		t.Fatal("retained stack did not move backward")
	}
}
func TestCursorRefusalCancelAndClose(t *testing.T) {
	m := NewMap[string, int](32)
	for i := 0; i < 1024; i++ {
		m.Set(fmt.Sprint(i), i)
	}
	c := testCursor(t, m)
	empty := iterator.OrdinalScanWork{}
	if err := c.Begin(First, "", &empty); err != ErrWork || c.Pending() {
		t.Fatal("start refusal changed state", err)
	}
	w := iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
	if err := c.Begin(First, "", &w); err != nil {
		t.Fatal(err)
	}
	if done, err := c.Step(&w); done || err != nil || len(c.stack) != 0 {
		t.Fatal("advanced after last record", done, err)
	}
	if err := c.Begin(Last, "", &w); err != ErrPending {
		t.Fatal("pending operation replaced", err)
	}
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if err := c.Begin(Close, "", &w); err != nil {
		t.Fatal(err)
	}
	if done, err := c.Step(&w); !done || err != nil || !c.Closed() {
		t.Fatal("cancel-close", done, err)
	}
	if err := c.Begin(First, "", &w); err != ErrClosed {
		t.Fatal("closed cursor reused")
	}
}

func TestCursorOneRecordQuantumProgress(t *testing.T) {
	m := NewMap[string, int](32)
	for i := 0; i < 4096; i++ {
		m.Set(fmt.Sprintf("%08d", i*2), i)
	}
	c := testCursor(t, m)
	for _, op := range []Operation{SeekGE, SeekLE, First, Next, Last, Prev, Close} {
		w := iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
		if err := c.Begin(op, "00004101", &w); err != nil {
			t.Fatal(err)
		}
		rounds := 0
		for {
			done, err := c.Step(&w)
			if err != nil {
				t.Fatal(err)
			}
			if w.Records > w.RecordLimit || w.Bytes > w.ByteLimit {
				t.Fatal("overcharged allowance")
			}
			if done {
				break
			}
			rounds++
			if rounds > 200 {
				t.Fatal("no one-record progress", op)
			}
			w = iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
		}
	}
}
