package cowbtree

import "testing"

type uintCursorTestWork struct{ records, bytes, recordLimit, byteLimit uint64 }

func (w *uintCursorTestWork) Reserve(records, bytes uint64) bool {
	if w.records > w.recordLimit || records > w.recordLimit-w.records || w.bytes > w.byteLimit || bytes > w.byteLimit-w.bytes {
		return false
	}
	w.records += records
	w.bytes += bytes
	return true
}
func uintCursorTestRequest(t *testing.T, c *UintCursor[int], op Operation, key uint64) (uint64, int, bool) {
	t.Helper()
	w := uintCursorTestWork{recordLimit: 1, byteLimit: 2048}
	if err := c.Begin(op, key, &w); err != nil {
		t.Fatal(err)
	}
	for rounds := 0; rounds < 10000; rounds++ {
		done, err := c.Step(&w)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			return c.Current()
		}
		if c.Ready() {
			t.Fatal("partial numeric cursor result escaped")
		}
		w = uintCursorTestWork{recordLimit: 1, byteLimit: 2048}
	}
	t.Fatal("numeric cursor made no progress")
	return 0, 0, false
}
func TestUintCursorResumableMultiLevelAndRebind(t *testing.T) {
	private := NewMap[uint64, int](32)
	for i := 0; i < 1280; i++ {
		private.Load(uint64(3*i+1), i)
	}
	tree := private.ImmutableCopy()
	w := uintCursorTestWork{recordLimit: 10, byteLimit: 1 << 20}
	c, ok := NewUintCursor(tree, 8, &w)
	if !ok {
		t.Fatal("constructor refused")
	}
	for _, pivot := range []uint64{0, 1, 2, 3, 4, 63, 64, 65, 1000, 3838, 3839, 3840, ^uint64(0)} {
		want := (pivot + 1) / 3
		if pivot == ^uint64(0) {
			want = 1280
		}
		key, value, found := uintCursorTestRequest(t, &c, SeekGE, pivot)
		if found != (want < 1280) || (found && (key != 3*want+1 || value != int(want))) {
			t.Fatal("GE", pivot, key, value, found, want)
		}
		var previous uint64
		valid := pivot >= 1
		if valid {
			previous = (pivot - 1) / 3
			if previous >= 1280 {
				previous = 1279
			}
		}
		key, value, found = uintCursorTestRequest(t, &c, SeekLE, pivot)
		if found != valid || (found && (key != 3*previous+1 || value != int(previous))) {
			t.Fatal("LE", pivot, key, value, found, previous)
		}
	}
	key, value, valid := uintCursorTestRequest(t, &c, First, 0)
	for i := 0; i < 1280; i++ {
		if !valid || key != uint64(3*i+1) || value != i {
			t.Fatal("forward", i, key, value, valid)
		}
		key, value, valid = uintCursorTestRequest(t, &c, Next, 0)
	}
	if valid {
		t.Fatal("forward escaped end")
	}
	key, value, valid = uintCursorTestRequest(t, &c, Last, 0)
	for i := 1279; i >= 0; i-- {
		if !valid || key != uint64(3*i+1) || value != i {
			t.Fatal("reverse", i, key, value, valid)
		}
		key, value, valid = uintCursorTestRequest(t, &c, Prev, 0)
	}
	if valid {
		t.Fatal("reverse escaped start")
	}
	other := NewMap[uint64, int](32)
	other.Set(7, 42)
	w = uintCursorTestWork{recordLimit: 1, byteLimit: 2048}
	if err := c.BeginAt(other.ImmutableCopy(), First, 0, &w); err != nil {
		t.Fatal(err)
	}
	for {
		w = uintCursorTestWork{recordLimit: 1, byteLimit: 2048}
		done, err := c.Step(&w)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	key, value, valid = c.Current()
	if !valid || key != 7 || value != 42 {
		t.Fatal("rebound", key, value, valid)
	}
	uintCursorTestRequest(t, &c, Close, 0)
	if !c.Closed() || c.Ready() {
		t.Fatal("close retained result")
	}
}
