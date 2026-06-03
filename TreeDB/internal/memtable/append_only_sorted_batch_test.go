package memtable

import (
	"encoding/binary"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
)

func TestAppendOnlyApplyCopySortedBatchTrusted_AppendKeepsOrderedAndCopiesKeys(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)

	keyA := []byte("a")
	keyB := []byte("b")
	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: keyA, Value: []byte("va")},
		{Type: batchpkg.OpPut, Key: keyB, Value: []byte("vb")},
	}
	if borrowed := m.ApplyCopySortedBatchTrusted(entries, true, true, nil); !borrowed {
		t.Fatalf("ApplyCopySortedBatchTrusted borrowed=false, want true for inline values")
	}

	// Keys must be table-owned even when values are borrowed from the caller.
	keyA[0] = 'z'
	if got, deleted, ok := m.Get([]byte("a")); !ok || deleted || string(got) != "va" {
		t.Fatalf("Get(a)=(%q,%v,%v), want (va,false,true)", string(got), deleted, ok)
	}
	if got, deleted, ok := m.Get([]byte("b")); !ok || deleted || string(got) != "vb" {
		t.Fatalf("Get(b)=(%q,%v,%v), want (vb,false,true)", string(got), deleted, ok)
	}

	m.mu.RLock()
	ordered := m.ordered
	latestDirty := m.latestDirty
	latestLen := len(m.latest) + len(m.latest64)
	count := m.count
	m.mu.RUnlock()
	if !ordered {
		t.Fatalf("ordered=false after trusted sorted unique append")
	}
	if latestDirty || latestLen != 0 {
		t.Fatalf("latest index built for ordered append: dirty=%v len=%d", latestDirty, latestLen)
	}
	if count != 2 {
		t.Fatalf("count=%d want 2", count)
	}
}

func TestAppendOnlyApplyCopySortedBatchTrusted_OverlappingSortedRunFallsBack(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("m"), []byte("old"))

	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("a"), Value: []byte("va")},
		{Type: batchpkg.OpPut, Key: []byte("m"), Value: []byte("new")},
	}
	m.ApplyCopySortedBatchTrusted(entries, false, true, nil)

	if got, deleted, ok := m.Get([]byte("a")); !ok || deleted || string(got) != "va" {
		t.Fatalf("Get(a)=(%q,%v,%v), want (va,false,true)", string(got), deleted, ok)
	}
	if got, deleted, ok := m.Get([]byte("m")); !ok || deleted || string(got) != "new" {
		t.Fatalf("Get(m)=(%q,%v,%v), want (new,false,true)", string(got), deleted, ok)
	}

	m.mu.RLock()
	ordered := m.ordered
	latestDirty := m.latestDirty
	m.mu.RUnlock()
	if ordered {
		t.Fatalf("ordered=true after sorted run overlapped existing max key")
	}
	if latestDirty {
		t.Fatalf("latestDirty=true after fallback; latest index should be immediately usable")
	}
}

func TestAppendOnlyKey64MonotonicOrderingMatchesLexicographic(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	var lo, hi [8]byte
	binary.BigEndian.PutUint64(lo[:], 0x00ff)
	binary.BigEndian.PutUint64(hi[:], 0x0100)

	m.Set(lo[:], []byte("lo"))
	m.Set(hi[:], []byte("hi"))

	m.mu.RLock()
	ordered := m.ordered
	latestLen := len(m.latest) + len(m.latest64)
	m.mu.RUnlock()
	if !ordered {
		t.Fatalf("ordered=false for lexicographically increasing 8-byte keys")
	}
	if latestLen != 0 {
		t.Fatalf("latest index len=%d want 0 while key64 run remains ordered", latestLen)
	}

	m.Set(lo[:], []byte("lo2"))
	if got, deleted, ok := m.Get(lo[:]); !ok || deleted || string(got) != "lo2" {
		t.Fatalf("Get(lo)=(%q,%v,%v), want (lo2,false,true)", string(got), deleted, ok)
	}
	m.mu.RLock()
	ordered = m.ordered
	latestDirty := m.latestDirty
	_, latest64OK := m.latest64[binary.BigEndian.Uint64(lo[:])]
	m.mu.RUnlock()
	if ordered {
		t.Fatalf("ordered=true after lower 8-byte key append")
	}
	if latestDirty || !latest64OK {
		t.Fatalf("latest64 not ready after key64 order break: dirty=%v ok=%v", latestDirty, latest64OK)
	}
}
