package collections

import (
	"bytes"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestFreezeSortRunTableLatestSortedAndTombstoneSemantics(t *testing.T) {
	table := newFreezeSortRunTable()
	table.Set([]byte("b"), []byte("old-b"))
	table.Set([]byte("a"), []byte("value-a"))
	table.Set([]byte("b"), []byte("new-b"))
	table.Delete([]byte("c"))

	if got, _, flags, ok := table.GetEntry([]byte("b")); !ok || flags&node.FlagTombstone != 0 || !bytes.Equal(got, []byte("new-b")) {
		t.Fatalf("mutable GetEntry b=(%q,%02x,%v), want latest new-b", got, flags, ok)
	}
	if _, _, flags, ok := table.GetEntry([]byte("c")); !ok || flags&node.FlagTombstone == 0 {
		t.Fatalf("mutable GetEntry c flags=%02x ok=%v, want tombstone", flags, ok)
	}

	requireFreezeSortRunIterator(t, table.NewIterator(nil, nil), []string{"a", "b", "c"})
	requireFreezeSortRunReverseIterator(t, table.NewReverseIterator(nil, nil), []string{"c", "b", "a"})

	table.Freeze()
	if table.Len() != 3 {
		t.Fatalf("frozen Len=%d want 3 coalesced entries", table.Len())
	}
	if got, _, flags, ok := table.GetEntry([]byte("b")); !ok || flags&node.FlagTombstone != 0 || !bytes.Equal(got, []byte("new-b")) {
		t.Fatalf("frozen GetEntry b=(%q,%02x,%v), want latest new-b", got, flags, ok)
	}
	requireFreezeSortRunIterator(t, table.NewIterator([]byte("b"), nil), []string{"b", "c"})
	requireFreezeSortRunIterator(t, table.NewIterator(nil, []byte("c")), []string{"a", "b"})
	requireFreezeSortRunReverseIterator(t, table.NewReverseIterator(nil, []byte("c")), []string{"b", "a"})
}

func TestFreezeSortRunIteratorAdvertisesTrustedMaterializeFastPath(t *testing.T) {
	table := newFreezeSortRunTable()
	key := []byte("a")
	value := []byte("value-a")
	table.SetSteal(key, value)
	table.Set([]byte("b"), []byte("value-b"))
	table.Freeze()

	it := table.NewIterator(nil, nil)
	stable, ok := it.(interface {
		StableUnsafeIteratorSlices() bool
	})
	if !ok || !stable.StableUnsafeIteratorSlices() {
		t.Fatalf("freeze-sort iterator exposes stable=%v, want stable unsafe slices", ok)
	}
	trusted, ok := it.(interface {
		OrderedUniqueUnsafeIterator() bool
	})
	if !ok || !trusted.OrderedUniqueUnsafeIterator() {
		t.Fatalf("freeze-sort iterator exposes trusted=%v, want ordered unique", ok)
	}
	lenHint, ok := it.(interface {
		Len() int
	})
	if !ok {
		t.Fatal("freeze-sort iterator does not expose Len hint")
	}
	if got := lenHint.Len(); got != 2 {
		t.Fatalf("freeze-sort iterator Len=%d want 2", got)
	}

	delta, err := backenddb.OrderedRootDeltaBatchFromIterator(it)
	if err != nil {
		t.Fatalf("materialize delta: %v", err)
	}
	defer func() { _ = delta.Close() }()
	if err := it.Close(); err != nil {
		t.Fatalf("close iterator: %v", err)
	}
	entries := delta.SortedEntries()
	if len(entries) != 2 {
		t.Fatalf("delta entries=%d want 2", len(entries))
	}
	if unsafe.SliceData(entries[0].Key) != unsafe.SliceData(key) {
		t.Fatal("trusted materialize path copied key instead of borrowing stable view")
	}
	if unsafe.SliceData(entries[0].Value) != unsafe.SliceData(value) {
		t.Fatal("trusted materialize path copied value instead of borrowing stable view")
	}

	ranged := table.NewIterator([]byte("b"), nil)
	defer func() { _ = ranged.Close() }()
	rangedLen, ok := ranged.(interface {
		Len() int
	})
	if !ok {
		t.Fatal("ranged iterator does not expose Len hint")
	}
	if got := rangedLen.Len(); got != 1 {
		t.Fatalf("ranged iterator Len=%d want 1", got)
	}
}

func TestFreezeSortRunTableApplyStealEntryFunc(t *testing.T) {
	table := newFreezeSortRunTable()
	appender, ok := table.(interface {
		ApplyStealEntryFunc(count int, emit func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error)) error
	})
	if !ok {
		t.Fatal("freeze-sort run table does not expose ApplyStealEntryFunc")
	}
	err := appender.ApplyStealEntryFunc(4, func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error) {
		switch i {
		case 0:
			return []byte("b"), []byte("old-b"), page.ValuePtr{}, node.FlagInline, nil
		case 1:
			return []byte("a"), []byte("value-a"), page.ValuePtr{}, node.FlagInline, nil
		case 2:
			return []byte("b"), []byte("new-b"), page.ValuePtr{}, node.FlagInline, nil
		default:
			return []byte("c"), nil, page.ValuePtr{}, node.FlagTombstone, nil
		}
	})
	if err != nil {
		t.Fatalf("ApplyStealEntryFunc: %v", err)
	}

	if got, _, flags, ok := table.GetEntry([]byte("b")); !ok || flags&node.FlagTombstone != 0 || !bytes.Equal(got, []byte("new-b")) {
		t.Fatalf("mutable GetEntry b=(%q,%02x,%v), want latest new-b", got, flags, ok)
	}
	table.Freeze()
	requireFreezeSortRunIterator(t, table.NewIterator(nil, nil), []string{"a", "b", "c"})
	if got := table.Len(); got != 3 {
		t.Fatalf("frozen Len=%d want 3", got)
	}
}

func TestFreezeSortRunTableReleaseReusesClearedEntryScratch(t *testing.T) {
	resetFreezeSortRunTablePoolForTest(t)

	table := newFreezeSortRunTable().(*freezeSortRunTable)
	for i := 0; i < 16; i++ {
		table.SetEntrySteal([]byte{byte('a' + i)}, []byte("old"), page.ValuePtr{}, node.FlagInline)
	}
	table.Freeze()
	if table.Len() != 16 {
		t.Fatalf("table Len=%d want 16 before release", table.Len())
	}
	capBefore := cap(table.entries)

	table.Release()
	table.Release()
	freezeSortRunTablePool.mu.Lock()
	pooledTables := len(freezeSortRunTablePool.tables)
	freezeSortRunTablePool.mu.Unlock()
	if pooledTables != 1 {
		t.Fatalf("pooled tables after double release=%d want 1", pooledTables)
	}

	reused := newFreezeSortRunTable().(*freezeSortRunTable)
	if reused != table {
		t.Fatal("freeze-sort table was not reused from pool")
	}
	if reused.Len() != 0 {
		t.Fatalf("reused table Len=%d want 0", reused.Len())
	}
	if reused.Size() != 0 {
		t.Fatalf("reused table Size=%d want 0", reused.Size())
	}
	if reused.frozen {
		t.Fatal("reused table is still frozen")
	}
	if cap(reused.entries) != capBefore {
		t.Fatalf("reused entry capacity=%d want %d", cap(reused.entries), capBefore)
	}

	reused.SetEntrySteal([]byte("z"), []byte("new"), page.ValuePtr{}, node.FlagInline)
	reused.Freeze()
	requireFreezeSortRunIterator(t, reused.NewIterator(nil, nil), []string{"z"})

	reused.Reset()
	reused.Release()
	freezeSortRunTablePool.mu.Lock()
	pooledTables = len(freezeSortRunTablePool.tables)
	freezeSortRunTablePool.mu.Unlock()
	if pooledTables != 1 {
		t.Fatalf("pooled tables after reacquire/reset/release=%d want 1", pooledTables)
	}
}

func TestFreezeSortRunTablePanicsOnMutationAfterRelease(t *testing.T) {
	resetFreezeSortRunTablePoolForTest(t)

	table := newFreezeSortRunTable().(*freezeSortRunTable)
	table.Set([]byte("a"), []byte("value-a"))
	table.Release()
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("Reset after Release did not panic")
		}
	}()
	table.Reset()
}

func resetFreezeSortRunTablePoolForTest(t *testing.T) {
	t.Helper()
	reset := func() {
		freezeSortRunTablePool.mu.Lock()
		freezeSortRunTablePool.tables = nil
		freezeSortRunTablePool.entryCapacity = 0
		freezeSortRunTablePool.mu.Unlock()
	}
	reset()
	t.Cleanup(reset)
}

func requireFreezeSortRunIterator(t *testing.T, it interface {
	Valid() bool
	Next()
	UnsafeKey() []byte
	Close() error
}, want []string) {
	t.Helper()
	defer func() { _ = it.Close() }()
	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, string(it.UnsafeKey()))
	}
	if len(got) != len(want) {
		t.Fatalf("iterator keys=%v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("iterator keys=%v want %v", got, want)
		}
	}
}

func requireFreezeSortRunReverseIterator(t *testing.T, it interface {
	Valid() bool
	Next()
	UnsafeKey() []byte
	Close() error
}, want []string) {
	t.Helper()
	requireFreezeSortRunIterator(t, it, want)
}
