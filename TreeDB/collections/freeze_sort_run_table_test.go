package collections

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
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
