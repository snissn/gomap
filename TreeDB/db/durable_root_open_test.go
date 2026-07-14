package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestOpenFreshUsesOneDurableRootSlotAndReopens(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	idx := database.idx.Load()
	if idx == nil || idx.pager == nil {
		t.Fatal("missing index pager")
	}
	meta, err := readDurableMetaSlotV1(idx.pager, MetaPage0ID)
	if err != nil {
		t.Fatal(err)
	}
	if meta.CommitSeq != 1 || database.metaPageID != MetaPage0ID {
		t.Fatalf("initial commit/slot=(%d,%d), want (1,0)", meta.CommitSeq, database.metaPageID)
	}
	if _, err := readDurableMetaSlotV1(idx.pager, MetaPage1ID); err == nil || errors.Is(err, page.ErrDurableMetaLegacyFormat) {
		t.Fatalf("unused slot error=%v, want unsealed non-legacy slot", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state := reopened.State()
	if state == nil || state.CommitSeq != 1 || state.RootPageID < 2 || state.SystemRootPageID < 2 {
		t.Fatalf("reopened state=%+v", state)
	}
	if reopened.idx.Load().allocator.COWGenerationV1() == nil {
		t.Fatal("reopened allocator did not install selected COW generation")
	}
}

func TestDurableRootPublicationAlternatesIndependentSlotsAndReopens(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetSync([]byte("a"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if database.metaPageID != MetaPage1ID || database.durableRoot.slotCommit != [2]uint64{1, 2} {
		t.Fatalf("after first publish slot/commits=(%d,%v), want (1,[1 2])", database.metaPageID, database.durableRoot.slotCommit)
	}
	if err := database.SetSync([]byte("b"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if database.metaPageID != MetaPage0ID || database.durableRoot.slotCommit != [2]uint64{3, 2} {
		t.Fatalf("after second publish slot/commits=(%d,%v), want (0,[3 2])", database.metaPageID, database.durableRoot.slotCommit)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.metaPageID != MetaPage0ID || reopened.durableRoot.slotCommit != [2]uint64{3, 2} {
		t.Fatalf("reopened slot/commits=(%d,%v), want (0,[3 2])", reopened.metaPageID, reopened.durableRoot.slotCommit)
	}
	for key, want := range map[string]string{"a": "one", "b": "two"} {
		got, err := reopened.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("Get(%q)=%q, want %q", key, got, want)
		}
	}
}
