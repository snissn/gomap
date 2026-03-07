package db

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestApplySystemTable_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	table, err := memtable.NewWithCapacityMode(0, memtable.ModeBTree)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	table.SetEntrySteal([]byte("sys:users"), []byte("v1"), page.ValuePtr{}, 0)

	if err := db.ApplySystemTable(true, table); err != nil {
		t.Fatalf("ApplySystemTable: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.GetSystem([]byte("sys:users"))
	if err != nil {
		t.Fatalf("GetSystem: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("GetSystem=%q want %q", got, []byte("v1"))
	}
}
