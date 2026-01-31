package db

import (
	"bytes"
	"testing"
)

func TestDB_IsEmptyish_CommitSeqZero(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	emptyish, err := db.IsEmptyish()
	if err != nil {
		t.Fatalf("IsEmptyish: %v", err)
	}
	if !emptyish {
		t.Fatalf("expected emptyish on fresh DB")
	}
}

func TestDB_IsEmptyish_EmptyLeafAndOneUserPageIsTrue(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	val := bytes.Repeat([]byte("a"), 128)

	{
		b := db.NewBatch()
		for i := 0; i < 1000; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, val); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}

	{
		b := db.NewBatch()
		for i := 0; i < 1000; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Delete(k); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("delete write: %v", err)
		}
		_ = b.Close()
	}

	rep, err := db.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	if rep["treedb.user.pages"] != "1" {
		t.Fatalf("expected 1 reachable user page, got %s", rep["treedb.user.pages"])
	}

	emptyish, err := db.IsEmptyish()
	if err != nil {
		t.Fatalf("IsEmptyish: %v", err)
	}
	if !emptyish {
		t.Fatalf("expected emptyish after delete-all collapse")
	}
}

func TestDB_IsEmptyish_NonEmptyTreeIsFalse(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	{
		b := db.NewBatch()
		if err := b.Set([]byte("k"), []byte("v")); err != nil {
			t.Fatalf("set: %v", err)
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}

	emptyish, err := db.IsEmptyish()
	if err != nil {
		t.Fatalf("IsEmptyish: %v", err)
	}
	if emptyish {
		t.Fatalf("expected non-emptyish after write")
	}
}
