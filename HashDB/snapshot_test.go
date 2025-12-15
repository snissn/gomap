package hashdb

import (
	"bytes"
	"testing"
)

func TestSnapshotExportRestore_DB(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenSingle(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ops := []BatchOp{
		PutOp([]byte("a"), []byte("va1")),
		PutOp([]byte("b"), []byte("vb1")),
		PutOp([]byte("c"), []byte("vc1")),
		DeleteOp([]byte("c")),
		PutOp([]byte("b"), []byte("vb2")),
	}
	if err := db.ApplyBatchSync(ops); err != nil {
		t.Fatalf("apply batch: %v", err)
	}

	var buf bytes.Buffer
	if err := db.Export(&buf); err != nil {
		t.Fatalf("export: %v", err)
	}

	restoreDir := t.TempDir()
	dst, err := OpenSingle(restoreDir)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	t.Cleanup(func() { _ = dst.Close() })

	if err := dst.Restore(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got, err := dst.Get([]byte("a"))
	if err != nil || string(got) != "va1" {
		t.Fatalf("a: got=%q err=%v", string(got), err)
	}
	got, err = dst.Get([]byte("b"))
	if err != nil || string(got) != "vb2" {
		t.Fatalf("b: got=%q err=%v", string(got), err)
	}
	got, err = dst.Get([]byte("c"))
	if err != nil || got != nil {
		t.Fatalf("c: got=%q err=%v", string(got), err)
	}
}

func TestSnapshotExportRestore_Shared(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenWithShards(dir, 8)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ops := []BatchOp{
		PutOp([]byte("a"), []byte("va1")),
		PutOp([]byte("b"), []byte("vb1")),
		PutOp([]byte("c"), []byte("vc1")),
		DeleteOp([]byte("c")),
		PutOp([]byte("b"), []byte("vb2")),
	}
	if err := db.ApplyBatchSync(ops); err != nil {
		t.Fatalf("apply batch: %v", err)
	}

	var buf bytes.Buffer
	if err := db.Export(&buf); err != nil {
		t.Fatalf("export: %v", err)
	}

	restoreDir := t.TempDir()
	dst, err := OpenWithShards(restoreDir, 8)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	t.Cleanup(func() { _ = dst.Close() })

	if err := dst.Restore(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got, err := dst.Get([]byte("a"))
	if err != nil || string(got) != "va1" {
		t.Fatalf("a: got=%q err=%v", string(got), err)
	}
	got, err = dst.Get([]byte("b"))
	if err != nil || string(got) != "vb2" {
		t.Fatalf("b: got=%q err=%v", string(got), err)
	}
	got, err = dst.Get([]byte("c"))
	if err != nil || got != nil {
		t.Fatalf("c: got=%q err=%v", string(got), err)
	}
}
