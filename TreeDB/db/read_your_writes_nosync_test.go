package db

import (
	"bytes"
	"testing"
)

func TestReadYourWrites_NoSyncDoesNotFlushSnapshots(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	val := bytes.Repeat([]byte("x"), 128*1024)
	if err := db.Set([]byte("k"), val); err != nil {
		t.Fatalf("set: %v", err)
	}

	snap := db.AcquireSnapshot()
	defer snap.Close()

	got, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("value mismatch")
	}
}
