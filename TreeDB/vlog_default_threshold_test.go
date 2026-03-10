package treedb

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
)

func TestDefaultValueLogPointerThreshold_RelaxedDurability_Keeps128BInline(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:            dir,
		FlushThreshold: 1,
		Durability:     db.DurabilityWALOffRelaxed,
		// ValueLog.PointerThreshold left at 0 (default).
	}

	dbh, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer dbh.Close()

	key := []byte("k1")
	val := bytes.Repeat([]byte("v"), 128)
	if err := dbh.Set(key, val); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := dbh.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	snap := dbh.backend.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("snapshot nil")
	}
	entry, err := snap.GetEntry(key)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("GetEntry: %v", err)
	}
	if entry.Flags&node.FlagPointer != 0 {
		_ = snap.Close()
		t.Fatalf("expected inline value for 128B value under relaxed durability, got flags=%#x file_id=%#x", entry.Flags, entry.ValuePtr.FileID)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot close: %v", err)
	}

	got, err := dbh.backend.Get(key)
	if err != nil {
		t.Fatalf("backend Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("backend Get mismatch")
	}
}

func TestDefaultValueLogPointerThreshold_Durable_Keeps128BInline(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:            dir,
		FlushThreshold: 1,
		Durability:     db.DurabilityDurable,
		// ValueLog.PointerThreshold left at 0 (default).
	}

	dbh, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer dbh.Close()

	key := []byte("k1")
	val := bytes.Repeat([]byte("v"), 128)
	if err := dbh.Set(key, val); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := dbh.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	snap := dbh.backend.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("snapshot nil")
	}
	entry, err := snap.GetEntry(key)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("GetEntry: %v", err)
	}
	if entry.Flags&node.FlagPointer != 0 {
		_ = snap.Close()
		t.Fatalf("expected inline value for 128B value under durable mode, got flags=%#x file_id=%#x", entry.Flags, entry.ValuePtr.FileID)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot close: %v", err)
	}

	got, err := dbh.backend.Get(key)
	if err != nil {
		t.Fatalf("backend Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("backend Get mismatch")
	}
}
