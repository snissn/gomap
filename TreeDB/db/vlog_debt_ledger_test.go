package db

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func TestValueLogDebtLedger_RebuildAndLoad(t *testing.T) {
	t.Setenv(envEnableVlogDebtLedger, "1")
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 310_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 320_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 256)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	closeNoErr(t, b)

	if err := db.rebuildValueLogDebtLedger(context.Background()); err != nil {
		t.Fatalf("rebuild debt ledger: %v", err)
	}
	liveByID, ok, err := db.liveBytesBySegmentFromDebtLedger(context.Background())
	if err != nil {
		t.Fatalf("live bytes from debt ledger: %v", err)
	}
	if !ok {
		t.Fatalf("expected debt ledger live bytes to be available")
	}
	if liveByID[ptrs1[0].FileID] <= 0 || liveByID[ptrs2[0].FileID] <= 0 {
		t.Fatalf("unexpected live bytes map: %+v", liveByID)
	}

	path := filepath.Join(dir, valueLogDebtLedgerFileName)
	if _, ok, err := loadValueLogDebtLedgerFromPath(path, db.currentCommitSeq()); err != nil {
		t.Fatalf("load persisted debt ledger: %v", err)
	} else if !ok {
		t.Fatalf("expected persisted debt ledger to load")
	}
}
