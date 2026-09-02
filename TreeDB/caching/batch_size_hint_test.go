package caching

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestNewBatchWithSize_NormalizesLargePublicHint(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		FlushThreshold: 1 << 20,
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const rawHint = 100_000
	b := db.NewBatchWithSize(rawHint)
	defer func() {
		if err := b.Close(); err != nil {
			t.Errorf("Close batch: %v", err)
		}
	}()

	want := backenddb.NormalizePublicBatchReserveHint(rawHint)
	gotCap := cap(b.entries)
	if gotCap < want {
		t.Fatalf("batch entry cap=%d want >= %d", gotCap, want)
	}
	if gotCap >= rawHint {
		t.Fatalf("batch entry cap=%d want < %d", gotCap, rawHint)
	}
	if got, wantCap := b.copyArenaCap, db.batchCopyArenaInitCap(want); got != wantCap {
		t.Fatalf("batch copyArenaCap=%d want %d", got, wantCap)
	}
	if got := b.copyArenaCap; got >= batchCopyArenaInitMax {
		t.Fatalf("batch copyArenaCap=%d want below max clamp for normalized hint", got)
	}
}
