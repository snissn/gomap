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
	defer b.Close()

	want := backenddb.NormalizePublicBatchReserveHint(rawHint)
	if got := cap(b.entries); got < want {
		t.Fatalf("batch entry cap=%d want >= %d", got, want)
	}
	if got := cap(b.entries); got >= rawHint {
		t.Fatalf("batch entry cap=%d want < %d", got, rawHint)
	}
}
