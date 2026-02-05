package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/limits"
)

func TestValueLogDictTargetK(t *testing.T) {
	db := &DB{valueLogDictFrameTargetBytes: 16 << 10}
	got := db.valueLogDictTargetK(128*1024, 1024) // avg=128B, target=16KiB => k=128
	if got != 128 {
		t.Fatalf("valueLogDictTargetK: got=%d want=%d", got, 128)
	}

	if got := db.valueLogDictTargetK(0, 1024); got != 0 {
		t.Fatalf("valueLogDictTargetK raw=0: got=%d want=0", got)
	}
	if got := db.valueLogDictTargetK(128*1024, 1); got != 0 {
		t.Fatalf("valueLogDictTargetK records=1: got=%d want=0", got)
	}
	if got := db.valueLogDictTargetK(128*1024, 1024); got <= 0 {
		t.Fatalf("valueLogDictTargetK: expected positive K, got=%d", got)
	}
}

func TestClampValueLogFrameKByMaxRecordSize(t *testing.T) {
	old := limits.MaxRecordSize
	limits.MaxRecordSize = 1024
	t.Cleanup(func() { limits.MaxRecordSize = old })

	db := &DB{}
	// With a 1KiB max record size and 256B max values, we should cap K to 4.
	got := db.clampValueLogFrameKByMaxRecordSize(128, 256)
	if got != 4 {
		t.Fatalf("clampValueLogFrameKByMaxRecordSize: got=%d want=%d", got, 4)
	}

	if got := db.clampValueLogFrameKByMaxRecordSize(1, 256); got != 1 {
		t.Fatalf("clampValueLogFrameKByMaxRecordSize k=1: got=%d want=1", got)
	}
	if got := db.clampValueLogFrameKByMaxRecordSize(128, 0); got != 128 {
		t.Fatalf("clampValueLogFrameKByMaxRecordSize maxValueLen=0: got=%d want=128", got)
	}
}
