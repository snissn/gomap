package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestChooseValueLogDictWriteK_SmallValuesBoostK(t *testing.T) {
	db := &DB{}
	got := db.chooseValueLogDictWriteK(8, 100, 12800) // avg=128
	if got != 96 {
		t.Fatalf("chooseValueLogDictWriteK small values: got=%d want=96", got)
	}
}

func TestChooseValueLogDictWriteK_RespectsConfiguredMax(t *testing.T) {
	db := &DB{valueLogDictMaxK: 64}
	got := db.chooseValueLogDictWriteK(8, 100, 12800) // avg=128 -> wants 96, clamps to 64
	if got != 64 {
		t.Fatalf("chooseValueLogDictWriteK clamp: got=%d want=64", got)
	}
}

func TestChooseValueLogDictWriteK_LeavesLargeValuesAlone(t *testing.T) {
	db := &DB{}
	got := db.chooseValueLogDictWriteK(16, 10, 64<<10) // avg ~6.4KiB
	if got != 16 {
		t.Fatalf("chooseValueLogDictWriteK large values: got=%d want=16", got)
	}
}

func TestChooseValueLogDictWriteK_DefaultOpenClampsToDefaultMaxK(t *testing.T) {
	backendDir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: backendDir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer backend.Close()

	cacheDir := t.TempDir()
	cached, err := Open(cacheDir, backend, Options{})
	if err != nil {
		t.Fatalf("caching open: %v", err)
	}
	defer cached.Close()

	if got, want := cached.valueLogDictMaxK, 32; got != want {
		t.Fatalf("default valueLogDictMaxK: got=%d want=%d", got, want)
	}
	got := cached.chooseValueLogDictWriteK(8, 100, 12800) // avg=128 -> boost, then clamp to default max K
	if got != 32 {
		t.Fatalf("chooseValueLogDictWriteK default-open clamp: got=%d want=32", got)
	}
}

func TestChooseValueLogDictWriteK_DefaultOpenForcePointersWalOffUsesForcePointerMaxK(t *testing.T) {
	backendDir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: backendDir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer backend.Close()

	cacheDir := t.TempDir()
	cached, err := Open(cacheDir, backend, Options{
		DisableWAL:            true,
		AllowUnsafe:           true,
		ForceValueLogPointers: true,
	})
	if err != nil {
		t.Fatalf("caching open: %v", err)
	}
	defer cached.Close()

	if got, want := cached.valueLogDictMaxK, 128; got != want {
		t.Fatalf("force-pointer wal-off default valueLogDictMaxK: got=%d want=%d", got, want)
	}
	got := cached.chooseValueLogDictWriteK(8, 100, 12800) // avg=128 -> boost to higher default max K
	if got != 128 {
		t.Fatalf("chooseValueLogDictWriteK force-pointer wal-off default clamp: got=%d want=128", got)
	}
}

func TestChooseValueLogDictWriteK_ForcePointersWalOffBoostsHigher(t *testing.T) {
	db := &DB{
		forceValueLogPointers: true,
		disableJournal:        true,
		valueLogDictMaxK:      valuelog.MaxFrameK,
	}
	got := db.chooseValueLogDictWriteK(8, 100, 12800) // avg=128
	if got != 128 {
		t.Fatalf("chooseValueLogDictWriteK force-pointer wal-off boost: got=%d want=128", got)
	}
}

func TestChooseValueLogDictWriteK_ForcePointersWalOffRespectsMax(t *testing.T) {
	db := &DB{
		forceValueLogPointers: true,
		disableJournal:        true,
		valueLogDictMaxK:      64,
	}
	got := db.chooseValueLogDictWriteK(8, 100, 12800) // avg=128 -> wants 128, clamps to 64
	if got != 64 {
		t.Fatalf("chooseValueLogDictWriteK force-pointer wal-off clamp: got=%d want=64", got)
	}
}
