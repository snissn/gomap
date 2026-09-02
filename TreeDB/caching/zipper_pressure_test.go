package caching

import (
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/zipper"
)

type zipperPressureRecordingBackend struct {
	*MockBackend
	src zipper.ParallelMergePressureSource
}

func (b *zipperPressureRecordingBackend) SetZipperParallelMergePressureSource(src zipper.ParallelMergePressureSource) {
	b.src = src
}

func TestOpen_DisableWALWiresZipperParallelMergePressure(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()

	resetPoolPressureStateForTest()
	savedNow := poolPressureNow
	savedReadMemStats := poolPressureReadMemStats
	savedMemLimit := poolPressureMemoryLimit
	t.Cleanup(func() {
		poolPressureNow = savedNow
		poolPressureReadMemStats = savedReadMemStats
		poolPressureMemoryLimit = savedMemLimit
		resetPoolPressureStateForTest()
	})
	now := time.Unix(1, 0)
	poolPressureNow = func() time.Time { return now }
	fake := runtime.MemStats{
		HeapAlloc: 1 << 20,
		HeapInuse: 1 << 20,
		HeapSys:   2 << 20,
		Sys:       4 << 20,
	}
	poolPressureReadMemStats = func(ms *runtime.MemStats) { *ms = fake }
	poolPressureMemoryLimit = func() int64 { return 1 << 30 }

	dir := t.TempDir()
	backend := &zipperPressureRecordingBackend{MockBackend: NewMockBackend()}

	db, err := Open(dir, backend, Options{
		FlushThreshold: 1 << 20,
		DisableWAL:     true,
		AllowUnsafe:    true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if backend.src == nil {
		t.Fatalf("expected DisableWAL open to install a zipper pressure source")
	}
	if got := backend.src(); got != zipper.ParallelMergePressureNormal {
		t.Fatalf("pressure=%v want %v", got, zipper.ParallelMergePressureNormal)
	}
}

func TestOpen_WALOnLeavesZipperParallelMergePressureUnset(t *testing.T) {
	dir := t.TempDir()
	backend := &zipperPressureRecordingBackend{MockBackend: NewMockBackend()}

	db, err := Open(dir, backend, Options{
		FlushThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if backend.src != nil {
		t.Fatalf("expected WAL-on open to keep zipper pressure source unset")
	}
}
