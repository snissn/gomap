package caching

import (
	"testing"

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
