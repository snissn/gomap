package caching

import (
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type readBarrierTrackingBackend struct {
	*MockBackend
	barrier func(fileID uint32) error
}

func (b *readBarrierTrackingBackend) SetCurrentValueLogReadBarrier(fn func(fileID uint32) error) {
	b.barrier = fn
}

func TestOpen_DoesNotInstallBackendReadBarrierWhenOpenFails(t *testing.T) {
	backend := &readBarrierTrackingBackend{MockBackend: NewMockBackend()}
	_, err := Open(t.TempDir(), backend, Options{
		DisableWAL:                 true,
		AllowUnsafe:                true,
		IndexOuterLeavesInValueLog: true,
	})
	if err == nil {
		t.Fatalf("expected open failure")
	}
	if !strings.Contains(err.Error(), "backend does not support value-log leaf pages") {
		t.Fatalf("open error=%v want missing leaf-page-log support", err)
	}
	if backend.barrier != nil {
		t.Fatalf("backend read barrier installed on failed open")
	}
}

func TestClose_KeepsBackendReadBarrierWhileSharedBackendStillOpen(t *testing.T) {
	backend := &readBarrierTrackingBackend{MockBackend: NewMockBackend()}
	opts := Options{
		DisableWAL:               true,
		AllowUnsafe:              true,
		ValueLogPointerThreshold: 1,
	}

	first, err := Open(t.TempDir(), backend, opts)
	if err != nil {
		t.Fatalf("open first: %v", err)
	}
	second, err := Open(t.TempDir(), backend, opts)
	if err != nil {
		_ = first.Close()
		t.Fatalf("open second: %v", err)
	}

	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		_ = second.Close()
		_ = first.Close()
		t.Fatalf("encode file id: %v", err)
	}
	if backend.barrier == nil {
		_ = second.Close()
		_ = first.Close()
		t.Fatalf("expected backend read barrier after opens")
	}
	if err := backend.barrier(fileID); err != nil {
		_ = second.Close()
		_ = first.Close()
		t.Fatalf("backend barrier pre-close: %v", err)
	}

	if err := first.Close(); err != nil {
		_ = second.Close()
		t.Fatalf("close first: %v", err)
	}
	if backend.barrier == nil {
		_ = second.Close()
		t.Fatalf("backend read barrier cleared while second DB still open")
	}
	if err := backend.barrier(fileID); err != nil {
		_ = second.Close()
		t.Fatalf("backend barrier after first close: %v", err)
	}

	if err := second.Close(); err != nil {
		t.Fatalf("close second: %v", err)
	}
	if backend.barrier != nil {
		t.Fatalf("backend read barrier not cleared after final close")
	}
}
