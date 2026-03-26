package caching

import (
	"strings"
	"testing"
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
