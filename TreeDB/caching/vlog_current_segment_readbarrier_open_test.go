package caching

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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

func TestOpen_FailsOnLegacyMixedWALValueLayout(t *testing.T) {
	for _, name := range []string{"value-l0-000001.log", "vlog-l0-000001.log"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			walDir := filepath.Join(dir, "wal")
			if err := os.MkdirAll(walDir, 0o700); err != nil {
				t.Fatalf("MkdirAll(wal): %v", err)
			}
			if err := os.WriteFile(filepath.Join(walDir, name), []byte("legacy"), 0o600); err != nil {
				t.Fatalf("WriteFile(value log): %v", err)
			}

			_, err := Open(dir, NewMockBackend(), Options{DisableWAL: true, AllowUnsafe: true})
			if err == nil {
				t.Fatalf("expected open failure")
			}
			if !strings.Contains(err.Error(), "split wal/value_vlog layout") {
				t.Fatalf("open error=%v want legacy mixed WAL/value-log layout", err)
			}
		})
	}
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

func TestBackendReadBarrier_SharedBackendFlushesAllCandidateDBs(t *testing.T) {
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
	defer func() {
		_ = second.Close()
		_ = first.Close()
	}()

	if backend.barrier == nil {
		t.Fatalf("expected backend read barrier after opens")
	}

	firstLane := &first.lanes[0]
	secondLane := &second.lanes[0]
	if _, _, err := first.appendValueLogOneInternal(firstLane, 0, nil, 1, []byte("first-buffered-tail"), journalDurabilityNone, false); err != nil {
		t.Fatalf("append first lane: %v", err)
	}
	if _, _, err := second.appendValueLogOneInternal(secondLane, 0, nil, 1, []byte("second-buffered-tail"), journalDurabilityNone, false); err != nil {
		t.Fatalf("append second lane: %v", err)
	}

	pendingBytes := func(l *lane) int {
		l.vlogMu.Lock()
		defer l.vlogMu.Unlock()
		if w, ok := l.vlog.(interface{ PendingBytes() int }); ok {
			return w.PendingBytes()
		}
		return 0
	}
	if got := pendingBytes(firstLane); got == 0 {
		t.Fatalf("expected first DB pending bytes > 0")
	}
	if got := pendingBytes(secondLane); got == 0 {
		t.Fatalf("expected second DB pending bytes > 0")
	}

	var firstFlushes atomic.Int32
	var secondFlushes atomic.Int32
	first.testOnVlogFlush = func(laneID int) {
		if laneID == 0 {
			firstFlushes.Add(1)
		}
	}
	second.testOnVlogFlush = func(laneID int) {
		if laneID == 0 {
			secondFlushes.Add(1)
		}
	}

	seq := first.currentValueLogSeq(firstLane)
	if seq <= 0 {
		t.Fatalf("unexpected first current value-log seq=%d", seq)
	}
	fileID, err := valuelog.EncodeFileID(0, uint32(seq))
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	if err := backend.barrier(fileID); err != nil {
		t.Fatalf("invoke backend barrier: %v", err)
	}

	if got := firstFlushes.Load(); got == 0 {
		t.Fatalf("expected first DB lane flush on shared barrier, got %d", got)
	}
	if got := secondFlushes.Load(); got == 0 {
		t.Fatalf("expected second DB lane flush on shared barrier, got %d", got)
	}
}
