package caching

import (
	"errors"
	"testing"
)

func TestCachingLeafPageLog_AppendLeafPageReturnsAppendError(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:               true,
		AllowUnsafe:              true,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	leafLog := &cachingLeafPageLog{db: db, laneID: 0}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = leafLog.AppendLeafPage([]byte("leaf"))
	if !errors.Is(err, errWALClosed) {
		t.Fatalf("AppendLeafPage error=%v want %v", err, errWALClosed)
	}
}
