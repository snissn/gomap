package mappedresource

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestAcquireOpenFileRangeIntoExclusiveCapacityAndRelease(t *testing.T) {
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "asset"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	original := []byte("0123456789abcdef")
	if _, err := file.Write(original); err != nil {
		t.Fatal(err)
	}
	mgr := NewManager()
	dst := make([]byte, 16, 64)
	h, err := mgr.AcquireOpenFileRangeInto(testKey(), testScope(), file, dst, AcquireOptions{ValidationMode: ValidationVerify})
	if err != nil {
		t.Fatal(err)
	}
	if &h.Bytes()[0] != &dst[0] || !bytes.Equal(h.Bytes(), original) {
		t.Fatal("range did not use exclusively owned destination")
	}
	if stats := mgr.Stats(); stats.ActiveHeapCopyBytes != 64 || stats.ActiveHandles != 1 {
		t.Fatalf("capacity accounting: %+v", stats)
	}
	if len(mgr.PinSummary()) != 1 {
		t.Fatal("missing logical pin")
	}
	if err := h.Release(); err != nil {
		t.Fatal(err)
	}
	if h.Bytes() != nil || len(mgr.PinSummary()) != 0 || mgr.Stats().ActiveHeapCopyBytes != 0 {
		t.Fatal("released handle retained ownership")
	}
	changed := []byte("fedcba9876543210")
	if _, err := file.WriteAt(changed, 0); err != nil {
		t.Fatal(err)
	}
	next, err := mgr.AcquireOpenFileRangeInto(testKey(), testScope(), file, dst, AcquireOptions{ValidationMode: ValidationVerify})
	if err != nil {
		t.Fatal(err)
	}
	defer next.Release()
	if &next.Bytes()[0] != &dst[0] || !bytes.Equal(next.Bytes(), changed) {
		t.Fatal("recycled destination did not read fresh file contents")
	}
}

func TestAcquireOpenFileRangeIntoFailurePublishesNoHandle(t *testing.T) {
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "short"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write([]byte("short")); err != nil {
		t.Fatal(err)
	}
	mgr := NewManager()
	dst := bytes.Repeat([]byte{0xa5}, 64)
	h, err := mgr.AcquireOpenFileRangeInto(testKey(), testScope(), file, dst, AcquireOptions{})
	if err == nil || h != nil || mgr.ActiveHandles() != 0 || len(mgr.PinSummary()) != 0 {
		t.Fatal("failed read published ownership")
	}
	if !bytes.Equal(dst, bytes.Repeat([]byte{0xa5}, 64)) {
		t.Fatal("bounds failure modified destination")
	}
	if h, err := mgr.AcquireOpenFileRangeInto(testKey(), testScope(), file, dst[:0:4], AcquireOptions{}); err == nil || h != nil {
		t.Fatal("undersized destination was admitted")
	}
}

// Captured extent is metadata authority, never content immutability. These
// cases exercise refusal before write and failure after a legitimate old
// extent becomes unavailable; neither may publish ownership.
func TestAcquireOpenFileRangeIntoAtCapturedSizeAuthority(t *testing.T) {
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "captured"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	original := []byte("0123456789abcdef")
	if _, err := file.Write(original); err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	mgr := NewManager()
	dst := bytes.Repeat([]byte{0xa5}, 64)
	if h, err := mgr.AcquireOpenFileRangeIntoAtCapturedSize(testKey(), testScope(), file, dst, info.Size()-1, AcquireOptions{}); err == nil || h != nil {
		t.Fatal("out-of-captured-bounds range accepted")
	}
	if !bytes.Equal(dst, bytes.Repeat([]byte{0xa5}, 64)) {
		t.Fatal("captured bounds refusal modified destination")
	}
	h, err := mgr.AcquireOpenFileRangeIntoAtCapturedSize(testKey(), testScope(), file, dst, info.Size(), AcquireOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(h.Bytes(), original) || &h.Bytes()[0] != &dst[0] {
		t.Fatal("captured range did not use exact destination")
	}
	if err := h.Release(); err != nil {
		t.Fatal(err)
	}
	changed := []byte("fedcba9876543210")
	if _, err := file.WriteAt(changed, 0); err != nil {
		t.Fatal(err)
	}
	h, err = mgr.AcquireOpenFileRangeIntoAtCapturedSize(testKey(), testScope(), file, dst, info.Size(), AcquireOptions{})
	if err != nil || h == nil {
		t.Fatalf("fresh captured-size read: %v", err)
	}
	if !bytes.Equal(h.Bytes(), changed) || &h.Bytes()[0] != &dst[0] {
		t.Fatal("captured size certified stale content")
	}
	if err := h.Release(); err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(5); err != nil {
		t.Fatal(err)
	}
	h, err = mgr.AcquireOpenFileRangeIntoAtCapturedSize(testKey(), testScope(), file, dst, info.Size(), AcquireOptions{})
	if !errors.Is(err, io.ErrUnexpectedEOF) || h != nil || mgr.ActiveHandles() != 0 || len(mgr.PinSummary()) != 0 {
		t.Fatalf("truncated captured range published ownership: handle=%v error=%v", h, err)
	}
	// The partial destination is quarantined. Closing the descriptor must also
	// fail under its old captured size instead of certifying stale dst contents.
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if h, err := mgr.AcquireOpenFileRangeIntoAtCapturedSize(testKey(), testScope(), file, dst, info.Size(), AcquireOptions{}); err == nil || h != nil || mgr.ActiveHandles() != 0 {
		t.Fatal("closed captured descriptor accepted")
	}
}
