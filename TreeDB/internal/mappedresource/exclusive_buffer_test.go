package mappedresource

import (
	"bytes"
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
