package slab

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestAppendLarge_ByteExactEncoding(t *testing.T) {
	dir := t.TempDir()
	mgr, _, err := Load(dir, 0, 0)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	k1 := []byte("k1")
	v1 := bytes.Repeat([]byte{0x11}, 150)
	ptr1, err := mgr.AppendLarge(k1, v1)
	if err != nil {
		t.Fatalf("AppendLarge(1): %v", err)
	}
	rec1, wantPtr1, err := EncodeRecordAt(k1, v1, 0, 0)
	if err != nil {
		t.Fatalf("EncodeRecordAt(1): %v", err)
	}
	if ptr1 != wantPtr1 {
		t.Fatalf("ptr1 mismatch: got %+v want %+v", ptr1, wantPtr1)
	}

	k2 := []byte("k2")
	v2 := bytes.Repeat([]byte{0x22}, 151)
	ptr2, err := mgr.AppendLarge(k2, v2)
	if err != nil {
		t.Fatalf("AppendLarge(2): %v", err)
	}
	rec2, wantPtr2, err := EncodeRecordAt(k2, v2, 0, uint64(len(rec1)))
	if err != nil {
		t.Fatalf("EncodeRecordAt(2): %v", err)
	}
	if ptr2 != wantPtr2 {
		t.Fatalf("ptr2 mismatch: got %+v want %+v", ptr2, wantPtr2)
	}

	got, err := os.ReadFile(filepath.Join(dir, "data-0000.slab"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	want := append(append([]byte(nil), rec1...), rec2...)
	if !bytes.Equal(got, want) {
		t.Fatalf("file bytes mismatch: got %d bytes want %d bytes", len(got), len(want))
	}
}

func TestAppendLarge_FallbackToWriteAtOnENOSYS(t *testing.T) {
	dir := t.TempDir()
	mgr, _, err := Load(dir, 0, 0)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	old := pwritevFunc
	calls := 0
	pwritevFunc = func(fd int, iovs [][]byte, offset int64) (int, error) {
		calls++
		return 0, unix.ENOSYS
	}
	t.Cleanup(func() { pwritevFunc = old })

	key := []byte("k")
	value := []byte("v")
	ptr, err := mgr.AppendLarge(key, value)
	if err != nil {
		t.Fatalf("AppendLarge: %v", err)
	}
	if calls == 0 {
		t.Fatalf("expected pwritev attempt")
	}

	rec, wantPtr, err := EncodeRecordAt(key, value, 0, 0)
	if err != nil {
		t.Fatalf("EncodeRecordAt: %v", err)
	}
	if ptr != wantPtr {
		t.Fatalf("ptr mismatch: got %+v want %+v", ptr, wantPtr)
	}

	got, err := os.ReadFile(filepath.Join(dir, "data-0000.slab"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, rec) {
		t.Fatalf("file bytes mismatch: got %d bytes want %d bytes", len(got), len(rec))
	}
}

func TestAppendLargeBatch_ByteExactEncoding(t *testing.T) {
	dir := t.TempDir()
	mgr, _, err := Load(dir, 0, 0)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	keys := [][]byte{
		[]byte("a"),
		[]byte("bb"),
		[]byte("ccc"),
	}
	values := [][]byte{
		bytes.Repeat([]byte{0x01}, 150),
		bytes.Repeat([]byte{0x02}, 150),
		bytes.Repeat([]byte{0x03}, 150),
	}

	ptrs, err := mgr.AppendLargeBatch(keys, values)
	if err != nil {
		t.Fatalf("AppendLargeBatch: %v", err)
	}
	if len(ptrs) != len(keys) {
		t.Fatalf("ptrs length: got %d want %d", len(ptrs), len(keys))
	}

	var want []byte
	var off uint64
	for i := range keys {
		rec, wantPtr, err := EncodeRecordAt(keys[i], values[i], 0, off)
		if err != nil {
			t.Fatalf("EncodeRecordAt(%d): %v", i, err)
		}
		if ptrs[i] != wantPtr {
			t.Fatalf("ptr[%d] mismatch: got %+v want %+v", i, ptrs[i], wantPtr)
		}
		want = append(want, rec...)
		off += uint64(len(rec))
	}

	got, err := os.ReadFile(filepath.Join(dir, "data-0000.slab"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("file bytes mismatch: got %d bytes want %d bytes", len(got), len(want))
	}
}
