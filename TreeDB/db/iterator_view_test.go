package db

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func sameBacking(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	return unsafe.Pointer(&a[0]) == unsafe.Pointer(&b[0])
}

func TestDBIteratorKeyValueReturnViewsAndCopiesRemainOwned(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:        dir,
		Durability: DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	keyA := []byte("scan/a")
	valA := []byte("value-log-backed-value-a")
	keyB := []byte("scan/b")
	valB := []byte("value-log-backed-value-b")
	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("mkdir value log: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	valueLogPath := filepath.Join(valueLogDir, "value-l0-000001.log")
	vw, err := valuelog.NewWriter(valueLogPath, fileID)
	if err != nil {
		t.Fatalf("new value log writer: %v", err)
	}
	ptrA, err := vw.Append(0, keyA, 1, valA)
	if err != nil {
		_ = vw.Close()
		t.Fatalf("append value A: %v", err)
	}
	ptrB, err := vw.Append(0, keyB, 2, valB)
	if err != nil {
		_ = vw.Close()
		t.Fatalf("append value B: %v", err)
	}
	if err := vw.Close(); err != nil {
		t.Fatalf("close value log writer: %v", err)
	}
	registerTestValueLogProducer(t, dir, valueLogPath, fileID)

	rawBatch := d.NewBatch()
	type pointerBatch interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
		Write() error
		Close() error
	}
	b, ok := rawBatch.(pointerBatch)
	if !ok {
		if rawBatch != nil {
			_ = rawBatch.Close()
		}
		t.Fatalf("NewBatch() returned %T, want SetPointer support", rawBatch)
	}
	defer b.Close()
	if err := b.SetPointer(keyA, ptrA); err != nil {
		t.Fatalf("SetPointer A: %v", err)
	}
	if err := b.SetPointer(keyB, ptrB); err != nil {
		t.Fatalf("SetPointer B: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write pointer batch: %v", err)
	}

	it, err := d.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	defer it.Close()
	if !it.Valid() {
		t.Fatalf("expected iterator to be valid")
	}

	keyView := it.Key()
	unsafeKey := it.UnsafeKey()
	if !bytes.Equal(keyView, keyA) {
		t.Fatalf("Key()=%q want %q", keyView, keyA)
	}
	if !sameBacking(keyView, unsafeKey) {
		t.Fatalf("Key() should return the current UnsafeKey view")
	}

	valueView := it.Value()
	unsafeValue := it.UnsafeValue()
	if err := it.Error(); err != nil {
		t.Fatalf("Value error: %v", err)
	}
	if !bytes.Equal(valueView, valA) {
		t.Fatalf("Value()=%q want %q", valueView, valA)
	}
	if !sameBacking(valueView, unsafeValue) {
		t.Fatalf("Value() should return the current UnsafeValue view")
	}

	keyCopyBuf := make([]byte, 0, 64)
	valueCopyBuf := make([]byte, 0, 64)
	keyCopy := it.KeyCopy(keyCopyBuf)
	valueCopy := it.ValueCopy(valueCopyBuf)
	if !bytes.Equal(keyCopy, keyA) {
		t.Fatalf("KeyCopy()=%q want %q", keyCopy, keyA)
	}
	if !bytes.Equal(valueCopy, valA) {
		t.Fatalf("ValueCopy()=%q want %q", valueCopy, valA)
	}
	if sameBacking(keyCopy, keyView) {
		t.Fatalf("KeyCopy() must return caller-owned bytes, not the iterator view")
	}
	if sameBacking(valueCopy, valueView) {
		t.Fatalf("ValueCopy() must return caller-owned bytes, not the iterator view")
	}

	it.Next()
	if !it.Valid() {
		t.Fatalf("expected second iterator item")
	}
	if !bytes.Equal(it.Key(), keyB) {
		t.Fatalf("second Key()=%q want %q", it.Key(), keyB)
	}
	if !bytes.Equal(it.Value(), valB) {
		t.Fatalf("second Value()=%q want %q", it.Value(), valB)
	}
	if err := it.Error(); err != nil {
		t.Fatalf("second Value error: %v", err)
	}

	// Views from Key()/Value() are only valid until iterator movement. Copies are
	// caller-owned and must remain stable after movement.
	if !bytes.Equal(keyCopy, keyA) {
		t.Fatalf("KeyCopy changed after Next: got %q want %q", keyCopy, keyA)
	}
	if !bytes.Equal(valueCopy, valA) {
		t.Fatalf("ValueCopy changed after Next: got %q want %q", valueCopy, valA)
	}
}
