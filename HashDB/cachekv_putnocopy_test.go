package hashdb

import (
	"bytes"
	"testing"
	"unsafe"
)

type putManyRecorder struct {
	keys [][]byte
	vals [][]byte
}

func (p *putManyRecorder) Get(key []byte) ([]byte, error) { return nil, nil }
func (p *putManyRecorder) Put(key, value []byte) error    { return nil }
func (p *putManyRecorder) Delete(key []byte) error        { return nil }
func (p *putManyRecorder) PutMany(keys, vals [][]byte) error {
	p.keys = append(p.keys, keys...)
	p.vals = append(p.vals, vals...)
	return nil
}

func TestCacheKVPutNoCopyValue_StoresKeyAndFlushUsesEntryKey(t *testing.T) {
	backend := &putManyRecorder{}
	c := NewCacheKV(backend, 100, 1<<20, 0)

	key := []byte("k1")
	val := bytes.Repeat([]byte("v"), 16)

	if err := c.PutNoCopyValue(key, val); err != nil {
		t.Fatalf("PutNoCopyValue: %v", err)
	}

	lookup := bytesToString(key)
	c.mu.RLock()
	e, ok := c.pending[lookup]
	c.mu.RUnlock()
	if !ok {
		t.Fatalf("expected key in pending map")
	}
	if len(e.key) == 0 {
		t.Fatalf("expected entry.key to be populated")
	}
	if !bytes.Equal(e.key, []byte("k1")) {
		t.Fatalf("unexpected entry.key contents: %q", e.key)
	}
	// Ensure key bytes are copied (mutation of input key must not affect entry key).
	key[0] = 'X'
	if !bytes.Equal(e.key, []byte("k1")) {
		t.Fatalf("entry.key changed after mutating input key: %q", e.key)
	}

	// Value is retained (no-copy), but cap is clamped to len to prevent in-place growth.
	if uintptr(unsafe.Pointer(&e.value[0])) != uintptr(unsafe.Pointer(&val[0])) {
		t.Fatalf("expected entry.value to reuse caller buffer")
	}
	if cap(e.value) != len(e.value) {
		t.Fatalf("expected cap(value)==len(value), got cap=%d len=%d", cap(e.value), len(e.value))
	}

	entryKeyPtr := uintptr(unsafe.Pointer(&e.key[0]))
	if err := c.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if len(backend.keys) != 1 {
		t.Fatalf("expected 1 flushed key, got %d", len(backend.keys))
	}
	if !bytes.Equal(backend.keys[0], []byte("k1")) {
		t.Fatalf("unexpected flushed key: %q", backend.keys[0])
	}
	// Ensure Flush used the stored entry.key bytes, not a fresh []byte(k) allocation.
	flushedKeyPtr := uintptr(unsafe.Pointer(&backend.keys[0][0]))
	if flushedKeyPtr != entryKeyPtr {
		t.Fatalf("Flush did not use entry.key buffer (ptr %x != %x)", flushedKeyPtr, entryKeyPtr)
	}
}

func TestCacheKVPutNoCopyKeyValueUnsafe_DoesNotCopyKeyAndFlushUsesEntryKey(t *testing.T) {
	backend := &putManyRecorder{}
	c := NewCacheKV(backend, 100, 1<<20, 0)

	key := []byte("k1")
	val := bytes.Repeat([]byte("v"), 16)

	if err := c.PutNoCopyKeyValueUnsafe(key, val); err != nil {
		t.Fatalf("PutNoCopyKeyValueUnsafe: %v", err)
	}

	lookup := bytesToString(key)
	c.mu.RLock()
	e, ok := c.pending[lookup]
	c.mu.RUnlock()
	if !ok {
		t.Fatalf("expected key in pending map")
	}
	if len(e.key) == 0 {
		t.Fatalf("expected entry.key to be populated")
	}
	if !bytes.Equal(e.key, []byte("k1")) {
		t.Fatalf("unexpected entry.key contents: %q", e.key)
	}
	// Ensure key bytes are not copied.
	if uintptr(unsafe.Pointer(&e.key[0])) != uintptr(unsafe.Pointer(&key[0])) {
		t.Fatalf("expected entry.key to reuse caller buffer")
	}
	if cap(e.key) != len(e.key) {
		t.Fatalf("expected cap(key)==len(key), got cap=%d len=%d", cap(e.key), len(e.key))
	}

	// Value is retained (no-copy), but cap is clamped to len to prevent in-place growth.
	if uintptr(unsafe.Pointer(&e.value[0])) != uintptr(unsafe.Pointer(&val[0])) {
		t.Fatalf("expected entry.value to reuse caller buffer")
	}
	if cap(e.value) != len(e.value) {
		t.Fatalf("expected cap(value)==len(value), got cap=%d len=%d", cap(e.value), len(e.value))
	}

	entryKeyPtr := uintptr(unsafe.Pointer(&e.key[0]))
	if err := c.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if len(backend.keys) != 1 {
		t.Fatalf("expected 1 flushed key, got %d", len(backend.keys))
	}
	if !bytes.Equal(backend.keys[0], []byte("k1")) {
		t.Fatalf("unexpected flushed key: %q", backend.keys[0])
	}
	// Ensure Flush used the stored entry.key bytes, not a fresh []byte(k) allocation.
	flushedKeyPtr := uintptr(unsafe.Pointer(&backend.keys[0][0]))
	if flushedKeyPtr != entryKeyPtr {
		t.Fatalf("Flush did not use entry.key buffer (ptr %x != %x)", flushedKeyPtr, entryKeyPtr)
	}
}
