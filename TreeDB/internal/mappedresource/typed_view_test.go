package mappedresource

import (
	"encoding/binary"
	"testing"
	"unsafe"
)

func alignedBytes(size, align int) []byte {
	buf := make([]byte, size+align)
	base := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
	for off := 0; off < align; off++ {
		if (base+uintptr(off))%uintptr(align) == 0 {
			return buf[off : off+size]
		}
	}
	panic("no aligned offset found")
}

func misalignedBytes(size, align int) []byte {
	buf := alignedBytes(size+1, align)
	if uintptr(unsafe.Pointer(unsafe.SliceData(buf[1:])))%uintptr(align) != 0 {
		return buf[1:]
	}
	return buf[:size]
}

func TestDirectTypedViewsAcceptAlignedRanges(t *testing.T) {
	mgr := NewManager()
	key := testKey()
	scope := testScope()

	u16Bytes := alignedBytes(4, int(unsafe.Alignof(uint16(0))))
	binary.LittleEndian.PutUint16(u16Bytes[0:2], 3)
	binary.LittleEndian.PutUint16(u16Bytes[2:4], 5)
	key.Length = int64(len(u16Bytes))
	u16Handle, err := mgr.AcquireBytes(key, scope, SourceMapped, u16Bytes, AcquireOptions{Reason: "u16"})
	if err != nil {
		t.Fatalf("AcquireBytes u16: %v", err)
	}
	defer u16Handle.Release()
	u16, err := mgr.Uint16View(u16Handle)
	if err != nil {
		t.Fatalf("Uint16View: %v", err)
	}
	if len(u16) != 2 || u16[0] != 3 || u16[1] != 5 {
		t.Fatalf("Uint16View=%v", u16)
	}

	u32Bytes := alignedBytes(8, int(unsafe.Alignof(uint32(0))))
	binary.LittleEndian.PutUint32(u32Bytes[0:4], 7)
	binary.LittleEndian.PutUint32(u32Bytes[4:8], 11)
	key.Length = int64(len(u32Bytes))
	u32Handle, err := mgr.AcquireBytes(key, scope, SourceMapped, u32Bytes, AcquireOptions{Reason: "u32"})
	if err != nil {
		t.Fatalf("AcquireBytes u32: %v", err)
	}
	defer u32Handle.Release()
	u32, err := mgr.Uint32View(u32Handle)
	if err != nil {
		t.Fatalf("Uint32View: %v", err)
	}
	if len(u32) != 2 || u32[0] != 7 || u32[1] != 11 {
		t.Fatalf("Uint32View=%v", u32)
	}

	i64Bytes := alignedBytes(16, int(unsafe.Alignof(int64(0))))
	binary.LittleEndian.PutUint64(i64Bytes[0:8], uint64(13))
	binary.LittleEndian.PutUint64(i64Bytes[8:16], uint64(17))
	key.Length = int64(len(i64Bytes))
	i64Handle, err := mgr.AcquireBytes(key, scope, SourceMapped, i64Bytes, AcquireOptions{Reason: "i64"})
	if err != nil {
		t.Fatalf("AcquireBytes i64: %v", err)
	}
	defer i64Handle.Release()
	i64, err := mgr.Int64View(i64Handle)
	if err != nil {
		t.Fatalf("Int64View: %v", err)
	}
	if len(i64) != 2 || i64[0] != 13 || i64[1] != 17 {
		t.Fatalf("Int64View=%v", i64)
	}

	f32Bytes := alignedBytes(8, int(unsafe.Alignof(float32(0))))
	key.Length = int64(len(f32Bytes))
	f32Handle, err := mgr.AcquireBytes(key, scope, SourceMapped, f32Bytes, AcquireOptions{Reason: "f32"})
	if err != nil {
		t.Fatalf("AcquireBytes f32: %v", err)
	}
	defer f32Handle.Release()
	if f32, err := mgr.Float32View(f32Handle); err != nil || len(f32) != 2 {
		t.Fatalf("Float32View len=%d err=%v", len(f32), err)
	}

	f64Bytes := alignedBytes(16, int(unsafe.Alignof(float64(0))))
	key.Length = int64(len(f64Bytes))
	f64Handle, err := mgr.AcquireBytes(key, scope, SourceMapped, f64Bytes, AcquireOptions{Reason: "f64"})
	if err != nil {
		t.Fatalf("AcquireBytes f64: %v", err)
	}
	defer f64Handle.Release()
	if f64, err := mgr.Float64View(f64Handle); err != nil || len(f64) != 2 {
		t.Fatalf("Float64View len=%d err=%v", len(f64), err)
	}

	u64Bytes := alignedBytes(16, int(unsafe.Alignof(uint64(0))))
	binary.LittleEndian.PutUint64(u64Bytes[0:8], uint64(19))
	binary.LittleEndian.PutUint64(u64Bytes[8:16], uint64(23))
	key.Length = int64(len(u64Bytes))
	u64Handle, err := mgr.AcquireBytes(key, scope, SourceMapped, u64Bytes, AcquireOptions{Reason: "u64"})
	if err != nil {
		t.Fatalf("AcquireBytes u64: %v", err)
	}
	defer u64Handle.Release()
	u64, err := mgr.Uint64View(u64Handle)
	if err != nil {
		t.Fatalf("Uint64View: %v", err)
	}
	if len(u64) != 2 || u64[0] != 19 || u64[1] != 23 {
		t.Fatalf("Uint64View=%v", u64)
	}

	if stats := mgr.Stats(); stats.DirectViewSuccesses != 6 || stats.DirectViewFailures != 0 {
		t.Fatalf("direct view stats=%+v", stats)
	}
}

func TestDirectTypedViewsRejectMisalignedAndTruncatedRanges(t *testing.T) {
	mgr := NewManager()
	key := testKey()
	scope := testScope()

	misaligned := misalignedBytes(8, int(unsafe.Alignof(uint32(0))))
	key.Length = int64(len(misaligned))
	h, err := mgr.AcquireBytes(key, scope, SourceMapped, misaligned, AcquireOptions{Reason: "misaligned"})
	if err != nil {
		t.Fatalf("AcquireBytes misaligned: %v", err)
	}
	defer h.Release()
	if _, err := mgr.Uint32View(h); err == nil {
		t.Fatal("Uint32View misaligned err=nil, want failure")
	}

	truncated := alignedBytes(6, int(unsafe.Alignof(uint32(0))))
	key.Length = int64(len(truncated))
	truncatedHandle, err := mgr.AcquireBytes(key, scope, SourceMapped, truncated, AcquireOptions{Reason: "truncated"})
	if err != nil {
		t.Fatalf("AcquireBytes truncated: %v", err)
	}
	defer truncatedHandle.Release()
	if _, err := mgr.Uint32View(truncatedHandle); err == nil {
		t.Fatal("Uint32View truncated err=nil, want failure")
	}

	if err := truncatedHandle.Release(); err != nil {
		t.Fatalf("Release truncated: %v", err)
	}
	if _, err := mgr.Uint32View(truncatedHandle); err == nil {
		t.Fatal("Uint32View released handle err=nil, want failure")
	}

	if stats := mgr.Stats(); stats.DirectViewFailures != 3 {
		t.Fatalf("direct view failure stats=%+v", stats)
	}
}
