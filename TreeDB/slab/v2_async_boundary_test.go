package slab

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestSlabV2_AsyncBoundaryCrossing(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Compression: CompressionOptions{
			Kind: CompressionZSTD,
		},
		OmitSlabKeys: true,
	}

	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("NewSlabManagerWithOptions: %v", err)
	}
	defer sm.Close()

	if sm.activeSlab.version != Version2 {
		t.Fatalf("expected V2 slab, got %d", sm.activeSlab.version)
	}

	payload := bytes.Repeat([]byte("x"), 128*1024)
	var firstPtr, lastPtr page.ValuePtr
	for sm.activeSlabWriter.Size() < ZoneSize-128*1024 {
		ptr, err := sm.AppendWithOptions(nil, payload, AppendOptions{DisableCompression: true})
		if err != nil {
			t.Fatalf("AppendWithOptions: %v", err)
		}
		if firstPtr.Offset == 0 {
			firstPtr = ptr
		}
		lastPtr = ptr
	}

	// Cross the boundary with another append (async writer still buffering).
	ptr, err := sm.AppendWithOptions(nil, payload, AppendOptions{DisableCompression: true})
	if err != nil {
		t.Fatalf("AppendWithOptions crossing: %v", err)
	}
	lastPtr = ptr

	if err := sm.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	val, err := sm.Read(firstPtr)
	if err != nil {
		t.Fatalf("Read first: %v", err)
	}
	if !bytes.Equal(val, payload) {
		t.Fatalf("first payload mismatch")
	}

	val, err = sm.Read(lastPtr)
	if err != nil {
		t.Fatalf("Read last: %v", err)
	}
	if !bytes.Equal(val, payload) {
		t.Fatalf("last payload mismatch")
	}
}
