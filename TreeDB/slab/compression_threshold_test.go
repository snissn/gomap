package slab

import (
	"bytes"
	"testing"
)

func TestSlabCompression_Threshold(t *testing.T) {
	dir := t.TempDir()

	// Options with default MinBytes (0 -> 256)
	opts := Options{
		Compression: CompressionOptions{
			Kind: CompressionZSTD,
			// MinBytes: 0,
		},
	}
	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("NewSlabManager: %v", err)
	}
	defer sm.Close()

	key := []byte("key")

	// 1. Write Small Value (100 bytes) -> Should NOT compress
	valSmall := bytes.Repeat([]byte{0xAA}, 100)
	ptr1, _ := sm.Append(key, valSmall)
	ptr2, _ := sm.Append(key, valSmall)

	distSmall := int(ptr2.Offset - ptr1.Offset)
	t.Logf("Small Value (100 bytes): Stored Size = %d", distSmall)

	if distSmall < 100 {
		t.Errorf("Small value compressed unexpectedly! Size: %d", distSmall)
	}

	// 2. Write Large Value (300 bytes) -> Should compress
	valLarge := bytes.Repeat([]byte{0xBB}, 300)
	ptr3, _ := sm.Append(key, valLarge)
	ptr4, _ := sm.Append(key, valLarge)

	distLarge := int(ptr4.Offset - ptr3.Offset)
	t.Logf("Large Value (300 bytes): Stored Size = %d", distLarge)

	if distLarge >= 300 {
		t.Errorf("Large value NOT compressed! Size: %d", distLarge)
	}
}
