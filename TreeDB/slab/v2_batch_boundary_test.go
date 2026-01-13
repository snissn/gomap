package slab

import (
	"bytes"
	"testing"
)

func TestSlabV2_AppendManyRespectsZoneBatchLimit(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSlabManagerWithOptions(dir, Options{
		Compression: CompressionOptions{
			Kind:            CompressionZSTD,
			MinBytes:        1,
			MinSavingsBytes: 1,
		},
		CompressionAdaptiveTrainBytes: -1,
	})
	if err != nil {
		t.Fatalf("new slab manager: %v", err)
	}
	defer func() { _ = sm.Close() }()

	const valueSize = 64 * 1024
	const recordCount = 64 // ~4MB payload total
	keys := make([][]byte, recordCount)
	values := make([][]byte, recordCount)
	for i := 0; i < recordCount; i++ {
		keys[i] = []byte{byte(i)}
		values[i] = bytes.Repeat([]byte("v"), valueSize)
	}

	if _, err := sm.AppendMany(keys, values); err != nil {
		t.Fatalf("append many: %v", err)
	}
}
