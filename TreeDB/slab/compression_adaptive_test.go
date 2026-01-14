package slab

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestCompressionPauseAndProbeResume(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Compression: CompressionOptions{
			Kind: CompressionZSTD,
		},
		CompressionDisableFullRecord:         true,
		CompressionAdaptiveRatio:             0.99,
		CompressionMetricsWindowBytes:        8 << 10,
		CompressionAdaptivePauseBytes:        64 << 10,
		CompressionAdaptiveProbeBytes:        16 << 10,
		CompressionAdaptivePauseSampleStride: 64,
		CompressionAdaptiveTrainBytes:        -1,
	}
	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("open slab manager: %v", err)
	}
	defer sm.Close()

	rng := rand.New(rand.NewSource(1))
	key := []byte("key")
	noise := make([]byte, 4<<10)

	for i := 0; i < 16; i++ {
		rng.Read(noise)
		if _, err := sm.Append(key, noise); err != nil {
			t.Fatalf("append noise: %v", err)
		}
		if sm.compressionPauseRemaining.Load() > 0 {
			break
		}
	}

	if sm.compressionPauseRemaining.Load() == 0 {
		t.Fatalf("expected compression pause to trigger")
	}

	payload := bytes.Repeat([]byte("abcd1234"), 2048)
	resumed := false
	for i := 0; i < 200; i++ {
		ptr, err := sm.Append(key, payload)
		if err != nil {
			t.Fatalf("append payload: %v", err)
		}
		if sm.compressionPauseRemaining.Load() == 0 && page.ValuePtrIsCompressed(ptr) {
			resumed = true
			break
		}
	}
	if !resumed {
		t.Fatalf("expected compression to resume after probe")
	}
}
