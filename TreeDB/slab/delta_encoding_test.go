package slab

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
)

func TestDeltaXorRoundTrip(t *testing.T) {
	prev := make([]byte, 64)
	curr := make([]byte, 64)
	for i := 0; i < len(prev); i++ {
		prev[i] = byte(i)
	}
	copy(curr, prev)
	binary.LittleEndian.PutUint64(curr[8:], 42)
	binary.LittleEndian.PutUint64(curr[32:], 99)

	delta, ok := encodeDeltaXor(prev, curr)
	if !ok {
		t.Fatal("expected delta encoding to succeed")
	}
	out, ok := applyDeltaXor(prev, delta)
	if !ok {
		t.Fatal("expected delta apply to succeed")
	}
	if !bytes.Equal(out, curr) {
		t.Fatalf("roundtrip mismatch")
	}
}

func BenchmarkDeltaXorEncodeHit(b *testing.B) {
	prev := make([]byte, 64)
	curr := make([]byte, 64)
	for i := 0; i < len(prev); i++ {
		prev[i] = byte(i)
	}
	copy(curr, prev)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(curr[8:], uint64(i))
		binary.LittleEndian.PutUint64(curr[40:], uint64(i+7))
		_, _ = encodeDeltaXor(prev, curr)
	}
}

func BenchmarkDeltaXorApplyHit(b *testing.B) {
	prev := make([]byte, 64)
	curr := make([]byte, 64)
	for i := 0; i < len(prev); i++ {
		prev[i] = byte(i)
	}
	copy(curr, prev)
	binary.LittleEndian.PutUint64(curr[8:], 123)
	binary.LittleEndian.PutUint64(curr[40:], 456)

	delta, ok := encodeDeltaXor(prev, curr)
	if !ok {
		b.Fatal("expected delta encoding to succeed")
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = applyDeltaXor(prev, delta)
	}
}

func BenchmarkDeltaXorEncodeMiss(b *testing.B) {
	prev := make([]byte, 64)
	curr := make([]byte, 64)
	for i := 0; i < len(prev); i++ {
		prev[i] = byte(i)
	}

	rnd := rand.New(rand.NewSource(1))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < len(curr); j++ {
			curr[j] = byte(rnd.Intn(256))
		}
		_, _ = encodeDeltaXor(prev, curr)
	}
}
