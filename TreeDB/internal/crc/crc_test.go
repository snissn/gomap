package crc

import (
	"bytes"
	"hash/crc32"
	"testing"
)

func TestChecksumPartsMatchesJoinedChecksum(t *testing.T) {
	parts := [][]byte{
		[]byte("alpha"),
		nil,
		[]byte(""),
		[]byte("beta"),
		[]byte("gamma"),
	}

	joined := bytes.Join(parts, nil)
	got := ChecksumParts(parts...)
	want := crc32.Checksum(joined, crc32.MakeTable(crc32.Castagnoli))
	if got != want {
		t.Fatalf("ChecksumParts()=%d, want %d", got, want)
	}
}

func TestUpdateIsIncrementalEquivalent(t *testing.T) {
	parts := [][]byte{
		[]byte("part-1"),
		[]byte("part-2"),
		[]byte("part-3"),
	}

	sum := uint32(0)
	for _, p := range parts {
		sum = Update(sum, p)
	}

	want := ChecksumParts(parts...)
	if sum != want {
		t.Fatalf("incremental sum=%d, want %d", sum, want)
	}
}

func TestUpdateEmptyIsNoop(t *testing.T) {
	const seed = uint32(12345)
	got := Update(seed, nil)
	if got != seed {
		t.Fatalf("Update(seed,nil)=%d, want %d", got, seed)
	}
}
