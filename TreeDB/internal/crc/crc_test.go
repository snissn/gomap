package crc

import (
	"errors"
	"hash/crc32"
	"testing"
)

func TestChecksumCastagnoli(t *testing.T) {
	data := []byte("hello world")
	want := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	got := Checksum(data)
	if got != want {
		t.Fatalf("Checksum mismatch: got %08x want %08x", got, want)
	}
}

func TestVerify(t *testing.T) {
	data := []byte("verify me")
	want := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))

	if err := Verify(data, want); err != nil {
		t.Fatalf("Verify unexpected error: %v", err)
	}
	if err := Verify(data, want+1); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("Verify expected ErrChecksumMismatch, got %v", err)
	}
}

