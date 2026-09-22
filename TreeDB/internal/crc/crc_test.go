package crc

import (
	"bytes"
	stdcrc32 "hash/crc32"
	"math/rand"
	"testing"
)

func TestChecksumIEEEKnownVectors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		data []byte
		want uint32
	}{
		{name: "empty", data: nil, want: 0x00000000},
		{name: "digits", data: []byte("123456789"), want: 0xcbf43926},
		{name: "hello_world", data: []byte("hello world"), want: 0x0d4a1185},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Checksum(tc.data); got != tc.want {
				t.Fatalf("Checksum(%q)=0x%08x want 0x%08x", tc.data, got, tc.want)
			}
			if got := ChecksumParts(tc.data); got != tc.want {
				t.Fatalf("ChecksumParts(%q)=0x%08x want 0x%08x", tc.data, got, tc.want)
			}
		})
	}
}

func TestChecksumIEEERandomizedCompatibility(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(0x1851c0de))
	lengths := []int{0, 1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1024, 4096, 65536}
	for caseNum := 0; caseNum < 200; caseNum++ {
		lengths = append(lengths, rng.Intn(128*1024))
	}

	for _, n := range lengths {
		data := make([]byte, n)
		if _, err := rng.Read(data); err != nil {
			t.Fatalf("rng.Read(%d): %v", n, err)
		}
		want := stdcrc32.ChecksumIEEE(data)
		if got := Checksum(data); got != want {
			t.Fatalf("Checksum len=%d got=0x%08x want stdlib=0x%08x", n, got, want)
		}
	}
}

func TestUpdateAndChecksumPartsStreamingCompatibility(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(0x18515ea1))
	data := make([]byte, 256*1024+333)
	if _, err := rng.Read(data); err != nil {
		t.Fatalf("rng.Read: %v", err)
	}
	table := stdcrc32.MakeTable(stdcrc32.IEEE)

	for _, chunks := range [][]int{
		{len(data)},
		{0, 1, 0, 2, 3, 5, 8, 13, 21, 34, 55, 89},
		{64, 256, 4096, 16 * 1024, 64 * 1024, 1 << 20},
	} {
		var gotUpdate uint32
		var wantUpdate uint32
		parts := make([][]byte, 0, len(chunks)+1)
		off := 0
		for off < len(data) {
			for _, chunk := range chunks {
				end := off + chunk
				if end > len(data) {
					end = len(data)
				}
				part := data[off:end]
				parts = append(parts, part)
				gotUpdate = Update(gotUpdate, part)
				wantUpdate = stdcrc32.Update(wantUpdate, table, part)
				off = end
				if off == len(data) {
					break
				}
			}
		}
		if gotUpdate != wantUpdate {
			t.Fatalf("Update chunks=%v got=0x%08x want stdlib=0x%08x", chunks, gotUpdate, wantUpdate)
		}
		if gotParts := ChecksumParts(parts...); gotParts != wantUpdate {
			t.Fatalf("ChecksumParts chunks=%v got=0x%08x want stdlib=0x%08x", chunks, gotParts, wantUpdate)
		}
		if gotWhole := Checksum(bytes.Join(parts, nil)); gotWhole != wantUpdate {
			t.Fatalf("Checksum joined chunks=%v got=0x%08x want stdlib=0x%08x", chunks, gotWhole, wantUpdate)
		}
	}
}
