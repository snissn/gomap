package page

import (
	"errors"
	"testing"

	"treedb/internal/crc"
)

func TestHeaderBodyCRC(t *testing.T) {
	pageBuf := make([]byte, PageSize)
	h, body, err := SplitPage(pageBuf)
	if err != nil {
		t.Fatalf("SplitPage error: %v", err)
	}

	copy(body, []byte("some body data"))
	h.SetBodyCRC(body)

	want := crc.Checksum(body)
	if h.CRC != want {
		t.Fatalf("SetBodyCRC mismatch: got %08x want %08x", h.CRC, want)
	}
	if err := h.VerifyBodyCRC(body); err != nil {
		t.Fatalf("VerifyBodyCRC unexpected error: %v", err)
	}

	body[0] ^= 0xff
	if err := h.VerifyBodyCRC(body); !errors.Is(err, crc.ErrChecksumMismatch) {
		t.Fatalf("VerifyBodyCRC expected mismatch, got %v", err)
	}
}

