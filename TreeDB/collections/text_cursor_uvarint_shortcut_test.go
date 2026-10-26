package collections

import (
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestTextCursorReadUvarintShortcutParity(t *testing.T) {
	// Exact pre-shortcut helper: keep the maintained binary decoder as oracle.
	before := func(c *textCursor) (uint64, error) {
		if c == nil || c.pos >= len(c.buf) {
			return 0, io.ErrUnexpectedEOF
		}
		value, n := binary.Uvarint(c.buf[c.pos:])
		if n == 0 {
			return 0, io.ErrUnexpectedEOF
		}
		if n < 0 {
			return 0, errors.New("varint overflow")
		}
		c.pos += n
		return value, nil
	}
	errorText := func(err error) string {
		if err == nil {
			return ""
		}
		return err.Error()
	}
	check := func(raw []byte, start int, nilCursor bool) {
		t.Helper()
		gotCursor, wantCursor := &textCursor{buf: raw, pos: start}, &textCursor{buf: raw, pos: start}
		if nilCursor {
			gotCursor, wantCursor = nil, nil
		}
		got, gotErr := gotCursor.readUvarint()
		want, wantErr := before(wantCursor)
		gotPos, wantPos := start, start
		if !nilCursor {
			gotPos, wantPos = gotCursor.pos, wantCursor.pos
		}
		if got != want || gotPos != wantPos || (gotErr == nil) != (wantErr == nil) || errorText(gotErr) != errorText(wantErr) || errors.Is(gotErr, io.ErrUnexpectedEOF) != errors.Is(wantErr, io.ErrUnexpectedEOF) {
			t.Fatalf("raw=%x start=%d nil=%t: got (%d, %v, pos=%d), want (%d, %v, pos=%d)", raw, start, nilCursor, got, gotErr, gotPos, want, wantErr, wantPos)
		}
	}
	check(nil, 0, true)
	check(nil, 0, false)
	check([]byte{1}, 1, false)
	check([]byte{1}, 2, false)
	for b := 0; b < 256; b++ {
		check([]byte{byte(b)}, 0, false)
	}

	values := []uint64{0, 1, 127, 128, 255, ^uint64(0)}
	for shift := uint(7); shift < 64; shift += 7 {
		boundary := uint64(1) << shift
		values = append(values, boundary-1, boundary, boundary+1)
	}
	var encodings [][]byte
	for _, value := range values {
		encodings = append(encodings, binary.AppendUvarint(nil, value))
	}
	// The old decoder accepts noncanonical encodings; preserve that contract.
	for width := 2; width <= binary.MaxVarintLen64; width++ {
		raw := make([]byte, width)
		for i := 0; i < width-1; i++ {
			raw[i] = 0x80
		}
		encodings = append(encodings, raw)
	}
	encodings = append(encodings, []byte{0x81, 0}, []byte{0xff, 0})
	for _, last := range []byte{2, 0x7f, 0x80} {
		raw := make([]byte, binary.MaxVarintLen64)
		for i := range raw {
			raw[i] = 0xff
		}
		raw[len(raw)-1] = last
		encodings = append(encodings, raw, append(append([]byte(nil), raw...), 0))
	}
	for _, raw := range encodings {
		// Check every truncation and full input, both at zero and an offset.
		for end := 0; end <= len(raw); end++ {
			check(raw[:end], 0, false)
			framed := append([]byte{0x42, 0x43}, raw[:end]...)
			check(framed, 2, false)
		}
		// A following field must not be consumed by a successful read.
		check(append(append([]byte(nil), raw...), 0x11, 0x80), 0, false)
	}
}
