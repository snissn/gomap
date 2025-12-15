package hashdb

import (
	"bytes"
	"io"
	"testing"
)

type shortWriter struct {
	w       io.Writer
	maxOnce int
}

func (s shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n := len(p)
	if n > s.maxOnce {
		n = s.maxOnce
	}
	return s.w.Write(p[:n])
}

func TestWriteAllHandlesShortWrites(t *testing.T) {
	var dst bytes.Buffer
	w := shortWriter{w: &dst, maxOnce: 3}

	src := []byte("abcdefghijklmnopqrstuvwxyz")
	n, err := writeAll(w, src)
	if err != nil {
		t.Fatalf("writeAll: %v", err)
	}
	if n != len(src) {
		t.Fatalf("writeAll wrote %d bytes, want %d", n, len(src))
	}
	if !bytes.Equal(dst.Bytes(), src) {
		t.Fatalf("writeAll content mismatch: got %q want %q", dst.String(), string(src))
	}
}
