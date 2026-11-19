package commitlog

import (
	"bufio"
	"errors"
	"io"
	"testing"
)

type cleanupReadHeaderError struct {
	prefix int
	err    error
	reads  int
}

func (r *cleanupReadHeaderError) Read(buf []byte) (int, error) {
	r.reads++
	if r.reads != 1 {
		return 0, io.EOF
	}
	return r.prefix, r.err
}

func TestCommandWALCleanupBoundedReaderHeaderError(t *testing.T) {
	readErr := errors.New("header read failed")
	for _, test := range []struct {
		name          string
		prefix        int
		failure, want error
	}{
		{"read error", 0, readErr, readErr},
		{"partial read error", 3, readErr, readErr},
		{"partial EOF", 3, io.EOF, ErrCommandWALTerminalTail},
		{"EOF", 0, io.EOF, io.EOF},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := &cleanupReadHeaderError{prefix: test.prefix, err: test.failure}
			buffer := &CleanupReadBuffer{input: bufio.NewReader(input)}
			reader := &Reader{input: buffer}
			if _, err := buffer.ReadValidatedBoundedCommandFrameLSN(reader, 1024); !errors.Is(err, test.want) || input.reads != 1 || buffer.offset != int64(test.prefix) {
				t.Fatalf("bounded header error=%v reads=%d offset=%d want=%v and one read (%d bytes)", err, input.reads, buffer.offset, test.want, test.prefix)
			}
		})
	}
}
