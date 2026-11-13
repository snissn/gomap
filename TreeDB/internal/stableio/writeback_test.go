package stableio

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

func TestBeginWritebackRangeAndErrors(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "writeback")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for _, tc := range []struct {
		name     string
		bytes    uint64
		outcomes []error
		want     error
		calls    int
	}{
		{name: "zero", bytes: 0},
		{name: "overflow", bytes: 1 << 63, want: os.ErrInvalid},
		{name: "maximum", bytes: 1<<63 - 1, calls: 1},
		{name: "interrupted", bytes: 7, outcomes: []error{syscall.EINTR, nil}, calls: 2},
		{name: "unavailable", bytes: 7, outcomes: []error{syscall.ENOSYS}, calls: 1},
		{name: "unsupported", bytes: 7, outcomes: []error{syscall.EOPNOTSUPP}, calls: 1},
		{name: "invalid", bytes: 7, outcomes: []error{syscall.EINVAL}, want: syscall.EINVAL, calls: 1},
		{name: "closed", bytes: 7, outcomes: []error{syscall.EBADF}, want: syscall.EBADF, calls: 1},
		{name: "io", bytes: 7, outcomes: []error{syscall.EIO}, want: syscall.EIO, calls: 1},
		{name: "space", bytes: 7, outcomes: []error{syscall.ENOSPC}, want: syscall.ENOSPC, calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := beginWriteback(file, tc.bytes, func(got *os.File, length int64) error {
				if got != file || length != int64(tc.bytes) {
					t.Fatalf("operand=%p/%d want %p/%d", got, length, file, tc.bytes)
				}
				calls++
				if calls <= len(tc.outcomes) {
					return tc.outcomes[calls-1]
				}
				return nil
			})
			if !errors.Is(err, tc.want) || calls != tc.calls {
				t.Fatalf("err=%v calls=%d want %v/%d", err, calls, tc.want, tc.calls)
			}
		})
	}
	if err := BeginWriteback(nil, 1); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("nil file=%v", err)
	}
	if n := testing.AllocsPerRun(100, func() {
		if err := beginWriteback(file, 1, func(*os.File, int64) error { return nil }); err != nil {
			panic(err)
		}
	}); n != 0 {
		t.Fatalf("hint bookkeeping allocates %g", n)
	}
}
