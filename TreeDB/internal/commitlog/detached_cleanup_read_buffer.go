package commitlog

import (
	"bufio"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

const cleanupReadWindowBytes = 64 << 10

// DetachedStreamCleanupReadBuffer serves only the frozen, already-identified
// WAL files of a detached cleanup. The journal can revoke their descriptors.
type DetachedStreamCleanupReadBuffer struct {
	CleanupReadBuffer
	fileIO   *sync.Mutex
	released *atomic.Bool
}

func NewDetachedStreamCleanupReadBuffer(fileIO *sync.Mutex, released *atomic.Bool) *DetachedStreamCleanupReadBuffer {
	return &DetachedStreamCleanupReadBuffer{fileIO: fileIO, released: released}
}

// Open borrows one validated ordinary stream, positioned at its first byte.
func (b *DetachedStreamCleanupReadBuffer) Open(file *os.File, opts Options) (*Reader, error) {
	if file == nil {
		return nil, os.ErrInvalid
	}
	b.file = file
	b.offset = 0
	physical := (*detachedCleanupPhysicalReader)(b)
	if b.input == nil {
		b.input = bufio.NewReaderSize(physical, cleanupReadWindowBytes)
	} else {
		b.input.Reset(physical)
	}
	return &Reader{input: b, f: file, maxSegmentSize: normalizeMaxSegmentSize(opts.MaxSegmentSize)}, nil
}

type detachedCleanupPhysicalReader DetachedStreamCleanupReadBuffer

func (b *detachedCleanupPhysicalReader) Read(p []byte) (int, error) {
	b.fileIO.Lock()
	defer b.fileIO.Unlock()
	if b.released.Load() {
		return 0, os.ErrClosed
	}
	// Limit direct reads too, so yielding waits for at most one read window.
	return b.file.Read(p[:min(len(p), cleanupReadWindowBytes)])
}

func (b *DetachedStreamCleanupReadBuffer) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekCurrent {
		if offset == 0 {
			return b.offset, nil
		}
		offset -= int64(b.input.Buffered())
	}
	b.fileIO.Lock()
	defer b.fileIO.Unlock()
	if b.released.Load() {
		return 0, os.ErrClosed
	}
	position, err := b.file.Seek(offset, whence)
	if err == nil {
		b.input.Reset((*detachedCleanupPhysicalReader)(b))
		b.offset = position
	}
	return position, err
}
